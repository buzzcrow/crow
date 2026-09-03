// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Election metrics tests for `PxLocalReplica`.
//!
//! Moved from `replica/election_metrics_test.rs` to the election unit binary.

use std::sync::{Arc, Mutex};

use crowdb_kv::cluster::local_replica::{PxLocalReplica, PxLocalReplicaRole};
use crowdb_kv::cluster::replica::StepDownRequestPayload;
use crowdb_kv::cluster::status::ElectionCounters;
use crowdb_kv::metrics::{MetricPoint, MetricsRegistry};

fn replica_with_registry() -> (PxLocalReplica, Arc<Mutex<MetricsRegistry>>) {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Follower);
    let registry = Arc::new(Mutex::new(MetricsRegistry::new()));
    replica.set_metrics_registry(&registry, 1, 1);
    (replica, registry)
}

fn election_total(registry: &Mutex<MetricsRegistry>) -> u64 {
    match registry
        .lock()
        .unwrap()
        .snapshot_named("s.1.g.1.paxos.elections.c", 1.0)
        .unwrap()
    {
        MetricPoint::Counter { total, .. } => total,
        point => panic!("unexpected metric: {point:?}"),
    }
}

fn counter_total(registry: &Mutex<MetricsRegistry>, name: &str) -> Option<u64> {
    match registry.lock().unwrap().snapshot_named(name, 1.0) {
        Some(MetricPoint::Counter { total, .. }) => Some(total),
        Some(point) => panic!("unexpected metric for {name}: {point:?}"),
        None => None,
    }
}

#[test]
fn election_count_bumps_on_become_candidate() {
    let (replica, registry) = replica_with_registry();
    let before = election_total(&registry);
    replica.become_candidate(1);
    replica.become_candidate(2);
    let after = election_total(&registry);
    assert_eq!(
        after - before,
        2,
        "two become_candidate calls should bump election_count by 2"
    );
}

#[test]
fn snapshot_reflects_current_term_and_role_state() {
    let replica = PxLocalReplica::new(7, PxLocalReplicaRole::Follower);
    replica.become_candidate(5);
    let snap = replica.election_state_view(3, ElectionCounters::default());
    assert_eq!(snap.current_term, 5);
    assert_eq!(snap.bulk_phase1_in_flight_slots, 3);
    assert!(snap.lease_remaining_ms.is_none(), "candidate has no lease");
    assert!(snap.last_heartbeat_age_ms.is_none());
}

#[test]
fn leader_snapshot_has_lease_remaining() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Leader);
    replica.extend_lease_read_until(std::time::Instant::now() + std::time::Duration::from_secs(10));
    let snap = replica.election_state_view(0, ElectionCounters::default());
    assert!(
        snap.lease_remaining_ms.is_some(),
        "leader with active lease should report remaining"
    );
}

#[test]
fn follower_snapshot_has_no_lease_remaining() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Follower);
    let snap = replica.election_state_view(0, ElectionCounters::default());
    assert!(snap.lease_remaining_ms.is_none(), "follower has no lease");
}

/// Candidate → leader → step-down must bump the election counter exactly
/// once (only the candidate transition), and the `ElectionStateView` must
/// track the role/term/lease state across the transitions. The replica
/// itself never bumps the step-down counters — the group election driver
/// owns those (covered by the store-level topology test).
#[test]
fn step_down_preserves_term_and_does_not_rebump_election_counter() {
    let (replica, registry) = replica_with_registry();
    replica.become_candidate(5);
    assert_eq!(
        election_total(&registry),
        1,
        "candidate transition bumps the counter once"
    );

    replica.become_leader();
    assert_eq!(
        election_total(&registry),
        1,
        "become_leader must not bump the election counter"
    );

    replica.extend_lease_read_until(std::time::Instant::now() + std::time::Duration::from_secs(10));
    let leader_view = replica.election_state_view(0, ElectionCounters::default());
    assert!(
        leader_view.lease_remaining_ms.is_some(),
        "leader with lease reports remaining"
    );
    assert_eq!(leader_view.current_term, 5);

    let reply = replica.handle_step_down(&StepDownRequestPayload {
        term: 5,
        target_leader_id: 1,
        reason: "metrics test".into(),
    });
    assert!(reply.accepted, "leader at matching term must accept");
    assert_eq!(replica.role(), PxLocalReplicaRole::Follower);
    assert_eq!(
        election_total(&registry),
        1,
        "step-down must not rebump the election counter"
    );
    assert_eq!(
        counter_total(&registry, "s.1.g.1.paxos.step_downs.higher_term.c"),
        Some(0),
        "the replica does not bump step-down counters; the group driver does"
    );
    assert_eq!(
        counter_total(&registry, "s.1.g.1.paxos.step_downs.lease.c"),
        Some(0)
    );
    assert_eq!(
        counter_total(&registry, "s.1.g.1.paxos.step_downs.admin.c"),
        Some(0)
    );

    let view = replica.election_state_view(0, ElectionCounters::default());
    assert_eq!(view.current_term, 5, "admin step-down preserves term");
    assert!(view.lease_remaining_ms.is_none(), "follower has no lease");

    let rejected = replica.handle_step_down(&StepDownRequestPayload {
        term: 5,
        target_leader_id: 1,
        reason: "second".into(),
    });
    assert!(!rejected.accepted, "already follower → reject");
    assert_eq!(election_total(&registry), 1);
}
