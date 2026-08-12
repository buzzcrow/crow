// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Election metrics tests for `PxLocalReplica`.
//!
//! Moved from `replica/election_metrics_test.rs` to the election unit binary.

use crow_kv::cluster::local_replica::{PxLocalReplica, PxLocalReplicaRole};

#[test]
fn election_count_bumps_on_become_candidate() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Follower);
    let before = replica.election_metrics_snapshot(0).election_count;
    replica.become_candidate(1);
    replica.become_candidate(2);
    let after = replica.election_metrics_snapshot(0).election_count;
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
    let snap = replica.election_metrics_snapshot(3);
    assert_eq!(snap.current_term, 5);
    assert_eq!(snap.bulk_phase1_in_flight_slots, 3);
    assert!(snap.lease_remaining_ms.is_none(), "candidate has no lease");
    assert!(snap.last_heartbeat_age_ms.is_none());
}

#[test]
fn leader_snapshot_has_lease_remaining() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Leader);
    replica.extend_lease_read_until(std::time::Instant::now() + std::time::Duration::from_secs(10));
    let snap = replica.election_metrics_snapshot(0);
    assert!(
        snap.lease_remaining_ms.is_some(),
        "leader with active lease should report remaining"
    );
}

#[test]
fn follower_snapshot_has_no_lease_remaining() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Follower);
    let snap = replica.election_metrics_snapshot(0);
    assert!(snap.lease_remaining_ms.is_none(), "follower has no lease");
}
