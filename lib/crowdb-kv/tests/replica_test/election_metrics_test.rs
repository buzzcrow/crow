// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Step 11: `ElectionMetrics` counter wiring on `PxLocalReplica`.

use crowdb_kv::cluster::local_replica::{PxLocalReplica, PxLocalReplicaRole};

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
    // Candidate (not leader) → lease_remaining_ms is None.
    assert!(snap.lease_remaining_ms.is_none(), "candidate has no lease");
    // No heartbeat received yet → age is None.
    assert!(snap.last_heartbeat_age_ms.is_none());
}
