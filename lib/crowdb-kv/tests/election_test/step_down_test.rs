// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `handle_step_down` tests: strict-fence policy.
//!
//! The handler accepts a step-down request only if the replica is currently
//! the leader, its node id matches `target_leader_id`, and the request term
//! matches the replica's current term. On accept, the role flips to follower
//! (same term) and `admin_step_down_signal` is notified. On reject, no state
//! changes.

use crowdb_kv::cluster::local_replica::{PxLocalReplica, PxLocalReplicaRole};
use crowdb_kv::cluster::replica::{ReplicaHandler, StepDownRequestPayload};

fn step_down_req(term: u64, target_leader_id: u64, reason: &str) -> StepDownRequestPayload {
    StepDownRequestPayload {
        term,
        target_leader_id,
        reason: reason.to_string(),
    }
}

// ── Accept cases ─────────────────────────────────────────────

#[tokio::test]
async fn step_down_accepts_when_leader_at_matching_term() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Follower);
    replica.become_candidate(5);
    replica.become_leader();

    assert_eq!(replica.role(), PxLocalReplicaRole::Leader);
    assert_eq!(replica.current_term_snapshot(), 5);

    let reply = <PxLocalReplica as ReplicaHandler>::on_step_down(&replica, &step_down_req(5, 1, "admin"), 1)
        .await
        .expect("step-down reply");

    assert!(reply.accepted, "should accept: leader at matching term");
    assert_eq!(reply.current_term, 5);
    assert_eq!(
        reply.current_leader_id, 1,
        "reply reports leader from pre-step-down snapshot"
    );
    assert_eq!(
        replica.role(),
        PxLocalReplicaRole::Follower,
        "role flipped to follower"
    );
}

#[tokio::test]
async fn step_down_accept_preserves_term() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Follower);
    replica.become_candidate(7);
    replica.become_leader();

    let reply =
        <PxLocalReplica as ReplicaHandler>::on_step_down(&replica, &step_down_req(7, 1, "transfer"), 1)
            .await
            .expect("reply");

    assert!(reply.accepted);
    assert_eq!(replica.current_term_snapshot(), 7, "term unchanged on step-down");
}

// ── Reject cases ──────────────────────────────────────────────

#[tokio::test]
async fn step_down_rejects_when_not_leader() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Follower);
    replica.become_candidate(3); // bumps to term 3, becomes candidate

    let reply = <PxLocalReplica as ReplicaHandler>::on_step_down(&replica, &step_down_req(3, 1, "admin"), 1)
        .await
        .expect("reply");

    assert!(!reply.accepted, "candidate is not leader → reject");
    assert_eq!(replica.role(), PxLocalReplicaRole::Candidate, "role unchanged");
}

#[tokio::test]
async fn step_down_rejects_when_term_mismatches_lower() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Follower);
    replica.become_candidate(5);
    replica.become_leader();

    let reply = <PxLocalReplica as ReplicaHandler>::on_step_down(&replica, &step_down_req(3, 1, "stale"), 1)
        .await
        .expect("reply");

    assert!(!reply.accepted, "lower term → reject");
    assert_eq!(replica.role(), PxLocalReplicaRole::Leader, "still leader");
    assert_eq!(reply.current_term, 5, "reply reports actual term");
}

#[tokio::test]
async fn step_down_rejects_when_term_mismatches_higher() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Follower);
    replica.become_candidate(5);
    replica.become_leader();

    let reply =
        <PxLocalReplica as ReplicaHandler>::on_step_down(&replica, &step_down_req(10, 1, "future"), 1)
            .await
            .expect("reply");

    assert!(
        !reply.accepted,
        "higher term → reject (strict fence requires exact match)"
    );
    assert_eq!(replica.role(), PxLocalReplicaRole::Leader, "still leader");
}

#[tokio::test]
async fn step_down_rejects_when_target_leader_id_mismatches() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Follower);
    replica.become_candidate(5);
    replica.become_leader();

    let reply =
        <PxLocalReplica as ReplicaHandler>::on_step_down(&replica, &step_down_req(5, 99, "wrong target"), 1)
            .await
            .expect("reply");

    assert!(!reply.accepted, "target_leader_id mismatch → reject");
    assert_eq!(replica.role(), PxLocalReplicaRole::Leader, "still leader");
}

#[tokio::test]
async fn step_down_rejects_when_follower() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Follower);

    let reply = <PxLocalReplica as ReplicaHandler>::on_step_down(&replica, &step_down_req(0, 1, "admin"), 1)
        .await
        .expect("reply");

    assert!(!reply.accepted, "follower is not leader → reject");
    assert_eq!(replica.role(), PxLocalReplicaRole::Follower, "role unchanged");
}

#[tokio::test]
async fn step_down_rejects_when_precandidate() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Follower);
    replica.become_precandidate();

    let reply = <PxLocalReplica as ReplicaHandler>::on_step_down(&replica, &step_down_req(0, 1, "admin"), 1)
        .await
        .expect("reply");

    assert!(!reply.accepted, "precandidate is not leader → reject");
    assert_eq!(replica.role(), PxLocalReplicaRole::PreCandidate, "role unchanged");
}

// ── Reply fields ──────────────────────────────────────────────

#[tokio::test]
async fn step_down_reply_reports_current_term_even_on_reject() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Follower);
    replica.become_candidate(8);
    replica.become_leader();

    // Reject with wrong target — reply should still report actual term.
    let reply = <PxLocalReplica as ReplicaHandler>::on_step_down(&replica, &step_down_req(8, 99, "wrong"), 1)
        .await
        .expect("reply");

    assert!(!reply.accepted);
    assert_eq!(reply.current_term, 8, "reply reports actual term");
    assert_eq!(reply.current_leader_id, 1, "reply reports actual leader (self)");
}

#[tokio::test]
async fn step_down_reply_reports_zero_leader_when_not_leader() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Follower);

    let reply = <PxLocalReplica as ReplicaHandler>::on_step_down(&replica, &step_down_req(0, 1, "admin"), 1)
        .await
        .expect("reply");

    assert!(!reply.accepted);
    assert_eq!(reply.current_leader_id, 0, "no leader when follower");
}

// ── Idempotency / double step-down ────────────────────────────

#[tokio::test]
async fn double_step_down_second_is_rejected() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Follower);
    replica.become_candidate(5);
    replica.become_leader();

    // First step-down: accepted.
    let r1 = <PxLocalReplica as ReplicaHandler>::on_step_down(&replica, &step_down_req(5, 1, "first"), 1)
        .await
        .expect("reply");
    assert!(r1.accepted);
    assert_eq!(replica.role(), PxLocalReplicaRole::Follower);

    // Second step-down: rejected (no longer leader).
    let r2 = <PxLocalReplica as ReplicaHandler>::on_step_down(&replica, &step_down_req(5, 1, "second"), 1)
        .await
        .expect("reply");
    assert!(!r2.accepted, "already stepped down → reject");
    assert_eq!(replica.role(), PxLocalReplicaRole::Follower);
}
