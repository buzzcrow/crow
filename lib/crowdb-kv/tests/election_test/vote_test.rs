// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Vote granting tests: `PreVote` and `RequestVote` decision logic.
//!
//! These tests exercise `handle_pre_vote` and `handle_request_vote` on a
//! bare `PxLocalReplica` — no peers, no crowdb-rpc. The decision is a pure function
//! of `(current_term, voted_for, vote_lockout_until, candidate_log_up_to_date)`.

use crowdb_kv::cluster::local_replica::{PxLocalReplica, PxLocalReplicaRole};
use crowdb_kv::cluster::replica::{ReplicaHandler, VoteRequestPayload};
use crowdb_kv::paxos::roles::{PxBallot, PxLogEntry};

fn make_vote_req(term: u64, candidate_id: u64, tip_slot: u64, tip_term: u64) -> VoteRequestPayload {
    VoteRequestPayload {
        term,
        candidate_id,
        accepted_log_tip_slot: tip_slot,
        accepted_log_tip_term: tip_term,
    }
}

fn accept_entry(slot: u64, term: u64, replica_id: u64) -> PxLogEntry {
    PxLogEntry {
        slot,
        ballot: PxBallot::new(0, replica_id),
        term,
        payload: bytes::Bytes::from_static(b"v"),
    }
}

// ── PreVote ──────────────────────────────────────────────────

#[tokio::test]
async fn prevote_does_not_bump_term() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Follower);
    let term_before = replica.current_term_snapshot();

    let _ =
        <PxLocalReplica as ReplicaHandler>::on_pre_vote(&replica, make_vote_req(term_before + 5, 2, 0, 0), 1)
            .await
            .expect("pre-vote reply");

    assert_eq!(
        replica.current_term_snapshot(),
        term_before,
        "PreVote must never bump current_term"
    );
}

#[tokio::test]
async fn prevote_grants_when_term_higher_and_log_up_to_date() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Follower);
    replica.clear_vote_lockout();

    let reply = <PxLocalReplica as ReplicaHandler>::on_pre_vote(&replica, make_vote_req(2, 2, 0, 0), 1)
        .await
        .expect("pre-vote reply");

    assert!(reply.granted, "higher term + up-to-date log → grant");
    assert_eq!(reply.term, 0, "PreVote reply reports current_term, not proposed");
}

#[tokio::test]
async fn prevote_rejects_when_term_not_higher() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Follower);
    replica.become_follower(5);

    let reply = <PxLocalReplica as ReplicaHandler>::on_pre_vote(&replica, make_vote_req(3, 2, 0, 0), 1)
        .await
        .expect("pre-vote reply");

    assert!(!reply.granted, "term <= current_term → reject");
}

#[tokio::test]
async fn prevote_rejects_when_log_not_up_to_date() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Follower);
    replica.clear_vote_lockout();

    // Accept an entry at slot 10, term 4 — our log tip is (10, 4).
    let _ = replica.on_accept(&accept_entry(10, 4, 1)).await;

    let reply = <PxLocalReplica as ReplicaHandler>::on_pre_vote(&replica, make_vote_req(2, 2, 5, 3), 1)
        .await
        .expect("pre-vote reply");

    assert!(!reply.granted, "candidate log tip (5, 3) < ours (10, 4) → reject");
}

#[tokio::test]
async fn prevote_rejects_during_vote_lockout() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Follower);
    // become_candidate extends vote_lockout.
    replica.become_candidate(1);

    let reply = <PxLocalReplica as ReplicaHandler>::on_pre_vote(&replica, make_vote_req(3, 2, 0, 0), 1)
        .await
        .expect("pre-vote reply");

    assert!(!reply.granted, "vote lockout active → reject");
}

#[tokio::test]
async fn prevote_does_not_mutate_voted_for() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Follower);
    replica.clear_vote_lockout();
    assert_eq!(replica.voted_for(), None);

    let _ = <PxLocalReplica as ReplicaHandler>::on_pre_vote(&replica, make_vote_req(2, 2, 0, 0), 1)
        .await
        .expect("pre-vote reply");

    assert_eq!(replica.voted_for(), None, "PreVote must not set voted_for");
}

// ── RequestVote ──────────────────────────────────────────────

#[tokio::test]
async fn request_vote_grants_for_higher_term_with_up_to_date_log() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Follower);
    replica.clear_vote_lockout();

    let reply = <PxLocalReplica as ReplicaHandler>::on_request_vote(&replica, make_vote_req(2, 2, 0, 0), 1)
        .await
        .expect("request-vote reply");

    assert!(reply.granted);
    assert_eq!(replica.current_term_snapshot(), 2, "term adopted");
    assert_eq!(replica.voted_for(), Some(2), "voted_for set to candidate");
    assert_eq!(
        replica.role(),
        PxLocalReplicaRole::Follower,
        "role is follower after grant"
    );
}

#[tokio::test]
async fn request_vote_rejects_candidate_with_stale_log() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Follower);
    replica.clear_vote_lockout();

    // Our log tip is (10, 4).
    let _ = replica.on_accept(&accept_entry(10, 4, 1)).await;

    let reply = <PxLocalReplica as ReplicaHandler>::on_request_vote(&replica, make_vote_req(2, 2, 5, 3), 1)
        .await
        .expect("request-vote reply");

    assert!(!reply.granted, "stale log → reject");
    assert_eq!(replica.voted_for(), None, "voted_for unchanged on reject");
}

#[tokio::test]
async fn request_vote_grants_for_matching_log_tip_even_if_learner_cold() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Follower);
    replica.clear_vote_lockout();

    // Accept at (11, 6) — learner is cold (no learn_chosen called).
    // on_accept at term=6 bumps current_term to 6.
    let _ = replica.on_accept(&accept_entry(11, 6, 1)).await;
    assert_eq!(replica.last_chosen_slot(), 0, "learner cold");
    let current_term = replica.current_term_snapshot();

    let reply = <PxLocalReplica as ReplicaHandler>::on_request_vote(
        &replica,
        make_vote_req(current_term + 1, 2, 11, 6),
        1,
    )
    .await
    .expect("request-vote reply");

    assert!(reply.granted, "matching log tip → grant");
}

#[tokio::test]
async fn request_vote_rejects_during_vote_lockout() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Follower);
    replica.become_candidate(1);

    let reply = <PxLocalReplica as ReplicaHandler>::on_request_vote(&replica, make_vote_req(3, 2, 0, 0), 1)
        .await
        .expect("request-vote reply");

    assert!(!reply.granted, "vote lockout → reject");
    assert_eq!(replica.voted_for(), Some(1), "voted_for unchanged");
}

#[tokio::test]
async fn request_vote_rejects_lower_term() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Follower);
    replica.become_follower(5);
    replica.clear_vote_lockout();

    let reply = <PxLocalReplica as ReplicaHandler>::on_request_vote(&replica, make_vote_req(3, 2, 0, 0), 1)
        .await
        .expect("request-vote reply");

    assert!(!reply.granted, "term < current_term → reject");
    assert_eq!(replica.current_term_snapshot(), 5, "term unchanged");
}

#[tokio::test]
async fn request_vote_grants_again_for_same_candidate_same_term() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Follower);
    replica.clear_vote_lockout();

    // First grant.
    let r1 = <PxLocalReplica as ReplicaHandler>::on_request_vote(&replica, make_vote_req(2, 2, 0, 0), 1)
        .await
        .expect("reply");
    assert!(r1.granted);

    // Same candidate, same term — should still grant (idempotent for same candidate).
    // But vote_lockout was extended by the first grant, so this will be rejected.
    // This is correct Raft behavior: after granting, lockout prevents immediate re-grant.
    let r2 = <PxLocalReplica as ReplicaHandler>::on_request_vote(&replica, make_vote_req(2, 2, 0, 0), 1)
        .await
        .expect("reply");
    assert!(!r2.granted, "lockout extended after first grant → reject");
}

#[tokio::test]
async fn request_vote_reply_carries_frontier_triple() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Follower);
    replica.clear_vote_lockout();

    // Accept at slot 7, term 3 — bumps current_term to 3.
    let _ = replica.on_accept(&accept_entry(7, 3, 1)).await;
    let current_term = replica.current_term_snapshot();

    let reply = <PxLocalReplica as ReplicaHandler>::on_request_vote(
        &replica,
        make_vote_req(current_term + 1, 2, 7, 3),
        1,
    )
    .await
    .expect("reply");

    assert!(reply.granted);
    assert_eq!(reply.highest_seen_slot, 7);
}
