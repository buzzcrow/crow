// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `frontier_triple` consistency tests.
//!
//! `frontier_triple` returns `(contiguous_chosen, last_chosen_term,
//! highest_seen_slot)` and is used in `PreVote`, `RequestVote`, and
//! `Heartbeat` replies. These tests verify that the triple is consistent
//! across role transitions and reflects the replica's actual state.
//!
//! Since `frontier_triple` is private, we test it indirectly through the
//! public `ReplicaHandler` trait methods that embed the triple in their
//! replies.

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

// ── Initial state ─────────────────────────────────────────────

#[tokio::test]
async fn frontier_triple_zero_on_fresh_replica() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Follower);
    replica.clear_vote_lockout();

    let reply = <PxLocalReplica as ReplicaHandler>::on_request_vote(&replica, make_vote_req(1, 2, 0, 0), 1)
        .await
        .expect("reply");

    assert_eq!(reply.contiguous_chosen, 0, "no slots chosen");
    assert_eq!(reply.highest_seen_slot, 0, "no slots seen");
}

// ── After accept, highest_seen_slot advances ──────────────────

#[tokio::test]
async fn frontier_triple_reflects_accepted_slot() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Follower);
    replica.clear_vote_lockout();

    // Accept at slot 5, term 3 — bumps current_term to 3.
    let _ = replica.on_accept(&accept_entry(5, 3, 1)).await;
    let current_term = replica.current_term_snapshot();

    let reply = <PxLocalReplica as ReplicaHandler>::on_request_vote(
        &replica,
        make_vote_req(current_term + 1, 2, 5, 3),
        1,
    )
    .await
    .expect("reply");

    assert_eq!(
        reply.highest_seen_slot, 5,
        "highest_seen_slot reflects accepted slot"
    );
    // contiguous_chosen is still 0 because we only accepted, not learned.
    assert_eq!(reply.contiguous_chosen, 0, "not yet chosen");
}

// ── After learn_chosen, contiguous_chosen advances ────────────

#[tokio::test]
async fn frontier_triple_reflects_learned_slot() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Follower);
    replica.clear_vote_lockout();

    // Accept + learn at slot 3, term 2 (must learn 1,2,3 for contiguous).
    for slot in 1..=3u64 {
        let entry = accept_entry(slot, 2, 1);
        let _ = replica.on_accept(&entry).await;
        replica.learn_chosen(&entry, &[]).await;
    }
    let current_term = replica.current_term_snapshot();

    let reply = <PxLocalReplica as ReplicaHandler>::on_request_vote(
        &replica,
        make_vote_req(current_term + 1, 2, 3, 2),
        1,
    )
    .await
    .expect("reply");

    assert_eq!(
        reply.contiguous_chosen, 3,
        "contiguous_chosen reflects learned slot"
    );
    assert_eq!(reply.highest_seen_slot, 3, "highest_seen_slot also advanced");
}

// ── Consistency across role transitions ───────────────────────

#[tokio::test]
async fn frontier_triple_consistent_after_become_candidate() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Follower);
    replica.clear_vote_lockout();

    // Accept + learn slots 1-7, term 4 (sequential for contiguous).
    for slot in 1..=7u64 {
        let entry = accept_entry(slot, 4, 1);
        let _ = replica.on_accept(&entry).await;
        replica.learn_chosen(&entry, &[]).await;
    }
    let term_before = replica.current_term_snapshot();

    // Transition to candidate — bumps term.
    replica.become_candidate(term_before + 1);
    let term_after = replica.current_term_snapshot();

    let reply = <PxLocalReplica as ReplicaHandler>::on_request_vote(
        &replica,
        make_vote_req(term_after + 1, 2, 7, 4),
        1,
    )
    .await
    .expect("reply");

    // The frontier should be unchanged by the role transition.
    assert_eq!(
        reply.contiguous_chosen, 7,
        "contiguous_chosen preserved across become_candidate"
    );
    assert_eq!(reply.highest_seen_slot, 7, "highest_seen_slot preserved");
}

#[tokio::test]
async fn frontier_triple_consistent_after_become_leader() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Follower);
    replica.clear_vote_lockout();

    // Accept + learn slots 1-10, term 5.
    for slot in 1..=10u64 {
        let entry = accept_entry(slot, 5, 1);
        let _ = replica.on_accept(&entry).await;
        replica.learn_chosen(&entry, &[]).await;
    }
    let term = replica.current_term_snapshot();

    // Become candidate then leader.
    replica.become_candidate(term + 1);
    let new_term = replica.current_term_snapshot();
    replica.become_leader();

    // As leader, we should still be able to report the frontier via a
    // request_vote reply (even though a real leader wouldn't grant votes,
    // the triple should be consistent).
    let reply = <PxLocalReplica as ReplicaHandler>::on_request_vote(
        &replica,
        make_vote_req(new_term + 1, 2, 10, 5),
        1,
    )
    .await
    .expect("reply");

    assert_eq!(
        reply.contiguous_chosen, 10,
        "contiguous_chosen preserved after become_leader"
    );
    assert_eq!(reply.highest_seen_slot, 10, "highest_seen_slot preserved");
}

#[tokio::test]
async fn frontier_triple_consistent_after_become_follower() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Follower);
    replica.clear_vote_lockout();

    // Build up state: accept + learn slots 1-4, term 2.
    for slot in 1..=4u64 {
        let entry = accept_entry(slot, 2, 1);
        let _ = replica.on_accept(&entry).await;
        replica.learn_chosen(&entry, &[]).await;
    }
    let term = replica.current_term_snapshot();

    // Become candidate, then leader, then step down to follower.
    replica.become_candidate(term + 1);
    let new_term = replica.current_term_snapshot();
    replica.become_leader();
    replica.become_follower(new_term);

    let reply = <PxLocalReplica as ReplicaHandler>::on_request_vote(
        &replica,
        make_vote_req(new_term + 1, 2, 4, 2),
        1,
    )
    .await
    .expect("reply");

    assert_eq!(
        reply.contiguous_chosen, 4,
        "contiguous_chosen preserved after step-down"
    );
    assert_eq!(reply.highest_seen_slot, 4, "highest_seen_slot preserved");
}

// ── Multi-slot frontier ───────────────────────────────────────

#[tokio::test]
async fn frontier_triple_with_gap_in_accepted_log() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Follower);
    replica.clear_vote_lockout();

    // Accept at slots 1 and 5 (gap at 2-4), learn slot 1.
    let entry1 = accept_entry(1, 2, 1);
    let _ = replica.on_accept(&entry1).await;
    replica.learn_chosen(&entry1, &[]).await;

    let entry5 = accept_entry(5, 2, 1);
    let _ = replica.on_accept(&entry5).await;

    let term = replica.current_term_snapshot();

    let reply =
        <PxLocalReplica as ReplicaHandler>::on_request_vote(&replica, make_vote_req(term + 1, 2, 5, 2), 1)
            .await
            .expect("reply");

    // contiguous_chosen = 1 (only slot 1 is learned), highest_seen_slot = 5.
    assert_eq!(reply.contiguous_chosen, 1, "contiguous_chosen stops at gap");
    assert_eq!(reply.highest_seen_slot, 5, "highest_seen_slot covers gap");
}

#[tokio::test]
async fn frontier_triple_advances_with_progressive_learn() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Follower);
    replica.clear_vote_lockout();

    // Accept + learn slots 1, 2, 3 sequentially.
    for slot in 1..=3u64 {
        let entry = accept_entry(slot, 2, 1);
        let _ = replica.on_accept(&entry).await;
        replica.learn_chosen(&entry, &[]).await;
    }

    let term = replica.current_term_snapshot();
    let reply =
        <PxLocalReplica as ReplicaHandler>::on_request_vote(&replica, make_vote_req(term + 1, 2, 3, 2), 1)
            .await
            .expect("reply");

    assert_eq!(reply.contiguous_chosen, 3, "all three slots chosen contiguously");
    assert_eq!(reply.highest_seen_slot, 3);
}

// ── PreVote reply also carries frontier triple ────────────────

#[tokio::test]
async fn prevote_reply_carries_frontier_triple() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Follower);
    replica.clear_vote_lockout();

    // Accept + learn at slot 8, term 3 (sequential 1-8 for contiguous).
    for slot in 1..=8u64 {
        let entry = accept_entry(slot, 3, 1);
        let _ = replica.on_accept(&entry).await;
        replica.learn_chosen(&entry, &[]).await;
    }
    let term = replica.current_term_snapshot();

    let reply =
        <PxLocalReplica as ReplicaHandler>::on_pre_vote(&replica, make_vote_req(term + 1, 2, 8, 3), 1)
            .await
            .expect("reply");

    assert_eq!(reply.contiguous_chosen, 8, "PreVote reply carries frontier");
    assert_eq!(reply.highest_seen_slot, 8);
}
