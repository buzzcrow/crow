// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Classic Paxos prepare/accept tracking on a bare `PxLocalReplica`.
//!
//! These tests exercise the full Phase-1 + Phase-2 cycle on a single replica
//! without peers: prepare → promise → accept → accepted, ballot fencing,
//! value-recovery on re-prepare, and frontier tracking (`highest_seen_slot`,
//! `accepted_at`, `promised_at`).

use bytes::Bytes;
use crowkv::cluster::local_replica::{PxLocalReplica, PxLocalReplicaRole};
use crowkv::paxos::roles::{PxAcceptReply, PxBallot, PxLogEntry, PxPrepareReply};

fn write_entry(slot: u64, ballot: PxBallot, term: u64, payload: &[u8]) -> PxLogEntry {
    PxLogEntry {
        slot,
        ballot,
        term,
        payload: Bytes::copy_from_slice(payload),
    }
}

#[tokio::test]
async fn prepare_then_accept_succeeds() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Follower);

    // Phase 1: prepare at ballot (1, 1).
    let reply = replica.on_prepare(1, PxBallot::new(1, 1), 1).await;
    assert!(matches!(
        reply,
        PxPrepareReply::Promised {
            slot: 1,
            accepted: None
        }
    ));
    assert_eq!(replica.promised_at(1).await, Some(PxBallot::new(1, 1)));

    // Phase 2: accept a value at the same ballot.
    let entry = write_entry(1, PxBallot::new(1, 1), 1, b"v1");
    let reply = replica.on_accept(entry.clone()).await;
    assert!(matches!(reply, PxAcceptReply::Accepted { .. }));
    assert_eq!(replica.accepted_at(1).await, Some(entry));
}

#[tokio::test]
async fn prepare_rejects_lower_ballot() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Follower);

    // Prepare at ballot (2, 1).
    let _ = replica.on_prepare(5, PxBallot::new(2, 1), 1).await;

    // A lower ballot (1, 1) is rejected.
    let reply = replica.on_prepare(5, PxBallot::new(1, 1), 1).await;
    assert!(matches!(reply, PxPrepareReply::Rejected { .. }));
    assert_eq!(replica.promised_at(5).await, Some(PxBallot::new(2, 1)));
}

#[tokio::test]
async fn accept_rejects_lower_ballot() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Follower);

    let _ = replica.on_prepare(3, PxBallot::new(3, 1), 1).await;

    let stale = write_entry(3, PxBallot::new(2, 1), 1, b"stale");
    let reply = replica.on_accept(stale).await;
    assert!(matches!(reply, PxAcceptReply::Rejected { .. }));
    assert!(replica.accepted_at(3).await.is_none());
}

#[tokio::test]
async fn re_prepare_returns_previously_accepted_value() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Follower);

    // Prepare + accept at ballot (1, 1).
    let _ = replica.on_prepare(7, PxBallot::new(1, 1), 1).await;
    let v1 = write_entry(7, PxBallot::new(1, 1), 1, b"v1");
    let _ = replica.on_accept(v1.clone()).await;

    // Re-prepare at a higher ballot — must return the previously accepted value.
    let reply = replica.on_prepare(7, PxBallot::new(2, 2), 2).await;
    match reply {
        PxPrepareReply::Promised { slot, accepted } => {
            assert_eq!(slot, 7);
            assert_eq!(accepted, Some(v1));
        }
        other => panic!("expected Promised with accepted value, got {other:?}"),
    }
}

#[tokio::test]
async fn highest_seen_slot_tracks_max_slot_prepared() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Follower);
    assert_eq!(replica.highest_seen_slot(), 0);

    let _ = replica.on_prepare(5, PxBallot::new(1, 1), 1).await;
    assert_eq!(replica.highest_seen_slot(), 5);

    // A lower slot does not regress highest_seen_slot.
    let _ = replica.on_prepare(3, PxBallot::new(1, 1), 1).await;
    assert_eq!(replica.highest_seen_slot(), 5);

    // A higher slot advances it.
    let _ = replica.on_prepare(10, PxBallot::new(1, 1), 1).await;
    assert_eq!(replica.highest_seen_slot(), 10);
}

#[tokio::test]
async fn prepare_with_stale_term_returns_term_stale() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Follower);
    replica.become_follower(5);
    assert_eq!(replica.current_term_snapshot(), 5);

    // Prepare stamped with term=1 < current_term=5.
    let reply = replica.on_prepare(1, PxBallot::new(1, 1), 1).await;
    assert!(matches!(reply, PxPrepareReply::TermStale { new_term: 5, .. }));
}

#[tokio::test]
async fn prepare_adopts_higher_term() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Follower);
    assert_eq!(replica.current_term_snapshot(), 0);

    // Prepare stamped with term=9 > current_term=0.
    let reply = replica.on_prepare(1, PxBallot::new(1, 1), 9).await;
    assert!(matches!(reply, PxPrepareReply::Promised { .. }));
    assert_eq!(replica.current_term_snapshot(), 9);
}

#[tokio::test]
async fn multiple_slots_track_independently() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Follower);

    // Slot 1: prepare + accept.
    let _ = replica.on_prepare(1, PxBallot::new(1, 1), 1).await;
    let e1 = write_entry(1, PxBallot::new(1, 1), 1, b"v1");
    let _ = replica.on_accept(e1.clone()).await;

    // Slot 2: prepare + accept with a different value.
    let _ = replica.on_prepare(2, PxBallot::new(1, 1), 1).await;
    let e2 = write_entry(2, PxBallot::new(1, 1), 1, b"v2");
    let _ = replica.on_accept(e2.clone()).await;

    // Both slots have independent accepted values.
    assert_eq!(replica.accepted_at(1).await, Some(e1));
    assert_eq!(replica.accepted_at(2).await, Some(e2));
    assert_eq!(replica.highest_seen_slot(), 2);
}

#[tokio::test]
async fn learn_chosen_advances_applied_frontier() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Leader);

    // Prepare + accept + learn slot 1.
    let _ = replica.on_prepare(1, PxBallot::new(0, 1), 1).await;
    let entry = write_entry(1, PxBallot::new(0, 1), 1, b"v1");
    let _ = replica.on_accept(entry.clone()).await;
    replica.learn_chosen(&entry, None, None).await;

    assert_eq!(replica.contiguous_applied(), 1);
    assert_eq!(replica.contiguous_chosen(), 1);
}

#[tokio::test]
async fn note_chosen_advances_last_chosen_without_applying() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Follower);

    // A chosen notice for slot 5 without a value — only the high-water mark moves.
    assert!(replica.note_chosen(5, 1));
    assert_eq!(replica.last_chosen_slot(), 5);
    assert_eq!(replica.contiguous_chosen(), 0, "no payload applied");
    assert_eq!(replica.contiguous_applied(), 0);
}
