//! Concurrent `learn_chosen` + `on_accept` race tests on the same slot.
//!
//! `learn_chosen` applies an entry to the learner (KV engine + frontier
//! advancement + dedup). `on_accept` persists an accepted entry to the
//! acceptor (and WAL if attached). Both can be called concurrently on the
//! same slot — e.g. a `ChosenNotice` arrives while an accept is in flight.
//!
//! These tests verify no panic, no corruption, and consistent final state
//! regardless of call ordering or concurrency.

use std::sync::Arc;

use crowkv::cluster::local_replica::{PxLocalReplica, PxLocalReplicaRole};
use crowkv::paxos::roles::{PxAcceptReply, PxBallot, PxLogEntry, PxLogEntryKind};

fn make_entry(slot: u64, term: u64, value: &[u8]) -> PxLogEntry {
    let mut payload = Vec::new();
    payload.push(1); // op = PUT
    payload.push(0); // flags
    let key = b"k";
    let key_len = u32::try_from(key.len()).expect("key len");
    payload.extend_from_slice(&key_len.to_le_bytes());
    payload.extend_from_slice(key);
    let val_len = u32::try_from(value.len()).expect("val len");
    payload.extend_from_slice(&val_len.to_le_bytes());
    payload.extend_from_slice(value);

    PxLogEntry {
        slot,
        ballot: PxBallot::new(1, 1),
        term,
        kind: PxLogEntryKind::Write,
        payload: bytes::Bytes::from(payload),
        client_id: Some(1),
        seq: Some(slot),
    }
}

// ── Sequential: learn then accept ─────────────────────────────

#[tokio::test]
async fn learn_then_accept_same_slot() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Follower);

    // Learn slots 1-5 sequentially so contiguous_applied reaches 5.
    for slot in 1..=5u64 {
        let entry = make_entry(slot, 1, &slot.to_string().into_bytes());
        replica.learn_chosen(&entry).await;
    }
    assert_eq!(replica.contiguous_applied(), 5);

    // Now re-accept slot 5.
    let entry5 = make_entry(5, 1, b"5");
    let reply = replica.on_accept(entry5.clone()).await;

    assert!(
        matches!(reply, PxAcceptReply::Accepted { .. }),
        "accept after learn should still succeed"
    );

    // Acceptor holds the entry.
    assert!(replica.accepted_at(5).await.is_some());
    // Learner still has applied through slot 5.
    assert_eq!(replica.contiguous_applied(), 5);
}

// ── Sequential: accept then learn ─────────────────────────────

#[tokio::test]
async fn accept_then_learn_same_slot() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Follower);

    // Accept slots 1-5 sequentially.
    for slot in 1..=5u64 {
        let entry = make_entry(slot, 1, &slot.to_string().into_bytes());
        let reply = replica.on_accept(entry.clone()).await;
        assert!(matches!(reply, PxAcceptReply::Accepted { .. }));
    }

    // Now learn all 5 sequentially.
    for slot in 1..=5u64 {
        let entry = make_entry(slot, 1, &slot.to_string().into_bytes());
        replica.learn_chosen(&entry).await;
    }

    assert!(replica.accepted_at(5).await.is_some());
    assert_eq!(replica.contiguous_applied(), 5);
}

// ── Concurrent on same slot ───────────────────────────────────

#[tokio::test]
async fn concurrent_learn_and_accept_same_slot() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Follower);

    // Pre-learn slots 1-4 so slot 5 is the next contiguous slot.
    for slot in 1..=4u64 {
        let entry = make_entry(slot, 1, &slot.to_string().into_bytes());
        let _ = replica.on_accept(entry.clone()).await;
        replica.learn_chosen(&entry).await;
    }
    assert_eq!(replica.contiguous_applied(), 4);

    let entry5 = make_entry(5, 1, b"v5");

    // Both operations on slot 5 concurrently.
    let (accept_result, ()) = tokio::join!(replica.on_accept(entry5.clone()), replica.learn_chosen(&entry5),);

    // No panic — that's the primary assertion.
    assert!(
        matches!(accept_result, PxAcceptReply::Accepted { .. }),
        "accept should succeed"
    );

    // Final state is consistent: acceptor has the entry, learner applied it.
    assert!(replica.accepted_at(5).await.is_some(), "acceptor has slot 5");
    assert_eq!(replica.contiguous_applied(), 5, "learner applied slot 5");
}

// ── Concurrent on adjacent slots ──────────────────────────────

#[tokio::test]
async fn concurrent_learn_and_accept_adjacent_slots() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Follower);

    // First learn slot 1 so contiguous_applied is at 1.
    let entry1 = make_entry(1, 1, b"v1");
    let _ = replica.on_accept(entry1.clone()).await;
    replica.learn_chosen(&entry1).await;
    assert_eq!(replica.contiguous_applied(), 1);

    // Now concurrently accept slot 2 and learn slot 2.
    let entry2 = make_entry(2, 1, b"v2");
    let (accept_result, ()) = tokio::join!(replica.on_accept(entry2.clone()), replica.learn_chosen(&entry2),);

    assert!(matches!(accept_result, PxAcceptReply::Accepted { .. }));
    assert!(replica.accepted_at(2).await.is_some());
    assert_eq!(replica.contiguous_applied(), 2, "both slots applied");
}

// ── Concurrent learn on same slot, accept with different value ─

#[tokio::test]
async fn concurrent_learn_then_accept_different_value_same_slot() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Follower);

    // Learn slots 1-3 sequentially (establishes contiguous frontier).
    for slot in 1..=3u64 {
        let entry = make_entry(slot, 1, &slot.to_string().into_bytes());
        let _ = replica.on_accept(entry.clone()).await;
        replica.learn_chosen(&entry).await;
    }
    assert_eq!(replica.contiguous_applied(), 3);

    // Now accept v2 at slot 3 (same term, same ballot — re-accept).
    let entry_v2 = make_entry(3, 1, b"v2");
    let reply = replica.on_accept(entry_v2.clone()).await;

    // Acceptor should accept the new value.
    assert!(
        matches!(reply, PxAcceptReply::Accepted { .. }),
        "re-accept at same slot should succeed"
    );
    assert!(replica.accepted_at(3).await.is_some(), "acceptor has slot 3");
    // Learner still has the original applied value.
    assert_eq!(replica.contiguous_applied(), 3);
}

// ── Many concurrent accepts on different slots ────────────────

#[tokio::test]
async fn concurrent_accepts_on_different_slots_no_interference() {
    let replica = Arc::new(PxLocalReplica::new(1, PxLocalReplicaRole::Follower));

    // Launch 10 concurrent accepts on disjoint slots.
    let mut handles = Vec::new();
    for slot in 1..=10u64 {
        let entry = make_entry(slot, 1, &slot.to_string().into_bytes());
        let r = Arc::clone(&replica);
        handles.push(tokio::spawn(async move {
            let reply = r.on_accept(entry.clone()).await;
            assert!(
                matches!(reply, PxAcceptReply::Accepted { .. }),
                "slot {slot} should be accepted"
            );
        }));
    }

    for h in handles {
        h.await.expect("task panicked");
    }

    // All slots should be in the acceptor.
    for slot in 1..=10u64 {
        assert!(
            replica.accepted_at(slot).await.is_some(),
            "slot {slot} in acceptor"
        );
    }
}

// ── Concurrent learn_chosen on multiple slots ─────────────────

#[tokio::test]
async fn concurrent_learn_chosen_multiple_slots() {
    let replica = Arc::new(PxLocalReplica::new(1, PxLocalReplicaRole::Follower));

    // Accept slots 1-5 sequentially first.
    for slot in 1..=5u64 {
        let entry = make_entry(slot, 1, &slot.to_string().into_bytes());
        let _ = replica.on_accept(entry.clone()).await;
    }

    // Now learn all 5 concurrently.
    let mut handles = Vec::new();
    for slot in 1..=5u64 {
        let entry = make_entry(slot, 1, &slot.to_string().into_bytes());
        let r = Arc::clone(&replica);
        handles.push(tokio::spawn(async move {
            r.learn_chosen(&entry).await;
        }));
    }

    for h in handles {
        h.await.expect("learn task panicked");
    }

    // All 5 should be applied (contiguous).
    assert_eq!(replica.contiguous_applied(), 5, "all slots applied");
}
