// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Dedup suppression at the replica level.
//!
//! When a client retries a `(client_id, seq)` that has already been committed,
//! the learner's dedup cache should return the prior commit slot without
//! re-applying the entry. These tests verify the dedup cache is populated by
//! `learn_chosen` and consulted via `learner.dedup_lookup` at the replica level.

use bytes::Bytes;
use crowkv::cluster::local_replica::{PxLocalReplica, PxLocalReplicaRole};
use crowkv::paxos::roles::{PxBallot, PxLogEntry};

#[allow(clippy::cast_possible_truncation)]
fn encode_put(key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.push(1u8); // op_count
    buf.push(0u8); // Put
    buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
    buf.extend_from_slice(key);
    buf.extend_from_slice(&(value.len() as u32).to_le_bytes());
    buf.extend_from_slice(value);
    buf
}

fn write_entry(slot: u64, key: &[u8], value: &[u8]) -> PxLogEntry {
    PxLogEntry {
        slot,
        ballot: PxBallot::new(0, 1),
        term: 1,
        payload: Bytes::from(encode_put(key, value)),
    }
}

#[tokio::test]
async fn learn_chosen_populates_dedup_cache() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Leader);

    let entry = write_entry(1, b"k", b"v");
    replica.learn_chosen(&entry, Some(42), Some(5)).await;

    // Dedup lookup for the same (client_id, seq) returns the commit slot.
    assert_eq!(replica.learner.dedup_lookup(42, 5), Some(1));
    // Lower seq also hits (already applied).
    assert_eq!(replica.learner.dedup_lookup(42, 4), Some(1));
    // Higher seq does not hit.
    assert!(replica.learner.dedup_lookup(42, 6).is_none());
}

#[tokio::test]
async fn dedup_suppresses_retried_request_at_same_slot() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Leader);

    // Commit slots 1–2 as NoOp fills, then slot 3 for client 10, seq 1.
    for slot in 1..=2u64 {
        let entry = PxLogEntry {
            slot,
            ballot: PxBallot::new(0, 1),
            term: 1,
            payload: Bytes::new(),
        };
        replica.learn_chosen(&entry, None, None).await;
    }
    let entry = write_entry(3, b"k", b"v1");
    replica.learn_chosen(&entry, Some(10), Some(1)).await;
    assert_eq!(replica.contiguous_applied(), 3);

    // A retried request for the same (client_id, seq) hits the dedup cache.
    let cached = replica.learner.dedup_lookup(10, 1);
    assert_eq!(cached, Some(3), "retry should return the original commit slot");

    // Re-applying the same entry is idempotent — frontier does not advance.
    replica.learn_chosen(&entry, Some(10), Some(1)).await;
    assert_eq!(replica.contiguous_applied(), 3, "re-learn is idempotent");
}

#[tokio::test]
async fn dedup_tracks_highest_seq_per_client_across_slots() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Leader);

    // Client 7: seq=1 at slot 1, seq=3 at slot 2.
    replica
        .learn_chosen(&write_entry(1, b"k", b"v1"), Some(7), Some(1))
        .await;
    replica
        .learn_chosen(&write_entry(2, b"k", b"v3"), Some(7), Some(3))
        .await;

    // Latest dedup record is seq=3 at slot 2.
    assert_eq!(replica.learner.dedup_lookup(7, 3), Some(2));
    assert_eq!(
        replica.learner.dedup_lookup(7, 1),
        Some(2),
        "older seq maps to latest slot"
    );
    assert!(replica.learner.dedup_lookup(7, 4).is_none());
}

#[tokio::test]
async fn dedup_is_per_client() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Leader);

    replica
        .learn_chosen(&write_entry(1, b"k", b"v1"), Some(10), Some(1))
        .await;
    replica
        .learn_chosen(&write_entry(2, b"k", b"v2"), Some(20), Some(1))
        .await;

    assert_eq!(replica.learner.dedup_lookup(10, 1), Some(1));
    assert_eq!(replica.learner.dedup_lookup(20, 1), Some(2));
    assert!(replica.learner.dedup_lookup(10, 2).is_none());
    assert!(replica.learner.dedup_lookup(20, 2).is_none());
}

#[tokio::test]
async fn dedup_ignores_client_id_zero() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Leader);

    // client_id = 0 is the "no client" sentinel — no dedup entry.
    let entry = PxLogEntry {
        slot: 1,
        ballot: PxBallot::new(0, 1),
        term: 1,
        payload: Bytes::from(encode_put(b"k", b"v")),
    };
    replica.learn_chosen(&entry, Some(0), Some(1)).await;

    assert!(replica.learner.dedup_lookup(0, 1).is_none());
}

#[tokio::test]
async fn dedup_ignores_entries_without_client_id() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Leader);

    let entry = PxLogEntry {
        slot: 1,
        ballot: PxBallot::new(0, 1),
        term: 1,
        payload: Bytes::new(),
    };
    replica.learn_chosen(&entry, None, None).await;

    // No client_id → no dedup entry for any client.
    assert!(replica.learner.dedup_lookup(1, 1).is_none());
}
