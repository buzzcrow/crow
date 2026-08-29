// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Dedup suppression at the replica level.
//!
//! When a client retries a `(client_id, seq)` that has already been committed,
//! the learner's dedup cache should return the prior commit slot without
//! re-applying the entry. These tests verify the dedup cache is populated by
//! `learn_chosen` and consulted via `learner.dedup_lookup` at the replica level.

use bytes::Bytes;
use crowdb_kv::cluster::local_replica::{PxLocalReplica, PxLocalReplicaRole};
use crowdb_kv::paxos::roles::{DedupTag, PxBallot, PxLogEntry};

fn tag(client_id: u64, seq: u64) -> [DedupTag; 1] {
    [DedupTag { client_id, seq }]
}

#[allow(clippy::cast_possible_truncation)]
fn encode_put(key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&1u16.to_le_bytes()); // op_count
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
    replica.learn_chosen(&entry, &tag(42, 5)).await;

    // Dedup lookup for the same (client_id, seq) returns the commit slot.
    assert_eq!(replica.learner.dedup_lookup(42, 5), Some(1));
    // An unrecorded lower seq is a miss, not a hit against a higher seq's
    // slot (exact-match lookup; outside-the-window outcome is unknown).
    assert!(replica.learner.dedup_lookup(42, 4).is_none());
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
        replica.learn_chosen(&entry, &[]).await;
    }
    let entry = write_entry(3, b"k", b"v1");
    replica.learn_chosen(&entry, &tag(10, 1)).await;
    assert_eq!(replica.contiguous_applied(), 3);

    // A retried request for the same (client_id, seq) hits the dedup cache.
    let cached = replica.learner.dedup_lookup(10, 1);
    assert_eq!(cached, Some(3), "retry should return the original commit slot");

    // Re-applying the same entry is idempotent — frontier does not advance.
    replica.learn_chosen(&entry, &tag(10, 1)).await;
    assert_eq!(replica.contiguous_applied(), 3, "re-learn is idempotent");
}

#[tokio::test]
async fn dedup_records_each_seq_at_its_own_slot() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Leader);

    // Client 7: seq=1 at slot 1, seq=3 at slot 2.
    replica
        .learn_chosen(&write_entry(1, b"k", b"v1"), &tag(7, 1))
        .await;
    replica
        .learn_chosen(&write_entry(2, b"k", b"v3"), &tag(7, 3))
        .await;

    // Each recorded seq maps to its own slot (exact-match lookup).
    assert_eq!(replica.learner.dedup_lookup(7, 1), Some(1));
    assert_eq!(replica.learner.dedup_lookup(7, 3), Some(2));
    // An unrecorded seq is a miss, not a hit against a higher seq's slot.
    assert!(replica.learner.dedup_lookup(7, 2).is_none());
    assert!(replica.learner.dedup_lookup(7, 4).is_none());
}

#[tokio::test]
async fn dedup_is_per_client() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Leader);

    replica
        .learn_chosen(&write_entry(1, b"k", b"v1"), &tag(10, 1))
        .await;
    replica
        .learn_chosen(&write_entry(2, b"k", b"v2"), &tag(20, 1))
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
    replica.learn_chosen(&entry, &tag(0, 1)).await;

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
    replica.learn_chosen(&entry, &[]).await;

    // No client_id → no dedup entry for any client.
    assert!(replica.learner.dedup_lookup(1, 1).is_none());
}

#[tokio::test]
async fn dedup_does_not_false_positive_on_out_of_order_higher_seq() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Leader);

    // Same client, two logically distinct writes: seq=100 (key "a") and
    // seq=105 (key "b"). seq=105 is learned FIRST (out-of-order slot
    // choice — e.g. seq=105's proposal happened to win its quorum round
    // before seq=100's did).
    let entry_b = write_entry(2, b"b", b"vb"); // slot 2, seq 105
    replica.learn_chosen(&entry_b, &tag(77, 105)).await;

    // BUG (pre-fix): seq=100 has never been recorded, but the old
    // single-entry "latest wins" dedup treats 100 <= 105 as a hit and
    // would return `Some(2)` here — the slot of an unrelated write, for
    // a payload ("a") that was never proposed. That is a silent
    // data-loss false positive: `propose` would short-circuit to
    // `Chosen { slot: 2 }` for the seq=100 caller without ever running
    // Paxos for key "a".
    //
    // FIXED behavior: an unrecorded seq is a miss, regardless of any
    // higher seq already committed for the same client.
    assert!(
        replica.learner.dedup_lookup(77, 100).is_none(),
        "seq=100 was never recorded; a higher committed seq=105 must not \
         produce a false-positive hit against its slot"
    );

    // Now seq=100 (key "a") is actually proposed and learned at slot 3.
    let entry_a = write_entry(3, b"a", b"va");
    replica.learn_chosen(&entry_a, &tag(77, 100)).await;

    // A genuine retry of seq=100 now correctly hits its own slot (3),
    // not seq=105's slot (2).
    assert_eq!(replica.learner.dedup_lookup(77, 100), Some(3));
    // seq=105's own record is unaffected.
    assert_eq!(replica.learner.dedup_lookup(77, 105), Some(2));
}

#[tokio::test]
async fn dedup_retains_at_least_64_requests_per_client() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Leader);

    // Commit seq=1..=80 for one client, each at its own slot.
    for seq in 1u64..=80 {
        let entry = write_entry(seq, format!("k{seq}").as_bytes(), b"v");
        replica.learn_chosen(&entry, &tag(5, seq)).await;
    }

    // The most recent >= 64 requests must still be individually
    // retrievable by their own seq (not just the latest).
    for seq in 17u64..=80 {
        assert_eq!(
            replica.learner.dedup_lookup(5, seq),
            Some(seq),
            "seq={seq} should still be in the retained window"
        );
    }
}
