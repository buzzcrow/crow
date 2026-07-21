// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! KV operation correctness at the replica level.
//!
//! Verifies that `learn_chosen` correctly applies Put, Delete, and Batch
//! operations to the learner's KV engine. No WAL, no restart — purely
//! checking that the operation encoding → decode → apply pipeline produces
//! the right KV state.

use bytes::Bytes;
use crowkv::cluster::local_replica::{PxLocalReplica, PxLocalReplicaRole};
use crowkv::paxos::roles::{PxBallot, PxLogEntry};

fn encode_put(key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.push(1); // op_count
    buf.push(0); // kind = Put
    let key_len = u32::try_from(key.len()).expect("key len");
    buf.extend_from_slice(&key_len.to_le_bytes());
    buf.extend_from_slice(key);
    let val_len = u32::try_from(value.len()).expect("val len");
    buf.extend_from_slice(&val_len.to_le_bytes());
    buf.extend_from_slice(value);
    buf
}

fn encode_delete(key: &[u8]) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.push(1); // op_count
    buf.push(1); // kind = Delete
    let key_len = u32::try_from(key.len()).expect("key len");
    buf.extend_from_slice(&key_len.to_le_bytes());
    buf.extend_from_slice(key);
    buf.extend_from_slice(&0u32.to_le_bytes()); // value_len = 0
    buf
}

#[allow(clippy::cast_possible_truncation)]
fn encode_batch(ops: &[(Vec<u8>, Option<Vec<u8>>)]) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.push(ops.len() as u8);
    for (key, value) in ops {
        if let Some(v) = value {
            buf.push(0); // Put
            let kl = u32::try_from(key.len()).expect("key len");
            buf.extend_from_slice(&kl.to_le_bytes());
            buf.extend_from_slice(key);
            let vl = u32::try_from(v.len()).expect("val len");
            buf.extend_from_slice(&vl.to_le_bytes());
            buf.extend_from_slice(v);
        } else {
            buf.push(1); // Delete
            let kl = u32::try_from(key.len()).expect("key len");
            buf.extend_from_slice(&kl.to_le_bytes());
            buf.extend_from_slice(key);
            buf.extend_from_slice(&0u32.to_le_bytes());
        }
    }
    buf
}

fn entry(slot: u64, payload: Vec<u8>) -> PxLogEntry {
    PxLogEntry {
        slot,
        ballot: PxBallot::new(1, 1),
        term: 1,
        payload: Bytes::from(payload),
    }
}

// ── Put correctness ───────────────────────────────────────────

#[tokio::test]
async fn put_applies_value_to_kv_engine() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Follower);
    let entry = entry(1, encode_put(b"key", b"value"));
    let _ = replica.on_accept(&entry).await;
    replica.learn_chosen(&entry, None, None).await;

    assert_eq!(
        replica.learner.engine_get(b"key").await.map(|(_, v)| v),
        Some(b"value".to_vec()),
        "put applies value"
    );
    assert_eq!(replica.contiguous_applied(), 1);
}

// ── Overwrite correctness ─────────────────────────────────────

#[tokio::test]
async fn overwrite_replaces_previous_value() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Follower);

    let e1 = entry(1, encode_put(b"k", b"v1"));
    let _ = replica.on_accept(&e1).await;
    replica.learn_chosen(&e1, None, None).await;

    let e2 = entry(2, encode_put(b"k", b"v2"));
    let _ = replica.on_accept(&e2).await;
    replica.learn_chosen(&e2, None, None).await;

    assert_eq!(
        replica.learner.engine_get(b"k").await.map(|(_, v)| v),
        Some(b"v2".to_vec()),
        "latest put wins"
    );
    assert_eq!(replica.contiguous_applied(), 2);
}

// ── Delete correctness ────────────────────────────────────────

#[tokio::test]
async fn delete_produces_tombstone() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Follower);

    let e1 = entry(1, encode_put(b"k", b"v1"));
    let _ = replica.on_accept(&e1).await;
    replica.learn_chosen(&e1, None, None).await;

    assert!(
        replica.learner.engine_get(b"k").await.is_some(),
        "key exists after put"
    );

    let e2 = entry(2, encode_delete(b"k"));
    let _ = replica.on_accept(&e2).await;
    replica.learn_chosen(&e2, None, None).await;

    assert_eq!(
        replica.learner.engine_get(b"k").await,
        None,
        "delete produces tombstone — engine_get returns None"
    );
    assert_eq!(replica.contiguous_applied(), 2);
}

// ── Delete on non-existent key ────────────────────────────────

#[tokio::test]
async fn delete_nonexistent_key_is_noop() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Follower);

    let e1 = entry(1, encode_delete(b"ghost"));
    let _ = replica.on_accept(&e1).await;
    replica.learn_chosen(&e1, None, None).await;

    assert_eq!(
        replica.learner.engine_get(b"ghost").await,
        None,
        "deleting non-existent key is a no-op"
    );
    assert_eq!(replica.contiguous_applied(), 1);
}

// ── Batch: multiple puts in one slot ──────────────────────────

#[tokio::test]
async fn batch_multiple_puts_apply_all() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Follower);

    let e1 = entry(
        1,
        encode_batch(&[
            (b"k1".to_vec(), Some(b"v1".to_vec())),
            (b"k2".to_vec(), Some(b"v2".to_vec())),
            (b"k3".to_vec(), Some(b"v3".to_vec())),
        ]),
    );
    let _ = replica.on_accept(&e1).await;
    replica.learn_chosen(&e1, None, None).await;

    assert_eq!(
        replica.learner.engine_get(b"k1").await.map(|(_, v)| v),
        Some(b"v1".to_vec())
    );
    assert_eq!(
        replica.learner.engine_get(b"k2").await.map(|(_, v)| v),
        Some(b"v2".to_vec())
    );
    assert_eq!(
        replica.learner.engine_get(b"k3").await.map(|(_, v)| v),
        Some(b"v3".to_vec())
    );
    assert_eq!(replica.contiguous_applied(), 1);
}

// ── Batch: intra-batch last-wins ──────────────────────────────

#[tokio::test]
async fn batch_intra_batch_last_wins() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Follower);

    // put k=v1, then put k=v2, then delete k — in same batch
    let e1 = entry(
        1,
        encode_batch(&[
            (b"k".to_vec(), Some(b"v1".to_vec())),
            (b"k".to_vec(), Some(b"v2".to_vec())),
            (b"k".to_vec(), None), // delete
        ]),
    );
    let _ = replica.on_accept(&e1).await;
    replica.learn_chosen(&e1, None, None).await;

    assert_eq!(
        replica.learner.engine_get(b"k").await,
        None,
        "delete is last op in batch → tombstone wins"
    );
}

// ── Batch: put then delete same key, delete wins ──────────────

#[tokio::test]
async fn batch_put_then_delete_same_key() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Follower);

    let e1 = entry(
        1,
        encode_batch(&[
            (b"k".to_vec(), Some(b"v1".to_vec())),
            (b"k".to_vec(), None), // delete
        ]),
    );
    let _ = replica.on_accept(&e1).await;
    replica.learn_chosen(&e1, None, None).await;

    assert_eq!(
        replica.learner.engine_get(b"k").await,
        None,
        "delete after put in same batch"
    );
}

// ── Batch: delete then put same key, put wins ─────────────────

#[tokio::test]
async fn batch_delete_then_put_same_key() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Follower);

    let e1 = entry(
        1,
        encode_batch(&[
            (b"k".to_vec(), None),                 // delete first
            (b"k".to_vec(), Some(b"v1".to_vec())), // then put
        ]),
    );
    let _ = replica.on_accept(&e1).await;
    replica.learn_chosen(&e1, None, None).await;

    assert_eq!(
        replica.learner.engine_get(b"k").await.map(|(_, v)| v),
        Some(b"v1".to_vec()),
        "put after delete in same batch → value wins"
    );
}

// ── Empty batch (NoOp) ────────────────────────────────────────

#[tokio::test]
async fn empty_batch_is_noop() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Follower);

    let e1 = entry(1, encode_batch(&[]));
    let _ = replica.on_accept(&e1).await;
    replica.learn_chosen(&e1, None, None).await;

    assert_eq!(
        replica.contiguous_applied(),
        1,
        "empty batch still advances frontier"
    );
    assert_eq!(replica.learner.engine_get(b"anything").await, None, "no keys");
}

// ── Multiple slots with mixed ops ─────────────────────────────

#[tokio::test]
async fn multiple_slots_mixed_ops_correctness() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Follower);

    // Slot 1: put k1, k2
    let e1 = entry(
        1,
        encode_batch(&[
            (b"k1".to_vec(), Some(b"v1".to_vec())),
            (b"k2".to_vec(), Some(b"v2".to_vec())),
        ]),
    );
    let _ = replica.on_accept(&e1).await;
    replica.learn_chosen(&e1, None, None).await;

    // Slot 2: overwrite k1
    let e2 = entry(2, encode_put(b"k1", b"v1b"));
    let _ = replica.on_accept(&e2).await;
    replica.learn_chosen(&e2, None, None).await;

    // Slot 3: delete k2, put k3
    let e3 = entry(
        3,
        encode_batch(&[(b"k2".to_vec(), None), (b"k3".to_vec(), Some(b"v3".to_vec()))]),
    );
    let _ = replica.on_accept(&e3).await;
    replica.learn_chosen(&e3, None, None).await;

    assert_eq!(
        replica.learner.engine_get(b"k1").await.map(|(_, v)| v),
        Some(b"v1b".to_vec()),
        "k1 overwritten"
    );
    assert_eq!(replica.learner.engine_get(b"k2").await, None, "k2 deleted");
    assert_eq!(
        replica.learner.engine_get(b"k3").await.map(|(_, v)| v),
        Some(b"v3".to_vec()),
        "k3 put"
    );
    assert_eq!(replica.contiguous_applied(), 3);
}
