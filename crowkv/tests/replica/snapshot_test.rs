// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Snapshot install / truncate-and-resume on a single replica.
//!
//! The `KVEngine::clear` method is the reset hook used by snapshot-install
//! (wipe the local engine, then re-apply a snapshot). These tests verify that
//! after clearing the learner's engine, re-applying entries from the acceptor's
//! accepted log restores the correct KV state and watermarks.

use crate::test_util::iter_all_dyn;
use bytes::Bytes;
use crowkv::cluster::local_replica::{PxLocalReplica, PxLocalReplicaRole};
use crowkv::paxos::roles::{PxBallot, PxLogEntry};

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

#[allow(clippy::cast_possible_truncation)]
fn encode_delete(key: &[u8]) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&1u16.to_le_bytes()); // op_count
    buf.push(1u8); // Delete
    buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
    buf.extend_from_slice(key);
    buf.extend_from_slice(&0u32.to_le_bytes());
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

fn delete_entry(slot: u64, key: &[u8]) -> PxLogEntry {
    PxLogEntry {
        slot,
        ballot: PxBallot::new(0, 1),
        term: 1,
        payload: Bytes::from(encode_delete(key)),
    }
}

#[tokio::test]
async fn clear_wipes_kv_state_but_preserves_accepted_log() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Leader);

    // Commit slots 1–3.
    for (slot, key, value) in [(1u64, b"k1", b"v1"), (2, b"k2", b"v2"), (3, b"k3", b"v3")] {
        let entry = write_entry(slot, key, value);
        let _ = replica.on_accept(&entry).await;
        replica.learn_chosen(&entry, &[]).await;
    }
    assert_eq!(replica.contiguous_applied(), 3);
    assert_eq!(
        replica.learner.engine_get(b"k1").await.map(|(_, v)| v),
        Some(b"v1".to_vec())
    );

    // Clear the engine (snapshot-install reset).
    replica.learner.engine().clear();
    assert_eq!(replica.learner.engine_get(b"k1").await, None);
    assert_eq!(replica.learner.engine_get(b"k2").await, None);
    assert_eq!(replica.learner.live_key_count(), 0);

    // The acceptor's accepted log is preserved — we can still read entries.
    assert!(replica.accepted_at(1).await.is_some());
    assert!(replica.accepted_at(2).await.is_some());
    assert!(replica.accepted_at(3).await.is_some());
}

#[tokio::test]
async fn re_apply_after_clear_restores_kv_state() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Leader);

    // Commit slots 1–2 with puts, slot 3 with a delete of k1.
    let e1 = write_entry(1, b"k1", b"v1");
    let e2 = write_entry(2, b"k2", b"v2");
    let e3 = delete_entry(3, b"k1");
    for entry in [&e1, &e2, &e3] {
        let _ = replica.on_accept(entry).await;
        replica.learn_chosen(entry, &[]).await;
    }
    assert_eq!(replica.contiguous_applied(), 3);
    assert_eq!(
        replica.learner.engine_get(b"k1").await,
        None,
        "k1 deleted at slot 3"
    );
    assert_eq!(
        replica.learner.engine_get(b"k2").await.map(|(_, v)| v),
        Some(b"v2".to_vec())
    );

    // Clear and re-apply from the accepted log.
    replica.learner.engine().clear();
    for slot in 1..=3u64 {
        let entry = replica.accepted_at(slot).await.expect("accepted entry");
        replica.learn_chosen(&entry, &[]).await;
    }

    // KV state is restored correctly, including the delete.
    assert_eq!(
        replica.learner.engine_get(b"k1").await,
        None,
        "delete survives re-apply"
    );
    assert_eq!(
        replica.learner.engine_get(b"k2").await.map(|(_, v)| v),
        Some(b"v2".to_vec()),
        "put survives re-apply"
    );
}

#[tokio::test]
async fn re_apply_after_clear_restores_watermarks() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Leader);

    for slot in 1..=3u64 {
        let entry = write_entry(slot, b"k", b"v");
        let _ = replica.on_accept(&entry).await;
        replica.learn_chosen(&entry, &[]).await;
    }
    assert_eq!(replica.contiguous_applied(), 3);

    // Clear and re-apply.
    replica.learner.engine().clear();
    for slot in 1..=3u64 {
        let entry = replica.accepted_at(slot).await.expect("accepted entry");
        replica.learn_chosen(&entry, &[]).await;
    }

    // Watermarks are restored.
    assert_eq!(replica.contiguous_applied(), 3);
    assert_eq!(replica.contiguous_chosen(), 3);
}

#[tokio::test]
async fn clear_drops_all_keys_including_tombstones() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Leader);

    // Put then delete the same key.
    let e1 = write_entry(1, b"k", b"v");
    let e2 = delete_entry(2, b"k");
    let _ = replica.on_accept(&e1).await;
    replica.learn_chosen(&e1, &[]).await;
    let _ = replica.on_accept(&e2).await;
    replica.learn_chosen(&e2, &[]).await;

    // Tombstone is retained internally.
    assert_eq!(iter_all_dyn(replica.learner.engine()).len(), 1);

    // Clear removes everything.
    replica.learner.engine().clear();
    assert!(iter_all_dyn(replica.learner.engine()).is_empty());
}
