// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Replica-layer persistence round-trip.
//!
//! Build a `PxLocalReplica` backed by a real `File` WAL, commit a sequence of
//! KV writes through the replica's own durability path (`on_accept` logs the
//! accepted value; `learn_chosen` applies it and persists the durable-commit
//! watermark), then drop the replica to simulate a process restart. Rebuilding
//! from the on-disk WAL via `replay_group` + `restore_from_replay` must reload
//! the committed KV state, the applied frontier, and the per-slot accepted log.
//!
//! This is the replica-level analogue of the WAL `restore_*` tests: it proves
//! the layer the local replica is responsible for — "do KV here, close, reopen,
//! and find the state again".

use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;
use crowdb_kv::cluster::local_replica::{PxLocalReplica, PxLocalReplicaRole};
use crowdb_kv::common::config::WalConfig;
use crowdb_kv::paxos::roles::{PxBallot, PxLogEntry};
use crowdb_kv::wal::replay::replay_group;
use crowdb_kv::wal::{IoBackend, WalEngine, WalRecordFormat};

const GROUP: u64 = 1;
const REPLICA_ID: u64 = 7;

fn encode_put_payload(key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&1u16.to_le_bytes()); // op_count = 1
    buf.push(0); // flags
    let key_len = u32::try_from(key.len()).expect("key length exceeds u32");
    buf.extend_from_slice(&key_len.to_le_bytes());
    buf.extend_from_slice(key);
    let value_len = u32::try_from(value.len()).expect("value length exceeds u32");
    buf.extend_from_slice(&value_len.to_le_bytes());
    buf.extend_from_slice(value);
    buf
}

fn encode_delete_payload(key: &[u8]) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&1u16.to_le_bytes()); // op_count = 1
    buf.push(1); // kind = Delete
    let key_len = u32::try_from(key.len()).expect("key length exceeds u32");
    buf.extend_from_slice(&key_len.to_le_bytes());
    buf.extend_from_slice(key);
    buf.extend_from_slice(&0u32.to_le_bytes()); // value_len = 0
    buf
}

#[allow(clippy::cast_possible_truncation)]
fn encode_batch_payload(ops: &[(Vec<u8>, Option<Vec<u8>>)]) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&(ops.len() as u16).to_le_bytes()); // op_count
    for (key, value) in ops {
        if let Some(v) = value {
            buf.push(0); // kind = Put
            let key_len = u32::try_from(key.len()).expect("key len");
            buf.extend_from_slice(&key_len.to_le_bytes());
            buf.extend_from_slice(key);
            let val_len = u32::try_from(v.len()).expect("val len");
            buf.extend_from_slice(&val_len.to_le_bytes());
            buf.extend_from_slice(v);
        } else {
            buf.push(1); // kind = Delete
            let key_len = u32::try_from(key.len()).expect("key len");
            buf.extend_from_slice(&key_len.to_le_bytes());
            buf.extend_from_slice(key);
            buf.extend_from_slice(&0u32.to_le_bytes()); // value_len = 0
        }
    }
    buf
}

fn write_entry(slot: u64, key: &[u8], value: &[u8]) -> PxLogEntry {
    PxLogEntry {
        slot,
        ballot: PxBallot::new(1, REPLICA_ID),
        term: 1,
        payload: Bytes::from(encode_put_payload(key, value)),
    }
}

fn delete_entry(slot: u64, key: &[u8]) -> PxLogEntry {
    PxLogEntry {
        slot,
        ballot: PxBallot::new(1, REPLICA_ID),
        term: 1,
        payload: Bytes::from(encode_delete_payload(key)),
    }
}

fn batch_entry(slot: u64, ops: &[(Vec<u8>, Option<Vec<u8>>)]) -> PxLogEntry {
    PxLogEntry {
        slot,
        ballot: PxBallot::new(1, REPLICA_ID),
        term: 1,
        payload: Bytes::from(encode_batch_payload(ops)),
    }
}

async fn create_file_wal(wal_dir: PathBuf) -> Arc<WalEngine> {
    let backend = Arc::new(IoBackend::File);
    let mut config = WalConfig::with_root(wal_dir);
    config.wal_record_format = WalRecordFormat::Binary;
    WalEngine::create(backend, config, GROUP)
        .await
        .expect("create file-backed wal")
}

#[tokio::test]
async fn wal_backed_replica_reloads_committed_kv_after_restart() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let wal_dir = tmp.path().join("wal");
    let disks = vec![wal_dir.clone()];

    // ── write phase: commit three slots through the replica, then close ──
    // Slot 3 overwrites "alpha" so we also prove highest-slot-wins survives.
    {
        let wal = create_file_wal(wal_dir.clone()).await;
        let mut replica = PxLocalReplica::new(REPLICA_ID, PxLocalReplicaRole::Leader);
        replica.set_wal(Arc::clone(&wal));

        for (slot, key, value) in [
            (1u64, b"alpha".as_slice(), b"1".as_slice()),
            (2, b"beta", b"2"),
            (3, b"alpha", b"3"),
        ] {
            let entry = write_entry(slot, key, value);
            let reply = replica.on_accept(&entry).await;
            assert!(
                matches!(reply, crowdb_kv::paxos::roles::PxAcceptReply::Accepted { .. }),
                "slot {slot} should be accepted: {reply:?}"
            );
            replica.learn_chosen(&entry, &[]).await;
        }
        assert_eq!(replica.contiguous_applied(), 3, "applied frontier at slot 3");
        wal.seal_all().await.expect("seal");
        // `replica` and `wal` drop here — the in-memory state is gone.
    }

    // ── reopen phase: rebuild purely from the on-disk WAL ──
    let backend = Arc::new(IoBackend::File);
    let replay = replay_group(&backend, &disks, GROUP).await.expect("replay");

    let restored = PxLocalReplica::restore_from_replay(REPLICA_ID, PxLocalReplicaRole::Leader, &replay)
        .await
        .expect("restore replica");

    // WAL replay now fully restores the learner: every accepted entry is
    // replayed into the state machine.
    assert_eq!(
        restored.learner.engine_get(b"alpha").await.map(|(_, v)| v),
        Some(b"3".to_vec()),
        "alpha = slot-3 value (highest-slot-wins)"
    );
    assert_eq!(
        restored.learner.engine_get(b"beta").await.map(|(_, v)| v),
        Some(b"2".to_vec()),
        "beta = slot-2 value"
    );
    assert_eq!(restored.contiguous_applied(), 3, "all 3 slots applied");
    assert!(restored.accepted_at(1).await.is_some());
    assert!(restored.accepted_at(3).await.is_some());
}

#[tokio::test]
async fn delete_survives_wal_restart() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let wal_dir = tmp.path().join("wal");
    let disks = vec![wal_dir.clone()];

    {
        let wal = create_file_wal(wal_dir.clone()).await;
        let mut replica = PxLocalReplica::new(REPLICA_ID, PxLocalReplicaRole::Leader);
        replica.set_wal(Arc::clone(&wal));

        // Slot 1: put "k1" = "v1"
        let e1 = write_entry(1, b"k1", b"v1");
        let _ = replica.on_accept(&e1).await;
        replica.learn_chosen(&e1, &[]).await;

        // Slot 2: delete "k1"
        let e2 = delete_entry(2, b"k1");
        let _ = replica.on_accept(&e2).await;
        replica.learn_chosen(&e2, &[]).await;

        assert_eq!(replica.contiguous_applied(), 2);
        assert_eq!(
            replica.learner.engine_get(b"k1").await,
            None,
            "key deleted in memory"
        );
        wal.seal_all().await.expect("seal");
    }

    let backend = Arc::new(IoBackend::File);
    let replay = replay_group(&backend, &disks, GROUP).await.expect("replay");
    let restored = PxLocalReplica::restore_from_replay(REPLICA_ID, PxLocalReplicaRole::Leader, &replay)
        .await
        .expect("restore");

    // Slot 2 deleted k1 — replay applies both slots, k1 stays deleted.
    assert_eq!(
        restored.learner.engine_get(b"k1").await,
        None,
        "k1 stays deleted after replay"
    );
    assert_eq!(restored.contiguous_applied(), 2, "both slots applied");
}

#[tokio::test]
async fn put_then_delete_same_key_survives_restart() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let wal_dir = tmp.path().join("wal");
    let disks = vec![wal_dir.clone()];

    {
        let wal = create_file_wal(wal_dir.clone()).await;
        let mut replica = PxLocalReplica::new(REPLICA_ID, PxLocalReplicaRole::Leader);
        replica.set_wal(Arc::clone(&wal));

        // Slot 1: put "k" = "v1"
        let e1 = write_entry(1, b"k", b"v1");
        let _ = replica.on_accept(&e1).await;
        replica.learn_chosen(&e1, &[]).await;

        // Slot 2: put "k" = "v2" (overwrite)
        let e2 = write_entry(2, b"k", b"v2");
        let _ = replica.on_accept(&e2).await;
        replica.learn_chosen(&e2, &[]).await;

        // Slot 3: delete "k"
        let e3 = delete_entry(3, b"k");
        let _ = replica.on_accept(&e3).await;
        replica.learn_chosen(&e3, &[]).await;

        assert_eq!(replica.contiguous_applied(), 3);
        assert_eq!(replica.learner.engine_get(b"k").await, None, "key deleted");
        wal.seal_all().await.expect("seal");
    }

    let backend = Arc::new(IoBackend::File);
    let replay = replay_group(&backend, &disks, GROUP).await.expect("replay");
    let restored = PxLocalReplica::restore_from_replay(REPLICA_ID, PxLocalReplicaRole::Leader, &replay)
        .await
        .expect("restore");

    // Slot 3 deleted k — replay applies all 3 slots, k stays deleted.
    assert_eq!(
        restored.learner.engine_get(b"k").await,
        None,
        "k stays deleted after replay"
    );
    assert_eq!(restored.contiguous_applied(), 3, "all 3 slots applied");
}

#[tokio::test]
async fn batch_with_put_and_delete_survives_restart() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let wal_dir = tmp.path().join("wal");
    let disks = vec![wal_dir.clone()];

    {
        let wal = create_file_wal(wal_dir.clone()).await;
        let mut replica = PxLocalReplica::new(REPLICA_ID, PxLocalReplicaRole::Leader);
        replica.set_wal(Arc::clone(&wal));

        // Slot 1: batch with 3 ops — put k1, put k2, delete k1 (intra-batch last-wins)
        let e1 = batch_entry(
            1,
            &[
                (b"k1".to_vec(), Some(b"v1".to_vec())),
                (b"k2".to_vec(), Some(b"v2".to_vec())),
                (b"k1".to_vec(), None), // delete k1 — should win over put in same batch
            ],
        );
        let _ = replica.on_accept(&e1).await;
        replica.learn_chosen(&e1, &[]).await;

        // Slot 2: put k3
        let e2 = write_entry(2, b"k3", b"v3");
        let _ = replica.on_accept(&e2).await;
        replica.learn_chosen(&e2, &[]).await;

        assert_eq!(replica.contiguous_applied(), 2);
        assert_eq!(
            replica.learner.engine_get(b"k1").await,
            None,
            "k1 deleted in batch"
        );
        assert_eq!(
            replica.learner.engine_get(b"k2").await.map(|(_, v)| v),
            Some(b"v2".to_vec()),
            "k2 put in batch"
        );
        assert_eq!(
            replica.learner.engine_get(b"k3").await.map(|(_, v)| v),
            Some(b"v3".to_vec()),
            "k3 put in slot 2"
        );
        wal.seal_all().await.expect("seal");
    }

    let backend = Arc::new(IoBackend::File);
    let replay = replay_group(&backend, &disks, GROUP).await.expect("replay");
    let restored = PxLocalReplica::restore_from_replay(REPLICA_ID, PxLocalReplicaRole::Leader, &replay)
        .await
        .expect("restore");

    // Replay applies both slots. k1 was deleted in the batch (last-wins),
    // k2 and k3 survive.
    assert_eq!(
        restored.learner.engine_get(b"k1").await,
        None,
        "k1 deleted in batch slot 1"
    );
    assert_eq!(
        restored.learner.engine_get(b"k2").await.map(|(_, v)| v),
        Some(b"v2".to_vec()),
        "k2 put in batch slot 1"
    );
    assert_eq!(
        restored.learner.engine_get(b"k3").await.map(|(_, v)| v),
        Some(b"v3".to_vec()),
        "k3 put in slot 2"
    );
    assert_eq!(restored.contiguous_applied(), 2, "both slots applied");
}

#[tokio::test]
async fn mixed_put_delete_batch_survives_restart() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let wal_dir = tmp.path().join("wal");
    let disks = vec![wal_dir.clone()];

    {
        let wal = create_file_wal(wal_dir.clone()).await;
        let mut replica = PxLocalReplica::new(REPLICA_ID, PxLocalReplicaRole::Leader);
        replica.set_wal(Arc::clone(&wal));

        // Slot 1: put k1, k2, k3
        let e1 = batch_entry(
            1,
            &[
                (b"k1".to_vec(), Some(b"v1".to_vec())),
                (b"k2".to_vec(), Some(b"v2".to_vec())),
                (b"k3".to_vec(), Some(b"v3".to_vec())),
            ],
        );
        let _ = replica.on_accept(&e1).await;
        replica.learn_chosen(&e1, &[]).await;

        // Slot 2: delete k2 (single-op entry)
        let e2 = delete_entry(2, b"k2");
        let _ = replica.on_accept(&e2).await;
        replica.learn_chosen(&e2, &[]).await;

        // Slot 3: batch — put k4, delete k1, put k5
        let e3 = batch_entry(
            3,
            &[
                (b"k4".to_vec(), Some(b"v4".to_vec())),
                (b"k1".to_vec(), None), // delete k1
                (b"k5".to_vec(), Some(b"v5".to_vec())),
            ],
        );
        let _ = replica.on_accept(&e3).await;
        replica.learn_chosen(&e3, &[]).await;

        assert_eq!(replica.contiguous_applied(), 3);

        // Verify correctness before restart.
        assert_eq!(
            replica.learner.engine_get(b"k1").await,
            None,
            "k1 deleted in batch slot 3"
        );
        assert_eq!(
            replica.learner.engine_get(b"k2").await,
            None,
            "k2 deleted in slot 2"
        );
        assert_eq!(
            replica.learner.engine_get(b"k3").await.map(|(_, v)| v),
            Some(b"v3".to_vec()),
            "k3 survives"
        );
        assert_eq!(
            replica.learner.engine_get(b"k4").await.map(|(_, v)| v),
            Some(b"v4".to_vec()),
            "k4 put in batch slot 3"
        );
        assert_eq!(
            replica.learner.engine_get(b"k5").await.map(|(_, v)| v),
            Some(b"v5".to_vec()),
            "k5 put in batch slot 3"
        );

        wal.seal_all().await.expect("seal");
    }

    let backend = Arc::new(IoBackend::File);
    let replay = replay_group(&backend, &disks, GROUP).await.expect("replay");
    let restored = PxLocalReplica::restore_from_replay(REPLICA_ID, PxLocalReplicaRole::Leader, &replay)
        .await
        .expect("restore");

    // Replay applies all 3 slots. k1 and k2 are deleted, k3/k4/k5 survive.
    assert_eq!(
        restored.learner.engine_get(b"k1").await,
        None,
        "k1 deleted in batch slot 3"
    );
    assert_eq!(
        restored.learner.engine_get(b"k2").await,
        None,
        "k2 deleted in slot 2"
    );
    assert_eq!(
        restored.learner.engine_get(b"k3").await.map(|(_, v)| v),
        Some(b"v3".to_vec()),
        "k3 survives"
    );
    assert_eq!(
        restored.learner.engine_get(b"k4").await.map(|(_, v)| v),
        Some(b"v4".to_vec()),
        "k4 put in batch slot 3"
    );
    assert_eq!(
        restored.learner.engine_get(b"k5").await.map(|(_, v)| v),
        Some(b"v5".to_vec()),
        "k5 put in batch slot 3"
    );
    assert_eq!(restored.contiguous_applied(), 3, "all 3 slots applied");
}
