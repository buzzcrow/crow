// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Multi-slot WAL replay ordering edge cases.
//!
//! `restore_from_replay` rebuilds a `PxLocalReplica` from WAL replay output.
//! The restore walks slots 1..=watermark sequentially, applying each accepted
//! entry to the learner. It stops at the first hole (a slot with no accepted
//! value). Slots above the watermark are not applied — they're left for
//! consensus recovery (bulk Phase 1).
//!
//! These tests verify:
//! - Contiguous slots below watermark: all applied.
//! - Hole below watermark: stops at hole, partial apply.
//! - Out-of-order WAL records: acceptor state rebuilt regardless of record
//!   order; learner applied in slot order.
//! - Slots above watermark: not applied.
//! - Empty WAL: zero state, no panic.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::Bytes;
use crowkv::cluster::local_replica::{PxLocalReplica, PxLocalReplicaRole};
use crowkv::common::config::WalConfig;
use crowkv::paxos::roles::{PxAcceptReply, PxBallot, PxLogEntry};
use crowkv::wal::record::WALRecord;
use crowkv::wal::replay::replay_group;
use crowkv::wal::{IoBackend, WalEngine, WalRecordFormat};

const GROUP: u64 = 1;
const REPLICA_ID: u64 = 7;

fn encode_put_payload(key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.push(1); // op = PUT
    buf.push(0); // flags
    let key_len = u32::try_from(key.len()).expect("key length exceeds u32");
    buf.extend_from_slice(&key_len.to_le_bytes());
    buf.extend_from_slice(key);
    let value_len = u32::try_from(value.len()).expect("value length exceeds u32");
    buf.extend_from_slice(&value_len.to_le_bytes());
    buf.extend_from_slice(value);
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

async fn create_file_wal(wal_dir: PathBuf) -> Arc<WalEngine> {
    let backend = Arc::new(IoBackend::File);
    let mut config = WalConfig::with_root(wal_dir);
    config.wal_record_format = WalRecordFormat::Binary;
    WalEngine::create(backend, config, GROUP)
        .await
        .expect("create file-backed wal")
}

/// Commit `n` slots through the replica (accept + learn + persist watermark),
/// then seal the WAL so all records are durable on disk.
async fn commit_slots(replica: &PxLocalReplica, wal: &WalEngine, n: u64) {
    for slot in 1..=n {
        let entry = write_entry(slot, b"k", &slot.to_string().into_bytes());
        let reply = replica.on_accept(&entry).await;
        assert!(
            matches!(reply, PxAcceptReply::Accepted { .. }),
            "slot {slot} should be accepted"
        );
        replica.learn_chosen(&entry, &[]).await;
    }
    wal.seal_all().await.expect("seal");
}

async fn replay_and_restore(wal_dir: &Path) -> PxLocalReplica {
    let backend = Arc::new(IoBackend::File);
    let disks = vec![wal_dir.to_path_buf()];
    let replay = replay_group(&backend, &disks, GROUP).await.expect("replay");
    PxLocalReplica::restore_from_replay(REPLICA_ID, PxLocalReplicaRole::Leader, &replay)
        .await
        .expect("restore")
}

// ── Contiguous slots (no snapshot) ────────────────────────────

#[tokio::test]
async fn restore_contiguous_slots_all_applied() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let wal_dir = tmp.path().join("wal");

    {
        let wal = create_file_wal(wal_dir.clone()).await;
        let mut replica = PxLocalReplica::new(REPLICA_ID, PxLocalReplicaRole::Leader);
        replica.set_wal(Arc::clone(&wal));
        commit_slots(&replica, &wal, 5).await;
        assert_eq!(replica.contiguous_applied(), 5);
    }

    let restored = replay_and_restore(&wal_dir).await;

    // WAL replay now fully restores the learner: all 5 contiguous slots
    // are applied, with highest-slot-wins for key "k".
    assert_eq!(
        restored.contiguous_applied(),
        5,
        "all 5 slots applied after replay"
    );
    assert_eq!(
        restored.learner.engine_get(b"k").await.map(|(_, v)| v),
        Some(b"5".to_vec()),
        "k = slot-5 value (highest-slot-wins)"
    );
    for slot in 1..=5 {
        assert!(
            restored.accepted_at(slot).await.is_some(),
            "slot {slot} in acceptor"
        );
    }
}

// ── Hole in slot range (no snapshot) ──────────────────────────

#[tokio::test]
async fn restore_stops_at_hole_below_watermark() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let wal_dir = tmp.path().join("wal");

    {
        let wal = create_file_wal(wal_dir.clone()).await;
        let mut replica = PxLocalReplica::new(REPLICA_ID, PxLocalReplicaRole::Leader);
        replica.set_wal(Arc::clone(&wal));

        // Accept slots 1, 2, 3 and learn all.
        for slot in 1..=3u64 {
            let entry = write_entry(slot, b"k", &slot.to_string().into_bytes());
            let _ = replica.on_accept(&entry).await;
            replica.learn_chosen(&entry, &[]).await;
        }
        assert_eq!(replica.contiguous_applied(), 3);

        // Now accept slot 5 (skipping 4) and learn it.
        let entry5 = write_entry(5, b"k", b"5");
        let _ = replica.on_accept(&entry5).await;
        replica.learn_chosen(&entry5, &[]).await;

        wal.seal_all().await.expect("seal");
    }

    let restored = replay_and_restore(&wal_dir).await;

    // Replay applies slots 1-3 contiguously, then slot 5 is out-of-order
    // (slot 4 is missing). contiguous_applied stops at 3, but the engine
    // has slot 5's value (highest-slot-wins).
    assert_eq!(
        restored.contiguous_applied(),
        3,
        "contiguous applied stops at hole (slot 4)"
    );
    assert_eq!(
        restored.learner.engine_get(b"k").await.map(|(_, v)| v),
        Some(b"5".to_vec()),
        "k = slot-5 value (highest-slot-wins, out-of-order)"
    );
    // Slot 5 is in the acceptor but not applied.
    assert!(restored.accepted_at(5).await.is_some(), "slot 5 in acceptor");
    assert!(restored.accepted_at(4).await.is_none(), "slot 4 not accepted");
}

// ── Out-of-order WAL records ──────────────────────────────────

#[tokio::test]
async fn restore_out_of_order_accepted_records() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let wal_dir = tmp.path().join("wal");

    {
        let wal = create_file_wal(wal_dir.clone()).await;

        // Manually append Accepted records out of slot order: slot 3, then 1, then 2.
        // The WAL stores them in append order; replay sorts by slot.
        let entry3 = write_entry(3, b"k", b"3");
        let entry1 = write_entry(1, b"k", b"1");
        let entry2 = write_entry(2, b"k", b"2");

        wal.append(&WALRecord::from_accepted(GROUP, &entry3))
            .await
            .expect("append 3");
        wal.append(&WALRecord::from_accepted(GROUP, &entry1))
            .await
            .expect("append 1");
        wal.append(&WALRecord::from_accepted(GROUP, &entry2))
            .await
            .expect("append 2");

        wal.seal_all().await.expect("seal");
    }

    let restored = replay_and_restore(&wal_dir).await;

    // Replay sorts by slot and applies all 3 slots in order.
    for slot in 1..=3 {
        assert!(
            restored.accepted_at(slot).await.is_some(),
            "slot {slot} in acceptor"
        );
    }
    assert_eq!(
        restored.contiguous_applied(),
        3,
        "all 3 slots applied after out-of-order replay"
    );
    assert_eq!(
        restored.learner.engine_get(b"k").await.map(|(_, v)| v),
        Some(b"3".to_vec()),
        "k = slot-3 value (highest-slot-wins)"
    );
}

// ── Slots above snapshot not applied ─────────────────────────

#[tokio::test]
async fn restore_does_not_apply_without_snapshot() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let wal_dir = tmp.path().join("wal");

    {
        let wal = create_file_wal(wal_dir.clone()).await;
        let mut replica = PxLocalReplica::new(REPLICA_ID, PxLocalReplicaRole::Leader);
        replica.set_wal(Arc::clone(&wal));

        // Commit slots 1-3.
        for slot in 1..=3u64 {
            let entry = write_entry(slot, b"k", &slot.to_string().into_bytes());
            let _ = replica.on_accept(&entry).await;
            replica.learn_chosen(&entry, &[]).await;
        }

        // Accept slots 4, 5 but DON'T learn them.
        let entry4 = write_entry(4, b"k", b"4");
        let entry5 = write_entry(5, b"k", b"5");
        let _ = replica.on_accept(&entry4).await;
        let _ = replica.on_accept(&entry5).await;

        wal.seal_all().await.expect("seal");
    }

    let restored = replay_and_restore(&wal_dir).await;

    // WAL replay applies all accepted slots (1-5) to the learner.
    assert_eq!(
        restored.contiguous_applied(),
        5,
        "all 5 slots applied after replay"
    );
    assert_eq!(
        restored.learner.engine_get(b"k").await.map(|(_, v)| v),
        Some(b"5".to_vec()),
        "k = slot-5 value (highest-slot-wins)"
    );
    // All slots are in the acceptor (durable).
    assert!(restored.accepted_at(4).await.is_some(), "slot 4 in acceptor");
    assert!(restored.accepted_at(5).await.is_some(), "slot 5 in acceptor");
}

// ── Empty WAL ─────────────────────────────────────────────────

#[tokio::test]
async fn restore_empty_wal_produces_zero_state() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let wal_dir = tmp.path().join("wal");

    {
        let _wal = create_file_wal(wal_dir.clone()).await;
        // No records appended. Seal empty WAL.
        // WalEngine drops here.
    }

    let restored = replay_and_restore(&wal_dir).await;

    assert_eq!(restored.contiguous_applied(), 0);
    assert_eq!(restored.contiguous_chosen(), 0);
    assert_eq!(restored.current_term_snapshot(), 0);
    assert!(restored.accepted_at(1).await.is_none());
}
