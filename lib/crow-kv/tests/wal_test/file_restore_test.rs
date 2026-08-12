// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! WAL durability round-trip over the real `File` backend.
//!
//! The user-facing contract: once records are durably appended, closing the
//! engine and reopening from the same on-disk directory must recover every
//! record plus the reconstructed term / vote / durable-commit watermark, and
//! the reopened engine must be able to resume appending new slots.
//!
//! These complement the `SimDisk` (`BlockDevice`) replay tests by exercising
//! the actual filesystem path (`tempfile` dir, `IoBackend::File`).

use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;
use crow_kv::common::config::WalConfig;
use crow_kv::paxos::roles::{PxBallot, PxLogEntry};
use crow_kv::wal::record::{RecordType, WALRecord};
use crow_kv::wal::replay::replay_group;
use crow_kv::wal::{IoBackend, WalEngine, WalRecordFormat};

const GROUP: u64 = 1;

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

fn accepted_write(slot: u64, term: u64, key: &[u8], value: &[u8]) -> PxLogEntry {
    PxLogEntry {
        slot,
        ballot: PxBallot::new(term, 7),
        term,
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

#[tokio::test]
async fn file_backed_wal_recovers_state_after_reopen() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let wal_dir = tmp.path().join("wal");
    let disks = vec![wal_dir.clone()];

    // ── write phase: append, seal, then drop the engine (close) ──
    {
        let wal = create_file_wal(wal_dir.clone()).await;
        for slot in 1..=3u64 {
            let entry = accepted_write(
                slot,
                5,
                format!("k{slot}").as_bytes(),
                format!("v{slot}").as_bytes(),
            );
            wal.append(&WALRecord::from_accepted(GROUP, &entry))
                .await
                .expect("append accepted");
        }
        wal.append(&WALRecord::from_vote_granted(GROUP, 5, 99))
            .await
            .expect("append vote");
        wal.seal_all().await.expect("seal");
    }

    // ── reopen phase: replay the on-disk directory from scratch ──
    let backend = Arc::new(IoBackend::File);
    let replay = replay_group(&backend, &disks, GROUP).await.expect("replay");

    assert_eq!(replay.current_term, 5, "term must survive reopen");
    assert_eq!(replay.voted_for, Some(99), "vote must survive reopen");
    let accepted_slots: Vec<u64> = replay
        .records
        .iter()
        .filter(|r| matches!(r.record_type, RecordType::Accepted))
        .map(|r| r.slot)
        .collect();
    assert_eq!(
        accepted_slots,
        vec![1, 2, 3],
        "all accepted slots recovered in order"
    );
}

#[tokio::test]
async fn file_backed_wal_resumes_writing_after_reopen() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let wal_dir = tmp.path().join("wal");
    let disks = vec![wal_dir.clone()];

    // ── first session: write slots 1..=2, seal, close ──
    {
        let wal = create_file_wal(wal_dir.clone()).await;
        for slot in 1..=2u64 {
            let entry = accepted_write(
                slot,
                1,
                format!("k{slot}").as_bytes(),
                format!("v{slot}").as_bytes(),
            );
            wal.append(&WALRecord::from_accepted(GROUP, &entry))
                .await
                .expect("append");
        }
        wal.seal_all().await.expect("seal");
    }

    // ── reopen and resume appending from the recovered segment id ──
    {
        let backend = Arc::new(IoBackend::File);
        let replay = replay_group(&backend, &disks, GROUP).await.expect("replay");
        let wal = create_file_wal(wal_dir.clone()).await;
        wal.set_next_segment_id(replay.max_segment_id.saturating_add(1).max(1));

        let entry = accepted_write(3, 1, b"k3", b"v3");
        wal.append(&WALRecord::from_accepted(GROUP, &entry))
            .await
            .expect("append after reopen");
        wal.seal_all().await.expect("seal");
    }

    // ── final reopen: all three slots must be present ──
    let backend = Arc::new(IoBackend::File);
    let replay = replay_group(&backend, &disks, GROUP).await.expect("replay");
    let accepted_slots: Vec<u64> = replay
        .records
        .iter()
        .filter(|r| matches!(r.record_type, RecordType::Accepted))
        .map(|r| r.slot)
        .collect();
    assert_eq!(
        accepted_slots,
        vec![1, 2, 3],
        "records written across two sessions all survive"
    );
}
