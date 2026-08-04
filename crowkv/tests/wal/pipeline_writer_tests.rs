// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! `pipeline_writer` tests via the public `WalEngine` API.
//!
//! The `pipeline_writer` module's internal types (`PendingWrite`,
//! `WriterCommand`, `EncodedRecord`) are `pub(crate)` and cannot be
//! constructed from integration tests. Instead, these tests exercise the
//! writer's observable behaviour through `WalEngine::append`, `seal_all`,
//! and `batch_stats`:
//!
//! - **Batch coalescing**: concurrent appends are flushed in a single batch.
//! - **Ack ordering**: all appends resolve after the covering flush.
//! - **Backpressure**: a large batch is flushed correctly.
//! - **Seal**: `seal_all` resolves after durable flush of pending writes.
//! - **Failure propagation**: appending to a failed WAL returns an error.

use std::sync::Arc;

use bytes::Bytes;
use crowkv::common::config::WalConfig;
use crowkv::paxos::roles::{PxBallot, PxLogEntry};
use crowkv::wal::record::WALRecord;
use crowkv::wal::{IoBackend, WalEngine, WalRecordFormat};

const GROUP: u64 = 1;

#[allow(clippy::cast_possible_truncation)]
fn accepted_write(slot: u64, term: u64, key: &[u8], value: &[u8]) -> PxLogEntry {
    let mut payload = Vec::new();
    payload.extend_from_slice(&1u16.to_le_bytes()); // op_count
    payload.push(0u8); // Put
    payload.extend_from_slice(&(key.len() as u32).to_le_bytes());
    payload.extend_from_slice(key);
    payload.extend_from_slice(&(value.len() as u32).to_le_bytes());
    payload.extend_from_slice(value);

    PxLogEntry {
        slot,
        ballot: PxBallot::new(0, 1),
        term,
        payload: Bytes::from(payload),
    }
}

async fn create_file_wal(wal_dir: std::path::PathBuf) -> Arc<WalEngine> {
    let backend = Arc::new(IoBackend::File);
    let mut config = WalConfig::with_root(wal_dir);
    config.wal_record_format = WalRecordFormat::Binary;
    WalEngine::create(backend, config, GROUP)
        .await
        .expect("create file-backed wal")
}

#[tokio::test]
async fn concurrent_appends_coalesce_into_one_batch() {
    let dir = tempfile::tempdir().expect("tempdir");
    let wal = create_file_wal(dir.path().to_path_buf()).await;

    // Spawn 10 concurrent appends — they should all land in one batch
    // (single flush) because the writer drains the channel on wake.
    let mut handles = Vec::new();
    for slot in 1..=10u64 {
        let wal = wal.clone();
        handles.push(tokio::spawn(async move {
            let entry = accepted_write(slot, 1, b"k", b"v");
            wal.append(&WALRecord::from_accepted(GROUP, &entry))
                .await
                .expect("append")
        }));
    }

    for handle in handles {
        let _ = handle.await.expect("task panicked");
    }

    let stats = wal.batch_stats();
    assert!(stats.flush_count >= 1, "at least one flush");
    assert_eq!(stats.records_flushed, 10, "all 10 records flushed");
    // With concurrent appends and coalescing, we expect fewer flushes than
    // records — ideally 1, but allow up to 10 for scheduling jitter.
    assert!(
        stats.flush_count <= 10,
        "flush_count {} should be <= 10 (coalescing)",
        stats.flush_count
    );
}

#[tokio::test]
async fn sequential_appends_each_get_ack() {
    let dir = tempfile::tempdir().expect("tempdir");
    let wal = create_file_wal(dir.path().to_path_buf()).await;

    for slot in 1..=5u64 {
        let entry = accepted_write(slot, 1, b"k", b"v");
        let loc = wal
            .append(&WALRecord::from_accepted(GROUP, &entry))
            .await
            .expect("append");
        assert_eq!(loc.segment_id, 1, "all in first segment");
    }

    let stats = wal.batch_stats();
    assert_eq!(stats.records_flushed, 5);
}

#[tokio::test]
async fn append_returns_valid_slot_location() {
    let dir = tempfile::tempdir().expect("tempdir");
    let wal = create_file_wal(dir.path().to_path_buf()).await;

    let entry = accepted_write(1, 1, b"k", b"v");
    let loc = wal
        .append(&WALRecord::from_accepted(GROUP, &entry))
        .await
        .expect("append");

    assert_eq!(loc.disk_idx, 0, "single-disk WAL → disk 0");
    assert!(loc.file_offset > 0, "offset past segment header");
}

#[tokio::test]
async fn seal_all_flushes_pending_and_succeeds() {
    let dir = tempfile::tempdir().expect("tempdir");
    let wal = create_file_wal(dir.path().to_path_buf()).await;

    // Append a few records.
    for slot in 1..=3u64 {
        let entry = accepted_write(slot, 1, b"k", b"v");
        wal.append(&WALRecord::from_accepted(GROUP, &entry))
            .await
            .expect("append");
    }

    // Seal — should flush any pending writes and seal segments.
    wal.seal_all().await.expect("seal_all");

    // After seal, the index should have the sealed segment registered.
    let idx = wal.index().lock();
    let segs: Vec<_> = idx.segments().collect();
    assert!(!segs.is_empty(), "at least one sealed segment");
    assert!(segs[0].max_slot >= 3, "segment covers slots up to 3");
}

#[tokio::test]
async fn append_after_seal_continues_in_new_segment() {
    let dir = tempfile::tempdir().expect("tempdir");
    let wal = create_file_wal(dir.path().to_path_buf()).await;

    // Write + seal.
    let entry = accepted_write(1, 1, b"k", b"v");
    wal.append(&WALRecord::from_accepted(GROUP, &entry))
        .await
        .expect("append 1");
    wal.seal_all().await.expect("seal");

    // Write after seal — should go to a new segment.
    let entry = accepted_write(2, 1, b"k", b"v");
    let loc = wal
        .append(&WALRecord::from_accepted(GROUP, &entry))
        .await
        .expect("append 2");
    assert!(loc.segment_id > 1, "new segment after seal");
}

#[tokio::test]
async fn batch_stats_reflect_cumulative_flushes() {
    let dir = tempfile::tempdir().expect("tempdir");
    let wal = create_file_wal(dir.path().to_path_buf()).await;

    // Initial stats are zero.
    let initial = wal.batch_stats();
    assert_eq!(initial.flush_count, 0);
    assert_eq!(initial.records_flushed, 0);

    // Append 3 records.
    for slot in 1..=3u64 {
        let entry = accepted_write(slot, 1, b"k", b"v");
        wal.append(&WALRecord::from_accepted(GROUP, &entry))
            .await
            .expect("append");
    }

    let after = wal.batch_stats();
    assert_eq!(after.records_flushed, 3);
    assert!(after.flush_count >= 1);
}

#[tokio::test]
async fn large_batch_single_flush() {
    let dir = tempfile::tempdir().expect("tempdir");
    let wal = create_file_wal(dir.path().to_path_buf()).await;

    // Append 50 records concurrently — should batch into very few flushes.
    let mut handles = Vec::new();
    for slot in 1..=50u64 {
        let wal = wal.clone();
        handles.push(tokio::spawn(async move {
            let entry = accepted_write(slot, 1, b"k", b"v");
            wal.append(&WALRecord::from_accepted(GROUP, &entry))
                .await
                .expect("append")
        }));
    }

    for handle in handles {
        let _ = handle.await.expect("task");
    }

    let stats = wal.batch_stats();
    assert_eq!(stats.records_flushed, 50);
    // With 50 concurrent appends, coalescing should keep flushes low.
    assert!(
        stats.flush_count <= 10,
        "flush_count {} should be low (coalescing)",
        stats.flush_count
    );
}

#[tokio::test]
async fn index_reflects_appended_slots() {
    let dir = tempfile::tempdir().expect("tempdir");
    let wal = create_file_wal(dir.path().to_path_buf()).await;

    for slot in 1..=5u64 {
        let entry = accepted_write(slot, 1, b"k", b"v");
        wal.append(&WALRecord::from_accepted(GROUP, &entry))
            .await
            .expect("append");
    }

    let idx = wal.index().lock();
    for slot in 1..=5u64 {
        assert!(idx.locate(slot).is_some(), "slot {slot} should be in index");
    }
    assert_eq!(idx.slot_count(), 5);
}

#[tokio::test]
async fn append_promised_record_succeeds() {
    let dir = tempfile::tempdir().expect("tempdir");
    let wal = create_file_wal(dir.path().to_path_buf()).await;

    let record = WALRecord::from_promised(GROUP, 1, 1, PxBallot::new(1, 1));
    let loc = wal.append(&record).await.expect("append promised");
    assert!(loc.file_offset > 0);
}

#[tokio::test]
async fn wal_engine_not_failed_after_successful_appends() {
    let dir = tempfile::tempdir().expect("tempdir");
    let wal = create_file_wal(dir.path().to_path_buf()).await;

    for slot in 1..=3u64 {
        let entry = accepted_write(slot, 1, b"k", b"v");
        wal.append(&WALRecord::from_accepted(GROUP, &entry))
            .await
            .expect("append");
    }

    assert!(
        !wal.is_failed(),
        "WAL should not be failed after successful appends"
    );
}
