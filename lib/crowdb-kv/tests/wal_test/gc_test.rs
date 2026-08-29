// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! GC worker tests (W14) — `SimDisk` backend.

use bytes::Bytes;
use crowdb_kv::paxos::roles::{PxBallot, PxLogEntry};
use crowdb_kv::wal::gc::{run_gc_pass, run_gc_with_watermark};
use crowdb_kv::wal::record::WALRecord;
use crowdb_kv::wal::replay::replay_group;
use crowdb_kv::wal::wal_engine::WalEngine;
use crowdb_kv::wal::{IoBackend, MemBlockDevice, WalConfig};
use std::path::PathBuf;
use std::sync::Arc;

fn sim_backend() -> Arc<IoBackend> {
    Arc::new(IoBackend::MemBlock(MemBlockDevice::new()))
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn gc_removes_segments_below_watermark() {
    let backend = sim_backend();
    let disks = vec![PathBuf::from("/wal")];
    // Small segments for easy rotation.
    let config = WalConfig {
        wal_disks: disks.clone(),
        wal_segment_size: 200,
        ..Default::default()
    };
    let wal = WalEngine::create(backend.clone(), config, 1).await.unwrap();

    // Write enough records to force multiple sealed segments.
    for slot in 1..=20 {
        let entry = PxLogEntry {
            slot,
            ballot: PxBallot::new(0, 1),
            term: 1,
            payload: Bytes::from("data"),
        };
        let record = WALRecord::from_accepted(1, &entry);
        wal.append(&record).await.unwrap();
    }
    wal.seal_all().await.unwrap();

    let seg_count_before = wal.index().lock().segments().count();
    assert!(seg_count_before >= 2, "need multiple segments for GC test");

    // GC with watermark above all slots.
    let unlinked = run_gc_with_watermark(&wal, 100).await.unwrap();
    assert!(unlinked > 0, "should have GC'd at least one segment");

    let seg_count_after = wal.index().lock().segments().count();
    assert!(seg_count_after < seg_count_before);
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn gc_zero_watermark_is_noop() {
    let backend = sim_backend();
    let disks = vec![PathBuf::from("/wal")];
    let config = WalConfig {
        wal_disks: disks.clone(),
        wal_segment_size: 200,
        ..Default::default()
    };
    let wal = WalEngine::create(backend.clone(), config, 1).await.unwrap();

    for slot in 1..=5 {
        let record = WALRecord::from_promised(1, 1, slot, PxBallot::new(0, 1));
        wal.append(&record).await.unwrap();
    }
    wal.seal_all().await.unwrap();

    let unlinked = run_gc_with_watermark(&wal, 0).await.unwrap();
    assert_eq!(unlinked, 0);
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn replay_after_gc_is_correct() {
    let backend = sim_backend();
    let disks = vec![PathBuf::from("/wal")];
    let config = WalConfig {
        wal_disks: disks.clone(),
        wal_segment_size: 200,
        ..Default::default()
    };
    let wal = WalEngine::create(backend.clone(), config, 1).await.unwrap();

    for slot in 1..=20 {
        let entry = PxLogEntry {
            slot,
            ballot: PxBallot::new(0, 1),
            term: 1,
            payload: Bytes::from("data"),
        };
        let record = WALRecord::from_accepted(1, &entry);
        wal.append(&record).await.unwrap();
    }
    wal.seal_all().await.unwrap();

    // GC slots below 10.
    let _unlinked = run_gc_with_watermark(&wal, 10).await.unwrap();

    // Replay should still work (only surviving segments).
    let result = replay_group(&backend, &disks, 1).await.unwrap();
    // All remaining records should have slot >= 10 (or be from segments
    // that weren't fully below the watermark).
    assert!(!result.records.is_empty());
    assert_eq!(result.current_term, 1);
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn gc_snapshot_slot_covered_prefix_is_removed() {
    let backend = sim_backend();
    let disks = vec![PathBuf::from("/wal")];
    let config = WalConfig {
        wal_disks: disks.clone(),
        wal_segment_size: 200,
        ..Default::default()
    };
    let wal = WalEngine::create(backend.clone(), config, 1).await.unwrap();

    // Accepted records for slots 1..=20.
    for slot in 1..=20 {
        let entry = PxLogEntry {
            slot,
            ballot: PxBallot::new(0, 1),
            term: 1,
            payload: Bytes::from("data"),
        };
        let record = WALRecord::from_accepted(1, &entry);
        wal.append(&record).await.unwrap();
    }

    // Set the snapshot slot in the engine state (no WAL record needed).
    wal.set_snapshot_slot(15);

    wal.seal_all().await.unwrap();

    let seg_count_before = wal.index().lock().segments().count();
    assert!(seg_count_before >= 2, "need multiple segments for GC test");

    let unlinked = run_gc_pass(&wal, u64::MAX).await.unwrap();
    assert!(
        unlinked > 0,
        "snapshot-covered prefix should GC at least one segment"
    );

    let seg_count_after = wal.index().lock().segments().count();
    assert!(seg_count_after < seg_count_before);

    // Replay after GC should still work.
    let result = replay_group(&backend, &disks, 1).await.unwrap();
    assert!(!result.records.is_empty());
    assert_eq!(result.current_term, 1);
}

/// `run_gc_pass`'s `safe_slot` parameter genuinely gates GC: even with the
/// snapshot slot covering everything, a `safe_slot` below it must hold GC
/// back to `min(safe_slot, snapshot_slot)` (a lagging voting member that
/// hasn't applied past `safe_slot` might still need those segments).
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn gc_pass_is_bounded_by_safe_slot_not_just_snapshot_slot() {
    let backend = sim_backend();
    let disks = vec![PathBuf::from("/wal")];
    let config = WalConfig {
        wal_disks: disks.clone(),
        wal_segment_size: 200,
        ..Default::default()
    };
    let wal = WalEngine::create(backend.clone(), config, 1).await.unwrap();

    for slot in 1..=20 {
        let entry = PxLogEntry {
            slot,
            ballot: PxBallot::new(0, 1),
            term: 1,
            payload: Bytes::from("data"),
        };
        wal.append(&WALRecord::from_accepted(1, &entry)).await.unwrap();
    }
    // Snapshot slot covers everything, but a lagging peer has only applied
    // through slot 3 -- `safe_slot` must be the limiting factor.
    wal.set_snapshot_slot(20);
    wal.seal_all().await.unwrap();

    let unlinked_at_low_safe_slot = run_gc_pass(&wal, 3).await.unwrap();
    assert_eq!(
        unlinked_at_low_safe_slot, 0,
        "a low safe_slot must hold GC back even though snapshot_slot covers everything"
    );

    // Once the lagging peer catches up, the same snapshot_slot now GCs.
    let unlinked_at_high_safe_slot = run_gc_pass(&wal, 20).await.unwrap();
    assert!(
        unlinked_at_high_safe_slot > 0,
        "raising safe_slot to match snapshot_slot should unblock GC"
    );
}
