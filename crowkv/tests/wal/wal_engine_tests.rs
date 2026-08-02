// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! `WalEngine` tests (W8) — `SimDisk` backend.

use bytes::Bytes;
use crowkv::paxos::roles::{PxBallot, PxLogEntry};
use crowkv::wal::pipeline_backend::{WalBlockAlignment, WalPipelineBackend};
use crowkv::wal::record::{WALRecord, WalRecordFormat, WAL_MAGIC};
use crowkv::wal::replay::replay_group;
use crowkv::wal::segment::SEG_HEADER_LEN;
use crowkv::wal::wal_engine::WalEngine;
use crowkv::wal::{IoBackend, MemBlockDevice, OpenOptions, WalConfig};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

fn sim_backend() -> Arc<IoBackend> {
    Arc::new(IoBackend::MemBlock(MemBlockDevice::new()))
}

fn test_config(disks: Vec<PathBuf>) -> WalConfig {
    WalConfig {
        wal_disks: disks,
        wal_segment_size: 1024 * 1024, // 1 MiB for tests
        ..Default::default()
    }
}

fn write_entry(group: u64, slot: u64) -> WALRecord {
    let entry = PxLogEntry {
        slot,
        ballot: PxBallot::new(0, 1),
        term: 1,
        payload: Bytes::from(format!("aligned-val-{slot}")),
    };
    WALRecord::from_accepted(group, &entry)
}

#[test]
fn wal_config_defaults_use_flush_names_and_wake_drain_flush() {
    let config = WalConfig::default();
    assert_eq!(config.wal_flush_batch_bytes, 64 * 1024);
    assert_eq!(config.wal_flush_coalesce_us, 0);
    assert_eq!(config.wal_flush_watchdog_ms, 100);
    assert_eq!(config.wal_record_format, WalRecordFormat::Auto);
}

/// W3: wake-drain-flush issues exactly one durable flush for a single record
/// with no batching delay (the default `wal_flush_coalesce_us = 0` policy).
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn single_record_durable_flush_without_interval_wait() {
    let device = MemBlockDevice::new();
    let backend = Arc::new(IoBackend::MemBlock(device.clone()));
    let config = test_config(vec![PathBuf::from("/wal")]);
    let wal = WalEngine::create(backend, config, 1).await.unwrap();

    let record = WALRecord::from_promised(1, 1, 10, PxBallot::new(0, 1));
    wal.append(&record).await.unwrap();

    // Exactly one durable flush covers the single record; no rotation/seal.
    assert_eq!(device.fdatasync_count(), 1);
}

/// T3.1: the watchdog safety-net timer wakes the idle writer periodically so a
/// queued record is drained even if a normal wake were missed. With a short
/// watchdog and real time, sleeping past several cycles fires the timer; the
/// `watchdog_wakeups` counter increments and the writer stays functional.
#[tokio::test(flavor = "current_thread")]
async fn watchdog_wakes_idle_writer_and_stays_functional() {
    let device = MemBlockDevice::new();
    let backend = Arc::new(IoBackend::MemBlock(device.clone()));
    let config = WalConfig {
        wal_disks: vec![PathBuf::from("/wal-wd")],
        wal_segment_size: 1024 * 1024,
        wal_flush_watchdog_ms: 10,
        ..Default::default()
    };
    let wal = WalEngine::create(backend, config, 1).await.unwrap();

    // Idle: sleep past several watchdog cycles. The writer's timeout fires,
    // try_recv drains (empty), and it re-parks each cycle.
    tokio::time::sleep(Duration::from_millis(35)).await;

    let stats = wal.batch_stats();
    assert!(
        stats.watchdog_wakeups >= 1,
        "watchdog should fire while idle, got {}",
        stats.watchdog_wakeups
    );

    // Writer is still alive — a real append flushes normally.
    let record = WALRecord::from_promised(1, 1, 10, PxBallot::new(0, 1));
    wal.append(&record).await.unwrap();
    assert_eq!(device.fdatasync_count(), 1);
}

/// R10: `wal_skip_fsync` skips the durable `fdatasync` call on every write
/// batch while still writing the record to the segment and resolving the
/// append ack (benchmark path-overhead isolation mode).
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn skip_fsync_avoids_durable_flush_but_still_appends() {
    let device = MemBlockDevice::new();
    let backend = Arc::new(IoBackend::MemBlock(device.clone()));
    let config = WalConfig {
        wal_skip_fsync: true,
        ..test_config(vec![PathBuf::from("/wal")])
    };
    let wal = WalEngine::create(backend, config, 1).await.unwrap();

    let record = WALRecord::from_promised(1, 1, 10, PxBallot::new(0, 1));
    wal.append(&record).await.unwrap();

    // The append ack still resolves and the batch is counted as flushed...
    let stats = wal.batch_stats();
    assert_eq!(stats.flush_count, 1);
    assert_eq!(stats.records_flushed, 1);
    // ...but no durable fdatasync was issued.
    assert_eq!(device.fdatasync_count(), 0);
}

/// W3: a burst of concurrent appends coalesces into fewer durable flushes than
/// records when a coalescing budget is configured.
#[tokio::test(flavor = "current_thread")]
async fn burst_appends_coalesce_into_fewer_flushes() {
    let device = MemBlockDevice::new();
    let backend = Arc::new(IoBackend::MemBlock(device.clone()));
    let config = WalConfig {
        wal_disks: vec![PathBuf::from("/wal-burst")],
        wal_segment_size: 16 * 1024 * 1024, // large: no rotation/seal flushes
        wal_flush_batch_bytes: 1024 * 1024, // large: batch bounded by coalesce window
        wal_flush_coalesce_us: 5_000,       // 5ms coalescing window
        ..Default::default()
    };
    let wal = WalEngine::create(backend, config, 1).await.unwrap();

    let records = 64u64;
    let mut tasks = Vec::new();
    for slot in 1..=records {
        let wal = wal.clone();
        tasks.push(tokio::spawn(async move {
            wal.append(&write_entry(1, slot)).await.unwrap();
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }

    let flushes = device.fdatasync_count();
    assert!(
        flushes < records,
        "expected coalescing into fewer than {records} flushes, got {flushes}"
    );
    assert_eq!(wal.index().lock().slot_count(), usize::try_from(records).unwrap());
}

/// W4: the file/byte-addressable backend runs exactly one durable flush per
/// drained batch. Driving the appends with `tokio::join!` polls every append
/// future in a single sweep so all requests are enqueued before the worker
/// runs; wake-drain then collapses them into one `fdatasync`.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn file_backend_durable_flush_once_per_drained_batch() {
    let device = MemBlockDevice::new();
    let backend = Arc::new(IoBackend::MemBlock(device.clone()));
    let config = WalConfig {
        wal_disks: vec![PathBuf::from("/wal-batch")],
        wal_segment_size: 16 * 1024 * 1024, // large: no rotation/seal flushes
        wal_flush_batch_bytes: 1024 * 1024, // large: whole batch fits
        ..Default::default()
    };
    let wal = WalEngine::create(backend, config, 1).await.unwrap();

    let records: Vec<WALRecord> = (1..=8).map(|slot| write_entry(1, slot)).collect();
    let r = tokio::join!(
        wal.append(&records[0]),
        wal.append(&records[1]),
        wal.append(&records[2]),
        wal.append(&records[3]),
        wal.append(&records[4]),
        wal.append(&records[5]),
        wal.append(&records[6]),
        wal.append(&records[7]),
    );
    for loc in [r.0, r.1, r.2, r.3, r.4, r.5, r.6, r.7] {
        loc.unwrap();
    }

    assert_eq!(
        device.fdatasync_count(),
        1,
        "a single drained batch must incur exactly one durable flush"
    );
    assert_eq!(wal.index().lock().slot_count(), 8);
}

/// Vectored batch write: a drained batch larger than `MAX_IOV` (1024) slices
/// is split into multiple vectored writes while still using a single flush and
/// recovering all records.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn large_binary_batch_splits_across_iov_max_and_replays() {
    let device = MemBlockDevice::new();
    let backend = Arc::new(IoBackend::MemBlock(device.clone()));
    let config = WalConfig {
        wal_disks: vec![PathBuf::from("/wal-vec-chunk")],
        wal_segment_size: 64 * 1024 * 1024,
        wal_flush_batch_bytes: 64 * 1024 * 1024,
        wal_record_format: WalRecordFormat::Binary,
        ..Default::default()
    };
    let wal = WalEngine::create(backend.clone(), config, 1).await.unwrap();

    // 300 binary records -> 1200 IoSlices -> 2 writev calls (MAX_IOV = 1024).
    let records: Vec<WALRecord> = (1..=300).map(|slot| write_entry(1, slot)).collect();
    let mut tasks = Vec::new();
    for rec in &records {
        let wal = wal.clone();
        let rec = rec.clone();
        tasks.push(tokio::spawn(async move { wal.append(&rec).await.unwrap() }));
    }
    for t in tasks {
        t.await.unwrap();
    }

    // Total write_count = 1 (segment header) + 2 (batch writev) = 3.
    assert_eq!(
        device.write_count(),
        3,
        "batch must be split into two vectored writes (plus 1 header write)"
    );
    assert_eq!(
        device.fdatasync_count(),
        1,
        "single fdatasync for the drained batch"
    );
    assert_eq!(wal.index().lock().slot_count(), 300);

    wal.seal_all().await.unwrap();
    let replay = replay_group(&backend, &[PathBuf::from("/wal-vec-chunk")], 1)
        .await
        .unwrap();
    assert_eq!(replay.records, records);
}

/// W3: a durable-flush failure in the worker fails the in-flight append and
/// marks the WAL failed; subsequent appends fail fast.
#[tokio::test(flavor = "current_thread")]
async fn flush_error_fails_append_and_marks_wal_failed() {
    let device = MemBlockDevice::new();
    let backend = Arc::new(IoBackend::MemBlock(device.clone()));
    let config = test_config(vec![PathBuf::from("/wal-syncerr")]);
    let wal = WalEngine::create(backend, config, 1).await.unwrap();

    // Write succeeds, but the worker's durable flush fails.
    device.controller().inject_sync_error(true);
    let r = wal
        .append(&WALRecord::from_promised(1, 1, 1, PxBallot::new(0, 1)))
        .await;
    assert!(r.is_err(), "append must fail when durable flush fails");
    assert!(wal.is_failed(), "WAL must be marked failed after a flush error");

    // Even after clearing the injection, the WAL stays failed and rejects writes.
    device.controller().inject_sync_error(false);
    let r2 = wal
        .append(&WALRecord::from_promised(1, 1, 2, PxBallot::new(0, 1)))
        .await;
    assert!(r2.is_err(), "append after failed WAL must return an error");
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn append_single_record() {
    let backend = sim_backend();
    let config = test_config(vec![PathBuf::from("/wal")]);
    let wal = WalEngine::create(backend, config, 1).await.unwrap();

    let record = WALRecord::from_promised(1, 1, 10, PxBallot::new(0, 1));
    let loc = wal.append(&record).await.unwrap();
    assert_eq!(loc.disk_idx, 0);
    assert_eq!(loc.segment_id, 1);

    // Check that the slot is in the index.
    let idx = wal.index().lock();
    assert!(idx.locate(10).is_some());
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn append_multiple_records_different_slots() {
    let backend = sim_backend();
    let config = test_config(vec![PathBuf::from("/wal")]);
    let wal = WalEngine::create(backend, config, 1).await.unwrap();

    for slot in 1..=50 {
        let entry = PxLogEntry {
            slot,
            ballot: PxBallot::new(0, 1),
            term: 1,
            payload: Bytes::from(format!("val-{slot}")),
        };
        let record = WALRecord::from_accepted(1, &entry);
        let loc = wal.append(&record).await.unwrap();
        assert_eq!(loc.segment_id, 1);
    }

    let idx = wal.index().lock();
    assert_eq!(idx.slot_count(), 50);
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn auto_format_unaligned_engine_replays_binary_records() {
    let group_id = 3u64;
    let backend = sim_backend();
    let disk = PathBuf::from("/wal-bin-auto");
    let config = test_config(vec![disk.clone()]);
    let wal = WalEngine::create(backend.clone(), config, group_id)
        .await
        .unwrap();

    let record = WALRecord::from_promised(group_id, 7, 11, PxBallot::new(2, 9));
    wal.append(&record).await.unwrap();
    wal.seal_all().await.unwrap();

    // Auto now resolves to Binary on all backends. Verify the segment
    // contains binary framing (magic bytes), not text-line encoding.
    let segment_path = disk.join(format!("group{group_id}")).join("seg-0000001.ck");

    let replay = replay_group(&backend, &[disk], group_id).await.unwrap();
    assert_eq!(replay.records, vec![record]);
    let mut file = backend
        .open(&segment_path, OpenOptions::read_only())
        .await
        .unwrap();
    // Binary record: [frame_len:4][magic:4]...
    let mut magic_buf = [0u8; 4];
    file.read_exact_at(&mut magic_buf, SEG_HEADER_LEN as u64 + 4)
        .await
        .unwrap();
    assert_eq!(u32::from_le_bytes(magic_buf), WAL_MAGIC);
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn same_slot_records_use_same_affinity_disk() {
    let backend = sim_backend();
    let config = test_config(vec![
        PathBuf::from("/wal-affinity0"),
        PathBuf::from("/wal-affinity1"),
        PathBuf::from("/wal-affinity2"),
        PathBuf::from("/wal-affinity3"),
    ]);
    let wal = WalEngine::create(backend, config, 1).await.unwrap();

    let r1 = WALRecord::from_promised(1, 1, 42, PxBallot::new(0, 1));
    let r2 = WALRecord::from_promised(1, 2, 42, PxBallot::new(1, 1));
    let loc1 = wal.append(&r1).await.unwrap();
    let loc2 = wal.append(&r2).await.unwrap();

    assert_eq!(loc1.disk_idx, loc2.disk_idx);
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn metadata_records_use_group_metadata_lane() {
    let backend = sim_backend();
    let config = test_config(vec![
        PathBuf::from("/wal-meta0"),
        PathBuf::from("/wal-meta1"),
        PathBuf::from("/wal-meta2"),
        PathBuf::from("/wal-meta3"),
    ]);
    let wal = WalEngine::create(backend, config, 9).await.unwrap();

    let r1 = WALRecord::from_vote_granted(9, 3, 100);
    let r2 = WALRecord::from_vote_granted(9, 4, 200);
    let loc1 = wal.append(&r1).await.unwrap();
    let loc2 = wal.append(&r2).await.unwrap();

    assert_eq!(loc1.disk_idx, loc2.disk_idx);
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn adjacent_slots_distribute_across_affinity_disks_and_replay() {
    let backend = sim_backend();
    let group_id = 11u64;
    let disks = vec![
        PathBuf::from("/wal-dist0"),
        PathBuf::from("/wal-dist1"),
        PathBuf::from("/wal-dist2"),
        PathBuf::from("/wal-dist3"),
    ];
    let config = test_config(disks.clone());
    let wal = WalEngine::create(backend.clone(), config, group_id)
        .await
        .unwrap();

    let mut used_disks = std::collections::BTreeSet::new();
    for slot in 1..=32 {
        let loc = wal.append(&write_entry(group_id, slot)).await.unwrap();
        used_disks.insert(loc.disk_idx);
    }
    wal.seal_all().await.unwrap();

    assert!(used_disks.len() > 1);
    let replay = replay_group(&backend, &disks, group_id).await.unwrap();
    let mut recovered: Vec<u64> = replay.records.iter().map(|r| r.slot).collect();
    recovered.sort_unstable();
    let expected: Vec<u64> = (1..=32).collect();
    assert_eq!(recovered, expected);
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn segment_rotation_on_size() {
    let backend = sim_backend();
    // Very small segment to trigger rotation.
    let mut config = test_config(vec![PathBuf::from("/wal")]);
    config.wal_segment_size = 200; // tiny
    let wal = WalEngine::create(backend, config, 1).await.unwrap();

    // Write enough records to force rotation.
    for slot in 1..=10 {
        let record = WALRecord::from_promised(1, 1, slot, PxBallot::new(0, 1));
        wal.append(&record).await.unwrap();
    }

    // Should have rotated at least once.
    let idx = wal.index().lock();
    assert!(
        idx.segments().count() >= 1,
        "expected at least one sealed segment"
    );
    assert_eq!(idx.slot_count(), 10);
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn seal_all_stops_writes() {
    let backend = sim_backend();
    let config = test_config(vec![PathBuf::from("/wal")]);
    let wal = WalEngine::create(backend, config, 1).await.unwrap();

    let r = WALRecord::from_promised(1, 1, 1, PxBallot::new(0, 1));
    wal.append(&r).await.unwrap();
    wal.seal_all().await.unwrap();

    // After seal, a new append should still work (it opens a new segment).
    let r2 = WALRecord::from_promised(1, 1, 2, PxBallot::new(0, 1));
    wal.append(&r2).await.unwrap();
}

/// Concurrent append from multiple tasks: all acks resolved, all records
/// present in replay. Exercises the lock-free enqueue + writer batching path
/// under real concurrency.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_appends_all_acks_resolved_and_replay_complete() {
    let group_id = 7u64;
    let device = MemBlockDevice::new();
    let backend = Arc::new(IoBackend::MemBlock(device.clone()));
    let disk = PathBuf::from("/wal-concurrent");
    let config = WalConfig {
        wal_disks: vec![disk.clone()],
        wal_segment_size: 4 * 1024 * 1024,
        wal_flush_batch_bytes: 256 * 1024,
        wal_flush_coalesce_us: 1_000,
        ..Default::default()
    };
    let wal = WalEngine::create(backend.clone(), config, group_id)
        .await
        .unwrap();

    let count = 200u64;
    let mut tasks = Vec::new();
    for slot in 1..=count {
        let wal = wal.clone();
        tasks.push(tokio::spawn(async move {
            wal.append(&write_entry(group_id, slot)).await.unwrap()
        }));
    }
    let mut locs = Vec::new();
    for t in tasks {
        locs.push(t.await.unwrap());
    }

    // Every ack resolved successfully.
    assert_eq!(locs.len(), usize::try_from(count).unwrap());
    for loc in &locs {
        assert_eq!(loc.disk_idx, 0);
    }

    // Index has all slots.
    assert_eq!(wal.index().lock().slot_count(), usize::try_from(count).unwrap());

    // Seal and replay — every record must be recovered.
    wal.seal_all().await.unwrap();
    let replay = replay_group(&backend, &[disk], group_id).await.unwrap();
    let mut recovered: Vec<u64> = replay.records.iter().map(|r| r.slot).collect();
    recovered.sort_unstable();
    let expected: Vec<u64> = (1..=count).collect();
    assert_eq!(recovered, expected, "all concurrent records recovered");
}

/// Writer failure: when `fdatasync` fails, pending append acks must fail and
/// the WAL must be marked `is_failed()`. Subsequent appends must be rejected.
#[tokio::test(flavor = "current_thread")]
async fn writer_failure_fails_acks_and_marks_wal_failed() {
    let device = MemBlockDevice::new();
    let backend = Arc::new(IoBackend::MemBlock(device.clone()));
    let config = WalConfig {
        wal_disks: vec![PathBuf::from("/wal-writer-fail")],
        wal_segment_size: 16 * 1024 * 1024,
        wal_flush_batch_bytes: 1024 * 1024,
        wal_flush_coalesce_us: 5_000,
        ..Default::default()
    };
    let wal = WalEngine::create(backend, config, 1).await.unwrap();

    // Inject sync error so the writer's fdatasync fails.
    device.controller().inject_sync_error(true);

    // Launch several concurrent appends — they should all fail.
    let records: Vec<WALRecord> = (1..=8).map(|slot| write_entry(1, slot)).collect();
    let results = tokio::join!(
        wal.append(&records[0]),
        wal.append(&records[1]),
        wal.append(&records[2]),
        wal.append(&records[3]),
        wal.append(&records[4]),
        wal.append(&records[5]),
        wal.append(&records[6]),
        wal.append(&records[7]),
    );
    let all_results = [
        results.0, results.1, results.2, results.3, results.4, results.5, results.6, results.7,
    ];
    for r in &all_results {
        assert!(r.is_err(), "append must fail when writer fdatasync fails");
    }
    assert!(wal.is_failed(), "WAL must be marked failed after writer error");

    // Subsequent appends are rejected fast (failed flag check).
    device.controller().inject_sync_error(false);
    let r = wal
        .append(&WALRecord::from_promised(1, 1, 99, PxBallot::new(0, 1)))
        .await;
    assert!(r.is_err(), "append after failed WAL must return an error");
}

/// B2 — Aligned (4 KiB SSD) end-to-end: append across several segment
/// rotations, seal, then `replay_group` must recover every record even though
/// each sealed segment is zero-padded to a block boundary (the B1 scenario).
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn aligned_engine_append_rotate_seal_replays_all_records() {
    let group_id = 4u64;
    let device = MemBlockDevice::with_alignment(WalBlockAlignment::default_aligned());
    let backend = Arc::new(IoBackend::MemBlock(device.clone()));
    let disk = PathBuf::from("/nvme0");
    let config = WalConfig {
        wal_disks: vec![disk.clone()],
        // Small segment so the run spans several rotations on the aligned path.
        wal_segment_size: 4 * 1024,
        wal_aligned: true,
        wal_io_unit_bytes: 4096,
        ..Default::default()
    };

    let wal = WalEngine::create(backend.clone(), config, group_id)
        .await
        .unwrap();

    // The engine must have built a block pipeline reflecting the configured
    // alignment (this reads `WalPipeline.backend`).
    for backend_desc in wal.pipeline_backends() {
        assert!(matches!(backend_desc, WalPipelineBackend::Block(_)));
        assert_eq!(backend_desc.alignment(), WalBlockAlignment::default_aligned());
    }

    let count = 60u64;
    for slot in 1..=count {
        wal.append(&write_entry(group_id, slot)).await.unwrap();
    }
    // Seal all active segments so the tail segment is footered + padded too.
    wal.seal_all().await.unwrap();

    // Aligned device must show write amplification: physical writes are widened
    // to whole 4 KiB blocks and sub-block appends trigger read-modify-write.
    assert!(
        device.rmw_count() > 0,
        "aligned path should record read-modify-write events"
    );
    assert!(
        device.physical_bytes_written() > device.logical_bytes_written(),
        "aligned path should amplify physical bytes beyond logical"
    );

    let replay = replay_group(&backend, &[disk], group_id).await.unwrap();
    let recovered: Vec<u64> = replay.records.iter().map(|r| r.slot).collect();
    let expected: Vec<u64> = (1..=count).collect();
    assert_eq!(
        recovered, expected,
        "every appended slot recovered in order past block padding"
    );
    assert_eq!(replay.current_term, 1);
}
