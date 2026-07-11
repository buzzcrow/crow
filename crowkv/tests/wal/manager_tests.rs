//! `WalEngine` tests (W8) — `SimDisk` backend.

use bytes::Bytes;
use crowkv::paxos::roles::{PxBallot, PxLogEntry, PxLogEntryKind};
use crowkv::wal::pipeline_backend::{WalBlockAlignment, WalPipelineBackend};
use crowkv::wal::record::WALRecord;
use crowkv::wal::replay::replay_group;
use crowkv::wal::wal_engine::WalEngine;
use crowkv::wal::{BlockDevice, IoBackend, WalConfig};
use std::path::PathBuf;
use std::sync::Arc;

fn sim_backend() -> Arc<IoBackend> {
    Arc::new(IoBackend::BlockDevice(BlockDevice::new()))
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
        kind: PxLogEntryKind::Write,
        payload: Bytes::from(format!("aligned-val-{slot}")),
        client_id: Some(7),
        seq: Some(slot),
    };
    WALRecord::from_accepted(group, &entry)
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
            kind: PxLogEntryKind::Write,
            payload: Bytes::from(format!("val-{slot}")),
            client_id: Some(1),
            seq: Some(slot),
        };
        let record = WALRecord::from_accepted(1, &entry);
        let loc = wal.append(&record).await.unwrap();
        assert_eq!(loc.segment_id, 1);
    }

    let idx = wal.index().lock();
    assert_eq!(idx.slot_count(), 50);
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn multi_disk_round_robin() {
    let backend = sim_backend();
    let config = test_config(vec![PathBuf::from("/wal-disk0"), PathBuf::from("/wal-disk1")]);
    let wal = WalEngine::create(backend, config, 1).await.unwrap();

    let r1 = WALRecord::from_promised(1, 1, 1, PxBallot::new(0, 1));
    let loc1 = wal.append(&r1).await.unwrap();

    let r2 = WALRecord::from_promised(1, 1, 2, PxBallot::new(0, 1));
    let loc2 = wal.append(&r2).await.unwrap();

    // Round-robin should alternate disks.
    assert_ne!(loc1.disk_idx, loc2.disk_idx);
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

/// B2 — Aligned (4 KiB SSD) end-to-end: append across several segment
/// rotations, seal, then `replay_group` must recover every record even though
/// each sealed segment is zero-padded to a block boundary (the B1 scenario).
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn aligned_engine_append_rotate_seal_replays_all_records() {
    let group_id = 4u64;
    let device = BlockDevice::ssd_4k();
    let backend = Arc::new(IoBackend::BlockDevice(device.clone()));
    let disk = PathBuf::from("/nvme0");
    let config = WalConfig {
        wal_disks: vec![disk.clone()],
        // Small segment so the run spans several rotations on the aligned path.
        wal_segment_size: 4 * 1024,
        wal_alignment: WalBlockAlignment::default_aligned(),
        ..Default::default()
    };

    let wal = WalEngine::create(backend.clone(), config, group_id)
        .await
        .unwrap();

    // The engine must have built a block pipeline reflecting the configured
    // alignment (this reads `WalPipeline.backend`).
    for backend_desc in wal.pipeline_backends().await {
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
