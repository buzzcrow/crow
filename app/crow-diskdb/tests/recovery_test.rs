// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 0.0.

//! R73 recovery integration tests.
//!
//! Verifies that:
//! - `ZoneLoader::rebuild_zone_bitmap_full_scan` (strategy 1)
//!   correctly reconstructs zone bitmaps after a simulated restart.
//! - `ZoneLoader::load_disk_group` (strategy 2 journal replay
//!   with strategy 1 fallback) correctly reconstructs after restart.
//! - `CompactionEngine::compact_zone` merges free records into a new
//!   snapshot and deletes the free records.
//! - `Zone::to_zone_value`/`from_zone_value` round-trip correctly.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::cluster::{wait_for_disks_ready, KvCluster};
use crow_diskdb::ddb_config::{CompactionConfig, KeepAliveConfig};
use crow_diskdb::liveness::keepalive::KeepAlive;
use crow_diskdb::model::alloc;
use crow_diskdb::model::disk_group_container::DdbDiskGroupContainer;
use crow_diskdb::model::zone::DdbZone;
use crow_diskdb::recovery::compaction::CompactionEngine;
use crow_diskdb::recovery::ZoneLoader;
use crow_kv_client::{GetOutcome, HardwareClient};
use crow_protocol::common::{ChunkId, DiskId, HwStatus, NodeValue, RackValue};
use crow_protocol::diskdb::rpc::{DiskGroupValue, DiskType, DiskValue};
use crow_protocol::key::BinaryKey;
use crow_protocol::{DiskGroupId, ZoneValueExt};

const RACK_ID: u64 = 1;
const NODE_ID: u64 = 10;
const DG_ID: DiskGroupId = 100;
const STORE_ID: u64 = 0;
const DATA_GROUP_ID: u64 = 1;
const INSTANCE_ID: u64 = 999;

const ZONE_SIZE_UNITS: u64 = 128;
const UNIT_SIZE_BYTES: u32 = 1024 * 1024;
const CAPACITY_UNITS: u64 = ZONE_SIZE_UNITS * 4;
const ZONE_COUNT: u32 = 4;

fn make_disk_id(high: u64, low: u64) -> DiskId {
    DiskId { high, low }
}

fn make_chunk_id(high: u64, mid: u64, low: u64) -> ChunkId {
    ChunkId { high, mid, low }
}

async fn seed_hardware(hw: &HardwareClient) {
    hw.add_rack(
        RACK_ID,
        &RackValue {
            status: HwStatus::Up as i32,
            node_ids: vec![NODE_ID],
        },
    )
    .await
    .expect("add rack");
    hw.add_node(
        RACK_ID,
        NODE_ID,
        &NodeValue {
            status: HwStatus::Up as i32,
            last_used_dg_id: 0,
            disk_group_ids: vec![DG_ID],
            status_changed_at_ms: 0,
            temp_failure_since_ms: None,
        },
    )
    .await
    .expect("add node");
    let disk_ids = vec![make_disk_id(0, 1), make_disk_id(0, 2), make_disk_id(0, 3)];
    hw.add_disk_group(
        RACK_ID,
        NODE_ID,
        DG_ID,
        &DiskGroupValue {
            status: HwStatus::Up as i32,
            disk_ids: disk_ids.clone(),
        },
    )
    .await
    .expect("add disk-group");
    for did in &disk_ids {
        hw.add_disk(
            RACK_ID,
            NODE_ID,
            DG_ID,
            did,
            &DiskValue {
                disk_type: DiskType::BlockSsd as i32,
                capacity_units: CAPACITY_UNITS,
                zone_size_units: ZONE_SIZE_UNITS,
                unit_size_bytes: UNIT_SIZE_BYTES,
                zone_count: ZONE_COUNT,
                status: HwStatus::Up as i32,
            },
        )
        .await
        .expect("add disk");
    }
    let lease_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
        + 3_600_000;
    hw.set_owner(RACK_ID, NODE_ID, DG_ID, INSTANCE_ID, lease_ms)
        .await
        .expect("set owner");
    hw.set_bind(RACK_ID, NODE_ID, DG_ID, STORE_ID, DATA_GROUP_ID)
        .await
        .expect("set bind");
}

/// Find the crow-kv-server binary (mirrors the cluster module's logic).
fn crow_kv_server_bin() -> Option<std::path::PathBuf> {
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            let mut p = dir.to_path_buf();
            for _ in 0..3 {
                let candidate = p.join("crow-kv-server");
                if candidate.exists() {
                    return Some(candidate);
                }
                if !p.pop() {
                    break;
                }
            }
        }
    }
    None
}

/// Strategy 1 (full scan) recovery: allocate + free blocks, simulate a
/// restart by dropping in-memory state, then recover via
/// `rebuild_zone_bitmap_full_scan` and verify the bitmap matches.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn recovery_strategy1_full_scan_rebuilds_bitmap() {
    if std::env::var("CROW_KV_SERVER_BIN").is_err() && crow_kv_server_bin().is_none() {
        eprintln!("skipping: CROW_KV_SERVER_BIN not set and binary not found");
        return;
    }

    // 1. Start cluster + seed hardware.
    let cluster = KvCluster::start().await;
    let hw = cluster.make_hardware_client();
    seed_hardware(&hw).await;

    // 2. First tick — populates in-memory state + writes baseline
    //    ZoneValues.
    let container = Arc::new(DdbDiskGroupContainer::new(INSTANCE_ID));
    let svc = cluster.make_service_registry_client();
    let hw2 = cluster.make_hardware_client();
    let dg_kv = cluster.make_ddb_kv_client();
    let keepalive_cfg = KeepAliveConfig {
        interval: Duration::from_secs(10),
        miss_threshold: 3,
        zone_rotate_count: 4,
        cas_retry_limit: 100,
        temp_failure_timeout_secs: 900,
    };
    let keepalive = KeepAlive::new(hw2, svc, Arc::clone(&container), keepalive_cfg).with_ddb_kv_client(dg_kv);
    let outcome = keepalive.tick().await;
    assert_eq!(outcome.groups_added, 1);
    assert_eq!(outcome.disks_added, 3);

    // Wait for background zone load (Init → Up).
    wait_for_disks_ready(&container, DG_ID, 3, ZONE_COUNT).await;

    let dg = container.get_disk_group(DG_ID).expect("disk-group exists");
    let bind = *dg.bind.read().unwrap();

    // 3. Allocate 3 blocks (anti-affinity spreads across 3 disks),
    //    free 1 of them. After this, 2 blocks remain busy.
    let owner_chunk = make_chunk_id(0, 0, 42);
    let alloc_kv = cluster.make_ddb_kv_client();
    let metrics = crow_diskdb::metrics::DiskdbMetrics::disabled();
    let segments = alloc::allocate_blocks(
        &dg,
        1,
        3,
        &[],
        &owner_chunk,
        UNIT_SIZE_BYTES,
        &alloc_kv,
        100,
        4,
        &metrics,
    )
    .await
    .expect("allocate 3");
    assert_eq!(segments.len(), 3);

    // Free the first 1.
    let free_kv = cluster.make_ddb_kv_client();
    alloc::free_blocks(&dg, &segments[0..1], &free_kv, false)
        .await
        .expect("free 1");

    // Record the expected busy segments (the 2 that remain).
    let remaining_segments: Vec<_> = segments[1..].to_vec();

    // 4. Simulate a restart: drop the in-memory container. A fresh
    //    tick will skip baseline ZoneValue writes (snapshots
    //    exist) and create empty zones.
    drop(dg);
    drop(container);
    let container2 = Arc::new(DdbDiskGroupContainer::new(INSTANCE_ID));
    let svc2 = cluster.make_service_registry_client();
    let hw3 = cluster.make_hardware_client();
    let dg_kv2 = cluster.make_ddb_kv_client();
    let keepalive_cfg2 = KeepAliveConfig {
        interval: Duration::from_secs(10),
        miss_threshold: 3,
        zone_rotate_count: 4,
        cas_retry_limit: 100,
        temp_failure_timeout_secs: 900,
    };
    let keepalive2 =
        KeepAlive::new(hw3, svc2, Arc::clone(&container2), keepalive_cfg2).with_ddb_kv_client(dg_kv2);
    let outcome2 = keepalive2.tick().await;
    assert_eq!(outcome2.groups_added, 1);
    assert_eq!(outcome2.disks_added, 3);

    let dg2 = container2
        .get_disk_group(DG_ID)
        .expect("disk-group exists after restart");
    let bind2 = *dg2.bind.read().unwrap();
    assert_eq!(bind, bind2);

    // 5. Run strategy 1 recovery on each zone of each disk.
    let recovery_kv = Arc::new(cluster.make_ddb_kv_client());
    let recovery = ZoneLoader::new(Arc::clone(&recovery_kv), 4);

    let disks = dg2.disks.read().unwrap().clone();
    for disk in &disks {
        let zone_size_units = disk.disk_value.read().unwrap().zone_size_units;
        let zone_count = disk.disk_value.read().unwrap().zone_count;
        for zi in 0..zone_count {
            #[allow(clippy::cast_possible_truncation)]
            let unit_capacity = if zi == zone_count - 1 {
                let remaining = CAPACITY_UNITS - (u64::from(zi) * zone_size_units);
                let rounded = (remaining / 64) * 64;
                rounded as u32
            } else {
                zone_size_units as u32
            };
            let (recovered_zone, _stats) = recovery
                .rebuild_zone_bitmap_full_scan(bind2, disk.disk_id, zi, DG_ID, unit_capacity)
                .await
                .expect("recovery should succeed");
            // Replace the empty zone with the recovered zone.
            let mut zones = disk.zones.write().unwrap();
            zones[zi as usize] = Arc::new(recovered_zone);
        }
        disk.rebuild_active_zones(4);
    }
    dg2.rebuild_allocating_disks();

    // 6. Verify: each remaining busy segment's bit is set, and the
    //    freed segments' bits are clear.
    for seg in &remaining_segments {
        let disk = dg2
            .disks
            .read()
            .unwrap()
            .iter()
            .find(|d| d.disk_id == seg.disk_id.unwrap_or_default())
            .cloned()
            .expect("disk exists");
        let zones = disk.zones.read().unwrap();
        let zone = &zones[seg.zone_index as usize];
        #[allow(clippy::cast_possible_truncation)]
        let bit = seg.unit_offset as u32;
        assert!(
            zone.usage_bits.is_set(bit),
            "bit {bit} should be set after recovery (busy segment)"
        );
    }
    for seg in &segments[0..1] {
        let disk = dg2
            .disks
            .read()
            .unwrap()
            .iter()
            .find(|d| d.disk_id == seg.disk_id.unwrap_or_default())
            .cloned()
            .expect("disk exists");
        let zones = disk.zones.read().unwrap();
        let zone = &zones[seg.zone_index as usize];
        #[allow(clippy::cast_possible_truncation)]
        let bit = seg.unit_offset as u32;
        assert!(
            !zone.usage_bits.is_set(bit),
            "bit {bit} should be clear after recovery (freed segment)"
        );
    }

    // 7. Verify the recovered used_count = 2 (2 remaining busy blocks
    //    of 1 unit each).
    let total_used: u64 = dg2
        .disks
        .read()
        .unwrap()
        .iter()
        .map(|d| {
            d.zones
                .read()
                .unwrap()
                .iter()
                .map(|z| u64::from(z.used_count.load(std::sync::atomic::Ordering::Acquire)))
                .sum::<u64>()
        })
        .sum();
    assert_eq!(total_used, 2, "total used units after recovery should be 2");

    eprintln!("recovery_strategy1_full_scan_rebuilds_bitmap: ALL CHECKS PASSED");
}

/// Unit test: `Zone::to_zone_value` / `from_zone_value` round-trip
/// preserves the bitmap, `used_count`, and `snapshot_slot`, and CRC
/// verification succeeds.
#[test]
fn zone_to_from_zone_value_roundtrip() {
    let disk_id = make_disk_id(0, 1);
    let zone = DdbZone::new(disk_id, 0, 100, 128);
    // Set bits 0..3 (4 units).
    let _ = zone.usage_bits.range_set(0, 4);
    zone.used_count.store(4, std::sync::atomic::Ordering::Release);
    zone.snapshot_slot.store(42, std::sync::atomic::Ordering::Release);
    zone.compact_ts.store(123, std::sync::atomic::Ordering::Release);

    let zv = zone.to_zone_value();
    assert!(zv.verify_checksum(), "CRC should be valid after to_zone_value");
    assert_eq!(zv.snapshot_slot, 42);
    assert_eq!(zv.compact_ts, 123);

    let restored = DdbZone::from_zone_value(disk_id, 0, 100, 128, &zv)
        .expect("from_zone_value should succeed with valid CRC");
    assert_eq!(restored.used_count.load(std::sync::atomic::Ordering::Acquire), 4);
    assert_eq!(
        restored.snapshot_slot.load(std::sync::atomic::Ordering::Acquire),
        42
    );
    assert_eq!(
        restored.compact_ts.load(std::sync::atomic::Ordering::Acquire),
        123
    );
    assert!(restored
        .compacted_ready
        .load(std::sync::atomic::Ordering::Acquire));
    assert!(restored.usage_bits.is_set(0));
    assert!(restored.usage_bits.is_set(3));
    assert!(!restored.usage_bits.is_set(4));
}

/// Unit test: `DdbZone::from_zone_value` returns `None` on CRC mismatch
/// (corrupted snapshot).
#[test]
fn zone_from_zone_value_rejects_bad_crc() {
    let disk_id = make_disk_id(0, 1);
    let zone = DdbZone::new(disk_id, 0, 100, 128);
    let _ = zone.usage_bits.range_set(0, 4);
    let mut zv = zone.to_zone_value();
    // Corrupt the bitmap (CRC no longer matches).
    zv.usage_bitmap[0] ^= 0xFF;
    assert!(DdbZone::from_zone_value(disk_id, 0, 100, 128, &zv).is_none());
}

/// Strategy 2 (journal replay) recovery: allocate + free blocks, then
/// load via `ZoneLoader::load_disk_group` (which tries
/// strategy 2 first, strategy 1 fallback). Verify the bitmap matches.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn recovery_strategy2_journal_replay() {
    if std::env::var("CROW_KV_SERVER_BIN").is_err() && crow_kv_server_bin().is_none() {
        eprintln!("skipping: CROW_KV_SERVER_BIN not set and binary not found");
        return;
    }

    let cluster = KvCluster::start().await;
    let hw = cluster.make_hardware_client();
    seed_hardware(&hw).await;

    // 1. First tick — populates state + writes baseline ZoneValues.
    let container = Arc::new(DdbDiskGroupContainer::new(INSTANCE_ID));
    let svc = cluster.make_service_registry_client();
    let hw2 = cluster.make_hardware_client();
    let dg_kv = cluster.make_ddb_kv_client();
    let keepalive_cfg = KeepAliveConfig {
        interval: Duration::from_secs(10),
        miss_threshold: 3,
        zone_rotate_count: 4,
        cas_retry_limit: 100,
        temp_failure_timeout_secs: 900,
    };
    let keepalive = KeepAlive::new(hw2, svc, Arc::clone(&container), keepalive_cfg).with_ddb_kv_client(dg_kv);
    let outcome = keepalive.tick().await;
    assert_eq!(outcome.groups_added, 1);
    assert_eq!(outcome.disks_added, 3);

    // Wait for background zone load (Init → Up).
    wait_for_disks_ready(&container, DG_ID, 3, ZONE_COUNT).await;

    let dg = container.get_disk_group(DG_ID).expect("disk-group exists");
    let bind = *dg.bind.read().unwrap();

    // 2. Allocate 3 blocks, free 1. 2 remain busy.
    let owner_chunk = make_chunk_id(0, 0, 42);
    let alloc_kv = cluster.make_ddb_kv_client();
    let metrics = crow_diskdb::metrics::DiskdbMetrics::disabled();
    let segments = alloc::allocate_blocks(
        &dg,
        1,
        3,
        &[],
        &owner_chunk,
        UNIT_SIZE_BYTES,
        &alloc_kv,
        100,
        4,
        &metrics,
    )
    .await
    .expect("allocate 3");
    let free_kv = cluster.make_ddb_kv_client();
    alloc::free_blocks(&dg, &segments[0..1], &free_kv, false)
        .await
        .expect("free 1");
    let remaining_segments: Vec<_> = segments[1..].to_vec();

    // 3. Simulate restart: drop container, fresh tick.
    drop(dg);
    drop(container);
    let container2 = Arc::new(DdbDiskGroupContainer::new(INSTANCE_ID));
    let svc2 = cluster.make_service_registry_client();
    let hw3 = cluster.make_hardware_client();
    let dg_kv2 = cluster.make_ddb_kv_client();
    let keepalive2 = KeepAlive::new(
        hw3,
        svc2,
        Arc::clone(&container2),
        KeepAliveConfig {
            interval: Duration::from_secs(10),
            miss_threshold: 3,
            zone_rotate_count: 4,
            cas_retry_limit: 100,
            temp_failure_timeout_secs: 900,
        },
    )
    .with_ddb_kv_client(dg_kv2);
    let outcome2 = keepalive2.tick().await;
    assert_eq!(outcome2.groups_added, 1);

    let dg2 = container2.get_disk_group(DG_ID).expect("dg exists after restart");
    let bind2 = *dg2.bind.read().unwrap();
    assert_eq!(bind, bind2);

    // 4. Load via load_disk_group (strategy 2 with fallback).
    let disks: Vec<(DiskId, DiskValue)> = {
        let disks_guard = dg2.disks.read().unwrap();
        disks_guard
            .iter()
            .map(|d| (d.disk_id, *d.disk_value.read().unwrap()))
            .collect()
    };
    let recovery_kv = Arc::new(cluster.make_ddb_kv_client());
    let recovery = ZoneLoader::new(Arc::clone(&recovery_kv), 4);
    let recovered_dg = recovery
        .load_disk_group(DG_ID, NODE_ID, RACK_ID, bind2, &disks, 4)
        .await;

    // 5. Verify busy segments' bits are set. With Option B (persist-
    // only recovery), freed segments' bits are ALSO set (conservative
    // over-estimate — compaction is the sole bit-clearer).
    for seg in &remaining_segments {
        let disk = recovered_dg
            .disks
            .read()
            .unwrap()
            .iter()
            .find(|d| d.disk_id == seg.disk_id.unwrap_or_default())
            .cloned()
            .expect("disk exists");
        let zones = disk.zones.read().unwrap();
        let zone = &zones[seg.zone_index as usize];
        #[allow(clippy::cast_possible_truncation)]
        let bit = seg.unit_offset as u32;
        assert!(
            zone.usage_bits.is_set(bit),
            "bit {bit} should be set after strategy 2 recovery (busy segment)"
        );
    }
    for seg in &segments[0..1] {
        let disk = recovered_dg
            .disks
            .read()
            .unwrap()
            .iter()
            .find(|d| d.disk_id == seg.disk_id.unwrap_or_default())
            .cloned()
            .expect("disk exists");
        let zones = disk.zones.read().unwrap();
        let zone = &zones[seg.zone_index as usize];
        #[allow(clippy::cast_possible_truncation)]
        let bit = seg.unit_offset as u32;
        assert!(
            zone.usage_bits.is_set(bit),
            "bit {bit} should be set after strategy 2 recovery (Option B: freed bits stay set until compaction)"
        );
    }

    // 6. Total used = 3 (conservative over-estimate: 2 busy + 1 freed-
    // but-not-compacted). Compaction will correct this.
    let total_used: u64 = recovered_dg
        .disks
        .read()
        .unwrap()
        .iter()
        .map(|d| {
            d.zones
                .read()
                .unwrap()
                .iter()
                .map(|z| u64::from(z.used_count.load(std::sync::atomic::Ordering::Acquire)))
                .sum::<u64>()
        })
        .sum();
    assert_eq!(
        total_used, 3,
        "total used after strategy 2 recovery should be 3 (conservative over-estimate)"
    );

    // 7. Run compaction on the zone that has the freed segment — this
    // is the common compaction flow that clears the freed bit.
    let freed_seg = &segments[0];
    let freed_disk_id = freed_seg.disk_id.unwrap();
    let freed_zone_idx = freed_seg.zone_index;
    let freed_disk = recovered_dg
        .disks
        .read()
        .unwrap()
        .iter()
        .find(|d| d.disk_id == freed_disk_id)
        .cloned()
        .expect("freed disk exists");
    let freed_zone = {
        let zones = freed_disk.zones.read().unwrap();
        Arc::clone(&zones[freed_zone_idx as usize])
    };
    let compaction_kv = cluster.make_ddb_kv_client();
    let engine = CompactionEngine::new(Arc::new(compaction_kv), CompactionConfig::default());
    let compaction_metrics = crow_diskdb::metrics::DiskdbMetrics::disabled();
    engine
        .compact_zone_now(
            bind2,
            freed_disk_id,
            &freed_zone,
            freed_zone_idx,
            &compaction_metrics,
        )
        .await
        .expect("compaction should succeed");

    // 8. After compaction, the freed bit is clear and used_count = 2.
    #[allow(clippy::cast_possible_truncation)]
    let freed_bit = freed_seg.unit_offset as u32;
    assert!(
        !freed_zone.usage_bits.is_set(freed_bit),
        "freed bit should be clear after compaction"
    );
    let total_used_after_compaction: u64 = recovered_dg
        .disks
        .read()
        .unwrap()
        .iter()
        .map(|d| {
            d.zones
                .read()
                .unwrap()
                .iter()
                .map(|z| u64::from(z.used_count.load(std::sync::atomic::Ordering::Acquire)))
                .sum::<u64>()
        })
        .sum();
    assert_eq!(
        total_used_after_compaction, 2,
        "total used after compaction should be 2 (freed bit cleared)"
    );

    eprintln!("recovery_strategy2_journal_replay: ALL CHECKS PASSED");
}

/// Compaction: allocate + free blocks, run `compact_zone`, verify the
/// snapshot is written and free records are deleted.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn compaction_compact_zone_writes_snapshot_and_deletes_free_records() {
    if std::env::var("CROW_KV_SERVER_BIN").is_err() && crow_kv_server_bin().is_none() {
        eprintln!("skipping: CROW_KV_SERVER_BIN not set and binary not found");
        return;
    }

    let cluster = KvCluster::start().await;
    let hw = cluster.make_hardware_client();
    seed_hardware(&hw).await;

    // 1. First tick — populates state + writes baseline ZoneValues.
    let container = Arc::new(DdbDiskGroupContainer::new(INSTANCE_ID));
    let svc = cluster.make_service_registry_client();
    let hw2 = cluster.make_hardware_client();
    let dg_kv = cluster.make_ddb_kv_client();
    let keepalive = KeepAlive::new(
        hw2,
        svc,
        Arc::clone(&container),
        KeepAliveConfig {
            interval: Duration::from_secs(10),
            miss_threshold: 3,
            zone_rotate_count: 4,
            cas_retry_limit: 100,
            temp_failure_timeout_secs: 900,
        },
    )
    .with_ddb_kv_client(dg_kv);
    let outcome = keepalive.tick().await;
    assert_eq!(outcome.groups_added, 1);

    // Wait for background zone load (Init → Up).
    wait_for_disks_ready(&container, DG_ID, 3, ZONE_COUNT).await;

    let dg = container.get_disk_group(DG_ID).expect("disk-group exists");
    let bind = *dg.bind.read().unwrap();

    // 2. Allocate 1 block, then free it. This creates 1 free record.
    let owner_chunk = make_chunk_id(0, 0, 42);
    let metrics = crow_diskdb::metrics::DiskdbMetrics::disabled();
    let alloc_kv = cluster.make_ddb_kv_client();
    let segment = alloc::allocate_block(&dg, 1, &owner_chunk, UNIT_SIZE_BYTES, &alloc_kv, 100, 4, &metrics)
        .await
        .expect("allocate");
    let free_kv = cluster.make_ddb_kv_client();
    alloc::free_block(&dg, &segment, &free_kv, false)
        .await
        .expect("free");

    // 3. Get the zone that has the free record.
    let disk_id = segment.disk_id.unwrap();
    let disk = dg
        .disks
        .read()
        .unwrap()
        .iter()
        .find(|d| d.disk_id == disk_id)
        .cloned()
        .expect("disk exists");
    let zone_idx = segment.zone_index;
    let zone = {
        let zones = disk.zones.read().unwrap();
        Arc::clone(&zones[zone_idx as usize])
    };

    // Verify the free record exists before compaction.
    let free_key = crow_protocol::key::FreeBlockKey {
        disk_id,
        zone_index: zone_idx,
        unit_offset: segment.unit_offset,
    };
    let verify_kv = cluster.make_ddb_kv_client();
    let free_before = verify_kv
        .kv()
        .get(
            STORE_ID,
            DATA_GROUP_ID,
            &free_key.to_bytes(),
            crow_kv_client::ReadMode::Linearizable,
            None,
        )
        .await
        .expect("get");
    assert!(
        matches!(free_before, GetOutcome::Found { .. }),
        "free record should exist before compaction"
    );

    // 4. Run compaction on the zone.
    let compaction_kv = Arc::new(cluster.make_ddb_kv_client());
    let compaction = CompactionEngine::new(compaction_kv, CompactionConfig::default());
    let compaction_metrics = crow_diskdb::metrics::DiskdbMetrics::disabled();
    compaction
        .compact_zone_now(bind, disk_id, &zone, zone_idx, &compaction_metrics)
        .await
        .expect("compaction should succeed");

    // 5. Verify the free record is deleted after compaction.
    let verify_kv2 = cluster.make_ddb_kv_client();
    let free_after = verify_kv2
        .kv()
        .get(
            STORE_ID,
            DATA_GROUP_ID,
            &free_key.to_bytes(),
            crow_kv_client::ReadMode::Linearizable,
            None,
        )
        .await
        .expect("get");
    assert!(
        matches!(free_after, GetOutcome::NotFound),
        "free record should be deleted after compaction"
    );

    // 6. Verify the zone's used_count is 0 (the block was freed).
    assert_eq!(
        zone.used_count.load(std::sync::atomic::Ordering::Acquire),
        0,
        "used_count should be 0 after compaction of a freed block"
    );

    // 7. Verify a ZoneValue snapshot exists (compaction writes one).
    let verify_kv3 = cluster.make_ddb_kv_client();
    let snapshot = verify_kv3
        .get_zone_value(bind, &disk_id, zone_idx)
        .await
        .expect("get_zone_value");
    assert!(snapshot.is_some(), "snapshot should exist after compaction");
    assert!(
        snapshot.unwrap().verify_checksum(),
        "snapshot CRC should be valid"
    );

    eprintln!("compaction_compact_zone_writes_snapshot_and_deletes_free_records: ALL CHECKS PASSED");
}

/// Simulate a legacy crashed compaction: write a `ZoneValue` with an
/// advanced `compact_ts` but leave orphaned free records on disk
/// (the legacy two-op design could crash after writing the snapshot
/// but before deleting free records). Then run compaction and verify
/// the watermark drops the orphaned records as stale (no double-free)
/// — the block was re-allocated after the orphaned free, so the bit
/// is SET and `range_clear` must NOT touch it.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn compaction_watermark_prevents_double_free_after_crashed_compaction() {
    if std::env::var("CROW_KV_SERVER_BIN").is_err() && crow_kv_server_bin().is_none() {
        eprintln!("skipping: CROW_KV_SERVER_BIN not set and binary not found");
        return;
    }

    let cluster = KvCluster::start().await;
    let hw = cluster.make_hardware_client();
    seed_hardware(&hw).await;

    // 1. First tick — populates state + writes baseline ZoneValues.
    let container = Arc::new(DdbDiskGroupContainer::new(INSTANCE_ID));
    let svc = cluster.make_service_registry_client();
    let hw2 = cluster.make_hardware_client();
    let dg_kv = cluster.make_ddb_kv_client();
    let keepalive = KeepAlive::new(
        hw2,
        svc,
        Arc::clone(&container),
        KeepAliveConfig {
            interval: Duration::from_secs(10),
            miss_threshold: 3,
            zone_rotate_count: 4,
            cas_retry_limit: 100,
            temp_failure_timeout_secs: 900,
        },
    )
    .with_ddb_kv_client(dg_kv);
    let outcome = keepalive.tick().await;
    assert_eq!(outcome.groups_added, 1);

    // Wait for background zone load (Init → Up).
    wait_for_disks_ready(&container, DG_ID, 3, ZONE_COUNT).await;

    let dg = container.get_disk_group(DG_ID).expect("disk-group exists");
    let bind = *dg.bind.read().unwrap();

    // 2. Allocate block A at offset 0, then free it. This creates a
    // free record with freed_ts = T1.
    let owner_chunk = make_chunk_id(0, 0, 42);
    let metrics = crow_diskdb::metrics::DiskdbMetrics::disabled();
    let alloc_kv = cluster.make_ddb_kv_client();
    let seg_a = alloc::allocate_block(&dg, 1, &owner_chunk, UNIT_SIZE_BYTES, &alloc_kv, 100, 4, &metrics)
        .await
        .expect("allocate A");
    let disk_id = seg_a.disk_id.unwrap();
    let zone_idx = seg_a.zone_index;
    let free_kv = cluster.make_ddb_kv_client();
    alloc::free_block(&dg, &seg_a, &free_kv, false)
        .await
        .expect("free A");

    // 3. Simulate a legacy crashed compaction: manually write a
    // ZoneValue with compact_ts = T1 (advanced past the free record)
    // but DON'T delete the free record. This is the legacy two-op
    // crash window: snapshot written, free records not deleted.
    let disk = dg
        .disks
        .read()
        .unwrap()
        .iter()
        .find(|d| d.disk_id == disk_id)
        .cloned()
        .expect("disk exists");
    let zone = {
        let zones = disk.zones.read().unwrap();
        Arc::clone(&zones[zone_idx as usize])
    };
    // Read the free record's freed_ts.
    let free_key = crow_protocol::key::FreeBlockKey {
        disk_id,
        zone_index: zone_idx,
        unit_offset: seg_a.unit_offset,
    };
    let check_kv = cluster.make_ddb_kv_client();
    let GetOutcome::Found {
        value: free_val_bytes,
        ..
    } = check_kv
        .kv()
        .get(
            STORE_ID,
            DATA_GROUP_ID,
            &free_key.to_bytes(),
            crow_kv_client::ReadMode::Linearizable,
            None,
        )
        .await
        .expect("get")
    else {
        panic!("free record should exist")
    };
    let free_val: crow_protocol::diskdb::rpc::FreeBlockValue =
        bincode::deserialize(&free_val_bytes).expect("deserialize");
    let freed_ts_a = free_val.freed_ts;

    // Manually write a ZoneValue with compact_ts = freed_ts_a (the
    // watermark). The bitmap should have the bit SET (persist-only
    // free doesn't clear it). This simulates the legacy crash: the
    // snapshot says "compacted up to T1" but the free record at T1
    // is still on disk.
    zone.compact_ts
        .store(freed_ts_a, std::sync::atomic::Ordering::Release);
    let zv = zone.to_zone_value();
    let put_kv = cluster.make_ddb_kv_client();
    put_kv
        .put_zone(bind, &disk_id, zone_idx, &zv)
        .await
        .expect("put_zone");

    // 4. Re-allocate block B at the same offset (the free record is
    // orphaned, but the bitmap still has the bit set from the
    // persist-only free — wait, actually after step 2 the bit IS set
    // because persist-only free doesn't clear it. But we need the
    // block to be re-allocated to test double-free prevention. Let's
    // allocate again — the bitmap CAS will find the bit set and skip
    // it. We need to first clear the bit via compaction to re-allocate.
    //
    // Actually, the scenario is: the legacy compaction wrote the
    // snapshot (with compact_ts = T1) but didn't delete the free
    // record. The bitmap in the snapshot has the bit CLEAR (legacy
    // compaction cleared it). Then the block was re-allocated (bit
    // SET again). Now the next compaction sees the orphaned free
    // record with freed_ts = T1 <= compact_ts = T1 → stale → dropped.
    // The bit stays SET (correct — block is busy).
    //
    // To set this up: manually clear the bit in the zone's bitmap,
    // write the snapshot, then simulate re-allocation by manually
    // setting the bit + persisting a BusyBlockKey (bypassing the
    // allocator, which might rotate to a different zone).
    #[allow(clippy::cast_possible_truncation)]
    let offset_a = seg_a.unit_offset as u32;
    let _ = zone.usage_bits.range_clear(offset_a, 1);
    zone.used_count.fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
    let zv2 = zone.to_zone_value();
    let put_kv2 = cluster.make_ddb_kv_client();
    put_kv2
        .put_zone(bind, &disk_id, zone_idx, &zv2)
        .await
        .expect("put_zone 2");

    // Simulate re-allocation: manually set the bit + persist a
    // BusyBlockKey on disk (the block is now busy again).
    let _ = zone.usage_bits.range_set(offset_a, 1);
    zone.used_count.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
    let busy_val = crow_protocol::diskdb::rpc::BusyBlockValue {
        unit_count: 1,
        owner_chunk: Some(owner_chunk),
        unit_size: UNIT_SIZE_BYTES,
        state: crow_protocol::diskdb::rpc::BlockState::Ok as i32,
    };
    let busy_kv = cluster.make_ddb_kv_client();
    busy_kv
        .persist_busy(bind, &disk_id, zone_idx, seg_a.unit_offset, &busy_val)
        .await
        .expect("persist busy B");

    // 5. Run compaction. The orphaned free record (freed_ts = T1) is
    // <= compact_ts (= T1) → stale → dropped (NOT range_clear). The
    // bit stays SET (block B is busy). No double-free.
    let compaction_kv = Arc::new(cluster.make_ddb_kv_client());
    let compaction = CompactionEngine::new(compaction_kv, CompactionConfig::default());
    let compaction_metrics = crow_diskdb::metrics::DiskdbMetrics::disabled();
    compaction
        .compact_zone_now(bind, disk_id, &zone, zone_idx, &compaction_metrics)
        .await
        .expect("compaction should succeed");

    // 6. Verify: the bit is still SET (block B is busy — no double-free).
    assert!(
        zone.usage_bits.is_set(offset_a),
        "block B's bit should still be SET after compaction (no double-free)"
    );
    assert_eq!(
        zone.used_count.load(std::sync::atomic::Ordering::Acquire),
        1,
        "used_count should be 1 (block B is busy)"
    );

    // 7. Verify: the orphaned free record is deleted (compaction
    // deletes all scanned free records, stale or new).
    let verify_kv = cluster.make_ddb_kv_client();
    let free_after = verify_kv
        .kv()
        .get(
            STORE_ID,
            DATA_GROUP_ID,
            &free_key.to_bytes(),
            crow_kv_client::ReadMode::Linearizable,
            None,
        )
        .await
        .expect("get");
    assert!(
        matches!(free_after, GetOutcome::NotFound),
        "orphaned free record should be deleted after compaction"
    );

    eprintln!("compaction_watermark_prevents_double_free_after_crashed_compaction: ALL CHECKS PASSED");
}

/// Verify persist-only recovery is idempotent: recover a disk-group
/// twice from the same KV state, and verify both recoveries produce
/// the same bitmap state (conservative over-estimate — only `Put`
/// `BusyBlockKey` applied, `Delete` `BusyBlockKey` ignored).
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn recovery_persist_only_is_idempotent() {
    if std::env::var("CROW_KV_SERVER_BIN").is_err() && crow_kv_server_bin().is_none() {
        eprintln!("skipping: CROW_KV_SERVER_BIN not set and binary not found");
        return;
    }

    let cluster = KvCluster::start().await;
    let hw = cluster.make_hardware_client();
    seed_hardware(&hw).await;

    // 1. First tick — populates state + writes baseline ZoneValues.
    let container = Arc::new(DdbDiskGroupContainer::new(INSTANCE_ID));
    let svc = cluster.make_service_registry_client();
    let hw2 = cluster.make_hardware_client();
    let dg_kv = cluster.make_ddb_kv_client();
    let keepalive = KeepAlive::new(
        hw2,
        svc,
        Arc::clone(&container),
        KeepAliveConfig {
            interval: Duration::from_secs(10),
            miss_threshold: 3,
            zone_rotate_count: 4,
            cas_retry_limit: 100,
            temp_failure_timeout_secs: 900,
        },
    )
    .with_ddb_kv_client(dg_kv);
    let outcome = keepalive.tick().await;
    assert_eq!(outcome.groups_added, 1);

    // Wait for background zone load (Init → Up).
    wait_for_disks_ready(&container, DG_ID, 3, ZONE_COUNT).await;

    let dg = container.get_disk_group(DG_ID).expect("disk-group exists");
    let bind = *dg.bind.read().unwrap();

    // 2. Allocate 3 blocks, then free 1. This creates a mix of busy
    // and free records on disk.
    let owner_chunk = make_chunk_id(0, 0, 42);
    let metrics = crow_diskdb::metrics::DiskdbMetrics::disabled();
    let alloc_kv = cluster.make_ddb_kv_client();
    let mut segments = Vec::new();
    for _ in 0..3 {
        let seg = alloc::allocate_block(&dg, 1, &owner_chunk, UNIT_SIZE_BYTES, &alloc_kv, 100, 4, &metrics)
            .await
            .expect("allocate");
        segments.push(seg);
    }
    let free_kv = cluster.make_ddb_kv_client();
    alloc::free_block(&dg, &segments[0], &free_kv, false)
        .await
        .expect("free");

    // 3. Collect the disk values for recovery.
    let disk_values: Vec<(DiskId, DiskValue)> = {
        let disks = dg.disks.read().unwrap();
        disks
            .iter()
            .map(|d| (d.disk_id, *d.disk_value.read().unwrap()))
            .collect()
    };

    // 4. First load — load_disk_group from KV state.
    let recovery_kv1 = Arc::new(cluster.make_ddb_kv_client());
    let recovery1 = ZoneLoader::new(Arc::clone(&recovery_kv1), 4);
    let dg1 = recovery1
        .load_disk_group(DG_ID, NODE_ID, RACK_ID, bind, &disk_values, 4)
        .await;

    // 5. Collect used_count per zone per disk from first recovery.
    // Use (disk_id_low, zone_index, used_count) — disk_id.low is a
    // u64 and sortable.
    let mut state1: Vec<(u64, u32, u32)> = Vec::new();
    {
        let disks = dg1.disks.read().unwrap();
        for disk in disks.iter() {
            let zones = disk.zones.read().unwrap();
            for zone in zones.iter() {
                state1.push((
                    disk.disk_id.low,
                    zone.zone_index,
                    zone.used_count.load(std::sync::atomic::Ordering::Acquire),
                ));
            }
        }
    }

    // 6. Second recovery — same KV state, new recovery engine.
    let recovery_kv2 = Arc::new(cluster.make_ddb_kv_client());
    let recovery2 = ZoneLoader::new(Arc::clone(&recovery_kv2), 4);
    let dg2 = recovery2
        .load_disk_group(DG_ID, NODE_ID, RACK_ID, bind, &disk_values, 4)
        .await;

    // 7. Collect used_count per zone per disk from second recovery.
    let mut state2: Vec<(u64, u32, u32)> = Vec::new();
    {
        let disks = dg2.disks.read().unwrap();
        for disk in disks.iter() {
            let zones = disk.zones.read().unwrap();
            for zone in zones.iter() {
                state2.push((
                    disk.disk_id.low,
                    zone.zone_index,
                    zone.used_count.load(std::sync::atomic::Ordering::Acquire),
                ));
            }
        }
    }

    // 8. Verify both recoveries produce the same state (idempotent).
    state1.sort_unstable();
    state2.sort_unstable();
    assert_eq!(
        state1, state2,
        "recovery should be idempotent — both runs produce the same used_count per zone"
    );

    // 9. Verify the freed block's bit is still SET in both recoveries
    // (persist-only — conservative over-estimate).
    let freed_disk_id = segments[0].disk_id.unwrap();
    let freed_zone_idx = segments[0].zone_index;
    #[allow(clippy::cast_possible_truncation)]
    let freed_offset = segments[0].unit_offset as u32;
    for (dg_ref, label) in [(&dg1, "first"), (&dg2, "second")] {
        let disk = dg_ref
            .disks
            .read()
            .unwrap()
            .iter()
            .find(|d| d.disk_id == freed_disk_id)
            .cloned()
            .expect("disk exists");
        let zones = disk.zones.read().unwrap();
        let zone = &zones[freed_zone_idx as usize];
        assert!(
            zone.usage_bits.is_set(freed_offset),
            "freed block's bit should be SET after {label} recovery (persist-only, conservative)"
        );
    }

    eprintln!("recovery_persist_only_is_idempotent: ALL CHECKS PASSED");
}

/// Verify the preparatory thread produces ready zones under churn:
/// allocate + free blocks to create uncompacted free records, run the
/// preparatory cycle, and verify zones are marked `compacted_ready`.
/// Uses `zone_rotate_count = 2` so only 2 of 4 zones are active,
/// leaving 2 non-active zones for the preparatory thread to compact.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn preparatory_thread_produces_ready_zones() {
    if std::env::var("CROW_KV_SERVER_BIN").is_err() && crow_kv_server_bin().is_none() {
        eprintln!("skipping: CROW_KV_SERVER_BIN not set and binary not found");
        return;
    }

    let cluster = KvCluster::start().await;
    let hw = cluster.make_hardware_client();
    seed_hardware(&hw).await;

    // 1. First tick — populates state + writes baseline ZoneValues.
    // Use zone_rotate_count = 2 so only 2 of 4 zones are active.
    let container = Arc::new(DdbDiskGroupContainer::new(INSTANCE_ID));
    let svc = cluster.make_service_registry_client();
    let hw2 = cluster.make_hardware_client();
    let dg_kv = cluster.make_ddb_kv_client();
    let keepalive = KeepAlive::new(
        hw2,
        svc,
        Arc::clone(&container),
        KeepAliveConfig {
            interval: Duration::from_secs(10),
            miss_threshold: 3,
            zone_rotate_count: 2,
            cas_retry_limit: 100,
            temp_failure_timeout_secs: 900,
        },
    )
    .with_ddb_kv_client(dg_kv);
    let outcome = keepalive.tick().await;
    assert_eq!(outcome.groups_added, 1);

    // Wait for background zone load (Init → Up).
    wait_for_disks_ready(&container, DG_ID, 3, ZONE_COUNT).await;

    let dg = container.get_disk_group(DG_ID).expect("disk-group exists");

    // 2. Churn: allocate + free 10 blocks across the disk-group.
    // This creates uncompacted free records on non-active zones
    // (rotation spreads allocations across zones).
    let owner_chunk = make_chunk_id(0, 0, 42);
    let metrics = crow_diskdb::metrics::DiskdbMetrics::disabled();
    let alloc_kv = cluster.make_ddb_kv_client();
    let free_kv = cluster.make_ddb_kv_client();
    let mut freed_segments = Vec::new();
    for _ in 0..10 {
        let seg = alloc::allocate_block(&dg, 1, &owner_chunk, UNIT_SIZE_BYTES, &alloc_kv, 100, 4, &metrics)
            .await
            .expect("allocate");
        alloc::free_block(&dg, &seg, &free_kv, false).await.expect("free");
        freed_segments.push(seg);
    }

    // 3. Collect which zones have uncompacted free records (backlog > 0).
    let zones_with_backlog: Vec<(DiskId, u32)> = {
        let disks = dg.disks.read().unwrap();
        let mut result = Vec::new();
        for disk in disks.iter() {
            let zones = disk.zones.read().unwrap();
            for zone in zones.iter() {
                let backlog = zone
                    .uncompacted_free_record_count
                    .load(std::sync::atomic::Ordering::Acquire);
                if backlog > 0 {
                    result.push((disk.disk_id, zone.zone_index));
                }
            }
        }
        result
    };
    assert!(
        !zones_with_backlog.is_empty(),
        "churn should have created uncompacted free records"
    );

    // 4. Run the preparatory cycle — this compacts non-active zones
    // and marks them compacted_ready.
    let prep_kv = Arc::new(cluster.make_ddb_kv_client());
    let prep =
        crow_diskdb::recovery::compaction::PreparatoryThread::new(prep_kv, CompactionConfig::default());
    let prep_metrics = crow_diskdb::metrics::DiskdbMetrics::disabled();
    prep.preparatory_cycle(&container, 2, &prep_metrics).await;

    // 5. Verify: at least some non-active zones with backlog are now
    // compacted_ready = true.
    let mut ready_count = 0u32;
    {
        let disks = dg.disks.read().unwrap();
        for disk in disks.iter() {
            // Collect active zone indices.
            let active_indices: std::collections::HashSet<u32> = {
                let active = disk.active_zone_context.read().unwrap();
                active.iter().map(|z| z.zone_index).collect()
            };
            let zones = disk.zones.read().unwrap();
            for zone in zones.iter() {
                if active_indices.contains(&zone.zone_index) {
                    continue;
                }
                if zone.compacted_ready.load(std::sync::atomic::Ordering::Acquire) {
                    ready_count += 1;
                }
            }
        }
    }
    assert!(
        ready_count > 0,
        "preparatory cycle should have produced at least one compacted_ready non-active zone"
    );

    // 6. Verify: the compacted zones have backlog = 0 (free records
    // deleted by compaction).
    let mut all_clear = true;
    {
        let disks = dg.disks.read().unwrap();
        for disk in disks.iter() {
            let active_indices: std::collections::HashSet<u32> = {
                let active = disk.active_zone_context.read().unwrap();
                active.iter().map(|z| z.zone_index).collect()
            };
            let zones = disk.zones.read().unwrap();
            for zone in zones.iter() {
                if active_indices.contains(&zone.zone_index) {
                    continue;
                }
                if zone.compacted_ready.load(std::sync::atomic::Ordering::Acquire) {
                    let backlog = zone
                        .uncompacted_free_record_count
                        .load(std::sync::atomic::Ordering::Acquire);
                    if backlog > 0 {
                        all_clear = false;
                        eprintln!(
                            "zone {} on disk {:?} is ready but has backlog {}",
                            zone.zone_index, disk.disk_id, backlog
                        );
                    }
                }
            }
        }
    }
    assert!(all_clear, "compacted_ready zones should have backlog = 0");

    eprintln!("preparatory_thread_produces_ready_zones: ALL CHECKS PASSED");
}
