// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 0.0.

//! R73 recovery integration tests.
//!
//! Verifies that `RecoveryEngine::rebuild_zone_bitmap_full_scan`
//! (strategy 1) correctly reconstructs zone bitmaps after a simulated
//! restart, and that `Zone::to_zone_value`/`from_zone_value` round-trip
//! correctly.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::cluster::KvCluster;
use crow_diskdb::data_group_client::DataGroupClient;
use crow_diskdb::domain::alloc;
use crow_diskdb::domain::disk_group_container::DdbDiskGroupContainer;
use crow_diskdb::domain::zone::DdbZone;
use crow_diskdb::recovery::RecoveryEngine;
use crow_diskdb::sync::{SyncConfig, SyncLoop};
use crow_kv_client::{ClientConfig, CrowkvClient, HardwareClient, ServiceRegistryClient};
use crow_protocol::common::{ChunkId, DiskId, HwStatus, NodeValue, RackValue};
use crow_protocol::diskdb::rpc::{DiskGroupValue, DiskType, DiskValue};
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

fn make_data_group_client(endpoint: &str) -> DataGroupClient {
    let kv = CrowkvClient::new(ClientConfig::new(vec![endpoint.to_string()]));
    kv.seed_leader(STORE_ID, DATA_GROUP_ID, endpoint.to_string());
    DataGroupClient::new(kv)
}

fn make_hardware_client(endpoint: &str) -> HardwareClient {
    let kv = CrowkvClient::new(ClientConfig::new(vec![endpoint.to_string()]));
    kv.seed_leader(0, 0, endpoint.to_string());
    HardwareClient::new(kv)
}

fn make_service_registry_client(endpoint: &str) -> ServiceRegistryClient {
    let kv = CrowkvClient::new(ClientConfig::new(vec![endpoint.to_string()]));
    kv.seed_leader(0, 0, endpoint.to_string());
    ServiceRegistryClient::new(kv)
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
    let hw = make_hardware_client(&cluster.group0_leader_endpoint);
    seed_hardware(&hw).await;

    // 2. First sync_once — populates in-memory state + writes baseline
    //    ZoneValues.
    let container = Arc::new(DdbDiskGroupContainer::new(INSTANCE_ID));
    let svc = make_service_registry_client(&cluster.group0_leader_endpoint);
    let hw2 = make_hardware_client(&cluster.group0_leader_endpoint);
    let dg_kv = make_data_group_client(&cluster.group1_leader_endpoint);
    let sync_cfg = SyncConfig {
        interval: Duration::from_secs(10),
        miss_threshold: 3,
        zone_rotate_count: 4,
        cas_retry_limit: 100,
    };
    let mut sync_loop =
        SyncLoop::new(hw2, svc, Arc::clone(&container), sync_cfg).with_data_group_client(dg_kv);
    let outcome = sync_loop.sync_once().await;
    assert_eq!(outcome.groups_added, 1);
    assert_eq!(outcome.disks_added, 3);

    let dg = container.get_disk_group(DG_ID).expect("disk-group exists");
    let bind = *dg.bind.read().unwrap();

    // 3. Allocate 3 blocks (anti-affinity spreads across 3 disks),
    //    free 1 of them. After this, 2 blocks remain busy.
    let owner_chunk = make_chunk_id(0, 0, 42);
    let alloc_kv = make_data_group_client(&cluster.group1_leader_endpoint);
    let segments = alloc::allocate_blocks(&dg, 1, 3, &[], &owner_chunk, UNIT_SIZE_BYTES, &alloc_kv, 100, 4)
        .await
        .expect("allocate 3");
    assert_eq!(segments.len(), 3);

    // Free the first 1.
    let free_kv = make_data_group_client(&cluster.group1_leader_endpoint);
    alloc::free_blocks(&dg, &segments[0..1], &free_kv, false)
        .await
        .expect("free 1");

    // Record the expected busy segments (the 2 that remain).
    let remaining_segments: Vec<_> = segments[1..].to_vec();

    // 4. Simulate a restart: drop the in-memory container. A fresh
    //    sync_once will skip baseline ZoneValue writes (snapshots
    //    exist) and create empty zones.
    drop(dg);
    drop(container);
    let container2 = Arc::new(DdbDiskGroupContainer::new(INSTANCE_ID));
    let svc2 = make_service_registry_client(&cluster.group0_leader_endpoint);
    let hw3 = make_hardware_client(&cluster.group0_leader_endpoint);
    let dg_kv2 = make_data_group_client(&cluster.group1_leader_endpoint);
    let sync_cfg2 = SyncConfig {
        interval: Duration::from_secs(10),
        miss_threshold: 3,
        zone_rotate_count: 4,
        cas_retry_limit: 100,
    };
    let mut sync_loop2 =
        SyncLoop::new(hw3, svc2, Arc::clone(&container2), sync_cfg2).with_data_group_client(dg_kv2);
    let outcome2 = sync_loop2.sync_once().await;
    assert_eq!(outcome2.groups_added, 1);
    assert_eq!(outcome2.disks_added, 3);

    let dg2 = container2
        .get_disk_group(DG_ID)
        .expect("disk-group exists after restart");
    let bind2 = *dg2.bind.read().unwrap();
    assert_eq!(bind, bind2);

    // 5. Run strategy 1 recovery on each zone of each disk.
    let recovery_kv = Arc::new(make_data_group_client(&cluster.group1_leader_endpoint));
    let recovery = RecoveryEngine::new(Arc::clone(&recovery_kv), 4);

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
                .rebuild_zone_bitmap_full_scan(bind2, disk.disk_id, zi, unit_capacity)
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

    let zv = zone.to_zone_value();
    assert!(zv.verify_checksum(), "CRC should be valid after to_zone_value");
    assert_eq!(zv.snapshot_slot, 42);

    let restored = DdbZone::from_zone_value(disk_id, 0, 100, 128, &zv)
        .expect("from_zone_value should succeed with valid CRC");
    assert_eq!(restored.used_count.load(std::sync::atomic::Ordering::Acquire), 4);
    assert_eq!(
        restored.snapshot_slot.load(std::sync::atomic::Ordering::Acquire),
        42
    );
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
