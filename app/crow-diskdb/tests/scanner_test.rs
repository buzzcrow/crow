// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Background scanner integration tests.
//!
//! Verifies that:
//! - `ScanState` request/consume/record lifecycle works.
//! - `ScanSummary` drift/corrupt totals exclude non-drift categories.
//! - `diff_bitmaps` detects ghost-busy, ghost-free, and respects
//!   `unit_capacity`.
//! - `is_zero_owner` / `is_zero_chunk` detect all-zero owner chunks.
//! - End-to-end: allocate blocks, inject a ghost-busy bit, run
//!   `scan_ghosts`, verify the ghost is detected + auto-corrected.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::cluster::{wait_for_disks_ready, KvCluster};
use crow_diskdb::ddb_config::KeepAliveConfig;
use crow_diskdb::ddb_kv_client::DdbKvClient;
use crow_diskdb::liveness::keepalive::KeepAlive;
use crow_diskdb::model::alloc;
use crow_diskdb::model::disk_group_container::DdbDiskGroupContainer;
use crow_diskdb::scanner::ghost::diff_bitmaps;
use crow_diskdb::scanner::integrity::{is_zero_chunk, is_zero_owner};
use crow_diskdb::scanner::{ScanState, ScanSummary};
use crow_kv_client::{ClientConfig, CrowkvClient, HardwareClient, ServiceRegistryClient};
use crow_protocol::common::{ChunkId, DiskId, HwStatus, NodeValue, RackValue};
use crow_protocol::diskdb::rpc::{BusyBlockValue, DiskGroupValue, DiskType, DiskValue};
use crow_protocol::key::BinaryKey;
use crow_protocol::{DiskGroupId, UsageBitmap};

const RACK_ID: u64 = 1;
const NODE_ID: u64 = 10;
const DG_ID: DiskGroupId = 100;
const STORE_ID: u64 = 0;
const DATA_GROUP_ID: u64 = 1;
const INSTANCE_ID: u64 = 999;

const ZONE_SIZE_UNITS: u64 = 128;
const UNIT_SIZE_BYTES: u32 = 1024 * 1024;
// 6 zones but `zone_rotate_count = 4` — the active set covers zones
// 0-3, leaving zones 4-5 non-active. The scanner tests inject drift
// into a non-active zone because both scans skip the active set.
const CAPACITY_UNITS: u64 = ZONE_SIZE_UNITS * 6;
const ZONE_COUNT: u32 = 6;

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

fn make_ddb_kv_client(endpoint: &str) -> DdbKvClient {
    let kv = CrowkvClient::new(ClientConfig::new(vec![endpoint.to_string()]));
    kv.seed_leader(STORE_ID, DATA_GROUP_ID, endpoint.to_string());
    DdbKvClient::new(kv)
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

// ── Unit tests (pure functions, no cluster needed) ───────────────

#[test]
fn scan_state_lifecycle() {
    let s = ScanState::new();
    assert!(s.last_summary().is_none());
    assert!(!s.is_scan_requested());
    assert!(!s.is_in_progress());

    let _ = s.request_scan();
    assert!(s.is_scan_requested());

    // Record a summary.
    let summary = ScanSummary {
        started_at_ms: 100,
        duration_ms: 50,
        zones_scanned: 10,
        ghost_busy: 2,
        ..Default::default()
    };
    s.record_summary_for_tests(summary.clone());
    let last = s.last_summary().expect("summary recorded");
    assert_eq!(last.started_at_ms, 100);
    assert_eq!(last.zones_scanned, 10);
    assert_eq!(last.ghost_busy, 2);
    assert_eq!(last.drift_total(), 2);
}

#[test]
fn scan_summary_drift_total_excludes_uncompacted() {
    let s = ScanSummary {
        ghost_busy: 3,
        ghost_free: 1,
        uncompacted_lag: 10,
        ..Default::default()
    };
    assert_eq!(s.drift_total(), 4);
}

#[test]
fn scan_summary_corrupt_total() {
    let s = ScanSummary {
        corrupt_snapshots: 2,
        corrupt_records: 5,
        ..Default::default()
    };
    assert_eq!(s.corrupt_total(), 7);
}

#[test]
fn diff_bitmaps_no_diff_when_equal() {
    let live = UsageBitmap::new(128);
    let rep = UsageBitmap::new(128);
    assert!(diff_bitmaps(&live, &rep, 128).is_empty());
}

#[test]
fn diff_bitmaps_detects_ghost_busy() {
    let live = UsageBitmap::new(128);
    let rep = UsageBitmap::new(128);
    let _ = live.cas_bit(3, true);
    let _ = live.cas_bit(4, true);
    let diffs = diff_bitmaps(&live, &rep, 128);
    assert_eq!(diffs.len(), 2);
    assert!(diffs.iter().all(|(_, gb)| *gb));
    assert!(diffs.iter().any(|(b, _)| *b == 3));
    assert!(diffs.iter().any(|(b, _)| *b == 4));
}

#[test]
fn diff_bitmaps_detects_ghost_free() {
    let live = UsageBitmap::new(128);
    let rep = UsageBitmap::new(128);
    let _ = rep.cas_bit(6, true);
    let diffs = diff_bitmaps(&live, &rep, 128);
    assert_eq!(diffs.len(), 1);
    assert_eq!(diffs[0].0, 6);
    assert!(!diffs[0].1);
}

#[test]
fn diff_bitmaps_respects_unit_capacity() {
    let live = UsageBitmap::new(128);
    let rep = UsageBitmap::new(128);
    let _ = rep.cas_bit(70, true);
    // Bit 70 is past unit_capacity=64 → not in diff.
    let diffs = diff_bitmaps(&live, &rep, 64);
    assert!(diffs.is_empty());
}

#[test]
fn is_zero_chunk_detects_all_zeros() {
    assert!(is_zero_chunk(&ChunkId {
        high: 0,
        mid: 0,
        low: 0
    }));
    assert!(!is_zero_chunk(&ChunkId {
        high: 0,
        mid: 0,
        low: 1
    }));
    assert!(!is_zero_chunk(&ChunkId {
        high: 1,
        mid: 0,
        low: 0
    }));
}

#[test]
fn is_zero_owner_handles_none() {
    let bv = BusyBlockValue {
        unit_count: 1,
        owner_chunk: None,
        unit_size: 1024,
        state: 0,
    };
    assert!(is_zero_owner(&bv));
}

#[test]
fn is_zero_owner_handles_zero_chunk() {
    let bv = BusyBlockValue {
        unit_count: 1,
        owner_chunk: Some(ChunkId {
            high: 0,
            mid: 0,
            low: 0,
        }),
        unit_size: 1024,
        state: 0,
    };
    assert!(is_zero_owner(&bv));
}

#[test]
fn is_zero_owner_handles_valid_chunk() {
    let bv = BusyBlockValue {
        unit_count: 1,
        owner_chunk: Some(ChunkId {
            high: 0,
            mid: 0,
            low: 42,
        }),
        unit_size: 1024,
        state: 0,
    };
    assert!(!is_zero_owner(&bv));
}

// ── Integration test: ghost detection + auto-correct ─────────────

/// Allocate blocks, inject a ghost-busy bit (set a bit with no
/// corresponding `BusyBlockKey`), run `scan_ghosts` with auto-correct,
/// and verify the ghost is detected + corrected.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn scan_ghosts_detects_and_corrects_ghost_busy() {
    if std::env::var("CROW_KV_SERVER_BIN").is_err() && crow_kv_server_bin().is_none() {
        eprintln!("skipping: CROW_KV_SERVER_BIN not set and binary not found");
        return;
    }

    // 1. Start cluster + seed hardware + first tick.
    let cluster = KvCluster::start().await;
    let hw = make_hardware_client(&cluster.group0_leader_endpoint);
    seed_hardware(&hw).await;

    let container = Arc::new(DdbDiskGroupContainer::new(INSTANCE_ID));
    let svc = make_service_registry_client(&cluster.group0_leader_endpoint);
    let hw2 = make_hardware_client(&cluster.group0_leader_endpoint);
    let dg_kv = make_ddb_kv_client(&cluster.group1_leader_endpoint);
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

    // R81: wait for the background Init→Up zone load before allocating.
    wait_for_disks_ready(&container, DG_ID, 3, ZONE_COUNT).await;

    let dg = container.get_disk_group(DG_ID).expect("disk-group exists");
    let bind = *dg.bind.read().unwrap();

    // 2. Allocate 1 block to have a real busy record.
    let owner_chunk = make_chunk_id(0, 0, 42);
    let alloc_kv = make_ddb_kv_client(&cluster.group1_leader_endpoint);
    let metrics = crow_diskdb::metrics::DiskdbMetrics::disabled();
    let segments = alloc::allocate_blocks(
        &dg,
        1,
        1,
        &[],
        &owner_chunk,
        UNIT_SIZE_BYTES,
        &alloc_kv,
        100,
        4,
        &metrics,
    )
    .await
    .expect("allocate 1");
    assert_eq!(segments.len(), 1);

    // 3. Inject a ghost-busy bit: pick a zone outside the active set
    //    (the scanner skips active zones) and a high bit index that
    //    allocation would never touch (allocation fills from low bits).
    let disk = dg
        .disks
        .read()
        .unwrap()
        .iter()
        .find(|d| d.disk_id == segments[0].disk_id.unwrap_or_default())
        .cloned()
        .expect("disk exists");
    let zone_idx: u32 = ZONE_COUNT - 1; // non-active zone (active set = 0..zone_rotate_count)
    let ghost_bit: u32 = 120; // high bit, no record there
    {
        let zones = disk.zones.read().unwrap();
        let zone = &zones[zone_idx as usize];
        // Verify the bit is currently clear.
        assert!(
            !zone.usage_bits.is_set(ghost_bit),
            "ghost bit should be clear before injection"
        );
        // Inject the ghost: set the bit without writing a BusyBlockKey.
        let _ = zone.usage_bits.cas_bit(ghost_bit, true);
        zone.used_count.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
    }

    // 4. Run scan_ghosts with auto_correct=true, reverify_delay_ms=0.
    let scan_kv = make_ddb_kv_client(&cluster.group1_leader_endpoint);
    let zones_list: Vec<(u32, u32, Arc<crow_diskdb::model::zone::DdbZone>)> = {
        let zones = disk.zones.read().unwrap();
        zones
            .iter()
            .map(|z| (z.zone_index, z.unit_capacity, Arc::clone(z)))
            .collect()
    };
    let active_zones: Vec<Arc<crow_diskdb::model::zone::DdbZone>> = {
        let active = disk.active_zone_context.read().unwrap();
        active.iter().cloned().collect()
    };
    let disk_id = disk.disk_id;
    let result = crow_diskdb::scanner::ghost::scan_ghosts(
        &scan_kv,
        bind,
        disk_id,
        &zones_list,
        &active_zones,
        true, // auto_correct
        0,    // reverify_delay_ms (no delay for test)
    )
    .await;

    // 5. Verify the ghost was detected.
    assert!(
        result.ghost_busy >= 1,
        "ghost-busy should be detected, got {}",
        result.ghost_busy
    );

    // 6. Verify the ghost bit was auto-corrected (cleared).
    {
        let zones = disk.zones.read().unwrap();
        let zone = &zones[zone_idx as usize];
        assert!(
            !zone.usage_bits.is_set(ghost_bit),
            "ghost bit should be cleared after auto-correct"
        );
    }

    eprintln!("scan_ghosts_detects_and_corrects_ghost_busy: ALL CHECKS PASSED");
}

/// Allocate a block, then corrupt the `ZoneValue` snapshot's CRC by
/// writing a bad `ZoneValue`. Run `scan_integrity` and verify the
/// corrupt snapshot is detected.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn scan_integrity_detects_corrupt_snapshot() {
    if std::env::var("CROW_KV_SERVER_BIN").is_err() && crow_kv_server_bin().is_none() {
        eprintln!("skipping: CROW_KV_SERVER_BIN not set and binary not found");
        return;
    }

    let cluster = KvCluster::start().await;
    let hw = make_hardware_client(&cluster.group0_leader_endpoint);
    seed_hardware(&hw).await;

    let container = Arc::new(DdbDiskGroupContainer::new(INSTANCE_ID));
    let svc = make_service_registry_client(&cluster.group0_leader_endpoint);
    let hw2 = make_hardware_client(&cluster.group0_leader_endpoint);
    let dg_kv = make_ddb_kv_client(&cluster.group1_leader_endpoint);
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

    // R81: wait for the background Init→Up zone load before scanning.
    wait_for_disks_ready(&container, DG_ID, 3, ZONE_COUNT).await;

    let dg = container.get_disk_group(DG_ID).expect("disk-group exists");
    let bind = *dg.bind.read().unwrap();

    // Pick the first disk + a non-active zone (the scanner skips the
    // active set).
    let disk = dg.disks.read().unwrap()[0].clone();
    let disk_id = disk.disk_id;
    let zone_idx: u32 = ZONE_COUNT - 1;

    // Write a corrupt ZoneValue (wrong CRC) directly to KV.
    let corrupt_zv = crow_protocol::diskdb::rpc::ZoneValue {
        usage_bitmap: vec![0u8; 16],
        snapshot_slot: 1,
        crc32: 999, // wrong CRC
        compact_ts: 0,
    };
    let zone_key = crow_protocol::key::ZoneKey {
        disk_id,
        zone_index: zone_idx,
    };
    let zone_bytes = zone_key.to_bytes();
    let corrupt_bytes = bincode::serialize(&corrupt_zv).unwrap();
    let write_kv = make_ddb_kv_client(&cluster.group1_leader_endpoint);
    let _ = write_kv
        .kv()
        .put(STORE_ID, DATA_GROUP_ID, &zone_bytes, &corrupt_bytes, None)
        .await;

    // Run scan_integrity.
    let scan_kv = make_ddb_kv_client(&cluster.group1_leader_endpoint);
    let zones_list: Vec<(u32, u32, Arc<crow_diskdb::model::zone::DdbZone>)> = {
        let zones = disk.zones.read().unwrap();
        zones
            .iter()
            .map(|z| (z.zone_index, z.unit_capacity, Arc::clone(z)))
            .collect()
    };
    let active_zones: Vec<Arc<crow_diskdb::model::zone::DdbZone>> = {
        let active = disk.active_zone_context.read().unwrap();
        active.iter().cloned().collect()
    };
    let result = crow_diskdb::scanner::integrity::scan_integrity(
        &scan_kv,
        bind,
        disk_id,
        &zones_list,
        &active_zones,
        false, // detect_owner_mismatch
    )
    .await;

    assert!(
        result.corrupt_snapshots >= 1,
        "corrupt snapshot should be detected, got {}",
        result.corrupt_snapshots
    );

    eprintln!("scan_integrity_detects_corrupt_snapshot: ALL CHECKS PASSED");
}
