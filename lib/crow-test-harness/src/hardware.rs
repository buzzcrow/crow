// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Hardware metadata seeding for E2E tests.

use crow_kv_client::HardwareClient;
use crow_protocol::common::{DiskId, HwStatus, NodeValue, RackValue};
use crow_protocol::diskdb::rpc::{DiskGroupValue, DiskType, DiskValue};

pub const RACK_ID: u64 = 1;
pub const NODE_ID: u64 = 10;
pub const DG_ID: u64 = 100;
pub const INSTANCE_ID: u64 = 999;
pub const STORE_ID: u64 = 0;
pub const DATA_GROUP_ID: u64 = 1;

// Small disks: 4 zones × 128 units × 1 MB = 512 MB per disk.
pub const ZONE_SIZE_UNITS: u64 = 128;
pub const UNIT_SIZE_BYTES: u32 = 1024 * 1024; // 1 MB
pub const ZONE_COUNT: u32 = 4;
pub const CAPACITY_UNITS: u64 = ZONE_SIZE_UNITS * ZONE_COUNT as u64;

pub fn make_disk_id(high: u64, low: u64) -> DiskId {
    DiskId { high, low }
}

/// Seed hardware metadata into group 0: rack, node, disk-group, disks,
/// owner lease, and KV-group bind. The `disk_ids` parameter controls
/// how many disks are created.
pub async fn seed_hardware(hw: &HardwareClient, disk_ids: &[DiskId]) {
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

    hw.add_disk_group(
        RACK_ID,
        NODE_ID,
        DG_ID,
        &DiskGroupValue {
            status: HwStatus::Up as i32,
            disk_ids: disk_ids.to_vec(),
        },
    )
    .await
    .expect("add disk-group");

    for did in disk_ids {
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
                device_path: String::new(),
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

/// Convenience: the standard 4-disk set used by diskio tests.
pub fn standard_disk_ids_4() -> Vec<DiskId> {
    vec![
        make_disk_id(0, 1),
        make_disk_id(0, 2),
        make_disk_id(0, 3),
        make_disk_id(0xAB, 4),
    ]
}

/// Convenience: the standard 3-disk set used by diskdb tests.
pub fn standard_disk_ids_3() -> Vec<DiskId> {
    vec![make_disk_id(0, 1), make_disk_id(0, 2), make_disk_id(0, 3)]
}

/// Check if the test can run (both kv-server and diskio binaries available).
pub fn check_binaries(diskio_bin: Option<&std::path::Path>) -> bool {
    if std::env::var("CROW_KV_SERVER_BIN").is_err() && crow_kv_server_bin().is_none() {
        eprintln!("skipping: crow-kv-server binary not found");
        return false;
    }
    if diskio_bin.is_none() {
        eprintln!("skipping: required binary not found");
        return false;
    }
    true
}

/// Check if only the kv-server binary is available.
pub fn check_kv_server_only() -> bool {
    if std::env::var("CROW_KV_SERVER_BIN").is_err() && crow_kv_server_bin().is_none() {
        eprintln!("skipping: crow-kv-server binary not found");
        return false;
    }
    true
}

use crate::cluster::crow_kv_server_bin;
