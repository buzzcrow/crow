// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Component-level integration test for crow-diskdb.
//!
//! Starts a real 3-node `crow-kv-server` cluster (store 0, groups 0
//! and 1), seeds hardware metadata into group 0, runs the diskdb
//! sync loop + allocate/free in-process, and verifies that busy/free
//! records are persisted to the kv cluster.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::cluster::KvCluster;
use crow_diskdb::node::NodeContainer;
use crow_diskdb::persistence::{self, DataGroupClient};
use crow_diskdb::sync::{SyncConfig, SyncLoop};
use crow_kv_client::{ClientConfig, CrowkvClient, GetOutcome, HardwareClient, ServiceRegistryClient};
use crow_protocol::common::{ChunkId, DiskId, HwStatus, NodeValue, RackValue};
use crow_protocol::diskdb::rpc::{DiskGroupValue, DiskType, DiskValue};
use crow_protocol::diskdb_type_util::ZoneValueExt;
use crow_protocol::key::{BinaryKey, BusyBlockKey, FreeBlockKey};

const RACK_ID: u64 = 1;
const NODE_ID: u64 = 10;
const DG_ID: u64 = 100;
const STORE_ID: u64 = 0;
const DATA_GROUP_ID: u64 = 1;
const INSTANCE_ID: u64 = 999;

/// Small disk: 4 zones × 128 units each (128 = 2 words, round number
/// for bitmap scanning). `unit_size` = 1 MB.
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

/// Point-lookup a key in the data group, returning the value bytes
/// if found.
async fn kv_get(kv: &DataGroupClient, key: &[u8]) -> Option<bytes::Bytes> {
    let outcome = kv
        .kv()
        .get(
            STORE_ID,
            DATA_GROUP_ID,
            key,
            crow_kv_client::ReadMode::Linearizable,
            None,
        )
        .await
        .expect("kv get");
    match outcome {
        GetOutcome::Found { value, .. } => Some(value),
        GetOutcome::NotFound => None,
    }
}

/// Seed hardware metadata into group 0: rack, node, disk-group, 3
/// disks, ownership, bind map.
async fn seed_hardware(hw: &HardwareClient) {
    // Rack
    hw.add_rack(
        RACK_ID,
        &RackValue {
            status: HwStatus::Up as i32,
            node_ids: vec![NODE_ID],
        },
    )
    .await
    .expect("add rack");

    // Node
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

    // Disk-group with 3 disks (allocate_blocks spreads across disks).
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

    // Disks
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

    // Ownership: this instance owns the disk-group.
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

    // Bind: disk-group's records live on store 0, group 1.
    hw.set_bind(RACK_ID, NODE_ID, DG_ID, STORE_ID, DATA_GROUP_ID)
        .await
        .expect("set bind");
}

/// Build a `DataGroupClient` seeded with the leader endpoint for
/// `(store_id, group_id)`.
fn make_data_group_client(endpoint: &str) -> DataGroupClient {
    let kv = CrowkvClient::new(ClientConfig::new(vec![endpoint.to_string()]));
    kv.seed_leader(STORE_ID, DATA_GROUP_ID, endpoint.to_string());
    DataGroupClient::new(kv)
}

/// Build a `HardwareClient` seeded with the group-0 leader endpoint.
fn make_hardware_client(endpoint: &str) -> HardwareClient {
    let kv = CrowkvClient::new(ClientConfig::new(vec![endpoint.to_string()]));
    kv.seed_leader(0, 0, endpoint.to_string());
    HardwareClient::new(kv)
}

/// Build a `ServiceRegistryClient` seeded with the group-0 leader.
fn make_service_registry_client(endpoint: &str) -> ServiceRegistryClient {
    let kv = CrowkvClient::new(ClientConfig::new(vec![endpoint.to_string()]));
    kv.seed_leader(0, 0, endpoint.to_string());
    ServiceRegistryClient::new(kv)
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn diskdb_e2e_allocate_free() {
    // Skip if crow-kv-server binary is not built.
    if std::env::var("CROW_KV_SERVER_BIN").is_err() && crow_kv_server_bin().is_none() {
        eprintln!("skipping: CROW_KV_SERVER_BIN not set and binary not found");
        return;
    }

    // 1. Start the kv cluster.
    let cluster = KvCluster::start().await;
    eprintln!(
        "kv cluster started: group0 leader={}, group1 leader={}",
        cluster.group0_leader_endpoint, cluster.group1_leader_endpoint
    );

    // 2. Seed hardware metadata into group 0.
    let hw = make_hardware_client(&cluster.group0_leader_endpoint);
    seed_hardware(&hw).await;
    eprintln!("hardware metadata seeded");

    // 3. Build diskdb in-process.
    let container = Arc::new(NodeContainer::new(INSTANCE_ID));
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

    // 4. Run one sync tick to populate in-memory state.
    let outcome = sync_loop.sync_once().await;
    eprintln!(
        "sync_once: groups_added={}, disks_added={}, duration_ms={}",
        outcome.groups_added, outcome.disks_added, outcome.sync_duration_ms
    );
    assert_eq!(outcome.groups_added, 1, "expected 1 disk-group added");
    assert_eq!(outcome.disks_added, 3, "expected 3 disks added");

    // 5. Verify the node has 3 disks with zones.
    let node = container
        .get_node(DG_ID)
        .expect("disk-group should be in container");
    let (bind, disk_count, zone_count) = {
        let bind = *node.bind.read().unwrap();
        let disks = node.disks.read().unwrap();
        assert_eq!(disks.len(), 3, "expected 3 disks");
        let zone_count = {
            let zones = disks[0].zones.read().unwrap();
            u32::try_from(zones.len()).unwrap()
        };
        (bind, disks.len(), zone_count)
    };
    assert_eq!(bind, (STORE_ID, DATA_GROUP_ID), "bind should be set");
    assert_eq!(disk_count, 3, "expected 3 disks");
    assert_eq!(zone_count, ZONE_COUNT, "expected {ZONE_COUNT} zones per disk");

    // 6. Verify baseline ZoneValue records were written to group 1
    //    for all 3 disks.
    let verify_kv = make_data_group_client(&cluster.group1_leader_endpoint);
    for did in &[make_disk_id(0, 1), make_disk_id(0, 2), make_disk_id(0, 3)] {
        let zone_records = verify_kv
            .read_zone_records((STORE_ID, DATA_GROUP_ID), did, 0)
            .await
            .expect("read zone records");
        assert!(
            zone_records.zone_value.is_some(),
            "zone 0 should have a ZoneValue for disk {did:?}"
        );
        let zv = zone_records.zone_value.unwrap();
        assert!(zv.verify_checksum(), "zone 0 checksum should be valid");
        assert_eq!(zv.snapshot_slot, 0, "snapshot_slot should be 0");
    }
    eprintln!("baseline ZoneValue verified for all 3 disks");

    // 7. Allocate one block.
    let alloc_kv = make_data_group_client(&cluster.group1_leader_endpoint);
    let owner_chunk = make_chunk_id(0, 0, 42);
    let segment = persistence::allocate_block(
        &node,
        1, // unit_count
        &owner_chunk,
        UNIT_SIZE_BYTES,
        &alloc_kv,
        100, // cas_retry_limit
        4,   // zone_rotate_count
    )
    .await
    .expect("allocate should succeed");
    eprintln!(
        "allocated: disk={:?} zone={} offset={} count={}",
        segment.disk_id, segment.zone_index, segment.unit_offset, segment.unit_count
    );
    assert_eq!(segment.unit_count, 1);
    assert_eq!(segment.owner_chunk, Some(owner_chunk));

    // 8. Verify the BusyBlockValue record was persisted.
    let busy_key = BusyBlockKey {
        disk_id: make_disk_id(0, 1),
        zone_index: segment.zone_index,
        unit_offset: segment.unit_offset,
    };
    let busy_bytes = busy_key.to_bytes();
    let kv_client = make_data_group_client(&cluster.group1_leader_endpoint);
    let busy_val = kv_get(&kv_client, &busy_bytes).await;
    assert!(busy_val.is_some(), "busy record should exist in kv");
    let busy_record: crow_protocol::diskdb::rpc::BusyBlockValue =
        bincode::deserialize(&busy_val.unwrap()).expect("deserialize BusyBlockValue");
    assert_eq!(busy_record.unit_count, 1);
    assert_eq!(busy_record.owner_chunk, Some(owner_chunk));
    eprintln!("BusyBlockValue record verified in kv");

    // 9. Free the block.
    let free_kv = make_data_group_client(&cluster.group1_leader_endpoint);
    persistence::free_block(&node, &segment, &free_kv)
        .await
        .expect("free should succeed");
    eprintln!("freed segment");

    // 10. Verify the FreeBlockValue record was persisted and the
    //     BusyBlockKey is gone.
    let free_key = FreeBlockKey {
        disk_id: make_disk_id(0, 1),
        zone_index: segment.zone_index,
        unit_offset: segment.unit_offset,
    };
    let free_bytes = free_key.to_bytes();
    let verify_kv2 = make_data_group_client(&cluster.group1_leader_endpoint);
    let free_val = kv_get(&verify_kv2, &free_bytes).await;
    assert!(free_val.is_some(), "free record should exist in kv");
    let free_record: crow_protocol::diskdb::rpc::FreeBlockValue =
        bincode::deserialize(&free_val.unwrap()).expect("deserialize FreeBlockValue");
    assert_eq!(free_record.unit_count, 1);
    assert_eq!(free_record.previous_owner, Some(owner_chunk));

    // Verify the busy key is gone.
    let busy_val2 = kv_get(&verify_kv2, &busy_bytes).await;
    assert!(busy_val2.is_none(), "busy record should be gone after free");
    eprintln!("FreeBlockValue record verified, BusyBlockKey gone");

    // 11. Allocate multiple blocks and verify.
    let alloc_kv2 = make_data_group_client(&cluster.group1_leader_endpoint);
    let segments = persistence::allocate_blocks(
        &node,
        1,   // unit_count
        3,   // count
        &[], // exclude_disks
        &owner_chunk,
        UNIT_SIZE_BYTES,
        &alloc_kv2,
        100,
        4,
    )
    .await
    .expect("allocate 3 blocks should succeed");
    assert_eq!(segments.len(), 3, "expected 3 segments");
    eprintln!("allocated 3 blocks");

    // 12. Free all 3 in one batch.
    let free_kv2 = make_data_group_client(&cluster.group1_leader_endpoint);
    persistence::free_blocks(&node, &segments, &free_kv2)
        .await
        .expect("free 3 blocks should succeed");
    eprintln!("freed 3 blocks in batch");

    // 13. Verify all 3 busy keys are gone and 3 free keys exist.
    let verify_kv3 = make_data_group_client(&cluster.group1_leader_endpoint);
    for seg in &segments {
        let bk = BusyBlockKey {
            disk_id: make_disk_id(0, 1),
            zone_index: seg.zone_index,
            unit_offset: seg.unit_offset,
        };
        let bk_bytes = bk.to_bytes();
        let result = kv_get(&verify_kv3, &bk_bytes).await;
        assert!(
            result.is_none(),
            "busy record should be gone for offset {}",
            seg.unit_offset
        );

        let fk = FreeBlockKey {
            disk_id: make_disk_id(0, 1),
            zone_index: seg.zone_index,
            unit_offset: seg.unit_offset,
        };
        let fk_bytes = fk.to_bytes();
        let result = kv_get(&verify_kv3, &fk_bytes).await;
        assert!(
            result.is_some(),
            "free record should exist for offset {}",
            seg.unit_offset
        );
    }
    eprintln!("all 3 batch free records verified");

    eprintln!("diskdb_e2e_allocate_free: ALL CHECKS PASSED");
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
