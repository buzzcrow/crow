// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Fresh process fixture for the diskdb regression benchmark.

use std::io::Write;
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crowdb_protocol::common::{DiskId, HwStatus, NodeValue, RackValue};
use crowdb_protocol::diskdb::rpc::{DiskGroupValue, DiskType, DiskValue};
use crowdb_test_harness::cluster::KvCluster;
use crowdb_test_harness::diskdb::DiskdbProcess;
use crowdb_test_harness::hardware::{INSTANCE_ID, UNIT_SIZE_BYTES, ZONE_COUNT};

const RACK_ID: u64 = 1;
const NODE_ID: u64 = 10;
const ZONE_SIZE_UNITS: u64 = 262_144;
const CAPACITY_UNITS: u64 = ZONE_SIZE_UNITS * ZONE_COUNT as u64;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let stop_path = std::env::args().nth(1).expect("stop-file argument");
    let cluster = KvCluster::start_mem_block().await;
    seed_topology(&cluster).await;
    let diskdb = DiskdbProcess::start(&cluster.mgmt_endpoints, false);
    diskdb.wait_for_ready().await;

    let endpoint = cluster.mgmt_endpoints.first().expect("management endpoint");
    println!("{}", endpoint.trim_start_matches("http://"));
    std::io::stdout().flush().expect("flush endpoint");

    while !Path::new(&stop_path).exists() {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    drop(diskdb);
    drop(cluster);
}

async fn seed_topology(cluster: &KvCluster) {
    let hw = cluster.make_hardware_client();
    let groups = vec![100, 101, 102];
    hw.add_rack(
        RACK_ID,
        &RackValue {
            status: HwStatus::Up as i32,
            node_ids: vec![NODE_ID],
        },
    )
    .await
    .expect("add benchmark rack");
    hw.add_node(
        RACK_ID,
        NODE_ID,
        &NodeValue {
            status: HwStatus::Up as i32,
            disk_group_ids: groups.clone(),
            ..Default::default()
        },
    )
    .await
    .expect("add benchmark node");

    let lease = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock")
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
        + 3_600_000;
    for group in groups {
        let disks: Vec<_> = (0..4)
            .map(|index| DiskId {
                high: group,
                low: index,
            })
            .collect();
        hw.add_disk_group(
            RACK_ID,
            NODE_ID,
            group,
            &DiskGroupValue {
                status: HwStatus::Up as i32,
                disk_ids: disks.clone(),
            },
        )
        .await
        .expect("add benchmark disk-group");
        for disk in &disks {
            hw.add_disk(
                RACK_ID,
                NODE_ID,
                group,
                disk,
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
            .expect("add benchmark disk");
        }
        hw.set_owner(RACK_ID, NODE_ID, group, INSTANCE_ID, lease)
            .await
            .expect("set benchmark owner");
        hw.set_bind(RACK_ID, NODE_ID, group, 0, 1)
            .await
            .expect("set benchmark bind");
    }
}
