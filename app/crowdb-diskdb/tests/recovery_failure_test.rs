// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

mod common;

use std::sync::Arc;

use common::cluster::KvCluster;
use crowdb_diskdb::recovery::ZoneLoader;
use crowdb_protocol::common::DiskId;
use crowdb_protocol::diskdb::rpc::{DiskType, DiskValue};

#[tokio::test]
async fn failed_recovery_does_not_return_writable_disk_group() {
    let cluster = KvCluster::start().await;
    let _hardware = cluster.make_hardware_client();
    let _registry = cluster.make_service_registry_client();
    let loader = ZoneLoader::new(Arc::new(cluster.make_ddb_kv_client()), 1);
    let disk = DiskValue {
        disk_type: DiskType::BlockSsd as i32,
        capacity_units: 128,
        zone_size_units: 128,
        unit_size_bytes: 4096,
        zone_count: 1,
        status: crowdb_protocol::common::HwStatus::Up as i32,
        device_path: String::new(),
    };

    let result = loader
        .load_disk_group(100, 10, 1, (0, 999), &[(DiskId { high: 0, low: 1 }, disk)], 1)
        .await;

    assert!(
        result.is_err(),
        "failed recovery must not synthesize empty Up zones"
    );
}
