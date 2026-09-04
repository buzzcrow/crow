// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::cluster::KvCluster;
use crowdb_diskdb::ddb_config::KeepAliveConfig;
use crowdb_diskdb::liveness::keepalive::KeepAlive;
use crowdb_diskdb::model::disk_group_container::DdbDiskGroupContainer;

const RACK_ID: u64 = 1;
const NODE_ID: u64 = 10;
const DG_ID: u64 = 100;
const INSTANCE_ID: u64 = 999;

#[tokio::test]
async fn owner_without_bind_does_not_publish_disk_group() {
    let cluster = KvCluster::start().await;
    let hardware = cluster.make_hardware_client();
    hardware
        .set_owner(RACK_ID, NODE_ID, DG_ID, INSTANCE_ID, u64::MAX)
        .await
        .expect("set owner");
    hardware
        .set_owner(RACK_ID, NODE_ID, DG_ID, INSTANCE_ID, u64::MAX - 1)
        .await
        .expect("renew same owner");
    let replacement = hardware
        .set_owner(RACK_ID, NODE_ID, DG_ID, INSTANCE_ID + 1, u64::MAX)
        .await;
    assert!(
        matches!(replacement, Err(crowdb_kv_client::Error::OwnerConflict { .. })),
        "different owner must be rejected"
    );

    let container = Arc::new(DdbDiskGroupContainer::new(INSTANCE_ID));
    let keepalive = KeepAlive::new(
        cluster.make_hardware_client(),
        cluster.make_service_registry_client(),
        Arc::clone(&container),
        KeepAliveConfig {
            interval: Duration::from_secs(10),
            miss_threshold: 1,
            zone_rotate_count: 4,
            cas_retry_limit: 100,
            temp_failure_timeout_secs: 900,
        },
    )
    .with_ddb_kv_client(cluster.make_ddb_kv_client());

    let outcome = keepalive.tick().await;

    assert_eq!(outcome.groups_added, 0);
    assert!(container.get_disk_group(DG_ID).is_none());
    assert!(container.is_degraded());
}
