// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! E2E group-0 sync test: verifies periodic group-0 heartbeat
//! (service registry entry appears) and disk-list reconciliation
//! (new disk added to group-0 after startup becomes writable).

use std::time::{Duration, Instant};

use crow_diskio_client::{DiskId as DioDiskId, DiskIoRetCode, DiskioClient, DiskioError};
use crow_protocol::common::HwStatus;
use crow_protocol::diskdb::rpc::{DiskGroupValue, DiskType, DiskValue};
use crow_test_harness::cluster::KvCluster;
use crow_test_harness::diskio::*;
use crow_test_harness::hardware::{
    make_disk_id, seed_hardware, standard_disk_ids_4, CAPACITY_UNITS, DG_ID, INSTANCE_ID, NODE_ID, RACK_ID,
    UNIT_SIZE_BYTES, ZONE_COUNT, ZONE_SIZE_UNITS,
};

/// Verify group-0 periodic sync: (a) heartbeat registers the diskio
/// instance in the service registry, and (b) disk-list reconciliation
/// picks up a new disk added to group-0 after startup.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn disk_io_e2e_group0_sync() {
    if !check_binaries() {
        return;
    }

    eprintln!("=== group0-sync: starting kv cluster ===");
    let cluster = KvCluster::start().await;
    let hw = cluster.make_hardware_client();
    seed_hardware(&hw, &standard_disk_ids_4()).await;
    eprintln!("hardware metadata seeded (3 initial disks)");

    eprintln!("=== group0-sync: starting diskio ===");
    let diskio = DiskioProcess::start(&DiskioStartOpts {
        dummy_disk: "mem",
        kv_seeds: &cluster.mgmt_endpoints,
        disks: &[],
        fault_error_rate: 0.0,
        no_o_direct: false,
    });
    let (rpc_server, conn, dio_client) = connect_to_diskio(&diskio);
    diskio.wait_for_disks(&dio_client, &rpc_server, &conn).await;

    eprintln!("=== group0-sync: verifying heartbeat ===");
    let svc = cluster.make_service_registry_client();
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut heartbeat_ok = false;
    while Instant::now() < deadline {
        if let Ok(Some(instance)) = svc.read_instance("diskdb", INSTANCE_ID).await {
            eprintln!(
                "  service registry: found diskdb instance {} at {}",
                instance.instance_id, instance.grpc_endpoint
            );
            assert!(
                !instance.grpc_endpoint.is_empty(),
                "heartbeat should register a non-empty grpc_endpoint"
            );
            heartbeat_ok = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert!(
        heartbeat_ok,
        "diskio should heartbeat to the service registry within 15s"
    );
    eprintln!("  heartbeat verified");

    eprintln!("=== group0-sync: adding new disk to group-0 ===");
    let new_disk_id = make_disk_id(0, 42);
    hw.add_disk(
        RACK_ID,
        NODE_ID,
        DG_ID,
        &new_disk_id,
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
    .expect("add new disk");

    let all_disk_ids = vec![
        make_disk_id(0, 1),
        make_disk_id(0, 2),
        make_disk_id(0, 3),
        make_disk_id(0xAB, 4),
        new_disk_id,
    ];
    hw.add_disk_group(
        RACK_ID,
        NODE_ID,
        DG_ID,
        &DiskGroupValue {
            status: HwStatus::Up as i32,
            disk_ids: all_disk_ids,
        },
    )
    .await
    .expect("update disk-group");
    eprintln!("  added disk {new_disk_id:?} to group-0");

    eprintln!("=== group0-sync: waiting for reconciliation ===");
    let new_disk_dio = DioDiskId::new(0, 42);
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut reconciled = false;
    while Instant::now() < deadline {
        let wf = dio_client
            .write(&rpc_server, &conn, new_disk_dio, 0, 0, vec![0xCD; 4096])
            .expect("reconcile write send");
        match DiskioClient::await_write_response(wf).await {
            Ok(DiskIoRetCode::Success) => {
                reconciled = true;
                break;
            }
            Err(DiskioError::IoError(DiskIoRetCode::DiskNotExist)) => {
                // Not yet reconciled — keep waiting.
            }
            other => {
                eprintln!("  reconcile write unexpected result: {other:?}");
            }
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert!(
        reconciled,
        "diskio should reconcile the new disk within 20s. Log:\n{}",
        diskio.log_content()
    );
    eprintln!("  new disk reconciled and writable");

    let rf = dio_client
        .read(&rpc_server, &conn, new_disk_dio, 0, 0, 4096, 0)
        .expect("reconcile read send");
    let (rc, rd) = DiskioClient::await_read_response(rf)
        .await
        .expect("reconcile read IO");
    assert_eq!(rc, DiskIoRetCode::Success, "reconciled disk read should succeed");
    let rd = rd.expect("reconciled read data should be present");
    assert_eq!(rd, vec![0xCD; 4096], "reconciled disk data should match");
    eprintln!("  reconciled disk read-back verified");

    drop(diskio);
    rpc_server.stop();

    eprintln!();
    eprintln!("disk_io_e2e_group0_sync: ALL CHECKS PASSED");
}
