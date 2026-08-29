// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! E2E error-paths test: verifies client error decoding for
//! `DiskNotExist` (write/read/fsync), `ZoneNotExist`, and `IoError`
//! (via fault injection with `--fault-error-rate 1.0`).

use crowdb_diskio_client::{DiskId as DioDiskId, DiskIoRetCode, DiskioClient, DiskioError};
use crowdb_test_harness::cluster::KvCluster;
use crowdb_test_harness::diskio::*;
use crowdb_test_harness::hardware::{seed_hardware, standard_disk_ids_4};

/// Verify client error decoding for `DiskNotExist`, `ZoneNotExist`,
/// and `IoError` (via fault injection).
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn disk_io_e2e_error_paths() {
    if !check_binaries() {
        return;
    }

    eprintln!("=== error-paths: starting kv cluster ===");
    let cluster = KvCluster::start().await;
    let hw = cluster.make_hardware_client();
    seed_hardware(&hw, &standard_disk_ids_4()).await;

    eprintln!("=== error-paths: starting diskio (mem) ===");
    let diskio = DiskioProcess::start(&DiskioStartOpts {
        dummy_disk: "mem",
        kv_seeds: &cluster.mgmt_endpoints,
        disks: &[],
        fault_error_rate: 0.0,
        no_o_direct: false,
    });
    let (rpc_server, conn, dio_client) = connect_to_diskio(&diskio);
    diskio.wait_for_disks(&dio_client, &rpc_server, &conn).await;

    let valid_disk = DioDiskId::new(0, 1);
    let bad_disk = DioDiskId::new(99, 99);

    eprintln!("=== error-paths: DiskNotExist (write) ===");
    let wf = dio_client
        .write(&rpc_server, &conn, bad_disk, 0, 0, vec![0xAB; 4096])
        .expect("write send");
    let result = DiskioClient::await_write_response(wf).await;
    assert!(
        matches!(result, Err(DiskioError::IoError(DiskIoRetCode::DiskNotExist))),
        "expected DiskNotExist for write, got {result:?}"
    );
    eprintln!("  write to bad disk: DiskNotExist OK");

    eprintln!("=== error-paths: DiskNotExist (read) ===");
    let rf = dio_client
        .read(&rpc_server, &conn, bad_disk, 0, 0, 4096, 0)
        .expect("read send");
    let result = DiskioClient::await_read_response(rf).await;
    assert!(
        matches!(result, Err(DiskioError::IoError(DiskIoRetCode::DiskNotExist))),
        "expected DiskNotExist for read, got {result:?}"
    );
    eprintln!("  read from bad disk: DiskNotExist OK");

    eprintln!("=== error-paths: DiskNotExist (fsync) ===");
    let ff = dio_client
        .fsync(&rpc_server, &conn, bad_disk)
        .expect("fsync send");
    let result = DiskioClient::await_fsync_response(ff).await;
    assert!(
        matches!(result, Err(DiskioError::IoError(DiskIoRetCode::DiskNotExist))),
        "expected DiskNotExist for fsync, got {result:?}"
    );
    eprintln!("  fsync bad disk: DiskNotExist OK");

    eprintln!("=== error-paths: ZoneNotExist (write) ===");
    let wf = dio_client
        .write(&rpc_server, &conn, valid_disk, 99, 0, vec![0xAB; 4096])
        .expect("write send");
    let result = DiskioClient::await_write_response(wf).await;
    assert!(
        matches!(result, Err(DiskioError::IoError(DiskIoRetCode::ZoneNotExist))),
        "expected ZoneNotExist for write, got {result:?}"
    );
    eprintln!("  write to bad zone: ZoneNotExist OK");

    eprintln!("=== error-paths: ZoneNotExist (read) ===");
    let rf = dio_client
        .read(&rpc_server, &conn, valid_disk, 99, 0, 4096, 0)
        .expect("read send");
    let result = DiskioClient::await_read_response(rf).await;
    assert!(
        matches!(result, Err(DiskioError::IoError(DiskIoRetCode::ZoneNotExist))),
        "expected ZoneNotExist for read, got {result:?}"
    );
    eprintln!("  read from bad zone: ZoneNotExist OK");

    drop(diskio);
    rpc_server.stop();

    eprintln!("=== error-paths: IoError (fault injection) ===");
    let diskio_fault = DiskioProcess::start(&DiskioStartOpts {
        dummy_disk: "mem",
        kv_seeds: &cluster.mgmt_endpoints,
        disks: &[],
        fault_error_rate: 1.0,
        no_o_direct: false,
    });
    let (rpc_server2, conn2, dio_client2) = connect_to_diskio(&diskio_fault);
    diskio_fault
        .wait_for_disks(&dio_client2, &rpc_server2, &conn2)
        .await;

    let wf = dio_client2
        .write(&rpc_server2, &conn2, valid_disk, 0, 0, vec![0xAB; 4096])
        .expect("write send");
    let result = DiskioClient::await_write_response(wf).await;
    assert!(
        matches!(result, Err(DiskioError::IoError(DiskIoRetCode::IoError))),
        "expected IoError for fault-injected write, got {result:?}"
    );
    eprintln!("  fault-injected write: IoError OK");

    let rf = dio_client2
        .read(&rpc_server2, &conn2, valid_disk, 0, 0, 4096, 0)
        .expect("read send");
    let result = DiskioClient::await_read_response(rf).await;
    assert!(
        matches!(result, Err(DiskioError::IoError(DiskIoRetCode::IoError))),
        "expected IoError for fault-injected read, got {result:?}"
    );
    eprintln!("  fault-injected read: IoError OK");

    drop(diskio_fault);
    rpc_server2.stop();

    eprintln!();
    eprintln!("disk_io_e2e_error_paths: ALL CHECKS PASSED");
}
