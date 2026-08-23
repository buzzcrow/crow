// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! E2E smoke test for both backends (`MemDisk` data integrity,
//! `NullDisk` pattern verification) across multiple IO sizes (including
//! 2 MB max and 0-byte boundary), plus read-before-write, overwrite,
//! non-zero `zone_offset`, non-zero `DiskId.high`, and a concurrent
//! benchmark with content verification.

use std::sync::Arc;
use std::time::Duration;

use crow_diskio_client::{DiskId as DioDiskId, DiskioClient};
use crow_rpc_ffi::RpcServer;
use crow_test_harness::cluster::KvCluster;
use crow_test_harness::diskio::*;
use crow_test_harness::hardware::{seed_hardware, standard_disk_ids_4, DG_ID, NODE_ID, RACK_ID};

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn disk_io_e2e_full_flow() {
    if !check_binaries() {
        return;
    }

    // 1. Start the kv cluster.
    eprintln!("=== starting kv cluster ===");
    let cluster = KvCluster::start().await;
    eprintln!(
        "kv cluster started: group0={}, group1={}",
        cluster.group0_leader_endpoint, cluster.group1_leader_endpoint
    );

    // 2. Seed hardware metadata into group 0.
    eprintln!("=== seeding hardware metadata ===");
    let hw = cluster.make_hardware_client();
    seed_hardware(&hw, &standard_disk_ids_4()).await;
    eprintln!("hardware metadata seeded (rack={RACK_ID}, node={NODE_ID}, dg={DG_ID}, 4 disks)");

    // 3. Test each backend.
    for backend in [Backend::Null, Backend::Mem] {
        eprintln!();
        eprintln!("=== testing {} backend ===", backend.name());

        let diskio = DiskioProcess::start(&DiskioStartOpts {
            dummy_disk: backend.cli_arg(),
            kv_seeds: &cluster.mgmt_endpoints,
            disks: &[],
            fault_error_rate: 0.0,
            no_o_direct: false,
        });

        let rpc_server = Arc::new(RpcServer::new(None));
        rpc_server.listen("127.0.0.1", 0).expect("listen for rpc client");
        rpc_server.start();
        std::thread::sleep(Duration::from_millis(50));

        let conn = rpc_server
            .connect("127.0.0.1", diskio.port)
            .expect("connect to diskio");
        let dio_client = Arc::new(DiskioClient::new());
        dio_client.attach(&conn);

        diskio.wait_for_disks(&dio_client, &rpc_server, &conn).await;

        let disk2 = DioDiskId::new(0, 2);
        test_read_before_write(&dio_client, &rpc_server, &conn, backend, disk2).await;

        let disk1 = DioDiskId::new(0, 1);
        for &(size, label) in IO_SIZES {
            test_io_round(
                &dio_client,
                &rpc_server,
                &conn,
                &IoRoundParams {
                    backend,
                    disk_id: disk1,
                    zone_index: 0,
                    zone_offset: 0,
                    size,
                    label,
                },
            )
            .await;
        }

        test_io_round(
            &dio_client,
            &rpc_server,
            &conn,
            &IoRoundParams {
                backend,
                disk_id: disk2,
                zone_index: 1,
                zone_offset: 0,
                size: 4096,
                label: "middle-disk2",
            },
        )
        .await;

        test_io_round(
            &dio_client,
            &rpc_server,
            &conn,
            &IoRoundParams {
                backend,
                disk_id: disk1,
                zone_index: 0,
                zone_offset: 8192,
                size: 4096,
                label: "middle-nz-offset",
            },
        )
        .await;

        let disk_hi = DioDiskId::new(0xAB, 4);
        test_io_round(
            &dio_client,
            &rpc_server,
            &conn,
            &IoRoundParams {
                backend,
                disk_id: disk_hi,
                zone_index: 0,
                zone_offset: 0,
                size: 4096,
                label: "middle-high-disk",
            },
        )
        .await;

        eprintln!("=== {} concurrent benchmark ===", backend.name());
        run_concurrent_benchmark(&dio_client, &rpc_server, &conn, backend, disk1).await;

        eprintln!("=== shutting down {} backend ===", backend.name());
        drop(diskio);
        rpc_server.stop();
    }

    eprintln!();
    eprintln!("disk_io_e2e_full_flow: ALL CHECKS PASSED");
}
