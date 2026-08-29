// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! E2E durability test: write + fsync + process restart + read with
//! `BlockDisk` on a temp file (I2 durability invariant).

use crowdb_diskio_client::{DiskId as DioDiskId, DiskIoRetCode, DiskioClient};
use crowdb_test_harness::diskio::*;
use crowdb_test_harness::test_dirs;

/// Verify I2 (durability): write + fsync + process restart + read
/// returns the same data. Uses `BlockDisk` on a temp file with
/// `--no-o-direct` (so unaligned small writes work).
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn disk_io_e2e_durability() {
    if !check_diskio_only() {
        return;
    }

    let temp_dir = test_dirs::test_data_dir();
    let data_path = temp_dir.join(format!("crowdb-diskio-durability-{}.dat", std::process::id()));
    eprintln!("=== durability: temp file {} ===", data_path.display());
    {
        let file = std::fs::File::create(&data_path).expect("create temp file");
        file.set_len(16 * 1024 * 1024).expect("truncate to 16 MB");
    }

    let disk_id = DioDiskId::new(0, 1);
    let disk_arg = DiskArg {
        id_high: 0,
        id_low: 1,
        path: data_path.to_string_lossy().to_string(),
        zone_capacity: 16 * 1024 * 1024,
    };

    eprintln!("=== durability: starting diskio (first run) ===");
    let diskio1 = DiskioProcess::start(&DiskioStartOpts {
        dummy_disk: "null",
        kv_seeds: &[],
        disks: std::slice::from_ref(&disk_arg),
        fault_error_rate: 0.0,
        no_o_direct: true,
    });
    let (rpc_server1, conn1, dio_client1) = connect_to_diskio(&diskio1);

    let write_data: Vec<u8> = (0..4096u32).map(|i| u8::try_from(i % 256).unwrap()).collect();
    let wf = dio_client1
        .write(&rpc_server1, &conn1, disk_id, 0, 0, write_data.clone())
        .expect("write send");
    let wc = DiskioClient::await_write_response(wf).await.expect("write IO");
    assert_eq!(wc, DiskIoRetCode::Success, "durability write should succeed");

    let ff = dio_client1
        .fsync(&rpc_server1, &conn1, disk_id)
        .expect("fsync send");
    let fc = DiskioClient::await_fsync_response(ff).await.expect("fsync IO");
    assert_eq!(fc, DiskIoRetCode::Success, "durability fsync should succeed");
    eprintln!("  wrote + fsync'd 4 KB");

    eprintln!("=== durability: killing diskio ===");
    drop(diskio1);
    rpc_server1.stop();

    eprintln!("=== durability: restarting diskio (second run) ===");
    let diskio2 = DiskioProcess::start(&DiskioStartOpts {
        dummy_disk: "null",
        kv_seeds: &[],
        disks: &[disk_arg],
        fault_error_rate: 0.0,
        no_o_direct: true,
    });
    let (rpc_server2, conn2, dio_client2) = connect_to_diskio(&diskio2);

    let rf = dio_client2
        .read(&rpc_server2, &conn2, disk_id, 0, 0, 4096, 0)
        .expect("read send");
    let (rc, rd) = DiskioClient::await_read_response(rf).await.expect("read IO");
    assert_eq!(rc, DiskIoRetCode::Success, "durability read should succeed");
    let rd = rd.expect("read data should be present");
    assert_eq!(rd.len(), 4096, "durability read length mismatch");
    assert_eq!(
        rd, write_data,
        "durability: read after restart must match written data"
    );
    eprintln!("  read after restart: data matches");

    drop(diskio2);
    rpc_server2.stop();

    let _ = std::fs::remove_file(&data_path);

    eprintln!();
    eprintln!("disk_io_e2e_durability: ALL CHECKS PASSED");
}
