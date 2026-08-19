// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! CLI e2e for the `diskdb` verb group (R77 Phase 2.4). Exercises
//! `diskdb instances`, `diskdb usage`, `diskdb set-status`,
//! `diskdb set-dg-status`, and `diskdb deploy`/`restart`/`stop`/
//! `delete` against a real console. The proxy endpoints (instances,
//! usage) return 502 without a live diskdb — we verify the CLI
//! surfaces the error with exit code 2. The set-status endpoints
//! return 502 without group-0. The deploy path requires the
//! `crow-diskdb` binary and skips silently if not built.

mod common;

use std::time::Duration;

use common::console::{crow_cli_bin, run, spawn_console_empty};
use crow_console_shared::lifecycle::crow_diskdb_bin;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diskdb_cli_proxy_endpoints_surface_error_without_group_zero() {
    let cli = crow_cli_bin();
    if !cli.exists() {
        eprintln!("skipping: crow-cli binary not built");
        return;
    }
    let (console, dir) = spawn_console_empty().await;
    let ip = console.ip().to_string();
    let port = console.port();

    // diskdb instances → 502 (no group-0) → exit code 2.
    let (code, _, stderr) = run(&cli, &ip, port, &["diskdb", "instances"]);
    assert_eq!(code, 2, "instances stderr={stderr}");
    assert!(stderr.contains("error"), "stderr={stderr}");

    // diskdb usage → 502 → exit code 2.
    let (code, _, stderr) = run(&cli, &ip, port, &["diskdb", "usage"]);
    assert_eq!(code, 2, "usage stderr={stderr}");

    // diskdb usage --dg 1 → 502 → exit code 2.
    let (code, _, stderr) = run(&cli, &ip, port, &["diskdb", "usage", "--dg", "1"]);
    assert_eq!(code, 2, "usage dg stderr={stderr}");

    // diskdb scan-status → 502 → exit code 2.
    let (code, _, stderr) = run(&cli, &ip, port, &["diskdb", "scan-status"]);
    assert_eq!(code, 2, "scan-status stderr={stderr}");

    // diskdb scan → 502 → exit code 2.
    let (code, _, stderr) = run(&cli, &ip, port, &["diskdb", "scan"]);
    assert_eq!(code, 2, "scan stderr={stderr}");

    // diskdb recalc → 502 → exit code 2.
    let (code, _, stderr) = run(&cli, &ip, port, &["diskdb", "recalc"]);
    assert_eq!(code, 2, "recalc stderr={stderr}");

    tokio::time::sleep(Duration::from_millis(50)).await;
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn diskdb_cli_set_status_on_unknown_disk_returns_error() {
    let cli = crow_cli_bin();
    if !cli.exists() {
        eprintln!("skipping: crow-cli binary not built");
        return;
    }
    let (console, dir) = spawn_console_empty().await;
    let ip = console.ip().to_string();
    let port = console.port();

    // set-status on a non-existent disk → 404 → exit code 2.
    let (code, _, stderr) = run(
        &cli,
        &ip,
        port,
        &["diskdb", "set-status", "--disk", "nonexistent", "--status", "Up"],
    );
    assert_eq!(code, 2, "set-status stderr={stderr}");

    // set-dg-status on a non-existent dg → 502 (no group-0) → exit code 2.
    let (code, _, stderr) = run(
        &cli,
        &ip,
        port,
        &[
            "diskdb",
            "set-dg-status",
            "--rack",
            "1",
            "--node",
            "1",
            "--dg",
            "1",
            "--status",
            "Up",
        ],
    );
    assert_eq!(code, 2, "set-dg-status stderr={stderr}");

    tokio::time::sleep(Duration::from_millis(50)).await;
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn diskdb_cli_deploy_restart_stop_delete_lifecycle() {
    let cli = crow_cli_bin();
    if !cli.exists() {
        eprintln!("skipping: crow-cli binary not built");
        return;
    }
    let Some(diskdb_bin) = crow_diskdb_bin().filter(|p| p.exists()) else {
        eprintln!("skipping: crow-diskdb binary not built");
        return;
    };
    let diskdb_bin = diskdb_bin.to_string_lossy().into_owned();
    // The deploy handler finds the binary via crow_diskdb_bin() which
    // searches next to the console's exe. Set CROW_DISKDB_BIN so the
    // in-process console can find it.
    std::env::set_var("CROW_DISKDB_BIN", &diskdb_bin);

    let (console, dir) = spawn_console_empty().await;
    let ip = console.ip().to_string();
    let port = console.port();

    // Create rack → node → disk-group → disk.
    let (code, _, stderr) = run(&cli, &ip, port, &["rack", "add", "--id", "1", "--name", "r1"]);
    assert_eq!(code, 0, "rack add stderr={stderr}");

    let (code, _, stderr) = run(&cli, &ip, port, &["node", "add", "--id", "1", "--rack", "1"]);
    assert_eq!(code, 0, "node add stderr={stderr}");

    let (code, _, stderr) = run(
        &cli,
        &ip,
        port,
        &["disk-group", "add", "--node", "1", "--id", "1", "--name", "dg1"],
    );
    assert_eq!(code, 0, "dg add stderr={stderr}");

    let (code, _, stderr) = run(
        &cli,
        &ip,
        port,
        &[
            "disk",
            "add",
            "--node",
            "1",
            "--group",
            "1",
            "--id",
            "00000000000000000000000000000001",
            "--capacity-bytes",
            "4398046511104",
            "--zone-size-bytes",
            "34359738368",
            "--unit-size-bytes",
            "1048576",
        ],
    );
    assert_eq!(code, 0, "disk add stderr={stderr}");

    // diskdb deploy → should succeed (local-fork).
    let (_rest_port, rpc_port) = common::console::pick_two_distinct_free_ports();
    let rpc_port = rpc_port.to_string();
    let (code, stdout, stderr) = run(
        &cli,
        &ip,
        port,
        &["diskdb", "deploy", "--node", "1", "--rpc-port", &rpc_port],
    );
    assert_eq!(code, 0, "diskdb deploy stderr={stderr}");
    assert!(stdout.contains("deployed diskdb on node 1"), "stdout={stdout}");

    // diskdb restart → should succeed.
    let (code, _, stderr) = run(&cli, &ip, port, &["diskdb", "restart", "--node", "1"]);
    assert_eq!(code, 0, "diskdb restart stderr={stderr}");

    // diskdb stop → should succeed. Stop kills the process but
    // preserves the ServerEntry so it can be restarted later.
    let (code, _, stderr) = run(&cli, &ip, port, &["diskdb", "stop", "--node", "1"]);
    assert_eq!(code, 0, "diskdb stop stderr={stderr}");

    // After stop, the ServerEntry is still present — delete succeeds
    // (best-effort stop + remove entry).
    let (code, _, _stderr) = run(&cli, &ip, port, &["diskdb", "delete", "--node", "1"]);
    assert_eq!(
        code, 0,
        "delete after stop should succeed (entry preserved by stop)"
    );

    // --- Second deploy → delete (without stop) ---
    let (_rest_port2, rpc_port2) = common::console::pick_two_distinct_free_ports();
    let rpc_port2 = rpc_port2.to_string();
    let (code, _, stderr) = run(
        &cli,
        &ip,
        port,
        &["diskdb", "deploy", "--node", "1", "--rpc-port", &rpc_port2],
    );
    assert_eq!(code, 0, "diskdb deploy 2 stderr={stderr}");

    // diskdb delete → should succeed (stop + remove entry).
    let (code, _, stderr) = run(&cli, &ip, port, &["diskdb", "delete", "--node", "1"]);
    assert_eq!(code, 0, "diskdb delete stderr={stderr}");

    std::env::remove_var("CROW_DISKDB_BIN");
    tokio::time::sleep(Duration::from_millis(50)).await;
    let _ = std::fs::remove_dir_all(dir);
}
