// Copyright 2026-present Gian <crow.db@outlook.com>.

//! CLI e2e for the `chunk diskdb` verb group. The `chunk diskdb`
//! subcommands are Phase 3 stubs (not yet wired to ops). These tests
//! are ignored until the chunk diskdb ops are implemented.

mod common;

use std::time::Duration;

use common::direct::{crowdb_cli_bin, run, spawn_group0};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "Phase 3: chunk diskdb ops not yet implemented"]
async fn diskdb_cli_usage_scan_recalc_surface_error_without_diskdb() {
    let Some(g0) = spawn_group0().await else {
        eprintln!("skipping: crowdb-kv-server binary not built");
        return;
    };
    let cli = crowdb_cli_bin();
    if !cli.exists() {
        eprintln!("skipping: crowdb-cli binary not built ({})", cli.display());
        return;
    }

    // chunk diskdb usage → stub returns exit code 1.
    let (code, _, stderr) = run(&cli, g0.mgmt_port, &g0.config_path, &["chunk", "diskdb", "usage"]);
    assert_eq!(code, 1, "usage stderr={stderr}");

    // chunk diskdb scan-status → stub returns exit code 1.
    let (code, _, stderr) = run(
        &cli,
        g0.mgmt_port,
        &g0.config_path,
        &["chunk", "diskdb", "scan-status"],
    );
    assert_eq!(code, 1, "scan-status stderr={stderr}");

    // chunk diskdb scan → stub returns exit code 1.
    let (code, _, stderr) = run(&cli, g0.mgmt_port, &g0.config_path, &["chunk", "diskdb", "scan"]);
    assert_eq!(code, 1, "scan stderr={stderr}");

    // chunk diskdb recalc → stub returns exit code 1.
    let (code, _, stderr) = run(
        &cli,
        g0.mgmt_port,
        &g0.config_path,
        &["chunk", "diskdb", "recalc"],
    );
    assert_eq!(code, 1, "recalc stderr={stderr}");

    tokio::time::sleep(Duration::from_millis(50)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "Phase 3: chunk diskdb ops not yet implemented"]
async fn diskdb_cli_deploy_restart_stop_delete_lifecycle() {
    let Some(g0) = spawn_group0().await else {
        eprintln!("skipping: crowdb-kv-server binary not built");
        return;
    };
    let cli = crowdb_cli_bin();
    if !cli.exists() {
        eprintln!("skipping: crowdb-cli binary not built ({})", cli.display());
        return;
    }

    // chunk diskdb deploy → stub returns exit code 1.
    let (code, _, stderr) = run(
        &cli,
        g0.mgmt_port,
        &g0.config_path,
        &["chunk", "diskdb", "deploy", "-n", "1"],
    );
    assert_eq!(code, 1, "deploy stderr={stderr}");

    tokio::time::sleep(Duration::from_millis(50)).await;
}
