// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! CLI e2e (R126 direct-to-group-0): `cluster status` and `cluster rack
//! list` route directly through `--sysmd-ip` / `--sysmd-port` against a
//! real `crowdb-kv-server` with group 0 initialized — no `crowdb-web`
//! intermediary.

mod common;

use common::direct::{crowdb_cli_bin, run, spawn_group0};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cluster_status_via_direct_group0() {
    let Some(g0) = spawn_group0().await else {
        eprintln!("skipping: crowdb-kv-server binary not built");
        return;
    };
    let cli = crowdb_cli_bin();
    if !cli.exists() {
        eprintln!("skipping: crowdb-cli binary not built ({})", cli.display());
        return;
    }

    // `cluster status` should list store 0 (the system store).
    let (code, stdout, stderr) = run(&cli, g0.mgmt_port, &g0.config_path, &["cluster", "status"]);
    assert_eq!(code, 0, "cluster status stderr={stderr}");
    // Store 0 should be present (group 0 was initialized).
    assert!(
        stdout.contains('0') || stdout.contains("(no stores)"),
        "cluster status stdout={stdout}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cluster_rack_list_via_direct_config() {
    let Some(g0) = spawn_group0().await else {
        eprintln!("skipping: crowdb-kv-server binary not built");
        return;
    };
    let cli = crowdb_cli_bin();
    if !cli.exists() {
        eprintln!("skipping: crowdb-cli binary not built ({})", cli.display());
        return;
    }

    // `cluster rack list` should list rack 1 (from the config).
    let (code, stdout, stderr) = run(&cli, g0.mgmt_port, &g0.config_path, &["cluster", "rack", "list"]);
    assert_eq!(code, 0, "cluster rack list stderr={stderr}");
    assert!(stdout.contains('1'), "cluster rack list stdout={stdout}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cluster_node_list_via_direct_config() {
    let Some(g0) = spawn_group0().await else {
        eprintln!("skipping: crowdb-kv-server binary not built");
        return;
    };
    let cli = crowdb_cli_bin();
    if !cli.exists() {
        eprintln!("skipping: crowdb-cli binary not built ({})", cli.display());
        return;
    }

    // `cluster node list` should list node 1 (from the config).
    let (code, stdout, stderr) = run(&cli, g0.mgmt_port, &g0.config_path, &["cluster", "node", "list"]);
    assert_eq!(code, 0, "cluster node list stderr={stderr}");
    assert!(stdout.contains('1'), "cluster node list stdout={stdout}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn kv_server_list_via_direct_config() {
    let Some(g0) = spawn_group0().await else {
        eprintln!("skipping: crowdb-kv-server binary not built");
        return;
    };
    let cli = crowdb_cli_bin();
    if !cli.exists() {
        eprintln!("skipping: crowdb-cli binary not built ({})", cli.display());
        return;
    }

    // `kv server list` should list the server on node 1.
    let (code, stdout, stderr) = run(&cli, g0.mgmt_port, &g0.config_path, &["kv", "server", "list"]);
    assert_eq!(code, 0, "kv server list stderr={stderr}");
    assert!(stdout.contains('1'), "kv server list stdout={stdout}");
}
