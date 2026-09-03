// Copyright 2026-present Gian <crow.db@outlook.com>.

//! CLI e2e for cluster observation: `cluster status` and `cluster
//! topology` through `--sysmd-ip` / `--sysmd-port` / `--config` against
//! a real `crowdb-kv-server` with group 0 initialized.

mod common;

use std::time::Duration;

use common::direct::{crowdb_cli_bin, run, spawn_group0};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn cluster_status_topology_via_direct_group0() {
    let Some(g0) = spawn_group0().await else {
        eprintln!("skipping: crowdb-kv-server binary not built");
        return;
    };
    let cli = crowdb_cli_bin();
    if !cli.exists() {
        eprintln!("skipping: crowdb-cli binary not built ({})", cli.display());
        return;
    }

    // cluster init — writes store/group/replica topology into group-0
    // sysdata (idempotent: group 0 already exists from spawn_group0,
    // init handles the 409 conflict and still writes topology).
    let (code, _, stderr) = run(
        &cli,
        g0.mgmt_port,
        &g0.config_path,
        &["cluster", "init", "-n", "1"],
    );
    assert_eq!(code, 0, "cluster init stderr={stderr}");

    // status — lists stores from group-0 sysdata.
    let (code, stdout, stderr) = run(&cli, g0.mgmt_port, &g0.config_path, &["cluster", "status"]);
    assert_eq!(code, 0, "status stderr={stderr}");
    assert!(stdout.contains('0'), "stdout={stdout}");

    // topology — from a node's /topology endpoint.
    let (code, stdout, stderr) = run(
        &cli,
        g0.mgmt_port,
        &g0.config_path,
        &["cluster", "topology", "-n", "1"],
    );
    assert_eq!(code, 0, "topology stderr={stderr}");
    assert!(stdout.contains("store"), "stdout={stdout}");

    // --json status decodes as an array of stores.
    let (code, stdout, _) = run(
        &cli,
        g0.mgmt_port,
        &g0.config_path,
        &["--json", "cluster", "status"],
    );
    assert_eq!(code, 0);
    let parsed: serde_json::Value = serde_json::from_str(&stdout).expect("json status");
    assert!(parsed.is_array(), "json={stdout}");

    tokio::time::sleep(Duration::from_millis(50)).await;
}
