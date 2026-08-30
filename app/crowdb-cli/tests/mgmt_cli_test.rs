// Copyright 2026-present Gian <crow.db@outlook.com>.

//! CLI e2e for the KV logical plane: `kv store / group / replica` verbs
//! through `--sysmd-ip` / `--sysmd-port` / `--config` against a real
//! `crowdb-kv-server` with group 0 initialized.

mod common;

use std::time::Duration;

use common::direct::{crowdb_cli_bin, run, spawn_group0};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn store_group_replica_round_trip() {
    let Some(g0) = spawn_group0().await else {
        eprintln!("skipping: crowdb-kv-server binary not built");
        return;
    };
    let cli = crowdb_cli_bin();
    if !cli.exists() {
        eprintln!("skipping: crowdb-cli binary not built ({})", cli.display());
        return;
    }

    let store_id = "9";
    let group_id = "90";
    let replica_id = "900";

    // store add — add store 9 on node 1.
    let (code, stdout, stderr) = run(
        &cli,
        g0.mgmt_port,
        &g0.config_path,
        &["kv", "store", "add", "-I", store_id, "-n", "1"],
    );
    assert_eq!(code, 0, "store add stdout={stdout}\nstderr={stderr}");

    // store list — should contain store 9.
    let (code, stdout, _) = run(&cli, g0.mgmt_port, &g0.config_path, &["kv", "store", "list"]);
    assert_eq!(code, 0);
    assert!(stdout.contains(store_id), "stdout={stdout}");

    // group add — create group 90 on store 9, replica 900 on node 1.
    let (code, _, stderr) = run(
        &cli,
        g0.mgmt_port,
        &g0.config_path,
        &[
            "kv", "group", "add", "-s", store_id, "-g", group_id, "-r", replica_id, "-n", "1",
        ],
    );
    assert_eq!(code, 0, "group add stderr={stderr}");

    // group list — should contain group 90.
    let (code, stdout, _) = run(
        &cli,
        g0.mgmt_port,
        &g0.config_path,
        &["kv", "group", "list", "-s", store_id],
    );
    assert_eq!(code, 0);
    assert!(stdout.contains(group_id), "stdout={stdout}");

    // group add — a second group 91 on store 9.
    let group_id_2 = "91";
    let replica_id_2 = "910";
    let (code, _, stderr) = run(
        &cli,
        g0.mgmt_port,
        &g0.config_path,
        &[
            "kv",
            "group",
            "add",
            "-s",
            store_id,
            "-g",
            group_id_2,
            "-r",
            replica_id_2,
            "-n",
            "1",
        ],
    );
    assert_eq!(code, 0, "group add 2 stderr={stderr}");

    // group list — should contain both groups.
    let (code, stdout, _) = run(
        &cli,
        g0.mgmt_port,
        &g0.config_path,
        &["kv", "group", "list", "-s", store_id],
    );
    assert_eq!(code, 0);
    assert!(
        stdout.contains(group_id) && stdout.contains(group_id_2),
        "stdout={stdout}"
    );

    // group remove — remove group 91.
    let (code, _, stderr) = run(
        &cli,
        g0.mgmt_port,
        &g0.config_path,
        &["kv", "group", "remove", "-s", store_id, "-g", group_id_2],
    );
    assert_eq!(code, 0, "group remove stderr={stderr}");

    // store remove — remove store 9.
    let (code, _, stderr) = run(
        &cli,
        g0.mgmt_port,
        &g0.config_path,
        &["kv", "store", "remove", "-I", store_id],
    );
    assert_eq!(code, 0, "store remove stderr={stderr}");

    tokio::time::sleep(Duration::from_millis(50)).await;
}
