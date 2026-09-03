// Copyright 2026-present Gian <crow.db@outlook.com>

//! CLI e2e for the KV data-plane: `kv data put / get / delete / scan`
//! through `--sysmd-ip` / `--sysmd-port` / `--config` against a real
//! `crowdb-kv-server` with group 0 initialized.

mod common;

use std::time::Duration;

use common::direct::{crowdb_cli_bin, run, spawn_group0};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn kv_put_get_delete_round_trip() {
    let Some(g0) = spawn_group0().await else {
        eprintln!("skipping: crowdb-kv-server binary not built");
        return;
    };
    let cli = crowdb_cli_bin();
    if !cli.exists() {
        eprintln!("skipping: crowdb-cli binary not built ({})", cli.display());
        return;
    }

    // put on group 0 (system store).
    let (code, stdout, stderr) = run(
        &cli,
        g0.mgmt_port,
        &g0.config_path,
        &[
            "kv", "data", "put", "-s", "0", "-g", "0", "-k", "color", "-v", "indigo",
        ],
    );
    assert_eq!(code, 0, "put stdout={stdout}\nstderr={stderr}");

    // get — should find the value.
    let (code, stdout, _) = run(
        &cli,
        g0.mgmt_port,
        &g0.config_path,
        &["kv", "data", "get", "-s", "0", "-g", "0", "-k", "color"],
    );
    assert_eq!(code, 0);
    assert!(stdout.contains("indigo"), "stdout={stdout}");

    // delete.
    let (code, _, stderr) = run(
        &cli,
        g0.mgmt_port,
        &g0.config_path,
        &["kv", "data", "delete", "-s", "0", "-g", "0", "-k", "color"],
    );
    assert_eq!(code, 0, "stderr={stderr}");

    // get → not found returns exit code 0 with "not found" message.
    let (code, stdout, _) = run(
        &cli,
        g0.mgmt_port,
        &g0.config_path,
        &["kv", "data", "get", "-s", "0", "-g", "0", "-k", "color"],
    );
    assert_eq!(code, 0, "stdout={stdout}");
    assert!(stdout.contains("not found"), "stdout={stdout}");

    // scan: seed two keys with a prefix, then scan.
    let (code, _, stderr) = run(
        &cli,
        g0.mgmt_port,
        &g0.config_path,
        &[
            "kv", "data", "put", "-s", "0", "-g", "0", "-k", "scan/a", "-v", "1",
        ],
    );
    assert_eq!(code, 0, "stderr={stderr}");

    let (code, _, stderr) = run(
        &cli,
        g0.mgmt_port,
        &g0.config_path,
        &[
            "kv", "data", "put", "-s", "0", "-g", "0", "-k", "scan/b", "-v", "2",
        ],
    );
    assert_eq!(code, 0, "stderr={stderr}");

    let (code, stdout, stderr) = run(
        &cli,
        g0.mgmt_port,
        &g0.config_path,
        &["kv", "data", "scan", "-s", "0", "-g", "0", "-P", "scan/"],
    );
    assert_eq!(code, 0, "stderr={stderr}");
    assert!(stdout.contains("scan/a"), "stdout={stdout}");
    assert!(stdout.contains("scan/b"), "stdout={stdout}");

    tokio::time::sleep(Duration::from_millis(50)).await;
}
