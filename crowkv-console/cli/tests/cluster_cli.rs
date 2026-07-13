// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! CLI e2e for cluster observation: `cluster status / topology /
//! inspect` route entirely through `--console` (no `--server`), against
//! a web-managed cluster with no persisted registry.

mod testkit;

use std::time::Duration;

use crowkv_console_shared::lifecycle;
use testkit::console::{crowkv_cli_bin, run, spawn_console, spawn_upstream, wait_for_leader};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn cluster_status_topology_inspect_via_console() {
    let Some(upstream) = spawn_upstream().await else {
        eprintln!("skipping: crowkv-server binary not built");
        return;
    };
    let cli = crowkv_cli_bin();
    if !cli.exists() {
        eprintln!("skipping: crowkv CLI binary not built ({})", cli.display());
        let _ = lifecycle::stop_pid(upstream.pid);
        return;
    }

    let console = spawn_console(&upstream).await;
    let console_url = format!("http://{console}");

    // Seed a store + group through the CLI so the logical tree is non-empty.
    let (code, _, stderr) = run(&cli, &console_url, &["store", "add", "--store-id", "9"]);
    assert_eq!(code, 0, "store add stderr={stderr}");
    let (code, _, stderr) = run(
        &cli,
        &console_url,
        &[
            "paxos",
            "add",
            "--store-id",
            "9",
            "--group-id",
            "90",
            "--replica-id",
            "900",
            "--nodes",
            "n1",
        ],
    );
    assert_eq!(code, 0, "paxos add stderr={stderr}");
    let _ = wait_for_leader(&console_url, 9, 90, Duration::from_secs(15)).await;

    // status — summarises servers + store/group counts.
    let (code, stdout, stderr) = run(&cli, &console_url, &["cluster", "status"]);
    assert_eq!(code, 0, "status stderr={stderr}");
    assert!(stdout.contains("servers:"), "stdout={stdout}");
    assert!(stdout.contains("n1"), "stdout={stdout}");
    assert!(stdout.contains("stores: 1"), "stdout={stdout}");

    // topology — logical + physical sections.
    let (code, stdout, stderr) = run(&cli, &console_url, &["cluster", "topology"]);
    assert_eq!(code, 0, "topology stderr={stderr}");
    assert!(stdout.contains("logical:"), "stdout={stdout}");
    assert!(stdout.contains("store 9"), "stdout={stdout}");
    assert!(stdout.contains("group 90"), "stdout={stdout}");
    assert!(stdout.contains("physical:"), "stdout={stdout}");
    assert!(stdout.contains("node n1"), "stdout={stdout}");

    // inspect node (bare token → node id).
    let (code, stdout, stderr) = run(&cli, &console_url, &["cluster", "inspect", "n1"]);
    assert_eq!(code, 0, "inspect node stderr={stderr}");
    assert!(stdout.contains("mgmt_url:"), "stdout={stdout}");

    // inspect store / group via the s.../g... id grammar.
    let (code, stdout, stderr) = run(&cli, &console_url, &["cluster", "inspect", "s9"]);
    assert_eq!(code, 0, "inspect store stderr={stderr}");
    assert!(stdout.contains("store 9"), "stdout={stdout}");

    let (code, stdout, stderr) = run(&cli, &console_url, &["cluster", "inspect", "s9/g90"]);
    assert_eq!(code, 0, "inspect group stderr={stderr}");
    assert!(stdout.contains("group 90"), "stdout={stdout}");

    // --json status decodes as an object with a servers array.
    let (code, stdout, _) = run(&cli, &console_url, &["--json", "cluster", "status"]);
    assert_eq!(code, 0);
    let parsed: serde_json::Value = serde_json::from_str(&stdout).expect("json status");
    assert!(parsed["servers"].is_array(), "json={stdout}");

    let _ = lifecycle::stop_pid(upstream.pid);
    tokio::time::sleep(Duration::from_millis(50)).await;
}
