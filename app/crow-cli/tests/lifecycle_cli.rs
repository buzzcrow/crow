// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! CLI e2e for the physical lifecycle verbs: `rack`, `node`, and
//! `server` round-trips through `--ip` / `--port` against an empty,
//! temp-rooted console (local-fork server deploy).

mod testkit;

use std::time::Duration;

use crow_console_shared::lifecycle::crow_kv_server_bin;
use testkit::console::{crow_cli_bin, run, spawn_console_empty};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn rack_node_server_lifecycle() {
    let cli = crow_cli_bin();
    if !cli.exists() {
        eprintln!("skipping: crow_kv CLI binary not built ({})", cli.display());
        return;
    }
    let Some(server_bin) = crow_kv_server_bin().filter(|p| p.exists()) else {
        eprintln!("skipping: crow-kv-server binary not built");
        return;
    };
    let server_bin = server_bin.to_string_lossy().into_owned();

    let (console, dir) = spawn_console_empty().await;
    let ip = console.ip().to_string();
    let port = console.port();

    // rack add / list
    let (code, _, stderr) = run(
        &cli,
        &ip,
        port,
        &["rack", "add", "--id", "r1", "--name", "rack-one"],
    );
    assert_eq!(code, 0, "rack add stderr={stderr}");
    let (code, stdout, _) = run(&cli, &ip, port, &["rack", "list"]);
    assert_eq!(code, 0);
    assert!(stdout.contains("r1"), "stdout={stdout}");

    // node add (two local-fork nodes) / list / ping
    for node in ["n1", "n2"] {
        let (code, _, stderr) = run(&cli, &ip, port, &["node", "add", "--id", node, "--rack", "r1"]);
        assert_eq!(code, 0, "node add {node} stderr={stderr}");
    }
    let (code, stdout, _) = run(&cli, &ip, port, &["node", "list"]);
    assert_eq!(code, 0);
    assert!(stdout.contains("n1") && stdout.contains("n2"), "stdout={stdout}");

    // local-fork node ping is a no-op success.
    let (code, stdout, stderr) = run(&cli, &ip, port, &["node", "ping", "n1"]);
    assert_eq!(code, 0, "ping stderr={stderr}");
    assert!(stdout.contains("reachable"), "stdout={stdout}");

    // server deploy / list / restart / stop on n1.
    let (mgmt_port, grpc_port) = testkit::console::pick_two_distinct_free_ports();
    let mgmt_port = mgmt_port.to_string();
    let grpc_port = grpc_port.to_string();
    let (code, stdout, stderr) = run(
        &cli,
        &ip,
        port,
        &[
            "server",
            "deploy",
            "--node",
            "n1",
            "--mgmt-port",
            &mgmt_port,
            "--grpc-port",
            &grpc_port,
            "--binary",
            &server_bin,
        ],
    );
    assert_eq!(code, 0, "server deploy stderr={stderr}");
    assert!(stdout.contains("deployed server on node n1"), "stdout={stdout}");

    let (code, stdout, stderr) = run(&cli, &ip, port, &["server", "list"]);
    assert_eq!(code, 0, "server list stderr={stderr}");
    assert!(stdout.contains("n1"), "stdout={stdout}");

    let (code, _, stderr) = run(&cli, &ip, port, &["server", "restart", "--node", "n1"]);
    assert_eq!(code, 0, "server restart stderr={stderr}");

    let (code, _, stderr) = run(&cli, &ip, port, &["server", "stop", "--node", "n1"]);
    assert_eq!(code, 0, "server stop stderr={stderr}");

    // node remove on the server-free node succeeds.
    let (code, _, stderr) = run(&cli, &ip, port, &["node", "remove", "--id", "n2"]);
    assert_eq!(code, 0, "node remove stderr={stderr}");
    let (code, stdout, _) = run(&cli, &ip, port, &["node", "list"]);
    assert_eq!(code, 0);
    assert!(!stdout.contains("n2"), "n2 should be gone: stdout={stdout}");

    tokio::time::sleep(Duration::from_millis(50)).await;
    let _ = std::fs::remove_dir_all(dir);
}
