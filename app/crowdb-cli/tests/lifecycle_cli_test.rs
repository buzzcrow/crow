// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! CLI e2e for the physical lifecycle verbs: `rack`, `node`, and
//! `server` round-trips through `--ip` / `--port` against an empty,
//! temp-rooted console (local-fork server deploy).

mod common;

use std::time::Duration;

use common::console::{crowdb_cli_bin, run, spawn_console_empty};
use crowdb_console_shared::lifecycle::crowdb_kv_server_bin;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "R126: rewrite for direct-to-group-0 CLI"]
#[allow(clippy::too_many_lines)]
async fn rack_node_server_lifecycle() {
    let cli = crowdb_cli_bin();
    if !cli.exists() {
        eprintln!("skipping: crowdb_kv CLI binary not built ({})", cli.display());
        return;
    }
    let Some(server_bin) = crowdb_kv_server_bin().filter(|p| p.exists()) else {
        eprintln!("skipping: crowdb-kv-server binary not built");
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
        &["rack", "add", "--id", "1", "--name", "rack-one"],
    );
    assert_eq!(code, 0, "rack add stderr={stderr}");
    let (code, stdout, _) = run(&cli, &ip, port, &["rack", "list"]);
    assert_eq!(code, 0);
    assert!(stdout.contains('1'), "stdout={stdout}");

    // node add (two local-fork nodes) / list / ping
    for node in ["1", "2"] {
        let (code, _, stderr) = run(&cli, &ip, port, &["node", "add", "--id", node, "--rack", "1"]);
        assert_eq!(code, 0, "node add {node} stderr={stderr}");
    }
    let (code, stdout, _) = run(&cli, &ip, port, &["node", "list"]);
    assert_eq!(code, 0);
    assert!(stdout.contains('1') && stdout.contains('2'), "stdout={stdout}");

    // local-fork node ping is a no-op success.
    let (code, stdout, stderr) = run(&cli, &ip, port, &["node", "ping", "1"]);
    assert_eq!(code, 0, "ping stderr={stderr}");
    assert!(stdout.contains("reachable"), "stdout={stdout}");

    // server deploy / list / restart / stop on n1.
    let (rest_port, rpc_port) = common::console::pick_two_distinct_free_ports();
    let rest_port = rest_port.to_string();
    let rpc_port = rpc_port.to_string();
    let (code, stdout, stderr) = run(
        &cli,
        &ip,
        port,
        &[
            "server",
            "deploy",
            "--node",
            "1",
            "--rest-port",
            &rest_port,
            "--rpc-port",
            &rpc_port,
            "--binary",
            &server_bin,
        ],
    );
    assert_eq!(code, 0, "server deploy stderr={stderr}");
    assert!(stdout.contains("deployed server on node 1"), "stdout={stdout}");

    let (code, stdout, stderr) = run(&cli, &ip, port, &["server", "list"]);
    assert_eq!(code, 0, "server list stderr={stderr}");
    assert!(stdout.contains('1'), "stdout={stdout}");

    let (code, _, stderr) = run(&cli, &ip, port, &["server", "restart", "--node", "1"]);
    assert_eq!(code, 0, "server restart stderr={stderr}");

    let (code, _, stderr) = run(&cli, &ip, port, &["server", "stop", "--node", "1"]);
    assert_eq!(code, 0, "server stop stderr={stderr}");

    // node remove on the server-free node succeeds.
    let (code, _, stderr) = run(&cli, &ip, port, &["node", "remove", "--id", "2"]);
    assert_eq!(code, 0, "node remove stderr={stderr}");
    let (code, stdout, _) = run(&cli, &ip, port, &["node", "list"]);
    assert_eq!(code, 0);
    assert!(
        !stdout.contains("2                 1             127.0.0.1"),
        "node 2 should be gone: stdout={stdout}"
    );

    tokio::time::sleep(Duration::from_millis(50)).await;
    let _ = std::fs::remove_dir_all(dir);
}
