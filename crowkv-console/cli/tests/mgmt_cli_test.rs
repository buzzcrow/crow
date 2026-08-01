// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! A12 CLI e2e: invoke the compiled `crowkv-cli` binary through a live
//! `crowkv-web` (`--ip` / `--port`), which itself proxies to a real
//! `crowkv-server`. Exercises the `store / paxos / replica` verbs
//! end-to-end and confirms the CLI no longer needs `--server`.

mod testkit;

use std::time::Duration;

use crowkv_console_shared::clients::console::ConsoleClient;
use crowkv_console_shared::lifecycle;
use testkit::console::{crowkv_cli_bin, run, spawn_console, spawn_upstream};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn store_paxos_replica_round_trip() {
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
    let ip = console.ip().to_string();
    let port = console.port();

    let console_client = ConsoleClient::new(format!("http://{ip}:{port}")).unwrap();
    console_client
        .cluster_init(&["n1".to_string()])
        .await
        .expect("cluster_init");

    let store_id = "9";
    let group_id = "90";
    let replica_id = "900";

    // store add (orchestrated; single node available, console picks n1)
    // Note: stores no longer auto-create groups; groups must be added separately via paxos add
    let (code, stdout, stderr) = run(&cli, &ip, port, &["store", "add", "--store-id", store_id]);
    assert_eq!(code, 0, "stdout={stdout}\nstderr={stderr}");
    assert!(stdout.contains("added store"));

    // store list
    let (code, stdout, _) = run(&cli, &ip, port, &["store", "list"]);
    assert_eq!(code, 0);
    assert!(stdout.contains(store_id));

    // paxos add (create the first group on n1)
    let (code, _, stderr) = run(
        &cli,
        &ip,
        port,
        &[
            "paxos",
            "add",
            "--store-id",
            store_id,
            "--group-id",
            group_id,
            "--replica-id",
            replica_id,
            "--nodes",
            "n1",
        ],
    );
    assert_eq!(code, 0, "stderr={stderr}");

    // store inspect should now show the group
    let (code, stdout, _) = run(&cli, &ip, port, &["store", "inspect", "--store-id", store_id]);
    assert_eq!(code, 0);
    assert!(stdout.contains(group_id));

    // paxos add (a second group on n1)
    let group_id_2 = "91";
    let replica_id_2 = "910";
    let (code, _, stderr) = run(
        &cli,
        &ip,
        port,
        &[
            "paxos",
            "add",
            "--store-id",
            store_id,
            "--group-id",
            group_id_2,
            "--replica-id",
            replica_id_2,
            "--nodes",
            "n1",
        ],
    );
    assert_eq!(code, 0, "stderr={stderr}");

    // paxos list
    let (code, stdout, _) = run(&cli, &ip, port, &["paxos", "list", "--store-id", store_id]);
    assert_eq!(code, 0);
    assert!(stdout.contains(group_id_2));

    // paxos inspect (logical view: lists replicas on each node)
    let (code, stdout, _) = run(
        &cli,
        &ip,
        port,
        &[
            "paxos",
            "inspect",
            "--store-id",
            store_id,
            "--group-id",
            group_id_2,
        ],
    );
    assert_eq!(code, 0);
    assert!(stdout.contains("n1"), "stdout={stdout}");

    // paxos remove
    let (code, _, stderr) = run(
        &cli,
        &ip,
        port,
        &[
            "paxos",
            "remove",
            "--store-id",
            store_id,
            "--group-id",
            group_id_2,
        ],
    );
    assert_eq!(code, 0, "stderr={stderr}");

    // Tear down.
    let _ = lifecycle::stop_pid(upstream.pid);
    tokio::time::sleep(Duration::from_millis(50)).await;
}
