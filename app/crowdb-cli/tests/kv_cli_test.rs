// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! A12 CLI e2e: invoke the compiled `crowdb-cli` binary through a live
//! `crowdb-web` (`--ip` / `--port`) which itself proxies to a real
//! `crowdb-kv-server`. Exercises the `kv put / get / delete / scan` verbs
//! end-to-end and verifies the legacy `--server` flag is no longer
//! required for the four KV verbs.

mod common;

use std::time::Duration;

use common::console::{crowdb_cli_bin, run, spawn_console, spawn_upstream, wait_for_leader};
use crowdb_console_shared::clients::console::ConsoleClient;
use crowdb_console_shared::lifecycle;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "R126: rewrite for direct-to-group-0 CLI"]
#[allow(clippy::too_many_lines)]
async fn kv_put_get_delete_round_trip() {
    let Some(upstream) = spawn_upstream().await else {
        eprintln!("skipping: crowdb-kv-server binary not built");
        return;
    };
    let cli = crowdb_cli_bin();
    if !cli.exists() {
        eprintln!("skipping: crowdb_kv CLI binary not built ({})", cli.display());
        let _ = lifecycle::stop_pid(upstream.pid);
        return;
    }

    let console = spawn_console(&upstream).await;
    let ip = console.ip().to_string();
    let port = console.port();

    let console_client = ConsoleClient::new(format!("http://{ip}:{port}")).unwrap();
    console_client.cluster_init(&[1]).await.expect("cluster_init");

    // Create store 1 / group 1 via the same CLI control path used by the
    // passing bench smoke test, then wait for the single-replica group to
    // report a leader before exercising KV verbs.
    let (code, _, stderr) = run(&cli, &ip, port, &["store", "add", "--store-id", "1"]);
    assert_eq!(code, 0, "store add stderr={stderr}");
    let (code, _, stderr) = run(
        &cli,
        &ip,
        port,
        &[
            "paxos",
            "add",
            "--store-id",
            "1",
            "--group-id",
            "1",
            "--replica-id",
            "1",
            "--nodes",
            "1",
        ],
    );
    assert_eq!(code, 0, "paxos add stderr={stderr}");
    assert!(
        wait_for_leader(&ip, port, 1, 1, Duration::from_secs(15)).await,
        "group 1 never reported a leader"
    );

    // put
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let (code, stdout, stderr) = loop {
        let result = run(
            &cli,
            &ip,
            port,
            &[
                "kv",
                "put",
                "--store-id",
                "1",
                "--group-id",
                "1",
                "--key",
                "color",
                "--value",
                "indigo",
            ],
        );
        if result.0 == 0 || tokio::time::Instant::now() >= deadline {
            break result;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    assert_eq!(code, 0, "stdout={stdout}\nstderr={stderr}");
    assert!(stdout.contains("ok:"));

    // get
    let (code, stdout, _) = run(
        &cli,
        &ip,
        port,
        &[
            "kv",
            "get",
            "--store-id",
            "1",
            "--group-id",
            "1",
            "--key",
            "color",
        ],
    );
    assert_eq!(code, 0);
    assert!(stdout.contains("indigo"), "stdout={stdout}");

    // delete
    let (code, _, stderr) = run(
        &cli,
        &ip,
        port,
        &[
            "kv",
            "delete",
            "--store-id",
            "1",
            "--group-id",
            "1",
            "--key",
            "color",
        ],
    );
    assert_eq!(code, 0, "stderr={stderr}");

    // get → not found returns exit code 3
    let (code, stdout, _) = run(
        &cli,
        &ip,
        port,
        &[
            "kv",
            "get",
            "--store-id",
            "1",
            "--group-id",
            "1",
            "--key",
            "color",
        ],
    );
    assert_eq!(code, 3);
    assert!(stdout.contains("not found"));

    // scan/list now returns real key/value rows. Seed two keys, then
    // scan with a prefix that captures only one of them. Retry the seed
    // puts and the list under the aggressive `test` election profile, whose
    // 25 ms lease can expire during CLI command scheduling.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let (code, _, stderr) = loop {
        let result = run(
            &cli,
            &ip,
            port,
            &[
                "kv",
                "put",
                "--store-id",
                "1",
                "--group-id",
                "1",
                "--key",
                "scan/a",
                "--value",
                "1",
            ],
        );
        if result.0 == 0 || tokio::time::Instant::now() >= deadline {
            break result;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    assert_eq!(code, 0, "stderr={stderr}");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let (code, _, stderr) = loop {
        let result = run(
            &cli,
            &ip,
            port,
            &[
                "kv",
                "put",
                "--store-id",
                "1",
                "--group-id",
                "1",
                "--key",
                "scan/b",
                "--value",
                "2",
            ],
        );
        if result.0 == 0 || tokio::time::Instant::now() >= deadline {
            break result;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    assert_eq!(code, 0, "stderr={stderr}");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let (code, stdout, stderr) = loop {
        let result = run(
            &cli,
            &ip,
            port,
            &[
                "kv",
                "list",
                "--store-id",
                "1",
                "--group-id",
                "1",
                "--prefix",
                "scan/",
            ],
        );
        if result.0 == 0 || tokio::time::Instant::now() >= deadline {
            break result;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    assert_eq!(code, 0, "stderr={stderr}");
    assert!(stdout.contains("scan/a\t1"), "stdout={stdout}");
    assert!(stdout.contains("scan/b\t2"), "stdout={stdout}");

    // Cleanup.
    let _ = lifecycle::stop_pid(upstream.pid);
    tokio::time::sleep(Duration::from_millis(50)).await;
}
