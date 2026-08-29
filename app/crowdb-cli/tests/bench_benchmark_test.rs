// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Integration tests for `bench`.
//!
//! These tests run the compiled `crowdb-cli` binary end-to-end:
//! `bench` deploys its own 3-node cluster via an embedded
//! console-web, drives a workload, collects server metrics, and writes
//! a report. The `crowdb-kv-server` binary must be built beforehand.

mod common;

use common::console::{crowdb_cli_bin, crowdb_rpc_fb_server_bin, run};
use crowdb_console_shared::lifecycle;
use std::sync::{Mutex, OnceLock};

static BENCH_MUTEX: OnceLock<Mutex<()>> = OnceLock::new();

fn bench_lock() -> std::sync::MutexGuard<'static, ()> {
    BENCH_MUTEX
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// `bench kv --mode mem --duration-secs 1` runs end-to-end,
/// exits 0, and prints a report path.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bench_benchmark_mem_end_to_end() {
    let _lock = bench_lock();
    let cli = crowdb_cli_bin();
    if !cli.exists() {
        eprintln!("skipping: crowdb_kv CLI binary not built ({})", cli.display());
        return;
    }
    if lifecycle::crowdb_kv_server_bin().is_none_or(|p| !p.exists()) {
        eprintln!("skipping: crowdb-kv-server binary not built");
        return;
    }

    let (code, stdout, stderr) = run(
        &cli,
        "127.0.0.1",
        0,
        &[
            "bench",
            "kv",
            "--mode",
            "mem",
            "--duration-secs",
            "1",
            "--loader-num",
            "2",
            "--connections",
            "2",
            "--key-space",
            "100",
            "--value-size",
            "32",
        ],
    );
    assert_eq!(code, 0, "stdout={stdout}\nstderr={stderr}");
    assert!(stdout.contains("report (json):"), "stdout={stdout}");
    assert!(stdout.contains("total_ops"), "stdout={stdout}");
}

/// `bench kv --mode file --duration-secs 1` runs
/// end-to-end with the crowdb-tree engine + file page store.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bench_benchmark_file_end_to_end() {
    let _lock = bench_lock();
    let cli = crowdb_cli_bin();
    if !cli.exists() {
        eprintln!("skipping: crowdb_kv CLI binary not built ({})", cli.display());
        return;
    }
    if lifecycle::crowdb_kv_server_bin().is_none_or(|p| !p.exists()) {
        eprintln!("skipping: crowdb-kv-server binary not built");
        return;
    }

    let (code, stdout, stderr) = run(
        &cli,
        "127.0.0.1",
        0,
        &[
            "bench",
            "kv",
            "--mode",
            "file",
            "--duration-secs",
            "1",
            "--loader-num",
            "2",
            "--connections",
            "2",
            "--key-space",
            "100",
            "--value-size",
            "32",
        ],
    );
    assert_eq!(code, 0, "stdout={stdout}\nstderr={stderr}");
    assert!(stdout.contains("report (json):"), "stdout={stdout}");
}

/// `bench compare` prints a comparison table when two valid report
/// JSON files exist. We generate two quick reports first, then compare.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bench_compare_two_reports() {
    let _lock = bench_lock();
    let cli = crowdb_cli_bin();
    if !cli.exists() {
        eprintln!("skipping: crowdb_kv CLI binary not built ({})", cli.display());
        return;
    }
    if lifecycle::crowdb_kv_server_bin().is_none_or(|p| !p.exists()) {
        eprintln!("skipping: crowdb-kv-server binary not built");
        return;
    }

    let run_id_1 = format!("compare-a-{}", chrono::Utc::now().timestamp_millis());
    let (code, _, stderr) = run(
        &cli,
        "127.0.0.1",
        0,
        &[
            "bench",
            "kv",
            "--mode",
            "mem",
            "--duration-secs",
            "1",
            "--loader-num",
            "1",
            "--connections",
            "1",
            "--key-space",
            "50",
            "--value-size",
            "16",
            "--run-id",
            &run_id_1,
        ],
    );
    assert_eq!(code, 0, "first benchmark stderr={stderr}");

    let run_id_2 = format!("compare-b-{}", chrono::Utc::now().timestamp_millis());
    let (code, _, stderr) = run(
        &cli,
        "127.0.0.1",
        0,
        &[
            "bench",
            "kv",
            "--mode",
            "mem",
            "--duration-secs",
            "1",
            "--loader-num",
            "1",
            "--connections",
            "1",
            "--key-space",
            "50",
            "--value-size",
            "16",
            "--run-id",
            &run_id_2,
        ],
    );
    assert_eq!(code, 0, "second benchmark stderr={stderr}");

    let (code, stdout, stderr) = run(&cli, "127.0.0.1", 0, &["bench", "compare", &run_id_1, &run_id_2]);
    assert_eq!(code, 0, "compare stderr={stderr}");
    assert!(stdout.contains("metric"), "stdout={stdout}");
    assert!(stdout.contains(&run_id_1), "stdout={stdout}");
    assert!(stdout.contains(&run_id_2), "stdout={stdout}");
}

/// `bench rpc --duration-secs 1` runs the 2-process RPC echo
/// benchmark end-to-end. The test starts the fb server as a child
/// process, runs the bench, then kills the server.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bench_benchmark_rpc_end_to_end() {
    let _lock = bench_lock();
    let cli = crowdb_cli_bin();
    if !cli.exists() {
        eprintln!("skipping: crowdb_kv CLI binary not built ({})", cli.display());
        return;
    }
    let Some(fb_server) = crowdb_rpc_fb_server_bin() else {
        eprintln!("skipping: crowdb-rpc-fb-server binary not built (run `pixi run build-cpp`)");
        return;
    };
    if !fb_server.exists() {
        eprintln!("skipping: crowdb-rpc-fb-server binary not built (run `pixi run build-cpp`)");
        return;
    }

    // Start the fb server on port 18080 (the bench default).
    let mut fb = std::process::Command::new(&fb_server);
    fb.arg("--port=18080")
        .arg("--io_workers=2")
        .arg("--logdir=/tmp/bench-rpc-test")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());
    let mut fb_child = match fb.spawn() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("skipping: failed to start crowdb-rpc-fb-server: {e}");
            return;
        }
    };
    // Wait for the server to bind.
    let mut ready = false;
    for _ in 0..50 {
        if std::net::TcpStream::connect("127.0.0.1:18080").is_ok() {
            ready = true;
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    if !ready {
        eprintln!("skipping: crowdb-rpc-fb-server did not bind on port 18080");
        let _ = fb_child.kill();
        return;
    }

    let (code, stdout, stderr) = run(
        &cli,
        "127.0.0.1",
        0,
        &[
            "bench",
            "rpc",
            "--duration-secs",
            "1",
            "--loader-num",
            "2",
            "--connections",
            "2",
            "--value-size",
            "64",
        ],
    );
    let _ = fb_child.kill();
    let _ = fb_child.wait();

    assert_eq!(code, 0, "stdout={stdout}\nstderr={stderr}");
    assert!(stdout.contains("report (json):"), "stdout={stdout}");
    assert!(stdout.contains("target          : rpc"), "stdout={stdout}");
    assert!(stdout.contains("total_ops"), "stdout={stdout}");
    // RPC echo should have zero errors.
    assert!(stdout.contains("total_errors    : 0"), "stdout={stdout}");
}
