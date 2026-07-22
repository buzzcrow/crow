// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Integration tests for `bench`.
//!
//! These tests run the compiled `crowkv-cli` binary end-to-end:
//! `bench` deploys its own 3-node cluster via an embedded
//! console-web, drives a workload, collects server metrics, and writes
//! a report. The `crowkv-server` binary must be built beforehand.

mod testkit;

use crowkv_console_shared::lifecycle;
use std::sync::{Mutex, OnceLock};
use testkit::console::{crowkv_cli_bin, run};

static BENCH_MUTEX: OnceLock<Mutex<()>> = OnceLock::new();

fn bench_lock() -> std::sync::MutexGuard<'static, ()> {
    BENCH_MUTEX.get_or_init(|| Mutex::new(())).lock().unwrap()
}

/// `bench run --mode mem --duration-secs 3` runs end-to-end,
/// exits 0, and prints a report path. The workspace is cleaned up
/// automatically (no `--keep-workspace`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bench_benchmark_mem_end_to_end() {
    let _lock = bench_lock();
    let cli = crowkv_cli_bin();
    if !cli.exists() {
        eprintln!("skipping: crowkv CLI binary not built ({})", cli.display());
        return;
    }
    if lifecycle::crowkv_server_bin().is_none_or(|p| !p.exists()) {
        eprintln!("skipping: crowkv-server binary not built");
        return;
    }

    let (code, stdout, stderr) = run(
        &cli,
        "127.0.0.1",
        0,
        &[
            "bench",
            "run",
            "--mode",
            "mem",
            "--duration-secs",
            "3",
            "--threads",
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

/// `bench run --mode file --duration-secs 3` runs
/// end-to-end with the crowtree engine + file page store.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bench_benchmark_file_end_to_end() {
    let _lock = bench_lock();
    let cli = crowkv_cli_bin();
    if !cli.exists() {
        eprintln!("skipping: crowkv CLI binary not built ({})", cli.display());
        return;
    }
    if lifecycle::crowkv_server_bin().is_none_or(|p| !p.exists()) {
        eprintln!("skipping: crowkv-server binary not built");
        return;
    }

    let (code, stdout, stderr) = run(
        &cli,
        "127.0.0.1",
        0,
        &[
            "bench",
            "run",
            "--mode",
            "file",
            "--duration-secs",
            "3",
            "--threads",
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
    let cli = crowkv_cli_bin();
    if !cli.exists() {
        eprintln!("skipping: crowkv CLI binary not built ({})", cli.display());
        return;
    }
    if lifecycle::crowkv_server_bin().is_none_or(|p| !p.exists()) {
        eprintln!("skipping: crowkv-server binary not built");
        return;
    }

    let run_id_1 = format!("compare-a-{}", chrono::Utc::now().timestamp_millis());
    let (code, _, stderr) = run(
        &cli,
        "127.0.0.1",
        0,
        &[
            "bench",
            "run",
            "--mode",
            "mem",
            "--duration-secs",
            "2",
            "--threads",
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
            "run",
            "--mode",
            "mem",
            "--duration-secs",
            "2",
            "--threads",
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
