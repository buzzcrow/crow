// Copyright 2026-present Gian <crow.db@outlook.com>.

//! Integration tests for `bench`.
//!
//! The `bench` subcommand is a Phase 3 stub (not yet wired to ops).
//! These tests are ignored until the bench lifecycle (deploy/prepare/
//! run/teardown/clean) is re-implemented on the direct-to-group-0 CLI.

mod common;

use common::direct::{crowdb_cli_bin, run, spawn_group0};

/// `bench kv read` stub returns exit code 1.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "Phase 3: bench ops not yet implemented"]
async fn bench_kv_read_stub_returns_error() {
    let Some(g0) = spawn_group0().await else {
        eprintln!("skipping: crowdb-kv-server binary not built");
        return;
    };
    let cli = crowdb_cli_bin();
    if !cli.exists() {
        eprintln!("skipping: crowdb-cli binary not built ({})", cli.display());
        return;
    }

    let (code, _, stderr) = run(&cli, g0.mgmt_port, &g0.config_path, &["bench", "kv", "read"]);
    assert_eq!(code, 1, "bench kv read should fail (stub): stderr={stderr}");
}

/// `bench kv write` stub returns exit code 1.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "Phase 3: bench ops not yet implemented"]
async fn bench_kv_write_stub_returns_error() {
    let Some(g0) = spawn_group0().await else {
        eprintln!("skipping: crowdb-kv-server binary not built");
        return;
    };
    let cli = crowdb_cli_bin();
    if !cli.exists() {
        eprintln!("skipping: crowdb-cli binary not built ({})", cli.display());
        return;
    }

    let (code, _, stderr) = run(&cli, g0.mgmt_port, &g0.config_path, &["bench", "kv", "write"]);
    assert_eq!(code, 1, "bench kv write should fail (stub): stderr={stderr}");
}

/// `bench rpc` stub returns exit code 1.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "Phase 3: bench ops not yet implemented"]
async fn bench_rpc_stub_returns_error() {
    let Some(g0) = spawn_group0().await else {
        eprintln!("skipping: crowdb-kv-server binary not built");
        return;
    };
    let cli = crowdb_cli_bin();
    if !cli.exists() {
        eprintln!("skipping: crowdb-cli binary not built ({})", cli.display());
        return;
    }

    let (code, _, stderr) = run(&cli, g0.mgmt_port, &g0.config_path, &["bench", "rpc"]);
    assert_eq!(code, 1, "bench rpc should fail (stub): stderr={stderr}");
}
