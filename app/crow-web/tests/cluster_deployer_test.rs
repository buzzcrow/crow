// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Integration tests for `CrowClusterDeployer` — verifies the full
//! cluster lifecycle (start/stop/reset) is fast and correct against
//! an embedded `crow-web` instance with real `crow-kv-server`
//! processes. Skips silently if the binary is not available.

use std::path::PathBuf;
use std::time::{Duration, Instant};

use crow_console_shared::cluster_deployer::{simple, CrowClusterDeployer};
use crow_console_shared::lifecycle::crow_kv_server_bin;
use crow_console_shared::ConsoleConfig;
use crow_web::{router, AppState};

fn tempdir(tag: &str) -> PathBuf {
    let base = std::env::temp_dir().join("crow-deployer-tests");
    let _ = std::fs::create_dir_all(&base);
    let pid = std::process::id();
    base.join(format!("{tag}-{pid}"))
}

async fn spawn_web(cfg_path: PathBuf) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    let cfg = ConsoleConfig::load(&cfg_path).unwrap_or_default();
    let state = AppState::with_config(cfg, Some(cfg_path));
    tokio::spawn(async move {
        let _ = axum::serve(listener, router(state)).await;
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    format!("http://{addr}")
}

fn kv_server_available() -> bool {
    crow_kv_server_bin().is_some_and(|p| p.exists())
}

#[tokio::test]
async fn deployer_start_stop_reset_cycle() {
    if !kv_server_available() {
        eprintln!("skipping: crow-kv-server binary not found");
        return;
    }
    let cfg_path = tempdir("cycle");
    let base_url = spawn_web(cfg_path).await;

    let mut deployer = CrowClusterDeployer::new(&base_url).expect("deployer");

    // Start: 3 nodes, 1 store, 1 group.
    let start = Instant::now();
    deployer.start(&simple()).await.expect("start");
    let start_ms = start.elapsed().as_millis();
    eprintln!("start took {start_ms}ms");

    // Verify cluster info.
    let info = deployer.info().expect("info after start");
    assert_eq!(info.nodes.len(), 3);
    assert_eq!(info.stores.len(), 1);
    assert_eq!(info.stores[0].groups.len(), 1);
    let group = &info.stores[0].groups[0];
    assert!(group.leader_node_id.is_some(), "leader elected");
    assert!(group.leader_endpoint.is_some(), "leader endpoint resolved");

    // Stop: should be fast.
    let stop = Instant::now();
    deployer.stop().await;
    let stop_ms = stop.elapsed().as_millis();
    eprintln!("stop took {stop_ms}ms");
    assert!(stop_ms < 5_000, "stop should be <5s, took {stop_ms}ms");

    // Reset after stop: should be very fast (no servers running).
    let reset = Instant::now();
    deployer.reset().await.expect("reset after stop");
    let reset_ms = reset.elapsed().as_millis();
    eprintln!("reset after stop took {reset_ms}ms");
    assert!(
        reset_ms < 1_000,
        "reset after stop should be <1s, took {reset_ms}ms"
    );
}

#[tokio::test]
async fn deployer_repeated_cycles_no_state_leakage() {
    if !kv_server_available() {
        eprintln!("skipping: crow-kv-server binary not found");
        return;
    }
    let cfg_path = tempdir("repeat");
    let base_url = spawn_web(cfg_path).await;

    let mut deployer = CrowClusterDeployer::new(&base_url).expect("deployer");

    for cycle in 0..3 {
        let start = Instant::now();
        deployer.start(&simple()).await.expect("start in cycle {cycle}");
        let start_ms = start.elapsed().as_millis();
        eprintln!("cycle {cycle}: start took {start_ms}ms");

        let info = deployer.info().expect("info");
        assert_eq!(info.nodes.len(), 3, "cycle {cycle}: 3 nodes");

        let teardown = Instant::now();
        deployer.teardown().await.expect("teardown in cycle {cycle}");
        let teardown_ms = teardown.elapsed().as_millis();
        eprintln!("cycle {cycle}: teardown took {teardown_ms}ms");
    }
}

#[tokio::test]
async fn deployer_reset_on_empty_is_fast() {
    let cfg_path = tempdir("empty-reset");
    let base_url = spawn_web(cfg_path).await;

    let deployer = CrowClusterDeployer::new(&base_url).expect("deployer");

    let reset = Instant::now();
    deployer.reset().await.expect("reset on empty");
    let reset_ms = reset.elapsed().as_millis();
    eprintln!("reset on empty took {reset_ms}ms");
    assert!(
        reset_ms < 500,
        "reset on empty should be <500ms, took {reset_ms}ms"
    );
}
