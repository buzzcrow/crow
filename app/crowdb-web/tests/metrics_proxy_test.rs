// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! R11 web e2e: spawn `crowdb-kv-server` + console web, then drive the
//! metrics proxy routes (`/api/nodes/:id/metrics`,
//! `/api/stores/:sid/groups/:gid/metrics`, `/api/stores/:sid/metrics`).

use std::net::SocketAddr;
use std::time::Duration;

use crowdb_console_shared::clients::http::ServerClient;
use crowdb_console_shared::cluster::NodeHealth;
use crowdb_console_shared::config::{NodeEntry, RackEntry, ServerEntry, ServiceType};
use crowdb_console_shared::lifecycle::{self, crowdb_kv_server_bin, DeployRequest};
use crowdb_console_shared::monitor::{legacy_topology_to_node_stores, NodeRecord};
use crowdb_console_shared::ConsoleConfig;
use crowdb_web::{router, AppState};
use serde_json::{json, Value};

// Suppress unused-import warnings for items used only in spawn_web.
#[allow(unused_imports)]
use crowdb_console_shared::cluster::NodeHealth as _NodeHealth;

fn pick_free_port() -> u16 {
    crowdb_console_shared::test_ports::unique_test_port()
}

struct Upstream {
    pid: u32,
    mgmt_url: String,
    rpc_url: String,
}

impl Drop for Upstream {
    fn drop(&mut self) {
        let _ = lifecycle::stop_pid_with_timeout(self.pid, Duration::from_secs(5));
    }
}

async fn spawn_upstream() -> Option<Upstream> {
    let bin = crowdb_kv_server_bin()?;
    if !bin.exists() {
        return None;
    }
    let node = NodeEntry {
        id: 1,
        rack_id: 1,
        host: "127.0.0.1".into(),
        ssh_port: 22,
        ssh_user: String::new(),
        ssh_key: None,
        ssh_password: None,
    };
    let req = DeployRequest {
        server_id: "n1".into(),
        rest_port: pick_free_port(),
        rpc_port: pick_free_port(),
        election_profile: Some("e2e".into()),
        binary: Some(bin),
        ..Default::default()
    };
    let deployed = lifecycle::deploy_local(&req, &node).await.expect("deploy_local");
    Some(Upstream {
        pid: deployed.pid,
        mgmt_url: deployed.mgmt_url,
        rpc_url: deployed.rpc_url,
    })
}

async fn spawn_web(upstream: &Upstream) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    let mut cfg = ConsoleConfig::default();
    cfg.racks.push(RackEntry {
        id: 1,
        name: "r1".into(),
    });
    cfg.nodes.push(NodeEntry {
        id: 1,
        rack_id: 1,
        host: "127.0.0.1".into(),
        ssh_port: 22,
        ssh_user: String::new(),
        ssh_key: None,
        ssh_password: None,
    });
    cfg.add_server(ServerEntry {
        id: "n1".into(),
        url: upstream.mgmt_url.clone(),
        node_id: Some(1),
        rpc_url: Some(upstream.rpc_url.clone()),
        rest_port: None,
        rpc_port: None,
        auto_start: true,
        binary: None,
        election_profile: None,
        pid: None,
        service_type: ServiceType::Kv,
        rpc_workers: None,
        no_fsync: false,
    })
    .unwrap();
    let state = AppState::with_config(cfg, None);
    // Register the upstream's pid so `refresh_node_cache` (which skips
    // nodes with no tracked runtime pid) refreshes after mutations.
    state.set_runtime_pid(1, upstream.pid);

    // Seed the monitor cache so leader resolution works.
    let client = ServerClient::new(upstream.mgmt_url.clone()).unwrap();
    if let Ok(stores) = client.topology().await {
        let rec = NodeRecord {
            health: NodeHealth::Up,
            last_seen_ms: 1,
            stores: legacy_topology_to_node_stores(1, &stores),
            last_error: None,
            recovering: false,
        };
        state.monitor_cache.set_node_report(1, rec).await;
    }

    tokio::spawn(async move {
        axum::serve(listener, router(state)).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    addr
}

/// Initialize the system group via the web's cluster init, then poll
/// the web's group endpoint until the system group has a leader.
async fn init_cluster(web: &SocketAddr) {
    let http = reqwest::Client::new();
    let base = format!("http://{web}");
    let resp = http
        .post(format!("{base}/api/cluster/init"))
        .json(&json!({"nodes": [1]}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201, "cluster init: {:?}", resp.text().await.ok());

    // Poll the web's group endpoint until the system group has a leader.
    for _ in 0..60 {
        let resp: Value = http
            .get(format!("{base}/api/stores/0/groups/0"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        if let Some(replicas) = resp["replicas"].as_array() {
            if replicas.iter().any(|r| r["role"] == "leader") {
                return;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("system group did not elect a leader in time");
}

#[tokio::test]
async fn node_metrics_proxy_returns_structured_snapshot() {
    let Some(upstream) = spawn_upstream().await else {
        eprintln!("skipping: crowdb-kv-server binary not built");
        return;
    };
    let web = spawn_web(&upstream).await;
    let base = format!("http://{web}");
    let http = reqwest::Client::new();
    init_cluster(&web).await;

    let resp: Value = http
        .get(format!("{base}/api/nodes/1/metrics"))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(resp["window_secs"].as_f64().is_some());
    assert!(resp["timestamp"].is_string());
    let metrics = resp["metrics"].as_array().unwrap();
    assert!(!metrics.is_empty(), "expected non-empty metrics, got: {resp}");
    for m in metrics {
        assert!(m["name"].is_string());
        assert!(m["kind"].is_string());
        assert!(m["fields"].as_array().is_some());
    }
}

#[tokio::test]
async fn group_metrics_proxy_returns_scoped_metrics() {
    let Some(upstream) = spawn_upstream().await else {
        eprintln!("skipping: crowdb-kv-server binary not built");
        return;
    };
    let web = spawn_web(&upstream).await;
    let base = format!("http://{web}");
    let http = reqwest::Client::new();
    init_cluster(&web).await;

    // The system group is store 0, group 0.
    let resp: Value = http
        .get(format!("{base}/api/stores/0/groups/0/metrics"))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let metrics = resp["metrics"].as_array().unwrap();
    assert!(
        !metrics.is_empty(),
        "expected non-empty group metrics, got: {resp}"
    );
    // Every metric name should start with the group prefix `s.0.g.0.`.
    for m in metrics {
        let name = m["name"].as_str().expect("metric name is string");
        assert!(
            name.starts_with("s.0.g.0."),
            "metric {name} does not match group prefix"
        );
    }
}

#[tokio::test]
async fn store_metrics_proxy_aggregates_across_groups() {
    let Some(upstream) = spawn_upstream().await else {
        eprintln!("skipping: crowdb-kv-server binary not built");
        return;
    };
    let web = spawn_web(&upstream).await;
    let base = format!("http://{web}");
    let http = reqwest::Client::new();
    init_cluster(&web).await;

    let resp: Value = http
        .get(format!("{base}/api/stores/0/metrics"))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    let metrics = resp["metrics"].as_array().unwrap();
    assert!(
        !metrics.is_empty(),
        "expected non-empty store metrics, got: {resp}"
    );
    // Every metric name should start with the store prefix `s.0.`.
    for m in metrics {
        let name = m["name"].as_str().expect("metric name is string");
        assert!(
            name.starts_with("s.0."),
            "metric {name} does not match store prefix"
        );
    }
}
