// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! A8 web e2e: spawn `crow-kv-server` + console web, then drive
//! `/api/stores/{sid}/groups/{gid}/kv/{put,get,delete}` over HTTP.
//! Leader resolution now uses the monitor cache (no `?server=`).

use std::net::SocketAddr;
use std::time::Duration;

use crow_console_shared::clients::http::ServerClient;
use crow_console_shared::cluster::NodeHealth;
use crow_console_shared::config::{NodeEntry, RackEntry, ServerEntry};
use crow_console_shared::lifecycle::{self, crow_kv_server_bin, DeployRequest};
use crow_console_shared::monitor::{legacy_topology_to_node_stores, NodeRecord};
use crow_console_shared::ConsoleConfig;
use crow_web::{router, AppState};
use serde_json::json;

fn pick_free_port() -> u16 {
    crow_console_shared::test_ports::unique_test_port()
}

struct Upstream {
    pid: u32,
    mgmt_url: String,
    grpc_url: String,
}

impl Drop for Upstream {
    fn drop(&mut self) {
        let _ = lifecycle::stop_pid_with_timeout(self.pid, Duration::from_secs(5));
    }
}

async fn spawn_upstream() -> Option<Upstream> {
    let bin = crow_kv_server_bin()?;
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
        server_id: "n1".to_string(),
        mgmt_port: pick_free_port(),
        grpc_port: pick_free_port(),
        election_profile: Some("e2e".into()),
        binary: Some(bin),
        ..Default::default()
    };
    let deployed = lifecycle::deploy_local(&req, &node).await.expect("deploy_local");
    Some(Upstream {
        pid: deployed.pid,
        mgmt_url: deployed.mgmt_url,
        grpc_url: deployed.grpc_url,
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
        id: "n1".to_string(),
        url: upstream.mgmt_url.clone(),
        node_id: Some(1),
        grpc_url: Some(upstream.grpc_url.clone()),
        mgmt_port: None,
        grpc_port: None,
        auto_start: true,
        binary: None,
        election_profile: None,
        pid: None,
    })
    .unwrap();
    let state = AppState::with_config(cfg, None);

    // Seed the monitor cache so leader resolution works.
    let client = ServerClient::new(upstream.mgmt_url.clone()).unwrap();
    if let Ok(stores) = client.topology().await {
        let rec = NodeRecord {
            health: NodeHealth::Up,
            last_seen_ms: 1,
            stores: legacy_topology_to_node_stores(1, &stores),
            last_error: None,
        };
        state.monitor_cache.set_node_report(1, rec).await;
    }

    tokio::spawn(async move {
        axum::serve(listener, router(state)).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    addr
}

#[tokio::test]
async fn kv_put_get_delete_through_web_routes() {
    let Some(upstream) = spawn_upstream().await else {
        eprintln!("skipping: crow-kv-server binary not built");
        return;
    };
    let web = spawn_web(&upstream).await;
    let base = format!("http://{web}");
    let http = reqwest::Client::new();

    // Initialize the system group so non-zero stores can be created.
    let resp = http
        .post(format!("{base}/api/cluster/init"))
        .json(&json!({"nodes": [1]}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201, "cluster init: {:?}", resp.text().await.ok());

    // Create store 1 and group 1 (stores no longer auto-create groups).
    let store_resp = http
        .post(format!("{base}/api/stores"))
        .json(&json!({"store_id": 1, "nodes": [1]}))
        .send()
        .await
        .expect("add_store");
    assert_eq!(store_resp.status(), 201, "add_store failed");
    let group_resp = http
        .post(format!("{base}/api/stores/1/groups"))
        .json(&json!({"group_id": 1, "replica_id": 1, "nodes": [1]}))
        .send()
        .await
        .expect("add_group");
    assert_eq!(group_resp.status(), 201, "add_group failed");

    let url = format!("{base}/api/stores/1/groups/1/kv");

    // PUT
    let resp = http
        .post(format!("{url}/put"))
        .json(&json!({"key": "alpha", "value": "beta"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "{:?}", resp.text().await.ok());

    // GET → found
    let resp = http.get(format!("{url}/get?key=alpha")).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["found"], true);
    assert_eq!(body["value_utf8"], "beta");

    // DELETE
    let resp = http
        .post(format!("{url}/delete"))
        .json(&json!({"key": "alpha"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // GET → not found
    let resp = http.get(format!("{url}/get?key=alpha")).send().await.unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["found"], false);

    // PUT with binary value via hex.
    let resp = http
        .post(format!("{url}/put"))
        .json(&json!({"key_hex": "ff00ff", "value_hex": "deadbeef"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let resp = http
        .get(format!("{url}/get?key_hex=ff00ff"))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["found"], true);
    assert_eq!(body["value_hex"], "deadbeef");
}

#[tokio::test]
async fn kv_get_returns_502_when_leader_unreachable() {
    use crow_console_shared::cluster::{LocalReplicaInfo, NodeGroup, NodeStore, ReplicaRole, ReplicaState};
    use std::collections::BTreeMap;

    // Pick a free port, drop the listener: nothing accepts on it now.
    let dead = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let dead_port = dead.local_addr().unwrap().port();
    drop(dead);

    let listener = tokio::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .await
        .unwrap();
    let web = listener.local_addr().unwrap();

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
    // The node has a configured grpc_url, but the port is dead.
    cfg.add_server(ServerEntry {
        id: "n1".to_string(),
        url: format!("http://127.0.0.1:{dead_port}"),
        node_id: Some(1),
        grpc_url: Some(format!("http://127.0.0.1:{dead_port}")),
        mgmt_port: None,
        grpc_port: None,
        auto_start: true,
        binary: None,
        election_profile: None,
        pid: None,
    })
    .unwrap();
    let state = AppState::with_config(cfg, None);

    // Seed a fake group on n1 with a leader hint, so resolve_kv_endpoint
    // returns Ok(grpc_url) and the handler proceeds to connect.
    let mut stores = BTreeMap::new();
    stores.insert(
        7,
        NodeStore {
            node_id: 1,
            store_id: 7,
            listen_addr: None,
            groups: vec![NodeGroup {
                node_id: 1,
                store_id: 7,
                group_id: 70,
                local: LocalReplicaInfo {
                    replica_id: 1,
                    role: ReplicaRole::Leader,
                    state: ReplicaState::Running,
                    engine_healthy: true,
                    crowtree_stats: None,
                    election: None,
                },
                remotes: vec![],
                leader_hint: Some(1),
                read_state: None,
            }],
        },
    );
    let rec = NodeRecord {
        health: NodeHealth::Up,
        last_seen_ms: 1,
        stores,
        last_error: None,
    };
    state.monitor_cache.set_node_report(1, rec).await;

    tokio::spawn(async move {
        axum::serve(listener, router(state)).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    let http = reqwest::Client::new();
    let url = format!("http://{web}/api/stores/7/groups/70/kv/get?key=anything");
    let resp = http.get(&url).send().await.unwrap();
    assert_eq!(
        resp.status(),
        502,
        "expected 502 when leader gRPC port is dead, got {}",
        resp.status()
    );
}
