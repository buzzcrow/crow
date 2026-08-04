// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! A5/A6 web e2e: spin up a real `crow-kv-server` plus the console web
//! backend, then exercise the orchestrated `/api/stores` and
//! `/api/stores/:sid/groups` routes through HTTP. Skips silently when
//! the `crow-kv-server` binary is not built.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use crow_console_shared::config::{NodeEntry, RackEntry, ServerEntry};
use crow_console_shared::lifecycle::{self, crow_kv_server_bin, stop_pid_with_timeout, DeployRequest};
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
    workspace: PathBuf,
}

impl Drop for Upstream {
    fn drop(&mut self) {
        let _ = stop_pid_with_timeout(self.pid, Duration::from_secs(5));
        let _ = std::fs::remove_dir_all(&self.workspace);
    }
}

async fn spawn_upstream() -> Option<Upstream> {
    let bin = crow_kv_server_bin()?;
    if !bin.exists() {
        return None;
    }
    let workspace = std::env::temp_dir().join(format!(
        "crow_kv-mgmt-routes-test-{}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        pick_free_port()
    ));
    std::fs::create_dir_all(&workspace).ok()?;
    std::fs::create_dir_all(workspace.join("bin")).ok()?;
    std::fs::create_dir_all(workspace.join("log")).ok()?;
    let node = NodeEntry {
        id: "n1".into(),
        rack_id: "r1".into(),
        host: "127.0.0.1".into(),
        ssh_port: 22,
        ssh_user: String::new(),
        ssh_key: None,
        ssh_password: None,
    };
    let req = DeployRequest {
        server_id: "n1".into(),
        mgmt_port: pick_free_port(),
        grpc_port: pick_free_port(),
        election_profile: Some("e2e".into()),
        binary: Some(bin),
        ..Default::default()
    };
    let deployed = lifecycle::deploy_local_in_dir(&req, &node, &workspace)
        .await
        .expect("deploy_local_in_dir");
    Some(Upstream {
        pid: deployed.pid,
        mgmt_url: deployed.mgmt_url,
        grpc_url: deployed.grpc_url,
        workspace,
    })
}

async fn spawn_web(upstream: &Upstream) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    let mut cfg = ConsoleConfig::default();
    cfg.racks.push(RackEntry {
        id: "r1".into(),
        name: "r1".into(),
    });
    cfg.nodes.push(NodeEntry {
        id: "n1".into(),
        rack_id: "r1".into(),
        host: "127.0.0.1".into(),
        ssh_port: 22,
        ssh_user: String::new(),
        ssh_key: None,
        ssh_password: None,
    });
    cfg.add_server(ServerEntry {
        id: "n1".into(),
        url: upstream.mgmt_url.clone(),
        node_id: Some("n1".into()),
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
    tokio::spawn(async move {
        axum::serve(listener, router(state)).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    addr
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn full_mgmt_cycle_through_web_routes() {
    let Some(upstream) = spawn_upstream().await else {
        eprintln!("skipping: crow-kv-server binary not built");
        return;
    };
    let web = spawn_web(&upstream).await;
    let base = format!("http://{web}");
    let http = reqwest::Client::new();

    // 1. Initialize the system group so non-zero stores can be created.
    let resp = http
        .post(format!("{base}/api/cluster/init"))
        .json(&json!({"nodes": ["n1"]}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201, "cluster init: {:?}", resp.text().await.ok());

    // 2. POST /api/stores → 201 (orchestrated create on node n1).
    let store_id: u64 = 7;
    let resp = http
        .post(format!("{base}/api/stores"))
        .json(&json!({"store_id": store_id, "nodes": ["n1"]}))
        .send()
        .await
        .expect("POST /api/stores");
    assert_eq!(resp.status(), 201, "{:?}", resp.text().await.ok());

    // 3. GET /api/stores → list contains the store (from cache).
    let stores: Vec<serde_json::Value> = http
        .get(format!("{base}/api/stores"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(
        stores
            .iter()
            .any(|s| s.get("store_id").and_then(serde_json::Value::as_u64) == Some(store_id)),
        "store_id={store_id} not found in {stores:?}"
    );

    // 4. GET /api/stores/:sid → store detail.
    let resp = http
        .get(format!("{base}/api/stores/{store_id}"))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());
    let detail: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(detail["store_id"], store_id);
    let groups_before = detail["groups"].as_array().expect("groups array");
    assert!(
        groups_before.is_empty(),
        "new store should start empty: {detail:?}"
    );

    // 5. POST /api/stores/:sid/groups → 201 (orchestrated group create).
    let group_id: u64 = 70;
    let replica_id: u64 = 700;
    let resp = http
        .post(format!("{base}/api/stores/{store_id}/groups"))
        .json(&json!({"group_id": group_id, "replica_id": replica_id, "nodes": ["n1"]}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201, "{:?}", resp.text().await.ok());

    // 6. POST /api/stores/:sid/groups → 201 (second group).
    let group_id_2: u64 = 80;
    let replica_id_2: u64 = 800;
    let resp = http
        .post(format!("{base}/api/stores/{store_id}/groups"))
        .json(&json!({"group_id": group_id_2, "replica_id": replica_id_2, "nodes": ["n1"]}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201, "{:?}", resp.text().await.ok());

    // 7. GET /api/stores/:sid/groups → 2 explicitly created groups.
    let groups: Vec<serde_json::Value> = http
        .get(format!("{base}/api/stores/{store_id}/groups"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(groups.len(), 2, "expected 2 groups, got {groups:?}");

    // 8. GET /api/stores/:sid/groups/:gid → group detail.
    let resp = http
        .get(format!("{base}/api/stores/{store_id}/groups/{group_id_2}"))
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());
    let gv: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(gv["group_id"], group_id_2);

    // 9. DELETE /api/stores/:sid/groups/:gid → removes the second group.
    let resp = http
        .delete(format!("{base}/api/stores/{store_id}/groups/{group_id_2}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 204);

    // 10. DELETE /api/stores/:sid → removes the store.
    let resp = http
        .delete(format!("{base}/api/stores/{store_id}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 204);

    // 11. GET /api/stores → store 7 should be gone (default store 1 may
    //    remain because crow-kv-server creates it on startup).
    let stores: Vec<serde_json::Value> = http
        .get(format!("{base}/api/stores"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(
        !stores
            .iter()
            .any(|s| s.get("store_id").and_then(serde_json::Value::as_u64) == Some(store_id)),
        "store {store_id} should be gone, got {stores:?}"
    );

    // Cleanup.
    let _ = lifecycle::stop_pid(upstream.pid);
    tokio::time::sleep(Duration::from_millis(50)).await;
}
