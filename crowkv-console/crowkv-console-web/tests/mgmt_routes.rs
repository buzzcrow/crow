//! C5 web e2e: spin up a real `crowkv-server` plus the console web
//! backend, then exercise the full `/api/stores...` route set through
//! HTTP. Skips silently when the `crowkv-server` binary is not built.

use std::net::SocketAddr;
use std::time::Duration;

use crowkv_console_core::config::NodeEntry;
use crowkv_console_core::lifecycle::{self, crowkv_server_bin, DeployRequest};
use crowkv_console_web::{router, AppState};
use serde_json::json;

fn pick_free_port() -> u16 {
    let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    port
}

async fn spawn_upstream() -> Option<(u32, String)> {
    let bin = crowkv_server_bin()?;
    if !bin.exists() {
        return None;
    }
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
        server_id: "s1".into(),
        mgmt_port: pick_free_port(),
        grpc_port: pick_free_port(),
        binary: Some(bin),
    };
    let deployed = lifecycle::deploy_local(&req, &node).await.expect("deploy_local");
    Some((deployed.pid, deployed.mgmt_url))
}

async fn spawn_web(default_server: String) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0))).await.expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    let state = AppState::new(vec![default_server]);
    tokio::spawn(async move {
        axum::serve(listener, router(state)).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    addr
}

#[tokio::test]
async fn full_mgmt_cycle_through_web_routes() {
    let Some((pid, upstream_url)) = spawn_upstream().await else {
        eprintln!("skipping: crowkv-server binary not built");
        return;
    };
    let web = spawn_web(upstream_url).await;
    let base = format!("http://{web}");
    let http = reqwest::Client::new();

    // 1. POST /api/stores → 201 + StoreSummary
    let store_id: u64 = 7;
    let group_id: u64 = 70;
    let replica_id: u64 = 700;
    let resp = http
        .post(format!("{base}/api/stores"))
        .json(&json!({"store_id": store_id, "group_id": group_id, "replica_id": replica_id}))
        .send()
        .await
        .expect("POST /api/stores");
    assert_eq!(resp.status(), 201, "{:?}", resp.text().await.ok());

    // 2. GET /api/stores → list contains it.
    let stores: Vec<serde_json::Value> = http.get(format!("{base}/api/stores")).send().await.unwrap().json().await.unwrap();
    assert!(stores.iter().any(|s| s.get("store_id").and_then(serde_json::Value::as_u64) == Some(store_id)));

    // 3. POST /api/stores/{sid}/groups → 201
    let group_id_2: u64 = 80;
    let replica_id_2: u64 = 800;
    let resp = http
        .post(format!("{base}/api/stores/{store_id}/groups"))
        .json(&json!({"group_id": group_id_2, "replica_id": replica_id_2}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);

    let groups: Vec<serde_json::Value> = http.get(format!("{base}/api/stores/{store_id}/groups")).send().await.unwrap().json().await.unwrap();
    assert_eq!(groups.len(), 2);

    // 4. POST remotes
    let resp = http
        .post(format!("{base}/api/stores/{store_id}/groups/{group_id_2}/remotes"))
        .json(&json!([{"replica_id": 801, "endpoint": "127.0.0.1:39998"}]))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201);

    let remotes: Vec<serde_json::Value> = http
        .get(format!("{base}/api/stores/{store_id}/groups/{group_id_2}/remotes"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(remotes.len(), 1);
    assert_eq!(remotes[0]["replica_id"].as_u64(), Some(801));

    // 5. DELETE remote
    let resp = http.delete(format!("{base}/api/stores/{store_id}/groups/{group_id_2}/remotes/801")).send().await.unwrap();
    assert_eq!(resp.status(), 204);

    let remotes: Vec<serde_json::Value> = http
        .get(format!("{base}/api/stores/{store_id}/groups/{group_id_2}/remotes"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(remotes.is_empty());

    // 6. DELETE group
    let resp = http.delete(format!("{base}/api/stores/{store_id}/groups/{group_id_2}")).send().await.unwrap();
    assert_eq!(resp.status(), 204);

    // 7. ?server=<override> path: hit upstream directly through the web
    //    proxy, asserting the override branch works.
    let resp = http.get(format!("{base}/api/stores?server=http://127.0.0.1:1")).send().await.unwrap();
    // Bogus override should produce 502 (transport error).
    assert_eq!(resp.status(), 502);

    // Cleanup.
    let _ = lifecycle::stop_pid(pid);
    tokio::time::sleep(Duration::from_millis(50)).await;
}
