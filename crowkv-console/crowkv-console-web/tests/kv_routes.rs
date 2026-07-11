//! C6 web e2e: spawn `crowkv-server` + console web, then drive
//! `/api/stores/{sid}/groups/{gid}/kv/{put,get,delete}` over HTTP.

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
    let listener = tokio::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0))).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let state = AppState::new(vec![default_server]);
    tokio::spawn(async move {
        axum::serve(listener, router(state)).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    addr
}

#[tokio::test]
async fn kv_put_get_delete_through_web_routes() {
    let Some((pid, upstream)) = spawn_upstream().await else {
        eprintln!("skipping: crowkv-server binary not built");
        return;
    };
    let web = spawn_web(upstream).await;
    let base = format!("http://{web}");
    let http = reqwest::Client::new();

    // Bootstrap store/group is store=1, group=1.
    let url = format!("{base}/api/stores/1/groups/1/kv");

    // PUT
    let resp = http.post(format!("{url}/put")).json(&json!({"key": "alpha", "value": "beta"})).send().await.unwrap();
    assert_eq!(resp.status(), 200, "{:?}", resp.text().await.ok());

    // GET → found
    let resp = http.get(format!("{url}/get?key=alpha")).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["found"], true);
    assert_eq!(body["value_utf8"], "beta");

    // DELETE
    let resp = http.post(format!("{url}/delete")).json(&json!({"key": "alpha"})).send().await.unwrap();
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
    let resp = http.get(format!("{url}/get?key_hex=ff00ff")).send().await.unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["found"], true);
    assert_eq!(body["value_hex"], "deadbeef");

    // Cleanup.
    let _ = lifecycle::stop_pid(pid);
    tokio::time::sleep(Duration::from_millis(50)).await;
}
