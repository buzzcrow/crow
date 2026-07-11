//! C8 smoke: console web boots, the vendored Swagger UI is served at
//! `/api/swagger/`, and `/api/openapi.json?server=...` proxies the
//! upstream `crowkv-server`'s `OpenAPI` document.

use std::net::SocketAddr;
use std::time::Duration;

use crowkv_console_core::config::NodeEntry;
use crowkv_console_core::lifecycle::{self, crowkv_server_bin, DeployRequest};
use crowkv_console_web::{router, AppState};

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
async fn swagger_index_is_served() {
    // No upstream needed for the static-file path.
    let listener = tokio::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0))).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let state = AppState::new(vec!["http://127.0.0.1:1".into()]);
    tokio::spawn(async move {
        axum::serve(listener, router(state)).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    let http = reqwest::Client::new();
    // Trailing-slash form serves index.html via tower-http's ServeDir
    // default (`append_index_html_on_directories`).
    let resp = http.get(format!("http://{addr}/api/swagger/")).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();
    assert!(body.contains("swagger-ui"), "unexpected body: {body}");
    assert!(body.contains("CrowKV"), "title missing: {body}");

    // Direct asset fetch.
    let resp = http.get(format!("http://{addr}/api/swagger/swagger-ui.css")).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let ct = resp.headers().get("content-type").unwrap().to_str().unwrap().to_owned();
    assert!(ct.contains("css") || ct.contains("text"), "unexpected content-type: {ct}");
}

#[tokio::test]
async fn openapi_proxy_returns_upstream_doc() {
    let Some((pid, upstream)) = spawn_upstream().await else {
        eprintln!("skipping: crowkv-server binary not built");
        return;
    };
    let web = spawn_web(upstream.clone()).await;
    let http = reqwest::Client::new();

    // Default-server path (no ?server=).
    let resp = http.get(format!("http://{web}/api/openapi.json")).send().await.unwrap();
    assert_eq!(resp.status(), 200, "{:?}", resp.text().await.ok());
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["openapi"], "3.1.0");
    assert!(body["paths"]["/health"].is_object());

    // Explicit ?server= override.
    let resp = http.get(format!("http://{web}/api/openapi.json?server={upstream}")).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(body["paths"]["/topology"].is_object());

    let _ = lifecycle::stop_pid(pid);
    tokio::time::sleep(Duration::from_millis(50)).await;
}
