//! Verify `x-crowkv-corr-id` propagates inbound → handler → response.
//!
//! Each test spins up the real `crowkv-web` router. The middleware
//! should:
//!
//! 1. Pass an inbound header straight through to the response.
//! 2. Mint one when the client doesn't send one.
//!
//! The third invariant — the same id appearing on the outbound call
//! `crowkv-web` makes to `crowkv-server` — is exercised by the
//! `ops_log` integration test (see same file) which spawns a stub
//! upstream and asserts the header was forwarded.

use std::net::SocketAddr;

use axum::routing::get;
use crowkv_console_shared::config::{NodeEntry, RackEntry, ServerEntry};
use crowkv_console_shared::corr_id;
use crowkv_console_shared::ConsoleConfig;
use crowkv_web::{router, AppState};

async fn spawn_router() -> SocketAddr {
    let listener = tokio::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0))).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let state = AppState::with_config(ConsoleConfig::default(), None);
    tokio::spawn(async move {
        axum::serve(listener, router(state)).await.unwrap();
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    addr
}

#[tokio::test]
async fn inbound_corr_id_is_echoed_back() {
    let addr = spawn_router().await;
    let http = reqwest::Client::new();
    let supplied = "deadbeefcafebabe";
    let resp = http.get(format!("http://{addr}/healthz")).header(corr_id::HEADER, supplied).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let echoed = resp.headers().get(corr_id::HEADER).and_then(|v| v.to_str().ok()).map(ToString::to_string);
    assert_eq!(echoed.as_deref(), Some(supplied));
}

#[tokio::test]
async fn missing_corr_id_is_minted_and_returned() {
    let addr = spawn_router().await;
    let http = reqwest::Client::new();
    let resp = http.get(format!("http://{addr}/healthz")).send().await.unwrap();
    assert_eq!(resp.status(), 200);
    let minted = resp
        .headers()
        .get(corr_id::HEADER)
        .and_then(|v| v.to_str().ok())
        .map(ToString::to_string)
        .expect("middleware must mint a corr-id when client doesn't send one");
    assert_eq!(minted.len(), 16, "corr-id should be 16 hex chars, got {minted:?}");
    assert!(minted.chars().all(|c| c.is_ascii_hexdigit()), "non-hex in {minted:?}");
}

#[tokio::test]
async fn corr_id_forwards_to_upstream_openapi_proxy() {
    use axum::http::HeaderName;
    use std::sync::{Arc, Mutex};

    // Stub upstream that captures the `x-crowkv-corr-id` header it
    // receives and returns a minimal OpenAPI doc.
    let captured: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let captured_for_stub = Arc::clone(&captured);

    let upstream_listener = tokio::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0))).await.unwrap();
    let upstream_addr = upstream_listener.local_addr().unwrap();
    let stub = axum::Router::new().route(
        "/openapi.json",
        get(move |headers: axum::http::HeaderMap| {
            let captured = Arc::clone(&captured_for_stub);
            async move {
                let v = headers.get(HeaderName::from_static(corr_id::HEADER)).and_then(|v| v.to_str().ok()).map(ToString::to_string);
                *captured.lock().unwrap() = v;
                axum::Json(serde_json::json!({"openapi": "3.1.0", "info": {"title": "stub", "version": "test"}, "paths": {}}))
            }
        }),
    );
    tokio::spawn(async move {
        axum::serve(upstream_listener, stub).await.unwrap();
    });

    // Web with the stub registered as node `n1`'s server.
    let listener = tokio::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0))).await.unwrap();
    let web_addr = listener.local_addr().unwrap();
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
        url: format!("http://{upstream_addr}"),
        node_id: Some("n1".into()),
        grpc_url: None,
        pid: None,
    })
    .unwrap();
    let state = AppState::with_config(cfg, None);
    tokio::spawn(async move {
        axum::serve(listener, router(state)).await.unwrap();
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let supplied = "0123456789abcdef";
    let http = reqwest::Client::new();
    let resp = http
        .get(format!("http://{web_addr}/api/nodes/n1/openapi.json"))
        .header(corr_id::HEADER, supplied)
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success(), "proxy must succeed");

    let forwarded = captured.lock().unwrap().clone();
    assert_eq!(forwarded.as_deref(), Some(supplied), "the same corr-id the client sent must reach the upstream stub");
}
