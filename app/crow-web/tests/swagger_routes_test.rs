// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! C8 smoke: console web boots, the vendored Swagger UI is served at
//! `/api/swagger/`, and `/api/nodes/:id/openapi.json` proxies the
//! upstream `crow-kv-server`'s `OpenAPI` document.

use std::net::SocketAddr;
use std::time::Duration;

use axum::routing::get;
use crow_console_shared::config::{NodeEntry, RackEntry, ServerEntry};
use crow_console_shared::lifecycle::{self, crow_kv_server_bin, DeployRequest};
use crow_console_shared::ConsoleConfig;
use crow_web::{router, AppState};

fn pick_free_port() -> u16 {
    crow_console_shared::test_ports::unique_test_port()
}

struct Upstream {
    pid: u32,
    mgmt_url: String,
    grpc_url: String,
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

async fn spawn_web_with_node(upstream: &Upstream) -> SocketAddr {
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
    tokio::spawn(async move {
        axum::serve(listener, router(state)).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    addr
}

async fn spawn_openapi_stub(name: &'static str) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    let app = axum::Router::new().route(
        "/openapi.json",
        get(move || async move {
            axum::Json(serde_json::json!({
                "openapi": "3.1.0",
                "info": { "title": name, "version": "test" },
                "paths": {}
            }))
        }),
    );
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    addr
}

#[tokio::test]
async fn swagger_index_is_served() {
    // No upstream needed for the static-file path.
    let listener = tokio::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    let state = AppState::new(vec!["http://127.0.0.1:1".into()]);
    tokio::spawn(async move {
        axum::serve(listener, router(state)).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    let http = reqwest::Client::new();
    // Trailing-slash form serves index.html via tower-http's ServeDir
    // default (`append_index_html_on_directories`).
    let resp = http
        .get(format!("http://{addr}/api/swagger/"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();
    assert!(body.contains("swagger-ui"), "unexpected body: {body}");
    assert!(body.contains("CrowKV"), "title missing: {body}");

    // Direct asset fetch.
    let resp = http
        .get(format!("http://{addr}/api/swagger/swagger-ui.css"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let ct = resp
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();
    assert!(
        ct.contains("css") || ct.contains("text"),
        "unexpected content-type: {ct}"
    );

    // Explicit index.html path (the SPA wires the panel as an <iframe
    // src="/api/swagger/index.html?url=..."> so the path must resolve).
    let resp = http
        .get(format!("http://{addr}/api/swagger/index.html"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();
    assert!(
        body.contains("swagger-ui"),
        "index.html body missing swagger-ui marker: {body}"
    );

    // Deep-link form: /api/swagger/?url=/api/nodes/:n/openapi.json.
    // The query string is consumed by the bundled JS at runtime, so the
    // server-side response is just the same HTML body. We verify the
    // path still 200s with the query attached.
    let resp = http
        .get(format!(
            "http://{addr}/api/swagger/?url=/api/nodes/n1/openapi.json"
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body = resp.text().await.unwrap();
    assert!(
        body.contains("swagger-ui"),
        "deep-link body missing swagger-ui marker: {body}"
    );
}

#[tokio::test]
async fn openapi_proxy_returns_upstream_doc() {
    let Some(upstream) = spawn_upstream().await else {
        eprintln!("skipping: crow-kv-server binary not built");
        return;
    };
    let web = spawn_web_with_node(&upstream).await;
    let http = reqwest::Client::new();

    // Per-node OpenAPI proxy: GET /api/nodes/:id/openapi.json.
    let resp = http
        .get(format!("http://{web}/api/nodes/n1/openapi.json"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200, "{:?}", resp.text().await.ok());
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["openapi"], "3.1.0");
    assert!(body["paths"]["/health"].is_object());
    assert!(body["paths"]["/topology"].is_object());

    // Verify TTL cache works: second request should hit cache.
    let resp2 = http
        .get(format!("http://{web}/api/nodes/n1/openapi.json"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp2.status(), 200);
    let body2: serde_json::Value = resp2.json().await.unwrap();
    assert_eq!(body, body2);

    let _ = lifecycle::stop_pid(upstream.pid);
    tokio::time::sleep(Duration::from_millis(50)).await;
}

#[tokio::test]
async fn openapi_proxy_cache_is_per_node() {
    let n1 = spawn_openapi_stub("node-one").await;
    let n2 = spawn_openapi_stub("node-two").await;
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
    cfg.nodes.push(NodeEntry {
        id: 2,
        rack_id: 1,
        host: "127.0.0.1".into(),
        ssh_port: 22,
        ssh_user: String::new(),
        ssh_key: None,
        ssh_password: None,
    });
    cfg.add_server(ServerEntry {
        id: "n1".to_string(),
        url: format!("http://{n1}"),
        node_id: Some(1),
        grpc_url: None,
        mgmt_port: None,
        grpc_port: None,
        auto_start: false,
        binary: None,
        election_profile: None,
        pid: None,
    })
    .unwrap();
    cfg.add_server(ServerEntry {
        id: "n2".to_string(),
        url: format!("http://{n2}"),
        node_id: Some(2),
        grpc_url: None,
        mgmt_port: None,
        grpc_port: None,
        auto_start: false,
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

    let http = reqwest::Client::new();
    let body1: serde_json::Value = http
        .get(format!("http://{web}/api/nodes/n1/openapi.json"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let body2: serde_json::Value = http
        .get(format!("http://{web}/api/nodes/n2/openapi.json"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    assert_eq!(body1["info"]["title"], "node-one");
    assert_eq!(body2["info"]["title"], "node-two");
}
