//! A2: `?recursive=` validation is honoured by every two-tree GET.
//!
//! The handlers currently embed their natural immediate children
//! inline (e.g. `GroupView` carries `replicas`), so for now the
//! value-add of the extractor is **validation**: malformed or
//! out-of-range values surface as `400 Validation` instead of being
//! silently ignored. Deeper `Expandable` walks land per-handler as
//! needed.
//!
//! This test fires every GET endpoint against a fresh `crowkv-web`
//! with `?recursive=nope` and asserts the response is a 400 with the
//! `not an integer or "all"` body produced by
//! `crowkv_console_shared::expand::RecursiveDepth::parse`.

use std::net::SocketAddr;
use std::time::Duration;

use crowkv_web::{router, AppState};

async fn spawn_web() -> SocketAddr {
    let listener = tokio::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0))).await.unwrap();
    let addr = listener.local_addr().unwrap();
    let state = AppState::new(vec!["http://127.0.0.1:1".into()]);
    tokio::spawn(async move {
        axum::serve(listener, router(state)).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    addr
}

#[tokio::test]
async fn malformed_recursive_yields_400_on_every_get() {
    let addr = spawn_web().await;
    let http = reqwest::Client::new();

    // Every GET under the two-tree contract. Some of these return 404
    // for unknown ids before the extractor fires — so we use
    // `?recursive=nope` which must be caught by the extractor first
    // and surface as 400 regardless of whether the id exists.
    let paths = [
        // Physical tree.
        "/api/racks",
        "/api/racks/r1",
        "/api/racks/r1/nodes",
        "/api/nodes",
        "/api/nodes/n1",
        "/api/nodes/n1/server",
        "/api/nodes/n1/stores",
        "/api/nodes/n1/stores/1",
        "/api/nodes/n1/stores/1/groups",
        "/api/nodes/n1/stores/1/groups/1",
        // Logical tree.
        "/api/stores",
        "/api/stores/1",
        "/api/stores/1/groups",
        "/api/stores/1/groups/1",
        "/api/stores/1/groups/1/replicas",
        "/api/stores/1/groups/1/replicas/1",
    ];

    for p in paths {
        let url = format!("http://{addr}{p}?recursive=nope");
        let resp = http.get(&url).send().await.unwrap();
        assert_eq!(resp.status(), 400, "expected 400 for {p}, got {}", resp.status());
        let body: serde_json::Value = resp.json().await.unwrap();
        let err = body["error"].as_str().unwrap_or_default();
        assert!(err.contains("not an integer") || err.contains("exceeds"), "{p} body: {body}");
    }
}

#[tokio::test]
async fn out_of_range_recursive_yields_400() {
    let addr = spawn_web().await;
    let http = reqwest::Client::new();

    // `MAX_DEPTH` is 8 (see `shared::expand`); anything larger must
    // surface as 400 instead of being silently clamped.
    let resp = http.get(format!("http://{addr}/api/racks?recursive=99")).send().await.unwrap();
    assert_eq!(resp.status(), 400);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert!(body["error"].as_str().unwrap().contains("exceeds maximum"));
}

#[tokio::test]
async fn absent_and_valid_recursive_values_are_accepted() {
    let addr = spawn_web().await;
    let http = reqwest::Client::new();

    // `/api/racks` always succeeds (the list may be empty).
    for q in ["", "?recursive=0", "?recursive=1", "?recursive=8", "?recursive=all", "?recursive=ALL"] {
        let url = format!("http://{addr}/api/racks{q}");
        let resp = http.get(&url).send().await.unwrap();
        assert_eq!(resp.status(), 200, "expected 200 for {url}, got {}", resp.status());
    }
}
