// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! C8 SPA fallback tests.
//!
//! Verifies that the SPA route (`/` and any non-API path) returns
//! either the built React shell when `web/ui/dist/index.html` is
//! present, or the instructional fallback when it is not. The fallback
//! must coexist with `/api/*` and `/healthz` without shadowing them.

use std::net::SocketAddr;
use std::path::PathBuf;

fn dist_dir() -> PathBuf {
    let manifest = std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR");
    PathBuf::from(manifest).join("ui").join("dist")
}

async fn spawn_server() -> SocketAddr {
    let listener = tokio::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    tokio::spawn(async move {
        let state = crowdb_web::AppState::default();
        axum::serve(listener, crowdb_web::router(state)).await.unwrap();
    });
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    addr
}

#[tokio::test]
async fn root_serves_spa_or_fallback() {
    let addr = spawn_server().await;

    let resp = reqwest::get(format!("http://{addr}/")).await.expect("GET /");
    assert!(resp.status().is_success(), "status was {}", resp.status());
    let body = resp.text().await.expect("body");

    let dist_index = dist_dir().join("index.html");
    if dist_index.is_file() {
        // Built path: must contain the React mount point.
        assert!(
            body.contains("id=\"root\""),
            "built SPA missing #root mount: {body}"
        );
    } else {
        // Fallback path: must contain the instructional message.
        assert!(
            body.contains("UI not built"),
            "fallback page missing marker: {body}"
        );
        assert!(body.contains("make ui-build"), "fallback page missing build hint");
    }
}

#[tokio::test]
async fn deep_link_falls_through_to_index() {
    // Any non-API path that isn't a real file under dist/ should fall
    // back to the SPA root (so HTML5 history routing works) — or, when
    // the build is missing, to the instructional page.
    let addr = spawn_server().await;
    let resp = reqwest::get(format!("http://{addr}/some/spa/route"))
        .await
        .expect("GET deep");
    assert!(resp.status().is_success());
    let body = resp.text().await.expect("body");
    assert!(
        body.contains("id=\"root\"") || body.contains("UI not built"),
        "expected SPA shell or fallback page; got: {body}",
    );
}

#[tokio::test]
async fn api_routes_are_not_shadowed_by_spa_fallback() {
    let addr = spawn_server().await;

    // /healthz must still hit the original handler, not the SPA.
    let body = reqwest::get(format!("http://{addr}/healthz"))
        .await
        .expect("healthz")
        .text()
        .await
        .expect("text");
    assert_eq!(body, "ok");

    // A live two-tree endpoint returns JSON (an empty list when nothing
    // is registered), not an HTML SPA fallback. Picked /api/racks since
    // it's the lightest non-trivial route in the new contract.
    let resp = reqwest::get(format!("http://{addr}/api/racks"))
        .await
        .expect("racks");
    assert!(resp.status().is_success());
    let ct = resp
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .map(|v| v.to_str().unwrap_or("").to_string())
        .unwrap_or_default();
    assert!(
        ct.starts_with("application/json"),
        "expected JSON, got Content-Type={ct}"
    );
}

#[tokio::test]
async fn path_traversal_is_rejected() {
    let addr = spawn_server().await;
    // Use a raw URI containing `..` to bypass client-side normalization.
    let client = reqwest::Client::new();
    let resp = client
        .get(format!("http://{addr}/../Cargo.toml"))
        .send()
        .await
        .expect("traversal");
    // reqwest may normalize the path before sending; accept either a
    // 400 from our explicit check or a 200 SPA fallback. The key
    // invariant is: we MUST NOT serve repository files.
    let body = resp.text().await.unwrap_or_default();
    assert!(
        !body.contains("[package]") && !body.contains("[workspace]"),
        "leaked Cargo.toml contents: {body}"
    );
}
