// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Smoke test: bind ephemeral port, hit `/healthz`, assert 200 + body.

use std::net::SocketAddr;

#[tokio::test]
async fn healthz_returns_ok() {
    let listener = tokio::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("local_addr");

    let server = tokio::spawn(async move {
        let state = crowkv_web::AppState::default();
        axum::serve(listener, crowkv_web::router(state)).await.unwrap();
    });

    // give axum a moment to be ready
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let body = reqwest::get(format!("http://{addr}/healthz"))
        .await
        .expect("request")
        .text()
        .await
        .expect("text");
    assert_eq!(body, "ok");

    server.abort();
}
