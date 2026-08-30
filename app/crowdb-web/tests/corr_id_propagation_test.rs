// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Verify `x-crowdb-kv-corr-id` propagates inbound → handler → response.
//!
//! Each test spins up the real `crowdb-web` router. The middleware
//! should:
//!
//! 1. Pass an inbound header straight through to the response.
//! 2. Mint one when the client doesn't send one.

use std::net::SocketAddr;

use crowdb_console_shared::corr_id;
use crowdb_console_shared::ConsoleConfig;
use crowdb_web::{router, AppState};

async fn spawn_router() -> SocketAddr {
    let listener = tokio::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .await
        .unwrap();
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
    let resp = http
        .get(format!("http://{addr}/healthz"))
        .header(corr_id::HEADER, supplied)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let echoed = resp
        .headers()
        .get(corr_id::HEADER)
        .and_then(|v| v.to_str().ok())
        .map(ToString::to_string);
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
    assert!(
        minted.chars().all(|c| c.is_ascii_hexdigit()),
        "non-hex in {minted:?}"
    );
}
