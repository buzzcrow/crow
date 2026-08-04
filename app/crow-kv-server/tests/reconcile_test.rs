// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Integration tests for node startup reconciliation with group 0.

mod testkit;

use serde_json::Value;
use testkit::process::start_test_server;

fn client() -> reqwest::Client {
    reqwest::Client::new()
}

#[tokio::test]
async fn reconcile_skips_when_group0_missing() {
    // No store 0 / group 0 → reconcile should skip silently.
    let server = start_test_server(&[]).await.expect("start crow-kv-server");
    // Just verify the server started fine (reconcile ran and returned).
    let resp = client()
        .get(format!("{}/health", server.base_url()))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 200);
}

#[tokio::test]
async fn reconcile_skips_when_not_finalized() {
    let server = start_test_server(&[]).await.expect("start crow-kv-server");

    // Init group 0 but don't finalize.
    client()
        .post(format!("{}/system/init", server.base_url()))
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap();

    // Reconcile would have run at startup before group 0 existed.
    // After init, group 0 exists but is not finalized.
    let resp: Value = client()
        .get(format!("{}/topology/ready", server.base_url()))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(resp["ready"], false);
}

#[tokio::test]
async fn reconcile_runs_when_finalized() {
    let server = start_test_server(&[]).await.expect("start crow-kv-server");

    // Init + finalize.
    client()
        .post(format!("{}/system/init", server.base_url()))
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap();

    client()
        .post(format!("{}/topology/finalize", server.base_url()))
        .send()
        .await
        .unwrap();

    // Verify topology is ready.
    let resp: Value = client()
        .get(format!("{}/topology/ready", server.base_url()))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(resp["ready"], true);

    // The reconcile at startup would have scanned group 0 and found
    // no topology entries (only /topology/ready). The server should
    // still be healthy.
    let resp = client()
        .get(format!("{}/health", server.base_url()))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 200);
}
