// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Integration tests for `POST /system/init` (system group bootstrap).

mod testkit;

use serde_json::Value;
use testkit::process::start_test_server;

fn client() -> reqwest::Client {
    reqwest::Client::new()
}

#[tokio::test]
async fn system_init_creates_store0_group0() {
    let server = start_test_server(&[]).await.expect("start crow-kv-server");

    let resp: Value = client()
        .post(format!("{}/system/init", server.base_url()))
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    assert_eq!(resp["store_id"], 0);
    assert_eq!(resp["group_id"], 0);
    assert_eq!(resp["replica_id"], 1);
    assert!(resp["listen_addr"].is_string());
}

#[tokio::test]
async fn system_init_idempotent_store0() {
    let server = start_test_server(&[]).await.expect("start crow-kv-server");

    // First init creates store 0 + group 0.
    let resp = client()
        .post(format!("{}/system/init", server.base_url()))
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 201);

    // Second init should conflict on group 0.
    let resp = client()
        .post(format!("{}/system/init", server.base_url()))
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 409);
}

#[tokio::test]
async fn system_init_with_custom_replica_id() {
    let server = start_test_server(&[]).await.expect("start crow-kv-server");

    let resp: Value = client()
        .post(format!("{}/system/init", server.base_url()))
        .json(&serde_json::json!({"replica_id": 5, "start_election": true}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    assert_eq!(resp["store_id"], 0);
    assert_eq!(resp["group_id"], 0);
    assert_eq!(resp["replica_id"], 5);
}

#[tokio::test]
async fn system_init_creates_store_visible_in_list() {
    let server = start_test_server(&[]).await.expect("start crow-kv-server");

    client()
        .post(format!("{}/system/init", server.base_url()))
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap();

    let resp: Value = client()
        .get(format!("{}/stores", server.base_url()))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    let stores = resp["stores"].as_array().expect("stores array");
    assert!(stores.iter().any(|s| s["store_id"] == 0), "store 0 should exist");
}

#[tokio::test]
async fn system_init_group_visible_in_list() {
    let server = start_test_server(&[]).await.expect("start crow-kv-server");

    client()
        .post(format!("{}/system/init", server.base_url()))
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap();

    let resp: Value = client()
        .get(format!("{}/stores/0/groups", server.base_url()))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    let groups = resp.as_array().expect("groups array");
    assert!(groups.iter().any(|g| g["group_id"] == 0), "group 0 should exist");
}
