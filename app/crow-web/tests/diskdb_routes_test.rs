// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Integration coverage for the `DiskDB` REST proxy + batch disk-add
//! routes (R77 Phase 1.7 + 1.10). Tests that require a live diskdb
//! instance (usage, scan, recalc, compact, rebuild) are covered by
//! the E2E suite; here we cover the config-only paths:
//!   - `/api/diskdb/instances` → 502 when no group-0 endpoint
//!   - `/api/diskdb/usage` → 502 when no group-0 endpoint
//!   - `PUT /api/disks/:disk_id/status` → 404 unknown disk, 400 invalid status
//!   - `POST .../disks/batch` → 3 valid, 1 malformed, duplicate ids, missing dg

use std::net::SocketAddr;
use std::time::Duration;

use crow_console_shared::ConsoleConfig;
use crow_web::{router, AppState};
use serde_json::{json, Value};

async fn spawn_web() -> SocketAddr {
    let addr_bind = SocketAddr::from(([127, 0, 0, 1], 0));
    let listener = tokio::net::TcpListener::bind(addr_bind).await.expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    let dir = std::env::temp_dir().join(format!(
        "crow-web-diskdb-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let cfg_path = dir.join("console.toml");
    let cfg = ConsoleConfig::load(&cfg_path).unwrap_or_default();
    let state = AppState::with_config(cfg, Some(cfg_path));
    tokio::spawn(async move {
        axum::serve(listener, router(state)).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    addr
}

async fn json_get(client: &reqwest::Client, url: &str) -> (reqwest::StatusCode, Value) {
    let r = client.get(url).send().await.unwrap();
    let status = r.status();
    let v = r.json::<Value>().await.unwrap_or(Value::Null);
    (status, v)
}

async fn json_post(client: &reqwest::Client, url: &str, body: Value) -> (reqwest::StatusCode, Value) {
    let r = client.post(url).json(&body).send().await.unwrap();
    let status = r.status();
    let v = r.json::<Value>().await.unwrap_or(Value::Null);
    (status, v)
}

async fn put_status(client: &reqwest::Client, url: &str, body: Value) -> reqwest::StatusCode {
    client.put(url).json(&body).send().await.unwrap().status()
}

fn valid_disk_body(id: &str) -> Value {
    json!({
        "disk_id": id,
        "disk_type": "Hdd",
        "capacity_bytes": 4_u64 * 1024 * 1024 * 1024 * 1024,
        "zone_size_bytes": 32_u64 * 1024 * 1024 * 1024,
        "unit_size_bytes": 1_048_576_u32,
    })
}

#[tokio::test]
async fn diskdb_instances_returns_502_without_group_zero() {
    let addr = spawn_web().await;
    let base = format!("http://{addr}");
    let client = reqwest::Client::new();

    let (s, v) = json_get(&client, &format!("{base}/api/diskdb/instances")).await;
    assert_eq!(s.as_u16(), 502, "instances: {s} {v}");
    assert!(v["error"].as_str().unwrap().contains("group-0"));
}

#[tokio::test]
async fn diskdb_usage_returns_502_without_group_zero() {
    let addr = spawn_web().await;
    let base = format!("http://{addr}");
    let client = reqwest::Client::new();

    // No dg → cluster merge path → 502 (no group-0 endpoint).
    let (s, v) = json_get(&client, &format!("{base}/api/diskdb/usage")).await;
    assert_eq!(s.as_u16(), 502, "usage cluster: {s} {v}");

    // With dg → single-instance path → 502 (no diskdb client).
    let (s, v) = json_get(&client, &format!("{base}/api/diskdb/usage?dg=1")).await;
    assert_eq!(s.as_u16(), 502, "usage dg: {s} {v}");
}

#[tokio::test]
async fn set_disk_status_returns_404_for_unknown_disk() {
    let addr = spawn_web().await;
    let base = format!("http://{addr}");
    let client = reqwest::Client::new();

    let s = put_status(
        &client,
        &format!("{base}/api/disks/nonexistent-disk-id/status"),
        json!({ "status": "Up" }),
    )
    .await;
    assert_eq!(s.as_u16(), 404, "unknown disk: {s}");
}

async fn setup_rack_node_dg(client: &reqwest::Client, base: &str, rack_id: u64, node_id: u64, dg_id: u64) {
    let (s, v) = json_post(
        client,
        &format!("{base}/api/racks"),
        json!({ "id": rack_id, "name": format!("r{rack_id}") }),
    )
    .await;
    assert!(s.is_success(), "create rack: {s} {v}");
    let (s, v) = json_post(
        client,
        &format!("{base}/api/racks/{rack_id}/nodes"),
        json!({ "id": node_id, "rack_id": rack_id, "host": "127.0.0.1" }),
    )
    .await;
    assert!(s.is_success(), "create node: {s} {v}");
    let (s, v) = json_post(
        client,
        &format!("{base}/api/nodes/{node_id}/disk-groups"),
        json!({ "id": dg_id }),
    )
    .await;
    assert!(s.is_success(), "create dg: {s} {v}");
}

#[tokio::test]
async fn set_disk_status_returns_400_for_invalid_status() {
    let addr = spawn_web().await;
    let base = format!("http://{addr}");
    let client = reqwest::Client::new();

    // We need a disk in the config first. Create rack → node → dg → disk.
    setup_rack_node_dg(&client, &base, 1, 10, 1).await;
    let (s, v) = json_post(
        &client,
        &format!("{base}/api/nodes/10/disk-groups/1/disks"),
        valid_disk_body("00000000000000000000000000000001"),
    )
    .await;
    assert_eq!(s.as_u16(), 201, "create disk: {s} {v}");

    let s = put_status(
        &client,
        &format!("{base}/api/disks/00000000000000000000000000000001/status"),
        json!({ "status": "Bogus" }),
    )
    .await;
    assert_eq!(s.as_u16(), 400, "invalid status: {s}");
}

#[tokio::test]
async fn batch_disk_add_creates_all_valid_disks() {
    let addr = spawn_web().await;
    let base = format!("http://{addr}");
    let client = reqwest::Client::new();

    setup_rack_node_dg(&client, &base, 2, 20, 1).await;

    let (s, v) = json_post(
        &client,
        &format!("{base}/api/nodes/20/disk-groups/1/disks/batch"),
        json!({
            "disks": [
                valid_disk_body("0000000000000000000000000000000a"),
                valid_disk_body("0000000000000000000000000000000b"),
                valid_disk_body("0000000000000000000000000000000c"),
            ]
        }),
    )
    .await;
    assert_eq!(s.as_u16(), 201, "batch ok: {s} {v}");
    assert_eq!(v["added"].as_array().unwrap().len(), 3);
    assert!(v["sysdata_errors"].as_array().unwrap().is_empty());

    // Verify the disks appear in the list endpoint.
    let (s, v) = json_get(&client, &format!("{base}/api/nodes/20/disk-groups/1/disks")).await;
    assert!(s.is_success(), "list disks: {s} {v}");
    assert_eq!(v.as_array().unwrap().len(), 3);
}

#[tokio::test]
async fn batch_disk_add_rejects_malformed_disk_id() {
    let addr = spawn_web().await;
    let base = format!("http://{addr}");
    let client = reqwest::Client::new();

    setup_rack_node_dg(&client, &base, 3, 30, 1).await;

    let (s, v) = json_post(
        &client,
        &format!("{base}/api/nodes/30/disk-groups/1/disks/batch"),
        json!({
            "disks": [
                valid_disk_body("0000000000000000000000000000000d"),
                { "disk_id": "not-a-valid-hex-id", "disk_type": "Hdd", "capacity_bytes": 4_u64 * 1024_u64.pow(4), "zone_size_bytes": 32_u64 * 1024_u64.pow(3), "unit_size_bytes": 1_048_576_u32 },
            ]
        }),
    )
    .await;
    assert_eq!(s.as_u16(), 400, "malformed: {s} {v}");

    // Verify 0 disks were created (atomic rollback).
    let (s, v) = json_get(&client, &format!("{base}/api/nodes/30/disk-groups/1/disks")).await;
    assert!(s.is_success(), "list disks: {s} {v}");
    assert_eq!(v.as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn batch_disk_add_rejects_duplicate_ids_within_batch() {
    let addr = spawn_web().await;
    let base = format!("http://{addr}");
    let client = reqwest::Client::new();

    setup_rack_node_dg(&client, &base, 4, 40, 1).await;

    let (s, v) = json_post(
        &client,
        &format!("{base}/api/nodes/40/disk-groups/1/disks/batch"),
        json!({
            "disks": [
                valid_disk_body("0000000000000000000000000000000e"),
                valid_disk_body("0000000000000000000000000000000e"),
            ]
        }),
    )
    .await;
    assert_eq!(s.as_u16(), 409, "duplicate: {s} {v}");
    assert!(v["error"].as_str().unwrap().contains("duplicate"));
}

#[tokio::test]
async fn batch_disk_add_returns_404_for_missing_disk_group() {
    let addr = spawn_web().await;
    let base = format!("http://{addr}");
    let client = reqwest::Client::new();

    // Create rack + node but no disk-group.
    let (s, v) = json_post(&client, &format!("{base}/api/racks"), json!({ "id": 5 })).await;
    assert!(s.is_success(), "create rack: {s} {v}");
    let (s, v) = json_post(
        &client,
        &format!("{base}/api/racks/5/nodes"),
        json!({ "id": 50, "rack_id": 5, "host": "127.0.0.1" }),
    )
    .await;
    assert!(s.is_success(), "create node: {s} {v}");

    let (s, v) = json_post(
        &client,
        &format!("{base}/api/nodes/50/disk-groups/999/disks/batch"),
        json!({ "disks": [valid_disk_body("0000000000000000000000000000000f")] }),
    )
    .await;
    assert_eq!(s.as_u16(), 404, "missing dg: {s} {v}");
}
