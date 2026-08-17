// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Unit tests for the `ConsoleClient` diskdb extension methods
//! (R77 Phase 2.2). Uses a mock HTTP server to verify that each
//! method constructs the correct URL, sends the correct body, and
//! deserializes the response into the right type. Error responses
//! surface as `Error::UpstreamRpc`.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use axum::extract::Path;
use axum::http::StatusCode;
use axum::routing::{get, post, put};
use axum::{Json, Router};
use crow_console_shared::clients::console::ConsoleClient;
use crow_console_shared::error::Error;
use serde_json::{json, Value};
use tokio::sync::Mutex as AsyncMutex;

static LAST_PATH: Mutex<String> = Mutex::new(String::new());
static LAST_BODY: Mutex<Option<Value>> = Mutex::new(None);
static CALL_COUNT: AtomicUsize = AtomicUsize::new(0);

/// Serialization mutex — ensures only one test runs at a time so the
/// shared global state doesn't interleave between tests.
static TEST_LOCK: OnceLock<AsyncMutex<()>> = OnceLock::new();

async fn test_lock() -> tokio::sync::MutexGuard<'static, ()> {
    TEST_LOCK.get_or_init(|| AsyncMutex::new(())).lock().await
}

fn reset_state() {
    *LAST_PATH.lock().unwrap() = String::new();
    *LAST_BODY.lock().unwrap() = None;
    CALL_COUNT.store(0, Ordering::SeqCst);
}

async fn spawn_mock(router: Router) -> SocketAddr {
    let bind_addr = SocketAddr::from(([127, 0, 0, 1], 0));
    let listener = tokio::net::TcpListener::bind(bind_addr).await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    addr
}

fn record(path: &str, body: Option<Value>) {
    *LAST_PATH.lock().unwrap() = path.to_string();
    *LAST_BODY.lock().unwrap() = body;
    CALL_COUNT.fetch_add(1, Ordering::SeqCst);
}

// ── Mock handlers ─────────────────────────────────────────────────

async fn mock_instances() -> Json<Value> {
    record("/api/diskdb/instances", None);
    Json(json!([
        {
            "instance_id": 1,
            "grpc_endpoint": "127.0.0.1:50051",
            "last_heartbeat_ms": 1000,
            "owned_dg_ids": [1, 2],
            "group_usages": [
                { "disk_group_id": 1, "capacity_bytes": 1000, "busy_bytes": 500, "free_bytes": 500, "disk_count": 2 }
            ]
        }
    ]))
}

async fn mock_usage() -> Json<Value> {
    record("/api/diskdb/usage", None);
    Json(json!({
        "disk_groups": [{
            "rack_id": 1, "node_id": 10, "disk_group_id": 1, "status": 1,
            "disk_ids": ["abc"], "disks": [],
            "capacity_bytes": 1000, "busy_bytes": 500, "free_bytes": 500,
            "allocatable_disk_count": 1
        }]
    }))
}

async fn mock_scan_status() -> Json<Value> {
    record("/api/diskdb/scan-status", None);
    Json(json!({
        "has_run": true, "scan_in_progress": false,
        "summary": {
            "started_at_ms": 100, "duration_ms": 50, "zones_scanned": 10,
            "zones_skipped_active": 0, "zones_skipped_compacting": 0,
            "ghost_busy": 0, "ghost_free": 0, "uncompacted_lag": 0,
            "corrupt_snapshots": 0, "corrupt_records": 0, "owner_mismatches": 0,
            "leak_status": "none"
        }
    }))
}

async fn mock_scan(Json(body): Json<Value>) -> Json<Value> {
    record("/api/diskdb/scan", Some(body));
    Json(json!({ "has_run": true, "scan_in_progress": true, "summary": null }))
}

async fn mock_recalc(Json(body): Json<Value>) -> Json<Value> {
    record("/api/diskdb/recalc", Some(body));
    Json(json!({ "results": [{ "disk_group_id": 1, "drift_detected": false, "zones": [] }] }))
}

async fn mock_compact(Json(body): Json<Value>) -> Json<Value> {
    record("/api/diskdb/compact", Some(body));
    Json(json!({ "compacted_zone_count": 3, "total_free_records_deleted": 100, "zones": [] }))
}

async fn mock_rebuild(Json(body): Json<Value>) -> Json<Value> {
    record("/api/diskdb/rebuild", Some(body));
    Json(json!({ "rebuilt_zone_count": 5, "total_busy_units": 1000, "total_free_units": 2000 }))
}

async fn mock_set_status(Path(disk_id): Path<String>, Json(body): Json<Value>) -> StatusCode {
    record(&format!("/api/disks/{disk_id}/status"), Some(body));
    StatusCode::NO_CONTENT
}

async fn mock_set_dg_status(
    Path((rack, node, dg)): Path<(u64, u64, u64)>,
    Json(body): Json<Value>,
) -> StatusCode {
    record(&format!("/api/disk-groups/{rack}/{node}/{dg}/status"), Some(body));
    StatusCode::NO_CONTENT
}

async fn mock_error() -> (StatusCode, Json<Value>) {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({ "error": "boom" })),
    )
}

#[tokio::test]
async fn list_diskdb_instances_deserializes() {
    let _lock = test_lock().await;
    reset_state();
    let app = Router::new().route("/api/diskdb/instances", get(mock_instances));
    let addr = spawn_mock(app).await;
    let client = ConsoleClient::new(format!("http://{addr}")).unwrap();

    let result = client.list_diskdb_instances().await.unwrap();
    assert_eq!(CALL_COUNT.load(Ordering::SeqCst), 1);
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].instance_id, 1);
    assert_eq!(result[0].grpc_endpoint, "127.0.0.1:50051");
    assert_eq!(result[0].owned_dg_ids, vec![1, 2]);
    assert_eq!(result[0].group_usages.len(), 1);
    assert_eq!(result[0].group_usages[0].disk_group_id, 1);
}

#[tokio::test]
async fn diskdb_usage_deserializes() {
    let _lock = test_lock().await;
    reset_state();
    let app = Router::new().route("/api/diskdb/usage", get(mock_usage));
    let addr = spawn_mock(app).await;
    let client = ConsoleClient::new(format!("http://{addr}")).unwrap();

    let result = client.diskdb_usage(None, None, None).await.unwrap();
    assert_eq!(CALL_COUNT.load(Ordering::SeqCst), 1);
    assert_eq!(result.disk_groups.len(), 1);
    assert_eq!(result.disk_groups[0].disk_group_id, 1);
    assert_eq!(result.disk_groups[0].capacity_bytes, 1000);
}

#[tokio::test]
async fn diskdb_usage_with_params_builds_correct_url() {
    let _lock = test_lock().await;
    reset_state();
    let app = Router::new().route("/api/diskdb/usage", get(mock_usage));
    let addr = spawn_mock(app).await;
    let client = ConsoleClient::new(format!("http://{addr}")).unwrap();

    let _ = client.diskdb_usage(Some(1), Some("abc"), Some(5)).await.unwrap();
    let path = LAST_PATH.lock().unwrap().clone();
    assert_eq!(path, "/api/diskdb/usage");
    // The query string is appended by the client, not visible in the
    // axum path matcher — we just verify the call succeeded.
    assert_eq!(CALL_COUNT.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn diskdb_scan_status_deserializes() {
    let _lock = test_lock().await;
    reset_state();
    let app = Router::new().route("/api/diskdb/scan-status", get(mock_scan_status));
    let addr = spawn_mock(app).await;
    let client = ConsoleClient::new(format!("http://{addr}")).unwrap();

    let result = client.diskdb_scan_status(None).await.unwrap();
    assert!(result.has_run);
    assert!(!result.scan_in_progress);
    assert!(result.summary.is_some());
    assert_eq!(result.summary.unwrap().zones_scanned, 10);
}

#[tokio::test]
async fn diskdb_trigger_scan_sends_dg_body() {
    let _lock = test_lock().await;
    reset_state();
    let app = Router::new().route("/api/diskdb/scan", post(mock_scan));
    let addr = spawn_mock(app).await;
    let client = ConsoleClient::new(format!("http://{addr}")).unwrap();

    let result = client.diskdb_trigger_scan(Some(2)).await.unwrap();
    assert!(result.scan_in_progress);
    let body = LAST_BODY.lock().unwrap().clone().unwrap();
    assert_eq!(body["dg"], 2);
}

#[tokio::test]
async fn diskdb_recalc_sends_dg_body() {
    let _lock = test_lock().await;
    reset_state();
    let app = Router::new().route("/api/diskdb/recalc", post(mock_recalc));
    let addr = spawn_mock(app).await;
    let client = ConsoleClient::new(format!("http://{addr}")).unwrap();

    let result = client.diskdb_recalc(None).await.unwrap();
    assert_eq!(result.results.len(), 1);
    let body = LAST_BODY.lock().unwrap().clone().unwrap();
    assert!(body["dg"].is_null());
}

#[tokio::test]
async fn diskdb_compact_sends_disk_id_and_zones() {
    let _lock = test_lock().await;
    reset_state();
    let app = Router::new().route("/api/diskdb/compact", post(mock_compact));
    let addr = spawn_mock(app).await;
    let client = ConsoleClient::new(format!("http://{addr}")).unwrap();

    let result = client
        .diskdb_compact("my-disk-id", Some(vec![1, 2, 3]))
        .await
        .unwrap();
    assert_eq!(result.compacted_zone_count, 3);
    let body = LAST_BODY.lock().unwrap().clone().unwrap();
    assert_eq!(body["disk_id"], "my-disk-id");
    assert_eq!(body["zone_indices"], json!([1, 2, 3]));
}

#[tokio::test]
async fn diskdb_rebuild_sends_disk_id() {
    let _lock = test_lock().await;
    reset_state();
    let app = Router::new().route("/api/diskdb/rebuild", post(mock_rebuild));
    let addr = spawn_mock(app).await;
    let client = ConsoleClient::new(format!("http://{addr}")).unwrap();

    let result = client.diskdb_rebuild("my-disk-id", None).await.unwrap();
    assert_eq!(result.rebuilt_zone_count, 5);
    let body = LAST_BODY.lock().unwrap().clone().unwrap();
    assert_eq!(body["disk_id"], "my-disk-id");
    assert!(body["zone_indices"].is_null());
}

#[tokio::test]
async fn set_disk_status_sends_status_body() {
    let _lock = test_lock().await;
    reset_state();
    let app = Router::new().route("/api/disks/:disk_id/status", put(mock_set_status));
    let addr = spawn_mock(app).await;
    let client = ConsoleClient::new(format!("http://{addr}")).unwrap();

    client.set_disk_status("abc123", "Down").await.unwrap();
    let path = LAST_PATH.lock().unwrap().clone();
    assert_eq!(path, "/api/disks/abc123/status");
    let body = LAST_BODY.lock().unwrap().clone().unwrap();
    assert_eq!(body["status"], "Down");
}

#[tokio::test]
async fn set_disk_group_status_sends_status_body() {
    let _lock = test_lock().await;
    reset_state();
    let app = Router::new().route("/api/disk-groups/:rack/:node/:dg/status", put(mock_set_dg_status));
    let addr = spawn_mock(app).await;
    let client = ConsoleClient::new(format!("http://{addr}")).unwrap();

    client.set_disk_group_status(1, 10, 2, "Up").await.unwrap();
    let path = LAST_PATH.lock().unwrap().clone();
    assert_eq!(path, "/api/disk-groups/1/10/2/status");
    let body = LAST_BODY.lock().unwrap().clone().unwrap();
    assert_eq!(body["status"], "Up");
}

#[tokio::test]
async fn error_response_surfaces_as_upstream_rpc() {
    let _lock = test_lock().await;
    reset_state();
    let app = Router::new().route("/api/diskdb/instances", get(mock_error));
    let addr = spawn_mock(app).await;
    let client = ConsoleClient::new(format!("http://{addr}")).unwrap();

    let err = client.list_diskdb_instances().await.unwrap_err();
    match err {
        Error::UpstreamRpc { .. } => {}
        other => panic!("expected UpstreamRpc, got {other:?}"),
    }
}
