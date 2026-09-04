// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Real-process integration tests for the `crowdb-kv-server` HTTP management API.

mod common;

use serde_json::Value;

use common::process::{start_test_server, ServerHandle};

fn client() -> reqwest::Client {
    reqwest::Client::new()
}

async fn start_server() -> ServerHandle {
    start_test_server(&["--stores", "0", "--groups", "1", "--replica", "1"])
        .await
        .expect("start crowdb-kv-server")
}

async fn add_store(base: &str, store_id: u64, _group_id: u64, _replica_id: u64) -> reqwest::Response {
    client()
        .post(format!("{base}/stores"))
        .json(&serde_json::json!({
            "store_id": store_id
        }))
        .send()
        .await
        .unwrap()
}

async fn add_group(base: &str, store_id: u64, group_id: u64, replica_id: u64) -> reqwest::Response {
    client()
        .post(format!("{base}/stores/{store_id}/groups"))
        .json(&serde_json::json!({
            "group_id": group_id,
            "replica_id": replica_id
        }))
        .send()
        .await
        .unwrap()
}

#[tokio::test]
async fn health_check() {
    let server = start_server().await;
    let resp: Value = client()
        .get(format!("{}/health", server.base_url()))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(resp["status"], "ok");
}

#[tokio::test]
async fn openapi_json_is_served() {
    let server = start_server().await;
    let resp: Value = client()
        .get(format!("{}/openapi.json", server.base_url()))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(resp["openapi"], "3.1.0");
    assert!(resp["paths"]["/health"].is_object());
    assert!(resp["paths"]["/topology"].is_object());
}

#[tokio::test]
async fn list_stores_with_initial_cli_store() {
    let server = start_server().await;
    let resp: Value = client()
        .get(format!("{}/stores", server.base_url()))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let stores = resp["stores"].as_array().unwrap();
    assert_eq!(stores.len(), 1);
    assert_eq!(stores[0]["store_id"], 0);
    assert!(stores[0]["group_count"].as_u64().unwrap() >= 1);
}

#[tokio::test]
async fn get_store_not_found() {
    let server = start_server().await;
    let resp = client()
        .get(format!("{}/stores/99", server.base_url()))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
}

#[tokio::test]
async fn add_store_via_api() {
    let server = start_server().await;
    let resp = add_store(server.base_url(), 5, 10, 2).await;
    assert_eq!(resp.status(), 201);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["store_id"], 5);
    assert_eq!(body["group_count"], 0);

    let list: Value = client()
        .get(format!("{}/stores", server.base_url()))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(list["stores"].as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn add_store_conflict() {
    let server = start_server().await;
    let resp = add_store(server.base_url(), 0, 1, 1).await;
    assert_eq!(resp.status(), 409);
}

#[tokio::test]
async fn remove_store_via_api() {
    let server = start_server().await;
    let resp = client()
        .delete(format!("{}/stores/0", server.base_url()))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let resp = client()
        .get(format!("{}/stores/0", server.base_url()))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
}

#[tokio::test]
async fn remove_store_not_found() {
    let server = start_server().await;
    let resp = client()
        .delete(format!("{}/stores/99", server.base_url()))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
}

#[tokio::test]
async fn list_groups() {
    let server = start_server().await;
    let groups: Vec<Value> = client()
        .get(format!("{}/stores/0/groups", server.base_url()))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(groups.len(), 1);
    assert_eq!(groups[0]["group_id"], 1);
}

#[tokio::test]
async fn add_group_via_api() {
    let server = start_server().await;
    let resp = add_group(server.base_url(), 0, 2, 1).await;
    assert_eq!(resp.status(), 201);

    let groups: Vec<Value> = client()
        .get(format!("{}/stores/0/groups", server.base_url()))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(groups.len(), 2);
}

#[tokio::test]
async fn add_group_conflict() {
    let server = start_server().await;
    let resp = add_group(server.base_url(), 0, 1, 1).await;
    assert_eq!(resp.status(), 409);
}

#[tokio::test]
async fn add_group_store_not_found() {
    let server = start_server().await;
    let resp = add_group(server.base_url(), 99, 1, 1).await;
    assert_eq!(resp.status(), 404);
}

#[tokio::test]
async fn remove_group_via_api() {
    let server = start_server().await;
    let resp = client()
        .delete(format!("{}/stores/0/groups/1", server.base_url()))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let groups: Vec<Value> = client()
        .get(format!("{}/stores/0/groups", server.base_url()))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(groups.len(), 0);
}

#[tokio::test]
async fn remove_group_not_found() {
    let server = start_server().await;
    let resp = client()
        .delete(format!("{}/stores/0/groups/99", server.base_url()))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
}

#[tokio::test]
async fn list_remote_replicas_empty() {
    let server = start_server().await;
    let resp: Value = client()
        .get(format!("{}/stores/0/groups/1/remotes", server.base_url()))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(resp["remotes"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn add_and_list_remote_replicas() {
    let server = start_server().await;
    let resp = client()
        .post(format!("{}/stores/0/groups/1/remotes", server.base_url()))
        .json(&serde_json::json!([
            {"replica_id": 2, "endpoint": "192.168.1.2:10100"},
            {"replica_id": 3, "endpoint": "192.168.1.3:10100"}
        ]))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let resp: Value = client()
        .get(format!("{}/stores/0/groups/1/remotes", server.base_url()))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(resp["remotes"].as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn add_remote_rejects_local_replica() {
    let server = start_server().await;
    let resp = client()
        .post(format!("{}/stores/0/groups/1/remotes", server.base_url()))
        .json(&serde_json::json!([{ "replica_id": 1, "endpoint": "127.0.0.1:9999" }]))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
}

#[tokio::test]
async fn remove_remote_replica() {
    let server = start_server().await;
    client()
        .post(format!("{}/stores/0/groups/1/remotes", server.base_url()))
        .json(&serde_json::json!([
            {"replica_id": 2, "endpoint": "192.168.1.2:10100"},
            {"replica_id": 3, "endpoint": "192.168.1.3:10100"}
        ]))
        .send()
        .await
        .unwrap();

    let resp = client()
        .delete(format!("{}/stores/0/groups/1/remotes/2", server.base_url()))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let resp: Value = client()
        .get(format!("{}/stores/0/groups/1/remotes", server.base_url()))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let remotes = resp["remotes"].as_array().unwrap();
    assert_eq!(remotes.len(), 1);
    assert_eq!(remotes[0]["replica_id"], 3);
}

#[tokio::test]
async fn remove_remote_rejects_local_replica() {
    let server = start_server().await;
    let resp = client()
        .delete(format!("{}/stores/0/groups/1/remotes/1", server.base_url()))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 400);
}

#[tokio::test]
async fn remove_remote_not_found() {
    let server = start_server().await;
    let resp = client()
        .delete(format!("{}/stores/0/groups/1/remotes/99", server.base_url()))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
}

#[tokio::test]
async fn step_down_via_api_flips_leader_to_follower() {
    let server = start_server().await;

    // Single-voter group (quorum 1) should self-elect quickly.
    let mut leader_id = 0u64;
    for _ in 0..40 {
        let topo: Value = client()
            .get(format!("{}/topology", server.base_url()))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        leader_id = topo["stores"][0]["groups"][0]["leader_id"].as_u64().unwrap_or(0);
        if leader_id != 0 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    assert_eq!(leader_id, 1, "single replica should self-elect as leader");

    let resp: Value = client()
        .post(format!(
            "{}/stores/0/groups/1/step-down?sync=true",
            server.base_url()
        ))
        .json(&serde_json::json!({"reason": "test"}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        resp["accepted"], true,
        "leader should accept its own step-down: {resp}"
    );
    assert_eq!(resp["current_leader_id"], 1);

    let topo: Value = client()
        .get(format!("{}/topology", server.base_url()))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let role = topo["stores"][0]["groups"][0]["local_replica"]["role"]
        .as_str()
        .unwrap();
    assert_eq!(
        role, "follower",
        "role should flip immediately, before the election driver re-elects"
    );
}

#[tokio::test]
async fn step_down_rejects_when_not_leader() {
    let server = start_server().await;

    // Wait for the single-voter group to self-elect before the first
    // step-down, so this test isn't racing the election driver's first
    // tick.
    for _ in 0..40 {
        let topo: Value = client()
            .get(format!("{}/topology", server.base_url()))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        if topo["stores"][0]["groups"][0]["leader_id"].as_u64().unwrap_or(0) != 0 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }

    // First step-down succeeds (self-elected leader) and flips to follower.
    let resp: Value = client()
        .post(format!(
            "{}/stores/0/groups/1/step-down?sync=true",
            server.base_url()
        ))
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(resp["accepted"], true);

    // Immediately retrying while still a follower must be rejected by
    // the strict fence (not leader).
    let resp: Value = client()
        .post(format!(
            "{}/stores/0/groups/1/step-down?sync=true",
            server.base_url()
        ))
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        resp["accepted"], false,
        "a follower must reject step-down: {resp}"
    );
}

#[tokio::test]
async fn step_down_group_not_found() {
    let server = start_server().await;
    let resp = client()
        .post(format!(
            "{}/stores/0/groups/99/step-down?sync=true",
            server.base_url()
        ))
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
}

#[tokio::test]
async fn flush_group_drains_local_engine() {
    let server = start_server().await;
    let resp = client()
        .post(format!("{}/stores/0/groups/1/flush", server.base_url()))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["store_id"], 0);
    assert_eq!(body["group_id"], 1);
    assert_eq!(body["accepted"], true);
}

#[tokio::test]
async fn flush_group_not_found() {
    let server = start_server().await;
    let resp = client()
        .post(format!("{}/stores/0/groups/99/flush", server.base_url()))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
}

#[tokio::test]
async fn topology_export() {
    let server = start_server().await;
    let resp: Value = client()
        .get(format!("{}/topology", server.base_url()))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let stores = resp["stores"].as_array().unwrap();
    assert_eq!(stores.len(), 1);
    assert_eq!(stores[0]["store_id"], 0);
    let groups = stores[0]["groups"].as_array().unwrap();
    assert_eq!(groups.len(), 1);
    assert_eq!(groups[0]["group_id"], 1);
    assert_eq!(groups[0]["local_replica_id"], 1);
}

#[tokio::test]
async fn topology_alias_top() {
    let server = start_server().await;
    let resp: Value = client()
        .get(format!("{}/top", server.base_url()))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(!resp["stores"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn metrics_endpoint_returns_structured_snapshot() {
    let server = start_server().await;
    let resp: Value = client()
        .get(format!("{}/metrics", server.base_url()))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    // Structure contract: window_secs, timestamp, metrics array.
    assert!(resp["window_secs"].as_f64().is_some());
    assert!(resp["timestamp"].is_string());
    let metrics = resp["metrics"].as_array().unwrap();
    // A freshly started server registers election/consensus counters;
    // the snapshot is non-empty and every point has name/kind/fields.
    assert!(!metrics.is_empty(), "expected non-empty metrics, got: {resp}");
    for m in metrics {
        assert!(m["name"].is_string(), "metric missing name: {m}");
        assert!(m["kind"].is_string(), "metric missing kind: {m}");
        let fields = m["fields"].as_array().expect("fields is array");
        assert!(!fields.is_empty(), "metric has no fields: {m}");
        for f in fields {
            assert!(f["key"].is_string());
            assert!(f["value"].as_f64().is_some());
        }
    }
}

#[tokio::test]
async fn metrics_prefix_filter_excludes_non_matching() {
    let server = start_server().await;
    // First fetch all to learn one name, then filter by a prefix that
    // matches nothing.
    let all: Value = client()
        .get(format!("{}/metrics", server.base_url()))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let all_metrics = all["metrics"].as_array().unwrap();
    assert!(!all_metrics.is_empty());
    let filtered: Value = client()
        .get(format!("{}/metrics?prefix=zzz.nonexistent.", server.base_url()))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let filtered_metrics = filtered["metrics"].as_array().unwrap();
    assert!(
        filtered_metrics.is_empty(),
        "expected empty metrics for bogus prefix, got: {filtered}"
    );
}

#[tokio::test]
async fn batch_add_remote_replicas_from_topology() {
    let server_a = start_test_server(&["--stores", "0", "--groups", "1", "--replica", "1"])
        .await
        .expect("start server A");
    let server_b = start_test_server(&["--stores", "0", "--groups", "1", "--replica", "2"])
        .await
        .expect("start server B");

    let topo_b: Value = client()
        .get(format!("{}/topology", server_b.base_url()))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    let resp = client()
        .post(format!("{}/stores/0/groups/1/remotes/batch", server_a.base_url()))
        .json(&topo_b)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let remotes: Value = client()
        .get(format!("{}/stores/0/groups/1/remotes", server_a.base_url()))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let r = remotes["remotes"].as_array().unwrap();
    assert_eq!(r.len(), 1);
    assert_eq!(r[0]["replica_id"], 2);
}

#[tokio::test]
async fn progressive_setup_multiple_stores_groups_replicas() {
    let server = start_server().await;

    for sid in 1..3u64 {
        let resp = add_store(server.base_url(), sid, 1, sid + 1).await;
        assert_eq!(resp.status(), 201, "store {sid} should be created");
        let resp = add_group(server.base_url(), sid, 1, sid + 1).await;
        assert_eq!(resp.status(), 201, "group 1 in store {sid}");
    }

    for sid in 0..3u64 {
        for gid in [2u64, 3] {
            let resp = add_group(server.base_url(), sid, gid, sid + 1).await;
            assert_eq!(resp.status(), 201, "group {gid} in store {sid}");
        }
    }

    for sid in 0..3u64 {
        let detail: Value = client()
            .get(format!("{}/stores/{sid}", server.base_url()))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(detail["groups"].as_array().unwrap().len(), 3);
    }

    let topo: Value = client()
        .get(format!("{}/topology", server.base_url()))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(topo["stores"].as_array().unwrap().len(), 3);

    let resp = client()
        .post(format!("{}/stores/0/groups/1/remotes/batch", server.base_url()))
        .json(&topo)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let remotes: Value = client()
        .get(format!("{}/stores/0/groups/1/remotes", server.base_url()))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(remotes["remotes"].as_array().unwrap().len(), 2);

    let resp = client()
        .delete(format!("{}/stores/2", server.base_url()))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    let list: Value = client()
        .get(format!("{}/stores", server.base_url()))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(list["stores"].as_array().unwrap().len(), 2);
}

// ── wipe-user-data ──────────────────────────────────────────────

use bytes::Bytes;
use common::test_client::TestKvClient;
use crowdb_kv::rpc::{KvGetRequest, KvResponse, KvSetRequest};

/// Normalize a `listen_addr` from topology (`0.0.0.0:port`) to a
/// loopback URL the test client can dial.
fn normalize_endpoint(endpoint: &str) -> String {
    endpoint
        .strip_prefix("0.0.0.0:")
        .map_or_else(|| endpoint.to_string(), |port| format!("127.0.0.1:{port}"))
}

/// Extract the store-0 RPC endpoint from a `/topology` response.
fn node_endpoint(topo: &Value) -> String {
    normalize_endpoint(
        topo["stores"][0]["listen_addr"]
            .as_str()
            .expect("store listen_addr"),
    )
}

/// Poll `/topology` until the group's `leader_id` is non-zero.
async fn wait_for_leader(base: &str, store_id: u64, group_id: u64) -> Value {
    for _ in 0..80 {
        let topo: Value = client()
            .get(format!("{base}/topology"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let lid = topo["stores"]
            .as_array()
            .and_then(|s| s.iter().find(|s| s["store_id"].as_u64() == Some(store_id)))
            .and_then(|s| s["groups"].as_array())
            .and_then(|g| g.iter().find(|g| g["group_id"].as_u64() == Some(group_id)))
            .and_then(|g| g["leader_id"].as_u64())
            .unwrap_or(0);
        if lid != 0 {
            return topo;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    panic!("no leader elected for store {store_id} group {group_id}");
}

/// Retry a KV put until it succeeds or the deadline expires. Under CI
/// load, the RPC path may not be ready immediately after the HTTP
/// topology reports a leader — the reaper times out the first request
/// before the leader's proposal handler is fully wired.
async fn kv_put_with_retry(kv: &TestKvClient, req: KvSetRequest, deadline_secs: u64) -> KvResponse {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(deadline_secs);
    loop {
        match kv.put(req.clone()).await {
            Ok(resp) => return resp.into_inner(),
            Err(e) => {
                assert!(
                    std::time::Instant::now() < deadline,
                    "kv put retry exhausted: {}",
                    e.message()
                );
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            }
        }
    }
}

/// Retry a KV get until it succeeds or the deadline expires.
async fn kv_get_with_retry(kv: &TestKvClient, req: KvGetRequest, deadline_secs: u64) -> KvResponse {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(deadline_secs);
    loop {
        match kv.get(req.clone()).await {
            Ok(resp) => return resp.into_inner(),
            Err(e) => {
                assert!(
                    std::time::Instant::now() < deadline,
                    "kv get retry exhausted: {}",
                    e.message()
                );
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            }
        }
    }
}

/// `wipe-user-data` on a populated single-replica group wipes the WAL
/// + engine user data; a subsequent get returns `not_found`, the
/// topology is unchanged (group0 preserved), and a new put succeeds
/// (the cluster re-elected a leader and is functional post-wipe).
#[tokio::test]
async fn wipe_user_data_clears_keys_and_preserves_group0() {
    const STORE: u64 = 0;
    const GROUP: u64 = 1;
    let server = start_server().await;
    let base = server.base_url();

    // Wait for the single replica to self-elect.
    let topo = wait_for_leader(base, STORE, GROUP).await;
    let endpoint = format!("http://{}", node_endpoint(&topo));
    let kv = TestKvClient::connect(endpoint).await;

    // Write a key (retry — under CI load the RPC path may not be
    // ready immediately after the HTTP topology reports a leader).
    let resp = kv_put_with_retry(
        &kv,
        KvSetRequest {
            version: 1,
            key: Bytes::from_static(b"wipe-test-key"),
            value: Bytes::from_static(b"wipe-test-value"),
            seq: 1,
            ttl_ms: 0,
            client_id: 200,
            request_id: 2001,
            request_create_ms: 20001,
            group_id: GROUP,
        },
        15,
    )
    .await;
    assert!(resp.ok, "put should succeed");

    // Verify the key is readable.
    let inner = kv_get_with_retry(
        &kv,
        KvGetRequest {
            version: 1,
            key: Bytes::from_static(b"wipe-test-key"),
            request_id: 2002,
            request_create_ms: 20002,
            group_id: GROUP,
            read_mode: 0,
            min_slot: 0,
        },
        15,
    )
    .await;
    assert!(inner.ok, "key should be found before wipe");
    assert_eq!(inner.value, Bytes::from_static(b"wipe-test-value"));

    // Wipe user data.
    let resp: Value = client()
        .post(format!("{base}/stores/{STORE}/groups/{GROUP}/wipe-user-data"))
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(resp["accepted"], true, "wipe should be accepted: {resp}");

    // Wait for re-election (the wipe steps down + recreates the group).
    let topo_after = wait_for_leader(base, STORE, GROUP).await;

    // group0 preserved: store + group still present in topology.
    assert!(
        topo_after["stores"]
            .as_array()
            .and_then(|s| s.iter().find(|s| s["store_id"].as_u64() == Some(STORE)))
            .and_then(|s| s["groups"].as_array())
            .and_then(|g| g.iter().find(|g| g["group_id"].as_u64() == Some(GROUP)))
            .is_some(),
        "store/group should still be present after wipe (group0 preserved)"
    );

    // The key is gone — get returns not_found.
    let endpoint_after = format!("http://{}", node_endpoint(&topo_after));
    let kv_after = TestKvClient::connect(endpoint_after).await;
    let inner = kv_get_with_retry(
        &kv_after,
        KvGetRequest {
            version: 1,
            key: Bytes::from_static(b"wipe-test-key"),
            request_id: 2003,
            request_create_ms: 20003,
            group_id: GROUP,
            read_mode: 0,
            min_slot: 0,
        },
        15,
    )
    .await;
    assert!(
        inner.not_found,
        "key should be gone after wipe: ok={} not_found={} value={:?}",
        inner.ok, inner.not_found, inner.value
    );

    // The cluster is functional post-wipe: a new put succeeds.
    let resp = kv_put_with_retry(
        &kv_after,
        KvSetRequest {
            version: 1,
            key: Bytes::from_static(b"wipe-post-key"),
            value: Bytes::from_static(b"wipe-post-value"),
            seq: 2,
            ttl_ms: 0,
            client_id: 200,
            request_id: 2004,
            request_create_ms: 20004,
            group_id: GROUP,
        },
        15,
    )
    .await;
    assert!(resp.ok, "put after wipe should succeed");
}

#[tokio::test]
async fn wipe_user_data_store_not_found() {
    let server = start_server().await;
    let resp = client()
        .post(format!("{}/stores/99/groups/1/wipe-user-data", server.base_url()))
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
}

#[tokio::test]
async fn wipe_user_data_group_not_found() {
    let server = start_server().await;
    let resp = client()
        .post(format!("{}/stores/0/groups/99/wipe-user-data", server.base_url()))
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);
}

/// The wipe-user-data path appears in the `OpenAPI` spec.
#[tokio::test]
async fn wipe_user_data_in_openapi() {
    let server = start_server().await;
    let resp: Value = client()
        .get(format!("{}/openapi.json", server.base_url()))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(
        resp["paths"]["/stores/{sid}/groups/{gid}/wipe-user-data"].is_object(),
        "wipe-user-data path missing from OpenAPI"
    );
}
