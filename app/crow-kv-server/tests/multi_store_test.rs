// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Multi-store-per-node process test.
//!
//! Boots a single `crow-kv-server` process hosting multiple stores and
//! verifies KV operations route correctly to each store. Three processes are
//! booted, each hosting two stores (1 and 2); each store forms an independent
//! 3-replica group across the processes. Writes to store A must not leak into
//! store B. Mirrors the Web UI multi-store topology E2E
//! (`e2e/flows/38-multi-store-isolation.spec.ts`) at the process level.

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use crow_kv::rpc::{KvGetRequest, KvSetRequest};
use crow_kv_client::KvRpcTransport;
use serde_json::Value;

use common::process::{start_test_server_with_ports, ServerHandle};
use common::test_client::TestKvClient;

const STORE_IDS: &[u64] = &[1, 2];
const GROUP_ID: u64 = 10;
const REPLICA_IDS: &[u64] = &[1, 2, 3];

fn client() -> reqwest::Client {
    reqwest::Client::new()
}

fn mgmt_base(handle: &ServerHandle) -> &str {
    handle.base_url()
}

fn normalize_endpoint(endpoint: &str) -> String {
    endpoint
        .strip_prefix("0.0.0.0:")
        .map_or_else(|| endpoint.to_string(), |port| format!("127.0.0.1:{port}"))
}

async fn topology(handle: &ServerHandle) -> Value {
    client()
        .get(format!("{}/topology", mgmt_base(handle)))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap()
}

/// Extract a single store's status (with normalized `listen_addr`) from a
/// process topology, filtered by `store_id`.
fn store_status(topo: &Value, store_id: u64) -> Value {
    let store = topo["stores"]
        .as_array()
        .expect("stores array")
        .iter()
        .find(|s| s["store_id"].as_u64() == Some(store_id))
        .unwrap_or_else(|| panic!("store {store_id} in topology"));
    let mut store = store.clone();
    if let Some(addr) = store["listen_addr"].as_str() {
        store["listen_addr"] = Value::String(normalize_endpoint(addr));
    }
    store
}

/// The normalized gRPC endpoint for a store on a process.
async fn store_endpoint(handle: &ServerHandle, store_id: u64) -> String {
    let topo = topology(handle).await;
    store_status(&topo, store_id)["listen_addr"]
        .as_str()
        .expect("store listen_addr")
        .to_string()
}

/// Wire one store's group remotes across all processes. Builds a per-store
/// combined topology (filtered by `store_id`) so the batch endpoint — which
/// matches remotes by `group_id`, not `store_id` — does not cross-wire
/// stores that share a group id.
async fn wire_store(handles: &[ServerHandle], store_id: u64, group_id: u64) {
    let mut combined_stores = Vec::new();
    for handle in handles {
        let topo = topology(handle).await;
        combined_stores.push(store_status(&topo, store_id));
    }
    let payload = serde_json::json!({ "stores": combined_stores });
    for handle in handles {
        let resp = client()
            .post(format!(
                "{}/stores/{}/groups/{group_id}/remotes/batch",
                mgmt_base(handle),
                store_id
            ))
            .json(&payload)
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            200,
            "batch wiring failed for store {store_id} on {}",
            mgmt_base(handle)
        );
    }
}

/// Find the process index that is currently leader for `store_id`/`group_id`.
/// Returns `None` until exactly one process reports the `leader` role.
async fn store_leader_index(handles: &[ServerHandle], store_id: u64, group_id: u64) -> Option<usize> {
    let mut leaders = Vec::new();
    for (i, handle) in handles.iter().enumerate() {
        let topo = topology(handle).await;
        let role = topo["stores"]
            .as_array()
            .and_then(|s| s.iter().find(|st| st["store_id"].as_u64() == Some(store_id)))
            .and_then(|st| st["groups"].as_array())
            .and_then(|g| g.iter().find(|gg| gg["group_id"].as_u64() == Some(group_id)))
            .and_then(|gg| gg["local_replica"]["role"].as_str())
            .unwrap_or("");
        if role == "leader" {
            leaders.push(i);
        }
    }
    if leaders.len() == 1 {
        Some(leaders[0])
    } else {
        None
    }
}

async fn wait_for_store_leader(
    handles: &[ServerHandle],
    store_id: u64,
    group_id: u64,
    timeout: Duration,
) -> usize {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if let Some(idx) = store_leader_index(handles, store_id, group_id).await {
            return idx;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("no unique leader for store {store_id} group {group_id} within {timeout:?}");
}

async fn kv_put(
    handles: &[ServerHandle],
    store_id: u64,
    group_id: u64,
    key: &[u8],
    val: &[u8],
    req_id: u64,
) -> bool {
    let deadline = Instant::now() + Duration::from_secs(15);
    let transport = Arc::new(KvRpcTransport::new());
    while Instant::now() < deadline {
        let leader_idx = wait_for_store_leader(handles, store_id, group_id, Duration::from_secs(10)).await;
        let addr = store_endpoint(&handles[leader_idx], store_id).await;
        let client = TestKvClient::with_transport(Arc::clone(&transport), format!("http://{addr}"));
        match client
            .put(KvSetRequest {
                version: 1,
                key: Bytes::copy_from_slice(key),
                value: Bytes::copy_from_slice(val),
                ttl_ms: 0,
                request_id: req_id,
                request_create_ms: req_id,
                client_id: 0,
                seq: 0,
                group_id,
            })
            .await
        {
            Ok(resp) => return resp.into_inner().ok,
            Err(status) => {
                let msg = status.message().to_string();
                if msg.to_lowercase().contains("not leader") {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    continue;
                }
                panic!("put rpc failed for store {store_id}: {msg}");
            }
        }
    }
    panic!("kv put timed out for store {store_id}");
}

async fn kv_get(handles: &[ServerHandle], store_id: u64, group_id: u64, key: &[u8]) -> Option<Vec<u8>> {
    let leader_idx = wait_for_store_leader(handles, store_id, group_id, Duration::from_secs(10)).await;
    let addr = store_endpoint(&handles[leader_idx], store_id).await;
    let client = TestKvClient::connect(format!("http://{addr}")).await;
    let resp = client
        .get(KvGetRequest {
            version: 1,
            key: Bytes::copy_from_slice(key),
            request_id: 9001,
            request_create_ms: 9001,
            group_id,
            read_mode: 0,
            min_slot: 0,
        })
        .await
        .ok()?
        .into_inner();
    if resp.ok && !resp.not_found {
        Some(resp.value.to_vec())
    } else {
        None
    }
}

/// A single process hosting two stores (1 and 2), each forming an independent
/// 3-replica group across three processes. Writes to store 1 must not appear
/// in store 2, and vice versa.
#[tokio::test]
async fn multi_store_per_node_isolation() {
    // Boot three processes, each hosting stores 1 and 2 with group 10.
    let mut handles: Vec<ServerHandle> = Vec::new();
    for &replica_id in REPLICA_IDS {
        let handle = start_test_server_with_ports(
            &[
                "--stores",
                "1,2",
                "--groups",
                &GROUP_ID.to_string(),
                "--replica",
                &replica_id.to_string(),
            ],
            &[0, 0],
        )
        .await
        .unwrap_or_else(|e| panic!("start multi-store process r{replica_id}: {e}"));
        handles.push(handle);
    }

    // Wire each store's group remotes across the three processes.
    for &store_id in STORE_IDS {
        wire_store(&handles, store_id, GROUP_ID).await;
    }

    // Wait for a leader in each store's group.
    for &store_id in STORE_IDS {
        wait_for_store_leader(&handles, store_id, GROUP_ID, Duration::from_secs(10)).await;
    }

    // Write distinct keys to each store through its own leader.
    assert!(
        kv_put(&handles, 1, GROUP_ID, b"ms-a", b"val-a", 1).await,
        "write to store 1 should commit"
    );
    assert!(
        kv_put(&handles, 2, GROUP_ID, b"ms-b", b"val-b", 2).await,
        "write to store 2 should commit"
    );

    // Verify isolation: each store sees only its own key.
    assert_eq!(
        kv_get(&handles, 1, GROUP_ID, b"ms-a").await.as_deref(),
        Some(b"val-a".as_slice()),
        "store 1 should read its own key"
    );
    assert_eq!(
        kv_get(&handles, 2, GROUP_ID, b"ms-b").await.as_deref(),
        Some(b"val-b".as_slice()),
        "store 2 should read its own key"
    );

    // Cross-store isolation: store 1 must not see store 2's key, and vice versa.
    assert_eq!(
        kv_get(&handles, 1, GROUP_ID, b"ms-b").await,
        None,
        "store 1 must not see store 2's key"
    );
    assert_eq!(
        kv_get(&handles, 2, GROUP_ID, b"ms-a").await,
        None,
        "store 2 must not see store 1's key"
    );
}
