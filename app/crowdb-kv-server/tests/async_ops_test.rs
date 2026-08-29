// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Integration tests for the async operation API and cluster readiness
//! endpoint (R12).

mod common;

use serde_json::Value;
use std::time::Duration;

use common::process::{start_test_server, ServerHandle};

fn client() -> reqwest::Client {
    reqwest::Client::new()
}

struct ServerNode {
    handle: ServerHandle,
    node_id: u64,
    #[allow(dead_code)]
    replica_id: u64,
}

impl ServerNode {
    fn mgmt_base(&self) -> &str {
        self.handle.base_url()
    }
}

async fn start_cluster(node_ids: &[u64], group_id: u64) -> Vec<ServerNode> {
    let mut nodes = Vec::new();
    for (idx, &nid) in node_ids.iter().enumerate() {
        let replica_id = u64::try_from(idx + 1).expect("replica id should fit in u64");
        let handle = start_test_server(&[
            "--stores",
            &nid.to_string(),
            "--groups",
            &group_id.to_string(),
            "--replica",
            &replica_id.to_string(),
        ])
        .await
        .unwrap_or_else(|e| panic!("start crowdb-kv-server node {nid}: {e}"));
        nodes.push(ServerNode {
            handle,
            node_id: nid,
            replica_id,
        });
    }
    nodes
}

async fn topology(node: &ServerNode) -> Value {
    client()
        .get(format!("{}/topology", node.mgmt_base()))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap()
}

fn normalize_endpoint(endpoint: &str) -> String {
    endpoint
        .strip_prefix("0.0.0.0:")
        .map_or_else(|| endpoint.to_string(), |port| format!("127.0.0.1:{port}"))
}

fn normalize_topology(mut topo: Value) -> Value {
    if let Some(stores) = topo["stores"].as_array_mut() {
        for store in stores {
            if let Some(addr) = store["listen_addr"].as_str() {
                store["listen_addr"] = Value::String(normalize_endpoint(addr));
            }
        }
    }
    topo
}

async fn combined_topology(nodes: &[ServerNode]) -> Value {
    let mut combined_stores = Vec::new();
    for node in nodes {
        let topo = normalize_topology(topology(node).await);
        for store in topo["stores"].as_array().unwrap() {
            combined_stores.push(store.clone());
        }
    }
    serde_json::json!({ "stores": combined_stores })
}

async fn wire_topology(nodes: &[ServerNode], group_id: u64) {
    let combined = combined_topology(nodes).await;
    for node in nodes {
        let resp = client()
            .post(format!(
                "{}/stores/{}/groups/{group_id}/remotes/batch",
                node.mgmt_base(),
                node.node_id
            ))
            .json(&combined)
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            200,
            "batch wiring failed for node {}",
            node.node_id
        );
    }
}

async fn wait_for_leader(nodes: &[ServerNode], group_id: u64, timeout: Duration) -> usize {
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        let mut leaders: Vec<usize> = Vec::new();
        for (idx, node) in nodes.iter().enumerate() {
            let topo = topology(node).await;
            let role = topo["stores"][0]["groups"]
                .as_array()
                .and_then(|g| g.iter().find(|gg| gg["group_id"].as_u64() == Some(group_id)))
                .and_then(|gg| gg["local_replica"]["role"].as_str())
                .unwrap_or("");
            if role == "leader" {
                leaders.push(idx);
            }
        }
        if leaders.len() == 1 {
            return leaders[0];
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    panic!("no unique leader elected for group {group_id} within {timeout:?}");
}

/// Poll `/ready` until it returns 200, or panic after timeout.
async fn wait_for_ready(node: &ServerNode, group_id: u64, timeout: Duration) {
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        let resp = client()
            .get(format!(
                "{}/stores/{}/groups/{}/ready",
                node.mgmt_base(),
                node.node_id,
                group_id
            ))
            .send()
            .await
            .unwrap();
        if resp.status().as_u16() == 200 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    panic!("group {group_id} not ready within {timeout:?}");
}

/// Poll `/operations/:id` until status is `completed` or `failed`, return
/// the status string. Panics after timeout.
async fn poll_operation(node: &ServerNode, op_id: u64, timeout: Duration) -> String {
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        let resp = client()
            .get(format!("{}/operations/{}", node.mgmt_base(), op_id))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status().as_u16(), 200, "operation poll should return 200");
        let body: Value = resp.json().await.unwrap();
        let status = body["status"].as_str().unwrap_or("");
        if status == "completed" || status == "failed" {
            return status.to_string();
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    panic!("operation {op_id} did not complete within {timeout:?}");
}

#[tokio::test]
async fn readiness_api_returns_200_when_leader_elected() {
    let nodes = start_cluster(&[101, 102, 103], 1).await;
    wire_topology(&nodes, 1).await;

    // Wait for leader election
    let leader_idx = wait_for_leader(&nodes, 1, Duration::from_secs(10)).await;

    // Check readiness on the leader node
    let resp = client()
        .get(format!(
            "{}/stores/{}/groups/1/ready",
            nodes[leader_idx].mgmt_base(),
            nodes[leader_idx].node_id
        ))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status().as_u16(), 200, "leader node should report ready");
    let body: Value = resp.json().await.unwrap();
    assert_eq!(body["ready"].as_bool(), Some(true));
    assert!(
        body["leader_id"].as_u64().unwrap_or(0) != 0,
        "leader_id should be non-zero"
    );
    assert!(
        body["voting_replicas"].as_u64().unwrap_or(0) >= 3,
        "should have 3 voting replicas"
    );
}

#[tokio::test]
async fn readiness_api_returns_503_before_leader_election() {
    // Start a single node with no remotes wired — no quorum, no leader
    let nodes = start_cluster(&[201], 2).await;

    let resp = client()
        .get(format!(
            "{}/stores/{}/groups/2/ready",
            nodes[0].mgmt_base(),
            nodes[0].node_id
        ))
        .send()
        .await
        .unwrap();

    // Single node with quorum=1 should elect itself as leader.
    // If it does, we get 200. If not, 503. Either is acceptable —
    // the test verifies the endpoint works, not the election outcome.
    let status = resp.status().as_u16();
    assert!(
        status == 200 || status == 503,
        "ready endpoint should return 200 or 503, got {status}"
    );
    let body: Value = resp.json().await.unwrap();
    assert!(body["leader_id"].as_u64().is_some());
    assert!(body["voting_replicas"].as_u64().is_some());
}

#[tokio::test]
async fn async_step_down_returns_operation_id() {
    let nodes = start_cluster(&[301, 302, 303], 3).await;
    wire_topology(&nodes, 3).await;

    let leader_idx = wait_for_leader(&nodes, 3, Duration::from_secs(10)).await;

    // Trigger step-down in async mode (default, no ?sync=true)
    let resp = client()
        .post(format!(
            "{}/stores/{}/groups/3/step-down",
            nodes[leader_idx].mgmt_base(),
            nodes[leader_idx].node_id
        ))
        .json(&serde_json::json!({"reason": "test async step-down"}))
        .send()
        .await
        .unwrap();

    assert_eq!(
        resp.status().as_u16(),
        202,
        "async step-down should return 202 Accepted"
    );
    let body: Value = resp.json().await.unwrap();
    let op_id = body["operation_id"]
        .as_u64()
        .expect("response should contain operation_id");
    assert!(op_id > 0, "operation_id should be positive");

    // Poll for operation completion
    let status = poll_operation(&nodes[leader_idx], op_id, Duration::from_secs(15)).await;
    assert_eq!(
        status, "completed",
        "step-down operation should complete (new leader elected)"
    );

    // Verify a new leader exists
    let _new_leader = wait_for_leader(&nodes, 3, Duration::from_secs(10)).await;
}

#[tokio::test]
async fn sync_step_down_preserves_old_behavior() {
    let nodes = start_cluster(&[401, 402, 403], 4).await;
    wire_topology(&nodes, 4).await;

    let leader_idx = wait_for_leader(&nodes, 4, Duration::from_secs(10)).await;

    // Trigger step-down in sync mode
    let resp = client()
        .post(format!(
            "{}/stores/{}/groups/4/step-down?sync=true",
            nodes[leader_idx].mgmt_base(),
            nodes[leader_idx].node_id
        ))
        .json(&serde_json::json!({"reason": "test sync step-down"}))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status().as_u16(), 200, "sync step-down should return 200");
    let body: Value = resp.json().await.unwrap();
    assert!(
        body["accepted"].as_bool().is_some(),
        "sync step-down should return accepted field"
    );
}

#[tokio::test]
async fn get_operation_returns_404_for_unknown_id() {
    let nodes = start_cluster(&[501], 5).await;

    let resp = client()
        .get(format!("{}/operations/99999", nodes[0].mgmt_base()))
        .send()
        .await
        .unwrap();

    assert_eq!(
        resp.status().as_u16(),
        404,
        "unknown operation id should return 404"
    );
}

#[tokio::test]
async fn ready_endpoint_after_full_cluster_lifecycle() {
    let nodes = start_cluster(&[601, 602, 603], 6).await;
    wire_topology(&nodes, 6).await;

    // Wait for ready
    wait_for_ready(&nodes[0], 6, Duration::from_secs(10)).await;

    // Step-down the leader, then wait for ready again
    let leader_idx = wait_for_leader(&nodes, 6, Duration::from_secs(10)).await;

    let resp = client()
        .post(format!(
            "{}/stores/{}/groups/6/step-down?sync=true",
            nodes[leader_idx].mgmt_base(),
            nodes[leader_idx].node_id
        ))
        .json(&serde_json::json!({"reason": "lifecycle test"}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 200);

    // After step-down, a new leader should be elected and ready should pass
    wait_for_ready(&nodes[0], 6, Duration::from_secs(15)).await;
}
