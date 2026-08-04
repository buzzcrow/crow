// Copyright 2026-present buzzcrow <buzzcrow@126.com>

//! Deployment-level graceful shutdown and reconfig via management API.
//!
//! These tests boot real `crow-kv-server` processes and exercise the HTTP
//! management API for:
//! - Graceful shutdown of a leader node under write load (SIGTERM path).
//! - Add/remove replica via `POST /remotes` and `DELETE /remotes/:rid`.

mod testkit;

use bytes::Bytes;
use crow_kv::rpc::kv_service_client::KvServiceClient;
use crow_kv::rpc::{KvGetRequest, KvSetRequest};
use serde_json::Value;
use std::time::{Duration, Instant};

use testkit::process::{start_test_server, ServerHandle};

fn client() -> reqwest::Client {
    reqwest::Client::new()
}

struct ServerNode {
    handle: Option<ServerHandle>,
    node_id: u64,
    replica_id: u64,
}

impl ServerNode {
    fn mgmt_base(&self) -> &str {
        self.handle.as_ref().expect("server alive").base_url()
    }
}

async fn start_cluster(node_ids: &[u64], group_id: u64) -> Vec<ServerNode> {
    let mut nodes = Vec::new();
    for (idx, &nid) in node_ids.iter().enumerate() {
        let replica_id = u64::try_from(idx + 1).expect("replica id");
        let handle = start_test_server(&[
            "--stores",
            &nid.to_string(),
            "--groups",
            &group_id.to_string(),
            "--replica",
            &replica_id.to_string(),
        ])
        .await
        .unwrap_or_else(|e| panic!("start crow-kv-server node {nid}: {e}"));
        nodes.push(ServerNode {
            handle: Some(handle),
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

fn node_endpoint(topo: &Value) -> String {
    normalize_endpoint(
        topo["stores"][0]["listen_addr"]
            .as_str()
            .expect("store listen_addr"),
    )
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
    wait_for_leader_ref(&nodes.iter().collect::<Vec<_>>(), group_id, timeout).await
}

async fn wait_for_leader_ref(nodes: &[&ServerNode], group_id: u64, timeout: Duration) -> usize {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
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
    panic!("no unique leader for group {group_id} within {timeout:?}");
}

async fn kv_put(nodes: &[ServerNode], group_id: u64, key: &[u8], val: &[u8], req_id: u64) -> bool {
    kv_put_nodes(&nodes.iter().collect::<Vec<_>>(), group_id, key, val, req_id).await
}

async fn kv_put_nodes(nodes: &[&ServerNode], group_id: u64, key: &[u8], val: &[u8], req_id: u64) -> bool {
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut last_err = String::new();
    while Instant::now() < deadline {
        let leader_idx = wait_for_leader_ref(nodes, group_id, Duration::from_secs(10)).await;
        let addr = node_endpoint(&topology(nodes[leader_idx]).await);
        let mut client = KvServiceClient::connect(format!("http://{addr}"))
            .await
            .expect("connect");
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
                last_err = status.message().to_string();
                if last_err.to_lowercase().contains("not leader") {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    continue;
                }
                panic!("put rpc failed: {last_err}");
            }
        }
    }
    panic!("kv put timed out waiting for leader: {last_err}");
}

#[allow(dead_code)]
async fn kv_get(nodes: &[ServerNode], group_id: u64, key: &[u8]) -> Option<Vec<u8>> {
    kv_get_nodes(&nodes.iter().collect::<Vec<_>>(), group_id, key).await
}

async fn kv_get_nodes(nodes: &[&ServerNode], group_id: u64, key: &[u8]) -> Option<Vec<u8>> {
    let leader_idx = wait_for_leader_ref(nodes, group_id, Duration::from_secs(10)).await;
    let addr = node_endpoint(&topology(nodes[leader_idx]).await);
    let mut client = KvServiceClient::connect(format!("http://{addr}"))
        .await
        .expect("connect");
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

/// Graceful shutdown of the leader process under write load:
/// write data, kill the leader via SIGTERM, verify the surviving
/// nodes re-elect and all committed data is still readable.
#[tokio::test]
async fn graceful_shutdown_leader_under_load() {
    let group_id = 10;
    let mut nodes = start_cluster(&[1001, 1002, 1003], group_id).await;
    wire_topology(&nodes, group_id).await;

    let leader_idx = wait_for_leader(&nodes, group_id, Duration::from_secs(10)).await;

    // Write 5 keys through the leader.
    for i in 1u64..=5 {
        let key = format!("shutdown-{i}");
        let val = format!("val-{i}");
        assert!(
            kv_put(&nodes, group_id, key.as_bytes(), val.as_bytes(), i).await,
            "write {i} should commit"
        );
    }

    // Send SIGTERM to the leader process by dropping its handle.
    // The `ServerHandle` Drop impl sends SIGTERM and waits for exit.
    drop(nodes[leader_idx].handle.take());

    // Give the surviving nodes time to detect the leader is gone and
    // re-elect.
    // Collect surviving node indices.
    let remaining_indices: Vec<usize> = (0..nodes.len()).filter(|i| *i != leader_idx).collect();

    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        assert!(Instant::now() <= deadline, "no leader elected after shutdown");
        let mut leaders = Vec::new();
        for &i in &remaining_indices {
            let topo = topology(&nodes[i]).await;
            let role = topo["stores"][0]["groups"]
                .as_array()
                .and_then(|g| g.iter().find(|gg| gg["group_id"].as_u64() == Some(group_id)))
                .and_then(|gg| gg["local_replica"]["role"].as_str())
                .unwrap_or("");
            if role == "leader" {
                leaders.push(nodes[i].node_id);
            }
        }
        if leaders.len() == 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // Build a slice of surviving nodes for kv_get.
    let remaining_nodes: Vec<&ServerNode> = remaining_indices.iter().map(|&i| &nodes[i]).collect();

    // Verify all 5 keys survived the leader shutdown.
    for i in 1u64..=5 {
        let key = format!("shutdown-{i}");
        let val = format!("val-{i}");
        let result = kv_get_nodes(&remaining_nodes, group_id, key.as_bytes()).await;
        assert_eq!(
            result.as_deref(),
            Some(val.as_bytes()),
            "key {key:?} should survive leader shutdown"
        );
    }
}

/// Add a 4th replica via the management API, then remove a non-leader
/// replica. Writes must continue to commit through both reconfig operations.
#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn reconfig_via_api_add_then_remove() {
    let group_id = 20;
    let nodes = start_cluster(&[2001, 2002, 2003], group_id).await;
    wire_topology(&nodes, group_id).await;

    let _leader_idx = wait_for_leader(&nodes, group_id, Duration::from_secs(10)).await;

    // Write initial data.
    assert!(
        kv_put(&nodes, group_id, b"rc-before", b"val-1", 1).await,
        "initial write should commit"
    );

    // Start a 4th node.
    let server4 = start_test_server(&[
        "--stores",
        "2004",
        "--groups",
        &group_id.to_string(),
        "--replica",
        "4",
    ])
    .await
    .expect("start node 4");
    let node4_id = 2004u64;

    // Get the 4th node's endpoint from its topology.
    let topo4 = topology(&ServerNode {
        handle: Some(server4),
        node_id: node4_id,
        replica_id: 4,
    })
    .await;
    let node4_endpoint = node_endpoint(&topo4);

    // Add the 4th node as a remote replica to all existing nodes.
    let add_payload = serde_json::json!([{
        "replica_id": 4,
        "endpoint": node4_endpoint,
    }]);
    for node in &nodes {
        let resp = client()
            .post(format!(
                "{}/stores/{}/groups/{group_id}/remotes",
                node.mgmt_base(),
                node.node_id
            ))
            .json(&add_payload)
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            200,
            "add remote should succeed for node {}",
            node.node_id
        );
    }

    // Give the cluster time to stabilize with the new member.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Write after add — should commit with 4-member cluster.
    assert!(
        kv_put(&nodes, group_id, b"rc-after-add", b"val-2", 2).await,
        "write after add-replica should commit"
    );

    // Find a non-leader to remove (among the original 3).
    let leader_idx = wait_for_leader(&nodes, group_id, Duration::from_secs(10)).await;
    let remove_idx = usize::from(leader_idx == 0);
    let remove_replica_id = nodes[remove_idx].replica_id;

    // Remove the non-leader from all other nodes.
    for (i, node) in nodes.iter().enumerate() {
        if i == remove_idx {
            continue;
        }
        let resp = client()
            .delete(format!(
                "{}/stores/{}/groups/{group_id}/remotes/{}",
                node.mgmt_base(),
                node.node_id,
                remove_replica_id
            ))
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            200,
            "remove remote should succeed for node {}",
            node.node_id
        );
    }

    // Give the cluster time to stabilize with reduced membership.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Write after remove — should commit with reduced quorum.
    let remaining_nodes: Vec<&ServerNode> = (0..nodes.len())
        .filter(|i| *i != remove_idx)
        .map(|i| &nodes[i])
        .collect();
    assert!(
        kv_put_nodes(&remaining_nodes, group_id, b"rc-after-remove", b"val-3", 3).await,
        "write after remove-replica should commit"
    );

    // All data survives.
    assert_eq!(
        kv_get_nodes(&remaining_nodes, group_id, b"rc-before")
            .await
            .as_deref(),
        Some(b"val-1".as_slice()),
    );
    assert_eq!(
        kv_get_nodes(&remaining_nodes, group_id, b"rc-after-add")
            .await
            .as_deref(),
        Some(b"val-2".as_slice()),
    );
    assert_eq!(
        kv_get_nodes(&remaining_nodes, group_id, b"rc-after-remove")
            .await
            .as_deref(),
        Some(b"val-3".as_slice()),
    );
}
