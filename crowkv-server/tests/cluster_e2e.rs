//! Real-process end-to-end cluster tests for crowkv-server.

mod testkit;

use crowkv::rpc::kv_service_client::KvServiceClient;
use crowkv::rpc::{KvBatchItem, KvBatchWriteRequest, KvDeleteRequest, KvSetRequest};
use serde_json::Value;

use testkit::process::{start_test_server, ServerHandle};

fn client() -> reqwest::Client {
    reqwest::Client::new()
}

struct ServerNode {
    handle: ServerHandle,
    node_id: u64,
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
            "--leader",
            "1",
        ])
        .await
        .unwrap_or_else(|e| panic!("start crowkv-server node {nid}: {e}"));
        nodes.push(ServerNode { handle, node_id: nid, replica_id });
    }
    nodes
}

async fn topology(node: &ServerNode) -> Value {
    client().get(format!("{}/topology", node.mgmt_base())).send().await.unwrap().json().await.unwrap()
}

fn node_endpoint(topo: &Value) -> String {
    normalize_endpoint(topo["stores"][0]["listen_addr"].as_str().expect("store listen_addr"))
}

fn normalize_endpoint(endpoint: &str) -> String {
    endpoint.strip_prefix("0.0.0.0:").map_or_else(|| endpoint.to_string(), |port| format!("127.0.0.1:{port}"))
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

fn group_endpoint(topo: &Value) -> String {
    topo["stores"][0]["listen_addr"].as_str().map(normalize_endpoint).expect("store listen_addr")
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
            .post(format!("{}/stores/{}/groups/{group_id}/remotes/batch", node.mgmt_base(), node.node_id))
            .json(&combined)
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 200, "batch wiring failed for node {}", node.node_id);
    }
}

async fn remotes(node: &ServerNode, group_id: u64) -> Value {
    client()
        .get(format!("{}/stores/{}/groups/{group_id}/remotes", node.mgmt_base(), node.node_id))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap()
}

#[tokio::test]
async fn e2e_three_node_cluster_kv_put_batch_delete() {
    let group_id = 1;
    let nodes = start_cluster(&[0, 1, 2], group_id).await;
    wire_topology(&nodes, group_id).await;

    for node in &nodes {
        let remotes = remotes(node, group_id).await;
        assert_eq!(remotes["remotes"].as_array().unwrap().len(), 2, "node {} should have 2 remotes", node.node_id);
    }

    let leader_topology = topology(&nodes[0]).await;
    let leader_addr = node_endpoint(&leader_topology);
    let mut kv = KvServiceClient::connect(format!("http://{leader_addr}")).await.expect("connect to leader");

    let resp = kv
        .put(KvSetRequest {
            version: 1,
            key: b"hello".to_vec(),
            value: b"world".to_vec(),
            seq: 1,
            ttl_ms: 0,
            client_id: 100,
            request_id: 1001,
            request_create_ms: 10001,
            group_id,
        })
        .await
        .expect("kv put")
        .into_inner();
    assert!(resp.ok, "put should succeed: {}", resp.error);

    let resp = kv
        .batch_write(KvBatchWriteRequest {
            version: 1,
            items: vec![
                KvBatchItem {
                    key: b"hello".to_vec(),
                    value: b"updated".to_vec(),
                    is_delete: false,
                },
                KvBatchItem {
                    key: b"foo".to_vec(),
                    value: b"bar".to_vec(),
                    is_delete: false,
                },
            ],
            seq: 2,
            client_id: 100,
            request_id: 1002,
            request_create_ms: 10002,
            group_id,
        })
        .await
        .expect("kv batch")
        .into_inner();
    assert!(resp.ok, "batch should succeed: {}", resp.error);

    let resp = kv
        .delete(KvDeleteRequest {
            version: 1,
            key: b"hello".to_vec(),
            seq: 3,
            client_id: 100,
            request_id: 1003,
            request_create_ms: 10003,
            group_id,
        })
        .await
        .expect("kv delete")
        .into_inner();
    assert!(resp.ok, "delete should succeed: {}", resp.error);
}

#[tokio::test]
async fn e2e_follower_returns_not_leader_hint() {
    let group_id = 1;
    let nodes = start_cluster(&[0, 1, 2], group_id).await;
    wire_topology(&nodes, group_id).await;

    let leader_addr = node_endpoint(&topology(&nodes[0]).await);
    let follower_addr = node_endpoint(&topology(&nodes[1]).await);
    let mut kv = KvServiceClient::connect(format!("http://{follower_addr}")).await.expect("connect to follower");

    let resp = kv
        .put(KvSetRequest {
            version: 1,
            key: b"k".to_vec(),
            value: b"v".to_vec(),
            seq: 1,
            ttl_ms: 0,
            client_id: 200,
            request_id: 2001,
            request_create_ms: 20001,
            group_id,
        })
        .await
        .expect("kv put to follower")
        .into_inner();

    assert!(!resp.ok);
    assert_eq!(resp.error, "not leader");
    assert_eq!(resp.not_leader_hint, leader_addr);
}

#[tokio::test]
async fn e2e_topology_reflects_cluster_state() {
    let group_id = 1;
    let nodes = start_cluster(&[10, 20, 30], group_id).await;

    for node in &nodes {
        let topo = topology(node).await;
        let stores = topo["stores"].as_array().unwrap();
        assert_eq!(stores.len(), 1);
        assert_eq!(stores[0]["store_id"], node.node_id);
        let groups = stores[0]["groups"].as_array().unwrap();
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0]["group_id"], group_id);
        assert_eq!(groups[0]["local_replica_id"], node.replica_id);
        assert!(group_endpoint(&topo).contains(':'));
    }
}

#[tokio::test]
async fn e2e_dynamic_group_management() {
    let group_id = 1;
    let nodes = start_cluster(&[0, 1, 2], group_id).await;
    wire_topology(&nodes, group_id).await;

    for node in &nodes {
        let resp = client()
            .post(format!("{}/stores/{}/groups", node.mgmt_base(), node.node_id))
            .json(&serde_json::json!({"group_id": 2, "replica_id": node.replica_id}))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 201);
    }

    for node in &nodes {
        let detail: Value = client()
            .get(format!("{}/stores/{}", node.mgmt_base(), node.node_id))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(detail["groups"].as_array().unwrap().len(), 2);
    }

    for node in &nodes {
        let resp = client().delete(format!("{}/stores/{}/groups/2", node.mgmt_base(), node.node_id)).send().await.unwrap();
        assert_eq!(resp.status(), 200);
    }

    for node in &nodes {
        let detail: Value = client()
            .get(format!("{}/stores/{}", node.mgmt_base(), node.node_id))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(detail["groups"].as_array().unwrap().len(), 1);
    }

    let leader_addr = node_endpoint(&topology(&nodes[0]).await);
    let mut kv = KvServiceClient::connect(format!("http://{leader_addr}")).await.unwrap();
    let resp = kv
        .put(KvSetRequest {
            version: 1,
            key: b"after-remove".to_vec(),
            value: b"still-works".to_vec(),
            seq: 1,
            ttl_ms: 0,
            client_id: 300,
            request_id: 3001,
            request_create_ms: 30001,
            group_id,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(resp.ok);
}
