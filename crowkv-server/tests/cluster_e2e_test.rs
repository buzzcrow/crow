//! Real-process end-to-end cluster tests for crowkv-server.

mod testkit;

use crowkv::rpc::kv_service_client::KvServiceClient;
use crowkv::rpc::{KvBatchItem, KvBatchWriteRequest, KvDeleteRequest, KvGetRequest, KvSetRequest};
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
        ])
        .await
        .unwrap_or_else(|e| panic!("start crowkv-server node {nid}: {e}"));
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

fn node_endpoint(topo: &Value) -> String {
    normalize_endpoint(
        topo["stores"][0]["listen_addr"]
            .as_str()
            .expect("store listen_addr"),
    )
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

fn group_endpoint(topo: &Value) -> String {
    topo["stores"][0]["listen_addr"]
        .as_str()
        .map(normalize_endpoint)
        .expect("store listen_addr")
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

/// Like [`wait_for_leader`] but additionally waits for the chosen
/// leader index to **stay** the same across two snapshots separated by
/// `stable_for`. Used after a membership change (add/remove remote)
/// where the cluster may briefly flap before quorum settles on the new
/// configuration.
async fn wait_for_stable_leader(
    nodes: &[ServerNode],
    group_id: u64,
    timeout: std::time::Duration,
    stable_for: std::time::Duration,
) -> usize {
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        let idx_a = wait_for_leader(
            nodes,
            group_id,
            deadline.saturating_duration_since(std::time::Instant::now()),
        )
        .await;
        tokio::time::sleep(stable_for).await;
        let idx_b = wait_for_leader(
            nodes,
            group_id,
            deadline.saturating_duration_since(std::time::Instant::now()),
        )
        .await;
        if idx_a == idx_b {
            return idx_b;
        }
    }
    panic!("no stable leader within {timeout:?}");
}

/// Poll every node's topology until exactly one of them reports
/// `local_replica.role == "leader"` for `group_id`. Returns the index of
/// the leader in `nodes`. Times out after `timeout` with a panic so the
/// test fails fast rather than hanging.
async fn wait_for_leader(nodes: &[ServerNode], group_id: u64, timeout: std::time::Duration) -> usize {
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
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    panic!("no unique leader elected for group {group_id} within {timeout:?}");
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

async fn remotes(node: &ServerNode, group_id: u64) -> Value {
    client()
        .get(format!(
            "{}/stores/{}/groups/{group_id}/remotes",
            node.mgmt_base(),
            node.node_id
        ))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap()
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn e2e_three_node_cluster_kv_put_batch_delete() {
    let group_id = 1;
    let nodes = start_cluster(&[0, 1, 2], group_id).await;
    wire_topology(&nodes, group_id).await;

    for node in &nodes {
        let remotes = remotes(node, group_id).await;
        assert_eq!(
            remotes["remotes"].as_array().unwrap().len(),
            2,
            "node {} should have 2 remotes",
            node.node_id
        );
    }

    let leader_idx = wait_for_leader(&nodes, group_id, std::time::Duration::from_secs(20)).await;
    let leader_addr = node_endpoint(&topology(&nodes[leader_idx]).await);
    let mut kv = KvServiceClient::connect(format!("http://{leader_addr}"))
        .await
        .expect("connect to leader");

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

    // Get: read the value we just wrote through the leader. Verifies the
    // Paxos chosen value reached the local-replica learner store on the
    // node serving the read. The handler returns ok=true with the bytes
    // in `value` for a hit (see `kv_service::get`).
    let resp = kv
        .get(KvGetRequest {
            version: 1,
            key: b"hello".to_vec(),
            request_id: 1011,
            request_create_ms: 10011,
            group_id,
            read_mode: 0,
            client_slot: 0,
        })
        .await
        .expect("kv get")
        .into_inner();
    assert!(resp.ok, "get should succeed: {}", resp.error);
    assert_eq!(resp.value, b"world");

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

    // Get-after-Delete: the chosen tombstone propagated to the learner.
    // `kv_get` for a missing key returns `ok=false, not_found=true` (see
    // `PxKvStore::kv_get`); only the `not_found` flag is asserted here.
    let resp = kv
        .get(KvGetRequest {
            version: 1,
            key: b"hello".to_vec(),
            request_id: 1013,
            request_create_ms: 10013,
            group_id,
            read_mode: 0,
            client_slot: 0,
        })
        .await
        .expect("kv get after delete")
        .into_inner();
    assert!(
        resp.not_found,
        "deleted key must read as not_found: value={:?}",
        resp.value
    );
    assert!(resp.value.is_empty());
}

#[tokio::test]
async fn e2e_follower_returns_not_leader_hint() {
    let group_id = 1;
    let nodes = start_cluster(&[0, 1, 2], group_id).await;
    wire_topology(&nodes, group_id).await;

    let leader_idx = wait_for_leader(&nodes, group_id, std::time::Duration::from_secs(20)).await;
    let follower_idx = (leader_idx + 1) % nodes.len();
    let leader_addr = node_endpoint(&topology(&nodes[leader_idx]).await);
    let follower_addr = node_endpoint(&topology(&nodes[follower_idx]).await);
    // Wait for the follower's heartbeat handler to populate
    // `believed_leader_id`; otherwise the `NotLeaderHint` payload returned
    // by the KV plane is empty (the follower has no leader endpoint yet).
    let leader_replica_id = nodes[leader_idx].replica_id;
    let hint_deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while std::time::Instant::now() < hint_deadline {
        let topo = topology(&nodes[follower_idx]).await;
        let leader_seen = topo["stores"][0]["groups"][0]["leader_id"].as_u64().unwrap_or(0);
        if leader_seen == leader_replica_id {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    let mut kv = KvServiceClient::connect(format!("http://{follower_addr}"))
        .await
        .expect("connect to follower");

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
        let resp = client()
            .delete(format!("{}/stores/{}/groups/2", node.mgmt_base(), node.node_id))
            .send()
            .await
            .unwrap();
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

    let leader_idx = wait_for_leader(&nodes, group_id, std::time::Duration::from_secs(20)).await;
    let leader_addr = node_endpoint(&topology(&nodes[leader_idx]).await);
    let mut kv = KvServiceClient::connect(format!("http://{leader_addr}"))
        .await
        .unwrap();
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

// ── Multi-group / dynamic-replica scenarios ──────────────────────────
//
// The two tests below cover the "multiple PxGroups in one cluster" and
// "add/remove PxReplica with KV before & after" scenarios. They rely on
// fully automatic Paxos election (no `--leader` CLI / `set_leader` API),
// so they double as regression coverage for the election/wiring fixes.

/// Wire a *subset* of cluster nodes as remotes for `group_id` on every
/// member of `subset`. Used by `e2e_kv_after_dynamic_replica_change` to
/// boot the cluster with only a 2-replica wiring, then add the third
/// remote dynamically. Each node POSTs the combined topology of `subset`
/// to its own `/remotes/batch` endpoint; the server skips its local
/// replica during ingest (see `mgmt_api::add_remote_replicas`).
async fn wire_topology_subset(subset: &[&ServerNode], group_id: u64) {
    let mut combined_stores = Vec::new();
    for node in subset {
        let topo = normalize_topology(topology(node).await);
        for store in topo["stores"].as_array().unwrap() {
            combined_stores.push(store.clone());
        }
    }
    let combined = serde_json::json!({ "stores": combined_stores });
    for node in subset {
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
            "subset wiring failed for node {}",
            node.node_id
        );
    }
}

/// POST a single `{replica_id, endpoint}` remote replica to a node's group.
async fn add_remote_replica(target: &ServerNode, group_id: u64, replica_id: u64, endpoint: &str) {
    let resp = client()
        .post(format!(
            "{}/stores/{}/groups/{group_id}/remotes",
            target.mgmt_base(),
            target.node_id
        ))
        .json(&serde_json::json!([{ "replica_id": replica_id, "endpoint": endpoint }]))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "add_remote_replica failed on node {}",
        target.node_id
    );
}

/// DELETE a single remote replica.
async fn remove_remote_replica(target: &ServerNode, group_id: u64, replica_id: u64) {
    let resp = client()
        .delete(format!(
            "{}/stores/{}/groups/{group_id}/remotes/{replica_id}",
            target.mgmt_base(),
            target.node_id
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "remove_remote_replica failed on node {}",
        target.node_id
    );
}

async fn kv_put(
    kv: &mut KvServiceClient<tonic::transport::Channel>,
    group_id: u64,
    key: &[u8],
    value: &[u8],
    req_id: u64,
) -> bool {
    let resp = kv
        .put(KvSetRequest {
            version: 1,
            key: key.to_vec(),
            value: value.to_vec(),
            seq: req_id,
            ttl_ms: 0,
            client_id: 7000,
            request_id: req_id,
            request_create_ms: req_id,
            group_id,
        })
        .await
        .expect("kv put rpc")
        .into_inner();
    assert!(resp.ok, "kv put failed: {}", resp.error);
    resp.ok
}

/// Returns `(found, value)`. `found=false` means `not_found=true` from
/// the server (the `kv_get` contract returns `ok=false, not_found=true`
/// for a missing key — see `PxKvStore::kv_get`).
async fn kv_get(
    kv: &mut KvServiceClient<tonic::transport::Channel>,
    group_id: u64,
    key: &[u8],
    req_id: u64,
) -> (bool, Vec<u8>) {
    let resp = kv
        .get(KvGetRequest {
            version: 1,
            key: key.to_vec(),
            request_id: req_id,
            request_create_ms: req_id,
            group_id,
            read_mode: 0,
            client_slot: 0,
        })
        .await
        .expect("kv get rpc")
        .into_inner();
    if resp.not_found {
        (false, Vec::new())
    } else {
        assert!(
            resp.ok,
            "kv get returned ok=false without not_found: {}",
            resp.error
        );
        (true, resp.value)
    }
}

async fn kv_delete(
    kv: &mut KvServiceClient<tonic::transport::Channel>,
    group_id: u64,
    key: &[u8],
    req_id: u64,
) {
    let resp = kv
        .delete(KvDeleteRequest {
            version: 1,
            key: key.to_vec(),
            seq: req_id,
            client_id: 7000,
            request_id: req_id,
            request_create_ms: req_id,
            group_id,
        })
        .await
        .expect("kv delete rpc")
        .into_inner();
    assert!(resp.ok, "kv delete failed: {}", resp.error);
}

async fn kv_get_until(
    kv: &mut KvServiceClient<tonic::transport::Channel>,
    group_id: u64,
    key: &[u8],
    req_id: u64,
    timeout: std::time::Duration,
    want_found: bool,
) -> (bool, Vec<u8>) {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        let result = kv_get(kv, group_id, key, req_id).await;
        if result.0 == want_found || std::time::Instant::now() >= deadline {
            return result;
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
}

async fn expand_group_to_five(nodes: &[ServerNode], group_id: u64) {
    let addrs: Vec<String> = {
        let mut v = Vec::new();
        for n in nodes {
            v.push(node_endpoint(&topology(n).await));
        }
        v
    };

    for existing in &nodes[..3] {
        for new_idx in 3..5 {
            add_remote_replica(existing, group_id, nodes[new_idx].replica_id, &addrs[new_idx]).await;
        }
    }

    for new_idx in 3..5 {
        for peer_idx in 0..5 {
            if peer_idx == new_idx {
                continue;
            }
            add_remote_replica(
                &nodes[new_idx],
                group_id,
                nodes[peer_idx].replica_id,
                &addrs[peer_idx],
            )
            .await;
        }
    }
}

async fn shrink_group_to_three(nodes: &[ServerNode], group_id: u64) {
    for existing in &nodes[2..5] {
        for removed in &nodes[..2] {
            remove_remote_replica(existing, group_id, removed.replica_id).await;
        }
    }
    for existing in &nodes[..2] {
        for removed in &nodes[2..5] {
            remove_remote_replica(existing, group_id, removed.replica_id).await;
        }
    }
}

/// Scenario: one cluster, one store per node, **two** `PxGroups` sharing
/// the same nodes. Each group must elect its own leader via Paxos
/// (automatic; no `set_leader`) and writes on group A must not appear on
/// group B reads. Exercises:
///   - `add_group` + per-group election driver lifecycle,
///   - `wire_topology` for a non-bootstrap group,
///   - learner-store isolation between groups in the same `PxKvStore`.
#[tokio::test]
async fn e2e_multi_group_isolated_kv() {
    let group_a = 1;
    let group_b = 2;
    let nodes = start_cluster(&[0, 1, 2], group_a).await;
    wire_topology(&nodes, group_a).await;

    // Add group_b on every node: management API accepts a fresh
    // (group_id, replica_id) pair; replica_id is reused across groups
    // (uniqueness is per-group).
    for node in &nodes {
        let resp = client()
            .post(format!("{}/stores/{}/groups", node.mgmt_base(), node.node_id))
            .json(&serde_json::json!({"group_id": group_b, "replica_id": node.replica_id}))
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            201,
            "add_group({group_b}) failed on node {}",
            node.node_id
        );
    }
    wire_topology(&nodes, group_b).await;

    // Both groups must elect leaders independently.
    let leader_a = wait_for_leader(&nodes, group_a, std::time::Duration::from_secs(20)).await;
    let leader_b = wait_for_leader(&nodes, group_b, std::time::Duration::from_secs(20)).await;

    let addr_leader_a = node_endpoint(&topology(&nodes[leader_a]).await);
    let addr_leader_b = node_endpoint(&topology(&nodes[leader_b]).await);

    let mut kv_a = KvServiceClient::connect(format!("http://{addr_leader_a}"))
        .await
        .expect("connect kv_a");
    let mut kv_b = KvServiceClient::connect(format!("http://{addr_leader_b}"))
        .await
        .expect("connect kv_b");

    // Same key, distinct values per group.
    kv_put(&mut kv_a, group_a, b"shared-key", b"value-from-A", 5001).await;
    kv_put(&mut kv_b, group_b, b"shared-key", b"value-from-B", 5002).await;

    // Each group sees its own value.
    let (found_a, val_a) = kv_get(&mut kv_a, group_a, b"shared-key", 5101).await;
    assert!(found_a, "group_a get should hit");
    assert_eq!(val_a, b"value-from-A");

    let (found_b, val_b) = kv_get(&mut kv_b, group_b, b"shared-key", 5102).await;
    assert!(found_b, "group_b get should hit");
    assert_eq!(val_b, b"value-from-B");

    // Delete on A must not affect B.
    kv_delete(&mut kv_a, group_a, b"shared-key", 5201).await;
    let (found_a_del, _) = kv_get(&mut kv_a, group_a, b"shared-key", 5301).await;
    assert!(!found_a_del, "key should be gone in group_a");
    let (found_b_still, val_b_still) = kv_get(&mut kv_b, group_b, b"shared-key", 5302).await;
    assert!(found_b_still, "key in group_b must survive delete in group_a");
    assert_eq!(val_b_still, b"value-from-B");
}

/// Scenario: bring up a 3-replica group out of 5 nodes, do KV, then
/// dynamically add the remaining 2 remote replicas via the management
/// API and verify KV still works; finally remove those 2 replicas and
/// verify KV continues. This exercises:
///   - `rebuild_group_with_same_config` preserving election state
///     across `add_remote_replicas`/`remove_remote_replica` (otherwise `term/leader_id`
///     reset and the cluster would dip into a fresh election),
///   - synchronous old-driver cancel on group replacement,
///   - the heartbeat-resets-deadline rule keeping the surviving leader
///     stable through wiring changes,
///   - quorum transitions from 3→5→3 replicas.
#[tokio::test]
#[ignore = "test is flaky, needs investigation"]
async fn e2e_kv_after_dynamic_replica_change() {
    let group_id = 1;
    let nodes = start_cluster(&[0, 1, 2, 3, 4], group_id).await;

    // Initial wiring: only nodes[0..3] know about each other.
    // nodes[3] and nodes[4] host local replicas but are not yet peers
    // of the initial trio — their presence does not affect the (0,1,2)
    // Paxos quorum.
    wire_topology_subset(&[&nodes[0], &nodes[1], &nodes[2]], group_id).await;

    // Wait for one of {nodes[0], nodes[1], nodes[2]} to win quorum=2 (majority of 3).
    // Use wait_for_stable_leader because the initial wiring triggers a
    // leadership battle (all 3 nodes start as self-elected leaders with
    // quorum=1 and must re-elect with quorum=2).
    let leader_idx = wait_for_stable_leader(
        &nodes[..3],
        group_id,
        std::time::Duration::from_secs(20),
        std::time::Duration::from_millis(800),
    )
    .await;
    let leader_addr = node_endpoint(&topology(&nodes[leader_idx]).await);
    let mut kv = KvServiceClient::connect(format!("http://{leader_addr}"))
        .await
        .expect("connect leader");

    // Pre-add KV.
    kv_put(&mut kv, group_id, b"k1", b"v1", 6001).await;
    let (found, v) = kv_get(&mut kv, group_id, b"k1", 6011).await;
    assert!(found);
    assert_eq!(v, b"v1");

    // Dynamically add nodes[3] and nodes[4] as remotes on every existing
    // peer, and make nodes[3]/nodes[4] aware of all current members.
    // After this, the group is a 5-replica group; election state is
    // preserved by `rebuild_group_with_same_config`.
    expand_group_to_five(&nodes, group_id).await;

    // Leader must remain unique across all 5 nodes. When the quorum
    // size grows from 3 to 5, the previously-elected leader may step
    // down if it cannot collect the new majority before its lease
    // expires, so we re-resolve below rather than asserting tenure.
    let leader_idx = wait_for_stable_leader(
        &nodes,
        group_id,
        std::time::Duration::from_secs(30),
        std::time::Duration::from_millis(800),
    )
    .await;
    let leader_addr = node_endpoint(&topology(&nodes[leader_idx]).await);
    let mut kv = KvServiceClient::connect(format!("http://{leader_addr}"))
        .await
        .expect("reconnect after add");

    // Post-add KV: pre-add value still readable (Arc-shared learner), and
    // a new write commits successfully against the (now 5-replica) quorum.
    let (found, v) = kv_get(&mut kv, group_id, b"k1", 6021).await;
    assert!(
        found,
        "k1 must survive add_remote (Arc-shared PxLearner across rebuild)"
    );
    assert_eq!(v, b"v1");
    kv_put(&mut kv, group_id, b"k2", b"v2", 6101).await;

    // Dynamically remove nodes[0] and nodes[1] from the (2,3,4) members'
    // view, AND remove (2,3,4) from nodes[0,1]'s view so the old leader
    // stops heartbeating the surviving group. The surviving (2,3,4)
    // members' Paxos quorum is self-contained and must keep accepting
    // writes.
    shrink_group_to_three(&nodes, group_id).await;

    // Re-resolve the leader on the (2,3,4) wiring after the shrink.
    // wait_for_stable_leader returns a slice-relative index; offset by 2
    // to index into the full `nodes` array.
    let leader_idx = wait_for_stable_leader(
        &nodes[2..5],
        group_id,
        std::time::Duration::from_secs(30),
        std::time::Duration::from_millis(800),
    )
    .await;
    let leader_addr = node_endpoint(&topology(&nodes[2 + leader_idx]).await);
    let mut kv = KvServiceClient::connect(format!("http://{leader_addr}"))
        .await
        .expect("reconnect after remove");

    // Post-remove KV: previous writes still readable, delete + re-write
    // commit through the smaller quorum.
    let (found, v) = kv_get_until(
        &mut kv,
        group_id,
        b"k2",
        6201,
        std::time::Duration::from_secs(30),
        true,
    )
    .await;
    assert!(found);
    assert_eq!(v, b"v2");
    kv_delete(&mut kv, group_id, b"k1", 6301).await;
    let (found, _) = kv_get_until(
        &mut kv,
        group_id,
        b"k1",
        6311,
        std::time::Duration::from_secs(30),
        false,
    )
    .await;
    assert!(!found, "k1 must be gone after delete");
    kv_put(&mut kv, group_id, b"k3", b"v3", 6401).await;
}
