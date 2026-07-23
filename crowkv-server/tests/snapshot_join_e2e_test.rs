// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Real-process end-to-end test for the new-member snapshot-join mgmt API
//!: a brand-new store
//! joins an already-populated group by pulling a snapshot via
//! `POST.../join`, instead of replaying full Paxos history from slot 1.

mod testkit;

use std::time::{Duration, Instant};

use bytes::Bytes;
use crowkv::rpc::kv_service_client::KvServiceClient;
use crowkv::rpc::{KvGetRequest, KvSetRequest};
use serde_json::Value;

use testkit::process::{start_test_server, ServerHandle};

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

fn client() -> reqwest::Client {
    reqwest::Client::new()
}

fn normalize_endpoint(endpoint: &str) -> String {
    endpoint
        .strip_prefix("0.0.0.0:")
        .map_or_else(|| endpoint.to_string(), |port| format!("127.0.0.1:{port}"))
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

async fn start_cluster_with_group(node_ids: &[u64], group_id: u64) -> Vec<ServerNode> {
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

/// Start a store with **no** group -- mirrors `main.rs`'s "`--stores`
/// without `--groups`" empty-boot path, so the group for `join_group_id`
/// gets created entirely through `POST.../join` instead.
async fn start_bare_store(node_id: u64) -> ServerNode {
    let handle = start_test_server(&["--stores", &node_id.to_string()])
        .await
        .unwrap_or_else(|e| panic!("start crowkv-server bare store {node_id}: {e}"));
    ServerNode {
        handle,
        node_id,
        replica_id: 0, // set by the caller once the /join replica_id is known
    }
}

async fn wait_for_leader(nodes: &[&ServerNode], group_id: u64, timeout: Duration) -> usize {
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
    panic!("no unique leader elected for group {group_id} within {timeout:?}");
}

async fn add_remote_replica(
    target: &ServerNode,
    group_id: u64,
    replica_id: u64,
    endpoint: &str,
    voting: bool,
) {
    add_remote_replicas(target, group_id, &[(replica_id, endpoint, voting)]).await;
}

/// Batched `POST.../remotes` with every entry in one HTTP call -- the
/// convention `crowkv-console/web/src/mgmt.rs::http_add_replica` uses to
/// wire a freshly-joined replica's own view of every already-established
/// peer in a single request (mirrored here rather than one call per
/// peer): a brand-new replica's *first-ever* remote wiring must land as
/// one bootstrap batch, since the server only recognizes "this replica
/// has no remote history yet, so this isn't a membership change" on a
/// still-empty remote list (the exact-match
/// epoch fence) -- splitting it into N separate single-entry calls would
/// let calls 2..N look like genuine post-bootstrap voting-set changes
/// and bump this replica's epoch out from under it, permanently
/// desyncing it from peers that never bump for a non-voting add.
async fn add_remote_replicas(target: &ServerNode, group_id: u64, remotes: &[(u64, &str, bool)]) {
    let body: Vec<Value> = remotes
        .iter()
        .map(|(replica_id, endpoint, voting)| {
            serde_json::json!({ "replica_id": replica_id, "endpoint": endpoint, "voting": voting })
        })
        .collect();
    let resp = client()
        .post(format!(
            "{}/stores/{}/groups/{group_id}/remotes",
            target.mgmt_base(),
            target.node_id
        ))
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "add_remote_replicas failed on node {}",
        target.node_id
    );
}

async fn kv_put(
    kv: &mut KvServiceClient<tonic::transport::Channel>,
    group_id: u64,
    key: &[u8],
    value: &[u8],
    req_id: u64,
) {
    let resp = kv
        .put(KvSetRequest {
            version: 1,
            key: Bytes::copy_from_slice(key),
            value: Bytes::copy_from_slice(value),
            seq: req_id,
            ttl_ms: 0,
            client_id: 9000,
            request_id: req_id,
            request_create_ms: req_id,
            group_id,
        })
        .await
        .expect("kv put rpc")
        .into_inner();
    assert!(resp.ok, "kv put failed: {}", resp.error);
}

/// Local (non-forwarded, `read_mode: MIN_SLOT`) get against whichever
/// node `kv` is connected to -- works regardless of that node's
/// voting/leadership status (unlike the default `LINEARIZABLE` mode, which
/// redirects a non-leader to the leader), which is what lets this test
/// verify state on a still-non-voting newly-joined replica.
async fn kv_get_local_until(
    kv: &mut KvServiceClient<tonic::transport::Channel>,
    group_id: u64,
    key: &[u8],
    req_id: u64,
    timeout: Duration,
) -> Option<Vec<u8>> {
    let deadline = Instant::now() + timeout;
    loop {
        let resp = kv
            .get(KvGetRequest {
                version: 1,
                key: Bytes::copy_from_slice(key),
                request_id: req_id,
                request_create_ms: req_id,
                group_id,
                read_mode: 1, // MIN_SLOT
                min_slot: 0,
            })
            .await
            .expect("kv get rpc")
            .into_inner();
        if !resp.not_found {
            assert!(
                resp.ok,
                "kv get returned ok=false without not_found: {}",
                resp.error
            );
            return Some(resp.value.to_vec());
        }
        if Instant::now() >= deadline {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

#[tokio::test]
async fn e2e_new_member_joins_via_snapshot_then_catches_up_wal_tail() {
    let group_id = 1;
    let nodes = start_cluster_with_group(&[0, 1], group_id).await;
    // 2-node group: wire both as each other's voting remote.
    for a in &nodes {
        for b in &nodes {
            if a.node_id == b.node_id {
                continue;
            }
            let b_addr = node_endpoint(&topology(b).await);
            add_remote_replica(a, group_id, b.replica_id, &b_addr, true).await;
        }
    }

    let refs: Vec<&ServerNode> = nodes.iter().collect();
    let leader_idx = wait_for_leader(&refs, group_id, Duration::from_secs(20)).await;
    let leader_addr = node_endpoint(&topology(&nodes[leader_idx]).await);
    let mut leader_kv = KvServiceClient::connect(format!("http://{leader_addr}"))
        .await
        .expect("connect leader");

    // Pre-join writes: the new member must recover these via snapshot, not
    // via live Paxos repair (it's never wired into the topology until
    // after the join call below).
    kv_put(&mut leader_kv, group_id, b"k1", b"v1", 9001).await;
    kv_put(&mut leader_kv, group_id, b"k2", b"v2", 9002).await;

    // A brand-new store, no group yet.
    let mut new_node = start_bare_store(2).await;
    new_node.replica_id = 9;

    let join_resp = client()
        .post(format!(
            "{}/stores/{}/groups/{group_id}/join",
            new_node.mgmt_base(),
            new_node.node_id
        ))
        .json(&serde_json::json!({
            "replica_id": new_node.replica_id,
            "peer_endpoint": leader_addr,
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        join_resp.status(),
        201,
        "join failed: {}",
        join_resp.text().await.unwrap_or_default()
    );

    // Snapshot-imported state must already be visible on the new node,
    // with no topology wiring and no heartbeat catch-up having happened
    // yet.
    let mut new_kv =
        KvServiceClient::connect(format!("http://{}", node_endpoint(&topology(&new_node).await)))
            .await
            .expect("connect new node");
    assert_eq!(
        kv_get_local_until(&mut new_kv, group_id, b"k1", 9101, Duration::from_secs(1)).await,
        Some(b"v1".to_vec()),
        "k1 should already be present via snapshot import alone"
    );
    assert_eq!(
        kv_get_local_until(&mut new_kv, group_id, b"k2", 9102, Duration::from_secs(1)).await,
        Some(b"v2".to_vec()),
        "k2 should already be present via snapshot import alone"
    );

    // Wire the new member into the topology: existing members as its
    // voting remotes, and itself as a non-voting remote on every existing
    // member (design: catch up before promoting to
    // voting).
    let new_addr = node_endpoint(&topology(&new_node).await);
    let mut existing_addrs = Vec::new();
    for existing in &nodes {
        existing_addrs.push((existing.replica_id, node_endpoint(&topology(existing).await)));
    }
    // One batched call, not one per peer -- see `add_remote_replicas`'s
    // doc comment for why splitting this into N single-entry calls would
    // desync the new replica's epoch from the rest of the cluster.
    let new_node_remotes: Vec<(u64, &str, bool)> = existing_addrs
        .iter()
        .map(|(rid, addr)| (*rid, addr.as_str(), true))
        .collect();
    add_remote_replicas(&new_node, group_id, &new_node_remotes).await;
    for existing in &nodes {
        add_remote_replica(existing, group_id, new_node.replica_id, &new_addr, false).await;
    }

    // Post-join write: the new (non-voting) member must catch up on just
    // the WAL tail above the snapshot's at_slot via normal heartbeat
    // repair, not a full replay.
    kv_put(&mut leader_kv, group_id, b"k3", b"v3", 9003).await;
    assert_eq!(
        kv_get_local_until(&mut new_kv, group_id, b"k3", 9103, Duration::from_secs(20)).await,
        Some(b"v3".to_vec()),
        "k3 (written after join) should reach the new member via WAL-tail catch-up"
    );
}
