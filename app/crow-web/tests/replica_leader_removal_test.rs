// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! M3 leader-removal coverage: remove the current leader from a group with
//! ≥ 3 remaining voters, exercising both the graceful `StepDown` path and
//! the lease-expiry fallback when the leader is unreachable.
//!
//! Uses the same real-subprocess harness as `cluster_restart_incremental_test.rs`
//! and `replica_routes_test.rs`.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use crow_console_shared::clients::http::ServerClient;
use crow_console_shared::cluster::NodeHealth;
use crow_console_shared::config::{NodeEntry, RackEntry, ServerEntry};
use crow_console_shared::lifecycle::{self, crow_kv_server_bin, process_is_alive, DeployRequest};
use crow_console_shared::monitor::{legacy_topology_to_node_stores, NodeRecord};
use crow_console_shared::ConsoleConfig;
use crow_web::{router, AppState};
use serde_json::json;

fn pick_free_port() -> u16 {
    crow_console_shared::test_ports::unique_test_port()
}

struct Upstream {
    node_id: String,
    pid: u32,
    mgmt_url: String,
    grpc_url: String,
}

struct Cluster {
    nodes: BTreeMap<String, Upstream>,
    web: SocketAddr,
}

impl Cluster {
    const fn sid() -> u64 {
        1
    }

    const fn gid() -> u64 {
        1
    }

    fn base_url(&self) -> String {
        format!("http://{}", self.web)
    }

    fn mgmt_url(&self, node_id: &str) -> String {
        self.nodes[node_id].mgmt_url.clone()
    }

    fn stop(&mut self) {
        for n in self.nodes.values() {
            let _ = lifecycle::stop_pid_with_timeout(n.pid, Duration::from_secs(5));
        }
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for n in self.nodes.values() {
            let _ = lifecycle::stop_pid_with_timeout(n.pid, Duration::from_secs(5));
        }
    }
}

fn tempdir(tag: &str) -> PathBuf {
    let base = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("test-logs");
    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let dir = base.join(format!("{tag}-{millis}"));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[allow(clippy::unused_async)]
async fn spawn_upstream(node_id: &str, workspace: &std::path::Path) -> Option<Upstream> {
    let bin = crow_kv_server_bin()?;
    if !bin.exists() {
        return None;
    }
    let node = NodeEntry {
        id: node_id.parse().unwrap(),
        rack_id: 1,
        host: "127.0.0.1".into(),
        ssh_port: 22,
        ssh_user: String::new(),
        ssh_key: None,
        ssh_password: None,
    };
    let req = DeployRequest {
        server_id: node_id.parse().unwrap(),
        mgmt_port: pick_free_port(),
        grpc_port: pick_free_port(),
        election_profile: Some("e2e".into()),
        binary: Some(bin),
        ..Default::default()
    };
    let node_dir = workspace.join(node_id);
    std::fs::create_dir_all(node_dir.join("bin")).unwrap();
    std::fs::create_dir_all(node_dir.join("log")).unwrap();
    let deployed = lifecycle::deploy_local_in_dir(&req, &node, &node_dir)
        .await
        .expect("deploy_local_in_dir");
    Some(Upstream {
        node_id: node_id.parse().unwrap(),
        pid: deployed.pid,
        mgmt_url: deployed.mgmt_url,
        grpc_url: deployed.grpc_url,
    })
}

async fn spawn_web(upstreams: &BTreeMap<String, Upstream>) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    let mut cfg = ConsoleConfig::default();
    cfg.racks.push(RackEntry {
        id: 1,
        name: "r1".into(),
    });
    for u in upstreams.values() {
        cfg.nodes.push(NodeEntry {
            id: u.node_id.parse().unwrap(),
            rack_id: 1,
            host: "127.0.0.1".into(),
            ssh_port: 22,
            ssh_user: String::new(),
            ssh_key: None,
            ssh_password: None,
        });
        cfg.add_server(ServerEntry {
            id: u.node_id.parse().unwrap(),
            url: u.mgmt_url.clone(),
            node_id: Some(u.node_id.parse().unwrap()),
            grpc_url: Some(u.grpc_url.clone()),
            mgmt_port: None,
            grpc_port: None,
            auto_start: true,
            binary: None,
            election_profile: None,
            pid: Some(u.pid),
        })
        .unwrap();
    }
    let state = AppState::with_config(cfg, None);

    for u in upstreams.values() {
        let client = ServerClient::new(u.mgmt_url.clone()).unwrap();
        if let Ok(stores) = client.topology().await {
            let rec = NodeRecord {
                health: NodeHealth::Up,
                last_seen_ms: 1,
                stores: legacy_topology_to_node_stores(u.node_id.parse().unwrap(), &stores),
                last_error: None,
            };
            state
                .monitor_cache
                .set_node_report(u.node_id.parse().unwrap(), rec)
                .await;
        }
    }

    tokio::spawn(async move {
        axum::serve(listener, router(state)).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    addr
}

#[allow(clippy::unused_async)]
async fn spawn_five_node_cluster(test_name: &str) -> Option<Cluster> {
    let workspace = tempdir(test_name);
    let mut nodes: BTreeMap<String, Upstream> = BTreeMap::new();
    for id in ["n1", "n2", "n3", "n4", "n5"] {
        let Some(node) = spawn_upstream(id, &workspace).await else {
            for n in nodes.into_values() {
                let _ = lifecycle::stop_pid_with_timeout(n.pid, Duration::from_secs(5));
            }
            eprintln!("skipping: crow-kv-server binary not built");
            return None;
        };
        nodes.insert(id.to_string(), node);
    }
    let web = spawn_web(&nodes).await;
    Some(Cluster { nodes, web })
}

async fn create_five_node_group(cluster: &Cluster) {
    let base = cluster.base_url();
    let http = reqwest::Client::new();
    let sid = Cluster::sid();
    let gid = Cluster::gid();

    // Initialize the system group so non-zero stores can be created.
    let resp = http
        .post(format!("{base}/api/cluster/init"))
        .json(&json!({"nodes": ["n1", "n2", "n3", "n4", "n5"]}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201, "cluster init: {:?}", resp.text().await.ok());

    let resp = http
        .post(format!("{base}/api/stores"))
        .json(&json!({"store_id": sid, "nodes": ["n1"]}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201, "create store: {:?}", resp.text().await.ok());

    let resp = http
        .post(format!("{base}/api/stores/{sid}/groups"))
        .json(&json!({"group_id": gid, "replica_id": 1, "nodes": ["n1"]}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201, "create group: {:?}", resp.text().await.ok());

    for node_id in ["n2", "n3", "n4", "n5"] {
        let resp = http
            .post(format!("{base}/api/stores/{sid}/groups/{gid}/replicas"))
            .json(&json!({"node_id": node_id, "replica_id": node_id[1..].parse::<u64>().unwrap()}))
            .send()
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            201,
            "add replica {node_id}: {:?}",
            resp.text().await.ok()
        );
    }
}

async fn wait_for_leader(cluster: &Cluster, timeout: Duration) -> Option<(u64, String)> {
    let base = cluster.base_url();
    let http = reqwest::Client::new();
    let sid = Cluster::sid();
    let gid = Cluster::gid();
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        let group: serde_json::Value = http
            .get(format!("{base}/api/stores/{sid}/groups/{gid}"))
            .send()
            .await
            .ok()?
            .json()
            .await
            .ok()?;
        if let Some(leader) = group["replicas"]
            .as_array()
            .unwrap_or(&vec![])
            .iter()
            .find(|r| r["role"] == "leader")
        {
            let rid = leader["replica_id"].as_u64()?;
            let node_id = leader["node_id"].as_str()?.to_string();
            return Some((rid, node_id));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    None
}

async fn wait_for_leader_after_removal(
    cluster: &Cluster,
    excluded_rid: u64,
    timeout: Duration,
) -> Option<(u64, String)> {
    let base = cluster.base_url();
    let http = reqwest::Client::new();
    let sid = Cluster::sid();
    let gid = Cluster::gid();
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        let group: serde_json::Value = http
            .get(format!("{base}/api/stores/{sid}/groups/{gid}"))
            .send()
            .await
            .ok()?
            .json()
            .await
            .ok()?;
        if let Some(leader) = group["replicas"]
            .as_array()
            .unwrap_or(&vec![])
            .iter()
            .find(|r| r["role"] == "leader" && r["replica_id"].as_u64() != Some(excluded_rid))
        {
            let rid = leader["replica_id"].as_u64()?;
            let node_id = leader["node_id"].as_str()?.to_string();
            return Some((rid, node_id));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    None
}

#[allow(clippy::unused_async)]
async fn assert_removed_absent_from_all(cluster: &Cluster, removed_rid: u64) {
    let sid = Cluster::sid();
    let gid = Cluster::gid();
    for u in cluster.nodes.values() {
        // The removed node no longer hosts the group (or is dead); querying
        // it for remotes is meaningless and would 404.
        if let Ok(client) = ServerClient::new(u.mgmt_url.clone()) {
            if let Ok(remotes) = client.list_remote_replicas(sid, gid).await {
                assert!(
                    !remotes.iter().any(|r| r.replica_id == removed_rid),
                    "node {} should not list removed replica {removed_rid} as remote: {remotes:?}",
                    u.node_id
                );
            }
        }
    }
}

async fn wait_for_replica_count(
    base: &str,
    http: &reqwest::Client,
    sid: u64,
    gid: u64,
    expected: usize,
    timeout: Duration,
) -> Vec<serde_json::Value> {
    let deadline = Instant::now() + timeout;
    loop {
        let list: Vec<serde_json::Value> = http
            .get(format!("{base}/api/stores/{sid}/groups/{gid}/replicas"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        if list.len() == expected {
            return list;
        }
        assert!(
            Instant::now() < deadline,
            "expected {expected} replicas, got {list:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn role_on_node(cluster: &Cluster, node_id: &str, rid: u64) -> Option<String> {
    let client = ServerClient::new(cluster.mgmt_url(node_id)).unwrap();
    let topology = client.topology().await.ok()?;
    topology
        .iter()
        .find(|s| s.store_id == Cluster::sid())
        .and_then(|s| s.groups.iter().find(|g| g.group_id == Cluster::gid()))
        .and_then(|g| {
            if g.local_replica_id == rid {
                Some(g.local_replica.role.clone())
            } else {
                None
            }
        })
}

#[tokio::test]
async fn remove_leader_from_five_node_group_elects_new_leader() {
    let Some(mut cluster) =
        spawn_five_node_cluster("remove_leader_from_five_node_group_elects_new_leader").await
    else {
        return;
    };
    create_five_node_group(&cluster).await;

    let (leader_rid, _leader_node) = wait_for_leader(&cluster, Duration::from_secs(10))
        .await
        .expect("leader should be elected in 5-node group");

    let base = cluster.base_url();
    let http = reqwest::Client::new();
    let sid = Cluster::sid();
    let gid = Cluster::gid();

    let resp = http
        .delete(format!(
            "{base}/api/stores/{sid}/groups/{gid}/replicas/{leader_rid}"
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        204,
        "delete leader replica should succeed: {:?}",
        resp.text().await.ok()
    );

    let (new_leader_rid, new_leader_node) =
        wait_for_leader_after_removal(&cluster, leader_rid, Duration::from_secs(10))
            .await
            .expect("a new leader should be elected among survivors within timeout");
    assert_ne!(
        new_leader_rid, leader_rid,
        "new leader must not be the removed replica"
    );
    assert!(
        cluster.nodes.contains_key(&new_leader_node),
        "new leader should be one of the surviving nodes"
    );

    let list = wait_for_replica_count(&base, &http, sid, gid, 4, Duration::from_secs(10)).await;
    assert!(
        !list.iter().any(|r| r["replica_id"] == leader_rid),
        "removed replica should not appear in list: {list:?}"
    );

    assert_removed_absent_from_all(&cluster, leader_rid).await;

    let role = role_on_node(&cluster, &new_leader_node, new_leader_rid)
        .await
        .expect("new leader node should report its role");
    assert_eq!(role, "leader", "survivor {new_leader_node} should be leader");

    cluster.stop();
}

#[tokio::test]
#[allow(clippy::unused_async)]
async fn remove_unreachable_leader_from_five_node_group_uses_lease_fallback() {
    let Some(mut cluster) =
        spawn_five_node_cluster("remove_unreachable_leader_from_five_node_group_uses_lease_fallback").await
    else {
        return;
    };
    create_five_node_group(&cluster).await;

    let (leader_rid, leader_node) = wait_for_leader(&cluster, Duration::from_secs(10))
        .await
        .expect("leader should be elected in 5-node group");

    // Kill the leader process before asking the console to remove it.
    // This forces the console's Step 0 `StepDown` call to fail and
    // exercises the lease-unrenewable fallback path.
    let pid = cluster.nodes[&leader_node].pid;
    let status = std::process::Command::new("kill")
        .arg("-KILL")
        .arg(pid.to_string())
        .status()
        .expect("kill leader process");
    assert!(status.success());

    let dead = std::time::Instant::now() + Duration::from_secs(5);
    while process_is_alive(pid) && std::time::Instant::now() < dead {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(!process_is_alive(pid), "leader process should be dead");

    let base = cluster.base_url();
    let http = reqwest::Client::new();
    let sid = Cluster::sid();
    let gid = Cluster::gid();

    let resp = http
        .delete(format!(
            "{base}/api/stores/{sid}/groups/{gid}/replicas/{leader_rid}"
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        204,
        "delete unreachable leader replica should succeed: {:?}",
        resp.text().await.ok()
    );

    let (new_leader_rid, new_leader_node) =
        wait_for_leader_after_removal(&cluster, leader_rid, Duration::from_secs(15))
            .await
            .expect("a new leader should be elected among survivors after lease expiry");
    assert_ne!(new_leader_rid, leader_rid);
    assert!(cluster.nodes.contains_key(&new_leader_node));

    // The console's monitor cache may still report the dead node as a
    // replica because the node is unreachable and cannot be refreshed.
    // The real safety check is that every survivor has removed the dead
    // leader from its remote list and elected a new leader.
    assert_removed_absent_from_all(&cluster, leader_rid).await;

    cluster.stop();
}
