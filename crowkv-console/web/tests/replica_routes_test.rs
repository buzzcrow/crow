// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! A7 web e2e: exercise the orchestrated replica plane against two
//! real `crowkv-server` upstreams via the console web backend.
//!
//! Flow:
//!   1. Spawn two `crowkv-server` processes (n1, n2).
//!   2. Boot `crowkv-web` with both nodes in the config; seed the
//!      monitor cache from each node's topology.
//!   3. POST /api/stores to create empty store 7 on n1.
//!   4. POST /api/stores/7/groups to create group 70 with replica 1 on n1.
//!   5. POST /api/stores/7/groups/70/replicas to add replica 2 on n2.
//!      - List replicas → 2 entries, one per node.
//!      - Physical detail on n1 lists n2's replica as a remote and
//!        vice versa (round-trip proves bidirectional wiring).
//!   5. DELETE the replica on n2 → list drops back to 1 entry; n1's
//!      physical `remotes` list is empty again.
//!   6. DELETE a non-existent replica → 404.
//!
//! Skips silently when the `crowkv-server` binary is not built
//! (matches the pattern in `mgmt_routes_test.rs` / `kv_routes_test.rs`).

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use crowkv_console_shared::clients::http::ServerClient;
use crowkv_console_shared::cluster::NodeHealth;
use crowkv_console_shared::config::{NodeEntry, RackEntry, ServerEntry};
use crowkv_console_shared::lifecycle::{self, crowkv_server_bin, DeployRequest};
use crowkv_console_shared::monitor::{legacy_topology_to_node_stores, NodeRecord};
use crowkv_console_shared::ConsoleConfig;
use crowkv_web::{router, AppState};
use serde_json::json;

fn pick_free_port() -> u16 {
    crowkv_console_shared::test_ports::unique_test_port()
}

struct Upstream {
    node_id: String,
    pid: u32,
    mgmt_url: String,
    grpc_url: String,
}

struct ProcessGuard {
    pids: BTreeMap<String, u32>,
}

impl Drop for ProcessGuard {
    fn drop(&mut self) {
        for pid in self.pids.values() {
            let _ = lifecycle::stop_pid_with_timeout(*pid, Duration::from_secs(5));
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

async fn spawn_upstream(node_id: &str, workspace: &std::path::Path) -> Option<Upstream> {
    let bin = crowkv_server_bin()?;
    if !bin.exists() {
        return None;
    }
    let node = NodeEntry {
        id: node_id.into(),
        rack_id: "r1".into(),
        host: "127.0.0.1".into(),
        ssh_port: 22,
        ssh_user: String::new(),
        ssh_key: None,
        ssh_password: None,
    };
    let req = DeployRequest {
        server_id: node_id.into(),
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
        node_id: node_id.into(),
        pid: deployed.pid,
        mgmt_url: deployed.mgmt_url,
        grpc_url: deployed.grpc_url,
    })
}

async fn spawn_web(upstreams: &[Upstream]) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    let mut cfg = ConsoleConfig::default();
    cfg.racks.push(RackEntry {
        id: "r1".into(),
        name: "r1".into(),
    });
    for u in upstreams {
        cfg.nodes.push(NodeEntry {
            id: u.node_id.clone(),
            rack_id: "r1".into(),
            host: "127.0.0.1".into(),
            ssh_port: 22,
            ssh_user: String::new(),
            ssh_key: None,
            ssh_password: None,
        });
        cfg.add_server(ServerEntry {
            id: u.node_id.clone(),
            url: u.mgmt_url.clone(),
            node_id: Some(u.node_id.clone()),
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

    // Seed the monitor cache from each upstream's topology so the
    // initial GETs work before the first mutation triggers a refresh.
    for u in upstreams {
        let client = ServerClient::new(u.mgmt_url.clone()).unwrap();
        if let Ok(stores) = client.topology().await {
            let rec = NodeRecord {
                health: NodeHealth::Up,
                last_seen_ms: 1,
                stores: legacy_topology_to_node_stores(&u.node_id, &stores),
                last_error: None,
            };
            state.monitor_cache.set_node_report(u.node_id.clone(), rec).await;
        }
    }

    tokio::spawn(async move {
        axum::serve(listener, router(state)).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    addr
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn replica_add_remove_wires_peers_bidirectionally() {
    let workspace = tempdir("replica_routes");
    let mut guard = ProcessGuard {
        pids: BTreeMap::new(),
    };
    let Some(n1) = spawn_upstream("n1", &workspace).await else {
        eprintln!("skipping: crowkv-server binary not built");
        return;
    };
    guard.pids.insert("n1".into(), n1.pid);
    let Some(n2) = spawn_upstream("n2", &workspace).await else {
        eprintln!("skipping: crowkv-server binary not built");
        return;
    };
    guard.pids.insert("n2".into(), n2.pid);
    let n1_mgmt = n1.mgmt_url.clone();
    let n2_mgmt = n2.mgmt_url.clone();

    let web = spawn_web(&[n1, n2]).await;
    let base = format!("http://{web}");
    let http = reqwest::Client::new();

    let sid: u64 = 7;
    let gid: u64 = 70;
    let rid1: u64 = 1;
    let rid2: u64 = 2;

    // 1. Create an empty store on n1.
    let resp = http
        .post(format!("{base}/api/stores"))
        .json(&json!({"store_id": sid, "nodes": ["n1"]}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201, "create store: {:?}", resp.text().await.ok());

    // 2. Create the initial group on n1 with replica 1.
    let resp = http
        .post(format!("{base}/api/stores/{sid}/groups"))
        .json(&json!({"group_id": gid, "replica_id": rid1, "nodes": ["n1"]}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201, "create group: {:?}", resp.text().await.ok());

    // 3. Initial replica list has exactly one entry on n1.
    let list: Vec<serde_json::Value> = http
        .get(format!("{base}/api/stores/{sid}/groups/{gid}/replicas"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(list.len(), 1, "expected 1 initial replica, got {list:?}");
    assert_eq!(list[0]["replica_id"], rid1);
    assert_eq!(list[0]["node_id"], "n1");

    // 4. Add replica 2 on n2 → orchestrated bidirectional wiring.
    let resp = http
        .post(format!("{base}/api/stores/{sid}/groups/{gid}/replicas"))
        .json(&json!({"node_id": "n2", "replica_id": rid2}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201, "add replica: {:?}", resp.text().await.ok());

    // 5. List now shows both replicas.
    let list: Vec<serde_json::Value> = http
        .get(format!("{base}/api/stores/{sid}/groups/{gid}/replicas"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(list.len(), 2, "expected 2 replicas after add, got {list:?}");
    let nodes: Vec<&str> = list.iter().map(|r| r["node_id"].as_str().unwrap()).collect();
    assert!(nodes.contains(&"n1") && nodes.contains(&"n2"), "{nodes:?}");

    // 6. Physical tree must show bidirectional remotes:
    //    n1's group has r2 as a remote, n2's group has r1 as a remote.
    //    Query the upstreams directly to prove the orchestration wired
    //    them, not just the console cache.
    let c1 = ServerClient::new(n1_mgmt.clone()).unwrap();
    let c2 = ServerClient::new(n2_mgmt.clone()).unwrap();
    let r1_remotes = c1.list_remote_replicas(sid, gid).await.unwrap();
    let r2_remotes = c2.list_remote_replicas(sid, gid).await.unwrap();
    assert!(
        r1_remotes.iter().any(|r| r.replica_id == rid2),
        "n1 should list r2 as remote: {r1_remotes:?}"
    );
    assert!(
        r2_remotes.iter().any(|r| r.replica_id == rid1),
        "n2 should list r1 as remote: {r2_remotes:?}"
    );

    // 7. GET single replica detail.
    let resp = http
        .get(format!("{base}/api/stores/{sid}/groups/{gid}/replicas/{rid2}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let detail: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(detail["replica_id"], rid2);
    assert_eq!(detail["node_id"], "n2");
    assert!(
        matches!(detail["role"].as_str(), Some("leader" | "follower")),
        "new replica should report a valid role: {detail}"
    );

    // Wait for leader election to complete (2-node group needs a moment).
    let leader_rid = {
        let mut leader = None;
        for _ in 0..20 {
            let group: serde_json::Value = http
                .get(format!("{base}/api/stores/{sid}/groups/{gid}"))
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            leader = group["replicas"]
                .as_array()
                .unwrap()
                .iter()
                .find(|r| r["role"] == "leader")
                .and_then(|r| r["replica_id"].as_u64());
            if leader.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        leader.expect("group should have a leader before deletion")
    };

    // 8. DELETE of a non-existent replica → 404.
    let resp = http
        .delete(format!("{base}/api/stores/{sid}/groups/{gid}/replicas/999"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);

    // 9. DELETE the current leader replica.
    let resp = http
        .delete(format!(
            "{base}/api/stores/{sid}/groups/{gid}/replicas/{leader_rid}"
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 204);

    // 10. List drops back to 1 entry.
    let list: Vec<serde_json::Value> = http
        .get(format!("{base}/api/stores/{sid}/groups/{gid}/replicas"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(list.len(), 1, "expected 1 replica after remove, got {list:?}");
    let surviving_rid = list[0]["replica_id"].as_u64().unwrap();
    let surviving_node = list[0]["node_id"].as_str().unwrap();

    let surviving_client = if surviving_node == "n1" { &c1 } else { &c2 };
    let r1_remotes_after = surviving_client.list_remote_replicas(sid, gid).await.unwrap();
    assert!(
        !r1_remotes_after.iter().any(|r| r.replica_id == leader_rid),
        "n1 should no longer list removed leader as remote: {r1_remotes_after:?}"
    );

    // After the leader is removed, the surviving replica is the lone
    // voter (quorum recomputes to 1), so it must re-elect itself. The
    // election walks Follower -> PreCandidate -> Candidate -> Leader, and
    // `topology()` reports the *raw* role string (including the transient
    // "pre_candidate" / "candidate" states). Poll until the re-election
    // converges to "leader" rather than racing a single snapshot.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let final_role = loop {
        let surviving_topology = surviving_client.topology().await.unwrap();
        let surviving_group = surviving_topology
            .iter()
            .find(|store| store.store_id == sid)
            .and_then(|store| store.groups.iter().find(|group| group.group_id == gid))
            .expect("surviving node should still host the group after replica removal");
        assert_eq!(surviving_group.local_replica_id, surviving_rid);
        let role = surviving_group.local_replica.role.clone();
        if role == "leader" || std::time::Instant::now() >= deadline {
            break role;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    };
    assert_eq!(
        final_role, "leader",
        "lone surviving replica should re-elect itself as leader after the leader was removed"
    );

    // Cleanup is handled by ProcessGuard Drop.
}
