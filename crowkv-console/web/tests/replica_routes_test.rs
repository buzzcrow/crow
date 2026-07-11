//! A7 web e2e: exercise the orchestrated replica plane against two
//! real `crowkv-server` upstreams via the console web backend.
//!
//! Flow:
//!   1. Spawn two `crowkv-server` processes (n1, n2).
//!   2. Boot `crowkv-web` with both nodes in the config; seed the
//!      monitor cache from each node's topology.
//!   3. POST /api/stores to create store 7 on n1 with replica 1.
//!   4. POST /api/stores/7/groups/70/replicas to add replica 2 on n2.
//!      - List replicas → 2 entries, one per node.
//!      - Physical detail on n1 lists n2's replica as a remote and
//!        vice versa (round-trip proves bidirectional wiring).
//!   5. DELETE the replica on n2 → list drops back to 1 entry; n1's
//!      physical `remotes` list is empty again.
//!   6. DELETE a non-existent replica → 404.
//!
//! Skips silently when the `crowkv-server` binary is not built
//! (matches the pattern in `mgmt_routes_test.rs` / `kv_routes_test.rs`).

use std::net::SocketAddr;
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
    let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    port
}

struct Upstream {
    node_id: String,
    pid: u32,
    mgmt_url: String,
    grpc_url: String,
}

async fn spawn_upstream(node_id: &str) -> Option<Upstream> {
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
        election_profile: Some("test".into()),
        binary: Some(bin),
    };
    let deployed = lifecycle::deploy_local(&req, &node).await.expect("deploy_local");
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
    let Some(n1) = spawn_upstream("n1").await else {
        eprintln!("skipping: crowkv-server binary not built");
        return;
    };
    let Some(n2) = spawn_upstream("n2").await else {
        let _ = lifecycle::stop_pid(n1.pid);
        eprintln!("skipping: crowkv-server binary not built");
        return;
    };
    let n1_mgmt = n1.mgmt_url.clone();
    let n2_mgmt = n2.mgmt_url.clone();
    let n1_pid = n1.pid;
    let n2_pid = n2.pid;

    let web = spawn_web(&[n1, n2]).await;
    let base = format!("http://{web}");
    let http = reqwest::Client::new();

    let sid: u64 = 7;
    let gid: u64 = 70;
    let rid1: u64 = 1;
    let rid2: u64 = 2;

    // 1. Create store + group on n1 with replica 1.
    let resp = http
        .post(format!("{base}/api/stores"))
        .json(&json!({"store_id": sid, "group_id": gid, "replica_id": rid1, "nodes": ["n1"]}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201, "create store: {:?}", resp.text().await.ok());

    // 2. Initial replica list has exactly one entry on n1.
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

    // 3. Add replica 2 on n2 → orchestrated bidirectional wiring.
    let resp = http
        .post(format!("{base}/api/stores/{sid}/groups/{gid}/replicas"))
        .json(&json!({"node_id": "n2", "replica_id": rid2}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201, "add replica: {:?}", resp.text().await.ok());

    // 4. List now shows both replicas.
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

    // 5. Physical tree must show bidirectional remotes:
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

    // 6. GET single replica detail.
    let resp = http
        .get(format!("{base}/api/stores/{sid}/groups/{gid}/replicas/{rid2}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let detail: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(detail["replica_id"], rid2);
    assert_eq!(detail["node_id"], "n2");

    // 7. DELETE of a non-existent replica → 404.
    let resp = http
        .delete(format!("{base}/api/stores/{sid}/groups/{gid}/replicas/999"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);

    // 8. DELETE replica 2 → orchestrated remove (deregister from n1,
    //    delete local on n2).
    let resp = http
        .delete(format!("{base}/api/stores/{sid}/groups/{gid}/replicas/{rid2}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 204);

    // 9. List drops back to 1 entry; n1's remotes list is empty again.
    let list: Vec<serde_json::Value> = http
        .get(format!("{base}/api/stores/{sid}/groups/{gid}/replicas"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(list.len(), 1, "expected 1 replica after remove, got {list:?}");
    assert_eq!(list[0]["replica_id"], rid1);

    let r1_remotes_after = c1.list_remote_replicas(sid, gid).await.unwrap();
    assert!(
        !r1_remotes_after.iter().any(|r| r.replica_id == rid2),
        "n1 should no longer list r2 as remote: {r1_remotes_after:?}"
    );

    // Cleanup.
    let _ = lifecycle::stop_pid(n1_pid);
    let _ = lifecycle::stop_pid(n2_pid);
    tokio::time::sleep(Duration::from_millis(50)).await;
}
