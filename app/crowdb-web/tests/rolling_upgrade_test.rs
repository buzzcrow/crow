// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! M5 rolling-upgrade version-compat test: exercise a 3-node cluster with
//! two different `crowdb-kv-server` binary builds and verify a KV workload
//! does not diverge.
//!
//! By default the test uses the current `crowdb-kv-server` binary for all
//! nodes, copying it to a distinct path so the harness treats the second
//! node as a separate "version" build. To test a real version boundary,
//! set `CROWDB_KV_SERVER_BIN_V2` to a different binary (e.g. an older build).
//!
//! Uses the same real-subprocess harness as `replica_leader_removal_test.rs`.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use crowdb_console_shared::clients::http::ServerClient;
use crowdb_console_shared::cluster::NodeHealth;
use crowdb_console_shared::config::{NodeEntry, RackEntry, ServerEntry, ServiceType};
use crowdb_console_shared::lifecycle::{self, crowdb_kv_server_bin, process_is_alive, DeployRequest};
use crowdb_console_shared::monitor::{legacy_topology_to_node_stores, NodeRecord};
use crowdb_console_shared::ConsoleConfig;
use crowdb_web::{router, AppState};
use serde_json::json;

fn pick_free_port() -> u16 {
    crowdb_console_shared::test_ports::unique_test_port()
}

struct Upstream {
    node_id: u64,
    pid: u32,
    mgmt_url: String,
    rpc_url: String,
    rest_port: u16,
    rpc_port: u16,
    binary: PathBuf,
}

struct Cluster {
    nodes: BTreeMap<u64, Upstream>,
    web: SocketAddr,
    workspace: PathBuf,
}

impl Cluster {
    const fn sid() -> u64 {
        3
    }

    const fn gid() -> u64 {
        3
    }

    fn base_url(&self) -> String {
        format!("http://{}", self.web)
    }

    fn stop(&mut self) {
        for n in self.nodes.values() {
            let _ = lifecycle::stop_pid_with_timeout(n.pid, Duration::from_secs(5));
        }
    }

    /// Restart a previously-killed node with the same ports, binary, and
    /// workspace directory so it recovers from WAL and rejoins the group.
    async fn restart_node(&mut self, node_id: u64) {
        let u = self.nodes.get(&node_id).expect("node exists");
        let node = NodeEntry {
            id: node_id,
            rack_id: 1,
            host: "127.0.0.1".into(),
            ssh_port: 22,
            ssh_user: String::new(),
            ssh_key: None,
            ssh_password: None,
        };
        let replica_id = node_id;
        let extra_args = vec![
            "--stores".to_string(),
            Cluster::sid().to_string(),
            "--groups".to_string(),
            Cluster::gid().to_string(),
            "--replica".to_string(),
            replica_id.to_string(),
        ];
        let req = DeployRequest {
            server_id: node_id.to_string(),
            rest_port: u.rest_port,
            rpc_port: u.rpc_port,
            election_profile: Some("e2e".into()),
            binary: Some(u.binary.clone()),
            ..Default::default()
        };
        let node_dir = self.workspace.join(node_id.to_string());
        let deployed = lifecycle::deploy_local_in_dir_with_extra_args(&req, &node, &node_dir, &extra_args)
            .await
            .expect("restart node");
        // Update the stored pid so stop() cleans up the new process.
        self.nodes.get_mut(&node_id).unwrap().pid = deployed.pid;
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

fn resolve_binary() -> Option<PathBuf> {
    let bin = crowdb_kv_server_bin()?;
    if !bin.exists() {
        return None;
    }
    Some(bin)
}

fn resolve_second_binary() -> Option<PathBuf> {
    if let Ok(v2) = std::env::var("CROWDB_KV_SERVER_BIN_V2") {
        let p = PathBuf::from(v2);
        if p.exists() {
            return Some(p);
        }
    }
    None
}

async fn spawn_upstream(node_id: u64, workspace: &std::path::Path, binary: &Path) -> Option<Upstream> {
    let node = NodeEntry {
        id: node_id,
        rack_id: 1,
        host: "127.0.0.1".into(),
        ssh_port: 22,
        ssh_user: String::new(),
        ssh_key: None,
        ssh_password: None,
    };
    let req = DeployRequest {
        server_id: node_id.to_string(),
        rest_port: pick_free_port(),
        rpc_port: pick_free_port(),
        election_profile: Some("e2e".into()),
        binary: Some(binary.to_path_buf()),
        ..Default::default()
    };
    let node_dir = workspace.join(node_id.to_string());
    std::fs::create_dir_all(node_dir.join("bin")).unwrap();
    std::fs::create_dir_all(node_dir.join("log")).unwrap();
    let deployed = lifecycle::deploy_local_in_dir(&req, &node, &node_dir)
        .await
        .expect("deploy_local_in_dir");
    Some(Upstream {
        node_id,
        pid: deployed.pid,
        mgmt_url: deployed.mgmt_url,
        rpc_url: deployed.rpc_url,
        rest_port: req.rest_port,
        rpc_port: req.rpc_port,
        binary: binary.to_path_buf(),
    })
}

async fn spawn_web(upstreams: &BTreeMap<u64, Upstream>) -> SocketAddr {
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
            id: u.node_id,
            rack_id: 1,
            host: "127.0.0.1".into(),
            ssh_port: 22,
            ssh_user: String::new(),
            ssh_key: None,
            ssh_password: None,
        });
        cfg.add_server(ServerEntry {
            id: u.node_id.to_string(),
            url: u.mgmt_url.clone(),
            node_id: Some(u.node_id),
            rpc_url: Some(u.rpc_url.clone()),
            rest_port: None,
            rpc_port: None,
            auto_start: true,
            binary: None,
            election_profile: None,
            pid: Some(u.pid),
            service_type: ServiceType::Kv,
            rpc_workers: None,
            no_fsync: false,
        })
        .unwrap();
    }
    let state = AppState::with_config(cfg, None);
    // Register each upstream's pid so `refresh_node_cache` (which
    // skips nodes with no tracked runtime pid) refreshes after
    // mutations.
    for u in upstreams.values() {
        state.set_runtime_pid(u.node_id, u.pid);
    }

    for u in upstreams.values() {
        let client = ServerClient::new(u.mgmt_url.clone()).unwrap();
        if let Ok(stores) = client.topology().await {
            let rec = NodeRecord {
                health: NodeHealth::Up,
                last_seen_ms: 1,
                stores: legacy_topology_to_node_stores(u.node_id, &stores),
                last_error: None,
            };
            state.monitor_cache.set_node_report(u.node_id, rec).await;
        }
    }

    tokio::spawn(async move {
        axum::serve(listener, router(state)).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    addr
}

async fn spawn_mixed_cluster(workspace: &std::path::Path, v2_binary: &PathBuf) -> Option<Cluster> {
    let current = resolve_binary()?;
    let mut nodes: BTreeMap<u64, Upstream> = BTreeMap::new();
    for (id, binary) in [(1u64, &current), (2u64, &current), (3u64, v2_binary)] {
        let Some(node) = spawn_upstream(id, workspace, binary).await else {
            for n in nodes.into_values() {
                let _ = lifecycle::stop_pid_with_timeout(n.pid, Duration::from_secs(5));
            }
            eprintln!("skipping: crowdb-kv-server binary not built");
            return None;
        };
        nodes.insert(id, node);
    }
    let web = spawn_web(&nodes).await;
    Some(Cluster {
        nodes,
        web,
        workspace: workspace.to_path_buf(),
    })
}

async fn create_three_node_group(cluster: &Cluster) {
    let base = cluster.base_url();
    let http = reqwest::Client::new();
    let sid = Cluster::sid();
    let gid = Cluster::gid();

    // Initialize the system group so non-zero stores can be created.
    let resp = http
        .post(format!("{base}/api/cluster/init"))
        .json(&json!({"nodes": [1, 2, 3]}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201, "cluster init: {:?}", resp.text().await.ok());

    let resp = http
        .post(format!("{base}/api/stores"))
        .json(&json!({"store_id": sid, "nodes": [1]}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201, "create store: {:?}", resp.text().await.ok());

    let resp = http
        .post(format!("{base}/api/stores/{sid}/groups"))
        .json(&json!({"group_id": gid, "replica_id": 1, "nodes": [1]}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 201, "create group: {:?}", resp.text().await.ok());

    for node_id in 2u64..=3 {
        let resp = http
            .post(format!("{base}/api/stores/{sid}/groups/{gid}/replicas"))
            .json(&json!({"node_id": node_id, "replica_id": node_id}))
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

async fn wait_for_leader(
    cluster: &Cluster,
    timeout: Duration,
    exclude_node: Option<u64>,
) -> Option<(u64, u64)> {
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
        if let Some(leader) = group["replicas"].as_array().unwrap_or(&vec![]).iter().find(|r| {
            r["role"] == "leader" && exclude_node.map_or(true, |ex| r["node_id"].as_u64() != Some(ex))
        }) {
            let rid = leader["replica_id"].as_u64()?;
            let node_id = leader["node_id"].as_u64()?;
            return Some((rid, node_id));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    None
}

async fn put_and_get(base: &str, http: &reqwest::Client, sid: u64, gid: u64, key: &str, value: &str) {
    let put_resp = http
        .post(format!("{base}/api/stores/{sid}/groups/{gid}/kv/put"))
        .json(&json!({"key": key, "value": value}))
        .send()
        .await
        .unwrap();
    assert_eq!(
        put_resp.status(),
        200,
        "kv put {key}: {:?}",
        put_resp.text().await.ok()
    );
    let put_body: serde_json::Value = put_resp.json().await.unwrap();
    assert_eq!(put_body["ok"], true, "kv put {key}: {put_body}");

    let get_resp = http
        .get(format!("{base}/api/stores/{sid}/groups/{gid}/kv/get?key={key}"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        get_resp.status(),
        200,
        "kv get {key}: {:?}",
        get_resp.text().await.ok()
    );
    let get_body: serde_json::Value = get_resp.json().await.unwrap();
    assert_eq!(get_body["value_utf8"], value, "kv get {key}: {get_body}");
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn mixed_version_3_node_cluster_kv_no_divergence() {
    let Some(current) = resolve_binary() else {
        eprintln!("skipping: crowdb-kv-server binary not built");
        return;
    };

    // If a second binary is not explicitly provided, make a copy of the
    // current binary so the test still validates the mixed-build harness.
    // For a real version boundary, set CROWDB_KV_SERVER_BIN_V2 to an old build.
    let v2_binary = resolve_second_binary().unwrap_or_else(|| {
        let copy = current.parent().unwrap().join("crowdb-kv-server-v2");
        let _ = std::fs::remove_file(&copy);
        std::fs::copy(&current, &copy).expect("copy current binary as v2");
        copy
    });

    let workspace = tempdir("rolling_upgrade");
    let Some(mut cluster) = spawn_mixed_cluster(&workspace, &v2_binary).await else {
        return;
    };

    create_three_node_group(&cluster).await;

    let _leader = wait_for_leader(&cluster, Duration::from_secs(10), None)
        .await
        .expect("leader should be elected in 3-node group");

    let base = cluster.base_url();
    let http = reqwest::Client::new();
    let sid = Cluster::sid();
    let gid = Cluster::gid();

    // Serve a small KV workload and verify each key round-trips.
    for i in 0..20 {
        let key = format!("rk-{i:02}");
        let value = format!("rv-{i:02}");
        put_and_get(&base, &http, sid, gid, &key, &value).await;
    }

    // Restart each node one at a time (rolling upgrade) and verify the
    // workload continues after each restart.
    for node_id in 1u64..=3 {
        let node = &cluster.nodes[&node_id];
        let pid = node.pid;
        let status = std::process::Command::new("kill")
            .arg("-TERM")
            .arg(pid.to_string())
            .status()
            .expect("terminate node");
        assert!(status.success());

        // Wait for graceful shutdown to complete. The server's per-layer
        // shutdown timeout is 10s (ServerConfig::DEFAULT.shutdown_timeout_ms)
        // and shutdown performs a real fsync of the WAL + engine snapshot,
        // which can take several seconds under CI disk contention. The poll
        // returns as soon as the process exits (normally ~200ms); the 15s
        // cap is a safety net above the server's own 10s/layer budget.
        let dead = Instant::now() + Duration::from_secs(15);
        while process_is_alive(pid) && Instant::now() < dead {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(!process_is_alive(pid), "node {node_id} should be stopped");

        // Wait for a new leader to be elected before attempting reads.
        let _new_leader = wait_for_leader(&cluster, Duration::from_secs(10), Some(node_id))
            .await
            .expect("survivors should elect a new leader after node stop");

        // The remaining nodes should still serve reads.
        for i in 0..5 {
            let key = format!("rk-{i:02}");
            let deadline = Instant::now() + Duration::from_secs(5);
            let get_body = loop {
                let get_resp = http
                    .get(format!("{base}/api/stores/{sid}/groups/{gid}/kv/get?key={key}"))
                    .send()
                    .await
                    .unwrap();
                if get_resp.status().is_success() {
                    break get_resp.json::<serde_json::Value>().await.unwrap();
                }
                if Instant::now() >= deadline {
                    let status = get_resp.status();
                    let body = get_resp.text().await.unwrap_or_default();
                    panic!("read during rolling restart failed for {key}: status {status}, body: {body}");
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            };
            let expected = format!("rv-{i:02}");
            assert_eq!(
                get_body["value_utf8"], expected,
                "value diverged for {key}: {get_body}"
            );
        }

        // Restart the killed node so it recovers from WAL and rejoins the
        // group, restoring full quorum before the next rolling step.
        cluster.restart_node(node_id).await;

        // Wait for the restarted node to come back up.
        let ready = Instant::now() + Duration::from_secs(5);
        while Instant::now() < ready {
            let client = ServerClient::new(cluster.nodes[&node_id].mgmt_url.clone()).unwrap();
            if client.health().await.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // Wait for leader to stabilize after the node rejoins.
        let _leader_after_restart = wait_for_leader(&cluster, Duration::from_secs(10), None)
            .await
            .expect("leader should stabilize after node restart");
    }

    cluster.stop();
}
