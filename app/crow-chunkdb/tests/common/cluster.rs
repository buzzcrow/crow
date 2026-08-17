// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Full-stack cluster harness for chunkdb integration tests.
//!
//! Starts a real 3-node `crow-kv-server` cluster (store 0, groups 0
//! and 1), seeds hardware metadata, starts diskdb in-process as a
//! gRPC server, registers it in the service registry, and wires the
//! chunkdb lifecycle handler. Tests call the handler directly.

use std::io as std_io;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use crow_chunkdb::allocator::{ChunkAllocator, DiskdbClientPool};
use crow_chunkdb::lifecycle::LifecycleHandler;
use crow_chunkdb::routing::{default_binding_table, BindingCache};
use crow_chunkdb::storage::ChunkStore;
use crow_chunkdb::topology::{refresh::run_refresh_loop, TopologyCache};
use crow_common::metrics::MetricsRegistry;
use crow_diskdb::ddb_config::{KeepAliveConfig, StorageDefaults};
use crow_diskdb::ddb_kv_client::DdbKvClient;
use crow_diskdb::liveness::keepalive::KeepAlive;
use crow_diskdb::liveness::lifecycle::StartupPhase;
use crow_diskdb::metrics::DiskdbMetrics;
use crow_diskdb::metrics::RecalcEngine;
use crow_diskdb::model::disk_group_container::DdbDiskGroupContainer;
use crow_diskdb::recovery::ZoneLoader;
use crow_diskdb::scanner::ScanState;
use crow_diskdb::service::DiskdbService;
use crow_kv_client::{ClientConfig, CrowkvClient, HardwareClient, ServiceRegistryClient};
use crow_protocol::common::{DiskId, HwStatus, NodeValue, RackValue};
use crow_protocol::diskdb::rpc::{DiskGroupValue, DiskType, DiskValue};
use serde_json::Value;

// ── process management ──────────────────────────────────────────

struct ServerHandle {
    child: Child,
    base_url: String,
    _wal_dir: tempfile::TempDir,
    _config_dir: tempfile::TempDir,
}

impl ServerHandle {
    fn base_url(&self) -> &str {
        &self.base_url
    }

    async fn wait_for_ready(&self, timeout: Duration) -> std_io::Result<()> {
        let client = reqwest::Client::new();
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            if let Ok(resp) = client.get(format!("{}/health", self.base_url)).send().await {
                if resp.status().is_success() || resp.status().as_u16() == 503 {
                    return Ok(());
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Err(std_io::Error::new(
            std_io::ErrorKind::TimedOut,
            "server was not ready before timeout",
        ))
    }
}

impl Drop for ServerHandle {
    fn drop(&mut self) {
        let pid = self.child.id();
        let _ = std::process::Command::new("kill")
            .arg("-TERM")
            .arg(pid.to_string())
            .status();
        let start = Instant::now();
        loop {
            match self.child.try_wait() {
                Ok(Some(_)) | Err(_) => break,
                Ok(None) => {
                    if start.elapsed() >= Duration::from_secs(2) {
                        let _ = std::process::Command::new("kill")
                            .arg("-KILL")
                            .arg(pid.to_string())
                            .status();
                        break;
                    }
                    std::thread::sleep(Duration::from_millis(10));
                }
            }
        }
    }
}

#[allow(dead_code)]
pub struct KvNode {
    handle: ServerHandle,
    pub node_id: u64,
    pub replica_id: u64,
}

impl KvNode {
    pub fn base_url(&self) -> &str {
        self.handle.base_url()
    }
}

pub fn crow_kv_server_bin() -> Option<PathBuf> {
    if let Ok(p) = std::env::var("CROW_KV_SERVER_BIN") {
        let path = PathBuf::from(p);
        if path.exists() {
            return Some(path);
        }
    }
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            let mut p = dir.to_path_buf();
            for _ in 0..3 {
                let candidate = p.join("crow-kv-server");
                if candidate.exists() {
                    return Some(candidate);
                }
                if !p.pop() {
                    break;
                }
            }
        }
    }
    None
}

// ── topology helpers ────────────────────────────────────────────

fn client() -> reqwest::Client {
    reqwest::Client::new()
}

async fn topology(node: &KvNode) -> Value {
    client()
        .get(format!("{}/topology", node.base_url()))
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

fn node_endpoint(topo: &Value) -> String {
    normalize_endpoint(
        topo["stores"][0]["listen_addr"]
            .as_str()
            .expect("store listen_addr"),
    )
}

async fn combined_topology(nodes: &[KvNode]) -> Value {
    let mut stores = Vec::new();
    for node in nodes {
        let topo = topology(node).await;
        if let Some(arr) = topo["stores"].as_array() {
            for s in arr {
                let mut s = s.clone();
                if let Some(addr) = s["listen_addr"].as_str() {
                    s["listen_addr"] = Value::String(normalize_endpoint(addr));
                }
                stores.push(s);
            }
        }
    }
    serde_json::json!({ "stores": stores })
}

async fn wire_topology(nodes: &[KvNode], group_id: u64) {
    let combined = combined_topology(nodes).await;
    for node in nodes {
        let resp = client()
            .post(format!(
                "{}/stores/{}/groups/{group_id}/remotes/batch",
                node.base_url(),
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

pub async fn wait_for_leader(nodes: &[KvNode], group_id: u64, timeout: Duration) -> usize {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        let mut leaders = Vec::new();
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

#[allow(dead_code)]
pub async fn leader_endpoint(nodes: &[KvNode], group_id: u64) -> String {
    let idx = wait_for_leader(nodes, group_id, Duration::from_secs(30)).await;
    node_endpoint(&topology(&nodes[idx]).await)
}

// ── kv cluster ──────────────────────────────────────────────────

#[allow(dead_code)]
pub struct KvCluster {
    pub nodes: Vec<KvNode>,
    pub group0_leader_endpoint: String,
    pub group1_leader_endpoint: String,
    pub mgmt_endpoints: Vec<String>,
}

impl KvCluster {
    pub async fn start() -> Self {
        let mut nodes = Vec::new();
        for (idx, nid) in [0u64, 1, 2].iter().enumerate() {
            let replica_id = u64::try_from(idx + 1).unwrap();
            let node = start_kv_node_with_groups(*nid, &[0, 1], replica_id)
                .await
                .unwrap_or_else(|e| panic!("start kv node {nid}: {e}"));
            nodes.push(node);
        }

        // Wire topology for both groups.
        wire_topology(&nodes, 0).await;
        wire_topology(&nodes, 1).await;

        // Wait for leaders.
        let g0_idx = wait_for_leader(&nodes, 0, Duration::from_secs(30)).await;
        let g1_idx = wait_for_leader(&nodes, 1, Duration::from_secs(30)).await;

        let group0_leader_endpoint = node_endpoint(&topology(&nodes[g0_idx]).await);
        let group1_leader_endpoint = node_endpoint(&topology(&nodes[g1_idx]).await);
        let mgmt_endpoints: Vec<String> = nodes.iter().map(|n| n.base_url().to_string()).collect();

        Self {
            nodes,
            group0_leader_endpoint,
            group1_leader_endpoint,
            mgmt_endpoints,
        }
    }

    pub fn make_crowkv_client(&self) -> Arc<CrowkvClient> {
        let kv = CrowkvClient::new(ClientConfig::new(self.mgmt_endpoints.clone()));
        kv.seed_leader(0, 0, self.group0_leader_endpoint.clone());
        kv.seed_leader(0, 1, self.group1_leader_endpoint.clone());
        Arc::new(kv)
    }

    pub fn make_hardware_client(&self) -> HardwareClient {
        HardwareClient::from_shared(self.make_crowkv_client())
    }

    pub fn make_service_registry_client(&self) -> ServiceRegistryClient {
        ServiceRegistryClient::from_shared(self.make_crowkv_client())
    }

    pub fn make_ddb_kv_client(&self) -> Arc<DdbKvClient> {
        Arc::new(DdbKvClient::from_shared(self.make_crowkv_client()))
    }
}

async fn start_kv_node_with_groups(
    node_id: u64,
    group_ids: &[u64],
    replica_id: u64,
) -> std_io::Result<KvNode> {
    let group_str = group_ids.iter().map(u64::to_string).collect::<Vec<_>>().join(",");
    let wal_dir = tempfile::tempdir()?;
    let wal_root = wal_dir.path().join("wal");
    let config_dir = tempfile::tempdir()?;
    let config_path = config_dir.path().join("crow_kv_server_config.toml");
    std::fs::write(&config_path, "# test config\n")?;
    let bin = crow_kv_server_bin()
        .ok_or_else(|| std_io::Error::new(std_io::ErrorKind::NotFound, "crow-kv-server binary not found"))?;
    let mut cmd = Command::new(bin);
    cmd.args([
        "--config",
        config_path.to_str().unwrap(),
        "--stores",
        &node_id.to_string(),
        "--groups",
        &group_str,
        "--replica",
        &replica_id.to_string(),
        "--management-addr",
        "127.0.0.1",
        "--management-port",
        "0",
        "--election-profile",
        "e2e",
        "--wal-root",
    ])
    .arg(&wal_root)
    .stdout(Stdio::piped())
    .stderr(Stdio::piped());
    let mut child = cmd.spawn()?;
    let stdout = child.stdout.take().expect("stdout captured");
    let stderr = child.stderr.take().expect("stderr captured");
    let (tx, rx) = mpsc::channel();
    let stderr_buf = Arc::new(Mutex::new(Vec::<String>::new()));
    let stderr_buf_clone = Arc::clone(&stderr_buf);
    thread::spawn(move || {
        use std::io::{BufRead, BufReader};
        for line in BufReader::new(stdout).lines().map_while(Result::ok) {
            if let Some(idx) = line.find("management_addr=") {
                let after = &line[idx + "management_addr=".len()..];
                let _ = tx.send(after.trim().to_string());
                break;
            }
        }
    });
    thread::spawn(move || {
        use std::io::{BufRead, BufReader};
        for line in BufReader::new(stderr).lines().map_while(Result::ok) {
            stderr_buf_clone.lock().unwrap().push(line);
        }
    });
    let deadline = Instant::now() + Duration::from_secs(10);
    let addr = match rx.recv_timeout(deadline.saturating_duration_since(Instant::now())) {
        Ok(addr) => addr,
        Err(e) => {
            let _ = child.wait();
            let stderr_lines = stderr_buf.lock().unwrap();
            let msg = format!(
                "no management_addr in stdout: {e}; stderr:\n{}",
                stderr_lines.join("\n")
            );
            drop(stderr_lines);
            return Err(std_io::Error::new(std_io::ErrorKind::BrokenPipe, msg));
        }
    };
    let handle = ServerHandle {
        child,
        base_url: format!("http://{addr}"),
        _wal_dir: wal_dir,
        _config_dir: config_dir,
    };
    handle.wait_for_ready(Duration::from_secs(10)).await?;
    Ok(KvNode {
        handle,
        node_id,
        replica_id,
    })
}

// ── hardware seeding ────────────────────────────────────────────

pub const STORE_ID: u64 = 0;
pub const DATA_GROUP_ID: u64 = 1;
pub const INSTANCE_ID: u64 = 999;
pub const ZONE_SIZE_UNITS: u64 = 128;
pub const UNIT_SIZE_BYTES: u32 = 1024 * 1024;
pub const CAPACITY_UNITS: u64 = ZONE_SIZE_UNITS * 4;
pub const ZONE_COUNT: u32 = 4;

pub fn make_disk_id(low: u64) -> DiskId {
    DiskId { high: 0, low }
}

/// Seed 3 racks × 1 node × 1 disk-group (3 disks each) — enough for
/// mirror 3-copy placement (distinct racks) and EC placement.
pub async fn seed_hardware(hw: &HardwareClient) {
    let lease_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
        + 3_600_000;

    for i in 0..3u64 {
        let rack_id = 100 + i;
        let node_id = 10 + i;
        let dg_id = 1000 + i;

        hw.add_rack(
            rack_id,
            &RackValue {
                status: HwStatus::Up as i32,
                node_ids: vec![node_id],
            },
        )
        .await
        .expect("add rack");

        hw.add_node(
            rack_id,
            node_id,
            &NodeValue {
                status: HwStatus::Up as i32,
                last_used_dg_id: 0,
                disk_group_ids: vec![dg_id],
                status_changed_at_ms: 0,
                temp_failure_since_ms: None,
            },
        )
        .await
        .expect("add node");

        let disk_ids = vec![
            make_disk_id(dg_id * 10 + 1),
            make_disk_id(dg_id * 10 + 2),
            make_disk_id(dg_id * 10 + 3),
        ];
        hw.add_disk_group(
            rack_id,
            node_id,
            dg_id,
            &DiskGroupValue {
                status: HwStatus::Up as i32,
                disk_ids: disk_ids.clone(),
            },
        )
        .await
        .expect("add disk-group");

        for did in &disk_ids {
            hw.add_disk(
                rack_id,
                node_id,
                dg_id,
                did,
                &DiskValue {
                    disk_type: DiskType::BlockSsd as i32,
                    capacity_units: CAPACITY_UNITS,
                    zone_size_units: ZONE_SIZE_UNITS,
                    unit_size_bytes: UNIT_SIZE_BYTES,
                    zone_count: ZONE_COUNT,
                    status: HwStatus::Up as i32,
                },
            )
            .await
            .expect("add disk");
        }

        hw.set_owner(rack_id, node_id, dg_id, INSTANCE_ID, lease_ms)
            .await
            .expect("set owner");
        hw.set_bind(rack_id, node_id, dg_id, STORE_ID, DATA_GROUP_ID)
            .await
            .expect("set bind");
    }
}

/// Disk-group IDs seeded by `seed_hardware`.
pub fn seeded_dg_ids() -> Vec<u64> {
    (0..3u64).map(|i| 1000 + i).collect()
}

// ── diskdb gRPC server (in-process) ─────────────────────────────

#[allow(dead_code)]
pub struct DiskdbServer {
    pub container: Arc<DdbDiskGroupContainer>,
    pub grpc_endpoint: String,
    _serve_handle: tokio::task::JoinHandle<()>,
}

impl DiskdbServer {
    /// Start diskdb in-process: run one keepalive tick to populate
    /// state, wait for zones, then start the gRPC server on a free
    /// port and register in the service registry.
    pub async fn start(cluster: &KvCluster) -> Self {
        let container = Arc::new(DdbDiskGroupContainer::new(INSTANCE_ID));
        let svc = cluster.make_service_registry_client();
        let hw = cluster.make_hardware_client();
        // Keepalive takes an owned DdbKvClient; the gRPC service takes Arc.
        let dg_kv_owned = DdbKvClient::from_shared(cluster.make_crowkv_client());

        let keepalive_cfg = KeepAliveConfig {
            interval: Duration::from_secs(10),
            miss_threshold: 3,
            zone_rotate_count: 4,
            cas_retry_limit: 100,
            temp_failure_timeout_secs: 900,
        };
        let keepalive =
            KeepAlive::new(hw, svc, Arc::clone(&container), keepalive_cfg).with_ddb_kv_client(dg_kv_owned);

        let outcome = keepalive.tick().await;
        eprintln!(
            "diskdb tick: groups_added={}, disks_added={}",
            outcome.groups_added, outcome.disks_added
        );
        assert_eq!(outcome.groups_added, 3, "expected 3 disk-groups");
        assert_eq!(outcome.disks_added, 9, "expected 9 disks");

        for dg_id in seeded_dg_ids() {
            wait_for_disks_ready(&container, dg_id, 3, ZONE_COUNT).await;
        }

        // Pick a free port before binding so we know the endpoint.
        let port = pick_free_port();
        let listen_addr: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();

        let mut registry = MetricsRegistry::new();
        let metrics = Arc::new(DiskdbMetrics::register(&mut registry));
        let grpc_service = DiskdbService::new(
            Arc::clone(&container),
            cluster.make_ddb_kv_client(),
            StorageDefaults::default(),
            Arc::new(ZoneLoader::new(cluster.make_ddb_kv_client(), 4)),
            Arc::new(RecalcEngine::new(
                cluster.make_ddb_kv_client(),
                Arc::clone(&container),
            )),
            ScanState::new(),
            metrics,
        )
        .into_server();

        container.set_lifecycle_phase(StartupPhase::Up);

        let serve_handle = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(grpc_service)
                .serve(listen_addr)
                .await
                .expect("diskdb gRPC server");
        });

        let grpc_endpoint = format!("http://127.0.0.1:{port}");

        // Register in service registry.
        let svc = cluster.make_service_registry_client();
        let dg_ids = seeded_dg_ids();
        svc.register_diskdb(INSTANCE_ID, &grpc_endpoint, &dg_ids, &[])
            .await
            .expect("register diskdb");

        // Give the server a moment to bind.
        tokio::time::sleep(Duration::from_millis(200)).await;

        Self {
            container,
            grpc_endpoint,
            _serve_handle: serve_handle,
        }
    }
}

fn pick_free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    listener.local_addr().expect("local_addr").port()
}

pub async fn wait_for_disks_ready(
    container: &DdbDiskGroupContainer,
    dg_id: u64,
    expected_disks: usize,
    expected_zones: u32,
) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(dg) = container.get_disk_group(dg_id) {
            let disks = dg.disks.read().unwrap();
            let all_ready = disks.len() == expected_disks
                && disks.iter().all(|d| {
                    *d.effective_status.read().unwrap() == HwStatus::Up
                        && u32::try_from(d.zones.read().unwrap().len()).unwrap_or(0) == expected_zones
                });
            if all_ready {
                return;
            }
        }
        assert!(Instant::now() <= deadline, "disks not ready after 10s");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

// ── chunkdb handler ─────────────────────────────────────────────

#[allow(dead_code)]
pub struct ChunkdbHarness {
    pub handler: Arc<LifecycleHandler>,
    pub store: Arc<ChunkStore>,
    pub allocator: Arc<ChunkAllocator>,
    pub topology: TopologyCache,
    _refresh_handle: tokio::task::JoinHandle<()>,
}

impl ChunkdbHarness {
    /// Wire the chunkdb lifecycle handler against the running kv
    /// cluster + diskdb server.
    pub async fn start(cluster: &KvCluster) -> Self {
        let kv = cluster.make_crowkv_client();

        // Topology cache + refresh loop.
        let topology = TopologyCache::new();
        let hw = cluster.make_hardware_client();
        let (_stop_tx, stop_rx) = tokio::sync::watch::channel(false);
        let refresh_cache = topology.clone();
        let refresh_handle = tokio::spawn(async move {
            run_refresh_loop(refresh_cache, hw, Duration::from_secs(5), stop_rx).await;
        });

        // Wait for topology to populate.
        tokio::time::sleep(Duration::from_millis(500)).await;

        // Binding cache — all buckets to store 0, group 1.
        let bindings = BindingCache::new();
        bindings.replace(default_binding_table(STORE_ID, DATA_GROUP_ID));
        let store = Arc::new(ChunkStore::new(Arc::clone(&kv), bindings));

        // Diskdb client pool.
        let svc = cluster.make_service_registry_client();
        let pool = Arc::new(DiskdbClientPool::new(svc));
        pool.refresh_endpoints().await.expect("refresh endpoints");
        let allocator = Arc::new(ChunkAllocator::new(Arc::clone(&pool)));

        let handler = Arc::new(
            LifecycleHandler::new(Arc::clone(&store), Arc::clone(&allocator), topology.clone()).with_locks(
                Arc::new(crow_chunkdb::lifecycle::ChunkLockMap::new(
                    10_000,
                    Arc::new(crow_chunkdb::metrics::LifecycleMetrics::new()),
                    std::time::Duration::from_secs(60),
                )),
            ),
        );

        Self {
            handler,
            store,
            allocator,
            topology,
            _refresh_handle: refresh_handle,
        }
    }
}
