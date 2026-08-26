// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! `BenchFixture` — fixture-based cluster provisioning for the `bench
//! benchmark` verb.
//!
//! Mirrors the UI test fixture's `setupCluster`/`teardownCluster`
//! (`crow-console/web/ui/e2e/fixtures/consoleSetup.ts`), but through
//! the typed `ConsoleClient` against an **embedded** `crow-web`
//! instance started in-process, so the benchmark is fully
//! self-contained: 1 rack + 3 nodes on localhost, forming a complete
//! Paxos replication group.

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use crow_console_shared::clients::console::{
    AddRackBody, ConsoleClient, CreateGroupBody, CreateStoreBody, DeployNodeServerBody,
};
use crow_console_shared::config::{ConsoleConfig, NodeEntry};
use crow_console_shared::error::{Error, Result};
use crow_console_shared::lifecycle::stop_pid_with_timeout;
use crow_console_shared::test_ports::unique_test_port;

use super::super::metrics_log::{aggregate_server_metrics, parse_metrics_log};
use super::super::report::ServerMetrics;

/// Number of nodes (and racks, 1:1) provisioned by the fixture.
const NODE_COUNT: usize = 3;
/// The single store/group provisioned by the fixture.
pub const STORE_ID: u64 = 1;
pub const GROUP_ID: u64 = 1;

/// Storage mode for the benchmarked cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BenchMode {
    /// Crowtree engine + mem-block page store (in-memory, no alignment),
    /// WAL on mem-block backend.
    Memory,
    /// Crowtree engine + file page store (file-based, no alignment),
    /// WAL on file backend.
    File,
    /// Crowtree engine + block page store (`O_DIRECT`, 4K aligned),
    /// WAL on block-device backend.
    Block,
}

impl BenchMode {
    /// Parse the `--mode` CLI value.
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "mem" => Some(Self::Memory),
            "file" => Some(Self::File),
            "block" | "block-device" => Some(Self::Block),
            _ => None,
        }
    }

    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::Memory => "mem",
            Self::File => "file",
            Self::Block => "block-device",
        }
    }

    /// Deploy-time flags for this mode, applied on top of the common
    /// `election_profile`/`metrics_interval` settings.
    fn apply_to(self, body: &mut DeployNodeServerBody) {
        match self {
            Self::Memory => {
                body.kv_backend = Some("mem-block".into());
                body.wal_backend = Some("mem-block".into());
            }
            Self::File => {
                body.kv_backend = Some("file".into());
                body.wal_backend = Some("file".into());
            }
            Self::Block => {
                body.kv_backend = Some("block".into());
                body.wal_backend = Some("block-device".into());
            }
        }
    }
}

/// A provisioned 3-node cluster fixture, backed by an embedded
/// `crow-web` console instance. Encapsulates the full deploy
/// lifecycle: embedded console-web startup, topology creation via
/// `ConsoleClient`, server deployment, and teardown.
pub struct BenchFixture {
    client: ConsoleClient,
    console_task: tokio::task::JoinHandle<()>,
    node_ids: Vec<u64>,
    node_pids: Vec<u32>,
    node_grpc_urls: Vec<String>,
    node_mgmt_urls: Vec<String>,
    leader_endpoint: String,
    workspace_dir: PathBuf,
    stopped: bool,
}

impl BenchFixture {
    /// Provision the fixture: start an embedded console-web instance,
    /// create 1 rack + 3 nodes, deploy a `crow-kv-server` per node in
    /// `mode`, wire them into a single 3-replica store/group, and wait
    /// for leader election.
    ///
    /// # Errors
    /// Returns an error if the console-web instance fails to bind, any
    /// provisioning call fails, or no leader is elected within the
    /// timeout.
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        mode: BenchMode,
        workspace_dir: PathBuf,
        max_inflight: usize,
        metrics_interval: u64,
        node_config: Option<String>,
        coalesce_max_keys: Option<usize>,
        coalesce_drain_threshold: Option<usize>,
    ) -> Result<Self> {
        std::fs::create_dir_all(&workspace_dir)?;

        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await?;
        let addr = listener.local_addr()?;
        let console_config_path = workspace_dir.join("console.toml");
        let state = crow_web::AppState::with_config(ConsoleConfig::default(), Some(console_config_path));
        let console_task = tokio::spawn(async move {
            let _ = axum::serve(listener, crow_web::router(state)).await;
        });
        let client = ConsoleClient::new(format!("http://{addr}"))?;

        let (ids, pids, grpc_urls, mgmt_urls) = match Self::provision_nodes(
            &client,
            mode,
            max_inflight,
            metrics_interval,
            node_config,
            coalesce_max_keys,
            coalesce_drain_threshold,
        )
        .await
        {
            Ok(v) => v,
            Err(e) => {
                console_task.abort();
                return Err(e);
            }
        };

        if let Err(e) = client.cluster_init(&ids).await {
            console_task.abort();
            return Err(upstream_err("cluster", "cluster_init", &e));
        }

        if let Err(e) = Self::provision_store_and_group(&client, &ids).await {
            console_task.abort();
            return Err(e);
        }

        let leader_endpoint = match wait_for_leader_endpoint(&client, Duration::from_secs(20)).await {
            Ok(ep) => ep,
            Err(e) => {
                console_task.abort();
                return Err(e);
            }
        };

        // Wait for cluster health: all replicas must know the leader
        // and the leader must be actively serving requests.
        if let Err(e) = wait_for_healthy_cluster(&mgmt_urls, &leader_endpoint).await {
            console_task.abort();
            return Err(e);
        }

        Ok(Self {
            client,
            console_task,
            node_ids: ids,
            node_pids: pids,
            node_grpc_urls: grpc_urls,
            node_mgmt_urls: mgmt_urls,
            leader_endpoint,
            workspace_dir,
            stopped: false,
        })
    }

    /// Create 1 rack + 1 node per rack (`NODE_COUNT` total) and deploy a
    /// `crow-kv-server` on each, in `mode`. Returns the node ids and their
    /// server pids (index-aligned).
    #[allow(clippy::too_many_arguments)]
    async fn provision_nodes(
        client: &ConsoleClient,
        mode: BenchMode,
        max_inflight: usize,
        metrics_interval: u64,
        node_config: Option<String>,
        coalesce_max_keys: Option<usize>,
        coalesce_drain_threshold: Option<usize>,
    ) -> Result<(Vec<u64>, Vec<u32>, Vec<String>, Vec<String>)> {
        let mut ids = Vec::with_capacity(NODE_COUNT);
        let mut pids = Vec::with_capacity(NODE_COUNT);
        let mut grpc_urls = Vec::with_capacity(NODE_COUNT);
        let mut mgmt_urls = Vec::with_capacity(NODE_COUNT);
        for i in 0..NODE_COUNT {
            let rack_id = i as u64;
            let node_id = i as u64;
            client
                .add_rack(&AddRackBody {
                    id: rack_id,
                    name: format!("br{i}"),
                })
                .await
                .map_err(|e| upstream_err(&rack_id.to_string(), "add_rack", &e))?;
            client
                .add_node(
                    rack_id,
                    &NodeEntry {
                        id: node_id,
                        rack_id,
                        host: "127.0.0.1".into(),
                        ssh_port: 22,
                        ssh_user: String::new(),
                        ssh_key: None,
                        ssh_password: None,
                    },
                )
                .await
                .map_err(|e| upstream_err(&node_id.to_string(), "add_node", &e))?;

            let mut body = DeployNodeServerBody {
                rest_port: unique_test_port(),
                rpc_port: unique_test_port(),
                election_profile: Some("e2e".into()),
                metrics_interval: Some(metrics_interval),
                max_inflight: Some(max_inflight),
                coalesce_max_keys,
                coalesce_drain_threshold,
                ..Default::default()
            };
            mode.apply_to(&mut body);
            body.config = node_config.clone();
            let deployed = client
                .deploy_node_server(node_id, &body)
                .await
                .map_err(|e| upstream_err(&node_id.to_string(), "deploy_node_server", &e))?;

            ids.push(node_id);
            pids.push(deployed.pid);
            grpc_urls.push(deployed.grpc_url);
            mgmt_urls.push(deployed.mgmt_url);
        }
        Ok((ids, pids, grpc_urls, mgmt_urls))
    }

    /// Create the single store spanning all nodes, then a 3-replica
    /// group over the same nodes.
    async fn provision_store_and_group(client: &ConsoleClient, node_ids: &[u64]) -> Result<()> {
        client
            .add_store(&CreateStoreBody {
                store_id: STORE_ID,
                nodes: node_ids.to_vec(),
            })
            .await
            .map_err(|e| upstream_err("store", "add_store", &e))?;
        client
            .add_group(
                STORE_ID,
                &CreateGroupBody {
                    group_id: GROUP_ID,
                    replica_id: 1,
                    nodes: node_ids.to_vec(),
                },
            )
            .await
            .map_err(|e| upstream_err("group", "add_group", &e))?;
        Ok(())
    }

    /// The elected leader's gRPC endpoint, ready to hand to
    /// `bench::runner::run_bench`.
    #[must_use]
    pub fn leader_endpoint(&self) -> &str {
        &self.leader_endpoint
    }

    /// Per-node `crow-kv-server` management-API URLs (one per deployed
    /// node). Each serves `/topology`, which a bench client with a
    /// distributed `ReadEndpointPolicy` fetches to learn the full
    /// replica list for `MinSlot` read distribution.
    #[must_use]
    pub fn node_mgmt_urls(&self) -> &[String] {
        &self.node_mgmt_urls
    }

    /// Node ids provisioned by this fixture, in deploy order.
    #[must_use]
    pub fn node_ids(&self) -> &[u64] {
        &self.node_ids
    }

    /// Workspace directory where node data/logs live.
    #[must_use]
    pub fn workspace_dir(&self) -> &Path {
        &self.workspace_dir
    }

    /// Build a map from gRPC endpoint URL to node ID, for resolving
    /// leader-change episode endpoints to node names in the report.
    #[must_use]
    pub fn endpoint_to_node_map(&self) -> std::collections::HashMap<String, String> {
        self.node_ids
            .iter()
            .zip(self.node_grpc_urls.iter())
            .map(|(nid, url)| (url.clone(), nid.to_string()))
            .collect()
    }

    /// Read and aggregate server-side metrics across every node's
    /// `log/metrics-*.log` file.
    #[must_use]
    pub fn collect_metrics(&self) -> ServerMetrics {
        let per_node: Vec<ServerMetrics> = self
            .node_ids
            .iter()
            .zip(&self.node_pids)
            .filter_map(|(node_id, pid)| self.read_node_metrics_log(*node_id, *pid))
            .map(|content| parse_metrics_log(&content))
            .collect();
        aggregate_server_metrics(&per_node)
    }

    /// Copy every node's `log/` directory into `run_dir/node-<id>/` for
    /// the bundled report artifacts.
    ///
    /// # Errors
    /// Returns an error if `run_dir` (or a per-node subdirectory) cannot
    /// be created.
    pub fn collect_logs(&self, run_dir: &Path) -> std::io::Result<()> {
        for node_id in &self.node_ids {
            let src = self.node_workspace(*node_id).join("log");
            let dst = run_dir.join(format!("node-{node_id}"));
            std::fs::create_dir_all(&dst)?;
            let Ok(entries) = std::fs::read_dir(&src) else {
                continue;
            };
            for entry in entries.flatten() {
                if entry.path().is_file() {
                    let _ = std::fs::copy(entry.path(), dst.join(entry.file_name()));
                }
            }
        }
        Ok(())
    }

    /// Stop every deployed server and shut down the embedded console-web
    /// task. The workspace directory is preserved (it lives inside the
    /// `run_dir`). Idempotent — safe to call more than once.
    pub async fn cleanup(&mut self) {
        if self.stopped {
            return;
        }
        self.stopped = true;
        for node_id in &self.node_ids {
            let _ = self.client.stop_node_server(*node_id).await;
        }
        self.console_task.abort();
    }

    fn node_workspace(&self, node_id: u64) -> PathBuf {
        self.workspace_dir.join(format!("N-{node_id}"))
    }

    /// Locate and read this node's `log/crow-kv-server-metrics-<timestamp>-<pid>.log`
    /// file (see `crow_kv::common::logging::open_metrics_log`).
    fn read_node_metrics_log(&self, node_id: u64, pid: u32) -> Option<String> {
        let log_dir = self.node_workspace(node_id).join("log");
        let suffix = format!("-{pid}.log");
        let entries = std::fs::read_dir(log_dir).ok()?;
        for entry in entries.flatten() {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name.contains("-metrics-") && name.ends_with(suffix.as_str()) {
                return std::fs::read_to_string(entry.path()).ok();
            }
        }
        None
    }
}

impl Drop for BenchFixture {
    fn drop(&mut self) {
        // Safety net if `cleanup()` was not called explicitly: abort the
        // embedded console-web task and best-effort SIGTERM the deployed
        // servers (short timeout — this must not block indefinitely).
        if !self.stopped {
            self.console_task.abort();
            for &pid in &self.node_pids {
                let _ = stop_pid_with_timeout(pid, Duration::from_millis(500));
            }
        }
    }
}

/// Poll `resolve_endpoint(STORE_ID, GROUP_ID)` until a leader is known
/// or `timeout` elapses.
async fn wait_for_leader_endpoint(client: &ConsoleClient, timeout: Duration) -> Result<String> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Ok(info) = client.resolve_endpoint(STORE_ID, GROUP_ID).await {
            if !info.grpc_url.is_empty() {
                return Ok(info.grpc_url);
            }
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(Error::Config(format!(
                "no leader elected for store {STORE_ID} group {GROUP_ID} within {timeout:?}"
            )));
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Wait for cluster health: (1) the leader can serve a test put, and
/// (2) every node's `/topology` reports the same non-zero `leader_id`.
/// This ensures the leader has sent heartbeats to all followers and they
/// all know who the leader is, preventing election disruptions at the
/// start of the benchmark.
async fn wait_for_healthy_cluster(mgmt_urls: &[String], leader_endpoint: &str) -> Result<()> {
    use crow_console_shared::clients::http::ServerClient;
    use crow_kv_client::{ClientConfig, CrowkvClient};

    // Phase 1: verify the leader can serve a test put.
    let mut cfg = ClientConfig::new(Vec::new());
    cfg.retry.max_retries = 2;
    let client = CrowkvClient::new(cfg);
    client.seed_leader(STORE_ID, GROUP_ID, leader_endpoint.to_string());

    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        match client
            .put(STORE_ID, GROUP_ID, b"__bench_health__", b"ok", None)
            .await
        {
            Ok(_) => break,
            Err(_) if tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            Err(e) => {
                return Err(Error::Config(format!(
                    "leader not serving requests within 15s: {e}"
                )));
            }
        }
    }

    // Phase 2: poll every node's /topology until all report the same
    // non-zero leader_id for our store/group.
    loop {
        let mut all_agree = true;
        let mut consensus_leader: Option<u64> = None;
        for url in mgmt_urls {
            let Ok(sc) = ServerClient::new(url) else {
                all_agree = false;
                break;
            };
            let Ok(stores) = sc.topology().await else {
                all_agree = false;
                break;
            };
            let leader_id = stores
                .iter()
                .find(|s| s.store_id == STORE_ID)
                .and_then(|s| s.groups.iter().find(|g| g.group_id == GROUP_ID))
                .map_or(0, |g| g.leader_id);

            if leader_id == 0 {
                all_agree = false;
                break;
            }
            match consensus_leader {
                None => consensus_leader = Some(leader_id),
                Some(l) if l != leader_id => {
                    all_agree = false;
                    break;
                }
                _ => {}
            }
        }
        if all_agree {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(Error::Config(
                "cluster not healthy within 15s: replicas disagree on leader".to_string(),
            ));
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

fn upstream_err(id: &str, op: &str, e: &Error) -> Error {
    Error::UpstreamRpc {
        node_id: id.to_string(),
        status: format!("{op}: {e}"),
    }
}

// ── KvTarget + KvBenchClient: BenchTarget/BenchClient impls ───────

use std::sync::Arc;

use crow_kv_client::Error as ClientError;
use crow_kv_client::{ClientConfig, CrowkvClient, GetOutcome, WriteOutcome};
use tokio::task::JoinHandle;

use super::super::metrics_flusher::{spawn_metrics_flusher, spawn_progress_snapshotter};
use super::super::report::OpOutcome;
use super::super::runner::BenchConfig;
use super::super::worker::WorkerCounters;
use super::super::workload::{format_key, value_for, OpGen, OpKind};
use super::{BenchClient, BenchTarget};

/// KV bench target: provisions a 3-node cluster via `BenchFixture`,
/// builds `CrowkvClient`-backed workers, and wires progress/metrics.
pub(crate) struct KvTarget {
    mode: BenchMode,
    workspace_dir: PathBuf,
    max_inflight: usize,
    metrics_interval: u64,
    node_config: Option<String>,
    coalesce_max_keys: Option<usize>,
    coalesce_drain_threshold: Option<usize>,
    fixture: Option<BenchFixture>,
    /// The shared client used by all workers + progress/metrics tasks.
    client: Option<Arc<CrowkvClient>>,
}

impl KvTarget {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        mode: BenchMode,
        workspace_dir: PathBuf,
        max_inflight: usize,
        metrics_interval: u64,
        node_config: Option<String>,
        coalesce_max_keys: Option<usize>,
        coalesce_drain_threshold: Option<usize>,
    ) -> Self {
        Self {
            mode,
            workspace_dir,
            max_inflight,
            metrics_interval,
            node_config,
            coalesce_max_keys,
            coalesce_drain_threshold,
            fixture: None,
            client: None,
        }
    }
}

impl BenchTarget for KvTarget {
    type Client = KvBenchClient;

    fn label(&self) -> &'static str {
        "kv"
    }

    async fn provision(&mut self, cfg: &BenchConfig) -> Result<()> {
        let fixture = BenchFixture::new(
            self.mode,
            self.workspace_dir.clone(),
            self.max_inflight,
            self.metrics_interval,
            self.node_config.clone(),
            self.coalesce_max_keys,
            self.coalesce_drain_threshold,
        )
        .await?;

        // For distributed read policies, use the fixture's first mgmt URL
        // as the topology seed so the client can fetch the full replica list.
        let topology_seed = cfg.topology_seed.clone().or_else(|| {
            if cfg.read_endpoint_policy.is_distributed() {
                Some(fixture.node_mgmt_urls()[0].clone())
            } else {
                None
            }
        });

        let mut client_config = ClientConfig::new(topology_seed.map(|s| vec![s]).unwrap_or_default());
        client_config.pool_size_per_endpoint = cfg.connections as usize;
        client_config.read_endpoint_policy = cfg.read_endpoint_policy;
        let client = CrowkvClient::new(client_config);
        client.seed_leader(cfg.store_id, cfg.group_id, fixture.leader_endpoint().to_string());
        self.client = Some(Arc::new(client));
        self.fixture = Some(fixture);
        Ok(())
    }

    async fn build_client(&self) -> Result<KvBenchClient> {
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| Error::Config("kv target not provisioned".to_string()))?;
        Ok(KvBenchClient {
            client: Arc::clone(client),
        })
    }

    async fn pre_populate(&self, _client: &KvBenchClient, cfg: &BenchConfig) -> Result<(u64, u64)> {
        let Some(count) = cfg.pre_populate.filter(|c| *c > 0) else {
            return Ok((0, 0));
        };
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| Error::Config("kv target not provisioned".to_string()))?;
        let pop_start = std::time::Instant::now();
        let mut errors: u64 = 0;
        for id in 0..count {
            let key = format_key(id);
            let vsize = cfg
                .value_size_mix
                .as_ref()
                .map_or(cfg.value_size, |mix| mix.size_for(id));
            let value = value_for(id, vsize);
            let mut attempts = 0u32;
            loop {
                attempts += 1;
                match client.put(cfg.store_id, cfg.group_id, &key, &value, None).await {
                    Ok(_) => break,
                    Err(ClientError::NotLeader { .. }) if attempts < 8 => {}
                    Err(_) => {
                        errors += 1;
                        break;
                    }
                }
            }
        }
        let ms = u64::try_from(pop_start.elapsed().as_millis()).unwrap_or(u64::MAX);
        Ok((ms, errors))
    }

    async fn cleanup(&mut self) {
        if let Some(f) = self.fixture.as_mut() {
            f.cleanup().await;
        }
    }

    fn spawn_progress(
        &self,
        interval: Duration,
        started: Instant,
        deadline: Instant,
        counters: Vec<Arc<WorkerCounters>>,
    ) -> Option<JoinHandle<()>> {
        let client = self.client.as_ref()?;
        Some(spawn_progress_snapshotter(
            interval,
            started,
            deadline,
            counters,
            Arc::clone(client),
        ))
    }

    fn spawn_metrics_flusher(
        &self,
        started: Instant,
        deadline: Instant,
        counters: Vec<Arc<WorkerCounters>>,
        path: PathBuf,
    ) -> Option<JoinHandle<()>> {
        let client = self.client.as_ref()?;
        Some(spawn_metrics_flusher(
            Duration::from_secs(5),
            started,
            deadline,
            counters,
            Arc::clone(client),
            path,
        ))
    }

    fn client_metrics_snapshot(&self) -> crow_kv_client::ClientMetricsSnapshot {
        self.client
            .as_ref()
            .map_or_else(crow_kv_client::ClientMetricsSnapshot::default, |c| c.metrics())
    }

    fn client_transport_stats(&self) -> super::super::report::TransportStatsSnapshot {
        self.client.as_ref().and_then(|c| c.transport_stats()).map_or(
            super::super::report::TransportStatsSnapshot::default(),
            |s| super::super::report::TransportStatsSnapshot {
                read_calls: s.read_calls,
                writev_calls: s.writev_calls,
                frames_sent: s.frames_sent,
                frames_parsed: s.frames_parsed,
                read_bytes: s.read_bytes,
                writev_bytes: s.writev_bytes,
                submit_to_writev_count: s.submit_to_writev.count,
                submit_to_writev_avg_us: s
                    .submit_to_writev
                    .sum_ns
                    .checked_div(s.submit_to_writev.count)
                    .unwrap_or(0)
                    / 1000,
            },
        )
    }

    fn node_ids(&self) -> Vec<u64> {
        self.fixture
            .as_ref()
            .map_or(Vec::new(), |f| f.node_ids().to_vec())
    }

    fn workspace_dir(&self) -> PathBuf {
        self.fixture
            .as_ref()
            .map_or_else(|| PathBuf::from("."), |f| f.workspace_dir().to_path_buf())
    }

    fn endpoint_to_node_map(&self) -> std::collections::HashMap<String, String> {
        self.fixture
            .as_ref()
            .map_or_else(std::collections::HashMap::new, BenchFixture::endpoint_to_node_map)
    }

    fn collect_artifacts(&mut self) -> (ServerMetrics, usize) {
        let Some(f) = self.fixture.as_ref() else {
            return (ServerMetrics::default(), 0);
        };
        let metrics = f.collect_metrics();
        (metrics, 0)
    }

    fn flush_mgmt_urls(&self) -> Vec<String> {
        self.fixture
            .as_ref()
            .map_or(Vec::new(), |f| f.node_mgmt_urls().to_vec())
    }
}

impl KvTarget {
    /// Copy every node's `log/` directory into `run_dir/node-<id>/`.
    pub(crate) fn collect_logs(&self, run_dir: &Path) -> std::io::Result<()> {
        self.fixture.as_ref().map_or(Ok(()), |f| f.collect_logs(run_dir))
    }
}

/// KV bench client: wraps a shared `CrowkvClient` and dispatches ops.
/// Cheaply cloneable (Arc handle).
#[derive(Clone)]
pub(crate) struct KvBenchClient {
    client: Arc<CrowkvClient>,
}

impl BenchClient for KvBenchClient {
    fn issue_op(
        &self,
        kind: OpKind,
        gen: &mut OpGen,
        cfg: &BenchConfig,
        worker_id: u32,
        iter: u64,
    ) -> impl std::future::Future<Output = OpOutcome> + Send {
        self.issue_op_inner(kind, gen, cfg, worker_id, iter)
    }
}

impl KvBenchClient {
    #[allow(
        clippy::too_many_lines,
        reason = "per-op dispatch; splitting reduces readability"
    )]
    async fn issue_op_inner(
        &self,
        kind: OpKind,
        gen: &mut OpGen,
        cfg: &BenchConfig,
        worker_id: u32,
        iter: u64,
    ) -> OpOutcome {
        // Reads draw from the populated range (`next_read_key`) and
        // carry the key_id for spot-check verification; other ops draw
        // from the full `key_space`.
        let (key, read_key_id) = if kind == OpKind::Read {
            let (id, k) = gen.next_read_key();
            (k, Some(id))
        } else {
            (gen.next_key(), None)
        };
        match kind {
            OpKind::Read => {
                let min_slot = cfg.min_slot_policy.to_min_slot();
                match self
                    .client
                    .get(cfg.store_id, cfg.group_id, &key, cfg.read_mode, min_slot)
                    .await
                {
                    Ok(GetOutcome::Found { value, .. }) => {
                        let ok_verify = read_key_id
                            .is_some_and(|id| gen.verify_value(id, value.as_ref(), cfg.verify_bytes));
                        OpOutcome {
                            ok: true,
                            correctness_error: !ok_verify,
                            ..Default::default()
                        }
                    }
                    Ok(GetOutcome::NotFound) => OpOutcome {
                        ok: true,
                        not_found: true,
                        ..Default::default()
                    },
                    Err(ClientError::NotLeader { .. }) => OpOutcome {
                        no_leader: true,
                        ..Default::default()
                    },
                    Err(_) => OpOutcome::default(),
                }
            }
            OpKind::Write => {
                let value = gen.make_value();
                let client_id = u64::from(worker_id) + 1;
                match self
                    .client
                    .put(cfg.store_id, cfg.group_id, &key, &value, Some((client_id, iter)))
                    .await
                {
                    Ok(WriteOutcome { .. }) => OpOutcome {
                        ok: true,
                        ..Default::default()
                    },
                    Err(ClientError::NotLeader { .. }) => OpOutcome {
                        no_leader: true,
                        ..Default::default()
                    },
                    Err(_) => OpOutcome::default(),
                }
            }
            OpKind::Delete => {
                let client_id = u64::from(worker_id) + 1;
                match self
                    .client
                    .delete(cfg.store_id, cfg.group_id, &key, Some((client_id, iter)))
                    .await
                {
                    Ok(_) => OpOutcome {
                        ok: true,
                        ..Default::default()
                    },
                    Err(ClientError::NotLeader { .. }) => OpOutcome {
                        no_leader: true,
                        ..Default::default()
                    },
                    Err(_) => OpOutcome::default(),
                }
            }
            OpKind::List => match self
                .client
                .scan(
                    cfg.store_id,
                    cfg.group_id,
                    &cfg.scan_prefix,
                    &cfg.scan_start_after,
                    &[],
                    cfg.scan_limit,
                    cfg.read_mode,
                    cfg.min_slot_policy.to_min_slot(),
                    false,
                    None,
                )
                .await
            {
                Ok(_) => OpOutcome {
                    ok: true,
                    ..Default::default()
                },
                Err(ClientError::NotLeader { .. }) => OpOutcome {
                    no_leader: true,
                    ..Default::default()
                },
                Err(_) => OpOutcome::default(),
            },
        }
    }
}
