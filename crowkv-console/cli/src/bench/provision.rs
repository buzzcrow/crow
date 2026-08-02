// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! `BenchFixture` — fixture-based cluster provisioning for the `bench
//! benchmark` verb.
//!
//! Mirrors the UI test fixture's `setupCluster`/`teardownCluster`
//! (`crowkv-console/web/ui/e2e/fixtures/consoleSetup.ts`), but through
//! the typed `ConsoleClient` against an **embedded** `crowkv-web`
//! instance started in-process (the same pattern used by the CLI's own
//! integration test harness,
//! `crowkv-console/cli/tests/testkit/console.rs::spawn_console_empty`),
//! so the benchmark is fully self-contained: 1 rack + 3 nodes on
//! localhost, forming a complete Paxos replication group.

use std::path::{Path, PathBuf};
use std::time::Duration;

use crowkv_console_shared::clients::console::{
    AddRackBody, ConsoleClient, CreateGroupBody, CreateStoreBody, DeployNodeServerBody,
};
use crowkv_console_shared::config::{ConsoleConfig, NodeEntry};
use crowkv_console_shared::error::{Error, Result};
use crowkv_console_shared::lifecycle::stop_pid_with_timeout;
use crowkv_console_shared::test_ports::unique_test_port;

use super::report::{aggregate_server_metrics, parse_metrics_log, ServerMetrics};

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
/// `crowkv-web` console instance. Encapsulates the full deploy
/// lifecycle: embedded console-web startup, topology creation via
/// `ConsoleClient`, server deployment, and teardown.
pub struct BenchFixture {
    client: ConsoleClient,
    console_task: tokio::task::JoinHandle<()>,
    node_ids: Vec<String>,
    node_pids: Vec<u32>,
    node_grpc_urls: Vec<String>,
    node_mgmt_urls: Vec<String>,
    leader_endpoint: String,
    workspace_dir: PathBuf,
    stopped: bool,
}

impl BenchFixture {
    /// Provision the fixture: start an embedded console-web instance,
    /// create 1 rack + 3 nodes, deploy a `crowkv-server` per node in
    /// `mode`, wire them into a single 3-replica store/group, and wait
    /// for leader election.
    ///
    /// # Errors
    /// Returns an error if the console-web instance fails to bind, any
    /// provisioning call fails, or no leader is elected within the
    /// timeout.
    pub async fn new(
        mode: BenchMode,
        workspace_dir: PathBuf,
        max_inflight: usize,
        inflight_queues: usize,
        metrics_interval: u64,
        node_config: Option<String>,
    ) -> Result<Self> {
        std::fs::create_dir_all(&workspace_dir)?;

        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await?;
        let addr = listener.local_addr()?;
        let console_config_path = workspace_dir.join("console.toml");
        let state = crowkv_web::AppState::with_config(ConsoleConfig::default(), Some(console_config_path));
        let console_task = tokio::spawn(async move {
            let _ = axum::serve(listener, crowkv_web::router(state)).await;
        });
        let client = ConsoleClient::new(format!("http://{addr}"))?;

        let (ids, pids, grpc_urls, mgmt_urls) = match Self::provision_nodes(
            &client,
            mode,
            max_inflight,
            inflight_queues,
            metrics_interval,
            node_config,
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
    /// `crowkv-server` on each, in `mode`. Returns the node ids and their
    /// server pids (index-aligned).
    async fn provision_nodes(
        client: &ConsoleClient,
        mode: BenchMode,
        max_inflight: usize,
        inflight_queues: usize,
        metrics_interval: u64,
        node_config: Option<String>,
    ) -> Result<(Vec<String>, Vec<u32>, Vec<String>, Vec<String>)> {
        let mut ids = Vec::with_capacity(NODE_COUNT);
        let mut pids = Vec::with_capacity(NODE_COUNT);
        let mut grpc_urls = Vec::with_capacity(NODE_COUNT);
        let mut mgmt_urls = Vec::with_capacity(NODE_COUNT);
        for i in 0..NODE_COUNT {
            let rack_id = format!("br{i}");
            let node_id = format!("bn{i}");
            client
                .add_rack(&AddRackBody {
                    id: rack_id.clone(),
                    name: rack_id.clone(),
                })
                .await
                .map_err(|e| upstream_err(&rack_id, "add_rack", &e))?;
            client
                .add_node(
                    &rack_id,
                    &NodeEntry {
                        id: node_id.clone(),
                        rack_id: rack_id.clone(),
                        host: "127.0.0.1".into(),
                        ssh_port: 22,
                        ssh_user: String::new(),
                        ssh_key: None,
                        ssh_password: None,
                    },
                )
                .await
                .map_err(|e| upstream_err(&node_id, "add_node", &e))?;

            let mut body = DeployNodeServerBody {
                mgmt_port: unique_test_port(),
                grpc_port: unique_test_port(),
                election_profile: Some("e2e".into()),
                metrics_interval: Some(metrics_interval),
                max_inflight: Some(max_inflight),
                inflight_queues: Some(inflight_queues),
                ..Default::default()
            };
            mode.apply_to(&mut body);
            body.config = node_config.clone();
            let deployed = client
                .deploy_node_server(&node_id, &body)
                .await
                .map_err(|e| upstream_err(&node_id, "deploy_node_server", &e))?;

            ids.push(node_id);
            pids.push(deployed.pid);
            grpc_urls.push(deployed.grpc_url);
            mgmt_urls.push(deployed.mgmt_url);
        }
        Ok((ids, pids, grpc_urls, mgmt_urls))
    }

    /// Create the single store spanning all nodes, then a 3-replica
    /// group over the same nodes.
    async fn provision_store_and_group(client: &ConsoleClient, node_ids: &[String]) -> Result<()> {
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

    /// Per-node `crowkv-server` management-API URLs (one per deployed
    /// node). Each serves `/topology`, which a bench client with
    /// `ReadEndpointPolicy::AnyReplica` fetches to learn the full
    /// replica list for `MinSlot` read distribution.
    #[must_use]
    pub fn node_mgmt_urls(&self) -> &[String] {
        &self.node_mgmt_urls
    }

    /// Node ids provisioned by this fixture, in deploy order.
    #[must_use]
    pub fn node_ids(&self) -> &[String] {
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
            .map(|(nid, url)| (url.clone(), nid.clone()))
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
            .filter_map(|(node_id, pid)| self.read_node_metrics_log(node_id, *pid))
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
            let src = self.node_workspace(node_id).join("log");
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
            let _ = self.client.stop_node_server(node_id).await;
        }
        self.console_task.abort();
    }

    fn node_workspace(&self, node_id: &str) -> PathBuf {
        self.workspace_dir.join(format!("N-{node_id}"))
    }

    /// Locate and read this node's `log/crowkv-server-metrics-<timestamp>-<pid>.log`
    /// file (see `crowkv::common::logging::open_metrics_log`).
    fn read_node_metrics_log(&self, node_id: &str, pid: u32) -> Option<String> {
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
    use crowkv_client::{ClientConfig, CrowkvClient};
    use crowkv_console_shared::clients::http::ServerClient;

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
