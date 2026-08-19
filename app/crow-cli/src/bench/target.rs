// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Target abstraction: each bench target (KV, RPC, future diskdb/chunkdb)
//! implements `BenchTarget` + `BenchClient`. The shared runner/worker
//! drive the op loop; the target handles provisioning, client creation,
//! and per-op dispatch.

// The submodules live in `targets/` (not `target/`) to avoid matching
// the `.gitignore` `target` entry for Rust build output.
#[path = "targets/kv.rs"]
pub(crate) mod kv;
#[path = "targets/rpc.rs"]
pub(crate) mod rpc;

use std::sync::Arc;
use std::time::{Duration, Instant};

use crow_console_shared::error::Result;
use crow_kv_client::ClientMetricsSnapshot;
use tokio::task::JoinHandle;

use super::report::OpOutcome;
use super::worker::WorkerCounters;
use super::workload::{OpGen, OpKind};
use crate::bench::runner::BenchConfig;

/// A benchmark target: provisions the server, builds per-worker clients,
/// and provides optional progress/metrics hooks. One instance per bench
/// run.
pub(crate) trait BenchTarget: Send {
    type Client: BenchClient;

    /// Short label for the report: "kv", "rpc", etc.
    fn label(&self) -> &'static str;

    /// Provision the server(s). Called before measurement.
    async fn provision(&mut self, cfg: &BenchConfig) -> Result<()>;

    /// Build a client for one worker. Called `cfg.threads` times.
    async fn build_client(&self) -> Result<Self::Client>;

    /// Pre-populate the key space. Returns (ms, errors). KV-only; RPC
    /// and future targets return (0, 0).
    async fn pre_populate(&self, client: &Self::Client, cfg: &BenchConfig) -> Result<(u64, u64)>;

    /// Cleanup: stop servers, kill processes, etc.
    async fn cleanup(&mut self);

    /// Whether this target supports pipelined sends. KV = false
    /// (closed-loop); RPC = true.
    #[allow(dead_code, reason = "used by RPC target in Phase 3")]
    fn supports_pipeline(&self) -> bool {
        false
    }

    /// Default pipeline depth when `--pipeline-depth` is not set.
    /// RPC: `connections * threads` (capped); others: 1.
    #[allow(dead_code, reason = "used by RPC target in Phase 3")]
    fn default_pipeline_depth(&self, _cfg: &BenchConfig) -> usize {
        1
    }

    /// Spawn the optional progress snapshotter. Returns `None` if the
    /// target has no progress reporting (RPC). KV overrides this with
    /// the existing `spawn_progress_snapshotter`.
    fn spawn_progress(
        &self,
        _interval: Duration,
        _started: Instant,
        _deadline: Instant,
        _counters: Vec<Arc<WorkerCounters>>,
    ) -> Option<JoinHandle<()>> {
        None
    }

    /// Spawn the optional metrics flusher. Returns `None` if the target
    /// has no metrics logging (RPC). KV overrides this with the existing
    /// `spawn_metrics_flusher`.
    fn spawn_metrics_flusher(
        &self,
        _started: Instant,
        _deadline: Instant,
        _counters: Vec<Arc<WorkerCounters>>,
        _path: std::path::PathBuf,
    ) -> Option<JoinHandle<()>> {
        None
    }

    /// Final client-side metrics snapshot for the report. KV returns
    /// the `CrowkvClient`'s counters; RPC returns default (empty).
    fn client_metrics_snapshot(&self) -> ClientMetricsSnapshot {
        ClientMetricsSnapshot::default()
    }

    /// Node IDs for the markdown report (KV: 3-node cluster; RPC: empty).
    fn node_ids(&self) -> Vec<u64> {
        Vec::new()
    }

    /// Workspace dir for the markdown report (KV: fixture workspace; RPC: empty).
    fn workspace_dir(&self) -> std::path::PathBuf {
        std::path::PathBuf::from(".")
    }

    /// Endpoint-to-node map for the markdown report (KV only).
    fn endpoint_to_node_map(&self) -> std::collections::HashMap<String, String> {
        std::collections::HashMap::new()
    }

    /// Collect server metrics + logs after the run (KV only). Returns
    /// (`server_metrics`, `log_warning_count`).
    fn collect_artifacts(&mut self) -> (super::report::ServerMetrics, usize) {
        (super::report::ServerMetrics::default(), 0)
    }

    /// Per-node management API URLs for L0 flush (KV only). Empty for
    /// RPC and other targets that don't have a flush API.
    fn flush_mgmt_urls(&self) -> Vec<String> {
        Vec::new()
    }
}

/// A client that can issue bench ops. One instance per worker. The
/// worker calls `issue_op` in a closed loop (or pipelined via semaphore
/// when `pipeline_depth > 1`). Must be cheaply cloneable (typically an
/// `Arc` handle to a shared client).
pub(crate) trait BenchClient: Send + Sync + Clone + 'static {
    /// Issue one op. The caller measures latency and records the outcome.
    fn issue_op(
        &self,
        kind: OpKind,
        gen: &mut OpGen,
        cfg: &BenchConfig,
        worker_id: u32,
        iter: u64,
    ) -> impl std::future::Future<Output = OpOutcome> + Send;
}
