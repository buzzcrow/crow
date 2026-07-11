//! Bench runner: connection pool + worker tasks + stats aggregation.
//!
//! Key work: build N gRPC channels (the connection pool), spawn M
//! tokio tasks (the workers) that each clone a channel, drive a loop
//! issuing ops until `duration` elapses, collect per-op-kind histograms
//! and counters, and emit a `BenchReport`.
//!
//! Deliberately uses tokio tasks instead of OS threads. The plan calls
//! the worker model "blocking-loop threads" but the underlying client
//! is async (`tonic::Channel` multiplexes over HTTP/2); tokio tasks on
//! a multi-thread runtime give us the same parallelism without the
//! bookkeeping cost of one `std::thread` per worker. A 1000-task ceiling
//! is well below tokio's per-task overhead at this scale.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::Utc;
use crowkv_console_core::clients::grpc::{GetOutcome, KvClient, WriteOutcome};
use crowkv_console_core::error::{Error, Result};
use tracing::{debug, info, warn};

use crate::report::{per_op_map, BenchReport, OpStats};
use crate::workload::{OpGen, OpKind, WorkloadKind};

/// Knobs controlling a single bench invocation.
#[derive(Debug, Clone)]
pub struct BenchConfig {
    /// Tonic-friendly endpoint, e.g. `"127.0.0.1:28001"` (no scheme).
    pub endpoint: String,
    pub store_id: u64,
    pub group_id: u64,
    pub workload: WorkloadKind,
    /// Number of independent gRPC channels (1..=64). Default 4.
    pub connections: u32,
    /// Number of worker tasks (1..=1000). Default 8.
    pub threads: u32,
    pub duration: Duration,
    pub key_space: u64,
    pub value_size: usize,
    /// Optional output dir. `None` → `~/.crowkv/bench`.
    pub report_dir: Option<std::path::PathBuf>,
    /// Optional run-id; defaults to `bench-<unix_millis>-<workload>`.
    pub run_id: Option<String>,
}

impl BenchConfig {
    #[must_use]
    pub fn defaults(endpoint: impl Into<String>, workload: WorkloadKind) -> Self {
        Self {
            endpoint: endpoint.into(),
            store_id: 1,
            group_id: 1,
            workload,
            connections: 4,
            threads: 8,
            duration: Duration::from_secs(5),
            key_space: 1_000,
            value_size: 64,
            report_dir: None,
            run_id: None,
        }
    }

    fn validate(&self) -> Result<()> {
        let bad = |reason: &str| Error::Config(reason.to_string());
        if !(1..=64).contains(&self.connections) {
            return Err(bad("--connections must be in 1..=64"));
        }
        if !(1..=1000).contains(&self.threads) {
            return Err(bad("--threads must be in 1..=1000"));
        }
        if self.duration.is_zero() {
            return Err(bad("--duration must be > 0"));
        }
        if self.key_space == 0 {
            return Err(bad("--key-space must be > 0"));
        }
        Ok(())
    }
}

/// Run a bench end-to-end. Returns the populated report and the path
/// where it was written.
///
/// # Errors
/// Configuration errors, all-connection-failure during pool build, or
/// I/O errors while writing the report file.
pub async fn run_bench(cfg: BenchConfig) -> Result<(BenchReport, std::path::PathBuf)> {
    cfg.validate()?;
    info!(
        endpoint = %cfg.endpoint,
        workload = ?cfg.workload,
        threads = cfg.threads,
        connections = cfg.connections,
        duration_ms = u64::try_from(cfg.duration.as_millis()).unwrap_or(u64::MAX),
        "bench: starting"
    );

    // Build connection pool. Each entry is a base `KvClient` whose inner
    // tonic `Channel` is cheap to clone per worker.
    let mut pool: Vec<KvClient> = Vec::with_capacity(cfg.connections as usize);
    for i in 0..cfg.connections {
        match KvClient::connect(cfg.endpoint.clone()).await {
            Ok(c) => pool.push(c),
            Err(e) => {
                warn!(idx = i, error = %e, "bench: connection failed; aborting");
                return Err(e);
            }
        }
    }
    let pool = Arc::new(pool);

    let started_at = Utc::now();
    let started_instant = Instant::now();
    let deadline = started_instant + cfg.duration;

    let mut handles = Vec::with_capacity(cfg.threads as usize);
    for worker_id in 0..cfg.threads {
        let pool = pool.clone();
        let cfg2 = cfg.clone();
        let handle = tokio::spawn(async move {
            // Each worker clones its assigned base client (per-worker
            // Channel is multiplexed; cloning is cheap).
            let mut kv = pool[worker_id as usize % pool.len()].clone();
            // Per-worker rng seed = worker_id for determinism.
            let mut gen = OpGen::new(u64::from(worker_id) ^ 0x9E37_79B9_7F4A_7C15, cfg2.key_space, cfg2.value_size);
            run_worker(&mut kv, &mut gen, &cfg2, deadline, worker_id).await
        });
        handles.push(handle);
    }

    // Reduce per-worker stats into one map.
    let mut by_kind: BTreeMap<OpKind, OpStats> = BTreeMap::new();
    for h in handles {
        let local = match h.await {
            Ok(s) => s,
            Err(e) => {
                warn!(error = %e, "bench: worker join failed");
                continue;
            }
        };
        for (k, s) in local {
            by_kind.entry(k).or_default().merge(&s);
        }
    }

    let finished_at = Utc::now();
    let actual_duration = started_instant.elapsed();
    let total_ops: u64 = by_kind.values().map(|s| s.ops).sum();
    let total_errors: u64 = by_kind.values().map(|s| s.errors).sum();
    #[allow(clippy::cast_precision_loss)]
    let error_rate = if total_ops == 0 { 0.0 } else { total_errors as f64 / total_ops as f64 };

    let run_id = cfg.run_id.clone().unwrap_or_else(|| {
        let ms = started_at.timestamp_millis();
        format!("bench-{ms}-{:?}", cfg.workload).to_ascii_lowercase()
    });

    let report = BenchReport {
        run_id: run_id.clone(),
        started_at,
        finished_at,
        duration_ms: u64::try_from(actual_duration.as_millis()).unwrap_or(u64::MAX),
        workload: cfg.workload,
        connections: cfg.connections,
        threads: cfg.threads,
        key_space: cfg.key_space,
        value_size: cfg.value_size,
        target_endpoint: cfg.endpoint.clone(),
        store_id: cfg.store_id,
        group_id: cfg.group_id,
        total_ops,
        total_errors,
        error_rate,
        by_op: per_op_map(by_kind),
    };

    let dir = cfg
        .report_dir
        .clone()
        .or_else(BenchReport::default_dir)
        .ok_or_else(|| Error::Config("cannot resolve report directory (no $HOME?)".into()))?;
    let path = report.write_to(&dir).map_err(|e| Error::Config(format!("write report: {e}")))?;
    info!(path = %path.display(), total_ops, error_rate, "bench: finished");

    Ok((report, path))
}

/// Run a single worker until `deadline`, returning its local per-op
/// stats. Errors during ops are recorded in the histogram (with `ok=false`)
/// rather than aborting the worker.
async fn run_worker(kv: &mut KvClient, gen: &mut OpGen, cfg: &BenchConfig, deadline: Instant, worker_id: u32) -> BTreeMap<OpKind, OpStats> {
    let mut stats: BTreeMap<OpKind, OpStats> = BTreeMap::new();
    let mut iter: u64 = 0;

    loop {
        if Instant::now() >= deadline {
            break;
        }
        iter = iter.wrapping_add(1);

        let kind = match cfg.workload {
            WorkloadKind::Read => OpKind::Read,
            WorkloadKind::Write => OpKind::Write,
            WorkloadKind::List => OpKind::List,
            WorkloadKind::Mix => gen.pick_mix_kind(),
        };

        let key = gen.next_key();
        let t0 = Instant::now();
        let (ok, not_found) = match kind {
            OpKind::Read => match kv.get(cfg.group_id, &key).await {
                Ok(GetOutcome::Found { .. }) => (true, false),
                Ok(GetOutcome::NotFound) => (true, true),
                Err(_) => (false, false),
            },
            OpKind::Write => {
                let value = gen.make_value();
                let client_id = u64::from(worker_id) + 1;
                match kv.put(cfg.group_id, &key, &value, client_id, iter).await {
                    Ok(WriteOutcome { .. }) => (true, false),
                    Err(_) => (false, false),
                }
            }
            OpKind::Delete => {
                let client_id = u64::from(worker_id) + 1;
                match kv.delete(cfg.group_id, &key, client_id, iter).await {
                    Ok(_) => (true, false),
                    Err(_) => (false, false),
                }
            }
            OpKind::List => match kv.scan(cfg.group_id, b"", 1).await {
                Ok(_) => (true, false),
                Err(_) => (false, false),
            },
        };
        let lat_us = u64::try_from(t0.elapsed().as_micros()).unwrap_or(u64::MAX);
        stats.entry(kind).or_default().record(lat_us, ok, not_found);

        // Yield periodically so heavy worker counts cooperate.
        if iter % 64 == 0 {
            tokio::task::yield_now().await;
        }
    }

    debug!(worker_id, total = iter, "bench: worker stop");
    stats
}
