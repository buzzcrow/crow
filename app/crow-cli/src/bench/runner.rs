// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Bench runner: connection pool + worker tasks + stats aggregation.
//!
//! Key work: build N gRPC channels (the connection pool), spawn M
//! tokio tasks (the workers) that each clone a channel, drive a loop
//! issuing ops until `duration` elapses, collect per-op-kind histograms
//! and counters, and emit a `BenchReport`.
//!
//! In-flight bound (closed loop, no extra knob).
//! Each worker is a strict closed loop: issue one op via `kv.{get,put,
//! delete,scan}().await`, await completion, record latency, repeat.
//! There is no internal queue and no per-worker pipelining, so the
//! number of in-flight requests at any instant is exactly bounded by
//! `cfg.threads`. Doubling `--threads` doubles both offered load and
//! observable concurrency by the same factor; tail latency cannot blow
//! up due to runner-side queueing. This is why no separate
//! `--max-in-flight` flag exists.
//!
//! Deliberately uses tokio tasks instead of OS threads. The underlying
//! client is async (`tonic::Channel` multiplexes over HTTP/2); tokio
//! tasks on a multi-thread runtime give us the same parallelism without
//! the bookkeeping cost of one `std::thread` per worker. A 1000-task
//! ceiling is well below tokio's per-task overhead at this scale.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::Utc;
use crow_console_shared::error::{Error, Result};
use crow_kv_client::{ClientConfig, CrowkvClient, ReadEndpointPolicy, ReadMode};
use tracing::{info, warn};

use super::metrics_flusher::{spawn_metrics_flusher, spawn_progress_snapshotter};
use super::report::{per_op_map, BenchReport, OpStats};
use super::worker::{run_worker, WorkerCounters};
use super::workload::{format_key, value_for, MinSlotPolicy, OpGen, OpKind, WorkloadKind};

/// Knobs controlling a single bench invocation.
#[derive(Debug, Clone)]
pub(crate) struct BenchConfig {
    /// Tonic-friendly endpoint, e.g. `"127.0.0.1:28001"` (no scheme).
    pub(crate) endpoint: String,
    pub(crate) store_id: u64,
    pub(crate) group_id: u64,
    pub(crate) workload: WorkloadKind,
    /// Storage mode label: `mem`, `file`, or `block`.
    pub(crate) mode: String,
    /// Number of independent gRPC channels (1..=64). Default 4.
    pub(crate) connections: u32,
    /// Number of worker tasks (1..=1000). Default 8.
    pub(crate) threads: u32,
    pub(crate) duration: Duration,
    pub(crate) key_space: u64,
    pub(crate) value_size: usize,
    /// Mixed value-size distribution for pre-population. When set,
    /// each pre-populated key gets a size from `ValueSizeMix::size_for(id)`
    /// instead of the fixed `value_size`. Scan benches use this to
    /// exercise multiple value sizes in a single run. `None` (default)
    /// uses the fixed `value_size`.
    pub(crate) value_size_mix: Option<super::workload::ValueSizeMix>,
    /// Report directory: `bench-runs/<run-folder>/`. The report is
    /// written as `report.json` inside this dir. If `None`, defaults
    /// to `bench-runs/`.
    pub(crate) report_dir: Option<std::path::PathBuf>,
    /// Optional run-id; defaults to `bench-<unix_millis>-<workload>`.
    pub(crate) run_id: Option<String>,
    /// If `Some(d)`, a tokio task wakes every `d` and emits one human
    /// progress line to stderr (`[+12s] ops=124k qps=10333 err=0`).
    /// The path is lock-free on the worker hot loop — workers only
    /// touch their own `Arc<WorkerCounters>` atomics with `Relaxed`
    /// ordering. `None` (or `Some(Duration::ZERO)`) disables progress.
    pub(crate) progress_interval: Option<Duration>,
    /// Optional path for a periodic client-side metrics log file.
    /// When set, a background task flushes per-op-kind latency
    /// percentiles and counters every 5 seconds to this file.
    pub(crate) metrics_log_path: Option<std::path::PathBuf>,
    /// Optional warmup window at the start of the run during which
    /// workers issue ops normally but **discard** all records — no
    /// histogram entries, no counter bumps, no contribution to the
    /// final report. This lets cold-start artifacts (TCP slow-start,
    /// channel handshakes, server JIT-equivalents) settle before
    /// measurement begins. `None` or `Duration::ZERO` disables warmup.
    /// The reported `duration_ms` reflects the **measurement** window
    /// (`duration` minus `warmup`); `warmup_ms` is surfaced separately
    /// in the report so operators can see what was discarded.
    pub(crate) warmup: Option<Duration>,
    /// Read mode for read ops. Default `Linearizable`. Ignored for
    /// write/delete ops.
    pub(crate) read_mode: ReadMode,
    /// `min_slot` resolution policy for `MinSlot` reads. `Auto` passes
    /// `None` so the client auto-attaches its write watermark; `Zero`
    /// forces `Some(0)`; `Fixed(n)` forces `Some(n)`. Ignored for
    /// `Linearizable` reads and non-read ops.
    pub(crate) min_slot_policy: MinSlotPolicy,
    /// Pre-population count: write `[0, count)` keys with deterministic
    /// values before warmup begins. `None` or `0` disables. Default
    /// 200,000. Not measured (excluded from latency/TPS); reported
    /// separately as `pre_pop_ms` / `pre_pop_errors`. Also establishes
    /// the client's `write_watermark` so `MinSlot` reads with
    /// `min_slot = auto` carry it.
    pub(crate) pre_populate: Option<u64>,
    /// Number of random bytes to spot-check per `Found` read against
    /// the deterministic `byte_at(key_id, offset)` formula. Default 8.
    /// 0 disables verification.
    pub(crate) verify_bytes: usize,
    /// `MinSlot` read-endpoint selection policy. `Leader` (default)
    /// routes `MinSlot` reads to the leader (same as `Linearizable`);
    /// `AnyReplica` distributes `MinSlot` reads round-robin across all
    /// replicas; `LeastConnections` routes to the fewest in-flight;
    /// `Latency` routes to the lowest recent RTT. Ignored for
    /// `Linearizable` reads (always target leader).
    pub(crate) read_endpoint_policy: ReadEndpointPolicy,
    /// Topology seed URL (the console-web's `/topology` endpoint).
    /// When set, the client can fetch the full replica list so
    /// distributed policies have endpoints to select from. `None`
    /// leaves the client with an empty seed list (no topology fetch —
    /// fine for `Leader` policy, but distributed policies would have
    /// no replicas).
    pub(crate) topology_seed: Option<String>,
    /// Scan limit (max entries per scan op) for `WorkloadKind::List`.
    /// Default 1 (the historical stub behavior).
    pub(crate) scan_limit: u32,
    /// Scan prefix for `WorkloadKind::List`. Empty = whole keyspace.
    pub(crate) scan_prefix: Vec<u8>,
    /// Scan exclusive lower bound (`start_after`) for
    /// `WorkloadKind::List`. Empty = start from the beginning.
    pub(crate) scan_start_after: Vec<u8>,
    /// If `true`, after pre-population completes the runner drains L0
    /// (`MemTable`) into L1 on every node via each `flush_mgmt_urls`
    /// management API `POST .../flush`, then opens the measurement
    /// window. Produces a clean L1-only scan baseline so the
    /// `MemTable::snapshot()` `O(N_l0)` cost is removed from the
    /// measurement. `false` (default) leaves L0 size dependent on the
    /// pre-pop write rate / value size (the historical behavior).
    pub(crate) flush_after_prepopulate: bool,
    /// Per-node management API URLs to hit with `POST .../flush` when
    /// `flush_after_prepopulate` is set. Empty (default) means the flag
    /// is a no-op even if set — the bench fixture populates this.
    pub(crate) flush_mgmt_urls: Vec<String>,
}

impl BenchConfig {
    #[must_use]
    pub(crate) fn defaults(endpoint: impl Into<String>, workload: WorkloadKind) -> Self {
        Self {
            endpoint: endpoint.into(),
            store_id: 1,
            group_id: 1,
            workload,
            mode: String::new(),
            connections: 4,
            threads: 8,
            duration: Duration::from_secs(5),
            key_space: 1_000,
            value_size: 64,
            value_size_mix: None,
            report_dir: None,
            run_id: None,
            progress_interval: None,
            metrics_log_path: None,
            warmup: None,
            read_mode: ReadMode::Linearizable,
            min_slot_policy: MinSlotPolicy::Auto,
            pre_populate: None,
            verify_bytes: 8,
            read_endpoint_policy: ReadEndpointPolicy::Leader,
            topology_seed: None,
            scan_limit: 1,
            scan_prefix: Vec::new(),
            scan_start_after: Vec::new(),
            flush_after_prepopulate: false,
            flush_mgmt_urls: Vec::new(),
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
        if let Some(w) = self.warmup {
            if w >= self.duration {
                return Err(bad("--warmup-secs must be strictly less than --duration-secs"));
            }
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
#[allow(clippy::too_many_lines)]
pub(crate) async fn run_bench(cfg: BenchConfig) -> Result<(BenchReport, std::path::PathBuf)> {
    cfg.validate()?;
    info!(
        endpoint = %cfg.endpoint,
        workload = ?cfg.workload,
        threads = cfg.threads,
        connections = cfg.connections,
        duration_ms = u64::try_from(cfg.duration.as_millis()).unwrap_or(u64::MAX),
        "bench: starting"
    );

    // Single `CrowkvClient`, seeded directly at `cfg.endpoint` (bench
    // targets one already-known leader, no `/topology` discovery needed --
    // an empty mgmt-seed list is fine). `pool_size_per_endpoint` reproduces
    // the old runner's "N independent gRPC channels, round-robined" pool
    // via the client's own per-endpoint channel pool, so every worker
    // shares one client and its internal pool rather than owning a
    // channel directly. When `topology_seed` is set (MinSlot benches
    // with a distributed policy), the seed list is non-empty so the
    // client can fetch `/topology` and learn the full replica list for
    // distribution.
    let mut client_config = ClientConfig::new(cfg.topology_seed.clone().map(|s| vec![s]).unwrap_or_default());
    client_config.pool_size_per_endpoint = cfg.connections as usize;
    client_config.read_endpoint_policy = cfg.read_endpoint_policy;
    let client = CrowkvClient::new(client_config);
    client.seed_leader(cfg.store_id, cfg.group_id, cfg.endpoint.clone());
    let client = Arc::new(client);

    // Pre-population phase: sequentially write `[0, pre_populate)` keys
    // with deterministic values before the measurement window begins.
    // Not measured (excluded from latency/TPS); reported separately as
    // `pre_pop_ms` / `pre_pop_errors`. Also establishes the client's
    // `write_watermark` so `MinSlot` reads with `min_slot = auto`
    // carry it. Retries on `NotLeader` (the client follows the hint
    // internally, so a plain `put` retry loop suffices).
    let (pre_pop_ms, pre_pop_errors) = match cfg.pre_populate {
        Some(count) if count > 0 => {
            info!(count, "bench: pre-populating key space");
            let pop_start = Instant::now();
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
                        Err(crow_kv_client::Error::NotLeader { .. }) if attempts < 8 => {}
                        Err(_) => {
                            errors += 1;
                            break;
                        }
                    }
                }
            }
            let ms = u64::try_from(pop_start.elapsed().as_millis()).unwrap_or(u64::MAX);
            info!(ms, errors, "bench: pre-population done");
            (ms, errors)
        }
        _ => (0, 0),
    };

    // Optional L0 drain: after pre-pop, force every node's engine to
    // flush its MemTable into L1 before the measurement window opens.
    // The leader has applied all pre-pop writes by the time the last
    // `put` returns; followers may still be applying the learn-stream
    // tail, so wait briefly for them to converge before flushing (flush
    // only drains entries with slot <= contiguous_slot). A failed flush
    // on one node is logged but does not abort the bench — it degrades
    // the measurement's cleanliness, not its correctness.
    if cfg.flush_after_prepopulate && !cfg.flush_mgmt_urls.is_empty() {
        info!(
            nodes = cfg.flush_mgmt_urls.len(),
            "bench: draining L0 after pre-pop"
        );
        let flush_start = Instant::now();
        tokio::time::sleep(Duration::from_millis(500)).await;
        let mut failures = 0u32;
        for url in &cfg.flush_mgmt_urls {
            match crow_console_shared::clients::http::ServerClient::new(url) {
                Ok(sc) => match sc.flush(cfg.store_id, cfg.group_id).await {
                    Ok(()) => {}
                    Err(e) => {
                        failures += 1;
                        warn!(url, error = %e, "bench: flush failed for node");
                    }
                },
                Err(e) => {
                    failures += 1;
                    warn!(url, error = %e, "bench: flush client build failed for node");
                }
            }
        }
        info!(
            ms = u64::try_from(flush_start.elapsed().as_millis()).unwrap_or(u64::MAX),
            failures, "bench: L0 drain done"
        );
    }

    let started_at = Utc::now();
    let started_instant = Instant::now();
    let deadline = started_instant + cfg.duration;
    // Records issued before `measure_start` are discarded by the
    // worker. When warmup is None / zero, this collapses to
    // `started_instant` so the first op already counts.
    let warmup_dur = cfg.warmup.unwrap_or(Duration::ZERO);
    let measure_start = started_instant + warmup_dur;

    // Per-worker live counters (lock-free). The optional progress
    // snapshotter task reads these every `progress_interval` and prints
    // a one-line summary; workers never read each other's counters.
    let mut counters: Vec<Arc<WorkerCounters>> = Vec::with_capacity(cfg.threads as usize);
    for _ in 0..cfg.threads {
        counters.push(Arc::new(WorkerCounters::new()));
    }

    let progress_handle = match cfg.progress_interval {
        Some(d) if !d.is_zero() => Some(spawn_progress_snapshotter(
            d,
            started_instant,
            deadline,
            counters.clone(),
            Arc::clone(&client),
        )),
        _ => None,
    };

    let metrics_handle = cfg.metrics_log_path.as_ref().map(|path| {
        spawn_metrics_flusher(
            Duration::from_secs(5),
            started_instant,
            deadline,
            counters.clone(),
            Arc::clone(&client),
            path.clone(),
        )
    });

    let mut handles = Vec::with_capacity(cfg.threads as usize);
    for worker_id in 0..cfg.threads {
        let client = client.clone();
        let cfg2 = cfg.clone();
        let counters = counters[worker_id as usize].clone();
        let handle = tokio::spawn(async move {
            // Per-worker rng seed = worker_id for determinism.
            let mut gen = OpGen::new(
                u64::from(worker_id) ^ 0x9E37_79B9_7F4A_7C15,
                cfg2.key_space,
                cfg2.value_size,
            );
            // Read benches with pre-population draw read keys from the
            // populated range so reads return `Found` (not `NotFound`).
            if let Some(count) = cfg2.pre_populate {
                if count > 0 {
                    gen.set_read_key_space(count);
                }
            }
            run_worker(
                &client,
                &mut gen,
                &cfg2,
                measure_start,
                deadline,
                worker_id,
                &counters,
            )
            .await
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

    if let Some(h) = progress_handle {
        // Snapshotter exits on its own once the deadline passes; await
        // it so its final tick (if any) is flushed before we print the
        // run summary.
        let _ = h.await;
    }

    if let Some(h) = metrics_handle {
        let _ = h.await;
    }

    let finished_at = Utc::now();
    let total_attempts: u64 = by_kind.values().map(|s| s.ops).sum();
    let total_errors: u64 = by_kind.values().map(|s| s.errors).sum();
    let total_correctness_errors: u64 = by_kind.values().map(|s| s.correctness_errors).sum();
    let total_ops = total_attempts - total_errors;
    #[allow(clippy::cast_precision_loss)]
    let error_rate = if total_attempts == 0 {
        0.0
    } else {
        total_errors as f64 / total_attempts as f64
    };

    let client_metrics = client.metrics();

    let run_id = cfg.run_id.clone().unwrap_or_else(|| {
        let ms = started_at.timestamp_millis();
        format!("bench-{ms}-{:?}", cfg.workload).to_ascii_lowercase()
    });

    // Measurement window is the configured duration minus warmup.
    // Workers stop at `deadline = started_instant + cfg.duration` and
    // only record between `measure_start` and `deadline`, so the
    // effective injection window is exactly `cfg.duration - warmup`.
    // Using `actual_duration` would include post-deadline overhead
    // (worker join, metrics flush), inflating the denominator and
    // deflating reported TPS.
    let measure_ms = u64::try_from(cfg.duration.saturating_sub(warmup_dur).as_millis()).unwrap_or(u64::MAX);
    let warmup_ms = u64::try_from(warmup_dur.as_millis()).unwrap_or(u64::MAX);

    let report = BenchReport {
        run_id: run_id.clone(),
        started_at,
        finished_at,
        duration_ms: measure_ms,
        warmup_ms,
        workload: cfg.workload,
        mode: cfg.mode.clone(),
        connections: cfg.connections,
        threads: cfg.threads,
        key_space: cfg.key_space,
        value_size: cfg.value_size,
        target_endpoint: cfg.endpoint.clone(),
        store_id: cfg.store_id,
        group_id: cfg.group_id,
        total_ops,
        total_attempts,
        total_errors,
        error_rate,
        correctness_errors: total_correctness_errors,
        pre_pop_ms,
        pre_pop_errors,
        by_op: per_op_map(by_kind),
        // Populated by `bench benchmark` (R10) after collecting each
        // node's `log/metrics.log`; plain `bench run`/`stress` have no
        // deployed nodes to collect from, so this stays default/empty.
        server_metrics: super::report::ServerMetrics::default(),
        client_metrics,
    };

    let dir = cfg.report_dir.clone().unwrap_or_else(BenchReport::default_dir);
    let path = report
        .write_to(&dir)
        .map_err(|e| Error::Config(format!("write report: {e}")))?;
    info!(path = %path.display(), total_ops, error_rate, "bench: finished");

    Ok((report, path))
}
