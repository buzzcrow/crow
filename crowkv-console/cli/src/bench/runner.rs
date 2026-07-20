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
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::Utc;
use crowkv_client::{
    ClientConfig, CrowkvClient, Error as ClientError, GetOutcome, ReadMode, WindowLatencySnapshot,
    WriteOutcome,
};
use crowkv_console_shared::error::{Error, Result};
use hdrhistogram::Histogram;
use tracing::{debug, info, warn};

use super::report::{per_op_map, percentiles_from_histogram, BenchReport, OpStats};
use super::workload::{OpGen, OpKind, WorkloadKind};

/// Lock-free per-worker counters used by the optional progress
/// snapshotter and the metrics flusher. Workers bump these on every op
/// with `Relaxed` ordering — there is no contention because each worker
/// owns its `Arc<WorkerCounters>` exclusively. Per-op-kind ok/err counts
/// let the metrics log distinguish successful from failed operations.
#[derive(Debug, Default)]
struct WorkerCounters {
    put_ok: AtomicU64,
    put_err: AtomicU64,
    get_ok: AtomicU64,
    get_err: AtomicU64,
    delete_ok: AtomicU64,
    delete_err: AtomicU64,
    scan_ok: AtomicU64,
    scan_err: AtomicU64,
}

impl WorkerCounters {
    fn total_ops(&self) -> u64 {
        self.put_ok.load(Ordering::Relaxed)
            + self.get_ok.load(Ordering::Relaxed)
            + self.delete_ok.load(Ordering::Relaxed)
            + self.scan_ok.load(Ordering::Relaxed)
    }

    fn total_errors(&self) -> u64 {
        self.put_err.load(Ordering::Relaxed)
            + self.get_err.load(Ordering::Relaxed)
            + self.delete_err.load(Ordering::Relaxed)
            + self.scan_err.load(Ordering::Relaxed)
    }

    fn record(&self, kind: OpKind, ok: bool) {
        match (kind, ok) {
            (OpKind::Write, true) => self.put_ok.fetch_add(1, Ordering::Relaxed),
            (OpKind::Write, false) => self.put_err.fetch_add(1, Ordering::Relaxed),
            (OpKind::Read, true) => self.get_ok.fetch_add(1, Ordering::Relaxed),
            (OpKind::Read, false) => self.get_err.fetch_add(1, Ordering::Relaxed),
            (OpKind::Delete, true) => self.delete_ok.fetch_add(1, Ordering::Relaxed),
            (OpKind::Delete, false) => self.delete_err.fetch_add(1, Ordering::Relaxed),
            (OpKind::List, true) => self.scan_ok.fetch_add(1, Ordering::Relaxed),
            (OpKind::List, false) => self.scan_err.fetch_add(1, Ordering::Relaxed),
        };
    }
}

/// Knobs controlling a single bench invocation.
#[derive(Debug, Clone)]
pub struct BenchConfig {
    /// Tonic-friendly endpoint, e.g. `"127.0.0.1:28001"` (no scheme).
    pub endpoint: String,
    pub store_id: u64,
    pub group_id: u64,
    pub workload: WorkloadKind,
    /// Storage mode label: `mem`, `file`, or `block`.
    pub mode: String,
    /// Number of independent gRPC channels (1..=64). Default 4.
    pub connections: u32,
    /// Number of worker tasks (1..=1000). Default 8.
    pub threads: u32,
    pub duration: Duration,
    pub key_space: u64,
    pub value_size: usize,
    /// Report directory: `bench-runs/<run-folder>/`. The report is
    /// written as `report.json` inside this dir. If `None`, defaults
    /// to `bench-runs/`.
    pub report_dir: Option<std::path::PathBuf>,
    /// Optional run-id; defaults to `bench-<unix_millis>-<workload>`.
    pub run_id: Option<String>,
    /// If `Some(d)`, a tokio task wakes every `d` and emits one human
    /// progress line to stderr (`[+12s] ops=124k qps=10333 err=0`).
    /// The path is lock-free on the worker hot loop — workers only
    /// touch their own `Arc<WorkerCounters>` atomics with `Relaxed`
    /// ordering. `None` (or `Some(Duration::ZERO)`) disables progress.
    pub progress_interval: Option<Duration>,
    /// Optional path for a periodic client-side metrics log file.
    /// When set, a background task flushes per-op-kind latency
    /// percentiles and counters every 5 seconds to this file.
    pub metrics_log_path: Option<std::path::PathBuf>,
    /// Optional warmup window at the start of the run during which
    /// workers issue ops normally but **discard** all records — no
    /// histogram entries, no counter bumps, no contribution to the
    /// final report. This lets cold-start artifacts (TCP slow-start,
    /// channel handshakes, server JIT-equivalents) settle before
    /// measurement begins. `None` or `Duration::ZERO` disables warmup.
    /// The reported `duration_ms` reflects the **measurement** window
    /// (`duration` minus `warmup`); `warmup_ms` is surfaced separately
    /// in the report so operators can see what was discarded.
    pub warmup: Option<Duration>,
}

impl BenchConfig {
    #[must_use]
    pub fn defaults(endpoint: impl Into<String>, workload: WorkloadKind) -> Self {
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
            report_dir: None,
            run_id: None,
            progress_interval: None,
            metrics_log_path: None,
            warmup: None,
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

    // Single `CrowkvClient`, seeded directly at `cfg.endpoint` (bench
    // targets one already-known leader, no `/topology` discovery needed --
    // an empty mgmt-seed list is fine). `pool_size_per_endpoint` reproduces
    // the old runner's "N independent gRPC channels, round-robined" pool
    // via the client's own per-endpoint channel pool, so every worker
    // shares one client and its internal pool rather than owning a
    // channel directly.
    let mut client_config = ClientConfig::new(Vec::new());
    client_config.pool_size_per_endpoint = cfg.connections as usize;
    let client = CrowkvClient::new(client_config);
    client.seed_leader(cfg.store_id, cfg.group_id, cfg.endpoint.clone());
    let client = Arc::new(client);

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
        counters.push(Arc::new(WorkerCounters::default()));
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
    let actual_duration = started_instant.elapsed();
    let total_ops: u64 = by_kind.values().map(|s| s.ops).sum();
    let total_errors: u64 = by_kind.values().map(|s| s.errors).sum();
    #[allow(clippy::cast_precision_loss)]
    let error_rate = if total_ops == 0 {
        0.0
    } else {
        total_errors as f64 / total_ops as f64
    };

    let client_metrics = client.metrics();

    let run_id = cfg.run_id.clone().unwrap_or_else(|| {
        let ms = started_at.timestamp_millis();
        format!("bench-{ms}-{:?}", cfg.workload).to_ascii_lowercase()
    });

    // Measurement duration excludes the warmup window. For a 5 s run
    // with `warmup = 1 s`, `duration_ms` reads as ≈ 4000.
    let measure_ms =
        u64::try_from(actual_duration.saturating_sub(warmup_dur).as_millis()).unwrap_or(u64::MAX);
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
        total_errors,
        error_rate,
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

/// Run a single worker until `deadline`, returning its local per-op
/// stats. Errors during ops are recorded in the histogram (with `ok=false`)
/// rather than aborting the worker.
///
/// Closed loop: at most one in-flight RPC per worker at any instant.
/// Combined across all workers, the runner-wide in-flight count is
/// bounded by `cfg.threads` exactly (see module-level docs).
///
/// `counters` is the worker's own `WorkerCounters` (shared with the
/// optional progress snapshotter). Both increments use `Relaxed` —
/// ordering doesn't matter since the snapshotter only sums and prints,
/// and the final report is computed from the returned `OpStats`.
#[allow(clippy::too_many_arguments)]
async fn run_worker(
    kv: &CrowkvClient,
    gen: &mut OpGen,
    cfg: &BenchConfig,
    measure_start: Instant,
    deadline: Instant,
    worker_id: u32,
    counters: &WorkerCounters,
) -> BTreeMap<OpKind, OpStats> {
    let mut stats: BTreeMap<OpKind, OpStats> = BTreeMap::new();
    let mut iter: u64 = 0;

    loop {
        let now_pre = Instant::now();
        if now_pre >= deadline {
            break;
        }
        // `recording` is sticky-`true` once the warmup window passes.
        // We re-evaluate per iteration to keep the implementation
        // robust to clock skew, but in practice it just flips once.
        let recording = now_pre >= measure_start;
        iter = iter.wrapping_add(1);

        let kind = match cfg.workload {
            WorkloadKind::Read => OpKind::Read,
            WorkloadKind::Write => OpKind::Write,
            WorkloadKind::List => OpKind::List,
            WorkloadKind::Mix => gen.pick_mix_kind(),
        };

        let key = gen.next_key();
        let t0 = Instant::now();
        let (ok, no_leader, not_found) = match kind {
            OpKind::Read => match kv
                .get(cfg.store_id, cfg.group_id, &key, ReadMode::Linearizable, None)
                .await
            {
                Ok(GetOutcome::Found { .. }) => (true, false, false),
                Ok(GetOutcome::NotFound) => (true, false, true),
                Err(ClientError::NotLeader { .. }) => (false, true, false),
                Err(_) => (false, false, false),
            },
            OpKind::Write => {
                let value = gen.make_value();
                let client_id = u64::from(worker_id) + 1;
                match kv
                    .put(cfg.store_id, cfg.group_id, &key, &value, Some((client_id, iter)))
                    .await
                {
                    Ok(WriteOutcome { .. }) => (true, false, false),
                    Err(ClientError::NotLeader { .. }) => (false, true, false),
                    Err(_) => (false, false, false),
                }
            }
            OpKind::Delete => {
                let client_id = u64::from(worker_id) + 1;
                match kv
                    .delete(cfg.store_id, cfg.group_id, &key, Some((client_id, iter)))
                    .await
                {
                    Ok(_) => (true, false, false),
                    Err(ClientError::NotLeader { .. }) => (false, true, false),
                    Err(_) => (false, false, false),
                }
            }
            OpKind::List => match kv
                .scan(cfg.store_id, cfg.group_id, b"", &[], 1, ReadMode::Linearizable)
                .await
            {
                Ok(_) => (true, false, false),
                Err(ClientError::NotLeader { .. }) => (false, true, false),
                Err(_) => (false, false, false),
            },
        };
        // During the warmup window we drive the same RPC sequence so
        // pool channels stay warm and OpGen state advances normally,
        // but we throw the latency / error result away — neither the
        // histogram, the per-kind counters, nor the live atomic
        // counters are touched. This keeps cold-start spikes (TCP
        // slow-start, channel handshake, server first-touch caches)
        // out of the published percentiles.
        if recording {
            let lat_us = u64::try_from(t0.elapsed().as_micros()).unwrap_or(u64::MAX);
            stats
                .entry(kind)
                .or_default()
                .record(lat_us, ok, no_leader, not_found);

            // Live per-op counters: each worker owns its `WorkerCounters`
            // so the increments are uncontended.
            counters.record(kind, ok);
        }

        // Yield periodically so heavy worker counts cooperate.
        if iter % 64 == 0 {
            tokio::task::yield_now().await;
        }
    }

    debug!(worker_id, total = iter, "bench: worker stop");
    stats
}

/// Spawn the optional progress snapshotter. Wakes every `interval`,
/// sums each worker's `Arc<WorkerCounters>` atomics, and emits one
/// human-readable line to stderr. The task self-terminates when wall
/// time crosses `deadline`. The snapshotter never blocks workers; it
/// only reads `Relaxed` atomics.
///
/// Output format (chosen to be greppable and copy-pastable):
///   `[+12s] ops=124000 qps=10333 err=0 nl_hint=0 xport_err=0`
///
/// Where `qps` is the **delta** ops since the previous tick divided by
/// the actual elapsed time between ticks (so it self-corrects if the
/// runtime can't quite hit the requested cadence). `nl_hint` and
/// `xport_err` are cumulative client-side counters for leader-redirect
/// and transport-error events.
fn spawn_progress_snapshotter(
    interval: Duration,
    started: Instant,
    deadline: Instant,
    counters: Vec<Arc<WorkerCounters>>,
    client: Arc<CrowkvClient>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut last_ops: u64 = 0;
        let mut last_tick = started;
        loop {
            tokio::time::sleep(interval).await;
            let now = Instant::now();
            if now >= deadline {
                break;
            }

            let total_ops: u64 = counters.iter().map(|c| c.total_ops()).sum();
            let total_err: u64 = counters.iter().map(|c| c.total_errors()).sum();
            let delta_ops = total_ops.saturating_sub(last_ops);
            let dt = now.duration_since(last_tick).as_secs_f64().max(1e-9);
            #[allow(
                clippy::cast_precision_loss,
                clippy::cast_possible_truncation,
                clippy::cast_sign_loss
            )]
            let qps = (delta_ops as f64 / dt).round() as u64;
            let elapsed_s = now.duration_since(started).as_secs();

            let cm = client.metrics();
            eprintln!(
                "[+{elapsed_s}s] ops={total_ops} qps={qps} err={total_err} nl_hint={} leader_query={} xport_err={}",
                cm.not_leader_hint_followed, cm.leader_query, cm.transport_error_retry,
            );

            last_ops = total_ops;
            last_tick = now;
        }
    })
}

/// One row of cumulative latency output in the periodic metrics log.
struct CumLine {
    name: String,
    count: u64,
    tps: u64,
    avg: u64,
    p50: u64,
    p90: u64,
    p99: u64,
    p999: u64,
    max: u64,
}

/// Cumulative latency histograms maintained by the metrics flusher.
/// Each tick, the drained `WindowLatencySnapshot` from the client is
/// merged into these for run-wide `bench.*.lh` percentiles.
#[derive(Debug)]
struct CumulativeLatency {
    put: Histogram<u64>,
    get: Histogram<u64>,
    delete: Histogram<u64>,
    scan: Histogram<u64>,
    batch_write: Histogram<u64>,
}

impl Default for CumulativeLatency {
    fn default() -> Self {
        let mk = || Histogram::<u64>::new(3).expect("hdr histogram precision");
        Self {
            put: mk(),
            get: mk(),
            delete: mk(),
            scan: mk(),
            batch_write: mk(),
        }
    }
}

impl CumulativeLatency {
    fn merge(&mut self, snap: &WindowLatencySnapshot) {
        let _ = self.put.add(snap.put.clone());
        let _ = self.get.add(snap.get.clone());
        let _ = self.delete.add(snap.delete.clone());
        let _ = self.scan.add(snap.scan.clone());
        let _ = self.batch_write.add(snap.batch_write.clone());
    }
}

/// Spawn a background task that flushes per-op-kind latency percentiles
/// and client counters to a metrics log file every 5 seconds. Format
/// mirrors the server-side `[metrics]` log for consistency.
///
/// Latency histograms (`client.*.lh`) are produced by the crowkv-client
/// library via `flush_latencies`. Cumulative latency (`bench.*.lh`) and
/// per-op gauges (`bench.*.ops.g`, `bench.*.errors.g`) are produced by
/// the bench runner from `WorkerCounters` and accumulated window data.
/// Client error/retry counters (`client.*_errors.c`, etc.) are read
/// from the client metrics snapshot.
#[allow(
    clippy::too_many_arguments,
    clippy::too_many_lines,
    clippy::uninlined_format_args
)]
fn spawn_metrics_flusher(
    interval: Duration,
    started: Instant,
    deadline: Instant,
    counters: Vec<Arc<WorkerCounters>>,
    client: Arc<CrowkvClient>,
    path: std::path::PathBuf,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        use std::fmt::Write;
        let mut last_tick = started;
        let mut prev_cm = client.metrics();
        let mut cum_lat = CumulativeLatency::default();

        // Previous per-op bench counter values for delta calculation.
        let mut prev_put_ok: u64 = 0;
        let mut prev_put_err: u64 = 0;
        let mut prev_get_ok: u64 = 0;
        let mut prev_get_err: u64 = 0;
        let mut prev_del_ok: u64 = 0;
        let mut prev_del_err: u64 = 0;
        let mut prev_scan_ok: u64 = 0;
        let mut prev_scan_err: u64 = 0;

        loop {
            // Align each tick to a wall-clock interval boundary (e.g. :05,
            // :10, :15) so timestamps land on round multiples. dt is
            // computed from actual elapsed time so TPS stays accurate.
            let now_epoch = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default();
            let interval_ns = interval.as_nanos();
            let rem_ns = now_epoch.as_nanos() % interval_ns;
            let delay = interval
                .checked_sub(Duration::from_nanos(rem_ns.try_into().unwrap_or(u64::MAX)))
                .unwrap_or(interval);
            tokio::time::sleep(delay).await;
            let now = Instant::now();
            if now >= deadline {
                break;
            }

            let dt = now.duration_since(last_tick).as_secs_f64().max(1e-9);
            let elapsed = now.duration_since(started).as_secs_f64().max(1e-9);
            let timestamp = Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string();

            // Drain window latency from the client library once.
            // The snapshot is used both for per-window flush and for
            // cumulative accumulation.
            let window_snap = client.drain_window();
            cum_lat.merge(&window_snap);

            let cm = client.metrics();

            // Per-window deltas from cumulative client error counters.
            let delta = |curr: u64, prev: u64| curr.saturating_sub(prev);
            let d_put_err = delta(cm.put_errors, prev_cm.put_errors);
            let d_get_err = delta(cm.get_errors, prev_cm.get_errors);
            let d_del_err = delta(cm.delete_errors, prev_cm.delete_errors);
            let d_scan_err = delta(cm.scan_errors, prev_cm.scan_errors);
            let d_bw_err = delta(cm.batch_write_errors, prev_cm.batch_write_errors);
            let d_nl_hint = delta(cm.not_leader_hint_followed, prev_cm.not_leader_hint_followed);
            let d_leader_q = delta(cm.leader_query, prev_cm.leader_query);
            let d_unknown = delta(cm.unknown_leader_wait, prev_cm.unknown_leader_wait);
            let d_xport = delta(cm.transport_error_retry, prev_cm.transport_error_retry);
            let d_retries = delta(cm.retries_exhausted, prev_cm.retries_exhausted);
            let d_no_leader = delta(cm.no_leader, prev_cm.no_leader);
            let d_topo = delta(cm.topology_refresh, prev_cm.topology_refresh);

            let cm_snap = cm.clone();
            prev_cm = cm;

            // Per-op bench counters (ok/err) from WorkerCounters.
            let put_ok: u64 = counters.iter().map(|c| c.put_ok.load(Ordering::Relaxed)).sum();
            let put_err: u64 = counters.iter().map(|c| c.put_err.load(Ordering::Relaxed)).sum();
            let get_ok: u64 = counters.iter().map(|c| c.get_ok.load(Ordering::Relaxed)).sum();
            let get_err: u64 = counters.iter().map(|c| c.get_err.load(Ordering::Relaxed)).sum();
            let del_ok: u64 = counters.iter().map(|c| c.delete_ok.load(Ordering::Relaxed)).sum();
            let del_err: u64 = counters
                .iter()
                .map(|c| c.delete_err.load(Ordering::Relaxed))
                .sum();
            let scan_ok: u64 = counters.iter().map(|c| c.scan_ok.load(Ordering::Relaxed)).sum();
            let scan_err: u64 = counters.iter().map(|c| c.scan_err.load(Ordering::Relaxed)).sum();

            let d_put_ok = put_ok.saturating_sub(prev_put_ok);
            let d_put_err_b = put_err.saturating_sub(prev_put_err);
            let d_get_ok = get_ok.saturating_sub(prev_get_ok);
            let d_get_err_b = get_err.saturating_sub(prev_get_err);
            let d_del_ok = del_ok.saturating_sub(prev_del_ok);
            let d_del_err_b = del_err.saturating_sub(prev_del_err);
            let d_scan_ok = scan_ok.saturating_sub(prev_scan_ok);
            let d_scan_err_b = scan_err.saturating_sub(prev_scan_err);

            prev_put_ok = put_ok;
            prev_put_err = put_err;
            prev_get_ok = get_ok;
            prev_get_err = get_err;
            prev_del_ok = del_ok;
            prev_del_err = del_err;
            prev_scan_ok = scan_ok;
            prev_scan_err = scan_err;

            // Column widths — match server defaults.
            let width = 24usize;
            let count_w = 5usize;
            let tps_w = 7usize;

            let mut out = String::new();
            let _ = writeln!(out, "[bench-metrics {timestamp} window={dt:.3}s]");

            // 1. Per-window latency histograms from the client library.
            client.flush_latencies(&mut out, &window_snap, dt);

            // 2. Cumulative latency histograms (bench.*.lh) with p90/p999.
            let mut cum_lines: Vec<CumLine> = Vec::new();
            let entries: [(&str, &Histogram<u64>); 5] = [
                ("bench.put.lh", &cum_lat.put),
                ("bench.get.lh", &cum_lat.get),
                ("bench.delete.lh", &cum_lat.delete),
                ("bench.scan.lh", &cum_lat.scan),
                ("bench.batch_write.lh", &cum_lat.batch_write),
            ];
            for (name, h) in &entries {
                if !h.is_empty() {
                    let p = percentiles_from_histogram(h);
                    cum_lines.push(CumLine {
                        name: (*name).to_string(),
                        count: h.len(),
                        tps: tps_calc(h.len(), elapsed),
                        avg: p.avg_us,
                        p50: p.p50_us,
                        p90: p.p90_us,
                        p99: p.p99_us,
                        p999: p.p999_us,
                        max: p.max_us,
                    });
                }
            }
            if !cum_lines.is_empty() {
                let _ = writeln!(
                    out,
                    "{:<width$}  {:>count_w$}  {:>tps_w$}  {:>8}  {:>8}  {:>8}  {:>8}  {:>8}  {:>8}",
                    "",
                    "count",
                    "tps(/s)",
                    "avg(us)",
                    "p50(us)",
                    "p90(us)",
                    "p99(us)",
                    "p999(us)",
                    "max(us)",
                    width = width,
                    count_w = count_w,
                    tps_w = tps_w,
                );
                for c in &cum_lines {
                    let name_w = c.name.len().max(width);
                    let _ = writeln!(
                        out,
                        "{:<name_w$}  {:>count_w$}  {:>tps_w$}  {:>8}  {:>8}  {:>8}  {:>8}  {:>8}  {:>8}",
                        c.name,
                        c.count,
                        c.tps,
                        c.avg,
                        c.p50,
                        c.p90,
                        c.p99,
                        c.p999,
                        c.max,
                        name_w = name_w,
                        count_w = count_w,
                        tps_w = tps_w,
                    );
                }
            }

            // 3. Bench per-op gauges (ok/err per window).
            let gauge_lines: Vec<(&str, u64)> = vec![
                ("bench.put.ops.g", d_put_ok),
                ("bench.put.errors.g", d_put_err_b),
                ("bench.get.ops.g", d_get_ok),
                ("bench.get.errors.g", d_get_err_b),
                ("bench.delete.ops.g", d_del_ok),
                ("bench.delete.errors.g", d_del_err_b),
                ("bench.scan.ops.g", d_scan_ok),
                ("bench.scan.errors.g", d_scan_err_b),
            ];
            let active_gauges: Vec<(&str, u64)> =
                gauge_lines.iter().filter(|(_, v)| *v > 0).copied().collect();
            if !active_gauges.is_empty() {
                let _ = writeln!(out, "{:<width$}  {:>8}", "", "value", width = width);
                for (name, val) in &active_gauges {
                    let name_w = name.len().max(width);
                    let _ = writeln!(out, "{:<name_w$}  {:>8}", name, val, name_w = name_w);
                }
            }

            // 4. Client error counters (from crowkv-client library).
            let err_lines: Vec<(&str, u64, u64)> = vec![
                ("client.put_errors.c", d_put_err, cm_snap.put_errors),
                ("client.get_errors.c", d_get_err, cm_snap.get_errors),
                ("client.delete_errors.c", d_del_err, cm_snap.delete_errors),
                ("client.scan_errors.c", d_scan_err, cm_snap.scan_errors),
                (
                    "client.batch_write_errors.c",
                    d_bw_err,
                    cm_snap.batch_write_errors,
                ),
            ];
            let active_err: Vec<(&str, u64, u64)> =
                err_lines.iter().filter(|(_, d, _)| *d > 0).copied().collect();
            if !active_err.is_empty() {
                let _ = writeln!(
                    out,
                    "{:<width$}  {:>count_w$}  {:>tps_w$}  {:>8}",
                    "",
                    "count",
                    "tps(/s)",
                    "total",
                    width = width,
                    count_w = count_w,
                    tps_w = tps_w,
                );
                for (name, d, total) in &active_err {
                    let name_w = name.len().max(width);
                    let _ = writeln!(
                        out,
                        "{:<name_w$}  {:>count_w$}  {:>tps_w$}  {:>8}",
                        name,
                        d,
                        tps_calc(*d, dt),
                        total,
                        name_w = name_w,
                        count_w = count_w,
                        tps_w = tps_w,
                    );
                }
            }

            // 5. Client retry/leader counters.
            let retry_lines: Vec<(&str, u64, u64)> = vec![
                (
                    "client.not_leader_hint.c",
                    d_nl_hint,
                    cm_snap.not_leader_hint_followed,
                ),
                ("client.leader_query.c", d_leader_q, cm_snap.leader_query),
                (
                    "client.unknown_leader_wait.c",
                    d_unknown,
                    cm_snap.unknown_leader_wait,
                ),
                ("client.transport_error.c", d_xport, cm_snap.transport_error_retry),
                ("client.retries_exhausted.c", d_retries, cm_snap.retries_exhausted),
                ("client.no_leader.c", d_no_leader, cm_snap.no_leader),
                ("client.topology_refresh.c", d_topo, cm_snap.topology_refresh),
            ];
            let active_retry: Vec<(&str, u64, u64)> =
                retry_lines.iter().filter(|(_, d, _)| *d > 0).copied().collect();
            if !active_retry.is_empty() {
                let _ = writeln!(
                    out,
                    "{:<width$}  {:>count_w$}  {:>tps_w$}  {:>8}",
                    "",
                    "count",
                    "tps(/s)",
                    "total",
                    width = width,
                    count_w = count_w,
                    tps_w = tps_w,
                );
                for (name, d, total) in &active_retry {
                    let name_w = name.len().max(width);
                    let _ = writeln!(
                        out,
                        "{:<name_w$}  {:>count_w$}  {:>tps_w$}  {:>8}",
                        name,
                        d,
                        tps_calc(*d, dt),
                        total,
                        name_w = name_w,
                        count_w = count_w,
                        tps_w = tps_w,
                    );
                }
            }

            let _ = writeln!(out);

            if let Ok(mut f) = std::fs::OpenOptions::new().create(true).append(true).open(&path) {
                use std::io::Write;
                let _ = f.write_all(out.as_bytes());
            }

            last_tick = now;
        }
    })
}

#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_precision_loss
)]
fn tps_calc(count: u64, window_secs: f64) -> u64 {
    (count as f64 / window_secs) as u64
}
