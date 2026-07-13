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
use crowkv_client::{ClientConfig, CrowkvClient, GetOutcome, ReadMode, WriteOutcome};
use crowkv_console_shared::error::{Error, Result};
use tracing::{debug, info, warn};

use super::report::{per_op_map, BenchReport, OpStats};
use super::workload::{OpGen, OpKind, WorkloadKind};

/// Lock-free per-worker counters used by the optional progress
/// snapshotter (G1). Workers bump these on every op with `Relaxed`
/// ordering — there is no contention because each worker owns its
/// `Arc<WorkerCounters>` exclusively. The snapshotter sums across all
/// workers on each tick to compute live throughput. The final report
/// is still computed from each worker's owned `OpStats` map at end of
/// run (which carries the percentile-quality histograms), so the
/// counters are observability-only and never feed the report.
#[derive(Debug, Default)]
struct WorkerCounters {
    ops: AtomicU64,
    errors: AtomicU64,
}

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
    /// If `Some(d)`, a tokio task wakes every `d` and emits one human
    /// progress line to stderr (`[+12s] ops=124k qps=10333 err=0`).
    /// The path is lock-free on the worker hot loop — workers only
    /// touch their own `Arc<WorkerCounters>` atomics with `Relaxed`
    /// ordering. `None` (or `Some(Duration::ZERO)`) disables progress.
    pub progress_interval: Option<Duration>,
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
            connections: 4,
            threads: 8,
            duration: Duration::from_secs(5),
            key_space: 1_000,
            value_size: 64,
            report_dir: None,
            run_id: None,
            progress_interval: None,
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
    //
    // `single_attempt` is mandatory here, not a knob: bench measures raw
    // per-RPC latency/error-rate against one specific endpoint. Any
    // client-side retry/redirect (the default, resilient behavior every
    // other `CrowkvClient` caller wants) would silently convert a real
    // `NotLeaderHint`/timeout into a slower success, corrupting exactly
    // the numbers this tool exists to produce.
    let mut client_config = ClientConfig::new(Vec::new());
    client_config.pool_size_per_endpoint = cfg.connections as usize;
    client_config.retry.single_attempt = true;
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
        )),
        _ => None,
    };

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
        let (ok, not_found) = match kind {
            OpKind::Read => match kv
                .get(cfg.store_id, cfg.group_id, &key, ReadMode::Linearizable, None)
                .await
            {
                Ok(GetOutcome::Found { .. }) => (true, false),
                Ok(GetOutcome::NotFound) => (true, true),
                Err(_) => (false, false),
            },
            OpKind::Write => {
                let value = gen.make_value();
                let client_id = u64::from(worker_id) + 1;
                match kv
                    .put(cfg.store_id, cfg.group_id, &key, &value, Some((client_id, iter)))
                    .await
                {
                    Ok(WriteOutcome { .. }) => (true, false),
                    Err(_) => (false, false),
                }
            }
            OpKind::Delete => {
                let client_id = u64::from(worker_id) + 1;
                match kv
                    .delete(cfg.store_id, cfg.group_id, &key, Some((client_id, iter)))
                    .await
                {
                    Ok(_) => (true, false),
                    Err(_) => (false, false),
                }
            }
            OpKind::List => match kv
                .scan(cfg.store_id, cfg.group_id, b"", 1, ReadMode::Linearizable)
                .await
            {
                Ok(_) => (true, false),
                Err(_) => (false, false),
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
            stats.entry(kind).or_default().record(lat_us, ok, not_found);

            // Live counters: each worker owns its `WorkerCounters` so
            // the increments are uncontended. The progress snapshotter
            // reads them with `load(Relaxed)` once per tick.
            counters.ops.fetch_add(1, Ordering::Relaxed);
            if !ok {
                counters.errors.fetch_add(1, Ordering::Relaxed);
            }
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
///   `[+12s] ops=124000 qps=10333 err=0`
///
/// Where `qps` is the **delta** ops since the previous tick divided by
/// the actual elapsed time between ticks (so it self-corrects if the
/// runtime can't quite hit the requested cadence).
fn spawn_progress_snapshotter(
    interval: Duration,
    started: Instant,
    deadline: Instant,
    counters: Vec<Arc<WorkerCounters>>,
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

            let total_ops: u64 = counters.iter().map(|c| c.ops.load(Ordering::Relaxed)).sum();
            let total_err: u64 = counters.iter().map(|c| c.errors.load(Ordering::Relaxed)).sum();
            let delta_ops = total_ops.saturating_sub(last_ops);
            let dt = now.duration_since(last_tick).as_secs_f64().max(1e-9);
            #[allow(
                clippy::cast_precision_loss,
                clippy::cast_possible_truncation,
                clippy::cast_sign_loss
            )]
            let qps = (delta_ops as f64 / dt).round() as u64;
            let elapsed_s = now.duration_since(started).as_secs();

            // stderr keeps stdout reserved for `--json` payloads and
            // the final report path; CLI tools conventionally print
            // progress on stderr.
            eprintln!("[+{elapsed_s}s] ops={total_ops} qps={qps} err={total_err}");

            last_ops = total_ops;
            last_tick = now;
        }
    })
}
