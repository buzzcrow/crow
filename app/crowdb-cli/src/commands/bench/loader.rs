// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Shared loader-loop helper for tokio-based bench workloads.
//!
//! `run_workload` spawns `num_loaders` async tasks, each looping the
//! supplied `op` closure until a deadline, plus one timer task that
//! flips a shared `running` flag. The closure receives the shared
//! [`BenchRecorder`] and records each op's latency / errors itself.
//! The coroutine RPC path does not use this (the C++ `co_spawn` manages
//! its own loop); it records into a `BenchRecorder` directly from the
//! `on_response` callback.
//!
//! `BenchRecorder` wraps the project's `Counter` + `LatencyHistogram` +
//! `Bandwidth` handles from `crowdb_common::metrics`, all registered on
//! the `MetricsRunner`'s registry. The `LatencyHistogram` tracks both
//! window state (flushed/reset each interval for the periodic metrics
//! log) and cumulative state (never reset, for the final JSON report)
//! in a single `observe()` call — no double recording. Its `count` is
//! the success-ops count, so a separate success counter is not needed.

use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crowdb_common::metrics::{Bandwidth, Counter, HistogramSnapshot, LatencyHistogram, MetricsRegistry};

/// Shared error counters + latency histogram + bandwidth + running flag
/// for one bench run. All fields are lock-free atomics — safe to update
/// from many tokio tasks or C++ I/O worker threads concurrently.
pub struct BenchRecorder {
    errors: Arc<Counter>,
    correctness_errors: Arc<Counter>,
    /// Latency histogram (registered → window flushed each interval,
    /// cumulative preserved for final report via `snapshot_total()`).
    /// Its `count` is the success-ops count (every `record_ok` observes).
    latency: Arc<LatencyHistogram>,
    /// Payload bandwidth (bytes/s, registered → flushed each interval).
    bytes: Arc<Bandwidth>,
    running: AtomicBool,
}

impl BenchRecorder {
    /// Build a recorder: all metrics registered on `reg` so the
    /// `MetricsRunner` periodic flush writes them to the metrics log.
    #[must_use]
    pub fn from_registry(reg: &mut MetricsRegistry) -> Self {
        Self {
            errors: reg.register_counter("bench.ops.error.c"),
            correctness_errors: reg.register_counter("bench.ops.correctness_error.c"),
            latency: reg.register_histogram("bench.latency.lh"),
            bytes: reg.register_bandwidth("bench.bytes.bw"),
            running: AtomicBool::new(true),
        }
    }

    /// Record one successful op with its latency (µs) and payload bytes.
    pub fn record_ok(&self, us: u64, bytes: u64) {
        self.latency.observe(us.saturating_mul(1000));
        self.bytes.observe(bytes);
    }

    /// Record one failed op (transport / consensus error).
    pub fn record_err(&self) {
        self.errors.inc();
    }

    /// Record one correctness mismatch (value bytes differ from expected).
    #[allow(dead_code)]
    pub fn record_correctness_err(&self) {
        self.correctness_errors.inc();
    }

    /// Whether loader tasks should keep running.
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }

    /// Stop all loader tasks (set by the timer task at the deadline).
    pub fn stop(&self) {
        self.running.store(false, Ordering::Relaxed);
    }

    /// Total successful ops (cumulative count from the latency histogram).
    #[must_use]
    pub fn ops(&self) -> u64 {
        self.latency.snapshot_total().total_count
    }
    #[must_use]
    pub fn errors(&self) -> u64 {
        self.errors.snapshot().total
    }
    #[must_use]
    #[allow(dead_code)]
    pub fn correctness_errors(&self) -> u64 {
        self.correctness_errors.snapshot().total
    }
    /// Cumulative latency snapshot (avg/p50/p99/max in ns) across all
    /// observations — for the final JSON report. Window state may have
    /// been reset by periodic flushes; cumulative state is preserved.
    #[must_use]
    pub fn hist_snapshot(&self) -> HistogramSnapshot {
        self.latency.snapshot_total()
    }
}

/// The recorder + measured wall-clock duration of a workload run.
pub struct WorkloadRun {
    pub recorder: Arc<BenchRecorder>,
    #[allow(dead_code)]
    pub duration_ms: u64,
}

/// Spawn `num_loaders` tasks each looping `op` until `duration` elapses.
/// `op` performs exactly one operation and records its outcome into the
/// shared recorder. Returns the recorder + the measured run duration.
///
/// `num_loaders` is clamped to >= 1.
pub async fn run_workload<F, Fut>(
    recorder: Arc<BenchRecorder>,
    num_loaders: usize,
    duration: Duration,
    op: F,
) -> WorkloadRun
where
    F: Fn(Arc<BenchRecorder>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let loaders = num_loaders.max(1);
    let start = Instant::now();
    let op = Arc::new(op);

    let mut tasks = Vec::with_capacity(loaders);
    for _ in 0..loaders {
        let rec = Arc::clone(&recorder);
        let op = Arc::clone(&op);
        tasks.push(tokio::spawn(async move {
            while rec.is_running() {
                (*op)(Arc::clone(&rec)).await;
            }
        }));
    }

    // Timer task: flip running=false at the deadline.
    let timer_rec = Arc::clone(&recorder);
    let timer = tokio::spawn(async move {
        tokio::time::sleep(duration).await;
        let ops_at_deadline = timer_rec.ops();
        timer_rec.stop();
        tracing::info!(
            deadline_secs = duration.as_secs(),
            ops_at_deadline,
            "bench: stop signaled — loaders draining in-flight ops"
        );
    });

    for (i, t) in tasks.into_iter().enumerate() {
        let _ = t.await;
        if i == 0 {
            tracing::info!(task = i, elapsed_ms = start.elapsed().as_millis(), "bench: first loader exited");
        }
    }
    let elapsed_ms = start.elapsed().as_millis();
    tracing::info!(
        elapsed_ms,
        ops = recorder.ops(),
        errors = recorder.errors(),
        "bench: all loaders joined"
    );
    let _ = timer.await;

    WorkloadRun {
        recorder,
        duration_ms: elapsed_ms.try_into().unwrap_or(u64::MAX),
    }
}
