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

use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use super::histogram::BenchHistogram;

/// Shared counters + histogram + running flag for one bench run.
/// All fields are lock-free atomics — safe to update from many tokio
/// tasks or C++ I/O worker threads concurrently.
pub struct BenchRecorder {
    ops: AtomicU64,
    errors: AtomicU64,
    #[allow(dead_code)]
    correctness_errors: AtomicU64,
    hist: BenchHistogram,
    running: AtomicBool,
}

impl BenchRecorder {
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            ops: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            correctness_errors: AtomicU64::new(0),
            hist: BenchHistogram::new(),
            running: AtomicBool::new(true),
        })
    }

    /// Record one successful op with its latency in microseconds.
    pub fn record_ok(&self, us: u64) {
        self.ops.fetch_add(1, Ordering::Relaxed);
        self.hist.record_us(us);
    }

    /// Record one failed op (transport / consensus error).
    pub fn record_err(&self) {
        self.errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Record one correctness mismatch (value bytes differ from expected).
    #[allow(dead_code)]
    pub fn record_correctness_err(&self) {
        self.correctness_errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Whether loader tasks should keep running.
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }

    /// Stop all loader tasks (set by the timer task at the deadline).
    pub fn stop(&self) {
        self.running.store(false, Ordering::Relaxed);
    }

    #[must_use]
    pub fn ops(&self) -> u64 {
        self.ops.load(Ordering::Relaxed)
    }
    #[must_use]
    pub fn errors(&self) -> u64 {
        self.errors.load(Ordering::Relaxed)
    }
    #[must_use]
    #[allow(dead_code)]
    pub fn correctness_errors(&self) -> u64 {
        self.correctness_errors.load(Ordering::Relaxed)
    }
    #[must_use]
    pub fn hist_snapshot(&self) -> super::histogram::BenchHistSnapshot {
        self.hist.snapshot()
    }
}

impl Default for BenchRecorder {
    fn default() -> Self {
        Self {
            ops: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            correctness_errors: AtomicU64::new(0),
            hist: BenchHistogram::new(),
            running: AtomicBool::new(true),
        }
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
pub async fn run_workload<F, Fut>(num_loaders: usize, duration: Duration, op: F) -> WorkloadRun
where
    F: Fn(Arc<BenchRecorder>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let recorder = BenchRecorder::new();
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
        timer_rec.stop();
    });

    for t in tasks {
        let _ = t.await;
    }
    let _ = timer.await;

    WorkloadRun {
        recorder,
        duration_ms: start.elapsed().as_millis().try_into().unwrap_or(u64::MAX),
    }
}
