// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Bench metrics wiring — creates the project `MetricsRunner` and
//! registers the bench counters + latency histogram on its registry.
//!
//! The runner writes periodic `[rust-metrics ...]` blocks to a metrics log
//! file (`crowdb-cli-bench-metrics-*.log`) in the CLI's per-invocation log
//! dir. The C++ crowdb-rpc metrics are flushed to a separate file
//! (`crowdb-cli-bench-rpc-metrics-*.log`) via `crowdb_rpc_metrics_start`.
//! After the workload, the final JSON report is appended to the metrics
//! log via `write_raw`.

use std::path::Path;
use std::sync::Arc;

use crowdb_common::logging::open_named_log;
use crowdb_common::metrics::{MetricsRegistry, MetricsRunner};

use super::loader::BenchRecorder;

/// Owns the metrics runner (if enabled) + the recorder built from its
/// registry handles. Drop stops the runner's flush task.
pub struct BenchMetrics {
    pub recorder: Arc<BenchRecorder>,
    runner: Option<MetricsRunner>,
    rpc_metrics_started: bool,
}

impl BenchMetrics {
    /// Create the metrics infrastructure for a bench run.
    ///
    /// `metrics_interval == 0` disables the runner (no metrics log
    /// file); the recorder still works with a standalone registry.
    /// `log_dir` is the per-invocation dir from `cli.log_dir`.
    ///
    /// # Errors
    /// Returns `None` if the metrics log file cannot be opened (logged
    /// via `eprintln!`). The recorder is still usable in that case but
    /// no periodic flush occurs.
    #[must_use]
    pub fn new(log_dir: &Path, metrics_interval: u64) -> Self {
        if metrics_interval == 0 {
            let mut reg = MetricsRegistry::new();
            let recorder = Arc::new(BenchRecorder::from_registry(&mut reg));
            return Self {
                recorder,
                runner: None,
                rpc_metrics_started: false,
            };
        }

        let file = match open_named_log(log_dir, "crowdb-cli-bench-metrics", 50, 5) {
            Ok(f) => f,
            Err(e) => {
                eprintln!("warn: failed to open metrics log: {e}");
                let mut reg = MetricsRegistry::new();
                let recorder = Arc::new(BenchRecorder::from_registry(&mut reg));
                return Self {
                    recorder,
                    runner: None,
                    rpc_metrics_started: false,
                };
            }
        };

        let runner = MetricsRunner::new(file, metrics_interval);
        let reg = runner.registry().clone();
        let recorder = Arc::new(BenchRecorder::from_registry(
            &mut reg.lock().expect("metrics registry poisoned"),
        ));
        Self {
            recorder,
            runner: Some(runner),
            rpc_metrics_started: false,
        }
    }

    /// Start the periodic flush task. No-op if the runner is disabled.
    pub fn start(&mut self) {
        if let Some(ref mut r) = self.runner {
            r.start();
        }
    }

    /// Stop the flush task + final flush. No-op if the runner is
    /// disabled.
    pub async fn stop(&mut self) {
        if let Some(ref mut r) = self.runner {
            r.stop().await;
        }
    }

    /// Start C++ crowdb-rpc metrics flush to a separate log file in
    /// `log_dir`. Writes writev/frame/read counters to
    /// `crowdb-cli-bench-rpc-metrics-*.log`. No-op if `interval == 0`.
    pub fn start_rpc_metrics(&mut self, log_dir: &Path, interval: u64) {
        if interval == 0 || self.rpc_metrics_started {
            return;
        }
        let pid = std::process::id();
        let path = log_dir.join(format!("crowdb-cli-bench-rpc-metrics-{pid}.log"));
        #[allow(clippy::cast_precision_loss)]
        let interval_secs = interval as f64;
        crowdb_rpc_ffi::metrics_start(
            path.to_str().unwrap_or("crowdb-rpc-metrics.log"),
            interval_secs,
            50,
            5,
            false,
        );
        self.rpc_metrics_started = true;
    }

    /// Stop C++ crowdb-rpc metrics flush + final flush.
    pub fn stop_rpc_metrics(&mut self) {
        if self.rpc_metrics_started {
            crowdb_rpc_ffi::metrics_stop();
            self.rpc_metrics_started = false;
        }
    }

    /// Write the final JSON report to the metrics log file. Falls back
    /// to `eprintln!` if the runner is disabled (no metrics log).
    pub fn write_report(&self, json: &serde_json::Value) {
        if let Some(ref r) = self.runner {
            r.write_raw(&format!("bench_report {json}"));
        } else {
            eprintln!("bench_report {json}");
        }
    }
}
