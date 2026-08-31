// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Bench metrics wiring — creates the project `MetricsRunner` and
//! registers the bench counters + latency histogram on its registry.
//!
//! The runner writes periodic `[rust-metrics ...]` blocks to a metrics log
//! file in the CLI's per-invocation log dir. After the workload, the
//! final JSON report is appended to the same file via `write_raw`.

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
            };
        }

        let file = match open_named_log(log_dir, "crowdb-cli-bench", 50, 5) {
            Ok(f) => f,
            Err(e) => {
                eprintln!("warn: failed to open metrics log: {e}");
                let mut reg = MetricsRegistry::new();
                let recorder = Arc::new(BenchRecorder::from_registry(&mut reg));
                return Self {
                    recorder,
                    runner: None,
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
}
