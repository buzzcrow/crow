// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Bench metrics wiring — creates the project `MetricsRunner` and
//! registers the bench counters + latency histogram on its registry.
//!
//! The runner writes periodic `[rust-metrics ...]` + `[cpp-rpc ...]`
//! blocks to a single metrics log file (`crowdb-cli-metrics-*.log`) in
//! the CLI's per-invocation log dir. The C++ crowdb-rpc process-level
//! counters (e.g. `rpc.client.*`) are flushed into the same file via a
//! `set_cpp_flush` callback that calls
//! `crowdb_rpc_ffi::flush_cpp_global_metrics` — the same pattern
//! `crowdb-kv-server` uses. After the workload, the final JSON report
//! is appended to the metrics log via `write_raw`.

use std::path::Path;
use std::sync::Arc;

use crowdb_common::logging::open_named_log;
use crowdb_common::metrics::{MetricsRegistry, MetricsRunner};

use super::loader::BenchRecorder;

/// Owns the metrics runner (if enabled) + the recorder built from its
/// registry handles. Drop stops the runner's flush task. C++ crowdb-rpc
/// process-level metrics are merged into the same log file via a
/// `set_cpp_flush` callback installed at construction time.
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
    /// When enabled, a `set_cpp_flush` callback is installed so the
    /// C++ crowdb-rpc global registry (`rpc.client.*`) is flushed into
    /// the same `crowdb-cli-metrics-*.log` alongside the Rust metrics.
    /// Must be called before `start()`.
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

        let file = match open_named_log(log_dir, "crowdb-cli-metrics", 50, 5) {
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

        let mut runner = MetricsRunner::new(file, metrics_interval);
        // Merge C++ crowdb-rpc process-level metrics (rpc.client.*) into
        // the same log file. Mirrors crowdb-kv-server's engine_collector.
        runner.set_cpp_flush(|writer, window_secs, timestamp, rust_width, count_w, tps_w| {
            let cpp_max = crowdb_rpc_ffi::cpp_global_metrics_max_name_len();
            let shared_width = rust_width.max(cpp_max);
            if let Some(str) = crowdb_rpc_ffi::flush_cpp_global_metrics(
                window_secs,
                timestamp,
                "cpp-rpc",
                shared_width,
                count_w,
                tps_w,
            ) {
                let _ = std::io::Write::write_all(writer, str.as_bytes());
            }
        });
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

    /// Stop the flush task + final flush (includes the C++ cpp-rpc
    /// block). No-op if the runner is disabled.
    pub async fn stop(&mut self) {
        if let Some(ref mut r) = self.runner {
            r.stop().await;
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
