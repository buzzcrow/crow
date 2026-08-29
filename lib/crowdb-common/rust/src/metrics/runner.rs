// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::io::Write;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use super::registry::MetricsRegistry;
use super::system::{flush_system, SystemCollector};
use super::timestamp::iso8601_now;

/// Lifecycle manager for the metrics registry. Owns the registry,
/// the metrics log file writer, and the tokio interval task.
///
/// Create with `MetricsRunner::new()`, start periodic flushing with
/// `start()`, and call `stop()` during shutdown for a final flush.
pub struct MetricsRunner {
    registry: Arc<Mutex<MetricsRegistry>>,
    writer: Arc<Mutex<crate::logging::RotatingLogWriter>>,
    task: Option<tokio::task::JoinHandle<()>>,
    interval_secs: f64,
    system_collector: Arc<std::sync::Mutex<SystemCollector>>,
    collector: Option<Arc<dyn Fn() + Send + Sync>>,
    #[allow(clippy::type_complexity)]
    cpp_flush: Option<Arc<dyn Fn(&mut dyn std::io::Write, f64, &str, usize, usize, usize) + Send + Sync>>,
    last_flush: Arc<std::sync::Mutex<Option<Instant>>>,
    #[allow(clippy::type_complexity)]
    cpp_negotiate: Option<Arc<dyn Fn() -> (usize, usize) + Send + Sync>>,
}

impl MetricsRunner {
    /// Create a new runner with the given metrics log writer.
    /// The registry starts empty; use `registry()` to register metrics.
    #[must_use]
    pub fn new(file: crate::logging::RotatingLogWriter, interval_secs: u64) -> Self {
        Self {
            registry: Arc::new(Mutex::new(MetricsRegistry::new())),
            writer: Arc::new(Mutex::new(file)),
            task: None,
            #[allow(clippy::cast_precision_loss)]
            interval_secs: interval_secs as f64,
            system_collector: Arc::new(std::sync::Mutex::new(SystemCollector::new())),
            collector: None,
            cpp_flush: None,
            cpp_negotiate: None,
            last_flush: Arc::new(std::sync::Mutex::new(None)),
        }
    }

    /// Access the registry for metric registration.
    #[must_use]
    pub fn registry(&self) -> &Arc<Mutex<MetricsRegistry>> {
        &self.registry
    }

    /// Set a pre-flush collector callback. Called before each flush
    /// tick so the caller can poll external stats (e.g. C++ engine
    /// counters) and update registered metrics via the registry.
    pub fn set_collector(&mut self, f: impl Fn() + Send + Sync + 'static) {
        self.collector = Some(Arc::new(f));
    }

    /// Set a post-flush C++ metrics callback. Called after the Rust
    /// `[metrics]` + misc section is written. The callback receives
    /// (writer, `window_secs`, timestamp, `shared_width`, `count_w`, `tps_w`) and should write
    /// the `[cpp-metrics]` block(s) to the writer.
    pub fn set_cpp_flush(
        &mut self,
        f: impl Fn(&mut dyn std::io::Write, f64, &str, usize, usize, usize) + Send + Sync + 'static,
    ) {
        self.cpp_flush = Some(Arc::new(f));
    }

    /// Set a negotiate callback to query C++ for its preferred column
    /// widths. Called before each flush. Returns (`count_w`, `tps_w`).
    pub fn set_cpp_negotiate(&mut self, f: impl Fn() -> (usize, usize) + Send + Sync + 'static) {
        self.cpp_negotiate = Some(Arc::new(f));
    }

    /// Start the periodic flush task. The first tick fires immediately
    /// and is skipped (no data yet); subsequent ticks flush with the
    /// real elapsed time since the previous flush as the window.
    pub fn start(&mut self) {
        let registry = Arc::clone(&self.registry);
        let writer = Arc::clone(&self.writer);
        let interval = self.interval_secs;
        let sys_collector = Arc::clone(&self.system_collector);
        let collector = self.collector.clone();
        let cpp_flush = self.cpp_flush.clone();
        let cpp_negotiate = self.cpp_negotiate.clone();
        let last_flush = Arc::clone(&self.last_flush);

        self.task = Some(tokio::spawn(async move {
            let mut ticker = tokio::time::interval(std::time::Duration::from_secs_f64(interval));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            ticker.tick().await;
            *last_flush
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Instant::now());
            loop {
                ticker.tick().await;
                let now = Instant::now();
                let window_secs = last_flush
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .map(|prev| now.duration_since(prev).as_secs_f64())
                    .unwrap_or(interval);
                *last_flush
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(now);

                let ts = iso8601_now();
                if let Some(ref col) = collector {
                    col();
                }
                if let Ok(reg) = registry.lock() {
                    let rust_max = reg.max_name_len();
                    let (cpp_count_w, cpp_tps_w) = cpp_negotiate.as_ref().map_or((5, 7), |neg| neg());
                    let count_w = 5.max(cpp_count_w);
                    let tps_w = 7.max(cpp_tps_w);
                    if let Ok(mut w) = writer.lock() {
                        reg.flush_with_width(&mut *w, window_secs, &ts, rust_max, count_w, tps_w);
                        let _ = writeln!(w, "misc");
                        if let Ok(mut sc) = sys_collector.lock() {
                            let snap = sc.collect();
                            flush_system(&mut *w, &snap);
                        }
                        let _ = writeln!(w);
                        if let Some(ref cpp) = cpp_flush {
                            cpp(&mut *w, window_secs, &ts, rust_max, count_w, tps_w);
                            let _ = w.flush();
                        }
                    }
                }
            }
        }));
    }

    /// Stop the flush task and perform a final flush with the real
    /// elapsed time since the last periodic flush.
    pub async fn stop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
            let _ = task.await;
        }
        if let Some(ref col) = self.collector {
            col();
        }
        let now = Instant::now();
        let window_secs = self
            .last_flush
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .map(|prev| now.duration_since(prev).as_secs_f64())
            .unwrap_or(self.interval_secs);
        let ts = iso8601_now();
        if let Ok(reg) = self.registry.lock() {
            let rust_max = reg.max_name_len();
            let (cpp_count_w, cpp_tps_w) = self.cpp_negotiate.as_ref().map_or((5, 7), |neg| neg());
            let count_w = 5.max(cpp_count_w);
            let tps_w = 7.max(cpp_tps_w);
            if let Ok(mut w) = self.writer.lock() {
                reg.flush_with_width(&mut *w, window_secs, &ts, rust_max, count_w, tps_w);
                let _ = writeln!(w, "misc");
                if let Ok(mut sc) = self.system_collector.lock() {
                    let snap = sc.collect();
                    flush_system(&mut *w, &snap);
                }
                let _ = writeln!(w);
                if let Some(ref cpp) = self.cpp_flush {
                    cpp(&mut *w, window_secs, &ts, rust_max, count_w, tps_w);
                }
                let _ = w.flush();
            }
        }
    }
}

impl Drop for MetricsRunner {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crowdb_test_harness::test_dirs::tempdir_in_test_data;

    #[tokio::test]
    async fn runner_lifecycle_produces_flush_blocks() {
        let tmp = tempdir_in_test_data("metrics-runner");
        let file = crate::logging::open_metrics_log(tmp.path(), "test", 30, 5).unwrap();

        let mut runner = MetricsRunner::new(file, 1);
        {
            let mut reg = runner.registry().lock().unwrap();
            let c = reg.register_counter("s.1.kv.test.c");
            c.inc();
        }
        runner.start();

        tokio::time::sleep(std::time::Duration::from_millis(2500)).await;
        runner.stop().await;

        let metrics_file = std::fs::read_dir(tmp.path())
            .unwrap()
            .flatten()
            .find(|e| e.file_name().to_string_lossy().contains("-metrics-"))
            .map(|e| e.path())
            .expect("metrics log file not found");
        let content = std::fs::read_to_string(&metrics_file).unwrap();
        let block_count = content.matches("[metrics").count();
        assert!(
            block_count >= 2,
            "expected >= 2 flush blocks, got {block_count}: {content}"
        );
        assert!(content.contains("s.1.kv.test.c"));
    }
}
