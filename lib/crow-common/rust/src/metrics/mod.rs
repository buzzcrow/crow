// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Metrics module: lightweight atomic counters, gauges, bandwidth,
//! latency histograms, and latency summaries with periodic flush to a
//! dedicated metrics log file.

pub mod bandwidth;
pub mod counter;
pub mod histogram;
pub mod precise;
pub mod summary;
pub mod system;

use std::sync::Arc;
use std::time::Instant;

pub use bandwidth::{Bandwidth, BandwidthSnapshot};
pub use counter::{Counter, CounterSnapshot, Gauge};
pub use histogram::{HistogramSnapshot, LatencyHistogram};
pub use precise::PreciseHistogram;
pub use summary::{LatencySummary, SummarySnapshot};
pub use system::{flush_system, SystemCollector, SystemMetrics};

/// Metric name: either a static string (process-global metrics) or an
/// owned `Arc<str>` (dynamic-name metrics like per-peer RPC stats).
#[derive(Debug, Clone)]
pub enum MetricName {
    Static(&'static str),
    Owned(Arc<str>),
}

impl MetricName {
    #[must_use]
    pub fn new_static(name: &'static str) -> Self {
        Self::Static(name)
    }

    #[must_use]
    pub fn new_owned(name: impl Into<String>) -> Self {
        Self::Owned(Arc::from(name.into().as_str()))
    }
}

impl std::ops::Deref for MetricName {
    type Target = str;

    fn deref(&self) -> &str {
        match self {
            Self::Static(s) => s,
            Self::Owned(s) => s,
        }
    }
}

impl PartialEq for MetricName {
    fn eq(&self, other: &Self) -> bool {
        **self == **other
    }
}

impl Eq for MetricName {}

impl PartialOrd for MetricName {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for MetricName {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (**self).cmp(&**other)
    }
}

impl AsRef<str> for MetricName {
    fn as_ref(&self) -> &str {
        self
    }
}

// ── Registry ─────────────────────────────────────────────────────

use std::io::Write;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

/// A registered metric entry: the `Arc` handle (returned to the caller)
/// plus a cached copy of the name for sorting during flush.
struct CounterEntry {
    handle: Arc<Counter>,
    name: String,
}
struct GaugeEntry {
    handle: Arc<Gauge>,
    name: String,
}
struct BandwidthEntry {
    handle: Arc<Bandwidth>,
    name: String,
}
struct HistogramEntry {
    handle: Arc<LatencyHistogram>,
    name: String,
}
struct SummaryEntry {
    handle: Arc<LatencySummary>,
    name: String,
}

/// Registry owning all metric instances. Metrics are grouped by type
/// for column-aligned flush output.
pub struct MetricsRegistry {
    counters: Vec<CounterEntry>,
    gauges: Vec<GaugeEntry>,
    bandwidths: Vec<BandwidthEntry>,
    histograms: Vec<HistogramEntry>,
    summaries: Vec<SummaryEntry>,
    /// Max metric name length across all types, for column alignment.
    max_name_len: usize,
}

impl MetricsRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self {
            counters: Vec::new(),
            gauges: Vec::new(),
            bandwidths: Vec::new(),
            histograms: Vec::new(),
            summaries: Vec::new(),
            max_name_len: 0,
        }
    }

    /// Register a counter, returning a handle for hot-path `inc()`/`inc_by()`.
    pub fn register_counter(&mut self, name: impl Into<MetricName>) -> Arc<Counter> {
        let name = name.into();
        let name_str = name.to_string();
        if let Some(existing) = self.counters.iter().find(|e| e.name == name_str) {
            return Arc::clone(&existing.handle);
        }
        self.max_name_len = self.max_name_len.max(name_str.len());
        let c = Arc::new(Counter::new(name));
        self.counters.push(CounterEntry {
            handle: Arc::clone(&c),
            name: name_str,
        });
        c
    }

    /// Register a gauge, returning a handle for hot-path `set()`.
    pub fn register_gauge(&mut self, name: impl Into<MetricName>) -> Arc<Gauge> {
        let name = name.into();
        let name_str = name.to_string();
        if let Some(existing) = self.gauges.iter().find(|e| e.name == name_str) {
            return Arc::clone(&existing.handle);
        }
        self.max_name_len = self.max_name_len.max(name_str.len());
        let g = Arc::new(Gauge::new(name));
        self.gauges.push(GaugeEntry {
            handle: Arc::clone(&g),
            name: name_str,
        });
        g
    }

    /// Register a bandwidth metric, returning a handle for `observe(bytes)`.
    pub fn register_bandwidth(&mut self, name: impl Into<MetricName>) -> Arc<Bandwidth> {
        let name = name.into();
        let name_str = name.to_string();
        if let Some(existing) = self.bandwidths.iter().find(|e| e.name == name_str) {
            return Arc::clone(&existing.handle);
        }
        self.max_name_len = self.max_name_len.max(name_str.len());
        let bw = Arc::new(Bandwidth::new(name));
        self.bandwidths.push(BandwidthEntry {
            handle: Arc::clone(&bw),
            name: name_str,
        });
        bw
    }

    /// Register a latency histogram, returning a handle for `observe(ns)`.
    pub fn register_histogram(&mut self, name: impl Into<MetricName>) -> Arc<LatencyHistogram> {
        let name = name.into();
        let name_str = name.to_string();
        if let Some(existing) = self.histograms.iter().find(|e| e.name == name_str) {
            return Arc::clone(&existing.handle);
        }
        self.max_name_len = self.max_name_len.max(name_str.len());
        let h = Arc::new(LatencyHistogram::new(name));
        self.histograms.push(HistogramEntry {
            handle: Arc::clone(&h),
            name: name_str,
        });
        h
    }

    /// Register a latency summary, returning a handle for `observe(ns)`.
    pub fn register_summary(&mut self, name: impl Into<MetricName>) -> Arc<LatencySummary> {
        let name = name.into();
        let name_str = name.to_string();
        if let Some(existing) = self.summaries.iter().find(|e| e.name == name_str) {
            return Arc::clone(&existing.handle);
        }
        self.max_name_len = self.max_name_len.max(name_str.len());
        let s = Arc::new(LatencySummary::new(name));
        self.summaries.push(SummaryEntry {
            handle: Arc::clone(&s),
            name: name_str,
        });
        s
    }

    /// Flush all metrics to `writer`, formatting per the design doc log
    /// format. Resets window state on each metric. Uses the registry's
    /// own `max_name_len` for column width.
    pub fn flush<W: Write>(&self, writer: &mut W, window_secs: f64, timestamp: &str) {
        self.flush_with_width(writer, window_secs, timestamp, self.max_name_len, 5, 7);
    }

    /// Flush with an explicit column width (for cross-section alignment
    /// with C++ `[cpp-metrics]`).
    pub fn flush_with_width<W: Write>(
        &self,
        writer: &mut W,
        window_secs: f64,
        timestamp: &str,
        width: usize,
        count_w: usize,
        tps_w: usize,
    ) {
        let _ = writeln!(writer, "[metrics {timestamp} window={window_secs:.3}s]");
        flush_counters(writer, &self.counters, window_secs, width, count_w, tps_w);
        flush_histograms(writer, &self.histograms, window_secs, width, count_w, tps_w);
        flush_summaries(writer, &self.summaries, window_secs, width, count_w, tps_w);
        flush_bandwidths(writer, &self.bandwidths, window_secs, width, count_w, tps_w);
        flush_gauges(writer, &self.gauges, width);
        let _ = writeln!(writer);
    }

    /// Current max metric name length across all types.
    #[must_use]
    pub fn max_name_len(&self) -> usize {
        self.max_name_len
    }

    /// Number of registered counters (for testing).
    #[must_use]
    pub fn counter_count(&self) -> usize {
        self.counters.len()
    }

    /// Number of registered gauges (for testing).
    #[must_use]
    pub fn gauge_count(&self) -> usize {
        self.gauges.len()
    }

    /// Return a snapshot of all metric names matching `prefix`, with their
    /// current window values as strings. Intended for FFI / management API
    /// consumption. Does NOT reset window state.
    #[must_use]
    pub fn snapshot(&self, prefix: &str) -> Vec<(String, String)> {
        let mut out = Vec::new();
        for e in &self.counters {
            if e.name.starts_with(prefix) {
                let s = e.handle.snapshot();
                out.push((e.name.clone(), format!("c:{}:{}", s.count, s.total)));
            }
        }
        for e in &self.gauges {
            if e.name.starts_with(prefix) {
                out.push((e.name.clone(), format!("g:{}", e.handle.snapshot())));
            }
        }
        for e in &self.bandwidths {
            if e.name.starts_with(prefix) {
                let s = e.handle.snapshot(1.0);
                out.push((
                    e.name.clone(),
                    format!("bw:{}:{}:{}", s.count, s.avg_size, s.rate),
                ));
            }
        }
        for e in &self.histograms {
            if e.name.starts_with(prefix) {
                let s = e.handle.snapshot();
                out.push((
                    e.name.clone(),
                    format!("h:{}:{}:{}:{}", s.count, s.p50, s.p99, s.total_count),
                ));
            }
        }
        for e in &self.summaries {
            if e.name.starts_with(prefix) {
                let s = e.handle.snapshot();
                out.push((
                    e.name.clone(),
                    format!("l:{}:{}:{}:{}", s.count, s.avg, s.max, s.total_count),
                ));
            }
        }
        out
    }

    /// Typed snapshot of all metric names matching `prefix`, returning
    /// structured `MetricPoint` values for the `/metrics` HTTP endpoint
    /// and GUI consumption. `window_secs` is used to compute approximate
    /// tps/rate for counters and bandwidths (the snapshot path does not
    /// reset window state, so this is the configured flush interval, not
    /// a measured elapsed). Does NOT reset window state.
    #[must_use]
    pub fn snapshot_struct(&self, prefix: &str, window_secs: f64) -> Vec<MetricPoint> {
        let mut out = Vec::new();
        for e in &self.counters {
            if e.name.starts_with(prefix) {
                let s = e.handle.snapshot();
                let tps = if window_secs > 0.0 {
                    #[allow(clippy::cast_precision_loss)]
                    {
                        s.count as f64 / window_secs
                    }
                } else {
                    0.0
                };
                out.push(MetricPoint::Counter {
                    name: e.name.clone(),
                    count: s.count,
                    tps,
                    total: s.total,
                });
            }
        }
        for e in &self.gauges {
            if e.name.starts_with(prefix) {
                out.push(MetricPoint::Gauge {
                    name: e.name.clone(),
                    value: e.handle.snapshot(),
                });
            }
        }
        for e in &self.bandwidths {
            if e.name.starts_with(prefix) {
                let s = e.handle.snapshot(window_secs);
                out.push(MetricPoint::Bandwidth {
                    name: e.name.clone(),
                    count: s.count,
                    avg_size: s.avg_size,
                    rate: s.rate,
                    total_bytes: s.total_bytes,
                });
            }
        }
        for e in &self.histograms {
            if e.name.starts_with(prefix) {
                let s = e.handle.snapshot();
                out.push(MetricPoint::Histogram {
                    name: e.name.clone(),
                    count: s.count,
                    avg_ns: s.avg,
                    p50_ns: s.p50,
                    p99_ns: s.p99,
                    max_ns: s.max,
                    total: s.total_count,
                });
            }
        }
        for e in &self.summaries {
            if e.name.starts_with(prefix) {
                let s = e.handle.snapshot();
                out.push(MetricPoint::Summary {
                    name: e.name.clone(),
                    count: s.count,
                    avg_ns: s.avg,
                    max_ns: s.max,
                    total: s.total_count,
                });
            }
        }
        out
    }
}

/// Typed metric point for the `/metrics` HTTP endpoint. Each variant
/// carries the metric `name` and the type-specific snapshot fields. The
/// UI renders by variant without parsing log-format strings.
#[derive(Debug, Clone, PartialEq)]
pub enum MetricPoint {
    Counter {
        name: String,
        count: u64,
        tps: f64,
        total: u64,
    },
    Gauge {
        name: String,
        value: u64,
    },
    Bandwidth {
        name: String,
        count: u64,
        avg_size: u64,
        rate: u64,
        total_bytes: u64,
    },
    Histogram {
        name: String,
        count: u64,
        avg_ns: u64,
        p50_ns: u64,
        p99_ns: u64,
        max_ns: u64,
        total: u64,
    },
    Summary {
        name: String,
        count: u64,
        avg_ns: u64,
        max_ns: u64,
        total: u64,
    },
}

impl MetricPoint {
    /// The metric name (e.g. `s.1.g.2.kv.put.c`).
    #[must_use]
    pub fn name(&self) -> &str {
        match self {
            Self::Counter { name, .. }
            | Self::Gauge { name, .. }
            | Self::Bandwidth { name, .. }
            | Self::Histogram { name, .. }
            | Self::Summary { name, .. } => name,
        }
    }

    /// Lowercase kind tag matching the metric type suffix
    /// (`counter`/`gauge`/`bandwidth`/`histogram`/`summary`).
    #[must_use]
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Counter { .. } => "counter",
            Self::Gauge { .. } => "gauge",
            Self::Bandwidth { .. } => "bandwidth",
            Self::Histogram { .. } => "histogram",
            Self::Summary { .. } => "summary",
        }
    }
}

impl Default for MetricsRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Format current time as ISO 8601 UTC: `YYYY-MM-DDTHH:MM:SS.mmmZ`.
fn iso8601_now() -> String {
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
    let secs = now.as_secs();
    let ms = now.subsec_millis();
    let days = secs / 86_400;
    let rem = secs % 86_400;
    let hour = rem / 3600;
    let min = (rem % 3600) / 60;
    let sec = rem % 60;
    let z = i64::try_from(days).unwrap_or(i64::MAX) + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = y + i64::from(m <= 2);
    format!("{year}-{m:02}-{d:02}T{hour:02}:{min:02}:{sec:02}.{ms:03}Z")
}

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
    /// Optional pre-flush collector (e.g. engine stats poller).
    collector: Option<Arc<dyn Fn() + Send + Sync>>,
    /// Optional post-flush callback to collect C++ metrics strings.
    /// Called after the Rust `[metrics]` + misc section is written.
    /// Receives (writer, `window_secs`, timestamp, `shared_width`, `count_w`, `tps_w`).
    #[allow(clippy::type_complexity)]
    cpp_flush: Option<Arc<dyn Fn(&mut dyn std::io::Write, f64, &str, usize, usize, usize) + Send + Sync>>,
    /// Timestamp of the last flush, used to compute the real window.
    last_flush: Arc<std::sync::Mutex<Option<Instant>>>,
    /// Optional callback to negotiate column widths with C++.
    /// Returns (`count_w`, `tps_w`) from C++.
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
            // First tick fires immediately — record it as the baseline
            // but don't flush (no data collected yet).
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

fn tps(count: u64, window_secs: f64) -> u64 {
    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        clippy::cast_precision_loss
    )]
    {
        (count as f64 / window_secs) as u64
    }
}

fn flush_counters<W: Write>(
    writer: &mut W,
    entries: &[CounterEntry],
    window_secs: f64,
    width: usize,
    count_w: usize,
    tps_w: usize,
) {
    if entries.is_empty() {
        return;
    }
    let mut sorted: Vec<&CounterEntry> = entries.iter().collect();
    sorted.sort_by(|a, b| a.name.cmp(&b.name));
    let active: Vec<(&CounterEntry, _)> = sorted
        .iter()
        .filter_map(|e| {
            let snap = e.handle.flush();
            (snap.count > 0).then_some((*e, snap))
        })
        .collect();
    if active.is_empty() {
        return;
    }
    let _ = writeln!(
        writer,
        "{:<width$}  {:>count_w$}  {:>tps_w$}  {:>8}",
        "",
        "count",
        "tps(/s)",
        "total",
        width = width,
        count_w = count_w,
        tps_w = tps_w
    );
    for (e, snap) in &active {
        let name_w = e.name.len().max(width);
        let _ = writeln!(
            writer,
            "{:<name_w$}  {:>count_w$}  {:>tps_w$}  {:>8}",
            e.name,
            snap.count,
            tps(snap.count, window_secs),
            snap.total,
            name_w = name_w,
            count_w = count_w,
            tps_w = tps_w
        );
    }
}

fn flush_histograms<W: Write>(
    writer: &mut W,
    entries: &[HistogramEntry],
    window_secs: f64,
    width: usize,
    count_w: usize,
    tps_w: usize,
) {
    if entries.is_empty() {
        return;
    }
    let mut sorted: Vec<&HistogramEntry> = entries.iter().collect();
    sorted.sort_by(|a, b| a.name.cmp(&b.name));
    let active: Vec<(&HistogramEntry, _)> = sorted
        .iter()
        .filter_map(|e| {
            let snap = e.handle.flush();
            (snap.count > 0).then_some((*e, snap))
        })
        .collect();
    if active.is_empty() {
        return;
    }
    let _ = writeln!(
        writer,
        "{:<width$}  {:>count_w$}  {:>tps_w$}  {:>8}  {:>8}  {:>8}  {:>8}",
        "",
        "count",
        "tps(/s)",
        "avg(us)",
        "p50(us)",
        "p99(us)",
        "max(us)",
        width = width,
        count_w = count_w,
        tps_w = tps_w
    );
    for (e, snap) in &active {
        let name_w = e.name.len().max(width);
        let _ = writeln!(
            writer,
            "{:<name_w$}  {:>count_w$}  {:>tps_w$}  {:>8}  {:>8}  {:>8}  {:>8}",
            e.name,
            snap.count,
            tps(snap.count, window_secs),
            snap.avg / 1000,
            snap.p50 / 1000,
            snap.p99 / 1000,
            snap.max / 1000,
            name_w = name_w,
            count_w = count_w,
            tps_w = tps_w
        );
    }
}

fn flush_summaries<W: Write>(
    writer: &mut W,
    entries: &[SummaryEntry],
    window_secs: f64,
    width: usize,
    count_w: usize,
    tps_w: usize,
) {
    if entries.is_empty() {
        return;
    }
    let mut sorted: Vec<&SummaryEntry> = entries.iter().collect();
    sorted.sort_by(|a, b| a.name.cmp(&b.name));
    let active: Vec<(&SummaryEntry, _)> = sorted
        .iter()
        .filter_map(|e| {
            let snap = e.handle.flush();
            (snap.count > 0).then_some((*e, snap))
        })
        .collect();
    if active.is_empty() {
        return;
    }
    let _ = writeln!(
        writer,
        "{:<width$}  {:>count_w$}  {:>tps_w$}  {:>8}  {:>8}",
        "",
        "count",
        "tps(/s)",
        "avg(us)",
        "max(us)",
        width = width,
        count_w = count_w,
        tps_w = tps_w
    );
    for (e, snap) in &active {
        let name_w = e.name.len().max(width);
        let _ = writeln!(
            writer,
            "{:<name_w$}  {:>count_w$}  {:>tps_w$}  {:>8}  {:>8}",
            e.name,
            snap.count,
            tps(snap.count, window_secs),
            snap.avg / 1000,
            snap.max / 1000,
            name_w = name_w,
            count_w = count_w,
            tps_w = tps_w
        );
    }
}

fn flush_bandwidths<W: Write>(
    writer: &mut W,
    entries: &[BandwidthEntry],
    window_secs: f64,
    width: usize,
    count_w: usize,
    tps_w: usize,
) {
    if entries.is_empty() {
        return;
    }
    let mut sorted: Vec<&BandwidthEntry> = entries.iter().collect();
    sorted.sort_by(|a, b| a.name.cmp(&b.name));
    let active: Vec<(&BandwidthEntry, _)> = sorted
        .iter()
        .filter_map(|e| {
            let snap = e.handle.flush(window_secs);
            (snap.count > 0).then_some((*e, snap))
        })
        .collect();
    if active.is_empty() {
        return;
    }
    let _ = writeln!(
        writer,
        "{:<width$}  {:>count_w$}  {:>tps_w$}  {:>12}  {:>10}  {:>9}",
        "",
        "count",
        "tps(/s)",
        "avg_size(KB)",
        "rate(KB/s)",
        "total(KB)",
        width = width,
        count_w = count_w,
        tps_w = tps_w
    );
    for (e, snap) in &active {
        #[allow(clippy::cast_precision_loss)]
        let avg_kb = snap.avg_size as f64 / 1024.0;
        let name_w = e.name.len().max(width);
        let _ = writeln!(
            writer,
            "{:<name_w$}  {:>count_w$}  {:>tps_w$}  {:>12.1}  {:>10}  {:>9}",
            e.name,
            snap.count,
            tps(snap.count, window_secs),
            avg_kb,
            snap.rate / 1024,
            snap.total_bytes / 1024,
            name_w = name_w,
            count_w = count_w,
            tps_w = tps_w
        );
    }
}

fn flush_gauges<W: Write>(writer: &mut W, entries: &[GaugeEntry], width: usize) {
    if entries.is_empty() {
        return;
    }
    let mut sorted: Vec<&GaugeEntry> = entries.iter().collect();
    sorted.sort_by(|a, b| a.name.cmp(&b.name));
    let non_zero: Vec<&GaugeEntry> = sorted
        .iter()
        .filter(|e| e.handle.snapshot() != 0)
        .copied()
        .collect();
    if non_zero.is_empty() {
        return;
    }
    let _ = writeln!(writer, "{:<width$}  {:>8}", "", "value", width = width);
    for e in &non_zero {
        let val = e.handle.snapshot();
        let name_w = e.name.len().max(width);
        let _ = writeln!(writer, "{:<name_w$}  {:>8}", e.name, val, name_w = name_w);
    }
}

impl From<&'static str> for MetricName {
    fn from(s: &'static str) -> Self {
        Self::Static(s)
    }
}

impl From<String> for MetricName {
    fn from(s: String) -> Self {
        Self::Owned(Arc::from(s.as_str()))
    }
}

#[cfg(test)]
mod registry_tests {
    use super::*;

    #[test]
    fn snapshot_struct_returns_typed_points_for_all_kinds() {
        let mut reg = MetricsRegistry::new();
        let c = reg.register_counter("s.1.kv.put.c");
        c.inc_by(10);
        let g = reg.register_gauge("s.1.inflight.g");
        g.set(7);
        let bw = reg.register_bandwidth("s.1.kv.bytes.bw");
        bw.observe(100);
        bw.observe(300);
        let h = reg.register_histogram("s.1.kv.get.lh");
        h.observe(1_000);
        h.observe(2_000);
        let s = reg.register_summary("s.1.kv.scan.l");
        s.observe(5_000);
        s.observe(15_000);

        let pts = reg.snapshot_struct("s.1.", 5.0);
        // All five kinds present, sorted by registration order within each type.
        let counter = pts.iter().find_map(|p| match p {
            MetricPoint::Counter {
                name,
                count,
                tps,
                total,
            } if name == "s.1.kv.put.c" => Some((*count, *tps, *total)),
            _ => None,
        });
        assert_eq!(counter, Some((10, 2.0, 10)), "counter count/tps/total");

        let gauge = pts.iter().find_map(|p| match p {
            MetricPoint::Gauge { name, value } if name == "s.1.inflight.g" => Some(*value),
            _ => None,
        });
        assert_eq!(gauge, Some(7), "gauge value");

        let bandwidth = pts.iter().find_map(|p| match p {
            MetricPoint::Bandwidth {
                name,
                count,
                avg_size,
                rate,
                total_bytes,
            } if name == "s.1.kv.bytes.bw" => Some((*count, *avg_size, *rate, *total_bytes)),
            _ => None,
        });
        assert_eq!(
            bandwidth,
            Some((2, 200, 80, 400)),
            "bandwidth count/avg/rate/total"
        );

        let histogram = pts.iter().find_map(|p| match p {
            MetricPoint::Histogram {
                name,
                count,
                p50_ns,
                p99_ns,
                total,
                ..
            } if name == "s.1.kv.get.lh" => Some((*count, *p50_ns, *p99_ns, *total)),
            _ => None,
        });
        // 2 obs (1µs, 2µs): p50 target=1, p99 target=1 (int div 2*99/100),
        // both hit bucket 0 (bound 1_000).
        assert_eq!(
            histogram,
            Some((2, 1_000, 1_000, 2)),
            "histogram count/p50/p99/total"
        );

        let summary = pts.iter().find_map(|p| match p {
            MetricPoint::Summary {
                name,
                count,
                avg_ns,
                max_ns,
                total,
            } if name == "s.1.kv.scan.l" => Some((*count, *avg_ns, *max_ns, *total)),
            _ => None,
        });
        assert_eq!(
            summary,
            Some((2, 10_000, 15_000, 2)),
            "summary count/avg/max/total"
        );
    }

    #[test]
    fn snapshot_struct_prefix_filter_excludes_non_matching() {
        let mut reg = MetricsRegistry::new();
        let c1 = reg.register_counter("s.1.kv.put.c");
        let c2 = reg.register_counter("s.2.kv.put.c");
        c1.inc();
        c2.inc();
        let pts = reg.snapshot_struct("s.1.", 5.0);
        assert!(pts.iter().all(|p| p.name().starts_with("s.1.")));
        assert_eq!(pts.len(), 1);
    }

    #[test]
    fn snapshot_struct_does_not_reset_window() {
        let mut reg = MetricsRegistry::new();
        let c = reg.register_counter("s.1.kv.put.c");
        c.inc_by(3);
        let _ = reg.snapshot_struct("s.1.", 5.0);
        // Second snapshot still sees the window count (not reset).
        let pts = reg.snapshot_struct("s.1.", 5.0);
        assert_eq!(
            pts.iter().find_map(|p| match p {
                MetricPoint::Counter { count, .. } => Some(*count),
                _ => None,
            }),
            Some(3)
        );
    }

    #[test]
    fn metric_point_kind_and_name() {
        let p = MetricPoint::Counter {
            name: "x.c".into(),
            count: 1,
            tps: 0.2,
            total: 1,
        };
        assert_eq!(p.name(), "x.c");
        assert_eq!(p.kind(), "counter");
        let p = MetricPoint::Gauge {
            name: "x.g".into(),
            value: 5,
        };
        assert_eq!(p.kind(), "gauge");
        let p = MetricPoint::Bandwidth {
            name: "x.bw".into(),
            count: 1,
            avg_size: 1,
            rate: 1,
            total_bytes: 1,
        };
        assert_eq!(p.kind(), "bandwidth");
        let p = MetricPoint::Histogram {
            name: "x.lh".into(),
            count: 1,
            avg_ns: 1,
            p50_ns: 1,
            p99_ns: 1,
            max_ns: 1,
            total: 1,
        };
        assert_eq!(p.kind(), "histogram");
        let p = MetricPoint::Summary {
            name: "x.l".into(),
            count: 1,
            avg_ns: 1,
            max_ns: 1,
            total: 1,
        };
        assert_eq!(p.kind(), "summary");
    }

    #[test]
    fn flush_counter_section_format() {
        let mut reg = MetricsRegistry::new();
        let _ = reg.register_counter("s.1.kv.delete.c");
        let _ = reg.register_counter("s.1.kv.errors.c");

        let c = reg.register_counter("s.1.kv.put.c");
        c.inc();
        c.inc_by(9);

        let mut buf = Vec::new();
        reg.flush(&mut buf, 5.0, "2026-07-15T16:30:05.123Z");
        let out = String::from_utf8(buf).unwrap();

        assert!(out.contains("[metrics 2026-07-15T16:30:05.123Z window=5.000s]"));
        assert!(out.contains("count") && out.contains("tps(/s)") && out.contains("total"));
        assert!(out.contains("s.1.kv.put.c"));
        assert!(out.contains("10"));
        assert!(out.contains('2')); // tps = 10/5
        assert!(!out.contains("s.1.kv.delete.c"));
        assert!(!out.contains("s.1.kv.errors.c"));
        assert!(out.ends_with("\n\n"));
    }

    #[test]
    fn flush_histogram_section_format() {
        let mut reg = MetricsRegistry::new();
        let h = reg.register_histogram("s.1.kv.get.lh");
        for _ in 0..100 {
            h.observe(500_000);
        }

        let mut buf = Vec::new();
        reg.flush(&mut buf, 5.0, "2026-07-15T16:30:05.123Z");
        let out = String::from_utf8(buf).unwrap();

        assert!(
            out.contains("avg(us)")
                && out.contains("p50(us)")
                && out.contains("p99(us)")
                && out.contains("max(us)")
        );
        assert!(out.contains("s.1.kv.get.lh"));
        assert!(out.contains("500"));
    }

    #[test]
    fn flush_gauge_section_always_printed() {
        let mut reg = MetricsRegistry::new();
        let g = reg.register_gauge("s.1.g.0.buf.resident.g");
        g.set(512);

        let mut buf = Vec::new();
        reg.flush(&mut buf, 5.0, "2026-07-15T16:30:05.123Z");
        let out = String::from_utf8(buf).unwrap();

        assert!(out.contains("value"));
        assert!(out.contains("s.1.g.0.buf.resident.g"));
        assert!(out.contains("512"));
    }

    #[test]
    fn flush_bandwidth_section_format() {
        let mut reg = MetricsRegistry::new();
        let bw = reg.register_bandwidth("s.1.kv.bytes_in.bw");
        for _ in 0..10 {
            bw.observe(1024);
        }

        let mut buf = Vec::new();
        reg.flush(&mut buf, 5.0, "2026-07-15T16:30:05.123Z");
        let out = String::from_utf8(buf).unwrap();

        assert!(out.contains("avg_size(KB)") && out.contains("rate(KB/s)"));
        assert!(out.contains("s.1.kv.bytes_in.bw"));
        assert!(out.contains("1.0"));
    }

    #[test]
    fn flush_summary_section_format() {
        let mut reg = MetricsRegistry::new();
        let s = reg.register_summary("s.1.kv.scan.l");
        s.observe(1_200_000);
        s.observe(5_100_000);

        let mut buf = Vec::new();
        reg.flush(&mut buf, 5.0, "2026-07-15T16:30:05.123Z");
        let out = String::from_utf8(buf).unwrap();

        assert!(out.contains("avg(us)") && out.contains("max(us)"));
        assert!(out.contains("s.1.kv.scan.l"));
        assert!(out.contains("3150")); // avg = 3150000 ns = 3150 µs
        assert!(out.contains("5100")); // max = 5100000 ns = 5100 µs
    }

    #[test]
    fn flush_sorted_by_name() {
        let mut reg = MetricsRegistry::new();
        let c3 = reg.register_counter("s.1.kv.z.c");
        let c1 = reg.register_counter("s.1.kv.a.c");
        let c2 = reg.register_counter("s.1.kv.m.c");
        c1.inc();
        c2.inc();
        c3.inc();

        let mut buf = Vec::new();
        reg.flush(&mut buf, 5.0, "2026-07-15T16:30:05.123Z");
        let out = String::from_utf8(buf).unwrap();

        let pos_a = out.find("s.1.kv.a.c").unwrap();
        let pos_m = out.find("s.1.kv.m.c").unwrap();
        let pos_z = out.find("s.1.kv.z.c").unwrap();
        assert!(pos_a < pos_m);
        assert!(pos_m < pos_z);
    }

    #[test]
    fn register_returns_usable_handle() {
        let mut reg = MetricsRegistry::new();
        let c = reg.register_counter("test.c");
        c.inc();
        assert_eq!(reg.counter_count(), 1);

        let g = reg.register_gauge("test.g");
        g.set(42);
        assert_eq!(reg.gauge_count(), 1);
    }

    #[test]
    fn flush_empty_registry_just_header() {
        let reg = MetricsRegistry::new();
        let mut buf = Vec::new();
        reg.flush(&mut buf, 5.0, "2026-07-15T16:30:05.123Z");
        let out = String::from_utf8(buf).unwrap();
        assert!(out.contains("[metrics"));
        // Just header + trailing blank line
        assert!(out.ends_with("\n\n"));
    }

    #[tokio::test]
    async fn runner_lifecycle_produces_flush_blocks() {
        let tmp = tempfile::tempdir().unwrap();
        let file = crate::logging::open_metrics_log(tmp.path(), "test", 30, 5).unwrap();

        let mut runner = MetricsRunner::new(file, 1);
        {
            let mut reg = runner.registry().lock().unwrap();
            let c = reg.register_counter("s.1.kv.test.c");
            c.inc();
        }
        runner.start();

        // Wait 2.5s for at least 2 flush ticks (first tick skipped,
        // flush at 1s, flush at 2s, then final flush on stop)
        tokio::time::sleep(std::time::Duration::from_millis(2500)).await;
        runner.stop().await;

        let metrics_file = std::fs::read_dir(tmp.path())
            .unwrap()
            .flatten()
            .find(|e| e.file_name().to_string_lossy().contains("-metrics-"))
            .map(|e| e.path())
            .expect("metrics log file not found");
        let content = std::fs::read_to_string(&metrics_file).unwrap();
        // Should have at least 2 flush blocks (initial tick + 1s tick + final flush)
        let block_count = content.matches("[metrics").count();
        assert!(
            block_count >= 2,
            "expected >= 2 flush blocks, got {block_count}: {content}"
        );
        // The counter should appear in at least one block
        assert!(content.contains("s.1.kv.test.c"));
    }
}
