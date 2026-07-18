// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Metrics module: lightweight atomic counters, gauges, bandwidth,
//! latency histograms, and latency summaries with periodic flush to a
//! dedicated metrics log file.

pub mod bandwidth;
pub mod counter;
pub mod histogram;
pub mod summary;
pub mod system;

use std::sync::Arc;
use std::time::Instant;

pub use bandwidth::{Bandwidth, BandwidthSnapshot};
pub use counter::{Counter, CounterSnapshot, Gauge};
pub use histogram::{HistogramSnapshot, LatencyHistogram};
pub use summary::{LatencySummary, SummarySnapshot};
pub use system::{flush_system, SystemCollector, SystemSnapshot};

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
    /// format. Resets window state on each metric.
    pub fn flush<W: Write>(&self, writer: &mut W, window_secs: f64, timestamp: &str) {
        let _ = writeln!(writer, "[metrics {timestamp} window={window_secs:.0}s]");
        let width = self.max_name_len;
        flush_counters(writer, &self.counters, window_secs, width);
        flush_histograms(writer, &self.histograms, window_secs, width);
        flush_summaries(writer, &self.summaries, window_secs, width);
        flush_bandwidths(writer, &self.bandwidths, window_secs, width);
        flush_gauges(writer, &self.gauges, width);
        let _ = writeln!(writer);
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
    writer: Arc<Mutex<crate::common::logging::RotatingLogWriter>>,
    task: Option<tokio::task::JoinHandle<()>>,
    interval_secs: f64,
    system_collector: Arc<std::sync::Mutex<SystemCollector>>,
    /// Optional pre-flush collector (e.g. engine stats poller).
    collector: Option<Arc<dyn Fn() + Send + Sync>>,
    /// Timestamp of the last flush, used to compute the real window.
    last_flush: Arc<std::sync::Mutex<Option<Instant>>>,
}

impl MetricsRunner {
    /// Create a new runner with the given metrics log writer.
    /// The registry starts empty; use `registry()` to register metrics.
    #[must_use]
    pub fn new(file: crate::common::logging::RotatingLogWriter, interval_secs: u64) -> Self {
        Self {
            registry: Arc::new(Mutex::new(MetricsRegistry::new())),
            writer: Arc::new(Mutex::new(file)),
            task: None,
            #[allow(clippy::cast_precision_loss)]
            interval_secs: interval_secs as f64,
            system_collector: Arc::new(std::sync::Mutex::new(SystemCollector::new())),
            collector: None,
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

    /// Start the periodic flush task. The first tick fires immediately
    /// and is skipped (no data yet); subsequent ticks flush with the
    /// real elapsed time since the previous flush as the window.
    pub fn start(&mut self) {
        let registry = Arc::clone(&self.registry);
        let writer = Arc::clone(&self.writer);
        let interval = self.interval_secs;
        let sys_collector = Arc::clone(&self.system_collector);
        let collector = self.collector.clone();
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
                    if let Ok(mut w) = writer.lock() {
                        reg.flush(&mut *w, window_secs, &ts);
                        let _ = writeln!(w, "misc");
                        if let Ok(mut sc) = sys_collector.lock() {
                            let snap = sc.collect();
                            flush_system(&mut *w, &snap);
                        }
                        let _ = writeln!(w);
                        let _ = w.flush();
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
            if let Ok(mut w) = self.writer.lock() {
                reg.flush(&mut *w, window_secs, &ts);
                let _ = writeln!(w, "misc");
                if let Ok(mut sc) = self.system_collector.lock() {
                    let snap = sc.collect();
                    flush_system(&mut *w, &snap);
                }
                let _ = writeln!(w);
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

fn flush_counters<W: Write>(writer: &mut W, entries: &[CounterEntry], window_secs: f64, width: usize) {
    if entries.is_empty() {
        return;
    }
    let _ = writeln!(
        writer,
        "{:<width$}  {:>8}  {:>8}  {:>8}",
        "",
        "count",
        "tps(/s)",
        "total",
        width = width
    );
    let mut sorted: Vec<&CounterEntry> = entries.iter().collect();
    sorted.sort_by(|a, b| a.name.cmp(&b.name));
    for e in &sorted {
        let snap = e.handle.flush();
        if snap.count == 0 {
            continue;
        }
        let _ = writeln!(
            writer,
            "{:<width$}  {:>8}  {:>8}  {:>8}",
            e.name,
            snap.count,
            tps(snap.count, window_secs),
            snap.total,
            width = width
        );
    }
}

fn flush_histograms<W: Write>(writer: &mut W, entries: &[HistogramEntry], window_secs: f64, width: usize) {
    if entries.is_empty() {
        return;
    }
    let _ = writeln!(
        writer,
        "{:<width$}  {:>8}  {:>8}  {:>8}  {:>8}  {:>8}  {:>8}",
        "",
        "count",
        "tps(/s)",
        "avg(us)",
        "p50(us)",
        "p99(us)",
        "max(us)",
        width = width
    );
    let mut sorted: Vec<&HistogramEntry> = entries.iter().collect();
    sorted.sort_by(|a, b| a.name.cmp(&b.name));
    for e in &sorted {
        let snap = e.handle.flush();
        if snap.count == 0 {
            continue;
        }
        let _ = writeln!(
            writer,
            "{:<width$}  {:>8}  {:>8}  {:>8}  {:>8}  {:>8}  {:>8}",
            e.name,
            snap.count,
            tps(snap.count, window_secs),
            snap.avg / 1000,
            snap.p50 / 1000,
            snap.p99 / 1000,
            snap.max / 1000,
            width = width
        );
    }
}

fn flush_summaries<W: Write>(writer: &mut W, entries: &[SummaryEntry], window_secs: f64, width: usize) {
    if entries.is_empty() {
        return;
    }
    let _ = writeln!(
        writer,
        "{:<width$}  {:>8}  {:>8}  {:>8}  {:>8}",
        "",
        "count",
        "tps(/s)",
        "avg(us)",
        "max(us)",
        width = width
    );
    let mut sorted: Vec<&SummaryEntry> = entries.iter().collect();
    sorted.sort_by(|a, b| a.name.cmp(&b.name));
    for e in &sorted {
        let snap = e.handle.flush();
        if snap.count == 0 {
            continue;
        }
        let _ = writeln!(
            writer,
            "{:<width$}  {:>8}  {:>8}  {:>8}  {:>8}",
            e.name,
            snap.count,
            tps(snap.count, window_secs),
            snap.avg / 1000,
            snap.max / 1000,
            width = width
        );
    }
}

fn flush_bandwidths<W: Write>(writer: &mut W, entries: &[BandwidthEntry], window_secs: f64, width: usize) {
    if entries.is_empty() {
        return;
    }
    let _ = writeln!(
        writer,
        "{:<width$}  {:>8}  {:>8}  {:>12}  {:>10}",
        "",
        "count",
        "tps(/s)",
        "avg_size(KB)",
        "rate(KB/s)",
        width = width
    );
    let mut sorted: Vec<&BandwidthEntry> = entries.iter().collect();
    sorted.sort_by(|a, b| a.name.cmp(&b.name));
    for e in &sorted {
        let snap = e.handle.flush(window_secs);
        if snap.count == 0 {
            continue;
        }
        #[allow(clippy::cast_precision_loss)]
        let avg_kb = snap.avg_size as f64 / 1024.0;
        let _ = writeln!(
            writer,
            "{:<width$}  {:>8}  {:>8}  {:>12.1}  {:>10}",
            e.name,
            snap.count,
            tps(snap.count, window_secs),
            avg_kb,
            snap.rate / 1024,
            width = width
        );
    }
}

fn flush_gauges<W: Write>(writer: &mut W, entries: &[GaugeEntry], width: usize) {
    if entries.is_empty() {
        return;
    }
    let _ = writeln!(writer, "{:<width$}  {:>8}", "", "value", width = width);
    let mut sorted: Vec<&GaugeEntry> = entries.iter().collect();
    sorted.sort_by(|a, b| a.name.cmp(&b.name));
    for e in &sorted {
        let val = e.handle.snapshot();
        let _ = writeln!(writer, "{:<width$}  {:>8}", e.name, val, width = width);
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

        assert!(out.contains("[metrics 2026-07-15T16:30:05.123Z window=5s]"));
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
        let file = crate::common::logging::open_metrics_log(tmp.path(), "test", 30, 5).unwrap();

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
