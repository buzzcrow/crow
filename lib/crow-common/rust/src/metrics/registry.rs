// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

use std::io::Write;
use std::sync::{Arc, Mutex, OnceLock};

use super::bandwidth::Bandwidth;
use super::counter::{Counter, Gauge};
use super::flush::{flush_bandwidths, flush_counters, flush_gauges, flush_histograms, flush_summaries};
use super::histogram::LatencyHistogram;
use super::name::MetricName;
use super::point::MetricPoint;
use super::summary::LatencySummary;

pub(super) struct CounterEntry {
    pub(super) handle: Arc<Counter>,
    pub(super) name: String,
}
pub(super) struct GaugeEntry {
    pub(super) handle: Arc<Gauge>,
    pub(super) name: String,
}
pub(super) struct BandwidthEntry {
    pub(super) handle: Arc<Bandwidth>,
    pub(super) name: String,
}
pub(super) struct HistogramEntry {
    pub(super) handle: Arc<LatencyHistogram>,
    pub(super) name: String,
}
pub(super) struct SummaryEntry {
    pub(super) handle: Arc<LatencySummary>,
    pub(super) name: String,
}

/// Registry owning all metric instances. Metrics are grouped by type
/// for column-aligned flush output.
pub struct MetricsRegistry {
    counters: Vec<CounterEntry>,
    gauges: Vec<GaugeEntry>,
    bandwidths: Vec<BandwidthEntry>,
    histograms: Vec<HistogramEntry>,
    summaries: Vec<SummaryEntry>,
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
    pub(crate) fn flush_with_width<W: Write>(
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
    pub(crate) fn max_name_len(&self) -> usize {
        self.max_name_len
    }

    /// Number of registered counters (for testing).
    #[must_use]
    #[cfg(test)]
    pub(crate) fn counter_count(&self) -> usize {
        self.counters.len()
    }

    /// Number of registered gauges (for testing).
    #[must_use]
    #[cfg(test)]
    pub(crate) fn gauge_count(&self) -> usize {
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

impl Default for MetricsRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ── Global registry ──────────────────────────────────────────────

/// Process-level singleton registry for unprefixed metrics (e.g.
/// `rpc.client.*`, `disk.*`). Use `global_counter("name")` etc. from
/// static variables to register without plumbing a registry through
/// constructors. Per-instance metrics with dynamic prefixes
/// (`s.{store_id}.g.{group_id}.*`) should continue to use a
/// per-process `Arc<Mutex<MetricsRegistry>>` passed explicitly.
pub fn global() -> &'static Mutex<MetricsRegistry> {
    static REGISTRY: OnceLock<Mutex<MetricsRegistry>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(MetricsRegistry::new()))
}

/// Register a counter with the global registry. Idempotent — returns
/// the existing handle if already registered.
///
/// # Panics
/// Panics if the global registry mutex is poisoned.
pub fn global_counter(name: impl Into<MetricName>) -> Arc<Counter> {
    global()
        .lock()
        .expect("global metrics registry poisoned")
        .register_counter(name)
}

/// Register a gauge with the global registry.
///
/// # Panics
/// Panics if the global registry mutex is poisoned.
pub fn global_gauge(name: impl Into<MetricName>) -> Arc<Gauge> {
    global()
        .lock()
        .expect("global metrics registry poisoned")
        .register_gauge(name)
}

/// Register a bandwidth metric with the global registry.
///
/// # Panics
/// Panics if the global registry mutex is poisoned.
pub fn global_bandwidth(name: impl Into<MetricName>) -> Arc<Bandwidth> {
    global()
        .lock()
        .expect("global metrics registry poisoned")
        .register_bandwidth(name)
}

/// Register a latency histogram with the global registry.
///
/// # Panics
/// Panics if the global registry mutex is poisoned.
pub fn global_histogram(name: impl Into<MetricName>) -> Arc<LatencyHistogram> {
    global()
        .lock()
        .expect("global metrics registry poisoned")
        .register_histogram(name)
}

/// Register a latency summary with the global registry.
///
/// # Panics
/// Panics if the global registry mutex is poisoned.
pub fn global_summary(name: impl Into<MetricName>) -> Arc<LatencySummary> {
    global()
        .lock()
        .expect("global metrics registry poisoned")
        .register_summary(name)
}

#[cfg(test)]
mod tests {
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
        assert!(out.contains('2'));
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
        assert!(out.contains("3150"));
        assert!(out.contains("5100"));
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
        assert!(out.ends_with("\n\n"));
    }
}
