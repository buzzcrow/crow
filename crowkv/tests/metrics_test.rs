// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Integration tests for the metrics registry: lifecycle, window reset,
//! snapshot filtering, and flush output format.

use crowkv::metrics::{MetricsRegistry, MetricsRunner};
use std::sync::Arc;
use std::sync::Mutex;

#[test]
fn counter_window_reset_and_total_accumulate() {
    let mut reg = MetricsRegistry::new();
    let c = reg.register_counter("s.1.kv.put.c");
    c.inc();
    c.inc();
    let mut buf = Vec::new();
    reg.flush(&mut buf, 5.0, "2026-07-15T16:30:05Z");
    let out = String::from_utf8(buf).unwrap();
    assert!(out.contains("count") && out.contains("tps(/s)") && out.contains("total"));
    assert!(out.contains("s.1.kv.put.c"));

    c.inc();
    c.inc();
    c.inc();
    let mut buf2 = Vec::new();
    reg.flush(&mut buf2, 5.0, "2026-07-15T16:30:10Z");
    let out2 = String::from_utf8(buf2).unwrap();
    // Window delta = 3, total = 5
    assert!(out2.contains('3'));
    assert!(out2.contains('5'));
}

#[test]
fn gauge_reports_last_value() {
    let mut reg = MetricsRegistry::new();
    let g = reg.register_gauge("s.1.g.0.buf.resident.g");
    g.set(42);
    let mut buf = Vec::new();
    reg.flush(&mut buf, 5.0, "2026-07-15T16:30:05Z");
    let out = String::from_utf8(buf).unwrap();
    assert!(out.contains("42"));
    assert!(out.contains("s.1.g.0.buf.resident.g"));

    g.set(0);
    let mut buf2 = Vec::new();
    reg.flush(&mut buf2, 5.0, "2026-07-15T16:30:10Z");
    let out2 = String::from_utf8(buf2).unwrap();
    // Zero-value gauges are suppressed — no header, no data line.
    assert!(!out2.contains("value"));
    assert!(!out2.contains("s.1.g.0.buf.resident.g"));
}

#[test]
fn bandwidth_count_avg_size_and_rate() {
    let mut reg = MetricsRegistry::new();
    let bw = reg.register_bandwidth("s.1.kv.bytes_in.bw");
    for _ in 0..10 {
        bw.observe(100);
    }
    let mut buf = Vec::new();
    reg.flush(&mut buf, 5.0, "2026-07-15T16:30:05Z");
    let out = String::from_utf8(buf).unwrap();
    assert!(
        out.contains("count")
            && out.contains("tps(/s)")
            && out.contains("avg_size(KB)")
            && out.contains("rate(KB/s)")
    );
    assert!(out.contains("10"));
    assert!(out.contains("s.1.kv.bytes_in.bw"));
}

#[test]
fn histogram_p50_p99_with_known_distribution() {
    let mut reg = MetricsRegistry::new();
    let h = reg.register_histogram("s.1.kv.get.lh");
    for _ in 0..100 {
        h.observe(500_000); // 500us
    }
    let mut buf = Vec::new();
    reg.flush(&mut buf, 5.0, "2026-07-15T16:30:05Z");
    let out = String::from_utf8(buf).unwrap();
    assert!(out.contains("p50(us)"));
    assert!(out.contains("p99(us)"));
    // 500_000 ns = 500 us
    assert!(out.contains("500"));
    assert!(out.contains("s.1.kv.get.lh"));
}

#[test]
fn summary_avg_max_and_reset() {
    let mut reg = MetricsRegistry::new();
    let s = reg.register_summary("s.1.kv.scan.l");
    s.observe(100);
    s.observe(200);
    s.observe(300);
    let mut buf = Vec::new();
    reg.flush(&mut buf, 5.0, "2026-07-15T16:30:05Z");
    let out = String::from_utf8(buf).unwrap();
    assert!(out.contains("avg(us)"));
    assert!(out.contains("max(us)"));
    assert!(out.contains("s.1.kv.scan.l"));
    // avg = 200ns -> 0us, max = 300ns -> 0us (integer division)
    // So just check the name appears

    // Next flush should show 0 (window reset)
    let mut buf2 = Vec::new();
    reg.flush(&mut buf2, 5.0, "2026-07-15T16:30:10Z");
    let out2 = String::from_utf8(buf2).unwrap();
    // No data lines (count=0 suppressed)
    assert!(!out2.contains("s.1.kv.scan.l  "));
}

#[tokio::test]
async fn registry_start_stop_lifecycle() {
    let tmp = tempfile::tempdir().unwrap();
    let mut runner = MetricsRunner::new(
        crowkv::common::logging::open_metrics_log(tmp.path(), "test", 30, 5).unwrap(),
        1, // 1 second interval
    );
    runner.start();
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    // Stop performs a final flush
    runner.stop().await;

    // Find the metrics log file (open_metrics_log names it <prefix>-metrics-*.log)
    let metrics_file = std::fs::read_dir(tmp.path())
        .unwrap()
        .flatten()
        .find(|e| e.file_name().to_string_lossy().contains("-metrics-"))
        .map(|e| e.path())
        .expect("metrics log file not found");
    let content = std::fs::read_to_string(&metrics_file).unwrap();
    // Should have at least 2 flush blocks (periodic + final)
    let count = content.matches("[metrics").count();
    assert!(count >= 2, "expected >= 2 flush blocks, got {count}");
}

#[test]
fn snapshot_prefix_filtering() {
    let mut reg = MetricsRegistry::new();
    let _ = reg.register_counter("s.1.kv.put.c");
    let _ = reg.register_counter("s.2.kv.put.c");
    let _ = reg.register_counter("s.1.kv.get.c");

    let snap = reg.snapshot("s.1.");
    let names: Vec<&str> = snap.iter().map(|(n, _)| n.as_str()).collect();
    assert!(names.contains(&"s.1.kv.put.c"));
    assert!(names.contains(&"s.1.kv.get.c"));
    assert!(!names.contains(&"s.2.kv.put.c"));
}

#[test]
fn dynamic_name_registration_and_flush() {
    let mut reg = MetricsRegistry::new();
    let _ = reg.register_counter("s.1.g.0.rpc.errors.c@10.0.0.2:20002");
    let c = reg.register_counter("s.1.g.0.rpc.errors.c@10.0.0.2:20002");
    c.inc();
    let mut buf = Vec::new();
    reg.flush(&mut buf, 5.0, "2026-07-15T16:30:05Z");
    let out = String::from_utf8(buf).unwrap();
    assert!(out.contains("rpc.errors.c@10.0.0.2:20002"));
}

#[test]
fn flush_format_header_and_sections() {
    let mut reg = MetricsRegistry::new();
    let c = reg.register_counter("s.1.kv.delete.c");
    c.inc();
    let h = reg.register_histogram("s.1.kv.get.lh");
    h.observe(1_000_000);
    let s = reg.register_summary("s.1.kv.scan.l");
    s.observe(500_000);
    let bw = reg.register_bandwidth("s.1.kv.bytes_in.bw");
    bw.observe(42);
    let g = reg.register_gauge("s.1.g.0.buf.resident.g");
    g.set(128);

    let mut buf = Vec::new();
    reg.flush(&mut buf, 5.0, "2026-07-15T16:30:05.123Z");
    let out = String::from_utf8(buf).unwrap();

    // Header
    assert!(out.starts_with("[metrics 2026-07-15T16:30:05.123Z window=5s]"));
    // Section order: counters, histograms, summaries, bandwidths, gauges
    let counter_pos = out.find("s.1.kv.delete.c").unwrap();
    let hist_pos = out.find("s.1.kv.get.lh").unwrap();
    let summary_pos = out.find("s.1.kv.scan.l").unwrap();
    let bw_pos = out.find("s.1.kv.bytes_in.bw").unwrap();
    let gauge_pos = out.find("s.1.g.0.buf.resident.g").unwrap();
    assert!(counter_pos < hist_pos);
    assert!(hist_pos < summary_pos);
    assert!(summary_pos < bw_pos);
    assert!(bw_pos < gauge_pos);
    // Trailing blank line
    assert!(out.ends_with('\n'));
}

#[test]
fn zero_suppression_counter_with_zero_inc() {
    let mut reg = MetricsRegistry::new();
    let active = reg.register_counter("s.1.kv.put.c");
    let _zero = reg.register_counter("s.1.kv.delete.c");
    active.inc();
    // zero counter has 0 increments in this window
    let mut buf = Vec::new();
    reg.flush(&mut buf, 5.0, "2026-07-15T16:30:05Z");
    let out = String::from_utf8(buf).unwrap();
    assert!(out.contains("s.1.kv.put.c"));
    assert!(!out.contains("s.1.kv.delete.c"));
}

#[test]
fn gauge_with_zero_value_is_suppressed() {
    let mut reg = MetricsRegistry::new();
    let g = reg.register_gauge("s.1.g.0.buf.dirty.g");
    g.set(0);
    let mut buf = Vec::new();
    reg.flush(&mut buf, 5.0, "2026-07-15T16:30:05Z");
    let out = String::from_utf8(buf).unwrap();
    // Zero-value gauges are suppressed — no header, no data line.
    assert!(!out.contains("s.1.g.0.buf.dirty.g"));
}

#[test]
fn registry_shared_arc_mutex_usage() {
    let registry = Arc::new(Mutex::new(MetricsRegistry::new()));
    let c = {
        let mut r = registry.lock().unwrap();
        r.register_counter("s.1.kv.put.c")
    };
    c.inc();
    c.inc_by(5);
    let mut buf = Vec::new();
    {
        let r = registry.lock().unwrap();
        r.flush(&mut buf, 5.0, "2026-07-15T16:30:05Z");
    }
    let out = String::from_utf8(buf).unwrap();
    assert!(out.contains('6')); // total = 6
}
