// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Integration tests for the metrics registry: lifecycle, window reset,
//! snapshot filtering, and flush output format.

use crowdb_kv::common::logging::RotatingLogWriter;
use crowdb_kv::metrics::{MetricsRegistry, MetricsRunner};
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
            && out.contains("total(KB)")
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
    let tmp = crowdb_test_harness::test_dirs::tempdir_in_test_data("metrics");
    let mut runner = MetricsRunner::new(
        crowdb_kv::common::logging::open_metrics_log(tmp.path(), "test", 30, 5).unwrap(),
        1, // 1 second interval
    );
    runner.start();
    // Poll until at least 1 periodic flush block appears in the log
    // (the runner flushes every 1s). Then stop() adds the final flush.
    let metrics_file = std::fs::read_dir(tmp.path())
        .unwrap()
        .flatten()
        .find(|e| e.file_name().to_string_lossy().contains("-metrics-"))
        .map(|e| e.path())
        .expect("metrics log file not found");
    let poll_start = std::time::Instant::now();
    loop {
        let content = std::fs::read_to_string(&metrics_file).unwrap();
        if content.matches("[metrics").count() >= 1 {
            break;
        }
        assert!(
            poll_start.elapsed() < std::time::Duration::from_secs(5),
            "no periodic flush within 5s"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    // Stop performs a final flush
    runner.stop().await;

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
    assert!(out.starts_with("[metrics 2026-07-15T16:30:05.123Z window=5.000s]"));
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
fn header_suppressed_when_all_counters_zero() {
    let mut reg = MetricsRegistry::new();
    let _zero = reg.register_counter("s.1.kv.put.c");
    // No inc() — all counters have count=0
    let mut buf = Vec::new();
    reg.flush(&mut buf, 5.0, "2026-07-15T16:30:05Z");
    let out = String::from_utf8(buf).unwrap();
    // Counter header should NOT appear since no counter has data
    assert!(
        !out.contains("tps(/s)     total"),
        "counter header should be suppressed when all counters are zero:\n{out}"
    );
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

#[tokio::test]
async fn cpp_metrics_block_appears_with_matching_window() {
    let dir = crowdb_test_harness::test_dirs::test_data_dir().join(format!(
        "crowdb_kv_metrics_test_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let writer = RotatingLogWriter::new(dir.clone(), "test", std::process::id(), 1, 1).unwrap();
    let mut runner = MetricsRunner::new(writer, 1);
    {
        let mut reg = runner.registry().lock().unwrap();
        let _ = reg.register_counter("s.1.kv.put.c");
    }
    runner.set_cpp_flush(|w, window, ts, _width, _count_w, _tps_w| {
        let _ = writeln!(w, "[cpp-metrics {ts} window={window:.3}s]");
        let _ = writeln!(
            w,
            "s.0.g.0.snapshot.apply.l  count 1 tps(/s)  avg 100us  max 100us"
        );
        let _ = writeln!(w);
    });
    runner.start();
    // Poll until at least 1 periodic flush (with both [metrics and
    // [cpp-metrics blocks) appears in the log. The runner flushes
    // every 1s; polling avoids a fixed 1200ms sleep.
    let poll_start = std::time::Instant::now();
    loop {
        let mut content = String::new();
        for entry in std::fs::read_dir(&dir).unwrap().flatten() {
            if entry.path().extension().is_some_and(|e| e == "log") {
                content.push_str(&std::fs::read_to_string(entry.path()).unwrap());
            }
        }
        if content.contains("[metrics ") && content.contains("[cpp-metrics ") {
            break;
        }
        assert!(
            poll_start.elapsed() < std::time::Duration::from_secs(5),
            "no flush with both blocks within 5s, last content:\n{content}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    runner.stop().await;

    // Read the log file and check for both blocks.
    let entries = std::fs::read_dir(&dir).unwrap();
    let mut content = String::new();
    for entry in entries {
        let path = entry.unwrap().path();
        if path.extension().is_some_and(|e| e == "log") {
            content.push_str(&std::fs::read_to_string(&path).unwrap());
        }
    }
    std::fs::remove_dir_all(&dir).ok();

    assert!(
        content.contains("[metrics "),
        "missing [metrics] block in:\n{content}"
    );
    assert!(
        content.contains("[cpp-metrics "),
        "missing [cpp-metrics] block in:\n{content}"
    );
    // Both blocks should have window= with 3 decimal places.
    assert!(
        content.contains("window=1."),
        "expected window=1.xxx in:\n{content}"
    );
}

#[test]
fn no_bridged_cpp_names_in_rust_section() {
    let mut reg = MetricsRegistry::new();
    let c = reg.register_counter("s.1.kv.put.c");
    c.inc();
    let g = reg.register_gauge("s.1.g.0.paxos.inflight_slots.g");
    g.set(5);

    let mut buf = Vec::new();
    reg.flush(&mut buf, 5.0, "2026-07-15T16:30:05Z");
    let out = String::from_utf8(buf).unwrap();

    // C++-bridged names should NOT appear in the Rust metrics section.
    assert!(!out.contains("tree.flush_entries.c"));
    assert!(!out.contains("tree.flush.drain.c"));
    assert!(!out.contains("tree.buf.hits.c"));
    assert!(!out.contains("tree.demand.load.l"));
    // Rust-native names should appear.
    assert!(out.contains("s.1.kv.put.c"));
    assert!(out.contains("s.1.g.0.paxos.inflight_slots.g"));
}

#[tokio::test]
async fn wal_fsync_and_write_bw_counts_match() {
    use crowdb_kv::paxos::roles::{PxBallot, SlotIndex};
    use crowdb_kv::wal::{IoBackend, RecordType, WALRecord, WalConfig, WalEngine};

    let dir = crowdb_test_harness::test_dirs::test_data_dir().join(format!(
        "crowdb_kv_wal_metrics_test_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));

    let mut wal_config = WalConfig::with_root(dir.join("wal"));
    wal_config.wal_skip_fsync = false;
    let backend = Arc::new(IoBackend::File);
    let wal = WalEngine::create(backend, wal_config, 1).await.unwrap();

    let registry = Arc::new(Mutex::new(MetricsRegistry::new()));
    {
        let mut r = registry.lock().unwrap();
        let fsync_summary = r.register_summary("s.0.g.1.wal.file.fsync.l");
        let write_bw = r.register_bandwidth("s.0.g.1.wal.file.write.bw");
        wal.set_fsync_metrics(fsync_summary, write_bw);
    }

    // Append a few records to trigger write_batch + fdatasync.
    for i in 1u64..=3 {
        let rec = WALRecord {
            record_type: RecordType::Accepted,
            group_id: 1,
            term: 1,
            slot: i as SlotIndex,
            ballot: PxBallot::new(1, 1),
            payload: bytes::Bytes::from(format!("v{i}")),
        };
        wal.append(&rec).await.unwrap();
    }

    // Flush the metrics and check counts.
    let mut buf = Vec::new();
    {
        let r = registry.lock().unwrap();
        r.flush(&mut buf, 5.0, "2026-07-15T16:30:05Z");
    }
    let out = String::from_utf8(buf).unwrap();

    // wal.file.fsync.l and wal.file.write.bw should both appear with non-zero count.
    assert!(
        out.contains("wal.file.fsync.l"),
        "missing wal.file.fsync.l in:\n{out}"
    );
    assert!(
        out.contains("wal.file.write.bw"),
        "missing wal.file.write.bw in:\n{out}"
    );

    std::fs::remove_dir_all(&dir).ok();
}
