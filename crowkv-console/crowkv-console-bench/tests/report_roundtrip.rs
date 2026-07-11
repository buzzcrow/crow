//! HDR / report JSON round-trip. Exercises the percentile extractor
//! and the on-disk report format used by `bench run` / `bench report`.

use std::collections::BTreeMap;

use chrono::Utc;
use crowkv_console_bench::report::{percentiles_from_histogram, OpStats};
use crowkv_console_bench::{BenchReport, OpReport, Percentiles, WorkloadKind};

#[test]
fn percentiles_match_after_recording() {
    let mut s = OpStats::new();
    // 100 evenly spaced samples between 100us and 10_000us.
    for i in 0..100u64 {
        let lat = 100 + i * 100;
        s.record(lat, true, false);
    }
    let p = percentiles_from_histogram(&s.histogram);
    assert!(p.p50_us >= 5_000 && p.p50_us <= 5_500, "p50={}", p.p50_us);
    assert!(p.p99_us >= 9_500, "p99={}", p.p99_us);
    assert!(p.max_us >= 9_900);
}

#[test]
fn report_json_round_trips() {
    let mut by_op = BTreeMap::new();
    by_op.insert(
        "read".to_string(),
        OpReport {
            ops: 1234,
            errors: 5,
            not_found: 7,
            latency_us: Percentiles {
                min_us: 100,
                p50_us: 250,
                p90_us: 800,
                p99_us: 1500,
                p999_us: 4000,
                max_us: 8000,
            },
        },
    );
    let original = BenchReport {
        run_id: "test-roundtrip".into(),
        started_at: Utc::now(),
        finished_at: Utc::now(),
        duration_ms: 1500,
        workload: WorkloadKind::Mix,
        connections: 4,
        threads: 8,
        key_space: 1000,
        value_size: 64,
        target_endpoint: "127.0.0.1:1234".into(),
        store_id: 1,
        group_id: 1,
        total_ops: 1234,
        total_errors: 5,
        error_rate: 5.0 / 1234.0,
        by_op,
    };

    let dir = tempfile::tempdir().unwrap();
    let path = original.write_to(dir.path()).unwrap();
    let reread = BenchReport::read_from(&path).unwrap();
    assert_eq!(original, reread);
    // Spot check the human summary contains the run id.
    assert!(reread.human_summary().contains("test-roundtrip"));
}
