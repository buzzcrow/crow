// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

use std::process::ExitCode;

use crate::commands::bench::find_report_path;

/// `bench report` — re-render a previously-saved report.
pub(crate) fn bench_report(run_id: &str, json: bool) -> ExitCode {
    use crate::bench::BenchReport;

    let path = find_report_path(run_id);
    match BenchReport::read_from(&path) {
        Ok(r) => {
            if json {
                crate::utils::print_json(&r)
            } else {
                let md_path = path.with_file_name("report.md");
                let md_text = if md_path.exists() {
                    match std::fs::read_to_string(&md_path) {
                        Ok(text) => text,
                        Err(e) => {
                            eprintln!("error: read markdown report {}: {e}", md_path.display());
                            return ExitCode::from(1);
                        }
                    }
                } else {
                    let node_ids = vec![0u64, 1, 2];
                    let workspace = path
                        .parent()
                        .map_or_else(|| std::path::PathBuf::from("."), |d| d.join("artifacts"));
                    let text = r.markdown_report(&node_ids, &workspace, &std::collections::HashMap::new());
                    let _ = std::fs::write(&md_path, &text);
                    text
                };
                println!("{md_text}");
                ExitCode::SUCCESS
            }
        }
        Err(e) => {
            eprintln!("error: read report {}: {e}", path.display());
            ExitCode::from(1)
        }
    }
}

/// `bench compare` — side-by-side comparison of two saved reports.
pub(crate) fn bench_compare(run_id_1: &str, run_id_2: &str, json: bool) -> ExitCode {
    use crate::bench::BenchReport;

    let path1 = find_report_path(run_id_1);
    let path2 = find_report_path(run_id_2);
    let a = match BenchReport::read_from(&path1) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("error: read report {}: {e}", path1.display());
            return ExitCode::from(1);
        }
    };
    let b = match BenchReport::read_from(&path2) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("error: read report {}: {e}", path2.display());
            return ExitCode::from(1);
        }
    };
    if json {
        return crate::utils::print_json(&serde_json::json!({ "a": a, "b": b }));
    }
    println!("{}", render_comparison(&a, &b));
    ExitCode::SUCCESS
}

/// Render a plain-text side-by-side comparison table: throughput,
/// per-op avg/p50/p99 latency, error rate, WAL metrics, system metrics.
#[allow(
    clippy::cast_precision_loss,
    reason = "display-only QPS, precision loss irrelevant"
)]
fn render_comparison(a: &crate::bench::BenchReport, b: &crate::bench::BenchReport) -> String {
    use crate::bench::report::Percentiles;
    use std::fmt::Write;

    let mut out = String::new();
    let _ = writeln!(out, "{:<24} {:>22} {:>22}", "metric", a.run_id, b.run_id);
    let qps = |r: &crate::bench::BenchReport| {
        let secs = (r.duration_ms as f64) / 1000.0;
        if secs > 0.0 {
            (r.total_ops as f64) / secs
        } else {
            0.0
        }
    };
    let _ = writeln!(
        out,
        "{:<24} {:>22.1} {:>22.1}",
        "throughput(ops/s)",
        qps(a),
        qps(b)
    );
    let _ = writeln!(
        out,
        "{:<24} {:>22} {:>22}",
        "total_ops (success)", a.total_ops, b.total_ops
    );
    let _ = writeln!(
        out,
        "{:<24} {:>22} {:>22}",
        "total_attempts", a.total_attempts, b.total_attempts
    );
    let _ = writeln!(
        out,
        "{:<24} {:>22.4} {:>22.4}",
        "error_rate", a.error_rate, b.error_rate
    );

    let mut op_kinds: std::collections::BTreeSet<&String> = a.by_op.keys().collect();
    op_kinds.extend(b.by_op.keys());
    let empty = Percentiles::empty();
    for kind in op_kinds {
        let pa = a.by_op.get(kind).map_or(&empty, |op| &op.latency_us);
        let pb = b.by_op.get(kind).map_or(&empty, |op| &op.latency_us);
        let _ = writeln!(
            out,
            "{:<24} {:>22} {:>22}",
            format!("{kind} avg/p50/p99(us)"),
            format!("{}/{}/{}", pa.avg_us, pa.p50_us, pa.p99_us),
            format!("{}/{}/{}", pb.avg_us, pb.p50_us, pb.p99_us),
        );
    }

    let (sa, sb) = (&a.server_metrics, &b.server_metrics);
    let _ = writeln!(
        out,
        "{:<24} {:>22} {:>22}",
        "wal_append_count", sa.wal_append_count, sb.wal_append_count
    );
    let _ = writeln!(
        out,
        "{:<24} {:>22} {:>22}",
        "kv_put_count", sa.kv_put_count, sb.kv_put_count
    );
    let _ = writeln!(
        out,
        "{:<24} {:>22} {:>22}",
        "kv_get_count", sa.kv_get_count, sb.kv_get_count
    );
    let _ = writeln!(
        out,
        "{:<24} {:>22} {:>22}",
        "cpu_user_us", sa.system.cpu_user_us, sb.system.cpu_user_us
    );
    let _ = writeln!(
        out,
        "{:<24} {:>22} {:>22}",
        "rss_kb", sa.system.rss_kb, sb.system.rss_kb
    );
    let _ = writeln!(
        out,
        "{:<24} {:>22} {:>22}",
        "tcp_retransmits", sa.system.tcp_retransmits, sb.system.tcp_retransmits
    );
    out
}
