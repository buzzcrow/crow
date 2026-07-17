// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

use clap::Subcommand;
use std::process::ExitCode;

use crate::Cli;

#[derive(Subcommand, Debug)]
pub enum BenchVerb {
    /// Run a workload (`read`, `write`, `list`, or `mix`).
    Run {
        /// Workload kind: `read | write | list | mix`.
        workload: String,
        /// Store id whose `listen_addr` is dialed for gRPC ops.
        #[arg(long, default_value_t = 1)]
        store_id: u64,
        #[arg(long, default_value_t = 1)]
        group_id: u64,
        /// Number of independent gRPC channels (1..=64).
        #[arg(long, default_value_t = 4)]
        connections: u32,
        /// Number of worker tasks (1..=1000).
        #[arg(long, default_value_t = 8)]
        threads: u32,
        /// Test duration in seconds.
        #[arg(long, default_value_t = 5)]
        duration_secs: u64,
        /// Distinct keys per worker key space.
        #[arg(long, default_value_t = 1_000)]
        key_space: u64,
        /// Per-op value size in bytes.
        #[arg(long, default_value_t = 64)]
        value_size: usize,
        /// Optional explicit run id; defaults to a timestamp-based one.
        #[arg(long)]
        run_id: Option<String>,
        /// If non-zero, emit a one-line throughput summary to stderr
        /// every N seconds (`[+12s] ops=124000 qps=10333 err=0`). The
        /// final JSON report is unaffected. Default `0` = disabled.
        #[arg(long, default_value_t = 0)]
        progress_interval_secs: u64,
        /// Discard the first N seconds of records (warmup window).
        /// Channels stay warm, `OpGen` state advances, but no histogram
        /// or counter touches happen until N seconds have elapsed.
        /// Must be strictly less than `--duration-secs`. Default `0`.
        #[arg(long, default_value_t = 0)]
        warmup_secs: u64,
    },
    /// Run a built-in stress scenario (`burst`, `soak`, `hotread`).
    Stress {
        scenario: String,
        #[arg(long, default_value_t = 1)]
        store_id: u64,
        #[arg(long, default_value_t = 1)]
        group_id: u64,
        /// Same as `bench run --progress-interval-secs`. Default `0`.
        #[arg(long, default_value_t = 0)]
        progress_interval_secs: u64,
        /// Same as `bench run --warmup-secs`. Default `0`.
        #[arg(long, default_value_t = 0)]
        warmup_secs: u64,
    },
    /// Re-render a previously-saved report.
    Report { run_id: String },
    /// Full self-contained lifecycle (R10): deploy a 3-node cluster
    /// (1 rack, 3 nodes), drive load, collect server metrics + logs,
    /// print a report, then clean up.
    Benchmark {
        /// Storage mode: `memory` (in-memory KV engine) or
        /// `file-nofsync` (crowtree engine + WAL, fsync skipped to
        /// isolate path-level overhead from disk IO).
        #[arg(long)]
        mode: String,
        /// Test duration in seconds.
        #[arg(long, default_value_t = 60)]
        duration_secs: u64,
        /// Workload kind: `read | write | list | mix`.
        #[arg(long, default_value = "mix")]
        workload: String,
        #[arg(long, default_value_t = 8)]
        threads: u32,
        #[arg(long, default_value_t = 4)]
        connections: u32,
        #[arg(long, default_value_t = 1_000)]
        key_space: u64,
        #[arg(long, default_value_t = 64)]
        value_size: usize,
        /// Retain the deploy workspace (server binaries, WAL/data
        /// dirs, logs) after the run for debugging. Default: removed.
        #[arg(long, default_value_t = false)]
        keep_workspace: bool,
        /// Config-driven (SSH) cluster deployment. Accepted but not
        /// yet implemented — only the local 3-node fixture runs in
        /// this iteration.
        #[arg(long)]
        config: Option<String>,
        /// Optional explicit run id; defaults to a timestamp-based one.
        #[arg(long)]
        run_id: Option<String>,
    },
    /// Print a side-by-side comparison of two previously-saved reports.
    Compare { run_id_1: String, run_id_2: String },
}

pub async fn run_bench_verb(cli: &Cli, verb: BenchVerb) -> ExitCode {
    match verb {
        BenchVerb::Run {
            workload,
            store_id,
            group_id,
            connections,
            threads,
            duration_secs,
            key_space,
            value_size,
            run_id,
            progress_interval_secs,
            warmup_secs,
        } => {
            bench_run(
                cli,
                BenchRunArgs {
                    workload,
                    store_id,
                    group_id,
                    connections,
                    threads,
                    duration_secs,
                    key_space,
                    value_size,
                    run_id,
                    progress_interval_secs,
                    warmup_secs,
                },
                cli.json,
            )
            .await
        }
        BenchVerb::Stress {
            scenario,
            store_id,
            group_id,
            progress_interval_secs,
            warmup_secs,
        } => {
            bench_stress(
                cli,
                scenario,
                store_id,
                group_id,
                progress_interval_secs,
                warmup_secs,
                cli.json,
            )
            .await
        }
        BenchVerb::Report { run_id } => bench_report(&run_id, cli.json),
        BenchVerb::Benchmark {
            mode,
            duration_secs,
            workload,
            threads,
            connections,
            key_space,
            value_size,
            keep_workspace,
            config,
            run_id,
        } => {
            bench_benchmark(
                BenchBenchmarkArgs {
                    mode,
                    duration_secs,
                    workload,
                    threads,
                    connections,
                    key_space,
                    value_size,
                    keep_workspace,
                    config,
                    run_id,
                },
                cli.json,
            )
            .await
        }
        BenchVerb::Compare { run_id_1, run_id_2 } => bench_compare(&run_id_1, &run_id_2, cli.json),
    }
}

struct BenchRunArgs {
    workload: String,
    store_id: u64,
    group_id: u64,
    connections: u32,
    threads: u32,
    duration_secs: u64,
    key_space: u64,
    value_size: usize,
    run_id: Option<String>,
    progress_interval_secs: u64,
    warmup_secs: u64,
}

async fn bench_run(cli: &Cli, args: BenchRunArgs, json: bool) -> ExitCode {
    use crate::bench::{run_bench, BenchConfig, WorkloadKind};
    use std::time::Duration;

    let kind = match WorkloadKind::parse(&args.workload) {
        Ok(k) => k,
        Err(bad) => {
            eprintln!("error: unknown workload {bad:?} (expected: read|write|list|mix)");
            return ExitCode::from(1);
        }
    };
    let endpoint = match resolve_bench_endpoint(cli, args.store_id, args.group_id).await {
        Ok(e) => e,
        Err(c) => return c,
    };
    let mut cfg = BenchConfig::defaults(endpoint, kind);
    cfg.store_id = args.store_id;
    cfg.group_id = args.group_id;
    cfg.connections = args.connections;
    cfg.threads = args.threads;
    cfg.duration = Duration::from_secs(args.duration_secs);
    cfg.key_space = args.key_space;
    cfg.value_size = args.value_size;
    cfg.run_id = args.run_id;
    cfg.progress_interval =
        (args.progress_interval_secs > 0).then(|| Duration::from_secs(args.progress_interval_secs));
    cfg.warmup = (args.warmup_secs > 0).then(|| Duration::from_secs(args.warmup_secs));
    match run_bench(cfg).await {
        Ok((report, path)) => {
            if json {
                return crate::utils::print_json(&report);
            }
            println!("{}", report.human_summary());
            println!("\nreport: {}", path.display());
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("error: bench run: {e}");
            ExitCode::from(2)
        }
    }
}

async fn bench_stress(
    cli: &Cli,
    scenario: String,
    store_id: u64,
    group_id: u64,
    progress_interval_secs: u64,
    warmup_secs: u64,
    json: bool,
) -> ExitCode {
    use crate::bench::{resolve_stress_scenario, run_bench, stress_scenario_names};
    use std::time::Duration;

    let endpoint = match resolve_bench_endpoint(cli, store_id, group_id).await {
        Ok(e) => e,
        Err(c) => return c,
    };
    // Layer in any [bench.stress.<name>] overrides from console.toml.
    // Missing or unreadable config silently falls back to built-ins
    // only — operators don't need a config file to use stress.
    let overrides = crate::utils::config::load_config(cli)
        .map(|c| c.bench.stress)
        .unwrap_or_default();
    let Ok(mut cfg) = resolve_stress_scenario(&scenario, endpoint, &overrides) else {
        eprintln!(
            "error: unknown scenario '{scenario}'. Available: {}",
            stress_scenario_names().join(", ")
        );
        return ExitCode::from(1);
    };
    cfg.store_id = store_id;
    cfg.group_id = group_id;
    cfg.progress_interval = (progress_interval_secs > 0).then(|| Duration::from_secs(progress_interval_secs));
    cfg.warmup = (warmup_secs > 0).then(|| Duration::from_secs(warmup_secs));
    match run_bench(cfg).await {
        Ok((report, path)) => {
            if json {
                return crate::utils::print_json(&report);
            }
            println!("{}", report.human_summary());
            println!("\nreport: {}", path.display());
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("error: bench stress: {e}");
            ExitCode::from(2)
        }
    }
}

/// Resolve the leader's gRPC endpoint for `(store, group)` through the
/// console. Bench dials gRPC directly for throughput, but the target is
/// looked up via `crowkv-web` so no `crowkv-server` registry is needed.
async fn resolve_bench_endpoint(cli: &Cli, store_id: u64, group_id: u64) -> Result<String, ExitCode> {
    let client = crate::utils::client::console_client(cli)?;
    client
        .resolve_endpoint(store_id, group_id)
        .await
        .map(|info| info.grpc_url)
        .map_err(|e| {
            eprintln!("error: resolve endpoint for store {store_id} group {group_id}: {e}");
            ExitCode::from(2)
        })
}

struct BenchBenchmarkArgs {
    mode: String,
    duration_secs: u64,
    workload: String,
    threads: u32,
    connections: u32,
    key_space: u64,
    value_size: usize,
    keep_workspace: bool,
    config: Option<String>,
    run_id: Option<String>,
}

/// `bench benchmark` — full self-contained lifecycle: deploy (fixture),
/// run, collect, cleanup, report (R10 benchmark framework).
async fn bench_benchmark(args: BenchBenchmarkArgs, json: bool) -> ExitCode {
    use crate::bench::provision::{GROUP_ID, STORE_ID};
    use crate::bench::{run_bench, BenchConfig, BenchFixture, BenchMode, WorkloadKind};
    use std::time::Duration;

    let Some(mode) = BenchMode::parse(&args.mode) else {
        eprintln!(
            "error: unknown mode {:?} (expected: memory|file-nofsync)",
            args.mode
        );
        return ExitCode::from(1);
    };
    let kind = match WorkloadKind::parse(&args.workload) {
        Ok(k) => k,
        Err(bad) => {
            eprintln!("error: unknown workload {bad:?} (expected: read|write|list|mix)");
            return ExitCode::from(1);
        }
    };
    if args.config.is_some() {
        eprintln!("note: --config is accepted but not yet implemented; using the local 3-node fixture");
    }

    let run_id = args
        .run_id
        .clone()
        .unwrap_or_else(|| format!("bench-{}-{}", chrono::Utc::now().timestamp_millis(), mode.label()));
    let workspace_dir = std::path::PathBuf::from("bench-runs")
        .join("workspaces")
        .join(&run_id);

    println!("provisioning 3-node cluster ({} mode)...", mode.label());
    let mut fixture = match BenchFixture::new(mode, workspace_dir).await {
        Ok(f) => f,
        Err(e) => {
            eprintln!("error: provision cluster: {e}");
            return ExitCode::from(2);
        }
    };

    let mut cfg = BenchConfig::defaults(fixture.leader_endpoint().to_string(), kind);
    cfg.store_id = STORE_ID;
    cfg.group_id = GROUP_ID;
    cfg.connections = args.connections;
    cfg.threads = args.threads;
    cfg.duration = Duration::from_secs(args.duration_secs);
    cfg.key_space = args.key_space;
    cfg.value_size = args.value_size;
    cfg.run_id = Some(run_id.clone());

    println!(
        "running {} workload for {}s...",
        args.workload, args.duration_secs
    );
    let (mut report, path) = match run_bench(cfg).await {
        Ok(v) => v,
        Err(e) => {
            eprintln!("error: bench run: {e}");
            fixture.cleanup(args.keep_workspace).await;
            return ExitCode::from(2);
        }
    };

    report.server_metrics = fixture.collect_metrics();
    let log_warning_count = match path.parent() {
        Some(report_dir) => {
            let bundle_dir = report_dir.join(format!("{run_id}-artifacts"));
            if let Err(e) = fixture.collect_logs(&bundle_dir) {
                eprintln!("warning: failed to collect node logs: {e}");
            }
            if let Err(e) = report.write_to(report_dir) {
                eprintln!("warning: failed to re-write report with server metrics: {e}");
            }
            count_log_warnings(&bundle_dir)
        }
        None => 0,
    };

    fixture.cleanup(args.keep_workspace).await;

    if json {
        return crate::utils::print_json(&report);
    }
    println!("{}", report.human_summary());
    println!("\nreport: {}", path.display());
    print_anomalies(&report, log_warning_count);
    ExitCode::SUCCESS
}

/// Count `WARN`/`ERROR` lines across every collected node stdout/stderr
/// log (skips `metrics-*.log`, which is structured data, not runtime
/// logging).
fn count_log_warnings(bundle_dir: &std::path::Path) -> usize {
    let Ok(node_dirs) = std::fs::read_dir(bundle_dir) else {
        return 0;
    };
    let mut count = 0;
    for node_dir in node_dirs.flatten() {
        let Ok(files) = std::fs::read_dir(node_dir.path()) else {
            continue;
        };
        for file in files.flatten() {
            let name = file.file_name().to_string_lossy().to_string();
            if name.starts_with("metrics-")
                || !std::path::Path::new(&name)
                    .extension()
                    .is_some_and(|ext| ext.eq_ignore_ascii_case("log"))
            {
                continue;
            }
            if let Ok(content) = std::fs::read_to_string(file.path()) {
                count += content
                    .lines()
                    .filter(|l| l.contains("WARN") || l.contains("ERROR"))
                    .count();
            }
        }
    }
    count
}

/// Print flagged anomalies: non-zero error rate, TCP retransmits/lost
/// segments, and server-log warnings/errors.
fn print_anomalies(report: &crate::bench::BenchReport, log_warning_count: usize) {
    let mut anomalies = Vec::new();
    if report.error_rate > 0.0 {
        anomalies.push(format!("non-zero client error rate: {:.4}", report.error_rate));
    }
    let sys = &report.server_metrics.system;
    if sys.tcp_retransmits > 0 {
        anomalies.push(format!("TCP retransmits: {}", sys.tcp_retransmits));
    }
    if sys.tcp_lost > 0 {
        anomalies.push(format!("TCP lost segments: {}", sys.tcp_lost));
    }
    if log_warning_count > 0 {
        anomalies.push(format!("server log WARN/ERROR lines: {log_warning_count}"));
    }
    if anomalies.is_empty() {
        println!("\nanomalies: none");
    } else {
        println!("\nanomalies:");
        for a in anomalies {
            println!("  - {a}");
        }
    }
}

/// `bench compare` — side-by-side comparison of two saved reports.
fn bench_compare(run_id_1: &str, run_id_2: &str, json: bool) -> ExitCode {
    use crate::bench::BenchReport;

    let dir = BenchReport::default_dir();
    let path1 = dir.join(format!("{run_id_1}.md"));
    let path2 = dir.join(format!("{run_id_2}.md"));
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
    let _ = writeln!(out, "{:<24} {:>22} {:>22}", "total_ops", a.total_ops, b.total_ops);
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

fn bench_report(run_id: &str, json: bool) -> ExitCode {
    use crate::bench::BenchReport;

    let dir = BenchReport::default_dir();
    let path = dir.join(format!("{run_id}.md"));
    match BenchReport::read_from(&path) {
        Ok(r) => {
            if json {
                return crate::utils::print_json(&r);
            }
            println!("{}", r.human_summary());
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("error: read report {}: {e}", path.display());
            ExitCode::from(1)
        }
    }
}
