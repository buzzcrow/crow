// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

use clap::{Args, Subcommand};
use std::process::ExitCode;

use crate::Cli;

/// Subcommands for `crow-cli bench`.
#[derive(Subcommand, Debug)]
pub enum BenchSub {
    /// Run a benchmark (deploy 3-node cluster, drive load, collect
    /// metrics, report, cleanup).
    Run(Box<RunArgs>),
    /// Re-render a previously-saved report.
    Report {
        /// Run ID of the report to re-render.
        run_id: String,
    },
    /// Print a side-by-side comparison of two previously-saved reports.
    Compare { run_id_1: String, run_id_2: String },
}

/// Arguments for `crow-cli bench run`.
#[derive(Args, Debug)]
pub struct RunArgs {
    /// Storage mode: `mem` (crow-tree + mem-block), `file` (crow-tree +
    /// file page store), or `block` (crow-tree + block page store).
    #[arg(long, default_value = "mem")]
    pub mode: String,

    /// Test duration in seconds.
    #[arg(long, default_value_t = 20)]
    pub duration_secs: u64,

    /// Workload kind: `read | write | list | mix`.
    #[arg(long, default_value = "mix")]
    pub workload: String,

    #[arg(long, default_value_t = 8)]
    pub threads: u32,

    #[arg(long, default_value_t = 4)]
    pub connections: u32,

    #[arg(long, default_value_t = 1_000_000)]
    pub key_space: u64,

    #[arg(long, default_value_t = 512)]
    pub value_size: usize,

    /// Deprecated: workspace is now always kept in the run directory.
    #[arg(long, default_value_t = false)]
    pub keep_workspace: bool,

    /// Config-driven (SSH) cluster deployment. Accepted but not yet
    /// implemented — only the local 3-node fixture runs in this
    /// iteration.
    #[arg(long)]
    pub config: Option<String>,

    /// Optional explicit run id; defaults to an auto-incremented
    /// sequence number.
    #[arg(long)]
    pub run_id: Option<String>,

    /// Maximum in-flight proposals per group (--max-inflight on each
    /// spawned server). Default: 32.
    #[arg(long, default_value_t = 32)]
    pub max_inflight: usize,

    /// Server metrics log flush interval in seconds (--metrics-interval
    /// on each spawned server). Default: 5. Set to 1 for short bench runs.
    #[arg(long, default_value_t = 5)]
    pub metrics_interval: u64,

    /// Read mode for read ops: `linearizable` (default) or `minslot`.
    /// Ignored for write/delete ops.
    #[arg(long, default_value = "linearizable")]
    pub read_mode: String,

    /// `min_slot` policy for `MinSlot` reads: `auto` (carry client
    /// write watermark, default), `zero` (always 0, max staleness), or
    /// a fixed slot number. Ignored for `Linearizable` reads.
    #[arg(long, default_value = "auto")]
    pub min_slot: String,

    /// Pre-population count: write `<count>` keys with deterministic
    /// values before measurement begins. Defaults to 200,000 for read
    /// workloads (so reads return `Found`), 0 for write/mix/list (no
    /// pre-pop needed). Set to 0 to disable. Not measured (reported
    /// separately as `pre_pop_ms`).
    #[arg(long)]
    pub pre_populate: Option<u64>,

    /// Number of random bytes to spot-check per `Found` read against
    /// the deterministic value formula. Default 8. Set to 0 to disable
    /// verification.
    #[arg(long, default_value_t = 8)]
    pub verify_bytes: usize,

    /// `MinSlot` read-endpoint selection policy: `leader` (default,
    /// routes `MinSlot` reads to the leader) or `any-replica`
    /// (distributes `MinSlot` reads across all replicas, exercising the
    /// real follower local-serve + fallback path). Ignored for
    /// `Linearizable` reads. When not specified, defaults to
    /// `any-replica` for `--read-mode minslot` and `leader` for
    /// `--read-mode linearizable`.
    #[arg(long)]
    pub read_endpoint_policy: Option<String>,

    /// Optional `CrowKVConfig` JSON file passed as `--config` to each
    /// benchmark node. Useful for tuning `wal_early_ack` or other
    /// config fields without changing defaults.
    #[arg(long)]
    pub node_config: Option<String>,

    /// R45 max ops per coalesced batch (--coalesce-max-keys on each
    /// spawned server). `0` disables (default).
    #[arg(long)]
    pub coalesce_max_keys: Option<usize>,

    /// R45b drain threshold (--coalesce-drain-threshold on each
    /// spawned server). Default `1`. `0` = always drain.
    #[arg(long)]
    pub coalesce_drain_threshold: Option<usize>,
}

/// Arguments for `crow-cli bench`.
#[derive(Args, Debug)]
pub struct BenchArgs {
    #[command(subcommand)]
    pub sub: Option<BenchSub>,
}

pub async fn run_bench_verb(cli: &Cli, args: BenchArgs) -> ExitCode {
    match args.sub {
        Some(BenchSub::Run(run_args)) => bench_benchmark(*run_args, cli.json).await,
        Some(BenchSub::Report { run_id }) => bench_report(&run_id, cli.json),
        Some(BenchSub::Compare { run_id_1, run_id_2 }) => bench_compare(&run_id_1, &run_id_2, cli.json),
        None => {
            eprintln!("usage: crow-cli bench <run|report|compare>");
            eprintln!("  run     Run a benchmark (deploy, drive load, collect, report)");
            eprintln!("  report  Re-render a previously-saved report");
            eprintln!("  compare Compare two previously-saved reports");
            ExitCode::from(1)
        }
    }
}

/// `bench run` — full self-contained lifecycle: deploy (fixture),
/// run, collect, cleanup, report.
#[allow(
    clippy::too_many_lines,
    reason = "orchestrates deploy/run/report; splitting reduces readability"
)]
async fn bench_benchmark(args: RunArgs, json: bool) -> ExitCode {
    use crate::bench::provision::{GROUP_ID, STORE_ID};
    use crate::bench::{run_bench, BenchConfig, BenchFixture, BenchMode, MinSlotPolicy, WorkloadKind};
    use crow_kv_client::{ReadEndpointPolicy, ReadMode};
    use std::time::Duration;

    let Some(mode) = BenchMode::parse(&args.mode) else {
        eprintln!("error: unknown mode {:?} (expected: mem|file|block)", args.mode);
        return ExitCode::from(1);
    };
    let kind = match WorkloadKind::parse(&args.workload) {
        Ok(k) => k,
        Err(bad) => {
            eprintln!("error: unknown workload {bad:?} (expected: read|write|list|mix)");
            return ExitCode::from(1);
        }
    };
    let read_mode = match args.read_mode.to_ascii_lowercase().as_str() {
        "linearizable" => ReadMode::Linearizable,
        "minslot" | "min_slot" => ReadMode::MinSlot,
        other => {
            eprintln!("error: unknown read-mode {other:?} (expected: linearizable|minslot)");
            return ExitCode::from(1);
        }
    };
    let min_slot_policy = match MinSlotPolicy::parse(&args.min_slot) {
        Ok(p) => p,
        Err(bad) => {
            eprintln!("error: unknown min-slot {bad:?} (expected: auto|zero|<n>)");
            return ExitCode::from(1);
        }
    };
    // Default: AnyReplica for MinSlot (exercises follower local-serve +
    // fallback), Leader for Linearizable (always targets leader anyway).
    let read_endpoint_policy = match args.read_endpoint_policy.as_deref() {
        Some(s) => match s.to_ascii_lowercase().as_str() {
            "leader" => ReadEndpointPolicy::Leader,
            "any-replica" | "any_replica" | "anyreplica" => ReadEndpointPolicy::AnyReplica,
            other => {
                eprintln!("error: unknown read-endpoint-policy {other:?} (expected: leader|any-replica)");
                return ExitCode::from(1);
            }
        },
        None => {
            if read_mode == ReadMode::MinSlot {
                ReadEndpointPolicy::AnyReplica
            } else {
                ReadEndpointPolicy::Leader
            }
        }
    };
    if args.config.is_some() {
        eprintln!("note: --config is accepted but not yet implemented; using the local 3-node fixture");
    }

    let run_id = args.run_id.clone().unwrap_or_else(next_run_id);

    let now = chrono::Utc::now();
    let folder_name = run_folder_name(&run_id, mode.label(), now);
    let run_dir = crate::bench::BenchReport::default_dir().join(&folder_name);
    let workspace_dir = run_dir.join("workspace");

    println!("provisioning 3-node cluster ({} mode)...", mode.label());
    let _ = std::io::Write::flush(&mut std::io::stdout());
    let mut fixture = match BenchFixture::new(
        mode,
        workspace_dir,
        args.max_inflight,
        args.metrics_interval,
        args.node_config.clone(),
        args.coalesce_max_keys,
        args.coalesce_drain_threshold,
    )
    .await
    {
        Ok(f) => f,
        Err(e) => {
            eprintln!("error: provision cluster: {e}");
            return ExitCode::from(2);
        }
    };

    let mut cfg = BenchConfig::defaults(fixture.leader_endpoint().to_string(), kind);
    cfg.store_id = STORE_ID;
    cfg.group_id = GROUP_ID;
    cfg.mode = mode.label().to_string();
    cfg.connections = args.connections;
    cfg.threads = args.threads;
    cfg.duration = Duration::from_secs(args.duration_secs);
    cfg.key_space = args.key_space;
    cfg.value_size = args.value_size;
    cfg.run_id = Some(run_id.clone());
    cfg.report_dir = Some(run_dir.clone());
    cfg.metrics_log_path = Some(run_dir.join("bench-metrics.log"));
    cfg.read_mode = read_mode;
    cfg.min_slot_policy = min_slot_policy;
    cfg.pre_populate = args
        .pre_populate
        .or_else(|| (kind == WorkloadKind::Read).then_some(200_000))
        .filter(|c| *c > 0);
    cfg.verify_bytes = args.verify_bytes;
    cfg.read_endpoint_policy = read_endpoint_policy;
    // AnyReplica needs the full replica list, which comes from a
    // `/topology` fetch against any `crow-kv-server`'s mgmt API. Leader
    // policy doesn't need it (the client seeds the leader directly).
    if cfg.read_endpoint_policy == ReadEndpointPolicy::AnyReplica {
        cfg.topology_seed = Some(fixture.node_mgmt_urls()[0].clone());
    }

    println!(
        "running {} workload for {}s...",
        args.workload, args.duration_secs
    );
    let _ = std::io::Write::flush(&mut std::io::stdout());
    let (mut report, path) = match run_bench(cfg).await {
        Ok(v) => v,
        Err(e) => {
            eprintln!("error: bench run: {e}");
            fixture.cleanup().await;
            return ExitCode::from(2);
        }
    };

    // Stop servers first so graceful shutdown flushes async C++ logs
    // (spdlog buffers info-level messages until flush/shutdown).
    fixture.cleanup().await;

    report.server_metrics = fixture.collect_metrics();
    let artifacts_dir = run_dir.join("artifacts");
    if let Err(e) = fixture.collect_logs(&artifacts_dir) {
        eprintln!("warning: failed to collect node logs: {e}");
    }
    if let Err(e) = report.write_to(&run_dir) {
        eprintln!("warning: failed to re-write report with server metrics: {e}");
    }
    let md_path = match report.write_md_to(
        &run_dir,
        fixture.node_ids(),
        fixture.workspace_dir(),
        &fixture.endpoint_to_node_map(),
    ) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("warning: failed to write markdown report: {e}");
            run_dir.join("report.md")
        }
    };
    let log_warning_count = count_log_warnings(&artifacts_dir);

    if json {
        return crate::utils::print_json(&report);
    }
    println!("{}", report.human_summary());
    println!("\nreport (json): {}", path.display());
    println!("report (md):   {}", md_path.display());
    print_anomalies(&report, log_warning_count);
    ExitCode::SUCCESS
}

/// Scan `bench-runs/` for existing run subdirectories named
/// `bench-{id}-...` and return the next incrementing id as a string
/// (e.g. `"1"`, `"2"`, ...).
fn next_run_id() -> String {
    use crate::bench::BenchReport;
    let dir = BenchReport::default_dir();
    let mut max_id: u64 = 0;
    if let Ok(entries) = std::fs::read_dir(&dir) {
        for entry in entries.flatten() {
            if let Some(name) = entry.file_name().to_str() {
                if let Some(rest) = name.strip_prefix("bench-") {
                    if let Some(num_end) = rest.find('-') {
                        if let Ok(n) = rest[..num_end].parse::<u64>() {
                            max_id = max_id.max(n);
                        }
                    }
                }
            }
        }
    }
    format!("{}-{}", max_id + 1, std::process::id())
}

/// Build the per-run folder name: `bench-{id}-{datetime}-{mode}`.
fn run_folder_name(run_id: &str, mode_label: &str, started_at: chrono::DateTime<chrono::Utc>) -> String {
    let dt = started_at.format("%Y%m%d-%H%M%S");
    format!("bench-{run_id}-{dt}-{mode_label}")
}

/// Locate `report.json` for a given run id by scanning `bench-runs/`
/// for a subdirectory named `bench-{run_id}-*`.
fn find_report_path(run_id: &str) -> std::path::PathBuf {
    use crate::bench::BenchReport;
    let dir = BenchReport::default_dir();
    let prefix = format!("bench-{run_id}-");
    if let Ok(entries) = std::fs::read_dir(&dir) {
        for entry in entries.flatten() {
            if let Some(name) = entry.file_name().to_str() {
                if name.starts_with(&prefix) {
                    let candidate = entry.path().join("report.json");
                    if candidate.exists() {
                        return candidate;
                    }
                }
            }
        }
    }
    dir.join(format!("{run_id}.json"))
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

/// `bench report` — re-render a previously-saved report.
fn bench_report(run_id: &str, json: bool) -> ExitCode {
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
                    let node_ids = vec!["bn0".to_string(), "bn1".to_string(), "bn2".to_string()];
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
fn bench_compare(run_id_1: &str, run_id_2: &str, json: bool) -> ExitCode {
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
