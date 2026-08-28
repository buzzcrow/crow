// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

use std::process::ExitCode;
use std::time::Duration;

use crate::commands::bench::{next_run_id, run_folder_name};

/// KV bench: deploy 3-node cluster, drive load, collect metrics, report.
#[allow(
    clippy::too_many_lines,
    reason = "orchestrates deploy/run/report; splitting reduces readability"
)]
pub(crate) async fn bench_benchmark_kv(args: super::KvArgs, json: bool) -> ExitCode {
    use crate::bench::target::kv::{BenchMode, KvTarget, GROUP_ID, STORE_ID};
    use crate::bench::target::BenchTarget;
    use crate::bench::{run_bench, BenchConfig, MinSlotPolicy, WorkloadKind};
    use crow_kv_client::{ReadEndpointPolicy, ReadMode};

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
    let read_endpoint_policy = match args.read_endpoint_policy.as_deref() {
        Some(s) => match s.to_ascii_lowercase().as_str() {
            "leader" => ReadEndpointPolicy::Leader,
            "any-replica" | "any_replica" | "anyreplica" => ReadEndpointPolicy::AnyReplica,
            "least-connections" | "least_connections" | "leastconnections" => {
                ReadEndpointPolicy::LeastConnections
            }
            "latency" => ReadEndpointPolicy::Latency,
            other => {
                eprintln!(
                    "error: unknown read-endpoint-policy {other:?} (expected: leader|any-replica|least-connections|latency)"
                );
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

    let mut target = KvTarget::new(
        mode,
        workspace_dir,
        args.max_inflight,
        args.metrics_interval,
        args.node_config.clone(),
        args.coalesce_max_keys,
        args.coalesce_drain_threshold,
        args.peer_pool_size,
        args.enable_nagle,
        args.quickack,
        args.event_write,
        args.send_queue_capacity,
    );

    let mut cfg = BenchConfig::defaults(String::new(), kind);
    cfg.target = "kv".to_string();
    cfg.store_id = STORE_ID;
    cfg.group_id = GROUP_ID;
    cfg.mode = mode.label().to_string();
    cfg.connections = args.connections;
    cfg.loader_num = args.loader_num;
    cfg.duration = Duration::from_secs(args.duration_secs);
    cfg.key_space = args.key_space;
    cfg.value_size = args.value_size;
    if let Some(ref mix_spec) = args.value_size_mix {
        match crate::bench::workload::ValueSizeMix::parse(mix_spec) {
            Ok(mix) => cfg.value_size_mix = Some(mix),
            Err(e) => {
                eprintln!("error: {e}");
                target.cleanup().await;
                return ExitCode::from(2);
            }
        }
    }
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
    cfg.scan_limit = args.scan_limit;
    cfg.scan_prefix = args.scan_prefix.into_bytes();
    cfg.scan_start_after = args.scan_start_after.into_bytes();
    cfg.flush_after_prepopulate = args.flush_after_prepopulate;

    println!(
        "running {} workload for {}s...",
        args.workload, args.duration_secs
    );
    let _ = std::io::Write::flush(&mut std::io::stdout());

    let (mut report, path) = match run_bench(&mut target, cfg).await {
        Ok(v) => v,
        Err(e) => {
            eprintln!("error: bench run: {e}");
            target.cleanup().await;
            return ExitCode::from(2);
        }
    };

    // Stop servers first so graceful shutdown flushes async C++ logs.
    target.cleanup().await;

    let (server_metrics, _) = target.collect_artifacts();
    report.server_metrics = server_metrics;
    report.client_transport_stats = target.client_transport_stats();

    let artifacts_dir = run_dir.join("artifacts");
    let node_ids = target.node_ids();
    let workspace = target.workspace_dir();
    if let Err(e) = target.collect_logs(&artifacts_dir) {
        eprintln!("warning: failed to collect node logs: {e}");
    }
    if let Err(e) = report.write_to(&run_dir) {
        eprintln!("warning: failed to re-write report with server metrics: {e}");
    }
    let md_path = match report.write_md_to(&run_dir, &node_ids, &workspace, &target.endpoint_to_node_map()) {
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
