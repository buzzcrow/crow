// Copyright 2026-present Gian <crow.db@outlook.com>

use std::process::ExitCode;
use std::time::Duration;

use crate::bench::handle::{ClusterHandle, DeployKind};
use crate::commands::bench::{next_run_id, run_folder_name};

/// `bench run` — run a workload against a previously-deployed cluster.
#[allow(
    clippy::too_many_lines,
    reason = "orchestrates run/report; splitting reduces readability"
)]
pub(crate) async fn bench_run(args: super::RunArgs, json: bool) -> ExitCode {
    use crate::bench::target::kv::AttachedKvTarget;
    use crate::bench::target::BenchTarget;
    use crate::bench::{run_bench, BenchConfig, MinSlotPolicy, WorkloadKind};
    use crowdb_kv_client::{ReadEndpointPolicy, ReadMode};

    let handle = match ClusterHandle::load(&args.target) {
        Ok(h) => h,
        Err(e) => {
            eprintln!("error: {e}");
            return ExitCode::from(2);
        }
    };

    // Kind check: kv handle → kv workload. RPC handle → error (use
    // `bench rpc` for RPC benches).
    if handle.kind != DeployKind::Kv {
        eprintln!(
            "error: kind mismatch — target '{}' is kind={} but `bench run` expects kind=kv",
            handle.name,
            handle.kind.label()
        );
        return ExitCode::from(2);
    }

    let kind = match WorkloadKind::parse(&args.workload) {
        Ok(k) => k,
        Err(bad) => {
            eprintln!("error: unknown workload {bad:?} (expected: read|write|list|mix)");
            return ExitCode::from(2);
        }
    };
    let read_mode = match args.read_mode.to_ascii_lowercase().as_str() {
        "linearizable" => ReadMode::Linearizable,
        "minslot" | "min_slot" => ReadMode::MinSlot,
        other => {
            eprintln!("error: unknown read-mode {other:?} (expected: linearizable|minslot)");
            return ExitCode::from(2);
        }
    };
    let min_slot_policy = match MinSlotPolicy::parse(&args.min_slot) {
        Ok(p) => p,
        Err(bad) => {
            eprintln!("error: unknown min-slot {bad:?} (expected: auto|zero|<n>)");
            return ExitCode::from(2);
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
                return ExitCode::from(2);
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

    let run_id = args.run_id.clone().unwrap_or_else(next_run_id);
    let now = chrono::Utc::now();
    let folder_name = run_folder_name(&run_id, &handle.mode, now);
    // Reports go to runtime/<name>/runs/<folder>/.
    let run_dir = ClusterHandle::runtime_dir(&handle.name)
        .join("runs")
        .join(&folder_name);

    let mut target = AttachedKvTarget::new(handle);

    let mut cfg = BenchConfig::defaults(String::new(), kind);
    cfg.target = "kv".to_string();
    cfg.store_id = target_handle_store_id(&target);
    cfg.group_id = target_handle_group_id(&target);
    cfg.mode = target_handle_mode(&target).to_string();
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
                return ExitCode::from(2);
            }
        }
    }
    cfg.run_id = Some(run_id.clone());
    cfg.report_dir = Some(run_dir.clone());
    cfg.metrics_log_path = Some(run_dir.join("bench-metrics.log"));
    cfg.read_mode = read_mode;
    cfg.min_slot_policy = min_slot_policy;
    cfg.pre_populate = args.pre_populate.filter(|c| *c > 0);
    cfg.verify_bytes = args.verify_bytes;
    cfg.read_endpoint_policy = read_endpoint_policy;
    cfg.scan_limit = args.scan_limit;
    cfg.scan_prefix = args.scan_prefix.into_bytes();
    cfg.scan_start_after = args.scan_start_after.into_bytes();
    cfg.flush_after_prepopulate = args.flush_after_prepopulate;

    println!(
        "running {} workload for {}s against target '{}'...",
        args.workload, args.duration_secs, args.target
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

    // No cleanup — the cluster stays running for future `bench run` calls.

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

    if json {
        return crate::utils::print_json(&report);
    }
    println!("{}", report.human_summary());
    println!("\nreport (json): {}", path.display());
    println!("report (md):   {}", md_path.display());
    ExitCode::SUCCESS
}

// ── Helpers to read handle fields before `provision` builds the client ──

fn target_handle_store_id(t: &crate::bench::target::kv::AttachedKvTarget) -> u64 {
    t.handle_store_id()
}
fn target_handle_group_id(t: &crate::bench::target::kv::AttachedKvTarget) -> u64 {
    t.handle_group_id()
}
fn target_handle_mode(t: &crate::bench::target::kv::AttachedKvTarget) -> &str {
    t.handle_mode()
}
