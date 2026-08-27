// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

use std::process::ExitCode;
use std::time::Duration;

use crate::commands::bench::{next_run_id, run_folder_name};

/// RPC bench: 2-process echo server (child) + client (CLI), measure
/// raw transport throughput.
pub(crate) async fn bench_benchmark_rpc(args: super::RpcArgs, json: bool) -> ExitCode {
    use crate::bench::target::rpc::RpcTarget;
    use crate::bench::target::BenchTarget;
    use crate::bench::{run_bench, BenchConfig, WorkloadKind};

    let run_id = args.run_id.clone().unwrap_or_else(next_run_id);
    let now = chrono::Utc::now();
    let folder_name = run_folder_name(&run_id, "rpc", now);
    let run_dir = crate::bench::BenchReport::default_dir().join(&folder_name);

    println!("provisioning 2-process RPC echo server...");
    let _ = std::io::Write::flush(&mut std::io::stdout());

    let mut target = RpcTarget::new();

    let mut cfg = BenchConfig::defaults(String::new(), WorkloadKind::Write);
    cfg.target = "rpc".to_string();
    cfg.mode = "rpc".to_string();
    cfg.connections = args.connections;
    cfg.loader_num = args.loader_num;
    cfg.duration = Duration::from_secs(args.duration_secs);
    cfg.key_space = args.key_space;
    cfg.value_size = args.value_size;
    cfg.io_engines = args.io_engines;
    cfg.io_workers = args.io_workers;
    cfg.enable_nagle = args.enable_nagle;
    cfg.server_port = args.server_port;
    cfg.log_dir = args
        .log_dir
        .clone()
        .or_else(|| Some(run_dir.to_string_lossy().to_string()));
    cfg.metrics_interval = args.metrics_interval;
    cfg.rpc_worker_mode = match args.mode.as_str() {
        "tokio" => crate::bench::runner::RpcWorkerMode::Tokio,
        "coroutine" => crate::bench::runner::RpcWorkerMode::Coroutine,
        other => {
            eprintln!("error: --mode must be 'coroutine' or 'tokio', got '{other}'");
            return ExitCode::from(2);
        }
    };
    cfg.run_id = Some(run_id.clone());
    cfg.report_dir = Some(run_dir.clone());

    println!(
        "running rpc echo for {}s (loaders={}, mode={})...",
        args.duration_secs,
        args.loader_num,
        cfg.rpc_worker_mode.label()
    );
    let _ = std::io::Write::flush(&mut std::io::stdout());

    let (report, path) = match run_bench(&mut target, cfg).await {
        Ok(v) => v,
        Err(e) => {
            eprintln!("error: bench run: {e}");
            target.cleanup().await;
            return ExitCode::from(2);
        }
    };

    target.cleanup().await;

    if json {
        return crate::utils::print_json(&report);
    }
    println!("{}", report.human_summary());
    println!("\nreport (json): {}", path.display());
    ExitCode::SUCCESS
}
