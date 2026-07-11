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
    use crate::commands::kv::resolve_kv_endpoint;
    use std::time::Duration;

    let kind = match WorkloadKind::parse(&args.workload) {
        Ok(k) => k,
        Err(bad) => {
            eprintln!("error: unknown workload {bad:?} (expected: read|write|list|mix)");
            return ExitCode::from(1);
        }
    };
    let endpoint = match resolve_kv_endpoint(cli, args.store_id).await {
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
    use crate::commands::kv::resolve_kv_endpoint;
    use std::time::Duration;

    let endpoint = match resolve_kv_endpoint(cli, store_id).await {
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

fn bench_report(run_id: &str, json: bool) -> ExitCode {
    use crate::bench::BenchReport;

    let Some(dir) = BenchReport::default_dir() else {
        eprintln!("error: cannot resolve $HOME for report dir");
        return ExitCode::from(1);
    };
    let path = dir.join(format!("{run_id}.json"));
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
