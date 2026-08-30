// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

mod bench_clean;
mod bench_deploy;
mod bench_kv;
mod bench_prepare;
mod bench_report;
mod bench_rpc;
mod bench_run;
mod bench_teardown;

pub(crate) use bench_clean::bench_clean;
pub(crate) use bench_deploy::bench_deploy;
pub(crate) use bench_kv::bench_benchmark_kv;
pub(crate) use bench_prepare::bench_prepare;
pub(crate) use bench_report::{bench_compare, bench_report};
pub(crate) use bench_rpc::bench_benchmark_rpc;
pub(crate) use bench_run::bench_run;
pub(crate) use bench_teardown::bench_teardown;

use clap::{Args, Subcommand};
use std::process::ExitCode;

use crate::Cli;

/// Subcommands for `crowdb-cli bench`.
#[derive(Subcommand, Debug)]
pub enum BenchSub {
    /// Run a KV benchmark (3-node Paxos cluster, full consensus + WAL + storage).
    Kv(Box<KvArgs>),
    /// Run an RPC echo benchmark (2-process transport throughput, no KV layer).
    Rpc(Box<RpcArgs>),
    /// Deploy a named cluster and leave it running for `bench run` to attach to.
    Deploy(Box<DeployArgs>),
    /// Pre-populate keys into a previously-deployed cluster.
    Prepare(Box<PrepareArgs>),
    /// Run a workload against a previously-deployed cluster.
    Run(Box<RunArgs>),
    /// Wipe user data on every node of a deployed cluster (keep group0).
    Clean(Box<CleanArgs>),
    /// Stop a previously-deployed cluster and remove its handle.
    Teardown(Box<TeardownArgs>),
    /// Re-render a previously-saved report.
    Report {
        /// Run ID of the report to re-render.
        run_id: String,
    },
    /// Print a side-by-side comparison of two previously-saved reports.
    Compare { run_id_1: String, run_id_2: String },
}

/// Arguments for `crowdb-cli bench kv`.
#[derive(Args, Debug)]
#[allow(clippy::struct_excessive_bools)]
pub struct KvArgs {
    /// Storage mode: `mem` (crowdb-tree + mem-block), `file` (crowdb-tree +
    /// file page store), or `block` (crowdb-tree + block page store).
    #[arg(short = 'M', long, default_value = "mem")]
    pub mode: String,

    /// Test duration in seconds.
    #[arg(short = 'd', long, default_value_t = 20)]
    pub duration_secs: u64,

    /// Workload kind: `read | write | list | mix`.
    #[arg(short = 'w', long, default_value = "mix")]
    pub workload: String,

    /// Number of worker threads.
    #[arg(short = 'L', long, default_value_t = 8)]
    pub loader_num: u32,

    #[arg(short = 'c', long, default_value_t = 4)]
    pub connections: u32,

    #[arg(short = 'k', long, default_value_t = 1_000_000)]
    pub key_space: u64,

    #[arg(short = 's', long, default_value_t = 512)]
    pub value_size: usize,

    /// Mixed value-size distribution for pre-population, as
    /// `size:percent,...` (e.g. `64:70,1024:20,16384:10`). Each
    /// pre-populated key gets a deterministic size based on its id.
    /// Percentages must sum to 100. When set, overrides `--value-size`
    /// for pre-population only. Useful for scan benches that want to
    /// exercise multiple value sizes in a single run.
    #[arg(short = 'x', long)]
    pub value_size_mix: Option<String>,

    /// Config-driven (SSH) cluster deployment. Accepted but not yet
    /// implemented — only the local 3-node fixture runs in this
    /// iteration.
    #[arg(long)]
    pub config: Option<String>,

    /// Optional explicit run id; defaults to an auto-incremented
    /// sequence number.
    #[arg(short = 'r', long)]
    pub run_id: Option<String>,

    /// Maximum in-flight proposals per group (--max-inflight on each
    /// spawned server). Default: 32.
    #[arg(long, default_value_t = 32)]
    pub max_inflight: usize,

    /// Server metrics log flush interval in seconds (--metrics-interval
    /// on each spawned server). Default: 5. Set to 1 for short bench runs.
    #[arg(short = 'm', long, default_value_t = 5)]
    pub metrics_interval: u64,

    /// Read mode for read ops: `linearizable` (default) or `minslot`.
    /// Ignored for write/delete ops.
    #[arg(short = 'R', long, default_value = "linearizable")]
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
    #[arg(short = 'P', long)]
    pub pre_populate: Option<u64>,

    /// Number of random bytes to spot-check per `Found` read against
    /// the deterministic value formula. Default 8. Set to 0 to disable
    /// verification.
    #[arg(short = 'v', long, default_value_t = 8)]
    pub verify_bytes: usize,

    /// `MinSlot` read-endpoint selection policy: `leader` (default,
    /// routes `MinSlot` reads to the leader), `any-replica`
    /// (distributes `MinSlot` reads round-robin across all replicas),
    /// `least-connections` (routes to the replica with the fewest
    /// in-flight reads), or `latency` (routes to the replica with the
    /// lowest recent RTT). Ignored for `Linearizable` reads. When not
    /// specified, defaults to `any-replica` for `--read-mode minslot`
    /// and `leader` for `--read-mode linearizable`.
    #[arg(short = 'e', long)]
    pub read_endpoint_policy: Option<String>,

    /// Optional `CrowDBConfig` JSON file passed as `--config` to each
    /// benchmark node. Useful for tuning `wal_early_ack` or other
    /// config fields without changing defaults.
    #[arg(short = 'N', long)]
    pub node_config: Option<String>,

    /// R45 max ops per coalesced batch (--coalesce-max-keys on each
    /// spawned server). `0` disables (default).
    #[arg(short = 'C', long)]
    pub coalesce_max_keys: Option<usize>,

    /// R45b drain threshold (--coalesce-drain-threshold on each
    /// spawned server). Default `1`. `0` = always drain.
    #[arg(short = 'D', long)]
    pub coalesce_drain_threshold: Option<usize>,

    /// Inter-server RPC connection pool size (--peer-pool-size on each
    /// spawned server). Default 2. Raise to 4 for high-concurrency.
    #[arg(long, default_value_t = 2)]
    pub peer_pool_size: usize,

    /// Enable Nagle on RPC connections (--enable-nagle on each spawned
    /// server). Default false.
    #[arg(short = 'n', long, default_value_t = false)]
    pub enable_nagle: bool,

    /// Enable `TCP_QUICKACK` on RPC connections (--quickack on each spawned
    /// server, Linux only). Breaks Nagle + delayed-ACK deadlock. Default false.
    #[arg(short = 'q', long, default_value_t = false)]
    pub quickack: bool,

    /// Event-write mode (--event-write on each spawned server).
    /// Coalesces frames via I/O worker. Default false.
    #[arg(short = 'E', long, default_value_t = false)]
    pub event_write: bool,

    /// Per-connection send queue capacity (--send-queue-capacity on
    /// each spawned server). Default 4096.
    #[arg(short = 'S', long, default_value_t = 4096)]
    pub send_queue_capacity: u32,

    /// Scan limit (max entries per scan op) for `--workload list`.
    /// Default 1 (the historical stub behavior). Set higher for
    /// bounded-limit / full-keyspace scan benches.
    #[arg(short = 'l', long, default_value_t = 1)]
    pub scan_limit: u32,

    /// Scan prefix for `--workload list`. Empty (default) = whole
    /// keyspace. Set to a bounded prefix (e.g. `k05`) for prefix-range
    /// scan benches.
    #[arg(long, default_value = "")]
    pub scan_prefix: String,

    /// Scan exclusive lower bound (`start_after`) for `--workload list`.
    /// Empty (default) = start from the beginning. Set near the end of
    /// the populated keyspace (in the same `k{id:020}` format) for
    /// deep-pagination scan benches.
    #[arg(long, default_value = "")]
    pub scan_start_after: String,

    /// After pre-population, drain L0 (`MemTable`) into L1 on every node
    /// via the management API before opening the measurement window.
    /// Produces a clean L1-only scan baseline (removes the
    /// `MemTable::snapshot()` `O(N_l0)` cost from the measurement),
    /// verifying the 1KiB anomaly hypothesis. Default off — without
    /// the flag, L0 size at scan time depends on value size (historical
    /// behavior).
    #[arg(short = 'f', long, default_value_t = false)]
    pub flush_after_prepopulate: bool,
}

/// Arguments for `crowdb-cli bench rpc`.
#[derive(Args, Debug)]
pub struct RpcArgs {
    /// Test duration in seconds.
    #[arg(short = 'd', long, default_value_t = 20)]
    pub duration_secs: u64,

    /// Number of C++ coroutines (load generators).
    #[arg(short = 'L', long, default_value_t = 128)]
    pub loader_num: u32,

    /// Worker execution model: `coroutine` (C++ coroutines on I/O
    /// threads, default) or `tokio` (Rust tokio tasks calling
    /// `RpcClient::call()` via oneshot channels). The tokio mode
    /// measures the async FFI path overhead (Box<oneshot::Sender> +
    /// scheduler wake/re-schedule per op).
    #[arg(short = 'M', long, default_value = "coroutine")]
    pub mode: String,

    /// Number of TCP connections to the fb server.
    #[arg(short = 'c', long, default_value_t = 4)]
    pub connections: u32,

    /// Number of independent epoll/kqueue instances. Each engine owns
    /// its own fd and its own set of connections (round-robin
    /// partitioned). 1 = single-engine (default). More than 1
    /// parallelizes event processing across independent kernel event
    /// queues with no ONESHOT re-arm overhead.
    #[arg(short = 'e', long, default_value_t = 1)]
    pub io_engines: u32,

    /// Total number of C++ I/O worker threads (across all engines).
    /// Per-engine = `io_workers` / `io_engines`. 1 = single-worker (fast
    /// path, no ONESHOT re-arm). More than 1 per engine enables
    /// `EV_ONESHOT`/`EPOLLONESHOT` within that engine for multi-worker
    /// safety. Must be divisible by `io_engines`.
    #[arg(short = 't', long, default_value_t = 2)]
    pub io_workers: u32,

    /// Enable Nagle's algorithm (disable `TCP_NODELAY`). Default false.
    #[arg(short = 'n', long, default_value_t = false)]
    pub enable_nagle: bool,

    /// Enable `TCP_QUICKACK` (Linux only). Breaks Nagle + delayed-ACK deadlock.
    #[arg(short = 'q', long, default_value_t = false)]
    pub quickack: bool,

    #[arg(short = 's', long, default_value_t = 128)]
    pub value_size: usize,

    /// Optional explicit run id; defaults to an auto-incremented
    /// sequence number.
    #[arg(short = 'r', long)]
    pub run_id: Option<String>,

    /// FB server port. Defaults to 18080 (the fb server's default
    /// port). The server must be started manually (e.g.
    /// `crowdb-rpc-fb-server --port=18080`); no auto-spawn. Use
    /// `tools/bench-rpc-regression.sh` for the wrapper that manages
    /// the server lifecycle.
    #[arg(short = 'P', long, default_value_t = 18080)]
    pub server_port: i32,

    /// Log directory for the fb server and client logs. Defaults to
    /// `bench-runs/<run>/`. All logs (server.log, metrics.log) go here.
    #[arg(short = 'l', long)]
    pub log_dir: Option<String>,

    /// Metrics flush interval in seconds (counters + latency histogram).
    /// Default 5.
    #[arg(short = 'm', long, default_value_t = 5)]
    pub metrics_interval: u64,
}

/// Arguments for `crowdb-cli bench`.
#[derive(Args, Debug)]
pub struct BenchArgs {
    #[command(subcommand)]
    pub sub: Option<BenchSub>,
}

/// Arguments for `crowdb-cli bench deploy`.
#[derive(Args, Debug)]
#[allow(clippy::struct_excessive_bools)]
pub struct DeployArgs {
    /// Deploy name — used as the `runtime/<name>/` folder and the
    /// `--target` value for `bench run`/`prepare`/`teardown`.
    #[arg(long)]
    pub name: String,

    /// Deploy kind: `kv` (default, 3-node Paxos cluster), `rpc`
    /// (2-process fb server), `chunk`, or `storage` (not yet
    /// implemented).
    #[arg(long, default_value = "kv")]
    pub kind: String,

    /// Spawn a standalone `crowdb-web` process (survives CLI exit) so
    /// the operator can use the UI. Default: headless (embedded
    /// console-web, detached after deploy).
    #[arg(long, default_value_t = false)]
    pub web: bool,

    // ── KV deploy tunables (same semantics as `bench kv`) ──────────
    #[arg(short = 'M', long, default_value = "mem")]
    pub mode: String,
    #[arg(long, default_value_t = 32)]
    pub max_inflight: usize,
    #[arg(short = 'm', long, default_value_t = 5)]
    pub metrics_interval: u64,
    #[arg(short = 'N', long)]
    pub node_config: Option<String>,
    #[arg(short = 'C', long)]
    pub coalesce_max_keys: Option<usize>,
    #[arg(short = 'D', long)]
    pub coalesce_drain_threshold: Option<usize>,
    #[arg(long, default_value_t = 2)]
    pub peer_pool_size: usize,
    #[arg(short = 'n', long, default_value_t = false)]
    pub enable_nagle: bool,
    #[arg(short = 'q', long, default_value_t = false)]
    pub quickack: bool,
    #[arg(short = 'E', long, default_value_t = false)]
    pub event_write: bool,
    #[arg(short = 'S', long, default_value_t = 4096)]
    pub send_queue_capacity: u32,
}

/// Arguments for `crowdb-cli bench prepare`.
#[derive(Args, Debug)]
pub struct PrepareArgs {
    /// Deploy name (from `bench deploy --name`).
    #[arg(long)]
    pub target: String,

    /// Number of keys to write (sequential `put` of `k{id:020}` for
    /// `id in 0..keys`).
    #[arg(short = 'P', long)]
    pub keys: u64,

    /// Value size in bytes (default 512).
    #[arg(short = 's', long, default_value_t = 512)]
    pub value_size: usize,

    /// Mixed value-size distribution, as `size:percent,...` (e.g.
    /// `64:70,1024:20,16384:10`). Overrides `--value-size` per key.
    #[arg(short = 'x', long)]
    pub value_size_mix: Option<String>,
}

/// Arguments for `crowdb-cli bench run`.
#[derive(Args, Debug)]
#[allow(clippy::struct_excessive_bools)]
pub struct RunArgs {
    /// Deploy name (from `bench deploy --name`).
    #[arg(long)]
    pub target: String,

    /// Workload kind: `read | write | list | mix`.
    #[arg(short = 'w', long, default_value = "mix")]
    pub workload: String,

    /// Test duration in seconds.
    #[arg(short = 'd', long, default_value_t = 20)]
    pub duration_secs: u64,

    /// Number of worker threads.
    #[arg(short = 'L', long, default_value_t = 8)]
    pub loader_num: u32,

    #[arg(short = 'c', long, default_value_t = 4)]
    pub connections: u32,

    #[arg(short = 'k', long, default_value_t = 1_000_000)]
    pub key_space: u64,

    #[arg(short = 's', long, default_value_t = 512)]
    pub value_size: usize,

    /// Mixed value-size distribution for pre-population (see
    /// `bench kv --value-size-mix`). Only used if `--pre-populate` is
    /// set.
    #[arg(short = 'x', long)]
    pub value_size_mix: Option<String>,

    /// Optional explicit run id; defaults to an auto-incremented
    /// sequence number.
    #[arg(short = 'r', long)]
    pub run_id: Option<String>,

    /// Read mode for read ops: `linearizable` (default) or `minslot`.
    #[arg(short = 'R', long, default_value = "linearizable")]
    pub read_mode: String,

    /// `min_slot` policy for `MinSlot` reads: `auto`, `zero`, or a
    /// fixed slot number.
    #[arg(long, default_value = "auto")]
    pub min_slot: String,

    /// Pre-population count: write `<count>` keys before measurement.
    /// Default 0 (use `bench prepare` for explicit pre-population).
    #[arg(short = 'P', long)]
    pub pre_populate: Option<u64>,

    /// Number of random bytes to spot-check per `Found` read. Default 8.
    #[arg(short = 'v', long, default_value_t = 8)]
    pub verify_bytes: usize,

    /// `MinSlot` read-endpoint selection policy: `leader`,
    /// `any-replica`, `least-connections`, or `latency`.
    #[arg(short = 'e', long)]
    pub read_endpoint_policy: Option<String>,

    /// Scan limit for `--workload list`. Default 1.
    #[arg(short = 'l', long, default_value_t = 1)]
    pub scan_limit: u32,

    /// Scan prefix for `--workload list`. Empty = whole keyspace.
    #[arg(long, default_value = "")]
    pub scan_prefix: String,

    /// Scan exclusive lower bound for `--workload list`.
    #[arg(long, default_value = "")]
    pub scan_start_after: String,

    /// After pre-population, drain L0 into L1 on every node before
    /// measurement.
    #[arg(short = 'f', long, default_value_t = false)]
    pub flush_after_prepopulate: bool,
}

/// Arguments for `crowdb-cli bench teardown`.
#[derive(Args, Debug)]
pub struct TeardownArgs {
    /// Deploy name (from `bench deploy --name`).
    #[arg(long)]
    pub target: String,
}

/// Arguments for `crowdb-cli bench clean`.
#[derive(Args, Debug)]
pub struct CleanArgs {
    /// Deploy name (from `bench deploy --name`).
    #[arg(long)]
    pub target: String,
}

pub(crate) async fn run_bench_verb(cli: &Cli, args: BenchArgs) -> ExitCode {
    match args.sub {
        Some(BenchSub::Kv(args)) => bench_benchmark_kv(*args, cli.json).await,
        Some(BenchSub::Rpc(args)) => bench_benchmark_rpc(*args, cli.json).await,
        Some(BenchSub::Deploy(args)) => bench_deploy(*args, cli.json).await,
        Some(BenchSub::Prepare(args)) => bench_prepare(*args, cli.json).await,
        Some(BenchSub::Run(args)) => bench_run(*args, cli.json).await,
        Some(BenchSub::Clean(args)) => bench_clean(*args, cli.json).await,
        Some(BenchSub::Teardown(args)) => bench_teardown(*args, cli.json).await,
        Some(BenchSub::Report { run_id }) => bench_report(&run_id, cli.json),
        Some(BenchSub::Compare { run_id_1, run_id_2 }) => bench_compare(&run_id_1, &run_id_2, cli.json),
        None => {
            eprintln!("usage: crowdb-cli bench <kv|rpc|deploy|prepare|run|clean|teardown|report|compare>");
            eprintln!("  kv        Run a KV benchmark (3-node cluster, all-in-one)");
            eprintln!("  rpc       Run an RPC echo benchmark (transport throughput)");
            eprintln!("  deploy    Deploy a named cluster and leave it running");
            eprintln!("  prepare   Pre-populate keys into a deployed cluster");
            eprintln!("  run       Run a workload against a deployed cluster");
            eprintln!("  clean     Wipe user data on a deployed cluster (keep group0)");
            eprintln!("  teardown  Stop a deployed cluster and remove its handle");
            eprintln!("  report    Re-render a previously-saved report");
            eprintln!("  compare   Compare two previously-saved reports");
            ExitCode::from(1)
        }
    }
}

/// Scan `bench-runs/` for existing run subdirectories named
/// `bench-{id}-...` and return the next incrementing id as a string
/// (e.g. `"1"`, `"2"`, ...).
pub(crate) fn next_run_id() -> String {
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
pub(crate) fn run_folder_name(
    run_id: &str,
    mode_label: &str,
    started_at: chrono::DateTime<chrono::Utc>,
) -> String {
    let dt = started_at.format("%Y%m%d-%H%M%S");
    format!("bench-{run_id}-{dt}-{mode_label}")
}

/// Locate `report.json` for a given run id by scanning `bench-runs/`
/// for a subdirectory named `bench-{run_id}-*`.
pub(crate) fn find_report_path(run_id: &str) -> std::path::PathBuf {
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
