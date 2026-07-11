//! `crowkv` CLI entrypoint.
//!
//! Every verb routes through `ConsoleClient` against a `crowkv-web`
//! console (`--console`, default `http://127.0.0.1:9920`): the CLI is a
//! thin clap argument-parsing layer over the same `shared` core the Web
//! UI uses, and the console resolves upstream `crowkv-server` nodes from
//! its config and monitor cache. There is no direct `crowkv-server` /
//! registry path; even `bench` resolves its gRPC target via the console.

mod bench;
mod commands;
mod utils;

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};

use commands::{
    run_bench_verb, run_cluster_inspect, run_cluster_status, run_cluster_topology, run_group_verb,
    run_kv_verb, run_node_verb, run_rack_verb, run_replica_verb, run_server_verb, run_store_verb,
};
use commands::{
    BenchVerb, ClusterVerb, GroupVerb, KvVerb, NodeVerb, RackVerb, ReplicaVerb, ServerVerb, StoreVerb,
};

#[derive(Parser, Debug)]
#[command(name = "crowkv", version, about = "CrowKV cluster console (CLI)")]
struct Cli {
    /// `crowkv-web` console base URL. The CLI talks to the console,
    /// not directly to a `crowkv-server`; the console resolves
    /// upstream nodes from its config and the monitor cache.
    #[arg(
        long,
        global = true,
        env = "CROWKV_CONSOLE",
        default_value = "http://127.0.0.1:9920"
    )]
    console: String,

    /// Path to the console config file. Defaults to
    /// `$CROWKV_CONSOLE_CONFIG` or `~/.crowkv/console.toml`.
    #[arg(long, global = true, env = "CROWKV_CONSOLE_CONFIG")]
    config: Option<PathBuf>,

    /// Emit JSON instead of human-readable output where applicable.
    #[arg(long, global = true)]
    json: bool,

    #[command(subcommand)]
    command: Group,
}

#[derive(Subcommand, Debug)]
enum Group {
    /// Cluster observation commands.
    Cluster {
        #[command(subcommand)]
        verb: ClusterVerb,
    },
    /// Simulated hardware: racks.
    Rack {
        #[command(subcommand)]
        verb: RackVerb,
    },
    /// Simulated hardware: nodes (host + ssh creds).
    Node {
        #[command(subcommand)]
        verb: NodeVerb,
    },
    /// crowkv-server lifecycle on a node.
    Server {
        #[command(subcommand)]
        verb: ServerVerb,
    },
    /// Store management within a server.
    Store {
        #[command(subcommand)]
        verb: StoreVerb,
    },
    /// Paxos group management.
    #[command(name = "group", alias = "paxos")]
    Paxos {
        #[command(subcommand)]
        verb: GroupVerb,
    },
    /// Replica add/remove.
    Replica {
        #[command(subcommand)]
        verb: ReplicaVerb,
    },
    /// Data-plane KV operations.
    Kv {
        #[command(subcommand)]
        verb: KvVerb,
    },
    /// Load testing (CLI-only).
    Bench {
        #[command(subcommand)]
        verb: BenchVerb,
    },
}

fn main() -> ExitCode {
    let cli = Cli::parse();

    // Open one per-invocation operation log file. Outbound calls from
    // `ConsoleClient` / `ServerClient` append a JSON-Lines record each
    // so a failed run can be replayed by copying the recorded curl
    // command. Best-effort: errors are logged via `tracing` and we keep
    // going.
    crowkv_console_shared::ops_log::init_default("cli");

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    // Bind a fresh correlation id for the whole invocation. Every
    // client call inside `dispatch` will attach it as
    // `x-crowkv-corr-id` and stamp it on every ops-log record.
    let cid = crowkv_console_shared::corr_id::generate();
    runtime.block_on(async move { crowkv_console_shared::corr_id::scope(cid, dispatch(cli)).await })
}

async fn dispatch(mut cli: Cli) -> ExitCode {
    let command = std::mem::replace(
        &mut cli.command,
        Group::Cluster {
            verb: ClusterVerb::Status,
        },
    );
    match command {
        Group::Cluster { verb } => match verb {
            ClusterVerb::Status => run_cluster_status(&cli).await,
            ClusterVerb::Topology => run_cluster_topology(&cli).await,
            ClusterVerb::Inspect { id } => run_cluster_inspect(&cli, &id).await,
        },
        Group::Rack { verb } => run_rack_verb(&cli, verb).await,
        Group::Node { verb } => run_node_verb(&cli, verb).await,
        Group::Server { verb } => run_server_verb(&cli, verb).await,
        Group::Store { verb } => run_store_verb(&cli, verb).await,
        Group::Paxos { verb } => run_group_verb(&cli, verb).await,
        Group::Replica { verb } => run_replica_verb(&cli, verb).await,
        Group::Kv { verb } => run_kv_verb(&cli, verb).await,
        Group::Bench { verb } => run_bench_verb(&cli, verb).await,
    }
}
