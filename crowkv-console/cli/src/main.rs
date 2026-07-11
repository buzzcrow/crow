//! `crowkv` CLI entrypoint.
//!
//! C2 status: `cluster status/topology` resolve servers from `--server`
//! (highest priority), `CROWKV_SERVER` env var, or the persisted
//! registry (`~/.crowkv/console.toml`). `server add/remove/list` manage
//! the registry. Other verbs remain placeholders.

mod bench;
mod commands;
mod utils;

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};

use commands::{
    run_bench_verb, run_cluster_status, run_cluster_topology, run_kv_verb, run_node_verb, run_paxos_verb, run_rack_verb, run_replica_verb, run_server_verb, run_store_verb,
};
use commands::{BenchVerb, ClusterVerb, KvVerb, NodeVerb, PaxosVerb, RackVerb, ReplicaVerb, ServerVerb, StoreVerb};

#[derive(Parser, Debug)]
#[command(name = "crowkv", version, about = "CrowKV cluster console (CLI)")]
struct Cli {
    /// `crowkv-web` console base URL. The CLI talks to the console,
    /// not directly to a `crowkv-server`; the console resolves
    /// upstream nodes from its config and the monitor cache.
    #[arg(long, global = true, env = "CROWKV_CONSOLE", default_value = "http://127.0.0.1:9920")]
    console: String,

    /// **Deprecated**: pre-A12 verbs that still talk directly to a
    /// single `crowkv-server` (`store`, `paxos`, `replica` legacy
    /// remote-replica primitives) read this. New verbs (`kv`, the
    /// orchestrated `store`/`group`/`replica` plane) ignore it and
    /// use `--console` instead. Keep this flag until the legacy
    /// commands are migrated, then remove.
    #[arg(long, global = true, env = "CROWKV_SERVER")]
    server: Option<String>,

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
    Paxos {
        #[command(subcommand)]
        verb: PaxosVerb,
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

    let runtime = tokio::runtime::Builder::new_multi_thread().enable_all().build().expect("tokio runtime");

    // Bind a fresh correlation id for the whole invocation. Every
    // client call inside `dispatch` will attach it as
    // `x-crowkv-corr-id` and stamp it on every ops-log record.
    let cid = crowkv_console_shared::corr_id::generate();
    runtime.block_on(async move { crowkv_console_shared::corr_id::scope(cid, dispatch(cli)).await })
}

async fn dispatch(mut cli: Cli) -> ExitCode {
    let command = std::mem::replace(&mut cli.command, Group::Cluster { verb: ClusterVerb::Status });
    match command {
        Group::Cluster { verb } => match verb {
            ClusterVerb::Status => run_cluster_status(&cli).await,
            ClusterVerb::Topology => run_cluster_topology(&cli).await,
            ClusterVerb::Inspect { .. } => utils::not_implemented("cluster inspect"),
        },
        Group::Rack { verb } => run_rack_verb(&cli, verb).await,
        Group::Node { verb } => run_node_verb(&cli, verb).await,
        Group::Server { verb } => run_server_verb(&cli, verb).await,
        Group::Store { verb } => run_store_verb(&cli, verb).await,
        Group::Paxos { verb } => run_paxos_verb(&cli, verb).await,
        Group::Replica { verb } => run_replica_verb(&cli, verb).await,
        Group::Kv { verb } => run_kv_verb(&cli, verb).await,
        Group::Bench { verb } => run_bench_verb(&cli, verb).await,
    }
}
