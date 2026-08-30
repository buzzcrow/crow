// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `crowdb-cli` CLI entrypoint (R126 restructure).
//!
//! Four top-level domains: `cluster`, `kv`, `chunk`, `bench`. The CLI
//! talks directly to group-0 system metadata via `CrowdbSysmdClient`
//! and to individual `crowdb-kv-server` management APIs — no
//! `crowdb-web` intermediary. The connection target is
//! `--sysmd-ip`/`--sysmd-port` (a group-0 leader's crowdb-rpc endpoint).

mod commands;

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};

use commands::{
    run_bench_verb, run_chunk_diskdb_verb, run_chunk_stub_verb, run_cluster_verb, run_group_verb,
    run_kv_data_verb, run_kv_server_verb, run_replica_verb, run_store_verb, BenchVerb, ChunkDiskdbVerb,
    ChunkStubVerb, ClusterVerb, GroupVerb, KvDataVerb, KvServerVerb, ReplicaVerb, StoreVerb,
};

#[derive(Parser, Debug)]
#[command(name = "crowdb-cli", version, about = "CrowDB cluster console (CLI)")]
struct Cli {
    /// Group-0 leader's IP address (the sysmd endpoint).
    #[arg(long, global = true, env = "CROWDB_SYSMD_IP", default_value = "127.0.0.1")]
    sysmd_ip: String,

    /// Group-0 leader's port (the sysmd endpoint's mgmt port).
    #[arg(long, global = true, env = "CROWDB_SYSMD_PORT", default_value_t = 9910)]
    sysmd_port: u16,

    /// Path to the console config file. Defaults to
    /// `$CROWDB_CONSOLE_CONFIG` or `~/.config/crowdb-kv/console.toml`.
    #[arg(short = 'p', long, global = true, env = "CROWDB_CONSOLE_CONFIG")]
    config: Option<PathBuf>,

    /// Emit JSON instead of human-readable output where applicable.
    #[arg(short = 'j', long, global = true)]
    json: bool,

    #[command(subcommand)]
    command: Domain,
}

#[derive(Subcommand, Debug)]
enum Domain {
    /// Hardware topology + cluster-level ops.
    #[command(alias = "cls")]
    Cluster {
        #[command(subcommand)]
        verb: ClusterVerb,
    },
    /// KV layer: server lifecycle + logical concepts + data-plane.
    Kv {
        #[command(subcommand)]
        verb: KvVerb,
    },
    /// Chunk storage service cluster.
    Chunk {
        #[command(subcommand)]
        verb: ChunkVerb,
    },
    /// Load injection only.
    Bench {
        #[command(subcommand)]
        verb: BenchVerb,
    },
}

#[derive(Subcommand, Debug)]
enum KvVerb {
    #[command(subcommand)]
    Server(KvServerVerb),
    #[command(subcommand)]
    Store(StoreVerb),
    #[command(subcommand)]
    Group(GroupVerb),
    #[command(subcommand)]
    Replica(ReplicaVerb),
    #[command(subcommand)]
    Data(KvDataVerb),
}

#[derive(Subcommand, Debug)]
enum ChunkVerb {
    #[command(subcommand)]
    Diskdb(ChunkDiskdbVerb),
    #[command(subcommand)]
    Stub(ChunkStubVerb),
}

fn main() -> ExitCode {
    let cli = Cli::parse();

    crowdb_console_shared::ops_log::init_default("cli");

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    let cid = crowdb_console_shared::corr_id::generate();
    runtime.block_on(async move { Box::pin(crowdb_console_shared::corr_id::scope(cid, dispatch(cli))).await })
}

async fn dispatch(mut cli: Cli) -> ExitCode {
    let command = std::mem::replace(
        &mut cli.command,
        Domain::Cluster {
            verb: ClusterVerb::Status,
        },
    );
    match command {
        Domain::Cluster { verb } => run_cluster_verb(&cli, verb).await,
        Domain::Kv { verb } => match verb {
            KvVerb::Server(sv) => run_kv_server_verb(&cli, sv).await,
            KvVerb::Store(sv) => run_store_verb(&cli, sv).await,
            KvVerb::Group(gv) => run_group_verb(&cli, gv).await,
            KvVerb::Replica(rv) => run_replica_verb(&cli, rv).await,
            KvVerb::Data(dv) => run_kv_data_verb(&cli, dv).await,
        },
        Domain::Chunk { verb } => match verb {
            ChunkVerb::Diskdb(dv) => run_chunk_diskdb_verb(&cli, dv).await,
            ChunkVerb::Stub(sv) => run_chunk_stub_verb(&cli, sv).await,
        },
        Domain::Bench { verb } => run_bench_verb(&cli, verb).await,
    }
}
