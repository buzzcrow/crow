// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Command module dispatch for the four-domain CLI (R126).
//!
//! Each subcommand module defines a `*Verb` enum (clap) and a `run_*`
//! async function that builds an [`OpContext`] and delegates to the
//! corresponding `ops::*` function.

pub(crate) mod bench;
pub(crate) mod chunk_diskdb;
pub(crate) mod chunk_stub;
pub(crate) mod cluster;
pub(crate) mod hardware;
pub(crate) mod kv_data;
pub(crate) mod kv_logical;
pub(crate) mod kv_server;

pub(crate) use bench::{run_bench_verb, BenchVerb};
pub(crate) use chunk_diskdb::{run_chunk_diskdb_verb, ChunkDiskdbVerb};
pub(crate) use chunk_stub::{run_chunk_stub_verb, ChunkStubVerb};
pub(crate) use cluster::{run_cluster_verb, ClusterVerb};
pub(crate) use hardware::{
    run_disk_group_verb, run_disk_verb, run_node_verb, run_rack_verb, DiskGroupVerb, DiskVerb, NodeVerb,
    RackVerb,
};
pub(crate) use kv_data::{run_kv_data_verb, KvDataVerb};
pub(crate) use kv_logical::{
    run_group_verb, run_replica_verb, run_store_verb, GroupVerb, ReplicaVerb, StoreVerb,
};
pub(crate) use kv_server::{run_kv_server_verb, KvServerVerb};

use std::process::ExitCode;

use crowdb_console_shared::ops::OpContext;
use crowdb_console_shared::ConsoleConfigEngine;

use crate::Cli;

/// Print a JSON value (if `--json`) or skip. Returns `ExitCode::SUCCESS`
/// on success.
pub(crate) fn print_json<T: serde::Serialize>(cli: &Cli, value: &T) -> ExitCode {
    if cli.json {
        match serde_json::to_string_pretty(value) {
            Ok(s) => {
                println!("{s}");
                ExitCode::SUCCESS
            }
            Err(e) => {
                eprintln!("error: serialize json: {e}");
                ExitCode::from(2)
            }
        }
    } else {
        ExitCode::SUCCESS
    }
}

/// Build an [`OpContext`] from the CLI global flags. The sysmd endpoint
/// is `http://{sysmd_ip}:{sysmd_port}` (a group-0 mgmt URL) and the
/// config is loaded from the configured path (or the default).
///
/// When the config has server entries, their mgmt URLs are added as
/// additional seeds so the client can discover the group-0 leader even
/// if `--sysmd-port` doesn't point at a running server (e.g. after
/// `local-deploy` which allocates dynamic ports).
pub(crate) fn op_context(cli: &Cli) -> Result<OpContext, ExitCode> {
    let config = load_config(cli)?;
    let mgmt_url = format!("http://{}:{}", cli.sysmd_ip, cli.sysmd_port);
    let group0_endpoint = format!("{}:{}", cli.sysmd_ip, cli.sysmd_port);

    // Collect mgmt seeds: the explicit --sysmd-port endpoint plus all
    // server URLs from the config (so local-deploy'd servers are found).
    let mut seeds = vec![mgmt_url];
    for server in &config.servers {
        if !seeds.contains(&server.url) {
            seeds.push(server.url.clone());
        }
    }

    // Use the first config server's RPC URL as the group0 endpoint hint
    // if available (more accurate than the default port). Strip the
    // `http://` prefix since the crowdb-rpc endpoint format is `ip:port`.
    let effective_g0 =
        config
            .servers
            .first()
            .and_then(|s| s.rpc_url.as_ref())
            .map_or(group0_endpoint, |url| {
                url.strip_prefix("http://")
                    .or_else(|| url.strip_prefix("https://"))
                    .unwrap_or(url)
                    .to_string()
            });

    Ok(OpContext::new(effective_g0, seeds, config))
}

/// Load the console config from the CLI's `--config` path or the
/// default location.
fn load_config(cli: &Cli) -> Result<crowdb_console_shared::ConsoleConfig, ExitCode> {
    let path = config_path(cli);
    if !path.exists() {
        return Ok(crowdb_console_shared::ConsoleConfig::default());
    }
    let engine = crowdb_console_shared::TomlFileEngine::new(path);
    engine.load().map_err(|e| {
        eprintln!("error: load config: {e}");
        ExitCode::from(2)
    })
}

/// Resolve the config file path from the CLI's `--config` flag or the
/// default location.
fn config_path(cli: &Cli) -> std::path::PathBuf {
    cli.config
        .clone()
        .or_else(|| {
            std::env::var("CROWDB_CONSOLE_CONFIG")
                .ok()
                .map(std::path::PathBuf::from)
        })
        .unwrap_or_else(|| {
            dirs::config_dir()
                .unwrap_or_else(|| std::path::PathBuf::from("."))
                .join("crowdb-kv")
                .join("console.toml")
        })
}

/// Persist the config from an [`OpContext`] back to the config file.
pub(crate) fn commit_config(cli: &Cli, ctx: &OpContext) -> Result<(), ExitCode> {
    let path = config_path(cli);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| {
            eprintln!("error: create config dir {}: {e}", parent.display());
            ExitCode::from(2)
        })?;
    }
    let engine = crowdb_console_shared::TomlFileEngine::new(path.clone());
    let cfg = ctx.config().clone();
    engine.save(&cfg).map_err(|e| {
        eprintln!("error: save config {}: {e}", path.display());
        ExitCode::from(2)
    })
}
