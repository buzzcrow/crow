// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `chunk diskdb` command handlers — diskdb lifecycle + maintenance.

use std::process::ExitCode;

use clap::Subcommand;

use crowdb_console_shared::ops::chunk;

use crate::commands::op_context;
use crate::Cli;

#[derive(Subcommand, Debug)]
pub enum ChunkDiskdbVerb {
    Deploy {
        #[arg(short = 'n', long)]
        node: String,
    },
    Restart {
        #[arg(short = 'n', long)]
        node: String,
    },
    Stop {
        #[arg(short = 'n', long)]
        node: String,
    },
    Delete {
        #[arg(short = 'n', long)]
        node: String,
    },
    /// List living diskdb instances discovered from the group-0
    /// service registry. Pass `--endpoint` to bypass discovery and
    /// query a specific diskdb directly.
    List {
        /// Explicit diskdb RPC endpoint (e.g. `127.0.0.1:11000`).
        /// When set, discovery is bypassed.
        #[arg(long)]
        endpoint: Option<String>,
    },
    Usage,
    ScanStatus,
    Scan,
    Recalc,
    Compact,
    Rebuild,
}

pub async fn run_chunk_diskdb_verb(cli: &Cli, verb: ChunkDiskdbVerb) -> ExitCode {
    match verb {
        ChunkDiskdbVerb::List { endpoint } => run_list(cli, endpoint.as_deref()).await,
        other => {
            eprintln!("chunk diskdb {other:?} — not yet implemented (Phase 3)");
            ExitCode::from(1)
        }
    }
}

async fn run_list(cli: &Cli, explicit_endpoint: Option<&str>) -> ExitCode {
    let ctx = match op_context(cli) {
        Ok(ctx) => ctx,
        Err(code) => return code,
    };

    // If an explicit endpoint is given, bypass discovery and just
    // print it (the caller asked for that specific instance).
    if let Some(ep) = explicit_endpoint {
        println!("diskdb endpoint (explicit): {ep}");
        return ExitCode::SUCCESS;
    }

    // Discover all living diskdb instances from group-0.
    let instances = match chunk::discover_all_diskdb_endpoints(&ctx).await {
        Ok(v) => v,
        Err(e) => {
            eprintln!("error: discover diskdb instances: {e}");
            return ExitCode::from(1);
        }
    };

    if instances.is_empty() {
        eprintln!("no living diskdb instances — register one with crowdb-web or start crowdb-diskdb");
        return ExitCode::from(1);
    }

    if cli.json {
        let json: Vec<serde_json::Value> = instances
            .iter()
            .map(|(id, ep)| {
                serde_json::json!({
                    "instance_id": id,
                    "rpc_endpoint": ep,
                })
            })
            .collect();
        println!("{}", serde_json::to_string_pretty(&json).unwrap_or_default());
    } else {
        println!("living diskdb instances ({}):", instances.len());
        for (id, ep) in &instances {
            println!("  instance {id}: {ep}");
        }
    }

    ExitCode::SUCCESS
}
