// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! `crow-diskdb` entry point.
//!
//! Skeleton — wiring filled in by follow-up requirements.

use clap::Parser;
use tracing::info;

use crow_diskdb::DiskdbConfig;

/// CROW diskdb server CLI.
#[derive(Parser, Debug)]
#[command(name = "crow-diskdb", about = "CROW distributed disk-block allocator")]
struct Cli {
    /// Config file path (JSON).
    #[arg(long)]
    config: Option<String>,

    /// gRPC listen address (overrides config).
    #[arg(long)]
    grpc_addr: Option<String>,

    /// HTTP management listen address (overrides config).
    #[arg(long)]
    http_addr: Option<String>,
}

#[tokio::main]
async fn main() {
    let args = Cli::parse();

    tracing_subscriber::fmt().init();

    let config = load_config(&args);
    info!(?config, "crow-diskdb starting (skeleton)");

    // TODO(R70+): wire up crow-kv-client, group-0 sync, gRPC server,
    // HTTP mgmt API, background loops.
    info!("skeleton ready — no services started yet");
}

fn load_config(args: &Cli) -> DiskdbConfig {
    let mut config = if let Some(path) = &args.config {
        let data = std::fs::read_to_string(path).unwrap_or_else(|e| {
            panic!("failed to read config file {path}: {e}");
        });
        serde_json::from_str(&data).unwrap_or_else(|e| {
            panic!("failed to parse config file {path}: {e}");
        })
    } else {
        DiskdbConfig::default()
    };

    if let Some(addr) = &args.grpc_addr {
        config.server.grpc_listen_addr.clone_from(addr);
    }
    if let Some(addr) = &args.http_addr {
        config.server.http_listen_addr.clone_from(addr);
    }

    config
}
