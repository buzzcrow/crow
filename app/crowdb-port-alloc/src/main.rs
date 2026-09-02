// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `crowdb-port-alloc` — CLI wrapper around the `port_alloc` library.
//!
//! The single place that picks ports for tests, shell scripts, and
//! the console UI E2E fixture. Uses a flock-coordinated claim file
//! with bind probes. No port 0 is ever used.
//!
//! Usage:
//!   crowdb-port-alloc --service kv-mgmt --instance 0
//!   crowdb-port-alloc --service kv-listen --instance 0 --count 3
//!   crowdb-port-alloc --service diskdb-rpc --instance 1 --offset 100
//!   crowdb-port-alloc --reset
//!   crowdb-port-alloc --mark-failed 10100

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, ValueEnum};
use crowdb_protocol::port_alloc::{self, PortAllocConfig};
use crowdb_protocol::ServicePort;

#[derive(Debug, Clone, ValueEnum)]
enum ServiceArg {
    #[value(name = "kv-mgmt")]
    KvMgmt,
    #[value(name = "kv-listen")]
    KvListen,
    #[value(name = "diskdb-listen")]
    DiskdbListen,
    #[value(name = "diskdb-http")]
    DiskdbHttp,
    #[value(name = "diskdb-rpc")]
    DiskdbRpc,
    #[value(name = "chunkdb-listen")]
    ChunkdbListen,
    #[value(name = "chunkdb-http")]
    ChunkdbHttp,
    #[value(name = "chunkdb-rpc")]
    ChunkdbRpc,
    #[value(name = "diskio-rpc")]
    DiskioRpc,
    #[value(name = "web")]
    Web,
}

impl From<ServiceArg> for ServicePort {
    fn from(arg: ServiceArg) -> Self {
        match arg {
            ServiceArg::KvMgmt => Self::KvServerMgmt,
            ServiceArg::KvListen => Self::KvServerListen,
            ServiceArg::DiskdbListen => Self::DiskdbListen,
            ServiceArg::DiskdbHttp => Self::DiskdbHttp,
            ServiceArg::DiskdbRpc => Self::DiskdbRpc,
            ServiceArg::ChunkdbListen => Self::ChunkdbListen,
            ServiceArg::ChunkdbHttp => Self::ChunkdbHttp,
            ServiceArg::ChunkdbRpc => Self::ChunkdbRpc,
            ServiceArg::DiskioRpc => Self::DiskioRpc,
            ServiceArg::Web => Self::Web,
        }
    }
}

#[derive(Parser)]
#[command(
    name = "crowdb-port-alloc",
    about = "Port allocator for CROWDB tests and cluster bootstrap"
)]
struct Cli {
    /// Workspace root directory (claim file lives under
    /// `<root>/.crowdb-port-alloc/claims`). Default: current directory.
    #[arg(long)]
    root: Option<PathBuf>,

    /// Port offset for multi-session isolation. Default: 0.
    #[arg(long, default_value_t = 0)]
    offset: u16,

    /// Service type (e.g. "kv-mgmt", "kv-listen", "diskdb-rpc").
    #[arg(long)]
    service: Option<ServiceArg>,

    /// Instance index (0-based). Default: 0.
    #[arg(long, default_value_t = 0)]
    instance: u16,

    /// Number of consecutive ports to allocate. Default: 1.
    #[arg(long, default_value_t = 1)]
    count: u16,

    /// Delete the claim file and exit.
    #[arg(long)]
    reset: bool,

    /// Mark a port as tried-and-failed (skip on next probe).
    #[arg(long, value_name = "PORT")]
    mark_failed: Option<u16>,
}

fn main() -> ExitCode {
    let cli = Cli::parse();

    let cfg = match cli.root {
        Some(ref root) => PortAllocConfig::new(root).with_offset(cli.offset),
        None => PortAllocConfig::default().with_offset(cli.offset),
    };

    if cli.reset {
        match port_alloc::reset_claims(&cfg) {
            Ok(()) => return ExitCode::SUCCESS,
            Err(e) => {
                eprintln!("error: {e}");
                return ExitCode::FAILURE;
            }
        }
    }

    if let Some(port) = cli.mark_failed {
        match port_alloc::mark_failed(port, &cfg) {
            Ok(()) => return ExitCode::SUCCESS,
            Err(e) => {
                eprintln!("error: {e}");
                return ExitCode::FAILURE;
            }
        }
    }

    let service = if let Some(s) = cli.service {
        s.into()
    } else {
        eprintln!("error: --service is required (unless --reset or --mark-failed)");
        return ExitCode::FAILURE;
    };

    if cli.count <= 1 {
        match port_alloc::alloc_port(service, cli.instance, &cfg) {
            Ok(port) => {
                println!("{port}");
                ExitCode::SUCCESS
            }
            Err(e) => {
                eprintln!("error: {e}");
                ExitCode::FAILURE
            }
        }
    } else {
        match port_alloc::alloc_port_range(service, cli.instance, cli.count, &cfg) {
            Ok(ports) => {
                for port in ports {
                    println!("{port}");
                }
                ExitCode::SUCCESS
            }
            Err(e) => {
                eprintln!("error: {e}");
                ExitCode::FAILURE
            }
        }
    }
}
