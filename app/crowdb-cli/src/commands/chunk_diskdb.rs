// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `chunk diskdb` command handlers — diskdb lifecycle + maintenance.

use std::process::ExitCode;

use clap::Subcommand;

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
    List,
    Usage,
    ScanStatus,
    Scan,
    Recalc,
    Compact,
    Rebuild,
}

pub async fn run_chunk_diskdb_verb(_cli: &Cli, verb: ChunkDiskdbVerb) -> ExitCode {
    eprintln!("chunk diskdb {verb:?} — not yet implemented (Phase 3)");
    ExitCode::from(1)
}
