// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `chunk chunkdb/diskio` + data-plane stub command handlers.

use std::process::ExitCode;

use clap::Subcommand;

use crate::Cli;

#[derive(Subcommand, Debug)]
pub enum ChunkStubVerb {
    #[command(subcommand)]
    Chunkdb(ChunkdbVerb),
    #[command(subcommand)]
    Diskio(DiskioVerb),
    Allocate,
    Free,
    Write,
    Read,
    Gc,
}

#[derive(Subcommand, Debug)]
pub enum ChunkdbVerb {
    Deploy {
        #[arg(short = 'n', long)]
        node: String,
    },
    List,
}

#[derive(Subcommand, Debug)]
pub enum DiskioVerb {
    Deploy {
        #[arg(short = 'n', long)]
        node: String,
    },
    List,
}

pub async fn run_chunk_stub_verb(_cli: &Cli, verb: ChunkStubVerb) -> ExitCode {
    eprintln!("chunk {verb:?} — not yet implemented (Phase 3)");
    ExitCode::from(1)
}
