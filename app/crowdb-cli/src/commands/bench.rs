// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `bench` command handlers — KV workload + RPC bench.

use std::process::ExitCode;

use clap::Subcommand;

use crate::Cli;

#[derive(Subcommand, Debug)]
pub enum BenchVerb {
    #[command(subcommand)]
    Kv(KvBenchWorkload),
    Rpc,
}

#[derive(Subcommand, Debug)]
pub enum KvBenchWorkload {
    Read,
    Write,
    Scan,
    Mix,
}

pub async fn run_bench_verb(_cli: &Cli, verb: BenchVerb) -> ExitCode {
    eprintln!("bench {verb:?} — not yet wired to ops (Phase 3)");
    ExitCode::from(1)
}
