// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

use clap::Parser;

/// `CrowKV` server — reference implementation wrapping the crowkv library.
#[derive(Parser, Debug)]
#[command(name = "crowkv-server", about = "CrowKV server daemon")]
pub struct Cli {
    /// HTTP management API listen port. Default: 9910.
    #[arg(long, default_value_t = 9910)]
    pub management_port: u16,

    /// HTTP management API bind address.
    #[arg(long, default_value = "0.0.0.0")]
    pub management_addr: String,

    /// Port pool for gRPC `PxKvStore` listeners (comma/range format, e.g. "28001,28002,28010..28020").
    #[arg(long)]
    pub ports: Option<String>,

    /// Store ID list (comma/range format). When omitted, the server
    /// starts with no stores; the operator (or the console) creates
    /// stores explicitly via the management API. Pass `--stores 1
    /// --groups 1` to keep the legacy auto-bootstrap behavior.
    #[arg(long)]
    pub stores: Option<String>,

    /// Group ID list (comma/range format). Required when `--stores` is
    /// set; ignored otherwise.
    #[arg(long)]
    pub groups: Option<String>,

    /// Local replica ID (single value, range \[1, 128\]). Default: 1.
    #[arg(long, default_value_t = 1)]
    pub replica: u64,

    /// Also print logs to console (in addition to file logging).
    #[arg(short = 'l', long)]
    pub log: bool,

    #[arg(long)]
    pub wal_root: Option<std::path::PathBuf>,

    /// Root directory for group config files. Default: sibling of `wal_root` named `conf`.
    #[arg(long)]
    pub config_root: Option<std::path::PathBuf>,

    #[arg(long, default_value = "default", value_parser = ["default", "test"])]
    pub election_profile: String,

    /// KV storage engine backing each group's learner. `crowtree` (default)
    /// durably persists each group's state to its
    /// own file under `--data-root` (recovered by replaying the WAL through
    /// it on every restart — see `PxLocalReplica::restore_from_replay_with_engine`);
    /// `memory` is the in-memory, non-durable `InMemKV`, kept available as
    /// the explicit low-durability/test/dev choice.
    #[arg(long, default_value = "crowtree", value_parser = ["memory", "crowtree"])]
    pub kv_engine: String,

    /// Root directory for durable per-group crowtree files (only used when
    /// `--kv-engine crowtree`). Default: sibling of `wal_root` named `ctdata`.
    #[arg(long)]
    pub data_root: Option<std::path::PathBuf>,

    /// Durable backend for the crowtree engine (only used when `--kv-engine
    /// crowtree`). `file` (default) is buffered file I/O; `block` opens
    /// `data_root`'s per-group file with `O_DIRECT` via `BlockPageStore`
    /// for a real SSD/SCM deployment target.
    #[arg(long, default_value = "file", value_parser = ["file", "block"])]
    pub kv_backend: String,
}

/// Parse a comma-separated list of numbers and ranges into a `Vec<u64>`.
///
/// Supported formats:
/// - Single value: `28`
/// - Inclusive range: `40..50` (produces 40..50)
/// - Mixed: `28,39,40..50,59`
///
/// Duplicates are silently deduplicated. Order is preserved (first occurrence wins).
///
/// # Errors
///
/// Returns an error if:
/// - The input string is empty
/// - Any part cannot be parsed as a number
/// - A range has start >= end
pub fn parse_id_list(input: &str) -> Result<Vec<u64>, String> {
    let mut seen = std::collections::HashSet::new();
    let mut result = Vec::new();

    for part in input.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }

        if let Some((start_s, end_s)) = part.split_once("..") {
            let start: u64 = start_s
                .trim()
                .parse()
                .map_err(|_| format!("invalid range start: '{start_s}'"))?;
            let end: u64 = end_s
                .trim()
                .parse()
                .map_err(|_| format!("invalid range end: '{end_s}'"))?;
            if start > end {
                return Err(format!("range start must be <= end: '{part}'"));
            }
            for v in start..=end {
                if seen.insert(v) {
                    result.push(v);
                }
            }
        } else {
            let v: u64 = part.parse().map_err(|_| format!("invalid number: '{part}'"))?;
            if seen.insert(v) {
                result.push(v);
            }
        }
    }

    if result.is_empty() {
        return Err("empty list".to_string());
    }

    Ok(result)
}

/// Parse port list — same format as `parse_id_list` but validates u16 range.
///
/// # Errors
///
/// Returns an error if:
/// - The input cannot be parsed by `parse_id_list`
/// - Any value is outside the u16 range (0-65535)
pub fn parse_port_list(input: &str) -> Result<Vec<u16>, String> {
    let ids = parse_id_list(input)?;
    ids.into_iter()
        .map(|v| u16::try_from(v).map_err(|_| format!("port out of range (0-65535): {v}")))
        .collect()
}
