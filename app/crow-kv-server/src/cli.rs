// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

use clap::Parser;

/// `CrowKV` server — reference implementation wrapping the `crow_kv` library.
#[derive(Parser, Debug)]
#[command(name = "crow-kv-server", about = "CrowKV server daemon")]
pub struct Cli {
    /// HTTP management API listen port. Default: 9910.
    #[arg(long, default_value_t = 9910)]
    pub management_port: u16,

    /// HTTP management API bind address.
    #[arg(long, default_value = "0.0.0.0")]
    pub management_addr: String,

    /// Path to a JSON config file. When provided, loads `CrowKVConfig`
    /// from the file; CLI args override individual fields. When omitted,
    /// uses `CrowKVConfig::default()`.
    #[arg(long)]
    pub config: Option<std::path::PathBuf>,

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

    #[arg(long, default_value = "default", value_parser = ["default", "test", "e2e"])]
    pub election_profile: String,

    /// Root directory for durable per-group crow-tree files. Default: sibling of `wal_root` named `ctdata`.
    #[arg(long)]
    pub data_root: Option<std::path::PathBuf>,

    /// Durable backend for the crow-tree engine. `file` (default) is the
    /// file-based page store (no alignment); `block` opens `data_root`'s
    /// per-group directory with `BlockPageStore` (array-of-blocks, `O_DIRECT`)
    /// for a real SSD/SCM deployment target; `mem-block` uses an in-memory
    /// block device (no alignment, RAM/SCM/PMEM model).
    #[arg(long, default_value = "file", value_parser = ["file", "block", "mem-block"])]
    pub kv_backend: String,

    /// WAL storage backend. `file` (default) uses `tokio::fs` for WAL
    /// segment files; `mem-block` uses an in-memory block device (no
    /// alignment); `block-device` uses an aligned block device model
    /// (SSD/NVMe, 4K I/O unit).
    #[arg(long, default_value = "file", value_parser = ["file", "mem-block", "block-device"])]
    pub wal_backend: String,

    /// Metrics flush interval in seconds. 0 disables metrics logging.
    /// Default: 5.
    #[arg(long, default_value_t = 5)]
    pub metrics_interval: u64,

    /// Skip the durable `fdatasync` on every WAL write batch. Records are
    /// still written to the segment file, but the flush is not durable.
    /// Unsafe for production — only for benchmark path-overhead isolation
    /// (R10 benchmark framework).
    #[arg(long)]
    pub no_fsync: bool,

    /// Max log file size in MiB before rotation. Default: 30.
    #[arg(long, default_value_t = 30)]
    pub log_max_file_mb: usize,

    /// Number of rotated log files to keep. Default: 5.
    #[arg(long, default_value_t = 5)]
    pub log_max_files: usize,

    /// Maximum in-flight (allocated-but-not-chosen) proposals per group.
    /// Each proposal acquires one permit from the admission semaphore;
    /// a full window fails fast with `Busy` instead of queuing. Default: 32.
    #[arg(long, default_value_t = 32)]
    pub max_inflight: usize,

    /// R45 max ops per coalesced batch (capped at 255). The leader
    /// event-batches concurrent single-key proposes into one multi-key
    /// Paxos proposal. `0` disables coalescing (default).
    #[arg(long)]
    pub coalesce_max_keys: Option<usize>,

    /// R45b drain threshold: skip draining the pending batch when
    /// in-flight slot-tasks >= this count. Default `max_inflight / 4`.
    /// `0` = always drain (disables the heuristic).
    #[arg(long)]
    pub coalesce_drain_threshold: Option<usize>,

    /// Instance ID for service-registry keep-alive. If omitted, a
    /// unique ID is generated at startup.
    #[arg(long)]
    pub instance_id: Option<u64>,

    /// Keep-alive heartbeat interval in seconds. 0 disables the
    /// keep-alive loop. Default: 10.
    #[arg(long, default_value_t = 10)]
    pub keepalive_interval: u64,
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
