// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Configuration for the diskdb server.

use std::net::SocketAddr;
use std::time::Duration;

use crow_common::config::BaseConfig;
use crow_protocol::{DISKDB_GRPC_BASE, DISKDB_HTTP_BASE, KV_SERVER_MGMT_BASE};
use serde::{Deserialize, Serialize};

/// Top-level configuration for a diskdb instance.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DdbConfig {
    pub server: ServerConfig,
    pub storage: StorageDefaults,
    pub heartbeat: HeartbeatConfig,
    pub persistence: PersistenceConfig,
    pub scanner: ScannerConfig,
    pub sync: SyncConfig,
    pub reporting: ReportingConfig,
}

impl BaseConfig for DdbConfig {
    fn validate(&self) -> Result<(), String> {
        validate(self)
    }
}

/// gRPC + HTTP listen addresses.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    /// static: gRPC listen address.
    pub listen_addr: String,
    /// static: HTTP management listen address.
    pub http_listen_addr: String,
    /// static: unique instance ID (auto-generated UUID if absent).
    pub instance_id: Option<String>,
    /// static: HTTP management-API seed endpoints (`http://host:port`)
    /// of the crow-kv-server(s) used to discover the system group
    /// (store 0, group 0) leader and, via `/topology`, the data-group
    /// leaders. The client refreshes this lazily on first use, so the
    /// leader does not need to be pre-seeded. At least one must be
    /// reachable for the diskdb to sync group 0.
    pub kv_server_mgmt_seeds: Vec<String>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen_addr: format!("0.0.0.0:{DISKDB_GRPC_BASE}"),
            http_listen_addr: format!("0.0.0.0:{DISKDB_HTTP_BASE}"),
            instance_id: None,
            kv_server_mgmt_seeds: vec![format!("http://127.0.0.1:{KV_SERVER_MGMT_BASE}")],
        }
    }
}

/// Storage defaults for zones and blocks.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageDefaults {
    /// static: default zone size in bytes (default: 16 GB).
    pub zone_size_bytes: u64,
    /// static: block size in bytes (default: 1 MB; configurable 512 KB–2 MB).
    pub block_size_bytes: u32,
    /// static: allocation granularity in bytes (default: 1 MB; must be
    /// power of 2). v1 enforces `allocate_granularity == block_size_bytes`.
    pub allocate_granularity: u32,
    /// static: number of zones in the disk-level active zone set
    /// (default: 4). The disk round-robins over this many zones at a
    /// time; when all are exhausted, the set rotates to a new batch.
    pub zone_rotate_count: u32,
    /// dynamic: per-bit CAS retry cap in the zone bitmap-scan allocator
    /// (default: 100). On exhaustion, the allocator falls through to
    /// the next bit / word / zone.
    pub cas_retry_limit: u32,
    /// dynamic: strict ownership validation before free (default:
    /// false). When true, the free path reads the `BusyBlockValue`
    /// from the data group first and validates `owner_chunk` (one
    /// extra paxos round-trip, doubles free latency).
    pub validate_owner_on_free: bool,
}

impl Default for StorageDefaults {
    fn default() -> Self {
        Self {
            zone_size_bytes: 16 * 1024 * 1024 * 1024,
            block_size_bytes: 1024 * 1024,
            allocate_granularity: 1024 * 1024,
            zone_rotate_count: 4,
            cas_retry_limit: 100,
            validate_owner_on_free: false,
        }
    }
}

/// Heartbeat / liveness configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatConfig {
    /// dynamic: heartbeat interval in seconds (default: 10).
    pub interval_secs: u32,
    /// dynamic: missed heartbeats before entering degraded mode (default: 3).
    pub miss_threshold: u32,
    /// dynamic: duration in `TempFailure` before transitioning to
    /// `Offline` (default: 900s).
    pub temp_failure_timeout_secs: u32,
}

impl Default for HeartbeatConfig {
    fn default() -> Self {
        Self {
            interval_secs: 10,
            miss_threshold: 3,
            temp_failure_timeout_secs: 900,
        }
    }
}

/// Group-0 sync configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncConfig {
    /// static: group-0 store id (default: 0).
    pub group0_store_id: u64,
    /// static: group-0 group id (default: 0).
    pub group0_group_id: u64,
    /// dynamic: sync interval in seconds (default: 10).
    pub sync_interval_secs: u32,
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            group0_store_id: 0,
            group0_group_id: 0,
            sync_interval_secs: 10,
        }
    }
}

/// Reporting loop configuration (R74 §7).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReportingConfig {
    /// dynamic: reporting loop interval in seconds (default: 10).
    pub interval_secs: u32,
}

impl Default for ReportingConfig {
    fn default() -> Self {
        Self { interval_secs: 10 }
    }
}

/// Free batch flush + snapshot compaction + recovery configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistenceConfig {
    /// dynamic: free batching toggle (default: false). When false,
    /// frees are immediate (one `batch_write` per free). When true,
    /// frees are grouped and flushed via one `batch_write` when the
    /// batch reaches `free_flush_max_batch` (R79; no timer).
    pub free_batch_enabled: bool,
    /// dynamic: free batch max size before forced flush (default: 256).
    pub free_flush_max_batch: u32,
    /// dynamic: periodic compaction interval in seconds (default: 300).
    /// The compaction loop sleeps this long between cycles.
    pub compaction_cadence_secs: u32,
    /// dynamic: compact a zone when its
    /// `uncompacted_free_record_count` exceeds this (default: 4096).
    /// Cadence OR threshold — whichever fires first for a given zone.
    pub snapshot_compaction_threshold: u32,
    /// static: max concurrent zone recoveries in `recover_node`
    /// (default: 16).
    pub recovery_concurrency: usize,
}

impl Default for PersistenceConfig {
    fn default() -> Self {
        Self {
            free_batch_enabled: false,
            free_flush_max_batch: 256,
            compaction_cadence_secs: 300,
            snapshot_compaction_threshold: 4096,
            recovery_concurrency: 16,
        }
    }
}

/// Background scanner configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScannerConfig {
    /// dynamic: scanner run interval in seconds (default: 600).
    pub scan_interval_secs: u32,
    /// dynamic: enable ghost allocation detection (default: true).
    pub detect_ghost_allocations: bool,
    /// dynamic: enable record integrity checks (default: true).
    pub verify_record_integrity: bool,
}

impl Default for ScannerConfig {
    fn default() -> Self {
        Self {
            scan_interval_secs: 600,
            detect_ghost_allocations: true,
            verify_record_integrity: true,
        }
    }
}

// ── Validation ──────────────────────────────────────────────────

/// Keep-alive loop configuration.
#[derive(Debug, Clone)]
pub struct KeepAliveConfig {
    pub interval: Duration,
    pub miss_threshold: u32,
    pub zone_rotate_count: u32,
    pub cas_retry_limit: u32,
    pub temp_failure_timeout_secs: u32,
}

impl Default for KeepAliveConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(10),
            miss_threshold: 3,
            zone_rotate_count: 4,
            cas_retry_limit: 100,
            temp_failure_timeout_secs: 900,
        }
    }
}

/// Compaction configuration.
#[derive(Debug, Clone)]
pub struct CompactionConfig {
    /// Periodic compaction interval.
    pub compaction_cadence: Duration,
    /// Free-record count per zone that triggers compaction (in
    /// addition to the periodic cadence). Whichever fires first.
    pub snapshot_compaction_threshold: u32,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            compaction_cadence: Duration::from_secs(300),
            snapshot_compaction_threshold: 4096,
        }
    }
}

// ── Validation ──────────────────────────────────────────────────

/// Validate a `DdbConfig`.
///
/// # Errors
/// Returns `Err(message)` on the first violation.
pub fn validate(config: &DdbConfig) -> Result<(), String> {
    let block = config.storage.block_size_bytes;
    let min_block: u32 = 512 * 1024;
    let max_block: u32 = 2 * 1024 * 1024;
    if block < min_block || block > max_block {
        return Err(format!("block_size_bytes {block} out of range [512 KB, 2 MB]"));
    }
    if !is_power_of_two(block) {
        return Err(format!("block_size_bytes {block} is not a power of 2"));
    }
    if config.storage.zone_size_bytes == 0 {
        return Err("zone_size_bytes must be > 0".to_string());
    }
    if config.storage.zone_size_bytes % u64::from(block) != 0 {
        return Err(format!(
            "zone_size_bytes {} is not a multiple of block_size_bytes {block}",
            config.storage.zone_size_bytes,
        ));
    }
    if config.storage.allocate_granularity != block {
        return Err(format!(
            "allocate_granularity {} must equal block_size_bytes {block} (v1)",
            config.storage.allocate_granularity,
        ));
    }
    if config.persistence.free_flush_max_batch == 0 {
        return Err("free_flush_max_batch must be > 0".to_string());
    }
    if config.storage.zone_rotate_count == 0 {
        return Err("zone_rotate_count must be > 0".to_string());
    }
    if config.storage.cas_retry_limit == 0 {
        return Err("cas_retry_limit must be > 0".to_string());
    }
    if config.persistence.compaction_cadence_secs == 0 {
        return Err("compaction_cadence_secs must be > 0".to_string());
    }
    if config.persistence.recovery_concurrency == 0 {
        return Err("recovery_concurrency must be > 0".to_string());
    }
    if config.server.listen_addr.parse::<SocketAddr>().is_err() {
        return Err(format!(
            "listen_addr {:?} is not a valid SocketAddr",
            config.server.listen_addr,
        ));
    }
    if config.server.http_listen_addr.parse::<SocketAddr>().is_err() {
        return Err(format!(
            "http_listen_addr {:?} is not a valid SocketAddr",
            config.server.http_listen_addr,
        ));
    }
    if config.sync.sync_interval_secs == 0 {
        return Err("sync.sync_interval_secs must be > 0".to_string());
    }
    if config.heartbeat.interval_secs == 0 {
        return Err("heartbeat.interval_secs must be > 0".to_string());
    }
    if config.heartbeat.miss_threshold == 0 {
        return Err("heartbeat.miss_threshold must be > 0".to_string());
    }
    if config.reporting.interval_secs == 0 {
        return Err("reporting.interval_secs must be > 0".to_string());
    }
    Ok(())
}

fn is_power_of_two(n: u32) -> bool {
    n > 0 && n.is_power_of_two()
}
