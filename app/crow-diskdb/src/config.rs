// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Configuration for the diskdb server.

use std::net::SocketAddr;

use serde::{Deserialize, Serialize};

/// Top-level configuration for a diskdb instance.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DiskdbConfig {
    pub server: ServerConfig,
    pub(crate) storage: StorageDefaults,
    pub heartbeat: HeartbeatConfig,
    pub(crate) persistence: PersistenceConfig,
    pub(crate) scanner: ScannerConfig,
    pub sync: SyncConfig,
}

/// gRPC + HTTP listen addresses.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    /// gRPC listen address.
    pub listen_addr: String,
    /// HTTP management listen address.
    pub http_listen_addr: String,
    /// Unique instance ID (auto-generated UUID if absent).
    pub instance_id: Option<String>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen_addr: "0.0.0.0:9941".to_string(),
            http_listen_addr: "0.0.0.0:9942".to_string(),
            instance_id: None,
        }
    }
}

/// Storage defaults for zones and blocks.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct StorageDefaults {
    /// Default zone size in bytes (default: 16 GB).
    pub(crate) zone_size_bytes: u64,
    /// Block size in bytes (default: 1 MB; configurable 512 KB–2 MB).
    pub(crate) block_size_bytes: u32,
    /// Allocation granularity in bytes (default: 1 MB; must be power of 2).
    /// v1 enforces `allocate_granularity == block_size_bytes`.
    pub(crate) allocate_granularity: u32,
    /// Number of zones in the disk-level active zone set (default: 4).
    /// The disk round-robins over this many zones at a time; when all
    /// are exhausted, the set rotates to a new batch of zones.
    pub(crate) zone_rotate_count: u32,
}

impl Default for StorageDefaults {
    fn default() -> Self {
        Self {
            zone_size_bytes: 16 * 1024 * 1024 * 1024,
            block_size_bytes: 1024 * 1024,
            allocate_granularity: 1024 * 1024,
            zone_rotate_count: 4,
        }
    }
}

/// Heartbeat / liveness configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatConfig {
    /// Heartbeat interval in seconds (default: 10).
    pub(crate) interval_secs: u32,
    /// Missed heartbeats before entering degraded mode (default: 3).
    pub miss_threshold: u32,
    /// Duration in `TempFailure` before transitioning to `Offline`
    /// (default: 900s).
    pub(crate) temp_failure_timeout_secs: u32,
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
    /// Group-0 store id (default: 0).
    pub(crate) group0_store_id: u64,
    /// Group-0 group id (default: 0).
    pub(crate) group0_group_id: u64,
    /// Sync interval in seconds (default: 10).
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

/// Free batch flush + snapshot compaction configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct PersistenceConfig {
    /// Free batch flush interval in milliseconds (default: 500).
    pub(crate) free_flush_interval_ms: u32,
    /// Free batch max size before forced flush (default: 256).
    pub(crate) free_flush_max_batch: u32,
    /// Snapshot compaction interval in seconds (default: 300).
    pub(crate) snapshot_interval_secs: u32,
    /// Compact when journal entries per zone exceed this (default: 4096).
    pub(crate) snapshot_journal_threshold: u32,
}

impl Default for PersistenceConfig {
    fn default() -> Self {
        Self {
            free_flush_interval_ms: 500,
            free_flush_max_batch: 256,
            snapshot_interval_secs: 300,
            snapshot_journal_threshold: 4096,
        }
    }
}

/// Background scanner configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ScannerConfig {
    /// Scanner run interval in seconds (default: 600).
    pub(crate) scan_interval_secs: u32,
    /// Enable ghost allocation detection (default: true).
    pub(crate) detect_ghost_allocations: bool,
    /// Enable record integrity checks (default: true).
    pub(crate) verify_record_integrity: bool,
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

/// Validate a `DiskdbConfig`.
///
/// # Errors
/// Returns `Err(message)` on the first violation.
pub fn validate(config: &DiskdbConfig) -> Result<(), String> {
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
    if config.persistence.snapshot_interval_secs == 0 {
        return Err("snapshot_interval_secs must be > 0".to_string());
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
    Ok(())
}

fn is_power_of_two(n: u32) -> bool {
    n > 0 && n.is_power_of_two()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_validate_accepts_default() {
        let config = DiskdbConfig::default();
        validate(&config).expect("default config should be valid");
    }

    #[test]
    fn config_validate_rejects_non_power_of_two_block_size() {
        let mut config = DiskdbConfig::default();
        config.storage.block_size_bytes = 700 * 1024;
        assert!(validate(&config).is_err());
    }

    #[test]
    fn config_validate_rejects_block_size_out_of_range() {
        let mut config = DiskdbConfig::default();
        config.storage.block_size_bytes = 256 * 1024;
        assert!(validate(&config).is_err());

        let mut config = DiskdbConfig::default();
        config.storage.block_size_bytes = 4 * 1024 * 1024;
        assert!(validate(&config).is_err());
    }

    #[test]
    fn config_validate_rejects_zone_not_multiple_of_block() {
        let mut config = DiskdbConfig::default();
        config.storage.zone_size_bytes = 16 * 1024 * 1024 * 1024 + 1;
        assert!(validate(&config).is_err());
    }

    #[test]
    fn config_validate_rejects_granularity_not_equal_to_block() {
        let mut config = DiskdbConfig::default();
        config.storage.allocate_granularity = 512 * 1024;
        config.storage.block_size_bytes = 1024 * 1024;
        assert!(validate(&config).is_err());
    }

    #[test]
    fn config_validate_rejects_bad_listen_addr() {
        let mut config = DiskdbConfig::default();
        config.server.listen_addr = "not-an-addr".to_string();
        assert!(validate(&config).is_err());
    }

    #[test]
    fn config_validate_rejects_zero_sync_interval() {
        let mut config = DiskdbConfig::default();
        config.sync.sync_interval_secs = 0;
        assert!(validate(&config).is_err());
    }

    #[test]
    fn config_validate_rejects_zero_zone_rotate_count() {
        let mut config = DiskdbConfig::default();
        config.storage.zone_rotate_count = 0;
        assert!(validate(&config).is_err());
    }
}
