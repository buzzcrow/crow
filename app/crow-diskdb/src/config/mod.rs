// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Configuration for the diskdb server.

pub mod validation;

pub use validation::validate;

use serde::{Deserialize, Serialize};

/// Top-level configuration for a diskdb instance.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DiskdbConfig {
    pub server: ServerConfig,
    pub storage: StorageDefaults,
    pub heartbeat: HeartbeatConfig,
    pub persistence: PersistenceConfig,
    pub scanner: ScannerConfig,
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
pub struct StorageDefaults {
    /// Default zone size in bytes (default: 16 GB).
    pub zone_size_bytes: u64,
    /// Block size in bytes (default: 1 MB; configurable 512 KB–2 MB).
    pub block_size_bytes: u32,
    /// Allocation granularity in bytes (default: 1 MB; must be power of 2).
    /// v1 enforces `allocate_granularity == block_size_bytes`.
    pub allocate_granularity: u32,
}

impl Default for StorageDefaults {
    fn default() -> Self {
        Self {
            zone_size_bytes: 16 * 1024 * 1024 * 1024,
            block_size_bytes: 1024 * 1024,
            allocate_granularity: 1024 * 1024,
        }
    }
}

/// Heartbeat / liveness configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatConfig {
    /// Heartbeat interval in seconds (default: 13).
    pub interval_secs: u32,
    /// Missed heartbeats before entering degraded mode (default: 3).
    pub miss_threshold: u32,
    /// Duration in `TempFailure` before transitioning to `Offline`
    /// (default: 900s).
    pub temp_failure_timeout_secs: u32,
}

impl Default for HeartbeatConfig {
    fn default() -> Self {
        Self {
            interval_secs: 13,
            miss_threshold: 3,
            temp_failure_timeout_secs: 900,
        }
    }
}

/// Group-0 sync configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncConfig {
    /// Group-0 store id (default: 0).
    pub group0_store_id: u64,
    /// Group-0 group id (default: 0).
    pub group0_group_id: u64,
    /// Sync interval in seconds (default: 13).
    pub sync_interval_secs: u32,
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            group0_store_id: 0,
            group0_group_id: 0,
            sync_interval_secs: 13,
        }
    }
}

/// Free batch flush + snapshot compaction configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistenceConfig {
    /// Free batch flush interval in milliseconds (default: 500).
    pub free_flush_interval_ms: u32,
    /// Free batch max size before forced flush (default: 256).
    pub free_flush_max_batch: u32,
    /// Snapshot compaction interval in seconds (default: 300).
    pub snapshot_interval_secs: u32,
    /// Compact when journal entries per zone exceed this (default: 4096).
    pub snapshot_journal_threshold: u32,
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
pub struct ScannerConfig {
    /// Scanner run interval in seconds (default: 600).
    pub scan_interval_secs: u32,
    /// Enable ghost allocation detection (default: true).
    pub detect_ghost_allocations: bool,
    /// Enable record integrity checks (default: true).
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
