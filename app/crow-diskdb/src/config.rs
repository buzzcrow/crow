// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Configuration for the diskdb server.
//!
//! Skeleton — fields filled in by follow-up requirements.

use serde::{Deserialize, Serialize};

/// Top-level configuration for a diskdb instance.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DiskdbConfig {
    pub server: ServerConfig,
    pub storage: StorageDefaults,
    pub kv: KvConfig,
    pub sync: SyncConfig,
    pub persistence: PersistenceConfig,
    pub scanner: ScannerConfig,
}

/// gRPC + HTTP listen addresses.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    /// gRPC listen address (e.g. "0.0.0.0:6070").
    pub grpc_listen_addr: String,
    /// HTTP management listen address (e.g. "0.0.0.0:6071").
    pub http_listen_addr: String,
    /// Unique instance ID (default: generated UUID).
    pub instance_id: Option<String>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            grpc_listen_addr: "0.0.0.0:6070".to_string(),
            http_listen_addr: "0.0.0.0:6071".to_string(),
            instance_id: None,
        }
    }
}

/// Storage defaults for zones and blocks.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageDefaults {
    /// Default zone size in bytes (default: 16 GB).
    pub zone_size_bytes: u64,
    /// Allocation granularity in bytes (default: 1 MB). Must be a power of 2.
    pub block_size_bytes: u32,
}

impl Default for StorageDefaults {
    fn default() -> Self {
        Self {
            zone_size_bytes: 16 * 1024 * 1024 * 1024,
            block_size_bytes: 1024 * 1024,
        }
    }
}

/// CROW KV client configuration (group 0 + data groups).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct KvConfig {
    /// Management API seed endpoints for topology discovery.
    pub mgmt_seeds: Vec<String>,
}

/// Group-0 sync / heartbeat configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncConfig {
    /// Sync interval in seconds (default: 13).
    pub interval_secs: u32,
    /// Missed syncs before entering degraded mode (default: 3).
    pub miss_threshold: u32,
    /// Duration in `TempFailure` before transitioning to Offline (default: 900s).
    pub temp_failure_timeout_secs: u32,
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            interval_secs: 13,
            miss_threshold: 3,
            temp_failure_timeout_secs: 900,
        }
    }
}

/// Free batch flush configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistenceConfig {
    /// Free batch flush interval in milliseconds (default: 500).
    pub free_flush_interval_ms: u32,
    /// Free batch max size before forced flush (default: 256).
    pub free_flush_max_batch: u32,
}

impl Default for PersistenceConfig {
    fn default() -> Self {
        Self {
            free_flush_interval_ms: 500,
            free_flush_max_batch: 256,
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
