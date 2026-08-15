// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Configuration for the chunkdb server.

use crow_common::config::BaseConfig;
use crow_protocol::{CHUNKDB_GRPC_BASE, CHUNKDB_HTTP_BASE, KV_SERVER_MGMT_BASE};
use serde::{Deserialize, Serialize};

/// Top-level configuration for a chunkdb instance.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ChunkdbConfig {
    pub server: ServerConfig,
    pub topology: TopologyConfig,
    #[serde(default)]
    pub range_guard: RangeGuardConfig,
}

impl BaseConfig for ChunkdbConfig {
    fn validate(&self) -> Result<(), String> {
        if self.server.kv_server_mgmt_seeds.is_empty() {
            return Err("server.kv_server_mgmt_seeds must not be empty".into());
        }
        if self.topology.refresh_interval_secs == 0 {
            return Err("topology.refresh_interval_secs must be > 0".into());
        }
        Ok(())
    }
}

/// Range guard configuration (R99 sharded mode).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RangeGuardConfig {
    /// When `true` (default), an empty range guard allows all
    /// requests — preserving v1 single-instance behavior before the
    /// binding table is loaded. When `false`, an empty guard rejects
    /// all mutating requests until the binding table is loaded.
    pub allow_all_when_empty: bool,
}

impl Default for RangeGuardConfig {
    fn default() -> Self {
        Self {
            allow_all_when_empty: true,
        }
    }
}

/// gRPC + HTTP listen addresses.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    pub listen_addr: String,
    pub http_listen_addr: String,
    pub instance_id: Option<String>,
    pub kv_server_mgmt_seeds: Vec<String>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen_addr: format!("0.0.0.0:{CHUNKDB_GRPC_BASE}"),
            http_listen_addr: format!("0.0.0.0:{CHUNKDB_HTTP_BASE}"),
            instance_id: None,
            kv_server_mgmt_seeds: vec![format!("http://127.0.0.1:{KV_SERVER_MGMT_BASE}")],
        }
    }
}

/// Topology cache refresh configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopologyConfig {
    pub refresh_interval_secs: u32,
}

impl Default for TopologyConfig {
    fn default() -> Self {
        Self {
            refresh_interval_secs: 30,
        }
    }
}
