// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Config validation.

use std::net::SocketAddr;

use super::DiskdbConfig;

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
