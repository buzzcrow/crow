// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! KV-cluster logical plane: store/group/replica orchestration.
//!
//! Mirrors the web handlers' fan-out + rollback logic, but calls the
//! kv-server mgmt endpoints directly and reads topology from group-0
//! sysdata instead of the monitor cache.

use crate::error::Result;
use crate::ops::OpContext;

/// Create an empty store across the listed nodes.
///
/// # Errors
/// Returns an error if any node is unreachable or the store creation fails.
pub async fn add_store(_ctx: &OpContext, store_id: u64, nodes: &[u64]) -> Result<()> {
    let _ = (store_id, nodes);
    todo!("Phase 1c: implement add_store")
}
