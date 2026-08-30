// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! KV data-plane operations: put, get, delete, scan, snapshot.

use crate::error::Result;
use crate::ops::OpContext;

/// Put a key-value pair into a store/group.
///
/// # Errors
/// Returns an error if the leader is unreachable or the put fails.
pub async fn put(_ctx: &OpContext, store_id: u64, group_id: u64, key: &[u8], value: &[u8]) -> Result<()> {
    let _ = (store_id, group_id, key, value);
    todo!("Phase 1e: implement put")
}
