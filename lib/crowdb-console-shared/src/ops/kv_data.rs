// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! KV data-plane operations: put, get, delete, scan, snapshot.
//!
//! Thin wrappers around [`CrowdbKvClient`] that resolve the leader from
//! the topology cache and translate the client's `Error` into the
//! console's `Error`.

use crowdb_kv_client::{
    CreateSnapshotResponse, GetOutcome, ReadMode, ReleaseSnapshotResponse, ScanOutcome, SnapshotInfo,
    SnapshotScanResponse, WriteOutcome,
};

use crate::error::Result;
use crate::ops::OpContext;

/// Put a key-value pair into a store/group.
///
/// # Errors
/// Returns an error if the leader is unreachable or the put fails.
pub async fn put(
    ctx: &OpContext,
    store_id: u64,
    group_id: u64,
    key: &[u8],
    value: &[u8],
) -> Result<WriteOutcome> {
    Ok(ctx.kv().put(store_id, group_id, key, value, None).await?)
}

/// Get a key from a store/group.
///
/// # Errors
/// Returns an error if the leader is unreachable or the get fails.
pub async fn get(ctx: &OpContext, store_id: u64, group_id: u64, key: &[u8]) -> Result<GetOutcome> {
    Ok(ctx
        .kv()
        .get(store_id, group_id, key, ReadMode::Linearizable, None)
        .await?)
}

/// Delete a key from a store/group.
///
/// # Errors
/// Returns an error if the leader is unreachable or the delete fails.
pub async fn delete(ctx: &OpContext, store_id: u64, group_id: u64, key: &[u8]) -> Result<()> {
    ctx.kv().delete(store_id, group_id, key, None).await?;
    Ok(())
}

/// Scan keys with a prefix in a store/group.
///
/// # Errors
/// Returns an error if the leader is unreachable or the scan fails.
pub async fn scan(
    ctx: &OpContext,
    store_id: u64,
    group_id: u64,
    prefix: &[u8],
    start_after: &[u8],
    limit: u32,
) -> Result<ScanOutcome> {
    Ok(ctx
        .kv()
        .scan(
            store_id,
            group_id,
            prefix,
            start_after,
            &[],
            limit,
            ReadMode::Linearizable,
            None,
            false,
            None,
        )
        .await?)
}

/// Create a point-in-time snapshot of a group.
///
/// # Errors
/// Returns an error if the leader is unreachable or snapshot creation fails.
pub async fn create_snapshot(
    ctx: &OpContext,
    store_id: u64,
    group_id: u64,
) -> Result<CreateSnapshotResponse> {
    Ok(ctx
        .kv()
        .create_snapshot(store_id, group_id, ReadMode::Linearizable, None)
        .await?)
}

/// List active snapshots for a group.
///
/// # Errors
/// Returns an error if the leader is unreachable or the list fails.
pub async fn list_snapshots(ctx: &OpContext, store_id: u64, group_id: u64) -> Result<Vec<SnapshotInfo>> {
    Ok(ctx.kv().list_snapshots(store_id, group_id).await?)
}

/// Scan a pinned snapshot with prefix/pagination.
///
/// # Errors
/// Returns an error if the leader is unreachable or the scan fails.
pub async fn scan_snapshot(
    ctx: &OpContext,
    store_id: u64,
    group_id: u64,
    snapshot_handle: u64,
    prefix: &[u8],
    start_after: &[u8],
    limit: u32,
) -> Result<SnapshotScanResponse> {
    Ok(ctx
        .kv()
        .snapshot_scan(store_id, group_id, snapshot_handle, prefix, start_after, limit)
        .await?)
}

/// Release a snapshot handle.
///
/// # Errors
/// Returns an error if the leader is unreachable or the release fails.
pub async fn release_snapshot(
    ctx: &OpContext,
    store_id: u64,
    group_id: u64,
    snapshot_handle: u64,
) -> Result<ReleaseSnapshotResponse> {
    Ok(ctx
        .kv()
        .release_snapshot(store_id, group_id, snapshot_handle)
        .await?)
}
