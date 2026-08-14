// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Hash bucket router — chunk ID → 16-bit bucket → KV group.
//!
//! Design §5.4a: logical hash bucket system. The binding table maps
//! bucket ranges to KV group IDs, stored in group-0. A watch/notify
//! cache keeps the binding table fresh for immediate routing.

use std::sync::Arc;

use parking_lot::RwLock;
use tracing::warn;

use crow_common::chunk_id::ChunkIdParts;
use crow_protocol::common::ChunkId;

/// Migration state for a bucket range (design §5.4b).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MigrationState {
    NotMigrating,
    Copying,
    Cutover,
    Cleanup,
    Complete,
}

/// A bucket range binding: `[start, end)` → KV group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BucketBinding {
    pub start: u16,
    pub end: u16,
    pub kv_store_id: u64,
    pub kv_group_id: u64,
    /// During migration, the old group for fallback reads.
    pub old_kv_store_id: Option<u64>,
    pub old_kv_group_id: Option<u64>,
    pub migration_state: MigrationState,
}

/// The binding table — a list of bucket range bindings.
#[derive(Debug, Clone, Default)]
pub struct BindingTable {
    bindings: Vec<BucketBinding>,
}

impl BindingTable {
    /// Create a new binding table from a list of bindings.
    #[must_use]
    pub fn new(bindings: Vec<BucketBinding>) -> Self {
        Self { bindings }
    }

    /// Route a bucket to its binding.
    pub fn route(&self, bucket: u16) -> Option<&BucketBinding> {
        self.bindings.iter().find(|b| bucket >= b.start && bucket < b.end)
    }

    /// Number of bindings.
    pub fn len(&self) -> usize {
        self.bindings.len()
    }

    /// Is the table empty?
    pub fn is_empty(&self) -> bool {
        self.bindings.is_empty()
    }

    /// Get all bindings.
    pub fn bindings(&self) -> &[BucketBinding] {
        &self.bindings
    }

    /// Get all bindings (mutable).
    pub fn bindings_mut(&mut self) -> &mut Vec<BucketBinding> {
        &mut self.bindings
    }
}

/// Thread-safe binding cache.
#[derive(Clone)]
pub struct BindingCache {
    inner: Arc<RwLock<BindingTable>>,
}

impl BindingCache {
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(BindingTable::default())),
        }
    }

    /// Replace the entire binding table.
    pub fn replace(&self, table: BindingTable) {
        *self.inner.write() = table;
    }

    /// Route a chunk ID to its binding.
    pub fn route(&self, chunk_id: &ChunkId) -> Option<BucketBinding> {
        let parts = chunk_id_to_parts(chunk_id);
        let bucket = parts.hash_to_bucket();
        self.inner.read().route(bucket).cloned()
    }

    /// Route a bucket directly.
    pub fn route_bucket(&self, bucket: u16) -> Option<BucketBinding> {
        self.inner.read().route(bucket).cloned()
    }

    /// Check if the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.inner.read().is_empty()
    }

    /// Get a snapshot of the binding table.
    pub fn snapshot(&self) -> BindingTable {
        self.inner.read().clone()
    }
}

impl Default for BindingCache {
    fn default() -> Self {
        Self::new()
    }
}

/// Convert a proto `ChunkId` to `ChunkIdParts` for hashing.
pub fn chunk_id_to_parts(id: &ChunkId) -> ChunkIdParts {
    ChunkIdParts {
        high: id.high,
        mid: id.mid,
        low: id.low,
    }
}

/// Hash a chunk ID to a 16-bit bucket (0-65535).
pub fn hash_to_bucket(id: &ChunkId) -> u16 {
    chunk_id_to_parts(id).hash_to_bucket()
}

/// Build a default binding table that maps all buckets to a single
/// KV group. Used when no binding table exists in group-0 yet.
pub fn default_binding_table(store_id: u64, group_id: u64) -> BindingTable {
    BindingTable {
        bindings: vec![BucketBinding {
            start: 0,
            end: 65535,
            kv_store_id: store_id,
            kv_group_id: group_id,
            old_kv_store_id: None,
            old_kv_group_id: None,
            migration_state: MigrationState::NotMigrating,
        }],
    }
}

/// Routing error.
#[derive(Debug, thiserror::Error)]
pub enum RouteError {
    #[error("binding table empty — no routing information")]
    NoBinding,
    #[error("bucket {bucket} has no binding")]
    BucketUnbound { bucket: u16 },
}

/// Route result: where to read/write the chunk metadata.
#[derive(Debug, Clone)]
pub struct Route {
    pub kv_store_id: u64,
    pub kv_group_id: u64,
    pub migration_state: MigrationState,
    pub old_kv_store_id: Option<u64>,
    pub old_kv_group_id: Option<u64>,
}

/// Route a chunk ID using the binding cache.
pub fn route(cache: &BindingCache, chunk_id: &ChunkId) -> Result<Route, RouteError> {
    if cache.is_empty() {
        warn!("routing: binding cache empty, no route available");
        return Err(RouteError::NoBinding);
    }
    let binding = cache.route(chunk_id).ok_or_else(|| RouteError::BucketUnbound {
        bucket: hash_to_bucket(chunk_id),
    })?;
    Ok(Route {
        kv_store_id: binding.kv_store_id,
        kv_group_id: binding.kv_group_id,
        migration_state: binding.migration_state,
        old_kv_store_id: binding.old_kv_store_id,
        old_kv_group_id: binding.old_kv_group_id,
    })
}
