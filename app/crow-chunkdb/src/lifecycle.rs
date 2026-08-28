// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Chunk lifecycle management — state machine + crow-rpc handlers.
//!
//! Design §9: `Init → Active → Sealed → Deleted` state machine.
//! Transitions are validated; invalid transitions return
//! `InvalidStateTransition`.

pub mod state;

use std::sync::Arc;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use quick_cache::sync::Cache;
use tokio::sync::{Mutex, OwnedMutexGuard};
use tracing::{info, warn};

use crow_protocol::chunkdb::rpc::{
    Chunk, ChunkState as ProtoChunkState, ChunkStrip, ChunkType, StripType as ProtoStripType,
};
use crow_protocol::common::ChunkId;
use crow_protocol::generate_chunk_id;

use crate::allocator::{AllocError, ChunkAllocator, StripAllocType};
use crate::metrics::LifecycleMetrics;
use crate::range_guard::RangeGuard;
use crate::routing::hash_to_bucket;
use crate::selector::PlacementConstraints;
use crate::storage::{ChunkStore, StoreError};
use crate::topology::TopologyCache;

pub use state::{ChunkState, StateTransitionError};

/// Default lock wait time for `LockPolicy::default()`.
const DEFAULT_LOCK_WAIT: Duration = Duration::from_secs(10);

/// Lifecycle error — maps to crow-rpc status codes in the service layer.
#[derive(Debug, thiserror::Error)]
pub enum LifecycleError {
    #[error("invalid state transition: {0}")]
    InvalidStateTransition(#[from] StateTransitionError),
    #[error("chunk not found")]
    ChunkNotFound,
    #[error("chunk already exists")]
    ChunkAlreadyExists,
    #[error("state conflict — concurrent modification")]
    StateConflict,
    #[error("allocation failed: {0}")]
    Allocation(#[from] AllocError),
    #[error("storage error: {0}")]
    Storage(#[from] StoreError),
    #[error("invalid request: {0}")]
    InvalidRequest(String),
    #[error("chunk bucket {bucket} not in owned ranges")]
    NotMyRange { bucket: u16 },
    #[error("chunk lock busy — retry later")]
    LockBusy,
    #[error("chunk lock acquire timed out")]
    LockTimeout,
    #[error("strip index {index} out of range (chunk has {len} strips)")]
    StripIndexOutOfRange { index: u32, len: usize },
}

/// Lock policy — how to handle contention on a per-chunk mutex.
#[derive(Debug, Clone)]
pub enum LockPolicy {
    /// Fail fast with `LockBusy` on contention.
    TryLock,
    /// Park the task up to `duration`, then `LockTimeout`.
    Wait(Duration),
}

impl Default for LockPolicy {
    fn default() -> Self {
        Self::Wait(DEFAULT_LOCK_WAIT)
    }
}

/// Cache hint — whether to populate the payload cache.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum CacheHint {
    /// Populate cache on miss, write to cache on refresh (default).
    #[default]
    Cache,
    /// Skip cache population — always fetch from store.
    NoCache,
}

/// Per-chunk lock map + payload cache.
pub struct ChunkLockMap {
    locks: DashMap<ChunkId, Arc<Mutex<()>>>,
    chunks: Arc<Cache<ChunkId, Chunk>>,
    metrics: Arc<LifecycleMetrics>,
    hold_warn_threshold: Duration,
}

impl ChunkLockMap {
    /// Create a new lock map with the given cache capacity.
    #[must_use]
    pub fn new(cache_capacity: usize, metrics: Arc<LifecycleMetrics>, hold_warn_threshold: Duration) -> Self {
        Self {
            locks: DashMap::new(),
            chunks: Arc::new(Cache::new(cache_capacity)),
            metrics,
            hold_warn_threshold,
        }
    }

    /// Acquire the per-chunk lock and serve the latest chunk record
    /// (cache hit → zero store round-trips; cache miss → one `get_chunk`).
    pub async fn acquire(
        &self,
        chunk_id: &ChunkId,
        store: &ChunkStore,
        policy: &LockPolicy,
        hint: CacheHint,
    ) -> Result<ChunkGuard, LifecycleError> {
        let mutex = self.get_or_create_lock(chunk_id);
        let wait_start = Instant::now();
        let guard = self.acquire_lock(&mutex, policy).await?;
        let wait_dur = wait_start.elapsed();
        self.metrics
            .record_lock_wait(u64::try_from(wait_dur.as_micros()).unwrap_or(u64::MAX));

        let chunk = if let Some(c) = self.chunks.get(chunk_id) {
            self.metrics.record_cache_hit();
            Some(c)
        } else {
            self.metrics.record_cache_miss();
            match store.get_chunk(chunk_id).await {
                Ok(c) => {
                    if hint == CacheHint::Cache {
                        self.chunks.insert(*chunk_id, c.clone());
                    }
                    Some(c)
                }
                Err(StoreError::ChunkNotFound) => return Err(LifecycleError::ChunkNotFound),
                Err(e) => return Err(LifecycleError::Storage(e)),
            }
        };
        Ok(ChunkGuard {
            guard,
            chunk,
            hint,
            chunk_id: *chunk_id,
            hold_start: Instant::now(),
            chunks: Arc::clone(&self.chunks),
            metrics: Arc::clone(&self.metrics),
            hold_warn_threshold: self.hold_warn_threshold,
        })
    }

    /// Acquire the per-chunk lock without fetching from the store
    /// (for `allocate_chunk` with a caller-supplied ID).
    pub async fn acquire_for_create(
        &self,
        chunk_id: &ChunkId,
        policy: &LockPolicy,
        hint: CacheHint,
    ) -> Result<ChunkGuard, LifecycleError> {
        let mutex = self.get_or_create_lock(chunk_id);
        let wait_start = Instant::now();
        let guard = self.acquire_lock(&mutex, policy).await?;
        let wait_dur = wait_start.elapsed();
        self.metrics
            .record_lock_wait(u64::try_from(wait_dur.as_micros()).unwrap_or(u64::MAX));
        Ok(ChunkGuard {
            guard,
            chunk: None,
            hint,
            chunk_id: *chunk_id,
            hold_start: Instant::now(),
            chunks: Arc::clone(&self.chunks),
            metrics: Arc::clone(&self.metrics),
            hold_warn_threshold: self.hold_warn_threshold,
        })
    }

    /// Populate the cache directly (for auto-generated chunk IDs that
    /// skip the lock).
    pub fn populate_cache(&self, chunk_id: &ChunkId, chunk: Chunk) {
        self.chunks.insert(*chunk_id, chunk);
    }

    /// Reap uncontended lock entries (`Arc::strong_count == 1`).
    pub fn reap_idle(&self) {
        let before = self.locks.len();
        self.locks.retain(|_, arc| Arc::strong_count(arc) > 1);
        let removed = before.saturating_sub(self.locks.len());
        self.metrics
            .record_reap_idle(u64::try_from(removed).unwrap_or(u64::MAX));
    }

    /// Invalidate a single chunk from the payload cache.
    pub fn invalidate_chunk(&self, chunk_id: &ChunkId) -> bool {
        let removed = self.chunks.remove(chunk_id).is_some();
        if removed {
            self.metrics.record_invalidate();
        }
        removed
    }

    /// Invalidate all cache entries whose chunk ID hashes to a bucket
    /// in `[bucket_start, bucket_end]`.
    pub fn invalidate_range(&self, bucket_start: u16, bucket_end: u16) -> u32 {
        let mut count = 0u32;
        let keys_to_remove: Vec<ChunkId> = self
            .chunks
            .iter()
            .filter(|(k, _)| {
                let bucket = hash_to_bucket(k);
                bucket >= bucket_start && bucket <= bucket_end
            })
            .map(|(k, _)| k)
            .collect();
        for k in &keys_to_remove {
            if self.chunks.remove(k).is_some() {
                count += 1;
                self.metrics.record_invalidate();
            }
        }
        count
    }

    /// Current number of entries in the payload cache.
    #[must_use]
    pub fn cache_len(&self) -> u64 {
        u64::try_from(self.chunks.len()).unwrap_or(u64::MAX)
    }

    /// Snapshot metrics.
    #[must_use]
    pub fn metrics_snapshot(&self) -> crate::metrics::LifecycleMetricsSnapshot {
        self.metrics.snapshot(self.cache_len())
    }

    fn get_or_create_lock(&self, chunk_id: &ChunkId) -> Arc<Mutex<()>> {
        self.locks
            .entry(*chunk_id)
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    async fn acquire_lock(
        &self,
        mutex: &Arc<Mutex<()>>,
        policy: &LockPolicy,
    ) -> Result<OwnedMutexGuard<()>, LifecycleError> {
        match policy {
            LockPolicy::TryLock => {
                if let Ok(g) = mutex.clone().try_lock_owned() {
                    Ok(g)
                } else {
                    self.metrics.record_lock_busy();
                    Err(LifecycleError::LockBusy)
                }
            }
            LockPolicy::Wait(d) => {
                if let Ok(g) = tokio::time::timeout(*d, mutex.clone().lock_owned()).await {
                    Ok(g)
                } else {
                    self.metrics.record_lock_timeout();
                    Err(LifecycleError::LockTimeout)
                }
            }
        }
    }
}

/// Guard — holds the per-chunk lock, carries the latest chunk record.
/// Lock is released on drop; hold time is recorded into metrics.
pub struct ChunkGuard {
    #[allow(dead_code)]
    // Held for Drop — releases the per-chunk lock. Never read directly.
    guard: OwnedMutexGuard<()>,
    chunk: Option<Chunk>,
    hint: CacheHint,
    chunk_id: ChunkId,
    hold_start: Instant,
    chunks: Arc<Cache<ChunkId, Chunk>>,
    metrics: Arc<LifecycleMetrics>,
    hold_warn_threshold: Duration,
}

impl std::fmt::Debug for ChunkGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChunkGuard")
            .field("chunk_id", &self.chunk_id)
            .field("hint", &self.hint)
            .field("has_chunk", &self.chunk.is_some())
            .finish_non_exhaustive()
    }
}

impl ChunkGuard {
    /// The latest chunk record (non-None for append/seal/delete;
    /// None for `acquire_for_create` before `refresh`).
    #[must_use]
    pub fn chunk(&self) -> Option<&Chunk> {
        self.chunk.as_ref()
    }

    /// Update the cache after a successful `put_chunk`. Caller MUST
    /// have persisted first. If `CacheHint::NoCache`, only updates
    /// the guard's local copy.
    pub fn refresh(&mut self, chunk: Chunk) {
        if self.hint == CacheHint::Cache {
            self.chunks.insert(self.chunk_id, chunk.clone());
        }
        self.chunk = Some(chunk);
    }
}

impl Drop for ChunkGuard {
    fn drop(&mut self) {
        let hold_dur = self.hold_start.elapsed();
        self.metrics
            .record_lock_hold(u64::try_from(hold_dur.as_micros()).unwrap_or(u64::MAX));
        if hold_dur > self.hold_warn_threshold {
            warn!(
                chunk_id = ?self.chunk_id,
                hold_ms = hold_dur.as_millis(),
                "chunk lock held longer than threshold"
            );
        }
    }
}

/// Lifecycle handler — orchestrates allocate/append/seal/delete/query/list.
pub struct LifecycleHandler {
    store: Arc<ChunkStore>,
    allocator: Arc<ChunkAllocator>,
    topology: TopologyCache,
    /// Range guard — enforces chunkdb instance sharding. `None` for
    /// v1 single-instance mode (no binding table); `Some` for R99
    /// sharded mode.
    range_guard: Option<Arc<RangeGuard>>,
    /// Per-chunk lock map + payload cache. `None` when R100 is not
    /// configured (no lifecycle section in config).
    locks: Option<Arc<ChunkLockMap>>,
}

impl LifecycleHandler {
    #[must_use]
    pub fn new(store: Arc<ChunkStore>, allocator: Arc<ChunkAllocator>, topology: TopologyCache) -> Self {
        Self {
            store,
            allocator,
            topology,
            range_guard: None,
            locks: None,
        }
    }

    /// Attach a range guard for R99 sharded mode.
    #[must_use]
    pub fn with_range_guard(mut self, guard: Arc<RangeGuard>) -> Self {
        self.range_guard = Some(guard);
        self
    }

    /// Attach a per-chunk lock map (R100).
    #[must_use]
    pub fn with_locks(mut self, locks: Arc<ChunkLockMap>) -> Self {
        self.locks = Some(locks);
        self
    }

    /// Get a reference to the lock map (if attached).
    #[must_use]
    pub fn locks(&self) -> Option<&Arc<ChunkLockMap>> {
        self.locks.as_ref()
    }

    /// Check the range guard (if present) before processing a
    /// mutating RPC. Read-only RPCs (query, list) bypass the guard.
    fn check_range(&self, chunk_id: &ChunkId) -> Result<(), LifecycleError> {
        if let Some(guard) = &self.range_guard {
            guard
                .check(chunk_id)
                .map_err(|e| LifecycleError::NotMyRange { bucket: e.bucket })?;
        }
        Ok(())
    }

    /// Allocate a new chunk.
    #[allow(clippy::too_many_arguments)]
    pub async fn allocate_chunk(
        &self,
        chunk_id: Option<ChunkId>,
        write_granularity_kb: u32,
        strip_count: u32,
        strip_type: ProtoStripType,
        data_num: u32,
        code_num: u32,
        copy_count: u32,
        chunk_type: ChunkType,
    ) -> Result<Chunk, LifecycleError> {
        let id = chunk_id.unwrap_or_else(|| {
            let parts = generate_chunk_id(chunk_type as u8);
            parts.to_proto()
        });
        self.check_range(&id)?;

        // Caller-supplied ID: acquire lock + existence check.
        // Auto-generated ID: skip lock (UUID collision negligible).
        let mut guard = if chunk_id.is_some() {
            if let Some(locks) = &self.locks {
                let g = locks
                    .acquire_for_create(&id, &LockPolicy::default(), CacheHint::Cache)
                    .await?;
                // Existence check inside the lock.
                match self.store.get_chunk(&id).await {
                    Ok(_) => return Err(LifecycleError::ChunkAlreadyExists),
                    Err(StoreError::ChunkNotFound) => {}
                    Err(e) => return Err(LifecycleError::Storage(e)),
                }
                Some(g)
            } else {
                None
            }
        } else {
            None
        };

        let snap = self.topology.snapshot();

        let mirror_copies = if copy_count == 0 { 3 } else { copy_count as usize };
        let strip_alloc_type = match strip_type {
            ProtoStripType::Mirror => StripAllocType::Mirror {
                copy_count: mirror_copies,
            },
            ProtoStripType::Ec => StripAllocType::Ec {
                data_num: data_num as usize,
                code_num: code_num as usize,
            },
        };

        let constraints = PlacementConstraints::new();
        // Convert write_granularity (KB) to unit_count using the unit
        // size from the topology snapshot. Fall back to treating KB as
        // units if unit_size_bytes is unavailable (0).
        let unit_size_kb = snap.unit_size_bytes() / 1024;
        let unit_count = write_granularity_kb
            .checked_div(unit_size_kb)
            .unwrap_or(write_granularity_kb)
            .max(1);

        let mut strips = Vec::with_capacity(strip_count as usize);
        for seq in 0..strip_count {
            let strip = self
                .allocator
                .allocate_strip(&snap, &id, strip_alloc_type, unit_count, seq, &constraints)
                .await?;
            strips.push(strip);
        }

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX));

        let chunk = Chunk {
            id: Some(id),
            state: ProtoChunkState::Active as i32,
            create_ts_ms: now_ms,
            sealed_ts_ms: 0,
            capacity: strips.iter().map(|s| s.capacity).sum(),
            sealed_length: 0,
            strips,
            chunk_type: chunk_type as i32,
        };

        // Persist the chunk record, then commit the disk blocks.
        self.store.put_chunk(&chunk).await?;
        self.commit_strip_segments(&chunk.strips).await;

        // Update cache.
        if let Some(ref mut g) = guard {
            g.refresh(chunk.clone());
        } else if chunk_id.is_none() {
            // Auto-generated ID: populate cache directly (no guard).
            if let Some(locks) = &self.locks {
                locks.populate_cache(&id, chunk.clone());
            }
        }
        info!(chunk_id = ?id, strips = strip_count, "chunk allocated");
        Ok(chunk)
    }

    /// Append strips to an active chunk.
    #[allow(clippy::too_many_arguments)]
    pub async fn append_chunk(
        &self,
        chunk_id: &ChunkId,
        strip_count: u32,
        strip_type: ProtoStripType,
        data_num: u32,
        code_num: u32,
        copy_count: u32,
        unit_count: u32,
    ) -> Result<Chunk, LifecycleError> {
        self.check_range(chunk_id)?;

        let mut guard = if let Some(locks) = &self.locks {
            Some(
                locks
                    .acquire(chunk_id, &self.store, &LockPolicy::default(), CacheHint::Cache)
                    .await?,
            )
        } else {
            None
        };

        let mut chunk = match &guard {
            Some(g) => g
                .chunk()
                .unwrap_or_else(|| unreachable!("acquire guarantees chunk on Ok"))
                .clone(),
            None => self.store.get_chunk(chunk_id).await?,
        };
        let current_state = ChunkState::from_proto(chunk.state);
        current_state.check_can_append()?;

        let snap = self.topology.snapshot();
        let mirror_copies = if copy_count == 0 { 3 } else { copy_count as usize };
        let strip_alloc_type = match strip_type {
            ProtoStripType::Mirror => StripAllocType::Mirror {
                copy_count: mirror_copies,
            },
            ProtoStripType::Ec => StripAllocType::Ec {
                data_num: data_num as usize,
                code_num: code_num as usize,
            },
        };

        let constraints = PlacementConstraints::new();
        let start_seq = u32::try_from(chunk.strips.len()).unwrap_or(u32::MAX);

        for i in 0..strip_count {
            let seq = start_seq + i;
            let strip = self
                .allocator
                .allocate_strip(&snap, chunk_id, strip_alloc_type, unit_count, seq, &constraints)
                .await?;
            chunk.strips.push(strip);
        }

        chunk.capacity = chunk.strips.iter().map(|s| s.capacity).sum();
        self.store.put_chunk(&chunk).await?;
        // Commit the newly-appended strip segments.
        let new_strips = &chunk.strips[chunk.strips.len() - strip_count as usize..];
        self.commit_strip_segments(new_strips).await;

        if let Some(ref mut g) = guard {
            g.refresh(chunk.clone());
        }
        info!(chunk_id = ?chunk_id, added_strips = strip_count, "chunk appended");
        Ok(chunk)
    }

    /// Seal a chunk — no more appends allowed.
    pub async fn seal_chunk(&self, chunk_id: &ChunkId, seal_length: u32) -> Result<Chunk, LifecycleError> {
        self.check_range(chunk_id)?;

        let mut guard = if let Some(locks) = &self.locks {
            Some(
                locks
                    .acquire(chunk_id, &self.store, &LockPolicy::default(), CacheHint::Cache)
                    .await?,
            )
        } else {
            None
        };

        let mut chunk = match &guard {
            Some(g) => g
                .chunk()
                .unwrap_or_else(|| unreachable!("acquire guarantees chunk on Ok"))
                .clone(),
            None => self.store.get_chunk(chunk_id).await?,
        };
        let current_state = ChunkState::from_proto(chunk.state);
        current_state.check_can_seal()?;

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX));

        chunk.state = ProtoChunkState::Sealed as i32;
        chunk.sealed_length = seal_length;
        chunk.sealed_ts_ms = now_ms;

        self.store.put_chunk(&chunk).await?;

        if let Some(ref mut g) = guard {
            g.refresh(chunk.clone());
        }
        info!(chunk_id = ?chunk_id, seal_length, "chunk sealed");
        Ok(chunk)
    }

    /// Delete a chunk — marks deleted and frees segments.
    /// Returns `ChunkNotFound` if the chunk is already deleted (callers
    /// that want idempotent delete treat `NOT_FOUND` as success).
    pub async fn delete_chunk(&self, chunk_id: &ChunkId) -> Result<Chunk, LifecycleError> {
        self.check_range(chunk_id)?;

        let mut guard = if let Some(locks) = &self.locks {
            Some(
                locks
                    .acquire(chunk_id, &self.store, &LockPolicy::default(), CacheHint::Cache)
                    .await?,
            )
        } else {
            None
        };

        let mut chunk = match &guard {
            Some(g) => g
                .chunk()
                .unwrap_or_else(|| unreachable!("acquire guarantees chunk on Ok"))
                .clone(),
            None => self.store.get_chunk(chunk_id).await?,
        };
        let current_state = ChunkState::from_proto(chunk.state);

        // Already deleted → not-found (error codes carry status, not
        // return flags).
        if current_state == ChunkState::Deleted {
            return Err(LifecycleError::ChunkNotFound);
        }

        current_state.check_can_delete()?;

        // Free all segments (best-effort, inside the lock per GAP-R100-1).
        let all_segments: Vec<_> = chunk.strips.iter().flat_map(extract_segments).collect();
        if !all_segments.is_empty() {
            if let Err(e) = self.allocator.pool().free_blocks(all_segments).await {
                warn!(error = %e, "delete_chunk: free_blocks failed (orphan segments logged)");
            }
        }

        chunk.state = ProtoChunkState::Deleted as i32;
        self.store.put_chunk(&chunk).await?;

        if let Some(ref mut g) = guard {
            g.refresh(chunk.clone());
        }
        info!(chunk_id = ?chunk_id, "chunk deleted");
        Ok(chunk)
    }

    /// Delete a range within a chunk (partial delete). Frees the
    /// segments of strips whose `[chunk_offset, chunk_offset +
    /// capacity)` range overlaps with `[offset, offset + size)`, then
    /// removes those strips from the chunk record. The chunk must be
    /// Active.
    pub async fn delete_chunk_range(
        &self,
        chunk_id: &ChunkId,
        offset: u32,
        size: u32,
    ) -> Result<(), LifecycleError> {
        self.check_range(chunk_id)?;

        let mut guard = if let Some(locks) = &self.locks {
            Some(
                locks
                    .acquire(chunk_id, &self.store, &LockPolicy::default(), CacheHint::Cache)
                    .await?,
            )
        } else {
            None
        };

        let mut chunk = match &guard {
            Some(g) => g
                .chunk()
                .unwrap_or_else(|| unreachable!("acquire guarantees chunk on Ok"))
                .clone(),
            None => self.store.get_chunk(chunk_id).await?,
        };
        let current_state = ChunkState::from_proto(chunk.state);
        current_state.check_can_append()?;

        let end = offset.saturating_add(size);
        // Find strips that overlap with [offset, end).
        let (to_remove, to_keep): (Vec<_>, Vec<_>) = chunk.strips.into_iter().partition(|s| {
            let s_start = s.chunk_offset;
            let s_end = s_start.saturating_add(s.capacity);
            s_start < end && offset < s_end
        });

        // Free segments of the removed strips.
        let all_segments: Vec<_> = to_remove.iter().flat_map(extract_segments).collect();
        if !all_segments.is_empty() {
            if let Err(e) = self.allocator.pool().free_blocks(all_segments).await {
                warn!(error = %e, "delete_chunk_range: free_blocks failed (orphan segments logged)");
            }
        }

        let removed_count = to_remove.len();
        chunk.strips = to_keep;
        chunk.capacity = chunk.strips.iter().map(|s| s.capacity).sum();
        self.store.put_chunk(&chunk).await?;

        if let Some(ref mut g) = guard {
            g.refresh(chunk.clone());
        }
        info!(chunk_id = ?chunk_id, offset, size, removed_strips = removed_count, "chunk range deleted");
        Ok(())
    }

    /// Update a single strip within a chunk (e.g. after EC parity
    /// computation). Replaces the strip at `strip_index` with the new
    /// strip, freeing the old strip's segments and committing the new
    /// strip's segments. The chunk must be Active or Sealed.
    pub async fn update_chunk_strip(
        &self,
        chunk_id: &ChunkId,
        strip_index: u32,
        new_strip: ChunkStrip,
    ) -> Result<Chunk, LifecycleError> {
        self.check_range(chunk_id)?;

        let mut guard = if let Some(locks) = &self.locks {
            Some(
                locks
                    .acquire(chunk_id, &self.store, &LockPolicy::default(), CacheHint::Cache)
                    .await?,
            )
        } else {
            None
        };

        let mut chunk = match &guard {
            Some(g) => g
                .chunk()
                .unwrap_or_else(|| unreachable!("acquire guarantees chunk on Ok"))
                .clone(),
            None => self.store.get_chunk(chunk_id).await?,
        };
        let current_state = ChunkState::from_proto(chunk.state);
        // Strip updates can happen on Active (EC encoding) or Sealed
        // (parity rebuild after seal) chunks.
        if current_state != ChunkState::Active && current_state != ChunkState::Sealed {
            return Err(LifecycleError::InvalidStateTransition(StateTransitionError::new(
                current_state,
                "Active|Sealed",
            )));
        }

        let idx = usize::try_from(strip_index).unwrap_or(usize::MAX);
        if idx >= chunk.strips.len() {
            return Err(LifecycleError::StripIndexOutOfRange {
                index: strip_index,
                len: chunk.strips.len(),
            });
        }

        // Free the old strip's segments.
        let old_segments = extract_segments(&chunk.strips[idx]);
        if !old_segments.is_empty() {
            if let Err(e) = self.allocator.pool().free_blocks(old_segments).await {
                warn!(error = %e, "update_chunk_strip: free_blocks failed for old strip (orphan segments logged)");
            }
        }

        // Commit the new strip's segments.
        self.commit_strip_segments(std::slice::from_ref(&new_strip)).await;

        // Replace the strip.
        chunk.strips[idx] = new_strip;
        chunk.capacity = chunk.strips.iter().map(|s| s.capacity).sum();
        self.store.put_chunk(&chunk).await?;

        if let Some(ref mut g) = guard {
            g.refresh(chunk.clone());
        }
        info!(chunk_id = ?chunk_id, strip_index, "chunk strip updated");
        Ok(chunk)
    }

    /// Query a chunk by ID.
    pub async fn query_chunk(&self, chunk_id: &ChunkId) -> Result<Chunk, LifecycleError> {
        self.store.get_chunk(chunk_id).await.map_err(|e| match e {
            StoreError::ChunkNotFound => LifecycleError::ChunkNotFound,
            other => LifecycleError::Storage(other),
        })
    }

    /// List chunks with pagination.
    pub async fn list_chunks(
        &self,
        start_after: Option<&ChunkId>,
        max_keys: u32,
    ) -> Result<Vec<Chunk>, LifecycleError> {
        if max_keys == 0 {
            return Ok(Vec::new());
        }
        self.store
            .list_chunks(start_after, max_keys)
            .await
            .map_err(LifecycleError::Storage)
    }

    /// Commit all segments in the given strips to diskdb (two-phase
    /// commit: mark tentative blocks as permanent after chunk persist).
    /// Best-effort — logs failures for the orphan scanner.
    async fn commit_strip_segments(&self, strips: &[ChunkStrip]) {
        let all_segments: Vec<_> = strips.iter().flat_map(extract_segments).collect();
        if all_segments.is_empty() {
            return;
        }
        if let Err(e) = self.allocator.pool().commit_blocks(all_segments).await {
            warn!(
                error = %e,
                "commit_blocks failed — blocks remain tentative (orphan scanner will reclaim)"
            );
        }
    }
}

/// Extract all segments from a strip (mirror or EC).
fn extract_segments(strip: &ChunkStrip) -> Vec<crow_protocol::diskdb::rpc::Segment> {
    use crow_protocol::chunkdb::rpc::Strip;
    match &strip.strip {
        Some(Strip::MirrorStrip(m)) => m.segments.clone(),
        Some(Strip::EcStrip(ec)) => ec.segments.clone(),
        None => Vec::new(),
    }
}
