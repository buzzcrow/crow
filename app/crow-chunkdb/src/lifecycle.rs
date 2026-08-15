// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Chunk lifecycle management — state machine + gRPC handlers.
//!
//! Design §9: `Init → Active → Sealed → Deleted` state machine.
//! Transitions are validated; invalid transitions return
//! `InvalidStateTransition`.

pub mod state;

use std::sync::Arc;

use tracing::{info, warn};

use crow_protocol::chunkdb::rpc::{
    Chunk, ChunkState as ProtoChunkState, ChunkStrip, ChunkType, StripType as ProtoStripType,
};
use crow_protocol::common::ChunkId;
use crow_protocol::generate_chunk_id;

use crate::allocator::{AllocError, ChunkAllocator, StripAllocType};
use crate::range_guard::RangeGuard;
use crate::selector::PlacementConstraints;
use crate::storage::{ChunkStore, StoreError};
use crate::topology::TopologyCache;

pub use state::{ChunkState, StateTransitionError};

/// Lifecycle error — maps to gRPC status codes in the service layer.
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
}

impl LifecycleHandler {
    #[must_use]
    pub fn new(store: Arc<ChunkStore>, allocator: Arc<ChunkAllocator>, topology: TopologyCache) -> Self {
        Self {
            store,
            allocator,
            topology,
            range_guard: None,
        }
    }

    /// Attach a range guard for R99 sharded mode.
    #[must_use]
    pub fn with_range_guard(mut self, guard: Arc<RangeGuard>) -> Self {
        self.range_guard = Some(guard);
        self
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
        let unit_count = write_granularity_kb;

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
        let mut chunk = self.store.get_chunk(chunk_id).await?;
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
        info!(chunk_id = ?chunk_id, added_strips = strip_count, "chunk appended");
        Ok(chunk)
    }

    /// Seal a chunk — no more appends allowed.
    pub async fn seal_chunk(&self, chunk_id: &ChunkId, seal_length: u32) -> Result<Chunk, LifecycleError> {
        self.check_range(chunk_id)?;
        let mut chunk = self.store.get_chunk(chunk_id).await?;
        let current_state = ChunkState::from_proto(chunk.state);
        current_state.check_can_seal()?;

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX));

        chunk.state = ProtoChunkState::Sealed as i32;
        chunk.sealed_length = seal_length;
        chunk.sealed_ts_ms = now_ms;

        self.store.put_chunk(&chunk).await?;
        info!(chunk_id = ?chunk_id, seal_length, "chunk sealed");
        Ok(chunk)
    }

    /// Delete a chunk — marks deleted and frees segments.
    /// Returns `ChunkNotFound` if the chunk is already deleted (callers
    /// that want idempotent delete treat `NOT_FOUND` as success).
    pub async fn delete_chunk(&self, chunk_id: &ChunkId) -> Result<Chunk, LifecycleError> {
        self.check_range(chunk_id)?;
        let mut chunk = self.store.get_chunk(chunk_id).await?;
        let current_state = ChunkState::from_proto(chunk.state);

        // Already deleted → not-found (error codes carry status, not
        // return flags).
        if current_state == ChunkState::Deleted {
            return Err(LifecycleError::ChunkNotFound);
        }

        current_state.check_can_delete()?;

        // Free all segments (best-effort).
        let all_segments: Vec<_> = chunk.strips.iter().flat_map(extract_segments).collect();
        if !all_segments.is_empty() {
            if let Err(e) = self.allocator.pool().free_blocks(all_segments).await {
                warn!(error = %e, "delete_chunk: free_blocks failed (orphan segments logged)");
            }
        }

        chunk.state = ProtoChunkState::Deleted as i32;
        self.store.put_chunk(&chunk).await?;
        info!(chunk_id = ?chunk_id, "chunk deleted");
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
    use crow_protocol::chunkdb::rpc::chunk_strip::Strip;
    match &strip.strip {
        Some(Strip::MirrorStrip(m)) => m.segments.clone(),
        Some(Strip::EcStrip(ec)) => ec.segments.clone(),
        None => Vec::new(),
    }
}
