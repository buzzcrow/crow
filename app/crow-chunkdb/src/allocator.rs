// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Chunk allocator — orchestrates strip layout → selector → parallel
//! diskdb `AllocateBlocks` with rollback on partial failure.
//!
//! Design §8: parallel allocation via `futures::join_all`; rollback
//! frees successfully-allocated segments on any failure.

pub mod pool;
pub mod rollback;

use std::sync::Arc;

use futures::future::join_all;
use tracing::{info, warn};

use crow_protocol::chunkdb::rpc::StripType as ProtoStripType;
use crow_protocol::chunkdb::rpc::{ChunkStrip, EcStrip, MirrorStrip};
use crow_protocol::common::ChunkId;
use crow_protocol::diskdb::rpc::Segment;

use crate::selector::{EcPlacement, MirrorPlacement, PlacementConstraints, PlacementPlan};
use crate::topology::TopologySnapshot;

pub use pool::DiskdbClientPool;
pub use rollback::rollback_segments;

/// Allocator error.
#[derive(Debug, thiserror::Error)]
pub enum AllocError {
    #[error("placement failed: {0}")]
    Placement(#[from] crate::selector::PlacementError),
    #[error("diskdb allocate failed for disk_group {dg_id}: {error}")]
    AllocateFailed { dg_id: u64, error: String },
    #[error("partial allocation: requested {requested}, got {got}")]
    PartialAllocation { requested: u32, got: u32 },
    #[error("rollback failed: {0}")]
    Rollback(String),
}

/// Strip type for allocation.
#[derive(Debug, Clone, Copy)]
pub enum StripAllocType {
    Mirror { copy_count: usize },
    Ec { data_num: usize, code_num: usize },
}

/// Result of a strip allocation.
pub struct AllocatedStrip {
    pub strip: ChunkStrip,
}

/// Chunk allocator — orchestrates placement + parallel diskdb calls.
pub struct ChunkAllocator {
    pool: Arc<DiskdbClientPool>,
}

impl ChunkAllocator {
    #[must_use]
    pub fn new(pool: Arc<DiskdbClientPool>) -> Self {
        Self { pool }
    }

    /// Get a reference to the diskdb client pool.
    pub fn pool(&self) -> &DiskdbClientPool {
        &self.pool
    }

    /// Allocate a single strip.
    ///
    /// # Errors
    /// Returns `AllocError` on placement failure, diskdb RPC failure,
    /// or partial allocation (triggers rollback).
    pub async fn allocate_strip(
        &self,
        snap: &TopologySnapshot,
        owner_chunk: &ChunkId,
        strip_type: StripAllocType,
        unit_count: u32,
        strip_sequence: u32,
        constraints: &PlacementConstraints,
    ) -> Result<AllocatedStrip, AllocError> {
        let plan = match strip_type {
            StripAllocType::Mirror { copy_count } => MirrorPlacement::select(snap, copy_count, constraints)?,
            StripAllocType::Ec { data_num, code_num } => {
                EcPlacement::select(snap, data_num, code_num, constraints)?
            }
        };

        let segments = self
            .allocate_blocks_parallel(owner_chunk, &plan, unit_count)
            .await?;

        let strip = assemble_strip(&segments, strip_type, strip_sequence, &plan);
        Ok(AllocatedStrip { strip })
    }

    /// Allocate blocks in parallel across all placement entries.
    async fn allocate_blocks_parallel(
        &self,
        owner_chunk: &ChunkId,
        plan: &PlacementPlan,
        unit_count: u32,
    ) -> Result<Vec<Segment>, AllocError> {
        let mut futures = Vec::new();
        for entry in &plan.entries {
            let pool = Arc::clone(&self.pool);
            let owner = *owner_chunk;
            let dg_id = entry.disk_group_id;
            let count = entry.block_count;
            futures.push(async move {
                pool.allocate_blocks(dg_id, count, unit_count, &owner)
                    .await
                    .map_err(|e| AllocError::AllocateFailed {
                        dg_id,
                        error: e.clone(),
                    })
            });
        }

        let results = join_all(futures).await;

        // Check for failures. On any failure, collect successful
        // segments for rollback.
        let mut all_segments = Vec::new();
        let mut errors = Vec::new();
        for result in results {
            match result {
                Ok(resp) => {
                    all_segments.extend(resp.segments);
                }
                Err(e) => {
                    errors.push(e);
                }
            }
        }

        if !errors.is_empty() {
            // Rollback successfully-allocated segments.
            if !all_segments.is_empty() {
                warn!(
                    segment_count = all_segments.len(),
                    error_count = errors.len(),
                    "allocation failed, rolling back"
                );
                if let Err(rb_err) = self.pool.free_blocks(all_segments.clone()).await {
                    // Rollback failure: log for orphan scanner.
                    warn!(
                        error = %rb_err,
                        segments = ?all_segments.iter().map(|s| (&s.disk_id, s.zone_index, s.unit_offset)).collect::<Vec<_>>(),
                        "rollback free_blocks failed — orphan segments logged for scanner"
                    );
                }
            }
            return Err(errors.into_iter().next().expect("at least one error"));
        }

        // Verify we got the expected number of segments.
        let expected: u32 = plan.entries.iter().map(|e| e.block_count).sum();
        let got = u32::try_from(all_segments.len()).unwrap_or(u32::MAX);
        if got < expected {
            warn!(expected, got, "partial allocation, rolling back");
            let _ = self.pool.free_blocks(all_segments).await;
            return Err(AllocError::PartialAllocation {
                requested: expected,
                got,
            });
        }

        info!(segment_count = all_segments.len(), "strip allocated");
        Ok(all_segments)
    }
}

/// Assemble a `ChunkStrip` from allocated segments.
fn assemble_strip(
    segments: &[Segment],
    strip_type: StripAllocType,
    strip_sequence: u32,
    _plan: &PlacementPlan,
) -> ChunkStrip {
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX));

    match strip_type {
        StripAllocType::Mirror { .. } => ChunkStrip {
            chunk_offset: 0,
            strip_sequence,
            unit_kb: 1024, // default 1MB units
            capacity: u32::try_from(segments.len()).unwrap_or(u32::MAX),
            create_ts_ms: now_ms,
            sealed_ts_ms: 0,
            sealed_length: 0,
            strip_type: ProtoStripType::Mirror as i32,
            strip: Some(crow_protocol::chunkdb::rpc::chunk_strip::Strip::MirrorStrip(
                MirrorStrip {
                    segments: segments.to_vec(),
                },
            )),
            usage_bitmap: Vec::new(),
        },
        StripAllocType::Ec { data_num, code_num } => ChunkStrip {
            chunk_offset: 0,
            strip_sequence,
            unit_kb: 1024,
            capacity: u32::try_from(segments.len()).unwrap_or(u32::MAX),
            create_ts_ms: now_ms,
            sealed_ts_ms: 0,
            sealed_length: 0,
            strip_type: ProtoStripType::Ec as i32,
            strip: Some(crow_protocol::chunkdb::rpc::chunk_strip::Strip::EcStrip(
                EcStrip {
                    data_num: u32::try_from(data_num).unwrap_or(u32::MAX),
                    code_num: u32::try_from(code_num).unwrap_or(u32::MAX),
                    ec_state: crow_protocol::chunkdb::rpc::EcState::NoParity as i32,
                    segments: segments.to_vec(),
                },
            )),
            usage_bitmap: Vec::new(),
        },
    }
}
