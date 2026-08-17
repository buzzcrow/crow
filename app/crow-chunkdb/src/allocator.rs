// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Chunk allocator — orchestrates strip layout → selector → parallel
//! diskdb `AllocateBlocks` with rollback on partial failure.
//!
//! Design §8: parallel allocation via `futures::join_all`; rollback
//! frees successfully-allocated segments on any failure.

pub mod pool;

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
    ) -> Result<ChunkStrip, AllocError> {
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
        Ok(strip)
    }

    /// Allocate blocks in parallel across all placement entries.
    ///
    /// Per-instance verification: each diskdb response is checked for
    /// the requested segment count. Partial responses trigger a retry
    /// for just the missing blocks (up to `MAX_ALLOC_RETRIES`). On
    /// final failure, all successfully-allocated segments are freed.
    async fn allocate_blocks_parallel(
        &self,
        owner_chunk: &ChunkId,
        plan: &PlacementPlan,
        unit_count: u32,
    ) -> Result<Vec<Segment>, AllocError> {
        const MAX_ALLOC_RETRIES: usize = 3;

        let mut all_segments: Vec<Segment> = Vec::new();
        // Remaining requests per entry: (disk_group_id, block_count).
        let mut pending: Vec<(u64, u32)> = plan
            .entries
            .iter()
            .map(|e| (e.disk_group_id, e.block_count))
            .collect();

        for attempt in 0..=MAX_ALLOC_RETRIES {
            if pending.is_empty() {
                break;
            }

            let mut futures = Vec::new();
            for (dg_id, count) in &pending {
                let pool = Arc::clone(&self.pool);
                let owner = *owner_chunk;
                let dg = *dg_id;
                let cnt = *count;
                futures.push(async move {
                    pool.allocate_blocks(dg, cnt, unit_count, &owner)
                        .await
                        .map_err(|e| AllocError::AllocateFailed {
                            dg_id: dg,
                            error: e.clone(),
                        })
                });
            }

            let results = join_all(futures).await;

            // Check for hard failures and per-instance count mismatches.
            let mut errors = Vec::new();
            let mut next_pending: Vec<(u64, u32)> = Vec::new();
            for (result, (dg_id, requested)) in results.into_iter().zip(&pending) {
                match result {
                    Ok(resp) => {
                        let got = u32::try_from(resp.segments.len()).unwrap_or(u32::MAX);
                        if got < *requested {
                            warn!(
                                disk_group_id = *dg_id,
                                requested, got, attempt, "partial response from diskdb, will retry missing"
                            );
                            all_segments.extend(resp.segments);
                            next_pending.push((*dg_id, *requested - got));
                        } else {
                            all_segments.extend(resp.segments);
                        }
                    }
                    Err(e) => {
                        errors.push(e);
                    }
                }
            }

            if !errors.is_empty() {
                // Hard failure — free everything allocated so far.
                self.free_all(&all_segments).await;
                return Err(errors.into_iter().next().expect("at least one error"));
            }

            pending = next_pending;
            if !pending.is_empty() && attempt < MAX_ALLOC_RETRIES {
                warn!(
                    pending_count = pending.len(),
                    attempt = attempt + 1,
                    "retrying partial allocation"
                );
            }
        }

        if !pending.is_empty() {
            let expected: u32 = plan.entries.iter().map(|e| e.block_count).sum();
            let got = u32::try_from(all_segments.len()).unwrap_or(u32::MAX);
            warn!(expected, got, "allocation retries exhausted, freeing all");
            self.free_all(&all_segments).await;
            return Err(AllocError::PartialAllocation {
                requested: expected,
                got,
            });
        }

        info!(segment_count = all_segments.len(), "strip allocated");
        Ok(all_segments)
    }

    /// Free all allocated segments (rollback). Logs failures for the
    /// orphan scanner but does not propagate the free error.
    async fn free_all(&self, segments: &[Segment]) {
        if segments.is_empty() {
            return;
        }
        if let Err(rb_err) = self.pool.free_blocks(segments.to_vec()).await {
            warn!(
                error = %rb_err,
                segments = ?segments.iter().map(|s| (&s.disk_id, s.zone_index, s.unit_offset)).collect::<Vec<_>>(),
                "rollback free_blocks failed — orphan segments logged for scanner"
            );
        }
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
