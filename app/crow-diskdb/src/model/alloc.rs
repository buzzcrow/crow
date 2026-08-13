// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Two-phase allocate/free orchestration — coordinates the in-memory
//! `DdbDiskGroup`/`DdbDisk`/`DdbZone` model with durable writes via
//! `DdbKvClient`.
//!
//! Phase 1 (sync): bitmap CAS on the in-memory zone.
//! Phase 2 (async): persist the durable record via `DdbKvClient`.
//!
//! On Phase 2 failure, rolls back the bitmap bits (Phase 1 undo).

use std::sync::Arc;

use crow_protocol::common::{ChunkId, DiskId};
use crow_protocol::diskdb::rpc::{BlockState, BusyBlockValue, FreeBlockValue, Segment};

use crate::ddb_kv_client::{Bind, DdbKvClient};
use crate::model::disk_group::{AllocClaim, AllocError, DdbDiskGroup};

/// Errors from the free path when `validate_owner_on_free` is enabled.
#[derive(Debug)]
pub enum FreeError {
    /// KV client error during validation or persist.
    Kv(crow_kv_client::Error),
    /// Block is not busy (no `BusyBlockKey` exists) — double-free or
    /// never allocated.
    NotBusy {
        disk_id: DiskId,
        zone_index: u32,
        unit_offset: u64,
    },
    /// `owner_chunk` in the `BusyBlockValue` does not match the
    /// `Segment`'s `owner_chunk` — ownership mismatch.
    OwnerMismatch {
        disk_id: DiskId,
        zone_index: u32,
        unit_offset: u64,
        expected: ChunkId,
        actual: ChunkId,
    },
}

impl std::fmt::Display for FreeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Kv(e) => write!(f, "kv error: {e}"),
            Self::NotBusy {
                disk_id,
                zone_index,
                unit_offset,
            } => write!(
                f,
                "block not busy: disk {disk_id:?} zone {zone_index} offset {unit_offset}"
            ),
            Self::OwnerMismatch {
                disk_id,
                zone_index,
                unit_offset,
                expected,
                actual,
            } => write!(
                f,
                "owner mismatch: disk {disk_id:?} zone {zone_index} offset {unit_offset}, expected {expected:?} actual {actual:?}"
            ),
        }
    }
}

impl std::error::Error for FreeError {}

impl From<crow_kv_client::Error> for FreeError {
    fn from(e: crow_kv_client::Error) -> Self {
        Self::Kv(e)
    }
}

/// Two-phase allocate a single block.
///
/// Phase 1 (sync): bitmap CAS via `dg.allocate_block`.
/// Phase 2 (async): persist `BusyBlockValue` via `DdbKvClient`.
///
/// On Phase 2 failure, rolls back the bitmap bits (Phase 1 undo) and
/// returns the error. See §4.5.
///
/// # Errors
/// Returns `AllocError::NoSpace` if no disk/zone can satisfy the
/// request, or a KV client error if the persist fails.
#[allow(clippy::too_many_arguments)]
pub async fn allocate_block(
    dg: &Arc<DdbDiskGroup>,
    unit_count: u32,
    owner_chunk: &ChunkId,
    unit_size: u32,
    kv: &DdbKvClient,
    cas_retry_limit: u32,
    zone_rotate_count: u32,
) -> std::result::Result<Segment, AllocError> {
    // Phase 1: bitmap CAS.
    let (disk, zone, range) = dg.allocate_block(unit_count, &[], cas_retry_limit, zone_rotate_count)?;

    // Record per-disk event counter after Phase 1 CAS succeeds.
    if let Some(m) = &disk.metrics {
        m.record_allocate(range.unit_count, unit_size);
    }

    // Phase 2: persist BusyBlockValue.
    let value = BusyBlockValue {
        unit_count: range.unit_count,
        owner_chunk: Some(*owner_chunk),
        unit_size,
        state: BlockState::Ok as i32,
    };
    let bind = *dg.bind.read().unwrap();
    if let Err(e) = kv
        .persist_busy(bind, &disk.disk_id, zone.zone_index, range.unit_offset, &value)
        .await
    {
        // Rollback Phase 1.
        let _ = zone.rollback_allocate(range.unit_offset, range.unit_count);
        tracing::warn!("allocate persist failed, rolled back bitmap: {e}");
        return Err(AllocError::NoSpace);
    }

    Ok(Segment {
        disk_id: Some(disk.disk_id),
        zone_index: zone.zone_index,
        unit_offset: range.unit_offset,
        unit_count: range.unit_count,
        owner_chunk: Some(*owner_chunk),
    })
}

/// Two-phase allocate multiple blocks (one `batch_write` per data
/// group). See §4.5.
///
/// # Errors
/// Returns `AllocError::NoSpace` if not all `count` blocks can be
/// placed, or a KV client error if the batch persist fails.
#[allow(clippy::too_many_arguments)]
pub async fn allocate_blocks(
    dg: &Arc<DdbDiskGroup>,
    unit_count: u32,
    count: u32,
    exclude_disks: &[DiskId],
    owner_chunk: &ChunkId,
    unit_size: u32,
    kv: &DdbKvClient,
    cas_retry_limit: u32,
    zone_rotate_count: u32,
) -> std::result::Result<Vec<Segment>, AllocError> {
    // Phase 1: bitmap CAS for all blocks.
    let claims: Vec<AllocClaim> = dg.allocate_blocks(
        unit_count,
        count,
        exclude_disks,
        cas_retry_limit,
        zone_rotate_count,
    )?;

    // Record per-disk event counters after Phase 1 CAS succeeds.
    for (disk, _zone, range) in &claims {
        if let Some(m) = &disk.metrics {
            m.record_allocate(range.unit_count, unit_size);
        }
    }

    // Phase 2: persist all in one batch_write.
    let records: Vec<(DiskId, u32, u64, BusyBlockValue)> = claims
        .iter()
        .map(|(disk, zone, range)| {
            (
                disk.disk_id,
                zone.zone_index,
                range.unit_offset,
                BusyBlockValue {
                    unit_count: range.unit_count,
                    owner_chunk: Some(*owner_chunk),
                    unit_size,
                    state: BlockState::Ok as i32,
                },
            )
        })
        .collect();

    let bind = *dg.bind.read().unwrap();
    if let Err(e) = kv.persist_busy_batch(bind, &records).await {
        // Rollback ALL Phase 1 claims.
        for (_, zone, range) in &claims {
            let _ = zone.rollback_allocate(range.unit_offset, range.unit_count);
        }
        tracing::warn!("allocate_blocks persist failed, rolled back {count} claims: {e}");
        return Err(AllocError::NoSpace);
    }

    let segments: Vec<Segment> = claims
        .iter()
        .map(|(disk, zone, range)| Segment {
            disk_id: Some(disk.disk_id),
            zone_index: zone.zone_index,
            unit_offset: range.unit_offset,
            unit_count: range.unit_count,
            owner_chunk: Some(*owner_chunk),
        })
        .collect();
    Ok(segments)
}

// ── Immediate free ──────────────────────────────────────────────

/// Free a single block. v1: synchronous (no batch, no timer).
///
/// Phase 0 (optional): when `validate_owner_on_free` is `true`, read
/// the `BusyBlockValue` from the data group and validate `owner_chunk`
/// before touching the bitmap. Rejects on `NotBusy` or `OwnerMismatch`.
/// When `false` (default), no KV read — `owner_chunk` comes from the
/// `Segment` (§14).
/// Phase 1: persist `FreeBlockValue` (delete `BusyBlockKey` + put
/// `FreeBlockKey` in one `batch_write`) — durable free first.
/// Phase 2: clear bitmap locally (per-bit CAS).
///
/// Persist-before-bitmap-clear prevents the double-allocate window: if
/// the persist fails, the bit stays set and the allocator will not
/// re-hand the block to another caller. The caller gets an error and
/// can retry safely (the block is still busy on both disk and memory).
/// If the persist succeeds but the bitmap clear fails (rare: race or
/// zone lookup failure), the block is free on disk but still shows busy
/// in memory — the stale bit prevents re-allocation until the §12
/// scanner reconciles (ghost-busy, self-correcting).
///
/// # Errors
/// Returns `FreeError::NotBusy` / `FreeError::OwnerMismatch` on
/// validation failure (no persist, no bitmap clear). Returns
/// `FreeError::Kv` if the persist fails — the bitmap was NOT cleared;
/// the block is still busy, the caller can retry.
pub async fn free_block(
    dg: &Arc<DdbDiskGroup>,
    segment: &Segment,
    kv: &DdbKvClient,
    validate_owner_on_free: bool,
) -> std::result::Result<(), FreeError> {
    let disk_id = segment.disk_id.ok_or_else(|| {
        FreeError::Kv(crow_kv_client::Error::SysdataDecode {
            key: "segment.disk_id".to_string(),
            reason: "missing disk_id in Segment".to_string(),
        })
    })?;
    let bind: Bind = *dg.bind.read().unwrap();

    // Phase 0: validate ownership (optional, one paxos round-trip).
    if validate_owner_on_free {
        let busy = kv
            .get_busy(bind, &disk_id, segment.zone_index, segment.unit_offset)
            .await?;
        match busy {
            None => {
                return Err(FreeError::NotBusy {
                    disk_id,
                    zone_index: segment.zone_index,
                    unit_offset: segment.unit_offset,
                });
            }
            Some(bv) => {
                if bv.owner_chunk != segment.owner_chunk {
                    return Err(FreeError::OwnerMismatch {
                        disk_id,
                        zone_index: segment.zone_index,
                        unit_offset: segment.unit_offset,
                        expected: segment.owner_chunk.unwrap_or_default(),
                        actual: bv.owner_chunk.unwrap_or_default(),
                    });
                }
            }
        }
    }

    // Phase 1: persist FreeBlockValue (durable free first).
    let value = FreeBlockValue {
        unit_count: segment.unit_count,
        previous_owner: segment.owner_chunk,
        freed_ts: dg.next_freed_ts(),
    };
    kv.persist_free(bind, &disk_id, segment.zone_index, segment.unit_offset, &value)
        .await
        .map_err(FreeError::from)?;
    // Persist succeeded — the block is free on disk. If we crash now,
    // R73 recovery sees no BusyBlockKey → bit is clear → block is free.

    // Phase 2: clear bitmap locally.
    if !dg.free_block(
        &disk_id,
        segment.zone_index,
        segment.unit_offset,
        segment.unit_count,
    ) {
        // Persist succeeded but bitmap clear failed (rare: double-free
        // race or zone lookup failure). The block is free on disk; the
        // stale in-memory bit prevents re-allocation until the §12
        // scanner reconciles. The caller's intent was achieved.
        tracing::warn!(
            "free persist succeeded but bitmap clear failed for disk {disk_id:?} zone {} offset {} — ghost-busy, scanner will reconcile",
            segment.zone_index,
            segment.unit_offset
        );
    }

    // Record per-disk event counter after durable free.
    let unit_size = dg.disk_unit_size(disk_id).unwrap_or(0);
    if let Some(m) = dg.disk_metrics(disk_id) {
        m.record_free(segment.unit_count, unit_size);
    }

    Ok(())
}

/// Free multiple blocks (one `batch_write` per data group).
///
/// When `validate_owner_on_free` is `true`, all segments are validated
/// first (all-or-nothing) — if any segment fails validation, no persist
/// and no bitmap clear happens.
///
/// Persist-before-bitmap-clear (same ordering as `free_block`): the
/// `batch_write` persists all `FreeBlockValue`s first, then the bitmap
/// bits are cleared. If the persist fails, no bits are cleared — the
/// blocks stay busy and the caller can retry safely.
///
/// # Errors
/// Returns `FreeError::NotBusy` / `FreeError::OwnerMismatch` on
/// validation failure (no persist, no bitmap clear). Returns
/// `FreeError::Kv` if the persist fails — no bitmap clears happened;
/// all blocks are still busy, the caller can retry.
pub async fn free_blocks(
    dg: &Arc<DdbDiskGroup>,
    segments: &[Segment],
    kv: &DdbKvClient,
    validate_owner_on_free: bool,
) -> std::result::Result<(), FreeError> {
    let bind: Bind = *dg.bind.read().unwrap();

    // Phase 0: validate ownership for all segments (all-or-nothing).
    if validate_owner_on_free {
        for seg in segments {
            let disk_id = seg.disk_id.ok_or_else(|| {
                FreeError::Kv(crow_kv_client::Error::SysdataDecode {
                    key: "segment.disk_id".to_string(),
                    reason: "missing disk_id in Segment".to_string(),
                })
            })?;
            let busy = kv
                .get_busy(bind, &disk_id, seg.zone_index, seg.unit_offset)
                .await?;
            match busy {
                None => {
                    return Err(FreeError::NotBusy {
                        disk_id,
                        zone_index: seg.zone_index,
                        unit_offset: seg.unit_offset,
                    });
                }
                Some(bv) => {
                    if bv.owner_chunk != seg.owner_chunk {
                        return Err(FreeError::OwnerMismatch {
                            disk_id,
                            zone_index: seg.zone_index,
                            unit_offset: seg.unit_offset,
                            expected: seg.owner_chunk.unwrap_or_default(),
                            actual: bv.owner_chunk.unwrap_or_default(),
                        });
                    }
                }
            }
        }
    }

    // Phase 1: persist all in one batch_write (durable free first).
    let records: Vec<(DiskId, u32, u64, FreeBlockValue)> = segments
        .iter()
        .filter_map(|seg| {
            seg.disk_id.map(|disk_id| {
                (
                    disk_id,
                    seg.zone_index,
                    seg.unit_offset,
                    FreeBlockValue {
                        unit_count: seg.unit_count,
                        previous_owner: seg.owner_chunk,
                        freed_ts: dg.next_freed_ts(),
                    },
                )
            })
        })
        .collect();

    if records.is_empty() {
        return Ok(());
    }

    kv.persist_free_batch(bind, &records)
        .await
        .map_err(FreeError::from)?;
    // Persist succeeded — all blocks are free on disk. If we crash now,
    // R73 recovery sees no BusyBlockKey → bits are clear → blocks are free.

    // Phase 2: clear all bitmaps locally.
    for seg in segments {
        if let Some(disk_id) = seg.disk_id {
            if !dg.free_block(&disk_id, seg.zone_index, seg.unit_offset, seg.unit_count) {
                // Persist succeeded but bitmap clear failed (rare:
                // double-free race or zone lookup failure). The block is
                // free on disk; the stale in-memory bit prevents
                // re-allocation until the §12 scanner reconciles.
                tracing::warn!(
                    "free persist succeeded but bitmap clear failed for disk {disk_id:?} zone {} offset {} — ghost-busy, scanner will reconcile",
                    seg.zone_index,
                    seg.unit_offset
                );
            }
            // Record per-disk event counter after durable free.
            let unit_size = dg.disk_unit_size(disk_id).unwrap_or(0);
            if let Some(m) = dg.disk_metrics(disk_id) {
                m.record_free(seg.unit_count, unit_size);
            }
        }
    }

    Ok(())
}
