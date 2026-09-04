// Copyright 2026-present Gian <crow.db@outlook.com>
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

use crowdb_protocol::common::{ChunkId, DiskId};
use crowdb_protocol::diskdb::rpc::{BlockState, BusyBlockValue, CommitState, FreeBlockValue, Segment};

use crate::ddb_kv_client::{Bind, DdbKvClient};
use crate::model::disk_group::{AllocClaim, AllocError, DdbDiskGroup};
use crate::recovery::compaction::compact_zone;

/// Elapsed nanoseconds as u64 (saturating cast from u128).
fn elapsed_ns(start: std::time::Instant) -> u64 {
    start.elapsed().as_nanos().try_into().unwrap_or(u64::MAX)
}

/// Errors from the free path when `validate_owner_on_free` is enabled.
#[derive(Debug)]
pub enum FreeError {
    /// KV client error during validation or persist.
    Kv(crowdb_kv_client::Error),
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

impl From<crowdb_kv_client::Error> for FreeError {
    fn from(e: crowdb_kv_client::Error) -> Self {
        Self::Kv(e)
    }
}

/// Synchronous compaction fallback: when `dg.allocate_block` returns
/// `NoSpace`, compact non-active zones on all disks in the disk-group
/// (up to `zone_rotate_count` per disk), then retry. This reclaims
/// freed space that hasn't been compacted yet (persist-only free
/// leaves bits set). See §5 Preparatory thread — Fallback.
async fn compact_fallback(
    dg: &Arc<DdbDiskGroup>,
    kv: &DdbKvClient,
    zone_rotate_count: u32,
    metrics: &crate::metrics::DiskdbMetrics,
) {
    let bind = dg.bind();
    let disks = dg.disks.read().unwrap().clone();
    for disk in disks {
        // Collect active zone indices to skip (I4).
        let active_zone_indices: std::collections::HashSet<u32> = {
            let active = disk.active_zone_context.load();
            active.iter().map(|z| z.zone_index).collect()
        };
        let zones = disk.zones.load_full();
        let mut compacted = 0u32;
        for zone in zones.iter() {
            if compacted >= zone_rotate_count {
                break;
            }
            // Skip active zones — no concurrent allocate (I4).
            if active_zone_indices.contains(&zone.zone_index) {
                continue;
            }
            // Skip zones that are already ready.
            if zone.compacted_ready.load(std::sync::atomic::Ordering::Acquire) {
                continue;
            }
            if let Err(e) = compact_zone(kv, bind, disk.disk_id, zone, zone.zone_index, metrics).await {
                tracing::warn!(
                    disk_id = ?disk.disk_id,
                    zone_index = zone.zone_index,
                    error = %e,
                    "synchronous compaction fallback failed"
                );
            } else {
                compacted += 1;
            }
        }
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
/// Synchronous compaction fallback: if Phase 1 returns `NoSpace`,
/// compacts non-active zones on all disks (reclaiming freed space),
/// then retries Phase 1 once. See §5 Preparatory thread — Fallback.
///
/// # Errors
/// Returns `AllocError::NoSpace` if no disk/zone can satisfy the
/// request (even after compaction fallback), or a KV client error if
/// the persist fails.
#[allow(clippy::too_many_arguments)]
pub async fn allocate_block(
    dg: &Arc<DdbDiskGroup>,
    unit_count: u32,
    owner_chunk: &ChunkId,
    unit_size: u32,
    kv: &DdbKvClient,
    cas_retry_limit: u32,
    zone_rotate_count: u32,
    metrics: &crate::metrics::DiskdbMetrics,
) -> std::result::Result<Segment, AllocError> {
    // Phase 1: bitmap CAS.
    let (disk, zone, range) = match dg.allocate_block(unit_count, &[], cas_retry_limit, zone_rotate_count) {
        Ok(claim) => claim,
        Err(AllocError::NoSpace) => {
            // Synchronous compaction fallback: compact non-active
            // zones to reclaim freed space, then retry once.
            tracing::info!("allocate NoSpace — running synchronous compaction fallback");
            compact_fallback(dg, kv, zone_rotate_count, metrics).await;
            dg.allocate_block(unit_count, &[], cas_retry_limit, zone_rotate_count)?
        }
        Err(error @ AllocError::Persistence) => return Err(error),
    };

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
        commit_state: CommitState::Tentative as i32,
    };
    let bind = dg.bind();
    if let Err(e) = kv
        .persist_busy(bind, &disk.disk_id, zone.zone_index, range.unit_offset, &value)
        .await
    {
        // Rollback Phase 1.
        let _ = zone.rollback_allocate(range.unit_offset, range.unit_count);
        tracing::warn!("allocate persist failed, rolled back bitmap: {e}");
        return Err(AllocError::Persistence);
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
/// Synchronous compaction fallback: if Phase 1 cannot place all
/// `count` blocks, compacts non-active zones on all disks (reclaiming
/// freed space), then retries Phase 1 once.
///
/// # Errors
/// Returns `AllocError::NoSpace` if not all `count` blocks can be
/// placed (even after compaction fallback), or a KV client error if
/// the batch persist fails.
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
    metrics: &crate::metrics::DiskdbMetrics,
) -> std::result::Result<Vec<Segment>, AllocError> {
    // Phase 1: bitmap CAS for all blocks.
    let phase1_start = std::time::Instant::now();
    let claims: Vec<AllocClaim> = match dg.allocate_blocks(
        unit_count,
        count,
        exclude_disks,
        cas_retry_limit,
        zone_rotate_count,
    ) {
        Ok(claims) if claims.len() == count as usize => claims,
        Ok(claims) => claims,
        Err(AllocError::NoSpace) => {
            // No space at all — try compaction fallback then retry.
            tracing::info!("allocate_blocks NoSpace — running synchronous compaction fallback");
            compact_fallback(dg, kv, zone_rotate_count, metrics).await;
            dg.allocate_blocks(
                unit_count,
                count,
                exclude_disks,
                cas_retry_limit,
                zone_rotate_count,
            )?
        }
        Err(error @ AllocError::Persistence) => return Err(error),
    };

    // Record per-disk event counters after Phase 1 CAS succeeds.
    for (disk, _zone, range) in &claims {
        if let Some(m) = &disk.metrics {
            m.record_allocate(range.unit_count, unit_size);
        }
    }
    metrics
        .allocate_bitmap_scan_latency
        .observe(elapsed_ns(phase1_start));

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
                    commit_state: CommitState::Tentative as i32,
                },
            )
        })
        .collect();

    let bind = dg.bind();
    let phase2_start = std::time::Instant::now();
    if let Err(e) = kv.persist_busy_batch(bind, &records).await {
        // Rollback ALL Phase 1 claims.
        for (_, zone, range) in &claims {
            let _ = zone.rollback_allocate(range.unit_offset, range.unit_count);
        }
        tracing::warn!("allocate_blocks persist failed, rolled back {count} claims: {e}");
        metrics.allocate_errors_total.inc();
        return Err(AllocError::Persistence);
    }
    metrics
        .allocate_kv_persist_latency
        .observe(elapsed_ns(phase2_start));

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
/// The free path is **persist-only**: one `batch_write` (Delete
/// `BusyBlockKey` + Put `FreeBlockValue`) makes the block free on
/// disk. The in-memory bitmap is **not** touched — the bit stays set,
/// `used_count` is not decremented (I1). Compaction is the sole
/// bit-clearer for freed blocks (I3); `rollback_allocate` is the
/// allocate-only bitmap clear and is never used here.
///
/// Phase 0 (optional): when `validate_owner_on_free` is `true`, read
/// the `BusyBlockValue` from the data group and validate `owner_chunk`
/// before persisting. Rejects on `NotBusy` or `OwnerMismatch`. When
/// `false` (default), no KV read — `owner_chunk` comes from the
/// `Segment`.
/// Phase 1: persist `FreeBlockValue` (delete `BusyBlockKey` + put
/// `FreeBlockKey` in one `batch_write`) — durable free first.
/// Post-persist: increment `uncompacted_free_record_count` on the zone
/// so compaction knows there is work to do.
///
/// If the persist fails, the block is still busy on both disk and
/// memory — the caller can retry safely. If the persist succeeds but
/// the in-memory zone lookup fails (rare: disk removed concurrently),
/// the free record is durable but the backlog counter is not bumped;
/// the periodic compaction cadence still reclaims the block.
///
/// # Errors
/// Returns `FreeError::NotBusy` / `FreeError::OwnerMismatch` on
/// validation failure (no persist). Returns `FreeError::Kv` if the
/// persist fails — the block is still busy, the caller can retry.
pub async fn free_block(
    dg: &Arc<DdbDiskGroup>,
    segment: &Segment,
    kv: &DdbKvClient,
    validate_owner_on_free: bool,
) -> std::result::Result<(), FreeError> {
    let disk_id = segment.disk_id.ok_or_else(|| {
        FreeError::Kv(crowdb_kv_client::Error::SysdataDecode {
            key: "segment.disk_id".to_string(),
            reason: "missing disk_id in Segment".to_string(),
        })
    })?;
    let bind: Bind = dg.bind();

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
    // Persist succeeded — the block is free on disk. The in-memory
    // bitmap is untouched (persist-only, I1); compaction reconciles.

    // Post-persist: bump the zone's uncompacted-free backlog. The
    // bitmap is NOT mutated — free is persist-only.
    if !dg.free_block(
        &disk_id,
        segment.zone_index,
        segment.unit_offset,
        segment.unit_count,
    ) {
        // Persist succeeded but the in-memory zone was not found
        // (rare: disk removed concurrently). The free record is
        // durable; the periodic compaction cadence still reclaims the
        // block. The caller's intent was achieved.
        tracing::warn!(
            "free persist succeeded but in-memory zone not found for disk {disk_id:?} zone {} offset {} — backlog counter not bumped",
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
/// Persist-only free (same contract as `free_block`): one `batch_write`
/// deletes all `BusyBlockKey`s and puts all `FreeBlockValue`s. The
/// in-memory bitmaps are **not** touched — bits stay set, `used_count`
/// is not decremented (I1); compaction is the sole bit-clearer (I3).
///
/// When `validate_owner_on_free` is `true`, all segments are validated
/// first (all-or-nothing) — if any segment fails validation, no
/// persist happens.
///
/// If the persist fails, no in-memory state changed — all blocks are
/// still busy and the caller can retry safely.
///
/// # Errors
/// Returns `FreeError::NotBusy` / `FreeError::OwnerMismatch` on
/// validation failure (no persist). Returns `FreeError::Kv` if the
/// persist fails — all blocks are still busy, the caller can retry.
pub async fn free_blocks(
    dg: &Arc<DdbDiskGroup>,
    segments: &[Segment],
    kv: &DdbKvClient,
    validate_owner_on_free: bool,
) -> std::result::Result<(), FreeError> {
    let bind: Bind = dg.bind();

    // Phase 0: validate ownership for all segments (all-or-nothing).
    if validate_owner_on_free {
        for seg in segments {
            let disk_id = seg.disk_id.ok_or_else(|| {
                FreeError::Kv(crowdb_kv_client::Error::SysdataDecode {
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
    // Persist succeeded — all blocks are free on disk. The in-memory
    // bitmaps are untouched (persist-only, I1); compaction reconciles.

    // Post-persist: bump each zone's uncompacted-free backlog. The
    // bitmaps are NOT mutated — free is persist-only.
    for seg in segments {
        if let Some(disk_id) = seg.disk_id {
            if !dg.free_block(&disk_id, seg.zone_index, seg.unit_offset, seg.unit_count) {
                // Persist succeeded but the in-memory zone was not
                // found (rare: disk removed concurrently). The free
                // record is durable; the periodic compaction cadence
                // still reclaims the block.
                tracing::warn!(
                    "free persist succeeded but in-memory zone not found for disk {disk_id:?} zone {} offset {} — backlog counter not bumped",
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

/// Commit blocks — mark previously-allocated blocks as permanent.
///
/// For each segment, reads the current `BusyBlockValue`, sets
/// `commit_state = COMMITTED`, and persists the update in one
/// `batch_write`. Tentative blocks not committed within a timeout are
/// reclaimable by the orphan scanner.
///
/// # Errors
/// Returns `FreeError::NotBusy` if a segment has no busy-block record.
/// Returns `FreeError::Kv` if the read or persist fails.
pub async fn commit_blocks(
    dg: &Arc<DdbDiskGroup>,
    segments: &[Segment],
    kv: &DdbKvClient,
) -> std::result::Result<u32, FreeError> {
    let bind: Bind = dg.bind();

    // Read each busy block and prepare the updated value.
    let mut records: Vec<(DiskId, u32, u64, BusyBlockValue)> = Vec::with_capacity(segments.len());
    for seg in segments {
        let disk_id = seg.disk_id.ok_or_else(|| {
            FreeError::Kv(crowdb_kv_client::Error::SysdataDecode {
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
            Some(mut bv) => {
                bv.commit_state = CommitState::Committed as i32;
                records.push((disk_id, seg.zone_index, seg.unit_offset, bv));
            }
        }
    }

    if records.is_empty() {
        return Ok(0);
    }

    // Persist all updates in one batch.
    kv.persist_busy_batch(bind, &records)
        .await
        .map_err(FreeError::from)?;

    Ok(u32::try_from(records.len()).unwrap_or(u32::MAX))
}
