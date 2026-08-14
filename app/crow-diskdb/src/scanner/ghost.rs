// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Ghost allocation + bitmap drift detection. Replays the journal
//! into a throwaway `DdbZone` (strategy 2 with strategy 1 fallback,
//! same as `RecalcEngine`), then does a bit-by-bit diff against the
//! live in-memory bitmap. Each differing bit is classified by
//! checking the record set:
//!
//! - **Real ghost-busy** (drift): bit set in live, clear in replayed,
//!   no `BusyBlockKey` AND no `FreeBlockKey` for that offset.
//! - **Normal uncompacted** (not drift): bit set in live, clear in
//!   replayed, no `BusyBlockKey` but a `FreeBlockKey` exists. This is
//!   expected in the persist-only model — compaction will clear it.
//! - **Ghost-free** (drift, defense-in-depth): bit clear in live, set
//!   in replayed, a `BusyBlockKey` exists.
//!
//! When `auto_correct_drift` is enabled and re-verify confirms the
//! drift is persistent, the live bitmap is corrected via `cas_bit` +
//! `used_count` adjustment. Normal uncompacted bits are never
//! auto-corrected (compaction's job). Fallback (CRC fail / GC gap)
//! suppresses auto-correct (corruption signal).

use std::collections::HashSet;
use std::sync::Arc;

use crow_protocol::common::DiskId;
use crow_protocol::{DiskGroupId, UsageBitmap};

use crate::ddb_kv_client::{Bind, DdbKvClient};
use crate::model::zone::DdbZone;
use crate::recovery::journal_replay::load_zone_inner;
use crate::recovery::{full_scan::rebuild_zone_bitmap_full_scan, ZoneLoadError};
use crate::scanner::FallbackReason;

/// Direction of a ghost block.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GhostDirection {
    /// Bit set in live, clear in replayed, no records — real drift.
    GhostBusy,
    /// Bit clear in live, set in replayed, busy record exists — drift.
    GhostFree,
}

/// One ghost block found by the scan.
#[derive(Debug, Clone, Copy)]
pub struct GhostBlock {
    pub disk_id: DiskId,
    pub zone_index: u32,
    pub unit_offset: u64,
    pub direction: GhostDirection,
}

/// Result of one ghost-scan cycle across all owned zones.
#[derive(Debug, Clone, Default)]
pub struct GhostScanResult {
    /// Real ghost-busy count (drift).
    pub ghost_busy: u64,
    /// Ghost-free count (drift, defense-in-depth).
    pub ghost_free: u64,
    /// Normal uncompacted count (not drift — compaction's job).
    pub uncompacted_lag: u64,
    /// Zones skipped because they are in the active set.
    pub skipped_active: u64,
    /// Zones skipped because the zone lock was held.
    pub skipped_compacting: u64,
    /// Per-block details (capped to avoid unbounded growth in v1).
    pub details: Vec<GhostBlock>,
}

/// Cap on `details` to avoid unbounded memory growth in v1. The
/// counts are always accurate; only the per-block list is capped.
const DETAILS_CAP: usize = 256;

/// Scan all zones in one disk-group for ghost allocations + drift.
///
/// `active_zones` is the set of `Arc<DdbZone>` pointers currently in
/// the disk's `active_zone_context` — zones whose `Arc` pointer
/// matches one in this set are skipped (in-flight allocations).
pub async fn scan_ghosts(
    kv: &DdbKvClient,
    bind: Bind,
    disk_id: DiskId,
    zones: &[(u32, u32, Arc<DdbZone>)],
    active_zones: &[Arc<DdbZone>],
    auto_correct: bool,
    reverify_delay_ms: u32,
) -> GhostScanResult {
    let mut result = GhostScanResult::default();
    for &(zone_idx, unit_capacity, ref live_zone) in zones {
        if active_zones.iter().any(|az| Arc::ptr_eq(az, live_zone)) {
            result.skipped_active += 1;
            continue;
        }
        if live_zone.zone_lock.try_write().is_err() {
            result.skipped_compacting += 1;
            continue;
        }

        let Some((replayed, records, fallback_used)) = replay_zone(
            kv,
            bind,
            disk_id,
            zone_idx,
            live_zone.disk_group_id,
            unit_capacity,
        )
        .await
        else {
            continue;
        };

        let busy_offsets: HashSet<u64> = records.busy.iter().map(|r| r.key.unit_offset).collect();
        let free_offsets: HashSet<u64> = records.free.iter().map(|r| r.key.unit_offset).collect();

        let diff = diff_bitmaps(&live_zone.usage_bits, &replayed.usage_bits, unit_capacity);
        if diff.is_empty() {
            continue;
        }

        let live_words_after_delay: Option<Vec<u64>> = if reverify_delay_ms > 0 {
            tokio::time::sleep(std::time::Duration::from_millis(u64::from(reverify_delay_ms))).await;
            Some(snapshot_live_words(&live_zone.usage_bits))
        } else {
            None
        };

        let lock_guard = live_zone.zone_lock.try_write();
        let can_auto_correct = auto_correct && lock_guard.is_ok();
        let _guard = lock_guard.ok();

        classify_and_correct_diff(
            &diff,
            &busy_offsets,
            &free_offsets,
            live_words_after_delay.as_ref(),
            live_zone,
            can_auto_correct,
            fallback_used.is_some(),
            disk_id,
            zone_idx,
            &mut result,
        );
    }
    result
}

/// Replay the journal into a throwaway zone (strategy 2 with strategy
/// 1 fallback) + load the zone's records for classification. Returns
/// `None` if both strategies fail.
async fn replay_zone(
    kv: &DdbKvClient,
    bind: Bind,
    disk_id: DiskId,
    zone_idx: u32,
    disk_group_id: DiskGroupId,
    unit_capacity: u32,
) -> Option<(
    DdbZone,
    crate::model::records::ZoneRecords,
    Option<FallbackReason>,
)> {
    match load_zone_inner(kv, bind, disk_id, zone_idx, disk_group_id, unit_capacity)
        .await
        .map(|(z, ts, _)| (z, ts))
    {
        Ok((z, _)) => {
            let recs = kv
                .read_zone_records(bind, &disk_id, zone_idx)
                .await
                .unwrap_or_default();
            Some((z, recs, None))
        }
        Err(ZoneLoadError::JournalScanGcGap) => {
            let (z, _) =
                rebuild_zone_bitmap_full_scan(kv, bind, disk_id, zone_idx, disk_group_id, unit_capacity)
                    .await
                    .ok()?;
            let recs = kv
                .read_zone_records(bind, &disk_id, zone_idx)
                .await
                .unwrap_or_default();
            Some((z, recs, Some(FallbackReason::JournalScanGcGap)))
        }
        Err(ZoneLoadError::SnapshotCrcFail) => {
            let (z, _) =
                rebuild_zone_bitmap_full_scan(kv, bind, disk_id, zone_idx, disk_group_id, unit_capacity)
                    .await
                    .ok()?;
            let recs = kv
                .read_zone_records(bind, &disk_id, zone_idx)
                .await
                .unwrap_or_default();
            Some((z, recs, Some(FallbackReason::SnapshotCrcFail)))
        }
        Err(_) => None,
    }
}

/// Classify each diff bit + apply auto-correct. Synchronous (no
/// awaits) — called under the zone write lock.
#[allow(clippy::too_many_arguments)]
fn classify_and_correct_diff(
    diff: &[(u32, bool)],
    busy_offsets: &HashSet<u64>,
    free_offsets: &HashSet<u64>,
    live_words_after_delay: Option<&Vec<u64>>,
    live_zone: &DdbZone,
    can_auto_correct: bool,
    fallback_used: bool,
    disk_id: DiskId,
    zone_idx: u32,
    result: &mut GhostScanResult,
) {
    for (bit_idx, is_ghost_busy) in diff {
        let bit = u64::from(*bit_idx);
        let has_busy = busy_offsets.contains(&bit);
        let has_free = free_offsets.contains(&bit);

        if *is_ghost_busy {
            if !has_busy && !has_free {
                if let Some(words) = live_words_after_delay {
                    let w_idx = *bit_idx as usize / 64;
                    let mask = 1u64 << (*bit_idx % 64);
                    if w_idx >= words.len() || words[w_idx] & mask == 0 {
                        continue;
                    }
                }
                result.ghost_busy += 1;
                push_detail(
                    &mut result.details,
                    disk_id,
                    zone_idx,
                    bit,
                    GhostDirection::GhostBusy,
                );
                if can_auto_correct && !fallback_used && live_zone.usage_bits.cas_bit(*bit_idx, false) {
                    live_zone
                        .used_count
                        .fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
                }
            } else if !has_busy && has_free {
                result.uncompacted_lag += 1;
            }
        } else if has_busy {
            if let Some(words) = live_words_after_delay {
                let w_idx = *bit_idx as usize / 64;
                let mask = 1u64 << (*bit_idx % 64);
                if w_idx < words.len() && words[w_idx] & mask != 0 {
                    continue;
                }
            }
            result.ghost_free += 1;
            push_detail(
                &mut result.details,
                disk_id,
                zone_idx,
                bit,
                GhostDirection::GhostFree,
            );
            if can_auto_correct && !fallback_used && live_zone.usage_bits.cas_bit(*bit_idx, true) {
                live_zone
                    .used_count
                    .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
            }
        }
    }
}

/// One bit-level difference between two bitmaps.
/// `(bit_index, is_ghost_busy)` — `is_ghost_busy` is true when the
/// bit is set in live but clear in replayed.
type BitDiff = (u32, bool);

/// Compute the bit-level diff between two bitmaps up to `unit_capacity`.
#[cfg_attr(not(feature = "test-util"), allow(dead_code))]
pub fn diff_bitmaps(live: &UsageBitmap, replayed: &UsageBitmap, unit_capacity: u32) -> Vec<BitDiff> {
    let mut diffs = Vec::new();
    let word_count = live.word_count().max(replayed.word_count());
    for i in 0..word_count {
        let live_word = live.load_word(i);
        let rep_word = replayed.load_word(i);
        let xor = live_word ^ rep_word;
        if xor == 0 {
            continue;
        }
        for bit in 0u32..64 {
            let mask = 1u64 << bit;
            if xor & mask == 0 {
                continue;
            }
            let abs_bit = i * 64 + bit as usize;
            #[allow(clippy::cast_possible_truncation)]
            if abs_bit as u32 >= unit_capacity {
                break;
            }
            let is_ghost_busy = live_word & mask != 0;
            #[allow(clippy::cast_possible_truncation)]
            diffs.push((abs_bit as u32, is_ghost_busy));
        }
    }
    diffs
}

/// Snapshot all words of a live bitmap (for re-verify).
fn snapshot_live_words(bits: &UsageBitmap) -> Vec<u64> {
    let wc = bits.word_count();
    (0..wc).map(|i| bits.load_word(i)).collect()
}

/// Push a detail entry, respecting the cap.
fn push_detail(
    details: &mut Vec<GhostBlock>,
    disk_id: DiskId,
    zone_index: u32,
    unit_offset: u64,
    dir: GhostDirection,
) {
    if details.len() < DETAILS_CAP {
        details.push(GhostBlock {
            disk_id,
            zone_index,
            unit_offset,
            direction: dir,
        });
    }
}
