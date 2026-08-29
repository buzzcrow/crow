// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! R74 §5 — recalculation verification. Replays the journal into a
//! **separate** bitmap per zone (reusing `load_zone_inner`,
//! strategy 2, with `rebuild_zone_bitmap_full_scan` strategy 1
//! fallback) and compares against the live `DdbZone` to detect drift.
//! v1 reports drift only — it does not auto-correct the live bitmap.

use std::sync::Arc;

use crowdb_protocol::common::DiskId;
use crowdb_protocol::DiskGroupId;

use crate::ddb_kv_client::{Bind, DdbKvClient};
use crate::model::disk_group_container::DdbDiskGroupContainer;
use crate::model::zone::DdbZone;
use crate::recovery::journal_replay::load_zone_inner;
use crate::recovery::ZoneLoadError;

/// Per-disk zone list: `(zone_index, unit_capacity, zone_arc)`.
type ZoneList = Vec<(u32, u32, Arc<DdbZone>)>;

/// Why strategy 2 fell back to strategy 1 for a zone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FallbackReason {
    JournalScanGcGap,
    SnapshotCrcFail,
}

impl FallbackReason {
    fn as_str(self) -> &'static str {
        match self {
            Self::JournalScanGcGap => "journal_scan_gc_gap",
            Self::SnapshotCrcFail => "snapshot_crc_fail",
        }
    }
}

/// Per-zone recalc result.
#[derive(Debug, Clone)]
pub struct RecalcResult {
    pub disk_id: DiskId,
    pub zone_index: u32,
    pub matches: bool,
    pub drift_detected: bool,
    pub live_busy_blocks: u32,
    pub replayed_busy_blocks: u32,
    pub live_snapshot_slot: u64,
    pub replayed_snapshot_slot: u64,
    pub fallback_used: Option<FallbackReason>,
}

/// Per-disk-group recalc result.
#[derive(Debug, Clone)]
pub struct DiskGroupRecalcResult {
    pub disk_group_id: DiskGroupId,
    pub zone_results: Vec<RecalcResult>,
    pub drift_detected: bool,
}

/// Recalculation verification engine. Owns a `DdbKvClient` (for journal
/// replay) + a reference to the `DdbDiskGroupContainer` (to read live
/// zones).
pub struct RecalcEngine {
    kv: Arc<DdbKvClient>,
    container: Arc<DdbDiskGroupContainer>,
}

impl RecalcEngine {
    #[must_use]
    pub fn new(kv: Arc<DdbKvClient>, container: Arc<DdbDiskGroupContainer>) -> Self {
        Self { kv, container }
    }

    /// Recalc one zone: replay the journal into a throwaway `DdbZone`,
    /// compare its popcount against the live zone's `used_count`.
    ///
    /// `live_zone` is the live in-memory zone; it is **not** mutated
    /// (v1 reports drift only).
    pub async fn recalc_zone(
        &self,
        bind: Bind,
        disk_id: DiskId,
        zone_idx: u32,
        unit_capacity: u32,
        live_zone: &DdbZone,
    ) -> RecalcResult {
        let live_busy_blocks = live_zone.busy_blocks();
        let live_snapshot_slot = live_zone.snapshot_slot.load(std::sync::atomic::Ordering::Acquire);

        // Strategy 2: journal replay into a throwaway zone.
        let (replayed_busy_blocks, replayed_snapshot_slot, fallback_used) = match load_zone_inner(
            &self.kv,
            bind,
            disk_id,
            zone_idx,
            live_zone.disk_group_id,
            unit_capacity,
        )
        .await
        .map(|(z, ts, _)| (z, ts))
        {
            Ok((replayed, _max_freed_ts)) => {
                let popcount = replayed.usage_bits.count_set();
                let slot = replayed.snapshot_slot.load(std::sync::atomic::Ordering::Acquire);
                (u32::try_from(popcount).unwrap_or(u32::MAX), slot, None)
            }
            Err(ZoneLoadError::JournalScanGcGap) => {
                // Fall back to strategy 1.
                match self
                    .strategy1_replay(bind, disk_id, zone_idx, live_zone.disk_group_id, unit_capacity)
                    .await
                {
                    Some((popcount, slot)) => (popcount, slot, Some(FallbackReason::JournalScanGcGap)),
                    None => (u32::MAX, 0, Some(FallbackReason::JournalScanGcGap)),
                }
            }
            Err(ZoneLoadError::SnapshotCrcFail) => {
                // Fall back to strategy 1; the live bitmap is suspect.
                match self
                    .strategy1_replay(bind, disk_id, zone_idx, live_zone.disk_group_id, unit_capacity)
                    .await
                {
                    Some((popcount, slot)) => (popcount, slot, Some(FallbackReason::SnapshotCrcFail)),
                    None => (u32::MAX, 0, Some(FallbackReason::SnapshotCrcFail)),
                }
            }
            Err(_) => (u32::MAX, 0, None),
        };

        // A CRC-fail fallback means the live snapshot was corrupt —
        // drift is flagged even if counts happen to match.
        let crc_drift = matches!(fallback_used, Some(FallbackReason::SnapshotCrcFail));
        let matches = !crc_drift && replayed_busy_blocks == live_busy_blocks;
        let drift_detected = !matches;

        RecalcResult {
            disk_id,
            zone_index: zone_idx,
            matches,
            drift_detected,
            live_busy_blocks,
            replayed_busy_blocks,
            live_snapshot_slot,
            replayed_snapshot_slot,
            fallback_used,
        }
    }

    /// Strategy 1 fallback: full-scan rebuild into a throwaway zone.
    /// Returns `(popcount, snapshot_slot)` on success.
    async fn strategy1_replay(
        &self,
        bind: Bind,
        disk_id: DiskId,
        zone_idx: u32,
        disk_group_id: DiskGroupId,
        unit_capacity: u32,
    ) -> Option<(u32, u64)> {
        match crate::recovery::full_scan::rebuild_zone_bitmap_full_scan(
            &self.kv,
            bind,
            disk_id,
            zone_idx,
            disk_group_id,
            unit_capacity,
        )
        .await
        {
            Ok((zone, _stats)) => {
                let popcount = zone.usage_bits.count_set();
                let slot = zone.snapshot_slot.load(std::sync::atomic::Ordering::Acquire);
                Some((u32::try_from(popcount).unwrap_or(u32::MAX), slot))
            }
            Err(_) => None,
        }
    }

    /// Recalc all zones across all disks in one disk-group.
    pub async fn recalc_disk_group(&self, dg_id: DiskGroupId) -> Option<DiskGroupRecalcResult> {
        let dg = self.container.get_disk_group(dg_id)?;
        let bind = *dg.bind.read().unwrap();
        let disks_snapshot: Vec<(DiskId, ZoneList)> = {
            let disks = dg.disks.read().unwrap();
            disks
                .iter()
                .map(|d| {
                    let zones = d.zones.read().unwrap();
                    let zone_info: Vec<(u32, u32, Arc<DdbZone>)> = zones
                        .iter()
                        .map(|z| (z.zone_index, z.unit_capacity, Arc::clone(z)))
                        .collect();
                    (d.disk_id, zone_info)
                })
                .collect()
        };

        let mut zone_results = Vec::new();
        for (disk_id, zones) in disks_snapshot {
            for (zone_idx, unit_capacity, live_zone) in zones {
                let r = self
                    .recalc_zone(bind, disk_id, zone_idx, unit_capacity, &live_zone)
                    .await;
                zone_results.push(r);
            }
        }
        let drift_detected = zone_results.iter().any(|r| r.drift_detected);
        Some(DiskGroupRecalcResult {
            disk_group_id: dg_id,
            zone_results,
            drift_detected,
        })
    }

    /// Recalc every owned disk-group.
    pub async fn recalc_all(&self) -> Vec<DiskGroupRecalcResult> {
        let dg_ids = self.container.disk_group_ids();
        let mut results = Vec::with_capacity(dg_ids.len());
        for dg_id in dg_ids {
            if let Some(r) = self.recalc_disk_group(dg_id).await {
                results.push(r);
            }
        }
        results
    }
}

impl RecalcResult {
    /// The fallback reason as a string (for the proto response).
    #[must_use]
    pub fn fallback_reason_str(&self) -> Option<&'static str> {
        self.fallback_used.map(FallbackReason::as_str)
    }
}
