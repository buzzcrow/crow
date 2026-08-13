// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Per-disk recovery scan task (R76). Spawned when a disk transitions
//! to `HwStatus::Bad`; cancelled when the disk transitions back to
//! `HwStatus::Up`. Iterates the bad disk's zones zone by zone, lists
//! live `BusyBlockValue`s, calls a placeholder recovery function (no
//! real data repair in v1), persists progress to KV after each zone,
//! and updates the `disk.bad.impacted_blocks` gauge.
//!
//! Progress is persisted per-disk at `RecoveryScanProgressKey` on the
//! bound data group. On restart, the sync loop reads the progress and
//! the scan resumes from `last_completed_zone + 1`.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crow_common::metrics::Gauge;
use crow_protocol::common::{ChunkId, DiskId, HwStatus};
use crow_protocol::diskdb::rpc::{RecoveryScanProgressValue, RecoveryScanStatus};
use tracing::{info, warn};

use crate::ddb_kv_client::{Bind, DdbKvClient};
use crate::model::disk::DdbDisk;

/// Placeholder recovery action. v1 is `LogOnly` — no data repair or
/// relocation. Future versions add `Relocate`/`RebuildFromEc` when the
/// `diskio` service exists.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryAction {
    /// Log impacted blocks + owner chunks; do not modify any records.
    LogOnly,
}

/// One busy block on a bad disk — the key + the owner chunk, collected
/// for the placeholder recovery function + the impacted-blocks gauge.
#[derive(Debug, Clone)]
pub struct ImpactedBlock {
    pub zone_index: u32,
    pub unit_offset: u64,
    pub unit_count: u32,
    pub owner_chunk: Option<ChunkId>,
}

/// Per-disk recovery scan task. Spawned when a disk transitions to
/// `HwStatus::Bad`; cancelled when the disk transitions back to
/// `HwStatus::Up` (via the `cancel` flag).
pub struct RecoveryScanTask {
    disk: Arc<DdbDisk>,
    bind: Bind,
    kv: DdbKvClient,
    /// Cancel flag — set by the caller on `HwStatus::Up` recovery.
    cancel: Arc<AtomicBool>,
    /// `disk.bad.impacted_blocks` counter handle.
    impacted_blocks_gauge: Arc<Gauge>,
}

impl RecoveryScanTask {
    /// Create a new recovery scan task for `disk`. The caller holds
    /// the `cancel` flag and sets it on `HwStatus::Up` recovery.
    #[must_use]
    pub fn new(
        disk: Arc<DdbDisk>,
        bind: Bind,
        kv: DdbKvClient,
        cancel: Arc<AtomicBool>,
        impacted_blocks_gauge: Arc<Gauge>,
    ) -> Self {
        Self {
            disk,
            bind,
            kv,
            cancel,
            impacted_blocks_gauge,
        }
    }

    /// Run the recovery scan to completion or cancellation. Reads
    /// persisted progress on start (resuming from
    /// `last_completed_zone + 1`), iterates zones, and persists
    /// progress after each zone. Returns the final impacted-blocks
    /// count.
    pub async fn run(&self) -> u64 {
        let disk_id = self.disk.disk_id;
        let zone_count = self
            .disk
            .zones
            .read()
            .unwrap()
            .len()
            .try_into()
            .unwrap_or(u32::MAX);

        // Read persisted progress (resume on restart).
        let mut start_zone: u32 = 0;
        let mut impacted_total: u64 = 0;
        let started_at_ms = now_ms();
        let _ = started_at_ms; // tracked for future use across resumes
        match self.kv.get_recovery_scan_progress(self.bind, &disk_id).await {
            Ok(Some(progress)) => {
                if progress.status == i32::from(RecoveryScanStatus::RecoveryScanComplete) {
                    info!(
                        disk = ?disk_id,
                        impacted = progress.impacted_blocks_count,
                        "recovery scan: already complete, skipping"
                    );
                    self.impacted_blocks_gauge.set(progress.impacted_blocks_count);
                    return progress.impacted_blocks_count;
                }
                start_zone = progress.last_completed_zone.saturating_add(1);
                impacted_total = progress.impacted_blocks_count;
                info!(
                    disk = ?disk_id,
                    start_zone = start_zone,
                    already_impacted = impacted_total,
                    "recovery scan: resuming from persisted progress"
                );
            }
            Ok(None) => {
                info!(disk = ?disk_id, "recovery scan: starting fresh from zone 0");
            }
            Err(e) => {
                warn!(disk = ?disk_id, error = %e, "recovery scan: read progress failed, starting from zone 0");
            }
        }

        // Iterate zones from start_zone to zone_count.
        for zi in start_zone..zone_count {
            if self.cancel.load(Ordering::Acquire) {
                info!(disk = ?disk_id, zone = zi, "recovery scan: cancelled, persisting progress");
                self.persist_progress(
                    zi.saturating_sub(1),
                    impacted_total,
                    RecoveryScanStatus::RecoveryScanStopped,
                )
                .await;
                return impacted_total;
            }

            match self.scan_zone(zi).await {
                Ok(zone_impacted) => {
                    impacted_total += u64::from(zone_impacted);
                    self.impacted_blocks_gauge.set(impacted_total);
                    self.persist_progress(zi, impacted_total, RecoveryScanStatus::RecoveryScanInProgress)
                        .await;
                    info!(
                        disk = ?disk_id,
                        zone = zi,
                        zone_impacted = zone_impacted,
                        total_impacted = impacted_total,
                        "recovery scan: zone complete"
                    );
                }
                Err(e) => {
                    warn!(
                        disk = ?disk_id,
                        zone = zi,
                        error = %e,
                        "recovery scan: zone failed, persisting progress and continuing"
                    );
                    self.persist_progress(
                        zi.saturating_sub(1),
                        impacted_total,
                        RecoveryScanStatus::RecoveryScanInProgress,
                    )
                    .await;
                }
            }
        }

        // All zones done.
        self.persist_progress(
            zone_count.saturating_sub(1),
            impacted_total,
            RecoveryScanStatus::RecoveryScanComplete,
        )
        .await;
        info!(
            disk = ?disk_id,
            total_impacted = impacted_total,
            "recovery scan: complete"
        );
        impacted_total
    }

    /// Scan one zone: read zone records, extract live busy blocks,
    /// call the placeholder recovery function. Returns the count of
    /// impacted blocks in this zone.
    async fn scan_zone(&self, zone_index: u32) -> Result<u32, crow_kv_client::Error> {
        let records = self
            .kv
            .read_zone_records(self.bind, &self.disk.disk_id, zone_index)
            .await?;
        let impacted: Vec<ImpactedBlock> = records
            .busy
            .iter()
            .map(|r| ImpactedBlock {
                zone_index,
                unit_offset: r.key.unit_offset,
                unit_count: r.value.unit_count,
                owner_chunk: r.value.owner_chunk,
            })
            .collect();
        let count = impacted.len().try_into().unwrap_or(u32::MAX);
        recover_zone_blocks(&self.disk.disk_id, zone_index, &impacted);
        Ok(count)
    }

    /// Persist scan progress to KV.
    async fn persist_progress(
        &self,
        last_completed_zone: u32,
        impacted_blocks_count: u64,
        status: RecoveryScanStatus,
    ) {
        let value = RecoveryScanProgressValue {
            status: status.into(),
            last_completed_zone,
            impacted_blocks_count,
            started_at_ms: 0, // not tracked across resumes
            updated_at_ms: now_ms(),
        };
        if let Err(e) = self
            .kv
            .put_recovery_scan_progress(self.bind, &self.disk.disk_id, &value)
            .await
        {
            warn!(
                disk = ?self.disk.disk_id,
                error = %e,
                "recovery scan: persist progress failed"
            );
        }
    }
}

/// Placeholder recovery function. v1 logs the impacted blocks + owner
/// chunks but does **not** perform any real data repair or relocation
/// (no `diskio` component). Returns `RecoveryAction::LogOnly`.
fn recover_zone_blocks(disk_id: &DiskId, zone_index: u32, blocks: &[ImpactedBlock]) -> RecoveryAction {
    if blocks.is_empty() {
        return RecoveryAction::LogOnly;
    }
    info!(
        disk = ?disk_id,
        zone = zone_index,
        impacted = blocks.len(),
        "recovery scan: impacted blocks (placeholder LogOnly — no repair)"
    );
    RecoveryAction::LogOnly
}

/// Current epoch time in milliseconds.
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis().try_into().unwrap_or(u64::MAX))
}

/// On-disk recovery: stop the scan (cancel flag), delete persisted
/// progress, run compaction on the disk's zones (strategy 3 — sole
/// bit-clearer), rebuild active zones, and re-include the disk in the
/// allocating set. Called by the keepalive sync loop when a disk
/// transitions `Missing → Up`, `Bad → Up`, or `Offline → Up`.
///
/// In v1 (placeholder recovery = `LogOnly`, no frees written),
/// compaction is a no-op — the bitmap is already correct, the disk
/// comes back with its data intact.
pub async fn recover_disk_to_up(disk: &Arc<DdbDisk>, bind: Bind, kv: &DdbKvClient, zone_rotate_count: u32) {
    // Transition to Up (caller has already validated the transition is
    // legal; set effective_status directly — the state machine's
    // on_enter_disk is a no-op for Up).
    disk.set_effective_status(HwStatus::Up);

    // Delete persisted recovery scan progress (if any).
    if let Err(e) = kv.delete_recovery_scan_progress(bind, &disk.disk_id).await {
        warn!(
            disk = ?disk.disk_id,
            error = %e,
            "disk recovery: delete scan progress failed (non-fatal)"
        );
    }

    // Run compaction on all the disk's zones (strategy 3 — sole
    // bit-clearer). In v1 this is a no-op (no frees written by the
    // placeholder scan). In the future, this merges the scan's
    // FreeBlockValues into the bitmap.
    let zone_count: u32 = disk.zones.read().unwrap().len().try_into().unwrap_or(u32::MAX);
    for zi in 0..zone_count {
        let zone = {
            let zones = disk.zones.read().unwrap();
            Arc::clone(&zones[zi as usize])
        };
        if let Err(e) = crate::recovery::compaction::compact_zone(kv, bind, disk.disk_id, &zone, zi).await {
            warn!(
                disk = ?disk.disk_id,
                zone = zi,
                error = %e,
                "disk recovery: compaction failed (non-fatal — bitmap may lag)"
            );
        }
    }

    // Rebuild the active zone deque + re-include the disk in the
    // allocating set.
    disk.rebuild_active_zones(zone_rotate_count);
}
