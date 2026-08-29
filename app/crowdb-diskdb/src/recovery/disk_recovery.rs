// Copyright 2026-present Gian <crow.db@outlook.com>
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

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use crowdb_common::metrics::Gauge;
use crowdb_protocol::common::{ChunkId, DiskId};
use crowdb_protocol::diskdb::rpc::{RecoveryScanProgressValue, RecoveryScanStatus};
use tracing::{info, warn};

use crate::ddb_kv_client::{Bind, DdbKvClient};
use crate::model::disk::DdbDisk;

/// Sentinel stored in `RecoveryScanProgressValue.last_completed_zone`
/// when no zone has completed yet (the scan was cancelled or failed at
/// zone 0 before any zone finished). On resume, `start_zone` is
/// computed as `0` for this sentinel — without it, `0` would be
/// ambiguous (could mean "zone 0 completed" → resume at 1, skipping
/// zone 0).
const NO_ZONE_COMPLETED: u32 = u32::MAX;

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

/// Cluster-aggregated `disk.bad.impacted_blocks` gauge. Each
/// `RecoveryScanTask` reports its disk's impacted-block count via
/// `set_disk`; the gauge reflects the **sum** across all concurrently
/// bad disks, not whichever scan wrote last. `remove_disk` drops a
/// disk's contribution on recovery-to-Up.
///
/// Without this aggregation, a single shared `Gauge` would be
/// overwritten by every concurrent scan — two bad disks at once would
/// show only the most-recent scan's count, not the cluster total.
pub struct ImpactedBlocksGauge {
    gauge: Arc<Gauge>,
    per_disk: RwLock<HashMap<DiskId, u64>>,
}

impl ImpactedBlocksGauge {
    /// Wrap a raw `disk.bad.impacted_blocks` gauge with per-disk
    /// aggregation state.
    #[must_use]
    pub fn new(gauge: Arc<Gauge>) -> Self {
        Self {
            gauge,
            per_disk: RwLock::new(HashMap::new()),
        }
    }

    /// Record `count` as `disk_id`'s impacted-block contribution and
    /// set the gauge to the new cluster total.
    pub fn set_disk(&self, disk_id: DiskId, count: u64) {
        let sum = {
            let mut m = self.per_disk.write().unwrap();
            m.insert(disk_id, count);
            m.values().copied().sum()
        };
        self.gauge.set(sum);
    }

    /// Drop `disk_id`'s contribution (on recovery-to-Up) and set the
    /// gauge to the new cluster total.
    pub fn remove_disk(&self, disk_id: &DiskId) {
        let sum = {
            let mut m = self.per_disk.write().unwrap();
            m.remove(disk_id);
            m.values().copied().sum()
        };
        self.gauge.set(sum);
    }
}

/// Result of reading persisted scan progress on resume.
enum ResumeOutcome {
    /// Scan already complete — return the impacted count immediately.
    AlreadyComplete(u64),
    /// Resume the scan from this state.
    Resume {
        start_zone: u32,
        impacted_total: u64,
        started_at_ms: u64,
    },
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
    /// Cluster-aggregated impacted-blocks gauge. The task reports its
    /// per-disk count via `set_disk`; the gauge sums across all bad
    /// disks.
    impacted_blocks: Arc<ImpactedBlocksGauge>,
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
        impacted_blocks: Arc<ImpactedBlocksGauge>,
    ) -> Self {
        Self {
            disk,
            bind,
            kv,
            cancel,
            impacted_blocks,
        }
    }

    /// Read persisted progress and decide how to resume. Returns
    /// `AlreadyComplete` (with the impacted count) if the scan is
    /// already done, or `Resume` with the resume state
    /// (`start_zone`, `impacted_total`, `started_at_ms`).
    /// `started_at_ms` is preserved across resumes so the record
    /// reflects the original scan start, not each resume time.
    async fn read_resume_state(&self, disk_id: DiskId) -> ResumeOutcome {
        match self.kv.get_recovery_scan_progress(self.bind, &disk_id).await {
            Ok(Some(progress)) => {
                if progress.status == i32::from(RecoveryScanStatus::RecoveryScanComplete) {
                    info!(
                        disk = ?disk_id,
                        impacted = progress.impacted_blocks_count,
                        "recovery scan: already complete, skipping"
                    );
                    self.impacted_blocks
                        .set_disk(disk_id, progress.impacted_blocks_count);
                    return ResumeOutcome::AlreadyComplete(progress.impacted_blocks_count);
                }
                let start_zone = last_completed_to_start(progress.last_completed_zone);
                let impacted_total = progress.impacted_blocks_count;
                let started_at_ms = progress.started_at_ms;
                info!(
                    disk = ?disk_id,
                    start_zone,
                    already_impacted = impacted_total,
                    "recovery scan: resuming from persisted progress"
                );
                ResumeOutcome::Resume {
                    start_zone,
                    impacted_total,
                    started_at_ms,
                }
            }
            Ok(None) => {
                info!(disk = ?disk_id, "recovery scan: starting fresh from zone 0");
                ResumeOutcome::Resume {
                    start_zone: 0,
                    impacted_total: 0,
                    started_at_ms: now_ms(),
                }
            }
            Err(e) => {
                warn!(disk = ?disk_id, error = %e, "recovery scan: read progress failed, starting from zone 0");
                ResumeOutcome::Resume {
                    start_zone: 0,
                    impacted_total: 0,
                    started_at_ms: now_ms(),
                }
            }
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

        // Read persisted progress (resume on restart). Preserve
        // `started_at_ms` across resumes so the record reflects the
        // original scan start, not each resume time.
        let (start_zone, mut impacted_total, started_at_ms) = match self.read_resume_state(disk_id).await {
            ResumeOutcome::AlreadyComplete(count) => return count,
            ResumeOutcome::Resume {
                start_zone,
                impacted_total,
                started_at_ms,
            } => (start_zone, impacted_total, started_at_ms),
        };

        // Iterate zones from start_zone to zone_count.
        for zi in start_zone..zone_count {
            if self.cancel.load(Ordering::Acquire) {
                info!(disk = ?disk_id, zone = zi, "recovery scan: cancelled, persisting progress");
                self.persist_progress(
                    last_completed_on_cancel(zi),
                    impacted_total,
                    started_at_ms,
                    RecoveryScanStatus::RecoveryScanStopped,
                )
                .await;
                return impacted_total;
            }

            match self.scan_zone(zi).await {
                Ok(zone_impacted) => {
                    impacted_total += u64::from(zone_impacted);
                    self.impacted_blocks.set_disk(disk_id, impacted_total);
                    self.persist_progress(
                        zi,
                        impacted_total,
                        started_at_ms,
                        RecoveryScanStatus::RecoveryScanInProgress,
                    )
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
                        last_completed_on_cancel(zi),
                        impacted_total,
                        started_at_ms,
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
            started_at_ms,
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
    async fn scan_zone(&self, zone_index: u32) -> Result<u32, crowdb_kv_client::Error> {
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
        started_at_ms: u64,
        status: RecoveryScanStatus,
    ) {
        let value = RecoveryScanProgressValue {
            status: status.into(),
            last_completed_zone,
            impacted_blocks_count,
            started_at_ms,
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

/// Compute the resume start zone from a persisted
/// `last_completed_zone`. `NO_ZONE_COMPLETED` (cancel/fail at zone 0
/// before any zone finished) resumes at 0; otherwise resumes at
/// `last_completed_zone + 1`.
fn last_completed_to_start(last_completed_zone: u32) -> u32 {
    if last_completed_zone == NO_ZONE_COMPLETED {
        0
    } else {
        last_completed_zone.saturating_add(1)
    }
}

/// `last_completed_zone` to persist when the scan is cancelled or a
/// zone fails at `zi`. For `zi == 0` no zone has completed, so the
/// `NO_ZONE_COMPLETED` sentinel is stored (resume restarts at zone 0,
/// not zone 1). For `zi > 0`, zones `0..zi` completed, so `zi - 1` is
/// the last completed.
fn last_completed_on_cancel(zi: u32) -> u32 {
    if zi == 0 {
        NO_ZONE_COMPLETED
    } else {
        zi - 1
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

/// On-disk recovery: delete persisted progress, run compaction on the
/// disk's zones (strategy 3 — sole bit-clearer), rebuild active zones,
/// and re-include the disk in the allocating set. Called by the
/// keepalive sync loop when a disk transitions `Missing → Up`,
/// `Bad → Up`, or `Offline → Up`.
///
/// The caller is responsible for the status transition itself — this
/// function does **not** set `effective_status`. The keepalive path
/// routes the transition through `HwStateMachine::transition_disk`
/// (which validates legality, including the operator-override
/// `Bad → Up` case, and sets the status) before calling this, so the
/// state machine stays the single source of truth for disk status.
///
/// In v1 (placeholder recovery = `LogOnly`, no frees written),
/// compaction is a no-op — the bitmap is already correct, the disk
/// comes back with its data intact.
pub async fn recover_disk_to_up(
    disk: &Arc<DdbDisk>,
    bind: Bind,
    kv: &DdbKvClient,
    zone_rotate_count: u32,
    metrics: &crate::metrics::DiskdbMetrics,
) {
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
        if let Err(e) =
            crate::recovery::compaction::compact_zone(kv, bind, disk.disk_id, &zone, zi, metrics).await
        {
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
