// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Snapshot compaction engine (R73 strategy 3) + preparatory thread.
//!
//! `CompactionEngine` runs as a background task, periodically
//! compacting zones whose `uncompacted_free_record_count` exceeds the
//! threshold. `PreparatoryThread` pre-compacts the next batch of zones
//! in the rotation order so rotation is instant. Both call the shared
//! `compact_zone` function.
//!
//! Crash safety: the `ZoneValue` snapshot + free-record deletes are one
//! atomic `batch_write` (I6) — they succeed or fail together. No window
//! where the snapshot is durable but the free records survive.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crow_protocol::common::DiskId;
use crow_protocol::key::BinaryKey;

use crate::bg_task::{BackgroundTask, BgCtx, CycleFut, Trigger};
use crate::ddb_config::CompactionConfig;
use crate::ddb_kv_client::{Bind, DdbKvClient};
use crate::metrics::DiskdbMetrics;
use crate::model::disk_group_container::DdbDiskGroupContainer;
use crate::model::zone::DdbZone;
use crate::recovery::ZoneLoadError;

/// RAII guard that clears the `compacting` flag on drop. Ensures the
/// flag is cleared even on panic or early return (HIGH-5).
struct CompactingGuard<'a>(&'a AtomicBool);
impl Drop for CompactingGuard<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

/// Compact one zone: partition free records by the `compact_ts`
/// watermark, `range_clear` only the new records, write a new
/// `ZoneValue` + delete all free records in one atomic `batch_write`
/// (I6). Sets `compacted_ready = true` on success.
///
/// A per-zone `AtomicBool` guard prevents concurrent compaction of the
/// same zone (HIGH-5). If another compaction is already in progress,
/// returns `Ok(())` immediately (skip — the in-progress compaction
/// will handle this zone).
///
/// Crash-safety: the atomic `batch_write` ensures the snapshot and
/// the free-record deletes succeed or fail together — no window
/// where the snapshot is durable but the free records survive (I6).
/// If diskdb crashes during the batch, the KV group's paxos
/// consensus ensures the batch is atomic — either all ops are
/// applied or none are.
pub async fn compact_zone(
    kv: &DdbKvClient,
    bind: Bind,
    disk_id: DiskId,
    zone: &Arc<DdbZone>,
    zone_idx: u32,
    metrics: &DiskdbMetrics,
) -> Result<(), ZoneLoadError> {
    // Per-zone compaction guard — prevents concurrent compaction of
    // the same zone (HIGH-5). If already in progress, skip.
    if zone
        .compacting
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        tracing::debug!(
            disk_id = ?disk_id,
            zone_index = zone_idx,
            "compaction already in progress; skipping"
        );
        return Ok(());
    }
    let _guard = CompactingGuard(&zone.compacting);
    let compaction_start = std::time::Instant::now();

    // Step 1: scan free records for the zone (no zone lock — KV read).
    let scan_start = std::time::Instant::now();
    let records = kv.read_zone_records(bind, &disk_id, zone_idx).await?;
    metrics
        .compaction_scan_free_latency
        .observe(scan_start.elapsed().as_nanos().try_into().unwrap_or(u64::MAX));
    let free_keys: Vec<Vec<u8>> = records.free.iter().map(|r| r.key.to_bytes()).collect();
    #[allow(clippy::cast_possible_truncation)]
    let free_count = free_keys.len() as u32;
    if free_count == 0 {
        // Nothing to compact.
        zone.uncompacted_free_record_count.store(0, Ordering::Release);
        zone.mark_compacted_ready();
        metrics.compaction_latency.observe(
            compaction_start
                .elapsed()
                .as_nanos()
                .try_into()
                .unwrap_or(u64::MAX),
        );
        return Ok(());
    }

    // Step 2: in-memory compaction — partition by watermark,
    // range_clear only new records, advance compact_ts (zone lock
    // held only for the bitmap mutation, I9).
    let merge_start = std::time::Instant::now();
    let result = zone.compact_zone_inner(&records.free);
    metrics
        .compaction_merge_bitmap_latency
        .observe(merge_start.elapsed().as_nanos().try_into().unwrap_or(u64::MAX));

    // Step 3: determine snapshot_slot = current applied frontier.
    let snapshot_slot = match kv.get_applied_slot(bind).await {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!(
                disk_id = ?disk_id,
                zone_index = zone_idx,
                error = %e,
                "get_applied_slot failed; anchoring at slot 0"
            );
            0
        }
    };

    // Step 4: build the new ZoneValue (with CRC + advanced compact_ts).
    zone.snapshot_slot.store(snapshot_slot, Ordering::Release);
    let zv = zone.to_zone_value();

    // Step 5: atomic batch_write — Put ZoneValue + Delete all free
    // records (I6). They succeed or fail together.
    let persist_start = std::time::Instant::now();
    kv.compact_zone_batch(bind, &disk_id, zone_idx, &zv, &free_keys)
        .await?;
    metrics
        .compaction_kv_persist_latency
        .observe(persist_start.elapsed().as_nanos().try_into().unwrap_or(u64::MAX));

    // Step 6: decrement uncompacted_free_record_count by the total
    // free records processed (both stale and new were deleted).
    zone.uncompacted_free_record_count
        .fetch_sub(free_count, Ordering::AcqRel);

    // Step 7: mark the zone as compacted and ready for rotation.
    zone.mark_compacted_ready();

    metrics
        .compaction_records_deleted_total
        .inc_by(u64::from(free_count));
    metrics.compaction_latency.observe(
        compaction_start
            .elapsed()
            .as_nanos()
            .try_into()
            .unwrap_or(u64::MAX),
    );

    tracing::info!(
        disk_id = ?disk_id,
        zone_index = zone_idx,
        free_count,
        new_free_count = result.new_free_count,
        stale_free_count = result.stale_free_count,
        compact_ts = result.new_compact_ts,
        snapshot_slot,
        "compaction completed"
    );

    Ok(())
}

/// Snapshot compaction engine. Owns a `DdbKvClient` and runs as a
/// background task (`tokio::spawn`).
pub struct CompactionEngine {
    kv: Arc<DdbKvClient>,
    config: CompactionConfig,
    /// Optional shared config handle for live-apply of the timer
    /// cadence. When set, `trigger()` returns `TimerFn` reading
    /// `persistence.compaction_cadence_secs` from this handle each
    /// tick; when `None`, falls back to the fixed snapshot.
    config_handle: Option<Arc<arc_swap::ArcSwap<crate::ddb_config::DdbConfig>>>,
}

impl CompactionEngine {
    /// Create a new compaction engine.
    #[must_use]
    pub fn new(kv: Arc<DdbKvClient>, config: CompactionConfig) -> Self {
        Self {
            kv,
            config,
            config_handle: None,
        }
    }

    /// Attach a shared config handle for live-apply of the timer
    /// cadence and compaction threshold.
    #[must_use]
    pub fn with_config_handle(
        mut self,
        handle: Arc<arc_swap::ArcSwap<crate::ddb_config::DdbConfig>>,
    ) -> Self {
        self.config_handle = Some(handle);
        self
    }

    /// Run one compaction cycle: for each owned disk-group/disk/zone,
    /// check `uncompacted_free_record_count` against
    /// `snapshot_compaction_threshold` and call `compact_zone` when
    /// exceeded. Skips zones in the disk's `active_zone_context` (I4 —
    /// no concurrent allocate).
    pub async fn compaction_cycle(
        &self,
        container: &DdbDiskGroupContainer,
        threshold: u32,
        metrics: &DiskdbMetrics,
    ) {
        for dg_id in container.disk_group_ids() {
            let Some(dg) = container.get_disk_group(dg_id) else {
                continue;
            };
            let bind = *dg.bind.read().unwrap();
            let disks = dg.disks.read().unwrap().clone();
            for disk in disks {
                // Collect active zone indices to skip (I4).
                let active_zone_indices: std::collections::HashSet<u32> = {
                    let active = disk.active_zone_context.read().unwrap();
                    active.iter().map(|z| z.zone_index).collect()
                };
                let zones = disk.zones.read().unwrap().clone();
                for zone in zones {
                    // Skip active zones — no concurrent allocate (I4).
                    if active_zone_indices.contains(&zone.zone_index) {
                        continue;
                    }
                    let backlog = zone
                        .uncompacted_free_record_count
                        .load(std::sync::atomic::Ordering::Acquire);
                    if backlog < threshold {
                        continue;
                    }
                    if let Err(e) =
                        compact_zone(&self.kv, bind, disk.disk_id, &zone, zone.zone_index, metrics).await
                    {
                        tracing::warn!(
                            disk_id = ?disk.disk_id,
                            zone_index = zone.zone_index,
                            error = %e,
                            "periodic compaction failed; will retry next cycle"
                        );
                    }
                }
            }
        }
    }

    /// On-demand compaction of one zone (operator-triggered or
    /// pre-ownership-transfer).
    pub async fn compact_zone_now(
        &self,
        bind: Bind,
        disk_id: DiskId,
        zone: &Arc<DdbZone>,
        zone_idx: u32,
        metrics: &DiskdbMetrics,
    ) -> Result<(), ZoneLoadError> {
        compact_zone(&self.kv, bind, disk_id, zone, zone_idx, metrics).await
    }
}

impl BackgroundTask for CompactionEngine {
    fn run_cycle<'a>(&'a self, ctx: &'a BgCtx) -> CycleFut<'a> {
        Box::pin(async move {
            let threshold = ctx.config.load().persistence.snapshot_compaction_threshold;
            self.compaction_cycle(&ctx.container, threshold, &ctx.metrics)
                .await;
            Ok(())
        })
    }

    fn trigger(&self) -> Trigger {
        match &self.config_handle {
            Some(handle) => {
                let handle = Arc::clone(handle);
                Trigger::TimerFn(Box::new(move || {
                    std::time::Duration::from_secs(u64::from(
                        handle.load().persistence.compaction_cadence_secs,
                    ))
                }))
            }
            None => Trigger::Timer(self.config.compaction_cadence),
        }
    }

    fn name(&self) -> &'static str {
        "compaction"
    }
}

/// Preparatory thread: pre-compacts the next batch of
/// `zone_rotate_count` zones in the rotation order so rotation is
/// instant. Runs as a background task with a short cadence (default
/// 5s). For each disk, it identifies the next zones starting from
/// `pos_v_zone + zone_rotate_count` (wrapping), and compacts any that
/// are not `compacted_ready` and not in the current active set.
pub struct PreparatoryThread {
    kv: Arc<DdbKvClient>,
    config: CompactionConfig,
    config_handle: Option<Arc<arc_swap::ArcSwap<crate::ddb_config::DdbConfig>>>,
}

impl PreparatoryThread {
    /// Create a new preparatory thread. Uses the same `DdbKvClient` as
    /// the compaction engine (shared `Arc`).
    #[must_use]
    pub fn new(kv: Arc<DdbKvClient>, config: CompactionConfig) -> Self {
        Self {
            kv,
            config,
            config_handle: None,
        }
    }

    /// Attach a shared config handle for live-apply of the cadence.
    #[must_use]
    pub fn with_config_handle(
        mut self,
        handle: Arc<arc_swap::ArcSwap<crate::ddb_config::DdbConfig>>,
    ) -> Self {
        self.config_handle = Some(handle);
        self
    }

    /// Run one preparatory cycle: for each owned disk-group/disk,
    /// identify the next `zone_rotate_count` zones in the rotation
    /// order and compact any that are not ready and not active.
    pub async fn preparatory_cycle(
        &self,
        container: &DdbDiskGroupContainer,
        zone_rotate_count: u32,
        metrics: &DiskdbMetrics,
    ) {
        for dg_id in container.disk_group_ids() {
            let Some(dg) = container.get_disk_group(dg_id) else {
                continue;
            };
            let bind = *dg.bind.read().unwrap();
            let disks = dg.disks.read().unwrap().clone();
            for disk in disks {
                self.preparatory_cycle_for_disk(bind, &disk, zone_rotate_count, metrics)
                    .await;
            }
        }
    }

    /// Pre-compact the next batch of zones for one disk.
    async fn preparatory_cycle_for_disk(
        &self,
        bind: Bind,
        disk: &Arc<crate::model::disk::DdbDisk>,
        zone_rotate_count: u32,
        metrics: &DiskdbMetrics,
    ) {
        let zones = disk.zones.read().unwrap().clone();
        let zone_num = zones.len();
        if zone_num == 0 || zone_rotate_count == 0 {
            return;
        }

        // Collect active zone indices to skip (I4).
        let active_zone_indices: std::collections::HashSet<u32> = {
            let active = disk.active_zone_context.read().unwrap();
            active.iter().map(|z| z.zone_index).collect()
        };

        // Identify the next batch: starting from
        // `pos_v_zone + zone_rotate_count`, wrapping around. This is
        // the set of zones that will be picked next when rotation
        // triggers.
        #[allow(clippy::cast_possible_truncation)]
        let start = (disk.pos_v_zone.load(std::sync::atomic::Ordering::Acquire) as usize
            + zone_rotate_count as usize)
            % zone_num;

        let mut checked = 0u32;
        for i in 0..zone_num {
            if checked >= zone_rotate_count {
                break;
            }
            let zone = &zones[(start + i) % zone_num];
            // Skip active zones — no concurrent allocate (I4).
            if active_zone_indices.contains(&zone.zone_index) {
                continue;
            }
            // Skip zones that are already ready.
            if zone.compacted_ready.load(std::sync::atomic::Ordering::Acquire) {
                checked += 1;
                continue;
            }
            // Compact this zone and mark it ready.
            if let Err(e) = compact_zone(&self.kv, bind, disk.disk_id, zone, zone.zone_index, metrics).await {
                tracing::warn!(
                    disk_id = ?disk.disk_id,
                    zone_index = zone.zone_index,
                    error = %e,
                    "preparatory compaction failed; will retry next cycle"
                );
            }
            checked += 1;
        }
    }
}

impl BackgroundTask for PreparatoryThread {
    fn run_cycle<'a>(&'a self, ctx: &'a BgCtx) -> CycleFut<'a> {
        Box::pin(async move {
            let zone_rotate_count = ctx.config.load().storage.zone_rotate_count;
            self.preparatory_cycle(&ctx.container, zone_rotate_count, &ctx.metrics)
                .await;
            Ok(())
        })
    }

    fn trigger(&self) -> Trigger {
        if let Some(handle) = &self.config_handle {
            let handle = Arc::clone(handle);
            Trigger::TimerFn(Box::new(move || {
                // Preparatory thread runs at 1/10 the compaction
                // cadence (more frequent) to keep ready zones
                // available ahead of rotation.
                let secs = u64::from(handle.load().persistence.compaction_cadence_secs);
                std::time::Duration::from_secs((secs / 10).max(1))
            }))
        } else {
            // Default: 1/10 the compaction cadence, minimum 1s.
            let secs = self.config.compaction_cadence.as_secs();
            let prep_secs = (secs / 10).max(1);
            Trigger::Timer(std::time::Duration::from_secs(prep_secs))
        }
    }

    fn name(&self) -> &'static str {
        "preparatory"
    }
}
