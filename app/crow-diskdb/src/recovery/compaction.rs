// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 0.0.

//! Snapshot compaction engine (R73 strategy 3).
//!
//! `CompactionEngine` runs as a background task, periodically
//! compacting zones whose `uncompacted_free_record_count` exceeds the
//! threshold. Compaction merges free records into a new `ZoneValue`
//! snapshot (clearing the freed bits) and deletes the free records,
//! keeping the uncompacted record set small so strategy 2's replay is
//! fast on restart.
//!
//! Crash safety: the `ZoneValue` snapshot is written **before** the
//! free records are deleted. If diskdb crashes after the snapshot
//! write but before the delete, the free records are orphaned but
//! harmless (strategy 2 replay treats `Put FreeBlockKey` as a no-op
//! for state; strategy 1 ignores free records entirely).

use std::sync::Arc;

use crow_protocol::common::DiskId;
use crow_protocol::key::BinaryKey;

use crate::bg_task::{BackgroundTask, BgCtx, CycleFut, Trigger};
use crate::ddb_config::CompactionConfig;
use crate::ddb_kv_client::{Bind, DdbKvClient};
use crate::model::disk_group_container::DdbDiskGroupContainer;
use crate::model::zone::DdbZone;
use crate::recovery::RecoveryError;

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
    pub async fn compaction_cycle(&self, container: &DdbDiskGroupContainer, threshold: u32) {
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
                    if let Err(e) = self
                        .compact_zone(bind, disk.disk_id, &zone, zone.zone_index)
                        .await
                    {
                        tracing::warn!(
                            disk_id = ?disk.disk_id,
                            zone_index = zone.zone_index,
                            error = %e,
                            "compaction failed; will retry next cycle"
                        );
                    }
                }
            }
        }
    }

    /// Compact one zone: partition free records by the `compact_ts`
    /// watermark, `range_clear` only the new records, write a new
    /// `ZoneValue` + delete all free records in one atomic `batch_write`
    /// (I6). Sets `compacted_ready = true` on success.
    ///
    /// Crash-safety: the atomic `batch_write` ensures the snapshot and
    /// the free-record deletes succeed or fail together — no window
    /// where the snapshot is durable but the free records survive (I6).
    /// If diskdb crashes during the batch, the KV group's paxos
    /// consensus ensures the batch is atomic — either all ops are
    /// applied or none are.
    pub async fn compact_zone(
        &self,
        bind: Bind,
        disk_id: DiskId,
        zone: &Arc<DdbZone>,
        zone_idx: u32,
    ) -> Result<(), RecoveryError> {
        // Step 1: scan free records for the zone (no zone lock — KV read).
        let records = self.kv.read_zone_records(bind, &disk_id, zone_idx).await?;
        let free_keys: Vec<Vec<u8>> = records.free.iter().map(|r| r.key.to_bytes()).collect();
        #[allow(clippy::cast_possible_truncation)]
        let free_count = free_keys.len() as u32;
        if free_count == 0 {
            // Nothing to compact.
            zone.uncompacted_free_record_count
                .store(0, std::sync::atomic::Ordering::Release);
            zone.mark_compacted_ready();
            return Ok(());
        }

        // Step 2: in-memory compaction — partition by watermark,
        // range_clear only new records, advance compact_ts (zone lock
        // held only for the bitmap mutation, I9).
        let result = zone.compact_zone_inner(&records.free);

        // Step 3: determine snapshot_slot = current applied frontier.
        let snapshot_slot = self.kv.get_applied_slot(bind).await.unwrap_or(0);

        // Step 4: build the new ZoneValue (with CRC + advanced compact_ts).
        zone.snapshot_slot
            .store(snapshot_slot, std::sync::atomic::Ordering::Release);
        let zv = zone.to_zone_value();

        // Step 5: atomic batch_write — Put ZoneValue + Delete all free
        // records (I6). They succeed or fail together.
        self.kv
            .compact_zone_batch(bind, &disk_id, zone_idx, &zv, &free_keys)
            .await?;

        // Step 6: decrement uncompacted_free_record_count by the total
        // free records processed (both stale and new were deleted).
        zone.uncompacted_free_record_count
            .fetch_sub(free_count, std::sync::atomic::Ordering::AcqRel);

        // Step 7: mark the zone as compacted and ready for rotation.
        zone.mark_compacted_ready();

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

    /// On-demand compaction of one zone (operator-triggered or
    /// pre-ownership-transfer).
    pub async fn compact_zone_now(
        &self,
        bind: Bind,
        disk_id: DiskId,
        zone: &Arc<DdbZone>,
        zone_idx: u32,
    ) -> Result<(), RecoveryError> {
        self.compact_zone(bind, disk_id, zone, zone_idx).await
    }
}

impl BackgroundTask for CompactionEngine {
    fn run_cycle<'a>(&'a self, ctx: &'a BgCtx) -> CycleFut<'a> {
        Box::pin(async move {
            let threshold = ctx.config.load().persistence.snapshot_compaction_threshold;
            self.compaction_cycle(&ctx.container, threshold).await;
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
