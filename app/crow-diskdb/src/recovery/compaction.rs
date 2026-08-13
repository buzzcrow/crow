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
use std::time::Duration;

use crow_protocol::common::DiskId;
use crow_protocol::key::BinaryKey;

use crate::bg_task::{BackgroundTask, BgCtx, BgError, CycleFut, Trigger};
use crate::data_group_client::{Bind, DataGroupClient};
use crate::domain::disk_group_container::DdbDiskGroupContainer;
use crate::domain::zone::DdbZone;
use crate::recovery::RecoveryError;

/// Compaction configuration.
#[derive(Debug, Clone)]
pub struct CompactionConfig {
    /// Periodic compaction interval.
    pub compaction_cadence: Duration,
    /// Free-record count per zone that triggers compaction (in
    /// addition to the periodic cadence). Whichever fires first.
    pub snapshot_compaction_threshold: u32,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            compaction_cadence: Duration::from_secs(300),
            snapshot_compaction_threshold: 4096,
        }
    }
}

/// Snapshot compaction engine. Owns a `DataGroupClient` and runs as a
/// background task (`tokio::spawn`).
pub struct CompactionEngine {
    kv: Arc<DataGroupClient>,
    config: CompactionConfig,
}

impl CompactionEngine {
    /// Create a new compaction engine.
    #[must_use]
    pub fn new(kv: Arc<DataGroupClient>, config: CompactionConfig) -> Self {
        Self { kv, config }
    }

    /// Run one compaction cycle: for each owned disk-group/disk/zone,
    /// check `uncompacted_free_record_count` against
    /// `snapshot_compaction_threshold` and call `compact_zone` when
    /// exceeded.
    pub async fn compaction_cycle(&self, container: &DdbDiskGroupContainer) {
        for dg_id in container.disk_group_ids() {
            let Some(dg) = container.get_disk_group(dg_id) else {
                continue;
            };
            let bind = *dg.bind.read().unwrap();
            let disks = dg.disks.read().unwrap().clone();
            for disk in disks {
                let zones = disk.zones.read().unwrap().clone();
                for zone in zones {
                    let backlog = zone
                        .uncompacted_free_record_count
                        .load(std::sync::atomic::Ordering::Acquire);
                    if backlog < self.config.snapshot_compaction_threshold {
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

    /// Run the compaction loop forever (legacy entry point for direct
    /// `tokio::spawn` without the `BgRunner` framework).
    pub async fn compaction_loop(self: Arc<Self>, container: Arc<DdbDiskGroupContainer>) -> ! {
        loop {
            tokio::time::sleep(self.config.compaction_cadence).await;
            self.compaction_cycle(&container).await;
        }
    }

    /// Compact one zone: merge free records into a new `ZoneValue`
    /// snapshot, write it, then delete the free records.
    ///
    /// Crash-safety invariant: the `ZoneValue` snapshot is written
    /// **before** the free records are deleted.
    pub async fn compact_zone(
        &self,
        bind: Bind,
        disk_id: DiskId,
        zone: &Arc<DdbZone>,
        zone_idx: u32,
    ) -> Result<(), RecoveryError> {
        // Step a: scan free records for the zone.
        let records = self.kv.read_zone_records(bind, &disk_id, zone_idx).await?;
        let free_keys: Vec<Vec<u8>> = records.free.iter().map(|r| r.key.to_bytes()).collect();
        #[allow(clippy::cast_possible_truncation)]
        let free_count = free_keys.len() as u32;
        if free_count == 0 {
            // Nothing to compact.
            zone.uncompacted_free_record_count
                .store(0, std::sync::atomic::Ordering::Release);
            return Ok(());
        }

        // Step b: merge free records into the in-memory bitmap
        // (range_clear per free record).
        for free in &records.free {
            #[allow(clippy::cast_possible_truncation)]
            let offset = free.key.unit_offset as u32;
            let _ = zone.usage_bits.range_clear(offset, free.value.unit_count);
        }
        // Update used_count = popcount of the merged bitmap.
        let popcount = zone.usage_bits.count_set();
        zone.used_count.store(
            u32::try_from(popcount).unwrap_or(u32::MAX),
            std::sync::atomic::Ordering::Release,
        );

        // Step c: determine snapshot_slot = current applied frontier.
        let snapshot_slot = self.kv.get_applied_slot(bind).await.unwrap_or(0);

        // Step d: build the new ZoneValue (with CRC).
        zone.snapshot_slot
            .store(snapshot_slot, std::sync::atomic::Ordering::Release);
        let zv = zone.to_zone_value();

        // Step e: write the new ZoneValue BEFORE deleting free records
        // (crash-safety invariant).
        self.kv.put_zone(bind, &disk_id, zone_idx, &zv).await?;

        // Step f: delete the free records in one batch_write.
        self.kv.delete_free_records_batch(bind, &free_keys).await?;

        // Step g: decrement uncompacted_free_record_count.
        zone.uncompacted_free_record_count
            .fetch_sub(free_count, std::sync::atomic::Ordering::AcqRel);

        tracing::info!(
            disk_id = ?disk_id,
            zone_index = zone_idx,
            free_count,
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
            self.compaction_cycle(&ctx.container).await;
            Ok(())
        })
    }

    fn trigger(&self) -> Trigger {
        Trigger::Timer(self.config.compaction_cadence)
    }

    fn name(&self) -> &'static str {
        "compaction"
    }
}

#[allow(dead_code)]
fn _bg_error_unused() -> BgError {
    BgError("unused".to_string())
}
