// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Strategy 1 — full-scan zone bitmap rebuild from live `BusyBlockKey`
//! records.
//!
//! Always available, O(all live busy records per zone). On-demand via
//! the `RebuildZoneBitmap` RPC and the §12 scanner (R75). Also used as
//! the fallback when strategy 2 cannot run.

use crowdb_protocol::common::DiskId;
use crowdb_protocol::DiskGroupId;
use std::collections::HashMap;

use crate::ddb_kv_client::{Bind, DdbKvClient};
use crate::model::records::ZoneRecords;
use crate::model::zone::DdbZone;
use crate::recovery::{ZoneLoadError, ZoneStats};

/// Full-scan rebuild of one zone's usage bitmap from the live
/// `BusyBlockKey` records on the data group.
///
/// Optionally writes a fresh `ZoneValue` snapshot after the rebuild so
/// the next restart can use strategy 2.
pub async fn rebuild_zone_bitmap_full_scan(
    kv: &DdbKvClient,
    bind: Bind,
    disk_id: DiskId,
    zone_idx: u32,
    disk_group_id: DiskGroupId,
    unit_capacity: u32,
) -> Result<(DdbZone, ZoneStats), ZoneLoadError> {
    let records: ZoneRecords = kv.read_zone_records(bind, &disk_id, zone_idx).await?;

    let zone = DdbZone::new(disk_id, zone_idx, disk_group_id, unit_capacity);
    for busy in &records.busy {
        #[allow(clippy::cast_possible_truncation)]
        let offset = busy.key.unit_offset as u32;
        let _ = zone.usage_bits.range_set(offset, busy.value.unit_count);
    }
    let busy_by_offset: HashMap<u64, _> = records
        .busy
        .iter()
        .map(|record| (record.key.unit_offset, record))
        .collect();
    for free in &records.free {
        if busy_by_offset.get(&free.key.unit_offset).is_some_and(|busy| {
            free.key.allocation_ts == free.value.pre_allocation_ts
                && busy.value.allocation_ts == free.value.pre_allocation_ts
                && busy.value.unit_count == free.value.unit_count
                && busy.value.owner_chunk == free.value.previous_owner
        }) {
            #[allow(clippy::cast_possible_truncation)]
            let offset = free.key.unit_offset as u32;
            let _ = zone.usage_bits.range_clear(offset, free.value.unit_count);
        }
    }
    // `used_count` = popcount of the rebuilt bitmap (may differ from
    // the sum of `unit_count`s if there were overlapping records —
    // shouldn't happen in normal operation, but popcount is the
    // truthful count).
    let popcount = zone.usage_bits.count_set();
    zone.used_count.store(
        u32::try_from(popcount).unwrap_or(u32::MAX),
        std::sync::atomic::Ordering::Release,
    );

    // Full-scan load rebuilds from busy records only (no free
    // records scanned), so compact_ts = 0. The next compaction will
    // advance it. The bitmap is accurate from records → compacted_ready.
    zone.compacted_ready
        .store(true, std::sync::atomic::Ordering::Release);

    // Optionally write a fresh ZoneValue snapshot so the next restart
    // can use strategy 2. Anchor it at the current applied frontier.
    let snapshot_slot = match kv.get_applied_slot(bind).await {
        Ok(slot) => slot,
        Err(e) => {
            tracing::warn!(
                disk_id = ?disk_id,
                zone_index = zone_idx,
                error = %e,
                "get_applied_slot failed; snapshot not written"
            );
            0
        }
    };
    if snapshot_slot > 0 {
        zone.snapshot_slot
            .store(snapshot_slot, std::sync::atomic::Ordering::Release);
        let zv = zone.to_zone_value();
        if let Err(e) = kv.put_zone(bind, &disk_id, zone_idx, &zv).await {
            tracing::warn!(
                disk_id = ?disk_id,
                zone_index = zone_idx,
                error = %e,
                "post-rebuild snapshot write failed; load still valid"
            );
        }
    }

    let stats = ZoneStats {
        capacity_units: unit_capacity,
        used_units: popcount,
        free_units: u64::from(unit_capacity).saturating_sub(popcount),
    };

    Ok((zone, stats))
}
