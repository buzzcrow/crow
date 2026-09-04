// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Strategy 2 — journal-scan replay zone loading.
//!
//! Loads the latest `ZoneValue` snapshot, then replays the journal
//! (slot-ordered `JournalScan` of busy ops) from `snapshot_slot + 1` to
//! the applied frontier. Only `Put BusyBlockKey` ops are applied to the
//! bitmap (Option B — persist-only load): `Delete BusyBlockKey` ops
//! (frees) are ignored, leaving the bitmap as a conservative over-
//! estimate. The free records on disk are the source of truth; the
//! common compaction flow will `range_clear` them naturally. This
//! eliminates the double-free risk that would arise if the loader
//! cleared bits for frees while the free records still exist on disk.

use crowdb_protocol::common::DiskId;
use crowdb_protocol::diskdb::rpc::{BusyBlockValue, FreeBlockValue};
use crowdb_protocol::key::{BinaryKey, BusyBlockKey};
use crowdb_protocol::{DiskGroupId, ZoneValueExt};

use crate::ddb_kv_client::{Bind, DdbKvClient};
use crate::model::zone::{DdbZone, DdbZoneHealth};
use crate::recovery::ZoneLoadError;

/// Inner zone load — strategy 2 (journal scan replay).
/// Returns a loaded `Zone`, the maximum allocation incarnation observed in
/// busy/free records, and whether a valid snapshot was
/// found (`true` = snapshot existed and was used; `false` = no
/// snapshot, fresh zone). The `snapshot_found` flag is used by
/// `background_zone_load` (B.2) to decide whether to write a baseline
/// `ZoneValue` record.
pub async fn load_zone_inner(
    kv: &DdbKvClient,
    bind: Bind,
    disk_id: DiskId,
    zone_idx: u32,
    disk_group_id: DiskGroupId,
    unit_capacity: u32,
) -> Result<(DdbZone, u64, bool), ZoneLoadError> {
    // Step a: load the latest ZoneValue snapshot.
    let snapshot = kv.get_zone_value(bind, &disk_id, zone_idx).await?;

    let (usage_bits, snapshot_slot, snapshot_compact_ts, snapshot_compact_slot, snapshot_found) =
        match &snapshot {
            Some(zv) if zv.verify_checksum() => {
                // Valid snapshot — restore bitmap + snapshot_slot + compact_ts.
                let bits = crowdb_protocol::UsageBitmap::restore(&zv.usage_bitmap);
                (bits, zv.snapshot_slot, zv.compact_ts, zv.compact_slot, true)
            }
            Some(_zv) => {
                // CRC fail — fall back to strategy 1.
                return Err(ZoneLoadError::SnapshotCrcFail);
            }
            None => {
                // No snapshot — empty bitmap, replay from slot 0.
                (crowdb_protocol::UsageBitmap::new(unit_capacity), 0, 0, 0, false)
            }
        };

    // Step b: journal scan busy ops from snapshot_slot+1 to MAX.
    // Only busy ops are needed — free ops (Delete BusyBlockKey, Put
    // FreeBlockKey) are not applied to the bitmap (Option B). The free
    // records on disk are the source of truth for what's freed; the
    // common compaction flow will process them.
    let min_slot = snapshot_slot + 1;
    let busy_ops = kv.journal_scan_busy(bind, min_slot, 0, &disk_id, zone_idx).await;
    let busy_ops = match busy_ops {
        Ok(ops) => ops,
        Err(crowdb_kv_client::Error::JournalScanGcGap) => {
            return Err(ZoneLoadError::JournalScanGcGap);
        }
        Err(e) => return Err(ZoneLoadError::Kv(e)),
    };

    // Step c: apply only Put BusyBlockKey ops (allocations). Delete
    // BusyBlockKey ops (frees) are ignored — the bitmap stays as a
    // conservative over-estimate. Compaction will range_clear the
    // freed bits when it processes the free records on disk.
    let mut used_count = usage_bits.count_set();
    let mut max_allocation_ts = 0;
    for op in &busy_ops {
        if op.is_delete {
            // Delete BusyBlockKey (free) — IGNORE (Option B).
            // The free record on disk is the source of truth.
            continue;
        }
        // Put BusyBlockKey → range_set (allocate).
        if let Ok(bk) = BusyBlockKey::from_bytes(&op.key) {
            if let Ok(bv) = bincode::deserialize::<BusyBlockValue>(&op.value) {
                max_allocation_ts = max_allocation_ts.max(bv.allocation_ts);
                #[allow(clippy::cast_possible_truncation)]
                let offset = bk.unit_offset as u32;
                let _ = usage_bits.range_set(offset, bv.unit_count);
                used_count += u64::from(bv.unit_count);
            }
        }
    }

    // Step d: scan free ops to count uncompacted free records on disk
    // (Put FreeBlockKey = +1, Delete FreeBlockKey = -1 from re-
    // allocate). This gives the compaction engine an accurate backlog
    // to trigger on. The bitmap is NOT mutated from these ops.
    let free_ops = kv.journal_scan_free(bind, min_slot, 0, &disk_id, zone_idx).await;
    let free_ops = match free_ops {
        Ok(ops) => ops,
        Err(crowdb_kv_client::Error::JournalScanGcGap) => {
            return Err(ZoneLoadError::JournalScanGcGap);
        }
        Err(e) => return Err(ZoneLoadError::Kv(e)),
    };
    let net_free_records: i64 = free_ops.iter().map(|op| if op.is_delete { -1 } else { 1 }).sum();
    let uncompacted_free_record_count = u32::try_from(net_free_records.max(0)).unwrap_or(0);

    // Extract max freed_ts from Put FreeBlockKey ops (for timestamp
    // source seeding — §8 Monotonic timestamp source). Delete ops
    // have empty values.
    let max_free_allocation_ts = free_ops
        .iter()
        .filter(|op| !op.is_delete)
        .filter_map(|op| bincode::deserialize::<FreeBlockValue>(&op.value).ok())
        .map(|fv| fv.pre_allocation_ts)
        .max()
        .unwrap_or(0);

    // Step e: build the loaded Zone. compact_ts stays at
    // snapshot_compact_ts — no advancement needed (Option B). The free
    // records on disk have freed_ts > snapshot_compact_ts (they were
    // written after the snapshot), so the next compaction will
    // classify them as "new" and range_clear their bits. This is
    // correct: those blocks ARE free (no BusyBlockKey on disk).
    let zone = DdbZone {
        disk_id,
        zone_index: zone_idx,
        disk_group_id,
        zone_state: std::sync::atomic::AtomicU8::new(DdbZoneHealth::Healthy as u8),
        unit_capacity,
        usage_bits,
        last_pos_64: std::sync::atomic::AtomicU64::new(0),
        used_count: std::sync::atomic::AtomicU32::new(u32::try_from(used_count).unwrap_or(u32::MAX)),
        snapshot_slot: std::sync::atomic::AtomicU64::new(snapshot_slot),
        compact_ts: std::sync::atomic::AtomicU64::new(snapshot_compact_ts),
        compact_slot: std::sync::atomic::AtomicU64::new(snapshot_compact_slot),
        compacted_ready: std::sync::atomic::AtomicBool::new(true),
        zone_lock: std::sync::RwLock::new(()),
        uncompacted_free_record_count: std::sync::atomic::AtomicU32::new(uncompacted_free_record_count),
        cas_retry_count: std::sync::atomic::AtomicU64::new(0),
        metrics_cas_retry: None,
        compacting: std::sync::atomic::AtomicBool::new(false),
    };

    Ok((
        zone,
        max_allocation_ts.max(max_free_allocation_ts),
        snapshot_found,
    ))
}
