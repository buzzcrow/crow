// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Strategy 2 — journal-scan replay zone recovery.
//!
//! Loads the latest `ZoneValue` snapshot, then replays the journal
//! (slot-ordered `JournalScan` of busy + free ops) from
//! `snapshot_slot + 1` to the applied frontier. Fast when compaction
//! (strategy 3) keeps the uncompacted record set small.

use crow_kv_client::JournalOp;
use crow_protocol::common::DiskId;
use crow_protocol::diskdb::rpc::{BusyBlockValue, FreeBlockValue};
use crow_protocol::key::{BinaryKey, BusyBlockKey, FreeBlockKey};
use crow_protocol::ZoneValueExt;

use crate::ddb_kv_client::{Bind, DdbKvClient};
use crate::model::zone::{DdbZone, DdbZoneHealth};
use crate::recovery::RecoveryError;

/// Inner zone recovery — strategy 2 (journal scan replay).
/// Returns a recovered `Zone` or an error indicating why strategy 2
/// failed (caller falls back to strategy 1).
pub async fn recover_zone_inner(
    kv: &DdbKvClient,
    bind: Bind,
    disk_id: DiskId,
    zone_idx: u32,
    unit_capacity: u32,
) -> Result<DdbZone, RecoveryError> {
    // Step a: load the latest ZoneValue snapshot.
    let snapshot = kv.get_zone_value(bind, &disk_id, zone_idx).await?;

    let (usage_bits, snapshot_slot, snapshot_compact_ts, dg_id) = match &snapshot {
        Some(zv) if zv.verify_checksum() => {
            // Valid snapshot — restore bitmap + snapshot_slot + compact_ts.
            let bits = crow_protocol::UsageBitmap::restore(&zv.usage_bitmap);
            (bits, zv.snapshot_slot, zv.compact_ts, 0)
        }
        Some(_zv) => {
            // CRC fail — fall back to strategy 1.
            return Err(RecoveryError::SnapshotCrcFail);
        }
        None => {
            // No snapshot — empty bitmap, replay from slot 0.
            (crow_protocol::UsageBitmap::new(unit_capacity), 0, 0, 0)
        }
    };

    // Step b: journal scan busy + free ops from snapshot_slot+1 to MAX.
    let min_slot = snapshot_slot + 1;
    let busy_ops = kv.journal_scan_busy(bind, min_slot, 0, &disk_id, zone_idx).await;
    let busy_ops = match busy_ops {
        Ok(ops) => ops,
        Err(crow_kv_client::Error::Server(ref msg)) if msg.contains("gc gap") => {
            return Err(RecoveryError::JournalScanGcGap);
        }
        Err(e) => return Err(RecoveryError::Kv(e)),
    };
    let free_ops = kv.journal_scan_free(bind, min_slot, 0, &disk_id, zone_idx).await;
    let free_ops = match free_ops {
        Ok(ops) => ops,
        Err(crow_kv_client::Error::Server(ref msg)) if msg.contains("gc gap") => {
            return Err(RecoveryError::JournalScanGcGap);
        }
        Err(e) => return Err(RecoveryError::Kv(e)),
    };

    // Step c: merge the two op lists by slot (both are slot-sorted).
    let merged = merge_ops_by_slot(&busy_ops, &free_ops);

    // Step d: apply ops in slot order.
    let mut used_count = usage_bits.count_set();
    for op in &merged {
        if op.is_delete {
            // Delete of a BusyBlockKey → range_clear. The key is a
            // BusyBlockKey (type tag 0x0006); parse it as such to get
            // unit_offset. The unit_count comes from the matching
            // FreeBlockValue Put at the same slot (the free batch_write
            // deletes BusyBlockKey + puts FreeBlockKey atomically).
            if let Ok(bk) = BusyBlockKey::from_bytes(&op.key) {
                let unit_count = find_free_unit_count_at_slot(&merged, op.slot, bk.unit_offset);
                if let Some(count) = unit_count {
                    #[allow(clippy::cast_possible_truncation)]
                    let offset = bk.unit_offset as u32;
                    let _ = usage_bits.range_clear(offset, count);
                    used_count = used_count.saturating_sub(u64::from(count));
                }
            }
        } else {
            // Put of a BusyBlockKey → range_set. Decode the
            // BusyBlockValue to get unit_count.
            if let Ok(bk) = BusyBlockKey::from_bytes(&op.key) {
                if let Ok(bv) = bincode::deserialize::<BusyBlockValue>(&op.value) {
                    #[allow(clippy::cast_possible_truncation)]
                    let offset = bk.unit_offset as u32;
                    let _ = usage_bits.range_set(offset, bv.unit_count);
                    used_count += u64::from(bv.unit_count);
                }
            }
        }
    }

    // Step e: build the recovered Zone. compact_ts is advanced to
    // max(snapshot.compact_ts, max(freed_ts of replayed free records))
    // — this is critical: replay clears bits for Delete BusyBlockKey
    // ops (frees after the snapshot), so the bitmap is accurate, but
    // the free records still exist on disk. Without advancing
    // compact_ts, the next compaction would classify those free
    // records as "new" and range_clear their bits — if a block was
    // freed then re-allocated during replay, the bit is SET and
    // range_clear would corrupt the live allocation (double-free).
    let max_replayed_freed_ts = free_ops
        .iter()
        .filter_map(|op| {
            if op.is_delete {
                return None;
            }
            bincode::deserialize::<FreeBlockValue>(&op.value)
                .ok()
                .map(|fv| fv.freed_ts)
        })
        .max()
        .unwrap_or(0);
    let recovered_compact_ts = snapshot_compact_ts.max(max_replayed_freed_ts);

    let zone = DdbZone {
        disk_id,
        zone_index: zone_idx,
        disk_group_id: dg_id,
        zone_state: std::sync::RwLock::new(DdbZoneHealth::Healthy),
        unit_capacity,
        usage_bits,
        last_pos_64: std::sync::atomic::AtomicU64::new(0),
        used_count: std::sync::atomic::AtomicU32::new(u32::try_from(used_count).unwrap_or(u32::MAX)),
        snapshot_slot: std::sync::atomic::AtomicU64::new(snapshot_slot),
        compact_ts: std::sync::atomic::AtomicU64::new(recovered_compact_ts),
        compacted_ready: std::sync::atomic::AtomicBool::new(true),
        zone_lock: std::sync::RwLock::new(()),
        uncompacted_free_record_count: std::sync::atomic::AtomicU32::new(0),
        cas_retry_count: std::sync::atomic::AtomicU64::new(0),
        metrics_cas_retry: None,
    };

    Ok(zone)
}

/// Merge two slot-sorted op lists into one slot-sorted list. Within a
/// slot, busy ops come before free ops (matching the `batch_write`
/// order in `persist_free`: delete busy, put free).
fn merge_ops_by_slot(busy: &[JournalOp], free: &[JournalOp]) -> Vec<JournalOp> {
    let mut merged = Vec::with_capacity(busy.len() + free.len());
    let mut bi = 0usize;
    let mut fi = 0usize;
    while bi < busy.len() && fi < free.len() {
        if busy[bi].slot <= free[fi].slot {
            merged.push(busy[bi].clone());
            bi += 1;
        } else {
            merged.push(free[fi].clone());
            fi += 1;
        }
    }
    while bi < busy.len() {
        merged.push(busy[bi].clone());
        bi += 1;
    }
    while fi < free.len() {
        merged.push(free[fi].clone());
        fi += 1;
    }
    merged
}

/// Find the `unit_count` from a `FreeBlockValue` Put at `slot` for the
/// given `unit_offset`. Used during replay to get the `unit_count` for
/// a `range_clear` when a `BusyBlockKey` Delete is encountered (the
/// Delete carries only the key, not the value).
fn find_free_unit_count_at_slot(ops: &[JournalOp], slot: u64, unit_offset: u64) -> Option<u32> {
    for op in ops {
        if op.slot != slot {
            continue;
        }
        if op.is_delete {
            continue;
        }
        if let Ok(fk) = FreeBlockKey::from_bytes(&op.key) {
            if fk.unit_offset == unit_offset {
                if let Ok(fv) = bincode::deserialize::<FreeBlockValue>(&op.value) {
                    return Some(fv.unit_count);
                }
            }
        }
    }
    None
}

/// Check whether a `ZoneValue` snapshot exists for any zone on the
/// given disk — used by the keep-alive loop to decide between recovery
/// (snapshots exist) and `disk_add_init` (fresh disks).
pub async fn zone_snapshots_exist(kv: &DdbKvClient, bind: Bind, disk_id: &DiskId, zone_count: u32) -> bool {
    // Check the first zone only — if it has a snapshot, the disk was
    // previously initialized (disk_add_init writes baseline snapshots
    // for all zones). This avoids `zone_count` round-trips in the
    // common case.
    if zone_count == 0 {
        return false;
    }
    matches!(kv.get_zone_value(bind, disk_id, 0).await, Ok(Some(_)))
}
