// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 0.0.

//! Crash recovery + snapshot compaction (R73).
//!
//! `RecoveryEngine` reconstructs in-memory `Node`/`ZoneDisk`/`Zone`
//! state from the durable records on the bound data group after a
//! restart or ownership transfer. Two strategies:
//!
//! - **Strategy 1** (`rebuild_zone_bitmap_full_scan`) — scan all live
//!   `BusyBlockKey` records for a zone and set bits. Always available,
//!   O(all live busy records per zone). Used as the fallback when
//!   strategy 2 cannot run (no snapshot, CRC fail, journal GC gap).
//! - **Strategy 2** (`recover_zone`) — load the latest `ZoneValue`
//!   snapshot, then replay the journal (slot-ordered `JournalScan` of
//!   busy + free ops) from `snapshot_slot + 1` to the applied
//!   frontier. Fast when compaction (strategy 3) keeps the
//!   uncompacted record set small.
//!
//! See `doc/working/design-diskdb-recovery.md` for the full design.

pub mod compaction;

use std::sync::Arc;

use crow_kv_client::JournalOp;
use crow_protocol::common::DiskId;
use crow_protocol::diskdb::rpc::{BusyBlockValue, DiskValue, FreeBlockValue};
use crow_protocol::key::{BinaryKey, BusyBlockKey, FreeBlockKey};
use crow_protocol::{DiskGroupId, NodeId, RackId, ZoneValueExt};

use crate::data_group_client::{Bind, DataGroupClient};
use crate::domain::disk::DdbDisk;
use crate::domain::disk_group::DdbDiskGroup;
use crate::domain::records::ZoneRecords;
use crate::domain::zone::{DdbZone, DdbZoneHealth};

/// Recovery errors.
#[derive(Debug)]
pub enum RecoveryError {
    /// KV client error.
    Kv(crow_kv_client::Error),
    /// A `JournalScan` returned `KV_ERROR_JOURNAL_SCAN_GC_GAP` — slots
    /// already GC'd below the WAL trim point. The caller should fall
    /// back to strategy 1 (full scan) for this zone.
    JournalScanGcGap,
    /// Snapshot CRC mismatch — the `ZoneValue` is corrupted. The
    /// caller should fall back to strategy 1.
    SnapshotCrcFail,
}

impl std::fmt::Display for RecoveryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Kv(e) => write!(f, "kv error: {e}"),
            Self::JournalScanGcGap => write!(f, "journal scan gc gap (slots already GC'd)"),
            Self::SnapshotCrcFail => write!(f, "snapshot CRC mismatch"),
        }
    }
}

impl std::error::Error for RecoveryError {}

impl From<crow_kv_client::Error> for RecoveryError {
    fn from(e: crow_kv_client::Error) -> Self {
        Self::Kv(e)
    }
}

/// Per-zone recovery stats (returned by strategy 1 for the
/// `RebuildZoneBitmap` RPC response).
#[derive(Debug, Clone, Default)]
pub struct ZoneStats {
    pub capacity_units: u32,
    pub used_units: u64,
    pub free_units: u64,
}

/// Crash recovery + ownership-transfer reconstruction engine.
///
/// Owns a `DataGroupClient` (from the server wiring). Disk metadata
/// (`DiskValue`s) is passed in by the caller (the sync loop already
/// fetches it from group 0 via `HardwareClient`); the recovery engine
/// does not need a group-0 client itself.
pub struct RecoveryEngine {
    kv: Arc<DataGroupClient>,
    /// Max concurrent zone recoveries in `recover_node`.
    recovery_concurrency: usize,
}

impl RecoveryEngine {
    /// Create a new recovery engine with the given data-group client
    /// and zone-recovery concurrency limit.
    #[must_use]
    pub fn new(kv: Arc<DataGroupClient>, recovery_concurrency: usize) -> Self {
        Self {
            kv,
            recovery_concurrency: recovery_concurrency.max(1),
        }
    }

    /// Recover a full disk-group from the data group's records.
    /// Creates an empty `DdbDiskGroup`, recovers each disk's zones
    /// (strategy 2 with strategy 1 fallback), and returns the
    /// reconstructed `DdbDiskGroup`.
    ///
    /// Zones within a disk-group are recovered in parallel, bounded by
    /// `recovery_concurrency` — each zone's recovery is independent.
    #[allow(clippy::too_many_arguments)]
    pub async fn recover_disk_group(
        &self,
        dg_id: DiskGroupId,
        node_id: NodeId,
        rack_id: RackId,
        bind: Bind,
        disks: &[(DiskId, DiskValue)],
        zone_rotate_count: u32,
    ) -> Arc<DdbDiskGroup> {
        let dg = Arc::new(DdbDiskGroup::new(dg_id, node_id, rack_id));
        *dg.bind.write().unwrap() = bind;

        for (disk_id, disk_value) in disks {
            let disk = Arc::new(DdbDisk::new(*disk_id, dg_id, node_id, rack_id, *disk_value));

            let zone_count = disk_value.zone_count;
            let zone_size_units = disk_value.zone_size_units;

            // Recover zones in parallel, bounded by the semaphore.
            let sem = Arc::new(tokio::sync::Semaphore::new(self.recovery_concurrency));
            let mut zone_handles = Vec::with_capacity(zone_count as usize);
            for zi in 0..zone_count {
                let unit_capacity = unit_capacity_for_zone(disk_value, zi, zone_count, zone_size_units);
                let disk_id = *disk_id;
                let kv = Arc::clone(&self.kv);
                let sem = Arc::clone(&sem);
                let handle = tokio::spawn(async move {
                    let _permit = sem.acquire().await;
                    recover_zone_inner(&kv, bind, disk_id, zi, unit_capacity).await
                });
                zone_handles.push((zi, handle));
            }

            for (zi, handle) in zone_handles {
                let unit_capacity = unit_capacity_for_zone(disk_value, zi, zone_count, zone_size_units);
                let zone = match handle.await {
                    Ok(Ok(zone)) => zone,
                    Ok(Err(e)) => {
                        tracing::warn!(
                            disk_id = ?*disk_id,
                            zone_index = zi,
                            error = %e,
                            "zone recovery failed; falling back to strategy 1 (full scan)"
                        );
                        // Fallback: strategy 1 full scan.
                        match self
                            .rebuild_zone_bitmap_full_scan(bind, *disk_id, zi, unit_capacity)
                            .await
                        {
                            Ok((zone, _stats)) => zone,
                            Err(e2) => {
                                tracing::error!(
                                    disk_id = ?*disk_id,
                                    zone_index = zi,
                                    error = %e2,
                                    "strategy 1 fallback also failed; using empty zone"
                                );
                                DdbZone::new(*disk_id, zi, dg_id, unit_capacity)
                            }
                        }
                    }
                    Err(e) => {
                        tracing::error!(
                            disk_id = ?*disk_id,
                            zone_index = zi,
                            error = %e,
                            "zone recovery task panicked; using empty zone"
                        );
                        DdbZone::new(*disk_id, zi, dg_id, unit_capacity)
                    }
                };
                disk.add_zone(Arc::new(zone));
            }

            disk.rebuild_active_zones(zone_rotate_count);
            dg.add_disk(disk);
            dg.rebuild_allocating_disks();
        }

        dg
    }

    /// Strategy 1 — full scan rebuild of one zone's usage bitmap from
    /// the live `BusyBlockKey` records on the data group. Always
    /// available, O(all live busy records per zone). On-demand via the
    /// `RebuildZoneBitmap` RPC and the §12 scanner (R75). Also used as
    /// the fallback when strategy 2 cannot run.
    ///
    /// Optionally writes a fresh `ZoneValue` snapshot after the rebuild
    /// so the next restart can use strategy 2.
    pub async fn rebuild_zone_bitmap_full_scan(
        &self,
        bind: Bind,
        disk_id: DiskId,
        zone_idx: u32,
        unit_capacity: u32,
    ) -> Result<(DdbZone, ZoneStats), RecoveryError> {
        let records: ZoneRecords = self.kv.read_zone_records(bind, &disk_id, zone_idx).await?;

        let zone = DdbZone::new(disk_id, zone_idx, 0, unit_capacity);
        for busy in &records.busy {
            #[allow(clippy::cast_possible_truncation)]
            let offset = busy.key.unit_offset as u32;
            let _ = zone.usage_bits.range_set(offset, busy.value.unit_count);
        }
        // `used_count` = popcount of the rebuilt bitmap (may differ
        // from the sum of `unit_count`s if there were overlapping
        // records — shouldn't happen in normal operation, but
        // popcount is the truthful count).
        let popcount = zone.usage_bits.count_set();
        zone.used_count.store(
            u32::try_from(popcount).unwrap_or(u32::MAX),
            std::sync::atomic::Ordering::Release,
        );

        // Optionally write a fresh ZoneValue snapshot so the next
        // restart can use strategy 2. Anchor it at the current applied
        // frontier.
        let snapshot_slot = match self.kv.get_applied_slot(bind).await {
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
            if let Err(e) = self.kv.put_zone(bind, &disk_id, zone_idx, &zv).await {
                tracing::warn!(
                    disk_id = ?disk_id,
                    zone_index = zone_idx,
                    error = %e,
                    "post-rebuild snapshot write failed; recovery still valid"
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
}

/// Compute the unit capacity for zone `zi` on a disk with the given
/// `zone_count`, `zone_size_units`, and `capacity_units`. The last
/// zone may be smaller (rounded down to a multiple of 64), matching
/// `sync.rs::disk_add_init`.
#[allow(clippy::cast_possible_truncation)]
fn unit_capacity_for_zone(disk_value: &DiskValue, zi: u32, zone_count: u32, zone_size_units: u64) -> u32 {
    if zi == zone_count - 1 {
        let remaining = disk_value.capacity_units - (u64::from(zi) * zone_size_units);
        let rounded = (remaining / 64) * 64;
        rounded as u32
    } else {
        zone_size_units as u32
    }
}

/// Inner zone recovery — strategy 2 (journal scan replay) with
/// strategy 1 fallback. Returns a recovered `Zone` or an error
/// indicating why strategy 2 failed (caller falls back to strategy 1).
async fn recover_zone_inner(
    kv: &DataGroupClient,
    bind: Bind,
    disk_id: DiskId,
    zone_idx: u32,
    unit_capacity: u32,
) -> Result<DdbZone, RecoveryError> {
    // Step a: load the latest ZoneValue snapshot.
    let snapshot = kv.get_zone_value(bind, &disk_id, zone_idx).await?;

    let (usage_bits, snapshot_slot, dg_id) = match &snapshot {
        Some(zv) if zv.verify_checksum() => {
            // Valid snapshot — restore bitmap + snapshot_slot.
            let bits = crow_protocol::UsageBitmap::restore(&zv.usage_bitmap);
            (bits, zv.snapshot_slot, 0)
        }
        Some(_zv) => {
            // CRC fail — fall back to strategy 1.
            return Err(RecoveryError::SnapshotCrcFail);
        }
        None => {
            // No snapshot — empty bitmap, replay from slot 0.
            (crow_protocol::UsageBitmap::new(unit_capacity), 0, 0)
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
            // Delete of a BusyBlockKey → range_clear. The unit_count
            // comes from the matching FreeBlockValue at the same slot
            // (the free batch_write deletes BusyBlockKey + puts
            // FreeBlockKey atomically). Decode the FreeBlockValue to
            // get unit_count.
            if let Ok(fk) = FreeBlockKey::from_bytes(&op.key) {
                // Look for a matching FreeBlockKey Put at the same
                // slot with the same unit_offset. The free ops list
                // was merged in; find it.
                let unit_count = find_free_unit_count_at_slot(&merged, op.slot, fk.unit_offset);
                if let Some(count) = unit_count {
                    #[allow(clippy::cast_possible_truncation)]
                    let offset = fk.unit_offset as u32;
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

    // Step e: build the recovered Zone.
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
        uncompacted_free_record_count: std::sync::atomic::AtomicU32::new(0),
        cas_retry_count: std::sync::atomic::AtomicU64::new(0),
        metrics_cas_retry: None,
    };

    Ok(zone)
}

/// Merge two slot-sorted op lists into one slot-sorted list. Within a
/// slot, busy ops come before free ops (matching the `batch_write` order
/// in `persist_free`: delete busy, put free).
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
/// given disk — used by the sync loop to decide between recovery
/// (snapshots exist) and `disk_add_init` (fresh disks).
pub async fn zone_snapshots_exist(
    kv: &DataGroupClient,
    bind: Bind,
    disk_id: &DiskId,
    zone_count: u32,
) -> bool {
    // Check the first zone only — if it has a snapshot, the disk was
    // previously initialized (disk_add_init writes baseline snapshots
    // for all zones). This avoids `zone_count` round-trips in the
    // common case.
    if zone_count == 0 {
        return false;
    }
    matches!(kv.get_zone_value(bind, disk_id, 0).await, Ok(Some(_)))
}
