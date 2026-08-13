// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Crash recovery + snapshot compaction (R73).
//!
//! `RecoveryEngine` reconstructs in-memory `DdbDiskGroup`/`DdbDisk`/
//! `DdbZone` state from the durable records on the bound data group
//! after a restart or ownership transfer. Two strategies:
//!
//! - **Strategy 1** (`recovery::full_scan::rebuild_zone_bitmap_full_scan`)
//!   — scan all live `BusyBlockKey` records for a zone and set bits.
//!   Always available, O(all live busy records per zone). Used as the
//!   fallback when strategy 2 cannot run.
//! - **Strategy 2** (`recovery::journal_replay::recover_zone_inner`) —
//!   load the latest `ZoneValue` snapshot, then replay the journal
//!   (slot-ordered `JournalScan` of busy + free ops) from
//!   `snapshot_slot + 1` to the applied frontier. Fast when compaction
//!   (strategy 3) keeps the uncompacted record set small.
//!
//! See `doc/working/design-diskdb-recovery.md` for the full design.

pub mod compaction;
pub mod disk_recovery;
pub mod full_scan;
pub mod journal_replay;

use std::sync::Arc;

use crow_protocol::common::DiskId;
use crow_protocol::diskdb::rpc::DiskValue;
use crow_protocol::{DiskGroupId, NodeId, RackId};

use crate::diskdb_kv_client::{Bind, DiskDBKVClient};
use crate::model::disk::DdbDisk;
use crate::model::disk_group::DdbDiskGroup;
use crate::model::zone::DdbZone;

pub use full_scan::rebuild_zone_bitmap_full_scan;
pub use journal_replay::zone_snapshots_exist;

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
/// Owns a `DiskDBKVClient` (from the server wiring). Disk metadata
/// (`DiskValue`s) is passed in by the caller (the keep-alive loop
/// already fetches it from group 0 via `HardwareClient`); the recovery
/// engine does not need a group-0 client itself.
pub struct RecoveryEngine {
    kv: Arc<DiskDBKVClient>,
    /// Max concurrent zone recoveries in `recover_disk_group`.
    recovery_concurrency: usize,
}

impl RecoveryEngine {
    /// Create a new recovery engine with the given data-group client
    /// and zone-recovery concurrency limit.
    #[must_use]
    pub fn new(kv: Arc<DiskDBKVClient>, recovery_concurrency: usize) -> Self {
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
                    journal_replay::recover_zone_inner(&kv, bind, disk_id, zi, unit_capacity).await
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
                        match full_scan::rebuild_zone_bitmap_full_scan(
                            &self.kv,
                            bind,
                            *disk_id,
                            zi,
                            unit_capacity,
                        )
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
    /// the live `BusyBlockKey` records on the data group. Delegates to
    /// `recovery::full_scan::rebuild_zone_bitmap_full_scan`.
    pub async fn rebuild_zone_bitmap_full_scan(
        &self,
        bind: Bind,
        disk_id: DiskId,
        zone_idx: u32,
        unit_capacity: u32,
    ) -> Result<(DdbZone, ZoneStats), RecoveryError> {
        full_scan::rebuild_zone_bitmap_full_scan(&self.kv, bind, disk_id, zone_idx, unit_capacity).await
    }
}

/// Compute the unit capacity for zone `zi` on a disk with the given
/// `zone_count`, `zone_size_units`, and `capacity_units`. The last
/// zone may be smaller (rounded down to a multiple of 64), matching
/// `keepalive.rs::disk_add_init`.
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
