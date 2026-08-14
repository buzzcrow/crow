// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Startup zone loading + snapshot compaction (R73).
//!
//! `ZoneLoader` reconstructs in-memory `DdbDiskGroup`/`DdbDisk`/
//! `DdbZone` state from the durable records on the bound data group
//! after a restart or ownership transfer. Two strategies:
//!
//! - **Strategy 1** (`recovery::full_scan::rebuild_zone_bitmap_full_scan`)
//!   — scan all live `BusyBlockKey` records for a zone and set bits.
//!   Always available, O(all live busy records per zone). Used as the
//!   fallback when strategy 2 cannot run.
//! - **Strategy 2** (`recovery::journal_replay::load_zone_inner`) —
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

use crow_protocol::common::{DiskId, HwStatus};
use crow_protocol::diskdb::rpc::DiskValue;
use crow_protocol::{DiskGroupId, NodeId, RackId};

use crate::ddb_kv_client::{Bind, DdbKvClient};
use crate::liveness::state_machine::HwStateMachine;
use crate::metrics::DiskMetrics;
use crate::model::disk::DdbDisk;
use crate::model::disk_group::DdbDiskGroup;
use crate::model::zone::DdbZone;

pub use full_scan::rebuild_zone_bitmap_full_scan;

/// Zone load errors.
#[derive(Debug)]
pub enum ZoneLoadError {
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

impl std::fmt::Display for ZoneLoadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Kv(e) => write!(f, "kv error: {e}"),
            Self::JournalScanGcGap => write!(f, "journal scan gc gap (slots already GC'd)"),
            Self::SnapshotCrcFail => write!(f, "snapshot CRC mismatch"),
        }
    }
}

impl std::error::Error for ZoneLoadError {}

impl From<crow_kv_client::Error> for ZoneLoadError {
    fn from(e: crow_kv_client::Error) -> Self {
        Self::Kv(e)
    }
}

/// Per-zone load stats (returned by strategy 1 for the
/// `RebuildZoneBitmap` RPC response).
#[derive(Debug, Clone, Default)]
pub struct ZoneStats {
    pub capacity_units: u32,
    pub used_units: u64,
    pub free_units: u64,
}

/// Startup zone loader — reconstructs in-memory `DdbDiskGroup`/
/// `DdbDisk`/`DdbZone` state from durable records on the bound data
/// group after a restart or ownership transfer.
///
/// Owns a `DdbKvClient` (from the server wiring). Disk metadata
/// (`DiskValue`s) is passed in by the caller (the keep-alive loop
/// already fetches it from group 0 via `HardwareClient`); the loader
/// does not need a group-0 client itself.
pub struct ZoneLoader {
    kv: Arc<DdbKvClient>,
    /// Max concurrent zone loads in `load_disk_group`.
    load_concurrency: usize,
}

impl ZoneLoader {
    /// Create a new zone loader with the given data-group client
    /// and zone-load concurrency limit.
    #[must_use]
    pub fn new(kv: Arc<DdbKvClient>, load_concurrency: usize) -> Self {
        Self {
            kv,
            load_concurrency: load_concurrency.max(1),
        }
    }

    /// Load a full disk-group from the data group's records.
    /// Creates an empty `DdbDiskGroup`, loads each disk's zones
    /// (strategy 2 with strategy 1 fallback), and returns the
    /// reconstructed `DdbDiskGroup`.
    ///
    /// Zones within a disk-group are loaded in parallel, bounded by
    /// `load_concurrency` — each zone's load is independent.
    #[allow(clippy::too_many_arguments)]
    pub async fn load_disk_group(
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

        // Track max freed_ts across all zones in all disks — used to
        // seed the per-disk-group monotonic timestamp source (§8).
        let mut max_freed_ts_in_dg: u64 = 0;

        for (disk_id, disk_value) in disks {
            let mut disk = DdbDisk::new(*disk_id, dg_id, node_id, rack_id, *disk_value);
            // Attach per-disk hot-path counters (R74 §3).
            disk.metrics = Some(Arc::new(DiskMetrics::new()));
            let disk = Arc::new(disk);

            let zone_count = disk_value.zone_count;
            let zone_size_units = disk_value.zone_size_units;

            // Load zones in parallel, bounded by the semaphore.
            let sem = Arc::new(tokio::sync::Semaphore::new(self.load_concurrency));
            let mut zone_handles = Vec::with_capacity(zone_count as usize);
            for zi in 0..zone_count {
                let unit_capacity = unit_capacity_for_zone(disk_value, zi, zone_count, zone_size_units);
                let disk_id = *disk_id;
                let kv = Arc::clone(&self.kv);
                let sem = Arc::clone(&sem);
                let handle = tokio::spawn(async move {
                    let _permit = sem.acquire().await;
                    journal_replay::load_zone_inner(&kv, bind, disk_id, zi, dg_id, unit_capacity)
                        .await
                        .map(|(z, ts, _)| (z, ts))
                });
                zone_handles.push((zi, handle));
            }

            for (zi, handle) in zone_handles {
                let unit_capacity = unit_capacity_for_zone(disk_value, zi, zone_count, zone_size_units);
                let (zone, zone_max_freed_ts) = match handle.await {
                    Ok(Ok((zone, max_freed_ts))) => (zone, max_freed_ts),
                    Ok(Err(e)) => {
                        tracing::warn!(
                            disk_id = ?*disk_id,
                            zone_index = zi,
                            error = %e,
                            "zone load failed; falling back to strategy 1 (full scan)"
                        );
                        // Fallback: strategy 1 full scan. No freed_ts
                        // available (strategy 1 doesn't scan free
                        // records).
                        match full_scan::rebuild_zone_bitmap_full_scan(
                            &self.kv,
                            bind,
                            *disk_id,
                            zi,
                            dg_id,
                            unit_capacity,
                        )
                        .await
                        {
                            Ok((zone, _stats)) => (zone, 0),
                            Err(e2) => {
                                tracing::error!(
                                    disk_id = ?*disk_id,
                                    zone_index = zi,
                                    error = %e2,
                                    "strategy 1 fallback also failed; using empty zone"
                                );
                                (DdbZone::new(*disk_id, zi, dg_id, unit_capacity), 0)
                            }
                        }
                    }
                    Err(e) => {
                        tracing::error!(
                            disk_id = ?*disk_id,
                            zone_index = zi,
                            error = %e,
                            "zone load task panicked; using empty zone"
                        );
                        (DdbZone::new(*disk_id, zi, dg_id, unit_capacity), 0)
                    }
                };
                disk.add_zone(Arc::new(zone));
                // Track max freed_ts across all zones for timestamp
                // source seeding (§8).
                if zone_max_freed_ts > max_freed_ts_in_dg {
                    max_freed_ts_in_dg = zone_max_freed_ts;
                }
            }

            disk.rebuild_active_zones(zone_rotate_count);
            // Transition Init → Up after zones are loaded. `DdbDisk::new`
            // defaults to `Init` (R81); the startup path loads zones
            // then transitions to `Up` so the disk becomes allocatable.
            let sm = HwStateMachine::new(0);
            if let Err(e) = sm.transition_disk(&disk, HwStatus::Up) {
                tracing::warn!(
                    disk_id = ?*disk_id,
                    error = %e,
                    "load_disk_group: Init → Up transition failed; disk stays Init"
                );
            }
            dg.add_disk(disk);
            dg.rebuild_allocating_disks();
        }

        // Seed the per-disk-group monotonic timestamp source to
        // max(now(), max(freed_ts of all scanned free records) + 1)
        // (§8). This ensures new frees get freed_ts values strictly
        // greater than any pre-existing free record — critical for
        // ownership transfer where the prior owner's clock may be
        // ahead of ours.
        dg.init_free_ts_source_after_load(max_freed_ts_in_dg);

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
        disk_group_id: DiskGroupId,
        unit_capacity: u32,
    ) -> Result<(DdbZone, ZoneStats), ZoneLoadError> {
        full_scan::rebuild_zone_bitmap_full_scan(
            &self.kv,
            bind,
            disk_id,
            zone_idx,
            disk_group_id,
            unit_capacity,
        )
        .await
    }
}

/// Compute the unit capacity for zone `zi` on a disk with the given
/// `zone_count`, `zone_size_units`, and `capacity_units`. The last
/// zone may be smaller (rounded down to a multiple of 64), matching
/// `keepalive.rs::disk_add_init`.
#[allow(clippy::cast_possible_truncation)]
pub(crate) fn unit_capacity_for_zone(
    disk_value: &DiskValue,
    zi: u32,
    zone_count: u32,
    zone_size_units: u64,
) -> u32 {
    if zi == zone_count - 1 {
        let remaining = disk_value.capacity_units - (u64::from(zi) * zone_size_units);
        let rounded = (remaining / 64) * 64;
        u32::try_from(rounded).unwrap_or(u32::MAX)
    } else {
        // zone_size_units is validated to fit u32 at disk-add time
        // (http_add_disk rejects zone_size_bytes / unit_size_bytes
        // > u32::MAX).
        u32::try_from(zone_size_units).unwrap_or(u32::MAX)
    }
}
