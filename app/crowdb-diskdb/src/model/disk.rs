// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! `DdbDisk` — disk struct with zone management and the disk-level
//! round-robin allocator (`disk_allocate`, `rotate_active_zones`).

use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU64, Ordering};
use std::sync::Arc;

use arc_swap::ArcSwap;
use crowdb_protocol::common::{DiskId, HwStatus};
use crowdb_protocol::diskdb::rpc::DiskValue;
use crowdb_protocol::{DiskGroupId, NodeId, RackId};

use crate::metrics::DiskMetrics;
use crate::model::zone::{AllocatedRange, DdbZone, ZoneUsage};

/// RCU-published active zone set — `zone_rotate_count` allocatable
/// zones, replaced via `Arc` swap on rotation.
pub type ActiveZoneContext = Vec<Arc<DdbZone>>;

/// Disk struct — one per physical disk in an owned disk-group.
pub struct DdbDisk {
    pub disk_id: DiskId,
    pub disk_group_id: DiskGroupId,
    pub node_id: NodeId,
    pub rack_id: RackId,
    pub disk_value: DiskValue,
    /// All zones on this disk, indexed by `zone_index`.
    pub zones: ArcSwap<Vec<Arc<DdbZone>>>,
    /// Round-robin cursor for zone rotation scan.
    pub pos_v_zone: AtomicU64,
    /// RCU-published active zone set.
    pub active_zone_context: ArcSwap<ActiveZoneContext>,
    /// Round-robin cursor over the active set.
    pub pos_v_zone_ctx: AtomicU64,
    /// Effective `HwStatus` for this disk (node/group/disk combined).
    effective_status: AtomicI32,
    /// R74 per-disk hot-path counters. `None` in tests that don't
    /// track metrics; attached during `disk_add_init` and
    /// `load_disk_group`.
    pub metrics: Option<Arc<DiskMetrics>>,
    /// Whether a background zone load task has been spawned for this
    /// disk. Set atomically before spawning to prevent duplicate
    /// spawns from `reconcile_existing_disk` across sync ticks.
    zone_load_spawned: AtomicBool,
}

impl DdbDisk {
    pub fn new(
        disk_id: DiskId,
        disk_group_id: DiskGroupId,
        node_id: NodeId,
        rack_id: RackId,
        disk_value: DiskValue,
    ) -> Self {
        Self {
            disk_id,
            disk_group_id,
            node_id,
            rack_id,
            disk_value,
            zones: ArcSwap::from_pointee(Vec::new()),
            pos_v_zone: AtomicU64::new(0),
            active_zone_context: ArcSwap::from_pointee(Vec::new()),
            pos_v_zone_ctx: AtomicU64::new(0),
            effective_status: AtomicI32::new(HwStatus::Init as i32),
            metrics: None,
            zone_load_spawned: AtomicBool::new(false),
        }
    }

    /// Add a zone to this disk.
    pub fn add_zone(&self, zone: &Arc<DdbZone>) {
        self.zones.rcu(|current| {
            let mut zones = (**current).clone();
            zones.push(Arc::clone(zone));
            Arc::new(zones)
        });
    }

    /// Atomically claim the right to spawn a background zone load task.
    /// Returns `true` if this call is the first to claim (the caller
    /// should spawn the task), `false` if a task was already spawned.
    #[must_use]
    pub fn try_claim_zone_load(&self) -> bool {
        self.zone_load_spawned
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    /// Whether this disk can accept allocations.
    pub fn allocatable(&self) -> bool {
        self.effective_status() == HwStatus::Up
    }

    pub fn effective_status(&self) -> HwStatus {
        HwStatus::try_from(self.effective_status.load(Ordering::Acquire)).unwrap_or(HwStatus::Init)
    }

    /// Directly set the effective status, bypassing transition
    /// legality. Production code uses `HwStateMachine::transition_disk`
    /// (which validates + applies + runs entry side-effects); this is
    /// a test/direct-set helper. Zones follow the disk-level
    /// `HwStatus` — no per-zone marking.
    pub fn set_effective_status(&self, status: HwStatus) {
        self.effective_status.store(status as i32, Ordering::Release);
    }

    /// Disk-level allocate — round-robin over the active zone set,
    /// rotating when exhausted.
    ///
    /// Returns `(zone, AllocatedRange)` on success, `None` if the disk
    /// is not `Up` or all zones are full.
    pub fn disk_allocate(
        &self,
        unit_count: u32,
        cas_retry_limit: u32,
        zone_rotate_count: u32,
    ) -> Option<(Arc<DdbZone>, AllocatedRange)> {
        if !self.allocatable() {
            return None;
        }
        let zones = self.zones.load();
        let zone_num = zones.len();
        if zone_num == 0 {
            return None;
        }
        let max_loop = zone_num / zone_rotate_count as usize + 2;

        for _ in 0..max_loop {
            // RCU read: clone the Arc, drop the lock.
            let ctx = self.active_zone_context.load_full();
            let active = ctx.as_ref();
            if active.is_empty() {
                if !self.rotate_active_zones(&ctx, zone_rotate_count) {
                    return None;
                }
                continue;
            }
            let ctx_len = active.len();
            #[allow(clippy::cast_possible_truncation)]
            let start = self.pos_v_zone_ctx.fetch_add(1, Ordering::Relaxed) as usize % ctx_len;
            for i in 0..ctx_len {
                let zone = &active[(start + i) % ctx_len];
                if let Some(range) = zone.allocate(unit_count, cas_retry_limit) {
                    return Some((Arc::clone(zone), range));
                }
            }
            // All zones in the active set failed — rotate.
            if !self.rotate_active_zones(&ctx, zone_rotate_count) {
                return None;
            }
        }
        None
    }

    /// Rotate the active zone set: scan all zones from `pos_v_zone`
    /// (rotating start), pick the first `zone_rotate_count` allocatable
    /// zones that are `compacted_ready` (I5 — compaction-before-rotation).
    /// If fewer ready zones exist than needed, falls back to any
    /// allocatable zone (synchronous compaction will run on the next
    /// cycle). RCU-publish the new context and clear `compacted_ready`
    /// on the published zones.
    ///
    /// Returns `false` if no allocatable zones remain.
    fn rotate_active_zones(&self, old_ctx: &Arc<ActiveZoneContext>, zone_rotate_count: u32) -> bool {
        let zones = self.zones.load();
        let zone_num = zones.len();
        if zone_num == 0 {
            return false;
        }
        // RCU check: if another thread already rotated, retry.
        let current = self.active_zone_context.load_full();
        if !Arc::ptr_eq(&current, old_ctx) {
            return true;
        }
        #[allow(clippy::cast_possible_truncation)]
        let start = self.pos_v_zone.load(Ordering::Relaxed) as usize % zone_num;
        let mut new_ctx: Vec<Arc<DdbZone>> = Vec::with_capacity(zone_rotate_count as usize);
        // First pass: pick compacted_ready + allocatable zones (I5).
        for i in 0..zone_num {
            if new_ctx.len() >= zone_rotate_count as usize {
                break;
            }
            let zone = &zones[(start + i) % zone_num];
            if zone.allocatable() && zone.compacted_ready.load(Ordering::Acquire) {
                new_ctx.push(Arc::clone(zone));
            }
        }
        // Second pass (fallback): if not enough ready zones, pick any
        // allocatable zone. The preparatory thread or periodic
        // compaction will compact them later.
        if new_ctx.len() < zone_rotate_count as usize {
            for i in 0..zone_num {
                if new_ctx.len() >= zone_rotate_count as usize {
                    break;
                }
                let zone = &zones[(start + i) % zone_num];
                if zone.allocatable() && !new_ctx.iter().any(|z| z.zone_index == zone.zone_index) {
                    new_ctx.push(Arc::clone(zone));
                }
            }
        }
        // Advance pos_v_zone past the scanned range.
        self.pos_v_zone.fetch_add(zone_num as u64, Ordering::Relaxed);
        let new_ctx = Arc::new(new_ctx);
        let populated = !new_ctx.is_empty();
        let previous = self
            .active_zone_context
            .compare_and_swap(old_ctx, Arc::clone(&new_ctx));
        if !Arc::ptr_eq(&previous, old_ctx) {
            return true;
        }
        for zone in new_ctx.iter() {
            zone.clear_compacted_ready();
        }
        populated
    }

    /// Record a persist-only free in a specific zone: increment
    /// `uncompacted_free_record_count`. No bitmap mutation, no
    /// `used_count` decrement — the bitmap is a conservative over-
    /// estimate (I1). Compaction is the sole bit-clearer (I3).
    pub fn free(&self, zone_index: u32, _unit_offset: u64, _unit_count: u32) -> bool {
        let zones = self.zones.load();
        let idx = zone_index as usize;
        if idx >= zones.len() {
            return false;
        }
        zones[idx].record_free();
        true
    }

    /// Build the initial active zone set with the first
    /// `zone_rotate_count` allocatable zones. Called by disk-add init
    /// (§3.5) and recovery (R73).
    pub fn rebuild_active_zones(&self, zone_rotate_count: u32) {
        let zones = self.zones.load();
        let mut new_ctx: Vec<Arc<DdbZone>> = Vec::with_capacity(zone_rotate_count as usize);
        for zone in zones.iter() {
            if new_ctx.len() >= zone_rotate_count as usize {
                break;
            }
            if zone.allocatable() {
                new_ctx.push(Arc::clone(zone));
            }
        }
        self.active_zone_context.store(Arc::new(new_ctx));
    }

    // ── R74 space-metrics accessors ───────────────────────────────

    /// The disk's `unit_size_bytes` (from `disk_value`).
    fn unit_size_bytes(&self) -> u32 {
        self.disk_value.unit_size_bytes
    }

    /// Aggregated usage across all zones (R74 §2). Reads `disk_value`
    /// and `zones` under their read locks; `active_zone_count` is the
    /// RCU active-set size. A `Bad` disk is still summed (its zones
    /// carry their last-known `used_count`); it is excluded from
    /// `allocatable_disk_count` at the disk-group level, not here.
    #[must_use]
    pub fn usage(&self) -> DiskUsage {
        let unit_size_bytes = self.unit_size_bytes();
        let zones = self.zones.load();
        let mut capacity_bytes = 0u64;
        let mut busy_bytes = 0u64;
        let mut busy_zone_count = 0u32;
        for zone in zones.iter() {
            capacity_bytes += zone.capacity_bytes(unit_size_bytes);
            busy_bytes += zone.busy_bytes(unit_size_bytes);
            if zone.busy_blocks() == zone.unit_capacity {
                busy_zone_count += 1;
            }
        }
        #[allow(clippy::cast_possible_truncation)]
        let zone_count = zones.len() as u32;
        #[allow(clippy::cast_possible_truncation)]
        let active_zone_count = self.active_zone_context.load().len() as u32;
        let free_bytes = capacity_bytes.saturating_sub(busy_bytes);
        let free_zone_count = zone_count.saturating_sub(busy_zone_count);
        DiskUsage {
            disk_id: self.disk_id,
            capacity_bytes,
            busy_bytes,
            free_bytes,
            zone_count,
            active_zone_count,
            busy_zone_count,
            free_zone_count,
        }
    }

    /// Build a list of brief per-zone `ZoneUsage` entries (counts only
    /// — no bitmap bytes). Used by the disk-level `QueryCapacityStats`
    /// shape (R74 §4).
    #[must_use]
    pub fn zone_usages(&self) -> Vec<ZoneUsage> {
        let unit_size_bytes = self.unit_size_bytes();
        let zones = self.zones.load();
        zones
            .iter()
            .map(|z| ZoneUsage::from_zone(z, unit_size_bytes))
            .collect()
    }
}

/// Per-disk usage (aggregated across zones, R74 §2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiskUsage {
    pub disk_id: DiskId,
    pub capacity_bytes: u64,
    pub busy_bytes: u64,
    pub free_bytes: u64,
    pub zone_count: u32,
    pub active_zone_count: u32,
    /// Zones with `used_count == unit_capacity`.
    pub busy_zone_count: u32,
    /// Zones with `used_count < unit_capacity`.
    pub free_zone_count: u32,
}
