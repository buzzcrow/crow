// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! `DdbDisk` — disk struct with zone management and the disk-level
//! round-robin allocator (`disk_allocate`, `rotate_active_zones`).
//!
//! See `doc/working/design-diskdb-server.md` §4.2.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use crow_protocol::common::{DiskId, HwStatus};
use crow_protocol::diskdb::rpc::DiskValue;
use crow_protocol::{DiskGroupId, NodeId, RackId};

use crate::model::zone::{AllocatedRange, DdbZone, DdbZoneHealth};

/// RCU-published active zone set — `zone_rotate_count` allocatable
/// zones, replaced via `Arc` swap on rotation.
pub type ActiveZoneContext = Vec<Arc<DdbZone>>;

/// Disk struct — one per physical disk in an owned disk-group.
pub struct DdbDisk {
    pub disk_id: DiskId,
    pub disk_group_id: DiskGroupId,
    pub node_id: NodeId,
    pub rack_id: RackId,
    pub disk_value: RwLock<DiskValue>,
    /// All zones on this disk, indexed by `zone_index`.
    pub zones: RwLock<Vec<Arc<DdbZone>>>,
    /// Round-robin cursor for zone rotation scan.
    pub pos_v_zone: AtomicU64,
    /// RCU-published active zone set.
    pub active_zone_context: RwLock<Arc<ActiveZoneContext>>,
    /// Round-robin cursor over the active set.
    pub pos_v_zone_ctx: AtomicU64,
    /// Effective `HwStatus` for this disk (node/group/disk combined).
    pub effective_status: RwLock<HwStatus>,
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
            disk_value: RwLock::new(disk_value),
            zones: RwLock::new(Vec::new()),
            pos_v_zone: AtomicU64::new(0),
            active_zone_context: RwLock::new(Arc::new(Vec::new())),
            pos_v_zone_ctx: AtomicU64::new(0),
            effective_status: RwLock::new(HwStatus::Up),
        }
    }

    /// Add a zone to this disk.
    pub fn add_zone(&self, zone: Arc<DdbZone>) {
        self.zones.write().unwrap().push(zone);
    }

    /// Whether this disk can accept allocations.
    pub fn allocatable(&self) -> bool {
        *self.effective_status.read().unwrap() == HwStatus::Up
    }

    /// Set the effective status (called by `StatusManager`). When
    /// `Bad`, marks all zones `Bad`.
    pub fn set_effective_status(&self, status: HwStatus) {
        *self.effective_status.write().unwrap() = status;
        if status == HwStatus::Bad {
            // Mark all zones Bad.
            let zones = self.zones.read().unwrap();
            for z in zones.iter() {
                z.set_health(DdbZoneHealth::Bad);
            }
        }
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
        let zones = self.zones.read().unwrap();
        let zone_num = zones.len();
        if zone_num == 0 {
            return None;
        }
        let max_loop = zone_num / zone_rotate_count as usize + 2;

        for _ in 0..max_loop {
            // RCU read: clone the Arc, drop the lock.
            let ctx = Arc::clone(&self.active_zone_context.read().unwrap());
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
    /// (rotating start), pick the first `zone_rotate_count`
    /// allocatable zones, RCU-publish the new context.
    ///
    /// Returns `false` if no allocatable zones remain.
    fn rotate_active_zones(&self, old_ctx: &Arc<ActiveZoneContext>, zone_rotate_count: u32) -> bool {
        let zones = self.zones.read().unwrap();
        let zone_num = zones.len();
        if zone_num == 0 {
            return false;
        }
        // RCU check: if another thread already rotated, retry.
        {
            let current = self.active_zone_context.read().unwrap();
            if !Arc::ptr_eq(&current, old_ctx) {
                return true; // caller retries disk_allocate
            }
        }
        // Take write lock and re-check (double-checked locking).
        let mut ctx_guard = self.active_zone_context.write().unwrap();
        if !Arc::ptr_eq(&ctx_guard, old_ctx) {
            return true;
        }
        #[allow(clippy::cast_possible_truncation)]
        let start = self.pos_v_zone.load(Ordering::Relaxed) as usize % zone_num;
        let mut new_ctx: Vec<Arc<DdbZone>> = Vec::with_capacity(zone_rotate_count as usize);
        for i in 0..zone_num {
            if new_ctx.len() >= zone_rotate_count as usize {
                break;
            }
            let zone = &zones[(start + i) % zone_num];
            if zone.allocatable() {
                new_ctx.push(Arc::clone(zone));
            }
        }
        // Advance pos_v_zone past the scanned range.
        self.pos_v_zone.fetch_add(zone_num as u64, Ordering::Relaxed);
        *ctx_guard = Arc::new(new_ctx);
        !ctx_guard.is_empty()
    }

    /// Free a range in a specific zone.
    pub fn free(&self, zone_index: u32, unit_offset: u64, unit_count: u32) -> bool {
        let zones = self.zones.read().unwrap();
        let idx = zone_index as usize;
        if idx >= zones.len() {
            return false;
        }
        zones[idx].free(unit_offset, unit_count)
    }

    /// Build the initial active zone set with the first
    /// `zone_rotate_count` allocatable zones. Called by disk-add init
    /// (§3.5) and recovery (R73).
    pub fn rebuild_active_zones(&self, zone_rotate_count: u32) {
        let zones = self.zones.read().unwrap();
        let mut new_ctx: Vec<Arc<DdbZone>> = Vec::with_capacity(zone_rotate_count as usize);
        for zone in zones.iter() {
            if new_ctx.len() >= zone_rotate_count as usize {
                break;
            }
            if zone.allocatable() {
                new_ctx.push(Arc::clone(zone));
            }
        }
        *self.active_zone_context.write().unwrap() = Arc::new(new_ctx);
    }
}
