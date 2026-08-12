// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! `ZoneDisk` — disk struct with zone management (allocation stubbed for R72).

use crow_protocol::common::DiskId;
use crow_protocol::diskdb::rpc::DiskValue;
use crow_protocol::{DiskGroupId, NodeId, RackId};
use std::sync::atomic::AtomicU64;
use std::sync::RwLock;

/// Reference to a zone (stub for R72).
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct ZoneRef {
    pub(crate) zone_index: u32,
}

/// Disk struct — one per physical disk in an owned disk-group.
#[allow(dead_code)]
pub(crate) struct ZoneDisk {
    pub(crate) disk_id: DiskId,
    pub(crate) disk_group_id: DiskGroupId,
    pub(crate) node_id: NodeId,
    pub(crate) rack_id: RackId,
    pub(crate) disk_value: RwLock<DiskValue>,
    pub(crate) zones: RwLock<Vec<ZoneRef>>,
    /// Round-robin cursor for zone rotation scan (R72).
    pub(crate) pos_v_zone: AtomicU64,
}

#[allow(dead_code)]
impl ZoneDisk {
    pub(crate) fn new(
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
        }
    }

    /// Add a zone to this disk (zone allocation logic is R72).
    pub(crate) fn add_zone(&self, zone: ZoneRef) {
        self.zones.write().unwrap().push(zone);
    }

    /// Rebuild the active zone set (R72).
    #[allow(clippy::unused_self)]
    pub(crate) fn rebuild_active_zones(&self) {
        // R72: implement active zone set selection.
    }
}
