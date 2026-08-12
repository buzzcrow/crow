// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Per-instance node/disk-group/disk container.

mod container;
mod disk;

pub use container::NodeContainer;
pub use disk::ZoneDisk;

use crow_protocol::common::HwStatus;
use crow_protocol::DiskGroupId;
use std::sync::RwLock;

/// A disk-group manager — one per owned disk-group.
pub struct Node {
    pub disk_group_id: DiskGroupId,
    pub node_id: u64,
    pub rack_id: u64,
    pub status: RwLock<HwStatus>,
    /// `(store_id, group_id)` for the bound paxos data group.
    pub bind: RwLock<(u64, u64)>,
    pub disks: RwLock<Vec<ZoneDisk>>,
}

impl Node {
    pub fn new(disk_group_id: DiskGroupId, node_id: u64, rack_id: u64) -> Self {
        Self {
            disk_group_id,
            node_id,
            rack_id,
            status: RwLock::new(HwStatus::Up),
            bind: RwLock::new((0, 0)),
            disks: RwLock::new(Vec::new()),
        }
    }
}

/// Round-robin cursor placeholder for R72 allocation.
pub type AllocateDiskContext = ();

/// RCU context placeholder for R72.
pub type ActiveZoneContext = ();
