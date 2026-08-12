// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Per-instance node/disk-group/disk container.

mod container;
mod disk;

pub use container::NodeContainer;
pub(crate) use disk::ZoneDisk;

use crow_protocol::common::HwStatus;
use crow_protocol::DiskGroupId;
use std::sync::RwLock;

/// A disk-group manager — one per owned disk-group.
#[allow(dead_code, clippy::struct_field_names)]
pub(crate) struct Node {
    pub(crate) disk_group_id: DiskGroupId,
    pub(crate) node_id: u64,
    pub(crate) rack_id: u64,
    pub(crate) status: RwLock<HwStatus>,
    /// `(store_id, group_id)` for the bound paxos data group.
    pub(crate) bind: RwLock<(u64, u64)>,
    pub(crate) disks: RwLock<Vec<ZoneDisk>>,
}

impl Node {
    pub(crate) fn new(disk_group_id: DiskGroupId, node_id: u64, rack_id: u64) -> Self {
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
#[allow(dead_code)]
pub(crate) type AllocateDiskContext = ();

/// RCU context placeholder for R72.
#[allow(dead_code)]
pub(crate) type ActiveZoneContext = ();
