// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Placement selector — chooses racks/nodes/disk-groups for strip blocks.
//!
//! Mirror: N distinct nodes across distinct racks (design §7.1).
//! EC: rack-aware safe/unsafe modes (design §7.2).

pub mod ec;
pub mod mirror;

use crow_protocol::{DiskGroupId, NodeId, RackId};

use crate::topology::TopologySnapshot;

/// Re-export the placement selector trait + implementations.
pub use ec::EcPlacement;
pub use mirror::MirrorPlacement;

/// Placement constraints — negative hints for exclusion.
#[derive(Debug, Clone, Default)]
pub struct PlacementConstraints {
    /// Nodes to exclude (e.g. failed or in recovery).
    pub exclude_nodes: Vec<NodeId>,
    /// Racks to exclude.
    pub exclude_racks: Vec<RackId>,
    /// Disk-groups to exclude.
    pub exclude_disk_groups: Vec<DiskGroupId>,
}

impl PlacementConstraints {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn exclude_node(mut self, node: NodeId) -> Self {
        self.exclude_nodes.push(node);
        self
    }

    #[must_use]
    pub fn exclude_rack(mut self, rack: RackId) -> Self {
        self.exclude_racks.push(rack);
        self
    }

    #[must_use]
    pub fn exclude_disk_group(mut self, dg: DiskGroupId) -> Self {
        self.exclude_disk_groups.push(dg);
        self
    }

    /// Check if a rack is excluded.
    pub fn is_rack_excluded(&self, rack: RackId) -> bool {
        self.exclude_racks.contains(&rack)
    }

    /// Check if a node is excluded.
    pub fn is_node_excluded(&self, node: NodeId) -> bool {
        self.exclude_nodes.contains(&node)
    }

    /// Check if a disk-group is excluded.
    pub fn is_dg_excluded(&self, dg: DiskGroupId) -> bool {
        self.exclude_disk_groups.contains(&dg)
    }
}

/// A single placement decision: where to place `block_count` blocks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlacementEntry {
    pub rack_id: RackId,
    pub node_id: NodeId,
    pub disk_group_id: DiskGroupId,
    pub block_count: u32,
}

/// The output of a placement selection — a list of entries.
#[derive(Debug, Clone)]
pub struct PlacementPlan {
    pub entries: Vec<PlacementEntry>,
    /// Whether safe mode was used (EC only; mirror always "safe").
    pub safe_mode: bool,
}

impl PlacementPlan {
    pub fn total_blocks(&self) -> u32 {
        self.entries.iter().map(|e| e.block_count).sum()
    }
}

/// Placement error.
#[derive(Debug, thiserror::Error)]
pub enum PlacementError {
    #[error("insufficient nodes: need {needed}, have {available}")]
    InsufficientNodes { needed: usize, available: usize },
    #[error("insufficient capacity after applying exclusion hints")]
    InsufficientCapacity,
    #[error("no healthy disk-groups available")]
    NoHealthyDiskGroups,
}

/// Filter healthy disk-groups from the snapshot, applying exclusion hints.
fn healthy_dgs(
    snap: &TopologySnapshot,
    constraints: &PlacementConstraints,
) -> Vec<crow_protocol::sysdata::DiskGroupEntry> {
    snap.healthy_disk_groups()
        .into_iter()
        .filter(|dg| {
            !constraints.is_rack_excluded(dg.rack_id)
                && !constraints.is_node_excluded(dg.node_id)
                && !constraints.is_dg_excluded(dg.dg_id)
        })
        .cloned()
        .collect()
}
