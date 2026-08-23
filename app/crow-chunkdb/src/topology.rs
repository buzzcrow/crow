// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Topology cache for chunkdb placement decisions.
//!
//! `TopologyCache` holds an `Arc<RwLock<TopologySnapshot>>` with the
//! current cluster hierarchy (racks, nodes, disk-groups). Placement
//! (R87) calls `snapshot()` to get a consistent point-in-time view.
//!
//! Update path: periodic full refresh from group-0 via
//! `HardwareClient` + watch/notify for immediate status changes.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;
use tracing::warn;

use crow_protocol::common::HwStatus;
use crow_protocol::sysdata::DiskGroupEntry;
use crow_protocol::{DiskGroupId, NodeId, RackId};

pub mod notify;
pub mod refresh;

/// Group-0 store + group ids (system group).
pub const G0_STORE: u64 = 0;
pub const G0_GROUP: u64 = 0;

/// Watch prefixes for chunkdb topology updates.
pub const CHUNKDB_WATCH_PREFIXES: &[&[u8]] = &[b"/hw/node/", b"/hw/dg/"];

/// `HwStatus::Up` as `i32` (prost represents enums as i32 in messages).
const HW_UP: i32 = HwStatus::Up as i32;

/// Point-in-time immutable topology snapshot.
///
/// Cloned from `TopologyCache` for a single placement decision;
/// concurrent cache updates do not affect the in-flight snapshot.
#[derive(Debug, Clone, Default)]
pub struct TopologySnapshot {
    /// rack_id → (status_i32, node_ids)
    racks: HashMap<RackId, (i32, Vec<NodeId>)>,
    /// (rack_id, node_id) → (status_i32, dg_ids)
    nodes: HashMap<(RackId, NodeId), (i32, Vec<DiskGroupId>)>,
    /// dg_id → disk-group entry (with rack_id, node_id, status)
    disk_groups: HashMap<DiskGroupId, DiskGroupEntry>,
    /// Unit size in bytes (from disk records). Used to convert
    /// `write_granularity` (KB) to `unit_count` for diskdb allocation.
    /// 0 if not yet populated.
    unit_size_bytes: u32,
}

impl TopologySnapshot {
    /// All disk-groups with healthy status (rack/node/dg all `Up`).
    pub fn healthy_disk_groups(&self) -> Vec<&DiskGroupEntry> {
        self.disk_groups
            .values()
            .filter(|dg| {
                let dg_ok = dg.value.status == HwStatus::Up as i32;
                let node_ok = self
                    .nodes
                    .get(&(dg.rack_id, dg.node_id))
                    .is_some_and(|(s, _)| *s == HW_UP);
                let rack_ok = self.racks.get(&dg.rack_id).is_some_and(|(s, _)| *s == HW_UP);
                dg_ok && node_ok && rack_ok
            })
            .collect()
    }

    /// Get a disk-group entry by ID.
    pub fn disk_group(&self, dg_id: DiskGroupId) -> Option<&DiskGroupEntry> {
        self.disk_groups.get(&dg_id)
    }

    /// Nodes in a given rack.
    pub fn nodes_in_rack(&self, rack_id: RackId) -> Vec<NodeId> {
        self.racks
            .get(&rack_id)
            .map(|(_, nodes)| nodes.clone())
            .unwrap_or_default()
    }

    /// Rack ID for a given node.
    pub fn rack_for_node(&self, node_id: NodeId) -> Option<RackId> {
        self.nodes
            .iter()
            .find(|((_, nid), _)| *nid == node_id)
            .map(|((rack_id, _), _)| *rack_id)
    }

    /// All rack IDs.
    pub fn rack_ids(&self) -> Vec<RackId> {
        self.racks.keys().copied().collect()
    }

    /// Number of disk-groups in the snapshot.
    pub fn disk_group_count(&self) -> usize {
        self.disk_groups.len()
    }

    /// Check if the snapshot is empty (no racks loaded yet).
    pub fn is_empty(&self) -> bool {
        self.racks.is_empty()
    }

    /// Unit size in bytes (0 if not yet populated).
    pub fn unit_size_bytes(&self) -> u32 {
        self.unit_size_bytes
    }
}

/// Thread-safe topology cache with point-in-time snapshots.
#[derive(Clone)]
pub struct TopologyCache {
    inner: Arc<RwLock<TopologySnapshot>>,
}

impl TopologyCache {
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(TopologySnapshot::default())),
        }
    }

    /// Get a point-in-time snapshot clone for placement.
    pub fn snapshot(&self) -> TopologySnapshot {
        self.inner.read().clone()
    }

    /// Replace the entire snapshot (periodic refresh).
    pub fn replace(&self, snapshot: TopologySnapshot) {
        *self.inner.write() = snapshot;
    }

    /// Update a single disk-group entry (watch/notify fine-grained update).
    pub fn update_disk_group(&self, entry: DiskGroupEntry) {
        self.inner.write().disk_groups.insert(entry.dg_id, entry);
    }

    /// Remove a disk-group entry (deleted disk-group).
    pub fn remove_disk_group(&self, dg_id: DiskGroupId) {
        self.inner.write().disk_groups.remove(&dg_id);
    }

    /// Update a node's status.
    pub fn update_node_status(
        &self,
        rack_id: RackId,
        node_id: NodeId,
        status: i32,
        dg_ids: Vec<DiskGroupId>,
    ) {
        self.inner
            .write()
            .nodes
            .insert((rack_id, node_id), (status, dg_ids));
    }

    /// Update a rack's status + node list.
    pub fn update_rack(&self, rack_id: RackId, status: i32, node_ids: Vec<NodeId>) {
        self.inner.write().racks.insert(rack_id, (status, node_ids));
    }

    /// Check if the cache is empty (no topology loaded yet).
    pub fn is_empty(&self) -> bool {
        self.inner.read().is_empty()
    }
}

impl Default for TopologyCache {
    fn default() -> Self {
        Self::new()
    }
}

/// Build a `TopologySnapshot` from `HardwareClient` data.
///
/// Fetches racks, nodes, and disk-groups from group-0 and assembles
/// them into a snapshot. If the fetch fails or returns empty, returns
/// `None` (caller should keep the previous snapshot).
pub async fn build_snapshot(hw: &crow_kv_client::HardwareClient) -> Option<TopologySnapshot> {
    let racks = match hw.list_racks().await {
        Ok(r) => r,
        Err(e) => {
            warn!(error = %e, "topology refresh: list_racks failed");
            return None;
        }
    };

    let nodes = match hw.list_nodes().await {
        Ok(n) => n,
        Err(e) => {
            warn!(error = %e, "topology refresh: list_nodes failed");
            return None;
        }
    };

    let disk_groups = match hw.list_disk_groups().await {
        Ok(d) => d,
        Err(e) => {
            warn!(error = %e, "topology refresh: list_disk_groups failed");
            return None;
        }
    };

    if racks.is_empty() && nodes.is_empty() && disk_groups.is_empty() {
        warn!("topology refresh: all lists empty, keeping previous snapshot");
        return None;
    }

    let mut snap = TopologySnapshot::default();
    for (rack_id, rv) in racks {
        snap.racks.insert(rack_id, (rv.status, rv.node_ids));
    }
    for (rack_id, node_id, nv) in nodes {
        snap.nodes
            .insert((rack_id, node_id), (nv.status, nv.disk_group_ids));
    }
    for dg in disk_groups {
        snap.disk_groups.insert(dg.dg_id, dg);
    }

    // Read unit_size_bytes from the first available disk. All disks
    // in the cluster use the same unit size, so one sample suffices.
    for dg in snap.disk_groups.values() {
        if let Some(disk_id) = dg.value.disk_ids.first() {
            if let Ok(Some(disk)) = hw.get_disk(dg.rack_id, dg.node_id, dg.dg_id, disk_id).await {
                snap.unit_size_bytes = disk.unit_size_bytes;
                break;
            }
        }
    }

    Some(snap)
}
