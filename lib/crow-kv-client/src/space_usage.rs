// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! R74 §10 — space-usage aggregation client. Reads
//! `/hw/dg_usage/<dg_id>` summaries from group 0 (piggybacked on
//! diskdb keepalive) and joins them with the hardware hierarchy for
//! cluster/rack/node/disk-group capacity views.
//!
//! This is the stale (≤1 sync interval) cluster-wide view; live
//! per-disk/per-zone drill-down is via the diskdb-client
//! `QueryCapacityStats` RPC.

use crow_protocol::common::DiskGroupUsageSummary;
use crow_protocol::key::{DiskGroupUsageKey, TextKey};
use crow_protocol::{DiskGroupId, NodeId, RackId};

use crate::hardware::{scan_prefix, HardwareClient};
use crate::Result;

/// Cluster-level capacity aggregation.
#[derive(Debug, Clone, Default)]
pub struct ClusterUsage {
    pub capacity_bytes: u64,
    pub used_bytes: u64,
    pub free_bytes: u64,
    pub disk_group_count: usize,
    pub rack_count: usize,
    pub node_count: usize,
}

/// Rack-level capacity aggregation.
#[derive(Debug, Clone, Default)]
pub struct RackUsage {
    pub rack_id: RackId,
    pub capacity_bytes: u64,
    pub used_bytes: u64,
    pub free_bytes: u64,
    pub disk_group_count: usize,
    pub node_count: usize,
}

/// Node-level capacity aggregation.
#[derive(Debug, Clone, Default)]
pub struct NodeUsage {
    pub rack_id: RackId,
    pub node_id: NodeId,
    pub capacity_bytes: u64,
    pub used_bytes: u64,
    pub free_bytes: u64,
    pub disk_group_count: usize,
}

/// Client for reading disk-group usage summaries from group 0 and
/// aggregating them with the hardware hierarchy.
pub struct SpaceUsageClient {
    hw: HardwareClient,
}

impl SpaceUsageClient {
    #[must_use]
    pub fn new(hw: HardwareClient) -> Self {
        Self { hw }
    }

    #[must_use]
    pub fn from_shared(hw: HardwareClient) -> Self {
        Self { hw }
    }

    /// Prefix-scan `/hw/dg_usage/` → one summary per disk-group.
    ///
    /// # Errors
    /// Returns `Error` if the group-0 scan fails.
    pub async fn list_disk_group_usages(&self) -> Result<Vec<(DiskGroupId, DiskGroupUsageSummary)>> {
        let prefix = DiskGroupUsageKey::prefix_all_text();
        let entries = scan_prefix::<DiskGroupUsageSummary>(self.hw.kv(), &prefix).await?;
        let mut out = Vec::with_capacity(entries.len());
        for (path, summary) in entries {
            // Parse the disk_group_id from the text path.
            let dg_id = parse_dg_id_from_path(&path).unwrap_or(summary.disk_group_id);
            out.push((dg_id, summary));
        }
        Ok(out)
    }

    /// Get one disk-group's usage summary, or `None` if not found.
    ///
    /// # Errors
    /// Returns `Error` if the underlying scan fails.
    pub async fn disk_group_usage(&self, dg_id: DiskGroupId) -> Result<Option<DiskGroupUsageSummary>> {
        let usages = self.list_disk_group_usages().await?;
        Ok(usages.into_iter().find(|(id, _)| *id == dg_id).map(|(_, s)| s))
    }

    /// Cluster-level aggregation: sum all disk-group summaries + count
    /// racks/nodes/disk-groups from the hardware hierarchy.
    ///
    /// # Errors
    /// Returns `Error` if any hardware hierarchy scan fails.
    pub async fn cluster_usage(&self) -> Result<ClusterUsage> {
        let usages = self.list_disk_group_usages().await?;
        let racks = self.hw.list_racks().await?;
        let nodes = self.hw.list_nodes().await?;
        let disk_groups = self.hw.list_disk_groups().await?;

        let mut capacity = 0u64;
        let mut used = 0u64;
        let mut free = 0u64;
        for (_, s) in &usages {
            capacity += s.capacity_bytes;
            used += s.used_bytes;
            free += s.free_bytes;
        }
        Ok(ClusterUsage {
            capacity_bytes: capacity,
            used_bytes: used,
            free_bytes: free,
            disk_group_count: disk_groups.len(),
            rack_count: racks.len(),
            node_count: nodes.len(),
        })
    }

    /// Rack-level aggregation: sum disk-group usage for disk-groups on
    /// the given rack.
    ///
    /// # Errors
    /// Returns `Error` if any hardware hierarchy scan fails.
    pub async fn rack_usage(&self, rack_id: RackId) -> Result<RackUsage> {
        let usages = self.list_disk_group_usages().await?;
        let nodes = self.hw.list_nodes_in_rack(rack_id).await?;

        // Sum usage for disk-groups on this rack. We need to know
        // which disk-groups belong to this rack — use the disk-group
        // entries from the hardware hierarchy.
        let all_disk_groups = self.hw.list_disk_groups().await?;
        let rack_dg_ids: Vec<DiskGroupId> = all_disk_groups
            .iter()
            .filter(|dg| dg.rack_id == rack_id)
            .map(|dg| dg.dg_id)
            .collect();

        let mut capacity = 0u64;
        let mut used = 0u64;
        let mut free = 0u64;
        let mut dg_count = 0usize;
        for (dg_id, s) in &usages {
            if rack_dg_ids.contains(dg_id) {
                capacity += s.capacity_bytes;
                used += s.used_bytes;
                free += s.free_bytes;
                dg_count += 1;
            }
        }
        Ok(RackUsage {
            rack_id,
            capacity_bytes: capacity,
            used_bytes: used,
            free_bytes: free,
            disk_group_count: dg_count,
            node_count: nodes.len(),
        })
    }

    /// Node-level aggregation: sum disk-group usage for disk-groups on
    /// the given node.
    ///
    /// # Errors
    /// Returns `Error` if any hardware hierarchy scan fails.
    pub async fn node_usage(&self, rack_id: RackId, node_id: NodeId) -> Result<NodeUsage> {
        let usages = self.list_disk_group_usages().await?;
        let node_dgs = self.hw.list_disk_groups_on_node(rack_id, node_id).await?;
        let node_dg_ids: Vec<DiskGroupId> = node_dgs.iter().map(|dg| dg.dg_id).collect();

        let mut capacity = 0u64;
        let mut used = 0u64;
        let mut free = 0u64;
        let mut dg_count = 0usize;
        for (dg_id, s) in &usages {
            if node_dg_ids.contains(dg_id) {
                capacity += s.capacity_bytes;
                used += s.used_bytes;
                free += s.free_bytes;
                dg_count += 1;
            }
        }
        Ok(NodeUsage {
            rack_id,
            node_id,
            capacity_bytes: capacity,
            used_bytes: used,
            free_bytes: free,
            disk_group_count: dg_count,
        })
    }
}

/// Parse the `disk_group_id` from a `/hw/dg_usage/<id>` text path.
fn parse_dg_id_from_path(path: &str) -> Option<DiskGroupId> {
    let parts: Vec<&str> = path.split('/').collect();
    // /hw/dg_usage/<id> → ["", "hw", "dg_usage", "<id>"]
    if parts.len() >= 4 && parts[2] == "dg_usage" {
        parts[3].parse().ok()
    } else {
        None
    }
}

/// Extension trait to get the text prefix for `DiskGroupUsageKey`.
trait DiskGroupUsageKeyExt {
    fn prefix_all_text() -> String;
}

impl DiskGroupUsageKeyExt for DiskGroupUsageKey {
    fn prefix_all_text() -> String {
        <DiskGroupUsageKey as TextKey>::prefix_all()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_dg_id_from_valid_path() {
        assert_eq!(parse_dg_id_from_path("/hw/dg_usage/100"), Some(100));
        assert_eq!(parse_dg_id_from_path("/hw/dg_usage/0"), Some(0));
    }

    #[test]
    fn parse_dg_id_from_invalid_path() {
        assert_eq!(parse_dg_id_from_path("/hw/rack/1"), None);
        assert_eq!(parse_dg_id_from_path("/hw/dg_usage/abc"), None);
        assert_eq!(parse_dg_id_from_path(""), None);
    }
}
