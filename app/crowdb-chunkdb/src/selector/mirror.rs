// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Mirror placement — select N distinct nodes across distinct racks.
//!
//! Design §7.1: one copy per rack when possible. If fewer racks than
//! copies, place remaining copies on distinct nodes in the first rack.
//! Random-start scan to avoid hot spots.

use std::collections::HashSet;

use rand::seq::SliceRandom;
use tracing::warn;

use crowdb_protocol::{DiskGroupId, NodeId, RackId};

use super::{healthy_dgs, PlacementConstraints, PlacementError, PlacementPlan};
use crate::topology::TopologySnapshot;

/// Mirror placement selector.
pub struct MirrorPlacement;

impl MirrorPlacement {
    /// Select `copy_count` nodes across distinct racks.
    ///
    /// # Errors
    /// Returns `PlacementError::NoHealthyDiskGroups` if no disk-groups
    /// pass the health + exclusion filters, or
    /// `PlacementError::InsufficientNodes` if fewer healthy nodes than
    /// `copy_count`.
    pub fn select(
        snap: &TopologySnapshot,
        copy_count: usize,
        constraints: &PlacementConstraints,
    ) -> Result<PlacementPlan, PlacementError> {
        if copy_count == 0 {
            return Err(PlacementError::InvalidShape(
                "mirror copy_count must be non-zero".to_string(),
            ));
        }
        let dgs = healthy_dgs(snap, constraints);
        if dgs.is_empty() {
            return Err(PlacementError::NoHealthyDiskGroups);
        }

        // Group disk-groups by rack.
        let mut by_rack: std::collections::HashMap<RackId, Vec<&crowdb_protocol::sysdata::DiskGroupEntry>> =
            std::collections::HashMap::new();
        for dg in &dgs {
            by_rack.entry(dg.rack_id).or_default().push(dg);
        }

        let mut rack_ids: Vec<RackId> = by_rack.keys().copied().collect();
        rack_ids.shuffle(&mut rand::thread_rng());

        // Phase 1: pick one node per rack (round-robin through racks).
        let mut selected: Vec<(RackId, NodeId, DiskGroupId)> = Vec::new();
        let mut used_nodes: HashSet<NodeId> = HashSet::new();
        let mut rack_index = 0;

        for _ in 0..copy_count {
            // Try to find a rack with an unused node.
            let mut found = false;
            for _ in 0..rack_ids.len() {
                let rack = rack_ids[rack_index];
                rack_index = (rack_index + 1) % rack_ids.len();
                if let Some(dgs_in_rack) = by_rack.get(&rack) {
                    if let Some(dg) = dgs_in_rack.iter().find(|dg| !used_nodes.contains(&dg.node_id)) {
                        used_nodes.insert(dg.node_id);
                        selected.push((rack, dg.node_id, dg.dg_id));
                        found = true;
                        break;
                    }
                }
            }
            if !found {
                break;
            }
        }

        // Phase 2: if not enough distinct racks, place remaining copies
        // on distinct nodes in any rack.
        if selected.len() < copy_count {
            let remaining = copy_count - selected.len();
            warn!(
                needed = copy_count,
                racks = rack_ids.len(),
                placed = selected.len(),
                remaining,
                "mirror placement: not enough distinct racks, placing remaining copies on distinct nodes"
            );
            for dg in &dgs {
                if selected.len() >= copy_count {
                    break;
                }
                if !used_nodes.contains(&dg.node_id) {
                    used_nodes.insert(dg.node_id);
                    selected.push((dg.rack_id, dg.node_id, dg.dg_id));
                }
            }
        }

        if selected.len() < copy_count {
            return Err(PlacementError::InsufficientNodes {
                needed: copy_count,
                available: selected.len(),
            });
        }

        let entries: Vec<super::PlacementEntry> = selected
            .into_iter()
            .map(|(rack_id, node_id, disk_group_id)| super::PlacementEntry {
                rack_id,
                node_id,
                disk_group_id,
                block_count: 1,
            })
            .collect();

        Ok(PlacementPlan {
            entries,
            safe_mode: true,
        })
    }
}
