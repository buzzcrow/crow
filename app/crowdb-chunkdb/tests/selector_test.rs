// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Placement selector unit tests.

#![allow(clippy::cast_possible_truncation)]

use crowdb_chunkdb::selector::{EcPlacement, MirrorPlacement, PlacementConstraints};
use crowdb_chunkdb::topology::TopologyCache;
use crowdb_protocol::common::HwStatus;
use crowdb_protocol::diskdb::rpc::DiskGroupValue;
use crowdb_protocol::sysdata::DiskGroupEntry;

fn make_dg_entry(dg_id: u64, rack: u64, node: u64, status: HwStatus) -> DiskGroupEntry {
    DiskGroupEntry {
        rack_id: rack,
        node_id: node,
        dg_id,
        value: DiskGroupValue {
            status: status as i32,
            disk_ids: vec![],
        },
    }
}

/// Build a topology with N racks, M nodes per rack, one DG per node.
fn build_topology(racks: &[(u64, &[u64])]) -> TopologyCache {
    let cache = TopologyCache::new();
    let mut dg_id = 100u64;
    for (rack_id, nodes) in racks {
        cache.update_rack(*rack_id, HwStatus::Up as i32, nodes.to_vec());
        for node_id in *nodes {
            cache.update_node_status(*rack_id, *node_id, HwStatus::Up as i32, vec![dg_id]);
            cache.update_disk_group(make_dg_entry(dg_id, *rack_id, *node_id, HwStatus::Up));
            dg_id += 1;
        }
    }
    cache
}

#[test]
fn mirror_select_3_copies_3_racks_distinct() {
    let cache = build_topology(&[(1, &[10, 11]), (2, &[20, 21]), (3, &[30, 31])]);
    let snap = cache.snapshot();

    let plan = MirrorPlacement::select(&snap, 3, &PlacementConstraints::new()).unwrap();
    assert_eq!(plan.entries.len(), 3);

    // Each entry should be in a distinct rack.
    let racks: std::collections::HashSet<_> = plan.entries.iter().map(|e| e.rack_id).collect();
    assert_eq!(racks.len(), 3);
}

#[test]
fn mirror_select_3_copies_2_racks_maximizes_spread() {
    let cache = build_topology(&[(1, &[10, 11]), (2, &[20, 21])]);
    let snap = cache.snapshot();

    let plan = MirrorPlacement::select(&snap, 3, &PlacementConstraints::new()).unwrap();
    assert_eq!(plan.entries.len(), 3);

    // Should use both racks (2 distinct racks).
    let racks: std::collections::HashSet<_> = plan.entries.iter().map(|e| e.rack_id).collect();
    assert_eq!(racks.len(), 2);

    // All nodes should be distinct.
    let nodes: std::collections::HashSet<_> = plan.entries.iter().map(|e| e.node_id).collect();
    assert_eq!(nodes.len(), 3);
}

#[test]
fn mirror_select_with_excluded_rack() {
    let cache = build_topology(&[(1, &[10]), (2, &[20]), (3, &[30])]);
    let snap = cache.snapshot();

    let constraints = PlacementConstraints::new().exclude_rack(1);
    let plan = MirrorPlacement::select(&snap, 2, &constraints).unwrap();
    assert_eq!(plan.entries.len(), 2);

    // No entry should be in rack 1.
    assert!(plan.entries.iter().all(|e| e.rack_id != 1));
}

#[test]
fn mirror_select_insufficient_nodes() {
    let cache = build_topology(&[(1, &[10])]);
    let snap = cache.snapshot();

    let result = MirrorPlacement::select(&snap, 3, &PlacementConstraints::new());
    assert!(result.is_err());
}

#[test]
fn ec_select_8_4_safe_mode_3_racks() {
    // 12 nodes across 3 racks (4 per rack).
    let cache = build_topology(&[
        (1, &[10, 11, 12, 13]),
        (2, &[20, 21, 22, 23]),
        (3, &[30, 31, 32, 33]),
    ]);
    let snap = cache.snapshot();

    let plan = EcPlacement::select(&snap, 8, 4, &PlacementConstraints::new()).unwrap();
    assert_eq!(plan.entries.len(), 12);
    assert!(plan.safe_mode);

    // Max 4 blocks per node (code_num=4).
    let mut node_load: std::collections::HashMap<_, u32> = std::collections::HashMap::new();
    for e in &plan.entries {
        *node_load.entry(e.node_id).or_insert(0) += 1;
    }
    for &load in node_load.values() {
        assert!(load <= 4, "safe mode: max 4 blocks per node, got {load}");
    }

    // Blocks across >= 3 racks.
    let racks: std::collections::HashSet<_> = plan.entries.iter().map(|e| e.rack_id).collect();
    assert!(racks.len() >= 3);
}

#[test]
fn ec_select_8_4_unsafe_fallback_3_nodes() {
    // Only 3 nodes — not enough for safe mode (12 blocks, max 4 per
    // node = 12 capacity, but safe mode requires spreading across
    // racks). With 3 nodes in 1 rack, safe mode would place 4 per
    // node = 12 total. Actually safe mode should work here. Let's use
    // 2 nodes to force unsafe.
    let cache = build_topology(&[(1, &[10, 11])]);
    let snap = cache.snapshot();

    // 8+4 = 12 blocks, 2 nodes, safe mode max 4 per node = 8 capacity
    // → unsafe mode needed (max 12 per node).
    let plan = EcPlacement::select(&snap, 8, 4, &PlacementConstraints::new().allow_unsafe_ec()).unwrap();
    assert_eq!(plan.entries.len(), 12);
    assert!(!plan.safe_mode);
}

#[test]
fn ec_select_unsafe_mode_succeeds_with_single_node() {
    let cache = build_topology(&[(1, &[10])]);
    let snap = cache.snapshot();

    // 8+4 = 12 blocks, 1 node. Safe mode fails (max 4 per node = 4
    // capacity < 12), but unsafe mode sets max_per_node = 12, so 1
    // node can hold all 12 blocks.
    let plan = EcPlacement::select(&snap, 8, 4, &PlacementConstraints::new().allow_unsafe_ec()).unwrap();
    assert_eq!(plan.entries.len(), 12);
    assert!(!plan.safe_mode);
}

#[test]
fn ec_select_no_healthy_dgs() {
    let cache = TopologyCache::new();
    let snap = cache.snapshot();

    let result = EcPlacement::select(&snap, 4, 2, &PlacementConstraints::new());
    assert!(result.is_err());
}

#[test]
fn ec_select_rejects_implicit_unsafe_fallback() {
    let cache = build_topology(&[(1, &[10, 11])]);
    let result = EcPlacement::select(&cache.snapshot(), 8, 4, &PlacementConstraints::new());

    assert!(matches!(
        result,
        Err(crowdb_chunkdb::selector::PlacementError::UnsafePlacementRequired)
    ));
}

#[test]
fn selectors_reject_zero_width_shapes() {
    let cache = build_topology(&[(1, &[10])]);
    let snap = cache.snapshot();

    assert!(MirrorPlacement::select(&snap, 0, &PlacementConstraints::new()).is_err());
    assert!(EcPlacement::select(&snap, 0, 1, &PlacementConstraints::new()).is_err());
    assert!(EcPlacement::select(&snap, 1, 0, &PlacementConstraints::new()).is_err());
}

#[test]
fn mirror_select_no_healthy_dgs() {
    let cache = TopologyCache::new();
    let snap = cache.snapshot();

    let result = MirrorPlacement::select(&snap, 3, &PlacementConstraints::new());
    assert!(result.is_err());
}

#[test]
fn ec_select_with_excluded_node() {
    let cache = build_topology(&[
        (1, &[10, 11, 12, 13]),
        (2, &[20, 21, 22, 23]),
        (3, &[30, 31, 32, 33]),
    ]);
    let snap = cache.snapshot();

    let constraints = PlacementConstraints::new().exclude_node(10);
    let plan = EcPlacement::select(&snap, 4, 2, &constraints).unwrap();
    assert_eq!(plan.entries.len(), 6);

    // No entry should be on node 10.
    assert!(plan.entries.iter().all(|e| e.node_id != 10));
}
