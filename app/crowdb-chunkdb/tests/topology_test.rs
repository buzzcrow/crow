// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Topology cache unit tests.

#![allow(clippy::cast_possible_truncation)]

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

/// Set up a healthy rack+node so disk-group health checks pass.
fn setup_healthy_rack_node(cache: &TopologyCache, rack: u64, node: u64, dg_ids: Vec<u64>) {
    cache.update_rack(rack, HwStatus::Up as i32, vec![node]);
    cache.update_node_status(rack, node, HwStatus::Up as i32, dg_ids);
}

#[test]
fn cache_starts_empty() {
    let cache = TopologyCache::new();
    assert!(cache.is_empty());
}

#[test]
fn cache_replace_and_snapshot() {
    let cache = TopologyCache::new();
    setup_healthy_rack_node(&cache, 1, 10, vec![100, 101]);
    cache.update_disk_group(make_dg_entry(100, 1, 10, HwStatus::Up));

    let snap = cache.snapshot();
    assert_eq!(snap.disk_group_count(), 1);
    assert!(!snap.is_empty());
}

#[test]
fn healthy_disk_groups_filters_by_status() {
    let cache = TopologyCache::new();
    setup_healthy_rack_node(&cache, 1, 10, vec![100, 101]);
    cache.update_disk_group(make_dg_entry(100, 1, 10, HwStatus::Up));
    cache.update_disk_group(make_dg_entry(101, 1, 10, HwStatus::Bad));

    let snap = cache.snapshot();
    let healthy = snap.healthy_disk_groups();
    assert_eq!(healthy.len(), 1);
    assert_eq!(healthy[0].dg_id, 100);
}

#[test]
fn healthy_disk_groups_excludes_maintenance_node() {
    let cache = TopologyCache::new();
    cache.update_rack(1, HwStatus::Up as i32, vec![10]);
    cache.update_node_status(1, 10, HwStatus::Maintenance as i32, vec![100]);
    cache.update_disk_group(make_dg_entry(100, 1, 10, HwStatus::Up));

    let snap = cache.snapshot();
    assert!(snap.healthy_disk_groups().is_empty());
}

#[test]
fn healthy_disk_groups_excludes_bad_rack() {
    let cache = TopologyCache::new();
    cache.update_rack(1, HwStatus::Bad as i32, vec![10]);
    cache.update_node_status(1, 10, HwStatus::Up as i32, vec![100]);
    cache.update_disk_group(make_dg_entry(100, 1, 10, HwStatus::Up));

    let snap = cache.snapshot();
    assert!(snap.healthy_disk_groups().is_empty());
}

#[test]
fn remove_disk_group() {
    let cache = TopologyCache::new();
    cache.update_disk_group(make_dg_entry(100, 1, 10, HwStatus::Up));
    assert_eq!(cache.snapshot().disk_group_count(), 1);

    cache.remove_disk_group(100);
    assert_eq!(cache.snapshot().disk_group_count(), 0);
}

#[test]
fn snapshot_is_point_in_time() {
    let cache = TopologyCache::new();
    cache.update_disk_group(make_dg_entry(100, 1, 10, HwStatus::Up));

    let snap1 = cache.snapshot();
    cache.update_disk_group(make_dg_entry(101, 1, 10, HwStatus::Up));

    // snap1 should not see dg 101.
    assert_eq!(snap1.disk_group_count(), 1);
    assert_eq!(cache.snapshot().disk_group_count(), 2);
}

#[test]
fn disk_group_lookup_by_id() {
    let cache = TopologyCache::new();
    cache.update_disk_group(make_dg_entry(100, 1, 10, HwStatus::Up));

    let snap = cache.snapshot();
    assert!(snap.disk_group(100).is_some());
    assert!(snap.disk_group(999).is_none());
}

#[test]
fn nodes_in_rack() {
    let cache = TopologyCache::new();
    cache.update_rack(1, HwStatus::Up as i32, vec![10, 20, 30]);

    let snap = cache.snapshot();
    let nodes = snap.nodes_in_rack(1);
    assert_eq!(nodes, vec![10, 20, 30]);
}

#[test]
fn rack_ids() {
    let cache = TopologyCache::new();
    cache.update_rack(1, HwStatus::Up as i32, vec![10]);
    cache.update_rack(2, HwStatus::Up as i32, vec![20]);

    let snap = cache.snapshot();
    let mut racks = snap.rack_ids();
    racks.sort_unstable();
    assert_eq!(racks, vec![1, 2]);
}
