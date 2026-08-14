// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! R74 usage aggregation tests — `DdbDisk::usage`,
//! `DdbDiskGroup::aggregate_usage`, `DdbDiskGroup::zone_usage`.

use std::sync::Arc;

use crow_diskdb::model::disk::DdbDisk;
use crow_diskdb::model::disk_group::DdbDiskGroup;
use crow_diskdb::model::zone::DdbZone;
use crow_protocol::common::{DiskId, HwStatus};
use crow_protocol::diskdb::rpc::{DiskType, DiskValue};

const UNIT_SIZE: u32 = 1024 * 1024;
const ZONE_CAP: u32 = 128;

fn make_disk_value(zone_count: u32) -> DiskValue {
    DiskValue {
        disk_type: DiskType::BlockHdd as i32,
        capacity_units: u64::from(ZONE_CAP) * u64::from(zone_count),
        zone_size_units: u64::from(ZONE_CAP),
        unit_size_bytes: UNIT_SIZE,
        zone_count,
        status: HwStatus::Up as i32,
    }
}

fn make_disk(disk_id: DiskId, dg_id: u64, zone_count: u32) -> Arc<DdbDisk> {
    let dv = make_disk_value(zone_count);
    let disk = Arc::new(DdbDisk::new(disk_id, dg_id, 10, 1, dv));
    disk.set_effective_status(HwStatus::Up);
    for zi in 0..zone_count {
        let zone = DdbZone::new(disk_id, zi, dg_id, ZONE_CAP);
        disk.add_zone(Arc::new(zone));
    }
    disk.rebuild_active_zones(2);
    disk
}

#[test]
fn disk_usage_sums_across_zones() {
    let disk = make_disk(DiskId { high: 0, low: 1 }, 100, 2);
    // Allocate 5 units in zone 0 only.
    let zones = disk.zones.read().unwrap();
    let _ = zones[0].allocate(5, 100);
    drop(zones);

    let u = disk.usage();
    assert_eq!(u.disk_id, DiskId { high: 0, low: 1 });
    assert_eq!(u.zone_count, 2);
    assert_eq!(u.active_zone_count, 2);
    // busy = zone-0 busy (5); free = sum of both zones' free.
    assert_eq!(u.busy_bytes, 5u64 * u64::from(UNIT_SIZE));
    assert_eq!(u.free_bytes, (2u64 * 128 - 5) * u64::from(UNIT_SIZE));
    assert_eq!(u.capacity_bytes, (2u64 * 128) * u64::from(UNIT_SIZE));
    assert_eq!(u.busy_zone_count, 0);
    assert_eq!(u.free_zone_count, 2);
}

#[test]
fn disk_usage_counts_full_zone_as_busy() {
    let disk = make_disk(DiskId { high: 0, low: 2 }, 100, 1);
    let zones = disk.zones.read().unwrap();
    // Fill the single zone completely (128 units, 1 at a time).
    for _ in 0..128 {
        assert!(zones[0].allocate(1, 100).is_some());
    }
    drop(zones);

    let u = disk.usage();
    assert_eq!(u.busy_zone_count, 1);
    assert_eq!(u.free_zone_count, 0);
    assert_eq!(u.free_bytes, 0);
}

#[test]
fn disk_group_aggregate_usage_sums_disks() {
    let dg = Arc::new(DdbDiskGroup::new(100, 10, 1));
    let d1 = make_disk(DiskId { high: 0, low: 1 }, 100, 2);
    let d2 = make_disk(DiskId { high: 0, low: 2 }, 100, 2);
    dg.add_disk(d1);
    dg.add_disk(d2);

    // Allocate in disk 1, zone 0.
    {
        let d1 = dg.disks.read().unwrap()[0].clone();
        let zones = d1.zones.read().unwrap();
        let _ = zones[0].allocate(5, 100);
    }

    let u = dg.aggregate_usage();
    assert_eq!(u.disk_group_id, 100);
    assert_eq!(u.disk_count, 2);
    assert_eq!(u.allocatable_disk_count, 2);
    assert_eq!(u.busy_bytes, 5u64 * u64::from(UNIT_SIZE));
    assert_eq!(u.capacity_bytes, (4u64 * 128) * u64::from(UNIT_SIZE));
    assert_eq!(u.disks.len(), 2);
}

#[test]
fn disk_group_aggregate_excludes_bad_from_allocatable() {
    let dg = Arc::new(DdbDiskGroup::new(100, 10, 1));
    let d1 = make_disk(DiskId { high: 0, low: 1 }, 100, 2);
    let d2 = make_disk(DiskId { high: 0, low: 2 }, 100, 2);
    d2.set_effective_status(HwStatus::Bad);
    dg.add_disk(d1);
    dg.add_disk(d2);

    let u = dg.aggregate_usage();
    assert_eq!(u.disk_count, 2);
    assert_eq!(u.allocatable_disk_count, 1);
    // Bad disk's capacity still counts in the total.
    assert_eq!(u.capacity_bytes, (4u64 * 128) * u64::from(UNIT_SIZE));
}

#[test]
fn disk_group_zone_usage_returns_brief_counts() {
    let dg = Arc::new(DdbDiskGroup::new(100, 10, 1));
    let d1 = make_disk(DiskId { high: 0, low: 1 }, 100, 2);
    {
        let zones = d1.zones.read().unwrap();
        let _ = zones[0].allocate(5, 100);
    }
    dg.add_disk(d1);

    let u = dg.zone_usage(DiskId { high: 0, low: 1 }, 0).expect("zone 0");
    assert_eq!(u.zone_index, 0);
    assert_eq!(u.busy_block_count, 5);
    assert_eq!(u.free_block_count, 123);
    assert_eq!(u.busy_bytes, 5u64 * u64::from(UNIT_SIZE));
}

#[test]
fn disk_group_zone_usage_none_for_unknown_disk_or_range() {
    let dg = Arc::new(DdbDiskGroup::new(100, 10, 1));
    let d1 = make_disk(DiskId { high: 0, low: 1 }, 100, 2);
    dg.add_disk(d1);

    assert!(dg.zone_usage(DiskId { high: 0, low: 99 }, 0).is_none());
    assert!(dg.zone_usage(DiskId { high: 0, low: 1 }, 99).is_none());
}
