// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! R74 whole-flow space verification — single-thread + multi-thread.
//! 3 disks × 4 zones × 128 units; fill, verify rotation, free,
//! reclaim, re-allocate. Multi-thread: 8 concurrent tasks × 192
//! units, verify no double-allocation.

use std::collections::HashSet;
use std::sync::Arc;

use crowdb_diskdb::model::disk::DdbDisk;
use crowdb_diskdb::model::disk_group::DdbDiskGroup;
use crowdb_diskdb::model::zone::DdbZone;
use crowdb_protocol::common::{DiskId, HwStatus};
use crowdb_protocol::diskdb::rpc::DiskValue;

const DG: u64 = 100;
const CAS_RETRY: u32 = 100;
const ZONE_ROTATE: u32 = 4;
const ZONE_CAP: u32 = 128;
const ZONE_COUNT: u32 = 4;
const DISK_COUNT: u32 = 3;
const UNIT_SIZE: u32 = 1024 * 1024;

fn disk_id(n: u64) -> DiskId {
    DiskId { high: 0, low: n }
}

fn make_disk_value() -> DiskValue {
    DiskValue {
        disk_type: 0,
        capacity_units: u64::from(ZONE_CAP) * u64::from(ZONE_COUNT),
        zone_size_units: u64::from(ZONE_CAP),
        unit_size_bytes: UNIT_SIZE,
        zone_count: ZONE_COUNT,
        status: 0,
        device_path: String::new(),
    }
}

fn make_disk(low: u64) -> Arc<DdbDisk> {
    let disk = Arc::new(DdbDisk::new(disk_id(low), DG, 10, 1, make_disk_value()));
    disk.set_effective_status(HwStatus::Up);
    for zi in 0..ZONE_COUNT {
        let zone = Arc::new(DdbZone::new(disk_id(low), zi, DG, ZONE_CAP));
        disk.add_zone(zone);
    }
    disk.rebuild_active_zones(ZONE_ROTATE);
    disk
}

fn make_dg() -> Arc<DdbDiskGroup> {
    let dg = Arc::new(DdbDiskGroup::new(DG, 10, 1));
    // Default group status is Init; set to Up for allocation tests.
    *dg.status.write().unwrap() = HwStatus::Up;
    for i in 1..=DISK_COUNT {
        dg.add_disk(make_disk(u64::from(i)));
    }
    dg.rebuild_allocating_disks();
    dg
}

/// Fill all capacity across 3 disks × 4 zones × 128 units = 1536
/// units. Verify rotation across all 4 zones per disk. In the
/// persist-only model, free does NOT reclaim space (the bitmap stays
/// set until compaction) — so this test verifies fill + rotation only.
/// The reclaim flow is verified in the compaction integration tests.
#[test]
fn single_thread_fill_and_rotate() {
    let dg = make_dg();
    let total_cap = u64::from(DISK_COUNT) * u64::from(ZONE_COUNT) * u64::from(ZONE_CAP);

    // Fill 1 unit at a time until NoSpace.
    let mut allocated: Vec<(DiskId, u32, u64, u32)> = Vec::new();
    while let Ok((disk, zone, range)) = dg.allocate_block(1, &[], CAS_RETRY, ZONE_ROTATE) {
        allocated.push((disk.disk_id, zone.zone_index, range.unit_offset, range.unit_count));
    }
    assert_eq!(allocated.len() as u64, total_cap);

    // Verify all 4 zones per disk were used (rotation).
    for i in 1..=DISK_COUNT {
        let did = disk_id(u64::from(i));
        let zones_used: HashSet<u32> = allocated
            .iter()
            .filter(|(d, _, _, _)| *d == did)
            .map(|(_, z, _, _)| *z)
            .collect();
        assert_eq!(
            zones_used.len(),
            ZONE_COUNT as usize,
            "disk {i} should use all {ZONE_COUNT} zones"
        );
    }

    // Verify aggregate usage is full.
    let usage = dg.aggregate_usage();
    assert_eq!(usage.busy_bytes, total_cap * u64::from(UNIT_SIZE));
    assert_eq!(usage.free_bytes, 0);

    // Free all — persist-only: increments backlog but does NOT clear
    // the bitmap or reclaim space.
    for (did, zi, offset, count) in &allocated {
        assert!(dg.free_block(did, *zi, *offset, *count));
    }

    // Aggregate usage is still full (bitmap not cleared).
    let usage = dg.aggregate_usage();
    assert_eq!(usage.busy_bytes, total_cap * u64::from(UNIT_SIZE));
    assert_eq!(usage.free_bytes, 0);

    // No space can be reclaimed without compaction.
    assert!(dg.allocate_block(1, &[], CAS_RETRY, ZONE_ROTATE).is_err());
}

/// 8 concurrent tasks × 192 units each = 1536 total. Verify no
/// double-allocation (all (`disk_id`, zone, offset) unique).
#[test]
fn multi_thread_no_double_allocation() {
    let dg = Arc::new(make_dg());
    let total_cap = u64::from(DISK_COUNT) * u64::from(ZONE_COUNT) * u64::from(ZONE_CAP);
    let tasks = 8u32;
    let per_task = total_cap / u64::from(tasks);

    let results = Arc::new(std::sync::Mutex::new(Vec::new()));
    let mut handles = Vec::new();
    for _ in 0..tasks {
        let dg = Arc::clone(&dg);
        let results = Arc::clone(&results);
        handles.push(std::thread::spawn(move || {
            let mut local = Vec::new();
            for _ in 0..per_task {
                if let Ok((disk, zone, range)) = dg.allocate_block(1, &[], CAS_RETRY, ZONE_ROTATE) {
                    local.push((disk.disk_id, zone.zone_index, range.unit_offset));
                }
            }
            results.lock().unwrap().extend(local);
        }));
    }
    for h in handles {
        h.join().unwrap();
    }

    let all = results.lock().unwrap().clone();
    assert_eq!(all.len() as u64, total_cap);

    // Verify no duplicates.
    let set: HashSet<_> = all.iter().collect();
    assert_eq!(set.len(), all.len(), "double-allocation detected");

    // Verify aggregate usage is full.
    let usage = dg.aggregate_usage();
    assert_eq!(usage.busy_bytes, total_cap * u64::from(UNIT_SIZE));
}

/// `zone_rotate_count=1` edge case: the active zone set has 1 zone,
/// but the allocator still round-robins through zones. Verify all
/// zones are used and no double-allocation occurs.
#[test]
fn zone_rotate_one_uses_all_zones() {
    let dg = Arc::new(DdbDiskGroup::new(DG, 10, 1));
    // Default group status is Init; set to Up for allocation tests.
    *dg.status.write().unwrap() = HwStatus::Up;
    let disk = make_disk(1);
    dg.add_disk(disk);
    dg.rebuild_allocating_disks();

    let total_cap = u64::from(ZONE_CAP) * u64::from(ZONE_COUNT);
    let mut zones_used: HashSet<u32> = HashSet::new();
    let mut offsets: HashSet<(u32, u64)> = HashSet::new();
    for _ in 0..total_cap {
        let (_d, z, r) = dg.allocate_block(1, &[], CAS_RETRY, 1).expect("alloc");
        zones_used.insert(z.zone_index);
        assert!(offsets.insert((z.zone_index, r.unit_offset)), "double-allocation");
    }
    assert_eq!(zones_used.len(), ZONE_COUNT as usize);
    assert_eq!(offsets.len() as u64, total_cap);
}
