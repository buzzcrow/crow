// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Zone bitmap-scan allocator tests — concurrent allocate, double-free,
//! multi-unit contiguous, CAS retry bound, derived state transitions.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use crow_diskdb::zone::{AllocatedRange, Zone, ZoneHealth};
use crow_protocol::common::DiskId;
use crow_protocol::diskdb::rpc::ZoneAllocationState;

fn disk_id(n: u64) -> DiskId {
    DiskId { high: 0, low: n }
}

const DG: u64 = 1;
const CAS_RETRY: u32 = 100;

fn make_zone(capacity: u32) -> Zone {
    Zone::new(disk_id(1), 0, DG, capacity)
}

// ── Basic allocate / free ───────────────────────────────────────

#[test]
fn zone_allocate_single_unit() {
    let zone = make_zone(128);
    let r = zone.allocate(1, CAS_RETRY).expect("allocate should succeed");
    assert_eq!(r.unit_count, 1);
    assert_eq!(r.unit_offset, 0);
    assert_eq!(zone.used_count.load(Ordering::Acquire), 1);
    assert!(zone.allocatable());
}

#[test]
fn zone_allocate_fills_capacity() {
    let zone = make_zone(64);
    for i in 0..64 {
        let r = zone.allocate(1, CAS_RETRY).expect("allocate should succeed");
        assert_eq!(r.unit_offset, i);
    }
    assert!(!zone.allocatable());
    assert_eq!(zone.allocate(1, CAS_RETRY), None);
}

#[test]
fn zone_free_then_reallocate() {
    let zone = make_zone(64);
    let r = zone.allocate(1, CAS_RETRY).unwrap();
    assert!(zone.free(r.unit_offset, r.unit_count));
    assert_eq!(zone.used_count.load(Ordering::Acquire), 0);
    // Re-allocate should find the freed bit.
    let r2 = zone.allocate(1, CAS_RETRY).unwrap();
    assert_eq!(r2.unit_offset, r.unit_offset);
}

#[test]
fn zone_double_free_detected() {
    let zone = make_zone(64);
    let r = zone.allocate(1, CAS_RETRY).unwrap();
    assert!(zone.free(r.unit_offset, r.unit_count));
    assert!(!zone.free(r.unit_offset, r.unit_count));
}

// ── Multi-unit ──────────────────────────────────────────────────

#[test]
fn zone_allocate_multi_unit_contiguous() {
    let zone = make_zone(128);
    let r = zone.allocate(4, CAS_RETRY).expect("multi-unit allocate");
    assert_eq!(r.unit_count, 4);
    assert_eq!(r.unit_offset, 0);
    // Verify bits 0-3 are set, 4+ are clear.
    assert_eq!(zone.used_count.load(Ordering::Acquire), 4);
    // Next allocate should start at offset 4.
    let r2 = zone.allocate(1, CAS_RETRY).unwrap();
    assert_eq!(r2.unit_offset, 4);
}

#[test]
fn zone_free_multi_unit() {
    let zone = make_zone(128);
    let r = zone.allocate(8, CAS_RETRY).unwrap();
    assert!(zone.free(r.unit_offset, r.unit_count));
    assert_eq!(zone.used_count.load(Ordering::Acquire), 0);
    // Double-free the multi-unit range.
    assert!(!zone.free(r.unit_offset, r.unit_count));
}

#[test]
fn zone_allocate_multi_unit_at_capacity_boundary() {
    let zone = make_zone(64);
    let r = zone.allocate(64, CAS_RETRY).expect("allocate full capacity");
    assert_eq!(r.unit_count, 64);
    assert!(!zone.allocatable());
}

// ── Derived state ───────────────────────────────────────────────

#[test]
fn zone_derived_alloc_state_transitions() {
    let zone = make_zone(64);
    assert_eq!(zone.derived_alloc_state(), ZoneAllocationState::ZoneAllocActive);
    let _ = zone.allocate(1, CAS_RETRY).unwrap();
    assert_eq!(
        zone.derived_alloc_state(),
        ZoneAllocationState::ZoneAllocAvailable
    );
    for _ in 1..64 {
        let _ = zone.allocate(1, CAS_RETRY).unwrap();
    }
    assert_eq!(zone.derived_alloc_state(), ZoneAllocationState::ZoneAllocFull);
}

// ── Zone health ─────────────────────────────────────────────────

#[test]
fn zone_bad_health_blocks_allocate() {
    let zone = make_zone(64);
    zone.set_health(ZoneHealth::Bad);
    assert!(!zone.allocatable());
    assert_eq!(zone.allocate(1, CAS_RETRY), None);
}

#[test]
fn zone_missing_health_blocks_allocate() {
    let zone = make_zone(64);
    zone.set_health(ZoneHealth::Missing);
    assert!(!zone.allocatable());
    assert_eq!(zone.allocate(1, CAS_RETRY), None);
}

// ── Concurrency ─────────────────────────────────────────────────

#[test]
fn zone_concurrent_allocate_no_double_alloc() {
    let zone = Arc::new(make_zone(256));
    let mut handles = Vec::new();
    for _ in 0..8 {
        let z = zone.clone();
        handles.push(std::thread::spawn(move || {
            let mut allocated = Vec::new();
            while let Some(r) = z.allocate(1, CAS_RETRY) {
                allocated.push(r);
            }
            allocated
        }));
    }
    let mut all: Vec<AllocatedRange> = Vec::new();
    for h in handles {
        all.extend(h.join().unwrap());
    }
    // Every bit should be allocated exactly once.
    assert_eq!(all.len(), 256);
    let mut offsets: Vec<u64> = all.iter().map(|r| r.unit_offset).collect();
    offsets.sort_unstable();
    offsets.dedup();
    assert_eq!(offsets.len(), 256);
    assert_eq!(zone.used_count.load(Ordering::Acquire), 256);
}

// ── CAS retry ───────────────────────────────────────────────────

#[test]
fn zone_cas_retry_counter_increments_under_contention() {
    use std::sync::Barrier;
    // Small zone, many threads with a barrier — forces CAS contention
    // on the same word.
    let zone = Arc::new(make_zone(64));
    let barrier = Arc::new(Barrier::new(16));
    let mut handles = Vec::new();
    for _ in 0..16 {
        let z = zone.clone();
        let b = barrier.clone();
        handles.push(std::thread::spawn(move || {
            b.wait();
            while z.allocate(1, CAS_RETRY).is_some() {}
        }));
    }
    for h in handles {
        let _ = h.join();
    }
    // Under contention with a barrier, CAS retries should have
    // occurred. (Not strictly guaranteed on all schedulers, but the
    // barrier makes it very likely.)
    let retries = zone.cas_retry_count.load(Ordering::Relaxed);
    assert!(
        retries > 0,
        "expected CAS retries under contention, got {retries}"
    );
}
