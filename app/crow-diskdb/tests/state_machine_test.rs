// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! `HwStateMachine` transition + permission tests.

use std::sync::Arc;

use crow_diskdb::liveness::state_machine::{HwStateMachine, IllegalTransition, Op};
use crow_diskdb::model::disk::DdbDisk;
use crow_diskdb::model::disk_group::DdbDiskGroup;
use crow_diskdb::model::zone::{DdbZone, DdbZoneHealth};
use crow_protocol::common::{DiskId, HwStatus};
use crow_protocol::diskdb::rpc::DiskValue;

fn make_zone(capacity: u32) -> Arc<DdbZone> {
    Arc::new(DdbZone::new(DiskId { high: 0, low: 1 }, 0, 1, capacity))
}

fn make_disk_with_zones(zone_count: u32, zone_capacity: u32) -> Arc<DdbDisk> {
    let disk = Arc::new(DdbDisk::new(
        DiskId { high: 0, low: 1 },
        1,
        1,
        1,
        DiskValue {
            capacity_units: 16 * 1024,
            zone_size_units: 16 * 1024,
            unit_size_bytes: 1024 * 1024,
            ..Default::default()
        },
    ));
    disk.set_effective_status(HwStatus::Up);
    for zi in 0..zone_count {
        disk.add_zone(Arc::new(DdbZone::new(
            DiskId { high: 0, low: 1 },
            zi,
            1,
            zone_capacity,
        )));
    }
    disk
}

#[test]
fn test_legal_transitions() {
    assert!(HwStateMachine::is_legal_transition(HwStatus::Init, HwStatus::Up));
    assert!(HwStateMachine::is_legal_transition(
        HwStatus::Up,
        HwStatus::Suspect
    ));
    assert!(HwStateMachine::is_legal_transition(
        HwStatus::Suspect,
        HwStatus::Up
    ));
    assert!(HwStateMachine::is_legal_transition(
        HwStatus::Suspect,
        HwStatus::Missing
    ));
    assert!(HwStateMachine::is_legal_transition(
        HwStatus::Missing,
        HwStatus::Bad
    ));
    assert!(HwStateMachine::is_legal_transition(
        HwStatus::Missing,
        HwStatus::Up
    ));
    assert!(HwStateMachine::is_legal_transition(
        HwStatus::Offline,
        HwStatus::Maintenance
    ));
    assert!(HwStateMachine::is_legal_transition(
        HwStatus::Maintenance,
        HwStatus::Offline
    ));
    assert!(HwStateMachine::is_legal_transition(
        HwStatus::Offline,
        HwStatus::Up
    ));
    // Operator override: mark a Bad disk Up after physical repair.
    assert!(HwStateMachine::is_legal_transition(HwStatus::Bad, HwStatus::Up));
}

#[test]
fn test_illegal_transitions() {
    assert!(!HwStateMachine::is_legal_transition(HwStatus::Up, HwStatus::Init));
    assert!(!HwStateMachine::is_legal_transition(HwStatus::Up, HwStatus::Bad));
    assert!(!HwStateMachine::is_legal_transition(
        HwStatus::Init,
        HwStatus::Suspect
    ));
}

#[test]
fn test_effective_status() {
    assert_eq!(
        HwStateMachine::effective_status(HwStatus::Up, HwStatus::Up, HwStatus::Up),
        HwStatus::Up
    );
    assert_eq!(
        HwStateMachine::effective_status(HwStatus::Up, HwStatus::Up, HwStatus::Offline),
        HwStatus::Offline
    );
    assert_eq!(
        HwStateMachine::effective_status(HwStatus::Up, HwStatus::Maintenance, HwStatus::Up),
        HwStatus::Maintenance
    );
}

#[test]
fn test_permits() {
    assert!(HwStateMachine::permits(HwStatus::Up, Op::Allocate));
    assert!(!HwStateMachine::permits(HwStatus::Maintenance, Op::Allocate));
    assert!(!HwStateMachine::permits(HwStatus::Offline, Op::Allocate));

    assert!(HwStateMachine::permits(HwStatus::Up, Op::Free));
    assert!(HwStateMachine::permits(HwStatus::Maintenance, Op::Free));
    assert!(HwStateMachine::permits(HwStatus::Suspect, Op::Free));
    assert!(!HwStateMachine::permits(HwStatus::Offline, Op::Free));
}

#[test]
fn test_transition_disk_applies_status_without_zone_marking() {
    let machine = HwStateMachine::new(900);
    let disk = make_disk_with_zones(3, 128);
    // Disk starts Up; transition to Bad is illegal from Up directly,
    // so go Up -> Suspect -> Missing -> Bad.
    machine
        .transition_disk(&disk, HwStatus::Suspect)
        .expect("Up -> Suspect");
    machine
        .transition_disk(&disk, HwStatus::Missing)
        .expect("Suspect -> Missing");
    machine
        .transition_disk(&disk, HwStatus::Bad)
        .expect("Missing -> Bad");

    // The disk-level status is the sole gatekeeper; zones are not
    // marked individually (R76 — no per-zone marking).
    assert_eq!(*disk.effective_status.read().unwrap(), HwStatus::Bad);
    let zones = disk.zones.read().unwrap();
    for z in zones.iter() {
        assert_eq!(*z.zone_state.read().unwrap(), DdbZoneHealth::Healthy);
    }
}

#[test]
fn test_transition_disk_rejects_illegal() {
    let machine = HwStateMachine::new(900);
    let disk = make_disk_with_zones(1, 128);
    // Up -> Init is illegal.
    let result = machine.transition_disk(&disk, HwStatus::Init);
    assert_eq!(
        result,
        Err(IllegalTransition {
            from: HwStatus::Up,
            to: HwStatus::Init
        })
    );
    // Status unchanged.
    assert_eq!(*disk.effective_status.read().unwrap(), HwStatus::Up);
}

#[test]
fn test_transition_disk_group_legal() {
    let machine = HwStateMachine::new(900);
    let dg = Arc::new(DdbDiskGroup::new(1, 1, 1));
    // Default status is Init; Init -> Up is legal, then Up -> Suspect.
    machine.transition_disk_group(&dg, HwStatus::Up).unwrap();
    let result = machine.transition_disk_group(&dg, HwStatus::Suspect);
    assert_eq!(result, Ok(HwStatus::Suspect));
    assert_eq!(*dg.status.read().unwrap(), HwStatus::Suspect);
}

#[test]
fn test_transition_disk_group_rejects_illegal() {
    let machine = HwStateMachine::new(900);
    let dg = Arc::new(DdbDiskGroup::new(1, 1, 1));
    // Default status is Init; Init -> Up is legal, then Up -> Init is illegal.
    machine.transition_disk_group(&dg, HwStatus::Up).unwrap();
    let result = machine.transition_disk_group(&dg, HwStatus::Init);
    assert!(result.is_err());
    assert_eq!(*dg.status.read().unwrap(), HwStatus::Up);
}

#[test]
fn test_check_suspect_timeout() {
    let machine = HwStateMachine::new(2);
    let now = std::time::Instant::now();
    let suspect_since = now;
    assert!(!machine.check_suspect_timeout(suspect_since, now));
    let later = now + std::time::Duration::from_secs(3);
    assert!(machine.check_suspect_timeout(suspect_since, later));
}

#[test]
fn test_zone_unused() {
    // Silence unused `make_zone` warning if not all tests use it.
    let _ = make_zone(128);
}
