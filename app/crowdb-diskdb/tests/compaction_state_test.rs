// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::atomic::Ordering;

use crowdb_diskdb::model::records::{BusyRecord, FreeRecord, ZoneRecords};
use crowdb_diskdb::model::zone::DdbZone;
use crowdb_diskdb::recovery::compaction::matching_free_records_for_tests;
use crowdb_protocol::common::DiskId;
use crowdb_protocol::diskdb::rpc::{BusyBlockValue, FreeBlockValue};
use crowdb_protocol::key::{BusyBlockKey, FreeBlockKey};
use crowdb_protocol::ZoneValueExt;

#[test]
fn prospective_compaction_snapshot_does_not_publish_live_state() {
    let disk_id = DiskId { high: 1, low: 2 };
    let zone = DdbZone::new(disk_id, 0, 7, 64);
    let allocated = zone.allocate(4, 100).expect("allocate range");
    zone.snapshot_slot.store(11, Ordering::Release);
    zone.compact_slot.store(11, Ordering::Release);
    zone.compact_ts.store(20, Ordering::Release);
    let free = FreeRecord {
        key: FreeBlockKey {
            disk_id,
            zone_index: 0,
            unit_offset: allocated.unit_offset,
            allocation_ts: 30,
        },
        value: FreeBlockValue {
            unit_count: 4,
            previous_owner: None,
            pre_allocation_ts: 30,
            free_ts: 30,
        },
        commit_slot: 12,
    };

    let prospective = zone.prepare_compaction_for_tests(&[free], 12);

    assert!(prospective.verify_checksum());
    assert_eq!(prospective.snapshot_slot, 12);
    assert_eq!(prospective.compact_slot, 12);
    assert_eq!(prospective.compact_ts, 30);
    assert_eq!(prospective.usage_bitmap, vec![0; 8]);
    assert_eq!(zone.used_count.load(Ordering::Acquire), 4);
    assert_eq!(zone.snapshot_slot.load(Ordering::Acquire), 11);
    assert_eq!(zone.compact_slot.load(Ordering::Acquire), 11);
    assert_eq!(zone.compact_ts.load(Ordering::Acquire), 20);
    for offset in allocated.unit_offset..allocated.unit_offset + 4 {
        assert!(zone
            .usage_bits
            .is_set(u32::try_from(offset).expect("test offset fits u32")));
    }
}

#[test]
fn empty_prospective_compaction_advances_durable_cutoff_only() {
    let zone = DdbZone::new(DiskId { high: 1, low: 2 }, 0, 7, 64);
    zone.snapshot_slot.store(11, Ordering::Release);
    zone.compact_slot.store(11, Ordering::Release);

    let prospective = zone.prepare_compaction_for_tests(&[], 12);

    assert!(prospective.verify_checksum());
    assert_eq!(prospective.snapshot_slot, 12);
    assert_eq!(prospective.compact_slot, 12);
    assert_eq!(zone.snapshot_slot.load(Ordering::Acquire), 11);
    assert_eq!(zone.compact_slot.load(Ordering::Acquire), 11);
}

#[test]
fn surviving_lower_slot_free_is_still_reconciled() {
    let disk_id = DiskId { high: 1, low: 2 };
    let records = ZoneRecords {
        zone_value: None,
        busy: vec![BusyRecord {
            key: BusyBlockKey {
                disk_id,
                zone_index: 0,
                unit_offset: 9,
            },
            value: BusyBlockValue {
                unit_count: 2,
                allocation_ts: 30,
                ..BusyBlockValue::default()
            },
            commit_slot: 40,
        }],
        free: vec![FreeRecord {
            key: FreeBlockKey {
                disk_id,
                zone_index: 0,
                unit_offset: 9,
                allocation_ts: 30,
            },
            value: FreeBlockValue {
                unit_count: 2,
                pre_allocation_ts: 30,
                ..FreeBlockValue::default()
            },
            commit_slot: 41,
        }],
    };

    let matching = matching_free_records_for_tests(&records, 100);

    assert_eq!(matching.len(), 1);
    assert_eq!(matching[0].commit_slot, 41);
}
