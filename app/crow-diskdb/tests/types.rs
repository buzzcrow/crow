// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Integration tests for diskdb core types, key layout, config
//! validation, bitmap, and CRC integrity.

use std::sync::atomic::Ordering;

use crow_diskdb::types::{
    effective_status, BusyRecord, ClaimSnapshot, DiskGroupId, DiskMeta, DiskState, DiskType, DiskUuid,
    FreeRecord, InstanceMeta, NodeId, NodeMeta, Segment, Status, ZoneAllocationState, ZoneRecord, ZoneState,
};
use crow_diskdb::UsageBitmap;
use crow_diskdb::{validate, DiskdbConfig};

// ── Identity + Segment ──────────────────────────────────────────

#[test]
fn disk_uuid_display_and_key_component() {
    let uuid = DiskUuid::new(0x1234_5678_90ab_cdef, 0xfedc_ba09_8765_4321);
    assert_eq!(uuid.to_string(), "1234567890abcdef-fedcba0987654321");
    assert_eq!(uuid.to_key_component(), "1234567890abcdeffedcba0987654321");
}

#[test]
fn segment_serde_json_roundtrip() {
    let seg = Segment {
        node_id: 42,
        disk_group_id: 3,
        disk_uuid: DiskUuid::new(1, 2),
        zone_index: 5,
        zone_offset: 1_048_576,
        size: 1_048_576,
        tag: 123_456_789,
    };
    let json = serde_json::to_string(&seg).expect("serialize");
    let restored: Segment = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(seg, restored);
}

#[test]
fn segment_bincode_roundtrip() {
    let seg = Segment {
        node_id: 7,
        disk_group_id: 1,
        disk_uuid: DiskUuid::new(0xa, 0xb),
        zone_index: 2,
        zone_offset: 0,
        size: 1024,
        tag: 99,
    };
    let bytes = bincode::serialize(&seg).expect("serialize");
    let restored: Segment = bincode::deserialize(&bytes).expect("deserialize");
    assert_eq!(seg, restored);
}

#[test]
fn claim_snapshot_is_copy() {
    let cs = ClaimSnapshot {
        prev_pos: 10,
        count: 4,
    };
    let copy = cs;
    assert_eq!(cs.prev_pos, copy.prev_pos);
    assert_eq!(cs.count, copy.count);
}

// ── Status ──────────────────────────────────────────────────────

#[test]
fn status_ordering() {
    assert!(Status::Online < Status::Init);
    assert!(Status::Init < Status::Maintenance);
    assert!(Status::Maintenance < Status::TempFailure);
    assert!(Status::TempFailure < Status::Offline);
}

#[test]
fn effective_status_picks_most_restrictive() {
    assert_eq!(
        effective_status(Status::Online, Status::Online, Status::Online),
        Status::Online
    );
    assert_eq!(
        effective_status(Status::Maintenance, Status::Online, Status::Online),
        Status::Maintenance
    );
    assert_eq!(
        effective_status(Status::Online, Status::Online, Status::TempFailure),
        Status::TempFailure
    );
    assert_eq!(
        effective_status(Status::Online, Status::Offline, Status::Online),
        Status::Offline
    );
}

#[test]
fn status_allows_allocate_and_free() {
    assert!(Status::Online.allows_allocate());
    assert!(!Status::Init.allows_allocate());
    assert!(!Status::Maintenance.allows_allocate());
    assert!(!Status::TempFailure.allows_allocate());
    assert!(!Status::Offline.allows_allocate());

    assert!(Status::Online.allows_free());
    assert!(!Status::Init.allows_free());
    assert!(Status::Maintenance.allows_free());
    assert!(Status::TempFailure.allows_free());
    assert!(!Status::Offline.allows_free());
}

// ── ZoneAllocationState ─────────────────────────────────────────

#[test]
fn zone_allocation_state_from_u8() {
    assert_eq!(ZoneAllocationState::from_u8(0), ZoneAllocationState::Active);
    assert_eq!(ZoneAllocationState::from_u8(1), ZoneAllocationState::Busy);
    assert_eq!(ZoneAllocationState::from_u8(2), ZoneAllocationState::Error);
    assert_eq!(ZoneAllocationState::from_u8(3), ZoneAllocationState::Full);
    // Unknown → Error (defensive).
    assert_eq!(ZoneAllocationState::from_u8(99), ZoneAllocationState::Error);
}

// ── Journal records ─────────────────────────────────────────────

#[test]
fn busy_record_bincode_roundtrip_and_size() {
    let rec = BusyRecord {
        zone_offset: 1_048_576,
        size: 1_048_576,
        tag: 12345,
    };
    let bytes = bincode::serialize(&rec).expect("serialize");
    assert!(
        bytes.len() <= 32,
        "BusyRecord serialized to {} bytes, expected <= 32",
        bytes.len()
    );
    let restored: BusyRecord = bincode::deserialize(&bytes).expect("deserialize");
    assert_eq!(rec, restored);
}

#[test]
fn free_record_bincode_roundtrip() {
    let rec = FreeRecord {
        zone_offset: 512,
        size: 256,
        tag: 999,
    };
    let bytes = bincode::serialize(&rec).expect("serialize");
    let restored: FreeRecord = bincode::deserialize(&bytes).expect("deserialize");
    assert_eq!(rec, restored);
}

// ── ZoneRecord + CRC ────────────────────────────────────────────

fn sample_zone_record() -> ZoneRecord {
    ZoneRecord {
        disk_uuid: DiskUuid::new(0xabc, 0xdef),
        zone_index: 7,
        disk_offset: 7 * 16 * 1024 * 1024 * 1024,
        zone_size_bytes: 16 * 1024 * 1024 * 1024,
        allocate_pos: 100,
        usage_bitmap: vec![0xFF; 16],
        zone_state: ZoneState::Healthy,
        snapshot_slot: 42,
        checksum: 0,
    }
}

#[test]
fn zone_record_crc_compute_and_verify() {
    let mut rec = sample_zone_record();
    rec.compute_checksum();
    assert_ne!(rec.checksum, 0);
    assert!(rec.verify_checksum());
}

#[test]
fn zone_record_crc_tamper_detected() {
    let mut rec = sample_zone_record();
    rec.compute_checksum();
    assert!(rec.verify_checksum());
    rec.allocate_pos = 999;
    assert!(!rec.verify_checksum());
}

#[test]
fn zone_record_bincode_roundtrip() {
    let mut rec = sample_zone_record();
    rec.compute_checksum();
    let bytes = rec.to_bytes();
    let restored = ZoneRecord::from_bytes(&bytes).expect("deserialize");
    assert_eq!(restored.disk_uuid, rec.disk_uuid);
    assert_eq!(restored.zone_index, rec.zone_index);
    assert_eq!(restored.allocate_pos, rec.allocate_pos);
    assert_eq!(restored.snapshot_slot, rec.snapshot_slot);
    assert!(restored.verify_checksum());
}

// ── Journal key layout ──────────────────────────────────────────

#[test]
fn journal_key_busy_format() {
    let key = crow_diskdb::types::journal::journal_key_busy(7, 3, &DiskUuid::new(0x1234, 0x5678), 5, 42);
    assert_eq!(
        key,
        "/diskdb/journal/7-3/00000000000012340000000000005678/z0005/busy/00000000000000000042"
    );
}

#[test]
fn journal_key_free_format() {
    let key = crow_diskdb::types::journal::journal_key_free(1, 0, &DiskUuid::new(0, 1), 0, 0);
    assert_eq!(
        key,
        "/diskdb/journal/1-0/00000000000000000000000000000001/z0000/free/00000000000000000000"
    );
}

#[test]
fn journal_key_snapshot_format() {
    let key = crow_diskdb::types::journal::journal_key_snapshot(99, 12, &DiskUuid::new(0xffff, 0xeeee), 255);
    assert_eq!(
        key,
        "/diskdb/journal/99-12/000000000000ffff000000000000eeee/z0255/snapshot"
    );
}

#[test]
fn journal_prefix_zone_format() {
    let key = crow_diskdb::types::journal::journal_prefix_zone(7, 3, &DiskUuid::new(0x1, 0x2), 5);
    assert_eq!(key, "/diskdb/journal/7-3/00000000000000010000000000000002/z0005/");
}

#[test]
fn journal_prefix_disk_format() {
    let key = crow_diskdb::types::journal::journal_prefix_disk(7, 3, &DiskUuid::new(0x1, 0x2));
    assert_eq!(key, "/diskdb/journal/7-3/00000000000000010000000000000002/");
}

#[test]
fn journal_prefix_dg_format() {
    let key = crow_diskdb::types::journal::journal_prefix_dg(7, 3);
    assert_eq!(key, "/diskdb/journal/7-3/");
}

#[test]
fn journal_slot_zero_padding_preserves_order() {
    // Lexicographic order must match numeric order.
    let k1 = crow_diskdb::types::journal::journal_key_busy(1, 1, &DiskUuid::new(0, 0), 0, 1);
    let k2 = crow_diskdb::types::journal::journal_key_busy(1, 1, &DiskUuid::new(0, 0), 0, 10);
    let k3 = crow_diskdb::types::journal::journal_key_busy(1, 1, &DiskUuid::new(0, 0), 0, 100);
    assert!(k1 < k2);
    assert!(k2 < k3);
}

// ── Sysdata key layout ──────────────────────────────────────────

#[test]
fn sysdata_key_formats() {
    assert_eq!(
        crow_diskdb::types::journal::sysdata_key_node(7),
        "/diskdb/node/7/meta"
    );
    assert_eq!(
        crow_diskdb::types::journal::sysdata_key_disk_group(7, 3),
        "/diskdb/node/7/dg/3/meta"
    );
    assert_eq!(
        crow_diskdb::types::journal::sysdata_key_disk(7, &DiskUuid::new(0x1, 0x2)),
        "/diskdb/node/7/disk/00000000000000010000000000000002/meta"
    );
    assert_eq!(
        crow_diskdb::types::journal::sysdata_key_owner(7, 3),
        "/diskdb/map/owner/7-3"
    );
    assert_eq!(
        crow_diskdb::types::journal::sysdata_key_bind(7, 3),
        "/diskdb/map/bind/7-3"
    );
    assert_eq!(
        crow_diskdb::types::journal::sysdata_key_instance("inst-abc"),
        "/diskdb/instance/inst-abc"
    );
}

// ── Meta types serde ────────────────────────────────────────────

#[test]
fn node_meta_serde_roundtrip() {
    let meta = NodeMeta {
        node_id: 5,
        dc_id: None,
        rack_id: None,
        status: Status::Online,
        last_used_dg_id: 2,
        disk_group_ids: vec![0, 1, 2],
        status_changed_at_ms: 1_700_000_000,
        temp_failure_since_ms: None,
    };
    let json = serde_json::to_string(&meta).expect("serialize");
    let restored: NodeMeta = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(restored.node_id, meta.node_id);
    assert_eq!(restored.last_used_dg_id, meta.last_used_dg_id);
    assert_eq!(restored.disk_group_ids, meta.disk_group_ids);
}

#[test]
fn disk_group_meta_serde_roundtrip() {
    let meta = crow_diskdb::types::DiskGroupMeta {
        node_id: 5,
        dg_id: 2,
        status: Status::Online,
        disk_uuids: vec![DiskUuid::new(1, 2), DiskUuid::new(3, 4)],
    };
    let json = serde_json::to_string(&meta).expect("serialize");
    let restored: crow_diskdb::types::DiskGroupMeta = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(restored.node_id, meta.node_id);
    assert_eq!(restored.dg_id, meta.dg_id);
    assert_eq!(restored.disk_uuids, meta.disk_uuids);
}

#[test]
fn disk_meta_serde_roundtrip() {
    let meta = DiskMeta {
        disk_uuid: DiskUuid::new(1, 2),
        node_id: 5,
        disk_type: DiskType::BlockSsd,
        capacity_bytes: 4 * 1024 * 1024 * 1024 * 1024,
        zone_size_bytes: 16 * 1024 * 1024 * 1024,
        block_size_bytes: 1024 * 1024,
        zone_count: 256,
        status: Status::Online,
    };
    let json = serde_json::to_string(&meta).expect("serialize");
    let restored: DiskMeta = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(restored.disk_uuid, meta.disk_uuid);
    assert_eq!(restored.disk_type, DiskType::BlockSsd);
    assert_eq!(restored.zone_count, 256);
}

#[test]
fn instance_meta_serde_roundtrip() {
    let meta = InstanceMeta {
        instance_id: "inst-1".to_string(),
        grpc_endpoint: "0.0.0.0:9941".to_string(),
        http_endpoint: "0.0.0.0:9942".to_string(),
        owned_dg_ids: vec![(5, 0), (5, 1)],
        last_heartbeat_ms: 1_700_000_000,
    };
    let json = serde_json::to_string(&meta).expect("serialize");
    let restored: InstanceMeta = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(restored.instance_id, meta.instance_id);
    assert_eq!(restored.owned_dg_ids, meta.owned_dg_ids);
}

// ── Config validation ───────────────────────────────────────────

#[test]
fn config_validate_accepts_default() {
    let config = DiskdbConfig::default();
    validate(&config).expect("default config should be valid");
}

#[test]
fn config_validate_rejects_non_power_of_two_block_size() {
    let mut config = DiskdbConfig::default();
    config.storage.block_size_bytes = 700 * 1024; // not a power of 2
    assert!(validate(&config).is_err());
}

#[test]
fn config_validate_rejects_block_size_out_of_range() {
    let mut config = DiskdbConfig::default();
    config.storage.block_size_bytes = 256 * 1024; // below 512 KB
    assert!(validate(&config).is_err());

    let mut config = DiskdbConfig::default();
    config.storage.block_size_bytes = 4 * 1024 * 1024; // above 2 MB
    assert!(validate(&config).is_err());
}

#[test]
fn config_validate_rejects_zone_not_multiple_of_block() {
    let mut config = DiskdbConfig::default();
    config.storage.zone_size_bytes = 16 * 1024 * 1024 * 1024 + 1; // not a multiple of 1 MB
    assert!(validate(&config).is_err());
}

#[test]
fn config_validate_rejects_granularity_not_equal_to_block() {
    let mut config = DiskdbConfig::default();
    config.storage.allocate_granularity = 512 * 1024;
    config.storage.block_size_bytes = 1024 * 1024;
    assert!(validate(&config).is_err());
}

#[test]
fn config_validate_rejects_bad_listen_addr() {
    let mut config = DiskdbConfig::default();
    config.server.listen_addr = "not-an-addr".to_string();
    assert!(validate(&config).is_err());
}

#[test]
fn config_validate_rejects_zero_sync_interval() {
    let mut config = DiskdbConfig::default();
    config.sync.sync_interval_secs = 0;
    assert!(validate(&config).is_err());
}

// ── Bitmap ──────────────────────────────────────────────────────

#[test]
fn bitmap_range_set_and_clear() {
    let bm = UsageBitmap::new(128);
    assert!(bm.range_set(0, 4));
    let word0 = bm.snapshot();
    assert_eq!(word0[0..8], [0x0F, 0, 0, 0, 0, 0, 0, 0]);
    assert!(bm.range_clear(0, 4));
    let word0 = bm.snapshot();
    assert_eq!(word0[0..8], [0, 0, 0, 0, 0, 0, 0, 0]);
}

#[test]
fn bitmap_double_set_fails() {
    let bm = UsageBitmap::new(128);
    assert!(bm.range_set(0, 4));
    assert!(!bm.range_set(2, 4)); // overlap → false
                                  // Original bits still set (rollback).
    let snap = bm.snapshot();
    assert_eq!(snap[0..8], [0x0F, 0, 0, 0, 0, 0, 0, 0]);
}

#[test]
fn bitmap_double_clear_fails() {
    let bm = UsageBitmap::new(128);
    assert!(bm.range_set(0, 4));
    assert!(bm.range_clear(0, 4));
    assert!(!bm.range_clear(0, 4)); // already clear → false
}

#[test]
fn bitmap_cross_word_boundary() {
    let bm = UsageBitmap::new(128);
    assert!(bm.range_set(62, 4)); // bits 62, 63, 64, 65
    let snap = bm.snapshot();
    let w0 = u64::from_le_bytes(snap[0..8].try_into().unwrap());
    let w1 = u64::from_le_bytes(snap[8..16].try_into().unwrap());
    assert_ne!(w0 & (1u64 << 62), 0);
    assert_ne!(w0 & (1u64 << 63), 0);
    assert_ne!(w1 & 1, 0);
    assert_ne!(w1 & 2, 0);
}

#[test]
fn bitmap_snapshot_restore_roundtrip() {
    let bm = UsageBitmap::new(128);
    let _ = bm.range_set(0, 10);
    let _ = bm.range_set(70, 5);
    let snap = bm.snapshot();
    let restored = UsageBitmap::restore(&snap);
    let restored_snap = restored.snapshot();
    assert_eq!(snap, restored_snap);
}

#[test]
fn bitmap_count_set() {
    let bm = UsageBitmap::new(128);
    assert_eq!(bm.count_set(), 0);
    let _ = bm.range_set(0, 10);
    assert_eq!(bm.count_set(), 10);
    let _ = bm.range_set(70, 5);
    assert_eq!(bm.count_set(), 15);
    let _ = bm.range_clear(0, 4);
    assert_eq!(bm.count_set(), 11);
}

#[test]
fn bitmap_block_and_word_count() {
    let bm = UsageBitmap::new(128);
    assert_eq!(bm.block_count(), 128);
    assert_eq!(bm.word_count(), 2);

    let bm = UsageBitmap::new(130);
    assert_eq!(bm.block_count(), 130);
    assert_eq!(bm.word_count(), 3); // ceil(130/64) = 3
}

#[test]
fn create_usage_bitmap_helper() {
    let bm = crow_diskdb::zone::bitmap::create_usage_bitmap(64);
    assert_eq!(bm.block_count(), 64);
    assert_eq!(bm.word_count(), 1);
    assert!(bm.range_set(0, 64));
    assert_eq!(bm.count_set(), 64);
}

// ── DiskState / DiskType sanity ─────────────────────────────────

#[test]
fn disk_state_variants_exist() {
    let _ = DiskState::Init;
    let _ = DiskState::Active;
    let _ = DiskState::Suspect;
    let _ = DiskState::Missing;
    let _ = DiskState::Bad;
}

#[test]
fn disk_type_variants_exist() {
    let _ = DiskType::BlockHdd;
    let _ = DiskType::BlockSsd;
    let _ = DiskType::ZoneSsd;
    let _ = DiskType::SmrHdd;
}

// ── Type alias sanity ───────────────────────────────────────────

#[test]
fn type_aliases_are_expected_types() {
    let node_id: NodeId = 42;
    let dg_id: DiskGroupId = 3;
    assert_eq!(node_id, 42u64);
    assert_eq!(dg_id, 3u32);
}

// ── Ordering const check (suppress unused must_use) ─────────────

#[test]
fn atomic_ordering_constants_compile() {
    let _ = Ordering::AcqRel;
    let _ = Ordering::Acquire;
}
