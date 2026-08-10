// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Tests for binary key encoding.
//!
//! Covers round-trip, byte-order, prefix-scan exactness, decode
//! rejection (bad magic, wrong tag, short input, trailing bytes), and
//! unknown-tag safety.

use super::*;
use crate::common::DiskId;

fn disk_id(high: u64, low: u64) -> DiskId {
    DiskId { high, low }
}

// ── Round-trip ──────────────────────────────────────────────────

#[test]
fn roundtrip_node_key() {
    let k = NodeKey { node_id: 42 };
    let bytes = k.to_bytes();
    assert_eq!(bytes.len(), 11);
    assert_eq!(NodeKey::from_bytes(&bytes), Ok(k));
}

#[test]
fn roundtrip_rack_key() {
    let k = RackKey {
        dc_id: 7,
        rack_id: 99,
    };
    let bytes = k.to_bytes();
    assert_eq!(bytes.len(), 19);
    assert_eq!(RackKey::from_bytes(&bytes), Ok(k));
}

#[test]
fn roundtrip_disk_group_key() {
    let k = DiskGroupKey {
        node_id: 100,
        disk_group_id: 5,
    };
    let bytes = k.to_bytes();
    assert_eq!(bytes.len(), 15);
    assert_eq!(DiskGroupKey::from_bytes(&bytes), Ok(k));
}

#[test]
fn roundtrip_disk_key() {
    let k = DiskKey {
        node_id: 100,
        disk_group_id: 5,
        disk_id: disk_id(0xDEAD, 0xBEEF),
    };
    let bytes = k.to_bytes();
    assert_eq!(bytes.len(), 31);
    assert_eq!(DiskKey::from_bytes(&bytes), Ok(k));
}

#[test]
fn roundtrip_zone_key() {
    let k = ZoneKey {
        disk_id: disk_id(1, 2),
        zone_index: 3,
    };
    let bytes = k.to_bytes();
    assert_eq!(bytes.len(), 23);
    assert_eq!(ZoneKey::from_bytes(&bytes), Ok(k));
}

#[test]
fn roundtrip_busy_block_key() {
    let k = BusyBlockKey {
        disk_id: disk_id(1, 2),
        zone_index: 3,
        unit_offset: 999,
    };
    let bytes = k.to_bytes();
    assert_eq!(bytes.len(), 31);
    assert_eq!(BusyBlockKey::from_bytes(&bytes), Ok(k));
}

#[test]
fn roundtrip_free_block_key() {
    let k = FreeBlockKey {
        disk_id: disk_id(1, 2),
        zone_index: 3,
        unit_offset: 999,
    };
    let bytes = k.to_bytes();
    assert_eq!(bytes.len(), 31);
    assert_eq!(FreeBlockKey::from_bytes(&bytes), Ok(k));
}

#[test]
fn roundtrip_owner_map_key() {
    let k = OwnerMapKey {
        node_id: 50,
        disk_group_id: 7,
    };
    let bytes = k.to_bytes();
    assert_eq!(bytes.len(), 15);
    assert_eq!(OwnerMapKey::from_bytes(&bytes), Ok(k));
}

#[test]
fn roundtrip_bind_map_key() {
    let k = BindMapKey {
        node_id: 50,
        disk_group_id: 7,
    };
    let bytes = k.to_bytes();
    assert_eq!(bytes.len(), 15);
    assert_eq!(BindMapKey::from_bytes(&bytes), Ok(k));
}

#[test]
fn roundtrip_instance_key() {
    let k = InstanceKey { instance_id: 123 };
    let bytes = k.to_bytes();
    assert_eq!(bytes.len(), 11);
    assert_eq!(InstanceKey::from_bytes(&bytes), Ok(k));
}

// ── Byte order ──────────────────────────────────────────────────

#[test]
fn node_key_order() {
    let a = NodeKey { node_id: 1 }.to_bytes();
    let b = NodeKey { node_id: 2 }.to_bytes();
    assert!(a < b, "u64 BE should sort numerically");
}

#[test]
fn busy_block_key_unit_offset_order() {
    let d = disk_id(1, 1);
    let a = BusyBlockKey {
        disk_id: d,
        zone_index: 0,
        unit_offset: 10,
    }
    .to_bytes();
    let b = BusyBlockKey {
        disk_id: d,
        zone_index: 0,
        unit_offset: 20,
    }
    .to_bytes();
    assert!(a < b, "unit_offset BE should sort numerically within a zone");
}

#[test]
fn disk_key_node_order() {
    let d = disk_id(1, 1);
    let a = DiskKey {
        node_id: 1,
        disk_group_id: 0,
        disk_id: d,
    }
    .to_bytes();
    let b = DiskKey {
        node_id: 2,
        disk_group_id: 0,
        disk_id: d,
    }
    .to_bytes();
    assert!(a < b, "node_id BE should sort numerically");
}

// ── Prefix exactness ─────────────────────────────────────────────

#[test]
fn disk_key_prefix_for_node_matches() {
    let k = DiskKey {
        node_id: 42,
        disk_group_id: 5,
        disk_id: disk_id(1, 2),
    };
    let prefix = DiskKey::prefix_for_node(42);
    assert!(
        k.to_bytes().starts_with(&prefix),
        "prefix_for_node should be a byte-prefix of matching keys"
    );

    let other = DiskKey {
        node_id: 43,
        disk_group_id: 5,
        disk_id: disk_id(1, 2),
    };
    assert!(
        !other.to_bytes().starts_with(&prefix),
        "prefix_for_node should not match keys with a different node_id"
    );
}

#[test]
fn disk_key_prefix_for_disk_group_matches() {
    let k = DiskKey {
        node_id: 42,
        disk_group_id: 5,
        disk_id: disk_id(1, 2),
    };
    let prefix = DiskKey::prefix_for_disk_group(42, 5);
    assert!(k.to_bytes().starts_with(&prefix));

    let other_dg = DiskKey {
        node_id: 42,
        disk_group_id: 6,
        disk_id: disk_id(1, 2),
    };
    assert!(!other_dg.to_bytes().starts_with(&prefix));
}

#[test]
fn busy_block_prefix_for_zone_matches() {
    let d = disk_id(1, 2);
    let k = BusyBlockKey {
        disk_id: d,
        zone_index: 3,
        unit_offset: 100,
    };
    let prefix = BusyBlockKey::prefix_for_zone(&d, 3);
    assert!(k.to_bytes().starts_with(&prefix));

    let other_zone = BusyBlockKey {
        disk_id: d,
        zone_index: 4,
        unit_offset: 100,
    };
    assert!(!other_zone.to_bytes().starts_with(&prefix));
}

#[test]
fn different_kinds_do_not_share_prefix() {
    // DiskGroupKey and OwnerMapKey have the same field shape but
    // different tags, so neither's key bytes start with the other's
    // prefix.
    let dg = DiskGroupKey {
        node_id: 1,
        disk_group_id: 2,
    }
    .to_bytes();
    let owner = OwnerMapKey {
        node_id: 1,
        disk_group_id: 2,
    }
    .to_bytes();
    assert!(!dg.starts_with(&owner), "different tags must not cross-match");
    assert!(!owner.starts_with(&dg));
}

// ── Rejection ───────────────────────────────────────────────────

#[test]
fn reject_bad_magic() {
    let mut bytes = NodeKey { node_id: 1 }.to_bytes();
    bytes[0] = 0x00;
    assert_eq!(NodeKey::from_bytes(&bytes), Err(KeyError::BadMagic));
}

#[test]
fn reject_wrong_tag() {
    let bytes = NodeKey { node_id: 1 }.to_bytes();
    assert_eq!(
        RackKey::from_bytes(&bytes),
        Err(KeyError::BadTag),
        "a NodeKey should not decode as a RackKey"
    );
}

#[test]
fn reject_short_input() {
    let bytes = [CROW_KEY_MAGIC, 0x00, 0x01]; // header only, no fields
    assert_eq!(NodeKey::from_bytes(&bytes), Err(KeyError::ShortInput));
}

#[test]
fn reject_trailing_bytes() {
    let mut bytes = NodeKey { node_id: 1 }.to_bytes();
    bytes.push(0xFF);
    assert_eq!(NodeKey::from_bytes(&bytes), Err(KeyError::TrailingBytes));
}

#[test]
fn reject_empty_input() {
    assert_eq!(NodeKey::from_bytes(&[]), Err(KeyError::ShortInput));
}

// ── Unknown tag ─────────────────────────────────────────────────

#[test]
fn unknown_tag_does_not_mispars() {
    // Construct a key with an unassigned tag (0xFFFF).
    let mut bytes = vec![CROW_KEY_MAGIC, 0xFF, 0xFF];
    bytes.extend_from_slice(&42u64.to_be_bytes());
    assert_eq!(
        NodeKey::from_bytes(&bytes),
        Err(KeyError::BadTag),
        "an unknown tag must not be misparsed as any known kind"
    );
}
