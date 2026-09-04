// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Tests for key encoding (binary + text).
//!
//! Covers round-trip, byte-order, prefix-scan exactness, decode
//! rejection (bad magic, wrong tag, short input, trailing bytes), and
//! unknown-tag safety for [`BinaryKey`]; round-trip and prefix
//! exactness for [`TextKey`].

use super::*;
use crate::common::DiskId;

fn disk_id(high: u64, low: u64) -> DiskId {
    DiskId { high, low }
}

// ── Binary round-trip ───────────────────────────────────────────

#[test]
fn roundtrip_rack_key() {
    let k = RackKey { rack_id: 99 };
    let bytes = k.to_bytes();
    assert_eq!(bytes.len(), 11);
    assert_eq!(RackKey::from_bytes(&bytes), Ok(k));
}

#[test]
fn roundtrip_node_key() {
    let k = NodeKey {
        rack_id: 7,
        node_id: 42,
    };
    let bytes = k.to_bytes();
    assert_eq!(bytes.len(), 19);
    assert_eq!(NodeKey::from_bytes(&bytes), Ok(k));
}

#[test]
fn roundtrip_disk_group_key() {
    let k = DiskGroupKey {
        rack_id: 1,
        node_id: 100,
        disk_group_id: 5,
    };
    let bytes = k.to_bytes();
    assert_eq!(bytes.len(), 27);
    assert_eq!(DiskGroupKey::from_bytes(&bytes), Ok(k));
}

#[test]
fn roundtrip_disk_key() {
    let k = DiskKey {
        rack_id: 1,
        node_id: 100,
        disk_group_id: 5,
        disk_id: disk_id(0xDEAD, 0xBEEF),
    };
    let bytes = k.to_bytes();
    assert_eq!(bytes.len(), 43);
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
        allocation_ts: 123,
    };
    let bytes = k.to_bytes();
    assert_eq!(bytes.len(), 39);
    assert_eq!(FreeBlockKey::from_bytes(&bytes), Ok(k));
}

#[test]
fn roundtrip_owner_map_key() {
    let k = OwnerMapKey {
        rack_id: 1,
        node_id: 50,
        disk_group_id: 7,
    };
    let bytes = k.to_bytes();
    assert_eq!(bytes.len(), 27);
    assert_eq!(OwnerMapKey::from_bytes(&bytes), Ok(k));
}

#[test]
fn roundtrip_bind_map_key() {
    let k = BindMapKey {
        rack_id: 1,
        node_id: 50,
        disk_group_id: 7,
    };
    let bytes = k.to_bytes();
    assert_eq!(bytes.len(), 27);
    assert_eq!(BindMapKey::from_bytes(&bytes), Ok(k));
}

#[test]
fn roundtrip_instance_key() {
    let k = InstanceKey {
        service: "diskdb".to_string(),
        instance_id: 123,
    };
    let bytes = k.to_bytes();
    assert_eq!(InstanceKey::from_bytes(&bytes), Ok(k));
}

#[test]
fn roundtrip_disk_group_usage_key() {
    let k = DiskGroupUsageKey { disk_group_id: 7 };
    let bytes = k.to_bytes();
    assert_eq!(bytes.len(), 11);
    assert_eq!(DiskGroupUsageKey::from_bytes(&bytes), Ok(k));
}

// ── Binary byte order ───────────────────────────────────────────

#[test]
fn node_key_rack_order() {
    let a = NodeKey {
        rack_id: 1,
        node_id: 42,
    }
    .to_bytes();
    let b = NodeKey {
        rack_id: 2,
        node_id: 42,
    }
    .to_bytes();
    assert!(a < b, "rack_id BE should sort numerically");
}

#[test]
fn node_key_node_order() {
    let a = NodeKey {
        rack_id: 1,
        node_id: 1,
    }
    .to_bytes();
    let b = NodeKey {
        rack_id: 1,
        node_id: 2,
    }
    .to_bytes();
    assert!(a < b, "node_id BE should sort numerically within a rack");
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
fn disk_key_rack_order() {
    let d = disk_id(1, 1);
    let a = DiskKey {
        rack_id: 1,
        node_id: 2,
        disk_group_id: 0,
        disk_id: d,
    }
    .to_bytes();
    let b = DiskKey {
        rack_id: 2,
        node_id: 2,
        disk_group_id: 0,
        disk_id: d,
    }
    .to_bytes();
    assert!(a < b, "rack_id BE should sort numerically");
}

#[test]
fn disk_key_node_order() {
    let d = disk_id(1, 1);
    let a = DiskKey {
        rack_id: 1,
        node_id: 1,
        disk_group_id: 0,
        disk_id: d,
    }
    .to_bytes();
    let b = DiskKey {
        rack_id: 1,
        node_id: 2,
        disk_group_id: 0,
        disk_id: d,
    }
    .to_bytes();
    assert!(a < b, "node_id BE should sort numerically within a rack");
}

// ── Binary prefix exactness ─────────────────────────────────────

#[test]
fn disk_key_prefix_for_node_matches() {
    let k = DiskKey {
        rack_id: 1,
        node_id: 42,
        disk_group_id: 5,
        disk_id: disk_id(1, 2),
    };
    let prefix = DiskKey::prefix_for_node(1, 42);
    assert!(
        k.to_bytes().starts_with(&prefix),
        "prefix_for_node should be a byte-prefix of matching keys"
    );

    let other = DiskKey {
        rack_id: 1,
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
        rack_id: 1,
        node_id: 42,
        disk_group_id: 5,
        disk_id: disk_id(1, 2),
    };
    let prefix = DiskKey::prefix_for_disk_group(1, 42, 5);
    assert!(k.to_bytes().starts_with(&prefix));

    let other_dg = DiskKey {
        rack_id: 1,
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
        rack_id: 1,
        node_id: 2,
        disk_group_id: 3,
    }
    .to_bytes();
    let owner = OwnerMapKey {
        rack_id: 1,
        node_id: 2,
        disk_group_id: 3,
    }
    .to_bytes();
    assert!(!dg.starts_with(&owner), "different tags must not cross-match");
    assert!(!owner.starts_with(&dg));
}

// ── Binary rejection ────────────────────────────────────────────

#[test]
fn reject_bad_magic() {
    let mut bytes = NodeKey {
        rack_id: 1,
        node_id: 1,
    }
    .to_bytes();
    bytes[0] = 0x00;
    assert_eq!(NodeKey::from_bytes(&bytes), Err(KeyError::BadMagic));
}

#[test]
fn reject_wrong_tag() {
    let bytes = NodeKey {
        rack_id: 1,
        node_id: 1,
    }
    .to_bytes();
    assert_eq!(
        RackKey::from_bytes(&bytes),
        Err(KeyError::BadTag),
        "a NodeKey should not decode as a RackKey"
    );
}

#[test]
fn reject_short_input() {
    let bytes = [CROWDB_KEY_MAGIC, 0x00, 0x01]; // header only, no fields
    assert_eq!(NodeKey::from_bytes(&bytes), Err(KeyError::ShortInput));
}

#[test]
fn reject_trailing_bytes() {
    let mut bytes = NodeKey {
        rack_id: 1,
        node_id: 1,
    }
    .to_bytes();
    bytes.push(0xFF);
    assert_eq!(NodeKey::from_bytes(&bytes), Err(KeyError::TrailingBytes));
}

#[test]
fn reject_empty_input() {
    assert_eq!(NodeKey::from_bytes(&[]), Err(KeyError::ShortInput));
}

// ── Binary unknown tag ──────────────────────────────────────────

#[test]
fn unknown_tag_does_not_mispars() {
    // Construct a key with an unassigned tag (0xFFFF).
    let mut bytes = vec![CROWDB_KEY_MAGIC, 0xFF, 0xFF];
    bytes.extend_from_slice(&42u64.to_be_bytes());
    assert_eq!(
        NodeKey::from_bytes(&bytes),
        Err(KeyError::BadTag),
        "an unknown tag must not be misparsed as any known kind"
    );
}

// ── TextKey round-trip ──────────────────────────────────────────

#[test]
fn text_roundtrip_rack_key() {
    let k = RackKey { rack_id: 99 };
    let path = k.to_path();
    assert_eq!(path, "/hw/rack/99");
    assert_eq!(RackKey::from_path(&path), Ok(k));
}

#[test]
fn text_roundtrip_node_key() {
    let k = NodeKey {
        rack_id: 7,
        node_id: 42,
    };
    let path = k.to_path();
    assert_eq!(path, "/hw/node/7/42");
    assert_eq!(NodeKey::from_path(&path), Ok(k));
}

#[test]
fn text_roundtrip_disk_group_key() {
    let k = DiskGroupKey {
        rack_id: 1,
        node_id: 100,
        disk_group_id: 5,
    };
    let path = k.to_path();
    assert_eq!(path, "/hw/dg/1/100/5");
    assert_eq!(DiskGroupKey::from_path(&path), Ok(k));
}

#[test]
fn text_roundtrip_disk_key() {
    let k = DiskKey {
        rack_id: 1,
        node_id: 100,
        disk_group_id: 5,
        disk_id: disk_id(0xDEAD, 0xBEEF),
    };
    let path = k.to_path();
    assert_eq!(path, "/hw/disk/1/100/5/000000000000dead000000000000beef");
    assert_eq!(DiskKey::from_path(&path), Ok(k));
}

#[test]
fn text_roundtrip_owner_map_key() {
    let k = OwnerMapKey {
        rack_id: 1,
        node_id: 50,
        disk_group_id: 7,
    };
    let path = k.to_path();
    assert_eq!(path, "/hw/dg_owner/1/50/7");
    assert_eq!(OwnerMapKey::from_path(&path), Ok(k));
}

#[test]
fn text_roundtrip_bind_map_key() {
    let k = BindMapKey {
        rack_id: 1,
        node_id: 50,
        disk_group_id: 7,
    };
    let path = k.to_path();
    assert_eq!(path, "/hw/dg_bind/1/50/7");
    assert_eq!(BindMapKey::from_path(&path), Ok(k));
}

#[test]
fn text_roundtrip_instance_key() {
    let k = InstanceKey {
        service: "diskdb".to_string(),
        instance_id: 123,
    };
    let path = k.to_path();
    assert_eq!(path, "/srv/diskdb/123");
    assert_eq!(InstanceKey::from_path(&path), Ok(k));
}

#[test]
fn text_roundtrip_disk_group_usage_key() {
    let k = DiskGroupUsageKey { disk_group_id: 7 };
    let path = k.to_path();
    assert_eq!(path, "/hw/dg_usage/7");
    assert_eq!(DiskGroupUsageKey::from_path(&path), Ok(k));
}

// ── TextKey KV-cluster round-trip ───────────────────────────────

#[test]
fn text_roundtrip_kv_store_key() {
    let k = KvStoreKey { store_id: 10 };
    let path = k.to_path();
    assert_eq!(path, "/kv/store/10");
    assert_eq!(KvStoreKey::from_path(&path), Ok(k));
}

#[test]
fn text_roundtrip_kv_group_key() {
    let k = KvGroupKey {
        store_id: 10,
        group_id: 2,
    };
    let path = k.to_path();
    assert_eq!(path, "/kv/group/10/2");
    assert_eq!(KvGroupKey::from_path(&path), Ok(k));
}

#[test]
fn text_roundtrip_kv_replica_key() {
    let k = KvReplicaKey {
        store_id: 10,
        group_id: 2,
        replica_id: 3,
    };
    let path = k.to_path();
    assert_eq!(path, "/kv/replica/10/2/3");
    assert_eq!(KvReplicaKey::from_path(&path), Ok(k));
}

// ── TextKey prefix exactness ────────────────────────────────────

#[test]
fn text_prefix_all_rack() {
    assert_eq!(<RackKey as TextKey>::prefix_all(), "/hw/rack/");
}

#[test]
fn text_prefix_all_node() {
    assert_eq!(<NodeKey as TextKey>::prefix_all(), "/hw/node/");
}

#[test]
fn text_prefix_for_rack_node() {
    assert_eq!(NodeKey::text_prefix_for_rack(7), "/hw/node/7/");
}

#[test]
fn text_prefix_for_node_disk_group() {
    assert_eq!(DiskGroupKey::text_prefix_for_node(1, 100), "/hw/dg/1/100/");
}

#[test]
fn text_prefix_for_disk_group_disk() {
    assert_eq!(
        DiskKey::text_prefix_for_disk_group(1, 100, 5),
        "/hw/disk/1/100/5/"
    );
}

#[test]
fn text_prefix_for_service_instance() {
    assert_eq!(InstanceKey::text_prefix_for_service("diskdb"), "/srv/diskdb/");
}

#[test]
fn text_prefix_kv_store_all() {
    assert_eq!(KvStoreKey::prefix_all(), "/kv/store/");
}

#[test]
fn text_prefix_kv_group_for_store() {
    assert_eq!(KvGroupKey::text_prefix_for_store(10), "/kv/group/10/");
}

#[test]
fn text_prefix_kv_replica_for_group() {
    assert_eq!(KvReplicaKey::text_prefix_for_group(10, 2), "/kv/replica/10/2/");
}

// ── TextKey rejection ───────────────────────────────────────────

#[test]
fn text_reject_bad_magic() {
    assert_eq!(RackKey::from_path("/xx/rack/99"), Err(KeyError::BadMagic));
}

#[test]
fn text_reject_bad_type() {
    assert_eq!(RackKey::from_path("/hw/node/99"), Err(KeyError::BadTag));
}

#[test]
fn text_reject_short_input() {
    assert_eq!(RackKey::from_path("/hw/rack"), Err(KeyError::ShortInput));
}

#[test]
fn text_reject_trailing_bytes() {
    assert_eq!(
        RackKey::from_path("/hw/rack/99/extra"),
        Err(KeyError::TrailingBytes)
    );
}

#[test]
fn text_reject_instance_bad_magic() {
    assert_eq!(InstanceKey::from_path("/xx/diskdb/123"), Err(KeyError::BadMagic));
}

#[test]
fn text_reject_instance_trailing() {
    assert_eq!(
        InstanceKey::from_path("/srv/diskdb/123/extra"),
        Err(KeyError::BadMagic)
    );
}
