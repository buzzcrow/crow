// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! diskdb binary key types.
//!
//! See `doc/design/protocol/design-crow-key.md` §5 for frozen layouts.
//! See `doc/design/diskdb/design-crow-diskdb.md` §5 and §7 for the
//! component-specific hierarchy.

use super::{
    check_exact, decode_disk_id, decode_header, decode_u32, decode_u64, encode_disk_id, encode_header,
    encode_u32, encode_u64, BinaryKey, KeyError,
};
use crate::common::DiskId;

// ── DiskGroupKey ────────────────────────────────────────────────

/// Key for a disk-group within a node.
/// Layout: `magic | 0x0003 | node_id:u64 BE | disk_group_id:u32 BE`.
/// Total 15 bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DiskGroupKey {
    pub node_id: u64,
    pub disk_group_id: u32,
}

impl BinaryKey for DiskGroupKey {
    const TYPE_TAG: u16 = 0x0003;

    fn encode_to(&self, out: &mut Vec<u8>) {
        encode_header(out, Self::TYPE_TAG);
        encode_u64(out, self.node_id);
        encode_u32(out, self.disk_group_id);
    }

    fn decode(buf: &[u8]) -> Result<Self, KeyError> {
        let fields = decode_header(buf, Self::TYPE_TAG)?;
        let (node_id, o) = decode_u64(fields, 0)?;
        let (disk_group_id, o) = decode_u32(fields, o)?;
        check_exact(fields, o)?;
        Ok(Self {
            node_id,
            disk_group_id,
        })
    }
}

impl DiskGroupKey {
    /// Prefix for scanning all disk-groups under a node:
    /// `magic | 0x0003 | node_id`.
    #[must_use]
    pub fn prefix_for_node(node_id: u64) -> Vec<u8> {
        let mut v = Vec::new();
        encode_header(&mut v, Self::TYPE_TAG);
        encode_u64(&mut v, node_id);
        v
    }
}

// ── DiskKey ─────────────────────────────────────────────────────

/// Key for a physical disk within a disk-group.
/// Layout: `magic | 0x0004 | node_id:u64 BE | disk_group_id:u32 BE |
/// disk_id:16 bytes`. Total 29 bytes.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DiskKey {
    pub node_id: u64,
    pub disk_group_id: u32,
    pub disk_id: DiskId,
}

impl BinaryKey for DiskKey {
    const TYPE_TAG: u16 = 0x0004;

    fn encode_to(&self, out: &mut Vec<u8>) {
        encode_header(out, Self::TYPE_TAG);
        encode_u64(out, self.node_id);
        encode_u32(out, self.disk_group_id);
        encode_disk_id(out, &self.disk_id);
    }

    fn decode(buf: &[u8]) -> Result<Self, KeyError> {
        let fields = decode_header(buf, Self::TYPE_TAG)?;
        let (node_id, o) = decode_u64(fields, 0)?;
        let (disk_group_id, o) = decode_u32(fields, o)?;
        let (disk_id, o) = decode_disk_id(fields, o)?;
        check_exact(fields, o)?;
        Ok(Self {
            node_id,
            disk_group_id,
            disk_id,
        })
    }
}

impl DiskKey {
    /// Prefix for scanning all disks under a node:
    /// `magic | 0x0004 | node_id`.
    #[must_use]
    pub fn prefix_for_node(node_id: u64) -> Vec<u8> {
        let mut v = Vec::new();
        encode_header(&mut v, Self::TYPE_TAG);
        encode_u64(&mut v, node_id);
        v
    }

    /// Prefix for scanning all disks under a disk-group:
    /// `magic | 0x0004 | node_id | disk_group_id`.
    #[must_use]
    pub fn prefix_for_disk_group(node_id: u64, disk_group_id: u32) -> Vec<u8> {
        let mut v = Vec::new();
        encode_header(&mut v, Self::TYPE_TAG);
        encode_u64(&mut v, node_id);
        encode_u32(&mut v, disk_group_id);
        v
    }
}

// ── ZoneKey ─────────────────────────────────────────────────────

/// Key for a zone within a disk.
/// Layout: `magic | 0x0005 | disk_id:16 bytes | zone_index:u32 BE`.
/// Total 23 bytes.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ZoneKey {
    pub disk_id: DiskId,
    pub zone_index: u32,
}

impl BinaryKey for ZoneKey {
    const TYPE_TAG: u16 = 0x0005;

    fn encode_to(&self, out: &mut Vec<u8>) {
        encode_header(out, Self::TYPE_TAG);
        encode_disk_id(out, &self.disk_id);
        encode_u32(out, self.zone_index);
    }

    fn decode(buf: &[u8]) -> Result<Self, KeyError> {
        let fields = decode_header(buf, Self::TYPE_TAG)?;
        let (disk_id, o) = decode_disk_id(fields, 0)?;
        let (zone_index, o) = decode_u32(fields, o)?;
        check_exact(fields, o)?;
        Ok(Self { disk_id, zone_index })
    }
}

impl ZoneKey {
    /// Prefix for scanning all zones of a disk:
    /// `magic | 0x0005 | disk_id`.
    #[must_use]
    pub fn prefix_for_disk(disk_id: &DiskId) -> Vec<u8> {
        let mut v = Vec::new();
        encode_header(&mut v, Self::TYPE_TAG);
        encode_disk_id(&mut v, disk_id);
        v
    }
}

// ── BusyBlockKey ────────────────────────────────────────────────

/// Key for an allocated block range.
/// Layout: `magic | 0x0006 | disk_id:16 bytes | zone_index:u32 BE |
/// unit_offset:u64 BE`. Total 31 bytes.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BusyBlockKey {
    pub disk_id: DiskId,
    pub zone_index: u32,
    pub unit_offset: u64,
}

impl BinaryKey for BusyBlockKey {
    const TYPE_TAG: u16 = 0x0006;

    fn encode_to(&self, out: &mut Vec<u8>) {
        encode_header(out, Self::TYPE_TAG);
        encode_disk_id(out, &self.disk_id);
        encode_u32(out, self.zone_index);
        encode_u64(out, self.unit_offset);
    }

    fn decode(buf: &[u8]) -> Result<Self, KeyError> {
        let fields = decode_header(buf, Self::TYPE_TAG)?;
        let (disk_id, o) = decode_disk_id(fields, 0)?;
        let (zone_index, o) = decode_u32(fields, o)?;
        let (unit_offset, o) = decode_u64(fields, o)?;
        check_exact(fields, o)?;
        Ok(Self {
            disk_id,
            zone_index,
            unit_offset,
        })
    }
}

impl BusyBlockKey {
    /// Prefix for scanning all busy blocks in a zone:
    /// `magic | 0x0006 | disk_id | zone_index`.
    #[must_use]
    pub fn prefix_for_zone(disk_id: &DiskId, zone_index: u32) -> Vec<u8> {
        let mut v = Vec::new();
        encode_header(&mut v, Self::TYPE_TAG);
        encode_disk_id(&mut v, disk_id);
        encode_u32(&mut v, zone_index);
        v
    }
}

// ── FreeBlockKey ────────────────────────────────────────────────

/// Key for a freed block range.
/// Layout: `magic | 0x0007 | disk_id:16 bytes | zone_index:u32 BE |
/// unit_offset:u64 BE`. Total 31 bytes.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FreeBlockKey {
    pub disk_id: DiskId,
    pub zone_index: u32,
    pub unit_offset: u64,
}

impl BinaryKey for FreeBlockKey {
    const TYPE_TAG: u16 = 0x0007;

    fn encode_to(&self, out: &mut Vec<u8>) {
        encode_header(out, Self::TYPE_TAG);
        encode_disk_id(out, &self.disk_id);
        encode_u32(out, self.zone_index);
        encode_u64(out, self.unit_offset);
    }

    fn decode(buf: &[u8]) -> Result<Self, KeyError> {
        let fields = decode_header(buf, Self::TYPE_TAG)?;
        let (disk_id, o) = decode_disk_id(fields, 0)?;
        let (zone_index, o) = decode_u32(fields, o)?;
        let (unit_offset, o) = decode_u64(fields, o)?;
        check_exact(fields, o)?;
        Ok(Self {
            disk_id,
            zone_index,
            unit_offset,
        })
    }
}

impl FreeBlockKey {
    /// Prefix for scanning all free blocks in a zone:
    /// `magic | 0x0007 | disk_id | zone_index`.
    #[must_use]
    pub fn prefix_for_zone(disk_id: &DiskId, zone_index: u32) -> Vec<u8> {
        let mut v = Vec::new();
        encode_header(&mut v, Self::TYPE_TAG);
        encode_disk_id(&mut v, disk_id);
        encode_u32(&mut v, zone_index);
        v
    }
}

// ── OwnerMapKey ─────────────────────────────────────────────────

/// Key for the ownership-map entry (disk-group → diskdb instance).
/// Layout: `magic | 0x0008 | node_id:u64 BE | disk_group_id:u32 BE`.
/// Total 15 bytes. Same field shape as `DiskGroupKey`, distinct tag.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct OwnerMapKey {
    pub node_id: u64,
    pub disk_group_id: u32,
}

impl BinaryKey for OwnerMapKey {
    const TYPE_TAG: u16 = 0x0008;

    fn encode_to(&self, out: &mut Vec<u8>) {
        encode_header(out, Self::TYPE_TAG);
        encode_u64(out, self.node_id);
        encode_u32(out, self.disk_group_id);
    }

    fn decode(buf: &[u8]) -> Result<Self, KeyError> {
        let fields = decode_header(buf, Self::TYPE_TAG)?;
        let (node_id, o) = decode_u64(fields, 0)?;
        let (disk_group_id, o) = decode_u32(fields, o)?;
        check_exact(fields, o)?;
        Ok(Self {
            node_id,
            disk_group_id,
        })
    }
}

impl OwnerMapKey {
    /// Prefix for scanning all ownership-map entries: `magic | 0x0008`.
    #[must_use]
    pub fn prefix_all() -> Vec<u8> {
        let mut v = Vec::new();
        encode_header(&mut v, Self::TYPE_TAG);
        v
    }
}

// ── BindMapKey ──────────────────────────────────────────────────

/// Key for the bind-map entry (disk-group → paxos data group).
/// Layout: `magic | 0x0009 | node_id:u64 BE | disk_group_id:u32 BE`.
/// Total 15 bytes. Same field shape as `DiskGroupKey`, distinct tag.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BindMapKey {
    pub node_id: u64,
    pub disk_group_id: u32,
}

impl BinaryKey for BindMapKey {
    const TYPE_TAG: u16 = 0x0009;

    fn encode_to(&self, out: &mut Vec<u8>) {
        encode_header(out, Self::TYPE_TAG);
        encode_u64(out, self.node_id);
        encode_u32(out, self.disk_group_id);
    }

    fn decode(buf: &[u8]) -> Result<Self, KeyError> {
        let fields = decode_header(buf, Self::TYPE_TAG)?;
        let (node_id, o) = decode_u64(fields, 0)?;
        let (disk_group_id, o) = decode_u32(fields, o)?;
        check_exact(fields, o)?;
        Ok(Self {
            node_id,
            disk_group_id,
        })
    }
}

impl BindMapKey {
    /// Prefix for scanning all bind-map entries: `magic | 0x0009`.
    #[must_use]
    pub fn prefix_all() -> Vec<u8> {
        let mut v = Vec::new();
        encode_header(&mut v, Self::TYPE_TAG);
        v
    }
}

// ── InstanceKey ─────────────────────────────────────────────────

/// Key for a diskdb instance registry entry.
/// Layout: `magic | 0x000A | instance_id:u64 BE`. Total 11 bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct InstanceKey {
    pub instance_id: u64,
}

impl BinaryKey for InstanceKey {
    const TYPE_TAG: u16 = 0x000A;

    fn encode_to(&self, out: &mut Vec<u8>) {
        encode_header(out, Self::TYPE_TAG);
        encode_u64(out, self.instance_id);
    }

    fn decode(buf: &[u8]) -> Result<Self, KeyError> {
        let fields = decode_header(buf, Self::TYPE_TAG)?;
        let (instance_id, o) = decode_u64(fields, 0)?;
        check_exact(fields, o)?;
        Ok(Self { instance_id })
    }
}

impl InstanceKey {
    /// Prefix for scanning all diskdb instances: `magic | 0x000A`.
    #[must_use]
    pub fn prefix_all() -> Vec<u8> {
        let mut v = Vec::new();
        encode_header(&mut v, Self::TYPE_TAG);
        v
    }
}
