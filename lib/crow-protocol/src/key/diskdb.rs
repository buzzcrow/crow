// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! diskdb key types.
//!
//! Group-0 keys (`DiskGroupKey`, `DiskKey`, `OwnerMapKey`, `BindMapKey`,
//! `InstanceKey`) implement both [`BinaryKey`] and [`TextKey`]. Data-
//! group keys (`ZoneKey`, `BusyBlockKey`, `FreeBlockKey`) implement
//! [`BinaryKey`] only.
//!
//! See `doc/design/protocol/design-crow-key.md` §5 for frozen layouts.
//! See `doc/design/diskdb/design-crow-diskdb.md` §5 and §7 for the
//! component-specific hierarchy.

use super::{
    check_exact, check_path_exact, decode_disk_id, decode_header, decode_path_disk_id, decode_path_u64,
    decode_u32, decode_u64, encode_disk_id, encode_header, encode_path_disk_id, encode_path_header,
    encode_path_u64, encode_u32, encode_u64, BinaryKey, KeyError, TextKey,
};
use crate::common::DiskId;
use crate::common_type::{DiskGroupId, NodeId, RackId};

// ── DiskGroupKey ────────────────────────────────────────────────

/// Key for a disk-group within a node.
/// Binary layout: `magic | 0x0003 | rack_id:u64 BE | node_id:u64 BE |
/// disk_group_id:u64 BE`. Total 27 bytes.
/// Text path: `/hw/dg/<rack_id>/<node_id>/<dg_id>`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DiskGroupKey {
    pub rack_id: RackId,
    pub node_id: NodeId,
    pub disk_group_id: DiskGroupId,
}

impl BinaryKey for DiskGroupKey {
    const TYPE_TAG: u16 = 0x0003;

    fn encode_to(&self, out: &mut Vec<u8>) {
        encode_header(out, Self::TYPE_TAG);
        encode_u64(out, self.rack_id);
        encode_u64(out, self.node_id);
        encode_u64(out, self.disk_group_id);
    }

    fn decode(buf: &[u8]) -> Result<Self, KeyError> {
        let fields = decode_header(buf, Self::TYPE_TAG)?;
        let (rack_id, o) = decode_u64(fields, 0)?;
        let (node_id, o) = decode_u64(fields, o)?;
        let (disk_group_id, o) = decode_u64(fields, o)?;
        check_exact(fields, o)?;
        Ok(Self {
            rack_id,
            node_id,
            disk_group_id,
        })
    }
}

impl TextKey for DiskGroupKey {
    const PATH_MAGIC: &'static str = "/hw";
    const PATH_TYPE: &'static str = "dg";

    fn encode_to_path(&self, out: &mut String) {
        encode_path_header(out, Self::PATH_MAGIC, Self::PATH_TYPE);
        encode_path_u64(out, self.rack_id);
        encode_path_u64(out, self.node_id);
        encode_path_u64(out, self.disk_group_id);
    }

    fn decode_path(parts: &[&str]) -> Result<Self, KeyError> {
        if parts.len() < 3 {
            return Err(KeyError::ShortInput);
        }
        let rack_id = decode_path_u64(parts[0])?;
        let node_id = decode_path_u64(parts[1])?;
        let disk_group_id = decode_path_u64(parts[2])?;
        check_path_exact(parts, 3)?;
        Ok(Self {
            rack_id,
            node_id,
            disk_group_id,
        })
    }
}

impl DiskGroupKey {
    /// Binary prefix for scanning all disk-groups: `magic | 0x0003`.
    #[must_use]
    pub fn prefix_all() -> Vec<u8> {
        let mut v = Vec::new();
        encode_header(&mut v, Self::TYPE_TAG);
        v
    }

    /// Binary prefix for scanning all disk-groups in a rack:
    /// `magic | 0x0003 | rack_id`.
    #[must_use]
    pub fn prefix_for_rack(rack_id: RackId) -> Vec<u8> {
        let mut v = Vec::new();
        encode_header(&mut v, Self::TYPE_TAG);
        encode_u64(&mut v, rack_id);
        v
    }

    /// Binary prefix for scanning all disk-groups under a node:
    /// `magic | 0x0003 | rack_id | node_id`.
    #[must_use]
    pub fn prefix_for_node(rack_id: RackId, node_id: NodeId) -> Vec<u8> {
        let mut v = Vec::new();
        encode_header(&mut v, Self::TYPE_TAG);
        encode_u64(&mut v, rack_id);
        encode_u64(&mut v, node_id);
        v
    }

    /// Text prefix for scanning all disk-groups under a node:
    /// `/hw/dg/<rack_id>/<node_id>/`.
    #[must_use]
    pub fn text_prefix_for_node(rack_id: RackId, node_id: NodeId) -> String {
        format!("/hw/dg/{rack_id}/{node_id}/")
    }
}

// ── DiskKey ─────────────────────────────────────────────────────

/// Key for a physical disk within a disk-group.
/// Binary layout: `magic | 0x0004 | rack_id:u64 BE | node_id:u64 BE |
/// disk_group_id:u64 BE | disk_id:16 bytes`. Total 35 bytes.
/// Text path: `/hw/disk/<rack_id>/<node_id>/<dg_id>/<disk_id_hex>`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DiskKey {
    pub rack_id: RackId,
    pub node_id: NodeId,
    pub disk_group_id: DiskGroupId,
    pub disk_id: DiskId,
}

impl BinaryKey for DiskKey {
    const TYPE_TAG: u16 = 0x0004;

    fn encode_to(&self, out: &mut Vec<u8>) {
        encode_header(out, Self::TYPE_TAG);
        encode_u64(out, self.rack_id);
        encode_u64(out, self.node_id);
        encode_u64(out, self.disk_group_id);
        encode_disk_id(out, &self.disk_id);
    }

    fn decode(buf: &[u8]) -> Result<Self, KeyError> {
        let fields = decode_header(buf, Self::TYPE_TAG)?;
        let (rack_id, o) = decode_u64(fields, 0)?;
        let (node_id, o) = decode_u64(fields, o)?;
        let (disk_group_id, o) = decode_u64(fields, o)?;
        let (disk_id, o) = decode_disk_id(fields, o)?;
        check_exact(fields, o)?;
        Ok(Self {
            rack_id,
            node_id,
            disk_group_id,
            disk_id,
        })
    }
}

impl TextKey for DiskKey {
    const PATH_MAGIC: &'static str = "/hw";
    const PATH_TYPE: &'static str = "disk";

    fn encode_to_path(&self, out: &mut String) {
        encode_path_header(out, Self::PATH_MAGIC, Self::PATH_TYPE);
        encode_path_u64(out, self.rack_id);
        encode_path_u64(out, self.node_id);
        encode_path_u64(out, self.disk_group_id);
        encode_path_disk_id(out, &self.disk_id);
    }

    fn decode_path(parts: &[&str]) -> Result<Self, KeyError> {
        if parts.len() < 4 {
            return Err(KeyError::ShortInput);
        }
        let rack_id = decode_path_u64(parts[0])?;
        let node_id = decode_path_u64(parts[1])?;
        let disk_group_id = decode_path_u64(parts[2])?;
        let disk_id = decode_path_disk_id(parts[3])?;
        check_path_exact(parts, 4)?;
        Ok(Self {
            rack_id,
            node_id,
            disk_group_id,
            disk_id,
        })
    }
}

impl DiskKey {
    /// Binary prefix for scanning all disks: `magic | 0x0004`.
    #[must_use]
    pub fn prefix_all() -> Vec<u8> {
        let mut v = Vec::new();
        encode_header(&mut v, Self::TYPE_TAG);
        v
    }

    /// Binary prefix for scanning all disks in a rack:
    /// `magic | 0x0004 | rack_id`.
    #[must_use]
    pub fn prefix_for_rack(rack_id: RackId) -> Vec<u8> {
        let mut v = Vec::new();
        encode_header(&mut v, Self::TYPE_TAG);
        encode_u64(&mut v, rack_id);
        v
    }

    /// Binary prefix for scanning all disks under a node:
    /// `magic | 0x0004 | rack_id | node_id`.
    #[must_use]
    pub fn prefix_for_node(rack_id: RackId, node_id: NodeId) -> Vec<u8> {
        let mut v = Vec::new();
        encode_header(&mut v, Self::TYPE_TAG);
        encode_u64(&mut v, rack_id);
        encode_u64(&mut v, node_id);
        v
    }

    /// Binary prefix for scanning all disks under a disk-group:
    /// `magic | 0x0004 | rack_id | node_id | disk_group_id`.
    #[must_use]
    pub fn prefix_for_disk_group(rack_id: RackId, node_id: NodeId, disk_group_id: DiskGroupId) -> Vec<u8> {
        let mut v = Vec::new();
        encode_header(&mut v, Self::TYPE_TAG);
        encode_u64(&mut v, rack_id);
        encode_u64(&mut v, node_id);
        encode_u64(&mut v, disk_group_id);
        v
    }

    /// Text prefix for scanning all disks under a disk-group:
    /// `/hw/disk/<rack_id>/<node_id>/<dg_id>/`.
    #[must_use]
    pub fn text_prefix_for_disk_group(
        rack_id: RackId,
        node_id: NodeId,
        disk_group_id: DiskGroupId,
    ) -> String {
        format!("/hw/disk/{rack_id}/{node_id}/{disk_group_id}/")
    }
}

// ── ZoneKey ─────────────────────────────────────────────────────

/// Key for a zone within a disk.
/// Binary layout: `magic | 0x0005 | disk_id:16 bytes | zone_index:u32 BE`.
/// Total 23 bytes. Binary-only (data-group key).
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
/// Binary layout: `magic | 0x0006 | disk_id:16 bytes | zone_index:u32 BE |
/// unit_offset:u64 BE`. Total 31 bytes. Binary-only (data-group key).
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
/// Binary layout: `magic | 0x0007 | disk_id:16 bytes | zone_index:u32 BE |
/// unit_offset:u64 BE`. Total 31 bytes. Binary-only (data-group key).
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
/// Binary layout: `magic | 0x0008 | rack_id:u64 BE | node_id:u64 BE |
/// disk_group_id:u64 BE`. Total 27 bytes. Same field shape as
/// `DiskGroupKey`, distinct tag.
/// Text path: `/hw/dg_owner/<rack_id>/<node_id>/<dg_id>`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct OwnerMapKey {
    pub rack_id: RackId,
    pub node_id: NodeId,
    pub disk_group_id: DiskGroupId,
}

impl BinaryKey for OwnerMapKey {
    const TYPE_TAG: u16 = 0x0008;

    fn encode_to(&self, out: &mut Vec<u8>) {
        encode_header(out, Self::TYPE_TAG);
        encode_u64(out, self.rack_id);
        encode_u64(out, self.node_id);
        encode_u64(out, self.disk_group_id);
    }

    fn decode(buf: &[u8]) -> Result<Self, KeyError> {
        let fields = decode_header(buf, Self::TYPE_TAG)?;
        let (rack_id, o) = decode_u64(fields, 0)?;
        let (node_id, o) = decode_u64(fields, o)?;
        let (disk_group_id, o) = decode_u64(fields, o)?;
        check_exact(fields, o)?;
        Ok(Self {
            rack_id,
            node_id,
            disk_group_id,
        })
    }
}

impl TextKey for OwnerMapKey {
    const PATH_MAGIC: &'static str = "/hw";
    const PATH_TYPE: &'static str = "dg_owner";

    fn encode_to_path(&self, out: &mut String) {
        encode_path_header(out, Self::PATH_MAGIC, Self::PATH_TYPE);
        encode_path_u64(out, self.rack_id);
        encode_path_u64(out, self.node_id);
        encode_path_u64(out, self.disk_group_id);
    }

    fn decode_path(parts: &[&str]) -> Result<Self, KeyError> {
        if parts.len() < 3 {
            return Err(KeyError::ShortInput);
        }
        let rack_id = decode_path_u64(parts[0])?;
        let node_id = decode_path_u64(parts[1])?;
        let disk_group_id = decode_path_u64(parts[2])?;
        check_path_exact(parts, 3)?;
        Ok(Self {
            rack_id,
            node_id,
            disk_group_id,
        })
    }
}

impl OwnerMapKey {
    /// Binary prefix for scanning all ownership-map entries:
    /// `magic | 0x0008`.
    #[must_use]
    pub fn prefix_all() -> Vec<u8> {
        let mut v = Vec::new();
        encode_header(&mut v, Self::TYPE_TAG);
        v
    }

    /// Binary prefix for scanning ownership entries in a rack:
    /// `magic | 0x0008 | rack_id`.
    #[must_use]
    pub fn prefix_for_rack(rack_id: RackId) -> Vec<u8> {
        let mut v = Vec::new();
        encode_header(&mut v, Self::TYPE_TAG);
        encode_u64(&mut v, rack_id);
        v
    }

    /// Binary prefix for scanning ownership entries for a node:
    /// `magic | 0x0008 | rack_id | node_id`.
    #[must_use]
    pub fn prefix_for_node(rack_id: RackId, node_id: NodeId) -> Vec<u8> {
        let mut v = Vec::new();
        encode_header(&mut v, Self::TYPE_TAG);
        encode_u64(&mut v, rack_id);
        encode_u64(&mut v, node_id);
        v
    }

    /// Text prefix for scanning ownership entries for a node:
    /// `/hw/dg_owner/<rack_id>/<node_id>/`.
    #[must_use]
    pub fn text_prefix_for_node(rack_id: RackId, node_id: NodeId) -> String {
        format!("/hw/dg_owner/{rack_id}/{node_id}/")
    }
}

// ── BindMapKey ──────────────────────────────────────────────────

/// Key for the bind-map entry (disk-group → paxos data group).
/// Binary layout: `magic | 0x0009 | rack_id:u64 BE | node_id:u64 BE |
/// disk_group_id:u64 BE`. Total 27 bytes. Same field shape as
/// `DiskGroupKey`, distinct tag.
/// Text path: `/hw/dg_bind/<rack_id>/<node_id>/<dg_id>`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BindMapKey {
    pub rack_id: RackId,
    pub node_id: NodeId,
    pub disk_group_id: DiskGroupId,
}

impl BinaryKey for BindMapKey {
    const TYPE_TAG: u16 = 0x0009;

    fn encode_to(&self, out: &mut Vec<u8>) {
        encode_header(out, Self::TYPE_TAG);
        encode_u64(out, self.rack_id);
        encode_u64(out, self.node_id);
        encode_u64(out, self.disk_group_id);
    }

    fn decode(buf: &[u8]) -> Result<Self, KeyError> {
        let fields = decode_header(buf, Self::TYPE_TAG)?;
        let (rack_id, o) = decode_u64(fields, 0)?;
        let (node_id, o) = decode_u64(fields, o)?;
        let (disk_group_id, o) = decode_u64(fields, o)?;
        check_exact(fields, o)?;
        Ok(Self {
            rack_id,
            node_id,
            disk_group_id,
        })
    }
}

impl TextKey for BindMapKey {
    const PATH_MAGIC: &'static str = "/hw";
    const PATH_TYPE: &'static str = "dg_bind";

    fn encode_to_path(&self, out: &mut String) {
        encode_path_header(out, Self::PATH_MAGIC, Self::PATH_TYPE);
        encode_path_u64(out, self.rack_id);
        encode_path_u64(out, self.node_id);
        encode_path_u64(out, self.disk_group_id);
    }

    fn decode_path(parts: &[&str]) -> Result<Self, KeyError> {
        if parts.len() < 3 {
            return Err(KeyError::ShortInput);
        }
        let rack_id = decode_path_u64(parts[0])?;
        let node_id = decode_path_u64(parts[1])?;
        let disk_group_id = decode_path_u64(parts[2])?;
        check_path_exact(parts, 3)?;
        Ok(Self {
            rack_id,
            node_id,
            disk_group_id,
        })
    }
}

impl BindMapKey {
    /// Binary prefix for scanning all bind-map entries: `magic | 0x0009`.
    #[must_use]
    pub fn prefix_all() -> Vec<u8> {
        let mut v = Vec::new();
        encode_header(&mut v, Self::TYPE_TAG);
        v
    }

    /// Binary prefix for scanning bind entries in a rack:
    /// `magic | 0x0009 | rack_id`.
    #[must_use]
    pub fn prefix_for_rack(rack_id: RackId) -> Vec<u8> {
        let mut v = Vec::new();
        encode_header(&mut v, Self::TYPE_TAG);
        encode_u64(&mut v, rack_id);
        v
    }

    /// Binary prefix for scanning bind entries for a node:
    /// `magic | 0x0009 | rack_id | node_id`.
    #[must_use]
    pub fn prefix_for_node(rack_id: RackId, node_id: NodeId) -> Vec<u8> {
        let mut v = Vec::new();
        encode_header(&mut v, Self::TYPE_TAG);
        encode_u64(&mut v, rack_id);
        encode_u64(&mut v, node_id);
        v
    }

    /// Text prefix for scanning bind entries for a node:
    /// `/hw/dg_bind/<rack_id>/<node_id>/`.
    #[must_use]
    pub fn text_prefix_for_node(rack_id: RackId, node_id: NodeId) -> String {
        format!("/hw/dg_bind/{rack_id}/{node_id}/")
    }
}

// ── InstanceKey ─────────────────────────────────────────────────

/// Key for a service instance registry entry.
/// Binary layout: `magic | 0x000A | service_len:u32 BE | service:UTF8 |
/// instance_id:u64 BE`. Variable length.
/// Text path: `/srv/<service>/<instance_id>`.
///
/// Does not implement [`TextKey`] because the path type segment is
/// dynamic (the service name). Use the inherent `to_path` / `from_path`
/// methods instead.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct InstanceKey {
    pub service: String,
    pub instance_id: u64,
}

impl BinaryKey for InstanceKey {
    const TYPE_TAG: u16 = 0x000A;

    fn encode_to(&self, out: &mut Vec<u8>) {
        encode_header(out, Self::TYPE_TAG);
        encode_u32(out, u32::try_from(self.service.len()).unwrap_or(u32::MAX));
        out.extend_from_slice(self.service.as_bytes());
        encode_u64(out, self.instance_id);
    }

    fn decode(buf: &[u8]) -> Result<Self, KeyError> {
        let fields = decode_header(buf, Self::TYPE_TAG)?;
        let (len, o) = decode_u32(fields, 0)?;
        let len = len as usize;
        if o + len > fields.len() {
            return Err(KeyError::ShortInput);
        }
        let service = std::str::from_utf8(&fields[o..o + len])
            .map_err(|_| KeyError::ShortInput)?
            .to_string();
        let o = o + len;
        let (instance_id, o) = decode_u64(fields, o)?;
        check_exact(fields, o)?;
        Ok(Self { service, instance_id })
    }
}

impl InstanceKey {
    /// Encode to a text path: `/srv/<service>/<instance_id>`.
    #[must_use]
    pub fn to_path(&self) -> String {
        format!("/srv/{}/{}", self.service, self.instance_id)
    }

    /// Parse from a text path: `/srv/<service>/<instance_id>`.
    ///
    /// # Errors
    /// Returns [`KeyError`] on bad format.
    pub fn from_path(s: &str) -> Result<Self, KeyError> {
        let parts: Vec<&str> = s.split('/').collect();
        // ["", "srv", "<service>", "<instance_id>"]
        if parts.len() != 4 || !parts[0].is_empty() || parts[1] != "srv" {
            return Err(KeyError::BadMagic);
        }
        let service = parts[2].to_string();
        let instance_id = decode_path_u64(parts[3])?;
        Ok(Self { service, instance_id })
    }

    /// Text prefix for scanning all instances of a service:
    /// `/srv/<service>/`.
    #[must_use]
    pub fn text_prefix_for_service(service: &str) -> String {
        format!("/srv/{service}/")
    }
}
