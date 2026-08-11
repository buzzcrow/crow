// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Common key types shared across CROW components (hardware hierarchy
//! root: rack, node).
//!
//! Each key type implements both [`BinaryKey`] and [`TextKey`]. The
//! binary encoding is used by diskdb data groups; the text encoding is
//! used by group 0 (see `doc/design/protocol/design-crow-key.md` §5).

use super::{
    check_exact, check_path_exact, decode_header, decode_path_u64, decode_u64, encode_header,
    encode_path_header, encode_path_u64, encode_u64, BinaryKey, KeyError, TextKey,
};
use crate::common_type::{NodeId, RackId};

// ── RackKey ─────────────────────────────────────────────────────

/// Key for a physical rack.
/// Binary layout: `magic | 0x0002 | rack_id:u64 BE`. Total 11 bytes.
/// Text path: `/hw/rack/<rack_id>`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RackKey {
    pub rack_id: RackId,
}

impl BinaryKey for RackKey {
    const TYPE_TAG: u16 = 0x0002;

    fn encode_to(&self, out: &mut Vec<u8>) {
        encode_header(out, Self::TYPE_TAG);
        encode_u64(out, self.rack_id);
    }

    fn decode(buf: &[u8]) -> Result<Self, KeyError> {
        let fields = decode_header(buf, Self::TYPE_TAG)?;
        let (rack_id, o) = decode_u64(fields, 0)?;
        check_exact(fields, o)?;
        Ok(Self { rack_id })
    }
}

impl TextKey for RackKey {
    const PATH_MAGIC: &'static str = "/hw";
    const PATH_TYPE: &'static str = "rack";

    fn encode_to_path(&self, out: &mut String) {
        encode_path_header(out, Self::PATH_MAGIC, Self::PATH_TYPE);
        encode_path_u64(out, self.rack_id);
    }

    fn decode_path(parts: &[&str]) -> Result<Self, KeyError> {
        if parts.is_empty() {
            return Err(KeyError::ShortInput);
        }
        let rack_id = decode_path_u64(parts[0])?;
        check_path_exact(parts, 1)?;
        Ok(Self { rack_id })
    }
}

impl RackKey {
    /// Binary prefix for scanning all racks: `magic | 0x0002`.
    #[must_use]
    pub fn prefix_all() -> Vec<u8> {
        let mut v = Vec::new();
        encode_header(&mut v, Self::TYPE_TAG);
        v
    }
}

// ── NodeKey ─────────────────────────────────────────────────────

/// Key for a physical node within a rack.
/// Binary layout: `magic | 0x0001 | rack_id:u64 BE | node_id:u64 BE`.
/// Total 19 bytes.
/// Text path: `/hw/node/<rack_id>/<node_id>`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NodeKey {
    pub rack_id: RackId,
    pub node_id: NodeId,
}

impl BinaryKey for NodeKey {
    const TYPE_TAG: u16 = 0x0001;

    fn encode_to(&self, out: &mut Vec<u8>) {
        encode_header(out, Self::TYPE_TAG);
        encode_u64(out, self.rack_id);
        encode_u64(out, self.node_id);
    }

    fn decode(buf: &[u8]) -> Result<Self, KeyError> {
        let fields = decode_header(buf, Self::TYPE_TAG)?;
        let (rack_id, o) = decode_u64(fields, 0)?;
        let (node_id, o) = decode_u64(fields, o)?;
        check_exact(fields, o)?;
        Ok(Self { rack_id, node_id })
    }
}

impl TextKey for NodeKey {
    const PATH_MAGIC: &'static str = "/hw";
    const PATH_TYPE: &'static str = "node";

    fn encode_to_path(&self, out: &mut String) {
        encode_path_header(out, Self::PATH_MAGIC, Self::PATH_TYPE);
        encode_path_u64(out, self.rack_id);
        encode_path_u64(out, self.node_id);
    }

    fn decode_path(parts: &[&str]) -> Result<Self, KeyError> {
        if parts.len() < 2 {
            return Err(KeyError::ShortInput);
        }
        let rack_id = decode_path_u64(parts[0])?;
        let node_id = decode_path_u64(parts[1])?;
        check_path_exact(parts, 2)?;
        Ok(Self { rack_id, node_id })
    }
}

impl NodeKey {
    /// Binary prefix for scanning all nodes: `magic | 0x0001`.
    #[must_use]
    pub fn prefix_all() -> Vec<u8> {
        let mut v = Vec::new();
        encode_header(&mut v, Self::TYPE_TAG);
        v
    }

    /// Binary prefix for scanning all nodes in a rack:
    /// `magic | 0x0001 | rack_id`.
    #[must_use]
    pub fn prefix_for_rack(rack_id: RackId) -> Vec<u8> {
        let mut v = Vec::new();
        encode_header(&mut v, Self::TYPE_TAG);
        encode_u64(&mut v, rack_id);
        v
    }

    /// Text prefix for scanning all nodes in a rack:
    /// `/hw/node/<rack_id>/`.
    #[must_use]
    pub fn text_prefix_for_rack(rack_id: RackId) -> String {
        format!("/hw/node/{rack_id}/")
    }
}
