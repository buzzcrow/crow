// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Common binary key types shared across CROW components.
//!
//! See `doc/design/protocol/design-crow-key.md` §5 for frozen layouts.

use super::{check_exact, decode_header, decode_u64, encode_header, encode_u64, BinaryKey, KeyError};

// ── NodeKey ─────────────────────────────────────────────────────

/// Key for a physical node. Layout: `magic | 0x0001 | node_id:u64 BE`.
/// Total 11 bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NodeKey {
    pub node_id: u64,
}

impl BinaryKey for NodeKey {
    const TYPE_TAG: u16 = 0x0001;

    fn encode_to(&self, out: &mut Vec<u8>) {
        encode_header(out, Self::TYPE_TAG);
        encode_u64(out, self.node_id);
    }

    fn decode(buf: &[u8]) -> Result<Self, KeyError> {
        let fields = decode_header(buf, Self::TYPE_TAG)?;
        let (node_id, o) = decode_u64(fields, 0)?;
        check_exact(fields, o)?;
        Ok(Self { node_id })
    }
}

impl NodeKey {
    /// Prefix for scanning all nodes: `magic | 0x0001`.
    #[must_use]
    pub fn prefix_all() -> Vec<u8> {
        let mut v = Vec::new();
        encode_header(&mut v, Self::TYPE_TAG);
        v
    }
}

// ── RackKey ─────────────────────────────────────────────────────

/// Key for a rack within a data center.
/// Layout: `magic | 0x0002 | dc_id:u64 BE | rack_id:u64 BE`.
/// Total 19 bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RackKey {
    pub dc_id: u64,
    pub rack_id: u64,
}

impl BinaryKey for RackKey {
    const TYPE_TAG: u16 = 0x0002;

    fn encode_to(&self, out: &mut Vec<u8>) {
        encode_header(out, Self::TYPE_TAG);
        encode_u64(out, self.dc_id);
        encode_u64(out, self.rack_id);
    }

    fn decode(buf: &[u8]) -> Result<Self, KeyError> {
        let fields = decode_header(buf, Self::TYPE_TAG)?;
        let (dc_id, o) = decode_u64(fields, 0)?;
        let (rack_id, o) = decode_u64(fields, o)?;
        check_exact(fields, o)?;
        Ok(Self { dc_id, rack_id })
    }
}

impl RackKey {
    /// Prefix for scanning all racks in a data center:
    /// `magic | 0x0002 | dc_id`.
    #[must_use]
    pub fn prefix_for_dc(dc_id: u64) -> Vec<u8> {
        let mut v = Vec::new();
        encode_header(&mut v, Self::TYPE_TAG);
        encode_u64(&mut v, dc_id);
        v
    }
}
