// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! `Location` — addressing unit for object data within chunks.

use crow_protocol::chunkdb::rpc::Location as ProtoLocation;
use crow_protocol::common::ChunkId;
use prost::Message;

/// Where a contiguous byte range of one object lives within one chunk.
///
/// An object spanning N chunks has a `Vec<Location>` of N entries
/// ordered by `logical_offset`, contiguous and non-overlapping.
///
/// `offset` is always 0 for R94 (the writer fills each dedicated chunk
/// from offset 0); the field exists for R106 (shared chunks) and R107
/// (range reads).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Location {
    pub chunk_id: ChunkId,
    pub offset: u64,
    pub length: u64,
    pub logical_offset: u64,
    pub logical_length: u64,
}

impl Location {
    /// Convert to the protobuf representation.
    pub fn to_proto(&self) -> ProtoLocation {
        ProtoLocation {
            chunk_id: Some(self.chunk_id),
            offset: self.offset,
            length: self.length,
            logical_offset: self.logical_offset,
            logical_length: self.logical_length,
        }
    }

    /// Convert from the protobuf representation.
    pub fn from_proto(p: &ProtoLocation) -> Self {
        Self {
            chunk_id: p.chunk_id.unwrap_or_default(),
            offset: p.offset,
            length: p.length,
            logical_offset: p.logical_offset,
            logical_length: p.logical_length,
        }
    }

    /// Encode to protobuf bytes (for KV-stored object metadata).
    pub fn to_proto_bytes(&self) -> Vec<u8> {
        self.to_proto().encode_to_vec()
    }

    /// Decode from protobuf bytes.
    pub fn from_proto_bytes(data: &[u8]) -> Result<Self, prost::DecodeError> {
        let p = ProtoLocation::decode(data)?;
        Ok(Self::from_proto(&p))
    }

    /// Compact binary encoding: 16 (chunk_id) + 4×8 = 48 bytes.
    /// Layout: high(8) + low(8) + offset(8) + length(8) +
    /// logical_offset(8) + logical_length(8), all big-endian.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(48);
        buf.extend_from_slice(&self.chunk_id.high.to_be_bytes());
        buf.extend_from_slice(&self.chunk_id.low.to_be_bytes());
        buf.extend_from_slice(&self.offset.to_be_bytes());
        buf.extend_from_slice(&self.length.to_be_bytes());
        buf.extend_from_slice(&self.logical_offset.to_be_bytes());
        buf.extend_from_slice(&self.logical_length.to_be_bytes());
        buf
    }

    /// Decode from compact binary encoding.
    ///
    /// # Panics
    ///
    /// Panics if `data.len() != 48` passes the check but slice indexing
    /// fails (impossible for a 48-byte slice).
    pub fn from_bytes(data: &[u8]) -> Result<Self, &'static str> {
        if data.len() != 48 {
            return Err("expected 48 bytes");
        }
        let chunk_id = ChunkId {
            high: u64::from_be_bytes(data[0..8].try_into().unwrap()),
            low: u64::from_be_bytes(data[8..16].try_into().unwrap()),
        };
        Ok(Self {
            chunk_id,
            offset: u64::from_be_bytes(data[16..24].try_into().unwrap()),
            length: u64::from_be_bytes(data[24..32].try_into().unwrap()),
            logical_offset: u64::from_be_bytes(data[32..40].try_into().unwrap()),
            logical_length: u64::from_be_bytes(data[40..48].try_into().unwrap()),
        })
    }
}
