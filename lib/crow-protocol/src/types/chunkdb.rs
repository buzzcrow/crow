// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

#![allow(
    clippy::all,
    clippy::pedantic,
    clippy::missing_errors_doc,
    clippy::doc_markdown,
    clippy::default_trait_access,
    clippy::too_many_lines
)]

//! Hand-written Rust types replacing the prost-generated `crow.chunkdb.rpc`
//! types. API-compatible with the former proto-generated structs.

use serde::{Deserialize, Serialize};

use crate::common::ChunkId;
use crate::diskdb::rpc::Segment;

/// Implement `From<Enum> for i32` and `TryFrom<i32> for Enum`.
macro_rules! impl_enum_conversions {
    ($enum:ident, $($variant:ident = $value:expr),+ $(,)?) => {
        impl From<$enum> for i32 {
            fn from(v: $enum) -> Self {
                v as i32
            }
        }

        impl std::convert::TryFrom<i32> for $enum {
            type Error = ();

            fn try_from(v: i32) -> Result<Self, Self::Error> {
                Ok(match v {
                    $($value => $enum::$variant,)+
                    _ => return Err(()),
                })
            }
        }
    };
}

// ── Enums ───────────────────────────────────────────────────────

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, Default)]
#[repr(i32)]
pub enum EcState {
    #[default]
    NoParity = 0,
    Parity = 1,
}
impl_enum_conversions!(EcState, NoParity = 0, Parity = 1);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, Default)]
#[repr(i32)]
pub enum ChunkState {
    #[default]
    Init = 0,
    Active = 1,
    Sealed = 2,
    Deleted = 3,
}
impl_enum_conversions!(ChunkState, Init = 0, Active = 1, Sealed = 2, Deleted = 3);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, Default)]
#[repr(i32)]
pub enum StripType {
    #[default]
    Mirror = 0,
    Ec = 1,
}
impl_enum_conversions!(StripType, Mirror = 0, Ec = 1);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, Default)]
#[repr(i32)]
pub enum ChunkType {
    #[default]
    Repo = 0,
    Wal = 1,
    BtreePage = 2,
    PageIndex = 3,
}
impl_enum_conversions!(ChunkType, Repo = 0, Wal = 1, BtreePage = 2, PageIndex = 3);

// ── Strip types ─────────────────────────────────────────────────

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct MirrorStrip {
    pub segments: Vec<Segment>,
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct EcStrip {
    pub data_num: u32,
    pub code_num: u32,
    pub ec_state: i32,
    pub segments: Vec<Segment>,
}

/// Oneof `strip` field in `ChunkStrip`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Strip {
    MirrorStrip(MirrorStrip),
    EcStrip(EcStrip),
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct ChunkStrip {
    pub chunk_offset: u32,
    pub strip_sequence: u32,
    pub unit_kb: u32,
    pub capacity: u32,
    pub create_ts_ms: u64,
    pub sealed_ts_ms: u64,
    pub sealed_length: u32,
    pub strip_type: i32,
    pub strip: Option<Strip>,
    pub usage_bitmap: Vec<u8>,
}

// ── Chunk ───────────────────────────────────────────────────────

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct Chunk {
    pub id: Option<ChunkId>,
    pub state: i32,
    pub create_ts_ms: u64,
    pub sealed_ts_ms: u64,
    pub capacity: u32,
    pub sealed_length: u32,
    pub strips: Vec<ChunkStrip>,
    pub chunk_type: i32,
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct NotMyRangeHint {
    pub range_start: u32,
    pub range_end: u32,
    pub instance_id: u64,
    pub rpc_endpoint: String,
    pub sub_range_index: u32,
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct Location {
    pub chunk_id: Option<ChunkId>,
    pub offset: u64,
    pub length: u64,
    pub logical_offset: u64,
    pub logical_length: u64,
}

// ── RPC request/response types ──────────────────────────────────

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct AllocateChunkRequest {
    pub chunk_id: Option<ChunkId>,
    pub write_granularity: u32,
    pub strip_count: u32,
    pub strip_type: i32,
    pub data_num: u32,
    pub code_num: u32,
    pub copy_count: u32,
    pub chunk_type: i32,
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct AllocateChunkResponse {
    pub chunk: Option<Chunk>,
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct AppendChunkRequest {
    pub chunk_id: Option<ChunkId>,
    pub strip_size: u32,
    pub strip_count: u32,
    pub strip_type: i32,
    pub data_num: u32,
    pub code_num: u32,
    pub copy_count: u32,
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct AppendChunkResponse {
    pub chunk: Option<Chunk>,
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct QueryChunkRequest {
    pub chunk_id: Option<ChunkId>,
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct QueryChunkResponse {
    pub chunk: Option<Chunk>,
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct SealChunkRequest {
    pub chunk_id: Option<ChunkId>,
    pub seal_length: u32,
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct SealChunkResponse {
    pub chunk: Option<Chunk>,
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct DeleteChunkRequest {
    pub chunk_id: Option<ChunkId>,
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct DeleteChunkResponse {
    pub chunk: Option<Chunk>,
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct DeleteChunkRangeRequest {
    pub chunk_id: Option<ChunkId>,
    pub chunk_offset: u32,
    pub chunk_size: u32,
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct DeleteChunkRangeResponse {}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct UpdateChunkStripRequest {
    pub chunk_id: Option<ChunkId>,
    pub strip_index: u32,
    pub strip: Option<ChunkStrip>,
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct UpdateChunkStripResponse {
    pub chunk: Option<Chunk>,
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct ListChunksRequest {
    pub start_token: Option<ChunkId>,
    pub partition: u32,
    pub max_keys: u32,
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct ListChunksResponse {
    pub chunks: Vec<Chunk>,
    pub next_token: Option<ChunkId>,
}
