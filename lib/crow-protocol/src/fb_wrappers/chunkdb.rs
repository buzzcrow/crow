// Copyright 2026-present buzzcrow <buzzcrow@126.com>

#![allow(clippy::must_use_candidate)]

//! Zero-copy `FB<Type>Ref` wrappers for chunkdb response types
//! (design-crow-rpc.md §6, R116).
//!
//! Each wrapper holds a `&[u8]` reference to the control buffer,
//! parses the root on construction, and exposes typed accessor methods
//! that read through the root pointer — no per-field copy, no owned
//! intermediate struct.

use crate::chunkdb_fb::{
    FBAllocateChunkResponse, FBAppendChunkResponse, FBChunk, FBChunkdbRetCode, FBDeleteChunkRangeResponse,
    FBDeleteChunkResponse, FBListChunksResponse, FBQueryChunkResponse, FBSealChunkResponse,
    FBUpdateChunkStripResponse,
};

use super::parse_root;

// ── Shared helpers ───────────────────────────────────────────────

/// Check if `ret_code` is `Success`.
fn is_ok(code: FBChunkdbRetCode) -> bool {
    code == FBChunkdbRetCode::Success
}

// ── FBAllocateChunkResponseRef ───────────────────────────────────

/// Zero-copy view over an `FBAllocateChunkResponse` control buffer.
pub struct FBAllocateChunkResponseRef<'a> {
    root: Option<FBAllocateChunkResponse<'a>>,
}

impl<'a> FBAllocateChunkResponseRef<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self {
            root: parse_root::<FBAllocateChunkResponse>(buf),
        }
    }
    pub fn valid(&self) -> bool {
        self.root.is_some()
    }
    pub fn ret_code(&self) -> FBChunkdbRetCode {
        self.root.map_or(FBChunkdbRetCode::Internal, |r| r.ret_code())
    }
    pub fn error_msg(&self) -> Option<&'a str> {
        self.root.and_then(|r| r.error_msg())
    }
    pub fn request_id(&self) -> Option<u64> {
        self.root.map(|r| r.id())
    }
    pub fn ok(&self) -> bool {
        self.root.is_some_and(|r| is_ok(r.ret_code()))
    }
    pub fn range_start(&self) -> u32 {
        self.root.map_or(0, |r| r.range_start())
    }
    pub fn range_end(&self) -> u32 {
        self.root.map_or(0, |r| r.range_end())
    }
    pub fn chunk(&self) -> Option<FBChunk<'a>> {
        self.root.and_then(|r| r.chunk())
    }
}

// ── FBAppendChunkResponseRef ─────────────────────────────────────

/// Zero-copy view over an `FBAppendChunkResponse` control buffer.
pub struct FBAppendChunkResponseRef<'a> {
    root: Option<FBAppendChunkResponse<'a>>,
}

impl<'a> FBAppendChunkResponseRef<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self {
            root: parse_root::<FBAppendChunkResponse>(buf),
        }
    }
    pub fn valid(&self) -> bool {
        self.root.is_some()
    }
    pub fn ret_code(&self) -> FBChunkdbRetCode {
        self.root.map_or(FBChunkdbRetCode::Internal, |r| r.ret_code())
    }
    pub fn error_msg(&self) -> Option<&'a str> {
        self.root.and_then(|r| r.error_msg())
    }
    pub fn request_id(&self) -> Option<u64> {
        self.root.map(|r| r.id())
    }
    pub fn ok(&self) -> bool {
        self.root.is_some_and(|r| is_ok(r.ret_code()))
    }
    pub fn range_start(&self) -> u32 {
        self.root.map_or(0, |r| r.range_start())
    }
    pub fn range_end(&self) -> u32 {
        self.root.map_or(0, |r| r.range_end())
    }
    pub fn chunk(&self) -> Option<FBChunk<'a>> {
        self.root.and_then(|r| r.chunk())
    }
}

// ── FBQueryChunkResponseRef ──────────────────────────────────────

/// Zero-copy view over an `FBQueryChunkResponse` control buffer.
pub struct FBQueryChunkResponseRef<'a> {
    root: Option<FBQueryChunkResponse<'a>>,
}

impl<'a> FBQueryChunkResponseRef<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self {
            root: parse_root::<FBQueryChunkResponse>(buf),
        }
    }
    pub fn valid(&self) -> bool {
        self.root.is_some()
    }
    pub fn ret_code(&self) -> FBChunkdbRetCode {
        self.root.map_or(FBChunkdbRetCode::Internal, |r| r.ret_code())
    }
    pub fn error_msg(&self) -> Option<&'a str> {
        self.root.and_then(|r| r.error_msg())
    }
    pub fn request_id(&self) -> Option<u64> {
        self.root.map(|r| r.id())
    }
    pub fn ok(&self) -> bool {
        self.root.is_some_and(|r| is_ok(r.ret_code()))
    }
    pub fn range_start(&self) -> u32 {
        self.root.map_or(0, |r| r.range_start())
    }
    pub fn range_end(&self) -> u32 {
        self.root.map_or(0, |r| r.range_end())
    }
    pub fn chunk(&self) -> Option<FBChunk<'a>> {
        self.root.and_then(|r| r.chunk())
    }
}

// ── FBSealChunkResponseRef ───────────────────────────────────────

/// Zero-copy view over an `FBSealChunkResponse` control buffer.
pub struct FBSealChunkResponseRef<'a> {
    root: Option<FBSealChunkResponse<'a>>,
}

impl<'a> FBSealChunkResponseRef<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self {
            root: parse_root::<FBSealChunkResponse>(buf),
        }
    }
    pub fn valid(&self) -> bool {
        self.root.is_some()
    }
    pub fn ret_code(&self) -> FBChunkdbRetCode {
        self.root.map_or(FBChunkdbRetCode::Internal, |r| r.ret_code())
    }
    pub fn error_msg(&self) -> Option<&'a str> {
        self.root.and_then(|r| r.error_msg())
    }
    pub fn request_id(&self) -> Option<u64> {
        self.root.map(|r| r.id())
    }
    pub fn ok(&self) -> bool {
        self.root.is_some_and(|r| is_ok(r.ret_code()))
    }
    pub fn range_start(&self) -> u32 {
        self.root.map_or(0, |r| r.range_start())
    }
    pub fn range_end(&self) -> u32 {
        self.root.map_or(0, |r| r.range_end())
    }
    pub fn chunk(&self) -> Option<FBChunk<'a>> {
        self.root.and_then(|r| r.chunk())
    }
}

// ── FBDeleteChunkResponseRef ─────────────────────────────────────

/// Zero-copy view over an `FBDeleteChunkResponse` control buffer.
pub struct FBDeleteChunkResponseRef<'a> {
    root: Option<FBDeleteChunkResponse<'a>>,
}

impl<'a> FBDeleteChunkResponseRef<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self {
            root: parse_root::<FBDeleteChunkResponse>(buf),
        }
    }
    pub fn valid(&self) -> bool {
        self.root.is_some()
    }
    pub fn ret_code(&self) -> FBChunkdbRetCode {
        self.root.map_or(FBChunkdbRetCode::Internal, |r| r.ret_code())
    }
    pub fn error_msg(&self) -> Option<&'a str> {
        self.root.and_then(|r| r.error_msg())
    }
    pub fn request_id(&self) -> Option<u64> {
        self.root.map(|r| r.id())
    }
    pub fn ok(&self) -> bool {
        self.root.is_some_and(|r| is_ok(r.ret_code()))
    }
    pub fn range_start(&self) -> u32 {
        self.root.map_or(0, |r| r.range_start())
    }
    pub fn range_end(&self) -> u32 {
        self.root.map_or(0, |r| r.range_end())
    }
    pub fn chunk(&self) -> Option<FBChunk<'a>> {
        self.root.and_then(|r| r.chunk())
    }
}

// ── FBDeleteChunkRangeResponseRef ────────────────────────────────

/// Zero-copy view over an `FBDeleteChunkRangeResponse` control buffer.
/// This response has no `chunk` field (range delete returns no data).
pub struct FBDeleteChunkRangeResponseRef<'a> {
    root: Option<FBDeleteChunkRangeResponse<'a>>,
}

impl<'a> FBDeleteChunkRangeResponseRef<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self {
            root: parse_root::<FBDeleteChunkRangeResponse>(buf),
        }
    }
    pub fn valid(&self) -> bool {
        self.root.is_some()
    }
    pub fn ret_code(&self) -> FBChunkdbRetCode {
        self.root.map_or(FBChunkdbRetCode::Internal, |r| r.ret_code())
    }
    pub fn error_msg(&self) -> Option<&'a str> {
        self.root.and_then(|r| r.error_msg())
    }
    pub fn request_id(&self) -> Option<u64> {
        self.root.map(|r| r.id())
    }
    pub fn ok(&self) -> bool {
        self.root.is_some_and(|r| is_ok(r.ret_code()))
    }
    pub fn range_start(&self) -> u32 {
        self.root.map_or(0, |r| r.range_start())
    }
    pub fn range_end(&self) -> u32 {
        self.root.map_or(0, |r| r.range_end())
    }
}

// ── FBUpdateChunkStripResponseRef ────────────────────────────────

/// Zero-copy view over an `FBUpdateChunkStripResponse` control buffer.
pub struct FBUpdateChunkStripResponseRef<'a> {
    root: Option<FBUpdateChunkStripResponse<'a>>,
}

impl<'a> FBUpdateChunkStripResponseRef<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self {
            root: parse_root::<FBUpdateChunkStripResponse>(buf),
        }
    }
    pub fn valid(&self) -> bool {
        self.root.is_some()
    }
    pub fn ret_code(&self) -> FBChunkdbRetCode {
        self.root.map_or(FBChunkdbRetCode::Internal, |r| r.ret_code())
    }
    pub fn error_msg(&self) -> Option<&'a str> {
        self.root.and_then(|r| r.error_msg())
    }
    pub fn request_id(&self) -> Option<u64> {
        self.root.map(|r| r.id())
    }
    pub fn ok(&self) -> bool {
        self.root.is_some_and(|r| is_ok(r.ret_code()))
    }
    pub fn range_start(&self) -> u32 {
        self.root.map_or(0, |r| r.range_start())
    }
    pub fn range_end(&self) -> u32 {
        self.root.map_or(0, |r| r.range_end())
    }
    pub fn chunk(&self) -> Option<FBChunk<'a>> {
        self.root.and_then(|r| r.chunk())
    }
}

// ── FBListChunksResponseRef ──────────────────────────────────────

/// Zero-copy view over an `FBListChunksResponse` control buffer.
pub struct FBListChunksResponseRef<'a> {
    root: Option<FBListChunksResponse<'a>>,
}

impl<'a> FBListChunksResponseRef<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self {
            root: parse_root::<FBListChunksResponse>(buf),
        }
    }
    pub fn valid(&self) -> bool {
        self.root.is_some()
    }
    pub fn ret_code(&self) -> FBChunkdbRetCode {
        self.root.map_or(FBChunkdbRetCode::Internal, |r| r.ret_code())
    }
    pub fn error_msg(&self) -> Option<&'a str> {
        self.root.and_then(|r| r.error_msg())
    }
    pub fn request_id(&self) -> Option<u64> {
        self.root.map(|r| r.id())
    }
    pub fn ok(&self) -> bool {
        self.root.is_some_and(|r| is_ok(r.ret_code()))
    }
    pub fn range_start(&self) -> u32 {
        self.root.map_or(0, |r| r.range_start())
    }
    pub fn range_end(&self) -> u32 {
        self.root.map_or(0, |r| r.range_end())
    }
    pub fn chunks(&self) -> Option<flatbuffers::Vector<'a, flatbuffers::ForwardsUOffset<FBChunk<'a>>>> {
        self.root.and_then(|r| r.chunks())
    }
    pub fn has_next_token(&self) -> bool {
        self.root.is_some_and(|r| r.has_next_token())
    }
    pub fn next_token(&self) -> Option<crate::chunkdb_fb::FBInt128> {
        self.root.and_then(|r| r.next_token()).copied()
    }
}
