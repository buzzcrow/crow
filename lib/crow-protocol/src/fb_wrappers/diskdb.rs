// Copyright 2026-present buzzcrow <buzzcrow@126.com>

#![allow(clippy::must_use_candidate)]

//! Zero-copy `FB<Type>Ref` wrappers for diskdb response types
//! (design-crow-rpc.md §6, R115 retrofit).
//!
//! Each wrapper holds a `&[u8]` reference to the control buffer,
//! parses the root on construction, and exposes typed accessor methods
//! that read through the root pointer — no per-field copy, no owned
//! intermediate struct. Mirrors R117's `kv_client.rs` pattern.

use crate::diskdb_fb::{
    FBAllocateResponse, FBCommitBlocksResponse, FBCompactZoneResponse, FBDiskGroupInfo,
    FBDiskGroupRecalcResult, FBDiskInfo, FBDiskdbRetCode, FBFreeResponse, FBGetDiskGroupInfoResponse,
    FBGetDiskInfoResponse, FBGetScanStatusResponse, FBQueryCapacityStatsResponse,
    FBRebuildZoneBitmapResponse, FBRecalcDiskUsageResponse, FBScanSummary, FBSegment, FBTriggerScanResponse,
    FBZoneCompactionResult,
};

use super::parse_root;

// ── FBAllocateResponseRef ───────────────────────────────────────

/// Zero-copy view over an `FBAllocateResponse` control buffer.
pub struct FBAllocateResponseRef<'a> {
    root: Option<FBAllocateResponse<'a>>,
}

impl<'a> FBAllocateResponseRef<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self {
            root: parse_root::<FBAllocateResponse>(buf),
        }
    }
    pub fn valid(&self) -> bool {
        self.root.is_some()
    }
    pub fn ret_code(&self) -> FBDiskdbRetCode {
        self.root.map_or(FBDiskdbRetCode::Internal, |r| r.ret_code())
    }
    pub fn error_msg(&self) -> Option<&'a str> {
        self.root.and_then(|r| r.error_msg())
    }
    pub fn request_id(&self) -> Option<u64> {
        self.root.map(|r| r.id())
    }
    pub fn segments(&self) -> Option<flatbuffers::Vector<'a, FBSegment>> {
        self.root.and_then(|r| r.segments())
    }
}

// ── FBFreeResponseRef ───────────────────────────────────────────

/// Zero-copy view over an `FBFreeResponse` control buffer.
pub struct FBFreeResponseRef<'a> {
    root: Option<FBFreeResponse<'a>>,
}

impl<'a> FBFreeResponseRef<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self {
            root: parse_root::<FBFreeResponse>(buf),
        }
    }
    pub fn valid(&self) -> bool {
        self.root.is_some()
    }
    pub fn ret_code(&self) -> FBDiskdbRetCode {
        self.root.map_or(FBDiskdbRetCode::Internal, |r| r.ret_code())
    }
    pub fn error_msg(&self) -> Option<&'a str> {
        self.root.and_then(|r| r.error_msg())
    }
    pub fn request_id(&self) -> Option<u64> {
        self.root.map(|r| r.id())
    }
    pub fn freed_count(&self) -> u32 {
        self.root.map_or(0, |r| r.freed_count())
    }
}

// ── FBCommitBlocksResponseRef ───────────────────────────────────

/// Zero-copy view over an `FBCommitBlocksResponse` control buffer.
pub struct FBCommitBlocksResponseRef<'a> {
    root: Option<FBCommitBlocksResponse<'a>>,
}

impl<'a> FBCommitBlocksResponseRef<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self {
            root: parse_root::<FBCommitBlocksResponse>(buf),
        }
    }
    pub fn valid(&self) -> bool {
        self.root.is_some()
    }
    pub fn ret_code(&self) -> FBDiskdbRetCode {
        self.root.map_or(FBDiskdbRetCode::Internal, |r| r.ret_code())
    }
    pub fn error_msg(&self) -> Option<&'a str> {
        self.root.and_then(|r| r.error_msg())
    }
    pub fn request_id(&self) -> Option<u64> {
        self.root.map(|r| r.id())
    }
    pub fn committed_count(&self) -> u32 {
        self.root.map_or(0, |r| r.committed_count())
    }
}

// ── FBQueryCapacityStatsResponseRef ─────────────────────────────

/// Zero-copy view over an `FBQueryCapacityStatsResponse` control buffer.
pub struct FBQueryCapacityStatsResponseRef<'a> {
    root: Option<FBQueryCapacityStatsResponse<'a>>,
}

impl<'a> FBQueryCapacityStatsResponseRef<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self {
            root: parse_root::<FBQueryCapacityStatsResponse>(buf),
        }
    }
    pub fn valid(&self) -> bool {
        self.root.is_some()
    }
    pub fn ret_code(&self) -> FBDiskdbRetCode {
        self.root.map_or(FBDiskdbRetCode::Internal, |r| r.ret_code())
    }
    pub fn error_msg(&self) -> Option<&'a str> {
        self.root.and_then(|r| r.error_msg())
    }
    pub fn request_id(&self) -> Option<u64> {
        self.root.map(|r| r.id())
    }
    pub fn disk_groups(
        &self,
    ) -> Option<flatbuffers::Vector<'a, flatbuffers::ForwardsUOffset<FBDiskGroupInfo<'a>>>> {
        self.root.and_then(|r| r.disk_groups())
    }
}

// ── FBGetDiskGroupInfoResponseRef ───────────────────────────────

/// Zero-copy view over an `FBGetDiskGroupInfoResponse` control buffer.
pub struct FBGetDiskGroupInfoResponseRef<'a> {
    root: Option<FBGetDiskGroupInfoResponse<'a>>,
}

impl<'a> FBGetDiskGroupInfoResponseRef<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self {
            root: parse_root::<FBGetDiskGroupInfoResponse>(buf),
        }
    }
    pub fn valid(&self) -> bool {
        self.root.is_some()
    }
    pub fn ret_code(&self) -> FBDiskdbRetCode {
        self.root.map_or(FBDiskdbRetCode::Internal, |r| r.ret_code())
    }
    pub fn error_msg(&self) -> Option<&'a str> {
        self.root.and_then(|r| r.error_msg())
    }
    pub fn request_id(&self) -> Option<u64> {
        self.root.map(|r| r.id())
    }
    pub fn group(&self) -> Option<FBDiskGroupInfo<'a>> {
        self.root.and_then(|r| r.group())
    }
}

// ── FBGetDiskInfoResponseRef ────────────────────────────────────

/// Zero-copy view over an `FBGetDiskInfoResponse` control buffer.
pub struct FBGetDiskInfoResponseRef<'a> {
    root: Option<FBGetDiskInfoResponse<'a>>,
}

impl<'a> FBGetDiskInfoResponseRef<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self {
            root: parse_root::<FBGetDiskInfoResponse>(buf),
        }
    }
    pub fn valid(&self) -> bool {
        self.root.is_some()
    }
    pub fn ret_code(&self) -> FBDiskdbRetCode {
        self.root.map_or(FBDiskdbRetCode::Internal, |r| r.ret_code())
    }
    pub fn error_msg(&self) -> Option<&'a str> {
        self.root.and_then(|r| r.error_msg())
    }
    pub fn request_id(&self) -> Option<u64> {
        self.root.map(|r| r.id())
    }
    pub fn disk(&self) -> Option<FBDiskInfo<'a>> {
        self.root.and_then(|r| r.disk())
    }
}

// ── FBRebuildZoneBitmapResponseRef ──────────────────────────────

/// Zero-copy view over an `FBRebuildZoneBitmapResponse` control buffer.
pub struct FBRebuildZoneBitmapResponseRef<'a> {
    root: Option<FBRebuildZoneBitmapResponse<'a>>,
}

impl<'a> FBRebuildZoneBitmapResponseRef<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self {
            root: parse_root::<FBRebuildZoneBitmapResponse>(buf),
        }
    }
    pub fn valid(&self) -> bool {
        self.root.is_some()
    }
    pub fn ret_code(&self) -> FBDiskdbRetCode {
        self.root.map_or(FBDiskdbRetCode::Internal, |r| r.ret_code())
    }
    pub fn error_msg(&self) -> Option<&'a str> {
        self.root.and_then(|r| r.error_msg())
    }
    pub fn request_id(&self) -> Option<u64> {
        self.root.map(|r| r.id())
    }
    pub fn rebuilt_zone_count(&self) -> u32 {
        self.root.map_or(0, |r| r.rebuilt_zone_count())
    }
    pub fn total_busy_units(&self) -> u64 {
        self.root.map_or(0, |r| r.total_busy_units())
    }
    pub fn total_free_units(&self) -> u64 {
        self.root.map_or(0, |r| r.total_free_units())
    }
}

// ── FBRecalcDiskUsageResponseRef ────────────────────────────────

/// Zero-copy view over an `FBRecalcDiskUsageResponse` control buffer.
pub struct FBRecalcDiskUsageResponseRef<'a> {
    root: Option<FBRecalcDiskUsageResponse<'a>>,
}

impl<'a> FBRecalcDiskUsageResponseRef<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self {
            root: parse_root::<FBRecalcDiskUsageResponse>(buf),
        }
    }
    pub fn valid(&self) -> bool {
        self.root.is_some()
    }
    pub fn ret_code(&self) -> FBDiskdbRetCode {
        self.root.map_or(FBDiskdbRetCode::Internal, |r| r.ret_code())
    }
    pub fn error_msg(&self) -> Option<&'a str> {
        self.root.and_then(|r| r.error_msg())
    }
    pub fn request_id(&self) -> Option<u64> {
        self.root.map(|r| r.id())
    }
    pub fn results(
        &self,
    ) -> Option<flatbuffers::Vector<'a, flatbuffers::ForwardsUOffset<FBDiskGroupRecalcResult<'a>>>> {
        self.root.and_then(|r| r.results())
    }
}

// ── FBCompactZoneResponseRef ────────────────────────────────────

/// Zero-copy view over an `FBCompactZoneResponse` control buffer.
pub struct FBCompactZoneResponseRef<'a> {
    root: Option<FBCompactZoneResponse<'a>>,
}

impl<'a> FBCompactZoneResponseRef<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self {
            root: parse_root::<FBCompactZoneResponse>(buf),
        }
    }
    pub fn valid(&self) -> bool {
        self.root.is_some()
    }
    pub fn ret_code(&self) -> FBDiskdbRetCode {
        self.root.map_or(FBDiskdbRetCode::Internal, |r| r.ret_code())
    }
    pub fn error_msg(&self) -> Option<&'a str> {
        self.root.and_then(|r| r.error_msg())
    }
    pub fn request_id(&self) -> Option<u64> {
        self.root.map(|r| r.id())
    }
    pub fn compacted_zone_count(&self) -> u32 {
        self.root.map_or(0, |r| r.compacted_zone_count())
    }
    pub fn total_free_records_deleted(&self) -> u32 {
        self.root.map_or(0, |r| r.total_free_records_deleted())
    }
    pub fn zones(
        &self,
    ) -> Option<flatbuffers::Vector<'a, flatbuffers::ForwardsUOffset<FBZoneCompactionResult<'a>>>> {
        self.root.and_then(|r| r.zones())
    }
}

// ── FBTriggerScanResponseRef ────────────────────────────────────

/// Zero-copy view over an `FBTriggerScanResponse` control buffer.
pub struct FBTriggerScanResponseRef<'a> {
    root: Option<FBTriggerScanResponse<'a>>,
}

impl<'a> FBTriggerScanResponseRef<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self {
            root: parse_root::<FBTriggerScanResponse>(buf),
        }
    }
    pub fn valid(&self) -> bool {
        self.root.is_some()
    }
    pub fn ret_code(&self) -> FBDiskdbRetCode {
        self.root.map_or(FBDiskdbRetCode::Internal, |r| r.ret_code())
    }
    pub fn error_msg(&self) -> Option<&'a str> {
        self.root.and_then(|r| r.error_msg())
    }
    pub fn request_id(&self) -> Option<u64> {
        self.root.map(|r| r.id())
    }
    pub fn summary(&self) -> Option<FBScanSummary<'a>> {
        self.root.and_then(|r| r.summary())
    }
    pub fn scan_in_progress(&self) -> bool {
        self.root.is_some_and(|r| r.scan_in_progress())
    }
}

// ── FBGetScanStatusResponseRef ──────────────────────────────────

/// Zero-copy view over an `FBGetScanStatusResponse` control buffer.
pub struct FBGetScanStatusResponseRef<'a> {
    root: Option<FBGetScanStatusResponse<'a>>,
}

impl<'a> FBGetScanStatusResponseRef<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self {
            root: parse_root::<FBGetScanStatusResponse>(buf),
        }
    }
    pub fn valid(&self) -> bool {
        self.root.is_some()
    }
    pub fn ret_code(&self) -> FBDiskdbRetCode {
        self.root.map_or(FBDiskdbRetCode::Internal, |r| r.ret_code())
    }
    pub fn error_msg(&self) -> Option<&'a str> {
        self.root.and_then(|r| r.error_msg())
    }
    pub fn request_id(&self) -> Option<u64> {
        self.root.map(|r| r.id())
    }
    pub fn summary(&self) -> Option<FBScanSummary<'a>> {
        self.root.and_then(|r| r.summary())
    }
    pub fn has_run(&self) -> bool {
        self.root.is_some_and(|r| r.has_run())
    }
}
