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

//! Hand-written Rust types replacing the prost-generated `crow.diskdb.rpc`
//! types. API-compatible with the former proto-generated structs.

use serde::{Deserialize, Serialize};

use crate::common::{ChunkId, DiskId};

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
pub enum DiskType {
    #[default]
    BlockHdd = 0,
    BlockSsd = 1,
    ZoneSsd = 2,
    SmrHdd = 3,
}
impl_enum_conversions!(DiskType, BlockHdd = 0, BlockSsd = 1, ZoneSsd = 2, SmrHdd = 3);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, Default)]
#[repr(i32)]
pub enum ZoneAllocationState {
    #[default]
    ZoneAllocActive = 0,
    ZoneAllocAvailable = 1,
    ZoneAllocFull = 2,
}
impl_enum_conversions!(
    ZoneAllocationState,
    ZoneAllocActive = 0,
    ZoneAllocAvailable = 1,
    ZoneAllocFull = 2
);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, Default)]
#[repr(i32)]
pub enum BlockState {
    #[default]
    Ok = 0,
    Suspect = 1,
    Corrupt = 2,
}
impl_enum_conversions!(BlockState, Ok = 0, Suspect = 1, Corrupt = 2);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, Default)]
#[repr(i32)]
pub enum CommitState {
    #[default]
    Tentative = 0,
    Committed = 1,
}
impl_enum_conversions!(CommitState, Tentative = 0, Committed = 1);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, Default)]
#[repr(i32)]
pub enum RecoveryScanStatus {
    #[default]
    RecoveryScanInProgress = 0,
    RecoveryScanStopped = 1,
    RecoveryScanComplete = 2,
}
impl_enum_conversions!(
    RecoveryScanStatus,
    RecoveryScanInProgress = 0,
    RecoveryScanStopped = 1,
    RecoveryScanComplete = 2
);

// ── Value types ─────────────────────────────────────────────────

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct ZoneValue {
    pub usage_bitmap: Vec<u8>,
    pub snapshot_slot: u64,
    pub crc32: u32,
    pub compact_ts: u64,
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct DiskValue {
    pub disk_type: i32,
    pub capacity_units: u64,
    pub zone_size_units: u64,
    pub unit_size_bytes: u32,
    pub zone_count: u32,
    pub status: i32,
    pub device_path: String,
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct DiskGroupValue {
    pub status: i32,
    pub disk_ids: Vec<DiskId>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
pub struct Segment {
    pub disk_id: Option<DiskId>,
    pub zone_index: u32,
    pub unit_offset: u64,
    pub unit_count: u32,
    pub owner_chunk: Option<ChunkId>,
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct BusyBlockValue {
    pub unit_count: u32,
    pub owner_chunk: Option<ChunkId>,
    pub unit_size: u32,
    pub state: i32,
    pub commit_state: i32,
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct FreeBlockValue {
    pub unit_count: u32,
    pub previous_owner: Option<ChunkId>,
    pub freed_ts: u64,
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct ZoneUsage {
    pub zone_index: u32,
    pub capacity_bytes: u64,
    pub busy_bytes: u64,
    pub free_bytes: u64,
    pub busy_block_count: u32,
    pub free_block_count: u32,
    pub alloc_state: i32,
    pub usage_bitmap: Option<Vec<u8>>,
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct DiskInfo {
    pub rack_id: u64,
    pub node_id: u64,
    pub disk_group_id: u64,
    pub disk_id: Option<DiskId>,
    pub disk_type: i32,
    pub capacity_units: u64,
    pub zone_size_units: u64,
    pub unit_size_bytes: u32,
    pub zone_count: u32,
    pub status: i32,
    pub busy_units: u64,
    pub free_units: u64,
    pub capacity_bytes: u64,
    pub busy_bytes: u64,
    pub free_bytes: u64,
    pub active_zone_count: u32,
    pub zone_usages: Vec<ZoneUsage>,
    pub device_path: String,
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct RecoveryScanProgressValue {
    pub status: i32,
    pub last_completed_zone: u32,
    pub impacted_blocks_count: u64,
    pub started_at_ms: u64,
    pub updated_at_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct DiskGroupInfo {
    pub rack_id: u64,
    pub node_id: u64,
    pub disk_group_id: u64,
    pub status: i32,
    pub disk_ids: Vec<DiskId>,
    pub disks: Vec<DiskInfo>,
    pub capacity_bytes: u64,
    pub busy_bytes: u64,
    pub free_bytes: u64,
    pub allocatable_disk_count: u32,
}

// ── RPC request/response types ──────────────────────────────────

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct AllocateBlocksRequest {
    pub disk_group_id: u64,
    pub unit_count: u32,
    pub count: u32,
    pub exclude_disk_ids: Vec<DiskId>,
    pub owner_chunk: Option<ChunkId>,
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct AllocateResponse {
    pub segments: Vec<Segment>,
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct FreeBlocksRequest {
    pub segments: Vec<Segment>,
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct FreeResponse {
    pub freed_count: u32,
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct CommitBlocksRequest {
    pub segments: Vec<Segment>,
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct CommitBlocksResponse {
    pub committed_count: u32,
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct QueryCapacityStatsRequest {
    pub disk_group_id: u64,
    pub disk_id: Option<DiskId>,
    pub zone_index: Option<u32>,
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct QueryCapacityStatsResponse {
    pub disk_groups: Vec<DiskGroupInfo>,
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct GetDiskGroupInfoRequest {
    pub disk_group_id: u64,
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct GetDiskGroupInfoResponse {
    pub group: Option<DiskGroupInfo>,
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct GetDiskInfoRequest {
    pub rack_id: u64,
    pub node_id: u64,
    pub disk_group_id: u64,
    pub disk_id: Option<DiskId>,
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct GetDiskInfoResponse {
    pub disk: Option<DiskInfo>,
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct RebuildZoneBitmapRequest {
    pub disk_id: Option<DiskId>,
    pub zone_index: u32,
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct RebuildZoneBitmapResponse {
    pub rebuilt_zone_count: u32,
    pub total_busy_units: u64,
    pub total_free_units: u64,
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct RecalcDiskUsageRequest {
    pub disk_group_id: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct RecalcDiskUsageResponse {
    pub results: Vec<DiskGroupRecalcResult>,
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct DiskGroupRecalcResult {
    pub disk_group_id: u64,
    pub drift_detected: bool,
    pub zones: Vec<ZoneRecalcResult>,
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct ZoneRecalcResult {
    pub disk_id: Option<DiskId>,
    pub zone_index: u32,
    pub matches: bool,
    pub drift_detected: bool,
    pub live_busy_blocks: u32,
    pub replayed_busy_blocks: u32,
    pub live_snapshot_slot: u64,
    pub replayed_snapshot_slot: u64,
    pub fallback_reason: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct CompactZoneRequest {
    pub disk_id: Option<DiskId>,
    pub zone_indices: Vec<u32>,
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct CompactZoneResponse {
    pub compacted_zone_count: u32,
    pub total_free_records_deleted: u32,
    pub zones: Vec<ZoneCompactionResult>,
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct ZoneCompactionResult {
    pub zone_index: u32,
    pub success: bool,
    pub free_records_deleted: u32,
    pub error: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct TriggerScanRequest {}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct TriggerScanResponse {
    pub summary: Option<ScanSummary>,
    pub scan_in_progress: bool,
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct GetScanStatusRequest {}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct GetScanStatusResponse {
    pub summary: Option<ScanSummary>,
    pub has_run: bool,
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct ScanSummary {
    pub started_at_ms: u64,
    pub duration_ms: u64,
    pub zones_scanned: u64,
    pub zones_skipped_active: u64,
    pub zones_skipped_compacting: u64,
    pub ghost_busy: u64,
    pub ghost_free: u64,
    pub uncompacted_lag: u64,
    pub corrupt_snapshots: u64,
    pub corrupt_records: u64,
    pub owner_mismatches: u64,
    pub leak_status: String,
}
