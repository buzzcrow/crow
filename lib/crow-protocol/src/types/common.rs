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

//! Hand-written Rust types replacing the prost-generated `crow.common`
//! types. API-compatible with the former proto-generated structs — same
//! field names, types, and derives. No `prost::Message` trait (removed
//! with the gRPC/protobuf cutover).

use serde::{Deserialize, Serialize};

/// Implement `From<Enum> for i32` and `TryFrom<i32> for Enum` for a
/// `#[repr(i32)]` enum, matching prost's generated conversions.
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

/// Hardware status shared by Rack, Node, DiskGroup, and Disk.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, Default)]
#[repr(i32)]
pub enum HwStatus {
    #[default]
    Init = 0,
    Up = 1,
    Maintenance = 2,
    Suspect = 3,
    Missing = 4,
    Bad = 5,
    Offline = 6,
}
impl_enum_conversions!(
    HwStatus,
    Init = 0,
    Up = 1,
    Maintenance = 2,
    Suspect = 3,
    Missing = 4,
    Bad = 5,
    Offline = 6
);

/// Structured error code.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, Default)]
#[repr(i32)]
pub enum ErrorCode {
    #[default]
    Unspecified = 0,
    InvalidArgument = 1,
    NotFound = 2,
    AlreadyExists = 3,
    PermissionDenied = 4,
    Internal = 5,
    Unavailable = 6,
    NoSpace = 10,
    NotOwner = 11,
    DiskNotFound = 12,
    DiskGroupNotFound = 13,
    NodeNotFound = 14,
    RackNotFound = 15,
    DiskBlockFreed = 16,
    Degraded = 17,
    ChunkNotFound = 20,
    ChunkExists = 21,
    ChunkSealed = 22,
    ChunkDeleted = 23,
    ChunkStripNotFound = 24,
    ChunkInactive = 25,
    NotMyRange = 30,
}
impl_enum_conversions!(
    ErrorCode,
    Unspecified = 0,
    InvalidArgument = 1,
    NotFound = 2,
    AlreadyExists = 3,
    PermissionDenied = 4,
    Internal = 5,
    Unavailable = 6,
    NoSpace = 10,
    NotOwner = 11,
    DiskNotFound = 12,
    DiskGroupNotFound = 13,
    NodeNotFound = 14,
    RackNotFound = 15,
    DiskBlockFreed = 16,
    Degraded = 17,
    ChunkNotFound = 20,
    ChunkExists = 21,
    ChunkSealed = 22,
    ChunkDeleted = 23,
    ChunkStripNotFound = 24,
    ChunkInactive = 25,
    NotMyRange = 30
);

/// Sub-range ownership status.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, Default)]
#[repr(i32)]
pub enum RangeStatus {
    #[default]
    Stable = 0,
    InTransition = 1,
}
impl_enum_conversions!(RangeStatus, Stable = 0, InTransition = 1);

/// chunkdb range migration lifecycle.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, Default)]
#[repr(i32)]
pub enum ChunkdbMigrationState {
    #[default]
    NotMigrating = 0,
    Copying = 1,
    Cutover = 2,
    Complete = 3,
}
impl_enum_conversions!(
    ChunkdbMigrationState,
    NotMigrating = 0,
    Copying = 1,
    Cutover = 2,
    Complete = 3
);

// ── Common messages ─────────────────────────────────────────────

/// 128-bit disk identifier, split into two uint64.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
pub struct DiskId {
    pub high: u64,
    pub low: u64,
}

/// 128-bit chunk identifier (owner of allocated blocks).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
pub struct ChunkId {
    pub high: u64,
    pub low: u64,
}

/// Error detail.
#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct ErrorInfo {
    pub code: i32,
    pub message: String,
}

// ── Rack / Node ─────────────────────────────────────────────────

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct RackValue {
    pub status: i32,
    pub node_ids: Vec<u64>,
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct NodeValue {
    pub status: i32,
    pub last_used_dg_id: u64,
    pub disk_group_ids: Vec<u64>,
    pub status_changed_at_ms: u64,
    pub temp_failure_since_ms: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct RackInfo {
    pub rack_id: u64,
    pub status: i32,
    pub node_ids: Vec<u64>,
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct NodeInfo {
    pub rack_id: u64,
    pub node_id: u64,
    pub status: i32,
    pub last_used_dg_id: u64,
    pub disk_group_ids: Vec<u64>,
    pub status_changed_at_ms: u64,
    pub temp_failure_since_ms: Option<u64>,
}

// ── KV-cluster topology records ─────────────────────────────────

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct StoreValue {
    pub store_id: u64,
    pub node_ids: Vec<u64>,
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct GroupValue {
    pub store_id: u64,
    pub group_id: u64,
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct ReplicaValue {
    pub store_id: u64,
    pub group_id: u64,
    pub replica_id: u64,
    pub node_id: u64,
    pub role: String,
    pub voting: bool,
    pub endpoint: String,
}

// ── Service instance registry ───────────────────────────────────

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct InstanceValue {
    pub instance_id: u64,
    pub rpc_endpoint: String,
    pub last_heartbeat_ms: u64,
    pub extra: Option<ServiceExtra>,
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct ServiceExtra {
    pub diskdb: Option<DiskdbExtra>,
    pub kv_server: Option<KvServerExtra>,
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct DiskdbExtra {
    pub owned_dg_ids: Vec<u64>,
    pub group_usages: Vec<DiskGroupUsageSummary>,
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct DiskGroupUsageSummary {
    pub disk_group_id: u64,
    pub capacity_bytes: u64,
    pub used_bytes: u64,
    pub free_bytes: u64,
    pub disk_count: u32,
    pub allocatable_disk_count: u32,
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct KvServerExtra {
    pub hosted_stores: Vec<u64>,
    pub hosted_groups: Vec<HostedGroup>,
    pub health: String,
    pub data_root: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
pub struct HostedGroup {
    pub store_id: u64,
    pub group_id: u64,
}

// ── Per-disk-group maps ─────────────────────────────────────────

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct OwnerMapValue {
    pub instance_id: u64,
    pub lease_expiry_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct BindMapValue {
    pub store_id: u64,
    pub group_id: u64,
}

// ── chunkdb instance range binding ──────────────────────────────

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct ChunkdbRangeBindingValue {
    pub sub_range_index: u32,
    pub range_start: u32,
    pub range_end: u32,
    pub instance_id: u64,
    pub rpc_endpoint: String,
    pub original_instance_id: u64,
    pub original_endpoint: String,
    pub status: i32,
    pub last_change_time_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct ChunkdbRangeMigrationValue {
    pub range_start: u32,
    pub range_end: u32,
    pub old_instance_id: u64,
    pub old_endpoint: String,
    pub new_instance_id: u64,
    pub new_endpoint: String,
    pub state: i32,
}
