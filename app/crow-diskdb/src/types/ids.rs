// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Identity and allocation-handle types.

use std::fmt;

use serde::{Deserialize, Serialize};

/// Node identifier (integer, assigned by the cluster).
pub type NodeId = u64;

/// Disk-group identifier (integer, unique within a node).
/// A disk-group is globally identified by the pair `(NodeId, DiskGroupId)`.
pub type DiskGroupId = u32;

/// 128-bit disk UUID, split into two `u64` for compact storage.
/// Globally unique. Formatted as `"{high:016x}-{low:016x}"` for display
/// and `"{high:016x}{low:016x}"` (32 hex chars, no dash) for KV keys.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DiskUuid {
    pub high: u64,
    pub low: u64,
}

impl DiskUuid {
    #[must_use]
    pub const fn new(high: u64, low: u64) -> Self {
        Self { high, low }
    }

    /// Compact key component: 32 hex chars, no dash.
    #[must_use]
    pub fn to_key_component(&self) -> String {
        format!("{:016x}{:016x}", self.high, self.low)
    }
}

impl fmt::Display for DiskUuid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:016x}-{:016x}", self.high, self.low)
    }
}

/// Handle returned to callers on allocation.
/// Contains all information needed to locate and free the block.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Segment {
    pub node_id: NodeId,
    pub disk_group_id: DiskGroupId,
    pub disk_uuid: DiskUuid,
    pub zone_index: u32,
    /// Byte offset within the zone (aligned to block granularity).
    pub zone_offset: u64,
    /// Size of the allocation in bytes (aligned to block granularity).
    pub size: u32,
    /// Nanosecond timestamp when this segment was created (debugging/tracking).
    pub tag: u64,
}

/// Snapshot of zone state before a claim, used for rollback.
#[derive(Clone, Copy, Debug)]
pub struct ClaimSnapshot {
    pub prev_pos: u32,
    pub count: u32,
}
