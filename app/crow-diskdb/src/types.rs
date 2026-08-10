// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Core types for diskdb.
//!
//! Skeleton — filled in by follow-up requirements (R70 types, R72
//! zone allocator, etc.).

use serde::{Deserialize, Serialize};

/// Disk-group / node identifier: `"{node_uuid}-{index}"`.
pub type DiskGroupId = String;

/// Disk identifier (UUID string).
pub type DiskUuid = String;

/// Status shared by nodes, disk-groups, and disks.
/// Ordered by restrictiveness: Offline is the most restrictive.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Status {
    Online = 0,
    Init = 1,
    Maintenance = 2,
    TempFailure = 3,
    Offline = 4,
}

impl Status {
    /// Whether allocations are allowed at this status.
    #[must_use]
    pub fn allows_allocate(&self) -> bool {
        matches!(self, Self::Online)
    }

    /// Whether frees are allowed at this status.
    #[must_use]
    pub fn allows_free(&self) -> bool {
        matches!(self, Self::Online | Self::Maintenance | Self::TempFailure)
    }
}

/// Zone hardware health state.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ZoneState {
    Healthy,
    Missing,
    Bad,
}

/// Zone allocation lifecycle state.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ZoneAllocationState {
    Active,
    Busy,
    Error,
    Full,
}

/// Disk type.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DiskType {
    Hdd,
    Ssd,
    Smr,
}
