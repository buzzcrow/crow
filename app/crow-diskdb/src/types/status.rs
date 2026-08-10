// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Operational status shared by `Node`, `DiskGroup`, and `Disk`.

use serde::{Deserialize, Serialize};

/// Operator-facing operational status, shared by `Node`, `DiskGroup`, and
/// `Disk`. Ordered by restrictiveness: `Online` is least restrictive,
/// `Offline` is most. The effective status for a disk is
/// `max(node, group, disk)`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
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

/// Compute the effective status for a disk: the most restrictive
/// (highest-priority) of node, group, and disk status.
#[must_use]
pub fn effective_status(node: Status, group: Status, disk: Status) -> Status {
    node.max(group).max(disk)
}
