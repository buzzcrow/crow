// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Disk-group metadata (group-0 sysdata).

use serde::{Deserialize, Serialize};

use super::ids::{DiskGroupId, DiskUuid, NodeId};
use super::status::Status;

/// Metadata for a disk-group (logical container of disks on one node).
/// The unit of ownership (assigned to one diskdb instance) and the unit
/// of paxos binding (all zone journals for a disk-group live on one
/// paxos data group). Globally identified by `(node_id, dg_id)`.
/// `disk_uuids` is the source of truth for membership.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DiskGroupMeta {
    pub node_id: NodeId,
    pub dg_id: DiskGroupId,
    pub status: Status,
    pub disk_uuids: Vec<DiskUuid>,
}
