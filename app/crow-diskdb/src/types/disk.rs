// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Disk metadata (group-0 sysdata).

use serde::{Deserialize, Serialize};

use super::disk_state::DiskType;
use super::ids::{DiskUuid, NodeId};
use super::status::Status;

/// Metadata for a physical disk, stored in group 0. `disk_state` (probed
/// locally) is NOT stored here — each diskdb instance probes its own
/// disks. `disk_group_id` is not duplicated here; membership is tracked
/// in `DiskGroupMeta.disk_uuids`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DiskMeta {
    pub disk_uuid: DiskUuid,
    pub node_id: NodeId,
    pub disk_type: DiskType,
    pub capacity_bytes: u64,
    pub zone_size_bytes: u64,
    pub block_size_bytes: u32,
    pub zone_count: u32,
    pub status: Status,
}
