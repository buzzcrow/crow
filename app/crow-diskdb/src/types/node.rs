// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Node metadata (group-0 sysdata).

use serde::{Deserialize, Serialize};

use super::ids::{DiskGroupId, NodeId};
use super::status::Status;

/// Metadata for a physical node, stored in group 0. `dc_id`/`rack_id`
/// are reserved (v1 ships flat — node list only). `last_used_dg_id` is
/// the auto-increment counter for new disk-group ids within this node.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NodeMeta {
    pub node_id: NodeId,
    pub dc_id: Option<String>,
    pub rack_id: Option<String>,
    pub status: Status,
    pub last_used_dg_id: DiskGroupId,
    pub disk_group_ids: Vec<DiskGroupId>,
    pub status_changed_at_ms: u64,
    pub temp_failure_since_ms: Option<u64>,
}
