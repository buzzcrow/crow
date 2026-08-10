// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Diskdb instance metadata (group-0 sysdata, written as keep-alive).

use serde::{Deserialize, Serialize};

use super::ids::{DiskGroupId, NodeId};

/// Metadata for a diskdb instance, written to group 0 on each sync as a
/// keep-alive. Group 0 uses this to learn the instance is alive and to
/// balance disk-groups across instances.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InstanceMeta {
    pub instance_id: String,
    pub grpc_endpoint: String,
    pub http_endpoint: String,
    pub owned_dg_ids: Vec<(NodeId, DiskGroupId)>,
    pub last_heartbeat_ms: u64,
}
