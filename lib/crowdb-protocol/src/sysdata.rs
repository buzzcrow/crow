// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Entry return types for group-0 sysdata reads.
//!
//! These are the decoded form of a text-path key + its JSON value,
//! produced by `HardwareClient` / `KVClusterMetaClient` reads. The key
//! fields (parsed from the path) are included alongside the value so
//! callers do not need to re-parse the path.
//!
//! See `doc/design/kv/design-crowdb-kv-group0.md` §3.3.

use crate::common_type::{DiskGroupId, NodeId, RackId};
use crate::diskdb::rpc::DiskGroupValue;

/// A disk-group entry: key fields + the stored `DiskGroupValue`.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct DiskGroupEntry {
    pub rack_id: RackId,
    pub node_id: NodeId,
    pub dg_id: DiskGroupId,
    pub value: DiskGroupValue,
}

/// An ownership-map entry: key fields + the owner instance + lease.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DiskdbOwnerEntry {
    pub rack_id: RackId,
    pub node_id: NodeId,
    pub dg_id: DiskGroupId,
    pub instance_id: u64,
    pub lease_expiry_ms: u64,
}

/// A bind-map entry: key fields + the bound paxos data group.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct KVGroupBindEntry {
    pub rack_id: RackId,
    pub node_id: NodeId,
    pub dg_id: DiskGroupId,
    pub store_id: u64,
    pub group_id: u64,
}
