// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Aggregated cluster snapshot returned to UI/CLI.
//!
//! The `/topology`, `/health`, and `/metrics` wire types live in
//! `crow_protocol::mgmt` (the single home for cross-component protocol
//! types). This module re-exports them under the console's traditional
//! names and adds `ClusterSnapshot` / `ServerSnapshot` — console-
//! internal aggregation wrappers that carry one entry per polled
//! server (including failed polls).

pub use crow_protocol::mgmt::{
    CrowTreeStatsView as CrowTreeStatsSnapshot, ElectionStateView as ElectionStateSnapshot,
    HealthResponse as HealthInfo, KvStoreStatus as KvStoreView, MetricField as MetricFieldView,
    MetricPoint as MetricPointView, MetricsResponse, MetricsSnapshot as RemoteMetrics,
    ReadStateView as ReadStateSnapshot, RemoteStatus as RemoteReplicaView, ReplicaStatus as LocalReplicaView,
    StoreStatus as StoreView, TopologyResponse,
};

// `GroupView` and `GroupStatus` have the same wire shape but the
// console's `cluster::GroupView` is a *different* logical type (with
// `replicas`, `state`, etc.). The wire type is re-exported here as
// `GroupView` for `snapshot.rs` callers that deserialize `/topology`;
// the logical type lives in `cluster.rs`.
pub use crow_protocol::mgmt::GroupStatus as GroupView;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ClusterSnapshot {
    /// One entry per server polled (in input order). Failed polls still
    /// produce an entry with `error` populated.
    pub servers: Vec<ServerSnapshot>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerSnapshot {
    /// Console-side identifier (the URL the user pointed at).
    pub mgmt_url: String,
    /// Health summary; `None` if `/health` failed.
    pub health: Option<HealthInfo>,
    /// Topology stores; empty if `/topology` failed.
    #[serde(default)]
    pub stores: Vec<StoreView>,
    /// Populated only when polling failed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}
