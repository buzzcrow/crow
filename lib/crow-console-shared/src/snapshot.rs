// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Aggregated cluster snapshot returned to UI/CLI.
//!
//! Mirror types of `crow-kv-server`'s `/topology` response. They are kept in a
//! single place so both the HTTP client and the public API surface use the
//! same shape. Field names match the server's JSON exactly.

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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthInfo {
    pub status: String,
    #[serde(default)]
    pub messages: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoreView {
    pub store_id: u64,
    pub listen_addr: Option<String>,
    pub groups: Vec<GroupView>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupView {
    pub group_id: u64,
    pub local_replica_id: u64,
    pub leader_id: u64,
    pub force_classic: bool,
    pub local_replica: LocalReplicaView,
    pub remotes: Vec<RemoteReplicaView>,
    /// Read-path state gauges; `None` until the group's read-registry
    /// handles are wired.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub read_state: Option<ReadStateSnapshot>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalReplicaView {
    pub id: u64,
    pub role: String,
    pub voting: bool,
    pub kv_store: KvStoreView,
    /// Election/lease state; `None` for replicas without election state.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub election: Option<ElectionStateSnapshot>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KvStoreView {
    pub key_count: u64,
    /// Mirrors `crow_kv`'s `KvStoreStatus::engine_healthy`. `true` for
    /// `InMemKV` always; `false` once a
    /// `CrowTreeEngine`'s durable I/O fault has latched.
    #[serde(default = "default_engine_healthy")]
    pub engine_healthy: bool,
    /// Mirrors `crow_kv`'s `KvStoreStatus::crowtree_stats`; `None` when the
    /// group's engine isn't `CrowTreeEngine` (e.g. `InMemKV`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub crowtree_stats: Option<CrowTreeStatsSnapshot>,
}

fn default_engine_healthy() -> bool {
    true
}

/// Mirrors `crow_kv`'s `CrowTreeStatsView` --
/// batched crow-tree engine diagnostics for a single group's local replica.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct CrowTreeStatsSnapshot {
    pub last_applied_slot: u64,
    pub contiguous_slot: u64,
    pub gc_watermark: u64,
    pub snapshot_pages_written: u64,
    pub snapshot_segments_written: u64,
    pub buffer_pool_hits: u64,
    pub buffer_pool_misses: u64,
    pub buffer_pool_evictions: u64,
    pub buffer_pool_writebacks: u64,
    pub buffer_pool_resident: u32,
    pub buffer_pool_dirty: u32,
    pub buffer_pool_used: u32,
    pub buffer_pool_num_frames: u32,
    pub mt_upsert_total: u64,
    pub mt_get_total: u64,
    pub mt_get_hit_total: u64,
    pub flush_drain_total: u64,
    pub flush_entries_total: u64,
    pub snapshot_total: u64,
    pub l1_get_total: u64,
    pub l1_get_hit_total: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteReplicaView {
    pub id: u64,
    pub endpoint: String,
    pub voting: bool,
    pub metrics: RemoteMetrics,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteMetrics {
    pub rpc_count: u64,
    pub err_count: u64,
    pub last_rtt_ms: u64,
}

/// Mirrors `crow_kv`'s `ElectionStateView` — election/lease state for
/// `/topology` and the GUI.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ElectionStateSnapshot {
    pub election_count: u64,
    pub current_term: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_heartbeat_age_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease_remaining_ms: Option<u64>,
    pub bulk_phase1_in_flight_slots: u64,
    pub step_downs_higher_term: u64,
    pub step_downs_lease_unrenewable: u64,
    pub step_downs_admin: u64,
}

/// Mirrors `crow_kv`'s `ReadStateView` — read-path state gauges.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ReadStateSnapshot {
    pub lease_valid: u64,
    pub contiguous_applied: u64,
    pub safe_slot: u64,
}

/// Wire shape of `crow-kv-server`'s `GET /metrics` response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsResponse {
    pub window_secs: f64,
    pub timestamp: String,
    pub metrics: Vec<MetricPointView>,
}

/// One typed metric point in the `/metrics` response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricPointView {
    pub name: String,
    pub kind: String,
    pub fields: Vec<MetricFieldView>,
}

/// One key/value field on a metric point. `value` is `f64` for uniform
/// JSON handling (counters/gauges/histograms all fit).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricFieldView {
    pub key: String,
    pub value: f64,
}

/// Wire shape of `crow-kv-server`'s `GET /topology`.
#[derive(Debug, Deserialize)]
pub(crate) struct TopologyResponse {
    pub stores: Vec<StoreView>,
}
