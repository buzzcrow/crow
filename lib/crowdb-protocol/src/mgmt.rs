// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! HTTP management API request/response types for `crowdb-kv-server`.
//!
//! These are the wire shapes for the kv-server's internal HTTP mgmt
//! API (lifecycle endpoints, runtime state export). They live in
//! `crowdb-protocol` (the single home for cross-component protocol
//! types) so that `crowdb-kv-client`'s `KVClusterAdmin`,
//! `crowdb-console-shared`, `crowdb-web`, and `crowdb-cli` all import from
//! one place.
//!
//! See `doc/design/kv/design-crowdb-kv-server.md` §2.4 for the endpoint
//! list and `doc/design/kv/design-crowdb-kv-group0.md` §2.2 for the
//! "internal API" decision.

#![allow(clippy::struct_field_names)]

use serde::{Deserialize, Serialize};

// ── Add group initial role ──────────────────────────────────────

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(utoipa::ToSchema))]
#[serde(rename_all = "snake_case")]
pub enum AddGroupInitialRole {
    Leader,
    Follower,
}

// ── Store lifecycle ─────────────────────────────────────────────

/// `POST /stores` body.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(utoipa::ToSchema))]
pub struct AddStoreRequest {
    pub store_id: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,
}

/// `GET /stores` response wrapper.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(utoipa::ToSchema))]
pub struct StoreListResponse {
    #[serde(default)]
    pub stores: Vec<StoreSummary>,
}

/// `GET /stores` item.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(utoipa::ToSchema))]
pub struct StoreSummary {
    pub store_id: u64,
    #[serde(default)]
    pub listen_addr: Option<String>,
    pub group_count: usize,
}

/// `GET /stores/{sid}` response.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(utoipa::ToSchema))]
pub struct StoreDetail {
    pub store_id: u64,
    #[serde(default)]
    pub listen_addr: Option<String>,
    #[serde(default)]
    pub groups: Vec<GroupSummary>,
}

// ── Group lifecycle ─────────────────────────────────────────────

/// `POST /stores/{sid}/groups` body.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(utoipa::ToSchema))]
pub struct AddGroupRequest {
    pub group_id: u64,
    pub replica_id: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub initial_role: Option<AddGroupInitialRole>,
    /// When `Some(false)`, the server adds the group without starting its
    /// election driver, so it cannot self-elect at `quorum == 1` before its
    /// remotes are wired. Used for multi-replica
    /// restore / creation; the subsequent remote-wiring rebuild starts the
    /// driver with a correct quorum. `None` keeps the default (start driver).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_election: Option<bool>,
}

/// `GET /stores/{sid}/groups` item.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(utoipa::ToSchema))]
pub struct GroupSummary {
    pub group_id: u64,
    pub local_replica_id: u64,
    pub leader_id: u64,
    pub remote_count: usize,
}

// ── Remote replica lifecycle ────────────────────────────────────

/// One element of `POST /stores/{sid}/groups/{gid}/remotes` body and
/// the `GET` response. `endpoint` is the `host:port` of the remote
/// replica's crowdb-rpc service.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(feature = "schema", derive(utoipa::ToSchema))]
pub struct RemoteReplicaInfo {
    pub replica_id: u64,
    pub endpoint: String,
    /// Whether this remote counts toward quorum. Defaults to `true`
    /// for backward compatibility. A newly-joined member is typically
    /// wired as `false` on its peers until it catches up via snapshot
    /// join, then promoted with a follow-up call that re-adds it as
    /// `true`.
    #[serde(default = "default_voting_true")]
    pub voting: bool,
}

fn default_voting_true() -> bool {
    true
}

/// `GET /stores/{sid}/groups/{gid}/remotes` response wrapper.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(utoipa::ToSchema))]
pub struct RemoteListResponse {
    #[serde(default)]
    pub remotes: Vec<RemoteReplicaInfo>,
}

// ── Step-down ───────────────────────────────────────────────────

/// `POST /stores/{sid}/groups/{gid}/step-down` body.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(utoipa::ToSchema))]
pub struct StepDownRequest {
    #[serde(default)]
    pub reason: String,
}

/// `POST /stores/{sid}/groups/{gid}/step-down` response.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(utoipa::ToSchema))]
pub struct StepDownResult {
    /// `false` when the target node was not leader (no-op fence miss).
    pub accepted: bool,
    pub current_term: u64,
    pub current_leader_id: u64,
}

// ── Wipe user data ──────────────────────────────────────────────

/// `POST /stores/{sid}/groups/{gid}/wipe-user-data` response.
///
/// `accepted` is `false` when the target replica had no WAL wired
/// (not yet bootstrapped) — a no-op, not an error. `true` means the
/// WAL + engine user data for the group was dropped and recreated on
/// this node; group0 sysdata + store/group/replica topology are
/// preserved.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(utoipa::ToSchema))]
pub struct WipeResult {
    pub store_id: u64,
    pub group_id: u64,
    pub accepted: bool,
}

// ── System init ─────────────────────────────────────────────────

/// `POST /system/init` body.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(utoipa::ToSchema))]
pub struct SystemInitRequest {
    #[serde(default = "default_replica_id")]
    pub replica_id: u64,
    #[serde(default = "default_start_election_true")]
    pub start_election: bool,
}

fn default_replica_id() -> u64 {
    1
}

fn default_start_election_true() -> bool {
    true
}

/// `POST /system/init` response.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(utoipa::ToSchema))]
pub struct SystemInitResponse {
    pub store_id: u64,
    pub group_id: u64,
    pub replica_id: u64,
    #[serde(default)]
    pub listen_addr: Option<String>,
}

// ── Topology / health / metrics runtime state ───────────────────
//
// These types are the wire shapes for `GET /topology`, `GET /health`,
// and `GET /metrics`. They are cross-component (consumed by
// `crowdb-kv-client`, `crowdb-console-shared`, `crowdb-web`, `crowdb-cli`)
// and therefore live here per `design-crowdb-kv-group0.md` §2.4.

/// Severity of a layer's runtime status. Serializes as a lowercase
/// string (`"ok"`, `"degraded"`, `"unhealthy"`).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(utoipa::ToSchema))]
#[serde(rename_all = "snake_case")]
pub enum StatusLevel {
    #[default]
    Ok,
    Degraded,
    Unhealthy,
}

impl StatusLevel {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Degraded => "degraded",
            Self::Unhealthy => "unhealthy",
        }
    }

    #[must_use]
    pub fn worst(a: Self, b: Self) -> Self {
        use StatusLevel::{Degraded, Ok, Unhealthy};
        match (a, b) {
            (Unhealthy, _) | (_, Unhealthy) => Unhealthy,
            (Degraded, _) | (_, Degraded) => Degraded,
            (Ok, Ok) => Ok,
        }
    }
}

/// Point-in-time read of `LayerMetrics`. Pure data; trivially serializable.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(utoipa::ToSchema))]
pub struct MetricsSnapshot {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rpc_count: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub err_count: Option<u64>,
}

/// `GET /topology` response wrapper.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(utoipa::ToSchema))]
pub struct TopologyResponse {
    pub stores: Vec<StoreStatus>,
}

/// `GET /topology` item — one hosted store's runtime state.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(utoipa::ToSchema))]
pub struct StoreStatus {
    pub store_id: u64,
    pub listen_addr: Option<String>,
    #[serde(default)]
    pub status: StatusLevel,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub messages: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub groups: Vec<GroupStatus>,
}

/// One group within a `StoreStatus`.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(utoipa::ToSchema))]
pub struct GroupStatus {
    pub group_id: u64,
    pub leader_id: u64,
    pub local_replica_id: u64,
    pub force_classic: bool,
    #[serde(default)]
    pub status: StatusLevel,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub messages: Vec<String>,
    pub local_replica: ReplicaStatus,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub remotes: Vec<RemoteStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inflight: Option<InflightStatus>,
    /// Read-path state gauges (lease validity, contiguous applied, safe
    /// slot). `None` until the group's read-registry handles are wired.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub read_state: Option<ReadStateView>,
}

/// Inflight admission status snapshot.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(utoipa::ToSchema))]
pub struct InflightStatus {
    pub window: usize,
    pub policy: String,
    pub occupied: u64,
    pub waiting: u64,
    pub total_enqueued: u64,
    pub total_wait_us: u64,
}

/// Election/lease state snapshot for `/topology` and the GUI.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(utoipa::ToSchema))]
pub struct ElectionStateView {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub election_count: Option<u64>,
    pub current_term: u64,
    /// Milliseconds since the most recent heartbeat. `None` before the
    /// first heartbeat has been observed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_heartbeat_age_ms: Option<u64>,
    /// Remaining lease window in milliseconds (leader only). `None`
    /// when the lease has expired or this replica is not the leader.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lease_remaining_ms: Option<u64>,
    /// Number of slots currently being repaired by bulk Phase 1.
    pub bulk_phase1_in_flight_slots: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub step_downs_higher_term: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub step_downs_lease_unrenewable: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub step_downs_admin: Option<u64>,
}

/// Read-path state gauges for `/topology` and the GUI.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(utoipa::ToSchema))]
pub struct ReadStateView {
    /// 1 if the leader's read lease is valid, 0 otherwise.
    pub lease_valid: u64,
    /// Current `contiguous_applied` on the local replica.
    pub contiguous_applied: u64,
    /// Current group safe slot.
    pub safe_slot: u64,
}

/// One replica within a `GroupStatus`.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(utoipa::ToSchema))]
pub struct ReplicaStatus {
    pub id: u64,
    pub role: String,
    pub voting: bool,
    #[serde(default)]
    pub status: StatusLevel,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub messages: Vec<String>,
    pub kv_store: KvStoreStatus,
    /// Election/lease state (term, election count, step-downs, heartbeat
    /// age, lease remaining). `None` for replicas without election state
    /// (e.g. a remote placeholder).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub election: Option<ElectionStateView>,
}

/// KV store state within a `ReplicaStatus`.
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(utoipa::ToSchema))]
pub struct KvStoreStatus {
    /// `true` for `InMemKV` always; for a `CrowdbTreeEngine`, `false`
    /// once a durable I/O fault has latched.
    #[serde(default = "default_true")]
    pub engine_healthy: bool,
    /// `CrowdbTreeEngine` stats, or `None` for `InMemKV`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub crowtree_stats: Option<CrowdbTreeStatsView>,
}

fn default_true() -> bool {
    true
}

/// Wire-serializable mirror of `CrowdbTreeStats` (that type lives in
/// `crowdb_tree_ffi` and isn't `Serialize`), for `/topology`/`/health`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(utoipa::ToSchema))]
pub struct CrowdbTreeStatsView {
    pub last_applied_slot: u64,
    pub contiguous_slot: u64,
    pub gc_watermark: u64,
    pub snapshot_pages_written: u64,
    pub snapshot_pages_total: u64,
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

/// One remote replica within a `GroupStatus`.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(utoipa::ToSchema))]
pub struct RemoteStatus {
    pub id: u64,
    pub endpoint: String,
    pub voting: bool,
    pub status: StatusLevel,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub messages: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metrics: Option<MetricsSnapshot>,
}

/// `GET /health` response.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(utoipa::ToSchema))]
pub struct HealthResponse {
    pub status: String,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub messages: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub stores: Vec<StoreStatus>,
}

/// `GET /metrics` response — structured snapshot of registry metrics.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(utoipa::ToSchema))]
pub struct MetricsResponse {
    /// Approximate window length in seconds.
    pub window_secs: f64,
    pub timestamp: String,
    pub metrics: Vec<MetricPoint>,
}

/// One typed metric point in the `/metrics` response.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(utoipa::ToSchema))]
pub struct MetricPoint {
    pub name: String,
    pub kind: String,
    pub fields: Vec<MetricField>,
}

/// One key/value field on a metric point.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "schema", derive(utoipa::ToSchema))]
pub struct MetricField {
    pub key: String,
    pub value: f64,
}
