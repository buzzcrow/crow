// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! HTTP management API request/response types for `crow-kv-server`.
//!
//! These are the wire shapes for the kv-server's internal HTTP mgmt
//! API (lifecycle endpoints, runtime state export). They live in
//! `crow-protocol` (the single home for cross-component protocol
//! types) so that `crow-kv-client`'s `KVClusterAdmin`,
//! `crow-console-shared`, `crow-web`, and `crow-cli` all import from
//! one place.
//!
//! See `doc/design/kv/design-crow-kv-server.md` §2.4 for the endpoint
//! list and `doc/design/kv/design-crow-kv-group0.md` §2.2 for the
//! "internal API" decision.

use serde::{Deserialize, Serialize};

// ── Add group initial role ──────────────────────────────────────

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AddGroupInitialRole {
    Leader,
    Follower,
}

// ── Store lifecycle ─────────────────────────────────────────────

/// `POST /stores` body.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AddStoreRequest {
    pub store_id: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,
}

/// `GET /stores` response wrapper.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoreListResponse {
    #[serde(default)]
    pub stores: Vec<StoreSummary>,
}

/// `GET /stores` item.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoreSummary {
    pub store_id: u64,
    #[serde(default)]
    pub listen_addr: Option<String>,
    pub group_count: usize,
}

/// `GET /stores/{sid}` response.
#[derive(Debug, Clone, Serialize, Deserialize)]
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
pub struct GroupSummary {
    pub group_id: u64,
    pub local_replica_id: u64,
    pub leader_id: u64,
    pub remote_count: usize,
}

// ── Remote replica lifecycle ────────────────────────────────────

/// One element of `POST /stores/{sid}/groups/{gid}/remotes` body and
/// the `GET` response. `endpoint` is the `host:port` of the remote
/// replica's gRPC service.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RemoteReplicaInfo {
    pub replica_id: u64,
    pub endpoint: String,
}

/// `GET /stores/{sid}/groups/{gid}/remotes` response wrapper.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteListResponse {
    #[serde(default)]
    pub remotes: Vec<RemoteReplicaInfo>,
}

// ── Step-down ───────────────────────────────────────────────────

/// `POST /stores/{sid}/groups/{gid}/step-down` body.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StepDownRequest {
    #[serde(default)]
    pub reason: String,
}

/// `POST /stores/{sid}/groups/{gid}/step-down` response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepDownResult {
    /// `false` when the target node was not leader (no-op fence miss).
    pub accepted: bool,
    pub current_term: u64,
    pub current_leader_id: u64,
}

// ── System init ─────────────────────────────────────────────────

/// `POST /system/init` body.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
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
pub struct SystemInitResponse {
    pub store_id: u64,
    pub group_id: u64,
    pub replica_id: u64,
    #[serde(default)]
    pub listen_addr: Option<String>,
}

// ── Topology finalize (removed in Stage 4/5, kept temporarily) ──

/// `POST /topology/finalize` response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopologyFinalizeResponse {
    pub ready: bool,
    pub already_finalized: bool,
}

/// `POST /topology/finalize` request body — carries the full cluster
/// topology from the console config so the server can write it into
/// group 0 KV.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TopologyFinalizeRequest {
    #[serde(default)]
    pub racks: Vec<TopologyRackInput>,
    #[serde(default)]
    pub nodes: Vec<TopologyNodeInput>,
    #[serde(default)]
    pub stores: Vec<TopologyStoreInput>,
    #[serde(default)]
    pub groups: Vec<TopologyGroupInput>,
    #[serde(default)]
    pub replicas: Vec<TopologyReplicaInput>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopologyRackInput {
    pub rack_id: String,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopologyNodeInput {
    pub node_id: String,
    pub rack_id: String,
    pub host: String,
    pub mgmt_endpoint: String,
    pub grpc_endpoint: String,
    #[serde(default)]
    pub election_profile: Option<String>,
    #[serde(default)]
    pub auto_start: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopologyStoreInput {
    pub store_id: u64,
    pub nodes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopologyGroupInput {
    pub group_id: u64,
    pub store_id: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopologyReplicaInput {
    pub group_id: u64,
    pub replica_id: u64,
    pub node_id: String,
    pub role: String,
    pub voting: bool,
    pub endpoint: String,
}

/// `GET /topology/ready` response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopologyReadyResponse {
    pub ready: bool,
}
