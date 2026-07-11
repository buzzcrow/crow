//! Aggregated cluster snapshot returned to UI/CLI.
//!
//! Mirror types of `crowkv-server`'s `/topology` response. They are kept in a
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalReplicaView {
    pub id: u64,
    pub role: String,
    pub voting: bool,
    pub kv_store: KvStoreView,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KvStoreView {
    pub key_count: u64,
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

/// Wire shape of `crowkv-server`'s `GET /topology`.
#[derive(Debug, Deserialize)]
pub(crate) struct TopologyResponse {
    pub stores: Vec<StoreView>,
}
