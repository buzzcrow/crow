//! Hierarchical point-in-time snapshot of the cluster, used by the topology
//! API (`/topology`, `/top`) and by tests.
//!
//! Composes bottom-up from leaf snapshots. Pure data — no `serde` dep in the lib;
//! the binary's management module wraps these into `Serialize` types.

use std::net::SocketAddr;

use crate::common::metrics::MetricsSnapshot;

#[derive(Clone, Debug)]
pub struct StoreSnapshot {
    pub store_id: u64,
    pub listen_addr: Option<SocketAddr>,
    pub groups: Vec<GroupSnapshot>,
}

#[derive(Clone, Debug)]
pub struct GroupSnapshot {
    pub group_id: u64,
    pub leader_id: u64,
    pub force_classic: bool,
    pub local_replica: LocalReplicaSnapshot,
    pub remotes: Vec<RemoteSnapshot>,
}

#[derive(Clone, Debug)]
pub struct LocalReplicaSnapshot {
    pub id: u64,
    pub role: &'static str, // "leader" | "follower"
    pub voting: bool,
    pub kv_store: KvStoreSnapshot,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct KvStoreSnapshot {
    /// O(1) read of the in-memory map length. Cheap; safe to call from
    /// `/topology` per-request.
    pub key_count: u64,
}

#[derive(Clone, Debug)]
pub struct RemoteSnapshot {
    pub id: u64,
    pub endpoint: String,
    pub voting: bool,
    pub metrics: MetricsSnapshot,
}
