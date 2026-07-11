//! Hierarchical point-in-time snapshot of the cluster, used by the topology
//! API (`/topology`, `/top`) and by tests.
//!
//! Composes bottom-up from leaf snapshots. Pure data — no `serde` dep in the lib;
//! the binary's management module wraps these into `Serialize` types.

use std::net::SocketAddr;

use crate::cluster::info;
use crate::common::metrics::MetricsSnapshot;

#[derive(Clone, Debug)]
pub struct StoreSnapshot {
    pub store_id: u64,
    pub listen_addr: Option<SocketAddr>,
    pub groups: Vec<GroupSnapshot>,
}

impl StoreSnapshot {
    /// Convert to JSON-serializable info struct.
    #[must_use]
    pub fn report_info(&self) -> info::StoreInfo {
        info::StoreInfo {
            store_id: self.store_id,
            listen_addr: self.listen_addr.map(|a| a.to_string()),
            groups: self.groups.iter().map(GroupSnapshot::report_info).collect(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct GroupSnapshot {
    pub group_id: u64,
    pub leader_id: u64,
    pub force_classic: bool,
    pub local_replica: LocalReplicaSnapshot,
    pub remotes: Vec<RemoteSnapshot>,
}

impl GroupSnapshot {
    /// Convert to JSON-serializable info struct.
    #[must_use]
    pub fn report_info(&self) -> info::GroupInfo {
        info::GroupInfo {
            group_id: self.group_id,
            local_replica: self.local_replica.report_info(),
            leader_id: self.leader_id,
            remotes: self.remotes.iter().map(RemoteSnapshot::report_info).collect(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct LocalReplicaSnapshot {
    pub id: u64,
    pub role: &'static str, // "leader" | "follower"
    pub voting: bool,
    pub kv_store: KvStoreSnapshot,
}

impl LocalReplicaSnapshot {
    /// Convert to JSON-serializable info struct.
    #[must_use]
    pub fn report_info(&self) -> info::ReplicaInfo {
        info::ReplicaInfo {
            id: self.id,
            role: self.role.to_string(),
            voting: self.voting,
            kv_store: info::KvStoreInfo {
                key_count: self.kv_store.key_count,
            },
        }
    }
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

impl RemoteSnapshot {
    /// Convert to JSON-serializable info struct.
    #[must_use]
    pub fn report_info(&self) -> info::RemoteReplicaInfo {
        info::RemoteReplicaInfo {
            replica_id: self.id,
            endpoint: self.endpoint.clone(),
        }
    }
}
