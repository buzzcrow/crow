//! Console-side cluster model — the two-tree data types.
//!
//! Key work: rack/node entities, deployed server process, per-node
//! store/group/replica (physical "debugging" view), and the cluster-wide
//! store/group/replica logical view. These are the in-memory shapes the
//! monitor cache (`crate::monitor`) maintains and that the two-tree HTTP
//! API surfaces.
//!
//! Persisted vs live state:
//! - `Rack`, `Node`, `SshCreds`, `ServerProcess` (the *intended*
//!   deployment record) are persisted in `ConsoleConfig`.
//! - `ProcState`, `NodeHealth`, every `NodeStore` / `NodeGroup`, and
//!   every `StoreView` / `GroupView` / `ReplicaView` are rebuilt at
//!   runtime by the monitor task.

use serde::{Deserialize, Serialize};

use crate::snapshot::CrowtreeStatsSnapshot;

pub type RackId = String;
pub type NodeId = String;
pub type StoreId = u64;
pub type GroupId = u64;
pub type ReplicaId = u64;

// ── Physical view ───────────────────────────────────────────────────

/// Rack: a logical grouping of nodes. Console-side identity is a string
/// so simulated and real racks can share the same namespace.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Rack {
    pub id: RackId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Child node ids. Populated by the aggregator; not serialized into
    /// the persisted config (`ConsoleConfig` derives the relation from
    /// each `NodeEntry`'s `rack_id`).
    #[serde(default)]
    pub nodes: Vec<NodeId>,
}

/// SSH authentication for a node. Plaintext is acceptable in v1; v2
/// pulls from OS keychain.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SshCreds {
    /// Default `~/.ssh/*` keys (agent + standard key paths).
    KeyDefault { user: String },
    /// Explicit private-key path.
    KeyPath { user: String, key_path: String },
    /// Plaintext password auth.
    Password { user: String, pass: String },
}

/// One host, one OS user. Mirrors the physical-tree root.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Node {
    pub id: NodeId,
    pub rack_id: RackId,
    /// `127.0.0.1` by default for simulated clusters.
    pub host: String,
    pub ssh: SshCreds,
    /// 0 or 1 `crowkv-server` per node, enforced console-side.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server: Option<ServerProcess>,
}

/// Console's view of one deployed `crowkv-server` process.
///
/// `mgmt_url` / `grpc_url` are the *intended* endpoints (persisted);
/// `pid` / `state` / `health` / `last_seen_ms` are live cache filled by
/// the monitor task.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServerProcess {
    pub mgmt_url: String,
    pub grpc_url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pid: Option<u32>,
    #[serde(default)]
    pub state: ProcState,
    #[serde(default)]
    pub health: NodeHealth,
    /// Unix-ms timestamp of the last successful observation. `0` when
    /// the monitor has never seen this process yet.
    #[serde(default)]
    pub last_seen_ms: u64,
}

/// Lifecycle state of the `crowkv-server` process (console's view).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProcState {
    /// Console has never observed this process.
    #[default]
    Unknown,
    Stopped,
    Starting,
    Running,
    Failed,
}

/// Health hint for a node, derived from the most recent `/health` probe.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeHealth {
    /// Reachable and the server reported a healthy status.
    Up,
    /// Probed and either unreachable or unhealthy.
    Down,
    /// No probe has happened yet (just registered / monitor starting).
    #[default]
    Unknown,
}

// ── Per-node store / group / replica (physical "debugging" view) ────

/// What `crowkv-server` reports for one local `PxStore` on one node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeStore {
    pub node_id: NodeId,
    pub store_id: StoreId,
    /// gRPC listen address (`host:port`) of this `PxKvStore`. Each store
    /// binds its own port, so the bootstrap grpc port reported in
    /// `ServerEntry::grpc_url` is only correct for the bootstrap store
    /// (id 1). Operator-created stores get a random port and must wire
    /// Paxos remotes to *this* address. `None` until the next monitor
    /// poll fills it in.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub listen_addr: Option<String>,
    pub groups: Vec<NodeGroup>,
}

/// Per-node group entry, including the `LocalReplica` plus the full
/// remote-proxy list as the server-side data structure holds them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeGroup {
    pub node_id: NodeId,
    pub store_id: StoreId,
    pub group_id: GroupId,
    pub local: LocalReplicaInfo,
    pub remotes: Vec<RemoteReplicaInfo>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub leader_hint: Option<ReplicaId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalReplicaInfo {
    pub replica_id: ReplicaId,
    pub role: ReplicaRole,
    pub state: ReplicaState,
    /// Mirrors `crowkv`'s `KvStoreStatus::engine_healthy` (doc/todo-sm.md
    /// Step 2/6), threaded through from `crate::snapshot::KvStoreView` by
    /// `crate::monitor::legacy_topology_to_node_stores`.
    #[serde(default = "default_engine_healthy")]
    pub engine_healthy: bool,
    /// Mirrors `KvStoreStatus::crowtree_stats`; `None` for `InMemKV`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub crowtree_stats: Option<CrowtreeStatsSnapshot>,
}

fn default_engine_healthy() -> bool {
    true
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoteReplicaInfo {
    pub replica_id: ReplicaId,
    pub node_id: NodeId,
    #[serde(default)]
    pub reachable: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplicaRole {
    Leader,
    Follower,
}

/// Operational status of a replica (mirrors `crowkv-server`'s reporting).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplicaState {
    #[default]
    Unknown,
    Initializing,
    Running,
    Draining,
    Failed,
}

// ── Logical (usage) view ────────────────────────────────────────────

/// Cluster-wide store. Aggregated by the monitor from every node that
/// hosts a `NodeStore` with this `store_id`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoreView {
    pub store_id: StoreId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// Every node observed hosting the store.
    pub nodes: Vec<NodeId>,
    pub groups: Vec<GroupSummary>,
}

/// Lightweight summary used by `StoreView::groups`. The full view is
/// `GroupView`, returned from `GET /api/stores/:s/groups/:g`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroupSummary {
    pub group_id: GroupId,
    pub replica_count: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub leader: Option<ReplicaId>,
}

/// Cluster-wide Paxos group. Replicas are unified — no local / remote
/// split; each entry tagged with the hosting `node_id`.
///
/// The current leader is not a top-level field: each `ReplicaView`
/// already carries the hosting node's `role`, so consumers identify the
/// leader as `replicas.iter().find(|r| r.role == ReplicaRole::Leader)`.
/// This avoids the previous redundancy where `leader` and per-replica
/// roles could disagree.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroupView {
    pub store_id: StoreId,
    pub group_id: GroupId,
    pub replicas: Vec<ReplicaView>,
    #[serde(default)]
    pub state: GroupHealth,
}

impl GroupView {
    /// Convenience accessor: the replica self-reporting `Leader` role,
    /// if any. Returns `None` when no replica reports as leader (the
    /// group is in the middle of an election or the monitor has not
    /// finished probing).
    #[must_use]
    pub fn leader(&self) -> Option<&ReplicaView> {
        self.replicas.iter().find(|r| r.role == ReplicaRole::Leader)
    }

    /// Convenience accessor: the leader's replica id, if any.
    #[must_use]
    pub fn leader_id(&self) -> Option<ReplicaId> {
        self.leader().map(|r| r.replica_id)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GroupHealth {
    /// All replicas observed `Up`.
    Healthy,
    /// At least one replica unreachable but quorum is intact.
    Degraded,
    /// Quorum cannot be formed from currently-reachable replicas.
    Unavailable,
    /// Monitor has not finished probing.
    #[default]
    Unknown,
}

/// One replica as seen from the logical tree. The `node_id` makes the
/// physical projection explicit without forcing the caller to round-trip
/// through `/api/nodes/:n/stores/...`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicaView {
    pub replica_id: ReplicaId,
    pub node_id: NodeId,
    pub role: ReplicaRole,
    pub state: ReplicaState,
    /// See [`LocalReplicaInfo::engine_healthy`]; forwarded verbatim by
    /// [`crate::monitor::Monitor::resolve_group`].
    #[serde(default = "default_engine_healthy")]
    pub engine_healthy: bool,
    /// See [`LocalReplicaInfo::crowtree_stats`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub crowtree_stats: Option<CrowtreeStatsSnapshot>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ssh_creds_serde_roundtrip() {
        let c = SshCreds::KeyDefault { user: "alice".into() };
        let s = serde_json::to_string(&c).unwrap();
        assert!(s.contains("\"kind\":\"key_default\""));
        let back: SshCreds = serde_json::from_str(&s).unwrap();
        assert_eq!(back, c);
    }

    #[test]
    fn proc_state_default_is_unknown() {
        assert_eq!(ProcState::default(), ProcState::Unknown);
    }

    #[test]
    fn node_health_default_is_unknown() {
        assert_eq!(NodeHealth::default(), NodeHealth::Unknown);
    }

    #[test]
    fn group_view_serializes_logical_fields() {
        let v = GroupView {
            store_id: 1,
            group_id: 2,
            replicas: vec![ReplicaView {
                replica_id: 10,
                node_id: "n1".into(),
                role: ReplicaRole::Leader,
                state: ReplicaState::Running,
                engine_healthy: true,
                crowtree_stats: None,
            }],
            state: GroupHealth::Healthy,
        };
        let s = serde_json::to_string(&v).unwrap();
        assert!(s.contains("\"node_id\":\"n1\""));
        assert!(s.contains("\"role\":\"leader\""));
        assert_eq!(v.leader_id(), Some(10));
    }
}
