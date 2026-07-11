//! Hierarchical point-in-time status of the cluster, used by the management APIs.
//!
//! Each layer (`PxKvStore` → `PxGroup` → `PxLocalReplica` / `PxRemoteReplica`)
//! exposes `status()` returning these structs. Status-specific fields
//! (`status`, `messages`) are defaulted; `#[serde(skip_serializing_if)]`
//! suppresses empty lists in topology output.

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::common::metrics::MetricsSnapshot;

/// Severity of a layer's runtime status. Serializes as a lowercase
/// string (`"ok"`, `"degraded"`, `"unhealthy"`) so the JSON wire shape
/// is identical to the previous `String`-typed fields.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize, ToSchema)]
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

#[derive(Clone, Debug, Default, Serialize, Deserialize, ToSchema)]
pub struct StoreStatus {
    pub store_id: u64,
    pub listen_addr: Option<String>,
    pub status: StatusLevel,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub messages: Vec<String>,
    pub groups: Vec<GroupStatus>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, ToSchema)]
pub struct GroupStatus {
    pub group_id: u64,
    pub leader_id: u64,
    pub local_replica_id: u64,
    pub force_classic: bool,
    pub status: StatusLevel,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub messages: Vec<String>,
    pub local_replica: ReplicaStatus,
    pub remotes: Vec<RemoteStatus>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, ToSchema)]
pub struct ReplicaStatus {
    pub id: u64,
    pub role: String,
    pub voting: bool,
    pub status: StatusLevel,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub messages: Vec<String>,
    pub kv_store: KvStoreStatus,
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, ToSchema)]
pub struct KvStoreStatus {
    /// O(1) read of the in-memory map length. Cheap; safe to call from
    /// `/topology` per-request.
    pub key_count: u64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, ToSchema)]
pub struct RemoteStatus {
    pub id: u64,
    pub endpoint: String,
    pub voting: bool,
    pub status: StatusLevel,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub messages: Vec<String>,
    pub metrics: MetricsSnapshot,
}
