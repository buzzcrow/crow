//! JSON-serializable info structs for API responses.
//!
//! Each layer (store, group, replica) implements `report_info()` methods
//! that return these structs. This ensures consistent info across all API endpoints
//! and allows adding new fields in lower layers without updating every API handler.

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Serialize, ToSchema)]
pub struct StoreInfo {
    pub store_id: u64,
    pub listen_addr: Option<String>,
    pub groups: Vec<GroupInfo>,
}

#[derive(Serialize, ToSchema)]
pub struct GroupInfo {
    pub group_id: u64,
    pub local_replica: ReplicaInfo,
    pub leader_id: u64,
    pub remotes: Vec<RemoteReplicaInfo>,
}

#[derive(Serialize, ToSchema)]
pub struct ReplicaInfo {
    pub id: u64,
    pub role: String,
    pub voting: bool,
    pub kv_store: KvStoreInfo,
}

#[derive(Serialize, ToSchema)]
pub struct KvStoreInfo {
    pub key_count: u64,
}

#[derive(Serialize, Deserialize, Clone, ToSchema)]
pub struct RemoteReplicaInfo {
    pub replica_id: u64,
    pub endpoint: String,
}
