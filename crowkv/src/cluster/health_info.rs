//! Health info structs for API responses.
//!
//! Each layer (store, group, replica) implements `report_health()` methods
//! that return these structs. This ensures consistent health info across all API endpoints
//! and allows adding new fields in lower layers without updating every API handler.

use serde::Serialize;

#[derive(Serialize)]
pub struct HealthStoreInfo {
    pub store_id: u64,
    pub status: String,
    pub messages: Vec<String>,
    pub groups: Vec<HealthGroupInfo>,
}

#[derive(Serialize)]
pub struct HealthGroupInfo {
    pub group_id: u64,
    pub status: String,
    pub messages: Vec<String>,
    pub local_replica: HealthReplicaInfo,
    pub remotes: Vec<HealthRemoteInfo>,
}

#[derive(Serialize)]
pub struct HealthReplicaInfo {
    pub id: u64,
    pub role: String,
    pub status: String,
    pub messages: Vec<String>,
}

#[derive(Serialize)]
pub struct HealthRemoteInfo {
    pub id: u64,
    pub endpoint: String,
    pub status: String,
    pub messages: Vec<String>,
}
