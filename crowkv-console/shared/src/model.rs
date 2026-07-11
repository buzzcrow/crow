//! Console data model placeholders. Real fields land in C1 (snapshot) and
//! C2 (registry); kept minimal here so downstream crates have stable names.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Rack {
    pub id: String,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Node {
    pub id: String,
    pub rack_id: String,
    pub host: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ServerInstance {
    pub id: String,
    pub node_id: String,
    pub mgmt_url: String,
    pub grpc_url: String,
}
