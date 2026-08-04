// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Topology KV schema for the system group (store 0, group 0).
//!
//! Defines the key layout and value codecs for cluster topology
//! metadata stored as regular KV entries in group 0. All keys use a
//! `/topology/` prefix. Values are JSON-encoded.
//!
//! Key layout:
//! - `/topology/ready` — flag key; presence means group 0 is authoritative
//! - `/topology/racks/<rack_id>` — rack metadata
//! - `/topology/nodes/<node_id>` — node metadata
//! - `/topology/stores/<store_id>` — store metadata
//! - `/topology/groups/<group_id>` — group metadata
//! - `/topology/replicas/<group_id>/<replica_id>` — replica metadata
//! - `/topology/counters/<entity>` — ID allocation counters

use serde::{Deserialize, Serialize};

// ── Key builders ──────────────────────────────────────────────

/// Prefix for all topology keys.
pub const TOPOLOGY_PREFIX: &[u8] = b"/topology/";

/// Flag key: presence means group 0 is authoritative.
pub const READY_KEY: &[u8] = b"/topology/ready";

/// Build a rack key: `/topology/racks/<rack_id>`.
#[must_use]
pub fn rack_key(rack_id: &str) -> Vec<u8> {
    format!("/topology/racks/{rack_id}").into_bytes()
}

/// Build a node key: `/topology/nodes/<node_id>`.
#[must_use]
pub fn node_key(node_id: &str) -> Vec<u8> {
    format!("/topology/nodes/{node_id}").into_bytes()
}

/// Build a store key: `/topology/stores/<store_id>`.
#[must_use]
pub fn store_key(store_id: u64) -> Vec<u8> {
    format!("/topology/stores/{store_id}").into_bytes()
}

/// Build a group key: `/topology/groups/<group_id>`.
#[must_use]
pub fn group_key(group_id: u64) -> Vec<u8> {
    format!("/topology/groups/{group_id}").into_bytes()
}

/// Build a replica key: `/topology/replicas/<group_id>/<replica_id>`.
#[must_use]
pub fn replica_key(group_id: u64, replica_id: u64) -> Vec<u8> {
    format!("/topology/replicas/{group_id}/{replica_id}").into_bytes()
}

/// Build a counter key: `/topology/counters/<entity>`.
#[must_use]
pub fn counter_key(entity: &str) -> Vec<u8> {
    format!("/topology/counters/{entity}").into_bytes()
}

/// Prefix for all rack keys.
pub const RACKS_PREFIX: &[u8] = b"/topology/racks/";

/// Prefix for all node keys.
pub const NODES_PREFIX: &[u8] = b"/topology/nodes/";

/// Prefix for all store keys.
pub const STORES_PREFIX: &[u8] = b"/topology/stores/";

/// Prefix for all group keys.
pub const GROUPS_PREFIX: &[u8] = b"/topology/groups/";

/// Prefix for all replica keys.
pub const REPLICAS_PREFIX: &[u8] = b"/topology/replicas/";

// ── Value structs ─────────────────────────────────────────────

/// Rack metadata stored in group 0.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopologyRack {
    pub rack_id: String,
    pub name: String,
}

/// Node metadata stored in group 0.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopologyNode {
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

/// Store metadata stored in group 0.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopologyStore {
    pub store_id: u64,
    /// Node IDs hosting this store.
    pub nodes: Vec<String>,
}

/// Group metadata stored in group 0.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopologyGroup {
    pub group_id: u64,
    pub store_id: u64,
}

/// Replica metadata stored in group 0.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopologyReplica {
    pub group_id: u64,
    pub replica_id: u64,
    pub node_id: String,
    pub role: String,
    pub voting: bool,
    pub endpoint: String,
}

// ── Codec helpers ─────────────────────────────────────────────

/// Encode a value as JSON bytes.
///
/// # Panics
/// Panics if serde fails (should never happen for these simple structs).
#[must_use]
pub fn encode<T: Serialize>(value: &T) -> Vec<u8> {
    serde_json::to_vec(value).expect("topology value is serializable")
}

/// Decode a JSON byte slice into a value.
///
/// # Errors
/// Returns an error string if the payload is not valid JSON.
pub fn decode<T: for<'de> Deserialize<'de>>(payload: &[u8]) -> Result<T, String> {
    serde_json::from_slice(payload).map_err(|e| format!("invalid topology JSON: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rack_key_format() {
        assert_eq!(rack_key("r1"), b"/topology/racks/r1");
    }

    #[test]
    fn node_key_format() {
        assert_eq!(node_key("n1"), b"/topology/nodes/n1");
    }

    #[test]
    fn store_key_format() {
        assert_eq!(store_key(5), b"/topology/stores/5");
    }

    #[test]
    fn group_key_format() {
        assert_eq!(group_key(3), b"/topology/groups/3");
    }

    #[test]
    fn replica_key_format() {
        assert_eq!(replica_key(3, 7), b"/topology/replicas/3/7");
    }

    #[test]
    fn counter_key_format() {
        assert_eq!(counter_key("store_id"), b"/topology/counters/store_id");
    }

    #[test]
    fn ready_key_is_const() {
        assert_eq!(READY_KEY, b"/topology/ready");
    }

    #[test]
    fn topology_rack_roundtrip() {
        let rack = TopologyRack {
            rack_id: "r1".to_string(),
            name: "Rack 1".to_string(),
        };
        let encoded = encode(&rack);
        let decoded: TopologyRack = decode(&encoded).expect("decode");
        assert_eq!(rack, decoded);
    }

    #[test]
    fn topology_node_roundtrip() {
        let node = TopologyNode {
            node_id: "n1".to_string(),
            rack_id: "r1".to_string(),
            host: "127.0.0.1".to_string(),
            mgmt_endpoint: "127.0.0.1:9921".to_string(),
            grpc_endpoint: "127.0.0.1:28001".to_string(),
            election_profile: Some("fast".to_string()),
            auto_start: true,
        };
        let encoded = encode(&node);
        let decoded: TopologyNode = decode(&encoded).expect("decode");
        assert_eq!(node, decoded);
    }

    #[test]
    fn topology_store_roundtrip() {
        let store = TopologyStore {
            store_id: 1,
            nodes: vec!["n1".to_string(), "n2".to_string()],
        };
        let encoded = encode(&store);
        let decoded: TopologyStore = decode(&encoded).expect("decode");
        assert_eq!(store, decoded);
    }

    #[test]
    fn topology_group_roundtrip() {
        let group = TopologyGroup {
            group_id: 5,
            store_id: 1,
        };
        let encoded = encode(&group);
        let decoded: TopologyGroup = decode(&encoded).expect("decode");
        assert_eq!(group, decoded);
    }

    #[test]
    fn topology_replica_roundtrip() {
        let replica = TopologyReplica {
            group_id: 5,
            replica_id: 1,
            node_id: "n1".to_string(),
            role: "leader".to_string(),
            voting: true,
            endpoint: "127.0.0.1:28001".to_string(),
        };
        let encoded = encode(&replica);
        let decoded: TopologyReplica = decode(&encoded).expect("decode");
        assert_eq!(replica, decoded);
    }

    #[test]
    fn topology_node_default_optionals() {
        let json = br#"{"node_id":"n1","rack_id":"r1","host":"h","mgmt_endpoint":"m","grpc_endpoint":"g"}"#;
        let decoded: TopologyNode = decode(json).expect("decode");
        assert!(decoded.election_profile.is_none());
        assert!(!decoded.auto_start);
    }
}
