//! Shared core for the `CrowKV` Console (web + CLI).
//!
//! Key work: data models for racks/nodes/servers/stores/groups/replicas,
//! HTTP and gRPC clients to `crowkv-server`, registry persistence,
//! topology aggregation, error model, structured operation log.
//!
//! C0 status: skeleton only. Real modules land in C1+.

#![cfg_attr(not(test), allow(dead_code))]

pub mod clients;
pub mod cluster;
pub mod config;
pub mod corr_id;
pub mod error;
pub mod expand;
pub mod lifecycle;
pub mod mgmt;
pub mod model;
pub mod monitor;
pub mod ops_log;
pub mod snapshot;
pub mod ssh;
pub mod test_ports;
pub mod topology;

pub use config::{
    ConsoleConfig, ConsoleConfigEngine, KvRetryConfig, NodeEntry, RackEntry, ServerEntry, TomlFileEngine,
};
pub use snapshot::{
    ClusterSnapshot, GroupView, HealthInfo, KvStoreView, LocalReplicaView, RemoteMetrics, RemoteReplicaView,
    ServerSnapshot, StoreView,
};

/// Library version string, exposed for diagnostic / `--version` use.
#[must_use]
pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

#[cfg(test)]
mod tests {
    use super::version;

    #[test]
    fn version_is_non_empty() {
        assert!(!version().is_empty());
    }
}
