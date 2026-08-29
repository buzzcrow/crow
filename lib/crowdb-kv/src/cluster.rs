// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Cluster group management: membership, topology, replica runtime, and reconfiguration support.

pub mod group;
pub mod group_accept;
pub mod group_coalesce;
pub mod group_config;
pub mod group_election;
pub mod group_election_candidate;
pub mod group_election_leader;
pub mod group_fetchgap;
pub mod group_inflight;
pub mod group_maintenance;
pub mod group_membership;
pub mod group_prepare;
pub mod group_propose;
#[cfg(feature = "test-util")]
mod group_tests;
pub mod kv_server;
pub mod kv_store;
pub mod local_replica;
pub mod local_replica_accept;
pub mod local_replica_apply;
pub mod local_replica_election;
pub mod local_replica_replay;
pub mod node_config;
pub mod px_kv_store;
mod px_kv_store_handler;
pub mod remote_replica;
pub mod replica;
pub mod status;
pub mod watch_registry;

pub use group::ProposeResult;
pub use group_config::{GroupConfigStore, PxGroupConfig, PxGroupMember};
pub use node_config::{NodeConfig, NodeConfigStore, NodeGroupEntry, NodeStoreEntry};

pub use kv_server::*;
pub use local_replica::*;
pub use px_kv_store::*;
pub use remote_replica::PxRemoteReplica;
pub use replica::{Replica, ReplicaClient, ReplicaHandler};
pub use status::{
    crowdb_tree_stats_to_view, GroupStatus, InflightStatus, KvStoreStatus, RemoteStatus, ReplicaStatus,
    StoreStatus,
};
