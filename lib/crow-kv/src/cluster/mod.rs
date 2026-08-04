// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Cluster group management: membership, topology, replica runtime, and reconfiguration support.

pub mod group;
pub mod group_config;
pub mod group_election;
pub mod group_maintenance;
pub mod kv_server;
pub mod kv_store;
pub mod learner_stream;
pub mod local_replica;
pub mod node_config;
pub mod px_kv_store;
pub mod remote_replica;
pub mod replica;
pub mod status;
pub mod topology_kv;

pub use group::ProposeResult;
pub use group_config::{GroupConfigStore, PxGroupConfig, PxGroupMember};
pub use node_config::{NodeConfig, NodeConfigStore, NodeGroupEntry, NodeStoreEntry};

pub use kv_server::*;
pub use local_replica::*;
pub use px_kv_store::*;
pub use remote_replica::PxRemoteReplica;
pub use replica::{Replica, ReplicaClient, ReplicaHandler};
pub use status::{GroupStatus, InflightStatus, KvStoreStatus, RemoteStatus, ReplicaStatus, StoreStatus};
