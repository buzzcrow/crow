//! Integration tests for cluster, KV, and node functionality.
//!
//! This file serves as the entrypoint for integration tests organized in the
//! `cluster/` directory.

#[path = "testkit/mod.rs"]
mod testkit;

#[path = "cluster/group.rs"]
mod group;

#[path = "cluster/group_propose.rs"]
mod group_propose;

#[path = "cluster/kv.rs"]
mod kv;

#[path = "cluster/multi_group.rs"]
mod multi_group;

#[path = "cluster/node.rs"]
mod node;

#[path = "cluster/remote_error.rs"]
mod remote_error;
