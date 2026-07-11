//! Integration tests for cluster, KV, and node functionality.
//!
//! This file serves as the entrypoint for integration tests organized in the
//! `cluster/` directory.

#[path = "testkit/mod.rs"]
mod testkit;

#[path = "cluster/group_test.rs"]
mod group;

#[path = "cluster/group_propose_test.rs"]
mod group_propose;

#[path = "cluster/kv_test.rs"]
mod kv;

#[path = "cluster/kv_forward_test.rs"]
mod kv_forward;

#[path = "cluster/multi_group_test.rs"]
mod multi_group;

#[path = "cluster/node_test.rs"]
mod node;

#[path = "cluster/remote_error_test.rs"]
mod remote_error;

#[path = "cluster/election_test.rs"]
mod election;
