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

#[path = "cluster/g1_step_down_survival_test.rs"]
mod g1_step_down_survival;

#[path = "cluster/a3_crash_restart_no_data_loss_test.rs"]
mod a3_crash_restart_no_data_loss;

#[path = "cluster/g2_crash_restart_no_data_loss_test.rs"]
mod g2_crash_restart_no_data_loss;

#[path = "cluster/full_restart_delete_test.rs"]
mod full_restart_delete;

// These suites drive crate-internal mechanisms via the `test-util` feature
// hooks on `PxGroup`; they compile only when that feature is enabled (the
// crate's self dev-dependency turns it on for `cargo test`).
#[cfg(feature = "test-util")]
#[path = "cluster/proposer_test.rs"]
mod proposer;

#[cfg(feature = "test-util")]
#[path = "cluster/safe_slot_test.rs"]
mod safe_slot;
