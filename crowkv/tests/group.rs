//! Group-layer tests (`PxGroup`).
//!
//! A group owns one local replica plus zero or more remote replicas and runs
//! the full Paxos write path, leader election driver, repair, and KV
//! forwarding. These tests drive real multi-node clusters (via the shared
//! `testkit::cluster` harness — separate `PxKvStore` nodes wired over loopback
//! gRPC, no mocks) as well as single-leader fast paths.
//!
//! Layout:
//! - propose / repair: `group_propose`, `proposer`, `safe_slot`, `kv_slot_retry`
//! - election & step-down: `election`, `g1_step_down_survival`
//! - KV through the group: `kv`, `kv_forward`
//! - remote replica transport: `remote_error`, `preemption_retry`, `paxos_error`
//! - durability across restart: `a3_*`, `g2_*`, `full_restart_delete`

#[path = "testkit/mod.rs"]
mod testkit;

#[path = "group/group_test.rs"]
mod group;

#[path = "group/group_propose_test.rs"]
mod group_propose;

#[path = "group/kv_test.rs"]
mod kv;

#[path = "group/kv_forward_test.rs"]
mod kv_forward;

#[path = "group/election_test.rs"]
mod election;

#[path = "group/remote_error_test.rs"]
mod remote_error;

#[path = "group/kv_slot_retry_test.rs"]
mod kv_slot_retry;

#[path = "group/preemption_retry_test.rs"]
mod preemption_retry;

#[path = "group/paxos_error_test.rs"]
mod paxos_error;

#[path = "group/g1_step_down_survival_test.rs"]
mod g1_step_down_survival;

#[path = "group/a3_crash_restart_no_data_loss_test.rs"]
mod a3_crash_restart_no_data_loss;

#[path = "group/g2_crash_restart_no_data_loss_test.rs"]
mod g2_crash_restart_no_data_loss;

#[path = "group/full_restart_delete_test.rs"]
mod full_restart_delete;

// These suites drive crate-internal mechanisms via the `test-util` feature
// hooks on `PxGroup`; they compile only when that feature is enabled (the
// crate's self dev-dependency turns it on for `cargo test`).
#[cfg(feature = "test-util")]
#[path = "group/proposer_test.rs"]
mod proposer;

#[cfg(feature = "test-util")]
#[path = "group/safe_slot_test.rs"]
mod safe_slot;
