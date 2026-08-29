// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Group-layer tests (`PxGroup`).
//!
//! A group owns one local replica plus zero or more remote replicas and runs
//! the full Paxos write path, leader election driver, repair, and KV
//! forwarding. These tests drive real multi-node clusters (via the shared
//! `common::cluster` harness — separate `PxKvStore` nodes wired over loopback
//! crowdb-rpc, no mocks) as well as single-leader fast paths.
//!
//! Layout:
//! - propose / repair: `group_propose`, `proposer`, `safe_slot`, `snapshot_slot`, `kv_slot_retry`
//! - new-member snapshot join: `snapshot_join`
//! - election & step-down: `election`, `g1_step_down_survival`
//! - KV through the group: `kv`, `kv_forward`
//! - remote replica transport: `remote_error`, `preemption_retry`, `paxos_error`
//! - durability across restart: `a3_*`, `g2_*`, `full_restart_delete`

mod common;

#[path = "kv_test/mem_kv_impl_test.rs"]
mod mem_kv;

#[path = "common/test_util.rs"]
mod test_util;

#[path = "group_test/group_test.rs"]
mod group;

#[path = "group_test/group_config_test.rs"]
mod group_config;

#[path = "group_test/group_propose_test.rs"]
mod group_propose;

#[path = "group_test/kv_test.rs"]
mod kv;

#[path = "group_test/kv_correctness_test.rs"]
mod kv_correctness;

#[path = "group_test/kv_edge_case_test.rs"]
mod kv_edge_case;

#[path = "group_test/kv_forward_test.rs"]
mod kv_forward;

#[path = "group_test/election_test.rs"]
mod election;

#[path = "group_test/remote_error_test.rs"]
mod remote_error;

#[path = "group_test/kv_slot_retry_test.rs"]
mod kv_slot_retry;

#[path = "group_test/preemption_retry_test.rs"]
mod preemption_retry;

#[path = "group_test/paxos_error_test.rs"]
mod paxos_error;

#[path = "group_test/g1_step_down_survival_test.rs"]
mod g1_step_down_survival;

#[path = "group_test/g3_leader_change_test.rs"]
mod g3_leader_change;

#[path = "group_test/g4_learner_stream_test.rs"]
mod g4_learner_stream;

#[path = "group_test/g5_recovery_test.rs"]
mod g5_recovery;

#[path = "group_test/g6_reconfig_test.rs"]
mod g6_reconfig;

#[path = "group_test/g7_reconfig_remove_leader_test.rs"]
mod g7_reconfig_remove_leader;

#[path = "group_test/a3_crash_restart_no_data_loss_test.rs"]
mod a3_crash_restart_no_data_loss;

#[path = "group_test/g2_crash_restart_no_data_loss_test.rs"]
mod g2_crash_restart_no_data_loss;

#[path = "group_test/full_restart_delete_test.rs"]
mod full_restart_delete;

#[path = "group_test/snapshot_join_test.rs"]
mod snapshot_join;

#[path = "group_test/membership_epoch_fence_test.rs"]
mod membership_epoch_fence;

// These suites drive crate-internal mechanisms via the `test-util` feature
// hooks on `PxGroup`; they compile only when that feature is enabled (the
// crate's self dev-dependency turns it on for `cargo test`).
#[cfg(feature = "test-util")]
#[path = "group_test/proposer_test.rs"]
mod proposer;

#[cfg(feature = "test-util")]
#[path = "group_test/safe_slot_test.rs"]
mod safe_slot;

#[cfg(feature = "test-util")]
#[path = "group_test/snapshot_slot_test.rs"]
mod snapshot_slot;

#[cfg(feature = "test-util")]
#[path = "group_test/maintenance_test.rs"]
mod maintenance;

#[cfg(feature = "test-util")]
#[path = "group_test/t1_early_ack_crash_test.rs"]
mod t1_early_ack_crash;

#[path = "group_test/coalesce_test.rs"]
mod coalesce;

#[path = "group_test/r65_replication_test.rs"]
mod r65_replication;
