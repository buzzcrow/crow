// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Replica-layer tests (`PxLocalReplica`).
//!
//! A local replica wires together the acceptor, learner, election state, and
//! (when configured) its WAL + slot list. These tests drive a single replica
//! in isolation — no peers, no group — covering WAL-backed persistence,
//! classic prepare/accept tracking, dedup suppression, and snapshot install.
//!
//! Election state machine unit tests live in the `election` binary;
//! multi-replica consensus lives in the `group` binary; pure Paxos role
//! units live in the `paxos` binary.

#[path = "kv_test/mem_kv_impl_test.rs"]
mod mem_kv;

#[path = "common/test_util.rs"]
mod test_util;

#[path = "replica_test/persistence_test.rs"]
mod persistence;

#[path = "replica_test/prepare_accept_test.rs"]
mod prepare_accept;

#[path = "replica_test/dedup_test.rs"]
mod dedup;

#[path = "replica_test/snapshot_test.rs"]
mod snapshot;

#[path = "replica_test/replay_ordering_test.rs"]
mod replay_ordering;

#[path = "replica_test/concurrent_test.rs"]
mod concurrent;

#[path = "replica_test/op_correctness_test.rs"]
mod op_correctness;
