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

#[path = "replica/persistence_test.rs"]
mod persistence;

#[path = "replica/prepare_accept_test.rs"]
mod prepare_accept;

#[path = "replica/dedup_test.rs"]
mod dedup;

#[path = "replica/snapshot_test.rs"]
mod snapshot;

#[path = "replica/replay_ordering_test.rs"]
mod replay_ordering;

#[path = "replica/concurrent_test.rs"]
mod concurrent;
