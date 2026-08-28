// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! Leader-election unit tests.
//!
//! Entrypoint for isolated unit tests of the election state machine on
//! [`PxLocalReplica`]: role transitions (`become_follower` /
//! `become_precandidate` / `become_candidate` / `become_leader`), vote
//! granting rules (`PreVote` / `RequestVote`), heartbeat handler, lease
//! management, and term fencing.
//!
//! These tests drive a single replica in isolation — no peers, no group,
//! no crow-rpc. Multi-replica election convergence lives in the `group` binary;
//! the replica-layer persistence / prepare-accept / dedup / snapshot tests
//! live in the `replica` binary.

#[path = "election_test/role_test.rs"]
mod role;

#[path = "election_test/vote_test.rs"]
mod vote;

#[path = "election_test/heartbeat_test.rs"]
mod heartbeat;

#[path = "election_test/lease_test.rs"]
mod lease;

#[path = "election_test/term_fence_test.rs"]
mod term_fence;

#[path = "election_test/metrics_test.rs"]
mod metrics;

#[path = "election_test/step_down_test.rs"]
mod step_down;

#[path = "election_test/frontier_test.rs"]
mod frontier;

#[path = "election_test/apply_loop_test.rs"]
mod apply_loop;
