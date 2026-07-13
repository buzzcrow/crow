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
//! no gRPC. Multi-replica election convergence lives in the `group` binary;
//! the replica-layer persistence / prepare-accept / dedup / snapshot tests
//! live in the `replica` binary.

#[path = "election/role_test.rs"]
mod role;

#[path = "election/vote_test.rs"]
mod vote;

#[path = "election/heartbeat_test.rs"]
mod heartbeat;

#[path = "election/lease_test.rs"]
mod lease;

#[path = "election/term_fence_test.rs"]
mod term_fence;

#[path = "election/metrics_test.rs"]
mod metrics;

#[path = "election/step_down_test.rs"]
mod step_down;

#[path = "election/frontier_test.rs"]
mod frontier;
