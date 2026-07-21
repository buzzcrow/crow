<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# R15 Implementation Plan

## Tasks

- [x] Change `Acceptor::accept` trait signature to `&PxLogEntry`
      (`crowkv/src/paxos/roles.rs:14`)
- [x] Change `PxAcceptor::accept` impl to `&PxLogEntry`
      (`crowkv/src/paxos/acceptor.rs:133`)
- [x] Change `ReplicaHandler::on_accept` trait signature to `&PxLogEntry`
      (`crowkv/src/cluster/replica.rs:144`)
- [x] Change `PxLocalReplica` ReplicaHandler `on_accept` impl to `&PxLogEntry`
      (`crowkv/src/cluster/local_replica.rs:295`)
- [x] Change `PxLocalReplica::on_accept` inherent method to `&PxLogEntry`
      (`crowkv/src/cluster/local_replica.rs:1112`)
- [x] Remove `entry.clone()` in `on_accept` acceptor call
      (`crowkv/src/cluster/local_replica.rs:1128`)
- [x] Remove `entry.clone()` in `run_accept_phase` call
      (`crowkv/src/cluster/group.rs:1546`)
- [x] Remove `entry.clone()` in `handle_accept_inner` call
      (`crowkv/src/rpc/px_service.rs:542`)
- [x] Update WAL replay call to pass `&entry`
      (`crowkv/src/cluster/local_replica.rs:509`)
- [x] Update test call sites across all test files
- [x] Run `cargo fmt --check` + `cargo clippy -- -D warnings`
- [x] Run relevant tests (paxos, election, group, kv)

## Files

- `crowkv/src/paxos/roles.rs` — `Acceptor` trait
- `crowkv/src/paxos/acceptor.rs` — `PxAcceptor` impl
- `crowkv/src/cluster/replica.rs` — `ReplicaHandler` trait
- `crowkv/src/cluster/local_replica.rs` — `PxLocalReplica` impls + WAL replay
- `crowkv/src/cluster/group.rs` — `run_accept_phase` call site
- `crowkv/src/rpc/px_service.rs` — `handle_accept_inner` call site
- Test files: `acceptor_test.rs`, `frontier_test.rs`, `heartbeat_test.rs`,
  `term_fence_test.rs`, `vote_test.rs`, `kv_slot_retry_test.rs`,
  `concurrent_test.rs`, `op_correctness_test.rs`, `persistence_test.rs`,
  `prepare_accept_test.rs`, `replay_ordering_test.rs`, `snapshot_test.rs`,
  `node_test.rs`

## Test Checklist

- [x] `pixi run test-core` — paxos/election/group/kv tests
- [x] `pixi run cargo clippy --all-targets -- -D warnings`
- [x] `pixi run cargo fmt --all -- --check`
