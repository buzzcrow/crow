<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# R18 Plan — Queue-based admission control

## Task breakdown

- [ ] T1: Add `AdmissionPolicy` enum + new fields to `PaxosConfig`
      (`crowkv/src/common/config.rs`)
- [ ] T2: Add `InflightAdmission` struct + `InflightStatus` to
      `crowkv/src/cluster/group.rs`; replace single semaphore fields
- [ ] T3: Update `PxGroup::propose()` admission logic
- [ ] T4: Update `PxGroup::set_inflight_window` → `set_inflight_config`;
      update `inflight_slot_count()`; update test-util hooks
- [ ] T5: Add `InflightStatus` to `GroupStatus` in
      `crowkv/src/cluster/status.rs`; wire in `PxGroup::status()`
- [ ] T6: Add CLI flags `--inflight-queues`, `--inflight-admission` to
      `crowkv-server/src/cli.rs`
- [ ] T7: Update `create_group_with_wal` signature in
      `crowkv-server/src/startup.rs`
- [ ] T8: Update `store_registry.rs` with new fields + builder setters
- [ ] T9: Wire CLI args in `crowkv-server/src/main.rs`
- [ ] T10: Wire `mgmt_api.rs` calls to `create_group_with_wal`
- [ ] T11: Add `inflight_queues` + `inflight_admission` to
       `DeployNodeServerBody` in console shared (`lifecycle.rs`,
       `console.rs`)
- [ ] T12: Update `provision.rs` to pass new params
- [ ] T13: Update existing test `propose_returns_busy_when_window_is_full`
- [ ] T14: Add new tests for queue mode + multi-queue
- [ ] T15: Lint (fmt + clippy) and run relevant tests

## File list

- `crowkv/src/common/config.rs` — `AdmissionPolicy` enum, `PaxosConfig` fields
- `crowkv/src/cluster/group.rs` — `InflightAdmission` struct, `propose()`,
  `set_inflight_config()`, `inflight_slot_count()`, test hooks, `status()`
- `crowkv/src/cluster/status.rs` — `InflightStatus` struct, add to `GroupStatus`
- `crowkv-server/src/cli.rs` — new CLI flags
- `crowkv-server/src/startup.rs` — `create_group_with_wal` signature
- `crowkv-server/src/store_registry.rs` — new fields + builders
- `crowkv-server/src/main.rs` — wire CLI args
- `crowkv-server/src/mgmt_api.rs` — wire to `create_group_with_wal`
- `crowkv-console/shared/src/lifecycle.rs` — `DeployNodeServerBody` fields
- `crowkv-console/shared/src/clients/console.rs` — `DeployNodeServerBody` fields
- `crowkv-console/cli/src/bench/provision.rs` — pass new params
- `crowkv/tests/group/proposer_test.rs` — update + add tests

## Test checklist

- [ ] `propose_returns_busy_when_window_is_full` passes (reject mode, 1 queue)
- [ ] `propose_queues_when_policy_is_queue` — queue mode blocks then succeeds
- [ ] `multi_queue_distributes_permits` — permits split across queues
- [ ] Existing Paxos tests pass (default config unchanged)
- [ ] `pixi run cargo fmt --all -- --check` passes
- [ ] `pixi run cargo clippy --all-targets -- -D warnings` passes
- [ ] `pixi run test-core` passes
