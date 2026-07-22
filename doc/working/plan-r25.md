<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# R25 Plan: Eliminate client-side batch copy via `Bytes`

## Task breakdown

- [x] Write design doc (`design-r25.md`)
- [ ] T1: Extend `prost-build` `.bytes([...])` in `crowkv/build.rs`
- [ ] T2: Update `crowkv/src/rpc/kv_response.rs` — `Vec<u8>` → `Bytes`
- [ ] T3: Update `crowkv/src/cluster/px_kv_store.rs` — `KvScanItem`
      construction, `encode_kv_batch_items` field access
- [ ] T4: Add `bytes` dep to `crowkv-client/Cargo.toml`
- [ ] T5: Update `crowkv-client/src/client.rs` — `BatchOp` → `Bytes`,
      `batch_write`, `put`, `delete`, `get`, `scan`
- [ ] T6: Update `crowkv/tests/` — all proto constructions
- [ ] T7: Update `crowkv-server/tests/` — all proto constructions
- [ ] T8: Update `crowkv-client/tests/` — all proto constructions
- [ ] T9: `cargo fmt --check` + `cargo clippy -- -D warnings`
- [ ] T10: Run relevant tests (test-core, test-server, test-cli)

## File list

- `crowkv/build.rs`
- `crowkv/src/rpc/kv_response.rs`
- `crowkv/src/cluster/px_kv_store.rs`
- `crowkv-client/Cargo.toml`
- `crowkv-client/src/client.rs`
- `crowkv/tests/store/kv_correctness_test.rs`
- `crowkv/tests/store/node_test.rs`
- `crowkv/tests/store/shutdown_under_load_test.rs`
- `crowkv/tests/store/multi_node_multi_group_test.rs`
- `crowkv/tests/store/persistence_test.rs`
- `crowkv/tests/group/kv_test.rs`
- `crowkv/tests/group/kv_forward_test.rs`
- `crowkv/tests/group/g1_step_down_survival_test.rs`
- `crowkv/tests/group/g2_crash_restart_no_data_loss_test.rs`
- `crowkv/tests/group/g3_leader_change_test.rs`
- `crowkv/tests/group/g4_learner_stream_test.rs`
- `crowkv/tests/group/g5_recovery_test.rs`
- `crowkv/tests/group/g6_reconfig_test.rs`
- `crowkv/tests/group/membership_epoch_fence_test.rs`
- `crowkv/tests/group/snapshot_join_test.rs`
- `crowkv/tests/group/full_restart_delete_test.rs`
- `crowkv-server/tests/cluster_e2e_test.rs`
- `crowkv-server/tests/deployment_reconfig_test.rs`
- `crowkv-server/tests/snapshot_join_e2e_test.rs`
- `crowkv-client/tests/e2e_single_node_test.rs`
- `crowkv-client/tests/e2e_retry_test.rs`

## Test checklist

- [ ] `pixi run test-core` (crowkv tests)
- [ ] `pixi run test-server` (crowkv-server tests)
- [ ] `pixi run test-cli` (crowkv-client + console tests)
