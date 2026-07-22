<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# R24 — Implementation Plan

## Tasks

- [ ] Update proto (`kv.proto`): collapse `ReadMode` enum, rename
      `client_slot` → `min_slot` in `KvGetRequest`, add `min_slot` to
      `KvScanRequest`
- [ ] Update `KvStore` trait (`kv_store.rs`): rename `client_slot` →
      `min_slot`, update doc comments
- [ ] Update `PxKvStore` (`px_kv_store.rs`): `resolve_read_point` 2-way
      match, `kv_get`/`kv_scan` parameter rename
- [ ] Update `KvStoreService` (`kv_service.rs`): forwarding comment update,
      `client_slot` → `min_slot` in pass-through
- [ ] Update `CrowkvClient` (`client.rs`): `resolve_client_slot` →
      `resolve_min_slot`, `get`/`scan` signature changes
- [ ] Update `crowkv-client/src/lib.rs`: doc comment update
- [ ] Update tests:
      - `crowkv/tests/store/node_test.rs`
      - `crowkv-client/tests/e2e_single_node_test.rs`
      - `crowkv-server/tests/snapshot_join_e2e_test.rs`
      - `crowkv/tests/group/full_restart_delete_test.rs`
      - `crowkv/tests/store/shutdown_under_load_test.rs`
      - `crowkv/tests/store/multi_node_multi_group_test.rs`
      - `crowkv-server/tests/cluster_e2e_test.rs`
      - `crowkv-server/tests/deployment_reconfig_test.rs`
      - `crowkv/tests/group/*.rs` (all using `read_mode: 0` with
        `client_slot: 0` → rename field)
- [ ] Update design docs: `design.md` §6, `read-flow-analysis.md`
- [ ] Run `cargo fmt --check`, `cargo clippy -- -D warnings`
- [ ] Run `pixi run test-core`, `pixi run test-server`
- [ ] Commit implementation + design/plan docs
- [ ] Run full test suite
- [ ] Merge design into formal docs, cleanup, final commit

## Files

- `crowkv/src/rpc/proto/kv.proto` — enum + field changes
- `crowkv/src/cluster/kv_store.rs` — trait signature + docs
- `crowkv/src/cluster/px_kv_store.rs` — `resolve_read_point` logic
- `crowkv/src/rpc/kv_service.rs` — forwarding comments + field pass-through
- `crowkv-client/src/client.rs` — `get`/`scan` API, `resolve_min_slot`
- `crowkv-client/src/lib.rs` — doc comment
- `crowkv/tests/store/node_test.rs` — test update
- `crowkv-client/tests/e2e_single_node_test.rs` — test update
- `crowkv-server/tests/snapshot_join_e2e_test.rs` — test update
- `crowkv/tests/group/full_restart_delete_test.rs` — test update
- `crowkv/tests/store/shutdown_under_load_test.rs` — field rename
- `crowkv/tests/store/multi_node_multi_group_test.rs` — field rename
- `crowkv-server/tests/cluster_e2e_test.rs` — field rename
- `crowkv-server/tests/deployment_reconfig_test.rs` — field rename
- `crowkv/tests/group/*.rs` — field rename (6 files)
- `doc/design/design.md` — §6 Read Modes update
- `doc/working/read-flow-analysis.md` — mode references update
