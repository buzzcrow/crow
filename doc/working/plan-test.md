<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROW Test Task Backlog

**Override:** This file is **persistent** — it is not deleted after the
requirement (R9) is complete. Only completed tasks are removed; the file
itself remains as the ongoing test task backlog. This overrides the
`/implement-requirement` workflow's cleanup step which would normally delete
`plan-<topic>.md`.

Unfinished test tasks, grouped by layer. Each task has a checkbox for tracking.
For test strategy, layer scope, and coverage details, see [`design/kv/design-crow-kv-test.md`](../design/kv/design-crow-kv-test.md).

## Suite Timing

Measured on 2026-08-04 (clean build, macOS, build 88.0 s) and 2026-08-06 (clean build, Linux, build 80.0 s).
Run `pixi run clean` before measuring for reproducible results.

| Suite | Tests | macOS | Linux |
| --- | --- | --- | --- |
| `test-tree-ct` | 354 | 6.4 s | 21.0 s |
| `test-tree-ffi` | 29 | 27.6 s | 78.9 s |
| `test-kv-core` | 536 | 47.4 s | 60.8 s |
| `test-kv-server` | 68 | 54.1 s | 66.8 s |
| `test-console-cli` | 13 | 26.6 s | 67.5 s |
| `test-console-server` | 50 | 45.3 s | 53.7 s |
| `test-console-ui` | 51 | 72.5 s | 108.5 s |

---

## WAL Subsystem

Source: `lib/crow-kv/src/wal/`. Tests: 12 files, ~92 tests.

- [ ] **WAL disk-loss recovery**: simulate fsync failure or file loss after
  write — verify the engine surfaces the error and that reads/replays are
  consistent with the last durable state. **Blocked**: the full fail-out
  procedure (step-out RPC + reconfiguration, `design-crow-kv-wal.md` §8.1) is
  not yet implemented. Error injection hooks already exist
  (`block_backend.rs::inject_sync_error`); the test can be written once the
  fail-out feature lands.

## Group

Source: `lib/crow-kv/src/cluster/group.rs`. Tests: 23 files, ~65 tests.

- [x] **Reconfig — remove leader**: the group-layer test for removing the
  current leader. Step-down is implemented (`group.rs::step_down_if_leader`),
  and non-leader removal is tested (`g6_reconfig_test.rs`), but no test
  combines step-down + leader removal. The test should: (1) elect a 3-node
  group, (2) call `step_down_if_leader` on the leader, (3) remove the
  stepped-down node, (4) verify a new leader is elected and CRUD still works
  on the remaining 2 nodes.

## Store

Source: `lib/crow-kv/src/store/`. Tests: 8 files, 26 tests.

- [ ] **Per-group WAL-root isolation**: verify that groups within a single
  `PxKvStore` get isolated WAL roots and that writes to one group's WAL do
  not leak into another. **Blocked**: `WalConfig.wal_disks` is per-`WalEngine`,
  not per-group within a store — the test harness cannot yet configure
  different WAL roots per group. Needs a store-level config change before the
  test can be written.

## Deployment

Source: `app/crow-kv-server/`. Tests: 9 files.

- [x] **Multi-store-per-node process test**: boot a single `crow-kv-server`
  process hosting multiple stores and verify KV operations route correctly to
  each store. Mirrors the Web UI multi-store topology end-to-end
  (`e2e/flows/38-multi-store-isolation.spec.ts`,
  `e2e/flows/46-multi-store-reconfig.spec.ts`). No such process-level test
  exists today — all deployment tests use a single store per node.
- [x] **Reconfig via API — remove leader**: the deployment-layer test for
  removing the current leader via the HTTP management API. Both the step-down
  API (`server_api_test.rs`) and the remove-replica API
  (`deployment_reconfig_test.rs`) are tested independently, but no test
  exercises the full workflow: (1) call step-down on the leader via API,
  (2) remove the stepped-down node via API, (3) verify a new leader is
  elected and CRUD still works through the client. Actionable now — both
  APIs exist.
- [ ] **Network partition between processes**: verify cluster behavior when
  network connectivity between processes is severed and restored. **Blocked**:
  no network partition simulation infrastructure exists in the testkit.
  Needs a partition/drop mechanism (e.g. a proxy layer or toxiproxy-style
  interceptor) before the test can be written.
