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

- [ ] **Network partition between processes**: verify cluster behavior when
  network connectivity between processes is severed and restored. **Blocked**:
  no network partition simulation infrastructure exists in the testkit.
  Needs a partition/drop mechanism (e.g. a proxy layer or toxiproxy-style
  interceptor) before the test can be written.
