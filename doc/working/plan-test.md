<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROW Test Task Backlog

<!-- DO NOT DELETE THIS FILE — it is a persistent backlog, not a per-task draft. -->

**Override:** This file is **persistent** — it is not deleted after the
requirement (R9) is complete. Only completed tasks are removed; the file
itself remains as the ongoing test task backlog. This overrides the
`/implement-requirement` workflow's cleanup step which would normally delete
`plan-<topic>.md`.

Unfinished test tasks, grouped by layer. Each task has a checkbox for tracking.
For test strategy, layer scope, and coverage details, see [`design/kv/design-crow-kv-test.md`](../design/kv/design-crow-kv-test.md).

## Suite Timing

Measured on 2026-08-17 (warm build, macOS) and 2026-08-15 (warm build, Linux, build 3m13s).
macOS times are wall-clock `time pixi run test-*` with build + test binaries cached.
Run `pixi run clean` before measuring for reproducible results.

| Suite | Tests | macOS | Linux |
| --- | --- | --- | --- |
| `test-tree-ct` | 387 | 20.1 s | 21.0 s |
| `test-tree-ffi` | 30 | 13.5 s | 81.1 s |
| `test-common` | 56 | 21.9 s | 22.5 s |
| `test-protocol` | 89 | 12.2 s | 15.3 s |
| `test-kv-core` | 560 | 43.2 s | 61.7 s |
| `test-kv-client` | 49 | 23.4 s | 33.7 s |
| `test-diskdb-client` | 6 | 13.9 s | 22.8 s |
| `test-chunkdb-client` | 15 | 13.8 s | 6.8 s |
| `test-kv-server` | 69 | 53.0 s | 54.1 s |
| `test-diskdb` | 127 | 42.8 s | 10.6 s |
| `test-chunkdb` | 76 | 27.8 s | 25.0 s |
| `test-console-shared` | 62 | 39.2 s | 46.4 s |
| `test-console-cli` | 16 | 69.4 s | 68.8 s |
| `test-console-server` | 61 | 50.7 s | 34.1 s |
| `test-console-ui` | 75 | 165.7 s | 134.7 s |

---

## WAL Subsystem

Source: `lib/crow-kv/src/wal/`. Tests: 12 files, ~92 tests.

- [ ] **WAL disk-loss recovery (full fail-out)**: the full fail-out procedure
  (step-out RPC + reconfiguration, `design-crow-kv-wal.md` §8.1) is not yet
  implemented. The test should verify the node fails out of the group and
  rejoins via snapshot install after the disk is replaced. **Blocked** on the
  fail-out feature landing.

## Store

Source: `lib/crow-kv/src/store/`. Tests: 8 files, 26 tests.

- [ ] **Per-group WAL disk isolation**: `WalConfig.wal_disks` is per-`WalEngine`,
  not per-group within a store — the server startup path
  (`create_group_with_wal`) derives `wal_disks` from the store-level config, so
  groups cannot be assigned different physical disks. **Blocked** on a
  store-level config change to support per-group `wal_disks` override.

## Deployment

Source: `app/crow-kv-server/`. Tests: 9 files.

- [ ] **Network partition between processes**: verify cluster behavior when
  network connectivity between processes is severed and restored. **Blocked**:
  no network partition simulation infrastructure exists in the testkit.
  Needs a partition/drop mechanism (e.g. a proxy layer or toxiproxy-style
  interceptor) before the test can be written.
