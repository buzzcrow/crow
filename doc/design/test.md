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

## CI Job Grouping Guide

CI splits tests into 6 parallel jobs. Each job pays a fixed setup overhead
(~2 min: checkout, apt-get, setup-pixi, rust-cache restore), so jobs run
in parallel to minimize wall-clock time. The critical path is the slowest
job, not the sum.

**Assignment rule:** a test task's job is determined by two questions:
1. Does it need CMake-built C++ binaries (ctest)? → **CppTests**
2. Does it spawn real subprocesses (crow-kv-server, crow-diskdb, crow-diskio)?
   - No → **UnitTests** (pure Rust, in-memory)
   - Yes, and it's a console/CLI test → **ConsoleTests**
   - Yes, and it's a server/storage test → **ServerTests**
3. Is it a Playwright browser test? → **UITests**
4. Is it a lint check (fmt, clippy)? → **Lint**

| Job | Pixi tasks | Build dep | Rule |
| --- | --- | --- | --- |
| **Lint** | `cargo fmt --check`, `cargo clippy` | none | Fast feedback; fails without blocking tests |
| **CppTests** | `test-tree-ct`, `test-common-ct`, `test-rpc-ct`, `test-diskio-ct`, `test-tree-ffi`, `test-rpc-ffi` | `build-cpp` + `build-tests` | C++ ctest needs CMake; FFI tests are Rust but test C++ via cc::Build |
| **UnitTests** | `test-common`, `test-protocol`, `test-kv-core`, `test-kv-client`, `test-chunkdb-client` | `build-tests` | Pure Rust, no subprocess spawning |
| **ServerTests** | `test-kv-server`, `test-diskdb`, `test-diskdb-client`, `test-chunkdb`, `test-chunk-client`, `test-diskio-client` | `build-tests` | Spawns crow-kv-server / crow-diskdb / crow-diskio subprocesses |
| **ConsoleTests** | `test-console-shared`, `test-console-cli`, `test-console-server` | `build-tests` | Spawns crow-kv-server via lifecycle::deploy_local |
| **UITests** | `test-console-ui` | `build-tests` + `install-ui-deps` | Playwright browser E2E + subprocess spawning |

**Adding a new test task:**
1. Add the task to `pixi.toml` under `# ── Test ──` with the right `depends-on`:
   - C++ ctest → `depends-on = ["build-cpp"]`
   - Rust test (no subprocess) → no `depends-on`
   - Rust test (spawns subprocess) → add `cargo build -p crow-kv-server` to the cmd
2. Assign it to the matching CI job in `.github/workflows/ci.yml` using the table above.
3. If the job doesn't already run `build-tests`, add a `Build all Rust test binaries` step.

## Suite Timing

Measured on 2026-08-28 on Linux in the sequential pixi test run after the
RPC shutdown fix. The C++ test execution totals were 15.94 s for tree, 2.71 s
for RPC, and 4.61 s for diskio. Rust suite timings include their subprocess
startup and shutdown work. The console-server suite completed; the console UI
suite completed after fixing the store-isolation scan race (test 24). Seven
pre-existing failures remain in capacity/diskdb and store-group/reconfig specs,
unrelated to the scan fix.

| Suite | Tests | macOS | Linux (08-28) |
| --- | --- | --- | --- |
| `test-tree-ct` | 416 | 20.1 s | 15.94 s |
| `test-common-ct` | 21 | — | 17.76 s |
| `test-tree-ffi` | 30 | 13.5 s | 0.54 s |
| `test-rpc-ct` | 57 | — | 2.71 s |
| `test-rpc-ffi` | 13 | — | 1.13 s |
| `test-diskio-ct` | 93 | — | 4.61 s |
| `test-common` | 65 | 21.9 s | 9.71 s |
| `test-protocol` | 121 | 12.2 s | 0.15 s |
| `test-kv-core` | 556 | 43.2 s | 68.02 s |
| `test-kv-client` | 49 | 23.4 s | 4.54 s |
| `test-chunkdb-client` | 10 | 13.8 s | 5.63 s |
| `test-kv-server` | 81 | 53.0 s | 39.57 s |
| `test-diskdb` | 127 | 42.8 s | 25.43 s |
| `test-diskdb-client` | 7 | 13.9 s | 19.39 s |
| `test-chunkdb` | 76 | 27.8 s | 20.73 s |
| `test-chunk-client` | 49 | — | 15.18 s |
| `test-diskio-client` | 4 | — | 42.89 s |
| `test-console-shared` | 62 | 39.2 s | 24.31 s |
| `test-console-cli` | 17 | 69.4 s | 58.42 s |
| `test-console-server` | 71 | 50.7 s | 25.02 s |
| `test-console-ui` | 102 | 165.7 s | 492.0 s (42 passed, 8 pre-existing failures) |

Pre-existing `test-console-ui` failures (not caused by the reset/deployer
work):

- `21-kv-cluster-reconfig` — "stopping a non-leader keeps quorum,
  stopping the leader triggers reelection"
- `21-kv-cluster-reconfig` — "deleting non-leader nodes preserves
  quorum down to majority"
- `50-capacity-diskdb` — "disk-group and disk CRUD via the UI"
- `50-capacity-diskdb` — "disk maintenance operations, set-status,
  and health badges"
- `50-capacity-diskdb` — "assign disk-group to diskdb via UI (owner +
  bind); capacity non-zero when gRPC reachable"
- `50-capacity-diskdb` — "full deploy flow: deploy diskdb via UI,
  restart, stop, delete via context menu"
- `51-capacity-canvas` — "CapacityPanel shows cluster totals and
  instance count"
- `51-capacity-canvas` — "datacenter root in Capacity sidebar;
  inspector shows cluster totals"

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
