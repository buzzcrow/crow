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

Measured on 2026-08-17 (warm build, macOS) and 2026-08-25 (warm build, Linux, build 1m32s).
macOS times are wall-clock `time pixi run test-*` with build + test binaries cached.
Linux times re-measured on 2026-08-25 after test speedup (shared `KvRpcTransport`,
condition polling replacing fixed sleeps, reduced polling intervals).
Run `pixi run clean` before measuring for reproducible results.
The **Linux (08-27)** column is the first run after rebase to origin/task-fb
(RPC tunables, send_queue_rejects counter, conn_for divide-by-zero fix, bench
rpc test fix). Times include compilation overhead (cold build); warm-build
times for `test-common`, `test-protocol`, and `test-chunkdb-client` match the
08-25 baseline. The large gaps on `test-kv-core` (245 s vs 87 s) and
`test-console-server` (351 s vs 84 s) are partly cold-build + partly parallel
test-load contention (see Test Failures section below).

| Suite | Tests | macOS | Linux | Linux (08-27) |
| --- | --- | --- | --- | --- |
| `test-tree-ct` | 416 | 20.1 s | 33.7 s | 65.1 s |
| `test-common-ct` | 21 | — | 19.1 s | 45.5 s |
| `test-tree-ffi` | 30 | 13.5 s | 0.5 s | 0.5 s |
| `test-rpc-ct` | 56 | — | 21.4 s | 20.5 s |
| `test-rpc-ffi` | 13 | — | 0.7 s | 0.8 s |
| `test-diskio-ct` | 92 | — | 24.7 s | 43.2 s |
| `test-common` | 65 | 21.9 s | 9.7 s | 9.7 s |
| `test-protocol` | 121 | 12.2 s | 0.1 s | 0.1 s |
| `test-kv-core` | 556 | 43.2 s | 87.2 s | 245.8 s |
| `test-kv-client` | 49 | 23.4 s | 4.1 s | 44.5 s |
| `test-chunkdb-client` | 10 | 13.8 s | 1.4 s | 1.4 s |
| `test-kv-server` | 81 | 53.0 s | 39.3 s | 76.5 s |
| `test-diskdb` | 127 | 42.8 s | 65.2 s | 55.3 s |
| `test-diskdb-client` | 7 | 13.9 s | 14.7 s | 30.9 s |
| `test-chunkdb` | 76 | 27.8 s | 16.2 s | 40.5 s |
| `test-chunk-client` | 49 | — | 10.4 s | 22.3 s |
| `test-diskio-client` | 4 | — | 42.9 s | 48.7 s |
| `test-console-shared` | 62 | 39.2 s | 13.2 s | 44.4 s |
| `test-console-cli` | 17 | 69.4 s | 52.5 s | 74.6 s |
| `test-console-server` | 71 | 50.7 s | 84.2 s | 351.5 s |
| `test-console-ui` | 75 | 165.7 s | 179.6 s | 1028.3 s |

---

## Test Failures (2026-08-27 post-rebase run)

Flaky failures observed under parallel test load. All pass in isolation.

- [ ] **`test-kv-core` / `t1_early_ack_crash::t1_1_kill_in_cas_persist_window_value_survives`**:
  "leader present after write" — election timing under parallel load. Passes in
  isolation (4.5 s). Root cause: election timeout too tight when CPU is
  saturated by concurrent test suites.
- [ ] **`test-kv-core` / `election::single_voter_with_prevote_enabled_becomes_leader`**:
  `Follower != Leader` — single-voter PreVote path doesn't reach Leader in time
  under load. Passes in isolation.
- [ ] **`test-kv-core` / `election::leader_heartbeat_tick_renews_lease`**:
  `Follower != Leader` — same timing issue as above. Passes in isolation.
- [ ] **`test-console-server` / `restart_5node_1group`**:
  "WAL did not converge for store 10 group 1 within 5s: slots=[0, 106, 106, 106, 106]"
  — node 1 has 0 accepted slots while others have 106. Passes in isolation
  (122 s). Root cause: WAL convergence 5 s timeout too tight under parallel load.
- [ ] **`test-console-server` / `restart_6node_2group_overlap`**:
  "restart 4: 502 Bad Gateway ... did not become healthy within timeout" —
  node fails to become healthy during restart under load. Passes in isolation.
- [ ] **`test-diskio-ct` (18 tests, parallel ctest only)**:
  `DiskioStartupTest.WriteReadRoundTrip`, `DiskSet.*`, `BlockDisk.*`,
  `DummyDisk.*`, `SqFullBackpressureTest.*`, `UringEngine.*` — all 18 fail
  when `ctest` runs in parallel with other C++ suites (resource/port
  conflict). All 92 pass when run alone. Root cause: parallel ctest
  execution across separate build directories conflicts on shared
  resources (likely `/tmp` or port allocation).

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
