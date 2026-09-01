<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB Test Task Backlog

<!-- DO NOT DELETE THIS FILE — it is a persistent backlog, not a per-task draft. -->

**Override:** This file is **persistent** — it is not deleted after the
requirement (R9) is complete. Only completed tasks are removed; the file
itself remains as the ongoing test task backlog. This overrides the
`/implement-requirement` workflow's cleanup step which would normally delete
`plan-<topic>.md`.

Unfinished test tasks, grouped by layer. Each task has a checkbox for tracking.
For test strategy, layer scope, and coverage details, see [`design/kv/design-crowdb-kv-test.md`](../design/kv/design-crowdb-kv-test.md).

## CI Job Grouping Guide

CI splits tests into 6 parallel jobs. Each job pays a fixed setup overhead
(~2 min: checkout, apt-get, setup-pixi, rust-cache restore), so jobs run
in parallel to minimize wall-clock time. The critical path is the slowest
job, not the sum.

**Assignment rule:** a test task's job is determined by two questions:
1. Does it need CMake-built C++ binaries (ctest)? → **CppTests**
2. Does it spawn real subprocesses (crowdb-kv-server, crowdb-diskdb, crowdb-diskio)?
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
| **ServerTests** | `test-kv-server`, `test-diskdb`, `test-diskdb-client`, `test-chunkdb`, `test-chunk-client`, `test-diskio-client` | `build-tests` | Spawns crowdb-kv-server / crowdb-diskdb / crowdb-diskio subprocesses |
| **ConsoleTests** | `test-console-shared`, `test-console-cli`, `test-console-server` | `build-tests` | Spawns crowdb-kv-server via lifecycle::deploy_local |
| **UITests** | `test-console-ui` | `build-tests` + `install-ui-deps` | Playwright browser E2E + subprocess spawning |

**Adding a new test task:**
1. Add the task to `pixi.toml` under `# ── Test ──` with the right `depends-on`:
   - C++ ctest → `depends-on = ["build-cpp"]`
   - Rust test (no subprocess) → no `depends-on`
   - Rust test (spawns subprocess) → add `cargo build -p crowdb-kv-server` to the cmd
2. Assign it to the matching CI job in `.github/workflows/ci.yml` using the table above.
3. If the job doesn't already run `build-tests`, add a `Build all Rust test binaries` step.

## Suite Timing

Measured on 2026-08-28 on Linux (post-task-fb pull `81d56124` + KV
client dead-connection fix) in a sequential pixi test run with all
binaries pre-compiled. C++ ctest suites report ctest's "Total Test
time"; `test-common-ct` and Rust suites report wall-clock time
including subprocess startup/shutdown. All console Rust suites pass
(0 failures); `test-console-ui` was re-run and dropped from 8
pre-existing failures to 1 (the `task-fb` pull fixed 7).

| Suite | Tests | macOS | Linux (08-28) |
| --- | --- | --- | --- |
| `test-tree-ct` | 416 | 20.1 s | 15.78 s |
| `test-common-ct` | 21 | — | 17.78 s |
| `test-tree-ffi` | 30 | 13.5 s | 0.54 s |
| `test-rpc-ct` | 57 | — | 2.59 s |
| `test-rpc-ffi` | 13 | — | 0.68 s |
| `test-diskio-ct` | 93 | — | 4.46 s |
| `test-common` | 65 | 21.9 s | 9.78 s |
| `test-protocol` | 121 | 12.2 s | 0.70 s |
| `test-kv-core` | 558 | 43.2 s | 63.20 s |
| `test-kv-client` | 49 | 23.4 s | 4.70 s |
| `test-chunkdb-client` | 10 | 13.8 s | 2.00 s |
| `test-kv-server` | 81 | 53.0 s | 43.52 s |
| `test-diskdb` | 127 | 42.8 s | 25.76 s |
| `test-diskdb-client` | 7 | 13.9 s | 16.81 s |
| `test-chunkdb` | 76 | 27.8 s | 19.75 s |
| `test-chunk-client` | 49 | — | 12.11 s |
| `test-diskio-client` | 4 | — | 43.22 s |
| `test-console-shared` | 64 | 39.2 s | 9.36 s |
| `test-console-cli` | 17 | 69.4 s | 44.09 s |
| `test-console-server` | 74 | 50.7 s | 42.72 s |
| `test-console-ui` | 102 | 165.7 s | 270.0 s |

---

## WAL Subsystem

Source: `lib/crowdb-kv/src/wal/`. Tests: 12 files, ~92 tests.

- [ ] **WAL disk-loss recovery (full fail-out)**: the full fail-out procedure
  (step-out RPC + reconfiguration, `design-crowdb-kv-wal.md` §8.1) is not yet
  implemented. The test should verify the node fails out of the group and
  rejoins via snapshot install after the disk is replaced. **Blocked** on the
  fail-out feature landing.

## Store

Source: `lib/crowdb-kv/src/store/`. Tests: 8 files, 26 tests.

- [ ] **Per-group WAL disk isolation**: `WalConfig.wal_disks` is per-`WalEngine`,
  not per-group within a store — the server startup path
  (`create_group_with_wal`) derives `wal_disks` from the store-level config, so
  groups cannot be assigned different physical disks. **Blocked** on a
  store-level config change to support per-group `wal_disks` override.

## Deployment

Source: `app/crowdb-kv-server/`. Tests: 9 files.

- [ ] **Network partition between processes**: verify cluster behavior when
  network connectivity between processes is severed and restored. **Blocked**:
  no network partition simulation infrastructure exists in the testkit.
  Needs a partition/drop mechanism (e.g. a proxy layer or toxiproxy-style
  interceptor) before the test can be written.

## Console UI E2E (`test-console-ui`)

Source: `app/crowdb-web/ui/e2e/`. 50 tests, ~4.5 min (single worker, real
backend + real `crowdb-kv-server` subprocess).

### Stability

Ran 8 consecutive rounds (400 total tests): 399 passed, 1 failed. The single
failure (round 8, `21-kv-cluster-reconfig` "4th replica catches up") was a
`toBeVisible({ timeout: 5_000 })` on `G-4500` in the sidebar tree — the tree
had not re-rendered after the prior test's cleanup within 5 s. Fixed by
raising the timeout to 10 s. Pass rate after fix: 100 % across all rounds.

### Full suite timing — 2026-09-01 (4-batch run, 49 tests)

Measured 2026-09-01 on Linux, debug binaries, single worker, real backend.
All 19 spec files run in 4 batches of 5 (last batch had 2). 48 passed,
1 failed (`12-cluster-node-inspect` "cross-jumps" — stale monitor cache:
`group0_leader_endpoint` returned a stopped node's listen_addr because it
didn't skip `Down` nodes; fixed by filtering `NodeHealth::Down` in
`group0_leader_endpoint` + adding `resetAll` at test start).

Top 10 slowest tests (per-test wall-clock):

| # | Duration | Spec:line | Test |
| --- | --- | --- | --- |
| 1 | 29.5 s | `21-kv-reconfig:182` | stopping a non-leader keeps quorum, stopping the leader triggers reelection |
| 2 | 18.2 s | `22-kv-topology:463` | comparative smoke suite passes on SIMPLE and COMPLEX topologies |
| 3 | 16.7 s | `21-kv-reconfig:389` | stopping shared node degrades both stores, restart recovers |
| 4 | 15.6 s | `90-flow-full-chain:20` | rack → node → server → store → group → replica → kv, both views (clusterInit fixed; test still fails at `add store 188 UI` — pre-existing leader election issue) |
| 5 | 14.8 s | `31-kv-ops-advanced:94` | prefix/selected/inline delete + copy, load more, all-groups |
| 6 | 14.4 s | `12-cluster-node-inspect:30` | cross-jumps between views (FAILED) |
| 7 | 14.1 s | `01-shell-ui-behaviors:31` | dialog defaults, cancel, and tree interactions |
| 8 | 13.4 s | `50-chunk-capacity-disk-group:421` | assign disk-group to diskdb via UI |
| 9 | 12.7 s | `21-kv-reconfig:284` | deleting non-leader nodes preserves quorum down to majority |
| 10 | 12.1 s | `22-kv-topology:297` | two groups on overlapping 3-node subsets operate independently |

Slow steps (>= 5 s, from `stepTimer` instrumentation):

| Duration | Spec | Step label |
| --- | --- | --- |
| 6818 ms | `12-cluster-node-inspect` | `xjump: setup replicas` |
| 6693 ms | `22-kv-topology` | `smoke-complex: setupCluster` |
| 6780 ms | `22-kv-topology` | `3groups: setup` |
| 6774 ms | `22-kv-topology` | `iso-stores: setup` |
| 6759 ms | `22-kv-topology` | `overlap: setup` |
| 6470 ms | `22-kv-topology` | `smoke-simple: setupCluster` |
| 6418 ms | `21-kv-reconfig` | `440: deleteNodeViaMenu(2)` |
| 6012 ms | `21-kv-reconfig` | `430: putKeyUi+getKeyUi x2` |
| 5987 ms | `21-kv-reconfig` | `quorum: putKeyUi+getKeyUi x2` (post-merge run) |
| 5926 ms | `21-kv-reconfig` | `420: createStore+addGroup+waitForLeader` |
| 5857 ms | `21-kv-reconfig` | `440: createStore+addGroup+waitForLeader` |
| 5849 ms | `21-kv-reconfig` | `440: putKeyUi+getKeyUi(1)` |
| 5725 ms | `21-kv-reconfig` | `430: createStore+addGroup+waitForLeader` |
| 5652 ms | `21-kv-reconfig` | `reelect: restartServerViaMenu` (post-merge run) |
| 5468 ms | `01-shell-ui-behaviors` | `shell: create store` |
| ~~5465 ms~~ | `90-flow-full-chain` | `full-chain: clusterInit (full)` — **fixed** (now < 2 s) |
| 5074 ms | `12-cluster-node-inspect` | `xjump2: resetAll` |

Batch totals: batch 1 (00,01,10,11,12) = 1.5 m; batch 2 (20,21,22,30,31) =
3.2 m; batch 3 (40,41,50,51,52) = 1.1 m; batch 4 (53,90) = 34.4 s. Grand
total ≈ 6.6 m (vs 4.5 m in the 08-28 measurement — the 08-28 run excluded
the failing `12-cluster-node-inspect` cross-jump test and used a warmer
cache from a prior run).

### Slow parts — easy wins (implemented)

- `50-capacity-diskdb` test 41 "assign disk-group to diskdb" — was 30.8 s
- `51-capacity-canvas` test 49 "datacenter root" — was 21.2 s
- `11-physical-server-lifecycle` "deploy-ui: poll server"
- `21-kv-reconfig:182` "stopping a non-leader + stopping the leader" — was 29.5 s → 9–13 s (see "Fixes — 2026-09-02" below)

### Fixes — 2026-09-02 (topology eviction + heartbeat short-circuit)

Two root causes were identified and fixed, eliminating all `VERY_SLOW`
(>= 5 s) steps in `21-kv-reconfig` and `22-kv-topology`:

- **Topology cache over-eviction** (`lib/crowdb-kv-client/src/topology.rs`):
  `merge()` evicted groups absent from a fresh `/topology` response, but
  a seed that doesn't host store 0 would evict group 0's leader — causing
  "no known leader for group (store_id=0, group_id=0)" on the next
  sysdata write (e.g. `addGroup` after `deleteNode`). Fix: only evict
  groups for stores that ARE present in the fresh body
  (`fresh_stores.contains(&sid)`). A seed that doesn't host a store
  shouldn't be authoritative for evicting groups in it.
- **Heartbeat round not short-circuiting on quorum**
  (`lib/crowdb-kv/src/cluster/group_election_leader.rs`):
  `run_heartbeat_round()` waited for ALL peers (including downed/stopped
  ones) even after quorum was reached. With a 1 s per-peer RPC timeout,
  a linearizable read barrier blocked for ~1 s per downed peer; with the
  5 s KV client RPC reaper timeout, this cascaded into ~5.5 s GET delays.
  Fix: `joinset.abort_all()` + early return when `acks >= quorum`. A
  missed higher-term from an aborted peer self-heals via the next
  heartbeat round or election driver timeout.

Post-fix timing (5 consecutive runs, 9/9 passed each, no `VERY_SLOW`):

| Test | Before | After |
| --- | --- | --- |
| `21-kv-reconfig:193` (stop non-leader + stop leader) | 29.5 s | 9–13 s |
| `21-kv-reconfig:275` (delete non-leader down to majority) | 12.7 s | 8–11 s |
| `21-kv-reconfig:380` (stop shared node, restart) | 16.7 s (flaky) | 9–11 s (stable) |
| `22-kv-topology:113` (multi-rack) | 7.8 s | 5–8 s |
| `22-kv-topology:463` (smoke SIMPLE+COMPLEX) | 18.2 s | 7–8 s |
| `putKeyUi+getKeyUi` steps | 5.5–6.0 s (VERY_SLOW) | < 2 s (no SLOW flag) |
| `createStore+addGroup+waitForLeader` | 5.7–5.9 s | < 2 s (no SLOW flag) |
| `22-kv-topology` setup steps | 6.5–6.8 s | 2.0–2.6 s |
| Full 9-test suite | ~1.4 m (flaky) | ~1.1 m (stable) |

- **Early mark-down in `http_stop_node_server`** (`app/crowdb-web/src/lifecycle.rs`):
  The node was marked `Down` in the monitor cache AFTER the ~700ms SIGTERM
  wait. During that window, concurrent `list_node_disk_groups` / tree health
  polls tried to use the stopping node as the group-0 leader, wasting ~1.4s
  on 4 retries before falling back to config. Fix: mark the node `Down`
  BEFORE sending SIGTERM. Also applied to `stop_and_remove_server_for_node`
  (used by `http_remove_node`).

### Slow parts — recorded (needs investigation)

These are slow steps where the observed time exceeds what the e2e config
predicts. Some may be inherent (UI interaction chains), but the
election/retry-related ones likely indicate real problems:

- **`22-kv-topology` setup steps (2.0–2.6 s each)** — `smoke-simple`,
  `smoke-complex`, `iso-stores`, `overlap`, `3groups` each do a full
  `setupCluster`: seed racks/nodes, deploy servers, create store/group/
  replica, wait for leader. The `deployNodeServer` (~400 ms per server,
  parallel) + sequential `clusterInit` + `createStore` + `addGroup` +
  `addReplica` calls dominate. Five tests each pay the full setup cost — a
  shared `beforeAll` cluster could amortize, but each test needs a
  different topology (different node subsets / store counts), so sharing
  is non-trivial.
- **`12-cluster-node-inspect:30` "cross-jumps" (13.5 s, FIXED)** —
  `xjump: setup replicas` (5.2 s) + `xjump2: resetAll` (4.5 s). The setup
  deploys 3 servers + creates a store/group with replicas across nodes;
  the `resetAll` cascade removes racks/stores one-by-one with 300 ms
  retries each. The `no known leader for group` warnings during
  `resetAll` are expected — after stopping all nodes, group-0 has no
  leader by design. The original failure was not the reset cascade but
  a stale monitor cache: `group0_leader_endpoint` returned a stopped
  node's `listen_addr` because it didn't skip `Down` nodes, shadowing
  the freshly-elected leader on the running node. Fixed by filtering
  `NodeHealth::Down` in `group0_leader_endpoint` + adding `resetAll` at
  test start to clear stale kv_client seeds.
- **`90-flow-full-chain:20` (15.6 s)** — `full-chain: clusterInit (full)`
  (5.5 s, intermittent — **fixed**). The `clusterInit` fixture is a
  single POST to `/api/cluster/init`. Per-phase tracing
  (`RUST_LOG=crowdb_console_shared::ops=info`) showed the 5.3 s was in
  Phase 5 `write_topology_to_sysdata` (5266 ms), matching the RPC reaper
  timeout (5 s + 500 ms scan, `kv_rpc_transport.rs:93`). Root cause:
  `resolve_group0_endpoint` fell back to the server's HTTP mgmt URL when
  no store 0 existed in config; `op_context()` seeded that as the
  group-0 RPC leader hint. TCP connected (REST server listening) but
  the RPC handshake never completed — the reaper killed the request
  after 5 s, then `handle_transport_err` refreshed topology and the
  retry succeeded. Fix: `resolve_group0_endpoint` now returns `None`
  when no store 0 exists (no HTTP fallback), so `op_context()` uses
  `with_shared_client_preserving_hint` instead of seeding a wrong
  endpoint. After fix, `clusterInit` completes in < 2 s (no SLOW flag).
  Remaining test failure: `add store 188 UI` — pre-existing leader
  election issue in the 2-node cluster (181, 182): precandidate fails
  to gather quorum (`grants=1 quorum=2`), likely because
  `clusterInit([181, 182])` returns 409 (store 0 already exists from
  `clusterInit([77])`) without re-bootstrapping the multi-node group.
- **`31-kv-ops-advanced:94` (14.8 s)** — `kv: scan` (3.2 s) +
  `kv: inline delete` (3.8 s) + 6 sub-assertions. The scan/delete API
  calls + table re-renders compound; both are O(n) in key count.
- **`01-shell-ui-behaviors:31` (14.1 s)** — `shell: create store` (5.5 s)
  + 10+ dialog/tree interaction assertions. The create-store step pays
  the full cluster-init + leader-election cost.
- **`440: openKvPanel` (5–10 s)** — `page.goto('/')` + KV panel init. The
  full page reload is required because `selectOption` hangs on stale options
  after node deletions without it (see comment in `openKvPanel`).
- **`440: putKeyUi+getKeyUi` (5–6 s, FIXED)** — each KV op goes through the UI →
  web server → crowdb-kv-server → consensus round → WAL → storage engine.
  Root cause: the linearizable read barrier's heartbeat round waited for
  ALL peers (including downed ones) instead of short-circuiting on quorum.
  Fix: heartbeat round aborts remaining peers once quorum is reached
  (see "Fixes — 2026-09-02" above). Post-fix: < 2 s, no SLOW flag.
- **`kv: scan` (3.5 s) / `kv: inline delete` (4.2 s)** — KV scan/delete API
  call + table re-render. The scan API serializes all matching keys; the
  table re-renders the full list. Both are O(n) in the number of keys.
- **`shell: Add Group dialog` (5 s)** — multi-step UI interaction: right-click
  → context menu → dialog → 6 assertions → 2 checkbox toggles →
  `waitForResponse` → tree re-render. Each step is fast individually but
  they compound.
- **`cascade: delete node/svc UI` (2.1 s)** — `page.goto('/')` + tree render
  + context menu + confirm dialog + API verify. The `page.goto('/')` is
  needed for a clean tree state after prior test operations.
- **`stopNodeServer x3/x5` teardown (2.2 s)** — `Promise.all` of server
  shutdown API calls. Each server needs ~400 ms to shut down gracefully.
- **`iso-stores: scan UI` (11 s, intermittent)** — KV scan through the UI
  after store isolation verification. The scan API + table render is slow
  when the store has many keys from prior test seeding.
- **`dc: select datacenter + inspector` (5.4 s)** — inspector loads
  capacity data from the backend on selection. The 6 `toBeVisible/
  toHaveText({ timeout: 10_000 })` calls resolve quickly but the data
  fetch + render takes ~5 s.

### Open issues — 2026-09-02

Prioritized list of remaining slow / flaky items to investigate:

1. **`stopNodeServer` teardown (2.2–2.5 s)** — each server shutdown takes
   ~700 ms (SIGTERM → graceful WAL flush → engine close → process exit).
   3–5 servers in parallel = ~2.2 s. The early mark-down fix (see above)
   eliminated the ~1.4s `list_node_disk_groups` retry cascade during
   stops, but the inherent ~700ms graceful-shutdown latency remains.
   Options: skip WAL flush on test-mode shutdown (`--no-fsync` already
   disabled, but the engine close path still flushes); or use `resetAll`
   in `finally` blocks instead of per-node `stopNodeServer` (one API call
   vs N, and `stop_all_services` already parallelizes SIGTERM).
   Impact: every test in `21`/`22` pays this in `finally`. ~20 tests ×
   2.2 s = ~44 s of total teardown time.
   ai-todo: we foce flush at the shutdown even with --no-fsync. It's a waste for test. 
   do you mean each test will start / stop cluster? For UI test, we can reuse the cluster.
   can we review, in tests in one file, if we can reuse the cluster?  if we cannot reuse , we can split the test to two files. It's a bigger change, but useful.

2. **`22-kv-topology` setup steps (2.0–2.6 s each)** — 5 tests each pay
   the full `setupCluster` cost: `seedRackAndNode` + `deployNodeServer`
   + `clusterInit` + `createStore` + `addGroup` + `addReplica`. The
   `deployNodeServer` (~400 ms × N, parallel) and the sequential
   `clusterInit` → `createStore` → `addGroup` chain dominate. Options:
   - Batch the `clusterInit` + `createStore` + `addGroup` into a single
     "setup store" API endpoint (reduces 3 HTTP round-trips to 1).
   - Allow `addGroup` to accept multiple nodes directly (currently
     `addReplica` is needed to extend to new nodes one-by-one).

3. **`90-flow-full-chain:20` — `add store 188 UI` failure (pre-existing)**
   — 2-node cluster (181, 182): precandidate fails to gather quorum
   (`grants=1 quorum=2`), likely because `clusterInit([181, 182])`
   returns 409 (store 0 already exists from `clusterInit([77])`) without
   re-bootstrapping the multi-node group. Needs investigation: should
   `clusterInit` on an already-initialized cluster re-bootstrap the
   group-0 membership to the new node set, or should the test use a
   different setup path?

4. **`iso-stores: scan UI` (11 s, intermittent)** — KV scan through the
   UI after store isolation verification. The scan API + table render is
   slow when the store has many keys from prior test seeding. Consider
   limiting the scan result set or paginating the table render.

5. **`dc: select datacenter + inspector` (5.4 s)** — inspector loads
   capacity data from the backend on selection. The data fetch + render
   takes ~5 s. Investigate whether the capacity query can be cached or
   made lazy (load on expand, not on select).

6. **`31-kv-ops-advanced:94` (14.8 s)** — `kv: scan` (3.2 s) +
   `kv: inline delete` (3.8 s) + 6 sub-assertions. The scan/delete API
   calls + table re-renders compound; both are O(n) in key count.
   Consider reducing the test's key count or splitting into smaller
   sub-tests.

7. **`01-shell-ui-behaviors:31` (14.1 s)** — `shell: create store` (5.5 s)
   + 10+ dialog/tree interaction assertions. The create-store step pays
   the full cluster-init + leader-election cost. Consider using a
   pre-initialized cluster fixture for UI-only interaction tests.

8. **`overlap: KV ops UI` (2.7 s)** — KV put/get/delete through the UI
   panel. The UI interaction chain (fill input → click → wait for
   response → assert) is ~700 ms per op × 4 ops. Consider batching
   assertions or reducing the number of UI round-trips.
