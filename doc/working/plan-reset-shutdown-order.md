<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Reset Shutdown Order Plan

Fix the `POST /internal/reset` endpoint in `crow-web` to shut down the
cluster in the correct dependency order (user groups → user stores →
group-0 sysdata → group-0/store-0 → stop processes → config cleanup),
and skip RPC steps when servers are already dead. This eliminates the
10-20s retry-backoff penalty that makes `test-console-ui` slow.

## Analysis

### Symptom

`test-console-ui` takes 631.7s on Linux (vs 165.7s on macOS). Per-test
timing shows the slowest tests are in `21-kv-cluster-reconfig` and
`22-kv-cluster-topology` (45-57s each), but the same tests take only
8.6s when run alone.

### Root cause

Instrumented timing (`[RESET-TIMING]` logs in `http_internal_reset`)
shows **step 0 (group-0 sysdata cleanup) takes 9-20s**; all other steps
take <0.1s combined.

The E2E test pattern is:
1. Test's `finally` block calls `stopNodeServer` for each node (SIGTERM
   the crow-kv-server processes).
2. Next test calls `resetAll` (`POST /internal/reset`) at the top.

`resetAll` step 0 builds a `HardwareClient` from the monitor cache
(stale endpoints) and issues RPC calls (`remove_rack_cascade`,
`remove_store`, `unregister`) to group-0 — but the servers hosting
group-0 are already dead. Each RPC retries 4 times with exponential
backoff (100ms → 200ms → 400ms → 800ms, capped at 5s) before failing.
With N racks × M stores × multiple RPCs, this compounds to 9-20s of
pure retry-wait per `resetAll` call.

### Current shutdown order (wrong)

| Step | What | Group-0 alive? |
|------|------|----------------|
| 0 | RPC to group-0: `remove_rack_cascade`, `remove_store`, `unregister` | Maybe (if servers running) |
| 1 | RPC to each node mgmt API: `remove_group`, `remove_store` for user stores | Maybe |
| 2 | `stop_pid` (SIGTERM) all KV + DDB processes, remove nodes from config | Killed here |
| 3 | Remove racks from config | No |
| 4 | Clear caches + workspace dirs | No |

Problems:
- Step 0 runs before steps 1-2, so it cleans sysdata before shutting
  down the services that write to it. If a server is mid-shutdown, the
  sysdata cleanup races with the server's own shutdown unregister.
- When servers are already dead (the E2E case), step 0 wastes 10-20s
  on doomed RPC retries.
- Group-0 (store 0 / group 0) is not explicitly shut down — it dies
  implicitly when the process is killed in step 2.

### Correct shutdown order

Graceful shutdown follows dependency order: remove data groups first,
then data stores, then clean sysdata (group-0 still alive), then shut
down group-0/store-0, then stop processes, then config cleanup.

| Step | What | Group-0 alive? | Servers alive? |
|------|------|----------------|----------------|
| 1 | Remove user groups (non-zero) via mgmt API RPC to each node | Yes | Yes |
| 2 | Remove user stores (non-zero) via mgmt API RPC to each node | Yes | Yes |
| 3 | Clean group-0 sysdata: `remove_rack_cascade`, `remove_store`, `unregister` diskdb | Yes | Yes |
| 4 | Remove group-0 / store-0 via mgmt API RPC to each node | Last data removed | Yes |
| 5 | Graceful stop all KV + DDB processes (SIGTERM → graceful shutdown) | No | Stopped |
| 6 | Remove nodes from config | No | No |
| 7 | Remove racks from config | No | No |
| 8 | Clear caches + workspace dirs (test deploy folder) | No | No |

When servers are already dead (E2E `finally` already stopped them):
steps 1-4 RPC calls fail fast (connection refused, no retry — see
Solution §2), so `resetAll` completes in <0.1s. Steps 5-8 are
in-process and always work.

## Solution

### 1. Split into reusable internal functions

Extract the shutdown logic from the monolithic `http_internal_reset`
into two private async functions in `lifecycle.rs`, so the steps are
reusable and testable independently:

- **`shutdown_kv_data(state) -> Vec<String>`** — steps 1-4: remove
  user groups (non-zero) → remove user stores (non-zero) → clean
  group-0 sysdata (`remove_rack_cascade`, `remove_store`, `unregister`
  diskdb) → remove group-0/store-0 via mgmt API. Returns the list of
  stopped node IDs (empty — actual process stop is in
  `stop_all_services`). Guards all RPC steps with
  `kv_pid_snapshot().is_empty()` — skips RPC when servers are dead.
- **`stop_all_services(state) -> Vec<String>`** — step 5: graceful
  stop (SIGTERM → graceful shutdown) all KV + DDB processes, clear
  runtime PIDs. Returns the list of stopped node IDs.

`http_internal_reset` becomes:
`shutdown_kv_data` → `stop_all_services` → config cleanup (nodes,
racks, disk-groups, caches, workspaces).

### 2. Skip RPC steps when no servers are running

Guard all RPC-based steps (1-4 inside `shutdown_kv_data`) with
`state.kv_pid_snapshot().is_empty()`. When empty (servers already
dead), skip the RPC calls and fall through to config-only cleanup.
This is the key fix for the E2E slowness — when the test's `finally`
block already stopped all servers, `resetAll` skips the doomed RPC
retries entirely.

### 3. Separate user stores from group-0

The current code treats all stores uniformly. The new order requires
removing user stores (non-zero) in step 2, then group-0/store-0 in
step 4. Split the store list into `user_stores` (sid != 0) and
`system_store` (sid == 0, if present).

### 4. Remove group-0/store-0 via mgmt API, not just kill

Currently group-0/store-0 is never explicitly removed — the process is
killed and the workspace dir is wiped. In the new order, step 4 calls
`client.remove_group(0, 0)` and `client.remove_store(0)` on each node
that hosts store 0, triggering the server's graceful shutdown of the
PxGroup/PxKvStore (flushes WAL, closes engine). This is cleaner than
killing the process and leaving the engine in an unflushed state.

### 5. Remove timing instrumentation

Remove the `[RESET-TIMING]` debug logging added during diagnosis, and
the `[TIMING]` logging added to the E2E test.

## Steps

- [x] **Extract `shutdown_kv_data` + `stop_all_services`**: split the
  monolithic `http_internal_reset` into two private async functions.
  `shutdown_kv_data` does steps 1-4 (user groups → user stores →
  sysdata cleanup → group-0/store-0 removal) with dead-server guard.
  `stop_all_services` does step 5 (graceful stop all KV + DDB
  processes). `http_internal_reset` calls both then does config
  cleanup (steps 6-8). Files: `app/crow-web/src/lifecycle.rs`.
- [x] **Remove timing instrumentation**: remove `[RESET-TIMING]` debug
  logging from `lifecycle.rs` and `[TIMING]` logging from
  `22-kv-cluster-topology.spec.ts`. Files:
  `app/crow-web/src/lifecycle.rs`,
  `app/crow-web/ui/e2e/flows/22-kv-cluster-topology.spec.ts`.
- [x] **Fix `build_hardware_client` empty seeds**: pass all group-0
  hosting nodes' mgmt URLs as topology discovery seeds. Files:
  `app/crow-web/src/mgmt.rs`, `app/crow-web/src/mgmt/cluster_init.rs`.
- [ ] **Add `CrowClusterDeployer` to `crow-console-shared`**: a reusable
  struct that encapsulates the full cluster lifecycle (reset →
  provision → cluster_init → stores/groups → wait healthy → collect
  info → stop). Wraps `ConsoleClient`. Returns `ClusterInfo` with
  collected node endpoints, leader addresses, store/group map.
  Handles the full cluster (racks, nodes, servers, diskdb, stores,
  groups) — not limited to KV. Files:
  `lib/crow-console-shared/src/cluster_deployer.rs`,
  `lib/crow-console-shared/src/lib.rs`.
- [ ] **Add `CrowClusterDeployer` unit tests**: test the deployer against
  an embedded `crow-web` instance — verify start/stop/reset are fast
  and correct, cluster info is accurate, repeated cycles have no
  state leakage. Files:
  `lib/crow-console-shared/src/cluster_deployer.rs` (test module).
- [ ] **Refactor bench fixture to use `CrowClusterDeployer`**: replace
  `BenchFixture`'s hand-rolled provision/wait/cleanup with the
  deployer. Files: `app/crow-cli/src/bench/targets/kv.rs`.
- [ ] **Migrate E2E tests to use `CrowClusterDeployer`**: replace the
  loose `setupCluster`/`teardownCluster`/`resetAll`/`stopNodeServer`
  helpers in `consoleSetup.ts` with a TS wrapper around the deployer
  (via REST API calls mirroring the Rust deployer's flow). All E2E
  test files use the wrapper for deploy/cleanup. Files:
  `app/crow-web/ui/e2e/fixtures/consoleSetup.ts` (rewrite),
  `app/crow-web/ui/e2e/flows/*.spec.ts` (update imports + calls).
- [ ] **Build + lint**: `pixi run cargo build`, `pixi run cargo clippy
  -- -D warnings`, `pixi run cargo fmt --check`.
- [ ] **Run deployer unit tests**: verify start/stop/reset cycle is
  fast and correct.
- [ ] **Run the slow E2E test in isolation**: verify
  `22-kv-cluster-topology.spec.ts` test 25 still passes alone.
- [ ] **Run full `test-console-ui` suite**: verify the suite time
  drops significantly from 631.7s and no new failures are introduced.
- [ ] **Update `doc/design/test.md`**: update the suite timing table
  with the new measurement. Files: `doc/design/test.md`.
- [ ] **Commit**: single commit with the restructured reset + seeds
  fix + deployer.

## File list

- `app/crow-web/src/lifecycle.rs` — reorder `http_internal_reset`
  steps, add dead-server guard, split user/system stores, remove
  debug timing.
- `app/crow-web/src/mgmt.rs` — fix `build_hardware_client` to pass
  all group-0 hosting nodes' mgmt URLs as topology discovery seeds
  (was empty seeds → "no seeds configured" on leader failover).
- `app/crow-web/src/mgmt/cluster_init.rs` — same empty-seeds fix in
  Phase 5 (one-time write, but consistency matters).
- `lib/crow-console-shared/src/cluster_deployer.rs` — new
  `CrowClusterDeployer` struct + `ClusterInfo` / `TopologyDescriptor`
  types + unit tests.
- `lib/crow-console-shared/src/lib.rs` — export `cluster_deployer`.
- `app/crow-cli/src/bench/targets/kv.rs` — refactor `BenchFixture` to
  use `KVClusterDeployer`.
- `app/crow-web/ui/e2e/flows/22-kv-cluster-topology.spec.ts` — remove
  debug timing instrumentation.
- `doc/design/test.md` — update suite timing table.

## Test checklist

- [ ] `CrowClusterDeployer` unit tests: start/stop/reset cycle <5s,
  cluster info accurate, 3x repeated cycles no state leakage.
- [ ] `22-kv-cluster-topology.spec.ts` test 25 passes alone (~8s, no
  regression).
- [ ] Full `test-console-ui` suite: time drops from ~631s, no new
  failures beyond the 7 pre-existing ones.
- [ ] `cargo clippy -- -D warnings` passes.
- [ ] `cargo fmt --check` passes.
