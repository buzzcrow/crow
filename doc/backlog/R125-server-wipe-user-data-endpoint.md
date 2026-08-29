<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R125: server — wipe-user-data management endpoint + bench clean verb

**Problem**

R124 splits the monolithic `bench kv` flow into deploy/prepare/run/
teardown lifecycle verbs, amortizing deploy + pre-populate across many
sub-tests. The write-regression flow needs one more primitive: a way to
reset a deployed cluster to a **data-empty, group0-intact** state between
write sub-tests, so each write measurement starts from a clean slate
without paying a full redeploy. R124 deliberately deferred this primitive
(the riskiest part of the original requirement) into this follow-up,
because it needs a **new** per-node server management endpoint that no
existing API provides.

Today there is no "wipe user data, keep cluster wiring" operation in
`crowdb-kv-server`. The closest existing primitives are the wrong shape:

- `KVEngine::clear` (`lib/crowdb-kv/src/kv/kv_engine.rs`) drops all
  engine state, but is an in-process trait method used only by snapshot-
  install reset and tests — it is not exposed over the management API,
  it does not touch the WAL, and it is not coordinated across a group's
  replicas.
- `http_internal_reset` (sysdata lifecycle §8) tears down the **whole
  cluster** — hardware hierarchy + KV-cluster topology records — and is
  the opposite of "keep group0." Group/store cleanup removes the group/
  store from `node_config` entirely.
- The WAL has per-segment `truncate` (`wal_file.rs` / `file_backend.rs` /
  `block_backend.rs`) but no "drop all segments" or "reset to empty"
  method on `WalEngine`; a wipe must coordinate engine + WAL together
  and re-establish a clean append point.

So a write regression sub-test that wants a fresh dataset today must
either redeploy the whole cluster (the cost R124 was meant to eliminate)
or skip the reset entirely (contaminating the measurement with the
previous sub-test's data). Neither is acceptable for a write-regression
sentinel that should run many sub-tests against one deployed cluster.

**Current behavior + impact**

- `bench kv` write path (`tools/bench-kv-write-regression.sh`) invokes
  `bench kv` once per sub-test (7× today), each paying deploy + teardown.
  R124's lifecycle split lets it deploy once, but without a clean verb
  it must still redeploy per sub-test to get a data-empty start —
  defeating the amortization for the write flow specifically.
- `crowdb-kv-server` management API (`app/crowdb-kv-server/src/mgmt.rs`
  router, `group_ops.rs` handlers) exposes per-group admin verbs
  (`flush`, `step-down`, `join`, `ready`) but no data-wipe verb. The
  router has no `wipe-user-data` route.
- `PxLocalReplica` (`lib/crowdb-kv/src/cluster/local_replica.rs`) holds
  `wal: Option<Arc<WalEngine>>` (line 225) and the engine via the
  learner; a coordinated wipe must reset both and re-establish a clean
  WAL append point + engine state, then re-elect / re-become healthy.
- Impact: (1) the write regression cannot benefit from R124's amortized
  deploy; (2) there is no safe admin primitive to reset a cluster's
  user data for any operational scenario (re-benching, re-seeding,
  debugging) without a full redeploy or a destructive full-cluster
  reset; (3) the absence of a deliberately-named wipe endpoint means
  any future "reset data" need is tempted to reach for the destructive
  `http_internal_reset`, which destroys group0.

**Design pointers**

- `doc/design/kv/design-crowdb-kv-sysdata-lifecycle.md` §8 (Cluster
  reset) — defines what `http_internal_reset` tears down and the
  group0-preservation rules. The wipe-user-data endpoint is the
  *complementary* primitive: it preserves exactly what §8 destroys
  (group0 sysdata + store/group/replica topology) and destroys exactly
  what §8 leaves alone (user-data WAL + engine keys). The design draft
  must state this boundary explicitly and confirm no overlap.
- `doc/design/kv/design-crowdb-kv-sysdata-lifecycle.md` §9 (Invariants,
  I1 ID-reuse safety) — the wipe must not violate I1: wiping user data
  must not make a group/store/disk ID reusable (only the sysdata
  removal paths in §1-§3 do that). The wipe leaves topology records
  intact, so IDs stay claimed.
- `doc/design/console/design-crowdb-console.md` — root console design;
  the `bench clean` verb is a CLI orchestration over the per-node
  endpoint, same shape as the other R124 verbs.
- `doc/backlog/R124-console-bench-lifecycle-split.md` — the parent
  requirement; R125 is the deferred phase-2 (clean verb + endpoint).
  R124's lifecycle verbs (deploy/prepare/run/teardown) land first;
  R125 depends on R124's `ClusterHandle` (mgmt endpoints recorded in
  the handle) to know which nodes to call.

**Use scenarios**

- Operator runs the write regression: script calls `bench deploy --name
  W --kind kv` once (R124), then for each write sub-test calls
  `bench clean --target W` (wipe user data on every node, keep
  group0/store/group/replicas intact) → `bench run --target W
  --workload write`. Each write test starts from a data-empty,
  group0-intact cluster without a full redeploy. Wall time drops from
  7× (deploy+teardown) to 1× deploy + 7× (clean+run).
- Operator re-seeds a deployed test cluster with a fresh dataset for
  debugging: `bench clean --target mycluster` → `bench prepare
  --target mycluster --keys N` (R124). No redeploy; cluster wiring
  (store/group/replicas/leader) stays valid across the wipe.
- Operator accidentally runs `bench clean` against a cluster with live
  data they wanted to keep → the wipe endpoint's deliberately non-
  trivial name (`wipe-user-data`, not `reset`/`clear`/`wipe`) plus the
  per-node invocation requirement (must hit every node, not one) makes
  accidental triggering hard; the CLI also logs a clear "wiping user
  data on cluster `<name>`, group0 preserved" banner before acting.

**Solution**

Add a per-group management API endpoint
`POST /stores/:sid/groups/:gid/wipe-user-data` to `crowdb-kv-server`
that wipes the WAL + engine user data for that group on the receiving
node while leaving group0 sysdata and the store/group/replica topology
untouched. The `bench clean` CLI verb (deferred from R124 item 5) reads
the `ClusterHandle` (R124), iterates the recorded per-node mgmt URLs,
invokes the endpoint on every node that hosts a replica of the target
group, then waits for the cluster to re-elect / re-become healthy. The
endpoint name is the deliberately non-trivial name/flow R124's Decisions
§2 required: `wipe-user-data` (not a bare `reset`/`wipe`), scoped under
the full `/stores/:sid/groups/:gid/` path so it is unambiguously a
per-group user-data operation, not a cluster-level reset.

**No clear solution yet — deferred to design** for the engine+WAL
reset coordination: whether the endpoint (a) calls `KVEngine::clear`
plus a new `WalEngine` "drop all segments / reset to empty" method, or
(b) drops and recreates the `WalEngine` + engine pair in place, or
(c) truncates every segment to zero and resets the append cursor. The
trade-off is between reusing `KVEngine::clear` (already used by
snapshot-install reset) vs. a fuller tear-down that also reclaims
on-disk segment files. The design draft must pick one and specify the
re-elect / re-healthy wait semantics.

**One-line summary:** Add `POST /stores/:sid/groups/:gid/wipe-user-data`
to `crowdb-kv-server` (wipes WAL + engine user data, keeps group0) and
the deferred `bench clean --target <deploy>` CLI verb that calls it on
every node of a deployed cluster, completing R124's write-regression
flow.

**Numbered work items**

1. **`wipe-user-data` management endpoint** (`app/crowdb-kv-server/src/
   mgmt/group_ops.rs`, `app/crowdb-kv-server/src/mgmt.rs`) — new
   `POST /stores/{sid}/groups/{gid}/wipe-user-data` axum handler,
   modeled on `flush_group` (same `State`/`Path<(u64,u64)>` shape,
   same `utoipa::path` annotation, same `err_json` 404 path for
   missing store/group). Registered in `mgmt.rs::router` alongside
   `/flush` and `/step-down`. The handler resolves the store → group
   → `local_replica()`, then performs the coordinated engine+WAL
   wipe (design-draft detail per the "No clear solution yet" note
   above), then returns a `WipeResult { store_id, group_id,
   accepted }`. Must preserve group0 sysdata — the handler touches
   only the target group's WAL + engine, never `node_config` or
   group0 sysdata keys. OpenAPI spec (`mgmt.rs::openapi_spec`) gains
   the new path.
2. **`WalEngine` wipe / reset primitive** (`lib/crowdb-kv/src/wal/
   wal_engine.rs`, possibly `wal_file.rs`/`file_backend.rs`/
   `block_backend.rs`) — a new method on `WalEngine` that resets the
   WAL to an empty, appendable state (drops or truncates all
   segments, resets the segment index, re-establishes a clean
   `next_segment_id` / append cursor). The design draft picks
   between drop-and-recreate vs. in-place truncate-all; either way
   the method must leave the `WalEngine` usable for fresh appends
   without a full `WalEngine::create` from the caller. This is the
   WAL-side counterpart to `KVEngine::clear`; today no such method
   exists (only per-segment `truncate`).
3. **Coordinated wipe on `PxLocalReplica`** (`lib/crowdb-kv/src/cluster/
   local_replica.rs`) — orchestrates `KVEngine::clear` (already
   exists, `kv_engine.rs` line 89) + the new `WalEngine` wipe
   primitive on the replica's `wal` (line 225) + engine, as a single
   reset that leaves the replica ready to re-join / re-elect. Must
   handle the `wal: None` case (replica not yet wired) and the
   failed-WAL case (`is_failed`). The design draft specifies whether
   a leader replica must step down before the wipe or whether the
   wipe forces a re-election.
4. **`bench clean` CLI verb** (`app/crowdb-cli/src/commands/bench.rs`,
   `app/crowdb-cli/src/bench/bench_kv.rs`) — the deferred R124 item 5.
   Reads the `ClusterHandle` (`--target <deploy-name>`), iterates the
   recorded per-node mgmt URLs, invokes `POST .../wipe-user-data` on
   every node hosting a replica of the target group via the
   `crowdb-kv-client` mgmt client, then waits for leader + health
   (reuses R124's wait-leader / wait-healthy helpers). Rejects with
   "cluster busy" if a `bench run` is in flight (active-connection
   probe, same as R124's edge-case spec). Logs a clear banner before
   acting. Depends on R124's `ClusterHandle` (mgmt URLs) and the
   lifecycle verbs being landed first.
5. **Client mgmt call** (`crowdb-kv-client` mgmt client, or
   `crowdb_console_shared`) — a `wipe_user_data(store_id, group_id)`
   method on the mgmt client that POSTs to the new endpoint and
   deserializes `WipeResult`. Reuses the existing mgmt-client HTTP
   plumbing used by `add_group`/`flush_group`/`step_down`. The bench
   verb calls this per node.
6. **Write-regression script rewrite** (`tools/bench-kv-write-
   regression.sh`) — restructure to `deploy --name W --kind kv` →
   (`clean --target W` → `run --target W --workload write`) × N →
   `teardown --target W` (this is the write-flow shape R124 item 7
   specified but could not land without R125's clean verb). Reference
   result blocks + headers updated to note the new flow. The read/
   scan scripts are unchanged (they don't need clean; R124 lands them).

**Flow diagram**

```
write regression (R125 completes R124's write flow)
----------------------
bench deploy --name W --kind kv          (R124)
        |
   (for each write sub-test:)
        bench clean --target W
            |-- POST /stores/:sid/groups/:gid/wipe-user-data  (per node)
            |-- wait re-elect + healthy
        |
        bench run --target W --workload write                 (R124)
        |
   (loop)
        bench teardown --target W                              (R124)

per-node wipe: KVEngine::clear + WalEngine wipe → group0 sysdata untouched
```

**Edge cases at a glance**

- Wipe on a leader replica → step down first (or force re-election);
  design picks one. Outcome: cluster re-elects a leader post-wipe,
  `bench clean` waits for it.
- Wipe on a replica whose WAL is already failed (`is_failed`) →
  reject with a clear "WAL failed — wipe not safe, redeploy" error;
  do not attempt the wipe on a known-bad WAL.
- Wipe on a replica with `wal: None` (not yet wired) → no-op with a
  warning log; the bench verb treats a unanimous set of no-ops as
  success (cluster is already data-empty).
- `bench clean` while a `bench run` is in flight → reject with
  "cluster busy" (active-connection probe); no wipe performed.
  Combined with the `wipe-user-data` name, prevents accidental data
  loss.
- Wipe invoked on a non-existent store/group → 404 via the existing
  `err_json` path (same as `flush_group`).
- Partial wipe failure (some nodes wiped, one failed) → the bench
  verb reports which nodes succeeded/failed; operator must `bench
  teardown` + redeploy (no automatic rollback — a half-wiped cluster
  is in an indeterminate state and a rollback would re-introduce
  divergent data).
- Wipe preserves group0 sysdata — verify via the console topology API
  post-wipe: store/group/replica records unchanged (invariant from
  sysdata lifecycle §9 I1).

**Dependencies**

- **Depends on R124** (lifecycle verbs + `ClusterHandle`) — the `bench
  clean` verb reads mgmt URLs from R124's handle and reuses R124's
  wait-leader / wait-healthy helpers. R124 must land first; R125 is
  its deferred phase-2.
- **Depends on** `KVEngine::clear` (exists, `kv_engine.rs` line 89) —
  reused as the engine-side wipe primitive.
- **No new external dependency** — the endpoint is plain axum + the
  existing mgmt-client HTTP plumbing; the WAL wipe is internal to
  `crowdb-kv`.
- **No item depends on R125 yet.** Future per-service wipe endpoints
  (rpc/chunk/storage) will follow the same shape if those services
  ever need a data-reset primitive; R125 establishes the pattern for
  the kv service first.

**Acceptance**

**wipe-user-data endpoint:**
- `POST /stores/{sid}/groups/{gid}/wipe-user-data` on a node hosting
  the target group returns 200 + `WipeResult { accepted: true }` and
  wipes that node's WAL + engine user data for the group; a
  subsequent `KVEngine::scan(b"", …)` returns 0 live keys.
  Integration test.
- After wipe, group0 sysdata is unchanged — the console topology API
  (`GET /topology`) returns the same store/group/replica records as
  before the wipe (invariant: sysdata lifecycle §9 I1 not violated).
  Integration test.
- `POST /stores/{sid}/groups/{gid}/wipe-user-data` on a non-existent
  store or group returns 404 with a clear `ErrorResponse` (same path
  as `flush_group`). Integration test.
- `wipe-user-data` on a replica with a failed WAL (`is_failed` ==
  true) returns a clear "WAL failed — wipe not safe" error and does
  not attempt the wipe. Unit test.
- The endpoint is registered in `mgmt.rs::router` and appears in the
  OpenAPI spec (`GET /openapi.json`). Unit test (static check).

**WalEngine wipe primitive:**
- `WalEngine::wipe` (or chosen method name) on a populated WAL
  resets it to an empty, appendable state: a subsequent
  `WalEngine::append` succeeds and starts at a clean slot/segment.
  Unit test.
- `WalEngine::wipe` on an already-empty WAL is a no-op and returns
  Ok. Unit test.

**Coordinated wipe on PxLocalReplica:**
- `PxLocalReplica::wipe_user_data` (or chosen method name) on a
  populated replica clears both the engine (`KVEngine::clear`) and
  the WAL (`WalEngine::wipe`); a subsequent `get` returns None for
  previously-set keys. Unit test.
- `PxLocalReplica::wipe_user_data` on a replica with `wal: None`
  logs a warning and no-ops (returns Ok). Unit test.

**bench clean verb:**
- `crowdb-cli bench clean --target t1` against a deployed+populated
  cluster wipes user data on every node so a subsequent `bench run
  --target t1 --workload read` returns 0 found keys, **but** the
  store/group/replica topology is intact (a `bench run --target t1
  --workload write` succeeds without re-wiring, and the leader
  endpoint from the handle still serves). Integration test.
- `bench clean --target t1` while a `bench run` is in flight rejects
  with a "cluster busy" error and does not wipe. Integration test.
- `bench clean --target nonexistent` errors with a clear message
  listing existing deploy names under `runtime/` (reuses R124's
  handle-not-found path). Integration test.
- `bench clean` logs a clear "wiping user data on cluster `<name>`,
  group0 preserved" banner before acting. Integration test.

**Write-regression script:**
- `tools/bench-kv-write-regression.sh` rewritten to `deploy --name W
  --kind kv` → (`clean --target W` → `run --target W --workload
  write`) × N → `teardown --target W`; produces the same result
  columns as today and 0 errors across all sub-tests. Manual run.

**Lint:**
- `pixi run cargo fmt --all -- --check` passes.
- `pixi run cargo clippy --all-targets -- -D warnings` passes.
- `pixi run test-cli` (or the equivalent bench CLI + mgmt integration
  test suite) passes.

**Open Questions**

1. **Engine+WAL wipe coordination — drop-and-recreate vs. in-place
   truncate-all.** — the work-item 2 "No clear solution yet" note.
   Drop-and-recreate (`WalEngine` + engine torn down and rebuilt in
   place) reclaims on-disk segment files and gives a guaranteed-clean
   state, but is heavier and must re-wire the replica. In-place
   truncate-all (truncate every segment to zero, reset the index +
   append cursor, `KVEngine::clear`) is lighter and reuses
   `KVEngine::clear`, but leaves empty segment files on disk (reclaimed
   lazily by GC) and must carefully reset every cursor. The design
   draft must pick one and specify the re-elect / re-healthy wait. This
   cannot be resolved automatically without measuring the on-disk
   footprint + re-wire cost trade-off.
2. **Leader step-down before wipe vs. force re-election.** — should a
   leader replica step down cleanly before the wipe (cleaner, but
   adds a step-down round-trip per node), or should the wipe simply
   force a re-election (faster, but a brief no-leader window)? The
   design draft must pick one and specify the `bench clean` wait
   semantics (wait for a specific new leader, or just wait for
   `any` leader + healthy). Trade-off: cleanliness vs. speed; the
   write-regression flow favors speed but must stay correct.
