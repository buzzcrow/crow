<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R124: console — split bench lifecycle into deploy/prepare/run/clean/teardown verbs

**Problem**

The `crowdb-cli bench kv` command is monolithic per invocation: every call
provisions a fresh 3-node cluster, pre-populates data, runs the workload,
then tears the whole cluster down. The three regression sentinels
(`tools/bench-kv-{read,scan,write}-regression.sh`) invoke `bench kv` once
per sub-test, so the deploy + pre-populate overhead is paid on every
single sub-test:

- read: 11 × (deploy + pre-pop 100k + 20s + teardown)
- scan: 14 × (deploy + pre-pop 100k + 20s + teardown)
- write: 7 × (deploy + 20s + teardown, no pre-pop)

For read/scan, the 100k-key pre-populate is repeated 11–14 times; the
deploy (embed console-web → deploy 3 nodes → `cluster_init` →
`add_store` → `add_group` → wait for leader → wait for healthy) is
repeated on every sub-test of all three scripts. This caps the
practical dataset size — a 10M-key pre-populate is infeasible when it
must run 14× — and wastes the majority of each script's wall time on
setup, not measurement.

The flow should instead be orchestrated by the script: deploy the
cluster once, prepare data once, then run many read/scan sub-tests
against the same cluster + data; for write, deploy once then
clean→run between sub-tests so each write test starts from a
data-empty (group0-only) state. This amortizes deploy + pre-pop and
unlocks larger datasets.

The same deploy primitive should serve `crowdb-kv-server` ad-hoc test
deployments (today those use `crowdb-cli server deploy` per node, with
no cluster_init/store/group wiring), and should be designed
multi-kind from the start — rpc, chunk, and storage clusters are
coming, and they all need the same deploy→prepare→run→teardown shape.

**Current behavior + impact**

- `bench kv` (`app/crowdb-cli/src/commands/bench/bench_kv.rs`) calls
  `run_bench` (`bench/runner.rs`), which calls `target.provision()` →
  `target.pre_populate()` → run workers → (back in `bench_kv.rs`)
  `target.cleanup()`. All four phases run inside one CLI process.
- `KvTarget::provision` (`bench/targets/kv.rs`) constructs a
  `BenchFixture` that embeds its own `crowdb-web` console instance
  (`ConsoleClient` + `axum::serve`), deploys 3 `crowdb-kv-server` nodes
  via `client.deploy_node_server`, runs `cluster_init` +
  `provision_store_and_group`, and waits for leader + health. The
  fixture holds the console task, node ids, pids, endpoints, and
  workspace dir **in-process** — nothing persists across CLI
  invocations.
- `KvTarget::cleanup` stops every node server and aborts the embedded
  console task. `BenchFixture::Drop` is a safety net that SIGTERMs
  pids if `cleanup` wasn't called.
- `KvTarget::pre_populate` is a sequential `client.put` loop over
  `0..count`, inside `run_bench`.
- Impact: (1) deploy + pre-pop overhead is paid per sub-test, capping
  dataset size; (2) the bench's embed-console-per-run model cannot be
  reused by ad-hoc `crowdb-kv-server` test deployments that want the
  same cluster_init/store/group wiring; (3) there is no "reset to
  clean group0" primitive, so write tests cannot cheaply reuse a
  deployed cluster across sub-tests.

**Design pointers**

- `doc/design/console/design-crowdb-console.md` — root console design
  (console-web, SSH/local-fork lifecycle, bootstrap). The deploy verb
  reuses the console deploy path the bench already uses.
- `doc/design/kv/design-crowdb-kv-sysdata-lifecycle.md` — sysdata
  lifecycle, including cluster reset. The clean/reset verb must be
  consistent with the group0 sysdata preservation rules defined here;
  if this doc already specifies a wipe/reset API, the clean verb
  wraps it rather than inventing a new one.
- `doc/design/kv/kv-{read,scan,write}-flow-analysis.md` — the
  regression sentinel reference docs that the rewritten scripts feed.
- No formal design doc covers the bench harness itself. The
  `BenchTarget` trait abstraction (`bench/target.rs`) is the
  extension point for multi-kind targets.

**Use scenarios**

- Operator runs the read regression: script calls `bench deploy
  --name R --kind kv` once, `bench prepare --target R --keys 10M`
  once, then `bench run --target R` for each read sub-test (1t, 6t,
  16t, 32t, verify, …) against the same 10M-key cluster, then
  `bench teardown --target R`. Wall time drops from 14× (deploy+
  pre-pop) to 1×, and the 10M dataset becomes practical. All logs
  land under `runtime/R/`.
- Operator runs the write regression: script calls `bench deploy
  --name W --kind kv` once, then for each write sub-test calls
  `bench clean --target W` (wipe data, keep group0) → `bench run
  --target W --workload write`. Each write test starts from a
  data-empty, group0-intact cluster without a full redeploy.
- Operator deploys a `crowdb-kv-server` test cluster ad-hoc (not for
  bench): `bench deploy --name mycluster --kind kv` brings up the
  cluster with cluster_init/store/group wiring, then the operator
  runs manual puts/gets against it (or `bench run --target mycluster
  --workload read` for a quick check), then `bench teardown --target
  mycluster`. Replaces the current per-node `server deploy` + manual
  wiring flow; works for production deploys in the future too.
- Future: operator deploys an rpc bench cluster (`bench deploy --name
  rpcR --kind rpc`) or a chunk/storage cluster (`--kind chunk`/
  `--kind storage`) using the same deploy/prepare/run/teardown verbs;
  console-web is opt-in via `--web` for any kind, off by default. Only
  the `BenchTarget` provision impl differs per kind.

**Solution**

Split the monolithic `bench kv` flow into discrete CLI verbs that a
script orchestrates. Each deploy gets a **named runtime folder**
(`runtime/<deploy-name>/`, e.g. `runtime/kv-bench-2026-08-28/`) that
holds the deploy metadata (handle), per-node workspaces, server logs,
and CLI bench logs together — so the relationship between a bench run
and its cluster is self-evident and a deployed cluster persists across
many `bench run` invocations. `bench run` targets a deployed cluster
by name (`--target <deploy-name>`), reading all connection info from
the folder — this works for both test deployments and future
production deploys, and reduces per-verb input parameters (no port
re-entry). Add a clean verb that wipes WAL + engine data per node
while preserving group0 sysdata, with a deliberately non-trivial
name/flow so it is not invoked accidentally. Design the deploy verb
multi-kind (`--kind kv|rpc|chunk|storage`) from the start, with
console-web opt-in via `--web` (off by default) for any kind — bench
tests run headless; rpc uses an in-process server regardless.

**One-line summary:** Split `bench kv`'s deploy/prepare/run/cleanup
into standalone, script-orchestrated CLI verbs with named runtime
folders, `bench run --target <name>`, a wipe-data-keep-group0 clean
verb, and multi-kind deploy dispatch with `--web` opt-in console-web.

**Numbered work items**

1. **Named runtime folder + cluster handle** (`bench/targets/kv.rs`,
   `bench/target.rs`, `bench/report.rs`) — introduce a top-level
   `runtime/` folder (gitignored, generalizing the existing
   `bench-runs/` pattern) with one subfolder per deploy:
   `runtime/<deploy-name>/` (e.g. `runtime/kv-bench-2026-08-28/`).
   Each subfolder holds: a serializable `ClusterHandle` (deploy name,
   kind, console URL, node ids, pids, RPC + mgmt endpoints, node
   workspaces, tunables used), per-node workspaces (`N-<id>/`, where
   servers write `log/`), and CLI bench-run logs. `bench deploy`
   takes `--name <deploy-name>` and writes the handle +
   `runtime/<name>/`; `bench run`/`prepare`/`clean`/`teardown` take
   `--target <deploy-name>` and read it. `BenchFixture::new` is split
   so provision returns the handle without owning the console task
   lifetime in-process. This is the core enabler — today everything
   lives in one process and `bench-runs/` is datetime-stamped per
   run, not named-per-deploy.
2. **`bench deploy` verb** (`commands/bench.rs`, `bench/bench_kv.rs`)
   — new subcommand taking `--name <deploy-name>`,
   `--kind kv|rpc|chunk|storage` (kv + rpc first; chunk/storage
   stubbed to return "not yet implemented"), plus the existing
   `KvArgs` tunables. Runs `BenchTarget::provision` only, writes the
   handle under `runtime/<name>/`, and **does not** cleanup. Console-
   web is opt-in via `--web` (default off): with it, a console-web
   instance stays up for the deploy's lifetime (see Decisions §1);
   without it, all kinds run headless (rpc uses an in-process server
   via `RpcTarget` regardless). Reuses the existing `BenchFixture`
   provision path (console deploy, cluster_init, store/group, wait
   leader) for kv.
3. **`bench prepare` verb** (`bench/bench_kv.rs`) — extracts the
   `KvTarget::pre_populate` sequential `put` loop into a standalone
   verb. Reads the handle (`--target <name>`), builds a `CrowdbClient`
   seeded from the recorded leader endpoint, pre-populates `--keys N`
   (with `--value-size`/`--value-size-mix`). Uses default `put`
   semantics (overwrite) — multiple `bench prepare` rounds can be
   run to accumulate a larger dataset, and `bench clean` resets
   between rounds when needed. No `--force`/skip-if-exists flag.
4. **`bench run` verb** (`commands/bench.rs`, `bench/bench_kv.rs`,
   `bench/runner.rs`) — measurement-only path taking
   `--target <deploy-name>` plus the workload args (workload,
   threads, connections, duration, read-mode, scan args, etc.). Reads
   the handle, builds workers from the recorded leader endpoint, runs
   the workload, collects + reports into `runtime/<name>/`, and
   **skips** provision, pre-populate, and cleanup. Connection info
   (endpoints, ports) comes from the handle, so per-verb input is
   just the workload shape — no port re-entry. `run_bench` gains an
   attach mode that bypasses `target.provision`/`pre_populate`/
   `cleanup`. The legacy monolithic `bench kv` (no `--target`) is
   kept as the all-in-one path for quick one-shot benches.
5. **`bench clean` verb — wipe data, keep group0**
   (`commands/bench.rs`, new per-service management API endpoint in
   `crowdb-kv-server`, client call in `crowdb-kv-client`/console) — wipes
   WAL + engine data on each node but preserves group0 sysdata so
   the cluster stays wired (store/group/replicas intact) and only
   user data is gone. Implemented as a new per-node management API
   endpoint invoked per node via the handle's mgmt URLs, then a wait
   for the cluster to re-elect / re-become healthy. The endpoint name
   and invocation flow are deliberately non-trivial (not a bare
   `reset`/`wipe`) so it cannot be triggered accidentally — exact
   name/flow TBD in design, but it must require an explicit,
   hard-to-mistake action. Must be consistent with
   `design-crowdb-kv-sysdata-lifecycle.md`'s cluster-reset rules — if
   that doc already defines a wipe API, wrap it instead of adding a
   new one.
6. **`bench teardown` verb** (`bench/bench_kv.rs`) — extracts
   `KvTarget::cleanup` (stop every node server + abort console task
   for kv/chunk/storage; stop in-process server for rpc) into a
   standalone verb that reads the handle (`--target <name>`).
   Idempotent; safe to call after a partial/crashed deploy (orphan
   cleanup). Leaves `runtime/<name>/` on disk for post-mortem logs.
7. **Regression script rewrite** (`tools/bench-kv-{read,scan,write}-
   regression.sh`) — restructure all three: read/scan become
   `deploy --name <run> --kind kv` → `prepare --target <run>
   --keys N` → `run --target <run>` × N → `teardown --target <run>`;
   write becomes `deploy` → (`clean --target <run>` → `run --target
   <run> --workload write`) × N → `teardown`. The `run_bench` shell
   helper is replaced by a `run_subtest` helper that calls `bench run
   --target <run>` with the sub-test args. Reference result blocks +
   headers updated to note the new flow.
8. **Multi-kind deploy dispatch** (`commands/bench.rs`,
   `bench/target.rs`) — `--kind` routes to the matching
   `BenchTarget`'s provision. `KvTarget` is the first concrete kind
   (3-node cluster; console-web only with `--web`); `RpcTarget`
   already exists (`bench/targets/rpc.rs`, in-process server, no
   console-web); chunk/storage are reserved names that return a clear
   "not yet implemented" error.
   This future-proofs the deploy verb so rpc/chunk/storage clusters
   reuse the same lifecycle verbs.

**Flow diagram**

```
read/scan regression                       write regression
-----------------------                   ----------------------
bench deploy --name R --kind kv           bench deploy --name W --kind kv
        |                                          |
   bench prepare --target R                (for each sub-test:)
   --keys N                                  bench clean --target W
        |                                          |
   (for each sub-test:)                      bench run --target W
        bench run --target R                       --workload write
        |                                          |
   bench teardown --target R               (loop)
                                           bench teardown --target W

runtime/<name>/ holds: handle + node workspaces + server logs + cli logs
```

**Edge cases at a glance**

- Handle stale (cluster already torn down or crashed) → `run`/
  `prepare`/`clean` detect unreachable console/leader, error with a
  clear "cluster `<name>` not running — redeploy" message; `teardown`
  is idempotent and best-effort cleans orphans.
- `--target <name>` not found (no `runtime/<name>/`) → clear error
  listing existing deploy names under `runtime/`.
- Crash mid-deploy (some nodes up, some not) → `teardown` walks the
  handle's pids + console task and SIGTERMs survivors; a `--force`
  flag reaps orphans not in the handle.
- `clean` called while a `run` is in flight → `clean` rejects with
  "cluster busy" (detect via active-connection probe) — clean is a
  between-sub-test primitive, not concurrent with measurement.
  Combined with the deliberately non-trivial wipe endpoint name/flow,
  this prevents accidental data loss.
- Attach to wrong cluster kind → handle carries `kind`; `run`
  validates it matches the requested workload's expected kind.
- Pre-populate on an already-populated cluster → `put` overwrites
  (default behavior); multiple `prepare` rounds accumulate a larger
  dataset; `clean` resets when a fresh dataset is needed.
- Console-web lifetime → only started when `--web` is passed to
  `deploy`, must outlive all subsequent verbs; see Decisions §1 for
  the lifetime model. Without `--web`, all kinds run headless.
- Port conflicts across repeated deploy/teardown → reuses
  `unique_test_port()`; R118 (port unification) is the longer-term
  fix.

**Dependencies**

- None hard. The clean/reset verb should align with
  `design-crowdb-kv-sysdata-lifecycle.md`'s cluster-reset rules.
  **Confirmed:** that doc does NOT define a "wipe data, keep group0"
  API — its reset (`http_internal_reset`) tears down hardware
  hierarchy + KV-cluster topology, and its group/store cleanup
  removes the group/store from `node_config` entirely. So the clean
  verb needs a **new** per-node management endpoint that wipes WAL +
  engine data while leaving group0 sysdata intact. R118 (unify port
  usage + port prober) is tangentially related — the bench's
  `unique_test_port()` is the stopgap R118 replaces; no block.
- Reusable groundwork already in place: the `BenchTarget` trait
  (`app/crowdb-cli/src/bench/target.rs`) with `provision` /
  `pre_populate` / `cleanup` / `run_workers` methods, and concrete
  `KvTarget` (`bench/targets/kv.rs`) + `RpcTarget`
  (`bench/targets/rpc.rs`) implementations. R124's work is mostly
  CLI orchestration, persistent handle, and the new clean endpoint —
  the target abstractions are ready.
- `.gitignore` already has `/runtime-data/` but not `/runtime/`; the
  named-folder work item must add `/runtime/` to `.gitignore`.
- No item depends on R124 yet. Future rpc/chunk/storage bench
  targets will reuse the deploy/prepare/run/teardown verbs R124
  establishes.

**Acceptance**

**Lifecycle verbs:**
- `crowdb-cli bench deploy --name t1 --kind kv` brings up a 3-node
  cluster headless (deploys nodes, cluster_init, store/group, waits
  for leader) and writes a handle under `runtime/t1/`; subsequent
  `bench run --target t1` attaches to it. Integration test.
- `crowdb-cli bench deploy --name t1w --kind kv --web` brings up the
  same 3-node cluster plus a console-web instance bound to a recorded
  port in the handle; `bench teardown --target t1w` stops both.
  Integration test.
- `crowdb-cli bench prepare --target t1 --keys 100000` against a
  deployed handle pre-populates 100k keys with 0 errors; a follow-up
  `bench run --target t1 --workload read` reads them back with 0
  `correctness_errors`. Integration test.
- `crowdb-cli bench run --target t1 --workload read` runs a workload
  against the deployed cluster, produces a JSON + markdown report
  under `runtime/t1/`, and does **not** tear the cluster down (a
  second `bench run --target t1` succeeds). Integration test.
- `crowdb-cli bench teardown --target t1` stops all node servers +
  aborts the console task; a second `teardown --target t1` is a
  no-op (idempotent). Integration test.
- `bench teardown --target t1` after a simulated mid-deploy crash
  (kill -9 one node before deploy finishes) still reaps the
  surviving nodes + console. Integration test.
- `bench run --target nonexistent` errors with a clear message
  listing existing deploy names under `runtime/`. Integration test.

**Clean (wipe data, keep group0):**
- `crowdb-cli bench clean --target t1` against a deployed+populated
  cluster wipes user data so a subsequent `bench run --target t1
  --workload read` returns 0 found keys, **but** the store/group/
  replica topology is intact (a `bench run --target t1 --workload
  write` succeeds without re-wiring, and the leader endpoint from
  the handle still serves). Integration test.
- `bench clean` preserves group0 sysdata — cluster topology records
  (`design-crowdb-kv-sysdata-lifecycle.md`) are unchanged after clean;
  verify via the console topology API. Integration test.
- `bench clean --target t1` while a `bench run` is in flight rejects
  with a "cluster busy" error and does not wipe. Integration test.
- The wipe endpoint name/flow is deliberately non-trivial (not a
  bare `reset`/`wipe`) — verify the endpoint requires an explicit,
  hard-to-mistake invocation. Integration test.

**Multi-kind dispatch:**
- `crowdb-cli bench deploy --name r1 --kind rpc` provisions the RPC
  bench target (reuses `bench/targets/rpc.rs` `RpcTarget::provision`,
  no console-web) and writes a handle with `kind=rpc`. Integration
  test.
- `crowdb-cli bench deploy --name c1 --kind chunk` (and `--kind
  storage`) returns a clear "not yet implemented" error (reserved
  kinds). Integration test.
- `bench run --target r1 --workload read` rejects a handle whose
  `kind` does not match the requested workload with a clear error.
  Integration test.

**Regression scripts:**
- `tools/bench-kv-read-regression.sh` rewritten to `deploy --name R
  --kind kv` → `prepare --target R --keys N` → `run --target R` × N
  → `teardown --target R`; produces the same result columns as today
  and 0 errors across all sub-tests. Manual run.
- `tools/bench-kv-scan-regression.sh` rewritten the same way; 0
  errors, including the `largeval_16k` R67 sentinel. Manual run.
- `tools/bench-kv-write-regression.sh` rewritten to `deploy --name W
  --kind kv` → (`clean --target W` → `run --target W --workload
  write`) × N → `teardown --target W`; 0 errors across all
  sub-tests. Manual run.
- Rewritten read/scan scripts support a `--keys` env override so a
  10M-key run is practical (amortized pre-pop). Manual run.

**Lint:**
- `pixi run cargo fmt --all -- --check` passes.
- `pixi run cargo clippy --all-targets -- -D warnings` passes.
- `pixi run test-cli` (or the equivalent bench CLI integration test
  suite) passes.

**Decisions**

1. **Console-web lifetime — opt-in via `--web`, off by default.**
   `bench deploy` takes a `--web` flag (default off). Without it, no
   console-web is started for any kind — rpc deploys never need one
   (`RpcTarget` provisions an in-process server), and kv/chunk/storage
   deploys run headless too. Bench tests pass `--web` only when a
   scenario needs the UI; the common bench path skips it. When `--web`
   is given, a console-web instance stays up for the deploy's lifetime
   (started by `bench deploy`, stopped by `bench teardown`). The exact
   mechanism for keeping the console-web alive across separate CLI
   invocations (daemonize the deploy process, or have `deploy` spawn +
   detach a console-web child whose pid is recorded in the handle) is a
   design-draft detail, not a backlog-level decision.
2. **Clean/wipe — per-service, deliberately non-trivial name/flow.**
   The wipe is a per-service management API endpoint (one per
   service kind, since each service owns its own data layout), not
   a single generic reset. The endpoint name and invocation flow
   are deliberately complex so it cannot be triggered accidentally
   — exact name/flow TBD in the design draft, but it must require
   an explicit, hard-to-mistake action (not a bare `reset`/`wipe`).
   Must still be consistent with
   `design-crowdb-kv-sysdata-lifecycle.md`'s cluster-reset rules; if
   that doc already defines a wipe API, the per-service endpoints
   wrap it rather than duplicate it. Open sub-question: read that
   doc's reset section during design to confirm no duplication.
3. **`bench run` — standalone verb with `--target <deploy-name>`.**
   A standalone `bench run` verb (not an `--attach` flag on
   `bench kv`). Each deploy has a name (`--name <deploy-name>` on
   `deploy`, e.g. `kv-xx`, `storage-xx`, `chunk-xx`, `rpc-xx`) and
   writes its metadata under `runtime/<deploy-name>/`. `bench run
   --target <deploy-name>` reads all connection info (endpoints,
   ports, kind) from that folder, so per-verb input is just the
   workload shape — no port re-entry, no arg duplication. This
   works for both test deployments and future production deploys.
   The legacy monolithic `bench kv` (no `--target`) is kept as the
   all-in-one path for quick one-shot benches.
4. **Pre-populate — default `put` (overwrite), multi-round supported.**
   `bench prepare` uses default `put` semantics (overwrite). No
   `--force`/`--skip-if-exists` flag. Multiple `prepare` rounds can
   be run to accumulate a larger dataset; `bench clean` resets when
   a fresh dataset is needed.
5. **Handle location — named `runtime/<deploy-name>/` folders.**
   Deploys use a top-level `runtime/` folder (gitignored,
   generalizing the existing `bench-runs/` pattern —
   `bench-runs/` is datetime-stamped per bench run; `runtime/` is
   named-per-deploy and persists across many bench runs against the
   same cluster). Each `runtime/<name>/` holds the handle, per-node
   workspaces (`N-<id>/` with server `log/`), and CLI bench-run
   logs together — so server logs and client logs are co-located
   and the bench↔cluster relationship is self-evident. Multi-cluster
   bench (kv + rpc simultaneously) uses distinct deploy names. The
   exact top-level folder name (`runtime/` vs reusing `bench-runs/`
   restructured) is a minor design-draft detail; `runtime/` is the
   working name.

**Open Questions**

1. **Split R124 into two phases?** — the deploy/prepare/run/teardown
   lifecycle verbs (items 1-4, 6-8) are moderate effort, re-use the
   existing `BenchTarget` trait, and deliver immediate value
   (amortized deploy + pre-pop, larger datasets, parallel sub-tests).
   The clean verb (item 5) is the riskiest part: it needs a **new**
   per-node server management endpoint that wipes WAL + engine data
   while preserving group0 sysdata — no such API exists in the
   sysdata lifecycle design or the server. Candidate split: (a) land
   the lifecycle verbs first (high value, moderate effort), then
   design + add the clean endpoint as a follow-up; or (b) do it all
   in one requirement. Trade-off: incremental delivery vs. completing
   the write-regression flow (which needs clean) in one pass. Needs a
   decision on whether the write regression can wait for the clean
   endpoint or should keep the current redeploy-per-sub-test flow
   temporarily.

2. **Clean endpoint name/flow — concrete decision needed.** — the
   doc says "deliberately non-trivial name/flow" but does not specify
   it. Candidates: `POST /stores/:sid/groups/:gid/wipe-user-data`
   with a required `confirm=<token>` query param (token derived from
   the deploy name); or a two-step `POST .../clean-request` →
   `POST .../clean-confirm` challenge-response. The design draft must
   pick one — the backlog's "TBD in design" is acceptable but the
   implementer needs a concrete name to build against. Needs a
   decision on the exact endpoint shape before implementation.
