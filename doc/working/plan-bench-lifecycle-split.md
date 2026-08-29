<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Bench Lifecycle Split Plan

Design: `doc/working/design-bench-lifecycle-split.md`
Backlog: `doc/backlog/R124-console-bench-lifecycle-split.md`

Goal: split `bench kv` into deploy/prepare/run/teardown CLI verbs
with named runtime folders and multi-kind deploy dispatch.

## Phase 1: ClusterHandle + module wiring

- [x] **ClusterHandle struct + persistence**: create
  `app/crowdb-cli/src/bench/handle.rs` with `ClusterHandle`,
  `DeployKind`, `HandleTunables` structs (serde Serialize/Deserialize).
  Methods: `runtime_dir(name)`, `save()`, `load(name)`,
  `list_deploys()`. Error on re-deploy (existing name). Files:
  `app/crowdb-cli/src/bench/handle.rs`,
  `app/crowdb-cli/src/bench.rs`.
- [x] **Unit tests for ClusterHandle**: save/load round-trip, load
  missing name, save on existing name, list_deploys. Files:
  `app/crowdb-cli/src/bench/handle.rs`.

## Phase 2: BenchFixture refactor + detach

- [~] **Extract provision_kv_cluster**: deferred — `bench deploy`
  reuses `BenchFixture::new` directly + `detach()`. The
  `provision_kv_cluster` extraction is not needed for the headless
  path (the `--web` path is also deferred — see Open Questions).
  Files: `app/crowdb-cli/src/bench/targets/kv.rs`.
- [x] **BenchFixture::detach**: add `detach()` method that marks
  `stopped = true` + aborts console task but leaves servers running.
  Also added `node_pids()` and `node_rpc_urls()` accessors. Files:
  `app/crowdb-cli/src/bench/targets/kv.rs`.

## Phase 3: CLI subcommands + dispatch

- [x] **Add Deploy/Prepare/Run/Teardown subcommands**: add arg
  structs (`DeployArgs`, `PrepareArgs`, `RunArgs`, `TeardownArgs`)
  + `BenchSub` variants + dispatch in `run_bench_verb`. Files:
  `app/crowdb-cli/src/commands/bench.rs`.

## Phase 4: bench deploy verb

- [x] **bench_deploy handler (kv headless)**: create BenchFixture
  (embedded console-web), provision, extract handle fields, save
  handle, detach. Files:
  `app/crowdb-cli/src/commands/bench/bench_deploy.rs`.
- [~] **bench_deploy handler (--web)**: deferred — the `--web` flag
  is accepted but not yet implemented (returns error). Will be
  implemented when the standalone `crowdb-web` spawn path is needed.
  Files: `app/crowdb-cli/src/commands/bench/bench_deploy.rs`.
- [x] **bench_deploy handler (rpc)**: spawn
  `crowdb-rpc-fb-server` as detached child, wait for bind, write
  handle with kind=rpc. Files:
  `app/crowdb-cli/src/commands/bench/bench_deploy.rs`.
- [x] **bench_deploy handler (chunk/storage stub)**: return "not
  yet implemented" error. Files:
  `app/crowdb-cli/src/commands/bench/bench_deploy.rs`.

## Phase 5: bench prepare verb

- [x] **bench_prepare handler**: load handle, validate kind=kv,
  build CrowdbClient from leader endpoint, run sequential put loop.
  Files: `app/crowdb-cli/src/commands/bench/bench_prepare.rs`.

## Phase 6: bench run verb

- [x] **AttachedKvTarget**: implement BenchTarget with no-op
  provision/cleanup, build client from handle. Files:
  `app/crowdb-cli/src/bench/targets/kv.rs`.
- [x] **bench_run handler**: load handle, validate kind, build
  BenchConfig + AttachedKvTarget, call run_bench, collect artifacts,
  report to runtime/<name>/. Files:
  `app/crowdb-cli/src/commands/bench/bench_run.rs`.

## Phase 7: bench teardown verb

- [x] **bench_teardown handler**: load handle, SIGTERM node pids
  via stop_pid_with_timeout, kill console-web pid if present, remove
  handle.json. Idempotent. Files:
  `app/crowdb-cli/src/commands/bench/bench_teardown.rs`.

## Phase 8: E2E tests

- [x] **Lifecycle E2E tests**: deploy+run+teardown, deploy+prepare+
  run, multiple runs same deploy, teardown idempotent, run on
  nonexistent target, deploy existing name, chunk/storage stub.
  Files: `app/crowdb-cli/tests/bench_benchmark_test.rs`.

## Phase 9: Regression script rewrite

- [x] **Rewrite bench-kv-read-regression.sh**: deploy → prepare →
  run × N → teardown. Files:
  `tools/bench-kv-read-regression.sh`.
- [x] **Rewrite bench-kv-scan-regression.sh**: same lifecycle flow.
  Files: `tools/bench-kv-scan-regression.sh`.

## Phase 10: Lint + test

- [x] **cargo fmt + clippy**: fix all warnings. Files: all touched
  `.rs` files.
- [x] **Run test-console-cli**: verify all bench tests pass. Files:
  `app/crowdb-cli/tests/`.

## File list

- `app/crowdb-cli/src/bench/handle.rs` — NEW: ClusterHandle, DeployKind, save/load
- `app/crowdb-cli/src/bench/mod.rs` — MOD: add mod handle
- `app/crowdb-cli/src/bench/targets/kv.rs` — MOD: extract provision_kv_cluster, add detach + AttachedKvTarget
- `app/crowdb-cli/src/commands/bench.rs` — MOD: add Deploy/Prepare/Run/Teardown subcommands
- `app/crowdb-cli/src/commands/bench/bench_deploy.rs` — NEW: deploy handler
- `app/crowdb-cli/src/commands/bench/bench_prepare.rs` — NEW: prepare handler
- `app/crowdb-cli/src/commands/bench/bench_run.rs` — NEW: run handler
- `app/crowdb-cli/src/commands/bench/bench_teardown.rs` — NEW: teardown handler
- `app/crowdb-cli/tests/bench_benchmark_test.rs` — MOD: add lifecycle E2E tests
- `tools/bench-kv-read-regression.sh` — MOD: rewrite to lifecycle flow
- `tools/bench-kv-scan-regression.sh` — MOD: rewrite to lifecycle flow
- `doc/working/design-bench-lifecycle-split.md` — NEW: design draft
- `doc/working/plan-bench-lifecycle-split.md` — NEW: this plan

## Test checklist

### Unit tests
- [ ] ClusterHandle save/load round-trip
- [ ] ClusterHandle load missing name (error + list deploys)
- [ ] ClusterHandle save on existing name (error)
- [ ] ClusterHandle list_deploys (0/1/3 deploys)
- [ ] DeployKind parse (kv/rpc/chunk/storage/bad)

### E2E tests (test-console-cli)
- [ ] deploy + run + teardown (headless kv)
- [ ] deploy + prepare + run (kv)
- [ ] multiple runs against same deploy
- [ ] teardown idempotent
- [ ] run on nonexistent target
- [ ] deploy with existing name
- [ ] kind mismatch (rpc handle, kv workload)
- [ ] chunk/storage not implemented
- [ ] deploy --web (kv) + teardown

### Lint
- [ ] pixi run cargo fmt --all -- --check
- [ ] pixi run cargo clippy --all-targets -- -D warnings
