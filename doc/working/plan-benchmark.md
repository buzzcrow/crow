<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# R10: Benchmark Framework — Plan

Reference design: [`design-benchmark.md`](design-benchmark.md).

## Task Breakdown

### Task 1: No-fsync WAL mode — DONE

- [x] Add `--no-fsync` flag to `crowkv-server/src/cli.rs` (`Cli` struct).
- [x] Add `wal_skip_fsync: bool` field to `WalConfig`
      (`crowkv/src/common/config.rs`). **Deviation from original plan**:
      a plain `bool` on `WalConfig` is used instead of
      `Arc<AtomicBool>` — the flag is decided once at engine
      construction and never toggled at runtime, so no atomic/shared
      state is needed. Simpler and matches the style of other
      `WalConfig` fields (e.g. `wal_record_format`).
- [x] Thread `skip_fsync` through `WalEngine::create` →
      `spawn_pipeline_writer` → `pipeline_writer_loop` → `write_batch`
      (`crowkv/src/wal/pipeline_writer.rs`) as a plain `bool` param.
- [x] In `write_batch`, skip `segment.fdatasync().await?` when
      `skip_fsync` is true (still update metadata, still ack).
- [x] `KvStoreRegistry` (`crowkv-server/src/store_registry.rs`) gained
      `wal_skip_fsync: bool` + `with_wal_skip_fsync` builder.
      `crowkv-server/src/main.rs` wires `args.no_fsync` into it.
      `create_group_with_wal` (`crowkv-server/src/startup.rs`) gained
      a `skip_fsync: bool` parameter, set on `WalConfig` before
      `WalEngine::create`. All 3 call sites (`main.rs`,
      `mgmt_api.rs` x2) updated; `startup_test.rs` callers updated.
- [x] Unit test `skip_fsync_avoids_durable_flush_but_still_appends`
      (`crowkv/tests/wal/wal_engine_tests.rs`) — asserts
      `fdatasync_count() == 0` while `batch_stats()` still shows the
      record flushed/acked.

### Task 2: Average latency in report — DONE

- [x] Add `avg_us: u64` to `Percentiles` struct
      (`crowkv-console/cli/src/bench/report.rs`).
      - `#[serde(default)]` for back-compat.
      - `Percentiles::empty()` includes `avg_us: 0`.
- [x] `percentiles_from_histogram` computes `avg_us` from
      `h.mean() as u64`.
- [x] `human_summary` prints avg alongside p50/p99.
- [x] Unit tests added: `percentiles_deserializes_without_avg_us_field`,
      `percentiles_from_histogram_computes_mean`,
      `percentiles_from_empty_histogram_is_empty`. Existing
      `bench_cli.rs` integration test (`bench_run_write_smoke`) passes
      unchanged.

### Task 3: Server-side metrics structs + metrics log parser — DONE

- [x] Add `ServerMetrics` + `SystemMetrics` structs to
      `crowkv-console/cli/src/bench/report.rs`.
- [x] Add `#[serde(default)] pub server_metrics: ServerMetrics` field
      to `BenchReport` (and default-populate it in
      `bench/runner.rs::run_bench`, since plain `bench run`/`stress`
      have no deployed nodes to collect from).
- [x] `parse_metrics_log(content: &str) -> ServerMetrics` — scans
      **every** flush block in the file (not just the last), since
      counters/histograms/summaries print only the per-window delta
      count, not a cumulative total (see
      `crowkv::metrics::mod::flush_histograms`/`flush_summaries`).
      **Deviation from original plan**: no `wal.flush.count` field —
      the current metrics implementation has no such counter (WAL
      append latency is tracked via a `LatencySummary` named
      `s.<store>.g.<group>.wal.append.l`, not a flush-count counter).
      `ServerMetrics` therefore has `wal_append_count` (summed from
      that summary's count column), `kv_put_count`/`kv_get_count`
      (summed from the `*.kv.put.lh`/`*.kv.get.lh` histogram count
      columns). `sys.cpu_user_us`/`cpu_sys_us`/`tcp_retrans`/
      `tcp_lost` are deltas-per-flush, summed; `sys.rss_kb` is an
      absolute snapshot, tracked as the run's peak.
- [x] `aggregate_server_metrics(per_node: &[ServerMetrics]) ->
      ServerMetrics` — sums counters and CPU, takes max of RSS and
      TCP stats.
- [x] Unit tests: `parse_metrics_log_sums_across_flush_blocks`,
      `parse_metrics_log_empty_content_is_default`,
      `aggregate_server_metrics_sums_counters_and_maxes_system`.
- **Note**: `parse_metrics_log`/`aggregate_server_metrics` are
  currently only reachable from tests (dead-code until Task 5's
  `BenchFixture` and Task 6's `bench_benchmark` call them) — expected
  interim state, resolved once those tasks land.

### Task 4: Deploy plumbing — pass engine/fsync/metrics flags through — DONE

- [x] Added typed fields to `DeployRequest`
      (`crowkv-console/shared/src/lifecycle.rs`): `kv_engine:
      Option<String>`, `kv_backend: Option<String>`, `no_fsync: bool`,
      `metrics_interval: Option<u64>`. `DeployRequest` now derives
      `Default` (all ~13 existing struct-literal call sites across
      the workspace updated to add `..Default::default()`, since none
      of them need the new benchmark-only fields).
- [x] `deploy_local_in_workspace` appends these as CLI args via a new
      `apply_benchmark_flags` helper (split out to stay under the
      clippy `too_many_lines` limit): `--kv-engine`, `--kv-backend`,
      `--no-fsync`, `--metrics-interval`.
- [x] Added same fields to web's `DeployNodeServerBody`
      (`crowkv-console/web/src/lifecycle.rs`) with `#[serde(default)]`;
      `http_deploy_node_server` forwards them into `DeployRequest`.
      (The restart/redeploy handlers in `web/src/lifecycle.rs` and
      `web/src/mgmt.rs` use `..Default::default()` for these fields —
      persisted `ServerEntry` doesn't carry them, out of scope for
      R10 since the benchmark fixture deploys once and never restarts
      nodes mid-run.)
- [x] Added same fields to `ConsoleClient`'s `DeployNodeServerBody`
      (`crowkv-console/shared/src/clients/console.rs`) with
      `#[serde(default, skip_serializing_if = "Option::is_none")]`;
      also added `election_profile` (was missing from this client
      body, needed by `BenchFixture` to pass `--election-profile
      test`). Derives `Default`; one existing call site
      (`crowkv-console/cli/src/commands/server.rs`) updated with
      `..Default::default()`.
- **Deferred**: SSH deploy path (`ssh::deploy_via_ssh`) does not yet
  forward these flags — out of scope since R10's fixture is
  local-only (SSH path is stubbed/future per the design doc).

### Task 5: BenchFixture — embedded console-web + cluster provisioning — DONE

- [x] Moved `crowkv-web` + `axum` from `[dev-dependencies]` to
      `[dependencies]` in `crowkv-console/cli/Cargo.toml` (needed at
      runtime, not just in tests); added `net` to the `tokio` feature
      list for `TcpListener`. **Deviation from plan**: no separate
      `crowkv-console-web` package exists — the web crate's actual
      name is `crowkv-web` (`crowkv-console/web`).
- [x] Created `crowkv-console/cli/src/bench/provision.rs` with
      `BenchFixture` struct — matches the plan's shape:
      - `BenchFixture::new(mode: BenchMode, workspace_dir: PathBuf) ->
        Result<Self>` — binds an ephemeral `127.0.0.1:0` listener,
        constructs `crowkv_web::AppState::with_config(
        ConsoleConfig::default(), Some(workspace_dir/console.toml))`,
        serves `crowkv_web::router` on a background `tokio::spawn`
        task, then builds a `ConsoleClient` against it.
      - `provision_nodes` — 3x `add_rack("br<i>")` +
        `add_node("bn<i>")` + `deploy_node_server` (auto-allocated
        ports via `crowkv_console_shared::test_ports::unique_test_port`,
        `election_profile="test"`, `metrics_interval=1`, plus
        `BenchMode::apply_to` setting `kv_engine`/`kv_backend`/
        `no_fsync`).
      - `provision_store_and_group` — `add_store(store_id=1,
        nodes=[bn0,bn1,bn2])` then `add_group(group_id=1,
        replica_id=1, nodes=[bn0,bn1,bn2])`.
      - `wait_for_leader_endpoint` polls
        `ConsoleClient::resolve_endpoint(1, 1)` every 200ms up to a
        20s timeout (mirrors `consoleSetup.ts::waitForLeader`).
      - `leader_endpoint()`, `node_ids()` accessors.
      - `collect_metrics() -> ServerMetrics` — locates each node's
        `log/metrics-<ts>-<pid>.log` (the actual filename pattern
        from `crowkv::common::logging::open_metrics_log` — **not**
        literally `metrics.log` as the design assumed), parses with
        `parse_metrics_log`, aggregates with
        `aggregate_server_metrics`.
      - `collect_logs(run_dir: &Path)` — copies every file under each
        node's `log/` dir into `<run_dir>/node-<id>/`.
      - `cleanup(keep_workspace: bool)` — stops all 3 servers via
        `ConsoleClient::stop_node_server`, aborts the embedded
        console-web task, optionally removes the workspace dir.
        Idempotent.
      - `Drop` — safety net if `cleanup()` wasn't called: aborts the
        console-web task and best-effort `stop_pid_with_timeout`
        (500ms) on each node's pid (can't `.await` in `Drop`, so this
        is synchronous SIGTERM, not the full async `cleanup` path).
- [x] Exported `BenchFixture` and `BenchMode` from
      `crowkv-console/cli/src/bench/mod.rs`.
- **Note**: builds clean; remaining dead-code warnings for
  `BenchFixture`/`parse_metrics_log`/`aggregate_server_metrics` are
  expected until Task 6 wires them into the `bench benchmark` verb.

### Task 6: `bench benchmark` verb

- [ ] Add `Benchmark` variant to `BenchVerb` enum
      (`crowkv-console/cli/src/commands/bench.rs`):
      - `--mode <memory|file-nofsync>` (required).
      - `--duration <secs>` (default 60).
      - `--workload <mix|read|write|list>` (default mix).
      - `--threads <n>` (default 8).
      - `--connections <n>` (default 4).
      - `--key-space <n>` (default 1000).
      - `--value-size <n>` (default 64).
      - `--keep-workspace` (flag, default false).
      - `--config <path>` (optional, accepted but stubbed for now).
- [ ] Implement `bench_benchmark` function:
      1. Create workspace dir under
         `~/.crowkv/bench-workspaces/<run-id>/`.
      2. `BenchFixture::new(mode, workspace_dir)` — deploys cluster.
      3. `run_bench(BenchConfig { endpoint: fixture.leader_endpoint(),
         .. })` — drives load.
      4. `fixture.collect_metrics()` — parse + aggregate server
         metrics.
      5. Attach `server_metrics` to `BenchReport`.
      6. `fixture.collect_logs(run_dir)` — bundle artifacts.
      7. Write `report.json` + `manifest.json` to
         `~/.crowkv/bench/<run-id>/`.
      8. Print summary (throughput, avg/p50/p99, WAL metrics, system
         metrics, anomaly flags).
      9. `fixture.cleanup()` (unless `--keep-workspace`).
- [ ] Add `Compare` variant to `BenchVerb`:
      - Two positional args: `<run-id-1> <run-id-2>`.
- [ ] Implement `bench_compare` function:
      - Reads both `report.json` files.
      - Prints side-by-side comparison table (throughput, avg/p50/p99
        latency, error rate, WAL metrics, system metrics).

### Task 7: Tests

- [ ] Unit test: `parse_metrics_log` with sample content.
- [ ] Unit test: `aggregate_server_metrics` with 3-node sample.
- [ ] Unit test: `Percentiles` serde back-compat (old JSON without
      `avg_us`).
- [ ] Unit test: WAL `write_batch` with `skip_fsync=true` (no
      `fdatasync` call).
- [ ] Integration test (`crowkv-console/cli/tests/bench_benchmark.rs`):
      - `bench benchmark --mode memory --duration 3s` runs end-to-end.
      - Report JSON contains `server_metrics` with non-zero
        `kv_put_count` / `kv_get_count`.
      - Workspace is cleaned up after run.
      - `--keep-workspace` retains workspace.
- [ ] Integration test: `bench compare <run1> <run2>` prints
      comparison table.
- [ ] Verify all existing `bench run` / `bench stress` / `bench
      report` tests pass unchanged.

## File List

- `crowkv/src/wal/wal_engine.rs` — add `skip_fsync` field, thread to
  writer.
- `crowkv/src/wal/pipeline_writer.rs` — accept `skip_fsync`, skip
  `fdatasync` in `write_batch`.
- `crowkv-server/src/cli.rs` — add `--no-fsync` flag.
- `crowkv-server/src/main.rs` — read `no_fsync`, pass to registry.
- `crowkv-server/src/startup.rs` — accept `skip_fsync` in
  `create_group_with_wal`.
- `crowkv-server/src/store_registry.rs` — store + forward
  `skip_fsync`.
- `crowkv-console/shared/src/lifecycle.rs` — add typed fields to
  `DeployRequest`, pass as CLI args in `deploy_local_in_workspace`.
- `crowkv-console/shared/src/clients/console.rs` — add fields to
  `DeployNodeServerBody`.
- `crowkv-console/web/src/lifecycle.rs` — add fields to web
  `DeployNodeServerBody`, forward to `DeployRequest`.
- `crowkv-console/cli/Cargo.toml` — add `crowkv-console-web` dep.
- `crowkv-console/cli/src/bench/report.rs` — `avg_us`, `ServerMetrics`
  + `SystemMetrics`, parser, aggregator.
- `crowkv-console/cli/src/bench/provision.rs` (new) — `BenchFixture`.
- `crowkv-console/cli/src/bench/mod.rs` — export new types.
- `crowkv-console/cli/src/commands/bench.rs` — `Benchmark` + `Compare`
  verbs.
- `crowkv-console/cli/tests/bench_benchmark.rs` (new) — integration
  tests.
- `pixi.toml` — add `bench`, `bench-nofsync`, `bench-quick`,
  `bench-compare` tasks.

## Dependency Ordering

```
Task 1 (no-fsync WAL)     ─┐
Task 2 (avg latency)      ─┤
Task 3 (metrics structs)  ─┤── Task 6 (benchmark verb) ── Task 7 (tests)
Task 4 (deploy plumbing)  ─┤
Task 5 (BenchFixture)     ─┘
```

Tasks 1–5 are independent of each other and can be done in parallel.
Task 6 depends on all of 1–5. Task 7 depends on 6.

## Test Checklist

- [ ] `pixi run test-core` — WAL no-fsync unit test.
- [ ] `pixi run test-cli` — bench benchmark + compare integration
      tests, existing bench tests unchanged.
- [ ] `pixi run cargo clippy --all-targets -- -D warnings` passes.
- [ ] `pixi run cargo fmt --all -- --check` passes.

## Acceptance Criteria Mapping

- `bench benchmark --mode memory --duration 10s` end-to-end → Task 5+6
- `bench benchmark --mode file-nofsync --duration 10s` → Task 1+4+5+6
- `--keep-workspace` → Task 6
- `--config <path>` accepted, stubbed → Task 6
- `bench compare` → Task 6
- `avg_us` in report → Task 2
- Server-side metrics in report → Task 3+6
- Anomaly detection → Task 6
- Baseline numbers in `plan-benchmark.md` → after implementation
- Existing bench commands unchanged → Task 7
- `pixi run test-cli` passes → Task 7
- `pixi run cargo clippy` passes → Task 7
