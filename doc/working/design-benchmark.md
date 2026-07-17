<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# R10: Benchmark Framework — Design

## Problem

There is no self-contained benchmark that deploys a server, provisions
topology, drives load, collects server-side metrics + logs, and
produces a report. The existing `bench` CLI subcommand
(`crowkv-cli bench run/stress/report`) is a load generator that dials
an already-running server — it requires the operator to manually start
`crowkv-server`, create a store + group, and resolve the leader
endpoint before invoking `bench run`.

The R10 requirement asks for a full benchmark lifecycle:
- **Deploy** — auto-provision a minimal cluster: 1 rack with 3 nodes
  on localhost, forming a complete Paxos replication group. Uses a
  fixture-based setup (similar to the UI test `setupCluster` pattern in
  `consoleSetup.ts`). Config-driven SSH deployment in the future.
- **Run** — drive KV put/get/delete at configurable concurrency in two
  storage modes: `memory` (in-memory KV engine) and `file-nofsync`
  (crowtree engine with WAL, but without fsync — to isolate path-level
  overhead from disk IO).
- **Collect** — gather server-side perf counters (WAL append rate, KV
  op counts), system metrics (CPU, memory RSS, TCP retransmits), and
  runtime logs from the server process.
- **Cleanup** — stop the server, clean up the workspace.
- **Report** — throughput (ops/s), latency (avg + p50 + p99), TPS,
  success/failure counts, and system resource usage. Baseline numbers
  recorded for future comparison.

## Current State

### What already exists

- **`bench` CLI subcommand**
  (`crowkv-console/cli/src/commands/bench.rs`): `run`, `stress`,
  `report` verbs. Configurable workload (read/write/list/mix),
  connections, threads, duration, key_space, value_size, warmup,
  progress reporting.
- **Load generator** (`crowkv-console/cli/src/bench/runner.rs`):
  Closed-loop per-worker model, HDR latency histograms, JSON report
  files, stress scenarios (burst/soak/hotread). `BenchReport` already
  tracks `total_ops`, `total_errors`, `error_rate`, per-op `ops`/
  `errors`/`not_found` — i.e. TPS and success/failure counts are
  already collected client-side.
- **Server CLI** (`crowkv-server/src/cli.rs`): `--kv-engine memory|
  crowtree`, `--kv-backend text|block`, `--election-profile`,
  `--stores`, `--groups`, `--ports`.
- **Metrics module** (R8, `crowkv/src/metrics/`): `MetricsRegistry`
  with counters, histograms, bandwidths, summaries. `MetricsRunner`
  for periodic flush to `log/metrics.log`. `snapshot(prefix)` for API
  consumption.
- **System metrics** (`crowkv/src/metrics/system.rs`):
  `SystemCollector` collects CPU user/sys time, RSS, TCP
  retransmits/lost. On Linux reads `/proc/self/stat` + `/proc/self/
  status` + `/proc/net/snmp`; on macOS uses `ps -o rss` for RSS (CPU
  deltas stubbed). Already integrated into `MetricsRunner` — flushed
  to `log/metrics.log` in the "misc" section on every tick. This is
  efficient: periodic polling with delta computation, no hot-path
  overhead, no locks. The existing implementation is sufficient for
  benchmark collection.
- **SSH infrastructure** (`crowkv-console/shared/src/ssh.rs`): Full
  SSH session support via `russh` — `Session::connect`, `exec`,
  `deploy_via_ssh`, `stop_via_ssh`, `run_remote`. `NodeEntry` config
  with SSH creds (key/password).
- **Lifecycle** (`crowkv-console/shared/src/lifecycle.rs`):
  `deploy_local_in_dir`, `deploy_via_ssh`, `stop_pid`, `wait_for_ready`,
  `wait_for_leader`, `remote_start_command`. The deploy + health-wait
  + leader-wait + stop pipeline is already there and tested.
- **WAL engine** (`crowkv/src/wal/`): `WalEngine` with `flush_count`
  and `records_flushed` atomic counters. `fdatasync` on every batch
  (the ack contract). No "no-fsync" mode exists.

### What is missing

1. **Auto-provisioning lifecycle**: `bench` requires a pre-existing
   server + topology. No fixture-based deploy (rack → nodes → servers →
   store → group → leader wait) → run → collect → cleanup flow.
2. **No-fsync WAL mode**: The WAL always `fdatasync`s every batch.
   There is no flag to skip fsync for benchmark isolation.
3. **Average latency**: `Percentiles` struct has min/p50/p90/p99/p999/
   max but no `avg_us`.
4. **Server-side metrics in report**: The bench report has client-side
   throughput + latency but no server-side perf counters (WAL append
   rate, KV op counts) or system metrics (CPU, RSS, TCP).
5. **Log + metrics collection**: No mechanism to gather server runtime
   logs and metrics log files after a bench run into a bundle.
6. **Per-mode comparison**: No way to run the same workload across
   modes and compare results side-by-side.

## Proposed Approach

### 1. New `bench benchmark` verb (full lifecycle)

Add a `benchmark` verb to the existing `BenchVerb` enum:

```
crowkv-cli bench benchmark --mode memory --duration 60s
crowkv-cli bench benchmark --mode file-nofsync --duration 60s
```

This verb orchestrates the full lifecycle:

**Phase 1 — Deploy (fixture-based cluster)**
- Every CLI verb routes through `ConsoleClient` — there is no direct
  `crowkv-server` client (`crowkv-console/cli/src/utils/client.rs`).
  `bench run` already resolves its leader endpoint through a running
  console-web instance (`resolve_bench_endpoint` in
  `crowkv-console/cli/src/commands/bench.rs`). The `benchmark` verb
  therefore first starts an **embedded console-web server**
  in-process: binds an ephemeral localhost port, constructs
  `AppState::with_config(ConsoleConfig::default(), ...)`, and serves
  `crowkv_web::router` on a background task — the same pattern used
  by the CLI's own integration test harness
  (`crowkv-console/cli/tests/testkit/console.rs::spawn_console_empty`).
  This avoids spawning a second binary and keeps the benchmark
  self-contained.
- Creates a temp workspace directory (e.g. `~/.crowkv/bench-workspaces/
  <run-id>/`).
- Provisions a minimal cluster topology — 1 rack, 3 nodes — through
  the embedded console-web's `ConsoleClient`, following the same
  sequence as the UI test fixture
  (`crowkv-console/web/ui/e2e/fixtures/consoleSetup.ts::setupCluster`),
  but via the typed Rust client instead of raw HTTP:
  - `ConsoleClient::add_rack` — create 1 rack (e.g. `br0`).
  - `ConsoleClient::add_node` — create 3 nodes under the rack (e.g.
    `bn0`, `bn1`, `bn2`), each with `host = 127.0.0.1`.
  - Deploy a `crowkv-server` process on each node (auto-allocated
    mgmt + gRPC port pairs) via the node deploy endpoint.
  - `ConsoleClient::add_store` — create 1 store spanning all 3 nodes.
  - `ConsoleClient::add_group` — create 1 group with 3 replicas (one
    per node) and wait for leader election.
- Each server is spawned with the appropriate flags:
  - `memory` mode: `--kv-engine memory`
  - `file-nofsync` mode: `--kv-engine crowtree --kv-backend text
    --no-fsync`
  - Common: `--election-profile test`, `--metrics-interval 1` (1-second
    flush for fine-grained system metrics during short bench runs).
  - No `--stores`/`--groups` flags — multi-replica group formation is
    driven by console-web's orchestrated `add_store`/`add_group`, not
    a server's local auto-bootstrap (`crowkv-server/src/cli.rs`).
- Reuses `wait_for_ready` + `wait_for_leader` from the shared
  lifecycle module.
- The fixture is a Rust struct (e.g. `BenchFixture`) that encapsulates
  the embedded console-web startup, topology creation via
  `ConsoleClient`, server deployment, and teardown — analogous to
  `setupCluster` / `teardownCluster` in the UI test fixtures.

**Phase 2 — Run**
- Calls the existing `run_bench()` with a default `BenchConfig`
  (workload=mix, threads=8, connections=4, key_space=1000,
  value_size=64 — overridable via CLI flags).
- The load generator already collects client-side TPS, success/failure
  counts, and per-op latency histograms.

**Phase 3 — Collect**
- After the bench run, reads `log/metrics.log` from each of the 3
  node workspaces. Each file contains periodic flush blocks with:
  - Counter lines: `wal.append.count`, `wal.flush.count`,
    `kv.put.count`, `kv.get.count`, etc.
  - Histogram lines: `kv.put.latency`, `kv.get.latency` (server-side
    p50/p99/avg).
  - System lines: `sys.cpu_user_us`, `sys.cpu_sys_us`, `sys.rss_kb`,
    `sys.tcp_retrans`, `sys.tcp_lost`.
- Also reads each server's stdout/stderr log file
  (`log/crowkv-server-<pid>.out.log`) for runtime error/warning
  detection.
- Aggregates metrics across all 3 nodes for the report: counters
  (`wal_append_count`, `wal_flush_count`, `kv_put_count`,
  `kv_get_count`) are summed; `rss_kb` and `tcp_retransmits`/
  `tcp_lost` take the max across nodes; `cpu_user_us`/`cpu_sys_us`
  are summed (aggregate CPU time across the replica set).
- Bundles all artifacts into a single directory:
  `~/.crowkv/bench/<run-id>/` containing:
  - `report.json` — the `BenchReport` with aggregated server metrics
  - `node-<id>/metrics.log` — copied server metrics log per node
  - `node-<id>/server.out.log` — copied server stdout/stderr per node
  - `manifest.json` — run metadata (mode, duration, server version,
    timestamps, topology: rack + node IDs, workspace paths)

**Phase 4 — Cleanup**
- Stops all 3 server processes via `ConsoleClient`'s node `server/stop`
  call (falls back to `stop_pid` with SIGTERM + wait if unreachable).
- Shuts down the embedded console-web background task.
- Optionally removes the workspace directory (`--keep-workspace` to
  retain).

**Phase 5 — Report**
- Prints a summary to stdout: throughput, latency (avg + p50 + p99),
  error rate, WAL metrics, system resource usage.
- Flags anomalies: non-zero error rate, TCP retransmits, unexpected
  warnings in server log.

**Why not a separate `benchmark` top-level command?** The existing
`bench` subcommand already has the load generator, report format, and
stress scenarios. Adding `benchmark` as a verb reuses all of that
while adding the lifecycle orchestration.

**Future: config-driven cluster deployment.** The `benchmark` verb
accepts an optional `--config <path>` flag pointing to a TOML file
with `[[nodes]]` entries (same format as `NodeEntry`). When provided,
the deploy phase uses `deploy_via_ssh` instead of local process
spawning, and the collect phase uses `ssh::run_remote` to fetch log
files from each node. For now, the default is the local 3-node
fixture (1 rack, 3 localhost nodes) with no SSH.

### 2. No-fsync WAL mode

Add a `--no-fsync` flag to `crowkv-server`:

- `crowkv-server/src/cli.rs`: new `#[arg(long)] pub no_fsync: bool`
  field.
- `crowkv-server/src/main.rs`: when `no_fsync` is true, set an
  `Arc<AtomicBool>` shared with the WAL engine.
- `crowkv/src/wal/wal_engine.rs`: `WalEngine` gets an optional
  `skip_fsync: Arc<AtomicBool>`. When true, `write_batch` skips the
  `segment.fdatasync()` call — the data is written to the file but
  not durably flushed. This is unsafe for production but valid for
  benchmarking disk-IO-isolated path overhead.
- The flag is threaded through `WalEngine::create` →
  `pipeline_writer_loop` → `write_batch`.

**Alternative considered**: An env var `CROWKV_WAL_NO_FSYNC=1`.
Rejected per project convention — prefer explicit CLI flags over env
vars.

### 3. Average latency in report

Add `avg_us: u64` to the `Percentiles` struct in
`crowkv-console/cli/src/bench/report.rs`:

- `Percentiles` gains `avg_us` alongside the existing min/p50/p90/
  p99/p999/max.
- `percentiles_from_histogram` computes `avg_us` from
  `h.mean() as u64`.
- `OpReport` and `BenchReport` serialization automatically include
  the new field. Historical reports without `avg_us` deserialize with
  `#[serde(default)]` = 0.
- `human_summary` prints avg alongside p50/p99.

### 4. Server-side metrics in bench report

Add a `server_metrics: ServerMetrics` field to `BenchReport`:

```rust
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct ServerMetrics {
    /// WAL records appended (total).
    pub wal_append_count: u64,
    /// WAL flush batches (total).
    pub wal_flush_count: u64,
    /// KV put count (total, server-side).
    pub kv_put_count: u64,
    /// KV get count (total, server-side).
    pub kv_get_count: u64,
    /// System metrics (last flush block during bench window).
    pub system: SystemMetrics,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct SystemMetrics {
    pub cpu_user_us: u64,
    pub cpu_sys_us: u64,
    pub rss_kb: u64,
    pub tcp_retransmits: u64,
    pub tcp_lost: u64,
}
```

Populated by parsing the server's `log/metrics.log` file after the
bench run. The parser reads the last flush block (or all blocks
within the bench time window) and extracts the relevant lines.

`#[serde(default)]` on the `server_metrics` field ensures historical
reports without server metrics still deserialize.

### 5. Report comparison

Add a `bench compare` verb that takes two run-ids and prints them
side-by-side:

```
crowkv-cli bench compare <run-id-1> <run-id-2>
```

This reads two JSON report files and prints a diff table showing
throughput, latency (avg/p50/p99), error rate, WAL metrics, and
system metrics for both runs. Simple, no new infrastructure needed.

## Alternatives Considered

- **Separate `benchmark` binary**: Rejected — duplicates the load
  generator and report infrastructure already in `bench`.
- **In-process server for memory mode**: Start a `PxKvStore` in the
  CLI process instead of spawning a child. Rejected — the CLI is a
  thin client; embedding the server would blur the architecture
  boundary and require linking `crowkv-server` as a library.
- **Env var for no-fsync**: Rejected per project convention (prefer
  explicit CLI flags over env vars).
- **New management API endpoint for metrics**: Rejected — reading the
  existing `log/metrics.log` file is simpler and avoids adding a new
  API route. The metrics log already contains all needed data in a
  structured text format.

## Acceptance Criteria

- `crowkv-cli bench benchmark --mode memory --duration 10s` runs
  end-to-end: deploys a 3-node cluster (1 rack, 3 nodes), provisions
  store + group, drives load, collects metrics + logs from all nodes,
  prints report with throughput + avg/p50/p99 latency + WAL metrics +
  system metrics, cleans up.
- `crowkv-cli bench benchmark --mode file-nofsync --duration 10s`
  does the same with crowtree engine + no-fsync WAL.
- `--keep-workspace` flag retains the workspace for debugging.
- `--config <path>` flag is accepted but only the local 3-node
  fixture (1 rack, 3 nodes) is implemented in this iteration (SSH
  path stubbed for future).
- `crowkv-cli bench compare <run1> <run2>` prints a side-by-side
  comparison table.
- Report includes `avg_us` latency alongside p50/p99.
- Report includes server-side metrics: WAL append/flush counts, KV
  put/get counts, CPU/RSS/TCP from system metrics.
- Report includes anomaly detection: non-zero error rate, TCP
  retransmits, server log warnings.
- Baseline numbers for both modes are recorded in
  `doc/working/plan-benchmark.md`.
- All existing `bench run` / `bench stress` / `bench report` commands
  continue to work unchanged.
- `pixi run test-cli` passes (includes new tests for the `benchmark`
  verb).
- `pixi run cargo clippy --all-targets -- -D warnings` passes.

## Files to Change

- `crowkv-console/cli/src/commands/bench.rs` — add `Benchmark` and
  `Compare` verbs to `BenchVerb`, implement `bench_benchmark` and
  `bench_compare` functions.
- `crowkv-console/cli/src/bench/mod.rs` — export new helpers.
- `crowkv-console/cli/src/bench/report.rs` — add `avg_us` to
  `Percentiles`, add `ServerMetrics` + `SystemMetrics` structs, add
  `server_metrics` field to `BenchReport`, add comparison rendering,
  add metrics log parser.
- `crowkv-console/cli/src/bench/provision.rs` (new) — `BenchFixture`
  struct: starts an embedded console-web server in-process, then
  rack/node/store/group provisioning via `ConsoleClient` (mirrors
  `consoleSetup.ts` `setupCluster`), server spawn per node,
  health/leader wait, metrics log parsing, log file collection from
  all 3 nodes, server stop + workspace cleanup (mirrors
  `teardownCluster`), embedded console-web shutdown.
- `crowkv-console/cli/Cargo.toml` — add `crowkv-console-web` as a
  dependency (for the embedded router/`AppState` used by
  `BenchFixture`).
- `crowkv-server/src/cli.rs` — add `--no-fsync` flag.
- `crowkv-server/src/main.rs` — read `no_fsync`, pass `Arc<AtomicBool>`
  to `KvStoreRegistry`.
- `crowkv-server/src/startup.rs` — accept `skip_fsync` in
  `create_group_with_wal`, forward to `WalEngine::create`.
- `crowkv-server/src/store_registry.rs` — store + forward
  `skip_fsync` to `create_group_with_wal`.
- `crowkv/src/wal/wal_engine.rs` — accept `skip_fsync` flag, thread to
  writer tasks.
- `crowkv/src/wal/pipeline_writer.rs` — accept `skip_fsync`, skip
  `fdatasync` in `write_batch`.
- `crowkv-console/shared/src/lifecycle.rs` — add `kv_engine`,
  `kv_backend`, `no_fsync`, `metrics_interval` typed fields to
  `DeployRequest`; append as CLI args in `deploy_local_in_workspace`.
- `crowkv-console/shared/src/clients/console.rs` — add same fields to
  `DeployNodeServerBody` with `#[serde(default)]`.
- `crowkv-console/web/src/lifecycle.rs` — add same fields to web
  `DeployNodeServerBody`, forward into `DeployRequest` in
  `http_deploy_node_server`.
