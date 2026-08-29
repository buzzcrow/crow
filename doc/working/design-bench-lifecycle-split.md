<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Bench Lifecycle Split (R124)

Design draft for splitting the monolithic `bench kv` flow into
discrete deploy/prepare/run/teardown CLI verbs with named runtime
folders and multi-kind deploy dispatch.

- Backlog: `doc/backlog/R124-console-bench-lifecycle-split.md`
- Root design: `doc/design/console/design-crowdb-console.md`
  (console-web, SSH/local-fork lifecycle, bootstrap).
- Already landed: the `BenchTarget` trait
  (`app/crowdb-cli/src/bench/target.rs`) with `provision` /
  `pre_populate` / `cleanup` / `run_workers` methods; concrete
  `KvTarget` (`bench/targets/kv.rs`) + `RpcTarget`
  (`bench/targets/rpc.rs`); the monolithic `run_bench` runner
  (`bench/runner.rs`); the `BenchFixture` embedded-console provision
  path (`bench/targets/kv.rs`); `ConsoleClient`
  (`crowdb-console-shared/src/clients/console.rs`) with
  `deploy_node_server` / `stop_node_server` / `cluster_init` /
  `add_store` / `add_group`; `lifecycle::deploy_local`
  (`crowdb-console-shared/src/lifecycle.rs`) which spawns
  `crowdb-kv-server` with `kill_on_drop(false)` (survives parent
  exit); standalone `crowdb-web` binary (`app/crowdb-web/src/main.rs`)
  with `--test-mode` (in-memory registry) + `--port` flags.

Architecture decisions and rationale are in the root design; this doc
does not repeat them.

## 1. ClusterHandle — persistent deploy metadata

### 1.1 Why

The current `BenchFixture` holds all deploy state (console task, node
pids, endpoints, workspace) in-process. When the CLI exits, the state
is gone — a subsequent `bench run` cannot attach to the same cluster.
A serializable handle persisted at `runtime/<name>/handle.json` is the
bridge: `bench deploy` writes it, every other verb reads it.

### 1.2 Struct + persistence

New file: `app/crowdb-cli/src/bench/handle.rs`.

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ClusterHandle {
    pub name: String,
    pub kind: DeployKind,           // kv | rpc | chunk | storage
    pub store_id: u64,
    pub group_id: u64,
    pub leader_endpoint: String,     // crowdb-rpc URL of the elected leader
    pub node_ids: Vec<u64>,
    pub node_pids: Vec<u32>,         // for teardown SIGTERM
    pub node_rpc_urls: Vec<String>,
    pub node_mgmt_urls: Vec<String>, // per-node mgmt API (for flush, topology)
    pub workspace_dir: PathBuf,      // relative to runtime/<name>/
    pub mode: String,                // "mem" | "file" | "block-device" | "rpc"
    pub tunables: HandleTunables,    // snapshot of deploy-time tunables
    pub console_url: Option<String>, // Some when --web
    pub console_pid: Option<u32>,    // Some when --web (standalone crowdb-web pid)
    pub created_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum DeployKind { Kv, Rpc, Chunk, Storage }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct HandleTunables {
    pub max_inflight: usize,
    pub metrics_interval: u64,
    pub peer_pool_size: usize,
    pub enable_nagle: bool,
    pub quickack: bool,
    pub event_write: bool,
    pub send_queue_capacity: u32,
}
```

Methods:
- `ClusterHandle::runtime_dir(name) -> PathBuf` — returns
  `runtime/<name>/`. Creates the `runtime/` root if missing.
- `ClusterHandle::save(&self) -> Result<PathBuf>` — serializes to
  `runtime/<name>/handle.json`.
- `ClusterHandle::load(name) -> Result<Self>` — reads + deserializes.
  Error if file missing: lists existing deploys under `runtime/`.
- `ClusterHandle::list_deploys() -> Vec<String>` — scans
  `runtime/*/handle.json` for valid deploy names.

Edge cases:
- `runtime/<name>/` already exists (re-deploy) → error: "deploy
  `<name>` already exists; teardown first." Prevents silent overwrite
  of a running cluster's handle.
- Handle file corrupted / partial → `load` returns a clear error;
  `teardown` still works by reading whatever fields are available
  (pids are the critical field).
- `runtime/` directory missing → `save` creates it.

## 2. bench deploy verb

### 2.1 Why

Today `bench kv` provisions + measures + tears down in one process.
The deploy verb extracts just the provision step, persists the handle,
and leaves the cluster running so many `bench run` invocations can
attach to it.

### 2.2 CLI shape

New `BenchSub` variants in `commands/bench.rs`:

```rust
pub enum BenchSub {
    Kv(Box<KvArgs>),
    Rpc(Box<RpcArgs>),
    Deploy(Box<DeployArgs>),
    Prepare(Box<PrepareArgs>),
    Run(Box<RunArgs>),
    Teardown(Box<TeardownArgs>),
    Report { run_id: String },
    Compare { run_id_1: String, run_id_2: String },
}
```

`DeployArgs` reuses the existing `KvArgs` tunables (mode, max_inflight,
metrics_interval, etc.) and adds:

```rust
#[derive(Args, Debug)]
pub struct DeployArgs {
    #[arg(long)]
    pub name: String,
    #[arg(long, default_value = "kv")]
    pub kind: String,           // kv | rpc | chunk | storage
    #[arg(long, default_value_t = false)]
    pub web: bool,              // opt-in console-web
    // ... all KvArgs fields (mode, max_inflight, etc.) via #[command(flatten)]
}
```

### 2.3 KV deploy flow (headless, default)

Reuses `BenchFixture::new` (embedded console-web provision path)
then detaches:

a. Create `runtime/<name>/` + `runtime/<name>/workspace/`.
b. Call `BenchFixture::new(mode, workspace_dir, ...tunables)` —
   starts embedded console-web, deploys 3 nodes, cluster_init,
   add_store/add_group, wait leader, wait healthy.
c. Extract handle fields from the fixture: node_ids, pids,
   rpc_urls, mgmt_urls, leader_endpoint, workspace_dir.
d. Build `ClusterHandle`, save to `runtime/<name>/handle.json`.
e. Call `BenchFixture::detach()` (new method) — marks `stopped =
   true` (so `Drop` is a no-op) and aborts the console task, but
   does NOT stop the `crowdb-kv-server` processes. Their pids are
   recorded in the handle; `bench teardown` will SIGTERM them.
f. Print "deployed cluster `<name>` (kind=kv, mode=mem, 3 nodes,
   leader=<endpoint>)".

The `crowdb-kv-server` child processes survive CLI exit because
`lifecycle::deploy_local` spawns them with `kill_on_drop(false)`.

### 2.4 KV deploy flow (--web)

When `--web` is passed, the console-web must survive CLI exit so the
operator can use the UI. Instead of an embedded console-web, spawn the
standalone `crowdb-web` binary as a detached child:

a. Create `runtime/<name>/` + `runtime/<name>/workspace/`.
b. Pick a free port for console-web via `unique_test_port()`.
c. Spawn `crowdb-web --test-mode --port <port>` as a detached child
   (`Command::process_group(0)` + `Stdio::null()` for stdout/stderr).
   Record pid.
d. Wait for the console-web to bind (poll `GET /health`).
e. Build `ConsoleClient::new("http://127.0.0.1:<port>")`.
f. Run the same provisioning logic as `BenchFixture::new` steps 2-7
   (add racks, nodes, deploy servers, cluster_init, store/group,
   wait leader, wait healthy) — factored into a shared function
   `provision_kv_cluster(client, mode, workspace_dir, tunables)`
   that both headless and `--web` paths call.
g. Build `ClusterHandle` with `console_url` + `console_pid` set,
   save.
h. Print "deployed cluster `<name>` (kind=kv, mode=mem, 3 nodes,
   web=<url>)".

The standalone `crowdb-web` process survives CLI exit (detached +
process group). `bench teardown` kills it by pid.

### 2.5 Shared provisioning function

Extract the provisioning logic from `BenchFixture::new` into a
standalone async function in `bench/targets/kv.rs`:

```rust
pub(crate) async fn provision_kv_cluster(
    client: &ConsoleClient,
    mode: BenchMode,
    workspace_dir: &Path,
    tunables: &KvTunables,
) -> Result<KvClusterInfo>
```

Where `KvClusterInfo` is a plain struct with node_ids, pids,
rpc_urls, mgmt_urls, leader_endpoint. Both `BenchFixture::new`
(headless) and the `--web` deploy path call this function. The
headless path creates an embedded console-web and passes its
`ConsoleClient`; the `--web` path creates a standalone console-web
and passes its `ConsoleClient`.

`BenchFixture::new` is refactored to:
1. Start embedded console-web.
2. Call `provision_kv_cluster`.
3. Return the fixture (holding the console task + `KvClusterInfo`).

### 2.6 BenchFixture::detach

New method on `BenchFixture`:

```rust
impl BenchFixture {
    /// Detach the fixture: abort the embedded console-web task but
    /// leave the deployed servers running. The pids are recorded
    /// elsewhere (in the ClusterHandle). After detach, Drop is a
    /// no-op (stopped = true).
    pub fn detach(&mut self) {
        if self.stopped { return; }
        self.stopped = true;
        self.console_task.abort();
    }
}
```

### 2.7 RPC deploy flow

`bench deploy --kind rpc` spawns the `crowdb-rpc-fb-server` binary as
a detached child (same mechanism as `--web` console-web: process
group + null stdio). The handle records the fb server pid + port.
`bench run --target <name>` connects to it; `bench teardown` kills
it.

The fb server binary is located via `crowdb_rpc_fb_server_bin()`
(already in `bench/targets/rpc.rs` and `tests/common/console.rs`).

### 2.8 Chunk/storage deploy

Return a clear "not yet implemented" error. The `DeployKind` enum
has the variants so the handle format is forward-compatible.

### 2.9 Edge cases

- Deploy name already exists → error (see §1.2).
- `crowdb-kv-server` binary not found → error from
  `lifecycle::deploy_local` (already handled).
- `crowdb-web` binary not found (`--web` mode) → error: "crowdb-web
  binary not found; run `pixi run build` or omit `--web` for
  headless deploy."
- Provisioning fails mid-deploy (some nodes up, some not) → the
  deploy verb cleans up: SIGTERMs any spawned node pids, aborts
  console task, removes `runtime/<name>/`. Returns the error.
- `--web` console-web fails to bind → error, no nodes spawned yet.

## 3. bench prepare verb

### 3.1 Why

Extracts the `KvTarget::pre_populate` sequential `put` loop into a
standalone verb so data can be loaded once and reused across many
`bench run` invocations.

### 3.2 CLI shape

```rust
#[derive(Args, Debug)]
pub struct PrepareArgs {
    #[arg(long)]
    pub target: String,
    #[arg(short = 'P', long)]
    pub keys: u64,
    #[arg(short = 's', long, default_value_t = 512)]
    pub value_size: usize,
    #[arg(short = 'x', long)]
    pub value_size_mix: Option<String>,
}
```

### 3.3 Flow

a. Load handle for `--target <name>`.
b. Validate `kind == Kv` (prepare is kv-only; rpc returns "prepare
   not applicable to kind=rpc").
c. Build `CrowdbClient` seeded from `handle.leader_endpoint`.
d. Run the sequential `put` loop for `0..keys` (same logic as
   `KvTarget::pre_populate`: format_key, value_for, retry on
   NotLeader up to 8 attempts).
e. Print "prepared <keys> keys in <ms>ms (<errors> errors)".

### 3.4 Edge cases

- Handle not found → clear error listing existing deploys.
- Leader unreachable → error: "cluster `<name>` not running —
  redeploy."
- `keys == 0` → no-op, prints "nothing to prepare."

## 4. bench run verb

### 4.1 Why

The measurement-only path: reads the handle, builds workers from the
recorded leader endpoint, runs the workload, reports to
`runtime/<name>/`. Skips provision, pre-populate, and cleanup.

### 4.2 CLI shape

```rust
#[derive(Args, Debug)]
pub struct RunArgs {
    #[arg(long)]
    pub target: String,
    // Workload args (same as KvArgs workload subset):
    #[arg(short = 'w', long, default_value = "mix")]
    pub workload: String,
    #[arg(short = 'd', long, default_value_t = 20)]
    pub duration_secs: u64,
    #[arg(short = 'L', long, default_value_t = 8)]
    pub loader_num: u32,
    #[arg(short = 'c', long, default_value_t = 4)]
    pub connections: u32,
    #[arg(short = 'k', long, default_value_t = 1_000_000)]
    pub key_space: u64,
    #[arg(short = 's', long, default_value_t = 512)]
    pub value_size: usize,
    #[arg(short = 'R', long, default_value = "linearizable")]
    pub read_mode: String,
    #[arg(long, default_value = "auto")]
    pub min_slot: String,
    #[arg(short = 'e', long)]
    pub read_endpoint_policy: Option<String>,
    #[arg(short = 'v', long, default_value_t = 8)]
    pub verify_bytes: usize,
    #[arg(short = 'l', long, default_value_t = 1)]
    pub scan_limit: u32,
    #[arg(long, default_value = "")]
    pub scan_prefix: String,
    #[arg(long, default_value = "")]
    pub scan_start_after: String,
    #[arg(short = 'r', long)]
    pub run_id: Option<String>,
    // ... (other workload args as needed)
}
```

### 4.3 AttachedKvTarget

New struct in `bench/targets/kv.rs` that implements `BenchTarget`
with no-op provision/cleanup and builds its client from a handle:

```rust
pub(crate) struct AttachedKvTarget {
    handle: ClusterHandle,
    client: Option<Arc<CrowdbClient>>,
    // topology_seed for distributed read policies
}
```

- `provision()` — no-op, returns `Ok(())`. The cluster is already
  deployed.
- `build_client()` — returns a `KvBenchClient` wrapping the shared
  `CrowdbClient` (built in the `bench run` verb from the handle's
  leader endpoint + tunables).
- `pre_populate()` — no-op, returns `(0, 0)`.
- `cleanup()` — no-op.
- `run_workers()`, `spawn_progress()`, `spawn_metrics_flusher()`,
  `client_metrics_snapshot()`, `client_transport_stats()`,
  `node_ids()`, `workspace_dir()`, `endpoint_to_node_map()`,
  `collect_artifacts()`, `flush_mgmt_urls()` — all delegate to the
  handle's data, same logic as `KvTarget`.

### 4.4 Flow

a. Load handle for `--target <name>`.
b. Validate `kind` matches the workload (kv handle → kv workload;
  rpc handle → rpc workload).
c. Build `BenchConfig` from the run args + handle's store_id /
  group_id / mode / tunables.
d. Build `AttachedKvTarget` from the handle.
e. Call `run_bench(&mut target, cfg)` — same runner as the legacy
  `bench kv` path. `provision()` is a no-op, `pre_populate` is None,
  `cleanup()` is a no-op. The runner builds workers, runs the
  workload, collects stats, writes the report to `runtime/<name>/`.
f. Collect server metrics from the handle's node mgmt URLs (same
  `collect_artifacts` logic as `KvTarget`).
g. Print the report summary + paths.

The report goes to `runtime/<name>/` (not `bench-runs/`), with a
timestamped subfolder per run: `runtime/<name>/runs/<timestamp>/`.

### 4.5 Legacy bench kv preserved

The existing `bench kv` subcommand (no `--target`) is kept as the
all-in-one path for quick one-shot benches. No changes to its
behavior.

### 4.6 Edge cases

- Handle not found → clear error listing existing deploys.
- Kind mismatch (kv handle, rpc workload) → error.
- Leader unreachable → `run_bench` fails with the existing
  connection-error path; the `bench run` verb reports the error
  (does NOT tear down the cluster — the operator may want to
  investigate or re-elect).
- Report dir already has a run with the same timestamp → appends
  `_<N>` suffix.

## 5. bench teardown verb

### 5.1 Why

Extracts `KvTarget::cleanup` (stop servers) into a standalone verb
that reads the handle. Idempotent; safe after partial/crashed deploys.

### 5.2 CLI shape

```rust
#[derive(Args, Debug)]
pub struct TeardownArgs {
    #[arg(long)]
    pub target: String,
    #[arg(long, default_value_t = false)]
    pub force: bool,  // reap orphans not in the handle
}
```

### 5.3 Flow

a. Load handle for `--target <name>` (best-effort: if handle is
   missing/corrupted, scan `runtime/<name>/` for partial data).
b. For kv: SIGTERM each node pid via `stop_pid_with_timeout`
   (reuses `crowdb_console_shared::lifecycle::stop_pid_with_timeout`).
   For `--web` deploys: also kill the console-web pid.
   For rpc: kill the fb server pid.
c. Wait briefly for processes to exit (best-effort; don't block
   forever).
d. Remove `runtime/<name>/handle.json` (mark as torn down). Leave
   the rest of `runtime/<name>/` (logs, reports) for post-mortem.
e. Print "torn down cluster `<name>` (stopped N nodes)".

Idempotent: if the handle is already missing or pids are already
dead, `stop_pid_with_timeout` is a no-op. A second `teardown` prints
"cluster `<name>` already torn down" and exits 0.

### 5.4 Edge cases

- Handle missing → check if `runtime/<name>/` exists; if yes, print
  "cluster `<name>` has no handle — cannot determine pids. Use
  --force to scan for orphan processes." Without `--force`, exit 1.
- `--force` → scan for `crowdb-kv-server` processes whose `--root`
  points under `runtime/<name>/` and SIGTERM them. (Best-effort;
  relies on `/proc/<pid>/cmdline` on Linux.)
- Partial teardown (some pids dead, some alive) → SIGTERM all pids
  in the handle; report which were already dead.
- Console-web pid already dead → no-op, continue with node pids.

## 6. Regression script rewrite (read/scan)

### 6.1 Why

The read/scan regression scripts invoke `bench kv` once per sub-test,
paying deploy + pre-pop overhead 11× (read) / 14× (scan). The rewrite
deploys once, prepares once, runs N sub-tests, tears down once.

### 6.2 Read script rewrite

`tools/bench-kv-read-regression.sh`:

a. `bench deploy --name kv-read --kind kv --mode mem`
b. `bench prepare --target kv-read --keys 100000 --value-size 64`
c. For each sub-test: `bench run --target kv-read --workload read
   --duration-secs 20 --loader-num N --connections C --read-mode ...
   --read-endpoint-policy ... --verify-bytes V --json`
d. `bench teardown --target kv-read`

The `run_bench` shell helper is replaced by a `run_subtest` helper
that calls `bench run --target kv-read` with the sub-test args.

A `--keys` env override (`KEYSPACE=10000000`) changes the prepare
count, enabling 10M-key runs (amortized pre-pop).

### 6.3 Scan script rewrite

Same shape as read, with `--workload list` + scan-specific args
(`--scan-limit`, `--scan-prefix`, `--scan-start-after`,
`--value-size-mix`).

### 6.4 Write script — unchanged (deferred to R125)

The write regression script keeps the current redeploy-per-sub-test
flow until R125 lands the `bench clean` verb.

## 7. Multi-kind deploy dispatch

### 7.1 Why

`--kind` routes to the matching provision path. Kv is the first
concrete kind; rpc spawns the fb server; chunk/storage are reserved.

### 7.2 Dispatch

In the `bench deploy` verb handler:

```rust
match kind {
    DeployKind::Kv => deploy_kv(name, web, tunables, handle_dir).await,
    DeployKind::Rpc => deploy_rpc(name, handle_dir).await,
    DeployKind::Chunk | DeployKind::Storage => {
        Err(Error::Config(format!("kind {:?} not yet implemented", kind)))
    }
}
```

`deploy_rpc` spawns `crowdb-rpc-fb-server --port <port>` as a
detached child, waits for bind, writes a handle with `kind=rpc`,
`node_pids=[fb_pid]`, `node_rpc_urls=["http://127.0.0.1:<port>"]`.

## Scope

- `app/crowdb-cli/src/bench/handle.rs` — **new**: `ClusterHandle`,
  `DeployKind`, `HandleTunables`, save/load/list.
- `app/crowdb-cli/src/bench/mod.rs` — add `mod handle;` + re-exports.
- `app/crowdb-cli/src/bench/targets/kv.rs` — extract
  `provision_kv_cluster` + `KvClusterInfo`; add `BenchFixture::detach`;
  add `AttachedKvTarget`.
- `app/crowdb-cli/src/commands/bench.rs` — add `Deploy`, `Prepare`,
  `Run`, `Teardown` subcommands + arg structs; dispatch in
  `run_bench_verb`.
- `app/crowdb-cli/src/commands/bench/bench_deploy.rs` — **new**:
  `bench_deploy` handler (kv headless + --web + rpc + chunk/storage
  stub).
- `app/crowdb-cli/src/commands/bench/bench_prepare.rs` — **new**:
  `bench_prepare` handler (load handle, build client, put loop).
- `app/crowdb-cli/src/commands/bench/bench_run.rs` — **new**:
  `bench_run` handler (load handle, build AttachedKvTarget, call
  `run_bench`).
- `app/crowdb-cli/src/commands/bench/bench_teardown.rs` — **new**:
  `bench_teardown` handler (load handle, SIGTERM pids, remove
  handle).
- `app/crowdb-cli/src/commands/bench/bench_kv.rs` — no changes
  (legacy `bench kv` preserved as-is).
- `tools/bench-kv-read-regression.sh` — rewrite to deploy → prepare
  → run × N → teardown.
- `tools/bench-kv-scan-regression.sh` — same rewrite.
- `app/crowdb-cli/tests/bench_benchmark_test.rs` — add integration
  tests for deploy/prepare/run/teardown lifecycle.

## Complexity

**Medium.** The core challenge is the process-lifetime split: the
current `BenchFixture` holds everything in-process, and the `Drop`
impl SIGTERMs the servers. The `detach()` method + `ClusterHandle`
persistence bridge this. The `AttachedKvTarget` reuses the existing
`run_bench` runner with no-op provision/cleanup — no new measurement
code. The `--web` path (standalone `crowdb-web` spawn) is the most
new code but reuses the existing provisioning logic via the shared
`provision_kv_cluster` function. The regression script rewrite is
mechanical. The rpc deploy path is new but small (spawn fb server,
write handle).

## Test Design

### Unit tests (UT)

- `ClusterHandle::save` + `load` round-trip: write a handle, read it
  back, verify all fields match. UT.
- `ClusterHandle::load` on missing name: returns error listing
  existing deploys. UT.
- `ClusterHandle::save` on existing name (re-deploy): returns error
  "already exists". UT.
- `ClusterHandle::list_deploys` with 0/1/3 deploys under
  `runtime/`. UT.
- `DeployKind` parse: "kv" → Kv, "rpc" → Rpc, "chunk" → Chunk,
  "storage" → Storage, "bad" → error. UT.

### End-to-end tests (E2E)

All E2E tests spawn the compiled `crowdb-cli` binary + require
`crowdb-kv-server` built. They use the existing `bench_lock()` mutex
to avoid port conflicts.

- **Deploy + run + teardown (headless kv)**: `bench deploy --name
  t1 --kind kv --mode mem` → exit 0, handle exists at
  `runtime/t1/handle.json`. `bench run --target t1 --workload read
  --duration-secs 1 --loader-num 2 --connections 2 --key-space 100
  --value-size 32` → exit 0, report in `runtime/t1/`. `bench
  teardown --target t1` → exit 0, handle removed. A second
  `teardown --target t1` → exit 0 (idempotent). E2E.
- **Deploy + prepare + run (kv)**: `bench deploy --name t2 --kind
  kv` → `bench prepare --target t2 --keys 100 --value-size 32` →
  `bench run --target t2 --workload read --duration-secs 1
  --key-space 100` → exit 0, 0 correctness_errors. `bench teardown
  --target t2`. E2E.
- **Multiple runs against same deploy**: `bench deploy --name t3
  --kind kv` → `bench run --target t3 --workload write
  --duration-secs 1` → exit 0 → `bench run --target t3 --workload
  read --duration-secs 1` → exit 0 (cluster still running). `bench
  teardown --target t3`. E2E.
- **Teardown idempotent**: `bench deploy --name t4 --kind kv` →
  `bench teardown --target t4` → exit 0 → `bench teardown --target
  t4` → exit 0. E2E.
- **Run on nonexistent target**: `bench run --target nonexistent`
  → exit 1, error message lists existing deploys. E2E.
- **Deploy with existing name**: `bench deploy --name t5 --kind
  kv` → exit 0 → `bench deploy --name t5 --kind kv` → exit 1,
  "already exists". `bench teardown --target t5`. E2E.
- **Kind mismatch**: `bench deploy --name t6 --kind rpc` → `bench
  run --target t6 --workload read` → exit 1, "kind mismatch". `bench
  teardown --target t6`. E2E.
- **Chunk/storage not implemented**: `bench deploy --name t7 --kind
  chunk` → exit 1, "not yet implemented". E2E.
- **Deploy --web (kv)**: `bench deploy --name t8 --kind kv --web`
  → exit 0, handle has `console_url` + `console_pid`. `bench
  teardown --target t8` → exit 0, console-web pid killed. E2E.

## Module Structure

```
app/crowdb-cli/src/
├── bench/
│   ├── handle.rs              # NEW: ClusterHandle, DeployKind, save/load
│   ├── mod.rs                 # MOD: add mod handle
│   ├── target.rs              # unchanged (BenchTarget trait)
│   ├── runner.rs              # unchanged (run_bench)
│   └── targets/
│       ├── kv.rs              # MOD: extract provision_kv_cluster, add detach + AttachedKvTarget
│       └── rpc.rs             # unchanged
├── commands/
│   └── bench.rs               # MOD: add Deploy/Prepare/Run/Teardown subcommands
│   └── bench/
│       ├── bench_deploy.rs    # NEW: deploy handler
│       ├── bench_prepare.rs   # NEW: prepare handler
│       ├── bench_run.rs       # NEW: run handler
│       ├── bench_teardown.rs  # NEW: teardown handler
│       ├── bench_kv.rs        # unchanged (legacy bench kv)
│       ├── bench_rpc.rs       # unchanged (legacy bench rpc)
│       └── bench_report.rs    # unchanged
└── tests/
    └── bench_benchmark_test.rs # MOD: add lifecycle E2E tests

tools/
├── bench-kv-read-regression.sh  # MOD: rewrite to lifecycle flow
└── bench-kv-scan-regression.sh  # MOD: rewrite to lifecycle flow
```

## Config Extensions

None. The `ClusterHandle` is a runtime artifact (JSON file under
`runtime/`), not a config extension. `/runtime/` is already in
`.gitignore`.

## Server Wiring

No server-side changes. The `bench deploy` verb reuses the existing
`BenchFixture` / `ConsoleClient` / `lifecycle::deploy_local` path.
The `bench run` verb reuses the existing `run_bench` runner via
`AttachedKvTarget`. The `bench teardown` verb reuses
`lifecycle::stop_pid_with_timeout`.

## Open Questions

None. All design decisions are resolved:
- Console-web lifetime: embedded for headless (detached after
  deploy), standalone for `--web` (survives CLI exit).
- `bench run` reuse: `AttachedKvTarget` with no-op
  provision/cleanup, same `run_bench` runner.
- Report location: `runtime/<name>/runs/<timestamp>/` for lifecycle
  verbs; `bench-runs/` unchanged for legacy `bench kv`.
