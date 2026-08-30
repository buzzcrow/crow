<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Console CLI command restructure by function (R126)

Implementation design for `doc/backlog/R126-console-cli-command-restructure.md`.
This draft covers the rewrite of `crowdb-cli` into four top-level service
layers (`cluster`/`kv`/`chunk`/`bench`) using group-0-direct calls, with a
new `ops` module in `crowdb-console-shared` holding the operation logic that
the CLI (and later the UI) can call. This file does not touch the Web UI;
that is the next task after R126.

Architecture decisions (command hierarchy, connection model, semantic gaps)
are in the backlog doc; this draft describes the implementation.

## 1. Shared operations module

### 1.1 Module layout

A new `ops` tree under `lib/crowdb-console-shared/src/`:

```
lib/crowdb-console-shared/src/
├── ops/
│   ├── mod.rs          — OpContext, connect helpers, result types
│   ├── cluster.rs      — cluster status/topology/init/reset/clean
│   ├── hardware.rs     — rack/node/disk-group/disk CRUD + set-status
│   ├── kv_server.rs    — crowdb-kv-server deploy/restart/stop/delete/list
│   ├── kv_logical.rs   — store/group/replica/inspect
│   ├── kv_data.rs      — put/get/delete/scan/snapshot data-plane
│   ├── chunk.rs        — diskdb (deploy/lifecycle/maintenance) + chunkdb/diskio stubs
│   └── bench.rs        — bench workload runners and RPC bench
```

Each `ops::<domain>` file exposes `async fn` operations that take
`&OpContext` and per-verb argument structs and return `Result<T, Error>`.

### 1.2 `OpContext`

```rust
// lib/crowdb-console-shared/src/ops/mod.rs
use std::sync::Arc;
use crowdb_kv_client::CrowdbClient;

pub struct OpContext {
    /// Config file (runtime-data/crowdb.temp.toml) — bootstrap phase
    /// and server tracking fallback.
    pub config: ConsoleConfig,
    /// KV client already seeded with a group-0 leader endpoint.
    pub kv: Arc<CrowdbClient>,
    pub meta: KVClusterMetaClient,
    pub hardware: HardwareClient,
    /// base URL for the currently connected group-0 replica's HTTP mgmt API.
    pub g0_http: String,
    pub http: reqwest::Client,
}

impl OpContext {
    pub async fn new(sysmd_ip: &str, sysmd_port: u16) -> Result<Self> { ... }

    /// Build a `KVClusterAdmin` against the group-0 leader's mgmt URL
    /// to perform lifecycle operations (add_store/add_group/...).
    pub fn admin(&self) -> KVClusterAdmin { ... }

    /// Resolve the current HTTP management base URL for a given node,
    /// using group-0 sysdata or the local TOML config.
    pub fn node_mgmt_url(&self, node_id: NodeId) -> Result<String> { ... }

    /// Resolve the `NodeEntry` for a node from config or sysdata.
    pub fn node_entry(&self, node_id: NodeId) -> Result<NodeEntry> { ... }
}
```

- `CrowdbClient` is created with a single seed endpoint `http://{sysmd_ip}:{sysmd_port}`
  and used for all sysdata and data-plane KV calls.
- `KVClusterMetaClient::new(kv)` provides the store/group/replica sysdata layer.
- `HardwareClient` wraps the same `CrowdbClient` and handles rack/node/disk-group/disk
  records (it already exists in `crowdb-kv-client`).
- `KVClusterAdmin` is built on demand using the group-0 leader's mgmt base URL.

### 1.3 Why group-0 direct is sufficient

The old CLI path `CLI → crowdb-web → ConsoleClient → group-0` can be flattened
to `CLI → group-0` because:

- `KVClusterMetaClient` already reads/writes store/group/replica records.
- `HardwareClient` already reads/writes rack/node/disk-group/disk records.
- `lifecycle::deploy_local` already spawns `crowdb-kv-server` from `crowdb-console-shared`.
- KV data-plane commands already create a `CrowdbClient` and `seed_leader`; they now
  resolve the leader endpoint from group-0 sysdata instead of from `crowdb-web`.

The remaining web-handler-side orchestration (multi-node fan-out, rollback,
monitor cache aggregation) is reimplemented in the `ops` module using direct
HTTP calls to node mgmt endpoints resolved from sysdata.

### 1.4 `cluster init` special case

`cluster init` runs before group-0 exists. It does not use `OpContext::new`.
It is a standalone async function in `ops::cluster`:

```rust
pub async fn cluster_init(
    config: &mut ConsoleConfig,
    nodes: &[NodeId],
) -> Result<InitSummary>
```

Behavior:

1. Validate each `node_id` has a `NodeEntry` in the TOML config; error if missing.
2. For each node, call `POST /system/init` on `http://{node.host}:{node.rest_port}`
   (currently the console-web delegates this to the kv-server mgmt endpoint).
   Use `SystemInitRequest` / `SystemInitResponse` from `crowdb-protocol`.
3. After all nodes acknowledge, write the topology cutover to group-0 using the
   first node as the seed. Subsequent commands can then use `--sysmd-ip`/`--sysmd-port`
   against that seed.
4. Persist a flag in the TOML config marking `cluster-init = true`.

### 1.5 Cluster teardown helpers

`ops::cluster::cluster_reset` and `ops::cluster::cluster_clean` are new.
They both operate in dependency order, resolved from group-0 sysdata.

`cluster_reset`:

1. Discover all resources from `OpContext`:
   - user stores/groups/replicas (via `meta.list_stores()`)
   - diskdb/chunkdb/diskio instances (via service-registry scan)
   - server entries (via config)
   - topology (racks/nodes)
2. Teardown in order: user groups → user stores → diskdb/chunkdb/diskio
   instances → stop each server's process (SIGTERM) → destroy group-0/store-0
   by stopping the node processes that host it → clear the TOML config.
3. Fast path: if group-0 is not reachable, use the TOML config to stop any
   pids in `ServerEntry` records and clear the config.

`cluster_clean`:

1. List all non-zero stores; for each, remove all groups then the store.
2. Stop all user `crowdb-kv-server` processes (not group-0 replicas).
3. Do not touch topology/group-0.
4. Keep `runtime-data/crowdb.temp.toml` intact except for user store/server entries.

### 1.6 Graceful require-empty deletion

`kv server delete` and `cluster node remove` check emptiness before removal:

```rust
pub async fn kv_server_delete(ctx: &OpContext, node_id: NodeId) -> Result<()>
```

- Load the `ServerEntry` for `node_id` from config.
- Query all stores/groups/replicas that reference `node_id` in group-0 sysdata.
- If any are found, return `Error::Validation` listing the resources that must be
  removed first (replicas → groups → stores → server → node).
- If empty, send `POST /system/step_down` and `POST /system/stop` to the node's
  mgmt URL, then remove the `ServerEntry` from the TOML config and group-0.

`cluster node remove` does the same at the node level: verify no servers/services
are hosted on the node, then remove the `NodeEntry` from the TOML config and
remove it from group-0 if mirrored there.

## 2. CLI command restructure

### 2.1 Main entry and global flags

`app/crowdb-cli/src/main.rs` keeps the `#[derive(Parser)]` entry but replaces
global flags:

```rust
#[derive(Parser, Debug)]
#[command(name = "crowdb-cli", version, about = "CrowDB cluster console (CLI)")]
struct Cli {
    #[arg(short = 'I', long, global = true, env = "CROWDB_SYSMD_IP", default_value = "127.0.0.1")]
    sysmd_ip: String,
    #[arg(short = 'O', long, global = true, env = "CROWDB_SYSMD_PORT", default_value_t = 9910)]
    sysmd_port: u16,
    /// Path to the console config file.
    #[arg(short = 'p', long, global = true, env = "CROWDB_CONSOLE_CONFIG")]
    config: Option<PathBuf>,
    #[arg(short = 'j', long, global = true)]
    json: bool,
    #[command(subcommand)]
    command: Domain,
}
```

Old `--ip`/`--port` are removed. `cluster init` is the only command that does
not inherit these globals; it takes `--nodes` directly and is not run through
`OpContext::new`.

### 2.2 Four-domain subcommand enum

```rust
#[derive(Subcommand, Debug)]
enum Domain {
    /// Hardware topology + cluster-level ops.
    #[command(alias = "cls")]
    Cluster { #[command(subcommand)] verb: ClusterVerb },
    /// KV layer: server lifecycle + logical concepts + data-plane.
    Kv { #[command(subcommand)] verb: KvVerb },
    /// Chunk storage service cluster.
    Chunk { #[command(subcommand)] verb: ChunkVerb },
    /// Load injection only.
    Bench { #[command(subcommand)] verb: BenchVerb },
}
```

### 2.3 Command module mapping

Old modules are removed or renamed under `app/crowdb-cli/src/commands/`:

```
app/crowdb-cli/src/commands/
├── mod.rs            — exports + dispatch
├── cluster.rs        — cluster reset/clean/status/topology
├── hardware.rs       — rack/node/disk-group/disk CRUD + set-status
├── kv_server.rs      — deploy/restart/stop/delete/list
├── kv_logical.rs     — store/group/replica + inspect
├── kv_data.rs        — put/get/delete/scan/snapshot
├── chunk_diskdb.rs   — diskdb lifecycle + maintenance
├── chunk_stub.rs     — chunkdb/diskio/allocate/free/write/read/gc stubs
└── bench.rs          — bench kv <workload> + bench rpc
```

### 2.4 `cluster` verb enum

```rust
#[derive(Subcommand, Debug)]
enum ClusterVerb {
    Init { #[arg(short = 'n', long, value_delimiter = ',')] nodes: Vec<String> },
    Reset,
    Clean,
    Status,
    Topology,
    Rack { #[command(subcommand)] verb: RackVerb },
    Node { #[command(subcommand)] verb: NodeVerb },
    #[command(name = "disk-group")]
    DiskGroup { #[command(subcommand)] verb: DiskGroupVerb },
    Disk { #[command(subcommand)] verb: DiskVerb },
}
```

`cluster inspect` is removed; per-resource `inspect` lives under `kv store inspect`
and `kv group inspect`. `cluster reset` / `cluster clean` are new.

### 2.5 `kv` verb enum

```rust
#[derive(Subcommand, Debug)]
enum KvVerb {
    #[command(subcommand)]
    Server,
    #[command(subcommand)]
    Store,
    #[command(subcommand)]
    Group,
    #[command(subcommand)]
    Replica,
    Put { ... },
    Get { ... },
    Delete { ... },
    Scan { ... },
    #[command(subcommand)]
    Snapshot,
}
```

- `kv server` verbs: `Deploy`, `Restart` (alias `start`), `Stop`, `Delete`, `List`.
- `kv store` verbs: `Add`, `Remove`, `List`, `Inspect`.
- `kv group` verbs: `Add`, `Remove`, `List`, `Inspect`.
- `kv replica` verbs: `Add`, `Remove`.
- `kv put/get/delete/scan/snapshot` are data-plane flat verbs.
- Old `KvVerb::List` is removed; `Scan` is the only scan/list command.

### 2.6 `chunk` verb enum

```rust
#[derive(Subcommand, Debug)]
enum ChunkVerb {
    #[command(subcommand)] Diskdb,
    #[command(subcommand)] Chunkdb,
    #[command(subcommand)] Diskio,
    // future data-plane
    Allocate, Free, Write, Read, Gc,
}
```

`chunk diskdb` has `deploy/restart/stop/delete/list/usage/scan-status/scan/recalc/compact/rebuild`.
`chunk chunkdb` and `chunk diskio` have the same lifecycle verbs but are stubs
that return `Error::NotImplemented`. `chunk allocate/free/write/read/gc` are stubs.

### 2.7 `bench` verb enum

```rust
#[derive(Subcommand, Debug)]
enum BenchVerb {
    #[command(subcommand)]
    Kv,
    Rpc(RpcArgs),
    #[command(subcommand)]
    Diskdb, // future
    #[command(subcommand)]
    Chunkdb, // future
    #[command(subcommand)]
    Chunk, // future
}

#[derive(Subcommand, Debug)]
enum KvBenchWorkload {
    Read(ReadArgs),
    Write(WriteArgs),
    Scan(ScanArgs),
    Mix(MixArgs),
}
```

- `bench kv <read|write|scan|mix>` replaces `bench run --workload <kind>`.
- `bench rpc` stays flat and self-contained.
- `bench deploy/prepare/run/clean/teardown/report/compare` and the all-in-one
  `bench kv` are removed.
- Each `bench kv` subcommand takes `--store-id`, `--group-id`, plus the
  same duration/connections/key-space/value-size parameters as today.

## 3. Bench restructure details

### 3.1 Reusing the current runner

The existing `bench/bench_run.rs` runner (`run_bench`) is preserved in
`crowdb-console-shared/src/ops/bench.rs` with one change: it receives a
`&CrowdbClient` (or store/group) target instead of a `ClusterHandle`.

`ops::bench::kv_workload`:

```rust
pub async fn kv_workload(
    ctx: &OpContext,
    store_id: u64,
    group_id: u64,
    workload: Workload,
) -> Result<BenchReport>
```

- Resolve the leader endpoint for `(store_id, group_id)` from `meta.get_group`.
- Seed the `CrowdbClient` with the leader URL.
- Dispatch to the existing `run_bench` runner (read/write/mix).
- For `scan`, use `client.scan` with the configured prefix/limit.

### 3.2 Pre-population

`bench kv write` can be used to pre-populate keys before `bench kv read`:

```bash
crowdb-cli bench kv write --store-id 1 --group-id 1 --duration 0 --pre-populate 200000
```

The `read` workload no longer pre-populates by default; the operator either
runs `bench kv write` first or uses a dedicated script.

### 3.3 Regression scripts

`tools/bench-kv-read-regression.sh`, `tools/bench-kv-scan-regression.sh`,
`tools/bench-kv-write-regression.sh` are rewritten to:

1. `cluster init --nodes 0,1,2`
2. `kv server deploy` on 0,1,2
3. `kv store add --store-id 1 --nodes 0,1,2`
4. `kv group add --store-id 1 --group-id 1 --replica-id 1 --nodes 0,1,2`
5. `kv replica add` for each node if not auto-added
6. Run the workload (`bench kv read|scan|write`)
7. `cluster reset`

The run reports are still saved to `bench-runs/<run>/`; `report`/`compare` are
removed from the CLI and left to external `jq`/`diff` tools.

## 4. Test strategy

### 4.1 Unit / crate tests

- `crowdb-console-shared`: new unit tests for `OpContext` construction and
  `ops::*` edge cases (empty server deletion fails, reset order, clean does not
  remove group-0, etc.).
- `crowdb-cli`: clap compile-time validation only; no unit tests for thin
  wrappers.

### 4.2 E2E tests to update

- `app/crowdb-cli/tests/cluster_cli_test.rs` — `cluster init` and
  `cluster status/topology` calls.
- `app/crowdb-cli/tests/kv_cli_test.rs` — `kv put/get/delete/scan/snapshot`.
- `app/crowdb-cli/tests/mgmt_cli_test.rs` — `kv store/group/replica` and
  `cluster rack/node/disk-group/disk`.
- `app/crowdb-cli/tests/lifecycle_cli_test.rs` — `kv server deploy/restart/stop`.
- `app/crowdb-cli/tests/diskdb_cli_test.rs` — `chunk diskdb` commands.
- `app/crowdb-cli/tests/bench_benchmark_test.rs` — rewrite to use the new
  `bench kv <workload>` shape.

### 4.3 Manual / regression verification

- Run `pixi run test-console-cli`.
- Run `tools/bench-kv-read-regression.sh` end-to-end.
- Run `tools/bench-rpc-regression.sh` (unchanged RPC path).

## 5. Scope

### 5.1 Modified files

- `app/crowdb-cli/src/main.rs` — global flags, four-domain subcommand routing.
- `app/crowdb-cli/src/commands.rs` — re-export new command modules.
- `app/crowdb-cli/src/commands/*.rs` — rewrite all 12 old modules into 9 new
  domain modules; delete old ones (`rack.rs`, `node.rs`, `server.rs`, `paxos.rs`,
  `replica.rs`, `store.rs`, `disk.rs`, `disk_group.rs`, `diskdb.rs`, `kv.rs`,
  `cluster.rs`, `bench.rs` and the `commands/bench/` subdirectory).
- `app/crowdb-cli/src/utils/client.rs` — remove `ConsoleClient` builder;
  replace with `OpContext` builder.
- `lib/crowdb-console-shared/src/lib.rs` — export `ops` module.
- `lib/crowdb-console-shared/src/ops/` — new `mod.rs`, `cluster.rs`,
  `hardware.rs`, `kv_server.rs`, `kv_logical.rs`, `kv_data.rs`, `chunk.rs`,
  `bench.rs`.
- `lib/crowdb-console-shared/src/ops.rs` (or `mod.rs`) — convenience re-export.
- `tools/bench-kv-read-regression.sh`, `tools/bench-kv-scan-regression.sh`,
  `tools/bench-kv-write-regression.sh` — rewrite to use new CLI commands.
- `doc/user-manual/user-guide.md` §7 — update command examples.
- `doc/design/console/design-crowdb-console.md` §7 — replace two-layer rule
  with three-layer rule, update verb vocabulary, remove ConsoleClient routing
  paragraph.

### 5.2 Untouched files

- `app/crowdb-web/src/**/*.rs` — web handlers stay as-is for now. The UI
  refactor is a later task.
- `lib/crowdb-console-shared/src/clients/console.rs` — `ConsoleClient` is not
  removed; it remains in use by `crowdb-web` until the UI refactor.
- `lib/crowdb-console-shared/src/lifecycle.rs`, `cluster_deployer.rs` —
  reused, not modified.

## 6. Complexity

**High.** The CLI is currently a thin HTTP client over `crowdb-web`; this
rewrite replaces the entire call path with group-0-direct and simultaneously
restructures the command surface. The hardest parts are:

1. `cluster init` and `cluster reset` — they cannot use `OpContext` because
   group-0 does not yet (or no longer) exists; they work against raw node mgmt
   endpoints and the TOML config.
2. `kv server deploy` — it must pick ports, resolve SSH creds from the TOML
   config (or later from group-0 sysdata), and call `lifecycle::deploy_local`.
3. Teardown ordering in `cluster reset` and `cluster clean` — must not destroy
   group-0 until all user data is gone.
4. Bench restructure — removing the all-in-one and lifecycle commands means
   rewriting the regression scripts and the E2E bench test to do explicit
   cluster setup/teardown.

Most other verbs are straightforward replacements of `ConsoleClient` calls with
`OpContext` operations.

## 7. Module structure

```
lib/crowdb-console-shared/src/
├── lib.rs                 — export ops module
├── ops/
│   ├── mod.rs             — OpContext, Error extensions, connect
│   ├── cluster.rs         — init, reset, clean, status, topology
│   ├── hardware.rs        — rack/node/disk-group/disk operations
│   ├── kv_server.rs       — crowdb-kv-server lifecycle
│   ├── kv_logical.rs      — store/group/replica CRUD/inspect
│   ├── kv_data.rs         — put/get/delete/scan/snapshot
│   ├── chunk.rs           — diskdb and future chunk stubs
│   └── bench.rs           — bench kv workloads and rpc bench

app/crowdb-cli/src/
├── main.rs                — Cli, Domain routing
├── commands.rs            — re-exports
├── commands/
│   ├── mod.rs             — helpers + dispatch
│   ├── cluster.rs         — ClusterVerb
│   ├── hardware.rs        — RackVerb/NodeVerb/DiskGroupVerb/DiskVerb
│   ├── kv_server.rs       — ServerVerb
│   ├── kv_logical.rs      — StoreVerb/GroupVerb/ReplicaVerb
│   ├── kv_data.rs         — Put/Get/Delete/Scan/SnapshotVerb
│   ├── chunk_diskdb.rs    — DiskdbVerb
│   ├── chunk_stub.rs      — chunkdb/diskio/data-plane stubs
│   └── bench.rs           — BenchVerb + KvBenchWorkload
└── utils/
    └── client.rs          — build OpContext
```

## 8. Config extensions

No new config fields. The existing `ConsoleConfig` default path
`runtime-data/crowdb.temp.toml` is already used. `ServerEntry` may gain a
`health` helper if needed, but existing fields are sufficient.

## 9. Open questions

None. The backlog doc resolves all command hierarchy, connection model,
backward compatibility, and lifecycle questions. The scope is CLI-only;
the Web UI refactor is tracked as the next task.