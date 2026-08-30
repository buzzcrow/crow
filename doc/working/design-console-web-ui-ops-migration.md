<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Console — Web UI migrate to shared ops + layout rework (R127)

This draft covers the implementation design for migrating the
`crowdb-web` Axum handlers to call `crowdb_console_shared::ops::*`
(eliminating ~2000 lines of duplicated orchestration) and reworking
the UI information architecture from three view-modes
(`Physical | Logical | Capacity`) to three domains
(`Cluster | KV | Chunk`) aligned with the CLI's four-domain structure.

- Backlog doc: `doc/backlog/R127-console-web-ui-ops-migration.md`
- Root design docs:
  - `doc/design/console/design-crowdb-console.md` §2.1 (Call Path),
    §2.2 (Reuse Boundary), §7 (CLI Design — four-domain structure),
    §12 (Cluster reset).
  - `doc/design/console/design-crowdb-console-ui.md` §3 (Information
    Architecture), §12 (View-Mode Restructure), §7 (Embedding
    Contract), §6.1 (KV Operator Panel).
- Already landed: R126 restructured the CLI to call `ops::*` directly.
  The `ops` module in `lib/crowdb-console-shared/src/ops/` has
  `context.rs` (`OpContext`), `hardware.rs`, `kv_server.rs`,
  `kv_logical.rs`, `kv_data.rs`, `cluster.rs`, `chunk.rs` (stubs),
  `bench.rs` (stubs). The web backend
  (`app/crowdb-web/src/{lifecycle.rs,mgmt/*,kv.rs,diskdb.rs,
  diskdb_lifecycle.rs}`) still hand-rolls the orchestration that `ops`
  now owns.

Architecture decisions and rationale are in the root design; this doc
does not repeat them.

## 1. `OpContext` provider for the web backend

### 1.1 Why

`OpContext` (in `lib/crowdb-console-shared/src/ops/context.rs`) holds
a `CrowdbSysmdClient`, an `Arc<CrowdbKvClient>`, and an
`RwLock<ConsoleConfig>`. The CLI builds one per invocation from
`--sysmd-ip` / `--sysmd-port`. The web backend currently has no
`OpContext` — each handler reaches into `AppState` fields directly
(`config`, `kv_client`, `monitor_cache`) and reconstructs the
`HardwareClient` / `KVClusterMetaClient` / `ServerClient` per request.
A web-side `OpContext` provider is required so handlers can call
`ops::*` with the same connection-sharing semantics as the CLI.

### 1.2 `AppState::op_context` builder

`app/crowdb-web/src/state.rs` gains a method that builds an `OpContext`
sharing the cached `CrowdbKvClient`:

```rust
impl AppState {
    /// Build an `OpContext` for a single request, sharing the cached
    /// `CrowdbKvClient` (topology cache + connection pool) and the
    /// persisted `ConsoleConfig`. `group0_endpoint` is resolved from
    /// the config's group-0 hosting nodes; if group 0 is not yet
    /// initialized, the endpoint is empty and `ops` functions that
    /// need sysdata will fail with a clear error.
    pub async fn op_context(&self) -> crowdb_console_shared::error::Result<OpContext> { ... }
}
```

a. Resolve the group-0 endpoint: read `config` for the first
   `ServerEntry` whose `node_id` is in `config.stores[0].nodes` (store
   0, group 0). If no group-0 store exists yet, use the first deployed
   server's mgmt URL as a bootstrap seed (the `cluster init` flow
   needs an endpoint to call `/system/init` on).
b. Acquire the cached `Arc<CrowdbKvClient>` via the existing
   `self.kv_client()` method. Seed the leader hint for store 0 / group
   0 with the resolved endpoint so the topology cache starts warm.
c. Build `CrowdbSysmdClient::from_shared(Arc::clone(&kv))`.
d. Clone the current `ConsoleConfig` out of `self.config` (read-lock)
   and pass it to `OpContext::new` as the initial config. The
   `OpContext`'s `RwLock<ConsoleConfig>` is a per-request snapshot —
   mutations are written back to `AppState.config` + persisted by the
   handler after `ops::*` returns (see §1.3).
e. `mgmt_seeds`: all deployed servers' mgmt URLs, so the client can
   find a new group-0 leader when the seeded one is down.

### 1.3 Config write-back

`OpContext` owns its own `RwLock<ConsoleConfig>` snapshot, so
mutations inside `ops::*` (e.g. `ops::hardware::add_rack` calls
`ctx.config_mut().add_rack(...)`) do not touch `AppState.config`. The
handler is responsible for writing the mutated config back:

a. Build the `OpContext` (snapshot of `AppState.config`).
b. Call the `ops::*` function (mutates `OpContext.config`).
c. On success, replace `AppState.config` contents with the
   `OpContext`'s mutated config (write-lock `AppState.config`, clone
   the `OpContext` config in, persist via `config_engine`).
d. On error, discard the `OpContext` — `AppState.config` is unchanged.

This avoids a shared `RwLock` between the per-request `OpContext` and
the long-lived `AppState.config` (which would require holding a lock
across an `await` — a lock-free-flow violation). The snapshot + write-
back is a short critical section with no `await` inside.

Edge cases:
- `OpContext` build fails (no deployed server, group-0 unreachable)
  → handler returns 502 with "cluster not initialized" or "no
  deployed server" message.
- Two concurrent requests both mutate config → both snapshot the same
  base, both write back; the second write wins. This is the same race
  the current handlers have (they mutate `AppState.config` under a
  write-lock). Acceptable for v1 — the console is single-operator.
- `ops::*` mutates config but the write-back persist fails → the
  in-memory config is updated but the TOML file is stale. Handler
  returns the persist error; the next console restart re-loads the
  stale TOML. Same behavior as current handlers.

## 2. Backend handler migration

### 2.1 Lifecycle handlers (`lifecycle.rs`)

The rack/node/disk-group/disk CRUD handlers
(`http_add_rack`, `http_add_node`, `http_add_node_disk_group`,
`http_add_disk`, `http_add_disks_batch`, `http_remove_*`) currently
write to `AppState.config` directly and call `HardwareClient` for
sysdata sync. They are rewritten to:

a. `let ctx = state.op_context().await?;`
b. Call the matching `ops::hardware::*` function (the `ops` module
   needs new disk-group/disk functions — see §2.4).
c. Write back the config + persist.

The server lifecycle handlers
(`http_deploy_node_server`, `http_restart_node_server`,
`http_stop_node_server`, `http_delete_node_server`) call
`ops::kv_server::{deploy, restart, stop, delete}`. `http_delete_node_server`
gains the require-empty check (it currently just stops + removes the
entry with no replica check).

`http_internal_reset` (the `POST /internal/reset` and
`POST /api/cluster/reset` handler) is rewritten to call
`ops::cluster::reset`. The `POST /api/cluster/clean` route is new —
calls `ops::cluster::clean`.

`http_node_openapi_proxy` and the `/api/swagger/` `ServeDir` nest are
removed (see §4).

### 2.2 Mgmt handlers (`mgmt/store_ops.rs`, `group_ops.rs`, `replica_ops.rs`, `cluster_init.rs`)

- `http_add_store` → `ops::kv_logical::add_store`
- `http_remove_store` → `ops::kv_logical::remove_store`
- `http_list_stores` → `ops::kv_logical::list_stores`
- `http_add_group` → `ops::kv_logical::add_group`
- `http_remove_group` → `ops::kv_logical::remove_group`
- `http_add_replica` → `ops::kv_logical::add_replica`
- `http_remove_replica` → `ops::kv_logical::remove_replica`
- `http_cluster_init` → `ops::cluster::init`

Each handler becomes: build `OpContext` → call `ops::*` → write back
config + persist → return JSON. The fan-out + rollback logic moves
entirely into `ops::kv_logical` (already implemented there).

The `http_get_store` / `http_get_group` / `http_get_replica` /
`http_list_groups` / `http_list_replicas` read handlers currently read
from the monitor cache. They are rewritten to read from group-0
sysdata via `ops::kv_logical::{list_groups, list_replicas}` (the `ops`
functions call `ctx.sysmd()` directly). The monitor cache is no longer
the read path for logical-tree endpoints — group-0 sysdata is the
source of truth, same as the CLI.

### 2.3 KV data-plane (`kv.rs`)

`http_kv_get`, `http_kv_put`, `http_kv_delete`, `http_kv_scan`,
`http_kv_endpoint` currently do manual leader resolution via the
monitor cache + `CrowdbKvClient`. They are rewritten to call
`ops::kv_data::{get, put, delete, scan}`. The `ops::kv_data` functions
use `ctx.kv()` (the shared `CrowdbKvClient`) which resolves the leader
from its topology cache — no monitor-cache lookup needed.

The snapshot endpoints (`create_snapshot`, `list_snapshots`,
`scan_snapshot`, `release_snapshot`) call the matching
`ops::kv_data::*` functions.

### 2.4 `ops::hardware` disk-group/disk extensions

`ops::hardware.rs` currently has rack/node CRUD only. The disk-group
and disk CRUD functions are added (mirroring the existing
`lifecycle.rs` handlers' logic, but going through `OpContext`):

```rust
pub async fn add_disk_group(ctx: &OpContext, node_id: u64, dg_id: u64, name: &str) -> Result<...>
pub async fn remove_disk_group(ctx: &OpContext, node_id: u64, dg_id: u64) -> Result<...>
pub async fn list_disk_groups(ctx: &OpContext, node_id: u64) -> Result<Vec<...>>
pub async fn add_disk(ctx: &OpContext, node_id: u64, dg_id: u64, disk: AddDiskItem) -> Result<...>
pub async fn add_disks_batch(ctx: &OpContext, node_id: u64, dg_id: u64, disks: Vec<AddDiskItem>) -> Result<...>
pub async fn remove_disk(ctx: &OpContext, node_id: u64, dg_id: u64, disk_id: &str) -> Result<...>
pub async fn list_disks(ctx: &OpContext, node_id: u64, dg_id: u64) -> Result<Vec<...>>
pub async fn set_disk_status(ctx: &OpContext, disk_id: &str, status: HwStatus) -> Result<...>
pub async fn set_disk_group_status(ctx: &OpContext, rack: u64, node: u64, dg: u64, status: HwStatus) -> Result<...>
```

Each writes to `ctx.config_mut()` first, then best-effort syncs to
group-0 sysdata via `ctx.sysmd()`. The batch add validates all disk
IDs upfront and is atomic (rollback on any failure).

### 2.5 DiskDB runtime proxy (unchanged)

`app/crowdb-web/src/diskdb.rs` and `diskdb_lifecycle.rs` are NOT
migrated in this requirement. They stay on `AppState.diskdb_client`
(the lazily-initialized `DiskdbClient`) and `AppState.runtime_pids`.
The `ops::chunk` module is stubs; migrating the diskdb runtime proxy
waits for `ops::chunk` to be implemented. No behavior change for
`/api/diskdb/*` endpoints.

### 2.6 DiskDB lifecycle (`diskdb_lifecycle.rs`)

`http_deploy_diskdb`, `http_restart_diskdb`, `http_stop_diskdb`,
`http_delete_diskdb` stay as-is (they use `AppState.runtime_pids` +
`lifecycle::deploy_local`). The `ops::chunk::diskdb_deploy` stub is not
ready. No behavior change.

## 3. UI layout rework

### 3.1 `ViewMode` → `Domain` enum

`app/crowdb-web/ui/src/types/index.ts`:

```typescript
export enum Domain {
  Cluster = 'Cluster',
  KV = 'KV',
  Chunk = 'Chunk',
}
```

The old `ViewMode` enum (`Physical | Logical | Capacity`) is removed.
All references are updated:
- `contexts/ViewModeContext.tsx` → `contexts/DomainContext.tsx`
  (renamed; `viewMode` → `domain`, `setViewMode` → `setDomain`,
  `initialViewMode` → `initialDomain`).
- `shell/Header.tsx` — domain toggle with three buttons
  (Cluster / KV / Chunk) replacing the Physical / KV Cluster / Capacity
  toggle. The Swagger API toggle + KV toggle are removed (see §4, §3.4).
- `App.tsx` — `centerPanel` state is replaced by per-domain center
  panel logic (see §3.4).

### 3.2 Domain 1 — Cluster (hardware topology)

Sidebar: rack → node → disk-group → disk tree (same data as the
current Physical view's rack/node + Capacity view's disk-group/disk,
merged into one tree). Right-click context menus:
- Rack: Add Node, Delete Rack.
- Node: Ping, Delete Node.
- DiskGroup: Add Disk (batch), Remove Disk Group, Set Status.
- Disk: Remove Disk, Move Disk, Set Disk Status.

Center panel: hierarchy chart (React Flow) showing rack → node, with
disk-groups rendered as boxes containing disk elements (cylinder icon
+ short UUID prefix). This reuses the existing `topology/` module's
layout + node types, extended with `DiskGroup` and `Disk` node types.

The `usePhysicalTree` hook is renamed to `useClusterTree` and extended
to fetch disk-groups + disks (currently only rack/node). The
`useCapacityTree` hook's disk-group/disk data is merged in.

### 3.3 Domain 2 — KV (server lifecycle + logical + data-plane)

Sidebar: rack → node only (disk-groups/disks hidden). Right-click on
a node opens the KV-server context menu (deploy / restart / stop /
delete). Under a deployed KV-server, the sidebar shows the logical
sub-tree (store → group → replica) — same as the current KV Cluster
view's logical tree, but nested under the node.

Center panel: tab bar with two tabs — **Cluster** and **KV**:
- **Cluster tab** — logical topology canvas (React Flow): store →
  group → replica. Reuses the existing logical layout from
  `topology/buildFlow.ts`.
- **KV tab** — the existing `KvOperatorPanel` (put/get/delete/scan).
  The selected store/group persists across tab switches via
  `SelectionContext`.

The `useLogicalTree` hook is retained (feeds the Cluster tab + the
sidebar's logical sub-tree).

### 3.4 Domain 3 — Chunk (chunkdb / diskdb / diskio)

Sidebar: rack → node, with chunkdb / diskdb / diskio server instances
under each node. Under a diskdb server, the owned disk-group + disks
expand (read-only — managed from the Cluster domain).

Center panel: button toggle with two sub-views — **Capacity** and
**Chunk**:
- **Capacity** — the existing `CapacityPanel` (canvas visualization:
  disk-group usage, disk health, allocation heatmaps). Reuses
  `useCapacityTree`.
- **Chunk** — empty but clickable (renders blank, no placeholder text).
  Future content drops in without UI changes when `ops::chunk` is
  implemented.

### 3.5 Cross-domain behaviors

- `SelectionContext` is unchanged — selection is shared across all
  three domains. Clicking a node in Cluster, switching to KV, and
  right-clicking shows the KV-server context menu for the same node.
- The inspector (right panel) is domain-agnostic — renders Details +
  Activity for whatever is selected.
- The header domain toggle is the only domain switcher; no separate KV
  toggle or Swagger toggle.

### 3.6 Embedding contract changes

`CrowdbConsoleProps` (`App.tsx`):
- `initialViewMode?: ViewMode` → `initialDomain?: Domain`.
- `modules` opt-out: remove `'swagger'` from the key union. The
  remaining keys: `'racks' | 'nodes' | 'stores' | 'groups' |
  'replicas' | 'kv' | 'activity'`.
- `initialNodeId?: string` removed (was "Pre-select a node for the
  Swagger panel").
- `embed.ts` re-exports `Domain` instead of `ViewMode`.

## 4. Swagger UI removal

Backend (`lib.rs`):
- Remove the `.nest_service("/api/swagger", ServeDir::new(SWAGGER_UI_DIR))` route.
- Remove the `.route("/api/nodes/:id/openapi.json", get(lifecycle::http_node_openapi_proxy))` route.
- Remove `http_node_openapi_proxy` from `lifecycle.rs`.
- Remove `SWAGGER_UI_DIR` const + `openapi_cache` field from `state.rs`.
- Delete `app/crowdb-web/swagger-ui/` directory (committed static assets).
- Delete `app/crowdb-web/tests/swagger_routes_test.rs`.

Frontend:
- Delete `app/crowdb-web/ui/src/panels/SwaggerPanel.tsx`.
- Remove the Swagger toggle + node selector from `Header.tsx`.
- Remove the `centerPanel === 'swagger'` branch from `App.tsx`.
- Remove the `'swagger'` module opt-out + `initialNodeId` prop.

The OpenAPI document remains served by `crowdb-kv-server` at
`/openapi.json` for direct access; the console no longer embeds or
proxies it.

## 5. E2E test rework — fast and stable

### 5.1 Why

The current E2E suite (15 spec files in `e2e/flows/`) targets the
three-view-mode layout. The domain rework changes the header toggle,
sidebar structure, and center panel wiring — every spec that switches
view-modes or asserts on the Physical/Logical/Capacity toggle breaks.
The suite also has stability concerns: the `50-capacity-diskdb.spec.ts`
file is 91 KB (one giant file), and full cluster redeploys per test
are slow.

### 5.2 Stability principles (applied to all rewritten specs)

- **Poll, don't sleep** — use `expect.poll` with `intervals: [100]`
  for all assertions on async state (leader election, health, tree
  render). No bare `page.waitForTimeout`.
- **Precise selectors** — `getByRole` / `getByLabel` / `getByTestId`
  only. Add `data-testid` to the domain toggle buttons
  (`domain-cluster`, `domain-kv`, `domain-chunk`) and the KV tab
  buttons (`kv-tab-cluster`, `kv-tab-kv`) for disambiguation.
- **Timeout discipline** — assertions ≤ 3 s, leader election ≤ 10 s
  (per the console-ui-e2e skill).
- **No silent error swallowing** — every `api.post` that can fail
  asserts on status; cleanup calls log warnings but don't throw.
- **Shared clusters** — `beforeAll` deploys one cluster for the whole
  spec file; `afterAll` tears down. `resetAll` only between test
  groups that need a clean slate, not between every test.

### 5.3 Speed improvements

- **Reuse clusters across tests in the same file** — the
  `CrowdbClusterDeployer` already supports `start` / `stop` / `teardown`.
  Specs that test different UI surfaces against the same topology use
  `beforeAll(deployer.start(...))` + `afterAll(deployer.teardown())`.
  Only specs that mutate cluster state (add/remove) reset between
  tests.
- **Split the 91 KB capacity spec** — `50-capacity-diskdb.spec.ts` is
  split into focused specs by capacity scope (disk-group, disk, zone),
  each with its own `beforeAll` cluster. This reduces the single-file
  runtime and isolates failures.
- **Reduce poll intervals on fast paths** — canvas render assertions
  use `intervals: [50, 100, 200]` (render is synchronous once data is
  fetched); leader election uses `intervals: [100, 200, 500]`.
- **Parallel-safe port allocation** — the `freePort()` counter stays
  monotonic (workers: 1), but `freePortRange` is used consistently for
  diskdb (3 ports) to avoid the port-conflict bug that already bit.

### 5.4 Spec file mapping (old → new)

- `00-shell-embedding-swagger.spec.ts` → `00-shell-embedding.spec.ts`
  (Swagger assertions removed; domain toggle + embedding contract
  assertions retained, updated for `initialDomain`).
- `01-shell-ui-behaviors.spec.ts` → updated for domain toggle.
- `10-physical-rack-node.spec.ts` → `10-cluster-rack-node.spec.ts`
  (Cluster domain).
- `11-physical-server-lifecycle.spec.ts` →
  `11-cluster-server-lifecycle.spec.ts` (Cluster domain, adds
  require-empty delete test).
- `12-physical-node-inspect.spec.ts` → `12-cluster-node-inspect.spec.ts`.
- `20-kv-cluster-store-group.spec.ts` → `20-kv-store-group.spec.ts`
  (KV domain, Cluster tab).
- `21-kv-cluster-reconfig.spec.ts` → `21-kv-reconfig.spec.ts`.
- `22-kv-cluster-topology.spec.ts` → `22-kv-topology.spec.ts`.
- `30-kv-ops-basic.spec.ts` → `30-kv-ops-basic.spec.ts` (KV domain,
  KV tab).
- `31-kv-ops-advanced.spec.ts` → `31-kv-ops-advanced.spec.ts`.
- `40-inspector-activity.spec.ts` → unchanged (inspector is
  domain-agnostic).
- `41-canvas-fit-pan.spec.ts` → updated for Cluster domain canvas.
- `50-capacity-diskdb.spec.ts` → split into `50-chunk-capacity-
  disk-group.spec.ts`, `51-chunk-capacity-disk.spec.ts`,
  `52-chunk-capacity-zone.spec.ts` (Chunk domain, Capacity sub-view).
- `51-capacity-canvas.spec.ts` → `53-chunk-capacity-canvas.spec.ts`.
- `90-flow-full-chain.spec.ts` → updated for the full chain across
  all three domains.

### 5.5 New `data-testid` anchors

- `domain-cluster`, `domain-kv`, `domain-chunk` — header domain toggle
  buttons.
- `kv-tab-cluster`, `kv-tab-kv` — KV domain center panel tab buttons.
- `chunk-tab-capacity`, `chunk-tab-chunk` — Chunk domain center panel
  toggle buttons.
- `cluster-init-btn`, `cluster-reset-btn`, `cluster-clean-btn` —
  cluster-level op buttons (header or toolbar).

## 6. Scope

**Backend (Rust):**
- `app/crowdb-web/src/state.rs` — add `op_context()` method; remove
  `openapi_cache` field + `SWAGGER_UI_DIR` const.
- `app/crowdb-web/src/lib.rs` — remove Swagger + openapi routes; add
  `POST /api/cluster/clean` route; migrate `POST /api/cluster/reset`
  handler.
- `app/crowdb-web/src/lifecycle.rs` — migrate rack/node/disk-group/
  disk/server handlers to `ops::*`; remove `http_node_openapi_proxy`;
  rewrite `http_internal_reset` to call `ops::cluster::reset`; add
  require-empty to `http_delete_node_server`.
- `app/crowdb-web/src/mgmt/store_ops.rs`, `group_ops.rs`,
  `replica_ops.rs`, `cluster_init.rs` — migrate to `ops::kv_logical::*`
  / `ops::cluster::*`.
- `app/crowdb-web/src/kv.rs` — migrate to `ops::kv_data::*`.
- `app/crowdb-web/src/diskdb.rs`, `diskdb_lifecycle.rs` — unchanged.
- `app/crowdb-web/src/physical.rs`, `physical_view.rs` — review for
  removal (logical reads move to sysdata; physical per-node primitives
  may still be needed for the inspector).
- `app/crowdb-web/tests/swagger_routes_test.rs` — deleted.
- `app/crowdb-web/swagger-ui/` — deleted.
- `lib/crowdb-console-shared/src/ops/hardware.rs` — add disk-group/disk
  CRUD functions.
- `lib/crowdb-console-shared/src/ops/context.rs` — possibly add a
  constructor variant that accepts a pre-built `Arc<CrowdbKvClient>`
  (for the web's cached client sharing).

**Frontend (TypeScript/React):**
- `app/crowdb-web/ui/src/types/index.ts` — `ViewMode` → `Domain` enum.
- `app/crowdb-web/ui/src/contexts/ViewModeContext.tsx` →
  `DomainContext.tsx`.
- `app/crowdb-web/ui/src/shell/Header.tsx` — domain toggle; remove
  Swagger + KV toggles.
- `app/crowdb-web/ui/src/shell/Sidebar.tsx` — per-domain tree logic.
- `app/crowdb-web/ui/src/App.tsx` — per-domain center panel wiring;
  remove Swagger/KV center panel modes; update `CrowdbConsoleProps`.
- `app/crowdb-web/ui/src/panels/SwaggerPanel.tsx` — deleted.
- `app/crowdb-web/ui/src/panels/CapacityPanel.tsx` — move under Chunk
  domain (no logic change, just the rendering condition).
- `app/crowdb-web/ui/src/panels/KvOperatorPanel.tsx` — render under KV
  domain KV tab (no logic change).
- `app/crowdb-web/ui/src/data/usePhysicalTree.ts` →
  `useClusterTree.ts` (extend with disk-group/disk).
- `app/crowdb-web/ui/src/embed.ts` — re-export `Domain`.
- `app/crowdb-web/ui/src/topology/buildFlow.ts` — add DiskGroup/Disk
  node types for Cluster domain canvas.

**E2E tests:**
- All 15 spec files in `app/crowdb-web/ui/e2e/flows/` — rewritten per
  §5.4 mapping.
- `app/crowdb-web/ui/e2e/fixtures/crowClusterDeployer.ts` — add
  `clusterClean` helper; no other changes.

## 7. Complexity

**High.** The backend migration touches ~2000 lines of handler logic
across 8 files, requiring a new `OpContext` provider with config
snapshot/write-back semantics and extensions to `ops::hardware` for
disk-group/disk CRUD. The UI rework replaces the core navigation
primitive (`ViewMode` → `Domain`) which ripples through every
component that switches on view-mode (Header, Sidebar, App center
panel, all tree hooks). The E2E suite is 15 files / ~250 KB of test
code, all of which must be rewritten for the new domain structure
while improving speed (cluster reuse, file splitting) and stability
(poll-not-sleep, precise selectors, `data-testid` anchors). The main
challenge is sequencing: the backend migration and UI rework are
independent enough to land in two phases, but the E2E rework depends
on the UI changes being in place.

## 8. Test Design

### Unit tests (UT)

- **`AppState::op_context` shares the cached `CrowdbKvClient`** —
  build an `AppState` with a pre-seeded `kv_client`, call
  `op_context()`, assert the returned `OpContext`'s `kv()` is the same
  `Arc` (same pointer / no new connection pool). UT.
- **`AppState::op_context` with no deployed server** — call
  `op_context()` on a default `AppState` (no servers), assert it
  returns an error ("no deployed server" or "cluster not
  initialized"). UT.
- **Config write-back after `ops::hardware::add_rack`** — build
  `AppState` + `OpContext`, call `ops::hardware::add_rack`, write back
  to `AppState.config`, assert the rack is in `AppState.config` and
  persisted to the TOML engine. UT.
- **`ops::hardware::add_disks_batch` atomic rollback** — add a batch
  where the 3rd disk has a duplicate ID, assert the whole batch is
  rejected and no disks are written to config or sysdata. UT.
- **`ops::kv_server::delete` require-empty** — seed a config with a
  server + a replica referencing the node, call `delete`, assert
  `Error::Conflict` with the replica list. UT (mock sysdata or use
  in-memory).
- **`Domain` enum string values** — assert `Domain.Cluster ===
  'Cluster'`, `Domain.KV === 'KV'`, `Domain.Chunk === 'Chunk'`. UT
  (Vitest).
- **`CrowdbConsoleProps` embedding contract** — assert `initialDomain`
  is accepted, `initialViewMode` / `initialNodeId` / `'swagger'`
  module are gone. UT (Vitest type + render).

### Integration tests (handler-level)

- **`POST /api/cluster/init` calls `ops::cluster::init`** — mock or
  stub the `OpContext` and assert the handler delegates (verify via
  the resulting topology write, not a hand-rolled 5-phase bootstrap).
  Integration test.
- **`POST /api/racks` calls `ops::hardware::add_rack`** — post a rack,
  assert config mutation + sysdata sync. Integration test.
- **`POST /api/nodes` calls `ops::hardware::add_node`** — same.
  Integration test.
- **`POST /api/nodes/:id/server/deploy` calls `ops::kv_server::deploy`**
  — deploy, assert process spawn + `ServerEntry` recording.
  Integration test.
- **`POST /api/stores` calls `ops::kv_logical::add_store`** — assert
  fan-out + rollback on partial failure. Integration test.
- **`POST /api/stores/:sid/groups` calls `ops::kv_logical::add_group`**
  — same. Integration test.
- **`GET /api/stores/:sid/groups/:gid/kv/get` calls `ops::kv_data::get`**
  — assert leader resolution + get. Integration test.
- **`/api/swagger/` returns 404** (removed) — Integration test.
- **`/api/nodes/:id/openapi.json` returns 404** (removed) —
  Integration test.

### E2E tests (Playwright, real backend)

- **Header domain toggle switches between Cluster / KV / Chunk** —
  click each, assert the sidebar + center panel render the domain's
  content. E2E test.
- **Cluster domain: sidebar rack → node → disk-group → disk tree;
  center panel hierarchy chart with disk-group boxes + disk elements**
  — E2E test.
- **KV domain: sidebar rack → node; right-click node shows KV-server
  context menu; center panel [Cluster] [KV] tab bar; Cluster tab
  shows logical topology; KV tab shows operator panel; selection
  persists across tab switches** — E2E test.
- **Chunk domain: sidebar rack → node → chunkdb/diskdb/diskio;
  center panel [Capacity] [Chunk] toggle; Capacity shows disk-group
  usage canvas; Chunk is blank but clickable** — E2E test.
- **Inspector shows Details + Activity across all three domains** —
  E2E test.
- **`DELETE /api/nodes/:id/server` require-empty** — deploy a server,
  add a store/group/replica on it, attempt delete, assert 409 with
  dependent replicas; remove replicas, delete again, assert 204.
  E2E test.
- **`POST /api/cluster/reset` full teardown** — deploy a full cluster,
  reset, assert all stores/groups/servers gone. E2E test.
- **`POST /api/cluster/clean` orphan removal** — deploy, stop a
  server, clean, assert orphaned store removed from sysdata. E2E
  test.
- **No Swagger panel or API toggle renders** — load the UI, assert no
  Swagger panel / API toggle / node selector. E2E test.
- **Embedding contract (`apiPrefix`, `basePath`, `readonly`,
  `modules`, `initialDomain`)** — render in embedded mode, assert
  domain toggle respects `initialDomain` + `modules` opt-out. E2E
  test.
- **Full chain across all three domains** — rack → node → deploy →
  store → group → replica → KV op → disk-group → disk → capacity.
  E2E test.

## 9. Module Structure

```
app/crowdb-web/
├── src/
│   ├── state.rs                    (modified: +op_context, -openapi_cache, -SWAGGER_UI_DIR)
│   ├── lib.rs                      (modified: -swagger/openapi routes, +cluster/clean)
│   ├── lifecycle.rs                (modified: migrate to ops::*, -openapi proxy, +require-empty)
│   ├── lifecycle/rack_node.rs      (modified: migrate to ops::hardware)
│   ├── mgmt/
│   │   ├── store_ops.rs            (modified: migrate to ops::kv_logical)
│   │   ├── group_ops.rs            (modified: migrate to ops::kv_logical)
│   │   ├── replica_ops.rs          (modified: migrate to ops::kv_logical)
│   │   ├── cluster_init.rs         (modified: migrate to ops::cluster::init)
│   │   └── topology.rs             (modified: read from sysdata, not monitor cache)
│   ├── kv.rs                       (modified: migrate to ops::kv_data)
│   ├── diskdb.rs                   (unchanged)
│   ├── diskdb_lifecycle.rs         (unchanged)
│   ├── physical.rs                 (review: may shrink or be removed)
│   ├── physical_view.rs            (review: may shrink or be removed)
│   └── spa.rs                      (unchanged)
├── swagger-ui/                     (deleted)
├── tests/
│   └── swagger_routes_test.rs      (deleted)
└── ui/
    ├── src/
    │   ├── types/index.ts          (modified: ViewMode → Domain)
    │   ├── contexts/
    │   │   ├── ViewModeContext.tsx → DomainContext.tsx  (renamed)
    │   │   ├── SelectionContext.tsx                     (unchanged)
    │   │   ├── ToastContext.tsx                         (unchanged)
    │   │   └── ActivityContext.tsx                      (unchanged)
    │   ├── shell/
    │   │   ├── Header.tsx          (modified: domain toggle, -swagger/-kv toggles)
    │   │   ├── Sidebar.tsx         (modified: per-domain tree)
    │   │   └── Inspector.tsx       (unchanged)
    │   ├── panels/
    │   │   ├── SwaggerPanel.tsx    (deleted)
    │   │   ├── CapacityPanel.tsx   (modified: render condition → Chunk domain)
    │   │   ├── KvOperatorPanel.tsx (modified: render condition → KV domain KV tab)
    │   │   └── ...                 (unchanged)
    │   ├── topology/
    │   │   ├── buildFlow.ts        (modified: +DiskGroup/Disk node types)
    │   │   └── ...                 (unchanged)
    │   ├── data/
    │   │   ├── usePhysicalTree.ts → useClusterTree.ts  (renamed + extended)
    │   │   ├── useLogicalTree.ts                      (unchanged)
    │   │   └── useCapacityTree.ts                     (unchanged)
    │   ├── App.tsx                 (modified: per-domain center panel, -swagger/-kv modes)
    │   └── embed.ts                (modified: re-export Domain)
    └── e2e/
        ├── flows/                  (15 specs rewritten per §5.4)
        └── fixtures/crowClusterDeployer.ts  (+clusterClean helper)

lib/crowdb-console-shared/src/ops/
├── context.rs                      (modified: +constructor for shared Arc<CrowdbKvClient>)
├── hardware.rs                     (modified: +disk-group/disk CRUD)
├── cluster.rs                      (unchanged)
├── kv_server.rs                    (unchanged)
├── kv_logical.rs                   (unchanged)
├── kv_data.rs                      (unchanged)
├── chunk.rs                        (unchanged — stubs)
└── bench.rs                        (unchanged — stubs)
```

## 10. Config Extensions

None. No new config fields. The `ConsoleConfig` struct is unchanged;
the `OpContext` provider snapshots it per request.

## 11. Server Wiring

`app/crowdb-web/src/main.rs` startup sequence (unchanged except Swagger
removal):
1. Load `ConsoleConfig` from TOML (existing).
2. Build `AppState` (existing; `openapi_cache` field removed).
3. Spawn monitor task (existing).
4. Build router (existing; Swagger + openapi routes removed,
   `cluster/clean` route added).
5. Bind + serve (existing).

The `op_context()` method is called per-request inside handlers, not
at startup — no startup sequence change.

## 12. Open Questions

None. All decisions are made in the backlog doc and this draft. The
DiskDB runtime proxy retention (Solution item 6) is decided; the
`ops::hardware` disk-group/disk extension is a straightforward mirror
of existing handler logic; the E2E rework approach is standard
Playwright practice.
