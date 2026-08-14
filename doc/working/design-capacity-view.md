<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Capacity View + Diskdb Console Integration (R77)

This draft expands R77's solution into implementation detail for the
**full end-to-end flow**: GUI action → `crow-web` REST →
`DiskdbClient` / `HardwareClient` → gRPC → `crow-diskdb`. It also
introduces a new **Capacity view** that re-partitions the console's
view-mode responsibilities, and adds canvas-based capacity
visualization.

- Backlog: [`doc/backlog/R77-diskdb-console-cli.md`](../backlog/R77-diskdb-console-cli.md)
- Root design (console): [`doc/design/console/design-crow-console-ui.md`](../design/console/design-crow-console-ui.md) §3 (IA), §6.1 (center panel), §9 (polling)
- Root design (diskdb): [`doc/design/diskdb/design-crow-diskdb.md`](../design/diskdb/design-crow-diskdb.md) §6, §8, §9
- Sub-design (space metrics): [`doc/design/diskdb/design-crow-diskdb-space-metrics.md`](../design/diskdb/design-crow-diskdb-space-metrics.md) §4 (QueryCapacityStats shapes), §8 (keepalive), §11 (client)

Already landed (prior R** + code paths):
- R70–R76: `DiskdbService` gRPC surface, `QueryCapacityStats` +
  `ZoneUsage.usage_bitmap`, scanner RPCs, `RebuildZoneBitmap`.
- R74 §8: keepalive piggyback (`DiskdbExtra.group_usages`,
  `DiskGroupUsageSummary`). R74 §11: `DiskdbClient` in
  `lib/crow-diskdb-client/src/client.rs` — wraps allocate/free/query/
  recalc/compact (lines 143–325); **scanner/rebuild wrappers missing**
  (R77 §1).
- R81: disk-group/disk lifecycle REST (`app/crow-web/src/lifecycle.rs`
  lines 1227–1479) + `crow disk`/`disk-group` CLI +
  `DiskEntry`/`AddDiskBody`/`MoveDiskBody` in `crow-console-shared`.
- KV server deploy/stop/restart/delete handlers:
  `http_deploy_node_server` (line 636), `http_stop_node_server`
  (line 882), `http_restart_node_server` (line 748),
  `http_delete_node_server` (line 925). Deployment is SSH
  (`ssh::deploy_via_ssh`) or local fork
  (`lifecycle::deploy_local_in_dir`) — **no Docker** anywhere.
- Port allocation: `lib/crow-protocol/src/ports.rs` —
  `DISKDB_GRPC_BASE=9941`, `DISKDB_HTTP_BASE=9942`, stride 2.

Architecture decisions and rationale are in the root design; this doc
does not repeat them.

## 1. View-mode restructure: Physical / Capacity / KV

### 1.1 Why

The current two-mode shell (Physical | Logical) mixes infrastructure
management (rack/node/server deploy) with capacity management (disk
groups/disks). Disk lifecycle actions (Add Disk Group, Add Disk) do
not belong on the Physical view's Node context menu — they are
capacity concerns, not infrastructure concerns. Splitting them into a
dedicated **Capacity view** gives each view a single responsibility
and lets the Capacity center panel render capacity visualization
without competing with the topology canvas.

### 1.2 Three view-modes

The header view-toggle expands from two to three:

- **Physical** — rack → node → server(s) → (KV: store → group;
  DiskDB: disk-group → disk). Infrastructure + service management
  only. Read-only resource listing under each server. Context menus:
  - Rack: Add Node, Delete Rack.
  - Node: Ping, Delete Node. **No** Add Disk Group / Add Disk.
  - Server (KV or DiskDB): Restart, Stop, (if no server: Deploy).
  - Server sub-tree (store/group/disk-group/disk): read-only, no
    context menu.
- **Capacity** — rack → node → disk-group → disk (no server nodes —
  infrastructure is visible in Physical). The **only** place to
  add/remove/move/set-status disk groups and disks. Center panel
  renders capacity visualization (§5). Context menus:
  - Node: Add Disk Group.
  - DiskGroup: Add Disk (batch), Remove Disk Group, Set Status.
  - Disk: Remove Disk, Move Disk, Set Disk Status.
- **KV** (renamed from "Logical") — cluster → store → group →
  replica. KV data-plane operations via the KV Operator panel
  (§6.1 of the root design). Unchanged from the current Logical view
  except the name.

`ViewModeContext` expands from `Physical | Logical` to
`Physical | Capacity | KV`. The sidebar tree data hook switches on
the active mode; the center panel mode set changes per mode (Physical
→ topology canvas; Capacity → capacity panel; KV → KV operator
panel).

### 1.3 Physical view: DiskDB Server sub-tree

The Physical view's `Server` tree node currently only covers KV
servers (Sidebar.tsx lines 68–77, `type: 'Server'`, id `KV-${nodeId}`).
Extend it to render **both** server types under a Node:

- `KV-${nodeId}` — existing; children `Store → Group → Replica`.
- `DDB-${nodeId}` — new; children `DiskGroup → Disk` (read-only,
  fetched from `GET /api/nodes/:id/disk-groups` +
  `GET /api/nodes/:id/disk-groups/:dg_id/disks`).

`TreeNode.type` gains `'DiskGroup' | 'Disk'` variants
(Tree.tsx line 17). The `serverByNodeId` lookup (Sidebar.tsx line 64)
is extended to a `servicesByNodeId: Map<number, ServiceEntry[]>` that
returns both KV and DiskDB server entries; each renders as a separate
`Server`-typed child with a distinguishing icon (Cog for KV, HardDrive
for DiskDB).

Context menu on a `Server` node (App.tsx `buildMenuItems`): add a
`Server` case (currently absent — right-clicking a Server yields an
empty menu). Items:
- Restart → `POST /api/nodes/:id/server/restart` (KV) or
  `POST /api/nodes/:id/diskdb/restart` (DiskDB, §3).
- Stop → `POST /api/nodes/:id/server/stop` (KV) or
  `POST /api/nodes/:id/diskdb/stop` (DiskDB, §3). Labeled "Stop" not
  "Delete" — it stops the process but keeps the deployment record so
  Restart/Deploy can bring it back (confirmed decision).
- If no server deployed: Deploy → opens `DeployServerDialog`
  (existing for KV) or `DeployDiskdbDialog` (new, §3).

The existing Restart/Stop items on the **Node** menu (App.tsx lines
261–274) are **removed** — server-process ops belong on the Server
node, not the Node. Node menu keeps only Ping + Delete Node (+ Deploy
if no server).

Edge cases:
- Node with KV deployed but no DiskDB → only `KV-${nodeId}` child
  renders; no `DDB-${nodeId}` node.
- DiskDB deployed but owns zero disk-groups → `DDB-${nodeId}` renders
  with empty children (leaf with no expand arrow).
- KV server stopped (pid cleared) but entry persisted → still renders
  in the tree; Restart available, health = Unknown.

## 2. DiskdbClient scanner/rebuild wrappers

### 2.1 Why

`DiskdbClient` (R74 §11) wraps allocate/free/query/recalc/compact but
not the scanner RPCs (`TriggerScan`, `GetScanStatus`) or
`RebuildZoneBitmap`. The proto + server handlers exist (R75/R76); only
the client wrappers are missing. Without them the REST proxy (§4) and
CLI (§7) cannot reach these RPCs.

### 2.2 How

Extend `lib/crow-diskdb-client/src/client.rs`:

```rust
use crow_protocol::diskdb::rpc::{
    TriggerScanRequest, TriggerScanResponse,
    GetScanStatusRequest, GetScanStatusResponse,
    RebuildZoneBitmapRequest, RebuildZoneBitmapResponse,
};

impl DiskdbClient {
    /// Trigger a scan on all owned groups (or one group if `dg_id`
    /// is set). Returns the last `ScanSummary` + `scan_in_progress`.
    pub async fn trigger_scan(&self, dg_id: Option<DiskGroupId>)
        -> Result<TriggerScanResponse>

    /// Get the last scan summary + `has_run` flag.
    pub async fn get_scan_status(&self, dg_id: Option<DiskGroupId>)
        -> Result<GetScanStatusResponse>

    /// Rebuild one zone's bitmap on a disk. Routes via `dg_for_disk`.
    pub async fn rebuild_zone_bitmap(
        &self, disk_id: DiskId, zone_index: u32,
    ) -> Result<RebuildZoneBitmapResponse>
}
```

a. `trigger_scan` / `get_scan_status` route to `first_cached_dg()`
   when `dg_id` is `None` (mirrors `recalc_disk_usage` line 297);
   otherwise route to the specified dg. Both go through `with_retry`.
b. `rebuild_zone_bitmap` routes via `dg_for_disk(disk_id)` (mirrors
   `compact_zone` line 319), then `with_retry`.
c. All three are admin/debug calls; transient `Unavailable` is retried
   per `RetryConfig`; `NotFound` (unknown disk/zone) is returned
   immediately.

Edge cases:
- `trigger_scan` while a scan is running → server returns
  `scan_in_progress: true`; client returns the response as-is (no
  error, no stacking).
- `rebuild_zone_bitmap` on unknown disk → `dg_for_disk` returns
  `Unreachable`; surfaced to the caller.
- Empty endpoint cache → `first_cached_dg()` returns `Unreachable`;
  caller sees "no cached endpoints; call refresh_endpoints".

## 3. DiskDB Server deploy / restart / stop

### 3.1 Why

The Physical view's DiskDB Server node needs the same service-lifecycle
ops as KV Server (Restart, Stop, Deploy). Today only KV deploy/stop/
restart handlers exist (`http_deploy_node_server` line 636,
`http_stop_node_server` line 882, `http_restart_node_server` line
748). `crow-diskdb` has no deploy path from the console — it must be
launched out-of-band. R77 adds the deploy/restart/stop handlers so
AddNodeDialog can auto-deploy DiskDB alongside KV, and the Server
context menu works for both types.

Deployment mechanism: **SSH or local fork** — same as KV. No Docker
(confirmed decision). The `crow-diskdb` binary is spawned via
`ssh::deploy_via_ssh` or `lifecycle::deploy_local_in_dir`, on the
paired ports from `ports.rs` (`DISKDB_GRPC_BASE` + `DISKDB_HTTP_BASE`).

### 3.2 How

New handlers in `app/crow-web/src/lifecycle.rs` (or a new
`app/crow-web/src/diskdb_lifecycle.rs`), mirroring the KV handlers:

```rust
pub struct DeployDiskdbBody {
    grpc_port: u16,
    http_port: u16,
    #[serde(default)]
    binary: Option<String>,
}

pub async fn http_deploy_node_diskdb(
    State(state), Path(node_id), Json(body),
) -> Result<(StatusCode, Json<DeployResult>), ...>

pub async fn http_restart_node_diskdb(
    State(state), Path(node_id),
) -> Result<Json<DeployResult>, ...>

pub async fn http_stop_node_diskdb(
    State(state), Path(node_id),
) -> Result<Json<StopResult>, ...>
```

a. `http_deploy_node_diskdb` — checks no existing diskdb on the node
   (409 if present), resolves the node, builds a `DeployRequest` with
   `grpc_port`/`http_port` from `ServicePort::DiskdbGrpc`/`DiskdbHttp`
   defaults (or body overrides), spawns via SSH or local fork, persists
   a `ServerEntry` with `service_type: Diskdb`, records the pid.
   Routes: `POST /api/nodes/:id/diskdb/deploy`.
b. `http_restart_node_diskdb` — stops the tracked pid, re-deploys on
   the same ports from the persisted entry. Route:
   `POST /api/nodes/:id/diskdb/restart`.
c. `http_stop_node_diskdb` — stops the tracked pid, clears it, keeps
   the entry. Route: `POST /api/nodes/:id/diskdb/stop`.
d. `ServerEntry` / `AppState` gains a `service_type` discriminator
   (`Kv | Diskdb`) so `server_for_node` can return the right entry per
   type. `runtime_pid` tracking is keyed by `(node_id, service_type)`.
e. `AddNodeDialog` calls `deployServer` (KV) then
   `deployDiskdb` (new API function) after `addNode` succeeds. Both
   are gated by the existing `enableCrowKV`-style checkbox (add
   `enableDiskDB`, default true).

Edge cases:
- Node with KV deployed but DiskDB deploy fails → KV stays deployed;
  the dialog reports the DiskDB failure; the operator can retry via
  the Server context menu's Deploy.
- DiskDB binary not found on the remote host → SSH deploy returns an
  error; surfaced as 502.
- Port conflict (another process on 9941/9942) → spawn fails; surfaced
  as 502. The handler does not pre-check ports (best-effort, matches
  KV behavior).

## 4. REST proxy for diskdb runtime

### 4.1 Why

`crow-web` has no REST proxy for diskdb runtime RPCs
(`QueryCapacityStats` drill-down, scan, recalc, compact, rebuild).
The CLI and web UI must route through `crow-web` (no direct gRPC from
the browser or CLI), so these endpoints are required for the E2E
flow.

### 4.2 How

New `app/crow-web/src/diskdb.rs` handlers + routes under
`/api/diskdb/`. `AppState` owns a `DiskdbClient` built from the same
`ServiceRegistryClient` the console already uses (mirror
`build_hardware_client` in `mgmt.rs`):

```rust
// AppState field:
diskdb_client: tokio::sync::RwLock<Option<DiskdbClient>>,

// Lazy init (mirror build_hardware_client):
async fn build_diskdb_client(state: &AppState) -> Option<DiskdbClient>
```

Handlers:

- `GET /api/diskdb/instances` — reads `read_all_diskdb_instances`
  from the service registry directly (no gRPC fan-out). Returns
  instance id, endpoint, `last_heartbeat_ms`, `owned_dg_ids`, and the
  keepalive `group_usages` summaries.
- `GET /api/diskdb/usage?dg=<id>&disk=<disk_id>&zone=<zi>` —
  `QueryCapacityStats` drill-down (all params optional). When `dg` is
  omitted, iterate all registered instances and merge the responses
  for cluster-wide totals. `DiskdbClient.query_capacity_stats(0)`
  routes to one instance only, so the merge lives in this handler.
- `GET /api/diskdb/scan-status?dg=<id>` — `get_scan_status`.
- `POST /api/diskdb/scan` — `trigger_scan` (optional `dg` in body).
- `POST /api/diskdb/recalc` — `recalc_disk_usage` (optional `dg`).
- `POST /api/diskdb/compact` — `compact_zone` (disk_id + optional
  zone_indices; empty = all zones).
- `POST /api/diskdb/rebuild` — `rebuild_zone_bitmap` (disk_id +
  optional zone_index; absent = all zones — handler loops over the
  disk's zones if zone_index is absent).
- `PUT /api/disks/:disk_id/status` — set a disk's `HwStatus` via
  `HardwareClient.set_disk_status`. Needed by the Set-Status dialog;
  no such endpoint exists today (only add/remove/move).

a. The `DiskdbClient` is lazily initialized on first diskdb REST
   request (the service registry may not be ready at console startup).
b. `GET /api/diskdb/usage` with no `dg` iterates
   `read_all_diskdb_instances`, calls `query_capacity_stats` per
   instance, merges `DiskGroupInfo` entries by id (summing
   capacity/busy/free). A dead instance yields a degraded indicator,
   not a failed page — its contribution is skipped with a warning.
c. `PUT /api/disks/:disk_id/status` resolves the disk's rack/node/dg
   from config, then calls `hw.set_disk_status`. 404 if the disk is
   not in config.

Edge cases:
- Cluster overview with a dead instance → merged response excludes
  it; the `/instances` endpoint still lists it (with stale heartbeat)
  so the UI can show the degraded card.
- Zone drill-down → bitmap is omitted at disk level (proto contract);
  the UI issues the zone-level query separately.
- Scan already running → `trigger_scan` returns `scan_in_progress:
  true`; handler passes it through (no error).

## 5. Web UI — Capacity view center panel (canvas)

### 5.1 Why

The Capacity view needs a visualization that scales to thousands of
zones per disk and tens of thousands of blocks per zone. DOM/SVG
rendering at that scale causes layout thrash and jank. Canvas with
offscreen double-buffering handles 84×84 zone grids and 181×181
bitmap grids without flicker.

### 5.2 How

New `app/crow-web/ui/src/panels/CapacityPanel.tsx`, rendered when
`viewMode === Capacity`. The panel content depends on the selected
entity (from `SelectionContext`):

- **Rack / Node selected** — hierarchical capacity summary. Rack →
  Node → DiskGroup rows, each with capacity/busy/free bars. Disk
  counts shown as an array icon + count (not per-disk boxes — too
  many). Data from `GET /api/diskdb/usage` (cluster merge).
- **DiskGroup selected** — per-disk boxes. Each disk is a box with a
  busy% gradient fill (green → amber → red, red = busy) + label.
  Data from `GET /api/diskdb/usage?dg=<id>`.
- **Disk selected** — zone grid. Each zone is a box in a square grid
  (side = ceil(sqrt(zone_count))) with a green→amber→red gradient
  based on busy%. Hover over a zone box shows a tooltip with the zone
  id and usage percentage (from the brief per-zone entry already
  loaded — no bitmap fetch). Click drills into the zone bitmap. A
  "jump to zone #" input handles direct navigation (7000 zones cannot
  be a dropdown). Data from `GET /api/diskdb/usage?dg=<id>&disk=<disk_id>`
  (brief per-zone entries, no bitmap).
- **Zone selected** — zone bitmap. Canvas grid of the zone's
  `usage_bitmap` (side = ceil(sqrt(unit_count))). Busy block = red
  filled cell, free block = green filled cell. Data from
  `GET /api/diskdb/usage?dg=<id>&disk=<disk_id>&zone=<zi>` (full
  bitmap, on-demand only).

Rendering technology — **canvas, not SVG/DOM**, for all levels:
a. Offscreen canvas double-buffering: draw to an offscreen canvas,
   then `drawImage` blit to the visible canvas in one call. The
   visible canvas is never cleared-then-slowly-drawn (that flickers).
b. Single `requestAnimationFrame` sync redraw for grids up to 181×181
   (32K cells) — fast enough to not flicker.
c. On data refresh (3 s poll), retain the previous frame until the
   new one is fully drawn, then swap. No blank intermediate state.
d. No DOM reflow — the canvas is a single element; only its bitmap
   content changes.

Color encoding — **green (free) → amber → red (busy)**:
a. Zone/disk boxes: gradient fill based on `busy_blocks /
   unit_capacity` ratio. 0% = green, ~50% = amber, 100% = red.
b. Bitmap cells: binary — busy = red filled, free = green filled.
c. Redundant encoding: each zone/disk box shows a `%` text label on
   hover (zone id + usage %) or inline so the information is not
   color-only (color-blind friendly).

Polling — **3 s refresh** of the currently focused visualization:
a. The poll refetches only the data for the selected entity level
   (rack/node → cluster merge; disk-group → dg query; disk → disk
   query; zone → zone query).
b. On refetch, the canvas redraws via double-buffer (no flicker).
c. If the selection changes, the poll target switches immediately;
   the old canvas is cleared on the next draw.

Zone count math (for layout):
- 200 TB disk / 32 GB zone = 6400 zones → 80×80 grid.
- 32 GB zone / 1 MB unit = 32K units → 181×181 grid.
- (R77 §5's "16K units / 128×128" was based on 16 GB zones; with the
  32 GB default the grid is 181×181 — still canvas-feasible.)

Edge cases:
- Disk with 0 zones (freshly added, zone load in progress) → empty
  grid placeholder with "loading" text.
- Zone with `used_count == unit_capacity` → all cells red; reported
  as-is.
- `usage_bitmap` shorter than `unit_capacity` (last zone rounded) →
  pad with free (green) cells.
- Poll response slower than 3 s → keep previous frame; next poll
  catches up. No spinner overlay (would flicker).

## 6. Web UI — Capacity view sidebar + dialogs

### 6.1 Why

The Capacity view sidebar renders rack → node → disk-group → disk
(no server nodes — infrastructure is visible in Physical). Disk
lifecycle dialogs (Add Disk Group, Add Disk, Remove, Move, Set Status)
are only accessible here. R81 landed the REST endpoints; R77 adds the
UI on top.

### 6.2 How

Sidebar tree for Capacity mode (`Sidebar.tsx`):
- Rack → Node → DiskGroup → Disk, fetched from
  `GET /api/nodes/:id/disk-groups` + `.../disks`.
- `TreeNode.type` variants `'DiskGroup' | 'Disk'` (added in §1.3).
- Disk rows show two status columns: "Group-0" (from
  `HardwareClient.get_disk` via `GET /api/nodes/:id/disk-groups/:dg_id/disks/:disk_id`)
  and "Runtime" (from `QueryCapacityStats` via `/api/diskdb/usage`).
  Both use the existing `toUiHealth`-style mapping over `HwStatus`.

Context menus (App.tsx `buildMenuItems`, Capacity branch):
- Node: Add Disk Group → `AddDiskGroupDialog`.
- DiskGroup: Add Disk → `AddDiskDialog` (batch, §6.3); Remove Disk
  Group; Set Status.
- Disk: Remove Disk; Move Disk; Set Disk Status.

Dialogs (follow `AddNodeDialog`/`ConfirmDeleteDialog` patterns):
- `AddDiskGroupDialog` — id, name. Calls
  `POST /api/nodes/:id/disk-groups`.
- `AddDiskDialog` — batch (§6.3). Calls
  `POST /api/nodes/:id/disk-groups/:dg_id/disks/batch`.
- `RemoveDiskDialog` / `RemoveDiskGroupDialog` — confirm + cascade
  warning. Calls `DELETE .../disks/:id` / `DELETE .../disk-groups/:id`.
- `MoveDiskDialog` — target rack/node/disk-group. Calls
  `PUT .../disks/:id/move`.
- `SetDiskStatusDialog` — `HwStatus` enum dropdown. Calls
  `PUT /api/disks/:id/status` (§4).

### 6.3 Batch Add Disk

**New batch endpoint** (confirmed decision — atomic all-or-nothing):

```rust
// app/crow-web/src/lifecycle.rs
pub struct AddDiskBatchBody {
    disks: Vec<AddDiskItem>,
}
pub struct AddDiskItem {
    disk_id: String,        // UUID, auto-generated, editable
    disk_type: String,      // selectable
    capacity_bytes: u64,    // default 4 TiB
    zone_size_bytes: u64,   // default 32 GiB
    unit_size_bytes: u32,   // fixed 1 MiB, not editable
}

pub async fn http_add_disks_batch(
    State(state), Path((node_id, dg_id)), Json(body),
) -> Result<(StatusCode, Json<Vec<DiskEntry>>), ...>
```

Route: `POST /api/nodes/:id/disk-groups/:dg_id/disks/batch`.

a. Validates all `disk_id` formats upfront; rejects the whole batch if
   any is malformed (atomic).
b. Writes all disks to config + group-0 sysdata in one transaction;
   if any write fails, rolls back (no partial success).
c. `AddDiskDialog` UI: a row-builder where each row auto-generates a
   UUID (editable), disk_type dropdown, capacity (default 4 TiB),
   zone_size (default 32 GiB), unit_size (fixed 1 MiB, disabled
   input). "Add Row" / "Remove Row" buttons. Submit sends the whole
   batch.
d. Defaults: `capacity_bytes = 4 * 1024^4`, `zone_size_bytes = 32 *
   1024^3`, `unit_size_bytes = 1024^2` (locked).

Edge cases:
- Batch with duplicate disk_ids → 400, whole batch rejected.
- Group-0 sysdata write fails for disk 3 of 5 → rollback all 5, 502.
- Empty batch → 400.

## 7. Console-shared diskdb runtime models + client

### 7.1 Why

`ConsoleClient` in `crow-console-shared` is the typed REST client used
by both the web UI (via `api.ts` wrappers) and `crow-cli`. It needs
diskdb runtime methods + serde model types so the CLI (§8) and UI
share one deserialization path.

### 7.2 How

New `lib/crow-console-shared/src/diskdb.rs` module:

```rust
impl ConsoleClient {
    pub async fn list_diskdb_instances(&self) -> Result<Vec<DiskdbInstanceInfo>>
    pub async fn query_diskdb_usage(&self, dg: Option<u64>, disk: Option<String>, zone: Option<u32>) -> Result<UsageResponse>
    pub async fn get_scan_status(&self, dg: Option<u64>) -> Result<ScanSummary>
    pub async fn trigger_scan(&self, dg: Option<u64>) -> Result<ScanSummary>
    pub async fn recalc(&self, dg: Option<u64>) -> Result<RecalcResult>
    pub async fn compact(&self, disk_id: &str, zones: Option<Vec<u32>>) -> Result<CompactionResult>
    pub async fn rebuild(&self, disk_id: &str, zone: Option<u32>) -> Result<RebuildResult>
    pub async fn set_disk_status(&self, disk_id: &str, status: HwStatus) -> Result<()>
}
```

Serde model types (mirrors of the proto responses):
`DiskdbInstanceInfo`, `DiskGroupUsageSummary`, `DiskGroupUsage`,
`DiskUsage`, `ZoneUsage`, `ScanSummary`, `RecalcResult`,
`CompactionResult`, `RebuildResult`, `UsageResponse`.

## 8. CLI `crow diskdb` subcommands

### 8.1 Why

Runtime queries (usage/zones/scan/recalc/compact/rebuild) are not
reachable from the command line. `crow disk`/`disk-group` (R81) cover
lifecycle only.

### 8.2 How

New `app/crow-cli/src/commands/diskdb.rs`, wired into the `Group` enum
in `app/crow-cli/src/main.rs`:

```
crow diskdb status                          — /api/diskdb/instances
crow diskdb usage [--dg <id>] [--disk <id>] [--zone <zi>]
crow diskdb scan [--dg <id>]                — trigger
crow diskdb scan-status [--dg <id>]
crow diskdb recalc [--dg <id>]
crow diskdb compact <disk_id> [--zones <zi,...>]
crow diskdb rebuild <disk_id> [--zone <zi>]
```

All route through `ConsoleClient` → `crow-web` → `DiskdbClient` →
gRPC; no direct talk to `crow-diskdb`. Lifecycle stays in
`crow disk` / `crow disk-group`; `diskdb` is runtime queries only.

## 9. Group-0 status write-back — close the `Init → Offline` gap

### 9.1 Why

`background_zone_load` (keepalive.rs line 956) transitions `Init →
Offline` when zone loading fails for all strategies (line 1082,
`all_ok = false`) **without writing back** to group 0. The function
is a `tokio::spawn`'d background task that does not receive
`HardwareClient`. Consequence: group 0 still says `Up`, the next sync
tick reads `Up` → `recover_disk_to_up` → the disk becomes `Up` with
broken zones.

### 9.2 How

a. Make `HardwareClient` `Clone` (it wraps `Arc<CrowkvClient>` —
   trivial).
b. Pass `hw` into `background_zone_load`; on `all_ok = false`, call
   `write_back_disk_status(rack_id, node_id, dg_id, &disk_id,
   Offline)` before the `Init → Offline` transition.
c. Verify via integration test that a disk whose zone load fails ends
   up `Offline` in both group 0 and the runtime state machine, and
   stays `Offline` across sync ticks (no flip-flop to `Up`).

## Scope

- `lib/crow-diskdb-client/src/client.rs` — add `trigger_scan`,
  `get_scan_status`, `rebuild_zone_bitmap` wrappers (§2).
- `app/crow-web/src/diskdb.rs` — **new**: REST handlers under
  `/api/diskdb/` + `PUT /api/disks/:id/status` (§4).
- `app/crow-web/src/diskdb_lifecycle.rs` — **new**: deploy/restart/stop
  handlers for `crow-diskdb` (§3).
- `app/crow-web/src/lifecycle.rs` — add `AddDiskBatchBody` +
  `http_add_disks_batch` batch endpoint (§6.3); add `service_type`
  to `ServerEntry`; remove Restart/Stop from Node context menu target
  (moved to Server).
- `app/crow-web/src/state.rs` — `DiskdbClient` field + lazy init;
  `runtime_pid` keyed by `(node_id, service_type)`.
- `app/crow-web/src/lib.rs` — wire new routes.
- `lib/crow-console-shared/src/diskdb.rs` — **new**: diskdb runtime
  methods on `ConsoleClient` + serde model types (§7).
- `lib/crow-console-shared/src/lifecycle.rs` — `DeployRequest` for
  diskdb (binary, ports); `ServiceType` discriminator.
- `app/crow-web/ui/src/contexts/ViewModeContext.tsx` — add
  `Capacity` mode.
- `app/crow-web/ui/src/shell/Sidebar.tsx` — Capacity tree
  (rack→node→disk-group→disk); Physical tree DiskDB Server sub-tree
  (§1.3).
- `app/crow-web/ui/src/App.tsx` — `buildMenuItems` Capacity branch +
  Server case; remove Restart/Stop from Node menu.
- `app/crow-web/ui/src/components/Tree.tsx` — `TreeNode.type` gains
  `'DiskGroup' | 'Disk'`.
- `app/crow-web/ui/src/panels/CapacityPanel.tsx` — **new**: canvas
  capacity visualization (§5).
- `app/crow-web/ui/src/components/dialogs/` — `AddDiskGroupDialog`,
  `AddDiskDialog` (batch), `RemoveDiskDialog`, `MoveDiskDialog`,
  `SetDiskStatusDialog`, `DeployDiskdbDialog` — **new**.
- `app/crow-web/ui/src/api.ts` — diskdb runtime + lifecycle API
  functions.
- `app/crow-web/ui/src/data/` — `useDiskdbInstances`,
  `useDiskdbUsage`, `useCapacityTree` hooks — **new**.
- `app/crow-cli/src/commands/diskdb.rs` — **new**: `crow diskdb`
  subcommands (§8).
- `app/crow-cli/src/main.rs` — wire `Diskdb` variant.
- `app/crow-diskdb/src/liveness/keepalive.rs` — `HardwareClient`
  `Clone` + pass into `background_zone_load` + write-back on
  `Init → Offline` (§9).

## Complexity

**High.** The work spans the full stack: Rust client wrappers, REST
handlers, console-shared models, CLI, and a new React canvas panel.
The genuinely hard parts: (1) the canvas capacity visualization with
offscreen double-buffering at 181×181 scale without flicker on a 3 s
poll; (2) the batch disk-add atomicity (config + group-0 sysdata
transaction across N disks); (3) the `Init → Offline` write-back fix
requires threading `HardwareClient` into a `tokio::spawn`'d background
task without introducing a deadlock or borrow-cycle. The diskdb deploy
path reuses the existing SSH/local-fork mechanism (no new deployment
tech). The view-mode restructure is mostly mechanical (adding a third
mode + moving context-menu items) but touches many files.

## Test Design

### Unit tests (UT)

**DiskdbClient wrappers** (§2):
- `trigger_scan` against a mock gRPC server → returns last
  `ScanSummary` + `scan_in_progress`; transient `Unavailable` →
  retried per `RetryConfig`.
- `get_scan_status` with `has_run=false` → returns empty summary +
  `has_run=false`.
- `rebuild_zone_bitmap(disk_id, zone_index)` routes via `dg_for_disk`
  and returns rebuilt counts; unknown disk → `Unreachable`.

**REST proxy** (§4):
- `GET /api/diskdb/usage` with no `dg` merges results across two
  mock instances (cluster totals = sum of per-instance responses).
- `GET /api/diskdb/usage?dg=1&disk=D&zone=2` returns `ZoneUsage` with
  non-empty `usage_bitmap`.
- `PUT /api/disks/:id/status` writes `HwStatus` via mock
  `HardwareClient`; 404 for unknown disk.

**Batch disk add** (§6.3):
- Batch of 3 valid disks → all 3 created in config + group-0 sysdata.
- Batch with 1 malformed `disk_id` → whole batch rejected, 0 created.
- Batch with duplicate `disk_ids` → 400, 0 created.
- Group-0 sysdata write fails on disk 2 of 3 → rollback all 3, 502.

**Console-shared client** (§7):
- `ConsoleClient` diskdb methods deserialize each REST response into
  the new model types; error responses surface as typed errors. Mock
  HTTP server.

### End-to-end tests (E2E)

**Diskdb deploy + lifecycle** (§3):
- AddNodeDialog creates a node → KV Server + DiskDB Server both
  auto-deploy → both appear in Physical tree under the node →
  DiskDB Server sub-tree shows disk-groups/disks (read-only).
- Right-click DiskDB Server → Stop → process stops, entry persists →
  Restart → process restarts on same ports.

**Capacity view lifecycle** (§6):
- Switch to Capacity view → right-click Node → Add Disk Group → group
  appears in tree → right-click DiskGroup → Add Disk (batch of 3) →
  3 disks appear in tree with `Up` status + zone counts.
- Remove Disk, Move Disk, Set Disk Status each mutate via REST and
  refresh the tree.
- Disk rows show two status columns (Group-0 + Runtime); both render
  `HwStatus` via the status-pill mapping.

**Capacity view visualization** (§5):
- Select a DiskGroup → per-disk boxes render with green→amber→red
  gradient based on busy%.
- Select a disk → zone grid renders (80×80 for 6400 zones); each zone
  box shows gradient; "jump to zone #" input navigates.
- Select a zone → bitmap canvas renders (181×181 for 32K units); busy
  cells red, free cells green; hover shows offset + state.
- 3 s poll refreshes the current view without flicker (canvas
  double-buffer).

**CLI** (§8):
- `crow diskdb status` / `usage` / `scan` / `scan-status` / `recalc`
  / `compact` / `rebuild` against a `crow-web` backed by
  `crow-diskdb` return correct results; unknown disk-id errors
  surface from the REST layer.

**Group-0 write-back** (§9):
- A disk whose zone load fails in `background_zone_load` (both
  strategies fail) transitions `Init → Offline` AND writes `Offline`
  back to group 0; stays `Offline` across sync ticks (no flip-flop).
- A disk going `Missing` then `Bad` (simulated by removing its
  `DiskKey` from group 0) → group-0 `DiskValue.status` updated to
  `Missing` then `Bad` via `write_back_disk_status`; console
  "Group-0" column reflects the detected state on the next read.

## Module Structure

```
lib/crow-diskdb-client/src/client.rs        # +trigger_scan, +get_scan_status, +rebuild_zone_bitmap
lib/crow-console-shared/src/diskdb.rs       # NEW: ConsoleClient diskdb methods + serde models
lib/crow-console-shared/src/lifecycle.rs    # +DeployRequest for diskdb, +ServiceType
app/crow-web/src/diskdb.rs                  # NEW: REST proxy /api/diskdb/*
app/crow-web/src/diskdb_lifecycle.rs        # NEW: diskdb deploy/restart/stop handlers
app/crow-web/src/lifecycle.rs               # +batch disk add, +service_type on ServerEntry
app/crow-web/src/state.rs                   # +DiskdbClient field, pid keyed by (node, type)
app/crow-web/src/lib.rs                     # wire new routes
app/crow-web/ui/src/contexts/ViewModeContext.tsx  # +Capacity mode
app/crow-web/ui/src/shell/Sidebar.tsx       # Capacity tree + Physical DiskDB sub-tree
app/crow-web/ui/src/App.tsx                 # buildMenuItems: Capacity branch + Server case
app/crow-web/ui/src/components/Tree.tsx     # +DiskGroup/Disk TreeNode types
app/crow-web/ui/src/panels/CapacityPanel.tsx     # NEW: canvas capacity viz
app/crow-web/ui/src/components/dialogs/          # NEW: AddDiskGroup, AddDisk (batch), Remove, Move, SetStatus, DeployDiskdb
app/crow-web/ui/src/api.ts                  # diskdb runtime + lifecycle API fns
app/crow-web/ui/src/data/                   # NEW: useDiskdbInstances, useDiskdbUsage, useCapacityTree
app/crow-cli/src/commands/diskdb.rs         # NEW: crow diskdb subcommands
app/crow-cli/src/main.rs                    # wire Diskdb variant
app/crow-diskdb/src/liveness/keepalive.rs   # HardwareClient Clone + Init→Offline write-back
```

## Config Extensions

- `ServerEntry` gains `service_type: ServiceType` (`Kv | Diskdb`),
  default `Kv` for backward compatibility with existing persisted
  configs. `validate()` checks that no two entries on the same node
  share both type and port.
- `AddNodeDialog` defaults: `enableDiskDB = true`,
  DiskDB ports default to `ServicePort::DiskdbGrpc.port(0)` /
  `DiskdbHttp.port(0)` (9941/9942).

## Server Wiring

1. `crow-web` startup: `AppState::new` initializes
   `diskdb_client: RwLock<Option<DiskdbClient>>` as `None` (lazy).
2. First `/api/diskdb/*` request → `build_diskdb_client(&state)`
   constructs a `DiskdbClient` from the `ServiceRegistryClient`,
   stores it in the `RwLock`, calls `refresh_endpoints()`.
3. `AddNodeDialog` submit → `POST /api/nodes` (addNode) →
   `POST /api/nodes/:id/server/deploy` (KV) →
   `POST /api/nodes/:id/diskdb/deploy` (DiskDB) → tree refresh.
4. `crow-diskdb` startup registers itself in the service registry
   (existing R74 behavior); `DiskdbClient.refresh_endpoints` picks it
   up on the next cache miss.

## Resolved decisions

1. **Capacity view sidebar hierarchy** — strictly rack → node →
   disk-group → disk. No server nodes (KV / DiskDB) rendered in the
   Capacity sidebar; infrastructure and service health are visible in
   the Physical view. This keeps the Capacity hierarchy focused on
   capacity concerns only.

2. **Zone grid interaction** — click-to-drill (clicking a zone box
   drills into the bitmap). No hover-preview mini-bitmap (would need a
   low-res bitmap not in the proto or a separate RPC). Hover shows a
   tooltip with the zone id and usage percentage — no bitmap fetch,
   just the brief per-zone entry already loaded at the disk level.