<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB - Design: Console Web UI

Depends on: [`design-crowdb-console.md`](design-crowdb-console.md), [`../kv/design-crowdb-kv.md`](../kv/design-crowdb-kv.md) §15.4.6
Satisfies: [`../kv/design-crowdb-kv.md`](../kv/design-crowdb-kv.md) §15.4.6

This document covers the **frontend SPA design decisions only**:
what we chose and why. Requirements (the *what*) live in
`../kv/design-crowdb-kv.md`; backend API contracts live in `design-crowdb-console.md`.

## Table of Contents

- [1. Goals (recap)](#1-goals-recap)
- [2. Stack decisions](#2-stack-decisions)
- [3. Information Architecture](#3-information-architecture)
  - [3.1 Selection & cross-jump](#31-selection--cross-jump)
- [4. Visual Language](#4-visual-language)
- [5. Topology Canvas (React Flow, slim)](#5-topology-canvas-react-flow-slim)
  - [5.1 Physical layout](#51-physical-layout)
  - [5.2 Logical layout](#52-logical-layout)
  - [5.3 Interactions](#53-interactions)
- [6. Inspector Panel](#6-inspector-panel)
- [6.1 KV Operator Panel (center panel)](#61-kv-operator-panel-center-panel)
- [7. Embedded Swagger Panel](#7-embedded-swagger-panel)
- [8. Embedding Contract](#8-embedding-contract)
- [9. Data & Polling Strategy](#9-data--polling-strategy)
- [10. Module Layout](#10-module-layout)
- [11. Accessibility](#11-accessibility)
- [12. Testing](#12-testing)
- [13. View-Mode Restructure: Physical / Capacity / KV](#13-view-mode-restructure-physical--capacity--kv)
  - [13.1 Three view-modes](#131-three-view-modes)
  - [13.2 Physical view: DiskDB Server sub-tree](#132-physical-view-diskdb-server-sub-tree)
  - [13.3 Capacity view sidebar + dialogs](#133-capacity-view-sidebar--dialogs)
  - [13.4 Batch Add Disk](#134-batch-add-disk)
- [14. DiskDB Server Deploy / Restart / Stop](#14-diskdb-server-deploy--restart--stop)
- [15. REST Proxy for DiskDB Runtime](#15-rest-proxy-for-diskdb-runtime)
- [16. Capacity Panel (Canvas Visualization)](#16-capacity-panel-canvas-visualization)
  - [16.1 Rendering](#161-rendering)
  - [16.2 Color encoding](#162-color-encoding)
  - [16.3 Polling](#163-polling)
  - [16.4 Scope dispatch and module structure](#164-scope-dispatch-and-module-structure)
- [17. Console-Shared DiskDB Client + CLI](#17-console-shared-diskdb-client--cli)
  - [17.1 Console-shared client](#171-console-shared-client)
  - [17.2 CLI subcommands](#172-cli-subcommands)

## 1. Goals (recap)

- Single page, no full-page navigation.
- Three first-class hierarchy views (Physical / Capacity / KV) that
  drive the sidebar tree, the topology canvas, and the inspector
  together.
- Full operator surface: rack/node/server lifecycle, store/group/replica
  CRUD, KV data plane, embedded Swagger.
- Offline-capable: no third-party CDN at runtime.
- Lean: minimal dependencies, no feature the requirement does not mandate.

## 2. Stack decisions

- **React + TypeScript + Vite + TailwindCSS** — carried over from the
  existing codebase; no framework migration.
- **React Flow for topology** — slim usage only (custom nodes, pan, click
  select). Deliberately no minimap, zoom toolbar, layout selector, or edge
  labels. The canvas is a navigation aid, not an analytics surface.
- **React Context for state** — view-mode, selection, toasts, activity.
  No Redux; the state surface is small enough that Context + local hooks
  suffice.
- **No client-side routing** — the SPA mounts at the document root;
  intra-SPA navigation is selection state, not URL navigation. This keeps
  embedding trivial (no history API conflicts).
- **Removed dependencies**: `recharts`, `jspdf`, `jspdf-autotable`,
  `uuid`, `react-router-dom` — none are needed for the lean v1 surface.

## 3. Information Architecture

A fixed three-pane shell. A single root-level **view-mode** (Physical /
Capacity / KV) selects which hierarchy every pane renders.

```
┌─ Header ───────────────────────────────────────────────────────────┐
│ brand · health pill · view toggle (Physical/Logical) · last refresh │
│ · refresh · node selector (Swagger only) · KV / API panel toggles   │
├─ Sidebar ─────┬─ Center panel ─────────────┬─ Inspector ────────────┤
│ filter input  │ Topology canvas (default)  │ Details (key/value)    │
│ hierarchy     │ Swagger panel (API toggle) │ Activity (recent ops)  │
│ tree of the   │ KV Operator panel (KV)     │                        │
│ active view   │                            │                        │
│ (+ context    │                            │                        │
│  menu)        │                            │                        │
└───────────────┴────────────────────────────┴────────────────────────┘
```

- **Header** (~56px): brand label, cluster health pill, Physical/Logical
  toggle, last-refresh time, manual refresh button, node selector
  (consumed only by the Swagger panel).
- **Sidebar** (~240px): a text filter plus the hierarchy tree for the
  active view. Click selects; right-click opens the per-layer context
  menu. No favorites, no recent, no saved presets.
- **Canvas**: React Flow rendering the active view's hierarchy. Drag pans,
  wheel zooms (React Flow default), click selects. Selection is shared
  with the sidebar and inspector via `SelectionContext`. No floating
  toolbar.
- **Center panel**: one of three modes, toggled from the header:
  Topology canvas (default), Swagger panel, or KV Operator panel. The
  KV and Swagger toggles are mutually exclusive with the topology view;
  selecting one replaces the canvas.
- **Inspector** (~320px, collapsible): tabs scoped to the selection:
  Details and Activity only. KV operations have moved to the center KV
  Operator panel (§6.1).

Selection is held in one `SelectionContext`. The shell is rendered once;
switching view-mode swaps the tree data and the canvas layout only.

### 3.1 Selection & cross-jump

Selection is `{ type, id, parentIds }` where `type ∈ { Rack, Node, Server,
Store, Group, Replica }`. Clicking any tree row or canvas node sets it.

Cross-jump (one click) is supported for the common case only:
- Logical `Replica` → "Show on node": switch to Physical, expand the
  owning `Node → Server → Store → Group`, select the matching
  `LocalReplica`.
- Physical `LocalReplica`/`Group` → "Show in cluster": switch to Logical,
  expand the owning `Store → Group`, select the unified row.

No navigation stack / back button in v1.

## 4. Visual Language

Single dark theme via CSS variables under `.crowdb-console` (existing
tokens in `src/index.css`). Status colors: `--healthy`, `--degraded`,
`--failed`, `--unknown`, plus `--remote` for remote-replica accent.

Status is never color-only. Every status row also carries a glyph
(✓ / ! / ✕ / ?). Leader replicas carry a crown badge. Remote replicas use
a dashed border + `--remote` accent so peer-list mis-wirings are visible.

Animations are minimal (selection/hover transitions); honor
`prefers-reduced-motion`.

## 5. Topology Canvas (React Flow, slim)

One layout at a time, chosen by view-mode. Layout is computed by a small
deterministic tree-layout pass in `topology/layout.ts` (columns by depth,
rows by sibling index). No dagre, no force simulation, no user-selectable
layouts.

### 5.1 Physical layout

Renders `Rack → Node → Server → PxStore → PxGroup → {Local, Remote…}`
read from the physical tree. Node types: `Rack`, `Node`, `Server`,
`PxStore`, `PxGroup`, `LocalReplica`, `RemoteReplica`. Edges follow
parent→child containment. Each `RemoteReplica` draws a solid edge to its
peer `LocalReplica` (a missing edge is the bug this view surfaces). The
leader radiates accent edges to followers.

### 5.2 Logical layout

Renders `Cluster → Store → Group → Replica…`. Node types: `Cluster`,
`Store`, `Group`, `Replica` (with a `node_id` badge). The leader radiates
accent edges to followers; no local/remote distinction.

### 5.3 Interactions

- Drag pans, wheel zooms (React Flow built-ins), click selects.
- Selecting a node drives the inspector and highlights the sidebar row.
- Right-click a node opens the same per-layer context menu as the tree.
- Tooltips on hover surface one useful fact (host, leader id, reachable).
- No minimap, zoom toolbar, search box, focus mode, export, or edge
  labels.

## 6. Inspector Panel

Tabs re-render against the current selection:

1. **Details** — labelled key/value table from the selected entity
   (physical or logical shape). Long values support copy-to-clipboard. A
   footer row shows the cross-jump link (§3.1).
2. **Activity** — chronological client-side list of UI-issued operations
   (timestamp, action, target, outcome). No filter/export in v1.

The KV tab has been removed from the Inspector. All KV operations now
live in the center KV Operator panel (§6.1), which provides a full-width
surface with store/group selectors, scan results, and an action bar.

## 6.1 KV Operator Panel (center panel)

A full-width center panel for KV data-plane operations, toggled from the
header via a "KV" button (mutually exclusive with Swagger and topology
canvas). Replaces the former Inspector KV tab, which was too cramped at
320px for comfortable key browsing.

**Design choices:**

- **Flat single-page layout (no tabs)** — action bar on top, scan
  results below. The user can scan, see results, and act (put/get/delete)
  without switching tabs.
- **Store/group selector with "All Groups" option** — when selected,
  scan iterates over every group and merges results (labeled by group).
  Demo inject randomly distributes keys across groups. This avoids
  forcing the user to pick a group when they want a store-wide view.
- **Auto-scan on first load** — when store and group are both set, the
  panel triggers a scan automatically so the user sees data immediately.
- **Independent of ViewMode** — KV operations are always logical
  (store/group), regardless of whether the topology canvas shows the
  physical or logical view.

**Scan pagination (`start_after` token):**

The scan API returns at most `limit` items with a `truncated` flag but
had no way to fetch the next page. Rather than adding a total count
(expensive on large keyspaces), we adopted an S3 ListObjectsV2-style
`start_after` cursor: the caller passes the last key from the previous
batch; the engine returns keys strictly greater than `start_after` that
still match the prefix. The UI shows a "Load more" button when
`truncated` is true; clicking it appends the next batch.

**Decision — `CrowdbTreeEngine` over-fetch + filter:** The C++ crowdb-tree
scan API takes only prefix + limit (no `start_after`). Rather than
modifying C++ immediately, `CrowdbTreeEngine` over-fetches with the
original prefix, then filters out keys ≤ `start_after` in Rust before
applying the limit. This is inefficient when `start_after` is deep into
a large prefix range. A follow-up can push `start_after` into the C++
engine. When `start_after` is empty, the fast path is identical to the
old behavior.

**Demo delete at scale:** "Delete all demo" scans for `demo_` prefix
with pagination (up to 1000 keys for the confirmation count), then
deletes with 16-way parallel `kvDelete`. If more than 1000 keys exist,
scan+delete continues in batches after confirmation. The confirmation
dialog shows "1000+" when the count may be higher.

## 7. Embedded Swagger Panel

- Lives inside the SPA; opening it does not navigate or open a new tab.
- Hosts an `<iframe>` at `${apiPrefix}/swagger/?url=${apiPrefix}/nodes/:node_id/openapi.json`,
  where `:node_id` is the header's node selector.
- Switching the node reloads the iframe `url` only.
- Loaded lazily (code-split) so initial page load is not blocked.

## 8. Embedding Contract

The SPA is mountable as a sub-component with a minimal props interface
(`apiPrefix`, `basePath`, `readonly`, `modules` opt-out, `initialViewMode`,
`onEvent` callback). Three isolation rules:

- **Style isolation** — everything wraps in `.crowdb-console`; Tailwind
  uses the `tw-` prefix and `important: '.crowdb-console'`.
- **API isolation** — every fetch resolves against `apiPrefix`.
- **Standalone** — `index.html` mounts at the document root with defaults;
  `embed.ts` exports the component for hosts.

## 9. Data & Polling Strategy

- **Two-tree contract** — the SPA speaks physical (`/api/racks`,
  `/api/nodes`) and logical (`/api/stores`) trees per `design-crowdb-console.md`.
  No panel constructs raw `host:port` URLs; `api.ts` is the single URL
  builder.
- **Asymmetric polling** — only the active view polls fast (~5s); the
  inactive view polls slow (~30s) so toggling renders immediately.
  Polling pauses while the tab is hidden.
- **Optimistic-free mutations** — mutations call the backend, await
  success, then trigger a refresh of the affected view; they do not
  hand-edit cached data. This trades a round-trip for correctness
  simplicity.

## 10. Module Layout

The source tree follows the pane structure: `shell/` (Header, Sidebar,
Inspector), `topology/` (canvas + layout), `panels/` (KvOperatorPanel,
SwaggerPanel, ActivityLog), `components/` (Dialog, ContextMenu, dialogs,
UI primitives), and `contexts/` (ViewMode, Selection, Toast, Activity).
`api.ts` and `types/index.ts` are the single URL-builder and data-model
modules respectively.

**Deleted from v1**: CommandPalette, favorites, fuzzy search, export
utils, bulk action dialog, metrics history, theme context. None are
needed for the lean surface.

## 11. Accessibility

- Keyboard reachable: Tab/Enter/Escape on tree rows, dialogs, and menus;
  context menus mirror to keyboard-activatable buttons where practical.
- Color is never the sole status channel (glyph + color).
- Strings go through a single `t(key)` helper (English only) so a future
  locale pack needs no source changes. (Optional for v1; may inline.)

## 12. Testing

- Existing Vitest unit tests for dialog request bodies and `listRacks`
  envelope handling are **retained** (they pin the backend contract).
- The Playwright real-backend E2E suite (`app/crowdb-web/ui/e2e/`)
  targets this lean SPA; selectors track the rewritten DOM. The full
  chain rack→node→deploy→store→group→replica→KV is the acceptance bar.

---

## 13. View-Mode Restructure: Physical / Capacity / KV

The header view-toggle expanded from two modes (Physical | Logical) to
three (Physical | Capacity / KV). This separation gives each view a
single responsibility: Physical handles infrastructure + service
management, Capacity handles disk-group/disk lifecycle + capacity
visualization, and KV handles KV data-plane operations. Disk lifecycle
actions (Add Disk Group, Add Disk) do not belong on the Physical view's
Node context menu. They are capacity concerns, not infrastructure
concerns. Splitting them into a dedicated Capacity view also lets the
Capacity center panel render capacity visualization without competing
with the topology canvas.

### 13.1 Three view-modes

- **Physical** — rack → node → server(s) → (KV: store → group;
  DiskDB: disk-group → disk). Infrastructure + service management
  only. Read-only resource listing under each server. Context menus:
  - Rack: Add Node, Delete Rack.
  - Node: Ping, Delete Node. No Add Disk Group / Add Disk.
  - Server (KV or DiskDB): Restart, Stop, (if no server: Deploy).
  - Server sub-tree (store/group/disk-group/disk): read-only, no
    context menu.
- **Capacity** — rack → node → disk-group → disk (no server nodes —
  infrastructure is visible in Physical). The only place to
  add/remove/move/set-status disk groups and disks. Center panel
  renders capacity visualization (§16). Context menus:
  - Node: Add Disk Group.
  - DiskGroup: Add Disk (batch), Remove Disk Group, Set Status.
  - Disk: Remove Disk, Move Disk, Set Disk Status.
- **KV** (renamed from "Logical") — cluster → store → group →
  replica. KV data-plane operations via the KV Operator panel (§6.1).
  Unchanged from the former Logical view except the name.

`ViewModeContext` expanded from `Physical | Logical` to
`Physical | Capacity | KV`. The sidebar tree data hook switches on
the active mode; the center panel mode set changes per mode (Physical
→ topology canvas; Capacity → capacity panel; KV → KV operator
panel).

### 13.2 Physical view: DiskDB Server sub-tree

The Physical view's `Server` tree node renders both server types
under a Node:

- `KV-${nodeId}` — existing; children `Store → Group → Replica`.
- `DDB-${nodeId}` — new; children `DiskGroup → Disk` (read-only,
  fetched from `GET /api/nodes/:id/disk-groups` +
  `GET /api/nodes/:id/disk-groups/:dg_id/disks`).

`TreeNode.type` gains `'DiskGroup' | 'Disk'` variants. The
`serverByNodeId` lookup is extended to a `servicesByNodeId` map that
returns both KV and DiskDB server entries; each renders as a separate
`Server`-typed child with a distinguishing icon (Cog for KV, HardDrive
for DiskDB).

Context menu on a `Server` node: Restart →
`POST /api/nodes/:id/server/restart` (KV) or
`POST /api/nodes/:id/diskdb/restart` (DiskDB, §14). Stop →
`POST /api/nodes/:id/server/stop` (KV) or
`POST /api/nodes/:id/diskdb/stop` (DiskDB, §14). Labeled "Stop" not
"Delete". It stops the process but keeps the deployment record so
Restart/Deploy can bring it back. If no server deployed: Deploy →
opens `DeployServerDialog` (existing for KV) or `DeployDiskdbDialog`
(new, §14).

The Restart/Stop items on the Node menu are removed. Server-process
ops belong on the Server node, not the Node. Node menu keeps only
Ping + Delete Node (+ Deploy if no server).

Edge cases:
- Node with KV deployed but no DiskDB → only `KV-${nodeId}` child
  renders; no `DDB-${nodeId}` node.
- DiskDB deployed but owns zero disk-groups → `DDB-${nodeId}` renders
  with empty children (leaf with no expand arrow).
- KV server stopped (pid cleared) but entry persisted → still renders
  in the tree; Restart available, health = Unknown.

### 13.3 Capacity view sidebar + dialogs

The Capacity view sidebar renders rack → node → disk-group → disk
(no server nodes; infrastructure is visible in Physical). Disk
lifecycle dialogs (Add Disk Group, Add Disk, Remove, Move, Set Status)
are only accessible here.

Sidebar tree for Capacity mode:
- Rack → Node → DiskGroup → Disk, fetched from
  `GET /api/nodes/:id/disk-groups` + `.../disks`.
- `TreeNode.type` variants `'DiskGroup' | 'Disk'` (from §13.2).
- Disk rows show two status columns: "Group-0" (from
  `HardwareClient.get_disk` via
  `GET /api/nodes/:id/disk-groups/:dg_id/disks/:disk_id`) and
  "Runtime" (from `QueryCapacityStats` via `/api/diskdb/usage`).
  Both use the existing `toUiHealth`-style mapping over `HwStatus`.

Context menus (Capacity branch):
- Node: Add Disk Group → `AddDiskGroupDialog`.
- DiskGroup: Add Disk → `AddDiskDialog` (batch, §13.4); Remove Disk
  Group; Set Status.
- Disk: Remove Disk; Move Disk; Set Disk Status.

Dialogs (follow `AddNodeDialog`/`ConfirmDeleteDialog` patterns):
- `AddDiskGroupDialog` — id, name. Calls
  `POST /api/nodes/:id/disk-groups`.
- `AddDiskDialog` — batch (§13.4). Calls
  `POST /api/nodes/:id/disk-groups/:dg_id/disks/batch`.
- `RemoveDiskDialog` / `RemoveDiskGroupDialog` — confirm + cascade
  warning. Calls `DELETE .../disks/:id` / `DELETE .../disk-groups/:id`.
- `MoveDiskDialog` — target rack/node/disk-group. Calls
  `PUT .../disks/:id/move`.
- `SetDiskStatusDialog` — `HwStatus` enum dropdown. Calls
  `PUT /api/disks/:id/status` (§15).

### 13.4 Batch Add Disk

A batch endpoint for atomic all-or-nothing disk creation:

```rust
pub struct AddDiskBatchBody {
    disks: Vec<AddDiskItem>,
}
pub struct AddDiskItem {
    disk_id: String,
    disk_type: String,
    capacity_bytes: u64,
    zone_size_bytes: u64,
    unit_size_bytes: u32,
}
```

Route: `POST /api/nodes/:id/disk-groups/:dg_id/disks/batch`.

- Validates all `disk_id` formats upfront; rejects the whole batch if
  any is malformed (atomic).
- Writes all disks to config + group-0 sysdata in one transaction;
  if any write fails, rolls back (no partial success).
- `AddDiskDialog` UI: a row-builder where each row auto-generates a
  UUID (editable), disk_type dropdown, capacity (default 4 TiB),
  zone_size (default 32 GiB), unit_size (fixed 1 MiB, disabled
  input). "Add Row" / "Remove Row" buttons. Submit sends the whole
  batch.
- Defaults: `capacity_bytes = 4 * 1024^4`, `zone_size_bytes = 32 *
  1024^3`, `unit_size_bytes = 1024^2` (locked).

Edge cases:
- Batch with duplicate disk_ids → 400, whole batch rejected.
- Group-0 sysdata write fails for disk 3 of 5 → rollback all 5, 502.
- Empty batch → 400.

## 14. DiskDB Server Deploy / Restart / Stop

The Physical view's DiskDB Server node needs the same service-lifecycle
ops as KV Server (Restart, Stop, Deploy). The deploy/restart/stop
handlers enable `AddNodeDialog` to auto-deploy DiskDB alongside KV,
and the Server context menu works for both types.

Deployment mechanism: SSH or local fork, same as KV. No Docker. The
`crowdb-diskdb` binary is spawned via `ssh::deploy_via_ssh` or
`lifecycle::deploy_local_in_dir`, on the paired ports from
`ports.rs` (`DISKDB_RPC_BASE` + `DISKDB_HTTP_BASE`).

New handlers mirroring the KV handlers:

```rust
pub struct DeployDiskdbBody {
    rpc_port: u16,
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

- `http_deploy_node_diskdb` — checks no existing diskdb on the node
  (409 if present), resolves the node, builds a `DeployRequest` with
  `rpc_port`/`http_port` from `ServicePort::DiskdbRpc`/`DiskdbHttp`
  defaults (or body overrides), spawns via SSH or local fork, persists
  a `ServerEntry` with `service_type: Diskdb`, records the pid.
  Route: `POST /api/nodes/:id/diskdb/deploy`.
- `http_restart_node_diskdb` — stops the tracked pid, re-deploys on
  the same ports from the persisted entry. Route:
  `POST /api/nodes/:id/diskdb/restart`.
- `http_stop_node_diskdb` — stops the tracked pid, clears it, keeps
  the entry. Route: `POST /api/nodes/:id/diskdb/stop`.
- `ServerEntry` / `AppState` gains a `service_type` discriminator
  (`Kv | Diskdb`) so `server_for_node` can return the right entry per
  type. `runtime_pid` tracking is keyed by `(node_id, service_type)`.
- `AddNodeDialog` calls `deployServer` (KV) then `deployDiskdb` (new
  API function) after `addNode` succeeds. Both are gated by the
  existing `enableCrowDB`-style checkbox (add `enableDiskDB`, default
  true).

Edge cases:
- Node with KV deployed but DiskDB deploy fails → KV stays deployed;
  the dialog reports the DiskDB failure; the operator can retry via
  the Server context menu's Deploy.
- DiskDB binary not found on the remote host → SSH deploy returns an
  error; surfaced as 502.
- Port conflict (another process on 9941/9942) → spawn fails; surfaced
  as 502. The handler does not pre-check ports (best-effort, matches
  KV behavior).

## 15. REST Proxy for DiskDB Runtime

`crowdb-web` proxies diskdb runtime RPCs (`QueryCapacityStats` drill-down,
scan, recalc, compact, rebuild) via REST endpoints under
`/api/diskdb/`. The CLI and web UI route through `crowdb-web` (no direct
crowdb-rpc from the browser or CLI). `AppState` owns a `DiskdbClient` built
from the same `ServiceRegistryClient` the console already uses:

```rust
diskdb_client: tokio::sync::RwLock<Option<DiskdbClient>>,
```

The `DiskdbClient` is lazily initialized on first diskdb REST request
(the service registry may not be ready at console startup).

Handlers:

- `GET /api/diskdb/instances` — reads `read_all_diskdb_instances`
  from the service registry directly (no crowdb-rpc fan-out). Returns
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
  no such endpoint existed before (only add/remove/move).

`GET /api/diskdb/usage` with no `dg` iterates
`read_all_diskdb_instances`, calls `query_capacity_stats` per
instance, merges `DiskGroupInfo` entries by id (summing
capacity/busy/free). A dead instance yields a degraded indicator,
not a failed page. Its contribution is skipped with a warning.

`PUT /api/disks/:disk_id/status` resolves the disk's rack/node/dg
from config, then calls `hw.set_disk_status`. 404 if the disk is
not in config.

Edge cases:
- Cluster overview with a dead instance → merged response excludes
  it; the `/instances` endpoint still lists it (with stale heartbeat)
  so the UI can show the degraded card.
- Zone drill-down → bitmap is omitted at disk level (flatbuffer contract);
  the UI issues the zone-level query separately.
- Scan already running → `trigger_scan` returns `scan_in_progress:
  true`; handler passes it through (no error).

## 16. Capacity Panel (Canvas Visualization)

The Capacity view center panel renders capacity visualization that
scales to thousands of zones per disk and tens of thousands of blocks
per zone. Canvas with offscreen double-buffering handles 84×84 zone
grids and 181×181 bitmap grids without flicker. DOM/SVG rendering at
that scale causes layout thrash and jank.

`CapacityPanel.tsx` renders when `viewMode === Capacity`. The panel
content depends on the selected entity (from `SelectionContext`):

- **Cluster (Datacenter or no selection)** — per-rack breakdown. One
  row per rack with DG count, node count, and a capacity/busy/free
  bar. The cluster-wide scan status summary + trigger
  (`ScannerPanel`) renders here only. Data from
  `GET /api/diskdb/usage` (cluster merge).
- **Rack selected** — per-node breakdown within the rack. One row per
  node with DG count and a capacity/busy/free bar. Data from
  `GET /api/diskdb/usage` (cluster merge, client-filtered).
- **Node selected** — per-DG breakdown. One row per DG on the node
  with disk count (array icon + count, not per-disk boxes) and a
  capacity/busy/free bar. Data from `GET /api/diskdb/usage` (cluster
  merge, client-filtered).
- **DiskGroup selected** — per-disk boxes. Each disk is a box with a
  busy% gradient fill (green → amber → red, red = busy) + inline `%`
  label + tooltip (disk id + busy%). Data from
  `GET /api/diskdb/usage?dg=<id>`.
- **Disk selected** — zone grid + per-disk actions. Each zone is a
  box in a square grid (side = ceil(sqrt(zone_count))) with a
  green→amber→red gradient based on busy%. Hover shows a tooltip
  with zone id + usage %. A "jump to zone #" input handles direct
  navigation (7000 zones cannot be a dropdown). All disk-scoped
  actions are inline in the disk header: Scan and Recalc target the
  disk's parent DG (`triggerDiskdbScan` / `recalcDiskdbUsage` with
  the DG id); Compact, Rebuild, Up, and Down target the disk itself
  (`compactDiskdbZones` / `rebuildDiskdbZoneBitmap` /
  `setDiskStatus`). The per-DG recalc result (`RecalcPanel`) renders
  here, scoped to the parent DG. Data from
  `GET /api/diskdb/usage?dg=<id>&disk=<disk_id>` (brief per-zone
  entries, no bitmap).
- **Zone selected (in-panel, within the Disk view)** — zone bitmap.
  Canvas grid of the zone's `usage_bitmap`
  (side = ceil(sqrt(unit_count))). Busy block = red filled cell, free
  block = green filled cell. Zone is not a sidebar entity; it is an
  in-panel click state inside the Disk view. Data from
  `GET /api/diskdb/usage?dg=<id>&disk=<disk_id>&zone=<zi>` (full
  bitmap, on-demand only).

### 16.1 Rendering

Canvas, not SVG/DOM, for all levels:
- Offscreen canvas double-buffering: draw to an offscreen canvas,
  then `drawImage` blit to the visible canvas in one call. The
  visible canvas is never cleared-then-slowly-drawn (that flickers).
- Single `requestAnimationFrame` sync redraw for grids up to 181×181
  (32K cells) — fast enough to not flicker.
- On data refresh (3 s poll), retain the previous frame until the
  new one is fully drawn, then swap. No blank intermediate state.
- No DOM reflow. The canvas is a single element; only its bitmap
  content changes.

### 16.2 Color encoding

Green (free) → amber → red (busy):
- Zone/disk boxes: gradient fill based on `busy_blocks /
  unit_capacity` ratio. 0% = green, ~50% = amber, 100% = red.
- Bitmap cells: binary — busy = red filled, free = green filled.
- Redundant encoding: each zone/disk box shows a `%` text label on
  hover (zone id + usage %) or inline so the information is not
  color-only (color-blind friendly).

### 16.3 Polling

3 s refresh of the currently focused visualization:
- The poll refetches only the data for the selected entity level
  (rack/node → cluster merge; disk-group → dg query; disk → disk
  query; zone → zone query).
- On refetch, the canvas redraws via double-buffer (no flicker).
- If the selection changes, the poll target switches immediately;
  the old canvas is cleared on the next draw.

Zone count math (for layout):
- 200 TB disk / 32 GB zone = 6400 zones → 80×80 grid.
- 32 GB zone / 1 MB unit = 32K units → 181×181 grid.

Edge cases:
- Disk with 0 zones (freshly added, zone load in progress) → empty
  grid placeholder with "loading" text.
- Zone with `used_count == unit_capacity` → all cells red; reported
  as-is.
- `usage_bitmap` shorter than `unit_capacity` (last zone rounded) →
  pad with free (green) cells.
- Poll response slower than 3 s → keep previous frame; next poll
  catches up. No spinner overlay (would flicker).

### 16.4 Scope dispatch and module structure

`CapacityPanel` derives a `CapacityScope` (`Cluster | Rack | Node |
DiskGroup | Disk`) from the selected entity and renders one branch per
scope. The header (title + totals cards) is common to all scopes; only
the body branches. Each scope has a dedicated subview:

- `ClusterView` — per-rack breakdown + `ScannerPanel` (cluster-wide
  scan status + trigger).
- `RackView` — per-node breakdown.
- `NodeView` — per-DG breakdown.
- `DiskGroupView` — per-disk box grid.
- `DiskView` — zone grid (`ZoneGrid`) + zone bitmap (`ZoneBitmap`) +
  jump-to-zone input + per-disk action buttons + `RecalcPanel`
  (scoped to the parent DG).

Shared color/format utilities live in `utils/capacity.ts`:
- `busyColor(pct)` — green → amber → red gradient (4-step thresholds
  30/60/85/100), shared by `DiskGroupView` disk boxes, `ZoneGrid`,
  and the per-rack/per-node bars.
- `busyPct`, `formatBytes` — formatting helpers.

`useZoneBitmap(dg, disk, zone)` fetches the zone bitmap on demand
when a zone is clicked and caches the last result; the 3 s poll
refetches the focused zone via its `refresh` callback.

## 17. Console-Shared DiskDB Client + CLI

### 17.1 Console-shared client

`ConsoleClient` in `crowdb-console-shared` is the typed REST client used
by both the web UI (via `api.ts` wrappers) and `crowdb-cli`. It has
diskdb runtime methods + serde model types so the CLI and UI share one
deserialization path.

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

Serde model types (mirrors of the flatbuffer responses):
`DiskdbInstanceInfo`, `DiskGroupUsageSummary`, `DiskGroupUsage`,
`DiskUsage`, `ZoneUsage`, `ScanSummary`, `RecalcResult`,
`CompactionResult`, `RebuildResult`, `UsageResponse`.

### 17.2 CLI subcommands

Runtime queries (usage/zones/scan/recalc/compact/rebuild) are
reachable from the command line via `crowdb diskdb` subcommands.
Lifecycle stays in `crowdb disk` / `crowdb disk-group`; `diskdb` is
runtime queries only.

```
crowdb diskdb status                          — /api/diskdb/instances
crowdb diskdb usage [--dg <id>] [--disk <id>] [--zone <zi>]
crowdb diskdb scan [--dg <id>]                — trigger
crowdb diskdb scan-status [--dg <id>]
crowdb diskdb recalc [--dg <id>]
crowdb diskdb compact <disk_id> [--zones <zi,...>]
crowdb diskdb rebuild <disk_id> [--zone <zi>]
```

All route through `ConsoleClient` → `crowdb-web` → `DiskdbClient` →
crowdb-rpc; no direct talk to `crowdb-diskdb`.
