<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R127: console — Web UI migrate to shared ops + layout rework

**Problem**

R126 restructured the CLI to call `crowdb_console_shared::ops::*` directly
(group-0 sysdata + kv-server mgmt), eliminating the `crowdb-web`
intermediary for CLI operations. The web backend (`crowdb-web`) still
duplicates that orchestration logic in its Axum handlers — each handler
manually wires `AppState` + `ServerClient` + `crowdb_kv_client` +
`HardwareClient` + `KVClusterMetaClient`, replicating what
`ops::hardware`, `ops::kv_server`, `ops::kv_logical`, `ops::kv_data`,
and `ops::cluster` already implement. This divergence means every
feature must be implemented twice, and the two surfaces drift (R126
already added `cluster reset`, `cluster clean`, and `kv server delete`
require-empty to the CLI — none are in the web backend).

Beyond the backend duplication, the current UI layout (three-pane shell
with Physical/Capacity/KV view-modes, `design-crowdb-console-ui.md` §3)
was designed before the `ops` module existed. The four-domain structure
(`cluster`/`kv`/`chunk`/`bench`) that R126 introduced for the CLI is a
cleaner mental model and should inform the UI's information architecture.
The current three view-modes (Physical / Capacity / KV) do not map
cleanly to the four domains, and the sidebar tree + topology canvas +
inspector layout was designed for resource-type browsing, not
domain-oriented workflows.

**Current behavior + impact**

- Web handlers in `app/crowdb-web/src/lifecycle.rs`,
  `mgmt/store_ops.rs`, `mgmt/group_ops.rs`, `mgmt/replica_ops.rs`,
  `mgmt/cluster_init.rs`, `kv.rs`, `diskdb.rs`, `diskdb_lifecycle.rs`
  contain ~2000 lines of orchestration logic that duplicates `ops::*`.
- `cluster reset` and `cluster clean` exist only in the CLI — the web
  UI has no equivalent (`POST /internal/reset` is a different, older
  teardown path).
- `kv server delete` with require-empty check exists only in the CLI.
- The web UI's three view-modes (Physical / Capacity / KV) predate the
  four-domain CLI structure and don't align with it.
- Root cause: R126 moved the CLI to `ops` but left the web backend
  unchanged; the UI layout was never revisited.

**Design pointers**

- `design-crowdb-console.md` §2.1 (Call Path) — the CLI now calls
  `shared`'s `ops` module directly; the web should do the same.
- `design-crowdb-console.md` §2.2 (Reuse Boundary) — "both frontends
  share the same `shared` entry points."
- `design-crowdb-console.md` §7 (CLI Design) — four-domain structure
  (`cluster`/`kv`/`chunk`/`bench`).
- `design-crowdb-console-ui.md` §3 (Information Architecture) — current
  three-pane shell + three view-modes.
- `design-crowdb-console-ui.md` §13 (View-Mode Restructure) — current
  Physical / Capacity / KV split.

**Use scenarios**

- Operator opens the web UI to initialize a new cluster: selects nodes,
  clicks "Init Cluster", sees progress + resulting topology. The web
  handler calls `ops::cluster::init` (same as CLI), not a hand-rolled
  5-phase bootstrap.
- Operator deploys a KV server on a node via the web UI: fills deploy
  dialog, clicks "Deploy". The handler calls `ops::kv_server::deploy`,
  which spawns the process and records the `ServerEntry` — same logic
  as `crowdb-cli kv server deploy`.
- Operator deletes a KV server via the web UI: the handler calls
  `ops::kv_server::delete`, which checks that no replicas reference the
  node (require-empty) before stopping the process. If replicas exist,
  the UI shows a conflict error listing the dependent resources.
- Operator resets the cluster via the web UI: clicks "Reset Cluster",
  confirms. The handler calls `ops::cluster::reset`, which tears down
  all groups, stores, servers, and sysdata in dependency order.
- Operator browses the cluster in the web UI using a domain-oriented
  layout (cluster / kv / chunk) instead of resource-type view-modes
  (Physical / Capacity / KV), matching the CLI's mental model.

**Solution**

**No clear solution yet — deferred to design.** The backend migration
approach is clear (share `ops` module, same as CLI), but the UI layout
needs agreement before implementation. Below are the two parts and
their open questions.

**Backend migration (approach clear):**

One-line summary: web Axum handlers become thin wrappers around
`ops::*` functions, constructing `OpContext` from `AppState` per
request.

1. **`OpContext` provider for web** — `app/crowdb-web/src/state.rs`:
   add a method to `AppState` that builds an `OpContext` from the
   persisted config + cached KV client + group-0 mgmt seeds. The web's
   `OpContext` shares the same `Arc<CrowdbKvClient>` as `AppState`'s
   cached client, avoiding duplicate connection pools.
2. **Migrate lifecycle handlers** — `app/crowdb-web/src/lifecycle.rs`:
   replace direct config writes + `HardwareClient` calls with
   `ops::hardware::*` and `ops::kv_server::*`.
3. **Migrate mgmt handlers** — `app/crowdb-web/src/mgmt/store_ops.rs`,
   `group_ops.rs`, `replica_ops.rs`, `cluster_init.rs`: replace
   fan-out `ServerClient` orchestration with `ops::kv_logical::*` and
   `ops::cluster::*`.
4. **Migrate KV data-plane** — `app/crowdb-web/src/kv.rs`: replace
   manual leader resolution + `CrowdbKvClient` calls with
   `ops::kv_data::*`.
5. **Add missing web endpoints** — `POST /api/cluster/reset`,
   `POST /api/cluster/clean`, `DELETE /api/nodes/:id/server` (with
   require-empty). These mirror CLI commands added in R126.
6. **DiskDB runtime** — `app/crowdb-web/src/diskdb.rs`: decide whether
   to route through `ops::chunk::*` (currently stubs) or retain the
   existing proxy layer. Deferred to design.

**UI layout (needs agreement):**

One-line summary: replace the current three view-modes (Physical /
Capacity / KV) with three domains (Cluster / KV / Chunk) that map to
the CLI's domain structure, each with a distinct sidebar tree, center
panel layout, and shared right inspector.

The shell keeps the same three-pane structure (sidebar / center /
inspector) but the header's view-mode toggle switches between three
domains instead of three view-modes. The `bench` domain is omitted from
the UI (CLI-only — load injection is not a UI workflow).

```
┌─ Header ───────────────────────────────────────────────────────────┐
│ brand · health pill · domain toggle (Cluster/KV/Chunk) · refresh   │
├─ Sidebar ─────┬─ Center panel ─────────────┬─ Inspector ────────────┤
│ (per-domain   │ (per-domain layout,        │ Details (key/value)    │
│  tree, see    │  see below)                │ Activity (recent ops)  │
│  below)       │                            │                        │
│               │                            │ (unchanged — scoped    │
│               │                            │  to current selection) │
└───────────────┴────────────────────────────┴────────────────────────┘
```

The right inspector panel is unchanged across all three domains: it
shows Details + Activity tabs scoped to the currently selected item
(rack, node, disk-group, disk, server, store, group, replica, chunkdb,
diskdb, diskio — whichever the operator clicked in the sidebar or
canvas).

**Domain 1 — Cluster (hardware topology)**

Sidebar shows the full hardware hierarchy: rack → node → disk-group →
disk. Right-click on each layer opens the CRUD context menu (add rack,
add node, add disk-group, add disk, remove, etc.). Cluster-level ops
(init / reset / clean / status) are triggered from the header or a
toolbar above the canvas.

Center panel shows a hierarchy chart of the hardware topology. Rack and
node render as standard tree/canvas nodes. Disk-groups and disks use
special visual elements:

- A **disk-group** renders as a box containing an array of disk
  elements.
- Each **disk** renders as a disk-style element (cylinder icon) showing
  a short UUID prefix.

```
┌─ Sidebar ─────┐┌─ Center: hierarchy chart ─────────────────────────┐
│ ▾ Rack 1      ││  Rack 1                                           │
│   ▾ Node 1    ││   ├─ Node 1                                       │
│     ▾ DG-0    ││   │   └─ ┌─────────────────────────────┐          │
│       • disk0 ││   │       │ DG-0                       │          │
│       • disk1 ││   │       │  ⬢ a3f1  ⬢ b7c2  ⬢ e9d4   │          │
│   ▸ Node 2    ││   │       └─────────────────────────────┘          │
│ ▾ Rack 2      ││   └─ Node 2 ...                                   │
│   ▸ Node 3    ││  Rack 2 ...                                       │
└───────────────┘└───────────────────────────────────────────────────┘
```

**Domain 2 — KV (server lifecycle + logical + data-plane)**

Sidebar shows rack → node only (disk-groups and disks are hidden — not
relevant to KV management). Right-click on a node opens the KV-server
context menu (deploy / restart / stop / delete). Right-click on a
deployed KV-server opens the logical context menu (add store, add
group, add replica).

Center panel is split vertically into two resizable halves:

- **Top half**: logical view — the cluster's store → group → replica
  hierarchy rendered as a topology canvas (React Flow). This is the
  same logical tree the current KV view-mode shows.
- **Bottom half**: KV operation panel — put / get / delete / scan
  controls targeting the selected store/group. This is the current KV
  Operator panel, moved into the bottom half of the center panel.

The top/bottom split is draggable (resize handle). The operator can
collapse the bottom half to give the topology canvas full height, or
collapse the top half to focus on KV operations.

```
┌─ Sidebar ─────┐┌─ Center: logical view (top) ──────────────────────┐
│ ▾ Rack 1      ││  Store 1                                          │
│   ▾ Node 1    ││   ├─ Group 1                                      │
│     ▸ kv-srv  ││   │   ├─ Replica 0 (node 1)                       │
│   ▸ Node 2    ││   │   └─ Replica 1 (node 2)                       │
│ ▾ Rack 2      ││   └─ Group 2 ...                                  │
│   ▸ Node 3    ││  Store 2 ...                                      │
│               │├ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┤
│               ││  KV Operator: [store ▾] [group ▾]                 │
│               ││  key: [_______]  value: [_______]  [Put]          │
│               ││  key: [_______]  [Get]  [Delete]                  │
│               ││  scan: [prefix___] [Scan]  results: ...           │
└───────────────┘└───────────────────────────────────────────────────┘
```

**Domain 3 — Chunk (chunkdb / diskdb / diskio management)**

Sidebar shows rack → node only (disk-groups and disks hidden at the
top level). Under each node, the sidebar shows the deployed chunkdb,
diskdb, and diskio server instances. Under a diskdb server, the sidebar
expands to show the owned disk-group and its disks (read-only — managed
from the Cluster domain).

Center panel has a button toggle with two sub-views:

- **Capacity** — the capacity canvas visualization (same as the current
  Capacity view-mode). Shows disk-group usage, disk health, and
  allocation heatmaps.
- **Chunk** — chunk-level view showing chunkdb instances, their bound
  ranges, and chunk metadata. (This sub-view is a stub initially,
  matching the CLI's `chunk` domain stubs — it will be filled in when
  `ops::chunk` is implemented.)

```
┌─ Sidebar ─────┐┌─ Center: [Capacity] [Chunk] ──────────────────────┐
│ ▾ Rack 1      ││  (Capacity sub-view shown by default)              │
│   ▾ Node 1    ││                                                   │
│     ▸ chunkdb ││  Rack 1 / Node 1 / DG-0                           │
│     ▸ diskdb  ││   ┌─────────────────────────────────────┐         │
│       ▸ DG-0  ││   │ ▓▓▓▓▓░░░░  ▓▓▓░░░░░░  ▓▓▓▓▓▓░░    │         │
│         • d0  ││   │ disk a3f1   disk b7c2   disk e9d4   │         │
│         • d1  ││   │ 78% used    42% used    88% used    │         │
│     ▸ diskio  ││   └─────────────────────────────────────┘         │
│   ▸ Node 2    ││                                                   │
│ ▾ Rack 2      ││  Rack 1 / Node 2 / DG-1 ...                       │
│   ▸ Node 3    ││                                                   │
└───────────────┘└───────────────────────────────────────────────────┘
```

**Cross-domain behaviors**

- Selection is shared across all three domains via the same
  `SelectionContext`. Clicking a node in the Cluster domain, switching
  to the KV domain, and right-clicking that node shows the KV-server
  context menu for the same node.
- The inspector (right panel) is domain-agnostic — it renders details
  for whatever is selected, regardless of which domain is active.
- The header domain toggle replaces the current Physical/Capacity/KV
  toggle. The embedding contract's `initialViewMode` becomes
  `initialDomain` with values `cluster` / `kv` / `chunk`.

**Edge cases at a glance**

- `OpContext` build fails (group-0 unreachable) → web handler returns
  502 with a clear error; UI shows "cluster not initialized" state.
- `ops::kv_server::delete` require-empty fails → web handler returns
  409 Conflict with the list of dependent replicas; UI shows a
  confirmation dialog listing what must be removed first.
- `ops::cluster::reset` partial failure → best-effort teardown; UI
  shows a progress log + any remaining resources.
- Web handler called while config is being mutated by another request
  → `RwLock` on `OpContext.config` serializes access; no corruption.
- DiskDB runtime proxy needs a `DiskdbClient` not in `OpContext` →
  retained as a separate `AppState`-scoped client until `ops::chunk`
  is implemented.

**Dependencies**

- **Depends on R126** (completed) — `ops` module in
  `crowdb-console-shared` must exist with `OpContext`,
  `ops::hardware`, `ops::kv_server`, `ops::kv_logical`, `ops::kv_data`,
  `ops::cluster`.
- **Depends on UI layout agreement** — the UI layout is defined in
  the Solution section above; implementation can proceed once the
  layout is reviewed and confirmed. The backend migration can proceed
  independently of the UI layout implementation.
- **Blocks future chunk/diskdb UI work** — the `ops::chunk` module is
  currently stubs; a full chunk/diskdb UI depends on implementing
  `ops::chunk` first.

**Acceptance**

**Backend migration:**

- `POST /api/cluster/init` calls `ops::cluster::init` (not hand-rolled
  5-phase bootstrap) → verify via handler unit test that mocks
  `OpContext` and asserts the `ops::cluster::init` call path.
  Integration test.
- `POST /api/racks` calls `ops::hardware::add_rack` → verify config
  mutation + sysdata sync via handler test. Integration test.
- `POST /api/nodes` calls `ops::hardware::add_node` → verify config
  mutation + sysdata sync via handler test. Integration test.
- `POST /api/nodes/:id/server/deploy` calls `ops::kv_server::deploy`
  → verify process spawn + `ServerEntry` recording via handler test.
  Integration test.
- `DELETE /api/nodes/:id/server` calls `ops::kv_server::delete` with
  require-empty check → verify 409 Conflict when replicas exist, 200
  when empty. E2E test.
- `POST /api/stores` calls `ops::kv_logical::add_store` → verify
  fan-out + rollback via handler test. Integration test.
- `POST /api/stores/:sid/groups` calls `ops::kv_logical::add_group`
  → verify fan-out + rollback via handler test. Integration test.
- `GET /api/stores/:sid/groups/:gid/kv/get` calls `ops::kv_data::get`
  → verify leader resolution + get via handler test. Integration test.
- `POST /api/cluster/reset` calls `ops::cluster::reset` → verify
  full teardown via E2E test. E2E test.
- `POST /api/cluster/clean` calls `ops::cluster::clean` → verify
  orphan removal via E2E test. E2E test.
- `AppState` builds `OpContext` sharing the cached `CrowdbKvClient`
  → verify no duplicate connection pool via unit test. Unit test.

**UI layout:**

- Header domain toggle switches between Cluster / KV / Chunk → verify
  via Playwright E2E that all three domains render. E2E test.
- Cluster domain: sidebar shows rack → node → disk-group → disk tree;
  center panel shows hierarchy chart with disk-group boxes containing
  disk elements with short UUIDs → verify via Playwright E2E. E2E test.
- KV domain: sidebar shows rack → node (no disk-groups/disks);
  right-click node shows KV-server context menu; center panel is
  split top (logical topology) / bottom (KV operator) with resize
  handle → verify via Playwright E2E. E2E test.
- Chunk domain: sidebar shows rack → node → chunkdb/diskdb/diskio
  servers; center panel has Capacity / Chunk button toggle; Capacity
  sub-view shows disk-group usage canvas → verify via Playwright E2E.
  E2E test.
- Inspector panel shows Details + Activity for the selected item
  across all three domains → verify via Playwright E2E. E2E test.
- All existing CRUD dialogs (AddRack, AddNode, DeployServer, AddStore,
  AddGroup, AddReplica, etc.) work under the new layout → verify via
  Playwright E2E. E2E test.
- Embedding contract (`apiPrefix`, `basePath`, `readonly`, `modules`,
  `initialDomain`) works under the new layout → verify via Playwright
  E2E with embedded mode. E2E test.

**Test commands:**

- `pixi run -- cargo test -p crowdb-web`
- `pixi run -- cargo test -p crowdb-console-shared`
- `pixi run -- cargo clippy -p crowdb-web -- -D warnings`
- `pixi run -- cargo fmt --all -- --check`
- Playwright E2E: `cd app/crowdb-web/ui && npx playwright test`
  (uses system browser, no `npx playwright install`)

**Open Questions**

1. **Chunk sub-view stub** — the Chunk domain's "Chunk" sub-view (the
   second button after "Capacity") is a stub initially because
   `ops::chunk` is not yet implemented. Should the stub show a
   placeholder ("chunk management coming soon") or be hidden entirely
   until `ops::chunk` is ready? The Capacity sub-view is fully
   functional regardless. Needs human input.

2. **Migration strategy** — should the backend migration and UI layout
   rework be done in one R-item or split into two? The backend
   migration is clear; the UI layout is now defined but needs
   implementation. Alternatives: do backend first (thin wrappers
   around `ops`, keep current UI), then UI rework in a follow-up; or
   do both together. Needs human input on preferred sequencing.
