<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Console Web UI Review

Scope: `app/crow-web/ui/` (React + TS) and its Rust-side HTTP handlers in
`app/crow-web/src/`. This doc captures the code-level review findings
(issues open, coverage gaps).

## Fixed

- **`as any` casts in entity traversal** (`shell/Inspector.tsx`,
  `topology/buildFlow.ts`, `data/usePhysicalTree.ts`,
  `data/useLogicalTree.ts`, `shell/Sidebar.tsx`): the casts existed
  because `StoreView.groups` was typed `GroupSummary[]` but
  `useLogicalTree` enriches each group to a full `GroupView` at runtime
  (it fetches every group via `getGroup` and replaces the summary), and
  because the recursive racks response carries `has_server` on node
  entries that the frontend `Node` type lacked. Added
  `EnrichedStoreView` (`StoreView` with `groups: GroupView[]`) and
  switched every consumer of `useLogicalTree.stores` to it
  (`Inspector`, `Sidebar`, `buildFlow`, `TopologyCanvas`,
  `KvOperatorPanel`, `AddGroupDialog`); added `has_server?` to `Node`
  (mirrors `crow_web::physical_view::NodeView`); fixed `GroupSummary`
  to match the backend struct (removed the phantom `health` field —
  `crow_console_shared::cluster::GroupSummary` is group_id /
  replica_count / leader only; the `useLogicalTree` fallback now uses
  `GroupHealth.Unknown` instead of the always-undefined `g.health`).
  All five cast sites are now typed. `tsc -b` and the e2e `tsc
  --noEmit` both pass.

## Open issues

### Dead code

- **`ConsoleClient::set_disk_group_status`** (`lib/crow-console-shared/src/diskdb.rs:338`):
  no callers — no CLI verb, no test. Held off per user note (diskdb /
  capacity view still in progress); revisit when that work lands.

### Consensus redirect not handled in UI layer

- **`leader_hint` used for display only** (`shell/Sidebar.tsx:59`,
  `topology/buildFlow.ts:39,142`): the UI reads `leader_hint` to render
  degraded state but does not retry a failed write against the hinted
  leader. The Rust `crow-kv-client` has `NotLeaderHint` retry; the web
  UI's `api.ts` does not. For direct KV ops through the console this
  means a write to a non-leader surfaces as an error toast instead of
  transparently redirecting. Whether this matters depends on whether
  the console's KV ops go through the aggregating console HTTP API
  (which hides leader location) or directly to a node — verify the path
  before implementing.

### Hardcoded defaults

- **Default ports `19910` / `19920` / `29920`** appear in
  `App.tsx:654,659-660,672-673`, `AddNodeDialog.tsx:34-36,51`,
  `DeployServerDialog.tsx:28-29`, `DeployDiskdbDialog.tsx:26,37`,
  `components/dialogs/defaults.ts`. Acceptable as dev defaults but not
  configurable for production. Low priority — extract when a deployment
  config story lands.

### File size

- `app/crow-web/ui/src/App.tsx` is 1018 lines (crossed the 1000-line
  "must split" threshold in this branch). `app/crow-web/src/lifecycle.rs`
  is 2114 (pre-existing). Both are split candidates.

## E2E coverage gaps

The 14-file E2E suite is clean: no `test.skip` / `test.fixme`, no
TODO/FIXME in test files. Two `waitForTimeout(500)` remain in
`41-canvas-fit-pan.spec.ts:113,159` (canvas animation settle). Coverage
by feature:

- **Fully covered**: shell embedding/swagger (00), shell UI behaviors
  (01), physical rack/node CRUD (10), server lifecycle (11), node
  inspect cross-jump (12), store/group/replica CRUD (20), reconfig /
  leader failover (21), multi-topology (22), KV basic (30), KV advanced
  (31), inspector activity (40), canvas fit/pan (41), capacity diskdb
  context-menu flows (50), full chain (90).
- **No direct coverage**:
  - `CapacityPanel.tsx` — exercised only via context menus in 50; the
    panel's own layout / empty-state / error-state rendering is
    untested.
  - `MetricsRegion.tsx` — metrics display has no E2E.
  - `AddDiskGroupDialog` / `AddDiskDialog` / `ZoneSelectDialog` /
    `ConfirmDeleteDialog` — reached via context menus but not tested as
    standalone dialog flows (cancel, validation error states).

## Pre-existing test failure (not introduced here)

- `tracked_config_file_loads_and_validates`
  (`app/crow-diskdb/tests/ddb_config_test.rs:88`) fails under bare
  `cargo test` because `conf/crow_diskdb_config.toml` is not at
  `CARGO_MANIFEST_DIR` in the bare-cargo layout. Passes under the pixi
  task. Unrelated to this review's changes (verified by stashing and
  re-running).

## E2E flow review — remaining findings

### Test 22 has an API-only third sub-test

`22-kv-cluster-topology.spec.ts` has three tests. The first two use
`page` (lines 113-206, 208-283) — they open the UI and verify tree
rendering and KV panel scan isolation. The third
(`two groups on overlapping 3-node subsets operate independently`,
line 285) takes only `{ baseURL }` — no `page`, entirely API-only. It
creates nodes/stores/groups via fixtures and verifies via `fetch`, never
opening the UI.

### Capacity test mocks API responses

`50-capacity-diskdb.spec.ts` uses `page.route()` to mock 5 endpoints:
disk-group status PUT (line 319), disk status PUT (line 330), diskdb
recalc (line 341), diskdb scan (line 352), and diskdb usage (line 512).
The compact/rebuild/scan/refresh flow is tested against these mocks,
not the real backend. This means:

- Real backend timing issues (e.g. disk state not updated synchronously
  after compact) are hidden.
- The test verifies the UI handles the mocked response correctly, not
  that the backend actually performs the operation.

This is acceptable for UI-interaction testing; the mock-based sections
are marked with comments in the test file. A companion test against the
real backend (even a smoke-level one) would close the gap.

### Flows that are verified correct

The following flows were traced end-to-end and confirmed to work
correctly — UI component → api.ts → backend handler all match:

- **KV put/get/scan/delete** (`30`, `31`): `KvOperatorPanel` sends
  correct store_id/group_id in all requests; `kv-get-result`,
  `kv-not-found`, `kv-scan-table`, `rev: N` testids all match real
  component rendering; delete confirm dialog is real.
- **All-groups mode** (`31`): `KvOperatorPanel` aggregates scan across
  groups via separate `kvScan` calls per group; Group column appears.
- **Demo inject/delete** (`31`): uses real `/kv/put` and `/kv/delete`
  endpoints, not test mocks.
- **Rack/node CRUD** (`10`): `AddRackDialog` / `AddNodeDialog` labels
  match; context-menu items exist in `App.tsx`; POST field names match
  backend handlers.
- **Deploy server** (`11`): `DeployServerDialog` sends `rest_port` /
  `rpc_port`; backend `http_deploy_node_server` accepts and returns
  `DeployResult` with PID.
- **Server restart/stop** (`11`): context-menu items exist on the
  server tree item (not the node); hit correct endpoints.
- **Cross-jump** (`12`): `Inspector.buildCrossJump` switches view mode
  and selects target entity; selection persists.
- **Node inspect replicas** (`12`): `Sidebar` renders LR-/RR- labels
  from `listNodeStores` data; remote replica health from `reachable`.
- **Store/group/replica CRUD** (`20`): dialog fields match; POST shapes
  match backend; tree renders new entities.
- **Reconfig / leader failover** (`21`): stop/restart via context menus,
  add replica via dialog, tree health badges, KV panel ops — all drive
  through the UI; replica catch-up verified by polling state = running
  + reading pre-existing keys via the UI.
- **Topology tree rendering** (`22`): multi-rack/multi-store hierarchy
  renders correctly in the sidebar.
- **Store isolation** (`22`): KV panel scan correctly shows only the
  selected store's keys.
- **Capacity dialogs** (`50`): `DeployDiskdbDialog`, `AddDiskGroupDialog`,
  `AddDiskDialog`, `ZoneSelectDialog` all have matching fields and POST
  shapes; zone validation works.
- **Activity log** (`40`): `runMutation` wrapper logs success/failure
  for all mutations; in-memory only (no persistence — design
  limitation, not a bug).
- **Canvas fit/pan** (`41`): `TopologyCanvas` implements real
  `fitView` with `requestAnimationFrame` retry; assertions check actual
  viewport transform, not just button existence.
- **Swagger iframe** (`00`): `SwaggerPanel` renders iframe with correct
  `src`; `key={nodeId}` forces remount on selection change.
- **Shell behaviors** (`01`): dialog defaults, cancel, chevron vs text
  click, filter, refresh, health pill — all match component behavior.

## Recommendation order

1. Add a direct `CapacityPanel` E2E (empty/error/loading states).
2. Decide on `NotLeaderHint` retry in `api.ts` after confirming the KV
   ops request path.
3. Rewrite test 22's third sub-test to drive through the UI, or rename
   to indicate it's API-only.
4. Add a real-backend smoke test for capacity compact/rebuild (companion
   to the mock-based test in 50).
5. Split `App.tsx` and `app/crow-web/src/lifecycle.rs`.
