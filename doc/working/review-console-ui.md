<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Console Web UI Review

Scope: `app/crow-web/ui/` (React + TS) and its Rust-side HTTP handlers in
`app/crow-web/src/`. Companion to `plan-console-ui-e2e-refactor.md` —
that plan covers the E2E suite reorganization; this doc captures the
code-level review findings (bugs fixed, issues open, coverage gaps).

## Fixed in this pass

- **Overflow panic — `rpc_port + 1`** (`lib/crow-console-shared/src/lifecycle.rs`):
  `let http_port = rpc_port + 1` panicked in debug builds when
  `rpc_port == 65535`. Now `saturating_add(1)`.
- **Overflow panic — rebuild accumulators** (`app/crow-web/src/diskdb.rs`):
  the multi-zone rebuild loop accumulated `total_rebuilt` / `total_busy`
  / `total_free` with plain `+=`, panicking in debug builds on overflow.
  Now `saturating_add` per field.
- **Generated TOML duplication** (`lib/crow-console-shared/src/lifecycle.rs`):
  `resolve_diskdb_config_path` hand-wrote a 39-line TOML config
  duplicating `DdbConfig::default()`. Any new required field on a
  `DdbConfig` sub-struct would have broken the fallback at deploy time
  with a TOML parse error and no compile-time signal. Added
  `#[serde(default)]` to all `DdbConfig` sub-struct fields
  (`app/crow-diskdb/src/ddb_config.rs`); collapsed the generated config
  to a 4-line `[server]`-only block. Added a regression test
  `minimal_server_only_config_uses_section_defaults` in
  `app/crow-diskdb/tests/ddb_config_test.rs` that round-trips the
  minimal shape through the deserializer + validator.
- **Hardcoded mgmt port** (same block): `kv_server_mgmt_seeds` hardcoded
  `9910`; now uses `crow_protocol::KV_SERVER_MGMT_BASE` (consistent with
  the `DISKDB_HTTP_BASE` use four lines below).
- **Silent parse failures** (`http_listen_addr_from_config`): read/parse
  failures were swallowed into `None` with no log. Now emits `warn!`
  with the path and error — `tracing` is already a dep.
- **Brittle `waitForTimeout` in E2E** (`31-kv-ops-advanced.spec.ts:103`):
  replaced a fixed 500ms sleep with `waitForResponse` for the
  component's auto-rescan, eliminating the flaky wait while preserving
  the race-avoidance semantics.

## Open issues

### Dead code

- **`KvPanel` is unreachable** (`app/crow-web/ui/src/panels/KvPanel.tsx`):
  exported but imported by nothing — `App.tsx` only mounts
  `KvOperatorPanel`. Either wire it in (single-group simple view) or
  delete it. The subagent flagged "no E2E coverage"; the real status is
  "not shipped."
- **`ConsoleClient::set_disk_group_status`** (`lib/crow-console-shared/src/diskdb.rs:338`):
  no callers — no CLI verb, no test. Held off per user note (diskdb /
  capacity view still in progress); revisit when that work lands.

### Silent error swallowing

- **`listServers` failure** (`app/crow-web/ui/src/App.tsx:158`):
  `catch { setAllServers([]); }` silently empties the server list with
  no toast. A user seeing an empty Capacity server list gets no signal
  that the backend is unreachable vs. genuinely empty. Surface a toast
  on failure (the `ToastContext` is already wired everywhere else).

### Type safety

- **`as any` casts in entity traversal**: `shell/Inspector.tsx:151-169`,
  `topology/buildFlow.ts:238-247`, `data/usePhysicalTree.ts:100`,
  `data/useLogicalTree.ts:115`, `shell/Sidebar.tsx:241-243` all cast
  group/replica/rack structures to `any` to read fields. The
  `types/index.ts` definitions exist but don't match the runtime shape
  these components navigate. Risk: a backend field rename silently
  breaks rendering with no type error. Tighten the `GroupView` /
  `StoreView` / `RackView` types to match the actual API responses and
  drop the casts.

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

- `app/crow-web/ui/src/App.tsx` is 1017 lines (crossed the 1000-line
  "must split" threshold in this branch). `app/crow-web/src/lifecycle.rs`
  is 2094 (pre-existing). Both are split candidates.

## E2E coverage gaps

The 14-file E2E suite is clean: no `test.skip` / `test.fixme`, no
TODO/FIXME in test files, only one `waitForTimeout(500)` in
`31-kv-ops-advanced.spec.ts:103` (auto-scan after delete). Coverage by
feature:

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
- **Dead (not shipped, so no coverage needed)**: `KvPanel.tsx`.

## Pre-existing test failure (not introduced here)

- `tracked_config_file_loads_and_validates`
  (`app/crow-diskdb/tests/ddb_config_test.rs:88`) fails under bare
  `cargo test` because `conf/crow_diskdb_config.toml` is not at
  `CARGO_MANIFEST_DIR` in the bare-cargo layout. Passes under the pixi
  task. Unrelated to this review's changes (verified by stashing and
  re-running).

## Recommendation order

1. Fix `App.tsx:158` silent `listServers` catch (one-line toast).
2. Delete or wire in `KvPanel.tsx`.
3. Tighten `as any` casts in Inspector / buildFlow / tree hooks against
   the real API types.
4. Add a direct `CapacityPanel` E2E (empty/error/loading states).
5. Decide on `NotLeaderHint` retry in `api.ts` after confirming the KV
   ops request path.
6. Split `App.tsx` and `app/crow-web/src/lifecycle.rs`.

## E2E flow review — does the tested flow really work?

Each E2E spec was traced through the full stack: UI interaction →
component handler → `api.ts` fetch → backend route. Findings below are
grouped by severity. All file:line references verified against the
current source.

### Backend gap — node delete doesn't cascade-stop the server

`http_remove_node` (`app/crow-web/src/lifecycle.rs:232-255`) removes
the node from config and cascades group-0 sysdata, but **does not stop
the server process**. The UI compensates by calling `removeServer`
before `removeNode` (`App.tsx:336-338`), so the UI flow works — but a
direct `DELETE /api/nodes/:id` API call orphans the running process.

The cascade-delete test
(`11-physical-server-lifecycle.spec.ts:182-217`) is named "deleting a
node cascades service shutdown" but only verifies the node disappears
from the tree (line 207) and returns 404 via API (line 213). It does
**not** verify the server was actually stopped — no check that
`/api/nodes/493/server` returns 404 or that the PID is gone. The test
passes even if the server process is still running.

### Test 21 is API-only — not a UI flow test

`21-kv-cluster-reconfig.spec.ts` has **zero `page` references** — every
operation uses direct `fetch()` calls and API polling. The test
describes leader election, quorum preservation, and replica catch-up,
but none of it goes through the UI. Specifically:

- Leader election on stop: stops via `fetch` (line 67-76), polls
  `/api/stores/:id/groups/:id` for a new leader (lines 133-137). The UI
  is never opened.
- Quorum preservation: verifies by calling `kvPut`/`kvGet` via direct
  `fetch` (lines 78-82). "No error" is the only health signal — no
  explicit group health status check.
- Replica catch-up: adds a replica via API (lines 254-259), then polls
  replica **count** (lines 262-266). Does not verify the new replica
  actually caught up on data — only that it appeared in the group
  status. A stuck replica that never catches up would pass this test.

This file should either be renamed to indicate it's an API integration
test, or rewritten to drive the same flows through the UI (context-menu
stop, tree health updates, AddReplicaDialog).

### Test 22 has an API-only third sub-test

`22-kv-cluster-topology.spec.ts` has three tests. The first two use
`page` (lines 113-206, 208-283) — they open the UI and verify tree
rendering and KV panel scan isolation. The third
(`two groups on overlapping 3-node subsets operate independently`,
line 285) takes only `{ baseURL }` — no `page`, entirely API-only. It
creates nodes/stores/groups via fixtures and verifies via `fetch`, never
opening the UI.

### Brittle wait in KV advanced test — fixed

`31-kv-ops-advanced.spec.ts:103` used `page.waitForTimeout(500)` after
a prefix delete, waiting for the component's 100ms auto-rescan
(`KvOperatorPanel.tsx:322`: `setTimeout(() => handleScan(), 100)`).
The auto-rescan is unconditional (not gated by the autoScan toggle), so
the sleep served a real purpose — ensuring the auto-rescan's
`/kv/scan` response completed before the manual `scanAndRefresh` at
line 106, avoiding a race where two scan responses overwrite each
other. But a fixed sleep is brittle. Replaced with
`page.waitForResponse((r) => r.url().includes('/kv/scan'))` — a
precise wait for the auto-rescan response, no fixed timeout.

### Ping has no success assertion

`11-physical-server-lifecycle.spec.ts:146-148` clicks the Ping menu
item and immediately moves on to restart/stop. There is no assertion
that the ping succeeded, no toast check, no activity-log check. If ping
silently failed, the test would still pass. The activity-log test
(`40-inspector-activity.spec.ts`) does verify ping appears in the log,
but that's a separate test — the lifecycle test itself doesn't verify
ping works.

### Health pill not verified after stop

`11-physical-server-lifecycle.spec.ts:160-173` stops the server and
verifies via API that `serverState` is not `'running'` (line 172), but
does not verify the UI health pill updated. The UI relies on
`usePhysicalTree` polling (5s interval) to update `nodeHealthById`. The
test doesn't wait for or assert on the health pill in the tree, so a
stale-health-pill bug would go undetected.

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

This is acceptable for UI-interaction testing but should be documented
as mock-based. A companion test against the real backend (even a
smoke-level one) would close the gap.

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

### E2E recommendation order

1. **Fix cascade-delete test**: add `expect((await api.get('/api/nodes/493/server')).status()).toBe(404)` after the cascade delete to verify the server was actually stopped.
2. **Decide on Test 21**: either rename to `21-kv-cluster-reconfig-api.spec.ts` to indicate API-only, or rewrite to drive flows through the UI.
3. **Remove `waitForTimeout(500)`** in `31-kv-ops-advanced.spec.ts:103` — done; replaced with `waitForResponse` for the auto-rescan.
4. **Add ping success assertion** in `11-physical-server-lifecycle.spec.ts` — check the activity log or toast after ping.
5. **Add health-pill assertion** after stop in `11-physical-server-lifecycle.spec.ts` — verify the tree health badge updates.
6. **Document mock-based capacity tests** — add a comment in `50-capacity-diskdb.spec.ts` noting which sections use `page.route` mocks.
7. **Strengthen replica catch-up** in Test 21 — verify data on the new replica, not just count.
