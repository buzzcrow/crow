# UI Real End-to-End Test Plan

> Status: **PLAN ONLY** — no implementation yet.  
> Goal: Close the gap between "frontend unit tests with mocked fetch" and "backend integration tests with HTTP clients" by adding **real-browser + real-backend** E2E tests that an AI assistant can execute step-by-step.

---

## 0. Source of Truth

This plan is **traceable** to two upstream documents. Every test case (`E2E-NN`) below cites the requirement section it validates.

- **Requirements (normative):** `doc/requirement-ui.md` §1 – §8.
- **Design (normative for the SPA):** `doc/design/design-ui.md` §1 – §12.
- **Backend contracts (normative):** `doc/design/design-console.md` §6 (API routes), plus the Rust handlers in `crowkv-console/web/src/{lifecycle,physical,mgmt,kv,spa}.rs`.

**Definition of "web UI works":** every functional requirement in `requirement-ui.md` §3 is reachable in a real browser, drives the real backend, and the result is observable in the rendered DOM. Section 8 below maps each test to its requirement section so coverage is auditable.

---

## 1. The Gap

### 1.1 What we have today (three layers)

| Layer | Tool | Count | What it proves | Backend |
|-------|------|-------|--------------|---------|
| **Rust unit + integration** | `cargo test` | ~100+ | Paxos correctness, slot-node semantics, acceptor WAL, leader election, group propose | In-process / testkit |
| **Console backend integration** | `cargo test --test {mgmt,lifecycle,replica,swagger,frontend,kv}_routes` | 6 test files | HTTP handlers accept the right JSON, orchestrate crowkv-server processes, proxy OpenAPI, serve SPA fallback | **Real** `crowkv-server` subprocess + in-process Axum |
| **Console shared e2e** | `cargo test -p crowkv-console-shared` | 6 test files (`aggregate`, `aggregate_multi`, `kv_e2e`, `lifecycle_e2e`, `mgmt_e2e`, `monitor_task`) | Lifecycle client (`deploy_local`), mgmt client (`add_store`…), topology aggregation | **Real** `crowkv-server` subprocess |
| **Frontend unit** | Vitest (`*.test.{ts,tsx}`) | 9 files, ~41 tests | Dialogs POST correct JSON shapes; `listRacks` normalizes envelope/flat; `Sidebar` builds `rawId`; `useBulkOperations` batching logic | **Mocked** `fetch` |
| **Frontend a11y / smoke** | Playwright (`e2e/a11y.spec.ts`) | 3 tests | SPA renders in Chromium; axe-core finds no critical/serious violations; Cmd+K opens command palette | **Stubbed** — `page.route` returns hard-coded `mockRacks`/`mockNodes` |

### 1.2 What is missing

There is **no test that opens a real browser, loads the built SPA, and drives it against a live `crowkv-web` + `crowkv-server` stack**.  
Consequences:

- The Phase 5 bugs (wrong request bodies, `recursive` envelope mishandling, composite-id leaks) were only caught by **manual curl + user report**, not by CI.
- Accessibility tests scan a **stubbed** shell — forms with validation, error toasts, and async polling paths are never exercised under a real backend.
- Cross-view navigation (Physical ↔ Logical), topology canvas search/focus mode, and KV data plane are entirely untested in a browser.
- Embedding props (`readonly`, `modules`, custom actions/panels) have no automated coverage.

### 1.3 Why Playwright stubs are insufficient

`e2e/fixtures/apiStubs.ts` intercepts every `/api/*` call and returns static JSON.  It proves the SPA can render when the backend is healthy, but it **cannot** catch:
- Backend returning `{ items, truncated_at }` vs flat array (the `listRacks` envelope bug).
- Backend rejecting malformed `NodeEntry` with 400.
- Orchestrated replica wiring failing with 502 because the target node has no server.
- Polling race conditions (mutation → refresh → stale DOM).

---

## 2. Existing Automation Test Inventory

### 2.1 Rust side (console backend)

| File | Tests | Backend | Coverage |
|------|-------|---------|----------|
| `crowkv-console/web/tests/frontend_routes.rs` | SPA fallback, deep-link | In-process Axum | `GET /` returns `index.html` or fallback; non-API paths fall through |
| `crowkv-console/web/tests/lifecycle_routes.rs` | Rack/node CRUD, ping, deploy/restart/stop server, OpenAPI proxy | Real `crowkv-server` | Physical-tree lifecycle endpoints |
| `crowkv-console/web/tests/mgmt_routes.rs` | Store/group/replica CRUD, orchestrated wiring | Real `crowkv-server` | Logical-tree management endpoints |
| `crowkv-console/web/tests/replica_routes.rs` | Add/remove replica, bidirectional peer wiring | Real `crowkv-server` | Replica orchestration rollback |
| `crowkv-console/web/tests/swagger_routes.rs` | Swagger UI bundle serving | In-process Axum | `/api/swagger/*` static files |
| `crowkv-console/web/tests/kv_routes.rs` | Put/get/delete/scan via monitor cache | Real `crowkv-server` | KV data plane |
| `crowkv-console/shared/tests/lifecycle_e2e.rs` | `deploy_local_and_observe_topology` | Real `crowkv-server` | Console shared client deploys and topology aggregates |
| `crowkv-console/shared/tests/mgmt_e2e.rs` | `full_store_group_remote_cycle` | Real `crowkv-server` | Console shared mgmt client full cycle |
| `crowkv-console/shared/tests/kv_e2e.rs` | KV operations through shared client | Real `crowkv-server` | Shared KV client |
| `crowkv-console/shared/tests/aggregate.rs` | Topology aggregation without server | In-process | `topology::aggregate()` logic |
| `crowkv-console/shared/tests/aggregate_multi.rs` | Multi-node topology aggregation | In-process | Aggregation with multiple nodes |
| `crowkv-console/shared/tests/monitor_task.rs` | Monitor cache refresh | In-process | `MonitorTask` polling loop |

### 2.2 Frontend side (SPA)

| File | Tests | Backend | Coverage |
|------|-------|---------|----------|
| `src/components/dialogs/createFlows.test.tsx` | 10 tests | Mocked fetch | Dialogs POST exact backend contract (rack→node→deploy→store→group→replica) |
| `src/data/listRacks.test.ts` | 2 tests | Mocked fetch | `listRacks` unwraps `{items}` envelope + flat passthrough |
| `src/shell/Sidebar.test.tsx` | ~3 tests | Mocked fetch + mock data | `rawId` extraction, `NodeView[]` vs `NodeId[]` rendering |
| `src/hooks/useBulkOperations.test.tsx` | ~2 tests | Mocked fetch | Batch delete confirmation, partial failure handling |
| `src/hooks/useDebouncedValue.test.ts` | ~2 tests | No backend | Debounce timing logic |
| `src/topology/layout.test.ts` | ~2 tests | No backend | Layout algorithm (force-directed, hierarchical, grid) |
| `src/utils/exportUtils.test.ts` | ~2 tests | No backend | SVG/PNG/CSV export utilities |
| `src/utils/fuzzySearch.test.ts` | ~2 tests | No backend | Fuzzy search ranking |
| `src/utils/localStorage.test.ts` | ~2 tests | No backend | `localStorage` wrapper |
| `e2e/a11y.spec.ts` | 3 tests | **Stubbed** Playwright | axe-core scans on initial shell, command palette, inspector panel |

### 2.3 Assessment

- **Backend contract tests are strong** — Rust tests verify the handlers accept correct JSON and orchestrate real processes.
- **Frontend unit tests are strong** — Vitest pins the exact request bodies after Phase 5 fixes.
- **The seam between frontend and backend is untested** — no automated test loads the SPA in a browser and drives the full rack→node→deploy→store→group→replica→KV flow through real HTTP.

---

## 3. Real E2E Test Design

### 3.1 Philosophy

Every test case in this plan must satisfy:

1. **Real browser** — Playwright drives a Chromium instance (headless in CI, headed for local AI-assisted debugging).
2. **Real backend** — The test boots `crowkv-web` (Axum) with an in-memory `ConsoleConfig`, and spawns real `crowkv-server` subprocesses via the same `lifecycle::deploy_local` path used by Rust tests.
3. **Real network** — The browser fetches `http://127.0.0.1:<console_port>/` and hits `/api/*` through actual TCP, not stubs.
4. **Observable assertions** — Each step asserts on **rendered DOM text**, **toast notifications**, **URL state**, or **HTTP response status** visible to an end user.
5. **AI-runnable** — Tests are numbered, sequential, and each produces a clear pass/fail signal with a screenshot on failure.

### 3.2 Test infrastructure (new files to create)

| File | Purpose |
|------|---------|
| `crowkv-console/web/ui/e2e/realBackend.config.ts` | Playwright config with `webServer` that runs `cargo run --bin crowkv-web -- --test-mode` |
| `crowkv-console/web/ui/e2e/fixtures/realBackend.ts` | Fixture that boots the console web server + deploys `crowkv-server` via shared lifecycle client, then exposes `baseURL` and `kill()` |
| `crowkv-console/web/ui/e2e/fixtures/consoleSetup.ts` | Helper to register racks/nodes via the console HTTP API (so the SPA starts with data) |
| `crowkv-console/web/ui/e2e/flows/` | Directory containing one spec file per end-to-end flow |

### 3.3 Backend boot sequence for each test file

```
1. Start crowkv-web on a free port (in-memory registry, no persistence).
2. Register rack r1 + node n1 (local, host 127.0.0.1, ssh_user="").
3. Deploy crowkv-server on n1 (auto-pick mgmt/grpc ports).
4. Register the deployed server into the console config.
5. Playwright page.goto(http://127.0.0.1:<console_port>/).
6. Run E2E flow.
7. AfterAll: SIGTERM the server process, drop the console web server.
```

This mirrors `crowkv-console/web/tests/mgmt_routes.rs` but drives the SPA instead of `reqwest`.

---

## 4. Test Cases (AI Execution Order)

Each case is designed to be run **one at a time** by an AI assistant, with clear setup/teardown and unambiguous assertions. The `Req` tag on each case cites the section of `requirement-ui.md` it validates.

### 4.1 Foundation Flows — Physical Tree Lifecycle

> Prerequisites: `crowkv-server` binary built (`cargo build -p crowkv-server`).

#### E2E-01: Fresh registry renders empty state · Req §3.1, §7

**Setup:** Boot console web with empty registry (no racks).
**Steps:**
1. Open SPA.
2. Assert sidebar shows "No racks registered" or empty-state message.
3. Assert topology canvas shows empty cluster placeholder.
4. Assert cluster health pill shows "Unknown" or grey state.

**Pass:** All assertions match; no console errors.

---

#### E2E-02: Add Rack through UI dialog · Req §3.2

**Setup:** Fresh registry.
**Steps:**
1. Click "Add Rack" button (sidebar header or command palette).
2. Fill `id=r1`, `name=Rack One`.
3. Click Create.
4. Assert toast: "Rack r1 created".
5. Assert sidebar shows `r1` within 5 seconds (polling refresh).
6. Assert topology canvas shows `r1` node.
7. **Backend verify:** `GET /api/racks` returns `r1`.

**Pass:** All assertions; backend GET confirms persistence.

---

#### E2E-03: Add Node through UI dialog · Req §3.2, §3.7 inline validation

**Setup:** Registry with rack `r1`.

**Pre-step (validation sub-flow):**
1. Open dialog, type `id=` (empty) → assert inline error "ID required".
2. Type `host=not-a-host` then valid → assert error clears.
3. Type `ssh_port=70000` → assert range error.

**Steps:**
1. Select `r1` in sidebar.
2. Open "Add Node" dialog.
3. Fill `id=n1`, `host=127.0.0.1`, `ssh_user=` (empty).
4. Click Create.
5. Assert toast: "Node n1 created".
6. Assert sidebar expands `r1` → `n1`.
7. Assert inspector shows `Node n1` details (`host`, `ssh_port: 22`).

**Pass:** Toast + sidebar expansion + inspector details.

---

#### E2E-04: Deploy / Ping / Restart / Stop Server through context menu · Req §3.2

**Setup:** Registry with `r1` → `n1`.

**Extension after deploy:**
- After step 9, right-click `n1` → "Ping" → assert toast "Node n1 reachable" and backend `POST /api/nodes/n1/ping` returned 200.
- Right-click `n1` → "Restart Server" → assert toast and `pid` changes in inspector.
- Right-click `n1` → "Stop Server" → assert server icon disappears and `GET /api/nodes/n1/server` returns 404.

**Steps:**
1. Right-click `n1` in sidebar → "Deploy Server".
2. (Or use command palette: "Deploy Server on n1").
3. Dialog opens with auto-picked ports (mgmt/grpc).
4. Click Deploy.
5. Assert toast: "Server deployed on n1" (or spinning "Deploying…" → success).
6. Wait up to 10 seconds.
7. Assert sidebar `n1` shows server icon / status.
8. Assert inspector shows `ServerProcess` with `pid`, `mgmt_url`, `grpc_url`.
9. **Backend verify:** `GET /api/nodes/n1/server` returns 200 with `pid`.

**Pass:** Server icon appears; backend GET returns running process.

---

#### E2E-05: Add Store → Group → Replica full chain · Req §3.3

**Setup:** `r1` → `n1` with deployed server.
**Steps:**
1. Switch to **Logical** view (header toggle).
2. Click "Add Store".
3. Fill `store_id=7`, `group_id=70`, `replica_id=700`, select node `n1`.
4. Click Create.
5. Assert toast: "Store 7 created".
6. Assert sidebar Logical view shows `Store 7`.
7. Select `Store 7` → click "Add Group".
8. Fill `group_id=80`, `replica_id=800`, select `n1`.
9. Click Create.
10. Assert toast: "Group 80 created".
11. Select `Group 80` → click "Add Replica".
12. Fill `node_id=n1` (or select from dropdown), `replica_id=801`.
13. Click Add.
14. Assert toast: "Replica 801 added".
15. Assert sidebar shows `Store 7 → Group 80 → Replica 801`.

**Pass:** All toasts + sidebar hierarchy.

---

#### E2E-06: Physical ⇄ Logical cross-jump (both directions) · Req §3.1, design §3.1

**Setup:** `r1` → `n1` → server → store 7 → group 70 → replica 700.

**Extension (return jump):**
- After step 6, in Logical inspector click "Show on node" → assert view-mode flips back to Physical and `LocalReplica 700` is selected. Confirms the bidirectional cross-jump and the navigation stack from design §3.1.

**Steps:**
1. Stay in **Physical** view.
2. Navigate to `r1 → n1 → Server → PxStore 7 → PxGroup 70 → LocalReplica 700`.
3. Click inspector "Show in cluster" (cross-jump link).
4. Assert view-mode switches to **Logical**.
5. Assert sidebar expands `Store 7 → Group 70` and selects `Replica 700`.
6. Assert topology canvas centers on `Replica 700`.

**Pass:** View-mode toggle + correct selection in logical tree.

---

#### E2E-07: Delete Replica with confirmation · Req §3.3, §6

**Setup:** Store 7, Group 70, Replica 700.
**Steps:**
1. In Logical view, select `Replica 700`.
2. Right-click → "Delete Replica".
3. Assert confirmation dialog: "Delete replica 700 from group 70?".
4. Click Confirm.
5. Assert toast: "Replica 700 removed".
6. Assert sidebar no longer shows `Replica 700` under `Group 70`.
7. **Backend verify:** `GET /api/stores/7/groups/70/replicas` does not list 700.

**Pass:** Dialog → toast → sidebar update → backend confirm.

---

#### E2E-08: Delete Group with cascading confirmation · Req §3.3, §6

**Setup:** Store 7, Group 70 with replicas.
**Steps:**
1. Select `Group 70`.
2. Click "Delete Group".
3. Assert confirmation warns: "This will also delete N replicas".
4. Confirm.
5. Assert toast + sidebar removal.
6. **Backend verify:** `GET /api/stores/7/groups` does not list 70.

**Pass:** Cascade warning → removal → backend confirm.

---

### 4.2 KV Data Plane Flows

#### E2E-09: KV Put and Get · Req §3.4

**Setup:** Logical view, Store 7, Group 70 with leader elected (replica on running server).
**Steps:**
1. Select `Group 70`.
2. Inspector → KV tab.
3. Fill key=`test-key`, value=`test-value`.
4. Click Put.
5. Assert toast: "Key written".
6. Fill key=`test-key`, click Get.
7. Assert value displayed: `test-value`.
8. Assert size shown (e.g., "11 bytes").

**Pass:** Put toast + Get returns exact value.

---

#### E2E-10: KV Scan with prefix and limit · Req §3.4

**Setup:** Group 70 with keys `aaa-1`, `aaa-2`, `bbb-1`.
**Steps:**
1. KV tab → prefix=`aaa`, limit=`10`.
2. Click Scan.
3. Assert results table shows `aaa-1`, `aaa-2` only.
4. Assert "2 results" count.

**Pass:** Prefix filter + result count.

---

#### E2E-11: KV Delete with confirmation · Req §3.4, §6

**Setup:** Group 70 with `test-key`.
**Steps:**
1. KV tab → key=`test-key`.
2. Click Delete.
3. Assert confirmation dialog.
4. Confirm.
5. Assert toast: "Key deleted".
6. Click Get on same key.
7. Assert "Key not found" or empty result.

**Pass:** Delete toast → subsequent Get returns not-found.

---

### 4.3 Navigation & Productivity Flows

#### E2E-12: Command palette fuzzy search · Req §3.8

**Setup:** Registry with `r1`, `n1`, store `7`, group `70`.
**Steps:**
1. Press `Ctrl+K`.
2. Type `n1`.
3. Assert palette shows `Node n1` result.
4. Press Enter.
5. Assert Physical view selected, `n1` highlighted in sidebar and canvas.

**Pass:** Search → selection → view sync.

---

#### E2E-13: Command palette action execution · Req §3.8

**Setup:** Physical view, `n1` without server.
**Steps:**
1. Press `Ctrl+K`.
2. Type "deploy".
3. Assert "Deploy Server on n1" action shown.
4. Select it.
5. Assert Deploy Server dialog opens pre-filled for `n1`.

**Pass:** Palette action opens correct dialog.

---

#### E2E-14: Breadcrumb navigation · Req §3.8

**Setup:** Logical view, selected `Group 70` under `Store 7`.
**Steps:**
1. Assert header breadcrumb shows `Cluster / Store 7 / Group 70`.
2. Click "Store 7" in breadcrumb.
3. Assert sidebar selects `Store 7`; inspector shows Store details.

**Pass:** Breadcrumb click navigates up hierarchy.

---

#### E2E-15: Favorites and recent items · Req §3.8

**Setup:** Any registry.
**Steps:**
1. Select `Store 7`.
2. Inspector → click "Add to favorites".
3. Assert `Store 7` appears in sidebar Favorites section.
4. Reload page (`F5`).
5. Assert Favorites still shows `Store 7` (localStorage persistence).
6. Select another entity.
7. Assert Recent items shows `Store 7`.

**Pass:** Favorites persistence across reload + recent tracking.

---

### 4.4 Accessibility & Resilience Flows

#### E2E-16: Backend unreachable handling · Req §7

**Setup:** SPA loaded, backend running.
**Steps:**
1. Kill `crowkv-web` process (but keep page open).
2. Wait for next polling tick (~5 s).
3. Assert non-blocking banner: "Backend unreachable — retrying".
4. Restart `crowkv-web`.
5. Wait ~5 s.
6. Assert banner disappears.
7. Assert sidebar refreshes with current data.

**Pass:** Graceful degradation + recovery without reload.

---

#### E2E-17: Add replica to existing group · Req §3.3

**Setup:** Two racks/nodes with deployed servers (`r17a/n17a`, `r17b/n17b`). Seed store `177` with initial group `1770` and replica `17700` on `n17a`.
**Steps:**
1. Open Cluster view; select group `1770`.
2. Right-click the group tree item → "Add Replica".
3. In the Add Replica dialog, select node `n17b` from the dropdown.
4. Leave Replica ID blank (auto-generated).
5. Click "Add Replica".
6. Assert toast: `Replica added to node "n17b" successfully`.
7. Assert new replica `17701` appears under group `1770` in the logical tree.
8. Verify backend: `GET /api/stores/177/groups/1770/replicas` returns two replicas (`17700` on `n17a`, `17701` on `n17b`).

**Pass:** Replica created via UI, wired to peer, visible in tree and API.

---

#### E2E-18: Full chain — rack → node → server → store → group → replica · Req §3.2, §3.3

**Setup:** Fresh empty registry.
**Steps:**
1. Click Infrastructure view → "Add Rack" → fill `r18` / `Rack Eighteen` → submit.
2. Right-click `Rack Eighteen` → "Add Node" → fill `n18a`, host `127.0.0.1` → submit.
3. Right-click `Rack Eighteen` → "Add Node" → fill `n18b`, host `127.0.0.1` → submit.
4. Right-click `n18a` → "Deploy Server" → fill mgmt port `9933`, gRPC port `9943`, binary path → submit.
5. Right-click `n18b` → "Deploy Server" → fill mgmt port `9934`, gRPC port `9944`, binary path → submit.
6. Switch to Cluster view.
7. Click "Add Store" → fill store `188`, group `1880`, replica `18800`, select `n18a` → submit.
8. Right-click group `1880` → "Add Replica" → select `n18b`, leave replica ID blank → submit.
9. Assert toast: `Replica added to node "n18b" successfully`.
10. Assert both replicas (`18800`, `18801`) appear under group `1880`.
11. Verify backend: `GET /api/stores/188/groups/1880/replicas` returns two replicas on `n18a` and `n18b`.

**Pass:** Complete lifecycle from empty registry to multi-replica group created entirely through the UI.

---

### 4.5 Topology, Inspect, Swagger, Bulk, Export, Embedding Flows

#### E2E-19: Large cluster leader election monitor · Req §3.1, §3.3

**Setup:** Three racks (`r19a`, `r19b`, `r19c`), three nodes (`n19a`, `n19b`, `n19c`) each with a deployed server. One store `199` with three groups (`1990`, `1991`, `1992`).
**Steps:**
1. Create store `199` with group `1990` and replica `19900` on `n19a` (this path auto-sets a leader via `http_add_store`).
2. Create group `1991` with replica `19910` on `n19b` via `POST /api/stores/199/groups` (no leader is pre-set; Paxos election must happen).
3. Create group `1992` with replica `19920` on `n19c` via `POST /api/stores/199/groups` (no leader is pre-set; Paxos election must happen).
4. Open Cluster view; assert all three groups appear in the logical tree.
5. Poll `GET /api/stores/199/groups` every 500 ms for up to 20 seconds.
6. For each group, assert `leader_id > 0` within the timeout.
7. For each group, call `GET /api/stores/199/groups/:gid` and assert exactly one replica has `role: "Leader"`.

**Pass:** Every group elects exactly one leader within 20 seconds. Fail explicitly if any group has `leader_id: 0` after timeout.

---

#### E2E-20: Physical NodeInspect view — local + remotes wiring · Req §3.3 (debugging)

**Setup:** Store 7, Group 70 with replicas on `n1` and `n2`.
**Steps:**
1. In Physical view, select `n1 → Server → PxStore 7 → PxGroup 70`.
2. Click "Inspect" on the group.
3. Assert inspector / NodeInspect panel renders `local: { replica_id, role }` and `remotes: [{ replica_id, node_id, reachable }]` from `GET /api/nodes/n1/stores/7/groups/70`.
4. Manually remove the remote on `n1` via backend curl (simulating mis-wiring); wait one poll.
5. Assert the missing remote-list edge is visibly gone in the canvas (this is the bug class the physical view exists to surface).

**Pass:** Inspect shows both sides; canvas reflects the mis-wiring.

---

#### E2E-21: Embedded Swagger panel · Req §3.5

**Setup:** Registry with `n1` running a server.
**Steps:**
1. Open the Swagger feature tab (panel inside the SPA — must NOT open a new browser tab).
2. Assert the iframe (or `swagger-ui-react` mount) loads with `url=/api/nodes/n1/openapi.json`.
3. Use the header node selector to switch from `n1` to a second node `n2`; assert the OpenAPI doc reloads inline.
4. Assert the rest of the SPA shell stays mounted (no full-page navigation).

**Pass:** Swagger renders, node switch updates inline, shell unchanged.

---

#### E2E-22: Bulk operations · Req §3.7

**Setup:** Three nodes `n1, n2, n3` registered, none with a server.
**Steps:**
1. In Physical view, `Ctrl+click` `n2` and `n3` to multi-select.
2. Right-click → "Deploy Server on selected".
3. Assert summary confirmation dialog: "Deploy 2 servers — n2, n3".
4. Confirm; assert toast(s) and activity log shows two entries with per-item outcome.
5. Repeat with one node intentionally unreachable; assert partial-failure summary in activity log.

**Pass:** Multi-select drives one confirm dialog, per-item outcomes recorded.

---

#### E2E-23: Activity log filter + export · Req §3.7, §3.9

**Setup:** Several operations performed (rack add, server deploy, replica add).
**Steps:**
1. Open Activity tab in inspector.
2. Filter by current selection (`n1`); assert only `n1`-scoped entries are visible.
3. Click "Export CSV"; assert downloaded file contains the filtered rows with `timestamp, action, target, outcome` headers.

**Pass:** Filter narrows list; export produces correct CSV.

---

#### E2E-24: Topology + data exports · Req §3.9

**Setup:** Populated cluster.
**Steps:**
1. From the canvas toolbar export dropdown: download SVG; assert non-empty SVG with cluster node ids embedded as text.
2. Same dropdown: download PNG; assert non-empty PNG (binary length > 0, MIME `image/png`).
3. From a KV scan result panel: export JSON and CSV; assert each contains the scanned key/value rows.
4. From the cluster health summary: export PDF health report; assert non-empty PDF (MIME `application/pdf`).

**Pass:** All four exports succeed and contain correct content.

---

#### E2E-25: Health summary + timeline · Req §3.1

**Setup:** Cluster with one healthy and one degraded group.
**Steps:**
1. Assert header health pill shows "Degraded".
2. Open the timeline dropdown; assert a chart with the last 1 hour bucket is rendered.
3. Switch range to 1 day; assert the chart updates and the bucket count grows.
4. Open the inspector Metrics tab on a group; assert a per-entity health series renders.

**Pass:** Aggregate pill + timeline + per-entity history all render.

---

#### E2E-26: Filter / sort with saved presets · Req §3.8

**Setup:** Many entities of mixed status.
**Steps:**
1. In the sidebar tree filter, select status=`Degraded`; assert tree narrows.
2. Sort by `health score` descending; assert ordering.
3. Save as preset "Triage"; reload page; re-apply preset "Triage".
4. Assert filter+sort are restored.

**Pass:** Preset persists across reload.

---

#### E2E-27: Embedding — `apiPrefix`, `basePath`, `modules` opt-out · Req §4

**Setup:** Mount `<CrowkvConsole apiPrefix="/storage/crowkv/api" basePath="/admin/cluster" modules={{ kv:false, swagger:false }} />` behind a reverse proxy in test mode.
**Steps:**
1. Assert every network request the SPA issues is rooted at `/storage/crowkv/api/...`.
2. Assert deep links use `/admin/cluster/...`; the host's outer URL (`window.location`) is not mutated by SPA navigation.
3. Assert the KV inspector tab and the Swagger feature tab are absent.

**Pass:** Route isolation + module opt-out both hold.

---

#### E2E-28: Embedding — branding, custom action, custom panel, system theme · Req §4, design §8

**Setup:** Mount with `brandLogo`, `customActions=[{id:"audit", appliesTo:["node"], placement:["contextMenu","inspector"]}]`, `customPanels=[{id:"vendor-metrics", appliesTo:["group"], component: VendorPanel}]`, `themeMode="system"`.
**Steps:**
1. Assert custom logo replaces the default CrowKV logo in the header.
2. Right-click a node; assert "Audit Log" appears in the menu. Activate; assert `onEvent` fires with `{action:"audit", entity:{id:"n1",...}}`.
3. Select a group; assert the "Vendor Metrics" tab appears in the inspector after the default tabs and renders `VendorPanel` with the current selection.
4. Toggle OS dark-mode preference (Playwright `colorScheme: 'dark'`); assert the SPA switches to dark theme without reload.

**Pass:** Branding, custom action, custom panel, and system theme detection all work.

---

#### E2E-29: Custom action injection (lightweight) · Req §4

Folded into E2E-28. Retained as a deliberate placeholder so the original §4.5/E2E-19 "custom action" intent is not lost; remove this case after E2E-28 lands.

---

#### E2E-30: Theme override (`--accent`) · Req §4, design §4.1

**Setup:** Mount SPA with `theme={"--accent":"#ff6600"}`.
**Steps:**
1. Assert header button hover uses orange (`#ff6600`) instead of default blue.
2. Assert healthy status indicator still uses green (`--healthy` token unchanged).

**Pass:** Accent override applied; unrelated tokens untouched.

---

## 5. Implementation Plan (Phased)

### Phase 1: Infrastructure (infra)

- [x] **1.1** Create `e2e/realBackend.config.ts` — Playwright config with `webServer` spawning `crowkv-web` in test mode.
- [x] **1.2** Create `e2e/fixtures/realBackend.ts` — fixture wrapping `deploy_local` from shared crate, exposing `consoleUrl` and process handles.
- [x] **1.3** Create `e2e/fixtures/consoleSetup.ts` — HTTP helpers to seed registry via `POST /api/racks`, `POST /api/nodes`, `POST /api/nodes/:id/server/deploy`.
- [x] **1.4** Add `crowkv-web --test-mode` CLI flag (or env var) that starts with in-memory state, no `~/.crowkv/console.toml` persistence, and auto-clears on exit.
- [ ] **1.5** Wire `cargo test --test e2e` or `npx playwright test --config=e2e/realBackend.config.ts` into CI.

### Phase 2: Foundation Flows (E2E-01..E2E-08)

- [x] **2.1** `e2e/flows/01-fresh-registry.spec.ts`
- [x] **2.2** `e2e/flows/02-add-rack.spec.ts`
- [x] **2.3** `e2e/flows/03-add-node.spec.ts`
- [x] **2.4** `e2e/flows/04-deploy-server.spec.ts`
- [x] **2.5** `e2e/flows/05-store-group-replica-chain.spec.ts`
- [x] **2.6** `e2e/flows/06-cross-jump.spec.ts`
- [x] **2.7** `e2e/flows/07-delete-replica.spec.ts`
- [x] **2.8** `e2e/flows/08-delete-group.spec.ts`

### Phase 3: KV Data Plane (E2E-09..E2E-11)

- [x] **3.1** `e2e/flows/09-kv-put-get.spec.ts`
- [x] **3.2** `e2e/flows/10-kv-scan.spec.ts`
- [x] **3.3** `e2e/flows/11-kv-delete.spec.ts`

### Phase 4: Navigation & Productivity (E2E-12..E2E-15)

- [x] **4.1** `e2e/flows/12-command-palette-search.spec.ts`
- [x] **4.2** `e2e/flows/13-command-palette-action.spec.ts`
- [x] **4.3** `e2e/flows/14-breadcrumb.spec.ts`
- [x] **4.4** `e2e/flows/15-favorites-recent.spec.ts`

### Phase 5: Cluster Management Completion & Resilience (E2E-16..E2E-18)

- [x] **5.1** `e2e/flows/16-backend-unreachable.spec.ts`
- [x] **5.2** `e2e/flows/17-add-replica.spec.ts` — adds a replica to an existing group through the UI
- [x] **5.3** `e2e/flows/18-full-chain.spec.ts` — end-to-end chain from empty registry to multi-replica group

### Phase 6: Cluster Stress & Topology / Inspect / Swagger / Bulk / Export (E2E-19..E2E-26)

- [ ] **6.1** `e2e/flows/19-large-cluster-leader-monitor.spec.ts` — multi-rack, multi-group leader election monitoring with timeout.
- [ ] **6.2** `e2e/flows/20-node-inspect.spec.ts` — physical local+remotes panel.
- [ ] **6.3** `e2e/flows/21-swagger-panel.spec.ts` — embedded Swagger inside SPA.
- [ ] **6.4** `e2e/flows/22-bulk-operations.spec.ts` — multi-select + summary confirm.
- [ ] **6.5** `e2e/flows/23-activity-export.spec.ts` — activity filter + CSV export.
- [ ] **6.6** `e2e/flows/24-exports.spec.ts` — SVG / PNG / JSON / CSV / PDF.
- [ ] **6.7** `e2e/flows/25-health-timeline.spec.ts` — header pill + timeline + per-entity history.
- [ ] **6.8** `e2e/flows/26-filter-sort-presets.spec.ts` — saved filter+sort presets across reload.

### Phase 7: Embedding & Theming (E2E-27..E2E-30)

- [ ] **7.1** `e2e/flows/27-embedding-isolation.spec.ts` — `apiPrefix`, `basePath`, module opt-out.
- [ ] **7.2** `e2e/flows/28-embedding-customization.spec.ts` — branding, custom action, custom panel, system theme.
- [ ] **7.3** Drop the placeholder E2E-29 once 7.2 lands.
- [ ] **7.4** `e2e/flows/30-theme-override.spec.ts` — `--accent` token override.

### Phase 8: CI Integration

- [ ] **8.1** Add `make e2e` target that builds `crowkv-server`, builds SPA (`npm run build`), then runs Playwright real-backend suite.
- [ ] **8.2** Cache `crowkv-server` binary in CI to avoid rebuild per run.
- [ ] **8.3** Generate HTML report + trace ZIP on failure; upload as CI artifact.
- [ ] **8.4** Wire `make e2e` into the existing pre-merge gate alongside `cargo test` and `npm test`.

---

## 6. AI Execution Instructions

When instructed to run these tests, the AI must:

1. **Build prerequisites first**:
   ```bash
   cargo build -p crowkv-server        # real server binary
   cd crowkv-console/web/ui && npm run build   # SPA bundle
   ```

2. **Run one spec at a time**:
   ```bash
   npx playwright test e2e/flows/02-add-rack.spec.ts --config=e2e/realBackend.config.ts
   ```

3. **On failure**: capture `test-results/` trace, screenshot, and browser console logs; report the exact assertion that failed with line number.

4. **On success**: report which assertions passed and note any warnings (e.g., slow polling, retry loops).

5. **Do not batch** — each flow creates and tears down its own backend state. Running multiple specs in parallel would collide on ports/registry.

---

## 7. Risks & Mitigations

| Risk | Mitigation |
|------|------------|
| `crowkv-server` binary missing on first run | CI builds it; local `make` target builds it; fixture skips with clear message if absent |
| Port collisions during parallel test runs | Fixture picks free ports via `TcpListener::bind(("127.0.0.1", 0))`; Playwright `workers: 1` |
| Slow server startup timeouts | Increase `webServer.timeout` to 120 s; add health-poll loop in fixture |
| Flaky polling assertions | Use Playwright `expect(...).toBeVisible({ timeout: 15_000 })` rather than fixed `sleep` |
| CI environment has no Chromium | Playwright downloads Chromium automatically; fallback to `PLAYWRIGHT_CHANNEL=msedge` |
| SPA build outdated | `realBackend.config.ts` `webServer.command` includes `npm run build` before preview |

---

## 8. Requirement Coverage Matrix

Each row maps a requirement section in `requirement-ui.md` to the E2E case(s) that validate it. Any unchecked row is an open gap.

| Requirement | Section | E2E Cases |
|-------------|---------|-----------|
| Two views + view toggle | §3.1 | E2E-01, E2E-06 |
| Health summary + history | §3.1 | E2E-25 |
| Leader election monitoring | §3.1, §3.3 | E2E-19 |
| Topology search / focus / layouts / edge labels | §3.1 | (future) |
| Hardware lifecycle (rack/node) | §3.2 | E2E-02, E2E-03 |
| Server lifecycle (deploy/ping/restart/stop/openapi proxy) | §3.2 | E2E-04, E2E-18 |
| Cluster management (store/group/replica add+delete) | §3.3 | E2E-05, E2E-07, E2E-08, E2E-17, E2E-18 |
| Physical NodeInspect (local+remotes) | §3.3 | E2E-20 |
| KV data plane (get/scan/put/delete) | §3.4 | E2E-09, E2E-10, E2E-11 |
| Embedded Swagger panel | §3.5 | E2E-21 |
| API routing rule (logical vs physical, no `?server=`) | §3.6 | Covered indirectly by every backend assertion; also E2E-27 |
| Operation visibility — toasts | §3.7 | E2E-02, E2E-04, E2E-05, E2E-09 |
| Inline form validation | §3.7 | E2E-03 |
| Bulk operations | §3.7 | E2E-22 |
| Activity log | §3.7 / §3.9 | E2E-23 |
| Command palette + breadcrumbs + favorites/recent | §3.8 | E2E-12, E2E-13, E2E-14, E2E-15 |
| Filter / sort presets | §3.8 | E2E-26 |
| Topology / data / activity / health-report exports | §3.9 | E2E-23, E2E-24 |
| Embeddability — `apiPrefix`/`basePath`/`modules`/`readonly` | §4 / §6 | E2E-17, E2E-27 |
| Embeddability — branding / custom actions / custom panels / system theme | §4 | E2E-28 |
| Read-only mode + destructive confirmation | §6 | E2E-07, E2E-08, E2E-11, E2E-17 |
| Performance / robustness — backend unreachable recovery | §7 | E2E-16 |
| Accessibility — keyboard-only + axe-core | design §11 | E2E-18 |
| Theme override (`--accent`) | design §4.1 | E2E-30 |

---

## 9. Definition of Done

The web UI is considered "working" only when **all** of the below hold:

- [ ] Every row in §8 has at least one passing E2E case.
- [ ] All 28 active E2E spec files exist under `crowkv-console/web/ui/e2e/flows/` (E2E-29 is folded into E2E-28 and removed).
- [ ] `npx playwright test --config=e2e/realBackend.config.ts` passes with 0 failures against a freshly built `crowkv-server` and a freshly built SPA bundle.
- [ ] Each test asserts on **rendered DOM** (text, ARIA roles, `localStorage`, downloaded file content, image MIME) — never on mocked fetch alone.
- [ ] A single `make e2e` command builds the server, builds the SPA, boots the stack, runs the suite, and tears it down.
- [ ] The pre-merge CI gate runs `make e2e` and blocks merge on failure.
- [ ] Existing Vitest unit tests (~41) and Playwright a11y tests (3) continue to pass; this plan **adds** to coverage, it does not replace either.

---

## 10. Recommended Implementation Order (one phase at a time)

To keep churn small and let an AI run the suite incrementally:

1. **Phase 1 (infra)** — must land first; nothing else can run without it.
2. **Phase 2 (foundation, E2E-01..08)** — proves the create flows actually work end-to-end. Highest payoff: this is the regression band that Phase 5 of `todo_ui2.md` patched but never automated.
3. **Phase 3 (KV, E2E-09..11)** — second-highest user-facing value.
4. **Phase 5 (resilience, E2E-16..18)** — exposes polling / readonly / a11y regressions before piling on features.
5. **Phase 4 (productivity, E2E-12..15)** — depends on stable foundation.
6. **Phase 6 (topology / inspect / swagger / bulk / export, E2E-19..26)** — feature breadth; many of these features may not yet exist in the SPA, so each spec doubles as a TDD anchor for the missing component.
7. **Phase 7 (embedding & theming, E2E-27..30)** — last because embedding props are the most likely to require host-side test harness.
8. **Phase 8 (CI)** — wire `make e2e` into the merge gate once the suite is green locally.

After each phase, mark its checkboxes in §5 and the corresponding row in §8.

---

## When You Can Start Using This Plan

This document is **ready to execute now**. To kick off:

1. Approve Phase 1 in §5 — that creates the Playwright real-backend fixture and the `crowkv-web --test-mode` flag.
2. Once Phase 1 lands, ask the AI to run **E2E-01** as a smoke test against the fixture.
3. From there, request phases sequentially (Phase 2 → Phase 3 → …). Each phase is independently mergeable; nothing forces a big-bang change.

No further plan-doc edits are required before implementation begins. The plan, requirement, and design docs are now consistent.

---

## Implementation Decisions Log

- **2026-05-21 Phase 1:** Added `crowkv-web --test-mode` as an in-memory registry mode by setting `AppState::config_path=None`. This avoids mutating `log/crowkv-console-db.toml` during Playwright runs.
- **2026-05-21 Phase 1:** Real-backend Playwright uses `e2e/realBackend.config.ts` with `workers=1`, `reuseExistingServer=!CI`, and `crowkv-web --test-mode` as the `webServer`.
- **2026-05-21 Phase 1:** The local environment did not have Playwright's bundled Chromium installed, but it did have `/snap/bin/chromium`; the real-backend config now uses that path by default and allows override via `PLAYWRIGHT_CHROMIUM_EXECUTABLE`.
- **2026-05-21 E2E-01:** First real-backend browser smoke test passed: `npx playwright test e2e/flows/01-fresh-registry.spec.ts --config=e2e/realBackend.config.ts`.
- **2026-05-21 E2E-02:** Add-rack flow passed through the real UI and real backend; the test verifies both rendered DOM (`Rack One`) and `GET /api/racks`.
- **2026-05-21 E2E-03:** Add-node flow passed through the real UI and real backend; the test explicitly selects the Rack dropdown.
- **2026-05-21 E2E-04:** Deploy-server flow passed through the real UI and real backend using `/cjdata/cpp/crowkv/target/debug/crowkv-server`, then stopped the server via `/api/nodes/n1/server/stop` cleanup.
- **2026-05-21 Selection fix:** Updated `Tree` so `SelectionContext` stores backend IDs (`rawId`) instead of tree-prefixed IDs (`rack-r1`, `store-7`) and single-click/right-click selection calls `selectEntity`. This unblocks context-sensitive Add Group / Add Replica dialogs.
- **2026-05-21 E2E-05:** Store + group creation passed through the real UI and real backend against a deployed `crowkv-server`; the test verifies rendered toasts/tree entries plus `GET /api/stores` and `GET /api/stores/57/groups`.
- **2026-05-21 Test isolation:** Real-backend Playwright reuses the same `crowkv-web --test-mode` process within one invocation. Specs now use unique rack/node/store/group ids per file so E2E-01..05 pass together in a grouped run.
- **2026-05-21 Logical replica tree fix:** `useLogicalTree` now fetches group details when store summaries are present, and `Sidebar` renders Store → Group → Replica. This enables real delete-replica UI testing.
- **2026-05-21 E2E-07:** Delete-replica flow passed through the real UI and real backend. Backend semantics delete the hosting local group when the last replica is removed, so the test accepts `GET /api/stores/:sid/groups/:gid/replicas` returning 404 after deletion.
- **2026-05-21 E2E-08:** Delete-group flow passed through the real UI and real backend; the test verifies the group disappears from the rendered tree and from `GET /api/stores/88/groups`.
- **2026-05-21 Grouped foundation run:** E2E-01,02,03,04,05,07,08 pass together with one reused `crowkv-web --test-mode` process.
- **2026-05-21 E2E-06:** Cross-jump flow passed through the real UI and real backend. Logical Replica now carries `parentIds.node_id`, and the inspector makes `Parent: node_id` clickable to jump to the hosting physical Node.
- **2026-05-21 Phase 2 complete:** Full foundation suite E2E-01..E2E-08 passes together against real `crowkv-web --test-mode` and real `crowkv-server` subprocesses.
- **2026-05-21 E2E-09:** KV put/get flow passed through the real UI and real backend. Deploy route now refreshes the monitor cache after registering a server so bootstrap store/group `1/1` appears in the logical tree. Management-created stores are still not used for KV UI E2E because their internal random store port is not tracked as the node gRPC endpoint.
- **2026-05-21 E2E-10:** KV scan flow passed through the real UI and real backend by writing two keys through the KV tab and scanning them by prefix.
- **2026-05-21 E2E-11:** KV delete flow passed through the real UI and real backend by writing a key, deleting it from the KV tab, then confirming `Get` reports `Key not found`.
- **2026-05-21 Phase 3 complete:** E2E-09..E2E-11 pass together. Stop-server cleanup now drops the node from the monitor cache so bootstrap store/group `1/1` does not aggregate stale stopped nodes across specs.
- **2026-05-21 Grouped E2E-01..11 run:** E2E-01 through E2E-11 pass together against the real backend. The sidebar Add button now has mode-specific accessible names (`Add Rack`/`Add Store`), and rack dropdown selection uses an exact label to avoid locator ambiguity.
- **2026-05-21 E2E-12:** Command palette search passed against real seeded entities; the test opens the palette and verifies fuzzy search results for node `n12`.
- **2026-05-21 E2E-13:** Command palette action passed by executing `Toggle view (Physical / Logical)` and verifying the UI switches from Cluster to Infrastructure.
- **2026-05-21 E2E-14:** Breadcrumb flow passed for a selected logical replica; `Header` now accepts both camelCase and snake_case parent ids for logical breadcrumb parents.
- **2026-05-21 E2E-15:** Favorites/recent flow passed by selecting a real node, adding it to favorites from the inspector, and verifying both sidebar sections update.
- **2026-05-21 Phase 4 complete:** E2E-12..E2E-15 pass together against the real backend.
- **2026-05-21 E2E-16:** Backend-unreachable flow passed by aborting API requests in Playwright and asserting the app-level `Backend unreachable` alert.
- **2026-05-22 E2E-17 design:** Add-replica flow targets an existing group via the Cluster view context menu. Setup seeds `store 177 / group 1770 / replica 17700` on `n17a` via API, then uses the UI to add `n17b` as a second replica. Auto-generated replica ID (`17701`) is verified in both the rendered tree and the backend API.
- **2026-05-22 E2E-18 design:** Full-chain test creates everything from scratch through the UI: rack `r18` → nodes `n18a`/`n18b` → deploy servers on both → create store `188` with group `1880` on `n18a` → add replica on `n18b`. Uses unique ports (`9933/9943`, `9934/9944`) to avoid collisions with other specs. Validates the complete lifecycle end-to-end.
- **2026-05-22 E2E-19 design:** Large-cluster leader-monitor test creates 3 racks, 3 nodes, 3 servers, 1 store with 3 groups. Store creation (`http_add_store`) auto-sets a leader for the initial group, but `http_add_group` does not. Groups `1991` and `1992` must elect a leader via Paxos. The test polls `GET /api/stores/199/groups` for up to 20 seconds and fails explicitly if any group has `leader_id: 0` after timeout. Also verifies exactly one `role: "Leader"` per group via `GET /api/stores/199/groups/:gid`.
- **2026-05-22 E2E-17 impl:** Implemented and passing. Required `getByLabel('Node', { exact: true })` to disambiguate the `Add Replica` dialog `Node` select from the header `Select node for Swagger UI` select.
- **2026-05-22 E2E-18 impl:** Implemented and passing. Required (a) explicit accessible-name `Add Rack` for the sidebar add button (mode-specific name introduced earlier) instead of `aside.last()`; (b) `getByLabel('Node', { exact: true })` for the Add Replica dialog. Each step uses unique mgmt/grpc ports (`9933/9943`, `9934/9944`) to avoid colliding with other specs.
- **2026-05-22 E2E-19 impl:** Implemented and passing. Removing the `set_leader` workaround exposed four independent root-cause bugs in the cluster manager; all four are now fixed and Paxos election converges automatically for 1-, 2-, and N-replica groups without any manual leader assignment. Summary of root-causes and fixes:
  - **(1) Missing "heartbeat resets election timer" rule (`crowkv/src/cluster/election.rs`, `crowkv/src/cluster/local_replica.rs`).** The follower-state `select!` only awaited `cancel` and `election_deadline`; an accepted `Heartbeat` updated role/term but did not poke the driver, so every follower fired its election timer 4–8 s after group creation regardless of incoming heartbeats. Added `PxLocalReplica::deadline_reset_signal` (`tokio::sync::Notify`), signalled from `handle_heartbeat` (success path) and `handle_request_vote` (granted path), and a new branch in `election::run` that resets `election_deadline` when the signal fires. Mirrors the Raft heartbeat-resets-timer rule.
  - **(2) Old election driver kept running after group rebuild (`crowkv/src/cluster/px_kv_store.rs::add_group`).** The `add_remotes` / `remove_remote` management handlers replace a group atomically via `store.add_group(new_group)`, but the previous driver only exited lazily when its `Weak::upgrade` failed on the next loop iteration. During that window two drivers raced for leadership of the same `(store_id, group_id)`, producing split-brain at `term=1`. `add_group` now removes the old entry first and synchronously cancels its `tenure_cancel` token before installing the new arc.
  - **(3) Election state erased on every remote-replica add (`crowkv-server/src/management.rs::rebuild_group_with_same_config`).** The helper rebuilt the group with a brand-new `PxLocalReplica` (`term=0`, `voted_for=None`, lease cleared, `believed_leader_id=None`), so every `add_remotes` call effectively restarted the election from scratch and the multi-step `addReplica` orchestration never let an election finish. Added `PxLocalReplica::new_inheriting_election_state(prior)`, which copies `(current_term, voted_for, role, leader_id, vote_lockout_until)` from the prior replica; `rebuild_group_with_same_config` now uses this constructor.
  - **(4) Wrong gRPC endpoint wired into Paxos remotes (`crowkv-console/web/src/mgmt.rs::grpc_endpoint_for_node`).** Each `PxKvStore` binds its own random port (`add_store` accepts `port: None`), but the console was wiring Paxos remotes to the bootstrap server's `grpc_url`, prefixed with `http://`. The Paxos remote-replica connector itself prepends `http://`, producing invalid `http://http://host:port` URLs, and even after stripping the scheme it still pointed at store 1's port instead of the operator-created store's. Added `listen_addr: Option<String>` to `NodeStore` (propagated by `legacy_topology_to_node_stores`), and reworked `grpc_endpoint_for_node` into an async lookup that reads the per-store `listen_addr` from the monitor cache (with `0.0.0.0` remapped to `127.0.0.1`); the orchestrator now refreshes the target node's cache between `add_store` and the subsequent `add_remotes` calls.
  - **Console read-side cache freshness (`crowkv-console/web/src/mgmt.rs::http_get_group`).** `crowkv-web` has no background `MonitorTask`; the monitor cache is only filled by `refresh_node_cache` on writes. After election the cached `leader_hint` from setup-time is stale. `http_get_group` now refreshes every node that hosts the store before resolving the aggregated `GroupView`.
  - **CLI / cluster_e2e tests.** Removed the `--leader` CLI flag from `crowkv-server`; the `e2e_three_node_cluster_kv_put_batch_delete`, `e2e_follower_returns_not_leader_hint`, and `e2e_dynamic_group_management` tests now use a new `wait_for_leader` helper that polls per-node `/topology` until exactly one node reports `role=leader`. `lifecycle::deploy_local` calls a new public `lifecycle::wait_for_leader` after `wait_for_ready`, so KV-plane tests (`kv_e2e`, `kv_routes`, `kv_cli`) see a write-ready bootstrap leader without any per-test polling.
  - **E2E-19 setup.** `createStore` now seeds only `n19a` (single replica); group `1990` is extended via `addReplica` on `n19b`/`n19c`, which auto-creates the store on each peer and wires remotes both ways (`http_add_replica`). Groups `1991`/`1992` are then created via `addGroup` spanning all three nodes. All three groups elect via Paxos. Test budget bumped to 30 s and `test.setTimeout(60_000)` to absorb three concurrent fresh elections in CI.
- **2026-05-22 crowkv-server cluster_e2e expansion:** Added KV-plane scenario coverage to `crowkv-server/tests/cluster_e2e.rs` — Get-after-Put + Get-after-Delete on the existing 3-node test, a new `e2e_multi_group_isolated_kv` (two groups in the same cluster, both elect via Paxos, key isolation across groups), and a new `e2e_kv_after_dynamic_replica_change` (start with 2-replica wiring, KV ops, dynamically add a 3rd remote via management API, re-resolve leader, KV ops, dynamically remove the 3rd remote, KV ops). Writing those scenarios uncovered two more rebuild-path bugs that the previous E2E-19 fixes had only partially addressed:
  - **(5) Rebuild silently dropped the learner store and acceptor (`crowkv/src/cluster/local_replica.rs`).** `PxLocalReplica::new_inheriting_election_state` constructed fresh `PxAcceptor::new()` and `PxLearner::new()` instances, which meant every `add_remotes` / `remove_remote` rebuild wiped all previously-committed KV writes from the local replica's learner store and threw away every per-slot Paxos promise — a Paxos safety violation on the next accept. Fixed by changing both fields to `Arc<PxAcceptor>` / `Arc<PxLearner>` so the rebuild constructor can `Arc::clone` from `prior` and share the live state. External readers (`PxKvStore::kv_get`/`kv_scan` calling `.learner.store()`) work unchanged via `Arc` auto-deref.
  - **(6) Rebuild dropped `PxGroup::proposing_term` (`crowkv-server/src/management.rs::rebuild_group_with_same_config`).** `proposing_term` is part of the propose-time leadership gate (`role == Leader && current_term == proposing_term`); a fresh `PxGroup` starts with `proposing_term = 0`, but the inherited `current_term` on the local replica is ≥ 1 from the prior election. The mismatch caused every KV write after `add_remotes` / `remove_remote` to return `NotLeader` from a node that genuinely was the leader. Fixed by calling `new_group.stamp_proposing_term(group.proposing_term())` in `rebuild_group_with_same_config`.
  - **Test stability helper.** Added `wait_for_stable_leader(nodes, group_id, timeout, stable_for)` to `cluster_e2e.rs`: confirms the same node reports `role=leader` across two snapshots `stable_for` apart, used after membership changes where single-config Paxos can briefly flap (no joint consensus). The successful-write path uses this helper rather than an unbounded retry loop.
