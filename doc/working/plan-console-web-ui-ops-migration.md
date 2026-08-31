<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Console — Web UI migrate to shared ops + layout rework Plan

Design draft: `doc/working/design-console-web-ui-ops-migration.md`
Backlog doc: `doc/backlog/R127-console-web-ui-ops-migration.md`

Goal: migrate `crowdb-web` Axum handlers to `ops::*`, rework the UI
to three domains (`Cluster | KV | Chunk`), remove Swagger, and
rewrite the E2E suite to be fast + stable.

**Landed so far** (Phase A, B, C1/C2/C3/C4/C5/C8, D1/D2/D5/D7/D8/D9/D10,
E1, E15): `ops::hardware` disk-group/disk CRUD + 16 unit tests,
`OpContext` provider + write-back helper, rack/node + disk-group/disk +
server lifecycle + delete handler migration, `cluster/reset` +
`cluster/clean` routes, Swagger removal (backend + UI), `Domain` enum +
`DomainContext`, Header domain toggle, App.tsx per-domain center panel,
KV/Chunk tab toggles, embedding contract update, `clusterClean` E2E
helper. `ops::kv_server` API extended to accept `DeployRequest` +
optional workspace dir + optional PID override.

Remaining work below.

## Phase C — Backend handler migration (remaining)

- [x] **C1: Migrate lifecycle rack/node handlers** —
  `http_add_rack`, `http_remove_rack`, `http_remove_node`,
  `http_add_rack_node` now call `ops::hardware::*` via
  `OpContext` + `commit_op_context`. Dead `lifecycle/rack_node.rs`
  deleted (was never compiled — no `mod rack_node` declaration).
  Files: `app/crowdb-web/src/lifecycle.rs`.
- [x] **C3: Migrate server lifecycle handlers** —
  `http_deploy_node_server`, `http_restart_node_server`,
  `http_stop_node_server` now call `ops::kv_server::{deploy, restart,
  stop}`. `ops::kv_server` API extended: `deploy` accepts
  `&DeployRequest` + `Option<&Path>` (workspace dir); `restart`
  accepts `Option<&Path>`; `stop` accepts `Option<u32>` (PID
  override for web's runtime PID). Web handlers retain web-specific
  concerns (runtime PID tracking, monitor cache refresh, RPC
  connection cache clearing).
  Files: `app/crowdb-web/src/lifecycle.rs`,
  `lib/crowdb-console-shared/src/ops/kv_server.rs`,
  `app/crowdb-cli/src/commands/kv_server.rs`.
- [x] **C4: Migrate `http_delete_node_server` + require-empty** —
  now calls `ops::kv_server::check_require_empty` (returns 409
  Conflict when replicas exist). Stops via
  `stop_and_remove_server_for_node` (preserves runtime PID
  handling).
  Files: `app/crowdb-web/src/lifecycle.rs`.
- [ ] **C6: Migrate mgmt store/group/replica/init handlers** —
  `http_add_store`, `http_remove_store`, `http_list_stores`,
  `http_add_group`, `http_remove_group`, `http_add_replica`,
  `http_remove_replica`, `http_cluster_init` → `ops::kv_logical::*`
  / `ops::cluster::*`. Read handlers (`http_get_store`,
  `http_list_groups`, `http_list_replicas`) read from sysdata via
  `ops::kv_logical::*`. `cluster_init.rs` is still the hand-rolled
  5-phase bootstrap — replace with `ops::cluster::init`.
  Files: `app/crowdb-web/src/mgmt/store_ops.rs`,
  `app/crowdb-web/src/mgmt/group_ops.rs`,
  `app/crowdb-web/src/mgmt/replica_ops.rs`,
  `app/crowdb-web/src/mgmt/cluster_init.rs`,
  `app/crowdb-web/src/mgmt/topology.rs`.
- [ ] **C7: Migrate KV data-plane handlers** — `http_kv_get`,
  `http_kv_put`, `http_kv_delete`, `http_kv_scan`,
  `http_kv_endpoint`, snapshot endpoints → `ops::kv_data::*`.
  `kv.rs` currently does manual leader resolution via monitor cache
  + direct `state.kv_client().await`.
  Files: `app/crowdb-web/src/kv.rs`.
- [ ] **C9: Integration tests for migrated handlers** — verify
  delegation paths (init → `ops::cluster::init`, racks →
  `ops::hardware::add_rack`, stores → `ops::kv_logical::add_store`,
  kv/get → `ops::kv_data::get`, etc.) + 404 for removed
  Swagger/openapi routes. Blocked on C1, C3, C4, C6, C7.
  Files: `app/crowdb-web/tests/` (new or updated integration tests).

## Phase D — UI layout rework (remaining)

- [ ] **D3: `usePhysicalTree` → `useClusterTree`** — rename +
  extend to fetch disk-groups + disks (merge `useCapacityTree`'s
  disk data for the Cluster domain sidebar). `useClusterTree.ts`
  does not exist; `usePhysicalTree.ts` is still present and imported
  by `App.tsx`.
  Files: `app/crowdb-web/ui/src/data/usePhysicalTree.ts` (renamed
  to `useClusterTree.ts`), `app/crowdb-web/ui/src/App.tsx`.
- [ ] **D4: Sidebar per-domain tree (IA fix)** — current Sidebar
  branches have the wrong information architecture:
  - Cluster branch shows `rack → node → servers` instead of
    `rack → node → disk-group → disk`.
  - KV branch shows `store → group → replica` instead of
    `rack → node` (+ logical sub-tree under deployed servers).
  - Chunk branch shows `rack → node → disk-group → disk` instead
    of `rack → node → chunkdb/diskdb/diskio`.
  Rewrite all three branches to match the design draft §3.2–§3.4.
  Files: `app/crowdb-web/ui/src/shell/Sidebar.tsx`.
- [ ] **D6: Topology canvas — Disk node type** — `DiskGroup` node
  type exists in `buildFlow.ts` but renders as a single labeled
  node, not a box containing disk elements. Add a `Disk` node type
  (cylinder icon + short UUID prefix) as children of `DiskGroup`
  nodes for the Cluster domain canvas.
  Files: `app/crowdb-web/ui/src/topology/buildFlow.ts`,
  `app/crowdb-web/ui/src/topology/CrowdbKVNode.tsx`.
- [ ] **D11: Vitest embedding-contract test** — `DomainContext`
  and `Header` unit tests exist, but there is no test asserting the
  `CrowdbConsoleProps` embedding contract (`initialDomain`
  accepted, `initialViewMode` / `initialNodeId` / `'swagger'`
  module gone).
  Files: `app/crowdb-web/ui/src/test/` (new).

## Phase E — E2E test rework (remaining)

E2E specs were patched in place for the domain toggle (domain-*
testids added) but **not renamed, not split**, and 7 files still
have stale `ViewMode` / `Physical` / `Logical` references. All
specs below need: rename per design §5.4, update stale references,
apply poll-not-sleep + shared-cluster-in-`beforeAll` principles.

- [ ] **E3: `10-cluster-rack-node.spec.ts`** (renamed from
  `10-physical-rack-node.spec.ts`) — Cluster domain sidebar + canvas;
  `beforeAll` shared cluster. Stale refs at lines 192, 321.
  Files: `app/crowdb-web/ui/e2e/flows/10-physical-rack-node.spec.ts`.
- [ ] **E4: `11-cluster-server-lifecycle.spec.ts`** (renamed from
  `11-physical-server-lifecycle.spec.ts`) — Cluster domain server
  deploy/restart/stop/delete; add require-empty delete test (409
  with replicas, 204 when empty). Depends on C4.
  Files: `app/crowdb-web/ui/e2e/flows/11-physical-server-lifecycle.spec.ts`.
- [ ] **E5: `12-cluster-node-inspect.spec.ts`** (renamed from
  `12-physical-node-inspect.spec.ts`) — Cluster domain inspector.
  Stale refs at lines 23, 187.
  Files: `app/crowdb-web/ui/e2e/flows/12-physical-node-inspect.spec.ts`.
- [ ] **E6: `20-kv-store-group.spec.ts`** (renamed from
  `20-kv-cluster-store-group.spec.ts`) — KV domain Cluster tab;
  shared cluster. Stale ref at line 37.
  Files: `app/crowdb-web/ui/e2e/flows/20-kv-cluster-store-group.spec.ts`.
- [ ] **E7: `21-kv-reconfig.spec.ts`** (renamed from
  `21-kv-cluster-reconfig.spec.ts`) — KV domain reconfig. Stale
  refs at lines 100, 202, 416.
  Files: `app/crowdb-web/ui/e2e/flows/21-kv-cluster-reconfig.spec.ts`.
- [ ] **E8: `22-kv-topology.spec.ts`** (renamed from
  `22-kv-cluster-topology.spec.ts`) — KV domain topology canvas.
  Files: `app/crowdb-web/ui/e2e/flows/22-kv-cluster-topology.spec.ts`.
- [ ] **E9: `30-kv-ops-basic.spec.ts`** + **`31-kv-ops-advanced.spec.ts`**
  — KV domain KV tab; poll-not-sleep, precise selectors.
  Files: `app/crowdb-web/ui/e2e/flows/30-kv-ops-basic.spec.ts`,
  `app/crowdb-web/ui/e2e/flows/31-kv-ops-advanced.spec.ts`.
- [ ] **E10: `40-inspector-activity.spec.ts`** — domain-agnostic
  inspector (minimal changes).
  Files: `app/crowdb-web/ui/e2e/flows/40-inspector-activity.spec.ts`.
- [ ] **E11: `41-canvas-fit-pan.spec.ts`** — Cluster domain canvas.
  Stale refs at lines 10, 43, 122, 152, 165, 170, 183, 184.
  Files: `app/crowdb-web/ui/e2e/flows/41-canvas-fit-pan.spec.ts`.
- [ ] **E12: Split `50-capacity-diskdb.spec.ts`** into
  `50-chunk-capacity-disk-group.spec.ts`,
  `51-chunk-capacity-disk.spec.ts`,
  `52-chunk-capacity-zone.spec.ts` — Chunk domain Capacity sub-view;
  each with `beforeAll` shared cluster; poll-not-sleep. The 91 KB
  file has 13 stale refs.
  Files: `app/crowdb-web/ui/e2e/flows/50-capacity-diskdb.spec.ts`
  (split into 3).
- [ ] **E13: `53-chunk-capacity-canvas.spec.ts`** (renamed from
  `51-capacity-canvas.spec.ts`) — Chunk domain canvas.
  Files: `app/crowdb-web/ui/e2e/flows/51-capacity-canvas.spec.ts`.
- [ ] **E14: `90-flow-full-chain.spec.ts`** — full chain across all
  three domains. Stale refs at lines 12, 37, 48, 61, 85, 97, 107,
  262.
  Files: `app/crowdb-web/ui/e2e/flows/90-flow-full-chain.spec.ts`.

## Phase F — Verification + commit

- [ ] **F1: Run `pixi run -- cargo test -p crowdb-console-shared`** —
  all unit tests pass.
- [ ] **F2: Run `pixi run -- cargo test -p crowdb-web`** —
  integration tests pass.
- [ ] **F3: Run `pixi run -- cargo clippy --all-targets -- -D warnings`**
  — no warnings.
- [ ] **F4: Run `pixi run -- cargo fmt --all -- --check`** — no
  formatting drift.
- [ ] **F5: Build binaries** — `pixi run -- cargo build -p
  crowdb-kv-server -p crowdb-diskdb`.
- [ ] **F6: Run Playwright E2E** — `cd app/crowdb-web/ui && npx
  playwright test --config=e2e/realBackend.config.ts`. All specs
  pass; check `slowReporter` output for slow tests.
- [ ] **F7: Commit** — implementation commits (Phase C–E) + final
  commit with design draft + plan.

## Remaining file list

- `app/crowdb-web/src/mgmt/store_ops.rs` — migrate to `ops::kv_logical`
  (C6).
- `app/crowdb-web/src/mgmt/group_ops.rs` — migrate to `ops::kv_logical`
  (C6).
- `app/crowdb-web/src/mgmt/replica_ops.rs` — migrate to
  `ops::kv_logical` (C6).
- `app/crowdb-web/src/mgmt/cluster_init.rs` — migrate to
  `ops::cluster::init` (C6).
- `app/crowdb-web/src/mgmt/topology.rs` — read from sysdata (C6).
- `app/crowdb-web/src/kv.rs` — migrate to `ops::kv_data` (C7).
- `app/crowdb-web/tests/` — integration tests for migrated handlers
  (C9).
- `app/crowdb-web/ui/src/data/usePhysicalTree.ts` — renamed to
  `useClusterTree.ts`, extended (D3).
- `app/crowdb-web/ui/src/shell/Sidebar.tsx` — per-domain IA fix (D4).
- `app/crowdb-web/ui/src/topology/buildFlow.ts` — +Disk node type
  (D6).
- `app/crowdb-web/ui/src/topology/CrowdbKVNode.tsx` — +Disk render
  (D6).
- `app/crowdb-web/ui/src/test/` — embedding-contract Vitest test
  (D11).
- `app/crowdb-web/ui/e2e/flows/*.spec.ts` — 12 specs
  renamed/updated/split (E3–E14).

## Remaining test checklist

**Unit (Rust):**
- [ ] `ops::kv_server::delete` require-empty conflict (via web
  handler integration test after C4).

**Unit (Vitest):**
- [ ] `CrowdbConsoleProps` embedding contract (`initialDomain`, no
  `initialViewMode`/`initialNodeId`/`'swagger'`).

**Integration (Rust handler tests):**
- [ ] `POST /api/cluster/init` → `ops::cluster::init`.
- [x] `POST /api/racks` → `ops::hardware::add_rack` (via
  `lifecycle_routes_test::rack_node_crud_through_web_routes`).
- [x] `POST /api/nodes` → `ops::hardware::add_node` (via
  `lifecycle_routes_test::rack_node_crud_through_web_routes`).
- [x] `POST /api/nodes/:id/server/deploy` → `ops::kv_server::deploy`
  (via `lifecycle_routes_test::deploy_then_restart_local_server` +
  `deploy_then_stop_local_server`).
- [ ] `DELETE /api/nodes/:id/server` → `ops::kv_server::delete`
  (409 + 204) — require-empty check added; 409 + 204 integration test
  pending C9.
- [ ] `POST /api/stores` → `ops::kv_logical::add_store`.
- [ ] `GET /api/stores/:sid/groups/:gid/kv/get` → `ops::kv_data::get`.
- [ ] `/api/swagger/` → 404.
- [ ] `/api/nodes/:id/openapi.json` → 404.

**E2E (Playwright):**
- [ ] Cluster domain sidebar + hierarchy chart (renamed spec).
- [ ] KV domain sidebar + [Cluster]/[KV] tabs + selection
  persistence (renamed spec).
- [ ] Chunk domain sidebar + [Capacity]/[Chunk] toggle (split specs).
- [ ] `DELETE /api/nodes/:id/server` require-empty (409 + 204).
- [ ] `POST /api/cluster/reset` full teardown.
- [ ] `POST /api/cluster/clean` orphan removal.
- [ ] Embedding contract (`initialDomain`, `modules`).
- [ ] Full chain across all three domains.
