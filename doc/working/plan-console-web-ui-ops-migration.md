<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Console — Web UI migrate to shared ops + layout rework Plan

Design draft: `doc/working/design-console-web-ui-ops-migration.md`
Backlog doc: `doc/backlog/R127-console-web-ui-ops-migration.md`

Goal: migrate `crowdb-web` Axum handlers to `ops::*`, rework the UI
to three domains (`Cluster | KV | Chunk`), remove Swagger, and
rewrite the E2E suite to be fast + stable.

## Phase A — `ops::hardware` disk-group/disk extensions

- [ ] **A1: Add disk-group CRUD to `ops::hardware`** — add
  `add_disk_group`, `remove_disk_group`, `list_disk_groups` mirroring
  the existing `lifecycle.rs` handler logic but going through
  `OpContext` (config write + best-effort sysdata sync).
  Files: `lib/crowdb-console-shared/src/ops/hardware.rs`.
- [ ] **A2: Add disk CRUD to `ops::hardware`** — add `add_disk`,
  `add_disks_batch` (atomic), `remove_disk`, `list_disks`,
  `set_disk_status`, `set_disk_group_status`.
  Files: `lib/crowdb-console-shared/src/ops/hardware.rs`.
- [ ] **A3: Add `OpContext` constructor for shared `Arc<CrowdbKvClient>`**
  — add a constructor variant that accepts a pre-built
  `Arc<CrowdbKvClient>` so the web's cached client can be shared.
  Files: `lib/crowdb-console-shared/src/ops/context.rs`.
- [ ] **A4: Unit tests for `ops::hardware` disk-group/disk** —
  atomic batch rollback, require-empty disk-group removal, sysdata
  best-effort skip when group 0 absent.
  Files: `lib/crowdb-console-shared/src/ops/hardware.rs` (inline
  `#[cfg(test)]` module).

## Phase B — `OpContext` provider for web

- [ ] **B1: `AppState::op_context` builder** — add the
  `op_context()` method that resolves the group-0 endpoint, shares
  the cached `CrowdbKvClient`, and snapshots `ConsoleConfig`.
  Files: `app/crowdb-web/src/state.rs`.
- [ ] **B2: Config write-back helper** — add a helper on `AppState`
  that takes a mutated `OpContext` config and writes it back to
  `AppState.config` + persists (short critical section, no `await`
  inside the lock).
  Files: `app/crowdb-web/src/state.rs`.
- [ ] **B3: Unit tests for `op_context`** — shared `Arc<CrowdbKvClient>`
  identity, no-deployed-server error, config write-back.
  Files: `app/crowdb-web/src/state.rs` (inline `#[cfg(test)]` module).

## Phase C — Backend handler migration

- [ ] **C1: Migrate lifecycle rack/node handlers** — `http_add_rack`,
  `http_add_node`, `http_remove_rack`, `http_remove_node`,
  `http_list_racks`, `http_list_nodes`, `http_get_rack`,
  `http_get_node`, `http_ping_node` → `ops::hardware::*`.
  Files: `app/crowdb-web/src/lifecycle.rs`,
  `app/crowdb-web/src/lifecycle/rack_node.rs`.
- [ ] **C2: Migrate lifecycle disk-group/disk handlers** —
  `http_add_node_disk_group`, `http_remove_node_disk_group`,
  `http_add_disk`, `http_add_disks_batch`, `http_remove_disk`,
  `http_list_node_disk_groups`, `http_list_disks_in_group`,
  `http_get_disk` → `ops::hardware::*` (new functions from Phase A).
  Files: `app/crowdb-web/src/lifecycle.rs`.
- [ ] **C3: Migrate server lifecycle handlers** —
  `http_deploy_node_server`, `http_restart_node_server`,
  `http_stop_node_server` → `ops::kv_server::*`.
  Files: `app/crowdb-web/src/lifecycle.rs`.
- [ ] **C4: Migrate `http_delete_node_server` + require-empty** —
  rewrite to call `ops::kv_server::delete` (adds require-empty check,
  returns 409 Conflict when replicas exist).
  Files: `app/crowdb-web/src/lifecycle.rs`.
- [ ] **C5: Migrate cluster-op handlers** — rewrite
  `http_internal_reset` to call `ops::cluster::reset`; add
  `http_cluster_clean` handler + `POST /api/cluster/clean` route.
  Files: `app/crowdb-web/src/lifecycle.rs`, `app/crowdb-web/src/lib.rs`.
- [ ] **C6: Migrate mgmt store/group/replica/init handlers** —
  `http_add_store`, `http_remove_store`, `http_list_stores`,
  `http_add_group`, `http_remove_group`, `http_add_replica`,
  `http_remove_replica`, `http_cluster_init` → `ops::kv_logical::*` /
  `ops::cluster::*`. Read handlers (`http_get_store`, `http_list_groups`,
  `http_list_replicas`) read from sysdata via `ops::kv_logical::*`.
  Files: `app/crowdb-web/src/mgmt/store_ops.rs`,
  `app/crowdb-web/src/mgmt/group_ops.rs`,
  `app/crowdb-web/src/mgmt/replica_ops.rs`,
  `app/crowdb-web/src/mgmt/cluster_init.rs`,
  `app/crowdb-web/src/mgmt/topology.rs`.
- [ ] **C7: Migrate KV data-plane handlers** — `http_kv_get`,
  `http_kv_put`, `http_kv_delete`, `http_kv_scan`, `http_kv_endpoint`,
  snapshot endpoints → `ops::kv_data::*`.
  Files: `app/crowdb-web/src/kv.rs`.
- [ ] **C8: Remove Swagger + openapi proxy from backend** — remove
  `/api/swagger` route, `/api/nodes/:id/openapi.json` route,
  `http_node_openapi_proxy`, `SWAGGER_UI_DIR`, `openapi_cache` field.
  Delete `swagger-ui/` dir + `tests/swagger_routes_test.rs`.
  Files: `app/crowdb-web/src/lib.rs`, `app/crowdb-web/src/state.rs`,
  `app/crowdb-web/src/lifecycle.rs`,
  `app/crowdb-web/tests/swagger_routes_test.rs` (deleted),
  `app/crowdb-web/swagger-ui/` (deleted).
- [ ] **C9: Integration tests for migrated handlers** — verify
  delegation paths (init → `ops::cluster::init`, racks →
  `ops::hardware::add_rack`, etc.) + 404 for removed Swagger/openapi
  routes.
  Files: `app/crowdb-web/tests/` (new or updated integration tests).

## Phase D — UI layout rework

- [ ] **D1: `ViewMode` → `Domain` enum** — replace the enum in
  `types/index.ts`; rename `ViewModeContext.tsx` → `DomainContext.tsx`
  (`viewMode` → `domain`, `initialViewMode` → `initialDomain`).
  Files: `app/crowdb-web/ui/src/types/index.ts`,
  `app/crowdb-web/ui/src/contexts/ViewModeContext.tsx` (renamed).
- [ ] **D2: Header domain toggle** — replace Physical/KV Cluster/
  Capacity toggle with Cluster/KV/Chunk; remove Swagger API toggle +
  node selector + KV toggle; add `data-testid` anchors
  (`domain-cluster`, `domain-kv`, `domain-chunk`).
  Files: `app/crowdb-web/ui/src/shell/Header.tsx`.
- [ ] **D3: `usePhysicalTree` → `useClusterTree`** — rename + extend
  to fetch disk-groups + disks (merge `useCapacityTree`'s disk data
  for the Cluster domain sidebar).
  Files: `app/crowdb-web/ui/src/data/usePhysicalTree.ts` (renamed).
- [ ] **D4: Sidebar per-domain tree** — Cluster: rack → node →
  disk-group → disk; KV: rack → node (+ logical sub-tree under
  deployed servers); Chunk: rack → node → chunkdb/diskdb/diskio.
  Files: `app/crowdb-web/ui/src/shell/Sidebar.tsx`.
- [ ] **D5: App.tsx per-domain center panel** — replace
  `centerPanel` state with per-domain logic: Cluster → hierarchy
  chart; KV → [Cluster] [KV] tab bar; Chunk → [Capacity] [Chunk]
  toggle. Remove `swagger`/`kv` center panel modes. Add `data-testid`
  for tab buttons.
  Files: `app/crowdb-web/ui/src/App.tsx`.
- [ ] **D6: Topology canvas — DiskGroup/Disk node types** — extend
  `buildFlow.ts` + `CrowdbKVNode.tsx` with DiskGroup (box of disks)
  and Disk (cylinder + UUID prefix) node types for the Cluster domain.
  Files: `app/crowdb-web/ui/src/topology/buildFlow.ts`,
  `app/crowdb-web/ui/src/topology/CrowdbKVNode.tsx`.
- [ ] **D7: KV domain tab bar** — [Cluster] [KV] tab bar in the KV
  domain center panel; Cluster tab shows logical topology, KV tab
  shows `KvOperatorPanel`; selection persists across tab switches.
  Files: `app/crowdb-web/ui/src/App.tsx` (or a new
  `panels/KvDomainCenter.tsx`).
- [ ] **D8: Chunk domain toggle** — [Capacity] [Chunk] button toggle
  in the Chunk domain center panel; Capacity shows `CapacityPanel`,
  Chunk is blank but clickable.
  Files: `app/crowdb-web/ui/src/App.tsx` (or a new
  `panels/ChunkDomainCenter.tsx`).
- [ ] **D9: Delete `SwaggerPanel.tsx`** — remove the component + all
  imports.
  Files: `app/crowdb-web/ui/src/panels/SwaggerPanel.tsx` (deleted),
  `app/crowdb-web/ui/src/App.tsx`.
- [ ] **D10: Embedding contract update** — `CrowdbConsoleProps`:
  `initialViewMode` → `initialDomain`, remove `'swagger'` from
  `modules`, remove `initialNodeId`. `embed.ts` re-exports `Domain`.
  Files: `app/crowdb-web/ui/src/App.tsx`,
  `app/crowdb-web/ui/src/embed.ts`.
- [ ] **D11: Vitest unit tests for Domain enum + embedding contract**
  — assert `Domain` string values, `initialDomain` accepted,
  `initialViewMode`/`initialNodeId`/`'swagger'` gone.
  Files: `app/crowdb-web/ui/src/test/` (new).

## Phase E — E2E test rework

- [ ] **E1: `00-shell-embedding.spec.ts`** — remove Swagger
  assertions; update for `initialDomain` + domain toggle; add
  `data-testid` assertions for domain buttons.
  Files: `app/crowdb-web/ui/e2e/flows/00-shell-embedding-swagger.spec.ts`
  (renamed).
- [ ] **E2: `01-shell-ui-behaviors.spec.ts`** — update for domain
  toggle; shared cluster in `beforeAll`.
  Files: `app/crowdb-web/ui/e2e/flows/01-shell-ui-behaviors.spec.ts`.
- [ ] **E3: `10-cluster-rack-node.spec.ts`** (renamed from
  `10-physical-rack-node`) — Cluster domain sidebar + canvas;
  `beforeAll` shared cluster.
  Files: `app/crowdb-web/ui/e2e/flows/10-physical-rack-node.spec.ts`
  (renamed).
- [ ] **E4: `11-cluster-server-lifecycle.spec.ts`** (renamed) —
  Cluster domain server deploy/restart/stop/delete; add require-empty
  delete test (409 with replicas, 204 when empty).
  Files: `app/crowdb-web/ui/e2e/flows/11-physical-server-lifecycle.spec.ts`
  (renamed).
- [ ] **E5: `12-cluster-node-inspect.spec.ts`** (renamed) — Cluster
  domain inspector.
  Files: `app/crowdb-web/ui/e2e/flows/12-physical-node-inspect.spec.ts`
  (renamed).
- [ ] **E6: `20-kv-store-group.spec.ts`** (renamed) — KV domain
  Cluster tab; shared cluster.
  Files: `app/crowdb-web/ui/e2e/flows/20-kv-cluster-store-group.spec.ts`
  (renamed).
- [ ] **E7: `21-kv-reconfig.spec.ts`** (renamed) — KV domain
  reconfig.
  Files: `app/crowdb-web/ui/e2e/flows/21-kv-cluster-reconfig.spec.ts`
  (renamed).
- [ ] **E8: `22-kv-topology.spec.ts`** (renamed) — KV domain
  topology canvas.
  Files: `app/crowdb-web/ui/e2e/flows/22-kv-cluster-topology.spec.ts`
  (renamed).
- [ ] **E9: `30-kv-ops-basic.spec.ts`** + **`31-kv-ops-advanced.spec.ts`**
  — KV domain KV tab; poll-not-sleep, precise selectors.
  Files: `app/crowdb-web/ui/e2e/flows/30-kv-ops-basic.spec.ts`,
  `app/crowdb-web/ui/e2e/flows/31-kv-ops-advanced.spec.ts`.
- [ ] **E10: `40-inspector-activity.spec.ts`** — domain-agnostic
  inspector (minimal changes).
  Files: `app/crowdb-web/ui/e2e/flows/40-inspector-activity.spec.ts`.
- [ ] **E11: `41-canvas-fit-pan.spec.ts`** — Cluster domain canvas.
  Files: `app/crowdb-web/ui/e2e/flows/41-canvas-fit-pan.spec.ts`.
- [ ] **E12: Split `50-capacity-diskdb.spec.ts`** into
  `50-chunk-capacity-disk-group.spec.ts`,
  `51-chunk-capacity-disk.spec.ts`,
  `52-chunk-capacity-zone.spec.ts` — Chunk domain Capacity sub-view;
  each with `beforeAll` shared cluster; poll-not-sleep.
  Files: `app/crowdb-web/ui/e2e/flows/50-capacity-diskdb.spec.ts`
  (split into 3).
- [ ] **E13: `53-chunk-capacity-canvas.spec.ts`** (renamed from
  `51-capacity-canvas`) — Chunk domain canvas.
  Files: `app/crowdb-web/ui/e2e/flows/51-capacity-canvas.spec.ts`
  (renamed).
- [ ] **E14: `90-flow-full-chain.spec.ts`** — full chain across all
  three domains.
  Files: `app/crowdb-web/ui/e2e/flows/90-flow-full-chain.spec.ts`.
- [ ] **E15: `crowClusterDeployer.ts` + `clusterClean` helper** —
  add `clusterClean(baseURL)` helper for the new `POST /api/cluster/clean`
  endpoint.
  Files: `app/crowdb-web/ui/e2e/fixtures/crowClusterDeployer.ts`.

## Phase F — Verification + commit

- [ ] **F1: Run `pixi run -- cargo test -p crowdb-console-shared`** —
  Phase A unit tests pass.
- [ ] **F2: Run `pixi run -- cargo test -p crowdb-web`** — Phase B/C
  unit + integration tests pass.
- [ ] **F3: Run `pixi run -- cargo clippy --all-targets -- -D warnings`**
  — no warnings.
- [ ] **F4: Run `pixi run -- cargo fmt --all -- --check`** — no
  formatting drift.
- [ ] **F5: Build binaries** — `pixi run -- cargo build -p
  crowdb-kv-server -p crowdb-diskdb`.
- [ ] **F6: Run Playwright E2E** — `cd app/crowdb-web/ui && npx
  playwright test --config=e2e/realBackend.config.ts`. All specs
  pass; check `slowReporter` output for slow tests.
- [ ] **F7: Commit** — implementation commits (Phase A–E) + final
  commit with design draft + plan.

## File list

- `lib/crowdb-console-shared/src/ops/context.rs` — +constructor for shared `Arc<CrowdbKvClient>`.
- `lib/crowdb-console-shared/src/ops/hardware.rs` — +disk-group/disk CRUD.
- `app/crowdb-web/src/state.rs` — +`op_context()`, +write-back helper, -`openapi_cache`, -`SWAGGER_UI_DIR`.
- `app/crowdb-web/src/lib.rs` — -swagger/openapi routes, +`cluster/clean`.
- `app/crowdb-web/src/lifecycle.rs` — migrate to `ops::*`, -openapi proxy, +require-empty.
- `app/crowdb-web/src/lifecycle/rack_node.rs` — migrate to `ops::hardware`.
- `app/crowdb-web/src/mgmt/store_ops.rs` — migrate to `ops::kv_logical`.
- `app/crowdb-web/src/mgmt/group_ops.rs` — migrate to `ops::kv_logical`.
- `app/crowdb-web/src/mgmt/replica_ops.rs` — migrate to `ops::kv_logical`.
- `app/crowdb-web/src/mgmt/cluster_init.rs` — migrate to `ops::cluster::init`.
- `app/crowdb-web/src/mgmt/topology.rs` — read from sysdata.
- `app/crowdb-web/src/kv.rs` — migrate to `ops::kv_data`.
- `app/crowdb-web/src/physical.rs` — review (may shrink).
- `app/crowdb-web/src/physical_view.rs` — review (may shrink).
- `app/crowdb-web/tests/swagger_routes_test.rs` — deleted.
- `app/crowdb-web/swagger-ui/` — deleted.
- `app/crowdb-web/ui/src/types/index.ts` — `ViewMode` → `Domain`.
- `app/crowdb-web/ui/src/contexts/ViewModeContext.tsx` — renamed to `DomainContext.tsx`.
- `app/crowdb-web/ui/src/shell/Header.tsx` — domain toggle, -swagger/-kv toggles.
- `app/crowdb-web/ui/src/shell/Sidebar.tsx` — per-domain tree.
- `app/crowdb-web/ui/src/App.tsx` — per-domain center panel, -swagger/-kv modes, embedding contract.
- `app/crowdb-web/ui/src/panels/SwaggerPanel.tsx` — deleted.
- `app/crowdb-web/ui/src/panels/CapacityPanel.tsx` — render condition → Chunk domain.
- `app/crowdb-web/ui/src/panels/KvOperatorPanel.tsx` — render condition → KV domain KV tab.
- `app/crowdb-web/ui/src/data/usePhysicalTree.ts` — renamed to `useClusterTree.ts`, extended.
- `app/crowdb-web/ui/src/topology/buildFlow.ts` — +DiskGroup/Disk node types.
- `app/crowdb-web/ui/src/topology/CrowdbKVNode.tsx` — +DiskGroup/Disk render.
- `app/crowdb-web/ui/src/embed.ts` — re-export `Domain`.
- `app/crowdb-web/ui/e2e/flows/*.spec.ts` — 15 specs rewritten/renamed/split.
- `app/crowdb-web/ui/e2e/fixtures/crowClusterDeployer.ts` — +`clusterClean` helper.

## Test checklist

**Unit (Rust):**
- [ ] `AppState::op_context` shares cached `CrowdbKvClient`.
- [ ] `AppState::op_context` no-deployed-server error.
- [ ] Config write-back after `ops::hardware::add_rack`.
- [ ] `ops::hardware::add_disks_batch` atomic rollback.
- [ ] `ops::kv_server::delete` require-empty conflict.

**Unit (Vitest):**
- [ ] `Domain` enum string values.
- [ ] `CrowdbConsoleProps` embedding contract (`initialDomain`, no `initialViewMode`/`initialNodeId`/`'swagger'`).

**Integration (Rust handler tests):**
- [ ] `POST /api/cluster/init` → `ops::cluster::init`.
- [ ] `POST /api/racks` → `ops::hardware::add_rack`.
- [ ] `POST /api/nodes` → `ops::hardware::add_node`.
- [ ] `POST /api/nodes/:id/server/deploy` → `ops::kv_server::deploy`.
- [ ] `POST /api/stores` → `ops::kv_logical::add_store`.
- [ ] `GET /api/stores/:sid/groups/:gid/kv/get` → `ops::kv_data::get`.
- [ ] `/api/swagger/` → 404.
- [ ] `/api/nodes/:id/openapi.json` → 404.

**E2E (Playwright):**
- [ ] Header domain toggle (Cluster/KV/Chunk).
- [ ] Cluster domain sidebar + hierarchy chart.
- [ ] KV domain sidebar + [Cluster]/[KV] tabs + selection persistence.
- [ ] Chunk domain sidebar + [Capacity]/[Chunk] toggle.
- [ ] Inspector across all three domains.
- [ ] `DELETE /api/nodes/:id/server` require-empty (409 + 204).
- [ ] `POST /api/cluster/reset` full teardown.
- [ ] `POST /api/cluster/clean` orphan removal.
- [ ] No Swagger panel / API toggle renders.
- [ ] Embedding contract (`initialDomain`, `modules`).
- [ ] Full chain across all three domains.
