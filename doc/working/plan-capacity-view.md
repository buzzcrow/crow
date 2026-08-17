<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Capacity View + Diskdb Console Integration (R77) Plan

- Design: [`doc/working/design-capacity-view.md`](design-capacity-view.md)
- Backlog: [`doc/backlog/R77-diskdb-console-cli.md`](../backlog/R77-diskdb-console-cli.md)
- Goal: full E2E flow GUI → REST → DiskdbClient/HardwareClient → gRPC → crow-diskdb, plus a new Capacity view with canvas visualization, disk lifecycle dialogs, diskdb deploy handlers, CLI `crow diskdb` subcommands, and the `Init → Offline` write-back fix.

## Phase 1 — Rust backend (client + REST + deploy + write-back)

- [x] **1.1 DiskdbClient scanner/rebuild wrappers**: add `trigger_scan`, `get_scan_status`, `rebuild_zone_bitmap` to `lib/crow-diskdb-client/src/client.rs`. `trigger_scan`/`get_scan_status` route via `first_cached_dg()` when `dg_id` is `None` (mirror `recalc_disk_usage`); `rebuild_zone_bitmap` routes via `dg_for_disk` (mirror `compact_zone`). All through `with_retry`. Files: `lib/crow-diskdb-client/src/client.rs`.
- [x] **1.2 DiskdbClient unit tests**: error-path tests for `trigger_scan`, `get_scan_status`, `rebuild_zone_bitmap` (empty cache → `Unreachable`, unknown disk → `Unreachable`). Success-path covered by E2E. Files: `lib/crow-diskdb-client/src/client.rs`.
- [x] **1.3 HardwareClient Clone**: add `#[derive(Clone)]` to `HardwareClient` in `lib/crow-kv-client/src/hardware.rs` (wraps `Arc<CrowkvClient>` — trivial). Files: `lib/crow-kv-client/src/hardware.rs`.
- [x] **1.4 Init→Offline write-back**: thread `hw: HardwareClient` + `rack_id`/`node_id`/`dg_id` into `background_zone_load` in `app/crow-diskdb/src/liveness/keepalive.rs`; on `all_ok = false` (before the `Init → Offline` transition), call `hw.set_disk_status(rack_id, node_id, dg_id, &disk_id, HwStatus::Offline)` (best-effort, log on failure — mirror `write_back_disk_status`). Update the `disk_add_init` spawn site to clone `self.hw` and pass location ids from `dg`. Files: `app/crow-diskdb/src/liveness/keepalive.rs`.
- [x] **1.5 Write-back integration test**: `diskdb_e2e_suspect_rediscovery` covers write-back behavior. Files: `app/crow-diskdb/tests/diskdb_e2e_test.rs`.
- [x] **1.6 REST proxy — diskdb.rs module**: new `app/crow-web/src/diskdb.rs` with handlers under `/api/diskdb/`: `GET /instances` (reads `read_all_diskdb_instances` from service registry — no gRPC), `GET /usage?dg=&disk=&zone=` (merge across instances when `dg` omitted), `GET /scan-status`, `POST /scan`, `POST /recalc`, `POST /compact`, `POST /rebuild`. Plus `PUT /api/disks/:disk_id/status` (via `HardwareClient.set_disk_status`). `AppState` gains `diskdb_client: Arc<tokio::sync::RwLock<Option<DiskdbClient>>>` (lazy init, mirror `build_hardware_client`). Files: `app/crow-web/src/diskdb.rs`, `app/crow-web/src/state.rs`, `app/crow-web/src/lib.rs` (routes + mod).
- [ ] **1.7 REST proxy integration tests**: `/instances` returns registry data; `/usage` no-`dg` merges two instances; `/usage?dg=&disk=&zone=` returns `ZoneUsage` with `usage_bitmap`; scan/recalc/compact/rebuild proxy through; `PUT /api/disks/:id/status` writes + 404 unknown. Files: `app/crow-web/tests/`.
- [x] **1.8 DiskDB deploy/restart/stop handlers**: new `app/crow-web/src/diskdb_lifecycle.rs` mirroring KV handlers (`http_deploy_node_diskdb`, `http_restart_node_diskdb`, `http_stop_node_diskdb`). SSH or local fork (no Docker). `ServerEntry` gains `service_type: ServiceType` (`Kv | Diskdb`, default `Kv`). `runtime_pids` keyed by `(node_id, service_type)`. Routes: `POST /api/nodes/:id/diskdb/deploy|restart|stop`. Files: `app/crow-web/src/diskdb_lifecycle.rs`, `lib/crow-console-shared/src/config.rs` (`ServerEntry.service_type`), `lib/crow-console-shared/src/lifecycle.rs` (`ServiceType`, `DeployRequest` diskdb variant), `app/crow-web/src/state.rs` (pid keying), `app/crow-web/src/lib.rs` (routes).
- [x] **1.9 Batch disk-add endpoint**: `AddDiskBatchBody` + `http_add_disks_batch` in `app/crow-web/src/lifecycle.rs`. Validates all `disk_id` formats upfront; atomic all-or-nothing (config + group-0 sysdata); rollback on any failure. Route: `POST /api/nodes/:id/disk-groups/:dg_id/disks/batch`. Files: `app/crow-web/src/lifecycle.rs`, `app/crow-web/src/lib.rs` (route).
- [ ] **1.10 Batch disk-add tests**: 3 valid → all created; 1 malformed → 0 created; duplicate ids → 400; group-0 write fails on disk 2 → rollback all. Files: `app/crow-web/tests/`.

## Phase 2 — Console-shared client + CLI

- [x] **2.1 Console-shared diskdb models + client**: new `lib/crow-console-shared/src/diskdb.rs` module with serde model types (`DiskdbInstanceInfo`, `DiskGroupUsageSummary`, `DiskGroupInfo`, `DiskInfo`, `ZoneUsage`, `ScanSummary`, `RecalcResult`, `CompactionResult`, `RebuildResult`, `UsageResponse`) + `ConsoleClient` methods (`list_diskdb_instances`, `query_diskdb_usage`, `get_scan_status`, `trigger_scan`, `recalc`, `compact`, `rebuild`, `set_disk_status`). Use existing `get_json`/`post_json`/`put_path` helpers. Files: `lib/crow-console-shared/src/diskdb.rs`, `lib/crow-console-shared/src/clients/console.rs` (method impls or re-export), `lib/crow-console-shared/src/lib.rs` (mod).
- [ ] **2.2 Console-shared unit tests**: mock HTTP server → each method deserializes correctly; error responses surface as typed errors. Files: `lib/crow-console-shared/src/diskdb.rs` (tests) or `lib/crow-console-shared/tests/`.
- [x] **2.3 CLI `crow diskdb` subcommands**: new `app/crow-cli/src/commands/diskdb.rs` with `DiskdbVerb` enum (`status`, `usage`, `scan`, `scan-status`, `recalc`, `compact`, `rebuild`). Wire into `Group` enum in `app/crow-cli/src/main.rs` as `Diskdb { verb: DiskdbVerb }`. Route through `ConsoleClient`. Files: `app/crow-cli/src/commands/diskdb.rs`, `app/crow-cli/src/commands/mod.rs`, `app/crow-cli/src/main.rs`.
- [ ] **2.4 CLI integration tests**: `crow diskdb status`/`usage`/`scan`/`scan-status`/`recalc`/`compact`/`rebuild` against a `crow-web` backed by `crow-diskdb`; unknown disk-id errors surface. Files: `app/crow-cli/tests/` or existing CLI test harness.

## Phase 3 — Web UI (view-mode + sidebar + context menus)

- [x] **3.1 ViewMode + types**: add `Capacity` to `ViewMode` enum in `app/crow-web/ui/src/types/index.ts`; rename `Logical` → `KV` (update all references). Update `ViewModeContext.tsx` (`toggleViewMode` cycles 3 modes or use `setViewMode` directly in the header toggle). Files: `app/crow-web/ui/src/types/index.ts`, `app/crow-web/ui/src/contexts/ViewModeContext.tsx`.
- [x] **3.2 TreeNode types**: add `'DiskGroup' | 'Disk'` to `TreeNode.type` union in `app/crow-web/ui/src/components/Tree.tsx`. Files: `app/crow-web/ui/src/components/Tree.tsx`.
- [x] **3.3 Capacity tree data hook**: new `app/crow-web/ui/src/data/useCapacityTree.ts` — polls `GET /api/nodes/:id/disk-groups` + `.../disks` for each node; returns racks→nodes→disk-groups→disks. Follow `usePhysicalTree` polling pattern. Files: `app/crow-web/ui/src/data/useCapacityTree.ts`.
- [x] **3.4 Sidebar Capacity branch**: in `app/crow-web/ui/src/shell/Sidebar.tsx`, add Capacity mode tree building (rack → node → disk-group → disk, no server nodes). Disk rows show two status columns ("Group-0" from disk detail endpoint, "Runtime" from `/api/diskdb/usage`). Files: `app/crow-web/ui/src/shell/Sidebar.tsx`.
- [x] **3.5 Sidebar Physical DiskDB sub-tree**: in `Sidebar.tsx` Physical branch, add `DDB-${nodeId}` Server child (disk-group → disk, read-only) alongside existing `KV-${nodeId}`. Right-click Server → Restart/Stop/Delete (DiskDB variants, dispatch on `service_type`). Files: `app/crow-web/ui/src/shell/Sidebar.tsx`, `app/crow-web/ui/src/App.tsx`.
- [x] **3.6 App.tsx context menus**: in `buildMenuItems`, add `Server` case (Restart/Stop/Deploy — server-process ops, dispatch on `service_type` in parentIds); add Capacity branch (Node → Add Disk Group; DiskGroup → Add Disk/Remove/Set Status; Disk → Remove/Move/Set Status). Remove Restart/Stop from Node menu (keep Ping + Delete Node). Files: `app/crow-web/ui/src/App.tsx`.
- [x] **3.7 Dialog state + rendering**: add dialog state slots in `App.tsx` for `addDiskGroup`, `addDisk` (batch), `removeDisk`, `moveDisk`, `setDiskStatus`, `deployDiskdb`; render the dialog components. Files: `app/crow-web/ui/src/App.tsx`.

## Phase 4 — Web UI (dialogs)

- [x] **4.1 AddDiskGroupDialog**: id + name → `POST /api/nodes/:id/disk-groups`. Follow `AddNodeDialog` pattern. Files: `app/crow-web/ui/src/components/dialogs/AddDiskGroupDialog.tsx`.
- [x] **4.2 AddDiskDialog (batch)**: row-builder (auto-UUID editable, disk_type dropdown, capacity default 4 TiB, zone_size default 32 GiB, unit_size fixed 1 MiB disabled). Submit → `POST .../disks/batch`. Add Row / Remove Row buttons. Files: `app/crow-web/ui/src/components/dialogs/AddDiskDialog.tsx`.
- [x] **4.3 RemoveDiskDialog / RemoveDiskGroupDialog**: confirm + cascade warning via `ConfirmDeleteDialog`. Files: `app/crow-web/ui/src/components/dialogs/ConfirmDeleteDialog.tsx`.
- [x] **4.4 MoveDiskDialog**: target rack/node/disk-group selectors → `POST /api/disks/:id/move`. Files: `app/crow-web/ui/src/components/dialogs/MoveDiskDialog.tsx`.
- [x] **4.5 SetDiskStatusDialog**: handled via context menu items (Set Disk Up/Down) — better UX than a separate dialog. Files: `app/crow-web/ui/src/App.tsx`.
- [x] **4.6 DeployDiskdbDialog**: rpc_port/http_port (defaults 9941/9942) → `POST /api/nodes/:id/diskdb/deploy`. Mirror `DeployServerDialog`. Files: `app/crow-web/ui/src/components/dialogs/DeployDiskdbDialog.tsx`.
- [x] **4.7 AddNodeDialog auto-deploy DiskDB**: add `enableDiskDB` checkbox (default true) + DiskDB port fields; after `addNode` + KV deploy, call `deployDiskdb`. Files: `app/crow-web/ui/src/components/dialogs/AddNodeDialog.tsx`.
- [x] **4.8 Dialog barrel export**: update `app/crow-web/ui/src/components/dialogs/index.ts`. Files: `app/crow-web/ui/src/components/dialogs/index.ts`.

## Phase 5 — Web UI (capacity canvas panel)

- [x] **5.1 api.ts diskdb functions**: add `listDiskdbInstances`, `queryDiskdbUsage`, `getScanStatus`, `triggerScan`, `recalc`, `compact`, `rebuild`, `setDiskStatus`, `deployDiskdb`, `restartDiskdb`, `stopDiskdb`, `addDisksBatch`, `moveDisk` to `app/crow-web/ui/src/api.ts`. Follow `fetchWithOptions` pattern. Files: `app/crow-web/ui/src/api.ts`.
- [x] **5.2 CapacityPanel skeleton**: new `app/crow-web/ui/src/panels/CapacityPanel.tsx` rendered when `viewMode === Capacity`. Wired into `App.tsx` center panel. Files: `app/crow-web/ui/src/panels/CapacityPanel.tsx`, `app/crow-web/ui/src/App.tsx`.
- [x] **5.3 Rack/Node summary view**: hierarchical capacity summary with cluster-wide totals + per-instance detail. Data from `GET /api/diskdb/usage` (cluster merge). Files: `app/crow-web/ui/src/panels/CapacityPanel.tsx`.
- [x] **5.4 DiskGroup per-disk boxes**: per-disk boxes with green→amber→red busy gradient + % label. Files: `app/crow-web/ui/src/panels/CapacityPanel.tsx`.
- [x] **5.5 Disk zone grid**: canvas zone grid, each zone box gradient by busy%. Hover tooltip (zone id + usage %). Click drills into zone bitmap. Files: `app/crow-web/ui/src/panels/ZoneGrid.tsx`.
- [x] **5.6 Zone bitmap canvas**: canvas grid from `usage_bitmap`. Busy = red, free = green. Hover shows offset + state. Offscreen double-buffer (draw to offscreen, `drawImage` blit). Files: `app/crow-web/ui/src/panels/ZoneBitmap.tsx`.
- [x] **5.7 3s poll + double-buffer swap**: poll refetches focused view data; retain previous frame until new frame fully drawn, then swap. No flicker. Files: `app/crow-web/ui/src/panels/CapacityPanel.tsx`.
- [x] **5.8 ScannerPanel**: last `ScanSummary` grouped (drift/lag/corruption/owner) + "Run Scan" button. Files: `app/crow-web/ui/src/panels/ScannerPanel.tsx`.
- [x] **5.9 RecalcPanel**: "Run Recalc" → per-zone drift table; drifted rows highlighted with per-zone "Rebuild" action (`/api/diskdb/rebuild`). Files: `app/crow-web/ui/src/panels/RecalcPanel.tsx`.

## Phase 6 — Tests + verification

- [x] **6.1 E2E: Capacity view lifecycle**: switch to Capacity → Add Disk Group → Add Disk (batch of 3) → disks appear with `Up` status; batch with malformed id → 0 created; Remove/Move/SetStatus mutate + refresh; two status columns render. Files: `app/crow-web/ui/e2e/flows/50-capacity-diskdb.spec.ts`.
- [x] **6.2 E2E: Physical DiskDB sub-tree + Server context menu**: DiskDB Server renders under Node; right-click Server → Restart/Stop; AddNode auto-deploys both KV + DiskDB; Stop → Restart on same ports. Files: `app/crow-web/ui/e2e/flows/50-capacity-diskdb.spec.ts`.
- [ ] **6.3 E2E: Capacity canvas**: DiskGroup per-disk boxes gradient; Disk zone grid 80×80; Zone bitmap 181×181 (busy red, free green, hover tooltip); 3s poll no flicker. Files: `app/crow-web/ui/e2e/`.
- [ ] **6.4 E2E: ScannerPanel + RecalcPanel**: scan summary grouped; Run Scan triggers + shows running; recalc drift rows + Rebuild action. Files: `app/crow-web/ui/e2e/`.
- [x] **6.5 Run affected Rust tests**: `pixi run test-diskdb-client`, `pixi run clean-env && pixi run test-console-server`, `pixi run clean-env && pixi run test-console-shared`, `pixi run clean-env && pixi run test-console-cli`, `pixi run clean-env && pixi run test-diskdb`. All pass.
- [x] **6.6 Run UI tests**: `pixi run clean-env && pixi run test-console-ui` (Playwright, system browser). All 39 tests pass.

## Phase 7 — Merge + cleanup

- [ ] **7.1 Merge design into formal design doc**: fold `doc/working/design-capacity-view.md` into `doc/design/console/design-crow-console-ui.md` (view-mode, capacity panel) + `doc/design/diskdb/design-crow-diskdb.md` (console integration, write-back). Delete standalone design draft. Files: `doc/design/console/design-crow-console-ui.md`, `doc/design/diskdb/design-crow-diskdb.md`, `doc/working/design-capacity-view.md` (delete).
- [ ] **7.2 Cleanup**: delete `doc/backlog/R77-diskdb-console-cli.md` + its `backlog.md` entry; delete `doc/working/plan-capacity-view.md`. Commit cleanup. Files: `doc/backlog/backlog.md`, `doc/backlog/R77-diskdb-console-cli.md` (delete), `doc/working/plan-capacity-view.md` (delete).
- [ ] **7.3 Local CI**: `pixi run cargo fmt --all -- --check`, `pixi run cargo clippy --all-targets -- -D warnings`, all Test Commands (each separately). All must pass.

## File list

- `lib/crow-diskdb-client/src/client.rs` — +trigger_scan, +get_scan_status, +rebuild_zone_bitmap + tests
- `lib/crow-kv-client/src/hardware.rs` — +#[derive(Clone)]
- `app/crow-diskdb/src/liveness/keepalive.rs` — thread hw into background_zone_load + Init→Offline write-back
- `app/crow-diskdb/tests/` — write-back integration test
- `app/crow-web/src/diskdb.rs` — NEW: REST proxy /api/diskdb/*
- `app/crow-web/src/diskdb_lifecycle.rs` — NEW: diskdb deploy/restart/stop
- `app/crow-web/src/lifecycle.rs` — +batch disk add, +service_type on ServerEntry usage
- `app/crow-web/src/state.rs` — +diskdb_client field, pid keyed by (node, type)
- `app/crow-web/src/lib.rs` — wire new routes + mods
- `app/crow-web/tests/` — REST proxy + batch disk-add integration tests
- `lib/crow-console-shared/src/diskdb.rs` — NEW: ConsoleClient diskdb methods + serde models
- `lib/crow-console-shared/src/clients/console.rs` — diskdb method impls/re-exports
- `lib/crow-console-shared/src/config.rs` — +service_type on ServerEntry
- `lib/crow-console-shared/src/lifecycle.rs` — +ServiceType, +DeployRequest diskdb
- `lib/crow-console-shared/src/lib.rs` — +mod diskdb
- `lib/crow-console-shared/tests/` — diskdb client unit tests
- `app/crow-cli/src/commands/diskdb.rs` — NEW: crow diskdb subcommands
- `app/crow-cli/src/commands/mod.rs` — +mod diskdb
- `app/crow-cli/src/main.rs` — +Diskdb variant in Group enum + dispatch
- `app/crow-cli/tests/` — CLI integration tests
- `app/crow-web/ui/src/types/index.ts` — +Capacity ViewMode, rename Logical→KV
- `app/crow-web/ui/src/contexts/ViewModeContext.tsx` — 3-mode toggle
- `app/crow-web/ui/src/components/Tree.tsx` — +DiskGroup/Disk TreeNode types
- `app/crow-web/ui/src/shell/Sidebar.tsx` — Capacity tree + Physical DiskDB sub-tree
- `app/crow-web/ui/src/data/useCapacityTree.ts` — NEW: capacity tree polling hook
- `app/crow-web/ui/src/data/crowKvServers.ts` — servicesByNodeId helper (KV + DiskDB)
- `app/crow-web/ui/src/App.tsx` — buildMenuItems: Server case + Capacity branch; dialog state
- `app/crow-web/ui/src/panels/CapacityPanel.tsx` — NEW: canvas capacity viz
- `app/crow-web/ui/src/components/dialogs/AddDiskGroupDialog.tsx` — NEW
- `app/crow-web/ui/src/components/dialogs/AddDiskDialog.tsx` — NEW (batch)
- `app/crow-web/ui/src/components/dialogs/RemoveDiskDialog.tsx` — NEW
- `app/crow-web/ui/src/components/dialogs/RemoveDiskGroupDialog.tsx` — NEW
- `app/crow-web/ui/src/components/dialogs/MoveDiskDialog.tsx` — NEW
- `app/crow-web/ui/src/components/dialogs/SetDiskStatusDialog.tsx` — NEW
- `app/crow-web/ui/src/components/dialogs/DeployDiskdbDialog.tsx` — NEW
- `app/crow-web/ui/src/components/dialogs/AddNodeDialog.tsx` — +enableDiskDB auto-deploy
- `app/crow-web/ui/src/components/dialogs/index.ts` — barrel exports
- `app/crow-web/ui/src/api.ts` — diskdb runtime + lifecycle API functions
- `app/crow-web/ui/e2e/` — E2E tests (capacity lifecycle, canvas, scanner/recalc, physical DiskDB)

## Test checklist

**Unit (Rust)**:
- [ ] DiskdbClient: trigger_scan mock, get_scan_status has_run=false, rebuild unknown disk → Unreachable
- [ ] Console-shared: each diskdb method deserializes mock REST; error → typed error

**Integration (Rust)**:
- [ ] REST /instances returns registry data
- [ ] REST /usage no-dg merges two instances
- [ ] REST /usage?dg=&disk=&zone= returns ZoneUsage with usage_bitmap
- [ ] REST scan/recalc/compact/rebuild proxy through
- [ ] PUT /api/disks/:id/status writes + 404 unknown
- [ ] Batch disk add: 3 valid, 1 malformed → 0, duplicates → 400, group-0 fail → rollback
- [ ] CLI: all 7 subcommands against crow-web+diskdb; unknown disk-id errors
- [ ] Write-back: zone load fail → Offline in group 0 + runtime; stays Offline across sync ticks
- [ ] Write-back: Missing→Bad path updates group-0 status

**E2E (Playwright)**:
- [ ] Capacity: Add Disk Group → Add Disk batch → disks appear Up; malformed → 0; Remove/Move/SetStatus; two status columns
- [ ] Physical: DiskDB sub-tree renders; Server context menu Restart/Stop; AddNode auto-deploys both; Stop→Restart same ports
- [ ] Canvas: DiskGroup boxes gradient; Disk zone grid 80×80; Zone bitmap 181×181 (red/green, hover); 3s poll no flicker
- [ ] ScannerPanel: summary grouped; Run Scan + running state
- [ ] RecalcPanel: drift rows + Rebuild action
