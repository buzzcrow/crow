<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R77: diskdb — Console + CLI Integration (Diskdb Runtime Console: REST Proxy, Web UI Panel + Disk Lifecycle UI, CLI Commands)

**Problem**: R70–R76 implement the core diskdb server (allocation,
recovery, metrics, scanning, health probing) with a full gRPC surface
(`DiskdbService`), and R81 lands the console-side disk/disk-group
**lifecycle** (group-0 sysdata writes via `HardwareClient`):
`crow-web` REST handlers for disk-group/disk add/remove/move and
`crow-cli` `disk` / `disk-group` commands. What is still missing:

- **No runtime data path to the console.** `QueryCapacityStats`
  (disk-group/disk/zone drill-down), `GetScanStatus`/`TriggerScan`,
  `RecalcDiskUsage`, `CompactZone`, and `RebuildZoneBitmap` are gRPC
  only. `crow-web` has no REST proxy for them, so operators cannot see
  capacity/busy/free, zone usage, scan results, or drift from the web
  UI or CLI.
- **No web UI for diskdb at all.** The physical tree renders
  rack → node only; disk-groups and disks never appear. There is no
  usage dashboard, no zone busy/free visualization, no scanner status,
  and no dialogs for the already-landed lifecycle REST endpoints.
- **No CLI for runtime queries.** `crow disk`/`disk-group` cover
  lifecycle only; usage/zones/scan/recalc/compact/rebuild are not
  reachable from the command line.
- **`DiskdbClient` gaps.** `lib/crow-diskdb-client` (R74 §11) wraps
  allocate/free/query/recalc/compact, but not the scanner RPCs
  (`TriggerScan`, `GetScanStatus`) or `RebuildZoneBitmap`.

The root design marks console integration as a follow-up: §2 non-goal
"full console integration (web + CLI) as a follow-up"; §9 "per-zone —
dive into busy/free blocks (for the console zone visualization)"; §9
keepalive piggyback "the console reads this for cluster-wide overview".

**Design pointers**:
- Root: `doc/design/diskdb/design-crow-diskdb.md` §6 (hierarchy),
  §8 (status), §9 (space metrics, keepalive piggyback), §13 (D7).
- Sub-designs: `design-crow-diskdb-space-metrics.md` §4
  (`QueryCapacityStats` shapes), §8 (keepalive), §11 (client).
- Console: `doc/design/console/design-crow-console-ui.md` §3 (3-pane
  shell), §6.1 (center-panel mode pattern), §9 (polling strategy).
- No direct aioss analog for a zone block-array visualization — new
  work.

**Use scenarios**:
- An operator opens the console, switches to the Diskdb panel, and
  sees one card per diskdb instance (endpoint, owned disk-group count,
  heartbeat age, aggregate capacity/busy/free bar) — the "is diskdb
  healthy" view, served from group-0 keepalive data, no gRPC fan-out.
- The operator drills into a disk-group: capacity/busy/free bars,
  `allocatable_disk_count` vs `disk_count` (degraded if they differ),
  then per-disk rows. One disk looks hot; the operator clicks its zone
  strip and opens the zone block chart — a canvas grid of 16K units
  with busy blocks filled — to see the allocation pattern.
- A scan finishes; the scanner panel shows drift (ghost busy/free),
  uncompacted lag, and corruption counts. The operator clicks "Run
  Scan" to re-run, and watches the in-progress state.
- A recalc reports drift on zone 7 of disk X; the operator clicks
  "Rebuild" for that zone (v1 recalc is report-only; rebuild is the
  manual correction).
- The operator adds a disk: right-clicks the node in the physical
  tree, "Add Disk", fills the dialog (id, type, capacity, zone/unit
  sizes). The disk appears in the tree and, once the diskdb sync +
  zone load completes, shows up allocatable in the Diskdb panel.
- From the shell, the operator runs `crow diskdb usage --dg 3` and
  `crow diskdb scan-status` to script monitoring.

**Solution**:

**One-line summary**: expose diskdb runtime data through `crow-web`
REST + `crow diskdb` CLI subcommands, and add disk lifecycle UI
(tree nodes + dialogs) plus a Diskdb center panel (instance overview,
usage dashboard, zone block chart, scanner, recalc) to the web UI.

1. **DiskdbClient scanner/rebuild wrappers** — extend
   `lib/crow-diskdb-client/src/client.rs`:
   - Add `trigger_scan()`, `get_scan_status()`,
     `rebuild_zone_bitmap(disk_id, zone_index)` methods wrapping the
     R75/R76 RPCs (`TriggerScanRequest` → `ScanSummary` +
     `scan_in_progress`; `GetScanStatusRequest` → summary + `has_run`;
     `RebuildZoneBitmapRequest` → rebuilt zone count + busy/free
     units). These are admin/debug calls; route through the existing
     `with_retry` wrapper and the `dg_for_disk` routing (for rebuild).
     The proto + server handlers exist — only the client wrappers are
     missing.

2. **REST proxy for diskdb runtime** — new `app/crow-web/src/diskdb.rs`
   handlers + routes under `/api/diskdb/`, plus a `DiskdbClient` owned
   by `AppState` (built from the same `ServiceRegistryClient` the
   console already uses — mirror the `build_hardware_client` pattern in
   `app/crow-web/src/mgmt.rs`):
   - `GET /api/diskdb/instances` — all diskdb instances from the
     service registry (`read_all_diskdb_instances`): instance id,
     endpoint, `last_heartbeat_ms`, `owned_dg_ids`, and the keepalive
     `group_usages` summaries. Cluster overview without touching gRPC.
   - `GET /api/diskdb/usage?dg=<id>&disk=<disk_id>&zone=<zi>` —
     `QueryCapacityStats` drill-down (all params optional). When `dg`
     is omitted, iterate all registered instances and merge the
     responses for cluster-wide totals — `DiskdbClient`'s
     `query_capacity_stats(0)` routes to one instance only, so the
     merge lives here.
   - `GET /api/diskdb/scan-status` — `GetScanStatus`.
   - `POST /api/diskdb/scan` — `TriggerScan`.
   - `POST /api/diskdb/recalc` — `RecalcDiskUsage` (optional `dg` in
     the body).
   - `POST /api/diskdb/compact` — `CompactZone` (disk_id + optional
     zone_indices; empty = all zones).
   - `POST /api/diskdb/rebuild` — `RebuildZoneBitmap` (disk_id +
     optional zone_index; absent = all zones).
   - `PUT /api/disks/:disk_id/status` — set a disk's `HwStatus` via
     `HardwareClient.set_disk_status` (needed by the Set-Status dialog;
     no such endpoint exists today — only add/remove/move).

3. **Console-shared diskdb runtime models + client** — extend
   `lib/crow-console-shared/src/`:
   - New `diskdb.rs` module with diskdb-specific methods on
     `ConsoleClient`: `list_diskdb_instances()`, `query_diskdb_usage(
     dg/disk/zone)`, `get_scan_status()`, `trigger_scan()`, `recalc()`,
     `compact()`, `rebuild()`, `set_disk_status()` — all calling the
     REST endpoints above.
   - Add serde model types (mirrors of the proto responses): 
     `DiskdbInstanceInfo`, `DiskGroupUsageSummary`, `DiskGroupUsage`,
     `DiskUsage`, `ZoneUsage`, `ScanSummary`, `RecalcResult`,
     `CompactionResult`.

4. **Web UI — disk lifecycle (hybrid tree)** — extend
   `app/crow-web/ui/src/`:
   - Physical tree renders `DiskGroup` and `Disk` nodes under `Node`
     (extend the physical-tree data hooks to fetch the existing
     `GET /api/nodes/:id/disk-groups` + `.../disks` endpoints).
   - Context menu on `DiskGroup`/`Disk` nodes + dialogs following the
     `AddNodeDialog`/`ConfirmDeleteDialog` pattern: `AddDiskDialog`
     (id, type, capacity, zone/unit sizes), `RemoveDiskDialog`,
     `MoveDiskDialog` (target rack/node/disk-group), 
     `SetDiskStatusDialog` (HwStatus enum).
   - **Two-column status display** on disk rows: "Group-0" (the
     `DiskValue.status` from `HardwareClient.get_disk`, fetched via the
     existing `GET /api/nodes/:id/disk-groups/:dg_id/disks/:disk_id`
     endpoint) and "Runtime" (the effective status from diskdb's state
     machine, `DiskInfo.status` from `GetDiskInfo`/`QueryCapacityStats`
     via `/api/diskdb/usage`). Both use the existing `toUiHealth`-style
     mapping over `HwStatus`. The two columns let the operator see
     group-0 intent vs diskdb's live state — they diverge briefly
     between a sync tick applying a change and the next keepalive
     write-back, and permanently if a write-back fails.

5. **Web UI — Diskdb center panel (runtime)** — new
   `app/crow-web/ui/src/panels/DiskdbPanel.tsx` with a header toggle
   (`CenterPanelMode`), following the KV Operator panel pattern:
   - `DiskdbInstanceCards` — per-instance overview from
     `/api/diskdb/instances`: endpoint, owned dg count, heartbeat age,
     aggregate capacity/busy/free bar, degraded indicator
     (`allocatable_disk_count < disk_count` on any dg). Poll 5–10 s.
   - `DiskGroupUsageCard` / `DiskUsageTable` — usage dashboard from
     `/api/diskdb/usage`: per-dg stacked bars + status; per-disk rows
     (id, type badge, busy/free + mini-bar, active_zone_count /
     zone_count, status). Render per-disk/per-zone fields only — the
     wrapping `DiskGroupInfo` aggregates are zeroed at drill-down
     levels (space-metrics §4) and must not be displayed.
   - `ZoneSummaryStrip` — per-zone rows for a selected disk (index,
     capacity, busy% bar, busy/free block counts, `alloc_state`
     badge) from the disk-level brief entries; no bitmap fetched.
   - `ZoneBlockChart` — canvas grid of the zone's `usage_bitmap`,
     fetched only on zone drill-down (2 KB for 16K units; one
     `query_zone` call). Decode to a bitset, render as a square grid
     (side = ceil(sqrt(units)), 128×128 for 16K), busy = filled cell,
     free = empty track, hover tooltip with offset + state, legend.
     No `allocate_pos` marker — `ZoneUsage` has no such field (dropped
     by decision). Canvas, not SVG/DOM, for zones
     > 4K units; redraw only on data change.
   - `ScannerPanel` — last `ScanSummary` grouped: Drift (ghost_busy,
     ghost_free), Lag (uncompacted_lag), Corruption (corrupt_snapshots,
     corrupt_records), Owner (owner_mismatches), plus duration and
     skipped-zone counts. "Run Scan" button; poll
     `/api/diskdb/scan-status` every 2 s while `scan_in_progress`,
     else 30 s.
   - `RecalcPanel` — "Run Recalc" → per-zone table (matches/drift,
     live vs replayed busy blocks, snapshot slots, fallback_reason);
     drifted rows highlighted with a per-zone "Rebuild" action
     (`/api/diskdb/rebuild`) — labeled manual, since v1 recalc is
     report-only.

6. **CLI `crow diskdb` subcommands** — new
   `app/crow-cli/src/commands/diskdb.rs`, wired into the existing
   `Group` enum in `app/crow-cli/src/main.rs` as `Diskdb {
   #[command(subcommand)] verb: DiskdbVerb }`:
   - `diskdb status` — instance overview (`/api/diskdb/instances`).
   - `diskdb usage [--dg <id>] [--disk <id>] [--zone <zi>]` — runtime
     drill-down.
   - `diskdb scan` / `diskdb scan-status` — trigger / query.
   - `diskdb recalc [--dg <id>]`.
   - `diskdb compact <disk_id> [--zones <zi,...>]`.
   - `diskdb rebuild <disk_id> [--zone <zi>]`.
   - All route through `ConsoleClient` → `crow-web` → `DiskdbClient` →
     gRPC; no direct talk to `crow-diskdb`. This closes the D7 open
     question — `crow diskdb` subcommands, not sub-wrapper binaries —
     consistent with how R81 landed `crow disk`/`disk-group`.
     Lifecycle stays in the existing `disk` / `disk-group` commands;
     `diskdb` is runtime queries only.

7. **Group-0 status write-back — close the `Init → Offline` gap** —
   the diskdb sync loop writes detected status changes back to group 0
   via `write_back_disk_status` → `HardwareClient.set_disk_status`
   (`app/crow-diskdb/src/liveness/keepalive.rs`: `Suspect →
   Missing`/`Offline` at line 686/724, `Missing → Bad` at line 751).
   `Suspect` is intentionally local-only (line 650 — writing it back
   would trap the disk in `Suspect` with no path back to `Up`).
   Effective-status derivations in `reconcile_existing_disk` (line 582)
   and `recover_disk_to_up` (line 860) also need no write-back — group
   0 is the source in both cases.

   **The gap**: `background_zone_load` (line 956) transitions `Init →
   Offline` when zone loading fails for all strategies (line 1082,
   `all_ok = false`) **without writing back** to group 0. The function
   is a `tokio::spawn`'d background task that does not receive
   `HardwareClient` (and `HardwareClient` is not `Clone` today — it
   wraps `Arc<CrowkvClient>`, so making it `Clone` is trivial).
   Consequence: the operator adds a disk as `Up` in group 0, zone load
   fails, diskdb sets `Offline` locally, but group 0 still says `Up`.
   On the next sync tick, `reconcile_existing_disk` reads `Up` from
   group 0 → `effective = Up` → calls `recover_disk_to_up` →
   transitions to `Up` + runs compaction (no-op in v1) but **does not
   re-attempt zone loading** — the disk becomes `Up` and allocatable
   with empty/broken zones.

   R77 fixes:
   - Make `HardwareClient` `Clone` (it wraps `Arc<CrowkvClient>`).
   - Pass `hw` into `background_zone_load`; on `all_ok = false`, call
     `write_back_disk_status(rack_id, node_id, dg_id, &disk_id,
     Offline)` before the `Init → Offline` transition, so group 0
     reflects the failure and the next sync tick sees `Offline` (not
     `Up`) — no recovery-to-Up loop.
   - Verify (integration test) that a disk whose zone load fails ends
     up `Offline` in both group 0 and the runtime state machine, and
     stays `Offline` across sync ticks (no flip-flop to `Up`).
   - Also verify the existing write-back paths (`Missing`/`Offline`/
     `Bad` from `reconcile_absent_disk`) so the console's two-column
     status display is trustworthy end-to-end.

Flow diagram (web path shown; CLI routes through the same REST layer):

```
Operator
   │  (Diskdb panel / crow-cli)
   ▼
crow-web (Axum REST /api/diskdb/*)
   │   reads service registry directly for /instances
   ├─────────────────────────────────────────► ServiceRegistryClient (group 0)
   │                                              └─ keepalive group_usages
   ▼  (lib/crow-diskdb-client DiskdbClient)
crow-diskdb (gRPC DiskdbService)
   ├─ QueryCapacityStats ─► dg/disk/zone usage (+ usage_bitmap at zone level)
   ├─ GetScanStatus / TriggerScan
   ├─ RecalcDiskUsage ─► drift rows ─► RebuildZoneBitmap (manual correction)
   └─ CompactZone / allocate / free (unchanged)
```

Edge cases at a glance:
- Cluster overview without `dg` → web layer iterates all instances and
  merges; a dead instance yields a degraded card, not a failed page.
- Zone drill-down → bitmap is omitted at disk level (proto contract);
  the UI issues the zone-level query and renders summary rows first.
- Zeroed group aggregates at disk/zone drill-down → UI renders
  per-disk/per-zone fields only.
- Scan already running → `TriggerScan` returns `scan_in_progress`;
  UI shows running state, does not stack triggers.
- Recalc drift → report-only; "Rebuild" is a separate manual action
  and is labeled as such.
- Group-0 status vs runtime divergence (e.g. disk `Bad` detected by
  diskdb, group 0 still `Up`) → the panel shows both signals in two
  columns; the write-back (work item 7) reconciles them on the next
  sync tick. The `Init → Offline` zone-load-failure path is the one
  remaining write-back gap — R77 closes it. Brief divergence is
  expected between a state-machine transition and the next write-back;
  persistent divergence indicates a write-back failure (logged as a
  warning).
- Large zones (16K units) → canvas grid, no per-block DOM; bitmap
  fetched on demand only.

**Dependencies**:
- R70–R76 (proto, admin RPCs, allocate/free, `QueryCapacityStats` +
  `ZoneUsage` with `usage_bitmap`, scanner RPCs, `RebuildZoneBitmap`)
  — all landed.
- R74 §8 keepalive piggyback (`DiskdbExtra.group_usages`,
  `DiskGroupUsageSummary`) and §11 `DiskdbClient` — landed; R77 adds
  the missing scanner/rebuild wrappers.
- R81 (disk-group/disk lifecycle REST + `crow disk`/`disk-group` CLI +
  `DiskEntry`/`AddDiskBody`/`MoveDiskBody` in `crow-console-shared`)
  — landed; R77 builds the UI/dialogs on these handlers.
- Nothing depends on R77.

**Acceptance**:

**DiskdbClient wrappers**:
- `trigger_scan()` / `get_scan_status()` against a mock gRPC server →
  returns the last `ScanSummary` + in-progress/has-run flags; transient
  `Unavailable` → retried per `RetryConfig`. Unit test.
- `rebuild_zone_bitmap(disk_id, zone_index)` routes via
  `dg_for_disk` and returns rebuilt counts; unknown disk →
  `Unreachable`/`Rpc` error. Unit test.
- `pixi run test-diskdb-client` passes.

**REST proxy**:
- `GET /api/diskdb/instances` returns registry data (endpoint,
  heartbeat, owned dg ids, keepalive `group_usages`) for each
  registered instance. Integration test with a `crow-web` backed by a
  `crow-diskdb` + group 0.
- `GET /api/diskdb/usage` with no `dg` merges results across two
  instances (cluster totals = sum of per-instance responses).
  Integration test.
- `GET /api/diskdb/usage?dg=1&disk=D&zone=2` returns `ZoneUsage` with
  a non-empty `usage_bitmap`. Integration test.
- `POST /api/diskdb/scan`, `/recalc`, `/compact`, `/rebuild` proxy to
  the gRPC RPCs and return the response bodies. Integration test.
- `PUT /api/disks/:disk_id/status` writes `HwStatus` via
  `HardwareClient` and returns 404 for an unknown disk. Integration
  test.
- `pixi run test-console-server` passes.

**Console-shared client + models**:
- `ConsoleClient` diskdb methods deserialize each REST response into
  the new model types; error responses surface as typed errors. Unit
  test with a mock HTTP server.
- `pixi run test-console-shared` passes.

**CLI**:
- `crow diskdb status` / `usage` / `scan` / `scan-status` / `recalc` /
  `compact` / `rebuild` against a `crow-web` backed by a `crow-diskdb`
  return correct results; unknown disk-id errors surface from the REST
  layer. Integration test.
- `pixi run test-console-cli` passes.

**Web UI lifecycle**:
- Physical tree renders `DiskGroup` and `Disk` nodes under `Node`;
  right-clicking a node offers "Add Disk"; the dialog adds a disk via
  REST → group 0 → diskdb sync, and the disk appears in the tree with
  `Up` status and its zone count. E2E test.
- `RemoveDiskDialog`, `MoveDiskDialog`, `SetDiskStatusDialog` mutate
  via REST and refresh the tree. E2E test.
- Disk rows show two status columns — "Group-0" (from
  `HardwareClient.get_disk` via the disk detail endpoint) and
  "Runtime" (from `GetDiskInfo`/`QueryCapacityStats`); both render
  `HwStatus` via the existing status-pill mapping. E2E test.

**Web UI runtime panel**:
- Diskdb panel instance cards show registry data + aggregate
  capacity/busy/free bar; a dg with `allocatable_disk_count <
  disk_count` renders a degraded indicator. E2E test.
- Dg/disk usage dashboard renders bars from `/api/diskdb/usage`; zone
  strip shows counts + `alloc_state` badges without fetching bitmaps.
  E2E test.
- `ZoneBlockChart` renders a zone's `usage_bitmap` as a canvas grid:
  allocate blocks, open the zone, verify busy cells are filled and
  free cells are empty; hover shows offset + state. E2E test.
- `ScannerPanel` shows the last `ScanSummary` grouped by
  drift/lag/corruption/owner; "Run Scan" triggers a scan and the panel
  reflects the running state. E2E test.
- `RecalcPanel` shows per-zone drift rows; "Rebuild" on a drifted zone
  calls `/api/diskdb/rebuild` and refreshes. E2E test.
- Playwright E2E tests pass (system browser per AGENTS.md — never run
  `npx playwright install`); `pixi run test-console-ui` passes.
- `pixi run cargo fmt --all -- --check` and
  `pixi run cargo clippy --all-targets -- -D warnings` clean.

**Group-0 status write-back**:
- A disk going `Missing` then `Bad` on the diskdb side (simulated by
  removing its `DiskKey` from group 0) results in the group-0
  `DiskValue.status` being updated to `Missing` then `Bad` via
  `write_back_disk_status` → `HardwareClient.set_disk_status`; the
  console's "Group-0" column reflects the detected state on the next
  read. Integration test.
- A disk whose zone load fails in `background_zone_load` (both
  strategy 2 and strategy 1 fail) transitions `Init → Offline` AND
  writes `Offline` back to group 0 via `HardwareClient.set_disk_status`
  — group 0's `DiskValue.status` becomes `Offline`. The disk stays
  `Offline` across subsequent sync ticks (no flip-flop to `Up`).
  Integration test.
- The console's "Runtime" column shows the state-machine effective
  status, which leads the "Group-0" column by one sync tick (write-back
  is best-effort, async). Integration test.
- `pixi run test-diskdb` passes.

**Resolved decisions** (from review open questions):
- **Effective status vs displayed status** — the diskdb sync loop
  already computes `effective_status(node, group, disk)` per §8
  (`keepalive.rs:565`, `HwStateMachine::effective_status`). The UI
  shows **two columns**: "Group-0" (the raw `DiskValue.status` from
  group 0, fetched via `HardwareClient.get_disk`) and "Runtime" (the
  effective status from diskdb's state machine, via `GetDiskInfo`/
  `QueryCapacityStats`). This lets the operator see group-0 intent vs
  diskdb's live state — they diverge briefly between a state-machine
  transition and the next write-back.
- **Group-0 write-back** — the sync loop writes detected status
  changes back to group 0 for `Missing`/`Offline`/`Bad` via
  `write_back_disk_status` (`keepalive.rs:686/724/751`; `Suspect` is
  intentionally local-only per line 650). One gap remains:
  `background_zone_load`'s `Init → Offline` on zone-load failure
  (line 1082) does not write back — R77 closes it (work item 7). R77
  ensures the end-to-end flow works, not just the display — the
  console's "Group-0" column is trustworthy because the write-back
  reconciles it on the next sync tick.
