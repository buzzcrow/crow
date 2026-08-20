<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Capacity View — Level-Specific Center Panel (R85 draft)

## Intro

This draft covers the capacity view center panel rendering one
level-appropriate visualization per selected entity, instead of the
current behavior where every selection (Disk / DiskGroup / Node / Rack /
Datacenter) renders the same nested per-instance → DG → disk → zone
drill-down.

Root design: `doc/design/console/design-crow-console-ui.md` §16
"Capacity Panel (Canvas Visualization)". That section already specifies
the per-level content (Rack/Node → hierarchical summary; DiskGroup →
per-disk boxes; Disk → zone grid; Zone → zone bitmap). This draft is the
implementation detail for actually wiring `CapacityPanel.tsx` to branch
its render by `selectedEntity.type`. Architecture decisions and
rationale are in the root design; this doc does not repeat them.

Already landed: `ZoneGrid.tsx`, `ZoneBitmap.tsx` (canvas + double-buffer),
`ScannerPanel.tsx`, `RecalcPanel.tsx`, the `useCapacityTree` data hook
(cluster merge via `getDiskdbUsage()` + `getHardwareCapacity()`), and the
sidebar capacity tree (`Sidebar.tsx` viewMode === Capacity:
Datacenter → Rack → Node → DiskGroup → Disk).

## Current behavior (the bug)

`CapacityPanel.tsx` receives `selectedEntity` and only uses it to:
- set `scopeLabel` (title text: "Rack 3", "DG-7", "Disk abcd1234…"),
- filter `filteredDgs` by rack/node/dg/disk id,
- recompute the 3 totals cards (Total Capacity / Busy / Free).

The body render (lines ~341–383) is **unconditional**: it always emits
`ScannerPanel` + a per-instance loop, each instance listing its owned
DGs as `DiskGroupRow` → `DiskRow` → `ZoneGrid` → `ZoneBitmap`. So
clicking a Disk, a DG, a Node, a Rack, or the Datacenter root all show
the same nested drill-down, just filtered and retitled. There is no
level-specific visualization; the design §16 per-level content is not
implemented.

## 1. Level dispatch

`CapacityPanel` renders exactly one branch based on
`selectedEntity?.type`. A small `CapacityScope` enum is derived from the
selection so the rest of the component switches on a closed set:

```ts
type CapacityScope =
  | 'Cluster'    // Datacenter or no selection
  | 'Rack'
  | 'Node'
  | 'DiskGroup'
  | 'Disk';
```

Zone is **not** a sidebar entity (`EntityType` has no `Zone`); it is an
in-panel click state inside the Disk view. Zone selection stays as local
state in the Disk view component, not in `SelectionContext`.

The header (title + 3 totals cards) stays common to all scopes — it
already adapts via `scopeLabel` / `totalCapacity` / `totalBusy` /
`totalFree`. Only the body below the header branches.

## 2. Per-scope body content

### 2.1 Cluster (Datacenter / no selection)

Cluster-wide overview. Body = per-rack breakdown.

- One row per rack (from `hardwareCapacity.racks`, fallback: derive
  from `usage.disk_groups` grouped by `rack_id`).
- Each rack row: rack label, DG count, node count, capacity/busy/free
  bar + `%`. Busy/free from joining `usage.disk_groups` by `rack_id`.
- Clicking a rack row calls `selectEntity({ type: 'Rack', id, viewMode:
  Capacity })` (drill-down via the same selection path the sidebar
  uses).
- `ScannerPanel` (cluster-wide scan status summary + trigger) renders
  here only — it is the only place the cluster-wide scan summary
  (zones scanned, ghost busy/free, integrity counts) is shown. The
  per-DG scan/recalc *buttons* are not here; they live at the Disk
  level (§2.5).
- No per-disk boxes, no zone grid.

### 2.2 Rack

Rack-scoped summary. Body = per-node breakdown within the rack.

- One row per node in the rack (from `hardwareCapacity.nodes` filtered
  by `rack_id`, fallback: `usage.disk_groups` grouped by `node_id`).
- Each node row: node label, DG count, capacity/busy/free bar + `%`.
- Clicking a node row → `selectEntity({ type: 'Node', ... })`.
- No scan/recalc actions (those are Disk-level only, §2.5).

### 2.3 Node

Node-scoped summary. Body = per-DG breakdown.

- One row per DG on the node (filtered `filteredDgs`).
- Each DG row: DG label, disk count (array icon + count, **not**
  per-disk boxes), capacity/busy/free bar + `%`.
- Clicking a DG row → `selectEntity({ type: 'DiskGroup', ... })`.
- No scan/recalc actions here — those are shown only at the Disk level
  (see §2.5), targeting the disk's parent DG.

### 2.4 DiskGroup

Per-disk boxes. Body = the disk box grid only.

- Each disk = one box with busy% gradient fill (green → amber → red,
  reusing the existing `busyColor` from `ZoneGrid.tsx` — extract to a
  shared util) + inline `%` label + `title` tooltip (disk id + busy%).
- Clicking a disk box → `selectEntity({ type: 'Disk', parentIds: {
  rack_id, node_id, disk_group_id, disk_id } })`.
- No scan/recalc actions here — those are shown only at the Disk level
  (see §2.5), targeting the disk's parent DG.

### 2.5 Disk

Zone grid. Body = the zone grid for the selected disk + zone bitmap
drill-down.

- Header line: disk id, type, status, zone count, capacity.
- **All disk-scoped actions inline in the disk header**, grouped:
  - Scan (targets the disk's parent DG via `triggerDiskdbScan(dgId)`)
  - Recalc (targets the parent DG via `recalcDiskdbUsage(dgId)`)
  - Compact (per-disk, `compactDiskdbZones(diskId)`)
  - Rebuild (per-disk, `rebuildDiskdbZoneBitmap(diskId)`)
  - Up / Down (per-disk, `setDiskStatus(diskId, status)`)
  Scan and recalc are DG-scoped operations (the API takes an optional
  DG id), but they are surfaced here — at the focused disk — rather
  than at the Node or DiskGroup level views, because the operator
  workflow is disk-centric: you pick a disk, then scan/recalc its DG,
  then compact/rebuild the disk itself.
- `RecalcPanel` (per-DG recalc result display) renders here, scoped to
  the disk's parent DG.
- `ZoneGrid` (existing canvas component) renders the disk's
  `zone_usages` as a square grid (side = ceil(sqrt(zone_count))) with
  green→amber→red by busy%.
- "Jump to zone #" input (numeric) for direct navigation — 7000 zones
  cannot be a dropdown. Selecting via input sets the in-panel
  `selectedZone` and scrolls the grid selection highlight.
- Hover → tooltip (zone id + usage%) — already in `ZoneGrid`.
- Click a zone box → set in-panel `selectedZone` → render `ZoneBitmap`
  below the grid for that zone.
- `ZoneBitmap` uses `selectedZone.usage_bitmap`. See §3 for the
  on-demand fetch question.

### 2.6 Zone (in-panel, not a sidebar entity)

Zone bitmap. Rendered inside the Disk scope body when `selectedZone` is
set (see §2.5). No separate scope branch.

## 3. Zone bitmap data source

The root design says the bitmap is on-demand only
(`GET /api/diskdb/usage?dg=&disk=&zone=`), omitted at disk level. The
current code reads `selectedZone.usage_bitmap` straight from the cluster
merge response (`disk.zone_usages[i].usage_bitmap`), which works only if
the backend includes the bitmap in the disk-level payload.

Two options:
- **A. Keep current** — assume the cluster/disk merge includes
  `usage_bitmap` for every zone. Simple, no extra fetch, but the
  payload is large (6400 zones × bitmap each) and contradicts the
  design's "on-demand only".
- **B. On-demand fetch** — when a zone is clicked, call
  `getDiskdbUsage(dg, disk, zoneIndex)` and use that response's
  `usage_bitmap`. Matches the design; smaller steady-state payload;
  adds one fetch per zone click (acceptable — zone click is rare).

**Decision: B.** Verified against the backend:
`app/crow-diskdb/src/service/diskdb_service.rs` `zone_usage_to_proto`
sets `usage_bitmap: None` by default; the bitmap is attached only in
the zone-level shape (disk_id + zone_index both set, line 410-420).
The disk-level shape (`disk.rs` `zone_usages()`) returns brief
`ZoneUsage` with no bitmap. So the current UI code reading
`selectedZone.usage_bitmap` from the cluster merge always gets
`undefined` — the bitmap is never populated at disk level. On-demand
fetch is required.

The `getDiskdbUsage(dg, disk, zone)` API already exists (`api.ts` line
902). Add a small `useZoneBitmap(dg, disk, zone)` hook that fetches on
`selectedZone` change and caches the last result. Polling (§4)
refetches the focused zone bitmap every 3 s.

## 4. Polling

Keep the existing 3 s poll in `CapacityPanel`, but make the refetch
target the selected scope so the focused view stays fresh:
- Cluster / Rack / Node / DiskGroup → `useCapacityTree.refresh()`
  (cluster merge; the body filters client-side). No new endpoint.
- Disk → same cluster merge refresh (zone_usages refresh in place).
- Zone (in-panel) → if §3-B, refetch the focused zone bitmap.

The poll retains previous data until new data arrives (already the
hook's behavior). Canvas components retain the previous frame via
double-buffer on redraw.

## 5. Shared color util

`busyColor(pct)` currently duplicated in `ZoneGrid.tsx` (line 12) and
inlined in `CapacityPanel.tsx` (DiskGroupRow disk boxes, line 460).
Extract to `utils/capacity.ts` as `busyColor(pct: number): string` and
reuse in: DiskGroup disk boxes, ZoneGrid, and the new per-rack/per-node
bars (if bars use gradient fills). The 4-step thresholds (30/60/85/100)
stay.

## Scope

- `ui/src/panels/CapacityPanel.tsx` — rewrite the body render to branch
  by `CapacityScope`; extract per-scope subviews (ClusterView, RackView,
  NodeView, DiskGroupView, DiskView) either as inline functions in this
  file or as new files under `panels/capacity/` (see Module Structure).
  Keep the header + totals cards + action handlers.
- `ui/src/panels/ZoneGrid.tsx` — no change (reused by DiskView).
- `ui/src/panels/ZoneBitmap.tsx` — no change (reused by DiskView).
- `ui/src/utils/capacity.ts` — new: `busyColor`, `busyPct`, `formatBytes`
  moved here from `CapacityPanel.tsx` / `ZoneGrid.tsx`.
- `ui/src/panels/capacity/ClusterView.tsx` — new: per-rack breakdown.
- `ui/src/panels/capacity/RackView.tsx` — new: per-node breakdown.
- `ui/src/panels/capacity/NodeView.tsx` — new: per-DG breakdown.
- `ui/src/panels/capacity/DiskGroupView.tsx` — new: per-disk box grid.
- `ui/src/panels/capacity/DiskView.tsx` — new: zone grid + zone bitmap
  + jump-to-zone + per-disk actions.
- `ui/src/data/useZoneBitmap.ts` — new (only if §3-B): on-demand zone
  bitmap fetch + cache.

No backend, proto, or CLI changes. All data already comes from
`getDiskdbUsage()` + `getHardwareCapacity()` (plus the on-demand zone
fetch which uses an existing endpoint).

## Complexity

Low. No new RPC, no proto, no backend. The work is a UI restructure:
split one unconditional render into five scope branches, reusing the
existing `ZoneGrid` / `ZoneBitmap` / `ScannerPanel` / `RecalcPanel`
components and the existing data hook. The only new logic is the
per-rack / per-node aggregation (group `hardwareCapacity` entries by
`rack_id` / `node_id`, sum capacity/busy) and the optional on-demand
zone bitmap fetch. Main risk is regression in the per-disk action
handlers (Scan/Recalc/Compact/Rebuild/Up/Down) which currently live on
`DiskGroupRow`/`DiskRow` and must move to `DiskView` without losing the
`actionLoading` gating.

## Test Design

E2E only (UI restructure; no pure-logic unit tests worth adding beyond
`busyColor`/`busyPct` which are trivial). The existing
`ui/e2e/flows/50-capacity-diskdb.spec.ts` is the base; extend it.

- **Cluster view** — load capacity view with no selection; assert
  per-rack rows render, each with a capacity bar and DG count; click a
  rack row → center panel switches to Rack view (per-node rows).
- **Rack view** — select a rack (sidebar); assert per-node rows; click a
  node row → Node view (per-DG rows).
- **Node view** — select a node; assert per-DG rows with disk count;
  assert **no** scan/recalc buttons present; click a DG row →
  DiskGroup view (per-disk boxes).
- **DiskGroup view** — select a DG; assert one box per disk with busy%
  label and gradient fill; assert **no** scan/recalc buttons present;
  click a disk box → Disk view (zone grid).
- **Disk view** — select a disk; assert `ZoneGrid` canvas renders with
  the disk's zone count; hover a zone → tooltip shows zone id + %;
  click a zone → `ZoneBitmap` renders below; "jump to zone #" input
  focuses a zone by index.
- **Zone bitmap on-demand (§3-B)** — click a zone; assert a
  `getDiskdbUsage?dg=&disk=&zone=` network call fires; bitmap canvas
  renders from that response.
- **Disk-level actions** — in Disk view, all five action buttons
  present: Scan (fires `triggerDiskdbScan(parentDgId)`), Recalc (fires
  `recalcDiskdbUsage(parentDgId)`), Compact, Rebuild, Up, Down. Each
  fires its API call and shows toast on success/failure. Assert Scan
  and Recalc are **absent** in Node and DiskGroup views.
- **Polling** — with a disk selected, wait 3 s; assert the zone grid
  redraws from refreshed data without flicker (canvas retains previous
  frame).

## Module Structure

```
ui/src/
  panels/
    CapacityPanel.tsx          # header + totals + scope dispatch + actions
    ZoneGrid.tsx               # unchanged (reused)
    ZoneBitmap.tsx             # unchanged (reused)
    ScannerPanel.tsx           # unchanged (reused, Cluster scope only — scan status + trigger)
    RecalcPanel.tsx            # unchanged (reused, Disk scope only — recalc result for parent DG)
    capacity/
      ClusterView.tsx          # per-rack breakdown
      RackView.tsx             # per-node breakdown
      NodeView.tsx             # per-DG breakdown
      DiskGroupView.tsx        # per-disk box grid
      DiskView.tsx             # zone grid + zone bitmap + jump-to-zone
  utils/
    capacity.ts                # busyColor, busyPct, formatBytes
  data/
    useZoneBitmap.ts           # on-demand zone bitmap fetch (§3-B)
```

## Open Questions

1. **Datacenter as a selectable scope** — the sidebar capacity tree has
   a Datacenter root node. Clicking it sets `selectedEntity.type =
   'Datacenter'`, which this draft maps to the Cluster scope. Confirm
   that is intended (Datacenter = cluster root, no per-datacenter
   aggregation since there is one datacenter). If multi-datacenter is
   ever added, Cluster scope becomes per-datacenter and a new
   "above-cluster" scope is needed.

   (Resolved: zone bitmap fetch — §3 now uses on-demand fetch, option
   B, verified against the diskdb backend.)
