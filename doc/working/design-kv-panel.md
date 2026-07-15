<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Design: KV Operator Panel (R7)

## Problem

The web console has no dedicated KV operation surface. The current KV
interaction is a tab inside the right-side Inspector panel, which:

- Only appears when a logical Group is selected.
- Has no store/group selector — the target is implicit from the selection.
- Provides scan/get/put/delete but no bulk operations or demo helpers.
- Is cramped in the 320px inspector width, making key list and value
  display hard to use.

The user wants a full-width center panel for KV operations, similar to how
Swagger is a center panel today.

## Current Architecture

- **3-pane shell**: Sidebar (tree) + Center (TopologyCanvas or SwaggerPanel)
  + Inspector (Details/KV/Activity tabs).
- **ViewMode toggle** in Header: Physical / Logical. This drives the sidebar
  tree and topology canvas.
- **Center panel switching**: `showSwagger` boolean in App.tsx toggles between
  TopologyCanvas and SwaggerPanel. No third option exists.
- **KV in Inspector**: `KvPanel.tsx` renders scan/get/put/delete tabs. It
  receives `storeId` and `groupId` from the selected entity's `parentIds`.
- **Backend**: HTTP routes at `/api/stores/:sid/groups/:gid/kv/{get,scan,put,delete}`.
  gRPC `batch_write` exists but has no HTTP endpoint.
- **API client** (`api.ts`): `kvGet`, `kvPut`, `kvDelete`, `kvScan` functions.
  No batch write client function.

## Proposed Approach

### 1. Add "KV" as a third center-panel mode

Extend the center panel switching from `showSwagger` to a tri-state:

- **Topology** (default) — React Flow canvas.
- **Swagger** — embedded API docs (existing).
- **KV Operator** — new full-width KV panel.

Add a "KV" toggle button in the Header next to the existing "API" button.
Use a `centerPanel` state (`'topology' | 'swagger' | 'kv'`) in App.tsx
replacing the `showSwagger` boolean.

### 2. KV Operator Panel layout

Single-page flat layout — no tabs. Action bar on top, scan results table
below. This avoids tab-switching overhead: the user can scan, see results,
and act (put/get/delete) without leaving the page.

```
┌─ KV Operator ──────────────────────────────────────────────────────┐
│ [Store ▾] [Group ▾]              [Scan] prefix:[____] [Refresh]    │
├────────────────────────────────────────────────────────────────────┤
│ ┌─ Action bar ────────────────────────────────────────────────────┐│
│ │ Get: [key____] [Get]     value: …  rev: …  [Copy]              ││
│ │ Put: [key____] [value____] [Put]   ☐ auto-scan after put       ││
│ │ Del: [key____] [Delete]  [Delete Prefix]  [Delete Selected (N)]││
│ │ Demo: Inject [100___] demo keys  [Inject]  [Delete all demo]   ││
│ └──────────────────────────────────────────────────────────────────┘│
├────────────────────────────────────────────────────────────────────┤
│  Key          Value           Revision    [☐] [🗑]                │
│  ───────────────────────────────────────────────────────────────   │
│  user_key_1   hello           42          [☐] [🗑]                │
│  demo_key_001 demo_val_001    43          [☐] [🗑]                │
│  ...                                                               │
│  (N results, truncated?)                                           │
└─────────────────────────────────────────────────────────────────────┘
```

**Store/Group selector** (top bar):
- Store dropdown lists all stores, defaults to the first store.
- Group dropdown lists groups in the selected store, plus an **"All
  Groups"** option.
- When "All Groups" is selected, scan iterates over every group in the
  store and merges results (labeled by group). Inject randomly distributes
  keys across groups.
- Defaults to the first store + first group, or to the currently selected
  entity if it's a logical Group.
- Changing store/group clears scan results and action state.

### 3. Action bar

All CRUD operations are inline in the action bar, always visible:

**Get**:
- Key input + Get button. Result (value, revision) displayed inline to
  the right. Copy button on the value.

**Put**:
- Key + Value inputs + Put button. "Auto-scan after put" checkbox —
  triggers scan immediately after a successful put.

**Delete**:
- Key input + Delete button with confirmation dialog.
- "Delete Prefix" — scans with the given prefix, then deletes all
  matching keys one by one (no batch delete endpoint exists). Confirmation
  dialog shows count.
- "Delete Selected (N)" — deletes all rows checked in the results table.
  N is the current selection count. Confirmation dialog.

**Demo** (labeled "Demo" with a note: keys use `demo_` prefix):
- "Inject N demo keys" — input count (default 100), generates keys
  `demo_key_001` ... `demo_key_N` with values `demo_val_001` ...
  `demo_val_N`. Sequential `kvPut` calls. When Group = All Groups, keys
  are randomly distributed across groups.
- "Delete all demo keys" — scans for `demo_` prefix across the selected
  scope, deletes all matching keys one by one. Confirmation dialog.

### 4. Results table

- Columns: checkbox, key, value, revision, inline delete button (🗑).
- Row click → loads key into the Get input.
- Checkbox column for multi-select → feeds "Delete Selected (N)".
- Inline delete (🗑) per row — single key delete with confirmation.
- Truncation indicator if scan results hit the limit.
- When Group = All Groups, an extra "Group" column shows which group each
  key belongs to.

### 5. Remove KV tab from Inspector

Remove the KV tab from `Inspector.tsx`. The Inspector keeps Details and
Activity tabs only. This avoids duplication — all KV operations live in the
center panel.

### 6. Scan pagination (start_after token)

**Problem**: The scan endpoint returns at most `limit` (default 100) items
with a `truncated` flag, but offers no way to fetch the next page. The user
must re-scan with a higher limit or a narrower prefix — impractical for
large keyspaces.

**Design**: Add a `start_after` parameter to the scan API, analogous to S3
ListObjectsV2's `start-after`. The caller passes the last key from the
previous batch; the engine returns keys strictly greater than `start_after`
that still match the prefix. No total count is needed — the UI uses
`truncated` to show a "Load more" button.

**Protocol changes** (all layers, all optional/default-empty):

- **gRPC** `KvScanRequest`: add `bytes start_after = 8;` (field 8, next
  available after `read_mode = 7`). Empty means "start from the beginning".
- **Rust engine trait** `KvEngine::scan`: add `start_after: &[u8]` parameter.
- **`MemKv`**: change `map.range(prefix.to_vec()..)` to
  `map.range(start_after.to_vec()..)` and keep the prefix filter. This is
  correct because BTreeMap range is inclusive-start exclusive-end in key
  order; `start_after` gives us the exclusive lower bound.
- **`CrowtreeEngine`**: pass `start_after` through to the FFI layer. The
  C++ `Crowtree::scan` needs a new `start_after` parameter (or a wrapper
  that filters post-scan — less efficient but simpler for a first cut).
  **Decision**: For the first implementation, `CrowtreeEngine` will
  over-fetch (scan with the original prefix, then filter out keys ≤
  `start_after` in Rust before applying the limit). This avoids C++ changes.
  If the prefix range is large and `start_after` is deep, this is
  inefficient — a follow-up can push `start_after` into the C++ engine.
- **HTTP** `KvScanQuery`: add `#[serde(default)] start_after: Option<String>`
  and `start_after_hex: Option<String>` (mirrors the prefix/prefix_hex
  pattern).
- **HTTP response** `KvScanResponseView`: no change — already has
  `truncated`.
- **Client** `crowkv-client`: `scan()` gains a `start_after: &[u8]` param.
- **Frontend** `api.ts`: `kvScan()` gains an optional `startAfter` param.
- **Frontend** `KvOperatorPanel`: after a scan, if `truncated`, show a
  "Load more" button. Clicking it calls `kvScan` with `startAfter` set to
  the last key in the current results. New rows are appended, not replaced.
  "Load more" is per-group for All Groups mode (each group tracks its own
  last key and truncated state).

**UI interaction**:
- Initial scan: `startAfter` = empty, `limit` = 100.
- "Load more" button appears when `truncated` is true.
- Click → fetch next batch with `startAfter` = last key of current batch.
- Results accumulate (append, not replace).
- Changing store/group/prefix resets to a fresh scan.

### 7. ViewMode interaction

The KV Operator panel is independent of ViewMode (Physical/Logical). It
shows store/group selectors regardless of which hierarchy view is active.
This is intentional — KV operations are always logical (store/group), even
when the topology canvas shows the physical view.

## Files to Change

- `web/ui/src/App.tsx` — replace `showSwagger` with `centerPanel` state;
  add KV panel rendering; pass store/group data.
- `web/ui/src/shell/Header.tsx` — add "KV" toggle button.
- `web/ui/src/panels/KvOperatorPanel.tsx` — **new** — full center panel
  with store/group selector, action bar, and results table.
- `web/ui/src/panels/KvPanel.tsx` — logic absorbed into KvOperatorPanel;
  file can be deleted or kept as a thin wrapper if reused elsewhere.
- `web/ui/src/shell/Inspector.tsx` — remove KV tab.
- `web/ui/src/api.ts` — add `startAfter` param to `kvScan`.
- `crowkv/src/rpc/proto/kv.proto` — add `start_after` field to
  `KvScanRequest`.
- `crowkv/src/rpc/kv_service.rs` — pass `start_after` through to
  `kv_scan`.
- `crowkv/src/cluster/kv_store.rs` — add `start_after` to `kv_scan`
  trait method.
- `crowkv/src/cluster/px_kv_store.rs` — pass `start_after` to
  `engine_scan`.
- `crowkv/src/paxos/learner.rs` — pass `start_after` to engine `scan`.
- `crowkv/src/kv/kv_engine.rs` — add `start_after` to `scan` trait
  method.
- `crowkv/src/kv/mem_kv.rs` — use `start_after` as range lower bound.
- `crowkv/src/kv/crowtree_engine.rs` — filter by `start_after`
  post-scan (over-fetch + filter, no C++ change).
- `crowkv-client/src/client.rs` — add `start_after` param to `scan`.
- `crowkv-console/web/src/kv.rs` — add `start_after` query params to
  `KvScanQuery`, pass through to client.

## Alternatives Considered

- **Keep KV in Inspector, add a second center panel**: rejected. Two KV
  surfaces is confusing. The Inspector is too narrow for comfortable KV
  browsing.
- **Make KV a ViewMode (Physical/Logical/KV)**: rejected. ViewMode drives
  the sidebar tree and topology canvas; KV doesn't need a tree or canvas.
  Mixing it into ViewMode conflates hierarchy selection with panel
  selection.
- **Modal/dialog for KV**: rejected. A modal is too restrictive for
  scan-and-delete workflows; the user needs to see results alongside
  actions.

## Acceptance Test Plan

- E2E: open KV panel from header, select store+group, scan keys, verify
  results displayed in table with revision.
- E2E: put a key, auto-scan, verify it appears in results.
- E2E: select rows in results, delete selected, verify removed.
- E2E: inline delete per row, verify removed.
- E2E: demo inject 100 keys, verify count in scan; delete all demo keys,
  verify cleaned up.
- E2E: select All Groups, scan, verify results from multiple groups.
- E2E: Inspector no longer shows KV tab.
- E2E: inject 150 keys, scan (limit 100), verify "Load more" button
  appears, click it, verify remaining 50 keys appended.
- Unit: store/group selector defaults to first store+group when no entity
  selected.
- Unit: store/group selector follows selected entity when a logical Group
  is selected.
- Unit: All Groups scan merges results from all groups in the store.
- Unit: scan with start_after returns keys strictly greater than
  start_after.
