<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Plan: KV Operator Panel (R7)

## Tasks

- [x] T1: App.tsx — replace `showSwagger` with `centerPanel` tri-state
- [x] T2: Header.tsx — add "KV" toggle button
- [x] T3: KvOperatorPanel.tsx — new panel: selector + action bar + results
- [x] T4: Inspector.tsx — remove KV tab
- [x] T5: Tests — updated E2E tests 00/09/10/11/24; unit tests pass; tsc + build clean
- [x] T6: Proto — add `start_after` field to `KvScanRequest`
- [x] T7: Engine — add `start_after` to `KvEngine::scan`, implement in `MemKv` and `CrowtreeEngine`
- [x] T8: Server — pass `start_after` through `kv_service` → `kv_store` → `learner` → engine
- [x] T9: Client — add `start_after` param to `CrowkvClient::scan`
- [x] T10: HTTP — add `start_after` query params to `KvScanQuery` in `web/src/kv.rs`
- [x] T11: Frontend API — add `startAfter` param to `kvScan` in `api.ts`
- [x] T12: Frontend UI — "Load more" button in `KvOperatorPanel` using `startAfter` + `truncated`
- [x] T13: Tests — unit test for start_after (conformance); E2E pending backend

## Task Detail

### T1: App.tsx — center panel tri-state

- Replace `const [showSwagger, setShowSwagger]` with
  `const [centerPanel, setCenterPanel] = useState<'topology'|'swagger'|'kv'>('topology')`
- Update Header props: `showSwagger` → `showKV`, `swaggerActive` →
  `centerPanel`, add `onToggleKV` and adapt `onToggleSwagger`.
- Update `<main>` render: switch on `centerPanel` — topology / swagger / kv.
- Pass `stores`, `groups`, `selectedEntity` to KvOperatorPanel.
- Inspector `marginRight` stays as-is (inspector still shows for entity
  details).

### T2: Header.tsx — add KV button

- Add a "KV" button next to "API" button, same styling.
- `onClick` toggles `centerPanel` between 'kv' and 'topology'.
- Icon: `Database` from lucide-react.

### T3: KvOperatorPanel.tsx — new panel

Props: `stores`, `groups`, `selectedEntity`, `readonly?`.

State:
- `storeId`, `groupId` (string, '' = All Groups)
- `scanPrefix`, `scanResults`, `scanTruncated`, `scanLoading`
- `selectedKeys: Set<string>`
- `getKey`, `getResult`
- `putKey`, `putValue`, `autoScan`
- `deleteKey`
- `demoCount`
- `error`, `confirmDialog`

Layout (flat, no tabs):
1. **Selector bar**: Store dropdown + Group dropdown (with "All Groups")
   + Scan prefix input + Scan button + Refresh button.
2. **Action bar**: Get / Put / Delete / Demo rows (compact, always visible).
3. **Results table**: checkbox, key, value, revision, inline delete, group
   column when All Groups.

Behavior:
- Store defaults to first store. Group defaults to first group in store.
- If `selectedEntity` is a logical Group, initialize store/group from it.
- Scan: single group → one `kvScan` call. All Groups → iterate groups,
  merge results, tag each item with group id.
- Get/Put/Delete: call `kvGet`/`kvPut`/`kvDelete` with current store+group.
  For All Groups + Put, pick a random group. For All Groups + Get/Delete,
  require a specific group selection (disable or prompt).
- Delete Prefix: scan with prefix, delete each key sequentially.
- Delete Selected: delete each checked key sequentially.
- Inline delete: single key delete with confirmation.
- Demo inject: loop `kvPut` for N keys with `demo_` prefix. All Groups →
  random distribution.
- Delete all demo: scan `demo_` prefix, delete each sequentially.

### T4: Inspector.tsx — remove KV tab

- Remove `KvPanel` import.
- Remove `'kv'` from `TabId` type.
- Remove `kvEnabled` logic and KV tab button.
- Remove KV tab content render.
- Remove `KvPanel` from module check in tabs.

### T5: Tests

Unit (Vitest):
- Store/group selector defaults to first store + first group.
- Store/group selector follows selectedEntity when logical Group selected.
- All Groups scan merges results from multiple groups.

E2E (Playwright, real backend):
- Open KV panel from header, select store+group, scan, verify table.
- Put a key, auto-scan, verify in results.
- Inline delete a row, verify removed.
- Select rows, delete selected, verify removed.
- Demo inject 100 keys, scan, verify count. Delete all demo, verify clean.
- All Groups scan, verify results from multiple groups.
- Inspector no longer shows KV tab.

### T6: Proto — add `start_after` to `KvScanRequest`

- `crowkv/src/rpc/proto/kv.proto`: add `bytes start_after = 8;` to
  `KvScanRequest`. Empty = start from beginning.
- Regenerate proto Rust code (`pixi run proto` or equivalent).

### T7: Engine — add `start_after` to scan

- `crowkv/src/kv/kv_engine.rs`: add `start_after: &[u8]` param to `scan`
  trait method.
- `crowkv/src/kv/mem_kv.rs`: change `map.range(prefix.to_vec()..)` to
  `map.range(start_after.to_vec()..)`, keep prefix filter. If
  `start_after` is empty, use `prefix.to_vec()..` as before (to avoid
  scanning non-matching keys before the prefix).
- `crowkv/src/kv/crowtree_engine.rs`: over-fetch with original prefix +
  limit, then filter out keys ≤ `start_after` in Rust before applying
  limit. No C++ change.

### T8: Server — pass `start_after` through

- `crowkv/src/paxos/learner.rs`: add `start_after: &[u8]` to
  `engine_scan`, pass to engine `scan`.
- `crowkv/src/cluster/px_kv_store.rs`: add `start_after: &[u8]` to
  `kv_scan`, pass to `engine_scan`.
- `crowkv/src/cluster/kv_store.rs`: add `start_after: &[u8]` to `kv_scan`
  trait method.
- `crowkv/src/rpc/kv_service.rs`: extract `start_after` from
  `KvScanRequest`, pass to `kv_scan`.

### T9: Client — add `start_after` to `CrowkvClient::scan`

- `crowkv-client/src/client.rs`: add `start_after: &[u8]` param to
  `scan()`, set `KvScanRequest.start_after`.

### T10: HTTP — add `start_after` query params

- `crowkv-console/web/src/kv.rs`: add `#[serde(default)] start_after:
  Option<String>` and `start_after_hex: Option<String>` to `KvScanQuery`.
  Decode and pass to `client.scan()`.

### T11: Frontend API — add `startAfter` to `kvScan`

- `crowkv-console/web/ui/src/api.ts`: add optional `startAfter?: string`
  param to `kvScan()`, append to query string.

### T12: Frontend UI — "Load more" button

- `KvOperatorPanel.tsx`:
  - Track `scanStartAfter` state (string, reset on fresh scan).
  - After scan, if `truncated`, show "Load more" button below results.
  - Click → call `kvScan` with `startAfter` = last key in current
    results. Append new rows.
  - For All Groups: track per-group `startAfter` and `truncated` state.
    "Load more" fetches next batch from all groups that still have
    `truncated = true`.
  - Changing store/group/prefix resets to fresh scan.

### T13: Tests

- E2E: inject 150 keys, scan (limit 100), verify "Load more" appears,
  click, verify 50 more keys appended.
- Unit: scan with `start_after` returns keys strictly greater than
  `start_after` (MemKv test).

## File List

- `crowkv-console/web/ui/src/App.tsx` — modify
- `crowkv-console/web/ui/src/shell/Header.tsx` — modify
- `crowkv-console/web/ui/src/panels/KvOperatorPanel.tsx` — new
- `crowkv-console/web/ui/src/panels/KvPanel.tsx — delete (logic absorbed)
- `crowkv-console/web/ui/src/shell/Inspector.tsx` — modify
- `crowkv-console/web/ui/src/api.ts` — add `startAfter` param
- `crowkv/src/rpc/proto/kv.proto` — add `start_after` field
- `crowkv/src/rpc/kv_service.rs` — pass `start_after` through
- `crowkv/src/cluster/kv_store.rs` — add `start_after` to trait
- `crowkv/src/cluster/px_kv_store.rs` — pass `start_after` to engine
- `crowkv/src/paxos/learner.rs` — pass `start_after` to engine
- `crowkv/src/kv/kv_engine.rs` — add `start_after` to trait
- `crowkv/src/kv/mem_kv.rs` — use `start_after` as range lower bound
- `crowkv/src/kv/crowtree_engine.rs` — filter by `start_after` post-scan
- `crowkv-client/src/client.rs` — add `start_after` param
- `crowkv-console/web/src/kv.rs` — add `start_after` query params

## Test Checklist

- [ ] Vitest: selector defaults
- [ ] Vitest: selector follows selection
- [ ] Vitest: All Groups merge
- [ ] E2E: scan + table display
- [ ] E2E: put + auto-scan
- [ ] E2E: inline delete
- [ ] E2E: multi-select delete
- [ ] E2E: demo inject + cleanup
- [ ] E2E: All Groups scan
- [ ] E2E: Inspector no KV tab
- [ ] E2E: pagination — Load more
- [ ] Unit: scan with start_after
