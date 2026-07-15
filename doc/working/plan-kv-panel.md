<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Plan: KV Operator Panel (R7)

## Tasks

- [x] T1: App.tsx — replace `showSwagger` with `centerPanel` tri-state
- [x] T2: Header.tsx — add "KV" toggle button
- [x] T3: KvOperatorPanel.tsx — new panel: selector + action bar + results
- [x] T4: Inspector.tsx — remove KV tab
- [x] T5: Tests — updated E2E tests 00/09/10/11/24; unit tests pass; tsc + build clean

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

## File List

- `crowkv-console/web/ui/src/App.tsx` — modify
- `crowkv-console/web/ui/src/shell/Header.tsx` — modify
- `crowkv-console/web/ui/src/panels/KvOperatorPanel.tsx` — new
- `crowkv-console/web/ui/src/panels/KvPanel.tsx — delete (logic absorbed)
- `crowkv-console/web/ui/src/shell/Inspector.tsx` — modify
- `crowkv-console/web/ui/src/api.ts` — no changes (Option A)
- `crowkv-console/web/src/lib.rs` — no changes (Option A)

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
