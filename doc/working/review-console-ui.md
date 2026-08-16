<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Console Web UI Review

Scope: `app/crow-web/ui/` (React + TS) and its Rust-side HTTP handlers in
`app/crow-web/src/`. This doc captures the code-level review findings
(issues open, coverage gaps).

## Open issues

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

  ai-todo: we should use crow-kv-client in UI, then we have same code for error handling. All kv operation should use crow-kv-client. reivew other case and fix. 

### Hardcoded defaults

- **Default ports `19910` / `19920` / `29920`** appear in
  `App.tsx:654,659-660,672-673`, `AddNodeDialog.tsx:34-36,51`,
  `DeployServerDialog.tsx:28-29`, `DeployDiskdbDialog.tsx:26,37`,
  `components/dialogs/defaults.ts`. Acceptable as dev defaults but not
  configurable for production. Low priority — extract when a deployment
  config story lands.

  ai-todo: avoid use static, we can define some port range for different services, but generate the ports dynamically, avoid use busy port.

### File size

- `app/crow-web/ui/src/App.tsx` is 1018 lines (crossed the 1000-line
  "must split" threshold in this branch). `app/crow-web/src/lifecycle.rs`
  is 2114 (pre-existing). Both are split candidates.

  ai-todo: split by review guide

## E2E coverage gaps

The 14-file E2E suite is clean: no `test.skip` / `test.fixme`, no
TODO/FIXME in test files. Two `waitForTimeout(500)` remain in
`41-canvas-fit-pan.spec.ts:113,159` (canvas animation settle). Coverage
by feature:

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

ai-todo: need add e2e test for these components and function

## E2E flow review — remaining findings

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

This is acceptable for UI-interaction testing; the mock-based sections
are marked with comments in the test file. A companion test against the
real backend (even a smoke-level one) would close the gap.

ai-todo: avoid mock-based test, start real service and inject real data. We need every design function works correct on UI and tack by e2e test. 

## Recommendation order

1. Add a direct `CapacityPanel` E2E (empty/error/loading states).
2. Decide on `NotLeaderHint` retry in `api.ts` after confirming the KV
   ops request path.
3. Add a real-backend smoke test for capacity compact/rebuild (companion
   to the mock-based test in 50).
4. Split `App.tsx` and `app/crow-web/src/lifecycle.rs`.
