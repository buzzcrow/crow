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

`50-capacity-diskdb.spec.ts` now uses real backend for set-status
operations (disk-group status PUT, disk status PUT) via a deployed
kv-server + cluster init in `beforeAll`. The recalc/scan/usage
endpoints remain mocked because they proxy to a running diskdb that
must own the disk-group — disk-group-to-instance ownership assignment
is R72 (not yet implemented), so the diskdb never takes ownership and
the real endpoints return "no diskdb instance owns dg <id>".

The compact/rebuild dialog flows (zone input validation, dialog open/
close) are UI-interaction tests that don't need a real backend.

- Real backend timing issues for set-status are now covered.
- Recalc/scan/usage mock-based sections are marked with comments in the
  test file explaining the R72 dependency.
- A companion test against the real backend for recalc/scan/usage can
  be added once R72 ownership assignment is implemented.

Also fixed: the UI was sending `"Down"` as the status value, but the
backend's `parse_hw_status` only accepts `Offline` (not `Down`). Fixed
to send `"Offline"` in `App.tsx`.

Also fixed: `randomDiskId()` was generating `16hex-16hex` (33 chars
with dash), but the backend's `parse_disk_id` expects 32 hex chars
without a dash. Fixed to generate 32 contiguous hex chars.

Also fixed: `deploy_diskdb_local` generated the config with the default
kv-server management port (9910) instead of the actual deployed
kv-server's port. Added `kv_server_mgmt_seeds` field to
`DiskdbDeployRequest` so the deploy handler passes the real kv-server
management URL(s) for the node.

## Recommendation order

1. Add a direct `CapacityPanel` E2E (empty/error/loading states).
2. Decide on `NotLeaderHint` retry in `api.ts` after confirming the KV
   ops request path.
3. Add real-backend coverage for recalc/scan/usage once R72 (disk-group
   ownership assignment) is implemented.
4. Split `App.tsx` and `app/crow-web/src/lifecycle.rs`.
