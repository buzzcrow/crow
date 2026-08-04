<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROW Console Web UI Design (v1, lean)

Upstream: `../kv/design-crow-kv.md` §15.4.6 (requirements spec). Sibling:
`doc/design/design-crow-console.md` (backend Axum routes, registry, SSH
lifecycle, Swagger asset hosting).

This document covers the **frontend SPA design decisions only** —
what we chose and why. Requirements (the *what*) live in
`../kv/design-crow-kv.md`; backend API contracts live in `design-crow-console.md`.


## 1. Goals (recap)

- Single page, no full-page navigation.
- Two first-class hierarchy views (Physical ⇄ Logical) that drive the
  sidebar tree, the topology canvas, and the inspector together.
- Full operator surface: rack/node/server lifecycle, store/group/replica
  CRUD, KV data plane, embedded Swagger.
- Offline-capable: no third-party CDN at runtime.
- Lean: minimal dependencies, no feature the requirement does not mandate.

## 2. Stack decisions

- **React + TypeScript + Vite + TailwindCSS** — carried over from the
  existing codebase; no framework migration.
- **React Flow for topology** — slim usage only (custom nodes, pan, click
  select). Deliberately no minimap, zoom toolbar, layout selector, or edge
  labels — the canvas is a navigation aid, not an analytics surface.
- **React Context for state** — view-mode, selection, toasts, activity.
  No Redux; the state surface is small enough that Context + local hooks
  suffice.
- **No client-side routing** — the SPA mounts at the document root;
  intra-SPA navigation is selection state, not URL navigation. This keeps
  embedding trivial (no history API conflicts).
- **Removed dependencies**: `recharts`, `jspdf`, `jspdf-autotable`,
  `uuid`, `react-router-dom` — none are needed for the lean v1 surface.

## 3. Information Architecture

A fixed three-pane shell. A single root-level **view-mode** (Physical |
Logical) selects which hierarchy every pane renders.

```
┌─ Header ───────────────────────────────────────────────────────────┐
│ brand · health pill · view toggle (Physical/Logical) · last refresh │
│ · refresh · node selector (Swagger only) · KV / API panel toggles   │
├─ Sidebar ─────┬─ Center panel ─────────────┬─ Inspector ────────────┤
│ filter input  │ Topology canvas (default)  │ Details (key/value)    │
│ hierarchy     │ Swagger panel (API toggle) │ Activity (recent ops)  │
│ tree of the   │ KV Operator panel (KV)     │                        │
│ active view   │                            │                        │
│ (+ context    │                            │                        │
│  menu)        │                            │                        │
└───────────────┴────────────────────────────┴────────────────────────┘
```

- **Header** (~56px): brand label, cluster health pill, Physical/Logical
  toggle, last-refresh time, manual refresh button, node selector
  (consumed only by the Swagger panel).
- **Sidebar** (~240px): a text filter plus the hierarchy tree for the
  active view. Click selects; right-click opens the per-layer context
  menu. No favorites, no recent, no saved presets.
- **Canvas**: React Flow rendering the active view's hierarchy. Drag pans,
  wheel zooms (React Flow default), click selects. Selection is shared
  with the sidebar and inspector via `SelectionContext`. No floating
  toolbar.
- **Center panel**: one of three modes, toggled from the header —
  Topology canvas (default), Swagger panel, or KV Operator panel. The
  KV and Swagger toggles are mutually exclusive with the topology view;
  selecting one replaces the canvas.
- **Inspector** (~320px, collapsible): tabs scoped to the selection —
  Details and Activity only. KV operations have moved to the center KV
  Operator panel (§6.1).

Selection is held in one `SelectionContext`. The shell is rendered once;
switching view-mode swaps the tree data and the canvas layout only.

### 3.1 Selection & cross-jump

Selection is `{ type, id, parentIds }` where `type ∈ { Rack, Node, Server,
Store, Group, Replica }`. Clicking any tree row or canvas node sets it.

Cross-jump (one click) is supported for the common case only:
- Logical `Replica` → "Show on node": switch to Physical, expand the
  owning `Node → Server → Store → Group`, select the matching
  `LocalReplica`.
- Physical `LocalReplica`/`Group` → "Show in cluster": switch to Logical,
  expand the owning `Store → Group`, select the unified row.

No navigation stack / back button in v1.

## 4. Visual Language

Single dark theme via CSS variables under `.crow-console` (existing
tokens in `src/index.css`). Status colors: `--healthy`, `--degraded`,
`--failed`, `--unknown`, plus `--remote` for remote-replica accent.

Status is never color-only — every status row also carries a glyph
(✓ / ! / ✕ / ?). Leader replicas carry a crown badge. Remote replicas use
a dashed border + `--remote` accent so peer-list mis-wirings are visible.

Animations are minimal (selection/hover transitions); honor
`prefers-reduced-motion`.

## 5. Topology Canvas (React Flow, slim)

One layout at a time, chosen by view-mode. Layout is computed by a small
deterministic tree-layout pass in `topology/layout.ts` (columns by depth,
rows by sibling index) — no dagre, no force simulation, no user-selectable
layouts.

### 5.1 Physical layout

Renders `Rack → Node → Server → PxStore → PxGroup → {Local, Remote…}`
read from the physical tree. Node types: `Rack`, `Node`, `Server`,
`PxStore`, `PxGroup`, `LocalReplica`, `RemoteReplica`. Edges follow
parent→child containment. Each `RemoteReplica` draws a solid edge to its
peer `LocalReplica` (a missing edge is the bug this view surfaces). The
leader radiates accent edges to followers.

### 5.2 Logical layout

Renders `Cluster → Store → Group → Replica…`. Node types: `Cluster`,
`Store`, `Group`, `Replica` (with a `node_id` badge). The leader radiates
accent edges to followers; no local/remote distinction.

### 5.3 Interactions

- Drag pans, wheel zooms (React Flow built-ins), click selects.
- Selecting a node drives the inspector and highlights the sidebar row.
- Right-click a node opens the same per-layer context menu as the tree.
- Tooltips on hover surface one useful fact (host, leader id, reachable).
- No minimap, zoom toolbar, search box, focus mode, export, or edge
  labels.

## 6. Inspector Panel

Tabs re-render against the current selection:

1. **Details** — labelled key/value table from the selected entity
   (physical or logical shape). Long values support copy-to-clipboard. A
   footer row shows the cross-jump link (§3.1).
2. **Activity** — chronological client-side list of UI-issued operations
   (timestamp, action, target, outcome). No filter/export in v1.

The KV tab has been removed from the Inspector. All KV operations now
live in the center KV Operator panel (§6.1), which provides a full-width
surface with store/group selectors, scan results, and an action bar.

## 6.1 KV Operator Panel (center panel)

A full-width center panel for KV data-plane operations, toggled from the
header via a "KV" button (mutually exclusive with Swagger and topology
canvas). Replaces the former Inspector KV tab, which was too cramped at
320px for comfortable key browsing.

**Design choices:**

- **Flat single-page layout (no tabs)** — action bar on top, scan
  results below. The user can scan, see results, and act (put/get/delete)
  without switching tabs.
- **Store/group selector with "All Groups" option** — when selected,
  scan iterates over every group and merges results (labeled by group).
  Demo inject randomly distributes keys across groups. This avoids
  forcing the user to pick a group when they want a store-wide view.
- **Auto-scan on first load** — when store and group are both set, the
  panel triggers a scan automatically so the user sees data immediately.
- **Independent of ViewMode** — KV operations are always logical
  (store/group), regardless of whether the topology canvas shows the
  physical or logical view.

**Scan pagination (`start_after` token):**

The scan API returns at most `limit` items with a `truncated` flag but
had no way to fetch the next page. Rather than adding a total count
(expensive on large keyspaces), we adopted an S3 ListObjectsV2-style
`start_after` cursor: the caller passes the last key from the previous
batch; the engine returns keys strictly greater than `start_after` that
still match the prefix. The UI shows a "Load more" button when
`truncated` is true; clicking it appends the next batch.

**Decision — `CrowTreeEngine` over-fetch + filter:** The C++ crow-tree
scan API takes only prefix + limit (no `start_after`). Rather than
modifying C++ immediately, `CrowTreeEngine` over-fetches with the
original prefix, then filters out keys ≤ `start_after` in Rust before
applying the limit. This is inefficient when `start_after` is deep into
a large prefix range — a follow-up can push `start_after` into the C++
engine. When `start_after` is empty, the fast path is identical to the
old behavior.

**Demo delete at scale:** "Delete all demo" scans for `demo_` prefix
with pagination (up to 1000 keys for the confirmation count), then
deletes with 16-way parallel `kvDelete`. If more than 1000 keys exist,
scan+delete continues in batches after confirmation. The confirmation
dialog shows "1000+" when the count may be higher.

## 7. Embedded Swagger Panel

- Lives inside the SPA; opening it does not navigate or open a new tab.
- Hosts an `<iframe>` at `${apiPrefix}/swagger/?url=${apiPrefix}/nodes/:node_id/openapi.json`,
  where `:node_id` is the header's node selector.
- Switching the node reloads the iframe `url` only.
- Loaded lazily (code-split) so initial page load is not blocked.

## 8. Embedding Contract

The SPA is mountable as a sub-component with a minimal props interface
(`apiPrefix`, `basePath`, `readonly`, `modules` opt-out, `initialViewMode`,
`onEvent` callback). Three isolation rules:

- **Style isolation** — everything wraps in `.crow-console`; Tailwind
  uses the `tw-` prefix and `important: '.crow-console'`.
- **API isolation** — every fetch resolves against `apiPrefix`.
- **Standalone** — `index.html` mounts at the document root with defaults;
  `embed.ts` exports the component for hosts.

## 9. Data & Polling Strategy

- **Two-tree contract** — the SPA speaks physical (`/api/racks`,
  `/api/nodes`) and logical (`/api/stores`) trees per `design-crow-console.md`.
  No panel constructs raw `host:port` URLs; `api.ts` is the single URL
  builder.
- **Asymmetric polling** — only the active view polls fast (~5s); the
  inactive view polls slow (~30s) so toggling renders immediately.
  Polling pauses while the tab is hidden.
- **Optimistic-free mutations** — mutations call the backend, await
  success, then trigger a refresh of the affected view; they do not
  hand-edit cached data. This trades a round-trip for correctness
  simplicity.

## 10. Module Layout

The source tree follows the pane structure: `shell/` (Header, Sidebar,
Inspector), `topology/` (canvas + layout), `panels/` (KvOperatorPanel,
SwaggerPanel, ActivityLog), `components/` (Dialog, ContextMenu, dialogs,
UI primitives), and `contexts/` (ViewMode, Selection, Toast, Activity).
`api.ts` and `types/index.ts` are the single URL-builder and data-model
modules respectively.

**Deleted from v1**: CommandPalette, favorites, fuzzy search, export
utils, bulk action dialog, metrics history, theme context — none are
needed for the lean surface.

## 11. Accessibility

- Keyboard reachable: Tab/Enter/Escape on tree rows, dialogs, and menus;
  context menus mirror to keyboard-activatable buttons where practical.
- Color is never the sole status channel (glyph + color).
- Strings go through a single `t(key)` helper (English only) so a future
  locale pack needs no source changes. (Optional for v1; may inline.)

## 12. Testing

- Existing Vitest unit tests for dialog request bodies and `listRacks`
  envelope handling are **retained** (they pin the backend contract).
- The Playwright real-backend E2E suite (`app/crow-web/ui/e2e/`)
  targets this lean SPA; selectors track the rewritten DOM. The full
  chain rack→node→deploy→store→group→replica→KV is the acceptance bar.
