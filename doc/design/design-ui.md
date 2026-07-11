# CrowKV Console Web UI Design (v1, lean)

Upstream: `doc/requirement.md` §15.4.6. Sibling: `doc/design/design-console.md`
(backend Axum routes, registry, SSH lifecycle, Swagger asset hosting).

This document covers the **frontend SPA only**. Backend-API contracts are
normative in `design-console.md` and `requirement.md` §15.


## 1. Goals (recap)

- Single page, no full-page navigation.
- Two first-class hierarchy views (Physical ⇄ Logical) that drive the
  sidebar tree, the topology canvas, and the inspector together.
- Full operator surface: rack/node/server lifecycle, store/group/replica
  CRUD, KV data plane, embedded Swagger.
- Offline-capable: no third-party CDN at runtime.
- Lean: minimal dependencies, no feature the requirement does not mandate.

## 2. Stack

| Concern | Choice | Notes |
| --- | --- | --- |
| Framework | React 18 + TypeScript + Vite | Existing. |
| Styling | TailwindCSS (`tw-` prefix, scoped to `.crowkv-console`) | Existing tokens in `index.css`. |
| Topology | React Flow | Slim usage: custom node types, pan + click select. **No** minimap, zoom buttons, layout selector, focus mode, or edge labels. |
| State | React Context + local state | View-mode, selection, toasts, activity. No Redux. |
| Icons | lucide-react | Already present. |

**Removed dependencies** (no longer used in v1): `recharts`, `jspdf`,
`jspdf-autotable`, `uuid`, `react-router-dom`. The SPA mounts at the
document root; intra-SPA routing is selection state, not URL navigation.

The build output is the existing `web/ui/dist/` tree consumed by
`crowkv-web::spa::serve_spa`.

## 3. Information Architecture

A fixed three-pane shell. A single root-level **view-mode** (Physical |
Logical) selects which hierarchy every pane renders.

```
┌─ Header ───────────────────────────────────────────────────────────┐
│ brand · health pill · view toggle (Physical/Logical) · last refresh │
│ · refresh · node selector (Swagger only)                            │
├─ Sidebar ─────┬─ Topology canvas ──────────┬─ Inspector ────────────┤
│ filter input  │ React Flow graph of the    │ Details (key/value)    │
│ hierarchy     │ active view; pan + click.  │ KV (logical Group)     │
│ tree of the   │ Click a node = select.     │ Activity (recent ops)  │
│ active view   │                            │ Swagger (node target)  │
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
- **Inspector** (~320px, collapsible): tabs scoped to the selection —
  Details always; KV only for a logical Group; Activity always; Swagger
  is a shared panel (node-targeted).

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

Single dark theme via CSS variables under `.crowkv-console` (existing
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
2. **KV** — enabled only for a logical `Group` selection. Wraps `kvGet`,
   `kvScan`, `kvPut`, `kvDelete`. Destructive ops confirm. Not offered for
   a physical `PxGroup` (KV resolves the leader from the logical monitor
   cache).
3. **Activity** — chronological client-side list of UI-issued operations
   (timestamp, action, target, outcome). No filter/export in v1.

The **Swagger** panel is a shared feature surface (not selection-scoped),
reachable from the inspector tab strip; it targets the header's selected
node.

## 7. Embedded Swagger Panel

- Lives inside the SPA; opening it does not navigate or open a new tab.
- Hosts an `<iframe>` at `${apiPrefix}/swagger/?url=${apiPrefix}/nodes/:node_id/openapi.json`,
  where `:node_id` is the header's node selector.
- Switching the node reloads the iframe `url` only.
- Loaded lazily (code-split) so initial page load is not blocked.

## 8. Embedding Contract (minimal)

```ts
interface CrowkvConsoleProps {
  apiPrefix?: string;          // default "/api"
  basePath?: string;           // default "/" (mount hint only)
  readonly?: boolean;          // default false — hides all mutating controls
  modules?: Partial<Record<
    "racks" | "nodes" | "stores" | "groups" | "replicas" |
    "kv" | "swagger" | "activity", boolean>>;
  initialViewMode?: "Physical" | "Logical"; // default "Logical"
  initialNodeId?: string;      // pre-selects the Swagger node
  onEvent?: (event: { type: string; payload?: unknown }) => void;
}
```

Rules:
- **Style isolation**: everything wraps in `.crowkv-console`; Tailwind uses
  the `tw-` prefix and `important: '.crowkv-console'`.
- **API isolation**: every fetch resolves against `apiPrefix` via `api.ts`.
- **Standalone**: `index.html` mounts `<App />` at the document root with
  defaults; `embed.ts` exports the component for hosts.

## 9. Data Model, Polling, API Routing

### 9.1 API routing
The SPA speaks the two-tree contract (`design-console.md` §6): physical
tree (`${apiPrefix}/racks`, `${apiPrefix}/nodes`) and logical tree
(`${apiPrefix}/stores`). `api.ts` is the single URL builder; no panel
constructs raw upstream `host:port`. The header node selector stores a
`node_id` consumed only by Swagger.

`api.ts` and `src/types/index.ts` are **kept from v1** unchanged (clean,
tested wrappers + data model).

### 9.2 Polling
- Two root hooks: `usePhysicalTree()` polls `GET /api/racks?recursive=all`
  + `GET /api/nodes?recursive=all`; `useLogicalTree()` polls
  `GET /api/stores?recursive=all`. Both publish into a selection map keyed
  by entity id so cross-jumps resolve in O(1).
- Only the active view polls fast (~5s); the inactive view polls slow
  (~30s) so toggling renders immediately. Polling pauses while the tab is
  hidden and resumes on focus.
- A poll failure surfaces a non-blocking banner ("backend unreachable —
  retrying"); the previous tree stays visible.
- Mutations call the backend, await success, then trigger a refresh of the
  affected view; they do not hand-edit cached data.

## 10. Module Layout (`crowkv-console/web/ui/src/`)

```
src/
  index.tsx / main.tsx       // standalone mount
  embed.ts                   // <CrowkvConsole /> export (minimal props)
  App.tsx                    // 3-pane shell + dialog/menu wiring
  api.ts                     // KEPT — physical + logical URL builders
  types/index.ts             // KEPT — data model
  contexts/
    ViewModeContext.tsx      // Physical | Logical
    SelectionContext.tsx     // single selection { type, id, parentIds }
    ToastContext.tsx         // toast queue
    ActivityContext.tsx      // client-side op log
  data/
    usePhysicalTree.ts       // KEPT/simplified
    useLogicalTree.ts        // KEPT/simplified
    crossJump.ts             // physical↔logical id resolution (§3.1)
  shell/
    Header.tsx               // brand, health pill, view toggle, refresh, node selector
    Sidebar.tsx              // filter + Tree
    Tree.tsx                 // hierarchy rows for the active view
    Inspector.tsx            // Details / KV / Activity tabs + Swagger
  topology/
    TopologyCanvas.tsx       // React Flow host (slim)
    layout.ts                // deterministic tree layout
    buildFlow.ts             // tree → nodes/edges for each view
    CrowKVNode.tsx           // node renderer (all layers)
  panels/
    KvPanel.tsx              // logical Group KV ops
    SwaggerPanel.tsx         // lazy iframe
    ActivityLog.tsx          // recent ops list
  components/
    ContextMenu.tsx          // per-layer right-click menu
    Dialog.tsx               // modal shell
    ToastContainer.tsx
    dialogs/                 // KEPT — Add{Rack,Node,Store,Group,Replica}, DeployServer, ConfirmDelete
    ui/{Button,Input,Badge}.tsx  // KEPT primitives
  index.css                  // KEPT tokens + tailwind
```

**Deleted from v1**: `shell/CommandPalette.tsx`,
`data/commandPaletteActions.ts`, `data/favorites*`, `utils/fuzzySearch.ts`,
`utils/exportUtils.ts`, `components/ExportDropdown.tsx`,
`components/FilterControls.tsx`, `components/BulkActionDialog.tsx`,
`hooks/useBulkOperations.ts`, `data/useMetricsHistory.ts`,
`contexts/ThemeContext.tsx`, and their tests. Tests under
`crowkv-web/tests/frontend_routes.rs` continue to assert SPA fallback.

## 11. Accessibility

- Keyboard reachable: Tab/Enter/Escape on tree rows, dialogs, and menus;
  context menus mirror to keyboard-activatable buttons where practical.
- Color is never the sole status channel (glyph + color).
- Strings go through a single `t(key)` helper (English only) so a future
  locale pack needs no source changes. (Optional for v1; may inline.)

## 12. Testing

- Existing Vitest unit tests for dialog request bodies and `listRacks`
  envelope handling are **retained** (they pin the backend contract).
- The Playwright real-backend E2E suite (`crowkv-console/web/ui/e2e/`)
  targets this lean SPA; selectors track the rewritten DOM. The full
  chain rack→node→deploy→store→group→replica→KV is the acceptance bar.
