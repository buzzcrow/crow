# CrowKV Console Web UI Design

Upstream: `doc/requirement-ui.md`. Sibling: `doc/design/design-console.md`
(backend Axum routes, registry, SSH lifecycle, Swagger asset hosting,
operation log).

This document covers the **frontend SPA only**: visual language,
information architecture, component decomposition, embedding contract,
and the polling / state model. Backend-API contracts are normative in
`design-console.md` and `requirement.md` §15.

## 1. Goals (recap from the requirement)

- Single page, no full-page navigation.
- Embeddable as a sub-component of a larger host product.
- Demo-grade aesthetics, operator-grade utility.
- Offline-capable: no third-party CDN at runtime.

## 2. Stack

| Concern | Choice | Rationale |
| --- | --- | --- |
| Framework | React 18 + TypeScript + Vite | Already in `crowkv-console/web/ui`; large ecosystem; tree-shakeable bundles. |
| Styling | TailwindCSS + shadcn/ui-style primitives | Consistent visual language without writing bespoke CSS; trivial theme overrides. |
| Topology graph | React Flow | Custom node types, dragging, viewport controls; the requirement (single-page topology) needs an interactive canvas, not a static SVG. |
| Charts | Recharts | Lightweight; covers the per-component metric panel. |
| State | React Context + local state | No Redux. The data fan-out is shallow: snapshot polling at the root, per-tab/per-panel local state for UI affordances. |
| Routing | `react-router` `MemoryRouter` | The SPA never controls `window.location`; it routes only inside its own tree so it can be mounted under any host path. |

The build output is the existing `web/ui/dist/` tree consumed by
`crowkv-web::spa::serve_spa` (see `design-console.md` §6.1).

## 3. Information Architecture

The SPA is a **single page with a fixed three-pane shell**, and every
data-bearing region (sidebar tree, topology canvas, inspector) is
driven by a **view-mode toggle** that selects either the *physical*
or the *logical* hierarchy from `design/design-console.md` §3.

```
┌─ Header ────────────────────────────────────────────────────────────┐
│ brand · cluster health pill · view-mode toggle (Physical/Logical) · │
│ last-refresh · refresh · node selector (Swagger only) · ⋯ menu      │
├─ Sidebar ──┬─ Canvas / Detail body ─────────┬─ Inspector ───────────┤
│ Hierarchy  │ React Flow topology canvas     │ Selection details     │
│ tree for   │   rendered in the active       │ (shape depends on the │
│ the active │   view-mode  *or*              │ active view-mode and  │
│ view-mode  │ active feature panel:          │ selection)            │
│ + search   │   Physical: Racks · Nodes ·    │ Metrics               │
│            │             Servers ·          │ KV browser            │
│            │             NodeInspect        │ Activity log          │
│            │   Logical : Stores · Groups ·  │                       │
│            │             Replicas · KV      │                       │
│            │   Shared  : Swagger ·          │                       │
│            │             Activity           │                       │
└────────────┴────────────────────────────────┴───────────────────────┘
```

- **Header** (fixed, ~56 px):
  - Left: customisable brand logo, cluster health pill (with optional
    timeline dropdown showing health history), **view-mode toggle**
    (Physical ⇄ Logical, persisted in `localStorage`),
  - Center: breadcrumb trail showing the full hierarchy path of the current
    selection, with clickable parent links,
  - Right: last refresh timestamp, manual-refresh button, *node selector*
    (consumed **only** by the Swagger panel), command palette trigger
    button, overflow menu (theme toggle: light/dark/system, export options,
    About).
- **Command Palette**: A global, keyboard-first modal accessible via
  `Cmd/Ctrl+K` or the header button. It uses fuzzy search over all entities
  and actions, with keyboard navigation (arrow keys to select, Enter to
  activate, Escape to close). Results are grouped by category (Entities,
  Actions, Views) for scannability.
- **Sidebar** (collapsible, ~240 px):
  - Top: search/filter input for the hierarchy tree, with filter options
    (status, role) in a dropdown.
  - Favorites section: pinned entities (cross-view, persisted in
    `localStorage`), with a "remove from favorites" action on hover.
  - Recent items section: last 10 accessed entities, with a "clear recent"
    action.
  - Hierarchy tree of the active view-mode:
    - Physical: `Rack → Node → Server → PxStore → PxGroup →
      { Local, Remote… }`. Local and Remote replicas render with
      distinct glyphs so a mis-wired peer is visible at a glance.
    - Logical: `Cluster → Store → Group → Replica…` with each replica
      rendered as a single row badged by `node_id`.
    - Tree items support multi-selection via `Ctrl/Cmd+click` for bulk
      operations.
  - Selecting any row drives both the topology canvas (focus / fit
    into view) and the inspector.
- **Body**: a tabbed feature surface. The default tab is the topology
  canvas; the remaining tabs are partitioned by which view they
  belong to (see §5.2 / §5.3 below). Switching view-mode swaps the
  tab strip; the canvas tab is always present in both modes. The
  Swagger and Activity tabs are view-mode-agnostic.
- **Inspector** (collapsible, ~320 px): contextual detail of the
  current selection. In the **physical** view, a `PxGroup` selection
  surfaces the full `local` + `remotes` split with reachability
  flags; in the **logical** view, a `Group` selection surfaces the
  unified replica list and the leader hint, and exposes KV
  operations.

The shell is implemented once and never re-rendered when feature tabs
or the view-mode change; only the body region and the sidebar's tree
data swap. View-mode is held in a single root-level context so every
descendant can react to it consistently.

### 3.1 Cross-jumping between views

The two views describe the same entities and must remain navigable
in both directions in one click:

- From a logical `Replica` → "Show on node" jumps to physical view,
  expands the owning `Node → Server → PxStore → PxGroup`, and selects
  the corresponding `LocalReplica`.
- From a physical `LocalReplica` (or `RemoteReplica`) → "Show in
  cluster" jumps to logical view, expands the owning `Store → Group`,
  and selects the unified `Replica` row sharing the same
  `replica_id`.
- From any physical `PxGroup` → "Show logical group" jumps to the
  logical `Group` view for the same `(store_id, group_id)`.
- These jumps preserve the current selection in a small navigation
  stack so "Back" returns to the originating view.

## 4. Visual Language

### 4.1 Theme tokens
The palette is exposed as CSS variables under a `.crowkv-console` scope
so that hosts can override them through the `theme` mount prop. A `--brand-accent` token is added for custom host branding, which overrides `--accent` when supplied.

| Token | Default (dark) | Use |
| --- | --- | --- |
| `--bg` | `#1f2937` | App background |
| `--panel` | `#111827` | Headers, sidebars, inspector |
| `--border` | `#374151` | Dividers |
| `--text` | `#f9fafb` | Primary text |
| `--muted` | `#9ca3af` | Secondary text |
| `--accent` | `#3b82f6` | Default interactive emphasis |
| `--brand-accent` | `inherit (uses --accent | Host custom brand color |
| `--healthy` | `#10b981` | Healthy / leader-ok |
| `--degraded` | `#f59e0b` | Degraded |
| `--failed` | `#ef4444` | Failed |
| `--unknown` | `#6b7280` | Unknown |
| `--remote` | `#8b5cf6` | Remote replica accent |
| `--shadow-sm` | `0 1px 2px 0 rgb(0 0 0 / 0.05)` | Subtle depth |
| `--shadow-md` | `0 4px 6px -1px rgb(0 0 0 / 0.1), 0 2px 4px -2px rgb(0 0 0 / 0.1)` | Panel depth |
| `--shadow-lg` | `0 10px 15px -3px rgb(0 0 0 / 0.1), 0 4px 6px -4px rgb(0 0 0 / 0.1)` | Modals/toasts |

Themes:
- Light theme ships as a complete token override;
- System theme detection is enabled by default (auto-switches based on `prefers-color-scheme`);
- User theme selection (light/dark/system) persists in `localStorage`;
- The host's `theme` prop, when supplied, takes priority over all defaults.

### 4.2 Status semantics

| State | Visual | Animation |
| --- | --- | --- |
| `Healthy` | solid border, accent-colored icon | none |
| `Degraded` | dashed border | slow pulse (~1.5 s) |
| `Failed` | thick border (2 px) | fast blink (~0.5 s), opt-out via "reduce motion" |
| `Unknown` | thin border, muted icon | none |
| `Leader` | solid border + crown badge + soft glow | slow pulse |
| `Follower (local)` | solid border | none |
| `Remote replica` | dashed border, `--remote` accent | none |
| `Selected` | 2px `--brand-accent` border, subtle elevation | fast scale-in transition on selection |

Animations respect `prefers-reduced-motion`; embedding hosts can disable
all animation through the theme contract.

### 4.3 Typography
System sans (Inter / Roboto / system-ui fallback). Title 20 px / 700,
section 14 px / 600, body 14 px / 400, meta 12 px / 400 in `--muted`. All text uses 1.5 line height for readability.

### 4.4 Motion Design
Subtle, purposeful animations are used throughout to provide feedback and guide attention:
- **State transitions**: 200ms ease transitions for all state changes (selection, hover, view mode switch, panel expansion)
- **Micro-interactions**: Hover effects on all interactive elements (scale 1.02, subtle shadow), press effects (scale 0.98)
- **Toast notifications**: Slide-in from the bottom-right corner, 4s auto-dismiss (persist for errors)
- **Canvas updates**: Smooth position transitions when nodes are added/removed or layout changes, to avoid jarring jumps

### 4.5 Toast Notification System
Unobtrusive toast notifications are used for operation feedback:
- Stacked in the bottom-right corner, 4s auto-dismiss for success/info, persistent for errors until manually dismissed
- Include an action button to jump directly to the activity log for full details
- Support different styles: success (green accent), error (red accent), info (blue accent), warning (orange accent)
- Support keyboard navigation (Tab to focus action, Escape to dismiss all)

## 5. Topology Canvas (React Flow)

The canvas renders one of **two layouts** at a time, selected by the
header's view-mode toggle. Each layout uses its own node types and
its own edge semantics, so the operator's mental model never has to
"translate" between physical and logical at the same time.

### 5.1 Physical layout (deployment view)

Reads `Rack → Node → Server → PxStore → PxGroup → {Local, Remote…}`
from the physical tree (`design-console.md` §6.3, with `?recursive=`
as needed). Parent-child containment is used wherever it reads well:
Racks contain their Nodes; each Node contains its (optional) Server;
each Server contains its `PxStore`s and `PxGroup`s.

| Layer | Shape | Required content | Status surface |
| --- | --- | --- | --- |
| Rack | Wide rounded rectangle, container | Rack id, name, child node count | summary of children |
| Node | Rectangle, container | Node id, host, SSH state | per-node status |
| Server | Rectangle | mgmt port, gRPC port, PID, `ProcState` | server lifecycle state |
| PxStore | Compact pill (inside Server) | Store id, group count | per-node store status |
| PxGroup | Compact pill (inside PxStore) | Group id, leader hint, replica count | leader / quorum status |
| LocalReplica | Filled circle | Short replica id, role | health, leader badge |
| RemoteReplica | Hollow circle, `--remote` accent | Short replica id, peer `node_id`, `reachable` flag | health, reachability |

Edges:

- A solid edge connects each `RemoteReplica` glyph on a Node to the
  matching `LocalReplica` on the peer Node. These edges are the
  **visual representation of the per-node remote list**: a missing
  edge is exactly the bug the physical view exists to surface.
- The current leader replica radiates accent-colored edges to all
  followers in the same group.

### 5.2 Logical layout (usage view)

Reads `Cluster → Store → Group → Replica…` from the logical tree
(`design-console.md` §6.4). Replicas are rendered as a single
unified ring around each Group; `node_id` is a badge on the replica
glyph rather than a separate hierarchy level.

| Layer | Shape | Required content | Status surface |
| --- | --- | --- | --- |
| Cluster | Outermost frame | Cluster name, store count, aggregate health | aggregate roll-up |
| Store | Wide rounded rectangle, container | Store id, name, member-node count | store health |
| Group | Compact pill (inside Store) | Group id, leader `replica_id`, `GroupHealth` | leader / quorum |
| Replica | Filled circle with `node_id` badge | Short replica id, role, state | health, leader badge |

Edges:

- The leader replica radiates accent edges to its followers (same
  visual as the physical leader, but no local-vs-remote distinction).
- No remote-list edges exist in this layout; mis-wirings are only
  visible by switching to the physical layout.

### 5.3 Auto-layout and interactions

- **Layout options**: Users can select between three auto-layout modes, persisted per view-mode in `localStorage`:
  - Force-directed (default): Balanced layout for most cluster sizes, groups related entities together
  - Hierarchical: Tree-like layout that strictly follows the parent-child containment hierarchy
  - Grid: Compact grid layout for large clusters with many similar entities
- React Flow's auto-layout runs once on data changes and again on
  user-initiated "auto layout" actions. Each view-mode keeps its own
  saved viewport (pan/zoom) so toggling between views does not lose
  the user's place.
- **Search & highlight**: A canvas search input in the topology controls lets users search for entities by ID/name; matching entities are highlighted with a pulsing border and automatically centered in the viewport.
- **Focus mode**: A toggle in the canvas controls hides all entities except the current selection and its direct peers/connections, simplifying debugging of specific groups or replication issues.
- **Edge labels**: A toggle in the canvas controls shows/hides metrics labels on replication edges (replication lag, throughput, last heartbeat time). Labels are positioned to avoid overlapping nodes.
- Click selects; selection drives the inspector and the sidebar tree
  in the active view. The selection is **translated** when the user
  toggles view-mode (see §3.1 cross-jumps) — selecting a logical
  `Replica` then toggling to Physical lands on its `LocalReplica`.
- `Ctrl/Cmd`-click multi-selects within a layer for bulk operations; selected entities show a common selection border.
- Right-click opens a context menu whose actions are exactly the
  per-layer mutations from the requirement: physical actions come
  from `requirement-ui.md` §3.2 (hardware lifecycle), logical actions
  from §3.3 (cluster management), plus any custom actions injected by the embedding host. The menu shown is the menu for the
  active view-mode; disabled actions show a tooltip explaining why.
- Drag pans; wheel zooms; mini-map, fit view, layout selector, search, focus mode, and edge label controls in a floating corner toolbar.
- Tooltips on hover surface the most useful single fact (host, leader
  id, peer reachability, replication lag) without forcing the user to open the
  inspector.
- Export controls: A dropdown in the canvas toolbar allows exporting the current view as SVG or PNG, with options to include/exclude labels, edges, and status indicators.

## 6. Inspector Panel

A four-tab inspector that re-renders against the current selection.
What the tabs show depends on the active view-mode (because the
underlying API shapes differ — see `design-console.md` §3 and §6).
Custom metrics panels injected by the embedding host are appended as additional tabs.

1. **Details** — labelled key/value table.
   - Physical selection: fields from the physical tree —
     `Rack { id, name, nodes }`, `Node { id, host, ssh, server? }`,
     `ServerProcess { mgmt_url, grpc_url, pid, state, health }`,
     `NodeStore { groups }`, `NodeGroup { local, remotes,
     leader_hint }`, `LocalReplicaInfo`, `RemoteReplicaInfo {
     replica_id, node_id, reachable }`. Long values support
     copy-to-clipboard.
   - Logical selection: fields from the logical tree —
     `StoreView { nodes, groups }`, `GroupView { leader, replicas,
     state }`, `ReplicaView { replica_id, node_id, role, state }`.
   - A footer row always shows the **cross-jump link** to the same
     entity in the other view (§3.1), plus an "Add to favorites" toggle.
   - Export dropdown in the tab header allows exporting the current entity's details as JSON.
2. **Metrics** — Recharts line/area charts driven by per-resource
   live reads (no `ClusterSnapshot`). The chart series is held in
   memory, sampled per polling tick, capped at a fixed window (1 hour default). Series
   are pulled from whichever endpoint matches the selection: per-node
   liveness for physical selections, per-group leader / quorum stats
   for logical selections.
   - Time range selector in the tab header allows adjusting the window (15m / 1h / 6h / 1d).
   - Export dropdown allows exporting metrics data as CSV.
3. **KV** — only enabled when a **logical** `Group` is selected.
   Wraps `kvGet`, `kvScan`, `kvPut`, `kvDelete` against
   `/api/stores/:s/groups/:g/kv/...`. Confirms destructive
   operations. KV is intentionally *not* offered for a physical
   `PxGroup` selection: KV is a cluster-wide concept and the backend
   resolves the leader from the logical view's monitor cache.
   - Filter/sort controls for scan results (filter by key prefix, sort by key/value size).
   - Export dropdown allows exporting scan results as JSON or CSV.
4. **Activity** — chronological list of UI-issued operations with
   timestamp, action, target, outcome. v1 keeps this client-side only;
   a future revision can persist it on the backend.
   - Filter controls to show only operations for the current selection, or all operations.
   - Export dropdown allows exporting the activity log as CSV.

Custom panels injected by the embedding host are rendered after the default tabs, with their own tab labels and content. Each custom panel receives the current selection, polling data, and `apiPrefix` as props.

## 7. Embedded Swagger Panel

This replaces the "open in new tab" behaviour from the v1 SPA.

- The Swagger panel is a feature tab inside the body region. Selecting
  it does not navigate; closing it returns to the previously active
  tab.
- The panel hosts an `<iframe>` (or a direct `swagger-ui-react` mount)
  pointing at `${apiPrefix}/swagger/`. The OpenAPI document URL passed
  to Swagger UI is `${apiPrefix}/openapi.json` resolved against the
  currently selected server in the header.
- Switching the selected server updates the Swagger UI's `url`
  parameter and reloads the request panel. Swagger's own state (open
  request bodies, examples) is reset on reload by design.
- Asset hosting stays in `crowkv-web` (`design-console.md` §8). The SPA
  does not bundle Swagger UI itself.

## 8. Embedding Contract

The SPA exposes a single React component (working name
`<CrowkvConsole />`) plus a Vite entry that ships a UMD bundle for
non-React hosts.

```ts
interface CrowkvConsoleProps {
  apiPrefix?: string;          // default "/api"
  basePath?: string;           // default "/"
  readonly?: boolean;          // default false
  modules?: Partial<Record<
    // Physical-tree panels
    "racks" | "nodes" | "nodeInspect" |
    // Logical-tree panels
    "stores" | "groups" | "replicas" | "kv" |
    // View-mode-agnostic panels
    "swagger" | "activity",
    boolean
  >>;
  theme?: ThemeOverride;       // CSS-variable overrides, including --brand-accent
  initialViewMode?: "physical" | "logical"; // default "logical"
  initialNodeId?: string;      // pre-selects a node for the Swagger panel
  onEvent?: (event: ConsoleEvent) => void; // structured op log fan-out

  // New embedding features
  brandLogo?: React.ReactNode; // Custom logo to replace the default CrowKV logo in the header
  themeMode?: "light" | "dark" | "system"; // Initial theme mode, defaults to "system"
  customActions?: CustomAction[]; // Custom context menu and inspector actions
  customPanels?: CustomPanel[]; // Custom inspector panels injected by the host
}

// Custom action definition
interface CustomAction {
  id: string;
  label: string;
  icon?: React.ReactNode;
  // Which entity types this action applies to
  appliesTo: ("rack" | "node" | "server" | "store" | "group" | "replica")[];
  // Which view modes this action is available in
  viewModes?: ("physical" | "logical")[];
  // Whether to show in context menu, inspector, or both
  placement?: ("contextMenu" | "inspector" | "both")[];
  // Disabled state callback
  isDisabled?: (entity: Entity) => boolean;
}

// Custom panel definition
interface CustomPanel {
  id: string;
  label: string;
  // Which entity types this panel applies to
  appliesTo: ("rack" | "node" | "server" | "store" | "group" | "replica")[];
  // React component to render as the panel content
  component: React.ComponentType<{
    entity: Entity;
    viewMode: "physical" | "logical";
    apiPrefix: string;
    pollingData: any;
  }>;
}
```

Implementation rules:

- **Style isolation**: every component is wrapped in a `.crowkv-console`
  root; Tailwind is configured with a `tw-` prefix and `important: '.crowkv-console'`
  so utility classes never escape the mount point.
- **Routing isolation**: a `MemoryRouter` rooted at `basePath` owns
  intra-SPA navigation. The host's URL is never touched.
- **API isolation**: every fetch call resolves against `apiPrefix`. The
  default standalone deployment maps `/api` to `crowkv-web`; an
  embedding host can rewrite it to `/storage/crowkv/api` (or similar)
  without touching SPA source.
- **Standalone shipping**: `index.html` mounts `<CrowkvConsole />` at
  the document root with default props; this is what `crowkv-web`
  serves today. The embeddable bundle exports the component instead.
- **Bundle**: tree-shaking on; React Flow and Recharts are imported via
  named entry points. The standalone bundle ships everything; the
  embed bundle marks `react` and `react-dom` as peer dependencies.

## 9. Data Model, Polling, and API Routing

### 9.1 API routing
The SPA speaks the **two-tree** contract from
`design/design-console.md` §6: a **physical tree**
(`${apiPrefix}/racks`, `${apiPrefix}/nodes`) for hardware lifecycle and
debugging, and a **logical tree** (`${apiPrefix}/stores`) for cluster
management and KV data. No route crosses the two trees.

- Physical example:
  `${apiPrefix}/nodes/:node_id/server/deploy`,
  `${apiPrefix}/nodes/:node_id/stores/:store_id/groups/:group_id`
  (debugging / inspect view).
- Logical example:
  `${apiPrefix}/stores/:store_id/groups/:group_id/replicas`,
  `${apiPrefix}/stores/:store_id/groups/:group_id/kv/get`.
- Every `GET` accepts `?recursive=<depth|all>` (see §6.5 of
  `design-console.md`).

The SPA never constructs a URL containing an upstream `host:port` and
never sends `?server=<url>`; both are gone. Upstream `crowkv-server`
instances are addressed by `node_id`, resolved by the backend from
`~/.crowkv/console.toml`.

`api.ts` is the single place that builds these URLs; every panel calls
through it. The header **node selector** (renamed from "server") stores
the chosen `node_id` in SPA state (and in `localStorage` for the
standalone build) and is consumed **only** by the Swagger panel; every
other panel operates on logical ids.

### 9.2 Polling
- There is **no aggregate snapshot endpoint**. The SPA owns two
  root-level data hooks, one per view:
  - `usePhysicalTree()` polls `GET /api/racks?recursive=all` (capped
    by the backend, see `design-console.md` §6.5) for the
    physical layout.
  - `useLogicalTree()` polls `GET /api/stores?recursive=all` for the
    logical layout.
  Both hooks publish their results into a shared selection map keyed
  by entity id so cross-jumps (§3.1) resolve in O(1).
- Only the hook for the **active** view-mode polls on the fast
  cadence; the other hook continues to poll on a slower keep-alive
  cadence so view-mode toggles render immediately.
- Polling cadence is configurable; default ~5 s active / ~30 s
  background. Polling pauses while the tab is hidden
  (`document.visibilityState`) and resumes on focus.
- A poll failure surfaces a non-blocking banner ("backend unreachable
  — retrying"); the previous tree stays visible, marked stale.
- Mutations call the backend, await success, then trigger a targeted
  refresh of the affected sub-tree (the backend's monitor cache is
  already refreshed by `design-console.md` §6.6); they do not
  hand-edit cached data.

## 10. Module Layout (`crowkv-console/web/ui/src/`)

Modules are organised by **view-mode** so that the two-view contract
is reflected in the source tree, not just at runtime.

```
src/
  index.tsx                  // standalone mount
  embed.ts                   // <CrowkvConsole /> export for hosts
  shell/
    Header.tsx               // brand logo, health pill (with timeline dropdown), view-mode toggle, breadcrumbs, refresh, command palette trigger, node selector, overflow menu
    Sidebar.tsx              // favorites, recent items, search/filter input, delegates to physical/Tree or logical/Tree
    Inspector.tsx            // delegates to physical/Inspector or logical/Inspector, renders custom host panels
    CommandPalette.tsx       // global Cmd/Ctrl+K modal with fuzzy search
    ToastContainer.tsx       // global toast notification container
    ViewModeContext.tsx      // root-level Physical | Logical context
    ThemeProvider.tsx        // theme mode management (light/dark/system)
    SelectionContext.tsx     // multi-selection state management for bulk operations
  topology/
    TopologyCanvas.tsx       // switches on view-mode, includes floating toolbar (layout selector, search, focus mode, edge labels, export)
    layout.ts                // layout algorithm implementations (force-directed, hierarchical, grid)
    physical/
      PhysicalLayout.tsx
      nodes/{Rack,Node,Server,PxStore,PxGroup,LocalReplica,RemoteReplica}Node.tsx
    logical/
      LogicalLayout.tsx
      nodes/{Cluster,Store,Group,Replica}Node.tsx
    EdgeLabel.tsx            // reusable edge label component for metrics
  panels/
    physical/
      RacksTab.tsx           // GET /api/racks?recursive=1
      NodesTab.tsx           // GET /api/nodes; server lifecycle lives here, bulk operations
      NodeInspectTab.tsx     // GET /api/nodes/:n/stores/:s/groups/:g (local+remotes)
    logical/
      StoresTab.tsx          // POST /api/stores with member-node list, bulk operations
      GroupsTab.tsx          // POST /api/stores/:s/groups with node subset, bulk operations
      ReplicasTab.tsx        // unified add/remove; "inspect" → NodeInspectTab, bulk operations
      KvPanel.tsx            // /api/stores/:s/groups/:g/kv/*; no node selector, filter/sort controls
    shared/
      SwaggerPanel.tsx       // /api/swagger/?url=/api/nodes/:n/openapi.json
      ActivityLog.tsx        // filter controls, export functionality
  components/
    FilterControls.tsx       // reusable filter/sort component for lists
    ExportDropdown.tsx       // reusable export dropdown with format options
    BulkActionDialog.tsx     // confirmation dialog for bulk operations
    Breadcrumbs.tsx          // reusable breadcrumb component
  data/
    api.ts                   // physical-tree + logical-tree URL builders, export endpoints
    usePhysicalTree.ts       // GET /api/racks?recursive=all polling
    useLogicalTree.ts        // GET /api/stores?recursive=all polling
    crossJump.ts             // physical↔logical id resolution (§3.1)
    selection.ts             // shared selection map, multi-selection state
    favorites.ts             // favorites/recent items persistence
    commandPaletteActions.ts // command palette search index and action definitions
  lib/
    exportUtils.ts           // SVG/PNG/CSV/PDF export utilities
    fuzzySearch.ts           // fuzzy search implementation for command palette and tree filters
  styles/
    tokens.css
    tailwind.css
    animations.css           // motion design keyframes and transition classes
```

The current flat `components/*Tab.tsx` layout is the v1 structure and
will be reorganized into the above tree as the redesign lands. Tests
under `crowkv-web/tests/frontend_routes.rs` continue to assert that the
SPA fallback still serves `index.html` for unknown routes.

## 11. Accessibility & Internationalization

- Keyboard-first: every interaction reachable via Tab / Enter / Escape;
  context menus mirror to a button-driven menu for keyboard users.
- Color is never the sole channel for status — every status surface
  also carries a glyph (✓ / ! / ✕ / ?).
- Strings are funnelled through a single `t(key)` helper from day one.
  The default and only locale shipped is English; the helper exists so
  that a future locale pack can be added without source changes.

## 12. Open Questions

- **Topology layout**: react-flow's auto-layout vs an explicit dagre
  pass for very large clusters. Defer until > 50 visible entities is a
  realistic scenario.
- **Activity log persistence**: client-only for v1. If the host product
  needs it, expose `onEvent` (the embedding callback) and let the host
  persist.
- **Per-component metrics retention**: currently in-memory only;
  acceptable for a console, with export to file available for demos.
- **Command palette performance**: For very large clusters (> 1000 entities),
  we need to ensure fuzzy search remains responsive. We may need to add
  pagination or incremental search if this becomes an issue.
- **Export file size limits**: For very large KV datasets or long activity logs,
  we may need to add streaming exports or size limits to avoid browser memory
  issues.
- **Custom panel isolation**: We need to ensure custom host panels don't leak
  styles or break the console's internal state. We may need to wrap custom
  panels in a shadow DOM or isolate their React context if security concerns
  arise.
