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

The SPA is a **single page with a fixed three-pane shell**:

```
┌─ Header ─────────────────────────────────────────────────────────┐
│ brand · cluster health pill · last-refresh · refresh · ⋯ menu     │
├─ Sidebar ──┬─ Canvas / Detail body ─────────┬─ Inspector ────────┤
│ Hierarchy  │                                │ Selection details  │
│ tree       │ React Flow topology  *or*      │ Metrics            │
│ + search   │ active feature panel:          │ KV browser         │
│            │   Snapshot · Racks · Nodes ·   │ Activity log       │
│            │   Servers · Stores · KV ·      │                    │
│            │   Swagger                      │                    │
└────────────┴────────────────────────────────┴────────────────────┘
```

- **Header** (fixed, ~56 px): brand, cluster health pill, last snapshot
  timestamp, manual-refresh button, server selector, overflow menu
  (theme toggle, Swagger panel toggle, About).
- **Sidebar** (collapsible, ~240 px): hierarchy tree backed by the latest
  snapshot. Selecting a node reflects in the inspector.
- **Body**: a tabbed feature surface. The default tab is the React Flow
  topology canvas; subsequent tabs are the existing feature panels
  (Racks, Nodes, Servers, Stores, KV, Swagger). The Swagger tab is an
  in-page panel — it does **not** open a new browser tab (this is the
  primary correction from the v1 implementation).
- **Inspector** (collapsible, ~320 px): contextual detail of the
  currently selected entity, with KV operations available when a Group
  is selected.

The shell is implemented once and never re-rendered when feature tabs
change; only the body region swaps.

## 4. Visual Language

### 4.1 Theme tokens
The palette is exposed as CSS variables under a `.crowkv-console` scope
so that hosts can override them through the `theme` mount prop.

| Token | Default (dark) | Use |
| --- | --- | --- |
| `--bg` | `#1f2937` | App background |
| `--panel` | `#111827` | Headers, sidebars, inspector |
| `--border` | `#374151` | Dividers |
| `--text` | `#f9fafb` | Primary text |
| `--muted` | `#9ca3af` | Secondary text |
| `--accent` | `#3b82f6` | Brand / interactive emphasis |
| `--healthy` | `#10b981` | Healthy / leader-ok |
| `--degraded` | `#f59e0b` | Degraded |
| `--failed` | `#ef4444` | Failed |
| `--unknown` | `#6b7280` | Unknown |
| `--remote` | `#8b5cf6` | Remote replica accent |

A light theme ships as a token override; the user toggle persists in
`localStorage`. The host's `theme` prop, when supplied, takes priority.

### 4.2 Status semantics

| State | Visual | Animation |
| --- | --- | --- |
| `Healthy` | solid border, accent-colored icon | none |
| `Degraded` | dashed border | slow pulse (~1.5 s) |
| `Failed` | thick border (2 px) | fast blink (~0.5 s), opt-out via "reduce motion" |
| `Unknown` | thin border, muted icon | none |
| `Leader` | solid border + crown badge + glow | slow pulse |
| `Follower (local)` | solid border | none |
| `Remote replica` | dashed border, `--remote` accent | none |

Animations respect `prefers-reduced-motion`; embedding hosts can disable
all animation through the theme contract.

### 4.3 Typography
System sans (Inter / Roboto / system-ui fallback). Title 20 px / 700,
section 14 px / 600, body 14 px / 400, meta 12 px / 400 in `--muted`.

## 5. Topology Canvas (React Flow)

### 5.1 Custom node types

| Layer | Shape | Required content | Status surface |
| --- | --- | --- | --- |
| Rack | Wide rounded rectangle, container | Rack id, name, child node count | summary of children |
| Node | Rectangle | Node id, host, SSH state | per-node status |
| Server Instance | Rectangle | Server id, mgmt URL, PID | server lifecycle state |
| Store | Compact pill | Store id, listen addr, group count | store status |
| Group | Compact pill | Group id, leader hint, replica count | leader / quorum status |
| Replica | Circle | Short replica id | local-vs-remote, health |

Hierarchy is rendered with parent-child containment where it reads well
(Rack contains Nodes; Group surfaces its replicas as adjacent satellites
linked by edges). React Flow's auto-layout is run once on data changes
and again on user-initiated "auto layout" actions.

### 5.2 Interactions
- Click selects; selection drives the inspector and the sidebar tree.
- `Ctrl/Cmd`-click multi-selects within a layer.
- Right-click opens a context menu whose actions are exactly the
  per-layer mutations from the requirement (`requirement-ui.md` §3.2,
  §3.3). Disabled actions show a tooltip explaining why.
- Drag pans; wheel zooms; mini-map and "fit view" controls in a corner.
- Tooltips on hover surface the most useful single fact (host, mgmt
  URL, leader id) without forcing the user to open the inspector.

## 6. Inspector Panel

A four-tab inspector that re-renders against the current selection:

1. **Details** — labelled key/value table sourced from the snapshot.
   Long values support copy-to-clipboard.
2. **Metrics** — Recharts line/area charts for the metrics that
   `ClusterSnapshot` exposes today (key count, RPC rates, replica RTT,
   etc.). The chart series is held in memory, sampled per snapshot
   poll, capped at a fixed window.
3. **KV** — only enabled when a Group is selected. Wraps `kvGet`,
   `kvScan`, `kvPut`, `kvDelete` from `web/ui/src/api.ts`. Confirms
   destructive operations.
4. **Activity** — chronological list of UI-issued operations with
   timestamp, action, target, outcome. v1 keeps this client-side only;
   a future revision can persist it on the backend.

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
    "snapshot" | "racks" | "nodes" | "servers" |
    "stores" | "kv" | "swagger" | "activity",
    boolean
  >>;
  theme?: ThemeOverride;       // CSS-variable overrides
  initialServer?: string;      // mgmt URL of an upstream crowkv-server
  onEvent?: (event: ConsoleEvent) => void; // structured op log fan-out
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
- A single root data hook owns the cluster snapshot, the registered
  server list, and a derived selection map keyed by entity id.
- Polling cadence is configurable; default ~5 s. Polling pauses while
  the tab is hidden (`document.visibilityState`) and resumes on focus.
- A poll failure surfaces a non-blocking banner ("backend unreachable —
  retrying"); the previous snapshot stays visible, marked stale.
- Per-tab data (rack list, node list, server list) is fetched on tab
  entry and refreshed against the same polling tick to avoid lag with
  the snapshot view.
- Mutations call the backend, await success, then trigger a snapshot
  refresh; they do not hand-edit the cached snapshot.

## 10. Module Layout (`crowkv-console/web/ui/src/`)

```
src/
  index.tsx                  // standalone mount
  embed.ts                   // <CrowkvConsole /> export for hosts
  shell/
    Header.tsx
    Sidebar.tsx
    Inspector.tsx
    ThemeProvider.tsx
  topology/
    TopologyCanvas.tsx
    nodes/{Rack,Node,Server,Store,Group,Replica}Node.tsx
    layout.ts
  panels/
    SnapshotTab.tsx
    RacksTab.tsx             // physical tree, recursive=1 to inline children
    NodesTab.tsx             // physical tree; server lifecycle lives here
    StoresTab.tsx            // logical tree; multi-node create form
    GroupsTab.tsx            // logical tree; subset-of-store-nodes create
    ReplicasTab.tsx          // logical add/remove; inspect opens physical detail
    KvPanel.tsx              // logical (store_id, group_id) only; no server selector
    SwaggerPanel.tsx         // iframe /api/swagger/?url=/api/nodes/:n/openapi.json
    ActivityLog.tsx
  data/
    api.ts                   // existing thin wrappers
    useSnapshot.ts
    useRegistry.ts
    selection.ts
  styles/
    tokens.css
    tailwind.css
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
  acceptable for a console, but a "save to file" affordance may be
  needed for demos.
