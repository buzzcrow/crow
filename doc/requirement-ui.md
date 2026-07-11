# CrowKV Console Web UI Requirements

Upstream: `requirement.md` §15 (the broader `crowkv-console` scope).
Downstream: `design/design-ui.md` (visual/interaction/implementation design).

This document is **requirements-only**: it states *what* the Web UI must
deliver. Pixel sizes, color tokens, component libraries, animation curves,
React module layout, and any other "how" choices live in `design-ui.md`.

## 1. Goals

The Web UI is a **single-page, embeddable cluster console** for a CrowKV
deployment. Concretely:

- **Single page**: one SPA shell, no full-page navigation. Every workflow
  (topology view, hardware lifecycle, KV ops, Swagger, logs) is reached by
  switching panels or opening overlays inside the same page.
- **Embeddable**: the SPA must be reusable as a sub-component of a larger
  product. Any system that links the `crowkv` library, exposes the
  `crowkv-server` HTTP/gRPC API surface, and runs the `crowkv-web` backend
  (or an API-compatible facade) can mount this UI as its cluster
  management view without code forks.
- **Demo-grade aesthetics**: the UI is shown to customers and operators.
  It must feel like a finished product, not a debugger.
- **Operator-grade utility**: every operation a CLI user can perform on
  the cluster (rack/node/server lifecycle, store/group/replica wiring, KV
  data plane, Swagger inspection) must be reachable from the UI with one
  to three clicks.

## 2. Non-Goals

- Authentication / authorization / RBAC (deferred everywhere in CrowKV).
- Mobile / phone form factor. Tablet (≥ 768 px) is best-effort; desktop
  (≥ 1200 px) is the supported target.
- Server-side rendering. The SPA is a static bundle served by
  `crowkv-web` (or a host system).
- Live streaming via WebSocket / SSE. Polling on a configurable interval
  is sufficient and matches the upstream HTTP-only API.
- Multi-tenant views, audit logs, billing — out of CrowKV scope.

## 3. Functional Requirements

The UI surfaces every endpoint that `crowkv-web` already exposes
(`crowkv-console/web/src/lib.rs`). Each row below is a hard requirement;
the listed endpoint is the contract the UI is allowed to assume.

### 3.1 Cluster Observation

The UI exposes **two hierarchy views** of the cluster, as defined
normatively in `design/design-console.md` §3. Both views describe the
same underlying entities but differ in viewpoint, and the UI must
keep them **first-class** — a user must be able to switch between
them, and never sees a single flattened tree that mixes the two.

- **Physical (deployment) view** — rooted at **Rack → Node**:
  `Rack → Node → Server → PxStore → PxGroup → { LocalReplica, RemoteReplica… }`.
  Used to inspect what hardware exists, what is running on each node,
  and how that node has wired its peers (the explicit local-vs-remote
  split makes it the **debugging view**). Identity is the parent
  chain `(rack_id, node_id, store_id, group_id, replica_id)`.
- **Logical (usage) view** — rooted at **Cluster**:
  `Cluster → Store → Group → Replica…` with a **unified** replica
  list (each replica tagged with its `node_id`). Used for routine
  cluster operations and KV traffic; identity is
  `(store_id[, group_id[, replica_id]])`.

The UI must:

- Provide a **view-mode toggle** (Physical ⇄ Logical) that drives the
  sidebar tree, the topology canvas, and the inspector simultaneously.
  Cross-jumping (e.g. "show this logical replica's physical detail")
  must be one click.
- Refresh both views from the per-resource live endpoints on a
  configurable polling interval (default: a few seconds). There is
  **no aggregate `/api/cluster/snapshot`** — the SPA reads each tree
  through its own per-resource endpoints, served from the backend's
  monitor cache.
- In the physical view, render **LocalReplica** and **RemoteReplica**
  with distinct visual treatment so an operator can spot peer-list
  mis-wirings at a glance. The current leader must also be
  distinguishable.
- In the logical view, render every replica in a single list with a
  `node_id` badge; do not surface the local/remote split here.
- Display **per-component status** (`Healthy / Degraded / Failed /
  Unknown`) for every node, server, store, group, and replica in
  whichever view is active.
- Keep a **cluster health summary** visible at all times (header /
  banner), independent of view-mode: an at-a-glance answer to "is
  anything wrong?".
- **Health history**: The cluster health summary includes an optional
  timeline view showing health trends over the last 1 hour / 1 day
  (configurable). The inspector's Metrics tab includes per-entity health
  history for the same window.
- **Topology enhancements**:
  - Search/highlight functionality in the topology canvas: users can search
    for an entity to have it centered and highlighted in the viewport
  - Multiple layout options for the topology canvas: force-directed
    (default), hierarchical, grid, with user selection persisted in
    `localStorage`
  - Focus mode: toggles visibility of all entities except the selected one
    and its direct peers, for simplified debugging of specific groups
  - Edge labels: replication edges between replicas show optional metrics
    (replication lag, throughput) at a glance, toggleable via a canvas
    control

### 3.2 Hardware Lifecycle (Physical-tree operations)
This section is the SPA's surface onto the **physical view** (§3.1).
The UI must drive the rack/node/server registry that `crowkv-web`
persists in `~/.crowkv/console.toml`. Servers are **addressed by the
hosting node** (one `crowkv-server` per node by console convention):

| Capability | Endpoints |
| --- | --- |
| List racks (with children) | `GET /api/racks[?recursive=<depth>]` |
| Add / get / remove a rack | `POST /api/racks`, `GET/DELETE /api/racks/:id` |
| List / add nodes under a rack | `GET POST /api/racks/:rack_id/nodes` |
| Flat node list / get / remove | `GET /api/nodes`, `GET/DELETE /api/nodes/:id` |
| Probe a node's reachability | `POST /api/nodes/:id/ping` |
| Deploy / start / stop the node's `crowkv-server` | `POST /api/nodes/:id/server/{deploy,start,stop}` |
| Get or drop the deployment record | `GET/DELETE /api/nodes/:id/server` |
| Proxy the node's `OpenAPI` document | `GET /api/nodes/:id/openapi.json` |

Form input must validate against the same constraints the backend enforces
(non-empty IDs, IP/host format, port range, SSH credential exclusivity)
and surface server-side validation errors inline.

### 3.3 Cluster Management (Logical-tree operations)
This section is the SPA's surface onto the **logical view** (§3.1).
The UI must drive the cluster-wide store/group/replica plane.
Endpoints are rooted at `/api/stores/...`; the backend aggregates
state from the monitor cache (§4.2 of `design-console.md`) and
orchestrates writes via the per-node primitives under
`/api/nodes/:n/stores/...`.

| Capability | Endpoints |
| --- | --- |
| List / add / inspect / remove stores | `GET POST /api/stores`, `GET DELETE /api/stores/:store_id` |
| List / add / remove groups | `GET POST /api/stores/:store_id/groups`, `GET DELETE /api/stores/:store_id/groups/:group_id` |
| List / add / remove replicas (unified local+remote) | `GET POST /api/stores/:s/groups/:g/replicas`, `GET DELETE /api/stores/:s/groups/:g/replicas/:replica_id` |

Replica `add`/`remove` is orchestrated: the backend creates the local
`PxGroup` on the target node, wires it bidirectionally with every
existing peer via the per-node `remotes` primitives, and rolls back on
partial failure. The UI exposes a single `ReplicasTab` that drives both
the first replica (created with the group) and every subsequent add.

For debugging, the physical tree exposes per-node detail at
`GET /api/nodes/:n/stores/:s/groups/:g` (showing `local` + `remotes`)
which the UI surfaces behind an "inspect" action on each replica.

### 3.4 KV Data Plane
All KV endpoints are rooted at `/api/stores/:store_id/groups/:group_id/kv/...`.
Leader resolution is performed server-side via the monitor cache; the
UI does not pass a node/server hint.

- **Browse keys** via `GET .../kv/scan?prefix=&limit=` (prefix filter +
  result limit exposed in the UI).
- **Read a key** via `GET .../kv/get?key=`. Both the value and its size
  are displayed; value contents are shown verbatim (this is an internal
  demo console; values are not protected).
- **Write a key** via `POST .../kv/put` (idempotent overwrite).
- **Delete a key** via `POST .../kv/delete`, with confirmation.

Operations must report success or failure inline; failed operations must
show the server-supplied error.

### 3.5 OpenAPI / Swagger Inspection
- The Swagger UI is a **panel inside the SPA**, not a separate browser
  page. Selecting it does **not** open a new tab and does not navigate
  away from the rest of the console.
- The user picks a target `node` from the registered nodes; the
  embedded Swagger UI loads that node's `crowkv-server` OpenAPI
  document via `GET /api/nodes/:node_id/openapi.json` (proxied by
  `crowkv-web`). The iframe `src` is
  `/api/swagger/?url=/api/nodes/:node_id/openapi.json`.
- Switching the selected node reloads the OpenAPI document inside the
  panel without disturbing the rest of the page.
- The Swagger UI bundle itself is served offline by `crowkv-web` from
  `crowkv-console/web/swagger-ui/`. The UI must not depend on internet
  connectivity at runtime.

### 3.6 API Routing Rule (normative)

The console exposes **two parallel URL trees**, one per hierarchy view,
and no route crosses trees:

- **Physical** (`/api/racks`, `/api/nodes`): addressed by the parent
  chain (`rack_id`, `node_id`). Used by the hardware-lifecycle and
  debugging views.
- **Logical** (`/api/stores`): addressed by `(store_id, group_id,
  replica_id)`. Used by the cluster-management and KV views.

Concretely:

- The SPA does not pass `?server=<url>` (or any other upstream-URL
  parameter) to the backend. That contract is gone.
- Upstream `crowkv-server` instances are addressed by `node_id`; the
  backend resolves `node_id → mgmt_url` from `~/.crowkv/console.toml`.
- The header selector (where present) chooses a `node_id` and is
  consumed **only** by the Swagger panel; the rest of the UI
  operates on logical ids.
- Every `GET` accepts `?recursive=<depth|all>` (see §6.5 of
  `design/design-console.md`).

> **Migration note:** earlier prototypes used `?server=<mgmt_url>` and
> the `/api/servers/:sid/...` namespace. Both are **removed**; old
> URLs return `404`. See `design/design-console.md` §6 for the full
> route table and `doc/todo_api.md` for the migration phase list.

### 3.7 Operation Visibility
- The UI must surface the success / failure of every action it issues.
  Errors must be readable (no opaque HTTP 500 traces).
- A view of recent UI-initiated operations (timestamp, action, target,
  outcome) must be reachable from the SPA. It is acceptable for this view
  to be backed entirely by client-side state for v1.
- **Toast notifications**: Operation success/failure is shown via
  unobtrusive toast notifications (stacked in a corner) instead of blocking
  banners, with a dismiss action and a link to the activity log for details.
- **Inline form validation**: All form inputs validate in real-time as the
  user types, with clear error messages shown inline before the form is
  submitted.
- **Bulk operations**: The UI supports multi-selection of entities in the
  sidebar tree and topology canvas, with bulk actions applicable to all
  selected items (start/stop servers, delete replicas, add nodes to stores).
  Bulk operations show a summary confirmation dialog before execution, and
  report per-item success/failure in the activity log.

### 3.8 Navigation & Productivity
The UI must include productivity features to speed up common workflows for
both new and power users:
- **Command palette**: Accessible via `Cmd/Ctrl+K` global shortcut, allowing:
  - Fuzzy search across all cluster entities (racks, nodes, stores, groups,
    replicas) by ID or name
  - Quick execution of common actions (deploy server, create store, add
    replica) without navigating through menus
  - One-click switching between view modes (Physical/Logical) and feature
    panels
  - Jump directly to KV operations for a specific group
- **Breadcrumb navigation**: A persistent breadcrumb trail in the header
  showing the full hierarchy path of the current selection, allowing
  one-click navigation to any parent entity.
- **Favorites & recent items**:
  - Users can pin frequently accessed entities to a "Favorites" section at
    the top of the sidebar
  - A "Recently viewed" section shows the last 10 accessed entities for
    quick navigation
  - Favorites and recent items are persisted across sessions in
    `localStorage`
- **Filter & sort controls**:
  - All entity lists (sidebar tree, replica lists, KV scan results) support
    filtering by status (healthy/degraded/failed), role (leader/follower),
    or ID/name
  - All entity lists support sorting by ID, name, status, or health score
  - Custom filter/sort presets can be saved and reused for frequent
    workflows

### 3.9 Export & Sharing
The UI must support exporting cluster state and operation data for
documentation, demos, and audit purposes:
- **Topology export**: Export the current topology canvas view as SVG or
  PNG, including all status indicators and labels
- **Data export**: Export KV scan results, replica lists, or node lists as
  JSON or CSV
- **Activity log export**: Export the full activity log as CSV, including
  timestamps, actions, targets, and outcomes
- **Health report export**: Generate and export a PDF cluster health report
  including summary status, entity health breakdown, and recent metrics

## 4. Embeddability Requirements

The Web UI is intended to be the cluster-management panel inside any
larger product that already uses CrowKV. To make that practical:

- **Single mountable component** with a stable, documented public
  interface. Embedding must not require forking the SPA source.
- **Configurable at mount time** with at least the following knobs:
  - `apiPrefix` — the path under which `crowkv-web`-compatible APIs are
    reachable (default `/api`).
  - `basePath` — the route prefix the SPA should consider its root.
  - `readonly` — when set, all mutating actions are hidden or disabled.
  - `modules` — opt-in/opt-out of feature areas (snapshot, racks, nodes,
    stores, groups, replicas, kv, swagger, logs). Server lifecycle is
    part of the `nodes` module since each node hosts at most one
    `crowkv-server`.
  - `theme` — color palette / typography overrides supplied by the host.
- **Style isolation**: the UI must not leak global CSS into the host page
  or rely on global styles defined by the host.
- **Routing isolation**: the UI must not assume control of the browser's
  top-level URL. A host that already uses a router must be able to mount
  the SPA on a sub-path without conflict.
- **Asset isolation**: the bundle must be self-contained — every
  dependency (icons, fonts, Swagger UI assets) is shipped with the UI or
  fetched from `apiPrefix`-relative paths, never from third-party CDNs.

The UI must also be runnable standalone — i.e. served directly by
`crowkv-web` without a host system — for development and customer demos.

Additional embeddability features:
- **Custom branding**: Embedding hosts can supply a custom logo and brand
  color via the mount props, which replaces the default CrowKV logo in the
  header and adjusts the accent color palette.
- **Custom actions**: Embedding hosts can inject custom context menu items
  and inspector actions via the mount props, which are passed to the
  `onEvent` callback when triggered.
- **Custom metrics panels**: Embedding hosts can inject custom metrics chart
  components into the inspector, which receive the current selection and
  polling data as props.
- **System theme detection**: The UI automatically switches between light
  and dark themes based on the user's OS preference, unless explicitly
  overridden by the user or host theme prop.

## 5. Scope of Resources Managed

The UI manages the same set of entities through **two views** of one
underlying cluster (see §3.1 and `design/design-console.md` §3):

```
Physical (deployment) view              Logical (usage) view
────────────────────────────            ────────────────────
Rack                                    Cluster
└── Node                                └── Store
    └── Server   (≤ 1 per node)             └── Group
        └── PxStore                             └── Replica…
            └── PxGroup                             (unified; each
                ├── LocalReplica                     tagged with
                └── RemoteReplica…                   node_id)
```

Mapping between the two views (which the SPA must support as
one-click cross-jumps):

- Each `Replica` in the logical view corresponds to **one** `LocalReplica`
  on the node identified by its `node_id` plus **N − 1** `RemoteReplica`
  proxies on the peer nodes of the same group.
- Each `Store` / `Group` in the logical view is the aggregate of the
  per-node `PxStore` / `PxGroup` records returned by the physical
  tree.

Constraints:

- The "≤ 1 server instance per node" rule is a console-level rule
  (not a CrowKV core limitation); the UI enforces it by hiding the
  deploy action when a node already hosts an instance.
- The UI must not surface upstream `host:port` strings as identifiers;
  servers are addressed only by `node_id`.

## 6. Read-Only Mode and Safety

- When mounted with `readonly=true`, every mutating control (create /
  delete / deploy / stop / put / delete-key / register / unregister) is
  hidden. Read-only mode must not break any view.
- All destructive operations (delete rack/node/server/store/group/replica,
  delete key, stop server) require an explicit confirmation step.
- The UI must never silently retry a destructive operation after a
  failure. The user is responsible for re-issuing the action.

## 7. Performance & Robustness Targets

- The SPA must remain responsive while the snapshot poll is in flight; it
  must not freeze the UI on slow backends.
- An unreachable backend must surface a visible error and continue
  attempting to recover; the UI must not require a manual page reload to
  resume after the backend comes back online.
- The Swagger panel must load lazily (only when the user opens it) so
  that the initial page load is not blocked by Swagger UI assets.

## 8. Out of Scope (for now)

- Persistent client-side state beyond user preferences (selected server,
  theme).
- Multi-cluster federation views.
- Editable cluster topology diagrams (drag-to-reconnect of replicas).
- Live log tails of `crowkv-server` processes (covered separately by the
  SSH lifecycle plan).
- `--prefix` KV browsing beyond what `crowkv-server` natively supports.
