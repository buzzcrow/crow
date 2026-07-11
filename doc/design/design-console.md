# CrowKV Console Design

Upstream: `doc/requirement.md` §15 (the `crowkv-console` component
overview) and `doc/requirement-ui.md` (Web UI requirements).
Sibling: `doc/design/design-ui.md` (frontend SPA design).

## 1. Goals and Non-Goals

### Goals
- Single workspace project `crowkv-console` delivering a Web UI and a CLI that share one Rust core.
- Operate against any number of `crowkv-server` instances via their public surfaces (HTTP management API + gRPC KV / health).
- Model a **Rack → Node → Server Instance → Store → Group → Replica** hierarchy, including a "simulated hardware" mode that runs entirely on `127.0.0.1`.
- Host the Swagger UI static bundle in `crowkv-web` (one pinned offline release); the OpenAPI document shown inside it is proxied from the user-selected `crowkv-server`, so the SPA can inspect a specific server's API even though all servers of the same version produce the same doc.

### Non-Goals
- Bypassing `crowkv-server` to talk to Paxos / WAL / storage internals.
- Authentication, authorization, multi-tenancy, audit logging.
- Persisting console state beyond local config files.

## 2. High-Level Architecture

`crowkv-console` is **one project** (one top-level directory `crowkv-console/`), shipped as several small libs and binaries inside the existing Cargo workspace. No `crates/` subdirectory; every name starts with `crowkv-`.

```
crowkv-console/
  cli/           (bin)   `crowkv` clap-based CLI; depends on shared
  shared/        (lib)   data models, HTTP+gRPC clients, registry, aggregator, error model, SSH session pool, workload generator
  web/           (bin)   Axum backend, static asset server, Swagger UI mount, proxy routes
    src/                  Rust source
    ui/                   React + Vite frontend source (TS, shadcn/ui, React Flow)
    swagger-ui/           committed Swagger UI assets (one pinned version, served by crowkv-web)
    tests/                integration tests
```

Targets:
- `shared` → reusable lib for both frontends.
- `web` → bin (`crowkv-web`), serves UI + API on `:9920`.
- `cli` → bin (`crowkv`), the user-facing CLI.

### 2.1 Call Path

Every console operation follows the same path. The frontend (web SPA
backed by Axum, **or** the `crowkv` CLI binary) is a thin presentation
layer; it always calls into `shared`, and `shared` is the only place
that talks to `crowkv-server` over HTTP / gRPC / SSH.

```
                ┌──────────────┐        ┌──────────────┐
   user ───►    │  crowkv-web  │   or   │ crowkv (CLI) │     (frontend)
                └──────┬───────┘        └──────┬───────┘
                       │   parse input,        │
                       │   render output       │
                       └──────────┬────────────┘
                                  ▼
                          ┌───────────────┐
                          │    shared     │              (business logic:
                          │  (lib crate)  │               monitor cache,
                          └──────┬────────┘               orchestration,
                                 │                        leader resolution,
                  ┌──────────────┼──────────────┐         SSH session pool)
                  ▼              ▼              ▼
               HTTP            gRPC            SSH
                  │              │              │
                  ▼              ▼              ▼
              ┌────────────────────────────────────┐
              │           crowkv-server            │     (one per node)
              └────────────────────────────────────┘
```

### 2.2 Reuse Boundary

- All "what to do" lives in `shared` (e.g. `add_group`,
  `deploy_server`, `kv_put`, `refresh_node`).
- `web` (Axum) and `cli` (clap) only parse input and render output.
- The web SPA **does not** reimplement business logic; it calls
  `shared` via the Axum backend, never `crowkv-server` directly.
- Both frontends share the same `shared` entry points, so any feature
  is reachable from both surfaces by construction.

## 3. Data Model

The console exposes **two hierarchy views** of the same cluster. Both
views describe the same underlying entities; they differ only in the
direction from which the cluster is observed.

### 3.1 Physical (deployment) view

> "What hardware exists, and what is running on each piece of it."

```
Rack
└── Node                              (one host, one OS user)
    └── Server                        (0 or 1 crowkv-server process per node)
        └── PxStore                   (per-node store instance)
            └── PxGroup               (per-node group instance)
                ├── LocalReplica      (the one replica that lives on this node)
                └── RemoteReplica…    (proxies of peer replicas on other nodes)
```

Properties of this view:

- Rooted at **Rack → Node**. Every entity below `Node` is described
  *from that node's vantage point*.
- A `PxGroup` on a node has exactly **one local replica** plus
  **N − 1 remote-replica proxies** (one per peer node in the group).
  This mirrors the `crowkv-server` internal data structure verbatim,
  which is why this view is also the "debugging view": the API
  surfaces the remote-list explicitly, so an operator can spot bugs
  where a node failed to register all of its peers.
- Identity in this view is the parent chain
  `(rack_id, node_id, store_id, group_id, replica_id)`. Listing a
  resource always takes the parent id as input.

### 3.2 Logical (usage) view

> "What stores and groups exist in the cluster, regardless of where
> they live."

```
Cluster                               (the deployment as a whole)
└── Store                             (logical, may span any subset of nodes)
    └── Group                         (Paxos consensus group)
        └── Replica…                  (unified list; each tagged with node_id)
```

Properties of this view:

- Rooted at the **cluster**. A cluster can host **many stores**
  (multi-tenant); there is no separate `StoreContainer` entity — the
  set of all stores *is* the cluster's store catalogue.
- A `Group`'s replica list is **unified**: no local / remote split.
  Each `ReplicaView` carries a `node_id` so the caller can map a
  logical replica back to its host.
- Identity in this view is `(store_id[, group_id[, replica_id]])`.
- This is the view that KV traffic, leader resolution, and routine
  cluster operations use. The web backend is the only component that
  needs to translate logical ids into upstream `(node_id, mgmt_url,
  grpc_url)` tuples; the SPA and the CLI never see those.

### 3.3 Rust types

```rust
// crowkv-console-shared::model

// ── Physical view (rack / node / server / per-node store-group-replica) ──

struct Rack  { id: RackId,  name: Option<String>, nodes: Vec<NodeId> }

struct Node {
    id: NodeId, rack_id: RackId,
    host: String,                         // 127.0.0.1 by default
    ssh: SshCreds,                        // user + (password|key)
    server: Option<ServerProcess>,        // 0 or 1, console-enforced
}

struct ServerProcess {
    mgmt_url: String,                     // intended endpoint; persisted
    grpc_url: String,                     // intended endpoint; persisted
    pid: Option<u32>,                     // live cache; refreshed by monitor
    state: ProcState,                     // live cache: Stopped|Starting|Running|Failed
    health: NodeHealth,                   // live cache: Up|Down|Unknown
    last_seen_ms: u64,                    // live cache
}

enum SshCreds {
    KeyDefault { user },
    KeyPath    { user, key_path },
    Password   { user, pass },
}

/// Per-node store entry: what `crowkv-server` reports for one PxStore.
struct NodeStore {
    node_id: NodeId,
    store_id: StoreId,
    groups: Vec<NodeGroup>,
}

/// Per-node group entry, including the `LocalReplica` + remote proxies
/// exactly as the server-side data structure holds them. Used by the
/// physical/debugging API; the logical view collapses these into
/// `ReplicaView`.
struct NodeGroup {
    node_id: NodeId,
    store_id: StoreId,
    group_id: GroupId,
    local: LocalReplicaInfo,
    remotes: Vec<RemoteReplicaInfo>,
    leader_hint: Option<ReplicaId>,
}

struct LocalReplicaInfo  { replica_id: ReplicaId, role: ReplicaRole, state: ReplicaState }
struct RemoteReplicaInfo { replica_id: ReplicaId, node_id: NodeId,   reachable: bool }

// ── Logical view (cluster-wide store / group / replica) ─────────────

struct StoreView {
    store_id: StoreId,
    name: Option<String>,
    nodes: Vec<NodeId>,                   // every node hosting this store
    groups: Vec<GroupSummary>,
}

struct GroupView {
    store_id: StoreId,
    group_id: GroupId,
    leader: Option<ReplicaId>,            // current leader hint
    replicas: Vec<ReplicaView>,           // unified — no local/remote split
    state: GroupHealth,
}

struct ReplicaView {
    replica_id: ReplicaId,
    node_id: NodeId,                      // where this replica runs
    role: ReplicaRole,                    // Leader | Follower
    state: ReplicaState,
}
```

### 3.4 Source of truth and freshness

- **Persisted (config file, see §4):** `Rack`, `Node`, `SshCreds`, and
  the *intended* `ServerProcess` deployment record (host, ports,
  binary path). These are the only things the console owns; they
  survive restart.
- **Live (rebuilt on every console start):** `ProcState`, `NodeHealth`,
  every `NodeStore` / `NodeGroup`, and every `StoreView` / `GroupView` /
  `ReplicaView`. After loading the persisted rack/node table, the
  console launches a **monitor task** (§4) that pings each node and,
  for healthy nodes, fetches the per-node store/group/replica state.
  The logical view is derived by aggregating those per-node reports.
- **No `ClusterSnapshot` polling endpoint.** The SPA does **not** poll
  one giant snapshot; it queries the live endpoints under §6 and
  relies on the backend's monitor cache for low-latency reads.

### 3.5 Notes

- **No `server_id` namespace.** The server's mgmt/gRPC URLs live
  inside `Node.server` and are **never** exposed in console-facing
  JSON URLs; they are an internal routing detail of `shared`. Since
  the console enforces one server per node, node identity *is* server
  identity from the console's point of view.
- **Local / remote split is visible only in the physical view.** The
  logical view collapses each group's replicas into a unified list so
  cluster-level operations can ignore where a replica happens to live.
  The physical view keeps the split so an operator can debug missing
  peer registrations (e.g. a node that failed to add all its remotes
  after a flaky join).
- `StoreView` / `GroupView` / `ReplicaView` reuse `crowkv::cluster::info`
  where possible; the console-side wrapper adds the `node_id`
  projection that the per-server protocol does not encode.

> **One server per node**: enforced by UI and `shared`. Lower layers
> (`crowkv-server`, tests) may run multiple; console operations reject
> a second deploy on the same node.

## 4. Console Backend Persistence and Monitor Task

### 4.1 Persisted state (config file)

- Single TOML file: `log/crowkv-console-db.toml` in the project root
  (override with `$CROWKV_CONSOLE_CONFIG`).
- Contents:
  - `rack` / `node` entries (id, rack_id, host, SSH creds).
  - Optional per-node server deployment record: management endpoint,
    gRPC endpoint, and binary/config path as implementation evolves.
    This records the operator's intended deployment target, not
    authoritative live state.
- **Plaintext** SSH credentials are acceptable for v1 (internal demo);
  a single `ConsoleConfig` struct is the only place that reads /
  writes the file, so a future move to OS keychain or libsodium
  sealed-box does not touch any caller.
- **Never persisted:** live process state, per-node store/group/
  replica state, leader hints, health flags. These are rebuilt on
  every console start.

### 4.2 Monitor task

On startup, after loading the rack/node table, `shared` spawns a
long-running monitor task that owns the live cache:

1. **Ping loop** — every `monitor.ping_interval` (default 2 s), the
   task probes each node's `/health` over HTTP (and SSH liveness on
   demand for the lifecycle API). It updates `NodeHealth` and
   `ProcState` in the cache.
2. **Monitor refresh** — for every node observed `Up`, the task
   calls the server's topology-report API to fetch `NodeStore` /
   `NodeGroup` data (per-node store, group, local replica, remote
   list). The aggregated `StoreView` / `GroupView` / `ReplicaView`
   needed by the logical API are derived from these per-node reports.
3. **Event-driven refresh** — every successful mutation through
   `shared` (deploy, store create, group create, replica add/remove)
   triggers an immediate refresh for the affected nodes so the next
   read reflects the change without waiting for the next ping tick.
4. **Cache reads are non-blocking.** API handlers read the most
   recent cached value; they do not issue an upstream RPC per
   request. A handler that needs a stronger guarantee ("force fresh")
   can request an inline refresh, but that is the exception.

## 5. Node Access Model

### 5.1 Two transports per node
| Purpose | Transport |
| --- | --- |
| Deploy / start / stop `crowkv-server` process; copy binary | SSH |
| Runtime mgmt API (add store/group, list, health) | HTTP |
| Runtime KV ops, paxos health | gRPC |

### 5.2 SSH defaults (russh)
- Crate: **`russh`** (pure Rust, async). No shell-out fallback.
- Default auth: `~/.ssh/*` keys (agent + standard key paths).
- Alternative auth: explicit key path; explicit `user/password`.
- Default host: `127.0.0.1` with the current OS user.
- Pre-flight: every operation calls `ssh::probe(node)` which performs a real handshake before any side-effecting work. Failure surfaces as `NodeUnreachable { node_id, reason }`.


### 5.3 Process lifecycle (deploy / start / stop)
1. SSH into node.
2. Ensure binary exists at `/opt/crowkv/bin/crowkv-server` (scp on first deploy).
3. Render config template; write to `/opt/crowkv/etc/<node-id>.toml`.
4. `nohup crowkv-server --config ... &`; capture pid; record in the persisted node server entry.
5. Health-check via the new server's HTTP `/health` until ready or timeout.

`server start` and `server stop` address a node. There is no separate
server id namespace in the console API.

## 6. Web UI Backend (Axum)

### 6.1 Design Rules

The console-facing API is split along the **two hierarchy views**
defined in §3, and every route lives under exactly one of them. The
rules below are normative and override any earlier route layout in
this document.

**R1. Two URL trees, one per hierarchy.**
- `/api/racks/...` and `/api/nodes/...` form the **physical**
  (deployment) tree. Every resource is addressed by its parent chain
  in the rack→node→server→store→group→replica hierarchy.
- `/api/stores/...` forms the **logical** (usage) tree. KV traffic
  and cluster-wide store/group/replica operations live here, addressed
  by `(store_id[, group_id[, replica_id]])`. Logical-tree **responses**
  still carry `node_id` on every store / group / replica entry so a
  caller can see where each piece lives without issuing a physical-
  tree query; only the **path** is node-free.
- A route never crosses trees. The physical tree never accepts a bare
  `store_id`; the logical tree never accepts a `node_id` in the path
  (it appears only inside request/response bodies, e.g. "place this
  store on these nodes").

**R2. Logical reads aggregate; physical reads are per-node.**
The same store, observed through the two trees, returns different
shapes:
- `GET /api/stores/:store_id` → aggregated `StoreView` (every node
  hosting the store, group summary across the cluster).
- `GET /api/nodes/:node_id/stores/:store_id` → that node's local
  `NodeStore` (its groups, local replica, full remote list).
This is how the operator inspects "is the cluster consistent?" vs.
"what does this one node think it has?".

**R3. Logical writes orchestrate; physical writes act on one node.**
- A logical write (`POST /api/stores`, `POST /api/stores/:s/groups`,
  `POST /api/stores/:s/groups/:g/replicas`) declares *intent*; the web
  backend fans out the per-node calls in `shared` and rolls back on
  partial failure. The SPA / CLI issue **one** request and observe
  **one** logical result.
- A physical write (`POST /api/nodes/:n/stores`,
  `POST /api/nodes/:n/stores/:s/groups`,
  `POST /api/nodes/:n/stores/:s/groups/:g/remotes`)
  is the low-level primitive — it touches exactly that node, never
  fans out. Logical writes are implemented on top of these primitives.

**R4. No `server_id` namespace.**
Per-node process lifecycle, reachability probes, and Swagger UI
proxying use `/api/nodes/:node_id/server/...`. Since the console
enforces one server per node, node identity *is* server identity.

> **Retired contracts (must migrate, no compatibility shim):**
> - `?server=<mgmt_url>` query parameter — leaked upstream addresses.
> - `/api/servers/:sid/...` — folded into `/api/nodes/:node_id/server/...`.
> - `/api/openapi.json?server=<id>` — replaced by
>   `/api/nodes/:node_id/openapi.json`.
> - `/api/cluster/snapshot` — removed; the SPA reads the per-resource
>   live endpoints below, all served from the monitor cache (§4.2).

### 6.2 Path identifiers

- `:rack_id`, `:node_id` — strings owned by `ConsoleConfig`
  (persisted, see §4.1).
- `:store_id`, `:group_id`, `:replica_id` — cluster-wide ids in their
  respective namespaces (`store_id` unique across the cluster;
  `group_id` unique within a store; `replica_id` unique within a
  group).

### 6.3 Physical tree — `/api/racks/...`, `/api/nodes/...`

Resources in this tree are addressed by parent chain. Every list /
get / add / remove takes the parent id as input, exactly as called
out in §3.1.

#### 6.3.1 Liveness

| Method | Path | Purpose |
| --- | --- | --- |
| GET | `/healthz` | Web backend liveness (the console process itself). |

#### 6.3.2 Rack

| Method | Path | Purpose |
| --- | --- | --- |
| GET    | `/api/racks`               | List racks. |
| POST   | `/api/racks`               | Create rack. Body `{ rack_id, name? }`. |
| GET    | `/api/racks/:rack_id`      | Rack detail (with child node ids). |
| DELETE | `/api/racks/:rack_id`      | Delete rack (409 if it still has nodes). |

#### 6.3.3 Node (child of rack)

| Method | Path | Purpose |
| --- | --- | --- |
| GET    | `/api/racks/:rack_id/nodes`            | List nodes in this rack. |
| POST   | `/api/racks/:rack_id/nodes`            | Create node. Body `{ node_id, host, ssh: { … } }`. |
| GET    | `/api/nodes`                            | List nodes across all racks (flat view; convenience). |
| GET    | `/api/nodes/:node_id`                   | Node detail (rack id, host, ssh summary, server status, health). |
| DELETE | `/api/nodes/:node_id`                   | Delete node (409 if a server is running or replicas live there). |
| POST   | `/api/nodes/:node_id/ping`              | Force inline SSH + HTTP reachability probe (out-of-cycle for the monitor task). |

#### 6.3.4 Server process (child of node)

| Method | Path | Purpose |
| --- | --- | --- |
| GET    | `/api/nodes/:node_id/server`         | Runtime info (`mgmt_url`, `grpc_url`, `pid`, `state`, `health`); 404 if not deployed. |
| POST   | `/api/nodes/:node_id/server/deploy`  | Deploy and start. Body `{ mgmt_port, grpc_port, binary? }`. Idempotent — returns the existing process if already deployed and matches. |
| POST   | `/api/nodes/:node_id/server/start`   | Start (re-start) an already-deployed process. |
| POST   | `/api/nodes/:node_id/server/stop`    | Stop the process. The deploy artefacts remain. |
| DELETE | `/api/nodes/:node_id/server`         | Stop and remove the deployment record (409 if replicas still live there). |
| GET    | `/api/nodes/:node_id/openapi.json`   | Reverse-proxy that node's `/openapi.json`. |

#### 6.3.5 Per-node store / group / replica (debugging view)

These read-only views expose what `crowkv-server` reports for one
node — including the `local` replica plus the full `remotes` list —
so an operator can diagnose mis-wired peers (e.g. a node missing a
remote-replica registration).

| Method | Path | Purpose |
| --- | --- | --- |
| GET    | `/api/nodes/:node_id/stores`                                                       | List `NodeStore`s on this node. |
| GET    | `/api/nodes/:node_id/stores/:store_id`                                             | Per-node store detail (its groups). |
| GET    | `/api/nodes/:node_id/stores/:store_id/groups`                                      | Per-node group list. |
| GET    | `/api/nodes/:node_id/stores/:store_id/groups/:group_id`                            | Per-node group detail: `local` replica + `remotes` list, leader hint, reachability. |

Low-level mutators (the primitives behind the logical orchestrator):

| Method | Path | Purpose |
| --- | --- | --- |
| POST   | `/api/nodes/:node_id/stores`                                                       | Create one local `PxStore` on this node. Body `{ store_id, name? }`. |
| DELETE | `/api/nodes/:node_id/stores/:store_id`                                             | Delete the local `PxStore` from this node. |
| POST   | `/api/nodes/:node_id/stores/:store_id/groups`                                      | Create one local `PxGroup` on this node. Body `{ group_id, replica_id, role? }`. The remote-replica list is wired separately. |
| DELETE | `/api/nodes/:node_id/stores/:store_id/groups/:group_id`                            | Delete the local `PxGroup` from this node. |
| POST   | `/api/nodes/:node_id/stores/:store_id/groups/:group_id/remotes`                    | Add a remote-replica entry to this node's group. Body `{ replica_id, node_id }`. |
| DELETE | `/api/nodes/:node_id/stores/:store_id/groups/:group_id/remotes/:replica_id`        | Remove a remote-replica entry. |

> **The physical mutators are not the SPA's primary surface.** They
> exist for low-level scripting and for the orchestrator in §6.4 to
> call. The CLI exposes them under a `crowkv node ...` namespace for
> operators who need to bypass the orchestrator (rare).

#### 6.3.6 Swagger UI (assets served by the console)

| Method | Path | Purpose |
| --- | --- | --- |
| GET | `/api/swagger/*` | Vendored Swagger UI bundle from `crowkv-console/web/swagger-ui/` (offline, one pinned release). The SPA mounts this in an in-page panel and points Swagger UI's `url` parameter at `/api/nodes/:node_id/openapi.json` for the selected node. |

### 6.4 Logical tree — `/api/stores/...`

Logical writes are *orchestrated*: a single request expands into
multiple per-node primitives, with rollback on partial failure (see
§6.6). Logical reads are *aggregated* from the monitor cache.

#### 6.4.1 Store

| Method | Path | Purpose |
| --- | --- | --- |
| GET    | `/api/stores`               | List stores aggregated across the cluster (`store_id`, `name?`, `nodes`, `group_count`). |
| POST   | `/api/stores`               | Create a store. Body `{ store_id, name?, nodes: [node_id, …] }`. The backend ensures every listed node has a running server, calls `POST /api/nodes/:n/stores` on each, and rolls back on partial failure. |
| GET    | `/api/stores/:store_id`     | Aggregated `StoreView` (member nodes, groups summary). |
| DELETE | `/api/stores/:store_id`     | Delete the store across every hosting node; idempotent on per-node 404. |

#### 6.4.2 Group

| Method | Path | Purpose |
| --- | --- | --- |
| GET    | `/api/stores/:store_id/groups`            | List groups in the store. |
| POST   | `/api/stores/:store_id/groups`            | Create group. Body `{ group_id, nodes: [node_id, …], leader_node? }`. `nodes` is a subset of the store's node set. The backend creates a local `PxGroup` on each listed node and wires every replica's remote-list, producing one consistent `PxGroup`. |
| GET    | `/api/stores/:store_id/groups/:group_id`  | Aggregated `GroupView` (`replicas`, `leader`, `state`). |
| DELETE | `/api/stores/:store_id/groups/:group_id`  | Delete the group across every hosting node. |

#### 6.4.3 Replica

| Method | Path | Purpose |
| --- | --- | --- |
| GET    | `/api/stores/:store_id/groups/:group_id/replicas`               | Unified replica list (`replica_id`, `node_id`, `role`, `state`). |
| POST   | `/api/stores/:store_id/groups/:group_id/replicas`               | Add a replica. Body `{ node_id, replica_id? }`. Orchestrates: (a) create local replica on `node_id`; (b) register the new replica as a remote on every existing replica; (c) register every existing replica as a remote on the new one. Roll back on any step failure. |
| GET    | `/api/stores/:store_id/groups/:group_id/replicas/:replica_id`   | Replica detail (logical view; for debugging detail use the physical tree). |
| DELETE | `/api/stores/:store_id/groups/:group_id/replicas/:replica_id`   | Remove a replica: (a) deregister it as a remote from every other replica; (b) delete the local replica on its host. |

#### 6.4.4 KV data plane

| Method | Path | Purpose |
| --- | --- | --- |
| GET    | `/api/stores/:store_id/groups/:group_id/kv/get?key=` (or `key_hex=`)               | Read a key. The backend resolves the group → leader replica → routes the gRPC call. |
| GET    | `/api/stores/:store_id/groups/:group_id/kv/scan?prefix=&limit=` (or `prefix_hex=`) | Prefix scan via the group's leader. |
| POST   | `/api/stores/:store_id/groups/:group_id/kv/put`                                    | Idempotent put. |
| POST   | `/api/stores/:store_id/groups/:group_id/kv/delete`                                 | Delete. |

The web backend is responsible for **leader resolution** on every KV
call: it consults the monitor cache, picks the current leader, and
falls back to any healthy replica with `NotLeader` retry handled by
`shared`. The SPA never sees "leader" / "follower" in the URL.

### 6.5 Recursive reads (`?recursive=<depth>`)

Any `GET` list or detail endpoint in either tree accepts an optional
`?recursive=<n>` query parameter. It instructs the handler to inline
up to `n` child levels in a single response, avoiding an O(N) fan-out
of follow-up requests for UIs that render a whole sub-tree.

- **Default** (`recursive` absent or `0`): the handler returns just
  the requested resource — child collections are represented by their
  counts or ids only, as described in the route tables above.
- **`recursive=1`**: inline the **immediate children** as full
  resources. Example: `GET /api/racks/:rack_id?recursive=1` embeds the
  full `Node` objects instead of just node ids;
  `GET /api/stores/:store_id?recursive=1` embeds full `GroupView`s.
- **`recursive=2`, …**: each additional level expands the next child
  collection. Example: `GET /api/racks?recursive=3` returns
  `Rack → Node → Server → NodeStore` in one shot.
- **`recursive=all`** (alias for a backend-capped max depth, default
  8): expand every remaining child level. Intended for the SPA's
  initial render after startup; the cap exists to keep a single
  response bounded.

Rules:

- The parameter is **read-only**. `POST` / `DELETE` ignore it.
- Depth counts **child hops** from the addressed resource. A list
  endpoint (`GET /api/racks`) counts the rack itself as depth 0, so
  `recursive=1` inlines every rack's nodes.
- The two trees expand along their own hierarchy: physical tree
  expands `rack → node → server → store → group → {local, remotes}`;
  logical tree expands `store → group → replica`. KV key/value
  payloads are never inlined via `recursive`.
- The handler may return a partial expansion with a
  `truncated_at: [...]` hint if the cap is hit; callers then issue
  targeted follow-up reads for the truncated paths.
- All responses use the monitor cache, so `recursive` is cheap even
  at high depth — it does not trigger upstream RPCs.

### 6.6 Orchestration semantics

For each multi-node operation in the logical tree, the backend obeys
these rules:

- **Plan first, act second.** Resolve every required node + replica id
  from the monitor cache before issuing any upstream RPC.
- **Built on §6.3.5 primitives.** The orchestrator only calls the
  per-node physical mutators; it never invents a side channel. This
  keeps the two trees coherent.
- **All-or-nothing where feasible.** On partial failure, attempt to
  undo successful sub-steps (delete the freshly-created local replica,
  roll back peer remote-replica registrations) and surface the
  resulting state in the error body so the user can reconcile.
- **Idempotent retries.** A repeat of the same logical request (same
  ids, same node set) must converge to the same state — useful both
  for the SPA's optimistic retry and for scripted CLI workflows.
- **Cache refresh on success.** Every successful mutation triggers an
  immediate monitor refresh for the affected nodes (§4.2 step 3) so
  the next read reflects the change.

### 6.7 Resolution rules (handler side)

- Unknown `:rack_id` / `:node_id` / `:store_id` / `:group_id` /
  `:replica_id` → `404`.
- A node whose `crowkv-server` is unreachable for a physical-tree call
  → `502 Bad Gateway`, body
  `{ "error": "node unreachable", "node_id": "…" }`.
- A logical-tree multi-node operation that partially succeeded →
  `207 Multi-Status` is **not** used; the backend rolls back and
  reports `409 Conflict` with a structured `details: [...]` listing
  per-node outcomes, so the SPA / CLI can show what happened.
- All handlers are thin wrappers around `shared` entry points; the
  CLI calls the same entry points and shares the orchestration code.

### 6.8 Frontend
The frontend SPA design (stack, visual language, topology canvas,
embedding contract, panel layout, polling model) lives in
`design/design-ui.md`. The backend-facing contract here:

- Bundle output is `crowkv-console/web/ui/dist/`; `crowkv-web` serves it
  via the SPA fallback handler with deep-link support.
- The SPA polls **per-resource** live endpoints (e.g. `GET /api/racks`,
  `GET /api/nodes`, `GET /api/stores`, `GET /api/stores/:s/groups`)
  on a short interval over a long-lived HTTP/1.1 keep-alive (or
  HTTP/2) connection. **No WebSocket / SSE.** All such reads are
  served from the monitor cache (§4.2) so they are cheap; responses
  use `Cache-Control: no-store` and ETag so unchanged bodies skip the
  network where possible.
- There is no `/api/cluster/snapshot` aggregate endpoint. A view that
  needs many resources at once issues parallel reads.
- The Swagger panel embeds `/api/swagger/?url=/api/nodes/:node_id/openapi.json`
  for the node selected in the header (no new browser tab).

## 7. CLI Design

- Binary: `crowkv` (the noun-verb command structure is self-explanatory).
- Parser: `clap` derive; one module per top-level group, each verb a subcommand struct.
- Output: pretty table (`comfy-table`) by default; `--json` global flag for scripting.
- Global flags: `--console <url>` (default `http://127.0.0.1:9920`),
  `--config <path>`, `--json`, `-v` / `-vv`.
- Command hierarchy as defined in `requirement.md` §15.4.5 ("CLI Command Hierarchy").
- **One call path.** Every verb — including cluster observation
  (`cluster status/topology/inspect`) and `bench` — routes through
  `ConsoleClient` against `crowkv-web`. The CLI never talks to a
  `crowkv-server` mgmt API or registry directly; there is no
  `--server` flag.

### 7.0 Cluster observation (`cluster status/topology/inspect`)
- `cluster status` and `cluster topology` build their snapshot purely
  from console reads: `list_stores` (logical tree), `list_nodes` +
  `list_servers` (physical tree). This works against a web-managed /
  `--test-mode` cluster that has no persisted registry.
- `cluster inspect <id>` resolves a single entity. **Id grammar:**
  - `s<store_id>` → store detail (`get_store`).
  - `s<store_id>/g<group_id>` → group detail (`get_group`).
  - `s<store_id>/g<group_id>/r<replica_id>` → replica detail
    (`get_replica`).
  - any other bare token → a node id (string), resolved via
    `get_node_server` (404 ⇒ "no server deployed").
  - `s…`/`g…`/`r…` ids are decimal. A token that is not a valid
    `s<digits>[…]` logical path is treated as a node id, so node ids
    that happen to look like `s7` are addressable only as nodes — keep
    node ids out of the `s\d+` shape.

### 7.0.1 `server list` and the bench endpoint
- `server list` is served by a console aggregate `GET /api/servers`
  (`ConsoleClient::list_servers`) returning, per deployed server,
  `{node_id, mgmt_url, grpc_url, pid, health}` from the config plus the
  monitor cache.
- `bench` dials gRPC directly for throughput, but resolves its target
  endpoint through the console (`GET /api/stores/:s/groups/:g/endpoint`
  → `ConsoleClient::resolve_endpoint`), so it needs no registry and no
  `--server`.

### 7.1 Bench subcommand internals
- `shared` exposes a `Workload` trait with built-in implementations for read / write / list / mix.
- Connection model:
  - User picks `--connections N` (1..=64 per server, default 4); each connection is a real, separate TCP/HTTP2 channel to the target `crowkv-server` (no multiplexing all RPCs over a single channel).
  - User picks `--threads M` (1..=1000); each thread runs a **blocking** loop: issue op → await response → next op. This is the lowest-latency model and is easy to reason about.
  - Threads are mapped round-robin onto the connection pool.
- Stats: HDR histogram for latency; atomic counters for ops/errors; sampled snapshots written to a ring buffer to avoid hot-path contention.
- Performance discipline: the bench tool itself must not become the bottleneck. Hot paths avoid allocations per op (key/value buffers reused), avoid logging in the inner loop, and use lock-free histograms.
- Reports: JSON file at `~/.crowkv/bench/<run-id>.json`; `bench report <run-id>` re-renders a saved file.

### 7.2 Bench command shape
```
crowkv bench run read    --connections N --threads M --duration T [--keys-from <file>] [--key-range a..b]
crowkv bench run write   --connections N --threads M --duration T [--value-size B] [--key-dist uniform|zipf]
crowkv bench run list    --connections N --threads M --duration T [--page-size P]
crowkv bench run mix     --connections N --threads M --duration T --read-ratio R [--value-size B]
crowkv bench stress      <scenario>          # predesigned scenarios bundled with the binary
crowkv bench report      <run-id>
```
- `bench run <op>` is the workhorse; one op per command keeps the flag set small per command.
- `bench mix` adds `--read-ratio` and is the only knob for read/write mixing.
- `bench stress` invokes a **predesigned scenario** baked into the binary (`saturating-write`, `mostly-read`, `latency-tail`, …); one positional name, no further tuning.
- `bench report` is unchanged.

## 8. Swagger UI Hosting

Split responsibility:

- **Swagger UI assets** (HTML / JS / CSS) are hosted by **`crowkv-web`**.
  `crowkv-console/web/swagger-ui/` holds one pinned, offline release of
  `swagger-ui-dist` (no GitHub fetch at build time, no internet at
  runtime). The version is recorded in a sibling `VERSION` file. Axum
  mounts the directory at `/api/swagger/` via
  `tower_http::services::ServeDir`.
- **OpenAPI document** is **per-node**: every `crowkv-server` instance
  exposes `/openapi.json` (generated by `utoipa::OpenApi` from its
  `ToSchema` derives). The console exposes a proxy at
  `/api/nodes/:node_id/openapi.json` so the user can pick which
  deployment to inspect.

This split lets users compare per-server documents (useful during
rolling upgrades when versions diverge) while keeping the heavyweight
JS bundle local to the console host.

**Why we don't host the bundle on `crowkv-server`.** It would force
every `crowkv-server` to ship Swagger UI even when not needed and would
require the SPA to load assets from the upstream's URL, which conflicts
with the embeddability rule that no upstream `host:port` ever appears
in the browser. Hosting in `crowkv-web` puts assets behind a single
stable URL (`/api/swagger/`) under the console's origin.

**SPA wiring.** The Swagger feature panel is an in-page `<iframe>`
pointed at `/api/swagger/?url=/api/nodes/:node_id/openapi.json` for the
currently selected node. Switching the node selector updates the
iframe's `src`, which reloads the OpenAPI doc inside the same panel
(no new browser tab, no full-page navigation).

**`crowkv-server` Cargo features.** With the bundle moved to the
console, `crowkv-server` keeps its `utoipa::ToSchema` derives and
`/openapi.json` route but **drops the `swagger-ui` Cargo feature and
the `utoipa-swagger-ui` dependency**. Its OpenAPI surface remains
unchanged from the SPA's point of view.

## 9. Error Model and Operation Logging

### 9.1 Error enum
- `crowkv-console-shared::Error` enum:
  - `NodeUnreachable { node_id, reason }`
  - `UpstreamRpc { node_id, status }`
  - `Validation { field, message }`
  - `NotFound { kind, id }`
  - `Conflict { kind, id }`
- HTTP layer maps to `4xx` / `5xx`; CLI layer maps to exit codes (`0` ok, `1` user error, `2` cluster/network error).

### 9.2 Operation log (CLI + Web)
A console session writes **two streams**:
1. **Console stream** — short user-friendly messages and errors (CLI: stderr; Web: log panel).
2. **Operation log** — a per-session file under `~/.crowkv/log/console-<UTC-timestamp>-<pid>.log`. Each line is a structured record of one outbound action with enough detail to reproduce it manually:
   - HTTP: method, full URL, headers (excluding secrets), JSON body, response status + body summary.
   - gRPC: service / method, request proto JSON, response status.
   - SSH: target, full remote command line, exit status, stdout/stderr snippets.

A new file is created on every CLI invocation and on every `crowkv-web` startup. Files are not auto-rotated within a session; users can prune `~/.crowkv/log/` themselves.

This makes any failed operation reproducible by copy-pasting the recorded command (curl/grpcurl/ssh) into a terminal.

## 10. Observability
- `tracing` everywhere; `--vv` switches CLI to debug.
- Web backend exposes `/healthz`. **`/metrics` is deferred**: the Rust Prometheus story has multiple competing crates (`prometheus`, `metrics` + `metrics-exporter-prometheus`, `opentelemetry`); we will pick one once we start the broader observability work for `crowkv-server` itself. Until then, the operation log + tracing are the source of truth.
- All console-issued operations attach a correlation id and propagate it as `x-crowkv-corr-id` to `crowkv-server` request headers (already supported by `crowkv-server`'s tracing middleware).

## 11. Open Questions

- **SSH crate**: `russh` (decided). Defaults to `~/.ssh/*`; `(user, password)` is an explicit alternative.
- **Frontend bundle**: built on demand during development. The `web/ui/` directory is the source; `npm run build` produces `dist/` which the Axum server serves. The committed repo does **not** include `web/dist/`. Production / release builds run `npm ci && npm run build` as part of `make`.
- **Credentials storage**: plaintext TOML, accessed only through the `ConsoleConfig` struct so the source can change later without touching call sites.
- **Multiple servers per node**: UI and console operations enforce one. The data model and `crowkv-server` itself remain unrestricted; lower-layer tests can still spawn many.
