<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROW Console Design

Upstream: `doc/../kv/design-crow-kv.md` §15 (the `crow-console` component
overview) and §15.4.6 (Web UI requirements).
Sibling: `doc/design/design-crow-console-ui.md` (frontend SPA design).

## 1. Goals and Non-Goals

### Goals
- Single workspace project `crow-console` delivering a Web UI and a CLI that share one Rust core.
- Operate against any number of `crow-kv-server` instances via their public surfaces (HTTP management API + gRPC KV / health).
- Model a **Rack → Node → Server Instance → Store → Group → Replica** hierarchy, including a "simulated hardware" mode that runs entirely on `127.0.0.1`.
- Host the Swagger UI static bundle in `crow-web` (one pinned offline release); the OpenAPI document shown inside it is proxied from the user-selected `crow-kv-server`, so the SPA can inspect a specific server's API even though all servers of the same version produce the same doc.

### Non-Goals
- Bypassing `crow-kv-server` to talk to Paxos / WAL / storage internals.
- Authentication, authorization, multi-tenancy, audit logging.
- Persisting console state beyond local config files.

## 2. High-Level Architecture

`crow-console` is **one project** split across the `lib/` and `app/`
workspace roots: a shared core lib plus two binaries. The console is a
general cluster-management surface (not limited to CROW), so crate
names use the `crow-*` prefix without `kv`.

```
lib/crow-console-shared/   (lib)   data models, HTTP+gRPC clients, registry, aggregator, error model, SSH session pool, workload generator
app/crow-web/              (bin)   Axum backend, static asset server, Swagger UI mount, proxy routes
  src/                             Rust source
  ui/                              React + Vite frontend source (TS, shadcn/ui, React Flow)
  swagger-ui/                      committed Swagger UI assets (one pinned version, served by crow-web)
  tests/                           integration tests
app/crow-cli/              (bin)   clap-based CLI; depends on shared
```

Targets:
- `crow-console-shared` → reusable lib for both frontends.
- `crow-web` → bin, serves UI + API on `:9920`.
- `crow-cli` → bin, the user-facing CLI.

### 2.1 Call Path

Every console operation follows the same path. The frontend (web SPA
backed by Axum, **or** the `crow-cli` CLI binary) is a thin presentation
layer; it always calls into `shared`, and `shared` is the only place
that talks to `crow-kv-server` over HTTP / gRPC / SSH.

```
                ┌──────────────┐        ┌──────────────┐
   user ───►    │  crow-web  │   or   │ crow-kv (CLI) │     (frontend)
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
              │           crow-kv-server            │     (one per node)
              └────────────────────────────────────┘
```

### 2.2 Reuse Boundary

- All "what to do" lives in `shared` (e.g. `add_group`,
  `deploy_server`, `kv_put`, `refresh_node`).
- `web` (Axum) and `cli` (clap) only parse input and render output.
- The web SPA **does not** reimplement business logic; it calls
  `shared` via the Axum backend, never `crow-kv-server` directly.
- Both frontends share the same `shared` entry points, so any feature
  is reachable from both surfaces by construction.

## 3. Data Model

The console exposes **two hierarchy views** of the same cluster. Both
views describe the same underlying entities; they differ only in the
direction from which the cluster is observed.

### 3.1 Physical (deployment) view

> "What hardware exists, and what is running on each piece of it."

Rooted at **Rack → Node → Server → PxStore → PxGroup → {LocalReplica,
RemoteReplica…}**. Every entity below `Node` is described from that
node's vantage point. A `PxGroup` has exactly one local replica plus
N−1 remote-replica proxies — this mirrors the `crow-kv-server` internal
data structure, which is why this view is also the "debugging view":
the API surfaces the remote-list explicitly so an operator can spot
bugs where a node failed to register all of its peers.

Identity is the parent chain
`(rack_id, node_id, store_id, group_id, replica_id)`.

### 3.2 Logical (usage) view

> "What stores and groups exist in the cluster, regardless of where
> they live."

Rooted at **Cluster → Store → Group → Replica…** with a unified replica
list (no local/remote split; each replica carries a `node_id`). This is
the view that KV traffic, leader resolution, and routine cluster
operations use. The web backend is the only component that needs to
translate logical ids into upstream `(node_id, mgmt_url, grpc_url)`
tuples; the SPA and the CLI never see those.

Identity is `(store_id[, group_id[, replica_id]])`.

### 3.3 Source of truth and freshness

- **Persisted (config file, see §4):** rack/node entries and the
  *intended* server deployment record (host, ports, binary path).
  These survive restart.
- **Live (rebuilt on every console start):** process state, health,
  per-node store/group/replica state, leader hints. The monitor task
  (§4) pings each node and fetches per-node state; the logical view is
  derived by aggregating those reports.
- **No `ClusterSnapshot` polling endpoint.** The SPA queries
  per-resource live endpoints, all served from the monitor cache.

### 3.4 Design decisions

- **No `server_id` namespace.** The server's mgmt/gRPC URLs live inside
  `Node.server` and are never exposed in console-facing JSON URLs. Since
  the console enforces one server per node, node identity *is* server
  identity.
- **Local/remote split is visible only in the physical view.** The
  logical view collapses replicas into a unified list so cluster-level
  operations can ignore placement. The physical view keeps the split
  for debugging missing peer registrations.
- `StoreView` / `GroupView` / `ReplicaView` reuse `crow_kv::cluster::info`
  where possible; the console-side wrapper adds the `node_id`
  projection that the per-server protocol does not encode.

## 4. Console Backend Persistence and Monitor Task

### 4.1 Persisted state (config file)

- Single TOML file: `~/.lib/crow-kv/console.toml` (override with `$CROW_CONSOLE_CONFIG`).
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

### 4.3 Persistent Cluster Config

**Problem**: The TOML config file is a single point of failure — losing
the console host loses the full topology. Per-node server config is also
not persisted independently; a node restart relies on the console to
re-push topology.

**Solution**: A designated Paxos group — **system group (store 0,
group 0)** — stores the full cluster topology as regular KV entries.
Since it is a Paxos group, the topology is replicated, consistent, and
HA by the same mechanism that protects user data. No external
coordinator needed. This is the standard industry pattern (closest
analog: CockroachDB system ranges).

- **Two-phase bootstrap**:
  - Phase 1: Console TOML is source of truth (existing behavior).
  - Phase 2: `POST /topology/finalize` writes all TOML topology into
    group 0 KV, sets `/topology/ready` flag. Idempotent and retry-safe.
  - Console restart: three-way fallback — group 0 missing → TOML mode;
    group 0 not ready → TOML mode + warning; group 0 ready → group 0
    authoritative.

- **Topology KV schema** (in group 0):
  - `/topology/ready` — flag key; presence means group 0 is authoritative
  - `/topology/racks/<rack_id>` — rack metadata
  - `/topology/nodes/<node_id>` — node metadata
  - `/topology/stores/<store_id>` — store metadata
  - `/topology/groups/<group_id>` — group metadata
  - `/topology/replicas/<group_id>/<replica_id>` — replica metadata
  - `/topology/counters/<entity>` — ID allocation counters

- **Per-node config cache** (`conf/node-config.json`): Local cache
  derived from the system group. On startup: load cache → create
  stores/groups → replay WAL → reconcile with group 0 KV. If cache is
  lost, node queries group 0 to rebuild it.

- **Divergence reconciliation**: On node startup, if group 0 is
  reachable and finalized, compare local cache against group 0 KV.
  Create missing stores/groups, remove stale ones. If group 0 not
  reachable, boot from local cache only (deferred).

- **Cluster init flow**: `POST /api/cluster/init` on the console
  orchestrates: calls `POST /system/init` on selected nodes, wires
  remotes for multi-node, persists topology in console config. Data
  store/group creation is blocked (`409`) until cluster is initialized.

- **Management API endpoints** (on `crow-kv-server`):
  - `POST /system/init` — bootstrap store 0 + group 0 on this node
  - `POST /topology/finalize` — idempotent cutover, sets `/topology/ready`
  - `GET /topology/ready` — check if group 0 is authoritative

- **Group 0 membership evolution**: Reuses shipped Model B
  reconfiguration (direct HTTP mutation + `membership_epoch` fence).
  No new consensus primitive required.

## 5. Node Access Model

### 5.1 Two transports per node
| Purpose | Transport |
| --- | --- |
| Deploy / start / stop `crow-kv-server` process; copy binary | SSH |
| Runtime mgmt API (add store/group, list, health) | HTTP |
| Runtime KV ops, paxos health | gRPC |

### 5.2 SSH defaults (russh)
- Crate: **`russh`** (pure Rust, async). No shell-out fallback.
- Default auth: `~/.ssh/*` keys (agent + standard key paths).
- Alternative auth: explicit key path; explicit `user/password`.
- Default host: `127.0.0.1` with the current OS user.
- Pre-flight: every operation calls `ssh::probe(node)` which performs a real handshake before any side-effecting work. Failure surfaces as `NodeUnreachable { node_id, reason }`.


### 5.3 Process lifecycle (deploy / start / stop)

**SSH path** (`ssh_user` non-empty):
1. SSH into node (`russh` crate, pure Rust async).
2. `nohup crow-kv-server --management-addr 127.0.0.1 --management-port <p> --ports <gp> &`;
   capture pid via `echo $!`; record in the persisted node server entry.
3. Health-check via the new server's HTTP `/health` until ready or timeout (10 s).

**Local-fork path** (`ssh_user` empty, for tests/dev on `127.0.0.1`):
1. `tokio::process::Command::new(crow-kv-server)` with the same args.
2. Stage the binary into a per-node workspace directory (`runtime-data/N-<node_id>/`).
3. Detach the child (do not kill on drop); track the pid.
4. Health-check via `/health`.

Binary resolution: `$CROW_KV_SERVER_BIN` → sibling of current executable →
`$PATH` lookup for `crow-kv-server`.

(Future: scp the binary to the remote host on first deploy and render
a config template. Not yet implemented — the SSH path assumes the
binary is already present on the remote host.)

`server deploy`, `server restart`, and `server stop` address a node. There is no separate
server id namespace in the console API.

## 6. Web UI Backend (Axum)

### 6.1 Design Rules

The console-facing API is split along the **two hierarchy views**
defined in §3, and every route lives under exactly one of them.

**R1. Two URL trees, one per hierarchy.**
- `/api/racks/...` and `/api/nodes/...` form the **physical** tree.
  Every resource is addressed by its parent chain.
- `/api/stores/...` forms the **logical** tree. KV traffic and
  cluster-wide operations live here, addressed by
  `(store_id[, group_id[, replica_id]])`. Logical-tree responses still
  carry `node_id` on every entry so a caller can see placement without
  a physical-tree query; only the **path** is node-free.
- A route never crosses trees.

**R2. Logical reads aggregate; physical reads are per-node.**
The same store, observed through the two trees, returns different
shapes — aggregated `StoreView` vs. that node's local `NodeStore`.
This is how the operator inspects "is the cluster consistent?" vs.
"what does this one node think it has?".

**R3. Logical writes orchestrate; physical writes act on one node.**
A logical write declares *intent*; the web backend fans out per-node
calls in `shared` and rolls back on partial failure. A physical write
is the low-level primitive — it touches exactly that node, never fans
out. Logical writes are implemented on top of physical primitives.

**R4. No `server_id` namespace.**
Process lifecycle, reachability probes, and Swagger proxying use
`/api/nodes/:node_id/server/...`. Node identity *is* server identity.

> **Retired contracts (no compatibility shim):** `?server=<mgmt_url>`
> query parameter, `/api/servers/:sid/...`,
> `/api/openapi.json?server=<id>`, `/api/cluster/snapshot`.

The full endpoint list is defined in the Axum route handlers and the
OpenAPI spec; this section covers design rules only.

### 6.2 Recursive reads (`?recursive=<depth>`)

Any `GET` in either tree accepts `?recursive=<n>` to inline up to `n`
child levels in one response, avoiding O(N) follow-up requests for
UIs that render a whole sub-tree. `recursive=all` is a capped alias
(default max depth 8) intended for the SPA's initial render.

Rules: read-only (mutations ignore it), depth counts child hops from
the addressed resource, each tree expands along its own hierarchy, KV
key/value payloads are never inlined, and all responses use the
monitor cache so `recursive` is cheap even at high depth.

### 6.3 Orchestration semantics

For each multi-node operation in the logical tree, the backend obeys
these rules:

- **Plan first, act second.** Resolve every required node + replica id
  from the monitor cache before issuing any upstream RPC.
- **Built on physical primitives.** The orchestrator only calls the
  per-node physical mutators; it never invents a side channel.
- **All-or-nothing where feasible.** On partial failure, attempt to
  undo successful sub-steps and surface the resulting state in the
  error body.
- **Idempotent retries.** A repeat of the same logical request must
  converge to the same state.
- **Cache refresh on success.** Every successful mutation triggers an
  immediate monitor refresh for the affected nodes.

### 6.4 Resolution rules

- Unknown ids → `404`.
- Unreachable node for a physical-tree call → `502 Bad Gateway`.
- Partial logical-tree failure → roll back and report `409 Conflict`
  with structured per-node outcomes (not `207 Multi-Status`).
- All handlers are thin wrappers around `shared` entry points; the
  CLI calls the same entry points.

### 6.5 Frontend contract

The frontend SPA design lives in `design/design-crow-console-ui.md`. The
backend-facing contract here:

- Bundle output is `app/crow-web/ui/dist/`; `crow-web` serves
  it via SPA fallback.
- The SPA polls per-resource live endpoints on a short interval. No
  WebSocket/SSE. All reads are served from the monitor cache.
- No `/api/cluster/snapshot` aggregate endpoint.
- The Swagger panel embeds `/api/swagger/?url=/api/nodes/:node_id/openapi.json`.

## 7. CLI Design

- Binary: `crow-kv` (noun-verb structure: `crow-kv <group> <verb>`).
- Parser: `clap` derive; one module per top-level group.
- **One call path.** Every verb routes through `ConsoleClient` against
  `crow-web`. The CLI never talks to a `crow-kv-server` directly; there
  is no `--server` flag.
- **Two layers max** — `crow-kv <group> <verb>`. No three-level chains.
- Verb vocabulary stays consistent: `add / remove / list / inspect`.
  Lifecycle verbs (`deploy / start / stop`) for `server`; data verbs
  (`put / get / delete / scan / list`) for `kv`.
- **Logical entity addressing**: store/group/replica/KV commands use
  `--store-id` / `--group-id`; the backend resolves placement. Server
  lifecycle uses `--node-id`.
- **Leaders are elected, not assigned.** `group add` takes no `--leader`
  flag; leadership is decided by Paxos election.
- `cluster inspect <id>` uses a compact id grammar: `s<store_id>`,
  `s<store_id>/g<group_id>`, `s<store_id>/g<group_id>/r<replica_id>`,
  or a bare node id string.
- Output: JSON by default for scripting; `--json` flag is a no-op.
  Human-readable table formatting is a future enhancement.

The full command hierarchy is defined in the `clap` derive structs;
this section covers design rules only.

### 7.1 Bench subcommand

- `shared` exposes a `Workload` trait with built-in read/write/list/mix
  implementations.
- Connection model: user picks `--connections N` (separate TCP/HTTP2
  channels) and `--threads M` (blocking issue→await loops, mapped
  round-robin onto the connection pool). This is the lowest-latency
  model and is easy to reason about.
- Performance discipline: hot paths avoid per-op allocations, logging,
  and lock contention (HDR histogram, atomic counters, ring-buffer
  snapshots).
- `bench stress` invokes predesigned scenarios baked into the binary;
  `bench report` re-renders saved JSON.

## 8. Swagger UI Hosting

Split responsibility:

- **Swagger UI assets** (HTML / JS / CSS) are hosted by `crow-web`
  from one pinned, offline release. No internet at runtime.
- **OpenAPI documents** are served by each `crow-kv-server` at
  `/openapi.json`. The console proxies this per-node via
  `/api/nodes/:node_id/openapi.json` so the SPA can inspect a specific
  server's API without CORS issues.

**Why not host the bundle on `crow-kv-server`?** It would force every
server to ship Swagger UI even when not needed, and would require the
SPA to load assets from the upstream's URL, conflicting with the
embeddability rule that no upstream `host:port` ever appears in the
browser.

## 9. Error Model and Operation Logging

- `shared::Error` enum covers `NodeUnreachable`, `UpstreamRpc`,
  `Validation`, `NotFound`, `Conflict`. HTTP maps to 4xx/5xx; CLI maps
  to exit codes (0 ok, 1 user error, 2 cluster/network error).
- **Operation log** — a per-session file under `~/.lib/crow-kv/log/` records
  every outbound action (HTTP/gRPC/SSH) with enough detail to reproduce
  by copy-pasting the equivalent curl/grpcurl/ssh command.

## 10. Observability

- `tracing` everywhere; `--vv` switches CLI to debug.
- Web backend exposes `/healthz`. **`/metrics` is deferred** — the Rust
  Prometheus story has multiple competing crates; we will pick one when
  broader observability work for `crow-kv-server` begins.
- All console-issued operations attach a correlation id propagated as
  `x-crow-kv-corr-id` to `crow-kv-server` request headers.

## 11. Open Questions

- **SSH crate**: `russh` (decided). Defaults to `~/.ssh/*`; `(user,
  password)` is an explicit alternative.
- **Frontend bundle**: built on demand; `npm run build` produces `dist/`
  which the Axum server serves. The committed repo does not include
  `web/dist/`.
- **Credentials storage**: plaintext TOML, accessed only through
  `ConsoleConfig` so the source can change later without touching call
  sites.
- **Multiple servers per node**: UI and console enforce one; lower
  layers remain unrestricted.
