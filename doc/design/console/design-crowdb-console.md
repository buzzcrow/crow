<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB - Design: Console (Overview)

The `crowdb-console` component provides a Web UI and a CLI that share one
Rust core for managing `crowdb-kv-server` clusters. This overview covers
the backend, data model, CLI, and hosting concerns; the frontend SPA
design is detailed in the sub-design `design-crowdb-console-ui.md`.

## Table of Contents

- [1. Goals and Non-Goals](#1-goals-and-non-goals)
  - [Goals](#goals)
  - [Non-Goals](#non-goals)
- [2. High-Level Architecture](#2-high-level-architecture)
  - [2.1 Call Path](#21-call-path)
  - [2.2 Reuse Boundary](#22-reuse-boundary)
- [3. Data Model](#3-data-model)
  - [3.1 Physical (deployment) view](#31-physical-deployment-view)
  - [3.2 Logical (usage) view](#32-logical-usage-view)
  - [3.3 Source of truth and freshness](#33-source-of-truth-and-freshness)
  - [3.4 Design decisions](#34-design-decisions)
- [4. Console Backend Persistence and Monitor Task](#4-console-backend-persistence-and-monitor-task)
  - [4.1 Persisted state (config file)](#41-persisted-state-config-file)
  - [4.2 Monitor task](#42-monitor-task)
  - [4.3 Persistent Cluster Config](#43-persistent-cluster-config)
- [5. Node Access Model](#5-node-access-model)
  - [5.1 Two transports per node](#51-two-transports-per-node)
  - [5.2 SSH defaults (russh)](#52-ssh-defaults-russh)
  - [5.3 Process lifecycle (deploy / start / stop)](#53-process-lifecycle-deploy--start--stop)
- [6. Web UI Backend (Axum)](#6-web-ui-backend-axum)
  - [6.1 Design Rules](#61-design-rules)
  - [6.2 Recursive reads (?recursive=<depth>)](#62-recursive-reads-recursivedepth)
  - [6.3 Orchestration semantics](#63-orchestration-semantics)
  - [6.4 Resolution rules](#64-resolution-rules)
  - [6.5 Frontend contract](#65-frontend-contract)
- [7. CLI Design](#7-cli-design)
  - [7.1 Four-Domain Hierarchy](#71-four-domain-hierarchy)
  - [7.2 Command Hierarchy](#72-command-hierarchy)
  - [7.3 `cluster init` — bootstrap special case](#73-cluster-init--bootstrap-special-case)
  - [7.4 `cluster clean` — data wipe boundary](#74-cluster-clean--data-wipe-boundary)
  - [7.5 `kv server delete` — graceful + require-empty](#75-kv-server-delete--graceful--require-empty)
  - [7.6 Bench subcommand](#76-bench-subcommand)
  - [7.7 Bench lifecycle verbs (deploy / prepare / run / teardown)](#77-bench-lifecycle-verbs-deploy--prepare--run--teardown)
- [8. Error Model and Operation Logging](#8-error-model-and-operation-logging)
- [9. Observability](#9-observability)
- [10. Open Questions](#10-open-questions)
- [11. Sysdata sync — rack/node/disk-group/disk handlers](#11-sysdata-sync--racknodedisk-groupdisk-handlers)
- [12. Cluster reset](#12-cluster-reset)

## 1. Goals and Non-Goals

### Goals
- Single workspace project `crowdb-console` delivering a Web UI and a CLI that share one Rust core.
- Operate against any number of `crowdb-kv-server` instances via their public surfaces (HTTP management API + crowdb-rpc KV / health).
- Model a **Rack → Node → Server Instance → Store → Group → Replica** hierarchy, including a "simulated hardware" mode that runs entirely on `127.0.0.1`.

### Non-Goals
- Bypassing `crowdb-kv-server` to talk to Paxos / WAL / storage internals.
- Authentication, authorization, multi-tenancy, audit logging.
- Persisting console state beyond local config files.

## 2. High-Level Architecture

`crowdb-console` is **one project** split across the `lib/` and `app/`
workspace roots: a shared core lib plus two binaries. The console is a
general cluster-management surface (not limited to CROWDB), so crate
names use the `crowdb-*` prefix without `kv`.

```
lib/crowdb-console-shared/   (lib)   data models, HTTP+crowdb-rpc clients, registry, aggregator, error model, SSH session pool, workload generator
app/crowdb-web/              (bin)   Axum backend, static asset server, proxy routes
  src/                             Rust source
  ui/                              React + Vite frontend source (TS, shadcn/ui, React Flow)
  tests/                           integration tests
app/crowdb-cli/              (bin)   clap-based CLI; depends on shared
```

Targets:
- `crowdb-console-shared` → reusable lib for both frontends.
- `crowdb-web` → bin, serves UI + API on `:9920`.
- `crowdb-cli` → bin, the user-facing CLI.

### 2.1 Call Path

The web frontend (SPA backed by Axum) and the CLI follow different
paths:

- **Web**: `user → crowdb-web (Axum) → shared → crowdb-kv-server`
- **CLI**: `user → crowdb-cli → shared (ops module) → group-0 sysdata + crowdb-kv-server mgmt`

The CLI talks directly to group-0 system metadata via
`CrowdbSysmdClient` and to individual `crowdb-kv-server` management
APIs via `ServerClient` — no `crowdb-web` intermediary. The `ops`
module in `shared` holds the operation logic that both the CLI and
(later) the web UI can call.

```
                ┌──────────────┐        ┌──────────────┐
   user ───►    │  crowdb-web  │   or   │  crowdb-cli  │     (frontend)
                └──────┬───────┘        └──────┬───────┘
                       │   parse input,        │
                       │   render output       │
                       └──────────┬────────────┘
                                  ▼
                          ┌───────────────┐
                          │    shared     │              (business logic:
                          │  (lib crate)  │               ops module,
                          └──────┬────────┘               monitor cache,
                                 │                        leader resolution,
                  ┌──────────────┼──────────────┐         SSH session pool)
                  ▼              ▼              ▼
               HTTP            crowdb-rpc            SSH
                  │              │              │
                  ▼              ▼              ▼
              ┌────────────────────────────────────┐
              │           crowdb-kv-server            │     (one per node)
              └────────────────────────────────────┘
```

### 2.2 Reuse Boundary

- All "what to do" lives in `shared` (e.g. `ops::kv_logical::add_group`,
  `ops::kv_server::deploy`, `ops::kv_data::put`, `ops::hardware::add_rack`).
- `web` (Axum) and `cli` (clap) only parse input and render output.
- The web SPA **does not** reimplement business logic; it calls
  `shared` via the Axum backend, never `crowdb-kv-server` directly.
- The CLI calls `shared`'s `ops` module directly, building an
  `OpContext` from `--sysmd-ip` / `--sysmd-port` global flags.
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
N−1 remote-replica proxies. This mirrors the `crowdb-kv-server` internal
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
translate logical ids into upstream `(node_id, mgmt_url, rpc_url)`
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

- **No `server_id` namespace.** The server's mgmt/crowdb-rpc URLs live inside
  `Node.server` and are never exposed in console-facing JSON URLs. Since
  the console enforces one server per node, node identity *is* server
  identity.
- **Local/remote split is visible only in the physical view.** The
  logical view collapses replicas into a unified list so cluster-level
  operations can ignore placement. The physical view keeps the split
  for debugging missing peer registrations.
- `StoreView` / `GroupView` / `ReplicaView` reuse `crowdb_kv::cluster::info`
  where possible; the console-side wrapper adds the `node_id`
  projection that the per-server protocol does not encode.

## 4. Console Backend Persistence and Monitor Task

### 4.1 Persisted state (config file)

- Single TOML file: `~/.lib/crowdb-kv/console.toml` (override with `$CROWDB_CONSOLE_CONFIG`).
- Contents:
  - `rack` / `node` entries (id, rack_id, host, SSH creds).
  - Optional per-node server deployment record: management endpoint,
    rpc endpoint, and binary/config path as implementation evolves.
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

**Problem**: The TOML config file is a single point of failure. Losing
the console host loses the full topology. Per-node server config is also
not persisted independently; a node restart relies on the console to
re-push topology.

**Solution**: A designated Paxos group, **system group (store 0,
group 0)**, stores the full cluster topology as regular KV entries.
Since it is a Paxos group, the topology is replicated and HA by the
same mechanism that protects user data. No external coordinator
needed. This is the standard industry pattern (closest
analog: CockroachDB system ranges).

- **Two-phase bootstrap**:
  - Phase 1: Console TOML is source of truth (existing behavior).
  - Phase 2: `HardwareClient` writes hardware hierarchy (racks, nodes)
    and `KVClusterMetaClient` writes KV-cluster topology (stores,
    groups, replicas) into group 0 via text-path keys with JSON
    values. No readiness flag. diskdb's sync loop treats empty group 0
    as "nothing assigned yet" and retries.
  - Console restart: two-way fallback. Group 0 missing → TOML mode;
    group 0 exists → group 0 authoritative.

- **Group-0 sysdata schema** (text-path keys, JSON values):
  - `/hw/rack/<rack_id>` — rack metadata (`RackValue`)
  - `/hw/node/<rack_id>/<node_id>` — node metadata (`NodeValue`)
  - `/hw/dg/<rack_id>/<node_id>/<dg_id>` — disk-group metadata
  - `/hw/disk/<rack_id>/<node_id>/<dg_id>/<disk_id_hex>` — disk metadata
  - `/hw/owner/<rack_id>/<node_id>/<dg_id>` — ownership map
  - `/hw/bind/<rack_id>/<node_id>/<dg_id>` — bind map
  - `/kv/store/<store_id>` — store metadata (`StoreValue`)
  - `/kv/group/<store_id>/<group_id>` — group metadata (`GroupValue`)
  - `/kv/replica/<store_id>/<group_id>/<replica_id>` — replica metadata
  - `/srv/<service>/<instance_id>` — service registry instances

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
  remotes for multi-node, persists topology in console config, then
  writes hardware + KV-cluster topology into group 0 via
  `HardwareClient` + `KVClusterMetaClient`. Data store/group creation
  is blocked (`409`) until cluster is initialized.

- **Management API endpoints** (on `crowdb-kv-server`, internal — only
  called by `crowdb-kv-client`'s `KVClusterAdmin`):
  - `POST /system/init` — bootstrap store 0 + group 0 on this node
  - Lifecycle: `add_store`, `remove_store`, `add_group`,
    `remove_group`, `add_remote_replicas`, `remove_remote_replica`,
    `step_down`, `join_group_via_snapshot`, `flush_group`
  - Query: `GET /topology` (export), `GET /health`, `GET /metrics`

- **Group 0 membership evolution**: Reuses shipped Model B
  reconfiguration (direct HTTP mutation + `membership_epoch` fence).
  No new consensus primitive required.

## 5. Node Access Model

### 5.1 Two transports per node
| Purpose | Transport |
| --- | --- |
| Deploy / start / stop `crowdb-kv-server` process; copy binary | SSH |
| Runtime mgmt API (add store/group, list, health) | HTTP |
| Runtime KV ops, paxos health | crowdb-rpc |

### 5.2 SSH defaults (russh)
- Crate: **`russh`** (pure Rust, async). No shell-out fallback.
- Default auth: `~/.ssh/*` keys (agent + standard key paths).
- Alternative auth: explicit key path; explicit `user/password`.
- Default host: `127.0.0.1` with the current OS user.
- Pre-flight: every operation calls `ssh::probe(node)` which performs a real handshake before any side-effecting work. Failure surfaces as `NodeUnreachable { node_id, reason }`.

**SSH credential storage lifecycle** — two phases:

- **Bootstrap phase** (before group 0 exists) — SSH creds are stored
  in the shared TOML config file `runtime-data/crowdb.temp.toml`
  (via `TomlFileEngine::default_path()` in
  `lib/crowdb-console-shared/src/config.rs`). This file stores
  rack/node/server/store/group/disk-group/disk entries, with SSH creds
  in `NodeEntry` (`ssh_user`, `ssh_key`, `ssh_password`). The CLI and
  UI share the same `ConsoleConfig` + `TomlFileEngine` flow — `cluster
  rack add` / `cluster node add` write to this file, `kv server deploy`
  reads SSH creds from it. No separate CLI-only config file.
- **Steady-state phase** (after group 0 exists) — SSH creds are moved
  into group-0 sysdata, encrypted with a default key. Subsequent `kv
  server deploy` calls read creds from group-0 sysdata via
  `KVClusterMetaClient`. The TOML file is no longer the source of truth
  for SSH creds; group 0 is. The TOML file remains as a local cache /
  bootstrap fallback.


### 5.3 Process lifecycle (deploy / start / stop)

**SSH path** (`ssh_user` non-empty):
1. SSH into node (`russh` crate, pure Rust async).
2. `nohup crowdb-kv-server --management-addr 127.0.0.1 --management-port <p> --ports <gp> &`;
   capture pid via `echo $!`; record in the persisted node server entry.
3. Health-check via the new server's HTTP `/health` until ready or timeout (10 s).

**Local-fork path** (`ssh_user` empty, for tests/dev on `127.0.0.1`):
1. `tokio::process::Command::new(crowdb-kv-server)` with the same args.
2. Stage the binary into a per-node workspace directory (`runtime-data/N-<node_id>/`).
3. Detach the child (do not kill on drop); track the pid.
4. Health-check via `/health`.

Binary resolution: `$CROWDB_KV_SERVER_BIN` → sibling of current executable →
`$PATH` lookup for `crowdb-kv-server`.

(Future: scp the binary to the remote host on first deploy and render
a config template. Not yet implemented. The SSH path assumes the
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
shapes: aggregated `StoreView` vs. that node's local `NodeStore`.
This is how the operator inspects "is the cluster consistent?" vs.
"what does this one node think it has?".

**R3. Logical writes orchestrate; physical writes act on one node.**
A logical write declares *intent*; the web backend fans out per-node
calls in `shared` and rolls back on partial failure. A physical write
is the low-level primitive. It touches exactly that node, never fans
out. Logical writes are implemented on top of physical primitives.

**R4. No `server_id` namespace.**
Process lifecycle and reachability probes use
`/api/nodes/:node_id/server/...`. Node identity *is* server identity.

> **Retired contracts (no compatibility shim):** `?server=<mgmt_url>`
> query parameter, `/api/servers/:sid/...`,
> `/api/openapi.json?server=<id>`, `/api/cluster/snapshot`,
> `/api/swagger/...`, `/api/nodes/:id/openapi.json`.

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

The frontend SPA design lives in `design-crowdb-console-ui.md`. The
backend-facing contract here:

- Bundle output is `app/crowdb-web/ui/dist/`; `crowdb-web` serves
  it via SPA fallback.
- The SPA polls per-resource live endpoints on a short interval. No
  WebSocket/SSE. All reads are served from the monitor cache.
- No `/api/cluster/snapshot` aggregate endpoint.

## 7. CLI Design

- Binary: `crowdb-cli` (four-domain structure: `crowdb-cli <domain> <verb>`).
- Parser: `clap` derive; one module per domain under `commands/`.
- **Direct-to-group-0 call path.** Every verb builds an `OpContext`
  seeded with `--sysmd-ip` / `--sysmd-port` (a group-0 mgmt endpoint)
  and talks directly to group-0 sysdata via `CrowdbSysmdClient` and to
  individual `crowdb-kv-server` mgmt APIs via `ServerClient`. There is
  no `crowdb-web` intermediary. The global connection flags are
  `--sysmd-ip` (default `127.0.0.1`, env `CROWDB_SYSMD_IP`) and
  `--sysmd-port` (default: group-0 REST port, env `CROWDB_SYSMD_PORT`).
- **Short flag aliases.** Global args occupy `-I` (sysmd-ip), `-O`
  (sysmd-port), `-p` (config), `-j` (json) across all subcommands.
- Output: human-readable by default; `--json` flag emits JSON for
  scripting.

The full command hierarchy is defined in the `clap` derive structs;
this section covers design rules only.

### 7.1 Four-Domain Hierarchy

The CLI is split by service domain into four top-level groups, each
cohesive and focused:

- **`cluster`** (alias `cls`) — hardware topology (rack, node,
  disk-group, disk, including runtime hardware state via
  `set-status`) + cluster-level ops (init, reset, clean, status,
  topology). `disk-group` and `disk` live here, not under `chunk`,
  because they are hardware topology concepts — physical disks grouped
  into disk-groups on nodes in racks. The `set-status` /
  `set-dg-status` verbs are executed through the diskdb service API,
  but the CLI verb belongs under `cluster` because it changes hardware
  topology state, not chunk service state. `chunk diskdb` owns only the
  diskdb service lifecycle and maintenance (scan/recalc/compact/
  rebuild).
- **`kv`** — KV layer: `kv server` (crowdb-kv-server lifecycle),
  `kv store` / `kv group` / `kv replica` (logical concepts), `kv put`
  / `get` / `delete` / `scan` / `snapshot` (data-plane). The verb
  distinguishes management from data-plane; no `kv` prefix needed on
  resource names.
- **`chunk`** — chunk storage service cluster: `chunk diskdb` /
  `chunk chunkdb` / `chunk diskio` (server lifecycle + maintenance) +
  future chunk data-plane (`allocate` / `free` / `write` / `read` /
  `gc`). diskdb (block allocator), chunkdb (chunk metadata), diskio
  (disk I/O), and the chunk client lib compose the chunk storage
  service cluster; the group name reflects the unified service, not
  individual servers. Stubs pending implementation.
- **`bench`** — load injection per layer.

The four-domain hierarchy is the **standard concept** across the
production system — not CLI-specific. The console UI (`crowdb-web`)
uses the same domain grouping for its navigation and operation
surfaces (see `design-crowdb-console-ui.md`). The operation logic
behind each verb lives in `crowdb-console-shared`'s `ops` module
(§2.2); both frontends call the same shared operations, so CLI and UI
behave identically.

### 7.2 Command Hierarchy

```
crowdb-cli
│
├── cluster  (alias: cls)           ← hardware topology + cluster-level ops
│   ├── init                        (--nodes; bootstraps group 0 — §7.3)
│   ├── reset                       (full teardown — §13)
│   ├── clean                       (wipe user data, keep metadata + group-0 — §7.4)
│   ├── status
│   ├── topology
│   ├── rack        { add, remove, list }
│   ├── node        { add, remove, list, ping }
│   ├── disk-group  { add, remove, list, set-status }
│   └── disk        { add, remove, list, set-status }
│
├── kv                              ← KV layer: server + logical concepts + data-plane
│   ├── server    { deploy, restart (alias start), stop, delete, list }   (delete — §7.5)
│   ├── store     { add, remove, list, inspect }
│   ├── group     { add, remove, list, inspect }
│   ├── replica   { add, remove }
│   ├── put / get / delete / scan
│   └── snapshot  { create, list, scan, release }
│
├── chunk                           ← chunk storage service cluster (stubs)
│   ├── diskdb    { deploy, restart, stop, delete, list, usage,
│   │               scan-status, scan, recalc, compact, rebuild }
│   ├── chunkdb   { deploy, restart, stop, delete, list }   (future)
│   ├── diskio    { deploy, restart, stop, delete, list }   (future)
│   └── allocate / free / write / read / gc                 (future data-plane)
│
└── bench                           ← load injection
    ├── kv { read, write, scan, mix }
    ├── rpc
    ├── diskdb { allocate, mix }                            (future)
    ├── chunkdb { allocate, mix }                           (future)
    └── chunk { write, read, mix }                          (future)
```

**Three layers max** — `crowdb-cli <domain> <subcommand> <verb>`
(e.g. `kv server deploy`, `kv store add`, `cluster rack list`).
Direct data-plane verbs are two layers (`kv put`, `chunk allocate`).

**Verb vocabulary:**
- Resource CRUD: `add / remove / list / inspect`.
- Server lifecycle: `deploy / restart / stop / delete` — consistent
  across `kv server`, `chunk diskdb`, `chunk chunkdb`, `chunk diskio`.
  `start` is an alias of `restart`. Servers are deployed one-per-node
  by default; `list` enumerates instances across all nodes.
- Data-plane: `put / get / delete / scan`. The API uses `scan` for
  prefix-scan; `list` is management-only (enumerates resources, not
  data), never data-plane.
- Hardware state: `set-status` on `cluster disk` / `cluster disk-group`.

**Logical entity addressing**: store/group/replica/KV commands use
`--store` / `--group`; the backend resolves placement. Server
lifecycle uses `--node`.

**Leaders are elected, not assigned.** `kv group add` takes no
`--leader` flag; leadership is decided by Paxos election.

### 7.3 `cluster init` — bootstrap special case

`cluster init` is the only command that runs before group 0 exists.
It takes `--nodes <n1,n2,n3>` directly (not `--sysmd-ip` /
`--sysmd-port`) and bootstraps group-0/store-0 on those nodes via
direct node REST calls (the `POST /system/init` mechanism, §4.3),
wires remotes, and writes the hardware + KV-cluster topology into
group-0 sysdata. After `cluster init` completes, subsequent commands
use `--sysmd-ip` / `--sysmd-port` to connect to the newly created
group 0.

### 7.4 `cluster clean` — data wipe boundary

`cluster clean` wipes user-layer data across all storage services,
keeping services running and group 0 intact:

- **KV user data** — remove all user stores + groups via the existing
  store/group removal flow (cascades to replicas and on-disk WAL/tree
  cleanup). group-0/store-0 preserved.
- **chunkdb metadata** — chunkdb stores metadata in CROWDB KV; cleaning
  the chunkdb KV store (same as any KV store removal) wipes chunkdb
  metadata.
- **diskio data** — diskio writes at positions it points to; later
  writes overwrite old data. No explicit clean needed — new writes
  supersede old data.
- **diskdb metadata + backing** — remove all diskdb metadata (clean the
  diskdb group(s) in KV sysdata). For file-simulated disks, trim or
  reset the backing file to reclaim space. For real devices, metadata
  removal is sufficient (zones are reclaimed on next allocation).

Services (`crowdb-kv-server`, `crowdb-diskdb`, `crowdb-chunkdb`,
`crowdb-diskio`) stay running. group-0 leadership continues — leaders
are elected, not assigned; as long as group-0 replicas survive, they
elect a leader. Topology (racks/nodes/disk-groups/disks) is preserved.

### 7.5 `kv server delete` — graceful + require-empty

All operations use graceful Paxos reconfiguration — no force-kill
path. `delete` requires the server to be **empty**: no replicas, no
groups, no stores hosted. The operator must delete in bottom-up order
— replicas → groups → stores → server → node — before the server or
node can be removed. The CLI refuses `kv server delete` if the server
still hosts replicas, with an error listing the replicas/groups/stores
that must be removed first. Same policy applies to `cluster node
remove` — the node must have no running servers/services before
removal.

Verb distinction:
- `kv server stop` — graceful process stop (keeps server entry, can
  restart later). No reconfiguration; replicas remain registered on
  peers.
- `kv server delete` — graceful removal (requires empty server).
  Removes the server entry after confirming emptiness. No cascading
  delete — the operator does the cascade manually in bottom-up order.

### 7.6 Bench subcommand

- `bench kv <read|write|scan|mix>` runs KV workloads against a target
  store/group. `bench rpc` measures raw RPC transport throughput.
- Both are stubs pending re-wiring to the `ops` module.

### 7.7 Bench lifecycle verbs (deploy / prepare / run / teardown)

The all-in-one `bench kv` verb deploys a 3-node cluster, pre-populates
keys, runs the workload, and tears down — all in one process. For
regression suites that run many sub-tests against the same cluster
configuration, this pays deploy + pre-pop overhead per sub-test. The
lifecycle verbs split this monolith into discrete steps with persistent
deploy metadata:

- **`bench deploy --name <n> --kind kv --mode mem`** — provisions a
  3-node cluster via `BenchFixture` (embedded console-web), then
  detaches the fixture so the `crowdb-kv-server` processes survive CLI
  exit. The deploy metadata (node pids, endpoints, tunables) is
  serialized to `runtime/<name>/handle.json` (`ClusterHandle`). The
  `--kind` flag dispatches to kv (default), rpc (spawns
  `crowdb-rpc-fb-server`), or chunk/storage (not yet implemented).
- **`bench prepare --target <n> --keys N`** — loads handle, builds a
  `CrowdbClient` from the recorded leader endpoint, and writes N keys
  via sequential `put`. Reuses the same `format_key` / `value_for`
  logic as `bench kv`'s pre-populate path.
- **`bench run --target <n> --workload read ...`** — loads handle,
  builds an `AttachedKvTarget` (implements `BenchTarget` with no-op
  provision/cleanup), and calls the shared `run_bench` runner. Reports
  go to `runtime/<name>/runs/<timestamp>/`. The cluster stays running
  after the run — multiple `bench run` invocations can attach to the
  same deploy.
- **`bench teardown --target <n>`** — loads handle, SIGTERMs the node
  pids via `stop_pid_with_timeout`, removes `handle.json`. Idempotent:
  a second teardown on the same name exits 0 with "already torn down".

The all-in-one `bench kv` verb is preserved as the quick one-shot path.
The regression scripts
(`tools/bench-kv-read-regression.sh`,
`tools/bench-kv-scan-regression.sh`) use the lifecycle flow: deploy
once → prepare once → run N sub-tests → teardown once, amortizing
overhead.

`ClusterHandle` is a runtime artifact (JSON under `runtime/`), not a
config extension. The `runtime/` directory is gitignored. The
`crowdb-kv-server` child processes survive CLI exit because
`lifecycle::deploy_local` spawns them with `kill_on_drop(false)`.

## 8. Error Model and Operation Logging

- `shared::Error` enum covers `NodeUnreachable`, `UpstreamRpc`,
  `Validation`, `NotFound`, `Conflict`. HTTP maps to 4xx/5xx; CLI maps
  to exit codes (0 ok, 1 user error, 2 cluster/network error).
- **Operation log** — a per-session file under `~/.lib/crowdb-kv/log/` records
  every outbound action (HTTP/crowdb-rpc/SSH) with enough detail to reproduce
  by copy-pasting the equivalent curl/crowdb-cli/ssh command.

## 9. Observability

- `tracing` everywhere; `--vv` switches CLI to debug.
- Web backend exposes `/healthz`. **`/metrics` is deferred**. The Rust
  Prometheus story has multiple competing crates; we will pick one when
  broader observability work for `crowdb-kv-server` begins.
- All console-issued operations attach a correlation id propagated as
  `x-crowdb-kv-corr-id` to `crowdb-kv-server` request headers.

## 10. Open Questions

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

## 11. Sysdata sync — rack/node/disk-group/disk handlers

Console add/remove handlers for racks, nodes, disk-groups, and disks
update the console config TOML then sync group-0 sysdata via
`HardwareClient` (built by `build_hardware_client` in
`app/crowdb-web/src/mgmt.rs`). If group 0 is not yet initialized, the
sysdata sync is skipped — `cluster_init` Phase 5 writes the full
hierarchy on bootstrap.

## 12. Cluster reset

`cluster destroy` is full teardown. It is implemented in
`crowdb-console-shared`'s `ops::cluster::reset` as a hybrid operation
— group-0 discovery + direct node teardown — so the CLI no longer
depends on a `crowdb-web` endpoint. The flow:

1. **Discovery** — connect to group 0 (via `--sysmd-ip` /
   `--sysmd-port`) to enumerate all resources: user stores/groups/
   replicas, diskdb/chunkdb/diskio instances, server entries, topology.
2. **Teardown in dependency order** — erase resources one by one:
   remove user groups → user stores → clean group-0 sysdata (rack
   cascade + store records + diskdb service unregister) → SIGTERM each
   node's processes.
3. **Destroy group 0** — tear down group-0/store-0 itself (last, after
   all user resources are gone).
4. **Delete topology** — remove all nodes and racks from
   `runtime-data/crowdb.temp.toml`.
5. **Fast path** — if group 0 is not created (e.g. `cluster init`
   failed or was never run), skip steps 1-3 and use the TOML config
   info (rack/node entries) to clean up any stray processes and clear
   the config.

The `POST /internal/reset` endpoint on `crowdb-kv-server` remains for
UI use; the CLI implements its own teardown via the shared `ops`
module. When no KV servers are running, the RPC steps are skipped
(fast path for E2E test fixtures).
