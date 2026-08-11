<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROW - Design: KV Server

Depends on: [`design-crow-kv.md`](design-crow-kv.md) [§15.2](design-crow-kv.md)
Satisfies: [`design-crow-kv.md`](design-crow-kv.md) §15.2

---

## 1. Overview

`crow-kv-server` is a thin binary that wires the `crow-kv` library into a
runnable process. It adds CLI argument parsing, an HTTP management
server for runtime topology control, and graceful startup/shutdown
orchestration. The server does **not** contain business logic — all
consensus, KV operations, and replication are handled by the `crow-kv`
library.

## 2. Design Decisions

### 2.1 KV engine selection

`--kv-engine` chooses the `KVEngine` implementation for all groups:

- **`crow-tree` (default)** — durable, file-backed. Each group gets its
  own file under `--data-root`, recovered by replaying the WAL on
  restart.
- **`memory`** — in-memory, non-durable. Explicit low-durability choice
  for tests and dev.

`--kv-backend` (`text` default, or `block` for `O_DIRECT`) only applies
when `--kv-engine crow-tree` is selected.

### 2.2 Startup ordering

The management API starts **before** stores so the server is observable
even if store creation fails. When `--stores` is omitted, the server
boots empty and stores are created via the management API.

With persistent cluster config, the server auto-loads its store/group configuration from
`conf/node-config.json` (per-node config cache) on startup. If the
cache exists, `--stores`/`--groups` CLI args are not needed — the
server restores all stores/groups from the cache, replays WAL, and
rejoins the cluster. If the cache is missing, explicit CLI args serve
as fallback. After store creation, the server reconciles with group 0
topology KV if group 0 is reachable and finalized.

### 2.3 Concurrency model

`KvStoreRegistry` holds stores in a `DashMap` (lock-free concurrent map).
`PxKvStore` uses `DashMap` for groups. `PxGroup` supports
`add_remote_replica` / `remove_remote_replica` for mutable remote
management. No additional synchronization is needed — all shared state
is already thread-safe via these structures.

### 2.4 HTTP framework: axum

Chosen for lightweight async + tower ecosystem fit with tokio. The full
endpoint list is defined in the Axum route handlers and the OpenAPI spec
at `/openapi.json`; this doc covers design decisions only.

Key endpoint groups:

- **Store/group CRUD** — `/stores`, `/stores/:sid/groups` for runtime
  topology control.
- **Remote replica wiring** — `/stores/:sid/groups/:gid/remotes` for
  adding/removing peer replicas. `batch` endpoint accepts another
  server's topology export for bulk wiring.
- **Topology export** — `GET /topology` produces a JSON document that
  another server can consume to batch-add remotes.
- **System group** — `POST /system/init` bootstraps store 0 + group 0
  on this node. (`POST /topology/finalize` and `GET /topology/ready`
  are removed — persistent topology-record management moves to
  `crow-kv-client`'s `KVClusterMetaClient` / `HardwareClient`. See
  [`design-crow-kv-group0.md`](design-crow-kv-group0.md) for the
  group-0 sysdata architecture.) The lifecycle endpoints (`add_store`,
  `add_group`, etc.) are now **internal** — only `crow-kv-client`'s
  `KVClusterAdmin` calls them. See `../console/design-crow-console.md` §4.3 for the
  full persistent cluster config design.
- **Admin operations** — `step-down` (force leader step-down), `join`
  (new-member snapshot join), `flush` (drain the local replica's L0
  memtable into L1 in memory; used by the bench's
  `--flush-after-prepopulate` flag and as an admin drain).

### 2.5 Group lifecycle

Local replicas start as `Follower` — no role assignment needed at group
creation. Leader assignment happens via topology wiring or leader
election. Groups can be added/removed at runtime via the management
API, not just at CLI bootstrap.

### 2.6 Shutdown

On SIGINT/SIGTERM, Axum stops accepting new HTTP requests, then
`graceful_shutdown` cascades through each store:

1. **`PxKvStore::shutdown`** — stops the gRPC server (cuts frontend
   load), then cascades into each group.
2. **`PxGroup::shutdown`** — cancels the tenure token and awaits the
   election driver + maintenance loop, closes remote gRPC channels,
   then cascades into the local replica.
3. **`PxLocalReplica::shutdown`** — calls `KVEngine::flush` (drain L0
   memtable into L1 B+tree, in-memory) then
   `KVEngine::persist_snapshot` (write dirty L1 pages + superblock to
   the page store, durable). For `InMemKV` both are no-ops. Acceptor
   and learner resource release is deferred to `Drop`.

This ensures in-memory engine state reaches the block file before the
process exits, so `resume_from_slot` is non-zero on restart and WAL
replay can skip the durable prefix.

## 3. Port Pool

The `--ports` argument supports comma-separated single ports and
inclusive ranges (e.g. `28,40..50,59`). Duplicates are silently
deduplicated. If `--stores` exceeds the port pool size, the server
exits with an error. When `--ports` is omitted, ports are OS-assigned
(zeros).
