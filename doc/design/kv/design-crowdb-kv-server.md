<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB - Design: KV Server

Depends on: [`design-crowdb-kv.md`](design-crowdb-kv.md) §15.2
Satisfies: [`design-crowdb-kv.md`](design-crowdb-kv.md) §15.2

---

## Table of Contents

- [1. Overview](#1-overview)
- [2. Design Decisions](#2-design-decisions)
  - [2.1 KV engine selection](#21-kv-engine-selection)
  - [2.2 Startup ordering](#22-startup-ordering)
  - [2.3 Concurrency model](#23-concurrency-model)
  - [2.4 HTTP framework: axum](#24-http-framework-axum)
  - [2.5 Group lifecycle](#25-group-lifecycle)
  - [2.6 Shutdown](#26-shutdown)
- [3. Port Pool](#3-port-pool)
- [4. Group/store cleanup on remove](#4-groupstore-cleanup-on-remove)
- [5. Wipe user data](#5-wipe-user-data)

---

## 1. Overview

`crowdb-kv-server` is a thin binary that wires the `crowdb-kv` library into a
runnable process. It adds CLI argument parsing, an HTTP management
server for runtime topology control, and graceful startup/shutdown
orchestration. The server does **not** contain business logic — all
consensus, KV operations, and replication are handled by the `crowdb-kv`
library.

## 2. Design Decisions

### 2.1 KV engine selection

`--kv-engine` chooses the `KVEngine` implementation for all groups:

- **`crowdb-tree` (default)** — durable, file-backed. Each group gets its
  own file under `--data-root`, recovered by replaying the WAL on
  restart.
- **`memory`** — in-memory, non-durable. Explicit low-durability choice
  for tests and dev.

`--kv-backend` (`text` default, or `block` for `O_DIRECT`) only applies
when `--kv-engine crowdb-tree` is selected.

### 2.2 Startup ordering

The management API starts **before** stores so the server is observable
even if store creation fails. `--root <dir>` is required on every
start; it derives the four node paths via `CrowDBConfig::apply_root`:
`wal_root = <root>/waldata`, `config_root = <root>/conf`,
`data_root = <root>/ctdata`, `log_dir = <root>/log` (fixed subfolder
names, the only supported layout). `--config <toml>` is optional and
supplies first-boot tunable overrides only; when omitted, tunables
come from `CrowDBConfig::default()`. The config-file watcher runs only
when `--config` is passed.

Boot has two modes, selected by whether group 0 is on disk
(`restore::group0_exists` checks `<wal_root>/store0/group0`):

- **Restore mode** — group 0 is on disk. `restore::scan_local_groups`
  enumerates every `store{S}/group{G}` directory under `<wal_root>`,
  and `restore::load_local_groups` creates each `PxKvStore` and loads
  every group via `create_group_with_wal` (replays the WAL, opens the
  crowdb-tree engine, and applies persisted membership from
  `conf/node-config.json` — including remote-replica endpoints). The
  local `replica_id` per group is read from `node-config.json`, falling
  back to the `--replica` CLI arg. `--stores`/`--groups` are ignored
  (warned); local disk is the source of truth for which stores/groups
  this node hosts. After load, `reconcile::reconcile_with_group0`
  compares local state against group 0's `/kv/replica/` records:
  groups that came up with no remotes (`node-config.json` missing or
  stale for them) get their remotes seeded from group 0 via a group
  rebuild; groups that already have remotes are verified and any
  peer present in group 0 but not wired locally is logged (the live
  membership is not forcibly overwritten — it may be legitimately
  ahead of group 0 during an in-flight reconfiguration). Reconciliation
  is best-effort: if group 0 is unreachable or has no `/kv/replica/`
  records, the node continues with local state and retries on the next
  restart.
- **First-boot mode** — no group 0 on disk. The server boots empty
  (or from `--stores`/`--groups` if given) and the operator calls
  `POST /system/init` to create store 0 / group 0.

A scan IO error is treated as empty (first-boot mode); a failed
`create_group_with_wal` for one group is logged and skipped while the
store still starts with its other groups.

### 2.3 Concurrency model

`KvStoreRegistry` holds stores in a `DashMap` (lock-free concurrent map).
`PxKvStore` uses `DashMap` for groups. `PxGroup` supports
`add_remote_replica` / `remove_remote_replica` for mutable remote
management. No additional synchronization is needed; all shared state
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
  are removed. Persistent topology-record management moves to
  `crowdb-kv-client`'s `KVClusterMetaClient` / `HardwareClient`. See
  [`design-crowdb-kv-group0.md`](design-crowdb-kv-group0.md) for the
  group-0 sysdata architecture.) The lifecycle endpoints (`add_store`,
  `add_group`) are now **internal**; only `crowdb-kv-client`'s
  `KVClusterAdmin` calls them. See `../console/design-crowdb-console.md` §4.3 for the
  full persistent cluster config design.
- **Admin operations** — `step-down` (force leader step-down), `join`
  (new-member snapshot join), `flush` (drain the local replica's L0
  memtable into L1 in memory; used by the bench's
  `--flush-after-prepopulate` flag and as an admin drain).

### 2.5 Group lifecycle

Local replicas start as `Follower`; no role assignment needed at group
creation. Leader assignment happens via topology wiring or leader
election. Groups can be added/removed at runtime via the management
API, not just at CLI bootstrap.

### 2.6 Shutdown

On SIGINT/SIGTERM, Axum stops accepting new HTTP requests, then
`graceful_shutdown` cascades through each store:

1. **`PxKvStore::shutdown`** — stops the crowdb-rpc server (cuts frontend
   load), then cascades into each group.
2. **`PxGroup::shutdown`** — cancels the tenure token and awaits the
   election driver + maintenance loop, closes remote crowdb-rpc channels,
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

## 4. Group/store cleanup on remove

The `remove_group` and `remove_store` mgmt handlers delete the WAL +
engine dirs and update `node-config.json` in addition to the
in-memory `PxKvStore::remove_*`. The `node-config.json` update is the
fence against resurrection on restart — without it, a removed group
reappears from persisted config and collides with a new group at the
same `(store_id, group_id)`.

- **I1 — ID reuse safety**: After a group/store is removed, its ID
  can be safely reused. The `node-config.json` update is the fence.

## 5. Wipe user data

`POST /stores/{sid}/groups/{gid}/wipe-user-data` drops and recreates
WAL + engine user data for a group, preserving group0 sysdata +
topology. Used by `bench clean` to reset a cluster between
write-regression sub-tests without a full redeploy. The handler
steps down if leader, deletes the WAL + engine dirs, recreates the
group via `create_group_with_wal` (replays empty WAL → fresh state),
and does `remove_group` + `add_group` (remove-before-add is critical:
`add_group` calls `inherit_local_state_from(prior)` which would
`Arc::clone` the prior's engine + WAL, undoing the wipe).

- **I2 — Wipe preserves group0**: `wipe-user-data` drops WAL + engine
  user data only; group0 sysdata + `node-config.json` + topology are
  untouched.
