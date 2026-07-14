<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CrowKV - Design: KV Server

Depends on: [`requirement.md`](../requirement.md) [§15.2](../requirement.md#152-crowkv-server)
Satisfies: [`requirement.md`](../requirement.md) §15.2

---

## Table of Contents

- [1. Overview](#1-overview)
- [2. Binary Structure](#2-binary-structure)
- [3. CLI Parsing](#3-cli-parsing)
  - [3.1 Port Pool Parser](#31-port-pool-parser)
  - [3.2 Argument Validation](#32-argument-validation)
  - [3.3 KV Engine Selection](#33-kv-engine-selection)
- [4. Server Architecture](#4-server-architecture)
  - [4.1 Component Diagram](#41-component-diagram)
  - [4.2 Shared State](#42-shared-state)
  - [4.3 Startup Sequence](#43-startup-sequence)
  - [4.4 Shutdown Sequence](#44-shutdown-sequence)
- [5. HTTP Management API](#5-http-management-api)
  - [5.1 Framework Choice](#51-framework-choice)
  - [5.2 Endpoint Design](#52-endpoint-design)
  - [5.3 JSON Schema](#53-json-schema)
  - [5.4 Error Response Format](#54-error-response-format)
- [6. Store Lifecycle](#6-store-lifecycle)
- [7. Group and Replica Wiring](#7-group-and-replica-wiring)
  - [7.1 Add Group Flow](#71-add-group-flow)
  - [7.2 Add Remote Replicas Flow](#72-add-remote-replicas-flow)
  - [7.3 Topology Export](#73-topology-export)
  - [7.4 Batch Wiring Flow](#74-batch-wiring-flow)
- [8. Dependencies](#8-dependencies)

---

## 1. Overview

`crowkv-server` is a thin binary that wires the `crowkv` library into a runnable process. It adds:
- CLI argument parsing for port pool and store count.
- An HTTP management server for runtime topology control.
- Graceful startup and shutdown orchestration.

The server does **not** contain business logic — all consensus, KV operations, and replication are handled by the `crowkv` library (`PxKvStore`, `PxGroup`, etc.).

## 2. Binary Structure

```
crowkv-server/src/
  main.rs          — entry point, CLI parsing, server bootstrap, shutdown
  cli.rs           — argument parsing and port/id list parser
  mgmt_api.rs      — HTTP management API handlers + router
  startup.rs       — store/group creation with WAL and KV engine
  store_registry.rs — KvStoreRegistry (shared state: stores, config, WAL backend)
  lib.rs           — library re-exports for integration tests
```

## 3. CLI Parsing

### 3.1 Port Pool Parser

The `--ports` argument is parsed into a `Vec<u16>`:

```
Input:  "28,39,40..50,59"
Output: [28, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 59]
```

Grammar:
```
port_list  = port_spec (',' port_spec)*
port_spec  = port_range | single_port
port_range = u16 '..' u16          // inclusive [start, end]
single_port = u16
```

Validation rules:
- Each port must be in range 0–65535.
- Range start must be <= end (a single-value range like `40..40` is valid).
- Duplicate ports are silently deduplicated.
- If `--stores` > available ports in pool, exit with error.

### 3.2 Argument Validation

On startup:
1. Parse `--ports` (if provided) into the port pool.
2. If `--stores` > port pool size and ports were explicitly provided, exit with error.
3. If `--ports` is omitted, generate a pool of `--stores` zeros (OS-assigned).

### 3.3 KV Engine Selection

`--kv-engine` chooses the `crowkv::kv::KVEngine` implementation every group's learner is created with, applied consistently at both the CLI bootstrap path (`main.rs`) and the runtime `POST /stores/:sid/groups` path (`mgmt_api.rs::add_group`) via `KvStoreRegistry::kv_engine`:
- `crowtree` (default) — durable, file-backed; each group gets its own file under `--data-root` (default: sibling of `--wal-root` named `ctdata`), recovered by replaying the WAL through it on restart.
- `memory` — in-memory, non-durable `InMemKV`; explicit low-durability/test/dev choice.

`--kv-backend` (`text` default, or `block` for `O_DIRECT` via `BlockPageStore`) only applies when `--kv-engine crowtree` is selected.

## 4. Server Architecture

### 4.1 Component Diagram

```
┌─────────────────────────────────────────────────────┐
│  crowkv-server process                              │
│                                                     │
│  ┌─────────────────────┐   ┌──────────────────────┐ │
│  │  HTTP Management    │   │  AppState               │ │
│  │  (axum, port 9910)  │──▶│  DashMap<u64,Arc<PxKvStore>>│ │
│  └─────────────────────┘   └──────┬───────────────┘ │
│                                   │                 │
│          ┌────────────────────────┼──────────┐      │
│          ▼                        ▼          ▼      │
│  ┌──────────────┐  ┌──────────────┐  ┌────────────┐ │
│  │  PxKvStore 0 │  │  PxKvStore 1 │  │  PxKvStore │ │
│  │  (gRPC :P0)  │  │  (gRPC :P1)  │  │  (gRPC :Pn)│ │
│  │  PxGroup 1   │  │  PxGroup 2   │  │  ...       │ │
│  │  PxGroup 3   │  │  PxGroup 4   │  │            │ │
│  └──────────────┘  └──────────────┘  └────────────┘ │
└─────────────────────────────────────────────────────┘
```

### 4.2 Shared State

```rust
struct KvStoreRegistry {
    stores: DashMap<u64, Arc<PxKvStore>>,
    election_cfg: PxElectionConfig,
    wal_root: PathBuf,
    config_root: PathBuf,
    wal_backend: Arc<IoBackend>,
    kv_engine: KvEngineKind,       // crowtree (default) | memory
    data_root: PathBuf,            // crowtree file root
    crowtree_backend: CrowtreeBackend, // file (default) | block
    port_pool: Mutex<Vec<u16>>,   // from --ports CLI arg
}
```

The `KvStoreRegistry` is created at startup, wrapped in `Arc`, and shared with the HTTP management handlers via `axum::State`. Individual `PxKvStore` instances are `Arc`-wrapped and thread-safe (internal `DashMap` for groups, `Mutex` for server state).

Stores are keyed by `store_id` (u64). The `DashMap` allows dynamic add/remove of stores at runtime via the management API.

No additional synchronization is needed because:
- `KvStoreRegistry::stores` uses `DashMap` (lock-free concurrent map).
- `PxKvStore::add_group` / `remove_group` use `DashMap`.
- `PxGroup` supports `add_remote_replica` / `remove_remote_replica` for mutable remote replica management.

### 4.3 Startup Sequence

```
1. init_file_logging()
2. parse CLI args (--stores, --groups, --replica, --ports, --wal-root,
   --config-root, --data-root, --election-profile, --kv-engine,
   --kv-backend, --management-addr, --management-port)
3. validate: if --ports given, count must >= store count
4. build KvStoreRegistry (election config, WAL root, KV engine, backend)
5. populate port pool from --ports
6. build axum Router from registry
7. bind HTTP management listener on --management-addr:--management-port
8. if --stores provided: create and start stores + groups (bootstrap)
9. serve with graceful shutdown (SIGINT / SIGTERM)
```

The management API starts **before** stores so the server is observable
even if store creation fails. When `--stores` is omitted, the server
boots empty and stores are created via the management API.

### 4.4 Shutdown Sequence

On SIGINT/SIGTERM:
1. Axum's `with_graceful_shutdown` stops accepting new HTTP requests.
2. `graceful_shutdown(registry)` cascades: for each store, `store.stop()`
   then `store.join().await` (waits for gRPC tasks to finish).
3. Log "server shut down cleanly".

## 5. HTTP Management API

### 5.1 Framework Choice

**axum** — lightweight, async, tower-based, good ecosystem fit with tokio. Minimal dependency footprint.

New dependency: `axum`, `serde`, `serde_json`.

### 5.2 Endpoint Design

All endpoints return JSON. Content-Type: `application/json`.

| Method | Path | Handler | Description |
|---|---|---|---|
| `GET` | `/health` | `health_check` | Liveness probe |
| `GET` | `/stores` | `list_stores` | List all stores |
| `GET` | `/stores/:sid` | `get_store` | Store detail |
| `POST` | `/stores` | `add_store` | Add a new store |
| `DELETE` | `/stores/:sid` | `remove_store` | Remove a store |
| `GET` | `/stores/:sid/groups` | `list_groups` | List groups in store |
| `POST` | `/stores/:sid/groups` | `add_group` | Add group to store |
| `DELETE` | `/stores/:sid/groups/:gid` | `remove_group` | Remove group |
| `GET` | `/stores/:sid/groups/:gid/remotes` | `list_remotes` | List remote replicas |
| `POST` | `/stores/:sid/groups/:gid/remotes` | `add_remotes` | Add remote replicas |
| `DELETE` | `/stores/:sid/groups/:gid/remotes/:rid` | `remove_remote` | Remove remote replica |
| `POST` | `/stores/:sid/groups/:gid/remotes/batch` | `batch_add_remotes` | Batch-add from topology export |
| `POST` | `/stores/:sid/groups/:gid/step-down` | `step_down` | Force leader step-down (admin fencing) |
| `POST` | `/stores/:sid/groups/:gid/join` | `join_group_via_snapshot` | New-member snapshot join |
| `GET` | `/topology` | `export_topology` | Full topology export |
| `GET` | `/top` | `export_topology` | Alias for `/topology` |
| `GET` | `/openapi.json` | `openapi_spec` | OpenAPI spec (utoipa-generated) |

### 5.3 JSON Schema

**GET /stores response:**
```json
{
  "stores": [
    {
      "store_id": 0,
      "listen_addr": "0.0.0.0:28001",
      "group_count": 2
    }
  ]
}
```

**GET /stores/:sid response:**
```json
{
  "store_id": 0,
  "listen_addr": "0.0.0.0:28001",
  "groups": [
    {
      "group_id": 1,
      "local_replica_id": 0,
      "leader_id": 0,
      "remote_count": 2
    }
  ]
}
```

**POST /stores request:**
```json
{
  "store_id": 0,
  "group_id": 1,
  "replica_id": 0
}
```

**POST /stores/:sid/groups request:**
```json
{
  "group_id": 1,
  "replica_id": 0
}
```

**POST /stores/:sid/groups/:gid/remotes request:**
```json
[
  {"replica_id": 1, "endpoint": "192.168.1.2:28001"},
  {"replica_id": 2, "endpoint": "192.168.1.3:28001"}
]
```

**GET /topology response:**
```json
{
  "stores": [
    {
      "store_id": 0,
      "listen_addr": "0.0.0.0:28001",
      "groups": [
        {
          "group_id": 1,
          "local_replica_id": 0,
          "endpoint": "0.0.0.0:28001"
        }
      ]
    }
  ]
}
```

### 5.4 Error Response Format

All errors return:
```json
{
  "error": "descriptive error message"
}
```

HTTP status codes:
- `400` — bad request (invalid JSON, missing field, invalid port format)
- `404` — store index or group not found
- `409` — conflict (group_id already exists in store)
- `500` — internal error

## 6. Store Lifecycle

Each `PxKvStore` follows this lifecycle:

```
Created → Groups added (CLI or API) → gRPC serving KV + Paxos → Shutdown
```

At CLI startup, stores are created with groups from `--groups`. The `kv_server.rs` `groups.is_empty()` guard must be removed (DG1) to allow starting with groups that have no remote replicas yet. Additional groups and stores can be added/removed at runtime via the management API.

## 7. Group and Replica Wiring

### 7.1 Add Group Flow

```
POST /stores/0/groups {"group_id": 1, "replica_id": 0}

1. Validate store_id exists
2. Check group_id not already in store
3. Create PxLocalReplica::new(replica_id, Follower)
4. Create PxGroup::new(group_id, local_replica)
5. store.add_group(group)
6. Return 201 Created
```

No role is needed — local replicas start as `Follower`. Leader assignment happens via the topology wiring or leader election.

### 7.2 Add Remote Replicas Flow

`PxGroup` supports `add_remote_replica()` directly (already implemented). No group reconstruction needed.

```
POST /stores/0/groups/1/remotes [{"replica_id": 1, "endpoint": "..."}]

1. Get existing group from store
2. For each remote in request:
   - group.add_remote_replica(PxRemoteReplica::new(replica_id, endpoint))
3. Return 200 OK
```

### 7.2.1 Remove Remote Replica Flow

```
DELETE /stores/0/groups/1/remotes/2

1. Get existing group from store
2. group.remove_remote_replica(replica_id)
3. Return 200 OK (or 404 if not found)
```

**Lib enhancement needed:** `PxGroup::remove_remote_replica()` — see DG2.

### 7.3 Topology Export

`GET /topology` (alias: `GET /top`) iterates all stores and groups, collecting:
- Store ID and bound gRPC address
- For each group: group_id, local_replica_id, and the store's gRPC endpoint

This produces a JSON document that another server can consume to batch-add remotes.

### 7.4 Batch Wiring Flow

`POST /stores/:sid/groups/:gid/remotes/batch` accepts another server's topology export and filters it to find groups matching `gid`, then adds those entries as remote replicas.

## 8. Dependencies

New crate dependencies for `crowkv-server`:

| Crate | Purpose |
|---|---|
| `clap` | CLI argument parsing with derive macros |
| `axum` | HTTP framework for management API |
| `serde` | JSON serialization/deserialization |
| `serde_json` | JSON format |
| `tokio` | Async runtime (already present) |
| `tracing` | Logging (already present) |
