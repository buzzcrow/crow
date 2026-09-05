<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# DiskDB and ChunkDB Module Boundaries (R134)

This draft expands the [R134 backlog item](../backlog/R134-diskdb-module-boundaries.md) for the DiskDB boundaries described in [the DiskDB design](../design/diskdb/design-crowdb-diskdb.md) §8, §11, and §17–§19. It also covers the equivalent ChunkDB service and lifecycle boundaries in [the ChunkDB design](../design/chunkdb/design-crowdb-chunkdb.md) §9–§13. Architecture decisions and rationale are in the root designs; this doc does not repeat them.

## 1. DiskDB Liveness

### 1.1 Why

`KeepAlive` owns a stable public scheduler interface, but its implementation previously placed heartbeat publication, group-0 observation, runtime reconciliation, and asynchronous zone loading in one file. These operations change for different reasons and already communicate through narrow method calls.

### 1.2 Structure

`keepalive.rs` becomes a pure module index. `scheduler.rs` retains `KeepAlive`, construction, `tick`, and `BackgroundTask`; the four internal subjects remain private child modules and add no state or synchronization.

Edge cases:

- Partial observation retains the existing last-known-good and degraded behavior.
- Owner or bind changes retain generation fencing during asynchronous loading.
- Recovery task cancellation and shutdown retain the existing ordering.

## 2. DiskDB Service

### 2.1 Why

`DiskdbRpcService` registration is stable infrastructure, while mutation, query, administrative, scanner, and FlatBuffer frame code evolve independently.

### 2.2 Structure

`diskdb_rpc_service.rs` becomes a pure index. `service.rs` owns dependencies and handler registration. Internal modules group mutations, queries, administrative/scanner operations, and frame construction. The existing `mutation_gate` remains the single mutation gate.

Edge cases:

- Request parsing remains zero-copy inside the current frame lifetime.
- Response message types and typed errors remain unchanged.
- Spawned tasks retain the same `Arc` ownership and connection handle lifetime.

## 3. ChunkDB Boundaries

### 3.1 Why

ChunkDB had the same mixed registration, handler, and frame construction in `ChunkdbRpcService`. Its lifecycle file also mixed the approved per-chunk serialization/cache primitive with lifecycle orchestration.

### 3.2 Structure

The service uses the same pure-index and private-child-module structure as DiskDB. The lifecycle index re-exports the existing public API from `handler.rs`; `lock_map.rs` owns `ChunkLockMap` and `ChunkGuard`. The existing bounded per-chunk `tokio::Mutex` is moved unchanged.

Edge cases:

- `NotMyRange` hints retain their response fields.
- Lock timeout, cache invalidation, and idle reaping behavior remain unchanged.
- No new blocking or asynchronous lock is introduced.

## 4. Scope

- `app/crowdb-diskdb/src/liveness/keepalive.rs` and `keepalive/` — pure index, scheduler, heartbeat, observation, reconciliation, and loading.
- `app/crowdb-diskdb/src/service/diskdb_rpc_service.rs` and `diskdb_rpc_service/` — pure index, registration, handler groups, and frame construction.
- `app/crowdb-chunkdb/src/service/chunkdb_rpc_service.rs` and `chunkdb_rpc_service/` — pure index, registration, handler groups, and frame construction.
- `app/crowdb-chunkdb/src/lifecycle.rs` and `lifecycle/` — pure index, lifecycle handler, lock/cache primitive, and state machine.
- `doc/design/diskdb/design-crowdb-diskdb.md` — permanent DiskDB module layout.
- `doc/design/chunkdb/design-crowdb-chunkdb.md` — permanent ChunkDB module layout.

## 5. Complexity

Low. The work moves established code behind private module boundaries without changing protocols, state, or algorithms. The main risk is accidental visibility or ownership change across extracted modules, covered by compilation, Clippy, and the existing integration suites.

## 6. Test Design

### 6.1 Integration tests

- Run the DiskDB server suite: existing sync, recovery, mutation, scanner, and concurrent allocation scenarios must remain unchanged.
- Run the ChunkDB server suite: lifecycle transitions, concurrent same-chunk serialization, different-chunk progress, routing, and full-stack RPC behavior must remain unchanged.

### 6.2 End-to-end tests

- Run the DiskDB client suite and verify allocate, commit, free, ownership routing, compaction, and reuse through the public client.
- Run the ChunkDB client suite and verify error classification and retry behavior through the unchanged public transport API.

## 7. Module Structure

```text
liveness/keepalive.rs                    pure index
liveness/keepalive/scheduler.rs          public facade and tick orchestration
liveness/keepalive/{heartbeat,observation,reconciliation,loading}.rs
service/diskdb_rpc_service.rs            pure index
service/diskdb_rpc_service/{service,mutations,queries,admin,wire}.rs

lifecycle.rs                             pure index
lifecycle/{handler,lock_map,state}.rs
service/chunkdb_rpc_service.rs            pure index
service/chunkdb_rpc_service/{service,mutations,queries,wire}.rs
```

## 8. Server Wiring

No wiring changes are required. Existing imports continue to resolve through public re-exports from the pure index modules.
