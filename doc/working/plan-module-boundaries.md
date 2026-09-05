<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# DiskDB and ChunkDB Module Boundaries Plan

This plan implements [the module-boundary design](design-module-boundaries.md) and [R134](../backlog/R134-diskdb-module-boundaries.md) without changing public behavior or synchronization.

## DiskDB

- [x] **Split liveness subjects**: retain scheduling in `scheduler.rs` and extract heartbeat, observation, reconciliation, and loading. Files: `app/crowdb-diskdb/src/liveness/keepalive.rs`, `app/crowdb-diskdb/src/liveness/keepalive/*.rs`.
- [x] **Split service subjects**: retain registration in `service.rs` and extract mutation, query, admin/scanner, and frame code. Files: `app/crowdb-diskdb/src/service/diskdb_rpc_service.rs`, `app/crowdb-diskdb/src/service/diskdb_rpc_service/*.rs`.

## ChunkDB

- [x] **Split RPC service**: separate registration, handlers, and frame construction behind the unchanged facade. Files: `app/crowdb-chunkdb/src/service/chunkdb_rpc_service.rs`, `app/crowdb-chunkdb/src/service/chunkdb_rpc_service/*.rs`.
- [x] **Split lifecycle coordination**: separate lifecycle orchestration from the existing lock/cache primitive. Files: `app/crowdb-chunkdb/src/lifecycle.rs`, `app/crowdb-chunkdb/src/lifecycle/*.rs`.

## Documentation and Cleanup

- [x] **Write implementation design**: record the module seams and test mapping. Files: `doc/working/design-module-boundaries.md`.
- [~] **Fold permanent design**: update DiskDB and ChunkDB crate layouts, then delete working and backlog artifacts. Files: `doc/design/diskdb/design-crowdb-diskdb.md`, `doc/design/chunkdb/design-crowdb-chunkdb.md`, `doc/backlog/backlog.md`.

## File List

- `app/crowdb-diskdb/src/liveness/keepalive.rs` and `keepalive/*.rs` — DiskDB liveness boundaries.
- `app/crowdb-diskdb/src/service/diskdb_rpc_service.rs` and `diskdb_rpc_service/*.rs` — DiskDB RPC boundaries.
- `app/crowdb-chunkdb/src/lifecycle.rs` and `lifecycle/*.rs` — ChunkDB lifecycle boundaries.
- `app/crowdb-chunkdb/src/service/chunkdb_rpc_service.rs` and `chunkdb_rpc_service/*.rs` — ChunkDB RPC boundaries.
- `doc/working/design-module-boundaries.md` — implementation design.
- `doc/working/plan-module-boundaries.md` — execution state.
- DiskDB and ChunkDB formal designs — folded module structure.
- R134 backlog files — removed after folding.

## Test Checklist

- [x] `pixi run test-diskdb` — liveness, recovery, RPC, scanner, and concurrent allocation behavior.
- [x] `pixi run test-diskdb-client` — public mutation/query/admin wire behavior.
- [x] `pixi run test-chunkdb` — lifecycle locking, routing, RPC, and full-stack behavior.
- [x] `pixi run test-chunkdb-client` — public transport retry and error behavior.
- [x] `pixi run -- cargo fmt --all -- --check` — Rust formatting.
- [x] `pixi run -- cargo clippy --all-targets -- -D warnings` — workspace lint.
- [~] Run every `test-*` task separately for the final local CI check.
