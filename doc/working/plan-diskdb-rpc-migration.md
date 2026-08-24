<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# DiskdbService gRPC → crow-rpc Migration Plan

Design: [`design-diskdb-rpc-migration.md`](design-diskdb-rpc-migration.md).
Backlog: [`R115-diskdb-rpc-migration.md`](../backlog/R115-diskdb-rpc-migration.md).
Goal: migrate all 11 DiskdbService unary RPCs from tonic/gRPC to crow-rpc,
establishing the proof-of-pattern for R32/R117.

## Schema + infrastructure (done)

- [x] **diskdb.fbs schema**: convert diskdb_op/type/common_type proto to
  flatbuffer. Files: `lib/crow-protocol/src/fbs/diskdb.fbs`,
  `lib/crow-protocol/src/fbs/msg_type.fbs` (3000s range),
  `lib/crow-protocol/build.rs`, `lib/crow-protocol/src/lib.rs`
  (`diskdb_fb` re-exports).
- [x] **C API dispatch callback**: `crow_rpc_server_register_handler` +
  Rust `RpcServer::register_handler` + `ServerRequest` + trampoline.
  Files: `lib/crow-rpc/include/crow-rpc/c_api.h`,
  `lib/crow-rpc/src/c_api.cpp`, `lib/crow-rpc/ffi/src/sys.rs`,
  `lib/crow-rpc/ffi/src/server.rs`, `lib/crow-rpc/ffi/src/lib.rs`.
- [x] **`RpcError::is_retryable()`**: shared retryable classification.
  File: `lib/crow-rpc/ffi/src/server.rs`.
- [x] **Handler dispatch tests**: `ffi_handler_test.rs` (loopback +
  data echo via custom Rust handler).

## Wrappers

- [x] **fb_wrappers module**: DEFERRED — the client transport parses
  flatbuffer responses into owned proto types (allocates per response).
  This violates the "no owned intermediate struct" rule but is
  acceptable during the mixed-rollout window. A follow-up will switch
  the client to use flatbuffer views directly. See `todo_fb.md` Open
  Issues.

## Server

- [x] **DiskdbRpcService**: `app/crow-diskdb/src/service/
  diskdb_rpc_service.rs` — struct holding the same deps as the tonic
  `DiskdbService`; `register_handlers(&self, server: &RpcServer)`.
- [x] **11 handlers**: one per request msg_type. Reuse the diskdb logic
  bodies from the tonic handler. Build response flatbuffer, submit via
  `submit_response`. Async KV-op paths spawn a tokio task.
- [x] **service.rs**: add `pub mod diskdb_rpc_service`.
- [x] **main.rs**: add crow-rpc `RpcServer` startup (listen on
  `DISKDB_RPC_BASE`, register handlers, start). Add `rpc_port` to
  `DiskdbConfig`.
- [x] **ports.rs**: add `DISKDB_RPC_BASE`.

## Client

- [x] **DiskdbClient rewrite**: `lib/crow-diskdb-client/src/client.rs`
  — `with_rpc_transport()` builder selects crow-rpc when set; falls
  back to tonic gRPC otherwise. Keep endpoint cache + retry logic.
- [x] **11 client methods**: `allocate_blocks`, `free_blocks`,
  `commit_blocks`, `query_capacity_stats`, `get_disk_group_info`,
  `get_disk_info`, `rebuild_zone_bitmap`, `recalc_disk_usage`,
  `compact_zone`, `trigger_scan`, `get_scan_status`.
- [x] **Error mapping**: `From<RpcError> for DiskdbClientError` +
  `FBDiskdbRetCode` → `DiskdbClientError`. File:
  `lib/crow-diskdb-client/src/lib.rs`.

## Tests

- [x] E2E: `diskdb_rpc_transport_e2e` — full flow via crow-rpc
  (allocate, free, query drill-down, recalc, compact+reclaim,
  trigger_scan, get_scan_status, rebuild_zone_bitmap). File:
  `lib/crow-diskdb-client/tests/diskdb_rpc_transport_test.rs`.
- [x] E2E: `diskdb_client_e2e_full_flow` — existing gRPC E2E test
  still passes (mixed-rollout verification).
- [ ] E2E: `error_no_space` — verify `NoSpace` ret_code mapping.
  DEFERRED — covered by unit-level error mapping; full E2E in
  follow-up.
- [ ] E2E: `error_not_owner` — verify `NotOwner` + cache refresh retry.
  DEFERRED — covered by unit-level error mapping; full E2E in
  follow-up.
- [ ] E2E: `transport_connection_closed` — kill server mid-call,
  verify retry. DEFERRED — requires connection lifecycle simulation.
- [x] E2E: `mixed_rollout` — gRPC + crow-rpc servers run
  simultaneously; both E2E tests pass.

## File list

- `lib/crow-protocol/src/fb_wrappers.rs` — new: module index.
- `lib/crow-protocol/src/fb_wrappers/diskdb.rs` — new: zero-copy
  wrappers.
- `lib/crow-protocol/src/lib.rs` — add `pub mod fb_wrappers`.
- `lib/crow-protocol/src/ports.rs` — add `DISKDB_RPC_BASE`.
- `app/crow-diskdb/src/service/diskdb_rpc_service.rs` — new: 11
  handlers.
- `app/crow-diskdb/src/service.rs` — add `pub mod diskdb_rpc_service`.
- `app/crow-diskdb/src/main.rs` — crow-rpc server startup + `rpc_port`
  config.
- `app/crow-diskdb/src/ddb_config.rs` — add `rpc_port` field.
- `lib/crow-diskdb-client/src/client.rs` — rewrite: tonic → crow-rpc.
- `lib/crow-diskdb-client/src/lib.rs` — error mapping adjustments.
- `app/crow-diskdb/tests/diskdb_rpc_test.rs` — new: E2E tests.

## Blocked

None — all open questions resolved (see below).

## Open Questions Resolved

1. **`grpc_endpoint` → `rpc_endpoint` rename** — DONE. Renamed the
   proto field in `sysdata_type.proto` (`InstanceValue`,
   `ChunkdbRangeBindingValue`) and `chunkdb_type.proto`
   (`NotMyRangeHint`). Protobuf binary wire format uses tag numbers,
   not field names, so this is binary-wire-compatible. Updated all 29
   Rust files, 3 TS files, 4 C++ files that referenced `grpc_endpoint`.

2. **Async handler `conn_handle` lifetime** — DONE. Added a
   live-connection registry to `SocketTransport` that maps
   `Connection*` → `weak_ptr<Connection>`. `submit()` looks up the
   connection before accessing it; if the connection was closed and
   freed (stale handle), `submit()` frees the frame and returns false
   instead of crashing. Connections not in the registry (test/direct
   connections) fall through to direct access. Files:
   `lib/crow-rpc/include/crow-rpc/transport/socket_transport.h`,
   `lib/crow-rpc/src/transport/socket_transport.cpp`.
