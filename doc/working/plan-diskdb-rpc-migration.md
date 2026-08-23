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

- [ ] **fb_wrappers module**: `lib/crow-protocol/src/fb_wrappers.rs`
  (index) + `lib/crow-protocol/src/fb_wrappers/diskdb.rs`. Zero-copy
  wrappers for the 11 request + 11 response types — typed accessor
  methods over the flatbuffer root pointer, null-safe.

## Server

- [ ] **DiskdbRpcService**: `app/crow-diskdb/src/service/
  diskdb_rpc_service.rs` — struct holding the same deps as the tonic
  `DiskdbService`; `register_handlers(&self, server: &RpcServer)`.
- [ ] **11 handlers**: one per request msg_type. Reuse the diskdb logic
  bodies from the tonic handler. Build response flatbuffer, submit via
  `submit_response`. Async KV-op paths spawn a tokio task.
- [ ] **service.rs**: add `pub mod diskdb_rpc_service`.
- [ ] **main.rs**: add crow-rpc `RpcServer` startup (listen on
  `DISKDB_RPC_BASE`, register handlers, start). Add `rpc_port` to
  `DiskdbConfig`.
- [ ] **ports.rs**: add `DISKDB_RPC_BASE`.

## Client

- [ ] **DiskdbClient rewrite**: `lib/crow-diskdb-client/src/client.rs`
  — replace tonic `Channel` pool with `RpcClient` + per-endpoint
  `Connection`. Keep endpoint cache + retry logic.
- [ ] **11 client methods**: `allocate_blocks`, `free_blocks`,
  `commit_blocks`, `query_capacity_stats`, `get_disk_group_info`,
  `get_disk_info`, `rebuild_zone_bitmap`, `recalc_disk_usage`,
  `compact_zone`, `trigger_scan`, `get_scan_status`.
- [ ] **Error mapping**: `From<RpcError> for DiskdbClientError` +
  `FBDiskdbRetCode` → `DiskdbClientError`. File:
  `lib/crow-diskdb-client/src/lib.rs`.

## Tests

- [ ] UT: `fb_wrappers::diskdb` — parse built request/response, verify
  accessors.
- [ ] UT: `DiskdbClientError` mapping — all `RpcError` + `FBDiskdbRetCode`
  variants.
- [ ] E2E: `allocate_blocks_crow_rpc` — allocate via crow-rpc, verify
  segments.
- [ ] E2E: `free_and_commit_crow_rpc` — free + commit via crow-rpc.
- [ ] E2E: `query_capacity_crow_rpc` — query via crow-rpc.
- [ ] E2E: `compact_and_scan_crow_rpc` — compact + scan via crow-rpc.
- [ ] E2E: `error_no_space` — verify `NoSpace` ret_code mapping.
- [ ] E2E: `error_not_owner` — verify `NotOwner` + cache refresh retry.
- [ ] E2E: `transport_connection_closed` — kill server mid-call, verify
  retry.
- [ ] E2E: `mixed_rollout` — gRPC + crow-rpc servers simultaneously.

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

None yet — proceeding through the task list. Open questions from the
design doc (grpc_endpoint rename scope, async handler conn_handle
lifetime) are noted there and in `todo_fb.md` Open Issues for review.
