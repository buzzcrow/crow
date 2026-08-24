<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# R116 ChunkdbService RPC Migration Plan

Design: `doc/working/design-r116-chunkdb-rpc.md`. Backlog:
`doc/backlog/R116-chunkdb-rpc-migration.md`. Goal: migrate all 8
ChunkdbService unary RPCs from tonic/gRPC to crow-rpc, following the
R115/R117 established patterns.

## Phase 1: Schema + ports

- [ ] **Add `CHUNKDB_RPC_BASE` + `ChunkdbRpc` variant** to
      `lib/crow-protocol/src/ports.rs` (base 9961, stride 1). Re-export
      from `lib.rs`. Files: `ports.rs`, `lib.rs`.
- [ ] **Create `chunkdb.fbs` schema** — enums (`FBChunkdbRetCode`,
      `FBEcState`, `FBChunkState`, `FBStripType`, `FBChunkType`),
      nested types (`FBMirrorStrip`, `FBEcStrip`, `FBStripBody` union,
      `FBChunkStrip`, `FBChunk`), 8 request + 8 response tables.
      File: `lib/crow-protocol/src/fbs/chunkdb.fbs`.
- [ ] **Add 3300-3315 message type IDs** to `msg_type.fbs`. File:
      `lib/crow-protocol/src/fbs/msg_type.fbs`.
- [ ] **Update `build.rs`** — add `chunkdb.fbs` to `fbs_files` +
      `flatc --rust --gen-all` invocation. File:
      `lib/crow-protocol/build.rs`.
- [ ] **Update `lib.rs`** — add `chunkdb_generated` private module +
      `chunkdb_fb` public re-export. File:
      `lib/crow-protocol/src/lib.rs`.
- [ ] **Verify schema compiles** — `pixi run cargo build -p
      crow-protocol`. Fix any flatc errors.

## Phase 2: Zero-copy wrappers

- [ ] **Create `fb_wrappers/chunkdb.rs`** — 8 `Ref` wrappers (one per
      response type): `FBAllocateChunkResponseRef`,
      `FBAppendChunkResponseRef`, `FBQueryChunkResponseRef`,
      `FBSealChunkResponseRef`, `FBDeleteChunkResponseRef`,
      `FBDeleteChunkRangeResponseRef`,
      `FBUpdateChunkStripResponseRef`, `FBListChunksResponseRef`.
      Each: `new`, `valid`, `ret_code`, `error_msg`, `request_id`,
      `ok`, `range_start`, `range_end`, + per-response data accessor.
      File: `lib/crow-protocol/src/fb_wrappers/chunkdb.rs`.
- [ ] **Register module** in `fb_wrappers.rs`. File:
      `lib/crow-protocol/src/fb_wrappers.rs`.
- [ ] **Write wrapper unit tests** — build responses, parse via `Ref`,
      verify accessors. Cover Success, NotMyRange, Internal, malformed
      buffer, union variants (Mirror/Ec). File:
      `lib/crow-protocol/tests/chunkdb_wrappers_test.rs`.
- [ ] **Add port allocation test** — verify `CHUNKDB_RPC_BASE` +
      `ChunkdbRpc` port computation, no overlap. File:
      `lib/crow-protocol/tests/ports_test.rs`.
- [ ] **Run protocol tests** — `pixi run test-protocol`.

## Phase 3: Server-side handler

- [ ] **Convert `service.rs` to directory** — `service.rs` →
      `service/mod.rs` + `service/chunkdb_service.rs` (move existing
      tonic service). Update `lib.rs` if needed. Files:
      `app/crow-chunkdb/src/service/`.
- [ ] **Create `chunkdb_rpc_service.rs`** — `ChunkdbRpcService` struct
      (`handler: Arc<LifecycleHandler>`, `rt: Handle`),
      `register_handlers` (8 handlers), `make_handler` closure,
      `handle_<type>` methods (parse → spawn → build → submit),
      `build_<type>_response` functions, `LifecycleError` →
      `FBChunkdbRetCode` mapping, `Chunk` → `FBChunk` converter. File:
      `app/crow-chunkdb/src/service/chunkdb_rpc_service.rs`.
- [ ] **Add deps to `crow-chunkdb` Cargo.toml** — `crow-rpc-ffi` +
      `flatbuffers`. File: `app/crow-chunkdb/Cargo.toml`.
- [ ] **Verify server crate compiles** — `pixi run cargo build -p
      crow-chunkdb`.

## Phase 4: Client transport

- [ ] **Add deps to `crow-chunkdb-client` Cargo.toml** —
      `crow-rpc-ffi` + `flatbuffers`. File:
      `lib/crow-chunkdb-client/Cargo.toml`.
- [ ] **Create `rpc_transport.rs`** — `ChunkdbRpcTransport` struct
      (`server`, `rpc`, `connections`, `next_req_id`), `conn_for`
      (port offset -10), 8 `send_*` methods (build → call → await →
      parse via `Ref` → map to proto), `RpcError` →
      `ChunkdbClientError` mapping, endpoint helpers. File:
      `lib/crow-chunkdb-client/src/rpc_transport.rs`.
- [ ] **Register module + re-export** in `lib.rs`. File:
      `lib/crow-chunkdb-client/src/lib.rs`.
- [ ] **Verify client crate compiles** — `pixi run cargo build -p
      crow-chunkdb-client`.

## Phase 5: ChunkdbClient transport selection

- [ ] **Add `rpc_transport` field + `with_rpc_transport` builder** to
      `ChunkdbClient`. File: `lib/crow-chunkdb-client/src/client.rs`.
- [ ] **Add `with_rpc_retry` helper** — mirrors R115's pattern:
      resolve endpoint, call `transport.send_*`, on `NotMyRange`
      refresh binding + re-route, on transient retry with backoff.
      File: `lib/crow-chunkdb-client/src/client.rs`.
- [ ] **Update all 8 public methods** — check `self.rpc_transport`
      first, delegate to `with_rpc_retry` when set, else existing
      tonic path. File: `lib/crow-chunkdb-client/src/client.rs`.
- [ ] **Verify client crate compiles** — `pixi run cargo build -p
      crow-chunkdb-client`.

## Phase 6: Server wiring

- [ ] **Add `start_chunkdb_rpc_server`** to `main.rs` — derive port,
      create `RpcServer`, listen, start, register handlers, store
      handle. File: `app/crow-chunkdb/src/main.rs`.
- [ ] **Add shutdown integration** — stop crow-rpc server before
      tonic server stops. File: `app/crow-chunkdb/src/main.rs`.
- [ ] **Verify full build** — `pixi run cargo build`.

## Phase 7: Tests + CI

- [ ] **Run affected tests** — `pixi run test-protocol`,
      `pixi run test-kv-core` (ports), `pixi run cargo test -p
      crow-chunkdb-client`, `pixi run cargo test -p crow-chunkdb`.
- [ ] **Run fmt + clippy** — `pixi run cargo fmt --all -- --check`,
      `pixi run cargo clippy --all-targets -- -D warnings`.
- [ ] **Fix any failures** (up to 3 retries per the blocking
      conditions).

## File list

- `lib/crow-protocol/src/ports.rs` — add `CHUNKDB_RPC_BASE` + `ChunkdbRpc`
- `lib/crow-protocol/src/lib.rs` — `chunkdb_generated` + `chunkdb_fb` + re-export
- `lib/crow-protocol/src/fbs/chunkdb.fbs` — NEW schema
- `lib/crow-protocol/src/fbs/msg_type.fbs` — add 3300-3315
- `lib/crow-protocol/build.rs` — add chunkdb.fbs codegen
- `lib/crow-protocol/src/fb_wrappers.rs` — add `pub mod chunkdb`
- `lib/crow-protocol/src/fb_wrappers/chunkdb.rs` — NEW 8 Ref wrappers
- `lib/crow-protocol/tests/chunkdb_wrappers_test.rs` — NEW wrapper tests
- `lib/crow-protocol/tests/ports_test.rs` — add ChunkdbRpc test
- `lib/crow-chunkdb-client/Cargo.toml` — add crow-rpc-ffi + flatbuffers
- `lib/crow-chunkdb-client/src/lib.rs` — add rpc_transport module
- `lib/crow-chunkdb-client/src/rpc_transport.rs` — NEW transport
- `lib/crow-chunkdb-client/src/client.rs` — with_rpc_transport + selection
- `app/crow-chunkdb/Cargo.toml` — add crow-rpc-ffi + flatbuffers
- `app/crow-chunkdb/src/service.rs` → `service/` directory
- `app/crow-chunkdb/src/service/mod.rs` — NEW re-exports
- `app/crow-chunkdb/src/service/chunkdb_service.rs` — MOVED tonic service
- `app/crow-chunkdb/src/service/chunkdb_rpc_service.rs` — NEW crow-rpc handlers
- `app/crow-chunkdb/src/main.rs` — start_chunkdb_rpc_server + shutdown

## Test checklist

**Unit tests:**
- [ ] Port allocation: `CHUNKDB_RPC_BASE` + `ChunkdbRpc` port computation
- [ ] Wrapper: `FBAllocateChunkResponseRef` Success path
- [ ] Wrapper: `NotMyRange` response (range_start/range_end)
- [ ] Wrapper: `Internal` + error_msg
- [ ] Wrapper: malformed buffer (valid() = false)
- [ ] Wrapper: `FBChunkStrip` Mirror union variant
- [ ] Wrapper: `FBChunkStrip` Ec union variant

**Integration tests (existing suite, verify no regression):**
- [ ] `pixi run test-protocol` — all protocol tests pass
- [ ] `pixi run cargo test -p crow-chunkdb-client` — client tests pass
- [ ] `pixi run cargo test -p crow-chunkdb` — server tests pass

**CI:**
- [ ] `pixi run cargo fmt --all -- --check`
- [ ] `pixi run cargo clippy --all-targets -- -D warnings`
