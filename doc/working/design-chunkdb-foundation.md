<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# chunkdb Project Foundation (R85)

Design draft for the chunkdb project foundation. See
`doc/backlog/R85-chunkdb-foundation.md` for the problem statement,
dependencies, and acceptance criteria. Architecture decisions and
rationale are in `doc/design/chunkdb/design-crow-chunkdb.md` §1-§14.

## 1. Proto refinement

### 1.1 Why

The reserved chunkdb protos are missing `ChunkType` (design §5.5) and
`CHUNK_STATE_INIT` (design §9). Without these, the server cannot
distinguish chunk types or represent the initial lifecycle state.

### 1.2 Changes to chunkdb_type.proto

- Add `ChunkType` enum: `CHUNK_TYPE_REPO=0`, `CHUNK_TYPE_WAL=1`,
  `CHUNK_TYPE_BTREE_PAGE=2`, `CHUNK_TYPE_PAGE_INDEX=3`, reserved 4-255.
- Add `CHUNK_STATE_INIT=0` to `ChunkState`, renumbering ACTIVE=1,
  SEALED=2, DELETED=3.
- Add `ChunkType chunk_type` field (field number 8) to `Chunk`.

### 1.3 ChunkId width

Keep the existing 192-bit `ChunkId` (3 × uint64) in
`common_type.proto` — it is already used by diskdb's
`BusyBlockValue.owner_chunk`. The design doc §5.4 specifies 128-bit;
this is a gap (see `chunkdb-gap.md` GAP-1). The chunk ID generator
packs type bits into the high byte and uses timestamp + random across
all 192 bits.

## 2. EC wrapper module

### 2.1 Why

EC strip allocation (R87) and EC encoding/decoding (design §10) need
an erasure-coding primitive. The design specifies isa-l via FFI, but
isa-l is not available on this system and would require an `unsafe`
exception in `crow-common`. See `chunkdb-gap.md` GAP-2.

### 2.2 Implementation

- Use the pure-Rust `reed-solomon-erasure` crate (v6.0.0, GF(2^8)).
- Public API in `lib/crow-common/rust/src/ec.rs`:
  - `EcScheme { data_num, code_num }` — EC configuration.
  - `encode(scheme, data: &[u8]) -> Result<Vec<Vec<u8>>>` — split data
    into `data_num` blocks, compute `code_num` parity blocks.
  - `decode(scheme, blocks: Vec<Option<Vec<u8>>>) -> Result<Vec<u8>>` —
    reconstruct lost blocks, return full data.
- Backend-agnostic API — isa-l can replace `reed-solomon-erasure`
  behind the same interface later.

## 3. Chunk ID generation

### 3.1 Implementation

`lib/crow-common/rust/src/chunk_id.rs`:
- `ChunkIdGen` — stateless generator using `getrandom` + system time.
- `generate(chunk_type: ChunkType) -> ChunkId` — packs chunk type into
  high byte, timestamp (ms since epoch) into high bits 8-55, random
  across remaining bits.
- `hash_to_bucket(id: &ChunkId) -> u16` — xxHash-style hash to 16-bit
  bucket (0-65535) per design §5.4a.
- `chunk_type(id: &ChunkId) -> ChunkType` — extract type from high byte.

## 4. Chunkdb server skeleton

### 4.1 Structure

`app/crow-chunkdb/` — binary + library crate following `app/crow-diskdb`
pattern:
- `src/main.rs` — CLI entrypoint (clap), config loading, gRPC server
  startup, graceful shutdown.
- `src/lib.rs` — module exports for integration tests.
- `src/chunkdb_config.rs` — TOML config (`BaseConfig` impl).
- `src/service.rs` — gRPC `ChunkdbService` stub impl (all RPCs return
  `Unimplemented`).

### 4.2 Config

Minimal config: `listen_addr`, `http_listen_addr`, `kv_server_mgmt_seeds`,
`instance_id`, `topology_refresh_interval_secs`.

## 5. Chunkdb client skeleton

`lib/crow-chunkdb-client/` — library crate following
`lib/crow-diskdb-client` pattern:
- `src/lib.rs` — error types, re-exports.
- `src/client.rs` — `ChunkdbClient` skeleton with method stubs matching
  the 8 RPCs, `DashMap` channel pool, `ServiceRegistryClient` for
  endpoint discovery (stubbed — no retry yet, lands in R90).

## 6. Workspace + pixi wiring

- Add `app/crow-chunkdb`, `lib/crow-chunkdb-client` to root `Cargo.toml`.
- Add `reed-solomon-erasure`, `getrandom` to `lib/crow-common/rust/Cargo.toml`.
- Add pixi tasks: `test-chunkdb`, `test-chunkdb-client`.

## Scope

- `lib/crow-protocol/src/proto/chunkdb_type.proto` — add ChunkType enum, CHUNK_STATE_INIT, chunk_type field on Chunk.
- `lib/crow-protocol/build.rs` — add serde derives for ChunkType.
- `lib/crow-common/rust/Cargo.toml` — add reed-solomon-erasure, getrandom deps.
- `lib/crow-common/rust/src/lib.rs` — add ec, chunk_id module exports.
- `lib/crow-common/rust/src/ec.rs` — EC wrapper module (new).
- `lib/crow-common/rust/src/chunk_id.rs` — chunk ID generation (new).
- `app/crow-chunkdb/Cargo.toml` — new crate.
- `app/crow-chunkdb/src/main.rs` — CLI entrypoint (new).
- `app/crow-chunkdb/src/lib.rs` — module exports (new).
- `app/crow-chunkdb/src/chunkdb_config.rs` — config (new).
- `app/crow-chunkdb/src/service.rs` — gRPC stub service (new).
- `lib/crow-chunkdb-client/Cargo.toml` — new crate.
- `lib/crow-chunkdb-client/src/lib.rs` — error types, re-exports (new).
- `lib/crow-chunkdb-client/src/client.rs` — client skeleton (new).
- `Cargo.toml` — add workspace members.
- `pixi.toml` — add test tasks.

## Complexity

Medium. The proto refinement and crate skeletons are straightforward
(following diskdb patterns). The EC wrapper is the only non-trivial
piece — wrapping `reed-solomon-erasure` behind a clean API. The chunk
ID generator is simple (timestamp + random + type bits).

## Test Design

### Unit tests

- `ec_test.rs` — EC round-trip: encode 6+3, lose up to 3 data blocks,
  decode, verify reconstructed data matches original. Test edge: lose
  exactly `code_num` blocks (recoverable), lose `code_num+1` (unrecoverable).
- `chunk_id_test.rs` — generate IDs for each chunk type, verify type
  bits, uniqueness across rapid generation, hash_to_bucket in 0-65535
  range, uniform distribution.

### Integration tests

- `chunkdb_skeleton_test.rs` — start chunkdb server in-process, connect
  via `ChunkdbClient`, call each stub RPC, verify `Unimplemented` status.
