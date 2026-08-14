<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# chunkdb Project Foundation Plan

Design: `doc/working/design-chunkdb-foundation.md`. Backlog:
`doc/backlog/R85-chunkdb-foundation.md`. Goal: land the chunkdb
project foundation — refined protos, EC wrapper, chunk ID generation,
server/client skeletons, workspace wiring.

## Proto + common

- [ ] **Refine chunkdb protos**: add ChunkType enum, CHUNK_STATE_INIT, chunk_type field. Files: `lib/crow-protocol/src/proto/chunkdb_type.proto`, `lib/crow-protocol/build.rs`.
- [ ] **Add EC module**: reed-solomon-erasure wrapper. Files: `lib/crow-common/rust/Cargo.toml`, `lib/crow-common/rust/src/lib.rs`, `lib/crow-common/rust/src/ec.rs`.
- [ ] **Add chunk_id module**: ChunkId generator + hash_to_bucket. Files: `lib/crow-common/rust/src/chunk_id.rs`.

## Server skeleton

- [ ] **Create chunkdb server crate**: Cargo.toml, main.rs, lib.rs, config, service stub. Files: `app/crow-chunkdb/Cargo.toml`, `app/crow-chunkdb/src/main.rs`, `app/crow-chunkdb/src/lib.rs`, `app/crow-chunkdb/src/chunkdb_config.rs`, `app/crow-chunkdb/src/service.rs`.

## Client skeleton

- [ ] **Create chunkdb client crate**: Cargo.toml, lib.rs, client.rs stubs. Files: `lib/crow-chunkdb-client/Cargo.toml`, `lib/crow-chunkdb-client/src/lib.rs`, `lib/crow-chunkdb-client/src/client.rs`.

## Wiring

- [ ] **Workspace + pixi**: add members to Cargo.toml, add test tasks to pixi.toml. Files: `Cargo.toml`, `pixi.toml`.

## Tests

- [ ] **EC unit test**: round-trip encode/decode. File: `lib/crow-common/rust/tests/ec_test.rs`.
- [ ] **Chunk ID unit test**: generation, type bits, hash, uniqueness. File: `lib/crow-common/rust/tests/chunk_id_test.rs`.
- [ ] **Build + clippy**: verify everything compiles and passes lint.

## File list

- `lib/crow-protocol/src/proto/chunkdb_type.proto` — add ChunkType, CHUNK_STATE_INIT, chunk_type field
- `lib/crow-protocol/build.rs` — add serde derive for ChunkType
- `lib/crow-common/rust/Cargo.toml` — add reed-solomon-erasure, getrandom
- `lib/crow-common/rust/src/lib.rs` — add ec, chunk_id modules
- `lib/crow-common/rust/src/ec.rs` — EC wrapper (new)
- `lib/crow-common/rust/src/chunk_id.rs` — chunk ID gen (new)
- `lib/crow-common/rust/tests/ec_test.rs` — EC tests (new)
- `lib/crow-common/rust/tests/chunk_id_test.rs` — chunk ID tests (new)
- `app/crow-chunkdb/Cargo.toml` — server crate (new)
- `app/crow-chunkdb/src/main.rs` — entrypoint (new)
- `app/crow-chunkdb/src/lib.rs` — module exports (new)
- `app/crow-chunkdb/src/chunkdb_config.rs` — config (new)
- `app/crow-chunkdb/src/service.rs` — gRPC stub (new)
- `lib/crow-chunkdb-client/Cargo.toml` — client crate (new)
- `lib/crow-chunkdb-client/src/lib.rs` — errors, re-exports (new)
- `lib/crow-chunkdb-client/src/client.rs` — client skeleton (new)
- `Cargo.toml` — add workspace members
- `pixi.toml` — add test tasks
