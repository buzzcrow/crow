<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R85: chunkdb — Project Foundation

**Problem**:

- **Current behavior + impact** — CROW has diskdb (disk-block allocation)
  but no chunk-level management service. The chunkdb proto surface is
  already reserved (`lib/crow-protocol/src/proto/chunkdb_service.proto`,
  `chunkdb_type.proto`, `chunkdb_op.proto`) and the root design doc
  exists (`doc/design/chunkdb/design-crow-chunkdb.md`), but there is no
  chunkdb server crate, no chunkdb client crate, no EC wrapper module in
  `crow-common`, and no chunk ID generation helper. Without these, every
  downstream chunkdb requirement (R86 topology, R87 placement, R88
  storage, R89 lifecycle, R90 client, R91 E2E) is blocked — there is no
  crate to put code in and no EC primitive for strip allocation. The
  reserved protos also have gaps relative to the design doc (no
  `ChunkType` enum, no `CHUNK_STATE_INIT`, `ChunkId` width mismatch —
  see Open Questions) that must be resolved before the server can
  implement the design faithfully.
- **Design pointers** —
  [`doc/design/chunkdb/design-crow-chunkdb.md`](../design/chunkdb/design-crow-chunkdb.md)
  §1 (overview — stateless client of KV + diskdb), §2 (non-goals — no
  data I/O, no local WAL, no consensus, no GC/conversion in v1), §3.5
  (EC at strip level via isa-l in `crow-common`), §3.7 (common protocol
  crate; gRPC now), §3.8 (proto types used directly; no Rust type
  duplication), §5.4 (chunk ID structure), §5.5 (chunk types), §10 (EC
  encoding/decoding), §11 (crate layout), §13 (configuration), §14
  (implementation scope — v1 = R85). aioss analog:
  `/cjdata/cpp/aioss/server/chunkdb/doc/design.md` (overall design),
  `/cjdata/cpp/aioss/libs/protocol/proto/chunkdb/chunkdb.proto` (proto
  reference), `/cpp/buzz-java/buzz-libs/buzz-ni/src/main/java/com/buzz/ni/EC.java`
  (isa-l EC wrapper reference).
- **Use scenarios** —
  - **Developer builds chunkdb for the first time** — a developer runs
    `pixi run build-chunkdb` and the chunkdb server binary compiles,
    links `crow-protocol` + `crow-common` (EC module), and produces an
    empty `crow-chunkdb` binary. `pixi run run-chunkdb` starts it and it
    listens on its configured gRPC port with stub RPC handlers.
  - **EC round-trip in isolation** — a developer calls
    `crow_common::ec::encode` then `decode` on a 6+3 scheme with a
    known data buffer; the decoded data matches the original even after
    simulating the loss of up to `code_num` data blocks. This verifies
    the isa-l FFI wrapper before any strip-allocation code uses it.
  - **Chunk ID generation and partition routing** — a developer calls
    the chunk ID generator for each chunk type (Repo/WAL/BTreePage/PageIndex)
    and verifies the type bits are set, IDs are unique across rapid
    generation, and the ID hashes to a valid logical bucket (0-65535).
  - **Client connects to server stub** — a developer constructs a
    `ChunkdbClient`, connects to a running chunkdb server, and calls
    each stub method (`allocate_chunk`, `seal_chunk`, `delete_chunk`,
    `query_chunk`, `list_chunks`); each returns `Unimplemented` (stub)
    without crashing, proving the gRPC wiring is correct.
  - **Proto gap resolution** — a reviewer checks the refined protos
    against the design doc and confirms the `ChunkType` enum,
    `ChunkState` transitions, and `ChunkId` width are consistent with
    §5.4/§5.5/§9 (see Open Questions for the unresolved width decision).

**Solution**:

**One-line summary**: land the chunkdb project foundation — refine the
reserved protos to match the design doc, add the isa-l EC wrapper in
`crow-common`, create the `crow-chunkdb` server and `crow-chunkdb-client`
skeleton crates, and wire them into the workspace + pixi.

1. **Proto refinement** — `lib/crow-protocol/src/proto/chunkdb_*.proto`
   (already exist as reserved surface; refine, do not recreate):
   - Add `ChunkType` enum to `chunkdb_type.proto` (Repo=0, WAL=1,
     BTreePage=2, PageIndex=3, reserved 4-255) per design §5.5; add
     `ChunkType chunk_type` field to `Chunk`.
   - Add `CHUNK_STATE_INIT = 0` to `ChunkState` (before ACTIVE) per
     design §9; renumber existing values so INIT is the zero-default.
   - Resolve `ChunkId` width (see Open Questions — 128-bit per design
     §5.4 vs 192-bit per existing `common_type.proto`); update proto
     and/or design doc to match the decision.
   - Verify `chunkdb_service.proto` RPC list matches design §4
     (`AllocateChunk`, `AppendChunk`, `SealChunk`, `DeleteChunk`,
     `QueryChunk`, `ListChunks`, `DeleteChunkRange`, `UpdateChunkStrip`).
   - Confirm `build.rs` already compiles the chunkdb protos (it does —
     they are reserved); no new build.rs changes unless fields are added.

2. **EC wrapper module** — `lib/crow-common/src/ec.rs` (new module):
   - Safe Rust FFI wrapper around isa-l (`make_gf_table`,
     `make_decode_gf_table`, `encode`, `decode`, `make_buffer`,
     `destroy_buffer`); reference aioss `EC.java` and
     `/cpp/buzz-java/buzz-libs/buzz-ni`.
   - Lifetime-managed buffers (`EcBuffer` RAII guard frees on drop);
     `unsafe` confined to this module (`unsafe_code = deny` excepted
     only for `crow-tree-ffi` per AGENTS.md — coordinate if `crow-common`
     needs an exception, or keep `unsafe` behind a private inner module
     so the crate-level deny still holds).
   - `isa-l-sys` dependency (or build isa-l from source via `build.rs`);
     Linux-only — tests skip on other platforms via `#[cfg(target_os =
     "linux")]`.
   - Public API: `EcScheme { data_num, code_num }`,
     `encode(scheme, data: &[u8]) -> Vec<u8>` (parity),
     `decode(scheme, data_blocks, parity_blocks, lost_indices) ->
     Vec<u8>` (reconstructed).

3. **Chunk ID generation** — `lib/crow-common/src/chunk_id.rs` (new
   module, reused by server + client + diskdb `owner_chunk`):
   - `ChunkId` newtype over the proto `ChunkId` (width per Open
     Questions decision); `generate(chunk_type) -> ChunkId` using
     `getrandom` for randomness + system timestamp; `chunk_type()`,
     `to_bytes()`, `from_bytes()`, `hash_to_bucket() -> u16` (16-bit
     logical bucket, 0-65535, per design §5.4a).
   - Uniqueness: timestamp + random bits; no global counter (stateless).

4. **Chunkdb server skeleton** — `app/crow-chunkdb/` (new crate):
   - `Cargo.toml`: deps `crow-protocol`, `crow-common`, `tokio`,
     `tracing`, `tonic`.
   - `src/main.rs`: CLI entrypoint (config loading, port binding,
     shutdown handling) following `app/crow-diskdb/src/main.rs` pattern.
   - `src/server.rs`: gRPC server with `ChunkdbService` stub impl —
     every RPC returns `tonic::Status::unimplemented` (real impl in
     R86-R89).
   - `src/types/`: type re-exports from proto (no Rust type duplication
     per design §3.8), chunk ID helpers, state-machine constants.

5. **Chunkdb client skeleton** — `lib/crow-chunkdb-client/` (new crate):
   - `Cargo.toml`: deps `crow-protocol`, `tokio`, `tonic`, `tracing`.
   - `src/client.rs`: `ChunkdbClient` skeleton — method stubs matching
     the 8 RPCs, connection setup, no retry yet (retry lands in R90).
   - Follow `lib/crow-diskdb-client/src/client.rs` structure
     (`DashMap` channel pool, `ServiceRegistryClient` for endpoint
     discovery — but stubbed for now since no chunkdb instances are
     registered yet).

6. **Workspace + pixi wiring**:
   - Add `app/crow-chunkdb`, `lib/crow-chunkdb-client` to root
     `Cargo.toml` workspace members.
   - Add `isa-l-sys` (or source-build) + `getrandom` to
     `lib/crow-common/Cargo.toml`.
   - Add pixi tasks: `build-chunkdb`, `run-chunkdb`, `test-chunkdb`
     (mirroring `build-diskdb` / `run-diskdb` / `test-diskdb`).

**Flow diagram**:

```
  ┌─────────────────────────────────────────────────────────────┐
  │ lib/crow-protocol (existing, refined)                       │
  │   chunkdb_service.proto  chunkdb_type.proto  chunkdb_op.proto│
  │      │                │                    │                 │
  │      │  (tonic codegen)│                    │                 │
  └──────┼────────────────┼────────────────────┘                 │
         ▼                ▼                                        │
  ┌──────────────────┐  ┌──────────────────┐                      │
  │ app/crow-chunkdb │  │ lib/crow-chunkdb-│                      │
  │  (server stub)   │  │  client (stub)   │                      │
  │  ChunkdbService  │  │  ChunkdbClient   │                      │
  │  = Unimplemented │  │  = method stubs  │                      │
  └──────┬───────────┘  └──────────────────┘                      │
         │                                                       │
         ▼                                                       │
  ┌──────────────────┐  ┌──────────────────┐                      │
  │ lib/crow-common  │  │ workspace + pixi │                      │
  │  ec.rs (isa-l)   │  │  members + tasks │                      │
  │  chunk_id.rs     │  └──────────────────┘                      │
  └──────────────────┘                                            │
```

- **Edge cases at a glance**:
  - isa-l not available on non-Linux → EC module compiles but tests
    skip; server/client crates still build (EC is only used by strip
    allocation, not the stub).
  - `ChunkId` width decision unresolved → proto refinement blocked on
    Open Question #1; fallback: keep 192-bit proto as-is, update design
    doc §5.4 to 192-bit (see Open Questions).
  - `unsafe_code = deny` at crate level vs EC FFI → keep `unsafe` in a
    private inner module so the crate-level deny holds; no exception
    needed.
  - Chunk ID collision (same timestamp + same random) → vanishingly
    rare with 72+ random bits; no dedup logic (stateless, KV
    `put_if_absent` catches collisions at persist time in R88).
  - Proto enum renumbering (`CHUNK_STATE_INIT` insertion) →
    wire-incompatible change; safe because no chunkdb data exists yet
    (greenfield).

**Dependencies**:

- **No dependencies on other chunkdb R-items** — this is the
  foundation. R86-R91 all depend on R85.
- **`crow-protocol`** must exist (it does) and compile the chunkdb
  protos (it does — reserved surface).
- **`crow-common`** must exist (it does) for the EC + chunk ID modules.
- **isa-l** system library (or `isa-l-sys` crate) — Linux build
  dependency; must be available in the pixi environment or vendored.
- **`getrandom`** crate — for chunk ID randomness; check it is not
  already a transitive dependency before adding.

**Acceptance**:

**Proto refinement**:
- `chunkdb_type.proto` defines `ChunkType` enum (Repo/WAL/BTreePage/
  PageIndex) and `Chunk` carries `chunk_type` field → `pixi run
  build-protocol` compiles; generated Rust types include `ChunkType`.
  Unit test.
- `ChunkState` includes `CHUNK_STATE_INIT` as the zero-default value;
  existing values renumbered → proto compiles; no existing chunkdb data
  is broken (greenfield). Unit test.
- `ChunkId` width is consistent between `common_type.proto` and design
  doc §5.4 (either both 128-bit or both 192-bit per Open Question #1
  resolution) → design doc updated in the same commit if the decision
  changes the doc. Unit test.

**EC wrapper**:
- `crow_common::ec::encode(6+3, data)` produces 3 parity blocks;
  `decode` with up to 3 lost data blocks reconstructs the original
  data byte-for-byte → Linux only; skip on other platforms. Unit test.
- `encode(8+4, data)` + `decode` with 4 lost blocks round-trips
  correctly → Linux only. Unit test.
- `EcBuffer` drops free isa-l buffers (no leak) → run under
  `pixi run cargo test` with miri or valgrind on one EC test. Unit
  test.
- EC module compiles on non-Linux (tests skip, no link errors) →
  `pixi run cargo check` on a non-Linux target or `#[cfg]` verification.
  Unit test.

**Chunk ID generation**:
- `ChunkId::generate(Repo)` sets the Repo type bits in the metadata
  field; `chunk_type()` returns `Repo` → 1000 IDs generated, all
  correct type. Unit test.
- 10000 rapid `generate` calls produce no duplicate IDs →
  `HashSet` size == 10000. Unit test.
- `hash_to_bucket()` returns values in 0..65535 uniformly → 10000 IDs,
  bucket distribution has no single bucket with > 1% of IDs. Unit
  test.

**Server/client skeleton**:
- `pixi run build-chunkdb` compiles `app/crow-chunkdb` and
  `lib/crow-chunkdb-client` with no errors. Build check.
- `pixi run run-chunkdb` starts the server; it listens on the
  configured gRPC port; calling any RPC returns
  `tonic::Status::unimplemented`. Integration test.
- `ChunkdbClient::connect(endpoint)` succeeds; each stub method
  returns the `unimplemented` status without panicking. Integration
  test.

**Workspace + lint**:
- `pixi run cargo fmt --all -- --check` passes.
- `pixi run cargo clippy --all-targets -- -D warnings` passes.
- `pixi run test-chunkdb` runs the EC + chunk ID unit tests (Linux).
- Root `Cargo.toml` workspace `members` includes `app/crow-chunkdb`
  and `lib/crow-chunkdb-client`; `pixi.toml` has `build-chunkdb`,
  `run-chunkdb`, `test-chunkdb` tasks. Build check.

**Open Questions**:

- **`ChunkId` width — 128-bit or 192-bit?** The design doc §5.4
  specifies 128-bit (8-bit chunk type + 48-bit timestamp + 72-bit
  randomness). The existing `common_type.proto` `ChunkId` is 192-bit
  (high/mid/low = 3×u64). R83 references 192-bit. The diskdb proto
  `Segment.owner_chunk` and `BusyBlockValue.owner_chunk` already use
  the 192-bit `ChunkId`. Options: (a) keep 192-bit (proto + diskdb
  already use it; update design doc §5.4 to 192-bit with a different
  bit layout — e.g. 16-bit metadata + 128-bit UUID + 48-bit reserved,
  matching aioss); (b) change proto to 128-bit (requires migrating
  `owner_chunk` fields in diskdb protos — breaking, but no diskdb data
  exists yet in production). Trade-off: (a) is consistent with existing
  protos and aioss; (b) matches the design doc's stated goal of
  compact 128-bit IDs. Recommendation: (a) — keep 192-bit, update the
  design doc. Cannot be resolved autonomously — it is a design-doc
  vs proto consistency decision that needs a human to confirm.
- **isa-l dependency — `isa-l-sys` crate or build from source?** The
  `isa-l-sys` crate may not exist on crates.io or may be unmaintained;
  building isa-l from source via `crow-common/build.rs` is more
  self-contained but adds a C build dependency. Trade-off: crate is
  simpler if available and maintained; source build is more portable
  but heavier. Needs a check of crates.io availability + maintenance
  status before deciding. Can be resolved autonomously by checking
  crates.io; flagged here in case the preferred path is source build.
- **`unsafe_code = deny` exception for `crow-common` EC module?**
  AGENTS.md says `unsafe_code = deny` except `crow-tree-ffi`. The EC
  FFI wrapper needs `unsafe`. Options: (a) keep `unsafe` in a private
  inner module so the crate-level `deny` still holds (the public API
  is safe); (b) add `crow-common` to the exception list. Recommendation:
  (a) — the public API is safe, `unsafe` is an implementation detail.
  Can be resolved autonomously by trying (a) first; flagged in case
  the lint policy requires an explicit exception.
