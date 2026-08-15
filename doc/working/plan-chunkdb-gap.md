<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# chunkdb Gap Remediation Plan

Gap analysis: `doc/working/chunkdb-gap.md`. Design:
`doc/design/chunkdb/design-crow-chunkdb.md`. Backlog:
`doc/backlog/R85-chunkdb-foundation.md` … `R99-kv-dynamic-range-binding-framework.md`.
Goal: address the 10 ai-todo items from the chunkdb gap review — switch
ChunkId to 128-bit, replace the EC backend with isa-l, harden allocation
with a two-phase commit, simplify storage semantics, return proper error
codes, and land the full-stack E2E harness.

Scope note — two gaps are deferred to R99 (GAP-4 broadcast-free routing,
GAP-7 binding table from group-0) and one needs a separate requirement
(GAP-8 chunk lifecycle lock). They are listed in their own sections with
cross-references; the executable tasks here cover GAP-1, GAP-2, GAP-3,
GAP-5, GAP-6, GAP-9, GAP-10.

## Phase 1 — Proto + ChunkId (GAP-1, GAP-3)

- [ ] **GAP-1: Change ChunkId proto to 128-bit**: replace the 3×uint64
  (high/mid/low) layout with 2×uint64 (high/low), matching the design §5.4
  layout (8 type + 48 timestamp + 72 random) and the existing `DiskId`
  pattern. Files: `lib/crow-protocol/src/proto/common_type.proto`.
- [ ] **GAP-1: Add UUID util in crow-protocol**: Rust wrapper for the
  128-bit `ChunkId` — generation (type + timestamp + random), type
  extraction, byte serialization, and xxHash → 16-bit bucket. Move the
  generation logic out of `crow-common` into `crow-protocol` so the proto
  type and its helpers live together. Files: `lib/crow-protocol/src/lib.rs`,
  `lib/crow-protocol/src/chunk_id.rs` (new), `lib/crow-protocol/src/common_type.rs`.
- [ ] **GAP-1: Update crow-common chunk_id to delegate**: replace the
  192-bit `ChunkIdParts` with a 128-bit struct (high/low) that mirrors the
  proto, or re-export the crow-protocol util. Keep `hash_to_bucket` API
  stable for routing. Files: `lib/crow-common/rust/src/chunk_id.rs`,
  `lib/crow-common/rust/src/lib.rs`.
- [ ] **GAP-1: Update chunkdb consumers of ChunkId.mid**: remove all
  `.mid` field accesses — `chunk_key` (24-byte → 16-byte key), routing
  `chunk_id_to_parts`, lifecycle `generate_chunk_id` call site.
  Files: `app/crow-chunkdb/src/storage.rs`, `app/crow-chunkdb/src/routing.rs`,
  `app/crow-chunkdb/src/lifecycle.rs`.
- [ ] **GAP-1: Update diskdb consumers of ChunkId.mid**: `Segment.owner_chunk`,
  `BusyBlockValue.owner_chunk`, `AllocateBlocksRequest.owner_chunk` all
  change shape; fix any `.mid` accesses in diskdb model + service code.
  Files: `app/crow-diskdb/src/model/alloc.rs`,
  `app/crow-diskdb/src/service/diskdb_service.rs`,
  `lib/crow-protocol/src/diskdb_type_util.rs`.
- [ ] **GAP-1: Update chunkdb-gap.md decision**: after the proto change,
  flip GAP-1's "Decision taken" to reflect the 128-bit design is now
  followed. Files: `doc/working/chunkdb-gap.md`.
- [ ] **GAP-3: Verify ChunkType enum completeness**: the enum + chunk_type
  field are already in `chunkdb_type.proto`; confirm `build.rs` emits serde
  derives and no consumer is missing the field. No code change expected.
  Files: `lib/crow-protocol/build.rs`, `app/crow-chunkdb/src/lifecycle.rs`.

## Phase 2 — EC backend (GAP-2)

- [ ] **GAP-2: Add isa-l FFI bindings**: bind the isa-l Reed-Solomon
  GF(2^8) encode/decode entry points (`ec_encode_data`, `ec_decode_data`)
  via FFI. isa-l is already a pixi dependency (`isa-l = "*"` in pixi.toml,
  libisal installed in the pixi env). Resolve the `unsafe_code = deny`
  constraint by isolating the FFI in a `crow-common` submodule with a
  scoped `#![allow(unsafe_code)]` (or a dedicated `crow-isal-ffi` crate
  if the workspace exception is preferred). Files: `lib/crow-common/rust/Cargo.toml`,
  `lib/crow-common/rust/src/ec_isal.rs` (new), `lib/crow-common/rust/src/lib.rs`.
- [ ] **GAP-2: Swap EC backend to isa-l**: replace the
  `reed-solomon-erasure` calls in `ec.rs` with the isa-l FFI; keep the
  public API (`EcScheme`, `encode`, `decode`, `encode_parity`,
  `decode_data`) unchanged so callers are unaffected. Remove the
  `reed-solomon-erasure` dependency. Files: `lib/crow-common/rust/src/ec.rs`,
  `lib/crow-common/rust/Cargo.toml`.
- [ ] **GAP-2: Write isa-l EC unit tests**: round-trip encode/decode, loss
  of data blocks, loss of parity blocks, mixed loss, too-many-lost error,
  non-divisible length. Files: `lib/crow-common/rust/tests/ec_test.rs`.
- [ ] **GAP-2: Update chunkdb-gap.md decision**: flip GAP-2 to "isa-l
  adopted via FFI wrapper". Files: `doc/working/chunkdb-gap.md`.

## Phase 3 — Allocation hardening (GAP-5)

- [ ] **GAP-5: Add per-instance segment count verification**: in
  `allocate_blocks_parallel`, check each diskdb response returned the
  requested `block_count` before aggregating, so a partial response from
  one instance is not masked by a full response from another. Files:
  `app/crow-chunkdb/src/allocator.rs`.
- [ ] **GAP-5: Retry partial allocation**: when the aggregate count is
  short, re-attempt allocation for just the missing blocks (against the
  same placement plan or a re-selected one), up to a bounded retry count.
  Only fail after retries are exhausted. Files:
  `app/crow-chunkdb/src/allocator.rs`.
- [ ] **GAP-5: Free all on final failure**: if retries are exhausted,
  free every successfully-allocated segment (not just the last batch)
  before returning the error. Files: `app/crow-chunkdb/src/allocator.rs`.
- [ ] **GAP-5 design: two-phase disk-block commit**: design the
  allocate-tentative → commit-on-persist flow. diskdb marks blocks as
  allocated (busy) on `AllocateBlocks` (current behavior, kept), and a
  new `CommitBlocks` RPC from chunkdb marks them as committed after the
  chunk record is persisted to the KV group. Uncommitted blocks are
  reclaimable by the orphan scanner. Add the proto RPC + a
  `commit_state` field to `BusyBlockValue`. Files:
  `lib/crow-protocol/src/proto/diskdb_op.proto`,
  `lib/crow-protocol/src/proto/diskdb_service.proto`,
  `lib/crow-protocol/src/proto/diskdb_type.proto`.
- [ ] **GAP-5: Implement diskdb CommitBlocks handler**: accept the commit
  RPC, flip `commit_state` on the matching `BusyBlockValue` records.
  Files: `app/crow-diskdb/src/service/diskdb_service.rs`,
  `app/crow-diskdb/src/model/alloc.rs`.
- [ ] **GAP-5: Call CommitBlocks after chunk persist**: in
  `allocate_chunk`/`append_chunk`, after `put_chunk` succeeds, send
  `CommitBlocks` for all newly-allocated segments via the diskdb client
  pool. Files: `app/crow-chunkdb/src/lifecycle.rs`,
  `app/crow-chunkdb/src/allocator/pool.rs`.
- [ ] **GAP-5: Update orphan scanner for tentative blocks**: the scanner
  treats `commit_state = tentative` blocks older than a threshold as
  orphans (reclaimable), so a chunkdb crash between allocate and commit
  does not leak. Files: `app/crow-diskdb/src/scanner/` (verify path).

## Phase 4 — Storage semantics (GAP-6)

- [ ] **GAP-6: Remove put_chunk_if_absent**: delete the check-then-write
  method; chunkdb owns a chunk exclusively at allocation time so a plain
  `put_chunk` (overwrite) is correct. Rename any remaining call site.
  Files: `app/crow-chunkdb/src/storage.rs`.
- [ ] **GAP-6: Update allocate_chunk to use put_chunk**: replace the
  `put_chunk_if_absent` call in `allocate_chunk` with `put_chunk`; drop
  the `ChunkAlreadyExists` path from the allocate flow. Files:
  `app/crow-chunkdb/src/lifecycle.rs`.
- [ ] **GAP-6: Update chunkdb-gap.md decision**: flip GAP-6 to "PUT
  override, no put-if-absent". Files: `doc/working/chunkdb-gap.md`.

## Phase 5 — Lifecycle error codes (GAP-9)

- [ ] **GAP-9: DeleteChunk returns not-found on already-deleted**: change
  `delete_chunk` so that deleting an already-deleted chunk returns
  `ChunkNotFound` (not the existing deleted chunk). The service layer
  maps this to gRPC `NOT_FOUND`; callers that want idempotent delete
  treat `NOT_FOUND` as success. Files: `app/crow-chunkdb/src/lifecycle.rs`.
- [ ] **GAP-9: Audit API return types for bool/result codes**: ensure no
  chunkdb RPC handler returns a bare `bool`/success-flag; all status is
  conveyed via gRPC status codes + `LifecycleError` variants. Files:
  `app/crow-chunkdb/src/service.rs`, `app/crow-chunkdb/src/lifecycle.rs`.
- [ ] **GAP-9: Update chunkdb-gap.md decision**: flip GAP-9 to "returns
  not-found; error codes carry status". Files: `doc/working/chunkdb-gap.md`.

## Phase 6 — Full-stack E2E (GAP-10)

- [ ] **GAP-10: Build ChunkdbCluster test harness**: follow the diskdb
  `tests/common/cluster.rs` pattern — start a real 3-node
  `crow-kv-server` cluster, seed hardware metadata, start diskdb +
  chunkdb in-process, wait for readiness. Files:
  `app/crow-chunkdb/tests/common/cluster.rs` (new),
  `app/crow-chunkdb/tests/common/mod.rs` (new).
- [ ] **GAP-10: Write full-stack E2E tests**: allocate → append → seal →
  query → delete round-trip against the real cluster; verify segments
  land in diskdb and chunk metadata in the KV group. Files:
  `app/crow-chunkdb/tests/full_stack_test.rs` (new).
- [ ] **GAP-10: Update chunkdb-gap.md decision**: flip GAP-10 to
  "full-stack harness landed". Files: `doc/working/chunkdb-gap.md`.

## Deferred to R99 (GAP-4, GAP-7)

These are in scope for R99 (`doc/backlog/R99-kv-dynamic-range-binding-framework.md`)
and are not executed in this plan. Listed for traceability.

- [ ] **GAP-4**: keep broadcast `free_blocks` for now; R99 adds precise
  `disk_id → disk_group_id` routing via the binding framework. No task
  here.
- [ ] **GAP-7**: R99 defines the binding table proto schema, adds
  `KVClusterMetaClient` binding-table methods, and wires watch/notify in
  `main.rs` (replacing `default_binding_table(0, 0)`). Review R99's
  current status and check for gaps when R99 starts. No task here.

## Separate requirement (GAP-8)

- [ ] **GAP-8: Create chunk lifecycle lock requirement**: design a
  per-chunk-id lock for lifecycle operations (seal/delete concurrency).
  Requirements: scoped to chunk id, waitable without blocking the thread
  (async-aware), wake on timeout or release, high performance + low cost.
  Create a new backlog doc (R-suffix TBD) before implementation. This
  plan does not implement the lock; it tracks the requirement creation.
  Files: `doc/backlog/backlog.md` (index entry),
  `doc/backlog/R1xx-chunkdb-lifecycle-lock.md` (new).

## File list

- `lib/crow-protocol/src/proto/common_type.proto` — ChunkId 192→128-bit (high/low)
- `lib/crow-protocol/src/proto/chunkdb_type.proto` — verify ChunkType (GAP-3, no change expected)
- `lib/crow-protocol/src/proto/diskdb_op.proto` — add CommitBlocks RPC + request/response (GAP-5)
- `lib/crow-protocol/src/proto/diskdb_service.proto` — add CommitBlocks service method (GAP-5)
- `lib/crow-protocol/src/proto/diskdb_type.proto` — add commit_state to BusyBlockValue (GAP-5)
- `lib/crow-protocol/src/chunk_id.rs` — 128-bit UUID util (new, GAP-1)
- `lib/crow-protocol/src/common_type.rs` — ChunkId helper updates (GAP-1)
- `lib/crow-protocol/src/lib.rs` — export chunk_id module (GAP-1)
- `lib/crow-protocol/src/diskdb_type_util.rs` — remove .mid accesses (GAP-1)
- `lib/crow-protocol/build.rs` — serde derives for ChunkType (GAP-3 verify)
- `lib/crow-common/rust/Cargo.toml` — drop reed-solomon-erasure, add isa-l link flags (GAP-2)
- `lib/crow-common/rust/src/lib.rs` — ec_isal module, chunk_id re-export (GAP-1, GAP-2)
- `lib/crow-common/rust/src/ec.rs` — swap backend to isa-l (GAP-2)
- `lib/crow-common/rust/src/ec_isal.rs` — isa-l FFI bindings (new, GAP-2)
- `lib/crow-common/rust/src/chunk_id.rs` — 128-bit ChunkIdParts (GAP-1)
- `lib/crow-common/rust/tests/ec_test.rs` — isa-l round-trip tests (GAP-2)
- `app/crow-chunkdb/src/storage.rs` — remove put_chunk_if_absent, 16-byte key (GAP-1, GAP-6)
- `app/crow-chunkdb/src/lifecycle.rs` — put_chunk, delete returns not-found, CommitBlocks call, ChunkId.mid removal (GAP-1, GAP-5, GAP-6, GAP-9)
- `app/crow-chunkdb/src/routing.rs` — chunk_id_to_parts 128-bit (GAP-1)
- `app/crow-chunkdb/src/allocator.rs` — per-instance verify, retry partial, free-all (GAP-5)
- `app/crow-chunkdb/src/allocator/pool.rs` — CommitBlocks client method (GAP-5)
- `app/crow-chunkdb/src/service.rs` — error-code audit (GAP-9)
- `app/crow-chunkdb/tests/common/cluster.rs` — full-stack harness (new, GAP-10)
- `app/crow-chunkdb/tests/common/mod.rs` — test module (new, GAP-10)
- `app/crow-chunkdb/tests/full_stack_test.rs` — full-stack E2E (new, GAP-10)
- `app/crow-diskdb/src/model/alloc.rs` — ChunkId.mid removal, CommitBlocks handler (GAP-1, GAP-5)
- `app/crow-diskdb/src/service/diskdb_service.rs` — CommitBlocks RPC, ChunkId.mid removal (GAP-1, GAP-5)
- `app/crow-diskdb/src/scanner/` — tentative-block reclaim (GAP-5, verify path)
- `doc/working/chunkdb-gap.md` — flip decisions for GAP-1/2/6/9/10
- `doc/backlog/backlog.md` — add lifecycle-lock requirement index (GAP-8)
- `doc/backlog/R1xx-chunkdb-lifecycle-lock.md` — lock requirement (new, GAP-8)

## Test checklist

Unit:
- [ ] `crow-protocol` chunk_id: 128-bit generation, type extraction, byte round-trip, hash-to-bucket distribution (GAP-1)
- [ ] `crow-common` ec: isa-l encode/decode round-trip, data loss, parity loss, mixed loss, too-many-lost, non-divisible (GAP-2)
- [ ] `crow-chunkdb` lifecycle: delete on already-deleted returns ChunkNotFound (GAP-9)
- [ ] `crow-chunkdb` allocator: per-instance count mismatch detected, partial retry succeeds then fails-after-exhaustion frees all (GAP-5)

Integration:
- [ ] `crow-chunkdb` storage: put_chunk overwrites (no already-exists path) (GAP-6)
- [ ] `crow-diskdb` CommitBlocks: tentative → committed transition (GAP-5)

E2E:
- [ ] `crow-chunkdb` full_stack_test: allocate → append → seal → query → delete against real KV + diskdb + chunkdb (GAP-10)
- [ ] `crow-chunkdb` full_stack_test: CommitBlocks called after persist; orphan scanner reclaims tentative blocks on simulated crash (GAP-5)

Lint/build:
- [ ] `cargo fmt --check`, `cargo clippy -- -D warnings` on changed crates
- [ ] `clang-format --dry-run --Werror` + `tree-lint` on changed C++ (none expected here)
- [ ] Relevant test tasks in pixi.toml pass
