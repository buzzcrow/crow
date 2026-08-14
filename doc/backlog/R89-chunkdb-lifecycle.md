<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R89: chunkdb — Lifecycle Management

**Problem**:

- **Current behavior + impact** — chunkdb must implement the core chunk
  lifecycle operations: allocate, seal, and delete chunks with proper
  state management. There is no lifecycle layer in the chunkdb server
  yet (R85-R88 land skeleton, topology, placement, storage). Without
  lifecycle handlers, the gRPC `ChunkdbService` RPCs (`AllocateChunk`,
  `AppendChunk`, `SealChunk`, `DeleteChunk`, `QueryChunk`,
  `ListChunks`) have no implementation — the R85 skeleton returns
  `Unimplemented` for every call. Without a state machine, concurrent
  or invalid transitions (e.g. sealing a deleted chunk, deleting a
  chunk twice) would corrupt chunk metadata or leak disk blocks. This
  is the user-facing API surface of chunkdb; every client call (R90)
  and every E2E test (R91) exercises it.
- **Design pointers** —
  [`doc/design/chunkdb/design-crow-chunkdb.md`](../design/chunkdb/design-crow-chunkdb.md)
  §3.2 (strip as atomic redundancy unit — allocate fully or not at
  all), §8 (allocation flow — generate ID, topology snapshot, strip
  layout, parallel allocate, rollback, persist, return), §9 (chunk
  lifecycle — `Init → Active → Sealed → Deleted` state machine,
  transition rules, concurrency via KV CAS), §10 (EC encoding —
  parity deferred in v1, `EC_STATE_NO_PARITY` at allocation),
  `chunkdb_service.proto` / `chunkdb_op.proto` (RPC request/response
  shapes). aioss analog: aioss chunkdb `allocate_chunk` / `seal_chunk`
  / `delete_chunk` handlers; CROW follows the same state machine with
  CROW's KV persistence + diskdb allocation (design §9 — direct port
  of the state machine, adapted to CROW's `ChunkStore` + `ChunkAllocator`).
- **Use scenarios** —
  - **Allocate a mirror chunk** — a client calls `AllocateChunk` with
    `strip_type=Mirror`, `strip_count=3`, chunk type Repo; chunkdb
    generates a chunk ID, fetches a topology snapshot (R86), calculates
    the strip layout, calls the `ChunkAllocator` (R87) for parallel
    diskdb allocation with rollback, builds the `Chunk` proto with
    `state=Active`, persists it to KV via `ChunkStore.put_if_absent`
    (R88), returns the chunk. The chunk is immediately writable.
  - **Allocate an EC chunk** — same flow with `strip_type=EC`,
    `data_num=8`, `code_num=4`; the strip is allocated with
    `EC_STATE_NO_PARITY` (parity is computed later by the caller or a
    deferred step, design §10).
  - **Append strips to an active chunk** — a client calls `AppendChunk`
    on an `Active` chunk to grow it; chunkdb validates the state,
    allocates new strips via the `ChunkAllocator`, appends them to the
    chunk's strip list, persists the updated `Chunk`.
  - **Seal a chunk** — a client calls `SealChunk` with `seal_length`
    after all writes are done; chunkdb validates the chunk is `Active`,
    sets `state=Sealed`, records `sealed_length` + `sealed_ts_ms`,
    persists. No further appends allowed.
  - **Delete a chunk** — a client calls `DeleteChunk`; chunkdb
    validates the chunk is `Active` or `Sealed`, sets `state=Deleted`,
    frees all disk blocks via diskdb `FreeBlocks` (parallel, best-
    effort), persists the deleted state. The chunk's `Segment`s are
    reclaimed.
  - **Query a chunk** — a client calls `QueryChunk` with a chunk ID;
    chunkdb routes to the KV group (R88), reads the `Chunk`, returns
    it. Works for any state (`Active`, `Sealed`, `Deleted`).
  - **List chunks** — a client calls `ListChunks` with pagination;
    chunkdb scans the KV group's keyspace (R88), returns a page of
    chunks in chunk ID order.
  - **Concurrent seal + delete rejected** — two clients call
    `SealChunk` and `DeleteChunk` on the same chunk simultaneously;
    the state machine + KV CAS ensures only one transition succeeds;
    the other gets a `StateConflict` error.

**Solution**:

**One-line summary**: implement the `ChunkdbService` gRPC handlers
(`AllocateChunk`, `AppendChunk`, `SealChunk`, `DeleteChunk`,
`QueryChunk`, `ListChunks`) with a state machine (`Init → Active →
Sealed → Deleted`) that validates transitions, orchestrates the
`ChunkAllocator` (R87) + `ChunkStore` (R88), and frees disk blocks on
delete.

1. **Lifecycle handlers** —
   `app/crow-chunkdb/src/lifecycle.rs` (new module, replaces the R85
   stub `server.rs` handlers):
   - `allocate_chunk(req) -> Chunk` — generate chunk ID (R85
     `crow-common`), fetch topology snapshot (R86), calculate strip
     layout, call `ChunkAllocator` (R87) for parallel allocation +
     rollback, build `Chunk` proto (`state=Active`), persist via
     `ChunkStore.put_if_absent` (R88), return chunk. Design §8.
   - `append_chunk(req) -> Chunk` — validate `state==Active`, allocate
     new strips via `ChunkAllocator`, append to strip list, persist
     updated `Chunk`.
   - `seal_chunk(req) -> Chunk` — validate `state==Active`, set
     `state=Sealed` + `sealed_length` + `sealed_ts_ms`, persist.
   - `delete_chunk(req) -> Chunk` — validate `state in {Active,
     Sealed}`, set `state=Deleted`, free all `Segment`s via diskdb
     `FreeBlocks` (parallel, best-effort — log failures for orphan
     scanner), persist deleted state. Design §9.
   - `query_chunk(req) -> Chunk` — route + read via `ChunkStore`.
   - `list_chunks(req) -> Vec<Chunk>` — scan via `ChunkStore`.

2. **State machine + validation** —
   `app/crow-chunkdb/src/lifecycle/state.rs` (new):
   - `ChunkState` transitions: `Init → Active` (after allocation +
     persist), `Active → Sealed` (via `SealChunk`), `Active → Deleted`
     (via `DeleteChunk`), `Sealed → Deleted` (via `DeleteChunk`).
     Invalid transitions (`Sealed → Active`, `Deleted → *`) return
     `InvalidStateTransition`. Design §9.
   - Concurrency: KV CAS (`put_if_absent` for create, compare-and-swap
     on `state` for seal/delete) prevents conflicting transitions.
     Last-writer-wins with validation; the loser gets `StateConflict`.

3. **gRPC service wiring** —
   `app/crow-chunkdb/src/server.rs` (replace R85 stubs):
   - `ChunkdbService` impl delegates each RPC to the corresponding
     `lifecycle.rs` handler; maps `ChunkdbError` to gRPC status codes
     (`InvalidStateTransition` → `FailedPrecondition`,
     `ChunkNotFound` → `NotFound`, `ChunkAlreadyExists` →
     `AlreadyExists`, `StateConflict` → `Aborted`).

**Flow diagram**:

```
  AllocateChunk request
       │
       ▼
  generate chunk ID (R85 crow-common)
       │
       ▼
  TopologyCache.snapshot() (R86)
       │
       ▼
  ChunkAllocator (R87) ── parallel diskdb AllocateBlocks + rollback
       │
       ▼
  build Chunk proto (state = Active)
       │
       ▼
  ChunkStore.put_if_absent (R88) ── route + KV persist
       │
       ▼
  return Chunk

  SealChunk / DeleteChunk request
       │
       ▼
  ChunkStore.get_chunk (R88) ── read current state
       │
       ▼
  state machine validation (item 2)
       │  Active → Sealed (seal)    Active|Sealed → Deleted (delete)
       ▼
  update Chunk proto (state, sealed_length, sealed_ts_ms)
       │
       ├── DeleteChunk: parallel diskdb FreeBlocks (best-effort)
       │
       ▼
  ChunkStore.put (R88) ── KV CAS on state
       │
       ├── CAS conflict → StateConflict error
       ▼
  return Chunk
```

- **Edge cases at a glance**:
  - `AllocateChunk` with a chunk ID that already exists (collision or
    replay) → `put_if_absent` returns `ChunkAlreadyExists`; the caller
    regenerates an ID or treats it as idempotent if the existing chunk
    matches.
  - `AppendChunk` on a `Sealed` or `Deleted` chunk →
    `InvalidStateTransition`; no strips allocated.
  - `SealChunk` on a `Deleted` chunk → `InvalidStateTransition`; no
    state change.
  - `DeleteChunk` on an already-`Deleted` chunk → idempotent return
    (return the existing `Deleted` chunk) or `InvalidStateTransition`
    (design decision — see Open Questions).
  - `DeleteChunk` where some `FreeBlocks` calls fail → the chunk state
    is still `Deleted` (persisted); the failed frees are logged with
    `owner_chunk` for the orphan scanner; the delete response
    succeeds (best-effort free, design §8 "Block Freeing").
  - Concurrent `SealChunk` + `DeleteChunk` → KV CAS: one wins, the
    other gets `StateConflict`; no torn state.
  - `QueryChunk` on a non-existent chunk ID → `ChunkNotFound`.
  - `QueryChunk` on a `Deleted` chunk → returns the `Deleted` chunk
    (deleted chunks are still readable until KV GC; design does not
    specify immediate removal).
  - `ListChunks` with `max_keys=0` → returns empty page with
    `next_token` = `start_token` (no error).
  - `AllocateChunk` where the `ChunkAllocator` rollback itself fails
    (orphan segments) → the allocate returns an error; orphan
    segments are logged; the chunk is not persisted (no `owner_chunk`
    record points to the orphans, but diskdb `BusyBlockValue` does —
    the orphan scanner reconciles).

**Dependencies**:

- **R85** (foundation) — chunkdb server crate + `crow-common` chunk ID
  + EC module.
- **R86** (topology) — `TopologyCache.snapshot()` is the placement
  input.
- **R87** (placement + allocation) — `ChunkAllocator` does the
  parallel diskdb allocation + rollback.
- **R88** (storage + routing) — `ChunkStore` persists/reads chunk
  metadata; the router determines the KV group.
- **R72** (diskdb core) — `FreeBlocks` RPC for delete; `DiskdbClient`
  (R74) for the free calls.
- **R90** (client) depends on R89 — the client wraps the gRPC RPCs
  that R89 implements.
- **R91** (E2E) depends on R89 — E2E tests exercise the full lifecycle.

**Acceptance**:

**Allocate**:
- `AllocateChunk(Mirror, strip_count=3, Repo)` on a 3-rack cluster →
  returns a `Chunk` with `state=Active`, 3 strips each with 3 mirror
  `Segment`s on distinct racks; `QueryChunk` returns the same chunk.
  Integration test (with KV + diskdb).
- `AllocateChunk(EC, data_num=8, code_num=4, Repo)` → returns a
  `Chunk` with `state=Active`, strips with `EC_STATE_NO_PARITY` and
  12 `Segment`s across ≥3 racks. Integration test.
- `AllocateChunk` with a duplicate chunk ID → `ChunkAlreadyExists`
  error; no new chunk created. Integration test.

**Append**:
- `AppendChunk` on an `Active` chunk with `strip_count=2` → chunk's
  strip list grows by 2; `QueryChunk` reflects the new strips.
  Integration test.
- `AppendChunk` on a `Sealed` chunk → `InvalidStateTransition`; no
  strips allocated. Integration test.
- `AppendChunk` on a `Deleted` chunk → `InvalidStateTransition`.
  Integration test.

**Seal**:
- `SealChunk` on an `Active` chunk with `seal_length=1024` →
  `state=Sealed`, `sealed_length=1024`, `sealed_ts_ms` set;
  `QueryChunk` reflects the sealed state. Integration test.
- `SealChunk` on a `Deleted` chunk → `InvalidStateTransition`.
  Integration test.
- `AppendChunk` after `SealChunk` → `InvalidStateTransition` (sealed
  chunks are read-only). Integration test.

**Delete**:
- `DeleteChunk` on an `Active` chunk → `state=Deleted`; all `Segment`s
  freed via diskdb `FreeBlocks` (verify via `QueryCapacityStats` —
  busy count drops). Integration test.
- `DeleteChunk` on a `Sealed` chunk → `state=Deleted`; segments freed.
  Integration test.
- `DeleteChunk` where one `FreeBlocks` call fails → chunk state still
  `Deleted` (persisted); the failed free is logged; delete response
  succeeds. Integration test.
- `QueryChunk` after `DeleteChunk` → returns the `Deleted` chunk.
  Integration test.

**Concurrency**:
- Concurrent `SealChunk` + `DeleteChunk` on the same chunk → one
  succeeds, the other gets `StateConflict`; no torn state (verify
  via `QueryChunk` — state is either `Sealed` or `Deleted`, not
  both). Integration test.

**Query + List**:
- `QueryChunk` on a non-existent ID → `ChunkNotFound`. Integration
  test.
- `ListChunks(start_token, max_keys=10)` on a KV group with 25 chunks
  → returns 10 chunks in ID order; `next_token` set; second call
  returns the next 10; third call returns the last 5 with
  `next_token` empty. Integration test.
- `ListChunks(max_keys=0)` → empty page, `next_token` = `start_token`.
  Unit test.

**Lint + test commands**:
- `pixi run cargo fmt --all -- --check` passes.
- `pixi run cargo clippy --all-targets -- -D warnings` passes.
- `pixi run test-chunkdb` (lifecycle integration tests with KV +
  diskdb pass).

**Open Questions**:

- **`DeleteChunk` on an already-`Deleted` chunk — idempotent or
  error?** Options: (a) idempotent — return the existing `Deleted`
  chunk (simpler for callers that retry); (b) error —
  `InvalidStateTransition` (stricter, surfaces caller bugs). aioss
  is idempotent. Recommendation: (a) — idempotent, matching aioss
  and simplifying retry. Design decision.
