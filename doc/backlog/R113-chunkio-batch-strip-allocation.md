<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R113: chunkio — Batch strip allocation + deferred chunkdb confirm

## Problem

**Current behavior + impact:** The large-object write path
(`crow-chunk-client`, R94) allocates strips one at a time. Each strip
triggers an `append_chunk` RPC to chunkdb with `strip_count=1`, which
internally calls diskdb to allocate `data_num + code_num` blocks,
persists the updated chunk metadata to KV, and returns the full
`Chunk`. For a 1 TB object with 4+1 EC and 1 MB blocks, that is 250K
strips = 250K `append_chunk` RPC round-trips, each carrying a full
chunkdb → diskdb → KV-persist → return cycle.

The strip prefetch task (inside `ChunkWriter` after the chunk-layer
refactor) hides allocation latency by pre-appending `prealloc_depth`
strips ahead of the write cursor. But each pre-append is still one
full RPC. At high throughput (1 GB/s), the prefetch task must sustain
250 `append_chunk` RPCs/second — one paxos round-trip each through
chunkdb's KV persistence. This is the allocation-rate bottleneck for
large objects.

**Design pointers:**
- `doc/design/chunkio/design-crow-chunkio.md` §3 (Write Flow —
  prealloc task), §2 (Key Design Decisions — bounded preallocation).
- `doc/design/chunkdb/design-crow-chunkdb.md` §8 (Allocation Flow —
  parallel strip allocation, rollback on failure), §9 (Chunk
  Lifecycle — state transitions), §3.6 (Stateless with KV
  persistence — all state changes persisted before acknowledge).
- `doc/design/diskdb/design-crow-diskio.md` — `BusyBlockValue`
  `commit_state` field (`TENTATIVE` → `COMMITTED`), two-phase
  allocate (sync bitmap claim + async KV persist).
- `lib/crow-protocol/src/proto/chunkdb_op.proto` —
  `AppendChunkRequest` already has `strip_count` field (currently
  always sent as 1).
- `lib/crow-protocol/src/proto/diskdb_type.proto` —
  `BusyBlockValue.commit_state`, `CommitState` enum.

**Use scenarios:**

- **Large object write, high throughput.** A 1 TB object upload at
  1 GB/s. The write path needs 250K strips allocated. With
  per-strip `append_chunk`, the prefetch task must sustain 250
  RPCs/s through chunkdb's KV persist path. Expected: batch
  allocation reduces RPC count by 10-50×, sustaining throughput
  without saturating chunkdb's paxos round-trip capacity.
- **Many concurrent large writes.** 10 concurrent 100 GB uploads.
  Each writer's prefetch task issues `append_chunk` RPCs
  independently. 10 writers × 25 strips/s = 250 RPCs/s aggregate,
  contending for the same chunkdb instance's KV persist path.
  Expected: batch allocation reduces aggregate RPC load, allowing
  more concurrent writers without chunkdb becoming the bottleneck.
- **Crash during batch allocation.** A writer has batch-allocated 10
  strips' worth of disk blocks from diskdb (all `TENTATIVE`), written
  data to 3 of them, then crashes before chunkdb confirms the batch.
  Expected: the 3 written strips' blocks are orphaned (busy in
  diskdb, no chunk metadata). A reaper/GC mechanism reclaims
  `TENTATIVE` blocks that are never confirmed. No data corruption —
  the chunk was never sealed, so no reader can reference it.
- **Crash after batch confirm, before seal.** A writer has
  batch-confirmed 10 strips to chunkdb (all `COMMITTED`), written
  data to 5, then crashes before `seal_chunk`. Expected: the chunk
  is `Active` with 10 strips, 5 written. A reaper/GC mechanism
  detects `Active` chunks with no live writer and deletes them
  (freeing blocks via diskdb). Same as current crash behavior —
  `seal_chunk` is the durability boundary.

## Solution

**No clear solution yet — deferred to design.** Two candidate
approaches, with the chunk allocate confirm flow as the key design
tension. The design draft must resolve which approach (or hybrid) is
viable.

**One-line summary:** Batch strip allocation to reduce `append_chunk`
RPC count, with safe handling of the `TENTATIVE` → `COMMITTED`
confirm flow for crash safety.

### Candidate approaches

1. **Batch `append_chunk` (simple, chunkdb-orchestrated).** Send
   `append_chunk(strip_count=N)` instead of N calls with
   `strip_count=1`. chunkdb allocates N strips' blocks in parallel
   (it already does parallel allocation per §8), persists the chunk
   metadata once (all N strips atomically), returns the full
   `Chunk`. The client's strip prefetch task requests a batch of
   `prealloc_depth` strips in one RPC instead of one-per-strip.
   - **Pro:** Minimal change — `AppendChunkRequest.strip_count`
     already exists; chunkdb's allocation flow already supports
     parallel strips. No new RPCs, no client-side placement logic.
   - **Con:** The client must wait for all N strips to allocate
     before writing any of them (the RPC is atomic). Latency for
     the first strip in a batch increases from one-strip-allocate
     to N-strip-allocate. The prefetch task must balance batch size
     vs. first-strip latency.
   - **Confirm flow:** Safe by construction — chunkdb persists all
     N strips atomically before returning. No `TENTATIVE` blocks
     escape to the client. Same crash safety as current per-strip
     flow.

2. **Direct diskdb allocation + deferred chunkdb confirm
   (aggressive, client-orchestrated).** The client calls diskdb
   directly to batch-allocate blocks (using the EC scheme to know
   `data_num + code_num` per strip), locally assembles strips into
   the `ChunkInfo`, starts writing immediately, and batch-confirms
   to chunkdb later (one `append_chunk` or a new `confirm_strips`
   RPC). This overlaps allocation + write + confirm.
   - **Pro:** Lowest latency for the first strip — the client
     writes as soon as diskdb allocates the first block, without
     waiting for chunkdb's KV persist. Maximum overlap of
     allocation, write, and confirm.
   - **Con:** The client must know placement policy (rack/node-aware
     disk-group selection) — currently chunkdb's job per §7. This
     either duplicates placement logic in the client or requires a
     new "placement hint" RPC from chunkdb. The client calls diskdb
     directly — a new dependency (`crow-diskio-client` → diskdb
     allocation RPCs, not just block IO). Crash recovery for
     `TENTATIVE` blocks with written data is the hard part.
   - **Confirm flow:** Blocks are `TENTATIVE` after diskdb
     allocation. The client writes data to `TENTATIVE` blocks. If
     the client crashes before chunkdb confirms, the written blocks
     are orphaned — busy in diskdb, no chunk metadata. A reaper
     must reclaim `TENTATIVE` blocks that are never confirmed
     (timeout-based or explicit abort). The `TENTATIVE` →
     `COMMITTED` transition is the confirm: chunkdb persists the
     chunk metadata (with the client-assembled strips) to KV, then
     transitions the blocks to `COMMITTED`. This requires a new
     chunkdb RPC that takes pre-allocated segments (not
     allocating new ones) and persists them.

### Work items (approach-dependent, refined in design)

1. **Batch `append_chunk` in `ChunkWriter` strip prefetch** —
   `lib/crow-chunk-client/src/chunk/chunk_writer.rs`. The internal
   strip prefetch task requests `prealloc_depth` strips in one
   `append_chunk(strip_count=N)` call instead of N calls. Config
   knob for batch size (default = `prealloc_depth`). Required for
   approach 1; optional fallback for approach 2.

2. **chunkdb batch allocation path** —
   `app/crow-chunkdb/src/lifecycle.rs`. Verify that
   `append_chunk(strip_count=N)` allocates N strips in parallel
   (§8 already supports this) and persists once. Optimize if the
   current implementation serializes per-strip. Required for
   approach 1.

3. **Client-side placement (approach 2 only)** —
   `lib/crow-chunk-client/src/chunk/`. New module that queries
   chunkdb for placement hints (which disk-groups to use) and calls
   diskdb directly to allocate blocks. Duplicates or extracts
   chunkdb's placement logic (§7). Blocked on a placement-hint RPC
   in chunkdb.

4. **Deferred confirm RPC (approach 2 only)** —
   `lib/crow-protocol/src/proto/chunkdb_op.proto`,
   `app/crow-chunkdb/src/lifecycle.rs`. New RPC
   (`ConfirmStripsRequest`) that takes a `ChunkId` + pre-allocated
   segments (from direct diskdb allocation) and persists them to
   KV, transitioning blocks from `TENTATIVE` to `COMMITTED`. Does
   not allocate new blocks — just confirms existing ones.

5. **TENTATIVE block reaper (approach 2 only)** —
   `app/crow-diskdb/src/`. Background task that reclaims
   `TENTATIVE` blocks older than a timeout (no chunkdb confirm
   received). Frees the blocks back to the free pool. Crash safety
   for the aggressive approach.

### Flow diagram (approach 1 — batch append_chunk)

```
                    ┌──────────────────────┐
                    │  ChunkWriter         │
                    │  strip prefetch task │
                    └──────────┬───────────┘
                               │ append_chunk(strip_count=N)
                               ▼
                    ┌──────────────────────┐
                    │  chunkdb             │
                    │  allocate N strips   │
                    │  in parallel         │
                    │  ├─ diskdb allocate  │  (N × data_num+code_num blocks)
                    │  ├─ KV persist once  │  (all N strips atomic)
                    │  └─ return Chunk     │  (with N new strips)
                    └──────────┬───────────┘
                               │ Chunk (N strips)
                               ▼
                    ┌──────────────────────┐
                    │  ChunkWriter         │
                    │  write strips 0..N   │  (all COMMITTED, safe to write)
                    └──────────────────────┘
```

### Flow diagram (approach 2 — direct diskdb + deferred confirm)

```
  ┌────────────┐     allocate blocks      ┌──────────┐
  │ ChunkWriter│──── (data_num+code_num) ─►│  diskdb  │
  │  prefetch  │◄─── segments (TENTATIVE)─│  blocks  │
  └─────┬──────┘                           └──────────┘
        │ assemble strips locally
        │ start writing immediately (TENTATIVE blocks)
        ▼
  ┌────────────┐     confirm strips       ┌──────────┐
  │ ChunkWriter│──── (ChunkId + segs) ───►│ chunkdb  │
  │  writer    │                          │  persist │
  │            │◄─── confirmed (COMMITTED)│  to KV   │
  └────────────┘                          └──────────┘
        │
        │ crash before confirm?
        ▼
  ┌──────────────────────────┐
  │ TENTATIVE block reaper   │  (diskdb background task)
  │ timeout → free TENTATIVE │
  │ blocks (reclaim orphans) │
  └──────────────────────────┘
```

### Edge cases at a glance

- **Batch allocation partial failure** (approach 1) — one strip's
  diskdb allocation fails → chunkdb rolls back all N strips (frees
  allocated blocks), returns error. Client retries with smaller
  batch or per-strip. Same rollback semantics as current §8.
- **Crash with TENTATIVE blocks** (approach 2) — client crashes
  after diskdb allocate, before chunkdb confirm → blocks are
  `TENTATIVE` in diskdb, no chunk metadata. Reaper frees them after
  timeout. No data corruption (chunk never sealed).
- **Crash after confirm, before seal** (both) — chunk is `Active`
  with confirmed strips, some written. Same as current: reaper
  detects orphaned `Active` chunks and deletes them.
- **diskdb allocate succeeds, chunkdb confirm fails** (approach 2)
  — blocks are `TENTATIVE`, client gets confirm error. Client
  retries confirm (idempotent — same segments) or aborts (frees
  blocks via diskdb). If client crashes, reaper handles it.
- **Batch size tuning** — too large → first-strip latency high,
  memory for pre-allocated segments. Too small → RPC count stays
  high. Default = `prealloc_depth` (2); configurable.

## Dependencies

- **Depends on R94** (large-object writer, landed) — the write path
  being optimized. The chunk-layer refactor
  (`doc/working/design-chunk-layer-refactor.md`) moves strip
  prefetch into `ChunkWriter`; this requirement optimizes that
  prefetch.
- **Depends on chunkdb `append_chunk(strip_count=N)` support**
  (approach 1) — the proto field exists; the server implementation
  must be verified to handle N > 1 correctly (parallel allocation +
  single persist).
- **Depends on diskdb `commit_state` two-phase commit** (approach 2)
  — `BusyBlockValue.commit_state: TENTATIVE → COMMITTED` already
  exists in the proto (`diskdb_type.proto:112`). The
  `TENTATIVE` → `COMMITTED` transition path must be implemented in
  diskdb (may be partially landed — verify in design).
- **Blocked by: chunk-layer refactor** — this optimization builds on
  the `ChunkWriter`-internal strip prefetch. If the refactor is not
  landed, the optimization applies to the current
  `ChunkPrefetch`-stream flow instead (less clean).
- **R110 (error handling) interaction** — batch allocation changes
  the retry granularity. A partial batch failure (approach 1)
  retries the whole batch; approach 2 can retry per-strip. R110's
  single-block replacement is compatible with both.

## Acceptance

**Batch append_chunk (approach 1):**

- `append_chunk(strip_count=N)` with N=10 → chunkdb allocates 10
  strips in parallel, persists chunk metadata once, returns `Chunk`
  with 10 new strips. Verify `Chunk.strips.len()` increased by 10.
  Integration test.
- `append_chunk(strip_count=N)` where one strip's diskdb allocation
  fails → chunkdb rolls back all 10 strips (frees allocated blocks
  via diskdb), returns error. Verify no `BusyBlockKey` records
  remain for the chunk. Integration test.
- `ChunkWriter` strip prefetch with batch size 10 → write a 40 MB
  object (10 strips, 4+1 EC, 1 MB blocks). Verify 1
  `append_chunk(strip_count=10)` call, not 10
  `append_chunk(strip_count=1)` calls. Verify data reconstructs
  correctly. E2E test.
- Batch size config knob `strip_batch_size` (default =
  `prealloc_depth`) → set to 1, verify per-strip behavior
  unchanged. Set to 50, verify 50 strips per `append_chunk`. Unit
  test.

**Direct diskdb + deferred confirm (approach 2, if pursued):**

- Client calls diskdb directly to allocate `data_num + code_num`
  blocks → receives segments with `commit_state = TENTATIVE`.
  Verify blocks are busy in diskdb, no chunk metadata in chunkdb.
  Integration test.
- Client writes data to TENTATIVE blocks → verify data is on disk
  (read back via diskio). Verify blocks remain TENTATIVE. E2E test.
- Client calls `confirm_strips` RPC → chunkdb persists chunk
  metadata, transitions blocks to COMMITTED. Verify
  `BusyBlockValue.commit_state = COMMITTED` for all blocks. Verify
  `query_chunk` returns the chunk with all strips. Integration test.
- Client crashes after diskdb allocate, before confirm → TENTATIVE
  block reaper frees blocks after timeout. Verify no `BusyBlockKey`
  records remain. Verify no chunk metadata in chunkdb. E2E test
  (kill writer process, wait for reaper, verify cleanup).
- `confirm_strips` with already-confirmed segments → idempotent,
  returns success without re-persisting. Unit test.
- `confirm_strips` with unknown segments (not in diskdb) → error,
  no chunk metadata persisted. Unit test.

**Both approaches:**

- Large object write (1 GB, 4+1 EC, 1 MB blocks, batch size 10) →
  verify `append_chunk` RPC count reduced by ~10× vs. per-strip.
  Verify write throughput ≥ per-strip baseline (no regression).
  Verify all `Location`s correct, data reconstructs. E2E test.
- `pixi run cargo test -p crow-chunk-client --all-targets` — all
  existing tests pass (no regression from batch optimization).
- `pixi run cargo test -p crow-diskdb --all-targets` — diskdb
  tests pass (approach 2: TENTATIVE/COMMITTED transition, reaper).
- `pixi run cargo fmt --all -- --check` +
  `pixi run cargo clippy --all-targets -- -D warnings`.

## Open Questions

- **Approach 1 vs 2 vs hybrid.** Approach 1 (batch `append_chunk`)
  is simple and safe but doesn't overlap allocation with write.
  Approach 2 (direct diskdb + deferred confirm) maximizes overlap
  but requires client-side placement, a new confirm RPC, and a
  TENTATIVE block reaper. A hybrid (batch `append_chunk` for the
  common path, direct diskdb for the first strip to reduce
  first-byte latency) is possible but adds complexity. The design
  draft must evaluate the latency/throughput tradeoff with
  benchmarks and recommend one.
- **TENTATIVE block reaper design** (approach 2). Timeout-based
  (simple, but delays reclamation) vs. explicit abort (client
  sends a "free these TENTATIVE blocks" RPC on error). The
  `commit_state` field exists in the proto but the reaper may not
  be implemented. Verify in design whether diskdb already has a
  TENTATIVE cleanup mechanism or if it must be built.
- **Placement logic location** (approach 2). If the client
  allocates from diskdb directly, it needs rack/node-aware
  placement (§7). Options: (a) duplicate placement in the client
  (drift risk), (b) new "placement hint" RPC from chunkdb (extra
  RPC, but chunkdb stays the placement authority), (c) client
  requests a batch of placement hints upfront, then allocates from
  diskdb using them. The design draft must resolve this.
- **Batch size vs. first-strip latency** (approach 1). A batch of
  N strips means the first strip waits for all N to allocate. For
  large N, this increases time-to-first-byte. The prefetch task
  could use a smaller batch for the first allocation (fast start)
  and larger batches for subsequent (throughput). Config knob
  vs. adaptive sizing — design decision.
