<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R112: chunkdb / diskdb / diskio — Small-Write IO Error Handling (Multi-Service Cooperation)

**Problem**

R106 (small object shared chunk writer) defines the small-write data
path: small objects (< EC strip data capacity, e.g. < 8 MB) are
packed into shared 256 MB chunks and written to **3 mirror strips
first** (mirror-first strategy), then R93 converts mirror strips to
EC in the background. The writer aggregates multiple small objects
into batch writes (patch writes to a shared chunk buffer) for
throughput, and dynamically scales writer pipelines with queue depth.

The small-write path assumes diskio succeeds. When a disk I/O error
occurs mid-write, the current design has no error handler: R106 is
not yet implemented, and R110 (large-write IO error handling) covers
only the large-object writer (`large_object.rs`), not the shared-
chunk writer. The small-write path has a different shape from the
large-write path, so R110's single-block replacement does not
transfer directly:

- **Batch writes** — a single diskio write carries a batch of N
  small objects packed into a mirror strip. If the write fails, all
  N objects in the batch are affected, not one. The writer must
  track per-object status within a batch and retry only the affected
  objects (or the whole batch, depending on where the failure
  occurred).
- **Mirror-first strategy** — small objects are written to 3 mirror
  replicas, not EC strips. A single replica write failure is
  tolerated (2 of 3 replicas is still durable), but the writer must
  re-allocate the failed replica on a healthy disk and update the
  strip metadata so the strip is not left with 2 replicas
  permanently (reduced fault tolerance).
- **Shared chunk rotation** — a shared chunk rotates when full
  (256 MB). A write failure mid-rotation must not corrupt the
  already-sealed portion of the chunk or lose the in-flight batch.
- **Background mirror→EC conversion (R93)** — the conversion runs
  after the write returns. A conversion failure (EC encode error or
  parity disk write failure) leaves the strip as mirror (still
  durable, but no EC savings). This is R93's concern, but R112 must
  define the boundary: which failures R112 handles inline (write
  path) vs. which R93 handles (conversion path).

Error handling on the small-write path spans **three services** that
must cooperate (same three as R110, different integration points):
- **diskio** (R105) — detects the I/O error (write/fsync failure on
  a mirror strip) and returns it to the writer.
- **diskdb** — allocates replacement mirror blocks on healthy disks;
  must exclude the failed disk from future allocations (negative
  list, shared with R110/R111).
- **chunkdb** — updates strip metadata (`update_chunk_strip`) to
  point at the replacement mirror block; tracks strips with reduced
  replica count.

There is no mechanism to:
- Re-allocate a single failed mirror replica within a batch write
  (instead of re-writing the whole batch) — requires per-object
  status tracking within the batch.
- Update the shared chunk's strip metadata to point at the
  replacement replica (via chunkdb `update_chunk_strip`).
- Reuse R110's negative list to avoid placing retries on the same
  bad disk across batch writes and pipeline scaling.
- Handle the boundary with R93's mirror→EC conversion: a write-path
  failure (mirror write) is R112; a conversion-path failure (EC
  encode / parity write) is R93. R112 must hand off cleanly to R93.
- Escalate from inline retry to background recovery (R83) when the
  disk is persistently failing — same escalation as R110.

R110 (large-write) and R111 (read-path) define the negative list,
single-block replacement, and escalation machinery. R112 is the
**small-write** error handler — it reuses R110's negative list and
escalation, but adds batch-aware error handling and mirror-replica
replacement specific to the small-object writer. Without R112, every
transient disk error on a small-object batch write either aborts the
whole batch (losing aggregation throughput) or silently leaves
strips with reduced replica count (no repair).

**Current behavior + impact**: No small-object writer exists (R106
not implemented). When R106 ships without R112, a mirror write
failure in a batch has no handler — the writer would either abort
the batch (wasting the successfully packed objects) or silently
drop the failed replica (leaving the strip with 2 of 3 replicas,
reduced fault tolerance, no repair). There is no negative list
reuse for the small-write path: retries may hit the same bad disk
across batches.

**Design pointers**: chunkdb root design §8 (Allocation Flow —
`append_chunk` for adding strips to an Active shared chunk), §10.6
(`update_chunk_strip` RPC — replace a strip's segment metadata),
§5.5 (Chunk types — "Shared" for small objects), §5.2 (Strip —
mirror vs EC, `replica_count`). diskdb design §8 (Disk Status
Management — `HwStatus` transitions, `Suspect`/`Bad` states,
allocation requires `Up`). R106 (small object shared chunk writer —
the write path being hardened; `lib/crow-chunk-client/src/writer/`,
shared chunk buffer, pipeline worker tasks). R93 (mirror→EC
conversion — the background conversion path; boundary with R112).
R110 (large-write IO error handling — defines the negative list
`lib/crow-chunk-client/src/negative_list.rs` and single-block
replacement that R112 reuses). R111 (read IO error handling —
mirror replica fallback + rebuild for the read path, same strips
R112 writes). R105 (diskio — `DiskIoClient::write` / `fsync` error
returns). R83 (complete recovery flow — escalation path when R112's
inline retries are exhausted).

**Use scenarios**:

- **Single mirror replica write failure, replaced mid-batch**: A
  batch of 50 small objects (16 KB each) is being written to a
  shared chunk's mirror strip (3 replicas). The diskio write to
  replica 2 fails (disk I/O error). The writer does NOT discard the
  whole batch — replicas 1 and 3 succeeded (2 of 3, still durable).
  The writer re-allocates replica 2 on a different disk (via diskdb
  `AllocateBlocks` with the negative list filter), retries the
  write of replica 2's block to the new disk, and updates the
  strip's segment metadata via `update_chunk_strip` to point at the
  new block. The batch completes. Expected: no data loss, the strip
  is complete with 3 replicas (1 replaced), the batch write does
  not stall beyond the retry.

- **Batch write failure, per-object retry**: A batch of 50 small
  objects is being written. The diskio write fails partway (e.g.
  the disk fills up after 30 objects are written to the mirror
  block). The writer must track which objects in the batch were
  successfully written and which were not, re-allocate a new mirror
  block, and retry only the 20 unwritten objects. Expected: no
  object is lost or double-written; the 30 successful objects keep
  their `Location`; the 20 retried objects get a new `Location` on
  the new block.

- **Disk added to negative list after error**: The disk that
  returned the I/O error is added to the negative list (shared with
  R110/R111 — TTL-based, default 60 s). Subsequent batch writes and
  pipeline scaling skip disks on the negative list. Expected:
  retries land on healthy disks; the same bad disk is not hit
  repeatedly across batches.

- **Persistent mirror write failure, escalation to abort**: The same
  disk keeps failing across multiple batches. After
  `max_replica_retries` (default 3) on a single replica, the writer
  aborts the batch, returns errors to the affected callers, and
  reports the failed disk to diskdb (which will transition it to
  `Suspect` via the sync path, §8), triggering R83 recovery. The
  shared chunk's already-sealed portion is intact. Expected: the
  batch aborts cleanly with no data loss on sealed strips; the bad
  disk is quarantined.

- **Shared chunk rotation mid-write failure**: A shared chunk is
  nearly full (255 MB of 256 MB). A batch write of 2 MB triggers
  chunk rotation (the batch spans the old chunk's tail and a new
  chunk's head). The write to the new chunk's first mirror strip
  fails. The writer must not corrupt the old chunk's sealed portion
  and must retry the batch on a new chunk. Expected: the old chunk
  is sealed intact; the batch is retried on a fresh chunk; no
  object spans a corrupted boundary.

- **Boundary with R93 mirror→EC conversion**: A small object is
  written to 3 mirror replicas (R112's write path succeeds). R93's
  background conversion later tries to convert the mirror strip to
  EC and the EC encode fails (not a disk error). The strip stays as
  mirror (still durable, 3 replicas). This is R93's failure, not
  R112's — R112's job ended when the mirror write succeeded.
  Expected: clear boundary — R112 handles write-path failures
  (mirror write/fsync), R93 handles conversion-path failures (EC
  encode/parity write); no overlap or gap.

- **Fsync failure on a mirror replica**: The `fsync` of one of 3
  mirror replicas fails. The data is written but not durable on
  that disk. The writer retries `fsync` once; if it still fails,
  the replica is treated as failed (re-allocated + re-written +
  `update_chunk_strip`). The negative list gets the disk. Expected:
  no replica is reported durable until `fsync` succeeds; a failed
  `fsync` triggers the same replacement flow as a failed write.

**Solution**

**No clear solution yet — deferred to design.** The high-level
approach reuses R110's negative list and escalation, but the
batch-aware error handling (per-object status within a batch,
partial batch retry) and the mirror-replica replacement specific to
the shared-chunk writer need design work. The boundary with R93's
mirror→EC conversion also needs to be drawn precisely. Key open
questions below.

**One-line summary**: In-line small-write error handler that reuses
R110's negative list and escalation, adds batch-aware per-object
retry and mirror-replica replacement specific to the shared-chunk
writer, and draws a clear boundary with R93's mirror→EC conversion.

**Numbered work items**:

1. **Negative list reuse** (`lib/crow-chunk-client/src/negative_list.rs`)
   — R112 reuses R110's negative list (TTL-based disk exclusion)
   verbatim; no new module. The small-object writer consults it
   before allocating replacement mirror blocks and before scaling
   out new pipelines (a new pipeline should not land on a
   negative-listed disk).
   **Services**: diskdb (allocation exclusion), crow-chunk-client
   (list consultation — shared with R110/R111).

2. **Mirror-replica replacement on write**
   (`lib/crow-chunk-client/src/writer/` — shared chunk writer) —
   when a diskio `write` or `fsync` fails for one mirror replica in
   a strip, the writer re-allocates that replica on a healthy disk
   (via diskdb `AllocateBlocks` with the negative list filter),
   retries the write via diskio, and calls chunkdb
   `update_chunk_strip` to replace the failed segment. The other
   replicas are untouched. Bounded by `max_replica_retries`
   (default 3) per replica. Reuses R110's single-block replacement
   shape, applied to mirror strips instead of EC strips.
   **Services**: diskio (error detection + retry write), diskdb
   (new block allocation), chunkdb (strip metadata update).

3. **Batch-aware per-object retry**
   (`lib/crow-chunk-client/src/writer/` — batch tracking) — when a
   batch write fails partway (e.g. disk fills up mid-batch), the
   writer tracks which objects in the batch were successfully
   written and which were not, re-allocates a new mirror block, and
   retries only the unwritten objects. Requires per-object status
   tracking within the batch (new — R106's batch writer does not
   track per-object write status today). The successful objects
   keep their `Location`; the retried objects get a new `Location`.
   **Services**: diskio (partial write detection), diskdb (new
   block for retried objects), chunkdb (strip metadata for the new
   block).

4. **Shared chunk rotation safety**
   (`lib/crow-chunk-client/src/writer/` — chunk rotation) — when a
   batch write triggers chunk rotation (batch spans the old chunk's
   tail and a new chunk's head) and the new chunk's first write
   fails, the writer must not corrupt the old chunk's sealed
   portion and must retry the batch on a fresh chunk. No object may
   span a corrupted boundary.
   **Services**: chunkdb (chunk sealing integrity), diskio (new
   chunk write failure detection).

5. **Boundary with R93 mirror→EC conversion** — R112 handles
   write-path failures (mirror write/fsync on the small-write
   path); R93 handles conversion-path failures (EC encode / parity
   write during background mirror→EC conversion). R112's
   responsibility ends when the mirror write succeeds (3 replicas
   durable). A conversion failure leaves the strip as mirror (still
   durable) — R93 retries or leaves it for a later conversion cycle.
   The boundary must be documented and enforced (no overlap, no
   gap).
   **Services**: R93 (conversion path), R112 (write path).

6. **Escalation to R83 recovery** — when inline retries are
   exhausted (`max_replica_retries` on a single replica), the
   writer aborts the batch, returns errors to the affected callers,
   and reports the failed disk to diskdb (which will transition it
   to `Suspect` via the sync path, §8). R83's recovery flow picks
   up the failed strips and rebuilds from surviving replicas. R112
   reuses R110's escalation reporting.
   **Services**: diskdb (disk status transition `Suspect`),
   chunkdb (hand off failed strips to R83).

**Flow diagram**:

```
  ┌─────────┐     ┌─────────┐     ┌─────────┐
  │ diskio  │     │ diskdb  │     │ chunkdb │
  │(R105)   │     │         │     │         │
  └────┬────┘     └────┬────┘     └────┬────┘
       │               │               │
       │ batch write   │               │
       │ (mirror strip)│               │
       ▼               │               │
  ┌─────────┐          │               │
  │  ok?    │          │               │
  └────┬────┘          │               │
  yes──┤──no           │               │
       │  │            │               │
       │  ▼            │               │
       │ ┌─────────────────────────────┐
       │ │ crow-chunk-client (writer)  │
       │ │  add disk → negative list   │
       │ │  (shared with R110/R111)    │
       │ └────────────┬────────────────┘
       │              │
       │              ▼
       │ ┌──────────────────────────────┐
       │ │ failure scope?               │
       │ └──┬───────────────────────┬───┘
       │    │whole batch            │1 replica
       │    ▼                       ▼
       │ ┌──────────────────┐  ┌──────────────────────┐
       │ │ per-object       │  │ re-alloc replica     │
       │ │ status: which    │  │ on healthy disk      │
       │ │ objects written? │  │ (neg. list filter)   │
       │ └──┬───────────────┘  └──┬───────────────────┘
       │    │                     │
       │    ▼                     ▼
       │ ┌──────────────────┐  ┌──────────────────────┐
       │ │ diskdb alloc new │  │ diskio retry write   │
       │ │ block for        │  │ to new replica       │
       │ │ unwritten objs   │  └──┬───────────────────┘
       │ └──┬───────────────┘     │
       │    │                     │
       │    ▼                     │
       │ ┌──────────────────┐     │
       │ │ retry unwritten  │     │
       │ │ objects to new   │     │
       │ │ block            │     │
       │ └──┬───────────────┘     │
       │    │                     │
       │    ▼                     │
       │ ┌──────────────────────────────┐
       │ │ retries < max_replica_retries?│
       │ └──┬───────────────────────┬───┘
       │  yes│                     │no
       │    │                     │
       │    │                     ▼
       │    │             ┌──────────────────────┐
       │    │             │ diskdb: report disk  │
       │    │             │ → Suspect (§8)       │
       │    │             │ chunkdb: hand off    │
       │    │             │ strips → R83         │
       │    │             │ abort batch, return  │
       │    │             │ errors to callers    │
       │    │             └──────────────────────┘
       ▼    ▼
  ┌──────────────────────────────────┐
  │ chunkdb: update_chunk_strip      │
  │ (replace failed segment → new)   │
  └──────────────────────────────────┘
```

**Edge cases at a glance**:

- Single mirror replica write fails → replace that replica only,
  keep other 2. Strip completes with 3 replicas (1 replaced).
- Batch write fails partway (disk fills mid-batch) → per-object
  retry of unwritten objects on a new block; successful objects
  keep their `Location`.
- All retries on a replica fail → abort batch, return errors to
  affected callers, escalate to R83.
- Shared chunk rotation mid-batch, new chunk write fails → old
  chunk sealed intact, batch retried on a fresh chunk; no object
  spans a corrupted boundary.
- `fsync` fails on a mirror replica → treat replica as not-durable,
  re-allocate + re-write + `update_chunk_strip`.
- Negative list disk comes back (TTL expires) → disk is retried on
  next allocation (shared with R110/R111); if it fails again, TTL
  is extended.
- R93 conversion fails (EC encode error) → not R112's concern; strip
  stays as mirror (durable), R93 retries. Boundary: R112 ends at
  mirror write success.
- Negative list full (all disks in a disk-group excluded) →
  allocation fails; writer aborts batch with
  `IoError::NoHealthyDisk`.
- Writer dropped mid-replacement → `Drop` impl frees the partial
  batch (same as R106); the replacement block is orphaned and freed
  by diskdb GC.

**Dependencies**

This requirement spans three services and reuses R110's machinery —
the dependency list is organized by service.

- **Depends on**:
  - **diskio** (R105) — `DiskIoClient::write` / `fsync` error
    returns are the trigger for R112's error handler. R112 consumes
    diskio errors; it does not modify diskio itself.
  - **diskdb** (landed) — `AllocateBlocks` RPC for replacement
    mirror blocks, with `exclude_disks` filter (see R110 Open
    Questions — same extension). Disk status transitions
    (`Suspect`) via the sync path (§8) for escalation to R83.
  - **chunkdb** (landed, R85) — `update_chunk_strip` RPC for
    replacing failed segments in mirror strip metadata.
  - **R106** (small object shared chunk writer) — the write path
    being hardened; R112 modifies the shared-chunk writer to add
    mirror-replica replacement and batch-aware retry. R106 must
    land first.
  - **R93** (mirror→EC conversion) — the background conversion
    path; R112 must draw a clean boundary with R93 (write-path vs.
    conversion-path failures). R93 must land first (R106 depends
    on it).
  - **R110** (large-write IO error handling) — defines the negative
    list (`lib/crow-chunk-client/src/negative_list.rs`) and
    escalation reporting that R112 reuses. R110 must land first
    (R112 reuses its machinery).
  - **R83** (complete recovery flow) — the escalation path when
    R112's inline retries are exhausted. R112 can ship before R83
    (it just aborts instead of escalating), but the full error
    recovery story needs both.
- **Depended on by**: none yet. R111 (read IO error handling) reads
  the mirror strips R112 writes and reuses the same negative list.

**Acceptance**

**Negative list reuse**:
- Small-object writer with a diskio mock that fails a mirror write
  → the failed disk is added to the negative list (shared with
  R110); a subsequent allocation in the same writer skips the
  negative-listed disk. Integration test (verify negative list is
  consulted, not re-created).

**Mirror-replica replacement on write**:
- Batch write with a diskio mock that fails replica 2 of a mirror
  strip → writer keeps replicas 1 and 3, allocates a new block for
  replica 2 on a different disk, retries the write, calls
  `update_chunk_strip` to replace the segment. Strip completes with
  3 replicas (1 replaced). Integration test (inject diskio error
  on 1 replica).
- Batch write with a diskio mock that fails the same replica 3
  times → writer exhausts `max_replica_retries`, aborts the batch,
  returns errors to the affected callers. Integration test.
- After a successful mirror-replica replacement, `query_chunk` shows
  the strip with the new segment (different `disk_id`). Integration
  test.

**Batch-aware per-object retry**:
- Batch of 50 objects with a diskio mock that fails partway (30
  objects written, 20 not) → writer tracks per-object status,
  re-allocates a new block, retries only the 20 unwritten objects.
  The 30 successful objects keep their `Location`; the 20 retried
  objects get a new `Location`. No object is lost or double-written.
  Integration test (inject partial write failure).

**Shared chunk rotation safety**:
- Batch write that triggers chunk rotation with a diskio mock that
  fails the new chunk's first write → old chunk sealed intact, batch
  retried on a fresh chunk, no object spans a corrupted boundary.
  Integration test.

**Fsync failure**:
- Batch write with a diskio mock that fails `fsync` on 1 of 3
  mirror replicas → writer retries `fsync` once; if it still fails,
  treats the replica as failed (re-allocate + re-write +
  `update_chunk_strip`). Integration test.

**Boundary with R93**:
- Mirror write succeeds (3 replicas durable), then R93 conversion
  fails (EC encode error) → strip stays as mirror (durable); R112
  does not intervene. Integration test (verify R112's error handler
  is not triggered by R93 conversion failures).

**Escalation to R83**:
- Batch write with persistent failures (all retries exhausted) →
  writer aborts batch, reports the failed disk to diskdb (triggers
  `Suspect` transition). Integration test (verify diskdb receives
  the report; R83 recovery is a separate test in R83's acceptance).

**Writer drop mid-replacement**:
- Drop the writer during a mirror-replica replacement → `Drop` impl
  frees the partial batch; the orphaned replacement block is freed
  by diskdb GC. Integration test (drop mid-write, verify partial
  batch deleted, no orphaned blocks).

**Test commands**: `pixi run cargo test -p crow-chunk-client --test
small_write_error_handling` (unit + integration), `pixi run cargo
test -p crow-chunk-client --test small_write_error_handling_e2e`
(E2E with real servers + fault injection), `pixi run cargo fmt --all
-- --check`, `pixi run cargo clippy --all-targets -- -D warnings`.

**Open Questions**

- **Per-object status tracking in batches**: R106's batch writer
  does not currently track per-object write status (it writes the
  whole batch as one diskio write). Does diskio report a partial
  write count on failure, or does the writer need to re-read the
  block to determine which objects were written? If diskio is
  all-or-nothing per write, partial-batch retry may require
  splitting the batch into smaller writes (trading aggregation for
  finer-grained retry). Needs investigation of diskio's failure
  semantics.

- **Mirror-replica replacement vs. R93 conversion timing**: If R93
  is mid-conversion of a mirror strip to EC and a write-path
  failure occurs on the same strip, who wins? R93 holds the strip
  lock during conversion; R112's `update_chunk_strip` would
  conflict. Likely R112 waits for R93 to finish (or abort), then
  retries. Needs design decision — tied to R93's locking model.

- **Negative list scope for pipeline scaling**: When the writer
  scales out a new pipeline (R106's dynamic scaling), should the
  new pipeline's chunk allocation avoid negative-listed disks? The
  negative list is shared, but pipeline scaling is a coarser
  decision (a pipeline owns a chunk for its lifetime). Likely yes
  — a new pipeline should not land on a disk that is currently
  failing. Needs confirmation.
