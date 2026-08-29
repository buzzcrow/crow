<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R110: chunkdb / diskdb / diskio — Large-Write IO Error Handling (Write Path)

**Problem**

R94 (large-object writer) defines the large-write data path:
`write_stream` writes EC strips via diskio. The write path assumes
diskio succeeds. When a disk I/O error occurs mid-write, the current
design has only a coarse retry-and-abort: R94 retries the whole strip
up to 3 times with a new allocation, then returns `IoError` to the
caller with a partial `Location` array.

Error handling on the large-write path spans **three services** that
must cooperate:
- **diskio** (R105) — detects the I/O error (write/fsync failure) and
  returns it to the writer.
- **diskdb** — allocates replacement blocks on healthy disks; must
  exclude the failed disk from future allocations (negative list).
  Eventually transitions the disk to `Suspect`/`Bad` via its sync
  path (§8).
- **chunkdb** — updates strip metadata (`update_chunk_strip`) to
  point at the replacement block; tracks degraded strips (parity
  missing).

There is no mechanism to:
- Replace a single failed disk block within a strip (instead of
  retrying the whole strip with a new allocation) — requires diskdb
  to allocate one new block + chunkdb to update one segment.
- Update the chunk's strip metadata to point at the replacement
  block (via chunkdb `update_chunk_strip`).
- Temporarily block the broken disk or node from receiving new
  allocations (a negative list) so diskdb doesn't place retries on
  the same bad disk.
- Escalate from inline retry to background recovery (R83) when the
  disk is persistently failing — requires diskdb to transition the
  disk status + chunkdb to hand off the failed strips.

R83 (complete recovery flow) handles **post-failure** rebuild: a disk
goes `Bad`, diskdb's scan lists impacted blocks, chunkdb rebuilds from
surviving replicas/parity. R110 is the **in-line** write error
handler — it fires at the moment a write fails, before the disk is
officially `Bad`. It does fast replacement + retry to keep the write
going; if the error is persistent (retries exhausted), it escalates
to R83's recovery path. Without R110, every transient disk error
aborts the entire write operation, even though the chunk has
redundancy to tolerate the failure.

The **read-path** error handler is a separate requirement, R111
(unified read path — serves both large EC and small mirror objects).
R110 defines the negative list and degraded-strip tracking that R111
and R112 (small-write error handling) reuse.

**Current behavior + impact**: R94's error handling is retry-whole-
strip-or-abort. A single block write failure on a 5-block EC strip
(4+1) discards the 4 successful block writes and re-allocates all 5
blocks. This wastes I/O, increases latency, and under sustained disk
errors causes aborts that could have been recovered with single-block
replacement. There is no negative list: retries may hit the same bad
disk repeatedly.

**Design pointers**: chunkdb root design §8 (Allocation Flow —
`allocate_chunk` / `append_chunk` for new placements), §10.6
(`update_chunk_strip` RPC — replace a strip's segment metadata),
§11 (EC Encoding/Decoding — isa-l decode for reconstructing lost
blocks), §7.2 (EC placement — rack/node-aware placement avoids
co-located failures). diskdb design §8 (Disk Status Management —
`HwStatus` transitions, `Suspect`/`Bad` states, allocation requires
`Up`). R83 (complete recovery flow — post-failure rebuild from
surviving replicas/parity, the escalation path when R110's inline
retries are exhausted). R105 (diskio — `DiskIoClient::write` /
`fsync` error returns). R111 (read IO error handling — reuses R110's
negative list and degraded-strip tracking for the read path).

**Use scenarios**:

- **Single block write failure, replaced mid-write**: A 50 MB object
  is being written (13 EC strips, 4+1). The diskio write to block 2
  of strip 5 fails (disk I/O error). The writer does NOT discard the
  whole strip — it keeps the 3 successful data blocks + the parity
  block, allocates a new block on a different disk (via diskdb
  `AllocateBlocks`), retries the write of block 2 to the new block,
  and updates the strip's segment metadata via `update_chunk_strip`
  to point at the new block. The write continues. Expected: no data
  loss, the strip is complete with 1 replaced block, the write
  pipeline does not stall beyond the retry.

- **Disk added to negative list after error**: The disk that
  returned the I/O error in the scenario above is added to a
  temporary negative list (in-memory, per-writer or per-client).
  Subsequent allocations (for replacement blocks, new strips, or
  new chunks in the same write) skip disks on the negative list.
  The negative list has a TTL (default 60 s) — after which the disk
  is retried (it may have been a transient error). Expected: retries
  land on healthy disks; the same bad disk is not hit repeatedly
  during a single write operation.

- **Persistent write failure, escalation to abort**: The same disk
  keeps failing across multiple strips. After `max_block_retries`
  (default 3) on a single block, and `max_strip_retries` (default 3)
  on a strip, the writer aborts `write_stream` with `IoError`,
  returns the partial `Location` array (sealed chunks), and frees
  the partial chunk. The caller can retry the whole upload or abort.
  The persistently-failing disk stays on the negative list with an
  extended TTL; diskdb's sync path will eventually transition it to
  `Suspect` → `Bad` (§8), triggering R83 recovery. Expected: the
  write aborts cleanly with no data loss on sealed chunks; the bad
  disk is quarantined.

- **Parity task failure**: The background parity task for strip N
  fails (EC encode error or parity disk write error). The main write
  task has already advanced to strip N+1. The parity task retries
  the parity write to a new disk (via the negative list). If the EC
  encode itself fails (not a disk error), the strip is marked
  degraded (data blocks written, parity missing) — the chunk can
  still be sealed, but R83 must rebuild parity before the strip is
  fully redundant. Expected: a strip with missing parity is
  explicitly tracked; the write does not abort for a parity-only
  failure (data is durable); R83 picks up the degraded strip.

- **Fsync failure**: The `fsync` of one of 5 disks in a strip fails.
  The data is written but not durable on that disk. The writer
  retries `fsync` once; if it still fails, the block is treated as
  failed (re-allocated + re-written + `update_chunk_strip`). The
  negative list gets the disk. Expected: no block is reported
  durable until `fsync` succeeds; a failed `fsync` triggers the
  same replacement flow as a failed write.

**Solution**

**No clear solution yet — deferred to design.** The high-level
approach is settled (single-block replacement + negative list +
degraded strip tracking), but the integration points with R94's
pipeline need design work. Key open questions below.

**One-line summary**: In-line large-write error handler that replaces
failed disk blocks (not whole strips), maintains a temporary
negative list to avoid bad disks, tracks degraded strips (parity
missing), and escalates to R83 recovery when retries are exhausted.

**Numbered work items**:

1. **Negative list** (`lib/crowdb-chunk-client/src/negative_list.rs`)
   — an in-memory, TTL-based set of disk IDs (and optionally node
   IDs) that are temporarily excluded from allocation placement.
   The writer consults it before allocating replacement blocks;
   diskdb's `AllocateBlocks` request includes an `exclude_disks`
   filter. Entries expire after a configurable TTL (default 60 s);
   a disk that keeps failing gets an extended TTL. Lives in
   `crowdb-chunk-client` (shared by writer + reader — R111 and R112
   reuse it).
   **Services**: diskdb (allocation exclusion), crowdb-chunk-client
   (list management).

2. **Single-block replacement on write**
   (`lib/crowdb-chunk-client/src/writer/large_object.rs`) — when a
   diskio `write` or `fsync` fails for one block in a strip, the
   writer does NOT discard the whole strip. It allocates a new
   block on a healthy disk (via diskdb `AllocateBlocks` with the
   negative list filter), retries the write via diskio, and calls
   chunkdb `update_chunk_strip` to replace the failed segment in
   the strip's metadata. The other blocks in the strip are
   untouched. Bounded by `max_block_retries` (default 3) per block.
   **Services**: diskio (error detection + retry write), diskdb
   (new block allocation), chunkdb (strip metadata update).

3. **Degraded strip tracking**
   (`lib/crowdb-chunk-client/src/writer/large_object.rs` +
   `lib/crowdb-chunkdb-client/` protocol) — when a parity task fails
   (EC encode error or parity write failure after retries), the
   strip is marked degraded: data blocks are durable but parity is
   missing. The chunk can still be sealed. The degraded state is
   recorded in chunk metadata (new field or strip state) so R83 can
   find and rebuild degraded strips, and so R111's reader can
   tolerate reading degraded strips. A degraded strip has reduced
   fault tolerance (no parity for that strip) — reads still work
   (all data blocks present), but a subsequent data block failure
   in the same strip cannot be recovered.
   **Services**: chunkdb (degraded state in strip metadata),
   diskio (parity write failure detection).

4. **Escalation to R83 recovery** — when inline retries are
   exhausted (`max_block_retries` on a single block, or
   `max_strip_retries` on a strip), the writer aborts the
   operation and reports the failed disk to diskdb (which will
   transition it to `Suspect` via the sync path, §8). R83's
   recovery flow picks up the failed strips and rebuilds from
   surviving replicas/parity. The boundary: R110 handles
   transient/recoverable write errors inline (fast, in-operation);
   R83 handles persistent failures (slow, background, post-failure).
   **Services**: diskdb (disk status transition `Suspect`),
   chunkdb (hand off failed strips to R83).

**Flow diagram**:

```
  ┌─────────┐     ┌─────────┐     ┌─────────┐
  │ diskio  │     │ diskdb  │     │ chunkdb │
  │(R105)   │     │         │     │         │
  └────┬────┘     └────┬────┘     └────┬────┘
       │               │               │
       │ write block   │               │
       ▼               │               │
  ┌─────────┐          │               │
  │  ok?    │          │               │
  └────┬────┘          │               │
  yes──┤──no           │               │
       │  │            │               │
       │  ▼            │               │
       │ ┌─────────────────────────────┐
       │ │ crowdb-chunk-client (writer)  │
       │ │  add disk → negative list   │
       │ │  (TTL 60s, exp. backoff)    │
       │ └────────────┬────────────────┘
       │              │
       │              ▼
       │ ┌──────────────────────────┐
       │ │ diskdb: AllocateBlocks   │
       │ │ (exclude neg. list disks)│
       │ └────────────┬─────────────┘
       │              │
       │              ▼
       │ ┌──────────────────────────┐
       │ │ diskio: retry write      │
       │ │ to new block             │
       │ └────────────┬─────────────┘
       │              │
       │              ▼
       │      ┌────────────┐
       │      │ retries<3? │
       │      └──┬─────┬───┘
       │      yes│     │no
       │         │     │
       │         │     ▼
       │         │ ┌──────────────────────┐
       │         │ │ diskdb: report disk  │
       │         │ │ → Suspect (§8)       │
       │         │ │ chunkdb: hand off    │
       │         │ │ strips → R83         │
       │         │ └──────────────────────┘
       │         │
       │         ▼
       │ ┌──────────────────────────────────┐
       │ │ chunkdb: update_chunk_strip      │
       │ │ (replace failed segment → new)   │
       │ └──────────────────────────────────┘
       ▼
  ┌──────────────────────────────────┐
  │ write continues (next block)     │
  └──────────────────────────────────┘
```

**Edge cases at a glance**:

- Single block write fails → replace that block only, keep other 4.
  Strip completes with 1 replaced block.
- All retries on a block fail → abort write (return partial
  `Location`), escalate to R83.
- Parity task fails (EC encode error) → strip marked degraded (data
  durable, parity missing); chunk sealed; R83 rebuilds parity later.
- Parity disk write fails → retry parity write to new disk (same
  single-block replacement as data blocks).
- `fsync` fails → treat block as not-durable, re-allocate + re-write
  + `update_chunk_strip`.
- Negative list disk comes back (TTL expires) → disk is retried on
  next allocation; if it fails again, TTL is extended (exponential
  backoff).
- Writer dropped mid-replacement → `Drop` impl frees the partial
  chunk (same as R94); the replacement block is orphaned and freed
  by diskdb GC.
- Negative list full (all disks in a disk-group excluded) →
  allocation fails; writer aborts with `IoError::NoHealthyDisk`.

**Dependencies**

This requirement spans three services — the dependency list is
organized by service.

- **Depends on**:
  - **diskio** (R105) — `DiskIoClient::write` / `fsync` error
    returns are the trigger for R110's error handler. R110 consumes
    diskio errors; it does not modify diskio itself.
  - **diskdb** (landed) — `AllocateBlocks` RPC for replacement
    blocks. May need an `exclude_disks` filter extension (see Open
    Questions). Disk status transitions (`Suspect`) via the sync
    path (§8) for escalation to R83.
  - **chunkdb** (landed, R85) — `update_chunk_strip` RPC for
    replacing failed segments in strip metadata. May need a
    degraded-strip state extension (see Open Questions).
  - **R94** (large-object writer) — the write path being hardened;
    R110 modifies `large_object.rs` to add single-block replacement.
  - **R83** (complete recovery flow) — the escalation path when
    R110's inline retries are exhausted. R110 can ship before R83
    (it just aborts instead of escalating), but the full error
    recovery story needs both.
- **Depended on by**:
  - **R111** (read IO error handling) — reuses R110's negative list
    and degraded-strip tracking for the read path.
  - **R112** (small-write IO error handling) — reuses R110's
    negative list and escalation reporting for the small-object
    writer.

**Acceptance**

**Negative list**:
- Create a `NegativeList` with 3 disks, TTL 60 s → all 3 are
  excluded from `AllocateBlocks` requests; after 60 s, all 3 are
  removed. Unit test.
- A disk that fails repeatedly gets an extended TTL (exponential
  backoff: 60 s → 120 s → 240 s). Unit test (simulate 3 consecutive
  failures, verify TTL growth).
- `AllocateBlocks` with `exclude_disks` containing all disks in a
  disk-group → returns `NoHealthyDisk` error. Unit test.

**Single-block replacement on write**:
- `write_stream` with a diskio mock that fails block 2 of strip 5
  → writer keeps blocks 0/1/3/4, allocates a new block for block 2
  on a different disk, retries the write, calls `update_chunk_strip`
  to replace the segment. Strip 5 completes with 1 replaced block.
  Integration test (inject diskio error on 1 block).
- `write_stream` with a diskio mock that fails the same block 3
  times → writer exhausts `max_block_retries`, aborts with
  `IoError`, returns partial `Location` (sealed chunks), frees
  partial chunk. Integration test.
- After a successful single-block replacement, `query_chunk` shows
  the strip with the new segment (different `disk_id`). Integration
  test.

**Degraded strip tracking**:
- `write_stream` with a parity task that fails EC encode → strip
  marked degraded, chunk sealed with degraded strip recorded.
  `query_chunk` shows the strip with degraded state (parity
  missing). Integration test.

**Fsync failure**:
- `write_stream` with a diskio mock that fails `fsync` on 1 of 5
  disks → writer retries `fsync` once; if it still fails, treats
  the block as failed (re-allocate + re-write + `update_chunk_strip`).
  Integration test.

**Escalation to R83**:
- `write_stream` with persistent failures (all retries exhausted)
  → writer aborts, reports the failed disk to diskdb (triggers
  `Suspect` transition). Integration test (verify diskdb receives
  the report; R83 recovery is a separate test in R83's acceptance).

**Writer drop mid-replacement**:
- Drop the writer during a single-block replacement → `Drop` impl
  frees the partial chunk; the orphaned replacement block is freed
  by diskdb GC. Integration test (drop mid-write, verify partial
  chunk deleted, no orphaned blocks).

**Test commands**: `pixi run cargo test -p crowdb-chunk-client --test
error_handling` (unit + integration), `pixi run cargo test -p
crowdb-chunk-client --test error_handling_e2e` (E2E with real servers
+ fault injection), `pixi run cargo fmt --all -- --check`,
`pixi run cargo clippy --all-targets -- -D warnings`.

**Open Questions**

- **`AllocateBlocks` disk exclusion**: Does diskdb's `AllocateBlocks`
  RPC already support an `exclude_disks` filter, or does it need to
  be added? The current proto may only support `disk_group_id`
  selection. If it needs extension, this is a diskdb protocol change
  that must be filed separately. Alternative: the client-side
  placement logic filters out negative-list disks after receiving
  candidates from diskdb (but this requires diskdb to return
  multiple candidates, which it may not). Needs investigation.

- **Degraded strip state representation**: Should a degraded strip
  (parity missing) be represented as a new `StripState` enum variant
  in the chunk metadata proto, or as a flag on the existing `EcStrip`?
  A new variant is cleaner but requires a proto change; a flag is
  simpler but less type-safe. The chunkdb design §5.2 defines
  `ECState` — does it already have a `Degraded` variant? Needs
  design work.

- **Negative list scope**: Should the negative list be per-writer
  (each `write_stream` call has its own), per-client (shared across
  all writes from one `ChunkdbClient`), or per-node (shared across
  all clients on a machine)? Per-writer is simplest but doesn't
  share learned failures; per-node is most effective but requires
  a shared component. Per-client is the likely middle ground. Needs
  design decision. This decision is shared with R111 (read path)
  and R112 (small-write path), which reuse the same negative list.
