<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R93: chunkdb — Mirror-to-EC Conversion

**Problem**

Shared chunks (R106 small-object writer) write data to 3 mirror strips
first for low write latency — the caller gets success after 3 mirror
writes, before EC encoding. Mirror strips use 3× storage (3 full
copies). As data ages and becomes colder, the 3× storage overhead is
wasteful: an 8+4 EC strip stores the same data capacity at 1.5×
overhead (12 blocks for 8 data blocks) vs 3× for 3-way mirror.

chunkdb v1 (R85) supports both mirror and EC strips but has no
mechanism to convert one to the other. The chunkdb design §2 Non-Goals
explicitly defers "Background conversion of mirror strips to EC strips
(for shared chunks)" to a future requirement. R106's small-object
writer depends on this conversion — without it, shared chunks
permanently carry 3× mirror overhead.

**Current behavior + impact**: A mirror strip stays a mirror strip
forever. There is no `convert_strip` or background conversion task.
Shared chunks (once R106 lands) will accumulate mirror strips at 3×
storage cost with no path to the more space-efficient EC encoding.
For a cluster storing 100 TB of small-object data, this is 200 TB of
wasted space (300 TB mirror vs 150 TB EC at 8+4).

**Design pointers**: chunkdb root design §2 (Non-Goals: "No
mirror-to-EC conversion in v1"), §5.2 (Strip — mirror vs EC data
capacity), §10.6 (`update_chunk_strip` RPC — the atomic strip
replacement primitive that R93 builds on), §11 (EC Encoding/Decoding
— isa-l encode from data blocks). The `update_chunk_strip` lifecycle
handler (§10.6) already supports replacing a strip: "free old strip's
segments → commit new strip's segments → replace strip → `put_chunk`".
R93 orchestrates the conversion: read mirror data, EC-encode, allocate
EC strip blocks, write EC data+parity, then `update_chunk_strip` to
swap atomically.

**Use scenarios**:

- **Background conversion of a sealed shared chunk**: A shared chunk
  has been sealed (no more writes). A background conversion task
  picks it up, reads each mirror strip's data, EC-encodes it into
  data+parity blocks, allocates an EC strip via chunkdb, writes the
  EC blocks via diskio (R105), and calls `update_chunk_strip` to
  replace the mirror strip with the EC strip. The old mirror blocks
  are freed. Expected: the chunk's data capacity is preserved; storage
  overhead drops from 3× to 1.5× (8+4 EC).

- **Conversion of an active shared chunk**: A shared chunk is still
  receiving writes (Active state). A mirror strip that is fully
  written (no more appends expected to that strip's offset range) can
  be converted while the chunk continues to receive writes to new
  strips. The conversion reads the completed mirror strip, EC-encodes,
  and swaps. Expected: no write latency impact for new writes (they
  go to new strips); the converted strip is read-only after swap.

- **Conversion under read load**: A reader (R107) is reading from a
  mirror strip that is being converted. The conversion is atomic via
  `update_chunk_strip` — the reader either sees the old mirror strip
  or the new EC strip, never a partial state. Expected: reads
  continue without error; the reader may need to switch from mirror
  read to EC read if the conversion completes mid-read.

- **Conversion failure recovery**: The EC encode fails (isa-l error)
  or an EC block write fails (disk error). The conversion aborts; the
  mirror strip is untouched; the partially allocated EC blocks are
  freed via chunkdb rollback. Expected: the chunk is unchanged; the
  conversion task retries later or skips the strip.

- **Batch conversion**: An operator triggers conversion of all sealed
  shared chunks older than 24 hours. The conversion task processes
  chunks in priority order (oldest first, or most-mirror-strips first)
  with configurable concurrency and bandwidth throttling. Expected:
  gradual space reclamation without foreground traffic impact.

**Solution**

A background conversion service in chunkdb that transforms mirror
strips to EC strips using the existing `update_chunk_strip` RPC as
the atomic swap primitive. The conversion reads mirror data via
diskio (R105), EC-encodes via isa-l (crow-common), allocates EC strip
blocks via chunkdb, writes EC data+parity via diskio, and swaps the
strip atomically. Conversion is triggered by a configurable policy
(seal age, mirror strip count, manual trigger) and throttled to avoid
starving foreground traffic.

**One-line summary**: Background mirror→EC strip conversion using
diskio for data read/write, isa-l for EC encode, and `update_chunk_strip`
for the atomic swap — reclaims 3×→1.5× storage on shared chunks.

**Numbered work items**:

1. **Conversion task** (`app/crow-chunkdb/src/conversion.rs`) — a
   background `BgRunner` task (following the diskdb `ScannerTask`/
   `BgRunner` pattern, §10) that scans for convertible chunks. A
   chunk is convertible if: (a) it has mirror strips, (b) it is
   Sealed or the specific strip's offset range is fully written, (c)
   it meets the conversion policy (age, strip count, manual trigger).
   The task enqueues convertible strips into a work queue with
   configurable concurrency (default 4 parallel conversions) and
   bandwidth throttling (default 50 MB/s, configurable).

2. **Conversion logic** (`app/crow-chunkdb/src/conversion.rs`) —
   for each mirror strip:
   - Read the mirror data from one replica via `DiskIoClient::read`
     (R105). If the primary replica's disk is `Bad`, fall back to
     another replica.
   - EC-encode the data via `crow-common` isa-l wrapper: split the
     mirror data into `data_num` data blocks, encode `code_num`
     parity blocks.
   - Allocate an EC strip via chunkdb's `AllocateStrip` (placement:
     rack-aware EC placement, §7.2).
   - Write the `data_num` + `code_num` blocks to the allocated
     segments via `DiskIoClient::write` (R105), in parallel.
   - `fsync` each disk via `DiskIoClient::fsync` (R105).
   - Call `update_chunk_strip(chunk_id, strip_index, new_ec_strip)`
     to atomically swap: frees old mirror segments, commits new EC
     segments, updates chunk metadata in KV.
   - On any failure: free the partially allocated EC blocks via
     chunkdb, leave the mirror strip untouched, log the error, retry
     later.

3. **Conversion policy** (`app/crow-chunkdb/src/conversion.rs`) —
   configurable triggers:
   - `conversion_min_seal_age_secs` (default 3600) — only convert
     strips in chunks sealed more than N seconds ago.
   - `conversion_min_mirror_strips` (default 4) — only convert chunks
     with at least N mirror strips (amortize the conversion overhead).
   - `conversion_max_concurrency` (default 4) — max parallel
     conversions.
   - `conversion_max_bandwidth_mbps` (default 50) — throttle read +
     write I/O to avoid starving foreground traffic.
   - Manual trigger via HTTP endpoint `POST /convert_chunk` with
     `{ "chunk_id": ... }` or `POST /convert_all` with a filter.

4. **Metrics + observability** (`app/crow-chunkdb/src/metrics.rs`) —
   extend `LifecycleMetrics` with conversion counters:
   `conversion_started_count`, `conversion_completed_count`,
   `conversion_failed_count`, `conversion_bytes_read`,
   `conversion_bytes_written`, `conversion_stripes_freed` (mirror
   blocks freed). HTTP endpoint `GET /conversion_metrics` returns the
   snapshot.

5. **ChunkdbClient conversion API** (`lib/crow-chunkdb-client/`) —
   `async fn trigger_conversion(&self, chunk_id: &ChunkId) ->
   Result<(), ConversionError>` (manual trigger for a single chunk),
   `async fn trigger_conversion_batch(&self, filter: &
   ConversionFilter) -> Result<u64, ConversionError>` (batch trigger,
   returns count of enqueued chunks). These are management APIs used
   by the console (R96) and CLI.

**Flow diagram**:

```
Conversion Task                chunkdb           diskio (R105)       isa-l
     │                            │                   │                │
     │ 1. Scan for convertible    │                   │                │
     │    chunks (mirror strips)  │                   │                │
     │ ◄──────────────────────────│                   │                │
     │                            │                   │                │
     │ 2. Read mirror data        │                   │                │
     │    from one replica        │                   │                │
     │ ──────────────────────────────────────────────►│                │
     │ ◄──────────────────────────────────────────────│                │
     │  (Bytes: mirror strip data)                    │                │
     │                            │                   │                │
     │ 3. EC encode (data + parity)                   │                │
     │ ───────────────────────────────────────────────────────────────►│
     │ ◄───────────────────────────────────────────────────────────────│
     │  (data_num + code_num blocks)                  │                │
     │                            │                   │                │
     │ 4. Allocate EC strip        │                   │                │
     │ ──────────────────────────►│                   │                │
     │ ◄──────────────────────────│                   │                │
     │  (EC strip segments)       │                   │                │
     │                            │                   │                │
     │ 5. Write EC blocks         │                   │                │
     │ ──────────────────────────────────────────────►│                │
     │ ◄──────────────────────────────────────────────│                │
     │                            │                   │                │
     │ 6. fsync all disks         │                   │                │
     │ ──────────────────────────────────────────────►│                │
     │ ◄──────────────────────────────────────────────│                │
     │                            │                   │                │
     │ 7. update_chunk_strip      │                   │                │
     │    (atomic swap)           │                   │                │
     │ ──────────────────────────►│                   │                │
     │    (frees mirror segs,     │                   │                │
     │     commits EC segs,       │                   │                │
     │     put_chunk)             │                   │                │
     │ ◄──────────────────────────│                   │                │
     │  Ok(())                    │                   │                │
```

**Edge cases at a glance**:

- Mirror replica read fails (disk `Bad`) → fall back to another
  replica; if all replicas fail, the strip is unrecoverable (data
  loss) — log critical error, skip, alert operator.
- EC encode fails (isa-l error) → abort conversion, free allocated EC
  blocks, leave mirror strip, retry later.
- EC block write fails (disk error on target) → abort, free allocated
  blocks, retry with a new EC strip allocation (different placement).
- `update_chunk_strip` fails (KV error) → the EC blocks are written
  but the metadata is not swapped; on retry, the conversion detects
  the stale state (EC blocks allocated but strip not swapped) and
  retries the swap or cleans up the orphaned blocks.
- Chunk is deleted during conversion → the conversion task detects
  the `Deleted` state and aborts; any allocated EC blocks are freed.
- Concurrent conversion + read → `update_chunk_strip` is atomic under
  the per-chunk lock (§10); the reader sees either the old mirror or
  the new EC strip, never a partial state.
- Conversion throttling under foreground load → bandwidth limiter
  (token bucket) reduces conversion I/O rate when foreground disk
  I/O is high; conversion pauses if the disk is near saturation.

**Dependencies**

- **Depends on**: **R105** (disk IO engine) — reads mirror data and
  writes EC blocks via `DiskIoClient`. **chunkdb** (landed, R85) —
  uses `update_chunk_strip` RPC, `AllocateStrip`, chunk metadata.
  **crow-common EC** (landed with R85) — isa-l encode.
- **Depended on by**:
  - **R106** (small object writer) — depends on R93 to convert the
    mirror strips that the writer produces. R106 can land before R93
    is fully implemented (mirror strips are correct, just space-
    inefficient), but R93 is the mechanism that makes R106's
    mirror-first strategy viable long-term.

**Acceptance**

**Conversion correctness**:
- A sealed shared chunk with 3 mirror strips (each 1 MB, 3 replicas)
  → run conversion → chunk has 3 EC strips (each 8+4, 1 MB data
  capacity), old mirror segments are freed. Verify via `query_chunk`:
  strip types are EC, old segment IDs are not present. Integration
  test.
- Data integrity after conversion: read the chunk's data via R107
  (read flow) before and after conversion → identical bytes.
  Integration test.
- EC parity correctness: after conversion, simulate one EC data block
  failure → EC decode reconstructs the data. Integration test (uses
  isa-l decode).

**Conversion atomicity**:
- A read (R107) concurrent with conversion → read succeeds with
  either the old mirror strip or the new EC strip, never errors.
  Integration test (start read mid-conversion).
- Conversion failure mid-way (inject diskio write error on 2nd EC
  block) → mirror strip is untouched, EC blocks are freed, chunk
  metadata shows the original mirror strip. Integration test.

**Conversion policy**:
- Chunk sealed < `conversion_min_seal_age_secs` ago → not enqueued
  for conversion. Unit test (mock time).
- Chunk with < `conversion_min_mirror_strips` → not enqueued. Unit
  test.
- Manual trigger via `trigger_conversion(chunk_id)` → chunk is
  enqueued regardless of policy. Integration test.

**Throttling**:
- `conversion_max_bandwidth_mbps = 10` → conversion I/O rate does not
  exceed 10 MB/s (measured over a 5-second window). Integration test.
- `conversion_max_concurrency = 2` → at most 2 strips are being
  converted simultaneously. Integration test.

**Fallback + error handling**:
- Primary mirror replica disk is `Bad` → conversion reads from
  secondary replica, succeeds. Integration test.
- All mirror replicas are `Bad` → conversion logs critical error,
  skips the strip, continues with other chunks. Integration test.
- Chunk deleted during conversion → conversion aborts, allocated EC
  blocks are freed. Integration test.

**Metrics**:
- After converting 3 mirror strips → `conversion_completed_count` =
  3, `conversion_bytes_read` = 3 MB, `conversion_bytes_written` = 36
  MB (3 × 12 blocks × 1 MB for 8+4 EC), `conversion_stripes_freed` =
  9 (3 strips × 3 mirror replicas). Integration test.

**Test commands**: `pixi run cargo test -p crow-chunkdb --test
conversion`, `pixi run cargo test -p crow-chunkdb-client --test
conversion_api`, `pixi run cargo fmt --all -- --check`,
`pixi run cargo clippy --all-targets -- -D warnings`.

**Open Questions**

- **Convert in-place vs new allocation**: Should the EC strip reuse
  the mirror strip's disk blocks (in-place, fewer allocations) or
  allocate fresh blocks (simpler, requires freeing mirror blocks)?
  In-place is complex (mirror blocks are on 3 nodes, EC blocks need
  12 nodes) and rarely possible. Fresh allocation is cleaner and
  decouples conversion from placement. The current design uses fresh
  allocation. Confirm this is the right trade-off.
