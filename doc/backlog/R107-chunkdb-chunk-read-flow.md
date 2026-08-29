<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R107: chunkdb — Chunk Object Read Flow

**Problem**

R94 (large-object writer) and R106 (small-object writer) produce
`Location` arrays that record where object data lives across chunks.
There is no reader that takes a `Location` array and reconstructs the
object's bytes. Without a read flow, the write path is a one-way
street — data goes in but cannot come out.

The read flow must handle both strip types:
- **EC strips** (large objects, R94): read `data_num` data blocks
  from diskio, concatenate them (minus padding on the last strip),
  and return the object's bytes. If some data blocks are missing
  (disk failure), read surviving data + parity blocks and EC-decode
  via isa-l.
- **Mirror strips** (small objects, R106, before R93 conversion):
  read one mirror replica; if the primary replica's disk is `Bad`,
  fall back to another replica. After R93 conversion, mirror strips
  become EC strips and the read flow must handle the transition
  transparently.

The read flow must also handle multi-chunk objects (R94's chunk
rotation): a `Location` array with N entries means the object spans
N chunks, and the reader must fetch and concatenate data from all N
locations in `logical_offset` order.

**Current behavior + impact**: No read flow exists. The `Location`
type is defined in R94 but nothing reads from it. The chunkdb client
has `query_chunk` (returns chunk metadata including strip layout) but
no data-read API. Without R107, written objects are inaccessible —
the system can write but not read, which is useless for any real
workload.

**Design pointers**: chunkdb root design §5.2 (Strip — mirror vs EC
data capacity), §5.3 (Chunk — strips + logical-to-physical mapping),
§11 (EC Encoding/Decoding — isa-l decode for recovery). R94 defines
the `Location` type (`chunk_id`, `offset`, `length`, `logical_offset`,
`logical_length`). R105 provides `DiskIoClient::read` for reading
strip blocks from disk. The read flow uses chunkdb's `query_chunk` to
resolve a `Location`'s `chunk_id` to its strip layout, then reads the
relevant strip blocks via diskio.

**Use scenarios**:

- **Read a single-chunk large object**: A caller provides a `Location`
  array with one entry: `{chunk_id, [0, 50MB), logical [0, 50MB)}`.
  The reader queries the chunk (7 EC strips, 8+4), maps the offset
  range [0, 50MB) to strips 0-6, reads the data blocks from each
  strip via diskio, concatenates them, and returns 50 MB. Expected:
  the returned bytes match the originally written object.

- **Read a multi-chunk large object**: A caller provides a `Location`
  array with 3 entries (2.5 GB object across 3 chunks). The reader
  fetches all 3 chunks' data in parallel (one `query_chunk` per
  chunk, then parallel diskio reads), concatenates the results in
  `logical_offset` order, and returns 2.5 GB. Expected: the full
  object is reconstructed correctly.

- **Read a small object from a mirror strip**: A caller provides a
  `Location` for a 16 KB object in a shared chunk. The reader queries
  the chunk, finds the mirror strip covering the offset range, reads
  one replica via diskio, extracts the 16 KB at the correct offset,
  and returns it. Expected: the 16 KB matches the original object.

- **Read a small object after EC conversion**: The same 16 KB
  object's chunk has been converted from mirror to EC by R93. The
  reader queries the chunk, finds an EC strip (not mirror) covering
  the offset range, reads the data blocks, and extracts the 16 KB.
  Expected: transparent — the caller sees the same bytes regardless
  of strip type.

- **Read with a failed disk (EC recovery)**: A large object's 3rd EC
  strip has a missing data block (disk is `Bad`). The reader detects
  the missing block (diskio returns `DiskOffline`), reads the
  surviving data + parity blocks for that strip, EC-decodes via isa-l
  to reconstruct the missing block, and continues. Expected: the
  read succeeds with no data loss, albeit with higher latency for
  the reconstructed strip.

- **Read with a failed disk (mirror fallback)**: A small object's
  primary mirror replica is on a `Bad` disk. The reader falls back
  to the secondary replica, then tertiary. Expected: the read
  succeeds from a surviving replica.

- **Range read (partial object)**: A caller requests bytes
  `[10MB, 20MB)` of a 50 MB object. The reader maps the logical
  offset range to the corresponding chunk(s) and strip(s), reads
  only the relevant blocks, and returns 10 MB. Expected: partial
  reads do not fetch the entire object.

- **Concurrent read + write (shared chunk)**: A reader reads from a
  shared chunk that is still receiving writes (Active state). The
  reader reads only from sealed/completed strips (strip offset range
  fully written). If the `Location` points to a strip that is still
  being written, the reader waits for the strip to complete or
  returns `IoError::NotYetAvailable`. Expected: no torn reads — the
  reader never sees partially written data.

**Solution**

A chunk object reader in the chunkdb client library that takes a
`Vec<Location>` and returns the object's bytes. The reader resolves
each `Location` to its chunk's strip layout via `query_chunk`, maps
the offset range to specific strips and blocks, reads the blocks via
diskio (R105), handles EC decode (for missing blocks) and mirror
fallback (for failed replicas), and concatenates the results in
`logical_offset` order. Supports full-object reads and partial
(range) reads.

**One-line summary**: A chunk object reader that reconstructs object
bytes from a `Location` array by querying chunk strip layouts, reading
blocks via diskio, handling EC decode + mirror fallback for failed
disks, and concatenating across multi-chunk objects.

**Numbered work items**:

1. **`ChunkReader`**
   (`lib/crowdb-chunk-client/src/reader.rs`) — the main read API:
   ```rust
   pub struct ChunkReader {
       chunkdb: ChunkdbClient,
       diskio: DiskIoClient,
       ec_scheme: EcScheme,
   }

   impl ChunkReader {
       /// Read the full object from a Location array.
       pub async fn read_object(
           &self, locations: &[Location],
       ) -> Result<Bytes, ReadError>;

       /// Read a partial range [start, end) from a Location array.
       pub async fn read_range(
           &self, locations: &[Location],
           start: u64, end: u64,
       ) -> Result<Bytes, ReadError>;
   }
   ```
   `read_object` is a convenience wrapper around `read_range` with
   `start=0` and `end=total_logical_length`.

2. **Offset-to-strip mapping** (`reader.rs`) — for each `Location`,
   the reader calls `chunkdb.query_chunk(chunk_id)` to get the
   chunk's strip layout. It maps the `Location`'s `[offset, length)`
   to the strips that cover that range:
   - Strip `i` covers data offsets `[i × strip_data_capacity, (i+1) ×
     strip_data_capacity)` within the chunk.
   - For a `Location` with `[offset, offset+length)`, the reader
     computes `start_strip = offset / strip_data_capacity` and
     `end_strip = (offset + length - 1) / strip_data_capacity`.
   - For each strip in `[start_strip, end_strip]`, compute the byte
     range within that strip: `strip_start = max(offset, i ×
     strip_data_capacity) - i × strip_data_capacity`,
     `strip_end = min(offset + length, (i+1) × strip_data_capacity)
     - i × strip_data_capacity`.
   - The last strip may be partial (`sealed_length < strip_data_
     capacity`) — the reader respects `sealed_length` as the actual
     data length.

3. **EC strip read** (`reader.rs`) — for an EC strip (8+4):
   - Read the `data_num` (8) data blocks via `DiskIoClient::read`,
     in parallel.
   - If all 8 succeed: concatenate the data blocks, extract the
     requested byte range, return.
   - If some data blocks fail (disk `Bad`): read the surviving data
     blocks + `code_num` (4) parity blocks, EC-decode via isa-l
     (`crowdb_common::ec::decode`) to reconstruct the missing data
     blocks, then concatenate and extract.
   - If more than `code_num` blocks are missing: the strip is
     unrecoverable → `ReadError::DataLoss`.

4. **Mirror strip read** (`reader.rs`) — for a mirror strip (3
   replicas):
   - Read from the primary replica via `DiskIoClient::read`.
   - If the primary fails (disk `Bad` or read error): fall back to
     the secondary replica, then tertiary.
   - If all replicas fail: `ReadError::DataLoss`.
   - Extract the requested byte range from the mirror block (the
     mirror block is a full copy of the strip's data).

5. **Multi-chunk assembly** (`reader.rs`) — for a `Location` array
   with N entries:
   - Sort the locations by `logical_offset`.
   - For each location, fetch the chunk's data (parallel across
     chunks via `futures::join_all`).
   - Concatenate the results in `logical_offset` order.
   - Verify the total length matches the sum of `logical_length`s.
   - For `read_range`: filter locations that overlap `[start, end)`,
     read only the overlapping byte ranges, concatenate.

6. **Partial range read** (`reader.rs`) — `read_range(start, end)`:
   - Map `[start, end)` (logical offsets) to the corresponding
     `Location` entries and their sub-ranges.
   - For each overlapping location, compute the sub-range:
     `local_start = max(start, loc.logical_offset) - loc.logical_offset`,
     `local_end = min(end, loc.logical_offset + loc.logical_length)
     - loc.logical_offset`.
   - Read only the blocks covering `[local_start, local_end)` within
     each chunk — do not fetch the entire chunk.
   - This requires block-level granularity: the reader maps the
     sub-range to specific disk blocks within the strip and reads
     only those blocks (plus any blocks needed for EC decode if the
     range spans partial blocks).

7. **Concurrency + streaming** (`reader.rs`) — for large objects
   (multi-GB), the reader does not buffer the entire object in
   memory. Instead, it returns a `ChunkReadStream` (an `async_stream`
   or `tokio::io::AsyncRead` impl) that yields data chunk-by-chunk:
   - The stream reads one chunk's data at a time (or N chunks in
     parallel if memory budget allows), yields the bytes, then reads
     the next.
   - Memory budget is configurable (default 64 MB) — the stream
     prefetches up to N chunks in parallel to saturate disk
     bandwidth, but never exceeds the budget.
   - For small objects, `read_object` returns `Bytes` directly (no
     stream needed).

8. **Read consistency** (`reader.rs`) — the reader queries the chunk
   metadata at read time to get the current strip layout. If the
   chunk has been converted (mirror→EC by R93) since the `Location`
   was written, the strip type may differ from what the writer
   recorded. The reader handles both strip types transparently — it
   reads whatever strip type `query_chunk` returns. The `Location`'s
   `offset` and `length` are stable (they refer to byte offsets
   within the chunk, not strip indices), so the offset-to-strip
   mapping works regardless of strip type changes.

**Flow diagram**:

```
Caller                ChunkReader           chunkdb          diskio (R105)    isa-l
  │                       │                    │                 │             │
  │ read_object(locs)     │                    │                 │             │
  │──────────────────────►│                    │                 │             │
  │                       │                    │                 │             │
  │                       │ for each Location: │                 │             │
  │                       │ query_chunk(id)    │                 │             │
  │                       │───────────────────►│                 │             │
  │                       │◄───────────────────│                 │             │
  │                       │ (strip layout)     │                 │             │
  │                       │                    │                 │             │
  │                       │ map offset→strips  │                 │             │
  │                       │                    │                 │             │
  │                       │ for each strip:    │                 │             │
  │                       │   EC: read 8 data  │                 │             │
  │                       │   blocks (parallel)│                 │             │
  │                       │────────────────────────────────────►│             │
  │                       │◄────────────────────────────────────│             │
  │                       │                    │                 │             │
  │                       │   if block missing:│                 │             │
  │                       │   read parity      │                 │             │
  │                       │────────────────────────────────────►│             │
  │                       │◄────────────────────────────────────│             │
  │                       │   EC decode        │                 │             │
  │                       │──────────────────────────────────────────────────►│
  │                       │◄──────────────────────────────────────────────────│
  │                       │                    │                 │             │
  │                       │   Mirror: read 1   │                 │             │
  │                       │   replica (fallback│                 │             │
  │                       │   on failure)      │                 │             │
  │                       │────────────────────────────────────►│             │
  │                       │◄────────────────────────────────────│             │
  │                       │                    │                 │             │
  │                       │ concatenate in     │                 │             │
  │                       │ logical_offset     │                 │             │
  │                       │ order              │                 │             │
  │                       │                    │                 │             │
  │ ◄─────────────────────│                    │                 │             │
  │ Bytes (full object)   │                    │                 │             │
```

**Edge cases at a glance**:

- Chunk deleted between write and read → `query_chunk` returns
  `ChunkNotFound` → `ReadError::ChunkDeleted`.
- Strip converted (mirror→EC) since write → reader handles both
  types transparently; `Location` offsets are stable.
- EC strip with 1-4 missing blocks (≤ `code_num`) → EC decode
  reconstructs; read succeeds with higher latency.
- EC strip with > 4 missing blocks (> `code_num` for 8+4) →
  `ReadError::DataLoss`.
- Mirror strip with all replicas on `Bad` disks →
  `ReadError::DataLoss`.
- Partial read spanning two chunks → both chunks read in parallel,
  results concatenated; no gap.
- Partial read within a single block → the reader reads the full
  block (diskio reads at block granularity) and extracts the
  requested sub-range; no sub-block read.
- Read from an Active shared chunk → only reads from completed
  strips; if the `Location` points to an in-progress strip, returns
  `ReadError::NotYetAvailable` (or waits, configurable).
- Empty `Location` array → returns empty `Bytes` (zero-length object).
- `Location` with `length = 0` → skipped (no read issued).

**Dependencies**

- **Depends on**: **R94** (large object writer) — uses the `Location`
  type. **R105** (disk IO engine) — reads strip blocks via
  `DiskIoClient`. **chunkdb** (landed, R85) — `query_chunk` RPC for
  strip layout. **crowdb-common EC** (landed) — isa-l decode for EC
  recovery.
- **Depended on by**: nothing (terminal data-path component). R83
  (chunkdb recovery) uses R107's EC decode logic for rebuilding lost
  data.

**Acceptance**

**Single-chunk read (EC)**:
- Write a 50 MB object via R94, then read it via `read_object` →
  returned bytes match the original. Integration test.
- Write a 5 MB object (partial strip), read it → correct bytes, no
  padding in the output. Integration test.

**Multi-chunk read**:
- Write a 2.5 GB object via R94 (3 chunks), read it via `read_object`
  → 2.5 GB returned, bytes match. Integration test (may use a hash
  comparison for large data).

**Small object read (mirror)**:
- Write a 16 KB object via R106, read it via `read_object` → 16 KB
  returned, bytes match. Integration test.
- Write 100 × 16 KB objects to a shared chunk, read each by its
  `Location` → all 100 reads return correct bytes. Integration test.

**EC recovery read**:
- Write a 50 MB object (7 EC strips), mark one data block's disk as
  `Bad`, read the object → EC decode reconstructs the missing block,
  full 50 MB returned. Integration test (inject disk failure).
- Mark 4 data blocks missing in one strip (≤ `code_num` for 8+4) →
  EC decode reconstructs all 4, read succeeds. Integration test.
- Mark 5 data blocks missing in one strip (> `code_num`) →
  `ReadError::DataLoss`. Integration test.

**Mirror fallback read**:
- Write a 16 KB object (3 mirror replicas), mark primary replica's
  disk `Bad`, read → falls back to secondary, correct bytes.
  Integration test.
- Mark all 3 replicas' disks `Bad` → `ReadError::DataLoss`.
  Integration test.

**Conversion transparency**:
- Write a 16 KB object (mirror strip), trigger R93 conversion to EC,
  read the object → correct bytes, no error. Integration test.

**Partial range read**:
- Write a 50 MB object, `read_range(10MB, 20MB)` → 10 MB returned,
  bytes match the corresponding section. Integration test.
- `read_range` spanning two chunks (object in 3 chunks, range covers
  end of chunk 1 + start of chunk 2) → correct bytes from both
  chunks, concatenated. Integration test.

**Multi-location (small objects in shared chunk)**:
- 100 objects in a shared chunk, read each by its `Location`
  independently → each returns only its own bytes, no cross-
  contamination. Integration test.

**Streaming read**:
- `read_object` on a 2.5 GB object with `ChunkReadStream` → data is
  yielded in chunks, total memory usage stays under 64 MB.
  Integration test (verify memory via metrics or allocation
  tracking).

**Edge cases**:
- Empty `Location` array → empty `Bytes`. Unit test.
- `Location` with `length = 0` → skipped, no diskio read. Unit test.
- Chunk deleted → `ReadError::ChunkDeleted`. Integration test.
- Read from Active shared chunk, `Location` points to in-progress
  strip → `ReadError::NotYetAvailable`. Integration test.

**Test commands**: `pixi run cargo test -p crowdb-chunkdb-client --test
chunk_reader`, `pixi run cargo fmt --all -- --check`,
`pixi run cargo clippy --all-targets -- -D warnings`.

**Open Questions**

- **Read from in-progress strips**: When a `Location` points to a
  strip in an Active shared chunk that is still being written, should
  the reader (a) return `ReadError::NotYetAvailable` immediately, (b)
  wait for the strip to complete (with a timeout), or (c) read the
  partially written data up to the strip's current write offset?
  Option (c) is dangerous (torn reads) and is rejected. Between (a)
  and (b): (a) is safer for correctness (the object may not be fully
  written yet), (b) is more convenient for callers who know the
  object is complete. Current design uses (a). Confirm.

- **Block-level vs strip-level reads for partial ranges**: A partial
  range read (`read_range(10MB, 20MB)`) maps to specific blocks
  within strips. For EC strips, reading a sub-range of one data block
  still requires reading the full 1 MB block from disk (diskio reads
  at block granularity). Should the reader support sub-block reads
  (e.g. `pread` with a sub-block offset and length) to avoid reading
  unnecessary data? This would require diskio (R105) to support
  arbitrary-offset reads, not just block-aligned reads. Trade-off:
  diskio simplicity (block-aligned only) vs read amplification (full
  block read for a small sub-range). For large objects, block-level
  reads are fine (1 MB block for a 10 MB range = 10 blocks, minimal
  amplification). For small objects in shared chunks, a 16 KB object
  in a 1 MB block = 64× amplification. Decision: diskio supports
  arbitrary-offset reads (pread is offset-based anyway); the reader
  requests only the bytes it needs. Confirm this aligns with R105's
  design.
