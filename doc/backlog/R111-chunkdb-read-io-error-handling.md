<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R111: chunkdb / diskdb / diskio — Chunk Read IO Error Handling (Unified Read Path)

**Problem**

R107 (chunk read flow) defines the unified read path that reconstructs
object bytes from a `Location` array. The same `ChunkReader`
(`lib/crowdb-chunk-client/src/reader.rs`) serves both object sizes:
- **Large objects** (R94) — EC strips; read `data_num` data blocks,
  EC-decode missing blocks from surviving data + parity.
- **Small objects** (R106) — mirror strips (before R93 conversion);
  read one replica, fall back to another replica on failure. After
  R93 conversion the strips become EC and the read path handles the
  transition transparently.

Both paths assume diskio succeeds. When a disk I/O error occurs
mid-read, R107 has only a coarse fallback: EC decode for already-
missing EC blocks, and mirror replica fallback for mirror strips.
There is no strategy for persistent disk failures during a live read,
no rebuild to repair the strip after a successful fallback, and no
partial-read contract when too many blocks are lost.

Read-path error handling spans **three services** that must
cooperate:
- **diskio** (R105) — detects the I/O error (`read` failure) and
  returns it to the reader.
- **diskdb** — allocates replacement blocks on healthy disks for the
  rebuild step; must exclude the failed disk from future allocations
  (negative list, shared with R110/R112). Eventually transitions the
  disk to `Suspect`/`Bad` via its sync path (§8) for escalation.
- **chunkdb** — updates strip metadata (`update_chunk_strip`) to
  point at the replacement block after a rebuild; tracks degraded
  strips (parity missing — set by the write path, R110/R112, but the
  reader must tolerate reading them).

There is no mechanism to:
- Rebuild + replace a failed block after a successful EC decode or
  mirror fallback (allocate a new block, write the reconstructed
  data, `update_chunk_strip`) so future reads don't pay the fallback
  cost.
- Handle read-path errors with a unified replace-and-retry strategy
  across both EC and mirror strips (EC decode for EC strips, replica
  fallback for mirror strips, then rebuild in both cases).
- Return a partial read result with explicit failed byte ranges when
  a read cannot complete (too many blocks lost for EC decode, or all
  mirror replicas failed) — no silent data corruption.
- Escalate from inline fallback to background recovery (R83) when the
  disk is persistently failing — requires diskdb to transition the
  disk status + chunkdb to hand off the failed strips.

R83 (complete recovery flow) handles **post-failure** rebuild: a disk
goes `Bad`, diskdb's scan lists impacted blocks, chunkdb rebuilds from
surviving replicas/parity. R111 is the **in-line** read error handler
— it fires at the moment a read fails, before the disk is officially
`Bad`. It does fast fallback + best-effort rebuild to keep reads
succeeding and to repair strips; if the failure is unrecoverable
inline (too many blocks lost), it returns a partial result and
escalates to R83's recovery path. Without R111, every live disk read
error either aborts the read (EC strips beyond tolerance) or silently
keeps paying the fallback cost on every subsequent read (no rebuild).

**Current behavior + impact**: R107's error handling is fallback-only.
For EC strips, a missing block is EC-decoded from surviving data +
parity, but the strip is never repaired — every subsequent read of
the same strip pays the EC decode cost again, and the strip stays
vulnerable (a second block failure in the same strip may exceed EC
tolerance). For mirror strips, a failed primary replica falls back to
another replica, but again no rebuild. A live disk read failure (not
a pre-existing missing block) aborts the read in R107 — there is no
inline retry or rebuild. There is no partial-read contract: when a
read cannot complete, the caller gets an opaque error with no
indication of which byte ranges succeeded.

**Design pointers**: chunkdb root design §11 (EC Encoding/Decoding —
isa-l decode for reconstructing lost blocks), §5.2 (Strip — mirror vs
EC, `code_num` tolerance), §10.6 (`update_chunk_strip` RPC — replace
a strip's segment metadata). diskdb design §8 (Disk Status
Management — `HwStatus` transitions, `Suspect`/`Bad` states,
allocation requires `Up`). R107 (chunk read flow — the reader being
hardened; `ChunkReader`, `read_range`, `ChunkReadStream`). R105
(diskio — `DiskIoClient::read` error returns). R83 (complete recovery
flow — post-failure rebuild, the escalation path when R111's inline
fallback is unrecoverable). R110 (large-write IO error handling —
defines the negative list and degraded-strip tracking that R111
reuses for the read path).

**Use scenarios**:

- **EC block read failure, EC decode fallback**: A 50 MB object is
  being read (13 EC strips, 4+1). The diskio read of block 1 of
  strip 3 fails (disk error). The reader has 4 data blocks + 1
  parity block per strip — losing 1 data block is within EC
  tolerance (`code_num = 1`). The reader reads the surviving 3 data
  blocks + 1 parity block, EC-decodes the missing block via isa-l,
  and returns the data. The read continues. Expected: the read
  completes with no data loss; the failed block is transparently
  reconstructed.

- **EC block read failure, rebuild + replace**: The same read
  failure as above, but the reader also triggers a background
  rebuild: allocate a new block on a healthy disk (via diskdb
  `AllocateBlocks` with the negative list filter), write the
  EC-decoded data to it via diskio, and `update_chunk_strip` to
  replace the failed segment. This repairs the strip so future reads
  don't need EC decode. Expected: the read completes; a follow-up
  read of the same strip hits all 5 blocks directly (no EC decode
  needed); the failed disk is added to the negative list.

- **Mirror replica read failure, fallback + rebuild**: A small
  object is being read from a shared chunk (3 mirror strips, written
  by R106). The primary replica's diskio read fails. The reader
  falls back to the second replica (mirror fallback), returns the
  data, and triggers a background rebuild: allocate a new block on a
  healthy disk, copy the data from the surviving replica, and
  `update_chunk_strip` to replace the failed segment. Expected: the
  read completes from the fallback replica; a follow-up read hits
  the primary (now the rebuilt block) directly; the failed disk is
  added to the negative list.

- **Read-path persistent failure, partial read**: A multi-chunk
  object (3 chunks) is being read. Chunk 2's diskio reads fail
  persistently (all retries exhausted, EC decode also fails because
  2 blocks are lost in a 4+1 strip). The reader returns a partial
  result: chunks 1 and 3's data, plus an error for chunk 2's
  missing range. The caller can retry chunk 2 later (after R83
  recovery rebuilds the lost blocks). Expected: no silent data
  corruption — the reader explicitly reports which byte ranges
  failed; the caller knows the read is incomplete.

- **Mirror strip with all replicas failed**: A small object's 3
  mirror replicas all fail to read (3 disks down in the disk-group).
  The reader cannot reconstruct the data — mirror strips have no
  parity. The reader returns a partial result with an error for the
  object's byte range and escalates to R83. Expected: no silent data
  loss; the caller knows the object is unreadable; R83 attempts
  rebuild from any surviving copy (e.g. a not-yet-converted mirror
  on another disk-group, if any).

- **Read of a degraded strip**: A strip was marked degraded by the
  write path (R110/R112 — parity missing, data durable). The reader
  reads all data blocks directly (no parity needed for a full-data
  read). If a data block fails, EC decode is impossible (no parity)
  — the reader returns a partial result and escalates to R83.
  Expected: degraded strips are readable for full-data reads; a
  data block failure on a degraded strip is reported, not silently
  corrupted.

- **Streaming read mid-stream failure**: A large object is being
  streamed via `ChunkReadStream` (memory-bounded). A block read
  fails mid-stream after 30 MB of 50 MB has been delivered. The
  reader applies EC decode fallback (if within tolerance) and
  continues the stream, or returns a partial stream result with the
  failed range and stops. Expected: the stream does not silently
  truncate; the caller knows exactly where the failure occurred.

**Solution**

**No clear solution yet — deferred to design.** The high-level
approach is settled (EC decode / mirror fallback + background rebuild
+ partial read result), but the integration with R107's reader
pipeline and the `ChunkReadStream` streaming path needs design work.
Key open questions below.

**One-line summary**: In-line chunk read error handler that falls
back to EC decode (EC strips) or another replica (mirror strips),
triggers a best-effort background rebuild to repair the strip, and
returns a partial read result with explicit failed ranges when the
failure is unrecoverable inline.

**Numbered work items**:

1. **EC decode fallback on read**
   (`lib/crowdb-chunk-client/src/reader.rs`) — when a diskio `read`
   fails for one block in an EC strip, the reader reads the
   surviving data + parity blocks via diskio and EC-decodes the
   missing block via isa-l (`crowdb_common::ec::decode`). This is
   transparent to the caller — the read completes with
   reconstructed data. Only fires when the failure is within EC
   tolerance (`failed_count ≤ code_num`).
   **Services**: diskio (error detection + surviving block reads),
   crowdb-common (EC decode).

2. **Mirror replica fallback on read**
   (`lib/crowdb-chunk-client/src/reader.rs`) — when a diskio `read`
   fails for a mirror strip's primary replica, the reader falls back
   to the next replica (mirror strips have `replica_count` replicas,
   no parity). Transparent to the caller. Fires for each failed
   replica until one succeeds or all are exhausted.
   **Services**: diskio (error detection + replica reads).

3. **Read-path rebuild + replace**
   (`lib/crowdb-chunk-client/src/reader.rs`) — after a successful EC
   decode or mirror fallback, the reader triggers a background
   rebuild: allocate a new block via diskdb (with the negative list
   filter from R110), write the reconstructed data via diskio,
   `update_chunk_strip` via chunkdb to replace the failed segment.
   This repairs the strip so future reads don't pay the fallback
   cost. The rebuild is best-effort (fire-and-forget); if it fails,
   R83 will eventually rebuild the strip. Reuses R110's negative
   list and single-block replacement machinery.
   **Services**: diskdb (new block, negative list), diskio (write
   reconstructed data), chunkdb (strip metadata update).

4. **Partial read result**
   (`lib/crowdb-chunk-client/src/reader.rs` + `crowdb-chunk-client`
   public API) — when a read cannot complete (EC decode fails
   because too many blocks are lost, or all mirror replicas
   failed), the reader returns a partial result: the byte ranges
   that were successfully read, plus an explicit error for the
   missing ranges. No silent data corruption — the caller knows
   exactly which ranges failed. Applies to both `read_range`
   (single-shot) and `ChunkReadStream` (streaming — the stream
   delivers successful ranges then a final error item for the
   failed range). The caller can retry after R83 recovery.
   **Services**: diskio (read failures), crowdb-chunk-client (partial
   result assembly).

5. **Degraded strip read tolerance**
   (`lib/crowdb-chunk-client/src/reader.rs`) — the reader must
   tolerate reading strips marked degraded by the write path
   (R110/R112 — parity missing, data durable). A full-data read of
   a degraded strip reads all data blocks directly (no parity
   needed). A data block failure on a degraded strip cannot be
   EC-decoded (no parity) — the reader returns a partial result and
   escalates to R83.
   **Services**: chunkdb (degraded state in strip metadata, set by
   R110/R112), diskio (data block reads).

6. **Escalation to R83 recovery** — when inline fallback is
   unrecoverable (EC decode fails — too many blocks lost; or all
   mirror replicas failed), the reader reports the failed disk to
   diskdb (which will transition it to `Suspect` via the sync path,
   §8) and returns a partial result. R83's recovery flow picks up
   the failed strips and rebuilds from surviving replicas/parity.
   The boundary: R111 handles recoverable errors inline (fast,
   in-operation, with rebuild); R83 handles unrecoverable failures
   (slow, background, post-failure). R111 reuses R110's negative
   list and escalation reporting.
   **Services**: diskdb (disk status transition `Suspect`),
   chunkdb (hand off failed strips to R83).

**Flow diagram**:

```
  ┌─────────┐     ┌─────────┐     ┌─────────┐
  │ diskio  │     │ diskdb  │     │ chunkdb │
  │(R105)   │     │         │     │         │
  └────┬────┘     └────┬────┘     └────┬────┘
       │               │               │
       │ read block    │               │
       ▼               │               │
  ┌─────────┐          │               │
  │  ok?    │          │               │
  └────┬────┘          │               │
  yes──┤──no           │               │
       │  │            │               │
       │  ▼            │               │
       │ ┌─────────────────────────────┐
       │ │ crowdb-chunk-client (reader)  │
       │ │  add disk → negative list   │
       │ │  (shared with R110/R112)    │
       │ └────────────┬────────────────┘
       │              │
       │              ▼
       │ ┌────────────────────────────┐
       │ │ strip type?                │
       │ └──┬─────────────────────┬───┘
       │    │EC                   │mirror
       │    ▼                     ▼
       │ ┌──────────────┐  ┌──────────────────┐
       │ │ EC decode    │  │ next replica     │
       │ │ (≤code_num)  │  │ read             │
       │ └──┬───────────┘  └──┬───────────────┘
       │  ok│     │fail    ok │         │fail
       │    │     │          │          │
       │    │     │          │          ▼
       │    │     │          │ ┌──────────────────┐
       │    │     │          │ │ all replicas     │
       │    │     │          │ │ failed?          │
       │    │     │          │ └──┬───────────┬───┘
       │    │     │          │  yes│         │no
       │    │     │          │     │         │
       │    │     ▼          │     ▼         │
       │    │ ┌────────────────────────────┐ │
       │    │ │ diskdb: report disk        │ │
       │    │ │ → Suspect (§8)             │ │
       │    │ │ chunkdb: hand off → R83    │ │
       │    │ └────────────────────────────┘ │
       │    │                                 │
       │    │    ┌────────────────────────────┘
       │    │    │
       │    ▼    ▼
       │ ┌──────────────────────────┐
       │ │ return partial read      │
       │ │ result (failed ranges)   │
       │ └──────────────────────────┘
       │
       ▼ (ok path: EC decode ok / replica ok)
  ┌──────────────────────────┐
  │ bg rebuild (best-effort):│
  │ diskdb alloc block       │
  │ diskio write data        │
  │ chunkdb update_chunk_strip│
  └────────────┬─────────────┘
               │
               ▼
  ┌──────────────────────────┐
  │ return full read result  │
  └──────────────────────────┘
```

**Edge cases at a glance**:

- Single EC data block read fails → EC decode from surviving data +
  parity; rebuild in background. Strip repaired.
- Single mirror replica read fails → fall back to next replica;
  rebuild in background. Strip repaired.
- Read fails on 2 blocks in a 4+1 EC strip → EC decode fails (needs
  ≥ 4 of 5); return partial read result with error for the failed
  range; escalate to R83.
- All mirror replicas failed → no parity to reconstruct; return
  partial read result; escalate to R83.
- Degraded strip (parity missing) read, all data blocks ok → read
  succeeds (no parity needed for full-data read).
- Degraded strip read, one data block fails → no parity to decode;
  return partial read result; escalate to R83.
- Background rebuild fails (new disk also fails) → rebuild is
  best-effort; read still returns correct data (from fallback); R83
  will eventually rebuild.
- Streaming read fails mid-stream → deliver successful ranges, then
  a final error item for the failed range; do not silently truncate.
- Negative list disk comes back (TTL expires) → disk is retried on
  next allocation (shared with R110/R112); if it fails again, TTL
  is extended.

**Dependencies**

This requirement spans three services and reuses R110's machinery —
the dependency list is organized by service.

- **Depends on**:
  - **diskio** (R105) — `DiskIoClient::read` error returns are the
    trigger for R111's error handler. R111 consumes diskio errors;
    it does not modify diskio itself.
  - **diskdb** (landed) — `AllocateBlocks` RPC for rebuild blocks,
    with `exclude_disks` filter (see R110 Open Questions — same
    extension). Disk status transitions (`Suspect`) via the sync
    path (§8) for escalation to R83.
  - **chunkdb** (landed, R85) — `update_chunk_strip` RPC for
    replacing failed segments in strip metadata after rebuild.
    Degraded-strip state (set by R110/R112) must be readable.
  - **R107** (chunk read flow) — the read path being hardened; R111
    modifies `reader.rs` to add fallback + rebuild + partial result.
  - **R110** (large-write IO error handling) — defines the negative
    list (`lib/crowdb-chunk-client/src/negative_list.rs`) and
    degraded-strip tracking that R111 reuses for the read path.
    R111 can ship before R110 (it just doesn't rebuild — only
    fallback + partial result), but the full read-error story needs
    both.
  - **R83** (complete recovery flow) — the escalation path when
    R111's inline fallback is unrecoverable. R111 can ship before
    R83 (it just returns partial results without escalation), but
    the full recovery story needs both.
- **Depended on by**: none yet. R112 (small-write IO error
  handling) will reuse R111's mirror fallback + rebuild for the
  small-object read path (same `ChunkReader`).

**Acceptance**

**EC decode fallback on read**:
- Read a 50 MB object with a diskio mock that fails block 1 of
  strip 3 → reader reads surviving 3 data + 1 parity, EC-decodes
  the missing block, returns correct 50 MB. Integration test
  (inject diskio error on 1 block).
- Read with 2 blocks failed in a 4+1 strip → EC decode fails
  (needs ≥ 4 of 5), reader returns partial result with error for
  the failed range. Integration test.

**Mirror replica fallback on read**:
- Read a small object from a shared chunk (3 mirror strips) with a
  diskio mock that fails the primary replica of strip 1 → reader
  falls back to the second replica, returns correct data.
  Integration test.
- Read with all 3 mirror replicas failed → reader returns partial
  result with error for the object's range; no silent data loss.
  Integration test.

**Read-path rebuild + replace**:
- After a successful EC decode fallback, verify a background rebuild
  allocated a new block, wrote the decoded data, and called
  `update_chunk_strip`. A follow-up read of the same strip hits all
  5 blocks directly (no EC decode). Integration test.
- After a successful mirror fallback, verify a background rebuild
  allocated a new block, copied data from the surviving replica,
  and called `update_chunk_strip`. A follow-up read hits the
  primary (rebuilt) block directly. Integration test.
- Rebuild failure (new disk also fails) → rebuild is best-effort,
  read still returns correct data (from fallback); R83 will
  eventually rebuild. Integration test (inject rebuild failure).

**Degraded strip read tolerance**:
- Read a chunk with a degraded strip (parity missing, set by R110
  write path) → read succeeds (all data blocks present, no parity
  needed for full-data read). Integration test.
- Read a degraded strip where one data block fails → no parity to
  decode, reader returns partial result with error for the failed
  range; escalates to R83. Integration test.

**Partial read result**:
- Read a 3-chunk object where chunk 2 has 2 failed blocks in a
  4+1 strip → reader returns chunks 1 + 3 data + error for chunk 2's
  range. The error includes the failed byte range. Integration test.
- Stream a 50 MB object via `ChunkReadStream` with a block failure
  after 30 MB delivered → stream delivers 30 MB, then a final error
  item for the failed range; no silent truncation. Integration test.

**Escalation to R83**:
- Read with unrecoverable failure (2 blocks lost in 4+1, or all
  mirror replicas failed) → reader reports the failed disk to diskdb
  (triggers `Suspect` transition) and returns partial result.
  Integration test (verify diskdb receives the report; R83 recovery
  is a separate test in R83's acceptance).

**Test commands**: `pixi run cargo test -p crowdb-chunk-client --test
read_error_handling` (unit + integration), `pixi run cargo test -p
crowdb-chunk-client --test read_error_handling_e2e` (E2E with real
servers + fault injection), `pixi run cargo fmt --all -- --check`,
`pixi run cargo clippy --all-targets -- -D warnings`.

**Open Questions**

- **Read-path rebuild timing**: Should the rebuild happen
  synchronously (before returning the read result) or asynchronously
  (fire-and-forget after returning the read result)? Synchronous
  adds latency to the first read but repairs immediately;
  asynchronous is faster for the caller but the strip stays
  vulnerable until the rebuild completes. Likely async (best-effort)
  with R83 as the safety net. Needs confirmation.

- **Mirror rebuild source**: When rebuilding a failed mirror
  replica, the source is a surviving replica (byte-for-byte copy).
  When rebuilding a failed EC data block, the source is EC-decoded
  data. Should these share a single rebuild code path (parameterized
  by data source) or stay separate? Sharing reduces duplication but
  couples EC and mirror logic. Needs design decision.

- **Partial result API shape**: Should the partial read result be a
  new `ReadResult` enum (`Ok(bytes)` / `Partial { ok_ranges, failed
  }`), or should `ChunkReadStream` yield `Ok(bytes)` items followed
  by a final `Err(failed_range)` item? The streaming path likely
  needs the latter regardless. Needs design work — tied to R107's
  public API.
