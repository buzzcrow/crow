<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R79: diskdb — Free Batch (Size-Threshold Grouping, No Timer)

**Problem**: R72 implements free as an immediate write — each free
deletes the `BusyBlockKey` and writes one `FreeBlockValue` to the
bound data group via one `batch_write` (per the record model in §3.4/§7:
on free, `BusyBlockKey` is deleted and `FreeBlockValue` carrying
`previous_owner` is written). This is simple and correct, but under
high-free-throughput workloads (mass delete, object store teardown,
chunk GC) each free is one KV round-trip, which limits free throughput.

The design doc (§8) originally specified a timer-based `FreeBatch`
(default 500 ms flush). A timer-based flush has two drawbacks:

- **Ghost-allocation window** — a crash between the local bitmap clear
  and the batch flush leaves the block appearing busy in KV but free
  in memory. The §12 scanner reconciles this, but it is a correctness
  wart.
- **Background task complexity** — a timer-driven flush loop is a
  separate background task with its own lifecycle, error handling, and
  shutdown ordering.

**Solution**: Add a **size-threshold** free batch — group frees into a
batch and flush via one `batch_write` when the batch reaches a
configurable size. **No timer.** The flush is synchronous on the free
path, not a background loop.

1. **`FreeBatch`** — create `app/crow-diskdb/src/persistence/free_batch.rs`:

   - `FreeBatch` — `inner: Mutex<Vec<FreeEntry>>` where `FreeEntry`
     is `{ disk_id, zone_idx, unit_offset, unit_count,
     previous_owner }`. `append(entry)`, `drain() -> Vec<FreeEntry>`,
     `re_enqueue(items)`, `len()`, `is_empty()`.
   - **No background flush loop.** No `FreeFlushLoop`, no
     `tokio::spawn`, no `sleep(interval)`.

2. **Free path** — update `app/crow-diskdb/src/persistence/free.rs`
   (from R72):

   - `free_block(node, segment, free_batch, journal) -> Result<()>`:
     a. `node.free_block(segment)` — clear bitmap locally (per-bit
        CAS clear).
     b. `free_batch.append(FreeEntry { ... })`.
     c. If `free_batch.len() >= free_flush_max_batch` (default 256):
        `drain()` the batch, group by `dg_id`, and for each affected
        data group `await journal.persist_free_batch(...)` (deletes
        each `BusyBlockKey` and writes each `FreeBlockValue` per the
        record model in §3.4/§7). On failure: `re_enqueue(items)` for
        retry on the next free that hits the threshold.
     d. Return `Ok(())` — if the threshold was not hit, the free is
        buffered and will flush on a later free that hits the
        threshold.

3. **Graceful shutdown** — update `app/crow-diskdb/src/main.rs`:

   - On graceful shutdown, drain and flush the `FreeBatch` before
     exit (one final `batch_write` per affected data group). This
     prevents ghost allocations on restart.
   - On ungraceful shutdown, unflushed frees are left for the §12
     ghost-allocation scanner to reconcile (the block appears busy in
     KV but is free in memory; the scanner detects and corrects).

4. **Configuration** — add to the diskdb config:

   - `free_batch_enabled` (default false) — toggle between immediate
     free (R72 behavior) and size-threshold batching (this
     requirement). Default false so R72's immediate-free behavior is
     preserved unless explicitly enabled.
   - `free_flush_max_batch` (default 256) — the size threshold that
     triggers a flush.

**Scope** (expected changed files):

- `app/crow-diskdb/src/persistence/free_batch.rs` — `FreeBatch` struct.
- `app/crow-diskdb/src/persistence/free.rs` — size-threshold flush on
  the free path.
- `app/crow-diskdb/src/main.rs` — graceful-shutdown drain + flush.
- `app/crow-diskdb/src/config.rs` — `free_batch_enabled`,
  `free_flush_max_batch`.

**Dependencies**: R72 (free path, `DataGroupClient`).

**Non-goals**:

- **No timer-based flush.** The flush is triggered by batch size only,
  not by a periodic timer. This avoids the ghost-allocation window
  and the background-task complexity.
- **No cross-group batching.** Each flush is one `batch_write` per
  affected data group (frees within one disk-group batch together;
  frees across disk-groups are separate batch_writes).
- **Not in v1.** v1 ships with immediate free (R72). This requirement
  is a follow-up for high-free-throughput workloads.
