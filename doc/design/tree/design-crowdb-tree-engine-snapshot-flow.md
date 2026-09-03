# Snapshot Flow Analysis

Snapshot makes the engine's materialized state durable on disk up to
`last_applied_slot`. The `tree.snapshot.l` metric measures the full
`Crowdbtree::snapshot()` call — CPU preparation plus synchronous I/O.

## Call chain

1. **Maintenance loop** (`group_maintenance.rs:234-257`) — decides
   whether to snapshot based on `slot_advance`, `time_elapsed`, or
   `flush_count` thresholds.
2. **Blocking wrapper** (`group_maintenance.rs:110-173`) —
   `spawn_blocking(|| engine_arc.persist_snapshot())`.
3. **Rust bridge** (`crowdb_tree_engine.rs:321-339`) — calls
   `handle().snapshot()` via FFI, logs `snapshot_ms` at INFO.
4. **C++ entry** (`persist.cpp:709-842`) — `Crowdbtree::snapshot()`.

## What `snapshot()` does

### A. Serialization gate

`acquire_snapshot_slot()` (`persist.cpp:697-701`) — atomic spin on
`snapshot_inflight_`; only one snapshot at a time.

### B. `prepare_snapshot_locked()` (`persist.cpp:367-663`) — CPU phase

Runs under `write_mutex_`. Measured by `tree.mem.snapshot.apply.l`.

1. **Allocator rebuild** (`persist.cpp:389-395`): read the best A/B
   anchor, collect live extents from the previous segment directory,
   build a `SpaceAllocator` of reusable gaps + append cursor.
2. **Pass 1 — fold deltas, queue dirty pages** (`persist.cpp:463-538`):
   - Scan every dirty mapping-table segment (not a full tree walk).
   - For each dirty leaf chain with `kBatchDelta`s: walk to the
     `kLeafBase`, fold the whole chain into a fresh consolidated leaf
     (`build_leaf_spilling_locked`), retire old delta/overflow pages.
   - For each page: `persist_one` (`persist.cpp:406-431`) — if dirty
     (`durable_addr == kNoAddr`), `encode_durable_page()` (compress +
     CRC), allocate on-disk address, push to `page_writes`. If clean,
     reuse existing address, increment `snapshot.page.write.cache.c`.
3. **Pass 2 — segment images + directory** (`persist.cpp:541-635`):
   - For each dirty segment: pack slot words, `encode_segment_image()`
     (`mapping_persist.cpp:73-96`), allocate address, push to
     `segment_writes`.
   - `encode_segment_directory()` (`mapping_persist.cpp:145-166`).
4. **Anchor encode** (`persist.cpp:638-655`): build `CommitAnchor` with
   `last_applied_slot`, `root_page_id`, `snapshot_seq`. Select A or B
   slot by parity.

### C. Synchronous I/O (`persist.cpp:733-802`)

1. `page_store->write_at()` for every dirty page blob (sequential).
2. `page_store->write_at()` for every dirty segment image.
3. `page_store->write_at()` for the segment directory.
4. **`page_store->sync()`** — first fsync/fdatasync barrier.
5. `page_store->write_at()` for the anchor (commit point).
6. **`page_store->sync()`** — second fsync/fdatasync barrier.

### D. Commit bookkeeping (`persist.cpp:665-695`)

Re-take `write_mutex_`. Update `durable_addr` / `durable_plen` /
`generation` for each page/segment. Publish new `version_`, increment
`snapshot_total_`.

### E. Block compaction (`persist.cpp:810-830`)

If `BlockPageStore`: delete blocks empty in two consecutive snapshots.

## Metric observation points

| Metric | Start | End | What |
|--------|-------|-----|------|
| `tree.snapshot.l` | `persist.cpp:714` | `persist.cpp:836-839` | Full wall time |
| `tree.mem.snapshot.apply.l` | `persist.cpp:719` | `persist.cpp:722-725` | `prepare_snapshot_locked` under `write_mutex_` |
| `tree.mem.snapshot.page.write.io.l` | `persist.cpp:734` | `persist.cpp:736-739` | Each `write_at` per page |
| `tree.mem.fsync.l` | `persist.cpp:764` / `781` | `767-769` / `784-786` | Two `sync()` calls |
| `tree.mem.snapshot.page.write.bw` | `persist.cpp:741-742` | — | Bytes per page write |
| `tree.mem.snapshot.meta.write.bw` | `persist.cpp:794-801` | — | Segment + directory + anchor bytes |
| `tree.snapshot.pages.c` | `persist.cpp:536-537` | — | Dirty page count |

Registration: `crowdb-tree.cpp:3686-3714`.

## Bottleneck analysis (2026-09-01 bench)

Observed: `tree.snapshot.l` = 2027ms. Sub-phase breakdown from metrics:

| Snapshot | `snapshot.apply.l` | `page.write.io.l` (avg/max) | `fsync.l` | pages |
|----------|-------------------:|-----------------------------:|----------:|------:|
| seq=1 (slot=9) | 532us | 67us / 67us | 0us | 1 |
| seq=1 (slot=36263) | 733ms | 388us / 129ms | 0us | 1045 |
| seq=2 (slot=159321) | 1309ms | 79us / 257us | 0us | 4167 |
| seq=3 (slot=239344) | 1304ms | 146us / 432ms | 0us | 4145 |
| seq=4 (slot=239349) | 1526us | 8us / 8us | 0us | 1 |

Rust log wall times: 0ms, 1141ms, 2029ms, 1914ms, 1ms.

### Primary bottleneck: `snapshot.apply.l` (CPU phase)

The `prepare_snapshot_locked` phase dominates: 733-1309ms for snapshots
with 1045-4167 dirty pages. This is the delta-chain folding +
`encode_durable_page` (LZ4 compression + CRC) for every dirty page,
all under `write_mutex_`.

The 2027ms total for seq=2 breaks down as:
- ~1309ms `snapshot.apply.l` (CPU: fold + compress + encode)
- ~432ms worst-case `page.write.io.l` (one slow pwrite, likely
  read-modify-write bounce on O_DIRECT misalignment)
- ~286ms remaining (other pwrites + segment/directory writes + overhead)

### Why `fsync.l` is 0ms

The bench uses `--mode mem` (in-memory page store). `sync()` is a no-op.
On `--mode file` or `--mode block`, fsync would dominate instead.

### Secondary: sequential pwrite with O_DIRECT bounce

`BlockPageStore` opens with `O_DIRECT` when `iu_size_ > 1`
(`block_page_store.cpp:126-142`). Misaligned writes trigger a
read-modify-write through a scratch buffer (`block_page_store.cpp:561-582`),
explaining the 432ms outlier on a single page write.

### `write_mutex_` blocking

`prepare_snapshot_locked` holds `write_mutex_` for the entire CPU phase
(733-1309ms). All `apply()` calls are blocked during this time,
contributing to the frozen queue buildup (20,026 full errors).

## Levers

- **Reduce `snapshot.apply.l`**: the delta folding + compression is the
  bottleneck. Options:
  - Parallelize `encode_durable_page` across pages (CPU-bound, no
    ordering constraint between independent pages).
  - Skip compression for small pages or when the compression ratio is
    poor (wastes CPU for no gain).
  - Pre-consolidate during flush instead of deferring to snapshot. If
    flush already folds delta chains, snapshot's Pass 1 has less work.
- **Overlap I/O with CPU**: write pages to disk while still encoding
  the next batch. The current design is fully sequential: encode all,
  then write all, then sync.
- **Reduce snapshot frequency**: increase `snapshot_slot_threshold` or
  `snapshot_time_threshold_ms` so fewer snapshots occur. Each snapshot
  with 4000+ pages is expensive; fewer snapshots = less total stall.
- **Move `prepare_snapshot_locked` off `write_mutex_`**: the delta
  folding and encoding don't need to block `apply()`. A copy-on-write
  snapshot of the dirty set could be prepared outside the lock, with
  only the final commit under the lock.
- **Use async I/O** (`io_uring` backend): overlap pwrite + fsync with
  the next maintenance tick instead of blocking the loop.
