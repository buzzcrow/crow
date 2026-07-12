# CrowKV - Design: crowtree Persistence

Parent: [`design-crowtree.md`](design-crowtree.md)
Depends on: [`design-crowtree-core.md`](design-crowtree-core.md), [`design-async-io.md`](design-async-io.md)

This document specifies how crowtree pages reach durable media: the `PageStore`
backend abstraction (local file, raw block device, remote/RDMA), the on-disk page
format and alignment, checkpoint, crash recovery, the internal-WAL decision, and
the C API surface used by the Rust FFI adapter.

## Table of Contents

- [1. PageStore Abstraction](#1-pagestore-abstraction)
- [2. Backends](#2-backends)
- [3. On-Disk Page Format](#3-on-disk-page-format)
- [4. Page Cache and Dirty Tracking](#4-page-cache-and-dirty-tracking)
- [5. Checkpoint](#5-checkpoint)
- [6. Internal WAL Decision](#6-internal-wal-decision)
- [7. Recovery](#7-recovery)
- [8. C API](#8-c-api)

---

## 1. PageStore Abstraction

crowtree's tree logic references pages by `PID` and is unaware of the storage
medium. A `PageStore` maps a durable page slot to bytes. It is the only part of
crowtree that does I/O, and it is page-granular and asynchronous.

```cpp
using PageAddr = uint64_t;   // backend-defined durable location id

struct IoResult { Status status; size_t bytes; };

class PageStore {
public:
    virtual ~PageStore() = default;

    // Page I/O. `buf` is IU-aligned and IU-sized (see §3). Completion is async;
    // the backend invokes the callback (or completes the future) on done.
    virtual void read_page (PageAddr a, uint8_t* buf, size_t len, IoCb cb) = 0;
    virtual void write_page(PageAddr a, const uint8_t* buf, size_t len, IoCb cb) = 0;

    // Space management for the page file/device.
    virtual PageAddr allocate(size_t iu_count) = 0;   // contiguous IUs
    virtual void     free(PageAddr a, size_t iu_count) = 0;

    // Durability barrier. Returns after all prior writes are persisted.
    virtual void flush(IoCb cb) = 0;

    // Backend geometry.
    virtual uint32_t iu_size() const = 0;             // 4K/16K/64K
    virtual uint64_t capacity_bytes() const = 0;
};
```

- **IU = Indivisible Unit.** The minimum atomically-writable size. Leaf base
  pages are padded to a multiple of it so a page write cannot tear (§3).
- The async signature matches the project I/O model
  (`design-async-io.md`): inside C++ the backend uses io_uring / O_DIRECT / RDMA
  verbs; the Rust FFI adapter bridges to tokio via `spawn_blocking` or a
  completion channel.
- A small **page-allocation map** (free IUs in the backing file/device) is
  persisted alongside the root pointer in the checkpoint superblock (§5).

---

## 2. Backends

| Backend | Medium | Notes |
| --- | --- | --- |
| `FilePageStore` | Local file on a filesystem | `pwrite`/`pread` or io_uring; `O_DIRECT` optional; IU defaults to 4 KiB. Dev/default. |
| `BlockDevicePageStore` | Raw block device | `O_DIRECT`, IU = SSD indivisible unit (16/64 KiB), no filesystem; allocation map owns the whole device. |
| `RdmaPageStore` | Remote page server | One-sided RDMA read/write to a remote page region; **requires a local page cache** (§4) with pinning + epoch-gated eviction; `flush` waits for remote completion + optional remote fsync. |

All three implement the same `PageStore`. The backend is selected at `ct_open`
via `page_tree_options` and lives entirely in C++ (it calls `aioss`
`chunkio`/`diskio`/`rdmaio` libraries directly — no FFI on the I/O hot path, per
decision D1).

RDMA specifics deferred to a later round (TODO-CONFIRM in
`design-crowtree.md §7`): cache eviction policy, pin accounting, and remote
allocation coordination.

---

## 3. On-Disk Page Format

A flushed page is self-describing and checksummed so recovery can validate it
without external metadata.

```
On-disk page (multiple of IU size):
+-------------------------------+ offset 0
| PageDiskHeader                |  type, version, self_pid, logical_len,
|                               |  last_applied_slot_hint, flags(compressed)
+-------------------------------+
| Page body                     |  consolidated leaf/inner base page (§3 core doc)
|                               |  (optionally LZ4/zstd compressed)
+-------------------------------+
| Zero padding to IU boundary   |
+-------------------------------+
| Trailer: logical_len, CRC32C  |  CRC covers header+body+padding
+-------------------------------+
```

- **Only base pages are flushed.** Delta chains are an in-memory, pre-consolidation
  optimization; a checkpoint consolidates them first (§5). Inner pages are flushed
  too (small).
- **`logical_len`** in the trailer lets the reader ignore IU zero padding (the
  alignment marker requirement carried over from the WAL block-alignment work).
- **CRC32C** mismatch ⇒ the page is treated as torn/corrupt; recovery falls back
  to the previous checkpoint (§7).
- Compression is a per-page option negotiated by the backend; the header flag
  records it so reads can decompress transparently.

---

## 4. Page Cache and Dirty Tracking

- In-memory base pages are the working set. A bounded **page cache** holds hot
  base pages; cold pages are read on demand via `read_page`. For `FilePageStore`
  and `BlockDevicePageStore` the OS page cache can back this; for `RdmaPageStore`
  crowtree owns an explicit cache because remote reads are expensive.
- The **`DirtyTracker`** (per tree) records PIDs whose in-memory base/chain
  differs from the last checkpoint. `apply`, consolidate, and split/merge mark
  dirty PIDs.
- Eviction of a clean cached page is epoch-gated (§core doc §10). A dirty page is
  not evicted until the next checkpoint flushes it.

---

## 5. Checkpoint

`persist_checkpoint()` makes the engine's materialized state durable up to
`last_applied_slot` and produces a `RootVersion` (core doc §9).

```
checkpoint():
    writer_lock:
        for pid in dirty.snapshot():
            consolidate(pid)                    // fold deltas into base pages
        version = freeze_root_version()         // immutable root + last_applied_slot
    // outside the lock: flush dirty base pages
    for page in version.reachable_dirty_pages():
        addr = page_store.allocate(iu_count(page))
        page_store.write_page(addr, serialize(page))   // header+body+pad+CRC
        remap_durable(page.pid -> addr)
    page_store.flush()
    write_superblock(version)                   // atomic root pointer swap
    dirty.clear_up_to(version)
    return version.last_applied_slot
```

The **superblock** is a small, double-buffered (A/B) record at a fixed location
holding: format version, current `root_pid`'s durable addr, `last_applied_slot`,
the page-allocation map root, and a CRC. The atomic A/B swap is the commit point:
a crash before the swap leaves the previous checkpoint intact; after the swap the
new state is live.

Checkpoint cadence: by size (dirty bytes threshold), by slot count
(`checkpoint_every_slots`), or on demand from the learner (e.g. before a snapshot
export or a clean shutdown).

---

## 6. Internal WAL Decision

**Decision: crowtree does NOT keep a redo WAL of data operations. Recovery is
checkpoint + consensus replay** (Model A, consistent with
`design-state-machine.md §2.1` and `requirement.md §8`).

Rationale:

- The consensus layer already has a durable WAL of chosen entries
  (`design-wal.md`). Adding a second op-log in crowtree duplicates durability
  machinery and was explicitly dropped in the state-machine design.
- crowtree persists a **checkpoint** (snapshot at `last_applied_slot`). On
  restart, slots `> last_applied_slot` are re-applied from the consensus WAL /
  re-learned via consensus recovery. So the worst-case redo work is bounded by
  the checkpoint interval, not unbounded.

Consequence: a crash between checkpoints loses only the in-memory deltas since the
last checkpoint, which the consensus layer re-applies. The engine must therefore
report its restored `last_applied_slot` so the learner knows where to resume
(snapshot-gc doc §3).

A *structural* mini-journal (for in-progress split/merge) is unnecessary because
SMOs are writer-exclusive and only their consolidated result is ever flushed;
a crash mid-SMO simply reverts to the last checkpoint.

> TODO-CONFIRM (later round): if checkpoint cost proves too high to run often
> enough, revisit adding a lightweight crowtree redo log for the delta tail only.

---

## 7. Recovery

```
recover(options) -> RestoredState:
    sb = read_superblock_best_valid()        // A/B, choose latest with valid CRC
    if none valid: return Empty{last_applied_slot = 0}
    load page-allocation map from sb
    root_pid = map_durable_to_pid(sb.root_addr)   // lazy: pages read on demand
    validate root reachable; CRC-check pages as they are first read
    return Restored{ root_pid, last_applied_slot = sb.last_applied_slot }
```

- Recovery is **lazy**: only the superblock and the page-allocation map are read
  eagerly; base pages are demand-loaded on first access. This keeps restart fast
  even for large trees.
- A page failing CRC on first read is a hard error for that page; since the
  superblock that referenced it was committed, this indicates media corruption →
  the engine fails the node out of the group (matching `design-state-machine.md
  §4.5`), and the node rejoins via snapshot install from a healthy peer.
- After recovery the learner sets `contiguous_applied = last_applied_slot` and
  resumes consensus catch-up for higher slots (snapshot-gc doc §3).

---

## 8. C API

The Rust FFI adapter (`CrowtreeEngine`) talks to `libcrowtree` through this C
ABI. All functions are `noexcept`, return a status code, and use explicit
`(ptr, len)` buffers. Output buffers are either copied by Rust immediately or
returned as opaque owned handles freed by a `ct_free_*`.

```c
typedef struct ct_tree ct_tree;            // opaque tree handle
typedef struct ct_view ct_view;            // opaque pinned snapshot view
typedef struct ct_iter ct_iter;            // opaque iterator
typedef int32_t   ct_status;               // 0 = ok; negative = error code

ct_status ct_open(const ct_options* opts, ct_tree** out);
ct_status ct_close(ct_tree*);

// Write: encoded batch (the existing Batch wire format) at a slot.
ct_status ct_apply(ct_tree*, uint64_t slot, const uint8_t* batch, size_t len);

// Point read: returns slot + value via out-params; ct_buf is freed by ct_free_buf.
ct_status ct_get(ct_tree*, const uint8_t* key, size_t klen,
                 uint64_t* out_slot, ct_buf* out_value, int32_t* found);

// Range scan: prefix + limit -> serialized (key,slot,value)* + truncated flag.
ct_status ct_scan(ct_tree*, const uint8_t* prefix, size_t plen, size_t limit,
                  ct_buf* out_entries, int32_t* truncated);

// Consistent view (compare / iter_all / export).
ct_status ct_snapshot_view(ct_tree*, ct_view** out);
ct_status ct_view_iter(ct_view*, ct_iter** out);
ct_status ct_iter_next(ct_iter*, ct_buf* key, uint64_t* slot, uint8_t* kind, ct_buf* value, int32_t* valid);
void      ct_view_release(ct_view*);

// Durability + GC.
uint64_t  ct_last_applied_slot(const ct_tree*);
ct_status ct_checkpoint(ct_tree*, uint64_t* out_last_applied_slot);
void      ct_set_gc_watermark(ct_tree*, uint64_t snapshot_slot, uint64_t safe_slot);
ct_status ct_collect_garbage(ct_tree*, ct_gc_stats* out);

// Snapshot transfer (streaming chunks).
ct_status ct_snapshot_export_begin(ct_tree*, uint64_t at_slot, ct_export** out);
ct_status ct_snapshot_export_next(ct_export*, ct_buf* chunk, int32_t* done);
void      ct_snapshot_export_end(ct_export*);
ct_status ct_snapshot_import(ct_tree*, const uint8_t* chunk, size_t len);
ct_status ct_snapshot_import_finish(ct_tree*);

ct_status ct_clear(ct_tree*);
void      ct_free_buf(ct_buf*);
```

`ct_options` carries: backend kind (file/block/rdma) + backend config (path /
device / remote endpoint), IU size, target page bytes, consolidation policy,
checkpoint cadence, and compression choice.
