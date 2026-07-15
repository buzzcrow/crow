<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CrowKV - Design: crowtree Durable Storage

Parent: [`design-crowtree.md`](design-crowtree.md)
Depends on: [`design-crowtree-engine.md`](design-crowtree-engine.md), [`design.md`](design.md) §12.1, [`design-state-machine.md`](design-state-machine.md), [`design-wal.md`](design-wal.md), [`design-reconfiguration.md`](design-reconfiguration.md)

This document specifies how crowtree pages reach durable media and how that
durability composes with the rest of CrowKV: the `PageStore` backend
abstraction, the on-disk page format and alignment, the buffer pool (frame
cache), snapshot + the internal-WAL decision + recovery, snapshot
export/import, the mapping table (the B+tree's PID indirection layer and its
own persistence), and the snapshot/GC flow integration with the learner and
consensus WAL.

## Table of Contents

- [1. PageStore Abstraction](#1-pagestore-abstraction)
- [2. Backends](#2-backends)
  - [2.1 TextPageStore — On-Disk Layout (Debug)](#21-textpagestore--on-disk-layout-debug)
  - [2.2 BlockPageStore — On-Disk Layout (Production)](#22-blockpagestore--on-disk-layout-production)
  - [2.3 Async I/O Architecture](#23-async-io-architecture)
  - [2.4 fsync Policy](#24-fsync-policy)
  - [2.5 Block Compaction](#25-block-compaction)
- [3. On-Disk Page Format (Zero-Copy Frame)](#3-on-disk-page-format-zero-copy-frame)
- [4. Buffer Pool (Frame Cache)](#4-buffer-pool-frame-cache)
- [5. Internal WAL Decision](#5-internal-wal-decision)
- [6. Snapshot, Recovery, and Export/Import](#6-snapshot-recovery-and-exportimport)
- [7. Interaction with Consensus WAL GC](#7-interaction-with-consensus-wal-gc)
- [8. Mapping Table](#8-mapping-table)
- [9. Garbage Collection](#9-garbage-collection)
- [10. New-Member Install Flow](#10-new-member-install-flow)

---

## 1. PageStore Abstraction

crowtree's tree logic references pages by `PID` and is unaware of the storage
medium. A `PageStore` maps a durable page slot to bytes — it is the only part
of crowtree that does I/O, and it is page-granular and **always asynchronous**
(`read_page`/`write_page` complete via callback/future, matching
[`design.md` §12.1](design.md#121-async-disk-io-substrate-moved-from-design-async-iomd)).
The upper layer always uses the async API — regardless of whether the
underlying platform has `io_uring` (Linux) or not (macOS/fallback). On
Linux, `BlockAsyncPageStore` + `Reactor` submit genuine `io_uring` SQEs;
on macOS or without liburing, the `*_async` methods fall back to
synchronous blocking I/O wrapped as immediately-ready completions, so the
upper layer has a **unified async interface** with no sync/async split.
No FFI on the I/O hot path.

**IU = Indivisible Unit.** The minimum atomically-writable size. Leaf base
pages are padded to a multiple of it so a page write cannot tear (§3). The IU
is backend-configurable and may be as small as **1 byte** for byte-addressable
media (in-memory test store, SCM) or a flash page (SSD); see §2. A small
**page-allocation map** (free IUs in the backing file/device) is persisted
alongside the root pointer in the commit anchor (§6).

---

## 2. Backends

There are **two** `PageStore` implementations — a text-encoded debug store
and a block-device store. RDMA is **not** a separate backend: a remote page
region is just a block device reached over the network, served by the same
`BlockPageStore` with an RDMA medium driver.

| Backend | Medium | IU | Notes |
| --- | --- | --- | --- |
| `TextPageStore` | Local filesystem directory | 1 byte (always) | Debug/test backend. Each page, anchor, and segment image is a separate human-readable text file. No compression. Implements `PageStore` with `iu_size()=1`; internally maps addresses to filenames. |
| `BlockPageStore` | Raw block device or regular file, served by a pluggable medium driver: **SSD** (`O_DIRECT`), **SCM** (byte-addressable), **mem** (test), **RDMA-remote** (one-sided verbs to a remote region) | SSD: 16/64 KiB; SCM/mem: down to **1 byte**; RDMA: the remote region's IU | Production backend. Array-of-blocks growth: a group owns multiple fixed-size block files (`{path}.blk-{NNNN}`), allocated on demand. No filesystem; the allocation map owns the whole device/region. Byte-IU media skip page padding. |

Both implement the same async `PageStore`. The backend is selected at `ct_open`
via `ct_options.backend` (0 = text debug, 1 = block). When `path` is
null/empty, an in-memory `BlockPageStore` with IU=1 is used (test path).

### 2.3 Async I/O Architecture

`BlockPageStore` has an async twin, `BlockAsyncPageStore`, that submits all
I/O through a `Reactor` (io_uring event loop). The async path is used by
`ct_get_async`, `ct_flush_async`, and `ct_snapshot_async`.

| Platform | Mechanism | Status |
| --- | --- | --- |
| Linux (liburing) | `BlockAsyncPageStore` + `Reactor` (io_uring SQE/CQE) | Production |
| macOS / no liburing | Synchronous fallback (blocking `pwrite`/`pread`/`fdatasync` wrapped as immediately-ready completions) | Dev |

**Linux (liburing)**: `BlockAsyncPageStore` maps global byte offsets to
per-extent fds via `BlockPageStore::fd_for_offset()`, then submits
`io_uring` SQEs through the `Reactor`. Writes spanning multiple extents are
split and chained; fsync chains across all dirty extent fds. Requires
`CROWTREE_HAVE_LIBURING` build flag.

**macOS / no liburing**: `CROWTREE_HAVE_LIBURING` is not defined. `ct_open`
does not wire a `Reactor` or `AsyncPageStore`. The `*_async` C API methods
fall back to completing synchronously (blocking I/O wrapped as an
immediately-ready completion). The upper-layer API is identical regardless
of platform.

**`FilePageStore`, `FileAsyncPageStore`, `IoEngine`, and `DirectIoEngine`
are removed.** The old sync `FilePageStore` was only used for testing;
`TextPageStore` now serves that role. The old `FileAsyncPageStore`
(single-file `io_uring`) is superseded by `BlockAsyncPageStore`
(array-of-blocks `io_uring`). The old `IoEngine`/`DirectIoEngine`
abstraction (blocking calls wrapped as async) is replaced by the
platform-level fallback described above.

### 2.4 fsync Policy

`fsync`/`fdatasync` is **configurable** via `ct_options.sync_mode`:

| Mode | Behavior | Use case |
| --- | --- | --- |
| `FullSync` (default) | `fdatasync` after every flush | Production durability |
| `SkipSync` | No fsync — writes are flushed by OS page cache only | Testing/benchmarks |
| `BatchSync` | fsync once per snapshot, not per flush | Throughput-sensitive testing |

On macOS, `fsync` costs ~3ms per call (stable). `SkipSync` or `BatchSync`
eliminates this overhead for test/CI runs. The same policy applies to WAL
file sync (see `design-wal.md`).

**RDMA-remote is deferred**, like any remote block device: it needs a local
page cache (§4) with pinning + epoch-gated eviction, and remote allocation
coordination. The v1 target is `TextPageStore` for debugging and
`BlockPageStore` (in-memory and SSD) for production/tests.

### 2.5 Block Compaction

After GC retires dead pages, their addresses become gaps in `SpaceAllocator`.
Over time, a block file (`.blk-{NNNN}`) may accumulate so many gaps that most
of its physical space is free. **Online block compaction** reclaims this space
without a separate compaction pass by controlling *where* new allocations land
during snapshot.

**Mechanism — gap filtering in `SpaceAllocator`.**

`SpaceAllocator` is rebuilt every snapshot from the committed anchor's live
extents (`build_allocator()` in `persist.cpp`). Its gap list is the complement
of live extents within `[region_base, file_size)`. The compaction mechanism is
simple: when `block_size > 0` (array-of-blocks mode), compute each block's free
ratio (`gap_bytes / block_size`) and **exclude gaps in blocks above a
threshold** (compile-time constant, 70%) from the allocator's gap list.

Filtered-out gaps are invisible to `alloc()`, so new writes never reuse space
in sparse blocks. They land in dense blocks' gaps or append past EOF (which
may trigger `allocate_new_block()`). Over successive snapshots, as dirty pages
are rewritten, sparse blocks lose their live pages.

**What gets relocated.**

Only **dirty pages** are rewritten during snapshot — clean pages keep their
existing durable address. So a single snapshot only relocates pages modified
since the last snapshot. Over multiple snapshots, as pages are touched and
rewritten, sparse blocks gradually drain. For high-churn workloads, this is
fast (most pages are dirty each snapshot). For low-churn workloads, blocks are
aturally dense — compaction rarely needed.

**Block deletion.**

After snapshot commit, compute per-block live page count from the
`PreparedSnapshot` (in-memory, no disk re-read). A block with zero live pages
is a deletion candidate. Deletion follows the **two-generation rule** (same
as old image cleanup, §6): a block is only deleted after it has zero live
pages in **both** the current snapshot and the previous one. This ensures a
crash mid-deletion falls back to the prior anchor, which still references the
block.

`BlockPageStore::delete_block(idx)` closes the fd, removes the `BlockExtent`
from `extents_`, and unlinks the `.blk-{NNNN}` file. The global address space
shrinks — future `write_at` / `read_at` calls never target the deleted block.
On reopen, `open_blocks()` scans the directory; the deleted file is gone and
its index is not re-opened.

**Crash safety.**

Identical to normal snapshot crash safety. If the process dies mid-snapshot:
- Old blocks still exist → old addresses still valid.
- New block has copied pages → new addresses valid.
- Recovery uses the anchor's snapshot to determine which addresses are live.

Block deletion is safe because it only happens after two consecutive snapshots
confirm zero live pages — the crash fallback anchor always references the
block.

**Cost.**

- Zero additional I/O beyond what snapshot already does. The page would be
  written anyway — we just choose a different destination address.
- O(gaps) for per-block free ratio computation during `build_allocator()`.
- O(live_pages) for per-block live page counting after commit.

**Scope.**

Only applies to `BlockPageStore` in array-of-blocks mode (`open_blocks`).
Single-medium mode (`open`, `open_mem`) and `TextPageStore` have no multiple
blocks — `block_size()` returns 0, gap filtering is skipped, behavior is
unchanged.

**Future extension — explicit compaction.**

If disk space is urgent and natural drain is too slow, an explicit
`compact_blocks()` API can force-rewrite all live pages from sparse blocks
(not just dirty ones) in a single snapshot. This is higher I/O burst and more
complex. Deferred — the online mechanism handles the common case.

### 2.1 TextPageStore — On-Disk Layout (Debug)

`TextPageStore` writes a directory of human-readable text files, one per
durable object. The directory layout at `{path}/{store_id}-{partition_id}/`:

```
{path}/{store_id}-{partition_id}/
  manifest.ck       # addr → (type, filename) mapping (text, one line per entry)
  anchor-A.ck       # CommitAnchor A (text key=value format)
  anchor-B.ck       # CommitAnchor B (text key=value format)
  page-{addr}.ck    # One file per B-tree page blob (debug_codec annotated text)
  seg-{N}.ck        # One file per mapping-table segment image (text)
  segdir.ck         # Segment directory (text, one line per DirEntry)
```

All files use the `.ck` extension — the CrowKV-specific file suffix. The
filename prefix (`anchor-`, `page-`, `seg-`, `segdir`, `manifest`) distinguishes
the type; the `.ck` suffix identifies the file as CrowKV-owned. Editors open
`.ck` files as text by default.

**Manifest file**: lists `(addr, len, type, filename)` for every blob written.
On open, `TextPageStore` reads the manifest to reconstruct the addr→file
mapping. The manifest itself is human-readable text.

**Anchor text format** (mirrors WAL's text segment header):
```
CROW_CT_ANCHOR magic=0x41435443 format_version=2 snapshot_seq=123 root_page_id=42
last_applied_slot=99 next_page_id=100 segment_slots=1024 segdir_addr=4096
segdir_len=2048 segdir_crc=deadbeef anchor_crc=cafebabe
```

**Page text format**: uses existing `encode_frame_text()` / `decode_frame_text()`
from `debug_codec.h`. Each `page-{addr}` file contains the annotated frame text
with `crowtree-frame-text` header, `type leaf`/`type inner`/`type overflow`,
per-slot fields, and a raw hex line for exact reconstruction.

**Segment image text format**: `CROW_CT_SEGIMG` header with `seg_idx`,
`generation`, `slot_count`, `live_count`, followed by one line per slot word
(`slot[N] = (iu_index, iu_count)` or `empty`).

**Segment directory text format**: `CROW_CT_SEGDIR` header, followed by one
line per `DirEntry`: `seg_idx=N generation=G image_addr=A image_len=L
crc=C`.

**Limitations**: Compression is always `kNone` (text mode is for debugging).
The `debug_codec` operates on uncompressed frames, so compressed blobs cannot
round-trip through text.

### 2.2 BlockPageStore — On-Disk Layout (Production)

A group's storage is an **array of fixed-size block files**, not one
pre-allocated file. When the current block fills up, `allocate_new_block()`
creates the next file.

**Block file naming**: `{store_id}-{partition_id}.blk-{NNNN}` (e.g.,
`1-3.blk-0000`, `1-3.blk-0001`). `store_id` and `partition_id` are passed
via `ct_options`. Block files use the `.blk` extension (binary, fixed-size);
text/debug files use `.ck` (see §2.1).

**Internal state**:
```
struct BlockExtent {
    std::unique_ptr<FileMedium> medium;  // one FileMedium per block file
    uint64_t base_offset;   // global offset = block_idx * block_size
    uint64_t used;          // high-water mark within this block
    bool     dirty;
};
std::vector<BlockExtent> extents_;
```

**Address space**: The global address space is linear —
`global_off = block_idx * block_size + local_off`. `write_at(global_off, buf,
len)` maps to `(extent_idx, local_off)`, splitting writes that cross extent
boundaries. If `global_off + len > total_capacity`, `allocate_new_block()` is
called first.

**On-disk binary layout** (within each `.blk-*` file):
```
block file (block_size bytes):
+--------------------------------------------------+ 0
| [Anchor A slot]  (superblock_slot_bytes, IU-aligned) |
+--------------------------------------------------+ slot_bytes
| [Anchor B slot]  (superblock_slot_bytes, IU-aligned) |
+--------------------------------------------------+ 2 * slot_bytes
| [Page/Segment image region]                       |
|   ... page blobs, segment images, segment dir ... |
|   ... allocated by SpaceAllocator, may have gaps ..|
+--------------------------------------------------+ block_size
```

- **Anchor A/B**: Two IU-aligned slots at offsets 0 and `superblock_slot_bytes`.
  Binary `CommitAnchor` struct (10 fields + CRC32C), zero-padded to IU boundary.
  Alternating A/B by `snapshot_seq` parity.
- **Page blobs**: Durable page frames (optionally LZ4-compressed), written at
  addresses allocated by `SpaceAllocator`. Each blob is `[plen u32][payload]
  [crc32c u32]`.
- **Segment images**: `SegmentImageHeader` + packed slot words + CRC.
- **Segment directory**: `DirEntry[]` + header + CRC.
- **Gaps**: Free space between live extents. Reused by `SpaceAllocator` for
  future allocations.

**Recovery**: On `open()`, scan the directory for `{store_id}-{partition_id}.blk-*`
files, sort by index, open all. The anchor in block 0 determines which blocks
are live (via segment directory → segment images → page addresses).

**Sync**: `fdatasync`/`fsync` all dirty extents (tracked per-extent dirty flag).

**Garbage collection**: Dead pages mark gaps in `SpaceAllocator` but block
files are not deleted (deferred optimization — see plan Task 14, block
compaction design).

---

## 3. On-Disk Page Format (Zero-Copy Frame)

**Decision: the in-memory and on-disk page representations are identical.**
A page is a fixed-size *frame*; loading is one `read_page` into a frame with
**no decode/copy**, and the B+tree reads, binary-searches, and compares keys
directly on the frame bytes. Persisting is the reverse: write the frame bytes
(optionally compressed) back. There is no separate "C++ object" form for base
pages — leaf/inner pages become thin **views** over a frame, so the same
comparison (`memcmp`) orders keys identically in memory, on disk, and across
nodes.

### 3.1 Frame geometry

A frame is `page_bytes` long (default **16 KiB**, configurable; a multiple of
the backend IU) — all frames in a pool are the same size, so the buffer pool
(§4) is a flat array with O(1) indexing. An entry larger than a frame's usable
space spills to an **overflow chain**; the overflow policy is tiered:
inline (≤ frame payload, zero-copy), small overflow (frame limit < v ≤ 1 MB,
spill + warn), large overflow (1 MB < v ≤ 16 MB, spill + warn), rejected
(> 16 MB, reject as a likely bug). Defaults: `Options.max_overflow_value` =
1 MB, `Options.max_value_hard_limit` = 16 MB.

### 3.2 Slotted layout

The frame layout is the standard "sorted slot directory growing forward +
records growing backward" so keys stay binary-searchable in place and inserts
don't rewrite record bytes:

```
frame (page_bytes):
+--------------------------------------------------+ 0
| FrameHeader (fixed, 64 B)                        |  magic, type, version, flags,
|   slot_count u16, free_lo u16, free_hi u16,      |  self_pid, right_sibling,
|   logical_len u32, lsn/last_applied_slot u64,    |  page_bytes, crc32c (trailer)
|   self_pid u64, right_sibling u64 (leaf only)    |
+--------------------------------------------------+ 64
| Slot directory: Slot[slot_count]                 |  grows forward (free_lo)
|   Slot = { rec_off u16, key_len u16, cell_len u16 } (sorted by key)
+--------------------------------------------------+
| ... free space ...                               |
+--------------------------------------------------+ free_hi
| Records: [key bytes][cell bytes] ...             |  grows backward from end
+--------------------------------------------------+ page_bytes - trailer
| Trailer: { logical_len u32, crc32c u32 }         |  CRC over [0, logical_len)
+--------------------------------------------------+
```

Inner pages hold `n+1` child PIDs (fixed array) plus `n` separator keys (same
sorted slot-directory style, no cells) — `ChildIndexFor(key)` is an
`upper_bound` binary search over the separators, reading them directly from
the frame.

The slot directory is **kept sorted by key**, so lookup is a binary search
reading each key directly as a `Slice` into the frame (zero copy). Insert
appends the record at `free_hi` and splices a slot into sorted position (a
small `memmove` of the slot array only, not the records); delete removes the
slot (records become dead space, reclaimed by rewriting a fresh frame when
dead space crosses a threshold — and because L1 is single-writer COW, most
leaf mutation already produces a fresh frame anyway).

### 3.3 Durability framing, alignment, compression

- **`logical_len` + CRC32C trailer.** `logical_len` lets a reader ignore IU
  zero padding; CRC32C covers `[0, logical_len)`. A CRC mismatch means a torn
  page, and recovery falls back to the previous snapshot (§6).
- **Alignment.** `BlockPageStore` with `iu_size > 1` (e.g. 4096 for NVMe)
  writes frames as IU multiples with bounce-buffer read-modify-write for
  unaligned offsets. `TextPageStore` always uses IU=1 (byte-addressable, no
  alignment) since it writes human-readable text files. `BlockPageStore::open_mem`
  also uses IU=1 for in-memory/SCM.
- **Compression is per-page, on-disk only.** The frame body is optionally
  compressed with LZ4 (a flag bit + algorithm id in the header), decompressed
  into a full frame on load so in-memory access stays zero-copy and uniform.
  A page whose compressed size ≥ its raw size is stored uncompressed. CRC is
  computed over the *stored* bytes so torn-write detection is independent of
  the codec.

### 3.6 Compression details

`Options.compression` defaults to `kNone`; LZ4 is opt-in (`kLz4`) so behavior
is deterministic across build environments rather than depending on what the
build machine happens to have installed. The algorithm id is recorded per
page, so mixed pages decode correctly regardless of the option in force when
written. **LZ4 is a system dependency, not vendored:** `CMakeLists.txt`
discovers a system LZ4 dev package and defines `CROWTREE_HAVE_LZ4` when
found; the FFI `cc`-based Rust build does not link LZ4 at all, and the
compressor degrades to an identity codec when the library isn't linked — so
correctness never depends on LZ4 being present, only the compression ratio
does.

---

## 4. Buffer Pool (Frame Cache)

The buffer pool is crowtree's **only** holder of base-page memory: a
fixed-capacity arena of equal-size frames, explicitly managed (not the OS page
cache) so memory is bounded and eviction is epoch-correct for RDMA/SCM/SSD
alike. This is the "4 GiB / 8 GiB btree cache" knob (`capacity_bytes` default
`min(8 GiB, 25% RAM)`).

- **No per-page heap objects.** Frames live in one contiguous arena
  (`capacity_bytes / page_bytes` frames); pin/unpin are atomic counter ops.
  The page table (`pid -> frame index`) is an open-addressing array, not
  `std::unordered_map`, to keep the hot path branch-light and cache-friendly.
- **Pinning and eviction.** A reader pins the leaf/inner frames along its
  descent, reads via the view, then releases. Pinned frames are never
  evicted. Eviction uses a **CLOCK (second-chance)** sweep: skip pinned and
  dirty frames, clear the ref bit on the first pass, evict a clean unpinned
  frame whose ref bit is 0. A dirty frame is written back before reuse.
  Capacity is a hard cap; if every frame is pinned/dirty, new pins block
  briefly and an eager dirty flush triggers.
- **COW + dirty tracking.** L1 stays single-writer COW: to mutate page `pid`,
  the Flusher builds the new page in a fresh frame, atomically publishes it
  (mapping store), marks it dirty, and epoch-retires the old frame's logical
  version. The **dirty set** (frames with `dirty == true`) is what snapshot
  walks (§6) instead of the whole tree — this is what makes snapshots
  incremental (only dirty frames are written) rather than a full rewrite.
- **Anonymous frames.** A newly built page (flush/consolidate/split/merge)
  has no durable address yet — it is dirty and pinned-resident until
  snapshot assigns it one and clears the pin, becoming evictable. Between
  snapshots, memory is bounded by *(working set since last snapshot) +
  (resident clean cache)*; a write storm that outruns snapshot triggers an
  eager snapshot (back-pressure, §6).

### 4.1 Why epoch reclamation, not pin counts, governs eviction safety

Picking a CLOCK victim is the easy part; the hard part is *"when is it safe
to reuse a page's frame bytes?"* — readers are lock-free and hold `Slice`s
pointing directly into a frame with no per-page refcount, so this is
identical to the general reclamation problem already solved by
[`design-crowtree-engine.md §1.6`](design-crowtree-engine.md#16-epoch-based-reclamation).
crowtree deliberately reuses that mechanism rather than introducing a second
one: dropping a resident page is structurally the same as a COW replace
(`mapping.StoreUnloaded(pid, addr, len); retire(old_page)`), and the evicted
frame is returned to the pool's free list only when the epoch reclaimer frees
the page — i.e. after every overlapping reader guard has drained. So frame
reuse is epoch-safe for free, with no extra read-path pins and no hazard
scan. This is also why the pool's own CLOCK must never evict a base frame
directly: the pool knows nothing about mapping slots or epochs and would
reuse bytes a lock-free reader still points at. Base frames are therefore
pinned so the pool's victim search skips them; eviction is instead driven by
the single writer, which does the retire + re-tag above.

One precondition matters for correctness: re-tagging a slot `unloaded(addr,
len)` is only valid once the page's durable bytes at `addr` equal its live
frame exactly. A demand-loaded page already satisfies this; a freshly
built/consolidated page does not until a snapshot assigns it an address and
writes that exact frame — so general eviction of freshly-written pages is
gated on the snapshot pipeline (§6) having run at least once for that page.

---

## 5. Internal WAL Decision

**Decision: crowtree does NOT keep a redo WAL of data operations. Recovery is
snapshot + consensus replay**, consistent with `design-state-machine.md §2.1`
and `design.md §8`.

- The consensus layer already has a durable WAL of chosen entries
  ([`design-wal.md`](design-wal.md)). Adding a second op-log in crowtree
  duplicates durability machinery.
- crowtree persists a **snapshot** at `last_applied_slot`. On restart, slots
  `> last_applied_slot` are re-applied from the consensus WAL / re-learned via
  consensus recovery. Worst-case redo work is bounded by the snapshot
  interval, not unbounded.

A crash between snapshots loses only the in-memory deltas since the last
snapshot, which the consensus layer re-applies — the engine reports its
restored `last_applied_slot` so the learner knows where to resume (§6.2). A
structural mini-journal for in-progress split/merge is unnecessary because
SMOs are writer-exclusive and only their consolidated result is ever flushed;
a crash mid-SMO simply reverts to the last snapshot.

---

## 6. Snapshot, Recovery, and Export/Import

`persist_snapshot()` makes the engine's materialized state durable up to
`last_applied_slot` and produces a `RootVersion`
([`design-crowtree-engine.md §1.5`](design-crowtree-engine.md#15-versioned-root-consistent-snapshots)).
Two staged movements, no leveled compaction (crowtree is not an LSM):

```
apply(slot,batch)            flush (Flusher, 1/tree)         snapshot (cadence)
   L0 MemTable   ───────────▶  L1 frames (buffer pool)  ───────────▶  PageStore
 (concurrent map)   batched,        single-writer COW          incremental, dirty
                  hot-key collapse   into frame builders        frames only + fsync
```

1. **L0 → L1 (flush).** The Flusher builds a **new frame** per touched leaf
   directly from the old frame's entries merged with the batch
   (highest-slot-wins) — no serialization step, the builder writes the final
   on-disk bytes directly, and the old frame is epoch-retired. This is fast
   because hot keys already collapsed in L0, so it's one frame rebuild per
   touched leaf per flush (not per key), and the rebuilt frame **is** the
   durable image (snapshot never re-encodes it).
2. **L1 → PageStore (snapshot).** Walks the dirty set (§4), not the whole
   tree, and writes only dirty frames (optionally compressed), remaps
   `pid -> PageAddr`, fsyncs, then commits the anchor (below). Cost is
   proportional to pages dirtied since the last snapshot, not tree size.
3. **Durability barrier ordering.** Dirty frames + directory fsync **before**
   the commit write — a crash never exposes a commit referencing a
   half-written frame (CRC also guards this).

**Back-pressure:** if L0 grows faster than the Flusher drains (e.g. a slow
device), `apply` throttles once L0 crosses a high-water mark, and the buffer
pool's dirty ratio triggers an eager snapshot — keeping memory bounded under
write storms.

**Recovery** is lazy: only the commit anchor and the mapping-table directory
(§8) are read eagerly; base pages are demand-loaded on first access, so
restart stays fast even for large trees. A page failing CRC on first read
indicates media corruption (the anchor that referenced it was already
committed) — the engine fails the node out of the group, and it rejoins via
snapshot install (§10) from a healthy peer. After recovery the learner sets
`contiguous_applied = last_applied_slot` and resumes consensus catch-up for
higher slots.

### 6.1 Snapshot Export / Import

**Decision: export is a byte stream, with a thin "to a file" convenience
wrapper.** The streaming form is the primitive — it feeds the network
snapshot-transfer service for new-member install (§10) without ever touching
local disk; dumping to a `.ctsnap` file is just streaming into a file writer.
The same code serves both "send to a peer" and "dump to a bin file."

Two on-the-wire formats, selected at export-begin:

| Format | Layout | Use |
| --- | --- | --- |
| **Portable** (default) | versioned header `{magic, format, slot}`, then key-sorted `(klen,key,slot,kind,vlen,value)` tuples in fixed ≤1 MiB chunks, end marker, whole-stream CRC32C | cross-engine parity (in-mem ↔ crowtree), durable archival; deterministic chunk boundaries ⇒ resumable |
| **Native** | header + raw frame images + a remapped manifest | fast crowtree→crowtree transfer; skips tuple re-encode |

The portable format is deterministic and engine-independent — an exported
`.ctsnap` re-imports identically on any backend/page size. The header's
`slot` is the engine's `last_applied_slot` at export time (there is no
`at_slot` parameter; historical snapshot export is not supported — a
snapshot always exports the current, latest durable state).

- **Export** pins the current `RootVersion` and streams over the pinned,
  immutable tree; the pin releases at the end. Newer snapshots may supersede
  it meanwhile (refcount keeps it alive).
- **Import** bulk-loads bottom-up into fully-consolidated immutable frames in
  a staging area (never touching the live tree), verifies the end-to-end
  CRC, then atomically swaps in a new `RootVersion` with `last_applied_slot`
  = the slot from the export stream header; the old version is
  epoch-retired. Readers see the previous state until the swap.
- Resumability/throttling live **above** the engine; the engine only
  guarantees deterministic, chunk-boundary-stable export and atomic import.

The exact C ABI (`ct_snapshot_export_begin/next/end`,
`ct_snapshot_import*`, and the rest of the surface — `ct_open`, `ct_apply`,
`ct_get`, `ct_scan`, `ct_snapshot_view`, `ct_set_gc_watermark`,
`ct_collect_garbage`, ...) is specified in
[`crowtree/include/crowtree/c_api.h`](../../../crowtree/include/crowtree/c_api.h)
— that header is the single source of truth for signatures; this document
only records the shape and rationale.

---

## 7. Interaction with Consensus WAL GC

crowtree consumes two slot watermarks the learner already tracks, plus its
own `last_applied_slot`:

| Watermark | Source | Meaning |
| --- | --- | --- |
| `last_applied_slot` | crowtree (per snapshot) | Highest slot whose effects are durable in the engine's last snapshot. |
| `snapshot_slot` | learner / replicator | State at this slot is durable on leader + ≥1 peer. |
| `safe_slot` | learner (`contiguous_applied` min across members) | Every learner has applied through here. |

`set_gc_watermark(snapshot_slot, safe_slot)` is called by the learner
whenever these advance; crowtree stores them and uses
`gc_slot = min(snapshot_slot, safe_slot)` to gate reclamation (§9), matching
`design-state-machine.md §7`.

The consensus WAL's own GC ([`design-wal.md`](design-wal.md)) uses this same
`gc_slot` rule: a WAL segment may be unlinked only when all its records have
`slot < min(last_applied_slot across members, safe_slot, snapshot_slot)`.
Because a restarting node resumes at its own `last_applied_slot` and
re-learns the rest, the consensus WAL must retain entries above the
**minimum** `last_applied_slot` of any member that might restart and replay
locally. This couples the two GC watermarks: crowtree advancing
`last_applied_slot` (via snapshots) is what eventually lets the consensus WAL
drop old segments.

**Restart flow:**

```
node restart:
    1. consensus WAL replay rebuilds ACCEPTOR state only
       (Promised / Accepted / VoteGranted)  — design-wal.md
    2. crowtree.recover() loads the commit anchor -> last_applied_slot = L
    3. learner sets contiguous_applied = L
    4. slots (L, max_chosen] are re-learned (heartbeat catch-up or new-leader
       bulk Phase 1) and re-applied to crowtree via apply(slot, batch)
```

Re-applying slots `<= L` is harmless: highest-slot-wins makes them no-ops. So
the consensus WAL need only retain entries `> min(last_applied_slot across
members)`.

---

## 8. Mapping Table

The mapping table is the B+tree's indirection layer: every structural
reference (root, sibling, inner child) is a logical `PID`, and the table
translates a `PID` to the page's current location — a resident in-memory
page or an unloaded durable address. It follows the Bw-Tree/LLAMA idea
(tree links are logical PIDs so moving a page between memory and disk changes
only one entry; page state is installed by replacing one entry) but
simplifies it for crowtree's constraints: **single-writer atomic store**
instead of a multi-writer CAS (the Flusher/reclaimer is the only writer;
readers only load), COW root versions, and no internal data WAL — so the
durable mapping table is a **snapshoted metadata image of `PID -> PageAddr`**,
not a second redo log.

**Decisions:**

| Decision | Rationale |
|----------|-----------|
| **No PID recycling** (`next_page_id_` is monotonic). | A reused PID could be seen by a stale reader as the new page — silent wrong data. Data integrity outweighs the memory saved. |
| **Segment recycling — yes.** | Slots are grouped into fixed-size segments (default 1024 slots/segment); when every slot in a segment is empty, its memory is freed. Bounds growth to live segments, not total split/merge count. Safe because PIDs are never reused, so a freed interior segment is never re-created. |
| **Sparse segments are acceptable.** | One live slot pins an ~8 KB segment. Migrating the slot to compact it would change the PID (referenced everywhere) — far worse. Segment size is tunable. |
| **Segment-level persistence, not a full manifest.** | Persist only *dirty* segments as self-describing images; the mapping table itself is the durable structure, avoiding an O(N) DFS-walk-and-write-everything on every snapshot (the old full-manifest approach cost ~120 MB of I/O per snapshot for a 100 GB tree even if few pages changed). |
| **Backend-neutral format.** | All I/O via `PageStore` (`allocate`/`write_page`/`read_page`/`free`/`flush`) — no fixed superblock/manifest region tied to the file backend, so block/RDMA work the same way. |
| **Tiny fixed commit anchor + a separate segment directory.** | The anchor is a small fixed A/B record (the atomic commit point); it points to a segment-directory image, so the commit stays atomic even for thousands of segments — the directory (cost ~195 KB for a 100 GB tree) decouples the possibly-large segment list from the tiny atomic anchor. |

**In-memory structure:** each `PID` maps to a 64-bit **packed slot word**,
the same encoding in memory and on disk — `0` = empty; `bit0=0` = resident
(an in-memory pointer, never persisted); `bit0=1` = unloaded (a durable
`(iu_index, iu_count)` descriptor). On disk a slot is only "empty" or a
tagged unloaded descriptor, so a segment image is literally the array of
packed words — recovery installs them with **zero decode**. `Get(pid)` is a
lock-free atomic load under an epoch guard; an unloaded slot demand-loads via
the buffer pool (§4), publishes the resident tag, and returns the frame.

**Segment recycling** runs in the epoch deleter, after all guards that could
see the page have drained: clear the slot, decrement the segment's live
count, and if it hits zero, atomically swap the segment pointer to null and
epoch-retire the old segment. A reader that loads a null segment or an empty
slot treats the PID as gone and retries from the root — never wrong data.

**On-disk format** (all records are `PageStore` allocations, self-describing,
CRC-protected): a **segment image** per dirty segment per snapshot (header +
the raw packed-word array, ~8 KB for 1024 slots); a **segment directory
image**, rewritten whenever any segment's generation changes, mapping
`seg_idx -> (generation, image_addr, image_len)`; and a **commit anchor**
(fixed size, A/B double-buffered at reserved IU 0/1) holding
`{snapshot_seq, root_pid, last_applied_slot, next_page_id, segment_slots,
segdir_addr/len/crc}` plus a CRC. The anchor is the commit point — its small
fixed size makes the A/B swap atomic on every backend, and a snapshot always
writes to the slot *not* named by the current highest-seq anchor, so a torn
write never destroys the last committed one.

**Binary layout** (BlockPageStore, see §2.2): The anchor, segment images,
and segment directory are stored as binary blobs at addresses allocated by
`SpaceAllocator` within `.blk-*` files. The anchor occupies IU slots 0 and 1
in block 0.

**Text layout** (TextPageStore, see §2.1): The anchor is stored as
`anchor-A.ck`/`anchor-B.ck` text files. Segment images are `seg-{N}.ck` text
files. The segment directory is a `segdir.ck` text file. A `manifest.ck` file
maps addresses to filenames. All are human-readable for debugging.

**Snapshot** integrates with §6's pipeline: write dirty frames and assign
durable addresses, serialize each dirty segment (pack `unloaded` or the
frame's new durable address per slot), rewrite the directory if any
generation changed, `flush()` (frames + images + directory durable), then
write the commit anchor and `flush()` again (the actual commit point), then
clear dirty bits and schedule old-image cleanup. **Recovery** picks the
highest-`snapshot_seq` anchor with a valid CRC, reads the segment directory
it points to, and for each entry reads that segment's image and installs its
packed words directly — no page bytes are read eagerly.

**Old image cleanup** uses a two-generation rule: after anchor `N` commits,
images referenced only by anchors `< N-1` are freed. This tolerates a crash
during cleanup, since the still-referenced generation is always intact.

**Concurrency invariants:** the Flusher (and the epoch reclaimer it drives)
are the only slot writers — no CAS needed for slot stores, readers only
atomic-load; segment install/retire is a single atomic store/CAS. Slot
clearing and segment retire happen in the epoch deleter, after all
overlapping reader guards drain.

---

## 9. Garbage Collection

`collect_garbage()` reclaims two kinds of space, both gated by
`gc_slot = min(snapshot_slot, safe_slot)` (§7):

1. **Tombstones.** A tombstone cell written at slot `t` is dropped during
   consolidation when `t < gc_slot`.
2. **Stale root versions.** A `RootVersion` with `refcount == 0` and
   `last_applied_slot < gc_slot` is retired, and pages reachable only from
   it are freed.

Triggers: periodic sweep (default every 5 min), a backend low-free-space
pressure signal (focused sweep / eager snapshot+GC), and post-snapshot
(eligible tombstones below the new `snapshot_slot` are swept). `GcStats`
reports tombstones dropped, versions retired, pages/bytes freed.

---

## 10. New-Member Install Flow

For a new or far-lagging member:

```
1. Leader picks a snapshot slot S (>= its last_applied_slot).
2. Leader engine: snapshot_export() streams chunks via the snapshot service.
   S = leader's last_applied_slot at export time (always the latest durable state).
3. New member engine: snapshot_import(stream) builds + swaps in the tree at S.
4. New member sets contiguous_applied = S, then streams consensus WAL
   (S+1, current_max_chosen] and applies via apply(slot, batch).
5. compare(view) against an existing learner -> empty diff (parity gate).
```

Resumability and throttling are handled by the snapshot module above the
engine; crowtree only provides deterministic, chunk-boundary-stable export
and atomic import (§6.1).

**Steady-state write + periodic durability, for reference:**

```
learner.learn(entry) -> engine.apply(slot, batch)         // in-memory deltas
... every snapshot_every_slots / dirty threshold ...
engine.persist_snapshot() -> last_applied_slot advances    // durable + new RootVersion
learner observes safe_slot/snapshot_slot advance -> engine.set_gc_watermark(...)
background -> engine.collect_garbage()                     // tombstones + stale versions
```

These flows require no change to the learner's public contract beyond the
async `KVEngine` surface (`design-crowtree.md §3`); `InMemKV` implements the
same methods (snapshot/GC are near-no-ops in memory) so tests exercise the
same code paths on both engines.
