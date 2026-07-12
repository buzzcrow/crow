# CrowKV - Design: crowtree Persistence

Parent: [`design-crowtree.md`](design-crowtree.md)
Depends on: [`design-crowtree-core.md`](design-crowtree-core.md), [`design-async-io.md`](design-async-io.md)

This document specifies how crowtree pages reach durable media: the `PageStore`
backend abstraction (local file, raw block device, remote/RDMA), the on-disk page
format and alignment, snapshot, crash recovery, the internal-WAL decision, and
the C API surface used by the Rust FFI adapter.

## Table of Contents

- [1. PageStore Abstraction](#1-pagestore-abstraction)
- [2. Backends](#2-backends)
- [3. On-Disk Page Format (Zero-Copy Frame)](#3-on-disk-page-format-zero-copy-frame)
- [4. Buffer Pool (Frame Cache) and Dirty Tracking](#4-buffer-pool-frame-cache-and-dirty-tracking)
- [5. Snapshot](#5-snapshot)
- [5A. High-Performance Write/Durability Pipeline](#5a-high-performance-writedurability-pipeline)
- [6. Internal WAL Decision](#6-internal-wal-decision)
- [7. Recovery](#7-recovery)
- [8. C API](#8-c-api)
- [9. Snapshot Export / Import](#9-snapshot-export--import)

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
    virtual uint32_t iu_size() const = 0;             // 1B (mem/SCM) .. 4K/16K/64K (SSD)
    virtual uint64_t capacity_bytes() const = 0;
};
```

- **IU = Indivisible Unit.** The minimum atomically-writable size. Leaf base
  pages are padded to a multiple of it so a page write cannot tear (§3). The IU is
  backend-configurable and may be as small as **1 byte** for byte-addressable
  media (in-memory test store, SCM) or a flash page (SSD); see §2.
- The async signature matches the project I/O model
  (`design-async-io.md`): inside C++ the backend uses io_uring / O_DIRECT / RDMA
  verbs; the Rust FFI adapter bridges to tokio via `spawn_blocking` or a
  completion channel.
- A small **page-allocation map** (free IUs in the backing file/device) is
  persisted alongside the root pointer in the snapshot superblock (§5).

---

## 2. Backends

There are **two** `PageStore` implementations (decision D-Q3 in
`design-crowtree.md §7`): a file store and a block-device store. RDMA is **not** a
separate backend — a remote page region is just a block device reached over the
network, served by the same `BlockPageStore` with an RDMA medium driver.

| Backend | Medium | IU | Notes |
| --- | --- | --- | --- |
| `FilePageStore` | Local file on a filesystem | 4 KiB default | `pwrite`/`pread` or io_uring; `O_DIRECT` optional. Dev/default. |
| `BlockPageStore` | Raw block device, served by a pluggable medium driver: **SSD** (`O_DIRECT`), **SCM** (byte-addressable), **mem** (test), **RDMA-remote** (one-sided verbs to a remote region) | SSD: 16/64 KiB; SCM/mem: down to **1 byte**; RDMA: the remote region's IU | No filesystem; the allocation map owns the whole device/region. Byte-IU media skip page padding. |

Both implement the same `PageStore`. The backend is selected at `ct_open` via
options and lives entirely in C++ (it calls `aioss` `chunkio`/`diskio`/`rdmaio`
directly — no FFI on the I/O hot path, per D1).

**RDMA-remote specifics deferred** (same as any remote block device): it needs a
local page cache (§4) with pinning + epoch-gated eviction, and remote allocation
coordination. These land with the block backend in a later milestone; the v1
target is `FilePageStore` (and the in-memory `BlockPageStore` for tests).

---

## 3. On-Disk Page Format (Zero-Copy Frame)

**Decision (D-Q6): the in-memory and on-disk page representations are
identical.** A page is a fixed-size *frame*; loading is one `read_page` into a
frame with **no decode/copy**, and the B+tree reads, binary-searches, and
compares keys directly on the frame bytes. Persisting is the reverse: write the
frame bytes (optionally compressed) back. There is no separate "C++ object" form
for base pages — `LeafBase`/`InnerBase` become thin **views** over a frame.

### 3.1 Frame geometry

- A frame is `page_bytes` long (default **16 KiB**, configurable; a multiple of
  the backend IU). All frames in a pool are the same size, so the buffer pool
  (§4) is a flat array of frames with O(1) indexing.
- An entry (key + cell) larger than a frame's usable space spills to an
  **overflow chain** (a linked list of overflow frames whose body is raw value
  bytes); the leaf slot stores an overflow pointer instead of an inline cell.
  **Overflow value size policy (OQ4 resolved):** tiered with configurable
  thresholds — inline (≤ frame payload, zero-copy), small overflow (frame limit
  < v ≤ 1 MB, spill + `WARN` log), large overflow (1 MB < v ≤ 16 MB, spill +
  `WARN`), rejected (> 16 MB, `InvalidArgument` error + `ERROR` log, assumed
  bug). Defaults: `Options.max_overflow_value` = 1 MB (soft warn),
  `Options.max_value_hard_limit` = 16 MB (reject).

### 3.2 Slotted layout (leaf)

The slotted page is the standard "sorted slot directory growing forward +
records growing backward" so keys stay binary-searchable in place and inserts
don't rewrite the record bytes.

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

- The slot directory is **kept sorted by key**, so lookup is a binary search
  reading `key(i)` directly from the frame (`Slice` into the record region — zero
  copy). `cell(i)` likewise returns a `Slice` over the cell payload (§2 core doc).
- Insert: append the record at `free_hi`, splice a `Slot` into the sorted
  position (a small `memmove` of the slot array only, not the records). Delete:
  remove the slot (records become dead space). **Compaction** rewrites a fresh
  frame when dead space crosses a threshold — and, because L1 is single-writer
  COW (§5), most leaf mutation already produces a fresh frame.
- `data_bytes` (for split/merge thresholds, core §8) = `free_lo` + (page_bytes −
  `free_hi`) − header/trailer, i.e. live slot + record bytes.

### 3.3 Slotted layout (inner)

Inner pages hold `n+1` child PIDs and `n` separators, no cells:

```
+ FrameHeader (type = inner) +
+ child PIDs: u64[slot_count + 1]            (fixed array, grows forward) +
+ separator slot dir: Slot{rec_off,key_len}[slot_count] (sorted) +
+ ... free ... +
+ separator records: [sep bytes] ... (grows backward) +
+ Trailer {logical_len, crc32c} +
```

`ChildIndexFor(key)` is an `upper_bound` binary search over the separator slots,
reading separators directly from the frame.

### 3.4 Views (the "easy to list & compare" surface)

The frame is accessed only through non-owning, zero-copy views; this is the API
C++ call sites use to list and compare keys without any deserialization:

```cpp
class LeafFrameView {            // wraps a const uint8_t* frame
  uint16_t count() const;
  Slice    key(uint16_t i) const;     // Slice into the frame, no copy
  Slice    cell(uint16_t i) const;
  uint64_t right_sibling() const;
  int      Find(Slice key) const;     // binary search -> index or -1
  uint16_t LowerBound(Slice key) const;
};
class LeafFrameBuilder {         // writes into a frame being constructed
  bool TryAppendSorted(Slice key, Slice cell);   // false if it wouldn't fit
  void Finish(uint64_t self_pid, uint64_t right_sibling);  // header + CRC
};
// InnerFrameView / InnerFrameBuilder analogous (separators + child PIDs).
```

Key comparison is `Slice::compare` (memcmp) directly on frame bytes — identical
ordering in memory and on disk, so a frame written by one node is searchable
byte-for-byte by another.

### 3.5 Durability framing, alignment, compression

- **`logical_len` + CRC32C trailer.** The trailer's `logical_len` lets a reader
  ignore IU zero padding; CRC32C covers `[0, logical_len)`. CRC mismatch ⇒ torn
  page ⇒ recovery falls back to the previous snapshot (§7).
- **Alignment modes.** For aligned block devices the frame is already an IU
  multiple. For unaligned/file-debug media a frame may be written without padding;
  the on-disk record then carries an explicit length prefix (see §5 manifest).
- **Compression** is per-page and **on-disk only**: the frame body is optionally
  compressed with **LZ4** (a `flags.compressed` bit + algorithm id in the header),
  written as `[FrameHeader][compressed body][trailer]`, and **decompressed into a
  full frame on load** so in-memory access stays zero-copy and uniform. A page
  whose compressed size ≥ its raw size is stored uncompressed (flag clear). The
  CRC is computed over the *stored* (compressed) bytes so torn-write detection is
  independent of the codec. See §3.6.

### 3.6 Compression details (PT10)

- Algorithm id: `0 = none` (**default** — `Options.compression = kNone`), `1 =
  LZ4` (opt-in via `Options.compression = kLz4`), `2 = zstd` (reserved). Recorded
  per page so mixed pages decode correctly regardless of the option in force when
  written. **Off by default**: a caller opts into LZ4 explicitly; this keeps
  behavior deterministic across build environments (see below) instead of
  silently changing based on what the build machine has installed.
- Compress at **write_page** time (snapshot / eviction), decompress at
  **read_page** time into a freshly-acquired frame. The buffer pool only ever
  holds *uncompressed* frames.
- **LZ4 is a system dependency, not vendored.** `CMakeLists.txt` discovers a
  system LZ4 dev package (`find_path(lz4.h)` / `find_library(lz4)`, resolved via
  the `pixi` environment) and defines `CROWTREE_HAVE_LZ4` when found; the FFI
  `cc`-based Rust build does not link LZ4 at all. `compressor.cpp` degrades to an
  identity codec (`lz4_available() == false`, requesting `kLz4` silently stores
  raw) when the library isn't linked, so correctness never depends on LZ4 being
  present — only the compression ratio does. (An earlier draft of this doc
  planned to vendor LZ4 as a single-file dependency under
  `crowtree/third_party/lz4`; that was superseded by the system-package
  approach above and never implemented. Fully vendoring is tracked as a
  low-priority follow-up — see plan-tree.md Open Issues.)

---

## 4. Buffer Pool (Frame Cache) and Dirty Tracking

The buffer pool is crowtree's **only** holder of base-page memory. It is a
fixed-capacity arena of equal-size frames (§3), explicitly managed (not the OS
page cache) so memory is bounded and eviction is epoch-correct for RDMA/SCM/SSD
alike. This is the "4 GiB / 8 GiB btree cache" knob.

### 4.1 Structures (no per-page heap objects)

```cpp
struct Frame {                 // one cache-line-aligned slot in the arena
    uint8_t*           bytes;  // page_bytes window into the arena
    std::atomic<uint64_t> pid; // logical page this frame holds (kInvalidPID = empty)
    std::atomic<int32_t>  pin; // pin count (readers/writer); >0 = not evictable
    std::atomic<uint8_t>  ref; // clock second-chance bit
    bool                  dirty;
    PageAddr              durable_addr;  // where it was last written (or unset)
};

class BufferPool {
    explicit BufferPool(size_t capacity_bytes, uint32_t page_bytes, PageStore*);

    // Pin the frame holding `pid`, reading it from the store on a miss. The
    // returned handle keeps the frame resident until released (RAII unpin).
    FrameRef Pin(uint64_t pid, PageAddr addr);
    // Allocate a fresh empty frame for a brand-new page (COW target / split).
    FrameRef PinNew(uint64_t pid);
    void     MarkDirty(uint64_t pid);
    // Write all dirty frames to the store (snapshot); returns addrs written.
    Status   FlushDirty(std::vector<PageBacking>* out);

    Stats stats() const;       // hits, misses, evictions, dirty_frames, bytes
};
```

- The **page table** maps `pid -> frame index` (an open-addressing array keyed by
  pid, sized to the frame count; **not** `std::unordered_map`, to honour "no C++
  containers on the hot path" and keep lookups branch-light and cache-friendly).
- Frames themselves live in **one contiguous arena** (`capacity_bytes /
  page_bytes` frames). No per-page allocation; pin/unpin are atomic counter ops.

### 4.2 Pinning, eviction, and epoch safety

- A reader `Pin`s the leaf/inner frames along its descent (pin++), reads via the
  view, then releases (pin--). Pinned frames are never evicted, so a reader's
  `Slice`s into a frame stay valid for the read's duration — this replaces the
  per-page epoch retire for *cache residency* (the epoch manager still governs
  logical page *version* lifetime, §4.3).
- **Eviction** uses a **CLOCK (second-chance)** sweep over frames: skip pinned and
  dirty frames, clear `ref` on first pass, evict a clean unpinned frame whose
  `ref` is 0. A dirty frame is written back (or skipped until snapshot,
  configurable) before reuse.
- Capacity is a hard cap; if every frame is pinned/dirty the pool blocks new pins
  briefly and triggers an eager dirty flush. Sizing guidance: `capacity_bytes`
  default = `min(8 GiB, 25% RAM)`, configurable to e.g. 4 GiB.

### 4.3 COW + dirty tracking interaction

L1 stays single-writer COW (core §5/§8): to mutate page `pid`, the Flusher
`PinNew`s a fresh frame, builds the new page with a `FrameBuilder`, atomically
publishes it (mapping `Store(pid, new_frame_pid)` — see §4.4), marks it dirty,
and **epoch-retires the old frame's logical version**. Readers holding the old
frame keep their pin until done; the frame returns to the free pool once both
unpinned and past the epoch. Thus:

- **DirtyTracker** = the set of frames with `dirty == true`. Snapshot walks it
  (§5) instead of the whole tree, enabling **incremental** snapshots (only
  dirty frames are written) — the high-performance win over PT3's full rewrite.
- A dirty frame is never evicted until written by a snapshot or an explicit
  write-back.

### 4.4 Mapping table over frames

The mapping table (core §4) changes from `pid -> PageBase*` to `pid ->
frame-resident page`. Concretely the slot holds a tagged value: either a
**cached frame id** or an **unloaded `PageAddr`** (high bit tags "on disk, not
resident"). `Get(pid)` that finds an unloaded slot pins it in (demand load),
publishes the resident tag, and returns the frame. This keeps readers lock-free
on the hot (resident) path and makes cold reads a pin-miss.

### 4.5 Live-engine wiring (PT6c-5): two hard problems and the rollout

§4.1–4.4 describe the steady state; this subsection resolves the two issues that
make wiring the pool into the *live* engine the largest task, and fixes the
order of implementation so each step builds and keeps the full suite green.

**Problem A — reads are lock-free, the pool is mutex-guarded.** The core's
headline property (core §4; `stress_test` under TSan) is that a reader does an
atomic `mapping_.Get(pid)` + an `EpochManager::Guard`, with **no lock per read**.
The PT6b pool guards pin/unpin/evict with a mutex *and reuses a victim frame's
bytes immediately on eviction*. If a read had to `Pin` (take that mutex) on every
descent step, or if a victim frame's bytes were reused while a lock-free reader
still held `Slice`s into it, we would either regress the headline property or get
a use-after-free.

*Resolution — epoch-deferred frame reuse, not per-read pinning.* Reads stay
lock-free: `mapping_.Get(pid)` returns the resident base directly and the reader
reads its frame bytes under its existing epoch guard. Residency is made safe **by
the epoch manager, not by pins**: when the writer (single, under `write_mutex_`)
replaces or evicts a base, the frame's memory is **retired through the epoch
manager** and only returned to the pool's free list once no guard that could hold
a pointer into it remains. Pins are retained only for the *writer/snapshot*
(building, write-back) and for *demand-load installation*, never on the read hot
path. This matches §4.2's "eviction waits for the epoch" and keeps `Pin`'s mutex
off the reader. (A future lock-free pin with hazard/epoch counters is possible but
is **not** required for PT6c-5 and is out of scope.)

**Problem B — anonymous frames (no durable addr yet).** The pool's `Pin`/
write-back are addr-keyed, but the live tree creates pages (flush/consolidate/
split/merge) long before a snapshot assigns them a durable `PageAddr`. Such
frames are **anonymous + dirty**.

*Resolution.* A newly built page gets a frame via `PinNew(pid, addr=kNoAddr)`; it
is dirty and **pinned-resident until snapshot** (it is the working set being
flushed, so this is the intended behavior, consistent with §4.3 "a dirty frame is
never evicted until written"). Snapshot's `FlushDirty` assigns each dirty
frame a durable `addr` (append cursor, §5), records `pid→addr`, clears dirty, and
**drops the build pin** so the frame becomes evictable. Thus between snapshots
memory is bounded by *(working set since last snapshot) + (resident clean
cache)*; a write storm that outruns snapshot triggers an eager snapshot
(§5A back-pressure). Anonymous frames are never written to a scratch area in v1.

**Rollout (each step compiles + 117 tests green, ASan/TSan-clean):**

- **PT6c-5.1 — pool ownership of live base frames (no eviction yet).** Give
  `Crowtree` a `BufferPool` sized by a new `Options.buffer_pool_bytes`
  (default `min(8 GiB, 25% RAM)`) with `page_bytes = Options.frame_bytes`.
  `LeafBase`/`InnerBase` are built into a `PinNew` frame and hold their pin for
  their (heap) lifetime; their bytes live in the arena. Capacity is sized to not
  evict yet. Net effect: base-page bytes now live in the pool arena; behavior
  unchanged. *Test:* existing suite + a pool-stats assertion (resident grows).
- **PT6c-5.2 — epoch-deferred frame free on retire.** `RetirePage` of a
  frame-backed base returns its frame to the pool's free list **via the epoch
  manager** (deferred), instead of `delete`. Add `BufferPool::FreeFrameDeferred`.
  *Test:* `stress_test` under TSan/ASan; `epoch_test`-style residency-after-guard.
- **PT6c-5.3 — mapping slot tagging + demand load.** Slot becomes a tagged word
  (`PageBase*` | `unloaded PageAddr`). Recovery stores `pid→addr` tags instead of
  eagerly loading; `Get` of an unloaded slot demand-loads (CRC-checked) and
  publishes. *Test:* `lazy_load_test` (first access loads; reopen equals).
- **PT6c-5.4 — CLOCK eviction of clean resident bases.** Under capacity pressure,
  evict a clean unpinned base: retire its heap handle via epoch and re-tag its
  slot to `unloaded addr`. Anonymous/dirty frames are skipped (Problem B).
  *Test:* `eviction_test` (small pool, many pages, residency capped, values
  correct, re-load on access).

PT6c-5.1 and 5.2 carry no on-disk or API change and are pure internal plumbing;
5.3/5.4 enable lazy recovery and bounded memory and are the prerequisites for
PT6d (incremental snapshot) and PT7 (export/import).

### 4.6 Eviction safety: epoch reclamation, not hazard pointers or pin counts

The hard part of eviction is **not** picking a victim (CLOCK does that); it is
answering *"when is it safe to actually reuse a page's frame bytes?"* Readers are
lock-free and read `Slice`s that point **directly into a frame** (zero-copy, no
lock, no per-page refcount), so freeing or reusing a frame while a reader still
holds a pointer into it is a use-after-free. Three classic disciplines solve this;
crowtree deliberately picks the third.

1. **Pin counts / refcounting** (textbook buffer pool): `pin++` before touching a
   page, `pin--` after; evict only at `pin == 0`. Correct, but it puts an atomic
   RMW on a shared cacheline **on every page touch on the read hot path** — the
   exact regression Problem A forbids.
2. **Hazard pointers**: before dereferencing pointer `X`, a reader publishes `X`
   into a per-thread single-writer slot (store + fence) and re-validates; the
   reclaimer scans all threads' hazard slots before freeing `X`. Lock-free and
   **tightly** memory-bounded (only the exact in-flight pointers are protected,
   so one stalled reader pins at most its few hazards). Cost: the reader publishes
   *every* pointer it touches (store + acq/rel fence + re-validation), and the
   reclaimer does an `O(threads × hazards)` scan per free. It protects *individual
   pointers* — a root→…→leaf descent needs a hazard per level / hand-over-hand.
3. **Epoch-based reclamation (EBR / RCU-style)** — *crowtree's choice*
   (`EpochManager`, core §10). A reader brackets its **whole** operation in one
   `Guard` (`Enter()` = "active in epoch *e*"; `~Guard` = "left"); it never
   publishes which pointers it holds. The writer **retires** an unlinked page
   tagged with the current epoch; the page is freed only once **no guard open
   at-or-before that epoch remains**. Reader cost is one Enter/Exit *per
   operation*, not per pointer. Trade-off vs. hazard pointers: a single stalled
   guard blocks *all* reclamation (worst-case unbounded retained memory). crowtree
   accepts this because reads are short (one `Get`/`Scan`) and there is a single
   writer, so the active-epoch window is tiny.

**Eviction reuses the COW-retire path verbatim.** Dropping a resident page is
structurally identical to the copy-on-write replace already done on every
flush/split/merge — only the replacement value differs:

```
// COW replace (existing):   slot := new resident page;  retire(old)
// Eviction (PT6c-5.4):       slot := unloaded(addr,plen); retire(old)
mapping_.StoreUnloaded(pid, addr, plen);   // unlink resident, leave a reload tag
RetirePage(old_page);                      // epoch-deferred free
```

The evicted page's frame is owned by its `FrameStore`; `~FrameStore` (which
returns the frame to the pool free list) runs **only when the epoch reclaimer
frees the page**, i.e. after every overlapping reader guard has drained. So frame
reuse is epoch-safe *for free* — no read-path pins, no hazard scan. A later reader
that loads the stale tag simply demand-loads again (§4.4, `Resident`).

This is also why **the pool's own CLOCK must never evict a base frame**: the pool
knows nothing about mapping slots or epochs and would reuse bytes a lock-free
reader still points at (Problem A). Base frames are therefore pinned (`pin=1`) so
the pool's victim search skips them; their lifetime is governed by epoch-retire,
not by the pool's `pin==0` search. Eviction decisions are made by the **single
writer under `write_mutex_`** (one writer of slots ⇒ no slot races) and executed
as the retire + re-tag above.

**Precondition (why 5.4 follows PT6d).** Re-tagging a slot `unloaded(addr,plen)`
is only correct if the page's **durable bytes at `addr` equal its live frame** (so
the reload reconstructs the same page). A demand-loaded page already satisfies
this; a freshly built/consolidated page does not until a snapshot assigns it an
`addr` and writes *that exact frame*. Today's snapshot writes a *folded temp*
image (it re-consolidates leaves into a throwaway page), so even post-snapshot a
live page has no `addr` whose bytes match it. PT6d makes snapshot assign addrs
to the live frames and track dirty state; that is the precondition that unblocks
general eviction. (Demand-loaded clean pages could be evicted earlier, but a
writer-driven CLOCK over the full clean set is deferred to land atop PT6d's dirty
tracking rather than build a throwaway partial.)

---

## 5. Snapshot

> **Note:** The snapshot mechanism (full manifest + superblock A/B) is the
> current v1 implementation. Task #14 (plan-tree) will replace the full manifest
> with segment-level incremental persistence — see [`design-crowtree-mappingtable.md`](design-crowtree-mappingtable.md).
> The design below documents the current approach for reference.

`persist_snapshot()` makes the engine's materialized state durable up to
`last_applied_slot` and produces a `RootVersion` (core doc §9).

```
snapshot():
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
a crash before the swap leaves the previous snapshot intact; after the swap the
new state is live.

Snapshot cadence: by size (dirty bytes threshold), by slot count
(`snapshot_every_slots`), or on demand from the learner (e.g. before a snapshot
export or a clean shutdown).

### Deferred design tasks for this section

- **Path-copy COW snapshot implementation.** The core currently keeps
  `SnapshotView()` as a materialized consistent view because the live tree uses
  in-place mapping-slot replacement. The persistence milestone should design the
  true zero-copy model: allocate new PIDs along the modified path, publish a new
  immutable root/version, define durable page-address remapping at snapshot, and
  let `SnapshotView()` pin that version/root instead of materializing an O(N)
  copy under the write lock.

---

## 5A. High-Performance Write/Durability Pipeline

This section answers "how do we dump the mem layer (L0) to the disk layer
(durable L1) at high performance" end to end, now that L1 base pages are
buffer-pool frames (§3, §4). crowtree is **not** an LSM: there is no leveled
compaction. There are exactly two staged movements:

```
apply(slot,batch)            flush (Flusher, 1/tree)         snapshot (cadence)
   L0 MemTable   ───────────▶  L1 frames (buffer pool)  ───────────▶  PageStore
 (concurrent map)   batched,        single-writer COW          incremental, dirty
                  hot-key collapse   into frame builders        frames only + fsync
```

1. **L0 → L1 (flush).** The Flusher drains the contiguous-applied prefix
   (core §6.2), groups by leaf, and for each affected leaf builds a **new frame**
   via `LeafFrameBuilder` from the old frame's entries merged with the batch
   (highest-slot-wins), then atomically republishes the pid. No serialization
   step — the builder writes the final on-disk bytes directly. The old frame is
   epoch-retired. Splits/merges (core §8) build 1–2 new frames the same way.
   - *Why fast:* hot keys already collapsed in L0; one frame rebuild per touched
     leaf per flush (not per key); the rebuilt frame **is** the durable image, so
     snapshot never re-encodes.
   - *Delta option:* to cut rebuild cost for tiny batches we may keep a bounded
     in-frame delta region appended to a leaf frame (search overlays it, capped at
     `max_delta_len`); this is an optional optimization gated behind a flag and
     measured against plain COW-rebuild. Default v1 = COW-rebuild (simpler, and
     the buffer pool keeps the source frame resident so the rebuild is memcpy-fast).

2. **L1 → PageStore (snapshot).** Walks the **DirtyTracker** (§4.3), not the
   whole tree, and `write_page`s only dirty frames (optionally LZ4-compressed),
   remaps `pid -> PageAddr`, fsyncs, then commits the A/B superblock (§5). This is
   **incremental** — cost is proportional to dirtied pages since the last
   snapshot, not tree size. The PT3 full-rewrite path remains as the fallback
   for backends without stable per-page addresses.

3. **Durability barrier ordering.** dirty frames + manifest fsync **before** the
   superblock write (the commit point), so a crash never exposes a superblock
   referencing a half-written frame (CRC also guards this).

Back-pressure: if L0 grows faster than the Flusher drains (e.g. slow device),
`apply` is throttled once L0 crosses a high-water mark; the buffer pool's dirty
ratio triggers an eager snapshot. These keep memory bounded under write storms.

---

## 6. Internal WAL Decision

**Decision: crowtree does NOT keep a redo WAL of data operations. Recovery is
snapshot + consensus replay** (Model A, consistent with
`design-state-machine.md §2.1` and `requirement.md §8`).

Rationale:

- The consensus layer already has a durable WAL of chosen entries
  (`design-wal.md`). Adding a second op-log in crowtree duplicates durability
  machinery and was explicitly dropped in the state-machine design.
- crowtree persists a **snapshot** (snapshot at `last_applied_slot`). On
  restart, slots `> last_applied_slot` are re-applied from the consensus WAL /
  re-learned via consensus recovery. So the worst-case redo work is bounded by
  the snapshot interval, not unbounded.

Consequence: a crash between snapshots loses only the in-memory deltas since the
last snapshot, which the consensus layer re-applies. The engine must therefore
report its restored `last_applied_slot` so the learner knows where to resume
(snapshot-gc doc §3).

A *structural* mini-journal (for in-progress split/merge) is unnecessary because
SMOs are writer-exclusive and only their consolidated result is ever flushed;
a crash mid-SMO simply reverts to the last snapshot.

> TODO-CONFIRM (later round): if snapshot cost proves too high to run often
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

The Rust FFI adapter talks to `libcrowtree` through this C ABI. All functions
are `noexcept`, return a status code, and use explicit `(ptr, len)` buffers.
Output buffers are either copied by Rust immediately or returned as opaque
owned handles freed by a `ct_free_*`.

> **This section is an illustrative sketch, not the authoritative signature
> list.** The real, up-to-date ABI is
> [`crowtree/include/crowtree/c_api.h`](../../../crowtree/include/crowtree/c_api.h)
> — consult it directly for exact signatures (e.g. `ct_apply` is split into
> `ct_apply_put`/`ct_apply_delete` + `ct_force_advance_slot` rather than one
> call with a `contiguous_slot` argument; `ct_set_gc_watermark` currently takes
> only `safe_slot`, not `(snapshot_slot, safe_slot)`; there is no `ct_gc_stats`
> output type yet — `ct_collect_garbage` just re-runs `snapshot()`). The
> **intended end-state contract** (dual GC watermark, richer GC stats) is
> still what's shown below; the gap to get there is tracked in this doc's
> parent plan (`plan-tree.md` § Open Issues).

```c
typedef struct ct_tree ct_tree;            // opaque tree handle
typedef struct ct_view ct_view;            // opaque pinned snapshot view
typedef struct ct_iter ct_iter;            // opaque iterator
typedef int32_t   ct_status;               // 0 = ok; negative = error code

ct_status ct_open(const ct_options* opts, ct_tree** out);
ct_status ct_close(ct_tree*);

// Write: encoded batch (the existing Batch wire format) at a slot, plus the
// learner's contiguous-applied frontier so the Flusher knows how far it may
// flush (NoOp/repair slots leave no entry; the frontier must come from the
// learner, not be inferred from MemTable contents — core doc §6.1).
ct_status ct_apply(ct_tree*, uint64_t slot, const uint8_t* batch, size_t len,
                   uint64_t contiguous_slot);
// Advance the contiguous frontier without a batch (e.g. after NoOp slots).
ct_status ct_advance_contiguous(ct_tree*, uint64_t contiguous_slot);

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
ct_status ct_snapshot(ct_tree*, uint64_t* out_last_applied_slot);
void      ct_set_gc_watermark(ct_tree*, uint64_t snapshot_slot, uint64_t safe_slot);
ct_status ct_collect_garbage(ct_tree*, ct_gc_stats* out);

// Snapshot transfer (streaming chunks).
ct_status ct_snapshot_export_begin(ct_tree*, ct_export** out);
ct_status ct_snapshot_export_next(ct_export*, ct_buf* chunk, int32_t* done);
void      ct_snapshot_export_end(ct_export*);
ct_status ct_snapshot_import(ct_tree*, const uint8_t* chunk, size_t len);
ct_status ct_snapshot_import_finish(ct_tree*);

ct_status ct_clear(ct_tree*);
void      ct_free_buf(ct_buf*);
```

`ct_options` carries: backend kind (file/block/rdma) + backend config (path /
device / remote endpoint), IU size, target page bytes, consolidation policy,
snapshot cadence, and compression choice.

---

## 9. Snapshot Export / Import

**Decision (D-Q7): export is a byte stream, with a thin "to a file" convenience
wrapper.** The streaming form is the primitive (it feeds the network
`SnapshotService` for new-member install, snapshot-gc doc §6, without ever
touching local disk); dumping to a `.ctsnap` file is just streaming into a file
writer. So the same code serves both "send to a peer" and "dump to a bin file".

### 9.1 Format

Two on-the-wire formats, selected at export-begin:

| Format | Layout | Use |
| --- | --- | --- |
| **Portable** (v1, default) | versioned header `{magic, format, slot}`, then key-sorted `(klen,key,slot,kind,vlen,value)` tuples in **fixed ≤1 MiB chunks**, end marker, whole-stream CRC32C | cross-engine parity (in-mem ↔ crowtree), durable archival; deterministic chunk boundaries ⇒ resumable |
| **Native** (later) | header + raw frame images + a remapped manifest | fast crowtree→crowtree transfer; skips tuple re-encode |

The portable format is deterministic and engine-independent, so an exported
`.ctsnap` re-imports identically on any backend/page size. The `slot` in the
header is the engine's `last_applied_slot` at export time (read-only metadata;
always the latest durable state).

### 9.2 API (C++ + the C ABI in §8)

```cpp
// Export: pins the current RootVersion, streams chunks; resumable.
class SnapshotExport {            // ct_snapshot_export_*
  Status NextChunk(std::string* out, bool* done);  // ≤ chunk_bytes
};
Status SnapshotExportBegin(Crowtree&, Format, std::unique_ptr<SnapshotExport>*);

// Convenience: dump a whole snapshot to a file (loops NextChunk).
Status SnapshotDumpToFile(Crowtree&, Format, const std::string& path);

// Import: builds a fresh tree in staging, verifies CRC, atomically swaps in.
class SnapshotImport {            // ct_snapshot_import*
  Status Feed(Slice chunk);
  Status Finish(uint64_t* out_slot);
};
Status SnapshotLoadFromFile(Crowtree&, const std::string& path);
```

### 9.3 Semantics

- **Export** pins the current `RootVersion` (core §9) and streams over the
  pinned immutable tree; the pin is released at end. Newer snapshots may
  supersede it meanwhile (refcount). The exported `slot` is the engine's
  `last_applied_slot` at export time — always the latest durable state.
- **Import** bulk-loads bottom-up into fully-consolidated immutable frames in a
  staging area (never touching the live tree), verifies the end-to-end CRC, then
  atomically swaps in a new `RootVersion` with `last_applied_slot` = the slot from
  the export stream; the old version is epoch-retired. Readers see the previous
  state until the swap.
- Resumability/throttling live **above** the engine (the snapshot module); the
  engine only guarantees deterministic, chunk-boundary-stable export and atomic
  import.
