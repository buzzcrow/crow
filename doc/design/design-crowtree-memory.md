# CrowKV - Design: crowtree Memory & Buffer Management

Parent: [`design-crowtree.md`](design-crowtree.md)
Depends on: [`design-crowtree-core.md`](design-crowtree-core.md) (cell, MemTable, read path), [`design-crowtree-persistence.md`](design-crowtree-persistence.md) (buffer pool, frame, RDMA)

This document specifies crowtree's **key/value memory ownership model**: the
`buffer` abstraction, the single-allocation zero-copy write pipeline (Rust API →
MemTable → B+tree frame), the zero-copy read path (borrowed views into resident
frames), the epoch guard that makes borrowed reads safe, and the future custom
memory pool / RDMA-pinned allocation.

The **buffer pool** (frame cache for L1 base pages) is a separate, already-designed
structure — see [`design-crowtree-persistence.md §4`](design-crowtree-persistence.md#4-buffer-pool-frame-cache-and-dirty-tracking).
This doc covers the *value/key* memory that flows through the write path before it
lands in a frame, and the *borrowed views* the read path hands back.

## Table of Contents

- [1. Goals](#1-goals)
- [2. The `buffer` Abstraction](#2-the-buffer-abstraction)
- [3. Write Pipeline (Single Allocation)](#3-write-pipeline-single-allocation)
- [4. Read Pipeline (Borrowed Views)](#4-read-pipeline-borrowed-views)
- [5. Rust FFI Interop](#5-rust-ffi-interop)
- [6. Future: Custom Memory Pool](#6-future-custom-memory-pool)
- [7. Future: RDMA-Pinned Buffers](#7-future-rdma-pinned-buffers)
- [8. Rollout](#8-rollout)

---

## 1. Goals

Today the write path copies key/value bytes **three times**: `encode_cell()` into a
`std::string`, `MemTable::upsert` stores the string, `flush()` drains into a
`vector`, and `LeafFrameBuilder` writes into the frame. At MemTable scale (a few
MiB per flush) this is not yet the bottleneck, but it forces a `std::string`
allocation per write and precludes a zero-copy path from the network buffer to the
tree.

**Goal: allocate key/value memory once, at the earliest point (the Rust/C API
boundary), then move it down to the MemTable and into the B+tree without copying.**
The only unavoidable copy is the final placement into the slotted frame layout
(that copy *is* page construction). On the read side, `get`/`scan` hand back a
**borrowed** view into the resident frame instead of a freshly-allocated
`std::string`; the caller copies only if it needs to outlive the read guard.

This also sets up two future capabilities without reworking call sites:

- A **custom memory pool** tuned to the workload's KV size distribution, replacing
  glibc `malloc` for value allocation.
- **RDMA-pinned** value/frame buffers, so the RDMA persistence engine transfers
  frame bytes directly with no bounce copy.

---

## 2. The `buffer` Abstraction

`buffer` (in `crowtree/include/crowtree/buffer.h`) is a move-only byte container
that is *either* owned (it frees on destruction) *or* borrowed (a non-owning view
into memory whose lifetime is guaranteed by something else — a resident frame under
an epoch guard).

```cpp
class buffer {
 public:
  enum class mode : uint8_t {
    kOwned,     // allocated (pool or malloc); frees on destruction
    kBorrowed,  // view into external memory (e.g. a B+tree frame); no free
  };

  // Owned allocation. `header_reserve` bytes are reserved at the front for cell
  // metadata (slot u64 + flags u8) so encode writes in place, no second alloc.
  static buffer alloc(size_t capacity, size_t header_reserve = 0);
  // Borrowed view over external bytes (read path; caller guarantees lifetime).
  static buffer wrap(const uint8_t* data, size_t len);
  // Take ownership of a raw pointer already allocated by the matching allocator.
  static buffer move_from(uint8_t* data, size_t len, size_t cap);

  buffer(buffer&&) noexcept;
  buffer& operator=(buffer&&) noexcept;
  buffer(const buffer&) = delete;             // no implicit copy
  buffer& operator=(const buffer&) = delete;
  buffer clone() const;                        // explicit deep copy (owned)

  uint8_t* data();
  const uint8_t* data() const;
  size_t size() const;
  size_t capacity() const;
  Slice slice() const;                         // {data(), size()} for view APIs

  uint8_t* header(size_t off);                 // into the reserved header region
  void set_size(size_t len);

  bool owned() const;
  mode ownership() const;

  // Byte-order comparison (memcmp semantics) so `buffer` can be a btree_map key.
  bool operator<(const buffer& o) const;
  bool operator==(const buffer& o) const;

  ~buffer();                                   // frees iff owned
};
```

Design rules:

- **Move-only.** A `buffer` is never implicitly copied; passing it down the write
  path is a `std::move`. Deep copies are explicit (`clone()`).
- **Two modes, one type.** The same type serves an owned write-path value and a
  borrowed read-path view. A borrowed `buffer` never frees.
- **Header reserve.** `alloc(cap, header_reserve)` over-allocates by
  `header_reserve` so the cell header (`[slot u64][flags u8]`, core §2) is written
  in the reserved prefix and the value bytes follow contiguously — the encoded cell
  is one contiguous buffer with no second allocation or copy. This folds the old
  `encode_cell()` allocation into the original value allocation.
- **Allocator seam.** `alloc()` routes through a single internal allocator hook.
  Step 1 = glibc `malloc`. Later = size-classed pool (§6) or RDMA-pinned (§7),
  with no call-site changes.
- **Comparable + move-only ⇒ valid `absl::btree_map` key/value (OQ2/OQ3).** The
  MemTable is `absl::btree_map<buffer, buffer>` (core §1, D-Q10). `operator<`
  gives the byte-order key ordering; the deleted copy ctor/assign + valid move
  ctor/assign let the B-tree relocate elements during node split/merge without
  ever copying. Insertion uses `try_emplace` / `emplace` to move both key and
  value in. `absl` is added as a full dependency (OQ2); static linking + LTO trim
  unreferenced template code so the binary stays small.

---

## 3. Write Pipeline (Single Allocation)

```
Rust API: user provides key: &[u8], value: &[u8]
  → allocate one buffer (value + reserved cell header)          ← the ONE allocation
  → C FFI: ct_apply_put(tree, slot, key_ptr,klen, val_ptr,vlen)
  → C++ wraps as buffer (move_from if Rust yields ownership; §5)
  → encode cell: write slot+flags into the reserved header       (no alloc, no copy)
  → MemTable::upsert(key_buf, cell_buf): std::move into the map   (no copy)
  → flush(): drain moves buffers out of the MemTable             (no copy)
  → LeafFrameBuilder copies bytes into the slotted frame layout  ← unavoidable
  → owned buffers freed after the frame is built
```

- **Before:** 3 copies (encode → MemTable → drain-vector → frame).
- **After:** 1 copy (frame construction — placing bytes at slotted offsets, which is
  not a redundant copy but the page format itself).
- MemTable key/value fields become `buffer` instead of `std::string`; `mem_entry`
  and `leaf_entry` carry `buffer`s moved end-to-end.
- Highest-slot-wins still collapses hot keys in the MemTable (core §6.1); a shadowed
  older `buffer` is freed on replacement.

The remaining frame copy can only be removed by making the MemTable itself
arena-backed (all entries in one contiguous block whose ownership transfers to the
frame builder); that is the §6 pool work and is deferred until profiling justifies
it.

---

## 4. Read Pipeline (Borrowed Views)

```
get(key):
  guard = epoch.enter()                       // keeps resident frames alive (core §10)
  L0 (MemTable) hit:
      value lives under the MemTable mutex → must COPY into an owned buffer
      (the mutex is released on return, so a borrow would dangle)
  L1 (B+tree) hit:
      return buffer::wrap(cell_value_ptr_in_frame, len)  // BORROWED, zero copy
      valid only while `guard` is held AND the frame stays resident
```

- **L1 reads are zero-copy:** the returned `buffer` borrows the cell's value bytes
  directly from the resident frame (`LeafFrameView::cell` already exposes a `Slice`
  into the frame — core §11 / persistence §3.4). **The epoch guard alone keeps the
  frame resident (OQ1 resolved).** The lifecycle chain: eviction
  (`evict_clean_leaves_locked`) does not free a page's frame directly — it
  re-tags the mapping slot unloaded and epoch-`retire()`s the `PageBase`, whose
  `FrameStore` destructor (which calls `release_frame`) only runs after the epoch
  reclaims it. A reader holding a `Guard` therefore keeps the `PageBase` alive →
  the frame's `pin` stays `> 0` → the BufferPool CLOCK sweep skips it. No separate
  pin needs to be bundled into the returned handle.
- **L0 reads must copy:** MemTable values live under a mutex that is released when
  `get` returns, so a borrow would dangle. L0 hits return an *owned* buffer (copied
  under the lock). This is the hard lifetime rule.
- **Caller contract:** a borrowed `buffer` is valid only while the caller holds the
  read guard. To retain a value past the guard, the caller calls `clone()` (owned
  copy) and releases the guard. The owning `get`/`multi_get`/`scan` that return
  `std::string` today become thin wrappers: zero-copy `get` + `clone` + release.
- This is the value-view work (formerly a separate plan item): `TreeValueView` is
  just a borrowed `buffer` + `slot`.

**Epoch ownership dependency.** Safe borrowed reads require the epoch manager to
outlive no longer than the tree and to be reachable from `get`. crowtree moves the
`EpochManager` from `CrowtreeEnv` into `Crowtree` (see `design-crowtree.md` D6,
core §10): the tree owns its epoch, buffer pool, and page lifecycle, so a borrowed
`buffer` + `Guard` is entirely tree-scoped.

---

## 5. Rust FFI Interop

The Rust API layer allocates the value memory. Two options, sequenced:

**Option A — copy at the boundary (step 1).** Rust passes `*const u8` + len; C++
`buffer::alloc()`s and copies once at the FFI boundary. Simpler; one copy remains at
the boundary but the *internal* pipeline is already zero-copy (§3). This is the
current ownership model with `buffer` replacing `std::string`.

**Option B — shared allocator, end-to-end zero-copy (step 2).** Rust allocates
through an allocator shared with C++ (a C API `ct_alloc`/`ct_free`, or a common
`jemalloc`), then *yields ownership* across the FFI; C++ wraps it with
`buffer::move_from()`. No copy from the network buffer all the way into the frame
build. Requires a clear ownership-transfer contract at the C ABI (who frees, and
when the tree has consumed the bytes into a frame).

The C API (`design-crowtree-persistence.md §8`) gains, for step 2, an allocation
pair so Rust and C++ share one heap:

```c
uint8_t* ct_alloc(size_t capacity, size_t header_reserve);  // owned by caller until yielded
void     ct_free(uint8_t* p);                               // if never yielded to a tree
```

Ownership rule (extends `design-crowtree.md §5`): a buffer *yielded* to `ct_apply_*`
is owned by the tree and freed by it once flushed into a frame; a buffer *borrowed*
into a call (not yielded) follows the existing "borrowed for the call's duration"
rule.

---

## 6. Future: Custom Memory Pool

Replace the glibc `malloc` behind `buffer::alloc()` with a size-classed pool:

- **Profile first.** Measure the real workload KV size distribution (e.g. 90% of
  values `< 256 B`). The pool's size classes are chosen from that histogram.
- **Size-classed slabs** (e.g. 64 B / 128 B / 256 B / 1 KiB / 4 KiB): `alloc` picks
  the smallest class ≥ request → O(1), no fragmentation, no per-alloc `malloc`
  syscall pressure.
- **MemTable arena.** All buffers ingested into one MemTable generation are carved
  from a single arena; drain transfers the arena to the flusher and frees it whole
  after the frame build, removing the per-entry free and enabling the arena→frame
  bulk copy. This ties into MemTable double-buffering (`design-crowtree-core.md`
  §6, plan #3).

The pool is an allocator behind the existing `buffer` seam; call sites do not
change.

---

## 7. Future: RDMA-Pinned Buffers

When the RDMA persistence backend lands (one block-device `PageStore`,
`design-crowtree.md` D-Q3):

- `buffer::alloc()` (and the frame arena) allocate **RDMA-registered (pinned)**
  memory.
- Flushing a page to remote storage RDMA-writes the frame bytes directly — no copy
  into a separate registered bounce buffer, because the frame already lives in
  pinned memory.
- Value buffers that flow into a frame are already pinned, so no extra registration
  on the write path.

This is why the buffer/allocator seam exists now: RDMA pinning becomes an allocator
choice, not a pipeline rewrite.

---

## 8. Rollout

1. **B1 — `buffer.h` + owned/borrowed modes (glibc malloc), move semantics, header
   reserve.** Unit-tested in isolation.
2. **B2 — internal write path on `buffer`:** `cell.h` encode, `MemTable`,
   `mem_entry`/`leaf_entry`, `flush`/`drain_up_to`, `LeafFrameBuilder` input. All
   internal; C API still copies at the boundary (Option A). Sequence with #9
   (`absl::btree_map` MemTable) since both touch MemTable storage.
3. **B3 — zero-copy read `get`/`scan`** returning borrowed `buffer` + guard.
   Depends on epoch-in-tree (D6, core §10). Owning variants become wrappers.
4. **B4 — Rust FFI Option B** (shared allocator, ownership yield). Sequence with the
   FFI migration.
5. **B5 (future) — size-classed pool** behind the seam, after profiling.
6. **B6 (future) — RDMA-pinned allocation**, with the RDMA backend.
