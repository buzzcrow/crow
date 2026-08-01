<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Design: Zero-Copy Engine Apply (R30)

Parent: [`design-crowtree-engine.md`](../design/design-crowtree-engine.md) §2.2–2.3
Backlog: [`R30-zero-copy-engine-apply.md`](../backlog/R30-zero-copy-engine-apply.md)

## Problem

After R23, `Batch::decode` is zero-copy (`payload.slice(...)` → `Bytes`
ref-bumps). The one remaining apply-critical-path copy is inside
`encode_cell_buf`, called by `ct_apply_batch_slices`:

```cpp
// crowtree/include/crowtree/cell.h:79
buffer encode_cell_buf(uint64_t slot, OpKind kind, Slice value) {
    buffer b = buffer::alloc(vlen, kCellHeaderSize);   // malloc [9B][vlen]
    std::memcpy(b.data(), hdr, kCellHeaderSize);        // write header
    std::memcpy(b.data()+9, value.data(), vlen);        // ★ 64 KiB copy (hot path)
    return b;
}
```

For a 64 KiB value this is a 64 KiB `memcpy` on the consensus/apply
thread. A second copy happens later at flush (`frame_page.cpp:187`,
`memcpy` into the slotted frame) — unavoidable, off the critical path.

The R3 handle path (`ct_alloc` + `ct_apply_put_owned`) does not help:
the caller still copies the value from the packed payload `Bytes` slice
into the handle's writable pointer — relocating the same copy to Rust.

## Obstacle: the contiguous cell

The cell is stored as one contiguous `buffer` `[9-byte header][value]`
everywhere: `mem_entry.cell`, `leaf_entry.cell`, `BatchDelta.entries_`,
B+tree frames, snapshot I/O. `CellView` reads `slot()`/`flags()`/
`value()` by slicing this contiguous memory (`cell.h:113`). ~50 sites
construct `CellView` from a cell slice. The frame builder does
`memcpy(dst, cell.data(), cell.size())` assuming physical contiguity
(`frame_page.cpp:187`).

A `Bytes`-backed external buffer cannot satisfy this contiguity: the
value bytes live in the packed payload `Bytes`, and the 9-byte header
(slot+flags, known only at apply time) is not adjacent to them. So a
naive "external buffer for the cell" breaks `CellView` and the frame
builder.

## Approach: split cell inside the MemTable, materialize at the boundary

Keep the contiguous cell everywhere downstream. Split the cell **only
while it lives in the MemTable**, and materialize the contiguous form at
the memtable API boundary (`get` / `drain_up_to` / `snapshot`) — points
where a copy already exists, all off the apply critical path.

### 1. `buffer::mode::kExternal` (zero size overhead)

A new buffer mode that borrows value bytes from a Rust-owned `Bytes` and
calls back into Rust to drop the refcount when freed.

```cpp
enum class mode : uint8_t { kOwned, kBorrowed, kExternal };
```

`kExternal` holds `{ptr, len, owner, drop_fn}`. To avoid growing
`buffer` (used pervasively), overlap the drop-control fields with the
SBO `inbuf_` array via a union — external buffers never use inline
storage:

```cpp
union {
    std::array<uint8_t, kInlineCap> inbuf_;   // owned-inline (SBO)
    struct { void (*drop_fn)(void*); void* owner; } ext_;  // kExternal
};
```

`kInlineCap` (24 B) holds two pointers (16 B) with 8 B spare. Zero size
overhead vs. today. Construction:

```cpp
static buffer wrap_external(const uint8_t* data, size_t len,
                            void* owner, void(*drop_fn)(void*));
```

Destruction calls `drop_fn(owner)` (never frees `data` — it is borrowed
from the Rust `Bytes`). `data()`/`size()`/`slice()` work unchanged
(`heap_` holds `data`). Move transfers `ext_`; the moved-from buffer is
released without calling `drop_fn`. `clone()` materializes an owned
copy (deep copy of the borrowed bytes) — used by `snapshot()`.

### 2. Split `cell_entry` internal to the MemTable

The MemTable's internal map value changes from `buffer` to:

```cpp
struct cell_entry {
    uint64_t slot;    // always populated (CellView-decoded for contiguous,
                      //  from apply for split)
    uint8_t  flags;   // always populated
    buffer   cell;    // contiguous: full [header][value] (kOwned)
                      // split:      value-only, borrowed (kExternal)
};
```

Tag: `cell.ownership() == mode::kExternal` → split; else contiguous.
(The memtable never stores `kBorrowed` cells, so this is unambiguous.)

Materialization (used at the API boundary):

```cpp
buffer materialize() const {
    if (cell.ownership() != mode::kExternal) return cell.clone(); // already contiguous
    buffer b = buffer::alloc(cell.size(), kCellHeaderSize);
    write_header(b.data(), slot, flags);                          // 9 bytes
    std::memcpy(b.data()+kCellHeaderSize, cell.data(), cell.size()); // value copy (off hot path)
    return b;
}
```

New upsert overload (the apply path):

```cpp
bool upsert_external(Slice key, uint64_t slot, uint8_t flags, buffer&& value);
```

The existing `upsert(Slice, slot, buffer&&)` (contiguous, used by
snapshot import) stays; it decodes `slot`/`flags` from the cell via
`CellView` and stores `contiguous`. Highest-slot-wins now reads
`it->second.slot` directly (no `CellView` construction) — simpler than
today.

### 3. `apply_external` entry point

```cpp
// crowtree.h
struct external_op {
    std::string key;    // copied (small, SBO — same as today)
    uint8_t     flags;  // kPut / kFlagTombstone
    buffer      value;  // kExternal (borrowed) for Put; empty for Delete
};
Status apply_external(uint64_t slot, std::vector<external_op> ops);
```

Same semantics as `apply_encoded` (oversized-key rejection, slot
bookkeeping, intra-batch last-key-wins, `maybe_swap_active`) but builds
`cell_entry` directly — no `encode_cell_buf`, no value `memcpy`. Delete
ops store an empty value buffer with `flags = kFlagTombstone`.

### 4. Flush / L0-read / snapshot (unchanged downstream)

- **Flush** — `drain_up_to` returns `vector<mem_entry{key, cell, slot}>`
  with `cell` materialized to contiguous. The flush grouping /
  `leaf_entry` / `BatchDelta::build` / frame builder all see contiguous
  cells exactly as today. The value `memcpy` now happens here (Flusher
  thread, off critical path) instead of in `encode_cell_buf` (apply
  thread). <ref_snippet file="/Users/cj/cpp/crowkv/crowtree/src/crowtree.cpp" lines="900-959" />
- **L0 read** — `MemTable::get(key, std::string*)` materializes the
  contiguous cell into the output string. It already copies today
  (`memtable.cpp:106`); for a split cell it writes 9 B header + value —
  same one copy + 9 bytes. No regression.
- **Scan** — `MemTable::snapshot()` materializes via `materialize()`
  (today it does `kv.second.clone()` — same cost).
- **Snapshot I/O** — consumes `leaf_entry` (contiguous, post-drain).
  Unchanged.

### 5. FFI: `ct_apply_batch_external` + Rust `Bytes` ref handle

C API:

```c
struct ct_ext_op {
    const uint8_t* key;    size_t key_len;
    const uint8_t* value;  size_t value_len;  // NULL/0 for Delete
    uint8_t kind;                            // 0=Put, 1=Delete
    void*  bytes_ref;                        // opaque Rust handle (Put only)
    void (*drop_fn)(void*);                   // Rust drop callback
};
int ct_apply_batch_external(ct_tree* t, uint64_t slot,
                            const ct_ext_op* ops, uint64_t count);
```

Rust side (`crowtree/ffi/src/lib.rs`):

- `BytesRef` — a boxed `Arc<Bytes>`: `struct BytesRef(Bytes)`. Created
  via `Box::into_raw(Box::new(BytesRef(bytes_clone)))` → `*mut c_void`.
  One per Put op (one `Arc::clone` — atomic increment — per op). All ops
  in a batch borrow the same payload allocation; the payload is freed
  when the last op's `Arc` is dropped (when the memtable frees that
  external buffer at drain/overwrite).
- `ct_release_bytes(owner)` — `extern "C" fn`: `drop(Box::from_raw(...))`
  → drops the `Arc<Bytes>` clone.
- `Crowtree::apply_batch_external(slot, ops)` — builds `ct_ext_op`s from
  `&[BatchOp]`-like input, clones the value `Bytes` per op into handles,
  calls `ct_apply_batch_external`. Ownership of handles transfers to C++.

### 6. `CrowtreeEngine::apply` wiring (no trait change)

`KVEngine::apply(&self, slot, &Batch)` is unchanged. `Batch` already
holds `Bytes` keys/values (R23). `CrowtreeEngine::apply` builds
`ct_ext_op`s from the `Batch`'s `Bytes` slices (cloning each value
`Bytes` into a `BytesRef` handle) and calls `apply_batch_external`.
`InMemKV::apply` is unchanged (copies into `DashMap` regardless). The
learner's `apply_entry` is unchanged — it already calls
`engine.apply(slot, &batch)`. WAL replay goes through the same
`learner.learn` → `apply_entry` path, so it benefits automatically.

## Data flow after R30

```
Payload Bytes (one refcounted alloc)
  → Batch::decode: Bytes slices (zero copy, R23)
  → CrowtreeEngine::apply: per-op Arc<Bytes> clone → BytesRef handle
  → ct_apply_batch_external: C++ wraps value as kExternal buffer (NO memcpy)
  → apply_external → MemTable::upsert_external: cell_entry{slot, flags, value:kExternal}
  → [payload Bytes stays alive, pinned by external buffers' Arc clones]
Flush (off hot path):
  → drain_up_to: materialize → contiguous [header][value] buffer (value memcpy HERE)
  → leaf_entry → BatchDelta → frame builder (memcpy into frame, unavoidable)
```

Apply critical path: **zero value `memcpy`**. Flush: one value `memcpy`
(off critical path) + one frame `memcpy` (unavoidable). Down from two
value copies on/near the critical path to zero on it.

## Alternatives considered

- **R3 handle path for the consensus apply** — rejected: copy-equivalent
  to Option A (the caller copies value from `Bytes` into the handle).
  Only helps a direct API path CrowKV doesn't use.
- **Engine-wide split-cell representation** (change `CellView`,
  `leaf_entry`, delta, frames, snapshot I/O) — rejected: ~50 `CellView`
  sites, high regression risk across reads/flush/snapshot/recovery, for
  no extra benefit (the flush copy is unavoidable regardless). R30
  scopes the split to the MemTable only.
- **`Bytes`-backed `buffer` without splitting** — rejected: impossible
  without contiguous `[header][value]`; the header is not adjacent to
  the value in the payload, and the payload is immutable/shared after
  encode.
- **Threshold gating (Option A for small, Option B for large)** — not
  needed: the external path has uniform cost ≤ the copy path (no malloc
  for either; the external path adds one atomic `Arc::clone` per op,
  negligible vs. a memcpy). Small values (≤ SBO) are already
  zero-malloc in both paths.

## Acceptance

- Profiling: zero value `memcpy` on the apply path for values > 4 KiB
  (`samply` / `perf`). The only remaining value copy is at flush.
- No regression for small values (≤ 256 B): apply latency within ±5%.
- Existing consensus tests pass (batch atomicity, WAL recovery).
- New integration test: large-value batch (3 keys, 64 KiB each)
  round-trip through Paxos commit → apply → read.
- New test: external-buffer lifetime — payload `Bytes` not freed until
  the MemTable drains the entries borrowing from it (ASan clean under
  stress apply + flush).
