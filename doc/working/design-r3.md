<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# R3 Design — Zero-Copy FFI Write Path

Upstream: `design/design-crowtree-engine.md` §2.2–§2.3 (buffer management,
FFI ownership), `doc/backlog/R3-zero-copy-ffi.md`.

## Problem

The current write path copies value bytes once at the FFI boundary.
`ct_apply_put` (`crowtree/src/c_api.cpp:518`) calls `encode_cell_buf`,
which does `buffer::alloc(vlen, kCellHeaderSize)` + `memcpy` of the
value (`crowtree/include/crowtree/cell.h:83-91`). The key is copied into
a `std::string` at the same boundary. Everything downstream is already
zero-copy (move into MemTable, move through flush, one final copy into
the slotted frame — page construction, not redundant).

For large values (>4 KiB) the boundary memcpy is avoidable if the caller
writes directly into crowtree-owned memory.

## Design

### Handle-based alloc-then-apply

Three new C API functions:

- `ct_alloc(key_len, val_len)` → `ct_write_handle*`
  Allocates a cell buffer via `buffer::alloc(val_len, kCellHeaderSize)`
  and a key `std::string(key_len, '\0')`. Returns an opaque handle
  wrapping both. The caller gets two writable pointers: `key_ptr` (raw
  key bytes) and `val_ptr` (value region at `cell.data() +
  kCellHeaderSize`). The cell header region `[0, kCellHeaderSize)` is
  not exposed — the C API hides `header_reserve`.

- `ct_apply_put_owned(tree, slot, handle)` → `ct_status`
  Writes the cell header (slot + `kFlagTombstone=0` for put) into
  `cell.data()[0..8]`, constructs `encoded_op{std::move(key),
  std::move(cell)}`, calls `apply_encoded`. Consumes (frees) the handle.
  Zero memcpy on the value; key is moved (pointer relocation for heap
  strings, inline copy for SBO — same as today).

- `ct_free_handle(handle)`
  Error-path cleanup: destroys the key string and cell buffer without
  applying. No-op if null.

### Handle struct (C++ internal, not exposed in C header)

```cpp
struct ct_write_handle {
    std::string key;    // pre-allocated, caller fills bytes
    buffer      cell;   // pre-allocated with kCellHeaderSize reserve
};
```

Only `ct_alloc`, `ct_apply_put_owned`, and `ct_free_handle` access this
struct. The C header declares `ct_write_handle` as an opaque type.

### Why two allocations (key + cell), not one

The key must be a `std::string` (MemTable is
`absl::btree_map<std::string, buffer>` — see §2.1 of the engine design
doc for why the key is not a `buffer`). The value must be a `buffer`
(with header reserve). These are different C++ types with different
allocators, so a single contiguous block would require manual slicing
and custom destructor logic — more complexity for no benefit. Two
allocations is clean and each type manages its own memory.

### SBO interaction

For small values (total ≤ `kInlineCap` = 24 B, i.e. 9-byte header + ≤15
B value), the cell buffer is inline — no malloc. The caller still writes
into `data() + kCellHeaderSize`, which points to inline bytes. This is
safe: the handle owns the buffer, and `ct_apply_put_owned` moves it
(relocating inline bytes). No special handling needed.

For small keys (≤ ~15 B depending on `std::string` SSO), the key string
is also inline. `std::move` copies inline bytes — same as today, no
regression.

### Delete support

Not needed. A delete cell carries no value (`encode_cell_buf` with
`OpKind::kDelete` drops the value). The existing `ct_apply_delete` path
already has no value copy — only the key copy, which is unavoidable
(`std::string` for the btree). No zero-copy benefit for deletes.

### Batch support

Out of scope for R3. A batch zero-copy path would need either multiple
handles or a batch-alloc API (`ct_alloc_batch(count, key_lens[],
val_lens[])`). The single-put handle proves the pattern; batch can
follow if profiling motivates it.

### Rust FFI wrapper

New safe Rust API in `crowtree-ffi`:

```rust
pub struct WriteHandle {
    ptr: *mut sys::ct_write_handle,
    consumed: bool,
}

impl WriteHandle {
    pub fn key_mut(&mut self) -> &mut [u8];
    pub fn value_mut(&mut self) -> &mut [u8];
    pub fn apply(self, slot: u64) -> Result<(), CtError>;
}

impl Drop for WriteHandle {
    // If not consumed, calls ct_free_handle (RAII safety).
}
```

`Crowtree::alloc_put(key_len, val_len) -> Result<WriteHandle, CtError>`
creates the handle. `WriteHandle::apply` calls `ct_apply_put_owned` and
marks `consumed = true` to prevent double-free.

`WriteHandle` is `!Send + !Sync` — the handle's buffers are not safe to
share across threads (the cell buffer's `data()` pointer is stable for
the handle's lifetime, but moving across threads would require the C++
side to be thread-aware, which it isn't for a pre-allocated handle).

### Engine integration (deferred)

The current `KVEngine::apply` in `crowtree_engine.rs` takes `&Batch`
(borrowed data already in Rust memory). The zero-copy handle only helps
when the caller can write directly into crowtree-owned memory from the
start (e.g. deserializing a Paxos value directly into a handle). That
integration touches the consensus/WAL deserialization path and is a
separate effort. R3 delivers the C API + Rust FFI wrapper; engine
integration is explicitly out of scope.

### ABI stability

The design doc §2.3 notes that Option B (shared allocator) requires
`header_reserve`'s layout to become a stable cross-FFI ABI contract.
R3 sidesteps this: the handle is opaque, `ct_alloc` returns a value
pointer into the pre-allocated cell, and only `ct_apply_put_owned`
knows about `kCellHeaderSize`. The header layout is never exposed
across the ABI boundary. If `kCellHeaderSize` changes in a future C++
version, the C API contract (opaque handle) is unaffected.

## Open questions

- **Threshold for zero-copy path**: Should `CrowtreeEngine::apply` use
  the handle path for large values and the slice path for small ones?
  Deferred to engine integration — not needed for R3's C API + FFI
  deliverable.

- **Overflow values**: Values exceeding `max_inline_value` spill to an
  overflow chain during flush. The handle allocates the inline cell
  buffer; overflow handling happens downstream (flush path, unchanged).
  No special handling needed in the handle API.
