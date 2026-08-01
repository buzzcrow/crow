<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R30: Zero-copy engine apply — eliminate the apply-path value copy

**Problem**: The write path from Paxos commit to crowtree memtable still
copies the value once on the apply critical path, even after R23's
slice-based `ct_apply_batch_slices` and R3's handle-based FFI.

R23 already eliminated the deserialization copy: `Batch::decode` uses
`payload.slice(...)` per key/value, producing `Bytes` ref-bumps (O(1),
zero copy) — no `Vec<u8>` materialization. The one remaining copy is in
`encode_cell_buf` (called inside `ct_apply_batch_slices`): it allocates
a contiguous `[9-byte header][value]` buffer and `memcpy`s the value
bytes out of the caller's `Bytes` slice into it. For a 64 KiB value
that is a 64 KiB copy on the consensus/apply thread.

A second copy happens later at flush (`frame_page.cpp` writes the cell
into the slotted frame via `memcpy`). That copy is unavoidable (page
construction) and off the critical path (async Flusher thread). R30
targets only the apply-path copy.

**Why the R3 handle path does not help here**: `ct_alloc` +
`ct_apply_put_owned` lets a caller write key/value into crowtree-owned
memory then move it in. But the caller still must copy the value *from
the packed payload `Bytes` slice* into the handle's writable pointer —
relocating the same copy from C++ to Rust, not eliminating it. The copy
is unavoidable *as long as the cell must be one contiguous
`[header][value]` buffer*, because the value bytes live in a shared
packed `Bytes` that cannot be moved into a per-op C++ allocation.

**Approach — split-cell with an external (Bytes-borrowed) value buffer**:

The cell's contiguous `[header][value]` layout is load-bearing across
the engine (`CellView`, delta chains, frames, snapshot I/O — ~50 sites).
Rather than refactor the cell representation everywhere, R30 splits the
cell *only while it lives in the MemTable*, and materializes the
contiguous form at flush / L0-read / snapshot-export — points where a
copy already exists.

- **`buffer::mode::kExternal`** — a new buffer mode that borrows value
  bytes from a Rust-owned `Bytes`. Holds `{ptr, len, owner, drop_fn}`;
  `drop_fn(owner)` is a C function pointer that calls back into Rust to
  decrement the `Bytes` refcount when the buffer is freed. No malloc,
  no memcpy at construction. The payload `Bytes` stays alive (refcount
  pinned by every external buffer borrowing a slice of it) until the
  MemTable drains those entries at flush.

- **Split `mem_entry`** — while in the MemTable, a cell is stored as
  `{ slot: u64, flags: u8, value: buffer }` rather than one contiguous
  `buffer` cell. The 9-byte header is NOT stored as bytes — it is two
  integer fields (zero allocation). The value is a `kExternal` buffer
  borrowing from the payload `Bytes`. `CellView` is not used on the
  split form; the memtable reads `slot`/`flags` from the fields and the
  value from the external buffer's slice directly.

- **Materialization at flush** — when the Flusher drains entries into a
  batch delta, it materializes each split cell into one contiguous
  `[header][value]` `buffer` (write header from `slot`/`flags`, `memcpy`
  value from the external pointer). This copy happens on the async
  Flusher thread, not the apply critical path. The frame builder then
  consumes the contiguous cell unchanged. (Optimization: fold header +
  value straight into the frame, skipping the intermediate cell buffer.)

- **L0 read path** — `MemTable::get` already copies the cell into a
  `std::string` today. For a split cell it writes the 9-byte header from
  the fields + `memcpy`s the value from the external pointer — same one
  copy plus 9 bytes. No read regression. `MemTable::snapshot` (scan
  merge) similarly materializes on clone.

- **FFI** — new `ct_apply_batch_external(tree, slot, ops, count)` where
  each op carries `{key_ptr, key_len, value_ptr, value_len, kind,
  bytes_ref, drop_fn}`. `bytes_ref` is an opaque pointer to a Rust
  refcount handle (e.g. a heap-allocated `Arc<Bytes>`); `drop_fn`
  decrements it. The C++ side wraps each value as a `kExternal` buffer
  and each key as a copied `std::string` (keys are small; SBO). The
  Rust FFI layer owns the handle lifecycle: it clones the payload
  `Bytes` ref once per op, passes the handles to C++, and the C++
  `kExternal` buffers' destructors call `drop_fn` at flush/drain time.

- **`KVEngine::apply` path** — the learner's `apply_entry` decodes the
  payload `Batch` (zero-copy `Bytes` slices, unchanged) and, for
  `CrowtreeEngine`, calls the new external apply path instead of
  `apply_batch`. `InMemKV` is unaffected (it copies into its own
  `DashMap` regardless). A new `KVEngine` method or a
  `CrowtreeEngine`-specific path avoids changing the trait for engines
  that cannot benefit.

- **WAL replay** — same path (`learner.learn` → `apply_entry`), so it
  benefits automatically. No separate replay change.

- **Small-value fast path** — values ≤ SBO threshold (~15 B) are inline
  in the current design with no malloc. The external path has identical
  cost for small values (no malloc either — it borrows). No threshold
  gating needed; the external path is uniformly ≤ the copy path. The
  only overhead is the refcount handle clone per op (one atomic
  increment), negligible vs. a memcpy.

**Priority**: Medium — eliminates the last copy on the apply critical
path; meaningful for large-value workloads (≥ 4 KiB).

**Complexity**: High — touches the C++ `buffer` abstraction (new mode),
`mem_entry` shape, MemTable upsert/get/drain/snapshot, flush
materialization, L0 read, snapshot I/O, the C API, the Rust FFI layer,
and the learner apply path. Cross-layer change; the cell representation
change is scoped to the MemTable to limit blast radius, but every
`CellView` consumer of memtable output must be audited.

**Depends on**: R3 (completed) — handle-based FFI C API + Rust wrapper;
R23 (completed) — slice-based `ct_apply_batch_slices` + zero-copy
`Batch::decode`.

**Files**:
- `crowtree/include/crowtree/buffer.h` — `kExternal` mode + drop callback
- `crowtree/include/crowtree/memtable.h` — split `mem_entry`
- `crowtree/src/memtable.cpp` — upsert/get/drain/snapshot for split cells
- `crowtree/src/crowtree.cpp` — `apply_encoded` external path, flush
  materialization, L0 read, snapshot I/O audit
- `crowtree/src/frame_page.cpp` — consume materialized contiguous cell
  (unchanged, or folded header+value directly)
- `crowtree/include/crowtree/c_api.h` — `ct_apply_batch_external`
- `crowtree/src/c_api.cpp` — external batch apply implementation
- `crowtree/ffi/src/lib.rs` — Rust `Bytes` ref handle, drop callback,
  `apply_batch_external` wrapper
- `crowkv/src/kv/crowtree_engine.rs` — `KVEngine::apply` external path
- `crowkv/src/paxos/learner.rs` — `apply_entry` wiring
- `crowkv/src/kv/op.rs` — expose `Bytes` slices for the external path

**Acceptance**:
- Profiling: zero value `memcpy` on the apply path for values > 4 KiB
  (verify with `samply` on macOS, `perf` on Linux). The only remaining
  value copy is at flush (off the critical path).
- No regression for small values (≤ 256 B): apply latency within ±5%
  of the current `ct_apply_batch_slices` path.
- Existing consensus tests pass (batch atomicity, WAL recovery).
- New integration test: large-value batch (3 keys, 64 KiB each)
  round-trip through Paxos commit → apply → read.
- New test: external-buffer lifetime — a payload `Bytes` is not freed
  until the MemTable drains the entries borrowing from it (valgrind /
  ASan clean under a stress apply + flush cycle).
