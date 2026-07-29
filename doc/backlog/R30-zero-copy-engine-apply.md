<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R30: Zero-copy engine apply — eliminate all write-path copies

**Problem**: The write path from Paxos commit to crowtree frame build still
has two copies even after R3's handle-based FFI:

1. **Deserialization copy** — `learn_chosen` decodes the Paxos payload into
   Rust `Vec<u8>` key/value buffers (`Batch` struct), then passes `&[u8]`
   borrows to `KVEngine::apply`.
2. **Boundary copy** — `CrowtreeEngine::apply` maps to `ct_apply_batch_slices`,
   which copies key+value into C++ internal `buffer`s (Option A, §2.3).

R3 delivered `ct_alloc` / `ct_apply_put_owned` / `ct_free_handle` and the
Rust `WriteHandle` wrapper, but the consensus layer does not use them. This
item wires the full path so data flows from Paxos payload bytes to crowtree
frame with zero intermediate copies.

**Approach**:

- **Paxos deserialization into handles** — Instead of materializing
  `Batch { Vec<u8> key, Vec<u8> value }`, the deserializer calls
  `Crowtree::alloc_put(key_len, val_len)` and writes decoded key/value bytes
  directly into the handle's writable pointers. The handle is then applied
  via `WriteHandle::apply(slot)`. No Rust-side `Vec<u8>` allocation for
  keys or values.

- **Batch handle API** — R3 only supports single-put handles. The consensus
  layer applies multi-key batches atomically. Either:
  - Extend the C API with `ct_alloc_batch(count)` returning a batch handle
    with per-entry alloc slots, plus `ct_apply_batch_owned` that consumes
    the whole batch; or
  - Keep single handles and add an atomic apply-variant that takes a
    `Vec<WriteHandle>` and applies all-or-nothing at the C++ level.

- **WAL replay path** — Same treatment: replay decoded entries directly
  into handles instead of materializing `Vec<u8>` intermediates.

- **`KVEngine::apply` signature** — Current trait method takes `&Batch`
  (borrowed data already in Rust memory). Needs a new variant or path
  that accepts owned handles, e.g. `apply_handles(slot, Vec<WriteHandle>)`
  or a streaming callback that writes into handle memory.

- **Fallback** — For small values (≤ SBO threshold, ~15 B), the handle
  path and the copy path have identical cost (inline storage, no malloc).
  The engine can use Option A for small values and Option B for large
  values, gated by a threshold, to avoid handle overhead on the fast path.

**Priority**: Medium — eliminates the last copies on the write critical
path; meaningful for large-value workloads (≥ 4 KiB).

**Complexity**: High — touches Paxos payload deserialization, WAL replay,
`KVEngine` trait, `CrowtreeEngine` adapter, and requires a batch-handle
C API extension. Cross-layer change with consensus semantics implications
(batch atomicity must be preserved).

**Depends on**: R3 (completed) — handle-based FFI C API + Rust wrapper.

**Files**:
- `crowkv/src/consensus/` — Paxos payload deserialization, `learn_chosen`
- `crowkv/src/kv/engine.rs` — `KVEngine` trait
- `crowkv/src/kv/crowtree_engine.rs` — `CrowtreeEngine::apply`
- `crowkv/src/wal/` — WAL replay path
- `crowtree/include/crowtree/c_api.h` — batch handle API (if needed)
- `crowtree/src/c_api.cpp` — batch handle implementation
- `crowtree/ffi/src/lib.rs` — Rust batch handle wrapper

**Acceptance**:
- Benchmark: zero `memcpy` calls on the apply path for values > 4 KiB
  (verify with profiling — `samply` on macOS, `perf` on Linux).
- No regression for small values (≤ 256 B): latency within ±5% of Option A.
- Existing consensus tests pass (batch atomicity, WAL recovery).
- New integration test: large-value batch (3 keys, 64 KiB each) round-trip
  through Paxos commit → apply → read.
