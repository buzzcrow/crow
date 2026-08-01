<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Plan: Zero-Copy Engine Apply (R30)

Design: [`design-zero-copy-engine-apply.md`](design-zero-copy-engine-apply.md)

## Task breakdown

- [ ] **T1 — `buffer::kExternal` mode** (`buffer.h`)
  - Add `mode::kExternal`; union `ext_{drop_fn, owner}` with `inbuf_`.
  - `wrap_external(data, len, owner, drop_fn)` factory.
  - Destructor calls `drop_fn(owner)` for `kExternal`; move transfers
    `ext_` and releases the source without calling `drop_fn`.
  - `clone()` deep-copies the borrowed bytes into an owned buffer.
  - `data()`/`size()`/`slice()` unchanged (`heap_` = borrowed ptr).
  - Audit `adopt`/`free_if_owned`/`release_fields` for the new mode.
- [ ] **T2 — `cell_entry` + MemTable split support** (`memtable.h`,
      `memtable.cpp`)
  - Internal map value: `buffer` → `cell_entry{slot, flags, cell}`.
  - `cell_entry::materialize()` → contiguous `buffer`.
  - `upsert_external(Slice, slot, flags, buffer&& value)` — split path.
  - Existing `upsert(Slice, slot, buffer&&)` — contiguous path; decode
    `slot`/`flags` from `CellView`, store `cell_entry` with
    `kOwned` cell.
  - `upsert(Slice, slot, Slice)` — delegate to contiguous.
  - `get` / `drain_up_to` / `snapshot` — materialize at the boundary.
  - Highest-slot-wins reads `cell_entry.slot` directly.
  - `approx_bytes` / `slot_range` accounting for both forms.
- [ ] **T3 — `apply_external` engine entry point** (`crowtree.h`,
      `crowtree.cpp`)
  - `struct external_op { std::string key; uint8_t flags; buffer value; }`.
  - `apply_external(slot, vector<external_op>)` — mirrors `apply_encoded`
    (key-size validation, intra-batch last-key-wins, slot bookkeeping,
    `maybe_swap_active`) but builds `cell_entry` directly, calls
    `upsert_external`. No `encode_cell_buf`.
- [ ] **T4 — C API `ct_apply_batch_external`** (`c_api.h`, `c_api.cpp`)
  - `struct ct_ext_op { key, key_len, value, value_len, kind,
    bytes_ref, drop_fn }`.
  - `ct_apply_batch_external(tree, slot, ops, count)` — builds
    `external_op`s: Put → `wrap_external(value, len, bytes_ref, drop_fn)`;
    Delete → empty buffer + `kFlagTombstone`. Calls `apply_external`.
- [ ] **T5 — Rust FFI `BytesRef` + `apply_batch_external`**
      (`crowtree/ffi/src/lib.rs`)
  - `struct BytesRef(Bytes)`; `ct_release_bytes` extern fn.
  - `Crowtree::apply_batch_external(slot, &[ExtOp])` — per Put op:
    `Bytes::clone` (Arc bump) → `Box::into_raw(BytesRef)` → handle;
    build `ct_ext_op`; call C. Ownership of handles transfers to C++.
  - `BatchOp`-to-`ExtOp` conversion helper.
- [ ] **T6 — `CrowtreeEngine::apply` wiring** (`crowtree_engine.rs`)
  - `apply(slot, &Batch)` → build `ExtOp`s from `Batch`'s `Bytes`
    key/value slices → `apply_batch_external`. No `KVEngine` trait
    change. `InMemKV` unchanged.
- [ ] **T7 — Tests**
  - C++ unit: `test-ct` — external buffer construct/move/drop;
    memtable split upsert/get/drain/snapshot; `apply_external`
    round-trip + highest-slot-wins + batch atomicity.
  - FFI: `test-ffi` — `apply_batch_external` round-trip; handle
    lifetime (drop callback fires at drain).
  - Integration: `test-core` — large-value batch (3 keys, 64 KiB)
    Paxos commit → apply → read; small-value no-regression.
  - ASan: stress apply + flush cycle, verify no leak/use-after-free
    on the payload `Bytes`.

## File list

- `crowtree/include/crowtree/buffer.h` — T1
- `crowtree/include/crowtree/memtable.h` — T2
- `crowtree/src/memtable.cpp` — T2
- `crowtree/include/crowtree/crowtree.h` — T3
- `crowtree/src/crowtree.cpp` — T3
- `crowtree/include/crowtree/c_api.h` — T4
- `crowtree/src/c_api.cpp` — T4
- `crowtree/ffi/src/lib.rs` — T5
- `crowkv/src/kv/crowtree_engine.rs` — T6
- Tests under `crowtree/tests/`, `crowtree/ffi/tests/`,
  `crowkv/tests/` — T7

## Dependency ordering

T1 → T2 → T3 → T4 → T5 → T6 → T7. T1–T3 are C++-internal; T4–T5 cross
the FFI; T6 wires the consensus layer; T7 verifies end-to-end.

## Test checklist

- [ ] `pixi run test-ct` — C++ engine tests pass
- [ ] `pixi run test-ffi` — FFI tests pass
- [ ] `pixi run test-core` — Rust core tests pass (incl. new integration)
- [ ] `pixi run cargo fmt --all -- --check`
- [ ] `pixi run cargo clippy --all-targets -- -D warnings`
- [ ] `pixi run ct-lint` (clang-tidy on changed C++)
- [ ] `pixi run clang-format --dry-run --Werror` (changed `.cpp`/`.h`)
