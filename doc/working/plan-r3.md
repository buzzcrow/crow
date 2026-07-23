<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# R3 Plan — Zero-Copy FFI Write Path

Design: `doc/working/design-r3.md`. Upstream: `design/design-crowtree-engine.md`
§2.2–§2.3.

## Phase 1 — C++ C API

- [ ] **1.1** Add `ct_write_handle` opaque type to `c_api.h`
  (`crowtree/include/crowtree/c_api.h`). Declare `ct_alloc`,
  `ct_apply_put_owned`, `ct_free_handle`. Document the lifecycle:
  alloc → write key/val → apply (consumes) OR free (error path).

- [ ] **1.2** Define `ct_write_handle` struct in `c_api.cpp`
  (`crowtree/src/c_api.cpp`): `{ std::string key; buffer cell; }`.
  Implement `ct_alloc(key_len, val_len)`:
  - `buffer::alloc(val_len, kCellHeaderSize)` for the cell
  - `std::string(key_len, '\0')` for the key
  - Return handle with writable pointers via an out-struct
    (`ct_write_ptrs { uint8_t* key; uint8_t* val; }`), or return the
    handle and provide separate `ct_handle_key`/`ct_handle_val` accessors.
  - Decision: single out-struct is simpler (one call, two pointers).

- [ ] **1.3** Implement `ct_apply_put_owned(tree, slot, handle)`:
  - Write slot bytes + flags=0 into `cell.data()[0..8]`
  - `encoded_op{std::move(handle->key), std::move(handle->cell)}`
  - Call `tree->apply_encoded(slot, {std::move(ops)})`
  - Delete the handle struct (consumed)

- [ ] **1.4** Implement `ct_free_handle(handle)`: delete handle struct
  (key string + cell buffer freed by destructors). No-op if null.

- [ ] **1.5** C++ unit test (`crowtree/tests/unit/`):
  - Alloc → write key/val → apply → `ct_get` verifies round-trip
  - Alloc → free (no apply) → no leak (ASan clean)
  - Alloc → apply → apply again (use-after-free, should be prevented
    by handle consumption — verify it returns error or is UB-documentated)
  - Large value (>4 KiB) round-trip

## Phase 2 — Rust FFI

- [ ] **2.1** Add `ct_write_handle`, `ct_write_ptrs`, `ct_alloc`,
  `ct_apply_put_owned`, `ct_free_handle` extern declarations to
  `crowtree/ffi/src/lib.rs` (in the `sys` module).

- [ ] **2.2** Add `WriteHandle` struct with `!Send + !Sync`:
  - `key_mut(&mut self) -> &mut [u8]`
  - `value_mut(&mut self) -> &mut [u8]`
  - `apply(self, slot: u64) -> Result<(), CtError>` (consumes)
  - `Drop` calls `ct_free_handle` if not consumed

- [ ] **2.3** Add `Crowtree::alloc_put(key_len, val_len) ->
  Result<WriteHandle, CtError>` method.

- [ ] **2.4** FFI integration test (`crowtree/ffi/tests/`):
  - Alloc → write → apply → `get` verifies round-trip
  - Alloc → drop (no apply) → no leak
  - Large value (>4 KiB) round-trip

## Phase 3 — Benchmark

- [ ] **3.1** Add benchmark comparing `apply_put` (copy path) vs
  `alloc_put` + `WriteHandle::apply` (zero-copy path) for values at
  1 KiB, 4 KiB, 16 KiB, 64 KiB. In `crowtree/ffi/` (Rust criterion) or
  `crowtree/bench/` (C++ gbench). Verify zero-copy path shows no memcpy
  for the value region.

## Phase 4 — Docs

- [ ] **4.1** Update `design/design-crowtree-engine.md` §2.3: mark
  Option B as partially implemented (R3 delivers handle-based
  ownership transfer; full engine integration deferred).

- [ ] **4.2** Delete `doc/backlog/R3-zero-copy-ffi.md` and remove the
  R3 entry from `doc/backlog/backlog.md` after all phases
  complete.

## Dependency ordering

Phase 1 → Phase 2 (Rust FFI wraps C API) → Phase 3 (benchmark needs
both paths) → Phase 4 (docs reflect final state).

## Quality gate (per AGENTS.md)

- `cargo fmt --check`, `cargo clippy -- -D warnings` (Rust FFI)
- `clang-format --dry-run --Werror` (changed `.cpp`/`.h`)
- `ct-lint` (clang-tidy on changed C++)
- `test-ct` (C++ unit tests), `test-ffi` (Rust FFI tests)
- Fix up to 3 times; skip pre-existing failures with stated reason
