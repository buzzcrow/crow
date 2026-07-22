<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# R21 — Zero-Copy Engine Read API

## Problem

`CrowtreeEngine::get` has two O(n) copies on the read path:

- **Key copy** — `try_get(key.to_vec())` (`crowtree_engine.rs:168`) allocates
  a `Vec<u8>` copy of the key for the FFI call. The C API `ct_get_async`
  takes `*const u8, len` and copies the key internally into a
  `std::shared_ptr<std::string>` (`crowtree.cpp:1586`), so the Rust-side
  `Vec` is a redundant allocation — the borrow is only needed for the
  duration of the synchronous `ct_get_async` call.
- **Value copy** — `copy_buf(value)` (`ffi/src/lib.rs:1173`) does
  `slice::from_raw_parts(..).to_vec()`. The C++ engine's zero-copy fast
  path returns a `ct_buf` that is a borrowed pointer into a still-live
  frame (epoch guard in `ct_future_impl::get_result`), but the Rust side
  must copy because `ct_future_free` is called immediately after
  `copy_buf`, releasing the epoch guard before the value is returned to
  the caller.

## Current Behavior (Fast Path Trace)

```
CrowtreeEngine::get(key: &[u8])
  1. key.to_vec()                          — allocate Vec (key copy)
  2. ct_get_async(tree, key.as_ptr(), len) — C++ copies key into shared_ptr<string>
  3. ct_future_poll → done=1, borrowed ct_buf
  4. copy_buf(value) → Vec<u8>::from_raw_parts  — allocate + memcpy (value copy)
  5. ct_future_free                        — release epoch guard
  6. return Vec<u8>
  7. caller: Bytes::from(v)                — move (zero-copy)
```

Total: 2 allocations (key Vec + value Vec), 2 copies (key memcpy + value
memcpy). The value `Vec` is then moved into `Bytes` at zero cost.

## Proposed Approach

### Key Copy Elimination

Change `AsyncCrowtree::try_get` to accept `&[u8]` instead of `Vec<u8>`.
The C API `ct_get_async` copies the key internally, so the Rust-side
borrow is only needed for the duration of that synchronous call. Zero C++
changes required.

### Value Copy Elimination (Fast Path)

Introduce a `PinnedValue` type in the FFI layer that holds the
`ct_future` handle and provides `as_bytes() -> &[u8]` borrowing directly
from the C++ frame. The epoch guard stays alive until `PinnedValue` is
dropped (which calls `ct_future_free`). `PinnedValue` is `!Send`
(`PhantomData<*mut ()>` marker) because the epoch guard is thread-local
(`epoch.cpp:43-55`).

New `AsyncCrowtree::try_get_pinned` method returns `PinnedGetOutcome`:

- `Ready(Option<(u64, PinnedValue)>)` — fast path, zero-copy borrow
- `Pending(fut)` — slow path, resolves to `Option<(u64, Vec<u8>)>`
  (same as today; the slow path always copies because
  `materialize_owned` runs on the reactor thread)

### Trait Extension: `get_bytes`

Add `KVEngine::get_bytes` returning `KVFuture<Option<(u64, Bytes)>>`
with a default implementation that delegates to `get` and converts
`Vec<u8>` to `Bytes` (zero-copy move). `InMemKV` uses the default.
`CrowtreeEngine` overrides `get_bytes` to use `try_get_pinned` on the
fast path, copying from `PinnedValue` into `Bytes` before dropping the
pin (releasing the guard). The `PinnedValue` never crosses the method
boundary — it is created, read, and dropped within `get_bytes`, all on
the same Tokio worker thread.

`PxLearner` gets a new `engine_get_bytes` method, and `PxKvStore::kv_get`
calls it instead of `engine_get`. The existing `engine_get` (returning
`Vec<u8>`) stays for tests and other callers — no test churn.

### New Fast Path Trace

```
CrowtreeEngine::get_bytes(key: &[u8])
  1. ct_get_async(tree, key.as_ptr(), len) — C++ copies key (no Rust-side copy)
  2. ct_future_poll → done=1, borrowed ct_buf
  3. PinnedValue { handle, data, len }     — no copy, no alloc
  4. Bytes::copy_from_slice(pinned.as_bytes()) — allocate + memcpy (value copy)
  5. drop PinnedValue → ct_future_free     — release epoch guard
  6. return Bytes
```

Total: 1 allocation (Bytes), 1 copy (value memcpy). Saves 1 allocation
(key Vec) and 1 copy (key memcpy) vs current. `copy_buf` is not called.

### Why Not True Zero-Copy?

True zero-copy (no value copy at all) would require `Bytes` to share the
C++ frame's memory via `Bytes::from_raw_parts` with a custom drop that
calls `ct_future_free`. This is blocked by R6 (cross-thread
`EpochManager::Guard`): `Bytes` is `Send` and could be dropped on
another thread, but the epoch guard must be released on the thread that
entered it (`epoch.cpp:57-66`). The guard's `Participant*` is
thread-local; releasing from the wrong thread is a data race on `nest`.

The current design eliminates `copy_buf` (the FFI-level copy) and moves
the copy to the `Bytes::copy_from_slice` call at the engine-caller
boundary. The total copy count is the same (1 value copy), but the
intermediate `Vec<u8>` allocation is eliminated — the copy goes directly
into `Bytes`, which is the final container the gRPC response needs.

### Slow Path (Unchanged)

The demand-load miss path always copies: `materialize_owned` runs on the
reactor thread, copying the borrowed value into an owned `buffer` and
releasing the guard before the completion callback fires. The Rust side
then copies from the owned buffer into `Vec<u8>` / `Bytes`. This is
structurally unavoidable without R6.

## Alternatives Considered

### A. Change `KVEngine::get` return type to `Bytes`

Rejected — 90+ test call sites use `engine_get` which returns
`Option<(u64, Vec<u8>)>`. Changing to `Bytes` would require updating all
of them (e.g., `v == b"val"` → `v.as_ref() == b"val"`). High churn for
marginal benefit.

### B. `EngineValue` enum (Borrowed/Owned) on the trait

Rejected — `PinnedValue` is `!Send`, so an enum containing it would be
`!Send`, which conflicts with `KVFuture::Pending`'s `Send` requirement.
Splitting into a separate outcome type adds complexity for no gain over
option C.

### C. Separate `get_bytes` method (chosen)

Default impl delegates to `get`, `CrowtreeEngine` overrides. `PinnedValue`
is internal to `CrowtreeEngine::get_bytes`, never exposed. Existing `get`
and `engine_get` stay for tests. Minimal ripple.

### D. `Bytes::from_raw_parts` with custom drop

Rejected — requires `PinnedValue: Send` or unsafe thread-check in drop.
The epoch guard's thread-locality makes this UB if `Bytes` is dropped on
another thread. Blocked by R6.

## Files

- `crowtree/ffi/src/lib.rs` — `PinnedValue`, `PinnedGetOutcome`,
  `try_get_pinned`, change `try_get` to `&[u8]`
- `crowkv/src/kv/kv_engine.rs` — `get_bytes` method on `KVEngine`
- `crowkv/src/kv/crowtree_engine.rs` — override `get_bytes`, update `get`
  to pass `&[u8]`
- `crowkv/src/kv/in_mem_kv.rs` — no change (uses default `get_bytes`)
- `crowkv/src/paxos/learner.rs` — `engine_get_bytes` method
- `crowkv/src/cluster/px_kv_store.rs` — `kv_get` calls
  `engine_get_bytes`
- `crowtree/ffi/tests/ffi_test.rs` — update `try_get` call if needed

## Acceptance Criteria

- All existing read tests pass unchanged.
- `CrowtreeEngine::get_bytes` fast path does not call `copy_buf` — the
  value is read from `PinnedValue::as_bytes()` and copied into `Bytes`
  directly.
- The epoch guard is held until `PinnedValue` is dropped (after the
  `Bytes` copy is complete).
- `InMemKV` continues to work via the default `get_bytes` impl (converts
  `Vec<u8>` to `Bytes`, zero-copy move).
- The key copy (`key.to_vec()`) is eliminated — `try_get` and
  `try_get_pinned` accept `&[u8]`.
- `PinnedValue` is `!Send` (compile-time enforced).
- Slow path (demand-load miss) still works — `Pending` future resolves
  to owned `Vec<u8>` → `Bytes::from(vec)` (move).
