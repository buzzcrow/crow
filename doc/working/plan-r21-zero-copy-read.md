<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Plan — R21 Zero-Copy Engine Read API

## Task Breakdown

### Phase 1: FFI Layer (`crowtree/ffi/src/lib.rs`)

- [ ] **T1** — Change `try_get` signature from `Vec<u8>` to `&[u8]`.
  Remove the `key` parameter ownership; pass `key.as_ptr()` / `key.len()`
  to `ct_get_async`. The C++ side copies the key internally, so the
  borrow is only needed for the synchronous call.
- [ ] **T2** — Add `PinnedValue` struct:
  - Fields: `handle: *mut sys::ct_future`, `data: *const u8`,
    `len: usize`, `_not_send: PhantomData<*mut ()>`
  - `as_bytes(&self) -> &[u8]` — `slice::from_raw_parts(data, len)`
  - `Drop` — calls `ct_future_free(handle)`
  - `!Send` + `!Sync` via `PhantomData<*mut ()>`
- [ ] **T3** — Add `PinnedGetOutcome` enum:
  - `Ready(Result<Option<(u64, PinnedValue)>, CtError>)`
  - `Pending(Pin<Box<dyn Future<Output = Result<Option<(u64, Vec<u8>)>, CtError>> + Send>>)`
- [ ] **T4** — Add `try_get_pinned(&self, key: &[u8]) -> PinnedGetOutcome`:
  - Call `ct_get_async`, poll with a new `try_poll_ct_future_pinned`
    that does NOT call `ct_future_free` on the Get path and instead
    returns a `PinnedValue` holding the handle.
  - If pending: return `Pending` with the slow-path future (same as
    `try_get`'s pending path — `drive_ct_future` which copies via
    `copy_buf` on completion).
- [ ] **T5** — Update FFI tests if `try_get` is called directly.

### Phase 2: KVEngine Trait (`crowkv/src/kv/kv_engine.rs`)

- [ ] **T6** — Add `get_bytes` method to `KVEngine`:
  ```rust
  fn get_bytes(&self, key: &[u8]) -> KVFuture<Option<(u64, bytes::Bytes>>;
  ```
  Default impl: match on `self.get(key)`, convert `Vec<u8>` to
  `Bytes::from(v)` (zero-copy move).

### Phase 3: CrowtreeEngine (`crowkv/src/kv/crowtree_engine.rs`)

- [ ] **T7** — Update `get` to pass `key` (not `key.to_vec()`) to
  `try_get` (now `&[u8]`).
- [ ] **T8** — Override `get_bytes`:
  - Call `self.inner.try_get_pinned(key)`
  - `Ready(Ok(Some((slot, pinned))))` →
    `KVFuture::ready(Some((slot, Bytes::copy_from_slice(pinned.as_bytes()))))`
    (pinned dropped here, guard released)
  - `Ready(Ok(None))` → `KVFuture::ready(None)`
  - `Ready(Err(_))` → `KVFuture::ready(None)` (same error-swallow as `get`)
  - `Pending(fut)` → `KVFuture::Pending(Box::pin(async move {
    fut.await.ok().flatten().map(|(s, v)| (s, Bytes::from(v)))
    }))`

### Phase 4: Caller Chain

- [ ] **T9** — Add `engine_get_bytes` to `PxLearner`
  (`crowkv/src/paxos/learner.rs`):
  ```rust
  pub async fn engine_get_bytes(&self, key: &[u8]) -> Option<(SlotIndex, Bytes)> {
      self.engine.get_bytes(key).await
  }
  ```
- [ ] **T10** — Update `PxKvStore::kv_get`
  (`crowkv/src/cluster/px_kv_store.rs`):
  - Replace `engine_get(key).await` with `engine_get_bytes(key).await`
  - Replace `bytes::Bytes::from(v)` with just `v` (already `Bytes`)

### Phase 5: Tests

- [ ] **T11** — Add FFI test: `try_get_pinned` fast path returns
  `PinnedGetOutcome::Ready` with a `PinnedValue` whose `as_bytes()`
  matches the written value.
- [ ] **T12** — Add FFI test: `try_get_pinned` slow path (after
  eviction) returns `PinnedGetOutcome::Pending` and resolves correctly.
- [ ] **T13** — Add integration test: `CrowtreeEngine::get_bytes` fast
  path returns `Bytes` matching the written value.
- [ ] **T14** — Run existing tests to verify no regressions:
  - `pixi run test-ffi`
  - `pixi run test-core`

## Dependency Ordering

```
T1 (try_get &[u8])  ──┐
                       ├──> T4 (try_get_pinned) ──> T8 (get_bytes override)
T2 (PinnedValue)  ────┘                                    │
T3 (PinnedGetOutcome) ────────────────────────────────────┘
                                                            │
T6 (trait get_bytes) ──────────────────────────────> T8 ───┘
                                                            │
                                                    T9 (engine_get_bytes)
                                                            │
                                                    T10 (kv_get)
                                                            │
                                                    T11-T14 (tests)
```

T1, T2, T3, T6 can be done in parallel. T4 depends on T1+T2+T3. T8
depends on T4+T6. T9 depends on T6. T10 depends on T9. Tests depend on
all implementation tasks.
