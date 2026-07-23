<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R6: Cross-thread `EpochManager::Guard`

**Problem**: `EpochManager::Guard` is thread-bound — must be created and
released on the same thread. This forces copies in three scenarios:

1. **`get_async` cross-thread handoff**: A `get_async` miss resolves on the
   Reactor (io_uring) thread. The epoch guard was entered on the caller's
   thread; the CQE callback fires on the Reactor thread. The guard cannot
   cross threads, so `materialize_owned()` copies the borrowed L1 value into
   an owned `buffer` and releases the guard before handoff. With a
   cross-thread Guard, the borrowed `Slice` could survive the thread boundary
   — true zero-copy async read.

2. **`snapshot_view()` / `install_snapshot()` consistency**:
   `snapshot_view()` materializes all entries into owned copies.
   `install_snapshot()` swaps the tree under `write_mutex_` — a concurrent
   lock-free reader may see a transient partial state. A pinned `RootVersion`
   (atomic pointer to the old root, kept alive by epoch) would give readers a
   consistent point-in-time view without copying, but the Guard holding the
   old root alive might need to cross threads (reader thread enters, result
   consumed on another thread).

3. **Deferred stale-`RootVersion` GC**: After `install_snapshot` swaps the
   root, old pages are epoch-retired. A reader that loaded the old root
   pointer keeps its pages alive until its guard drains. If the reader hands
   the result to another thread (e.g. Rust async runtime), the guard can't
   follow — old root pages can't be reclaimed until the original thread's
   guard drains, which may be delayed.

**Fix options**:
- (a) **Cross-thread Guard**: per-thread `Participant` but release-by-token
  (hand off a release token to another thread, which drains on its own
  participant). Lighter per-access, but adds token management complexity.
- (b) **Page-level refcount**: increment on pin, decrement on unpin, free at
  zero — independent of thread. Heavier per-access (atomic per pin/unpin vs.
  per-thread epoch), but decouples lifetime from threads entirely.

**Priority**: Low — no current consumer requires cross-thread zero-copy. All
three scenarios work correctly today (with copies).

**Complexity**: High — touches the epoch reclamation core, which is
correctness-critical for all lock-free readers. Option (a) requires careful
token protocol design; option (b) adds per-page atomic overhead to every
`resident()` call.

**Files**: `crowtree/include/crowtree/epoch.h`, `crowtree/src/epoch.cpp`,
`crowtree/include/crowtree/crowtree.h` (`GetView`, `get_async_attempt`,
`materialize_owned`), `crowtree/src/crowtree.cpp`

**Acceptance**:
- `get_async` miss on Reactor thread returns borrowed `Slice` (no copy)
  verified via test that checks pointer equality with frame address.
- `snapshot_view()` returns a pinned view that stays consistent across
  `install_snapshot()` — reader sees old or new tree, never partial.
- Epoch reclamation stress test: concurrent readers + writers + snapshot
  swaps, no use-after-free under ASan/TSan.
