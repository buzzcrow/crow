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
  Ruled out: `Participant::nest` is an owner-thread plain field and the
  seq_cst publish in `enter()` assumes the same thread publishes and clears
  `local_epoch`. A correct token protocol would need a separate
  "handed-off" participant state, dual-thread nest accounting, and a
  reentrancy story for a receiver that enters its own guard while holding a
  token — disproportionate complexity for a Low-priority item.
- (b) **Page-level refcount**: increment on pin, decrement on unpin, free at
  zero — independent of thread. Heavier per-access (atomic per pin/unpin vs.
  per-thread epoch), but decouples lifetime from threads entirely.

**Decision: option (b) refcount on the three handoff scenarios only.**
Sync `get` / `scan` keep EBR (no per-`resident()` atomic on the hot path
— the prior rejection in design-crowtree-engine §1.6 stands). Refcount
composes with EBR as an orthogonal cross-thread lifetime mechanism: EBR
protects the same-thread walk, refcount extends page lifetime across
threads after the walk hands off a borrowed `Slice` or pinned snapshot.
The per-pin atomic is uncontended on the handoff paths (one holder at a
time during a walk; contention only at snapshot handoff, which is off the
hot path). Traded for that is the elimination of the thread-bound `Guard`
contract on the handoff paths: any `Slice` borrowing a frame can cross
threads for as long as its refcount is held. No token protocol, no
dual-thread nest accounting, no participant state to manage.

**`PinnedSnapshot` sketch** (covers scenarios 2/3; scenario 1 reuses the
same per-page refcount on the `get_async` path):
- `snapshot_view` enters an epoch guard, loads `root_page_id_`, walks via
  `resident()`. During the walk, captures every `PageBase*` touched
  (inner descent + leaf chain) and pins each (`refcount++`).
- Releases the epoch guard. Returns a `PinnedSnapshot` holding the
  captured pins + root pointer + leaf chain.
- `PinnedSnapshot` traverses via the captured `PageBase*` pointers, NOT
  via `resident()` / the mapping table. So `install_snapshot` clearing
  slots mid-walk doesn't affect an in-progress pinned walk.
- Consistency window: the initial root→leaf descent (O(log N)
  `resident()` calls) has the same tiny window as today. The leaf chain
  walk (O(N), the bulk) is fully consistent (captured pointers, no
  mapping dependency). Strict improvement over today. Closing the
  descent window requires a `RootVersion` immutable-snapshot pointer
  (design-crowtree-engine §1.5), a separate larger change — deferred.

**Scenario (1) — `get_async` miss**: same per-page refcount. The Reactor
thread, under its epoch guard, pins the leaf base page(s) whose frame is
borrowed, releases the guard, hands the `GetView` (still borrowing the
frame) to the runtime thread. The runtime thread unpins on
`ct_future_free`. `materialize_owned` is removed from the slow path for
frame-borrowed values (kept only for overflow-chain values, which assemble
from multiple pages and have no single frame to borrow — same as today's
L0 / overflow case in `GetView`'s doc).

**Priority**: Low — no current consumer requires cross-thread zero-copy. All
three scenarios work correctly today (with copies). Reprioritize when a
workload shows `snapshot_view` copy cost or stale-root GC latency as a
measured hot spot, or when a dependent requirement (e.g. a zero-copy Raft
InstallSnapshot consumer) lands.

**Complexity**: High — touches the reclamation core, which is
correctness-critical for all lock-free readers. Option (b) adds a
per-page atomic on the handoff paths (snapshot pin walk, get_async slow
path); the sync `get`/`scan` hot path is unchanged (EBR only).

**Files**:
- `crowtree/include/crowtree/page_types.h` (`PageBase` gains `pin_state_`,
  `pin()`/`unpin()`/`retire_with_pins()` helpers)
- `crowtree/include/crowtree/crowtree.h` (`GetView` gains pin vector +
  debug `frame_base()`, `snapshot_view()` return type → `PinnedSnapshot`)
- `crowtree/src/crowtree.cpp` (`retire_page` deleter, `snapshot_view`,
  `install_snapshot`, `install_snapshot_native`, `get_async_attempt`)
- `crowtree/include/crowtree/snapshot.h` (new `PinnedSnapshot` class
  alongside `Snapshot`)
- `crowtree/src/c_api.cpp`, `crowtree/ffi/src/lib.rs` (`PinnedValue`
  becomes `Send`; `ct_future_free` unpins from any thread)
- `crowtree/bench/read_path_bench.cpp` (new perf-gate benchmark)

**Acceptance**:
- `snapshot_view()` returns a `PinnedSnapshot` that stays consistent across
  `install_snapshot()` — the leaf chain the pinned view captured is not
  truncated or mixed by the slot clears. Verified by a test that takes a
  `PinnedSnapshot`, runs `install_snapshot` concurrently, and confirms the
  pinned view's entries are unchanged.
- `get_async` miss on Reactor thread returns borrowed `Slice` (no copy)
  verified via a test that checks pointer equality with the frame address
  (debug-only `GetView::frame_base()` accessor, no production API change).
- Epoch reclamation stress test: concurrent readers + writers + snapshot
  swaps, no use-after-free under ASan. TSan: run if a TSan build target
  exists; if not, document the seq_cst/acq-rel ordering argument instead
  and add TSan to the build as a follow-up.
- Perf gate: new `crowtree/bench/read_path_bench.cpp` sync `get`/`scan`
  microbenchmark shows no regression > 2%. The hot path is unchanged
  (EBR only, no refcount); the gate catches accidental cacheline-padding
  regression from the new `pin_state_` field on `PageBase`.

**Interactions**:
- **R30** (zero-copy engine apply, write path): no dependency — R30 touches
  the apply/FFI write boundary, R6 touches the read/snapshot boundary. Both
  modify `GetView`/`Slice` usage but on disjoint code paths; confirm no
  shared struct change conflicts at merge time.
