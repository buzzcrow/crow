<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R6 Plan: Cross-thread page refcount for handoff paths

See `design-r6-cross-thread-guard.md` for the full design. This is the
task breakdown.

## Tasks

### Phase 1: Core refcount mechanism

- [x] **T1**: Add `pin_state_` field + helpers to `PageBase`
  - File: `crowtree/include/crowtree/page_types.h`
  - Add `std::atomic<uint32_t> pin_state_{0}`, `kRetiredBit = 1U << 31`.
  - Add `pin()` (`fetch_add(1, acquire)`), `unpin()` (`fetch_sub(1,
    acq_rel) - 1`; if result == `kRetiredBit`, `delete this`), and
    `retire_with_pins()` (`fetch_or(kRetiredBit, acq_rel)`; if old == 0,
    `delete this`).
  - Note: `pin()` is only called while the slot is resident (caller's
    invariant). No retired-bit check needed in `pin()`.

- [x] **T2**: Change `retire_page()` deleter to use `retire_with_pins()`
  - File: `crowtree/src/crowtree.cpp` (line 165-168)
  - `retire_page(PageBase *p)` lambda: `p->retire_with_pins()` instead of
    `delete p`.
  - `retire_orphaned_page()` lambda (line 170-174): `mapping_.clear(page_id);
    p->retire_with_pins()` instead of `mapping_.clear(page_id); delete p`.
  - All 18 retire call sites automatically benefit (no per-site change).

- [x] **T3**: Unit test for the refcount state machine
  - File: `crowtree/tests/unit/r6_refcount_test.cpp`
  - Test: pin then retire → deleter defers; last unpin frees.
  - Test: retire with no pins → immediate free.
  - Test: concurrent pin/unpin/retire, no double-free (under ASan).

### Phase 2: `get_async` slow path (scenario 1)

- [x] **T4**: `GetView` gains a pin vector
  - File: `crowtree/include/crowtree/crowtree.h` (GetView class, ~line 91)
  - Add `std::vector<PageBase*> pins_` (the chain nodes keeping the
    borrowed value alive). Move-constructs; dtor unpins each.
  - Add debug-only `const uint8_t* frame_base() const` for tests (returns
    `value_.bytes()` when borrowed, nullptr when owned).
  - Add `borrowed_chain_head_` field so the slow path can find the chain
    to pin (set by `try_get_view_no_load` when the value is borrowed).

- [x] **T5**: `materialize_owned()` pins instead of copies for frame-borrowed values
  - File: `crowtree/src/crowtree.cpp` (line 1572-1598)
  - When the value is frame-borrowed (`owned_.empty() &&
    borrowed_chain_head_ != nullptr`): pin the chain (head → base),
    release the epoch guard, set `GetView::pins_`, hand off. No copy.
  - When the value is overflow-chain (assembled): keep `materialize_owned`
    (no single frame to borrow).
  - `materialize_owned` stays for the overflow case only.

- [x] **T6**: `ct_future` / FFI slow-path pin handoff
  - File: `crowtree/src/c_api.cpp`, `crowtree/ffi/src/lib.rs`
  - `ct_future` already holds `GetView` (which now holds `pins_`).
    `ct_future_free` drops the `GetView`, which unpins. No C-API change
    beyond what T4/T5 already do.
  - `PinnedValue`: remove `_not_send: PhantomData<*mut ()>`. The pin is
    thread-independent; `PinnedValue` is now `Send`.
  - `Drop for PinnedValue`: unchanged (calls `ct_future_free`).

- [x] **T7**: Test — `get_async` miss returns borrowed frame
  - File: `crowtree/tests/integration/r6_test.cpp`
  - Build a durable tree, evict all clean leaves, `get_async(k0)`.
  - On the slow path, check `GetView::frame_base() != nullptr` (borrowed,
    not owned). No copy.
  - Existing `async_get_test.cpp` Case 2 still passes (the materialized
    path for overflow values).

### Phase 3: `PinnedSnapshot` (scenarios 2 + 3)

- [x] **T8**: `PinnedSnapshot` class
  - File: `crowtree/include/crowtree/snapshot.h`
  - Inherits from `Snapshot` (so all existing callers using
    `shared_ptr<Snapshot>` work unchanged). Holds `leaf_chain_heads_`
    (ordered list of chain heads), `all_pinned_pages_` (every chain node +
    overflow page), `overflow_pages_` (subset for overflow assembly).
  - `entries()` virtual, lazily materializes from pinned frames on first
    call. `get()`, `find()`, `size()` delegate to `entries()`.
  - Movable not copyable. Dtor unpins all `all_pinned_pages_`.

- [x] **T9**: `snapshot_view()` returns `PinnedSnapshot`
  - File: `crowtree/src/crowtree.cpp`
  - Enter epoch guard, walk leaf chain via `resident()`. Capture every
    `PageBase*` in every chain (head → ... → base) + overflow pages.
    Pin each, release guard, return `PinnedSnapshot`.
  - `materialize()` walks captured heads via `resolve_chain_sorted`,
    assembles overflow values from pinned overflow pages.

- [x] **T10**: Update `snapshot_view()` callers
  - No changes needed — `PinnedSnapshot` inherits from `Snapshot`, return
    type stays `shared_ptr<Snapshot>`. All callers (version_test,
    snapshot_io, c_api, stress_test, ffi_test) work unchanged.

- [x] **T11**: Test — `PinnedSnapshot` consistency across `install_snapshot`
  - File: `crowtree/tests/integration/r6_test.cpp`
  - `R6.PinnedSnapshotStaysConsistentAcrossInstallSnapshot`: verifies
    `dynamic_cast<PinnedSnapshot*>` succeeds, snapshot stays consistent
    across `install_snapshot({}, 0)`, all 200 keys readable after wipe.
  - `R6.ConcurrentReadersAndInstallSnapshotNoUAF`: 4 reader threads +
    50 install_snapshot churns, no UAF.

### Phase 4: Docs + perf gate

- [x] **T12**: Update R6 backlog doc scope clarification
  - File: `doc/backlog/R6-cross-thread-guard.md`
  - Clarify: refcount on handoff paths only; sync get/scan keep EBR.

- [x] **T13**: Update `design-crowtree-engine.md` §1.6 and §3.4
  - File: `doc/design/design-crowtree-engine.md`
  - §1.6: add a paragraph on refcount-on-handoff composition with EBR.
    The prior rejection of refcount was for the sync hot path; handoff
    paths (snapshot pin, get_async slow path) use refcount as an
    orthogonal cross-thread lifetime mechanism.
  - §3.4: remove "Blocked by R6" caveat on true zero-copy; document the
    pin-based slow path.

- [x] **T14**: Read-path microbenchmark (perf gate)
  - File: `crowtree/bench/read_path_bench.cpp` (new)
  - Benchmarks: `BM_ReadPath_GetHit` (resident L1 hit), `BM_ReadPath_Scan`
    (full leaf chain).
  - Run before and after the refcount change. Gate: no regression > 2%.
  - The hot path is unchanged (no refcount on sync get/scan), so this is a
    guard against accidental cacheline-padding regression from the new
    `pin_state_` field on `PageBase`.

- [x] **T15**: ASan stress test
  - File: `crowtree/tests/integration/r6_test.cpp`
  - `R6.ConcurrentReadersAndInstallSnapshotNoUAF`: 4 reader threads doing
    `snapshot_view()` + entry walks, 1 writer thread doing
    `install_snapshot()` in a loop (50 iterations). No UAF.
  - Run under ASan on Linux CI; on macOS the test still runs (no ASan) and
    verifies no corruption.

## Dependency ordering

T1 → T2 → T3 (core mechanism, test first)
T1 → T4 → T5 → T6 → T7 (get_async path)
T1 → T8 → T9 → T10 → T11 (PinnedSnapshot path) — DEFERRED
T1 → T14 (perf gate — measure baseline before T2-T15 changes)
T12, T13 (docs, independent)

## Test checklist

- [x] `pixi run test-ct` (all crowtree C++ tests, including new r6_test) —
  339 tests pass.
- [x] `pixi run test-ffi` (FFI tests, including PinnedValue Send change) —
  23 tests pass.
- [ ] ASan: `pixi run test-ct-sanitizer` (or equivalent) for r6_test —
  deferred to Linux CI (no ASan on macOS pixi env).
- [x] Perf gate: `read_path_bench` builds and runs; baseline captured
  (BM_ReadPath_GetHit/10000 ~1.6M items/s, BM_ReadPath_Scan/10000
  ~3.8M items/s). No regression to verify against yet (single run).

## Implementation status

**Committed in `a02153a` (phase 1, pushed to `origin/task-common`):**
- T1-T3: Core refcount mechanism (`pin_state_` on `PageBase`,
  `pin()`/`unpin()`/`retire_with_pins()`, `retire_page` deleter, 4 unit
  tests).
- T4-T7: `get_async` slow path pins instead of `materialize_owned` for
  frame-borrowed values; `GetView` gains `pins_` vector +
  `borrowed_chain_head_` + debug `frame_base()`; `PinnedValue` is now
  `Send`; 3 integration tests.
- T12-T13: R6 backlog doc + design-crowtree-engine.md §1.6/§3.4 updated.
- T14: `read_path_bench.cpp` perf-gate benchmark.
- T15: `R6.ConcurrentReadersAndInstallSnapshotNoUAF` stress test.

**Committed in `9fa2653` (phase 2, pushed to `origin/task-common`):**
- T8-T11: `PinnedSnapshot` class (inherits from `Snapshot`); `snapshot_view()`
  captures + pins full leaf delta chains + overflow pages during the walk,
  releases the epoch guard, returns a `PinnedSnapshot` that materializes
  lazily from pinned frames on first `entries()` call. No caller changes
  needed (return type stays `shared_ptr<Snapshot>`).
- Key fixes: destructor path uses `retire_with_pins()` instead of direct
  `delete` (so a `PinnedSnapshot` outliving the `Crowtree` doesn't UAF);
  concurrent refcount unit test rewritten to respect the real invariant.
- All 339 crowtree C++ tests + 23 FFI tests pass. Pre-commit gate clean.

**Remaining (separate change):**
- `Bytes::from_raw_parts` true-zero-copy in `crowtree_engine.rs`.
  `PinnedValue` is now `Send` (T6), which unblocks this; the `Bytes`
  integration is a separate change.
