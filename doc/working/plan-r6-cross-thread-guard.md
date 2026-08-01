<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R6 Plan: Cross-thread page refcount for handoff paths

See `design-r6-cross-thread-guard.md` for the full design. This is the
task breakdown.

## Tasks

### Phase 1: Core refcount mechanism

- [ ] **T1**: Add `pin_state_` field + helpers to `PageBase`
  - File: `crowtree/include/crowtree/page_types.h`
  - Add `std::atomic<uint32_t> pin_state_{0}`, `kRetiredBit = 1u << 31`.
  - Add `pin()` (`fetch_add(1, acquire)`), `unpin()` (`fetch_sub(1,
    acq_rel) - 1`; if result == `kRetiredBit`, `delete this`), and
    `retire_with_pins()` (`fetch_or(kRetiredBit, acq_rel)`; if old == 0,
    `delete this`).
  - Note: `pin()` is only called while the slot is resident (caller's
    invariant). No retired-bit check needed in `pin()`.

- [ ] **T2**: Change `retire_page()` deleter to use `retire_with_pins()`
  - File: `crowtree/src/crowtree.cpp` (line 165-168)
  - `retire_page(PageBase *p)` lambda: `p->retire_with_pins()` instead of
    `delete p`.
  - `retire_orphaned_page()` lambda (line 170-174): `mapping_.clear(page_id);
    p->retire_with_pins()` instead of `mapping_.clear(page_id); delete p`.
  - All 18 retire call sites automatically benefit (no per-site change).

- [ ] **T3**: Unit test for the refcount state machine
  - File: `crowtree/tests/unit/r6_refcount_test.cpp`
  - Test: pin then retire → deleter defers; last unpin frees.
  - Test: retire with no pins → immediate free.
  - Test: concurrent pin/unpin/retire, no double-free (under ASan).

### Phase 2: `get_async` slow path (scenario 1)

- [ ] **T4**: `GetView` gains a pin vector
  - File: `crowtree/include/crowtree/crowtree.h` (GetView class, ~line 91)
  - Add `std::vector<PageBase*> pins_` (the chain nodes keeping the
    borrowed value alive). Move-constructs; dtor unpins each.
  - Add debug-only `const uint8_t* frame_base() const` for tests (returns
    `value_.data()` when borrowed, nullptr when owned).

- [ ] **T5**: `get_async_attempt` slow path pins instead of `materialize_owned`
  - File: `crowtree/src/crowtree.cpp` (line 1590-1602, 1567-1579)
  - When `same_thread == false` and the value is frame-borrowed: pin the
    chain (head + base), release the epoch guard, set `GetView::pins_`,
    hand off. No copy.
  - When the value is overflow-chain (assembled): keep `materialize_owned`
    (no single frame to borrow).
  - `materialize_owned` stays for the overflow case only.

- [ ] **T6**: `ct_future` / FFI slow-path pin handoff
  - File: `crowtree/src/c_api.cpp`, `crowtree/ffi/src/lib.rs`
  - `ct_future` already holds `GetView` (which now holds `pins_`).
    `ct_future_free` drops the `GetView`, which unpins. No C-API change
    beyond what T4/T5 already do.
  - `PinnedValue`: remove `_not_send: PhantomData<*mut ()>`. The pin is
    thread-independent; `PinnedValue` is now `Send`.
  - `Drop for PinnedValue`: unchanged (calls `ct_future_free`).

- [ ] **T7**: Test — `get_async` miss returns borrowed frame
  - File: `crowtree/tests/integration/r6_test.cpp`
  - Build a durable tree, evict the leaf for key `k5`, `ct_get_async(k5)`.
  - On the slow path, check `GetView::frame_base()` == the resolved leaf's
    frame pointer (debug accessor). No copy.
  - Existing `async_get_test.cpp` Case 2 still passes (the materialized
    path for overflow values).

### Phase 3: `PinnedSnapshot` (scenarios 2 + 3)

- [ ] **T8**: `PinnedSnapshot` class
  - File: `crowtree/include/crowtree/snapshot.h`
  - Holds: `uint64_t at_slot_`, `std::vector<PageBase*> pinned_pages_`,
    `uint64_t root_page_id_` (for descent), leaf chain head IDs (captured).
  - Provides: `at_slot()`, `size()`, `find()`, `get()` — reading from
    pinned frames (same semantics as `Snapshot` but zero-copy).
  - Provides: `entries()` — one-time materialization into
    `std::vector<leaf_entry>` (for `compare` / export callers).
  - Provides: `compare()` — delegates to materialized `entries()`.
  - Movable, not copyable. Dtor unpins all `pinned_pages_`.
  - Walks via captured `PageBase*` pointers, NOT via `resident()`.

- [ ] **T9**: `snapshot_view()` returns `PinnedSnapshot`
  - File: `crowtree/src/crowtree.cpp` (line 2453-2488)
  - Enter epoch guard, load `root_page_id_`, walk via `resident()`.
  - Capture every `PageBase*` touched (inner descent + leaf chain) into
    `pinned_pages_`, call `pin()` on each.
  - Release epoch guard. Return `make_shared<PinnedSnapshot>(...)`.
  - The walk logic (collect_in_order) is refactored to capture pointers
    during the walk and defer materialization to `PinnedSnapshot::entries()`.

- [ ] **T10**: Update `snapshot_view()` callers
  - Files: any caller of `snapshot_view()` that uses `Snapshot` directly.
  - Grep for `snapshot_view()` across `crowtree/` and `crowkv/`.
  - Most callers use `->entries()`, `->compare()`, `->get()`,
    `->at_slot()` — all provided by `PinnedSnapshot`. Type name changes
    from `Snapshot` to `PinnedSnapshot`; behavior unchanged for
    same-thread callers.

- [ ] **T11**: Test — `PinnedSnapshot` consistency across `install_snapshot`
  - File: `crowtree/tests/integration/r6_test.cpp`
  - Build tree A with 1000 keys. `snapshot_view()` → `PinnedSnapshot snap`.
  - In another thread, `install_snapshot({}, 0)` (wipes the tree).
  - Confirm `snap->entries()` still has all 1000 keys (leaf chain not
    truncated/mixed). Confirm `snap->size() == 1000`.
  - Drop `snap`. Confirm old pages are freed (buffer_pool stats return to
    baseline after GC).

### Phase 4: Docs + perf gate

- [ ] **T12**: Update R6 backlog doc scope clarification
  - File: `doc/backlog/R6-cross-thread-guard.md`
  - Clarify: refcount on handoff paths only; sync get/scan keep EBR.

- [ ] **T13**: Update `design-crowtree-engine.md` §1.6 and §3.4
  - File: `doc/design/design-crowtree-engine.md`
  - §1.6: add a paragraph on refcount-on-handoff composition with EBR.
    The prior rejection of refcount was for the sync hot path; handoff
    paths (snapshot pin, get_async slow path) use refcount as an
    orthogonal cross-thread lifetime mechanism.
  - §3.4: remove "Blocked by R6" caveat on true zero-copy; document the
    pin-based slow path.

- [ ] **T14**: Read-path microbenchmark (perf gate)
  - File: `crowtree/bench/read_path_bench.cpp` (new)
  - Benchmarks: `BM_GetHit` (resident L1 hit), `BM_Scan` (full leaf chain).
  - Run before and after the refcount change. Gate: no regression > 2%.
  - The hot path is unchanged (no refcount on sync get/scan), so this is a
    guard against accidental cacheline-padding regression from the new
    `pin_state_` field on `PageBase`.

- [ ] **T15**: ASan stress test
  - File: `crowtree/tests/integration/r6_test.cpp`
  - Concurrent: 4 threads doing `snapshot_view()` + `get_async()`, 1
    thread doing `install_snapshot()` in a loop, 1 thread doing
    `flush()` + `apply()` churn. Run for 5 seconds. No UAF under ASan.

## Dependency ordering

T1 → T2 → T3 (core mechanism, test first)
T1 → T4 → T5 → T6 → T7 (get_async path)
T1 → T8 → T9 → T10 → T11 (PinnedSnapshot path)
T1 → T14 (perf gate — measure baseline before T2-T15 changes)
T12, T13 (docs, independent)

## Test checklist

- [ ] `pixi run test-ct` (all crowtree C++ tests, including new r6_test)
- [ ] `pixi run test-ffi` (FFI tests, including PinnedValue Send change)
- [ ] ASan: `pixi run test-ct-sanitizer` (or equivalent) for r6_test
- [ ] Perf gate: `read_path_bench` before/after, ≤ 2% regression

## Implementation status

**Implemented in this pass:**
- T1-T3: Core refcount mechanism (`pin_state_` on `PageBase`,
  `pin()`/`unpin()`/`retire_with_pins()`, `retire_page` deleter, 4 unit
  tests).
- T4-T7: `get_async` slow path pins instead of `materialize_owned` for
  frame-borrowed values; `GetView` gains `pins_` vector + debug
  `frame_base()`; `PinnedValue` is now `Send`; 3 integration tests
  (get_async no-copy, snapshot consistency, concurrent UAF stress).
- T12-T13: R6 backlog doc + design-crowtree-engine.md §1.6/§3.4 updated.
- T14: `read_path_bench.cpp` perf-gate benchmark.

**Deferred (separate change):**
- T8-T11: `PinnedSnapshot` class + `snapshot_view()` return type change.
  Scenario 2's full consistency (leaf chain not truncated/mixed) needs the
  `PinnedSnapshot` to capture page pointers during the walk and traverse
  via those pointers (not via `resident()`/mapping table). The current
  `snapshot_view()` still materializes a copy — correct but not zero-copy.
  The refcount mechanism (T1-T3) is in place; the `PinnedSnapshot` class
  is the next step. The R6 test `PinnedSnapshotStaysConsistentAcrossInstallSnapshot`
  verifies the *current* behavior (materialized copy is consistent because
  the epoch guard protects the walk); the zero-copy pinned version is the
  follow-up.
- T15: `Bytes::from_raw_parts` true-zero-copy in `crowtree_engine.rs`.
  `PinnedValue` is now `Send` (T6), which unblocks this; the `Bytes`
  integration is a separate change.
