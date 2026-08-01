<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R6 Design: Cross-thread page refcount for handoff paths

**Problem**: `EpochManager::Guard` is thread-bound (enter and release on
the same thread). Three scenarios copy data or risk inconsistency because
a borrowed `Slice` cannot survive a thread boundary:

1. `get_async` miss: Reactor resolves the value under its own epoch guard,
   must `materialize_owned` (copy) before handing the `GetView` to the
   runtime thread.
2. `snapshot_view` vs `install_snapshot`: the walk reads the live mapping
   table; a concurrent `install_snapshot` can clear slots mid-walk,
   truncating or mixing the view.
3. Stale-root GC: old pages retired by `install_snapshot` can't be freed
   until the original thread's epoch guard drains — delayed if the result
   was handed to another thread.

**Approach**: per-page refcount on the handoff paths only. Sync `get` /
`scan` keep EBR (no per-`resident()` atomic). Refcount composes with EBR:
EBR protects the walk (same-thread critical section), refcount extends
lifetime across threads after the walk.

#### State machine

`PageBase` gains `std::atomic<uint32_t> pin_state_`:
- bits 0–30: pin count (number of cross-thread handles holding this page)
- bit 31 (`kRetiredBit`): set by the epoch deleter when EBR has drained

```
pin(p):   p->pin_state_.fetch_add(1, acquire)   // only while slot is resident
unpin(p): s = p->pin_state_.fetch_sub(1, acq_rel) - 1
          if (s == kRetiredBit) delete p        // last unpin of a retired page
retire (epoch deleter):
          s = p->pin_state_.fetch_or(kRetiredBit, acq_rel)
          if (s == 0) delete p                  // no pins, free now
```

Correctness argument (no double-free, no UAF):
- `pin` only runs while the slot is resident (the pin path calls
  `resident()` or captures the pointer under an epoch guard before the
  slot is cleared). After `retire` clears the slot + sets `kRetiredBit`,
  no new `pin` can start.
- `retire`'s `fetch_or` returns the old value. If old == 0, no pins
  exist, deleter frees. No `unpin` can race (refcount was 0).
- If old > 0, deleter defers. The last `unpin` does `fetch_sub` bringing
  the value to `kRetiredBit` (count 0 + retired), and frees.
- Race between `retire`'s `fetch_or` and `unpin`'s `fetch_sub`: atomics
  serialize. If `fetch_sub` goes first (count 0, no retired bit), `fetch_or`
  sees 0 and frees; `unpin` sees 0 ≠ `kRetiredBit`, doesn't free. If
  `fetch_or` goes first (retired bit + count N), `fetch_sub` eventually
  brings it to `kRetiredBit`, last `unpin` frees. Exactly one frees.

#### Scenario 1: `get_async` slow path

Reactor thread, under its own epoch guard, resolves the `GetView`. If the
value is borrowed from a base page frame, pin the chain (head + base, or
just the base if head == base). Release the epoch guard. Hand the `GetView`
across threads with the pin held. `ct_future_free` unpins.

- `materialize_owned` is removed from the slow path for frame-borrowed
  values. Kept for overflow-chain values (assembled from multiple pages,
  no single frame to borrow — same as L0/overflow today).
- `GetView` gains a small vector of pinned `PageBase*` (the chain nodes
  keeping the borrowed bytes alive). Move-constructs across threads; dtor
  unpins on any thread.
- FFI: `PinnedValue` becomes `Send` (the pin is thread-independent). The
  `!Send` marker is removed. `ct_future_free` unpins from the dropping
  thread.

#### Scenario 2: `snapshot_view` consistency

`snapshot_view` enters an epoch guard, loads `root_page_id_`, walks via
`resident()`. During the walk, captures every `PageBase*` touched (inner +
leaf chain) and pins them. Releases the epoch guard. Returns a
`PinnedSnapshot` holding the captured pins + the root pointer + the leaf
chain ordering.

The `PinnedSnapshot` traverses via the captured `PageBase*` pointers, NOT
via `resident()` / the mapping table. So `install_snapshot` clearing slots
mid-walk doesn't affect an in-progress pinned walk.

Consistency window: the initial root→leaf descent (O(log N) `resident()`
calls) has the same tiny window as today — a concurrent `install_snapshot`
could swap `root_page_id_` between descent steps. The leaf chain walk
(O(N), the bulk) is fully consistent (captured pointers, no mapping
dependency). This is a strict improvement over today (where the entire walk
can see mixed/truncated state). Closing the descent window requires a
`RootVersion` immutable-snapshot pointer (design-crowtree-engine §1.5),
which is a separate, larger change — deferred.

`PinnedSnapshot` API:
- `at_slot()`, `size()`, `find()`, `get()` — same as `Snapshot` today,
  but reading from pinned frames instead of a materialized vector.
- `entries()` — materializes on demand (for `compare` / export callers
  that need a flat vector). One-time copy from pinned frames.
- Movable across threads; dtor unpins all held pages from any thread.

`snapshot_view()` return type changes from `shared_ptr<Snapshot>` to
`shared_ptr<PinnedSnapshot>`. Existing callers that call `entries()` /
`compare()` / `get()` work unchanged (PinnedSnapshot provides these).
Callers that hold the snapshot across threads (Rust async runtime) now
get a consistent view without copy.

#### Scenario 3: stale-root GC

Direct consequence of scenario 2's pinning. `install_snapshot` retires
old pages (as today: `free_all_resident_pages(retire=true)`). The epoch
deleter sets `kRetiredBit`; if a `PinnedSnapshot` holds pins, deleter
defers. When the `PinnedSnapshot` drops (on any thread), the last `unpin`
frees. No dependence on the original thread's epoch participant draining.

#### What does NOT change

- Sync `get` / `scan` / `get_view`: unchanged. EBR guard, no refcount, no
  per-`resident()` atomic. The hot path stays lock-free with one
  enter/exit per operation.
- `EpochManager`: unchanged. Still used for same-thread walks. The
  `Guard` stays thread-bound; refcount is a separate, orthogonal lifetime
  mechanism for cross-thread handoff.
- `retire_page()` call sites (18 places): unchanged signature. The
  deleter lambda changes from `delete p` to the `fetch_or(kRetiredBit)`
  protocol. All 18 sites benefit automatically (any retired page with
  pins is deferred).
- Mapping table: unchanged. Pages are still reached via page IDs through
  slots. The PinnedSnapshot captures pointers during the walk and doesn't
  re-enter the mapping table.

#### Alternatives considered

- **Option (a) cross-thread Guard token**: rejected (R6 doc). `nest` is
  owner-thread, seq_cst publish assumes same thread, token protocol
  complexity disproportionate.
- **RootVersion immutable snapshot pointer (design §1.5)**: closes the
  descent consistency window but requires RootVersion to own pages
  separately from the mapping table (or a snapshot of the mapping table).
  Larger architectural change; deferred. The PinnedSnapshot approach
  captures the same benefit for the leaf chain (the O(N) bulk) without
  the mapping-table redesign.
- **Hold `write_mutex_` during `snapshot_view` walk**: rejected by
  existing code (snapshot_view's comment: "no longer holds write_mutex_
  for the O(N) walk"). Blocks writers.

#### Acceptance

- `get_async` miss on Reactor thread returns borrowed `Slice` (no copy):
  test checks pointer equality with frame address via a debug-only
  `GetView::frame_base()` accessor.
- `PinnedSnapshot` stays consistent across `install_snapshot`: test takes
  a `PinnedSnapshot`, runs `install_snapshot` concurrently, confirms the
  pinned view's entries are unchanged (leaf chain not truncated/mixed).
- Epoch reclamation stress: concurrent readers + writers + snapshot swaps,
  no UAF under ASan. TSan if a target exists; else document the ordering
  argument.
- Perf gate: sync `get`/`scan` microbenchmark (new bench file) shows no
  regression > 2% (hot path unchanged; gate catches accidental
  cacheline-padding regression from the new `pin_state_` field on
  `PageBase`).

#### Files

- `crowtree/include/crowtree/page_types.h` — add `pin_state_` to
  `PageBase`, `pin()` / `unpin()` / `retire_with_pins()` helpers.
- `crowtree/src/crowtree.cpp` — `retire_page()` deleter lambda,
  `snapshot_view()` returns `PinnedSnapshot`, `get_async_attempt` slow
  path pins instead of `materialize_owned`.
- `crowtree/include/crowtree/snapshot.h` — `PinnedSnapshot` class
  (captures pinned pages, provides `Snapshot`-compatible API).
- `crowtree/include/crowtree/crowtree.h` — `GetView` gains pin vector,
  `snapshot_view()` return type change, debug `frame_base()` accessor.
- `crowtree/src/c_api.cpp` — `ct_future` holds pins instead of (or
  alongside) the epoch guard for the slow path.
- `crowtree/ffi/src/lib.rs` — `PinnedValue` becomes `Send`,
  `_not_send` marker removed.
- `crowkv/src/kv/crowtree_engine.rs` — `get_bytes` slow-path arm may
  skip the `Bytes::copy_from_slice` if `PinnedValue` is `Send` (true
  zero-copy via `Bytes::from_raw_parts` with a custom drop). Phase 2.
- `crowtree/bench/read_path_bench.cpp` — new file: sync get/scan
  microbenchmark for the perf gate.
- `crowtree/tests/integration/r6_test.cpp` — new test file: pinned
  snapshot consistency, get_async no-copy, ASan stress.
