# Crowtree Refactoring Plan

Task tracker for crowtree implementation work. Design rationale lives in the
[`design/`](design/) docs; this file tracks **what** to do, **when**, and in what
order.

**Priority levels:**
- **P0 — Must:** required for correctness, safety, or unblocking other work
- **P1 — Should:** important performance or operational improvement
- **P2 — Low:** optimization, future, or nice-to-have

**Layer groupings** match the sub-design document structure
([`design-crowtree.md §6`](design/design-crowtree.md#6-sub-design-document-map)):

| Layer | Design doc |
| --- | --- |
| Overview | `design-crowtree.md` |
| Memory | `design-crowtree-memory.md` |
| Async FFI | `design-crowtree-async.md` |
| Core (Tree & Epoch) | `design-crowtree-core.md` |
| Persistence | `design-crowtree-persistence.md` |
| Snapshot & GC | `design-crowtree-snapshot-gc.md` |
| Test | `design-crowtree-test.md` |
| Mapping table redesign | `design/design-crowtree-mappingtable.md` |

---

## Completed Tasks (Summary)

| Task | Description |
| --- | --- |
| #1 | FFI migration |
| #2 | API redesign |
| #3 | MemTable double buffering (active + flushing) |
| #5 B1 | Buffer core — owned/borrowed `buffer` with SBO |
| #5 B2a–c | Write path on `buffer` (cell encoders, MemTable→`btree_map<...,buffer>`, `leaf_entry.cell`→`buffer`) |
| #5 B3 (get) | Zero-copy `get_view()` — borrowed `Slice` for L1 hits, epoch-guarded |
| #5 B3 (scan) | Lock-free `scan()` via `right_sibling` chain walk under epoch guard |
| #6 | STL rename |
| #7 | Epoch ownership moved into `Crowtree`; `CrowtreeEnv` deleted |
| #8 (flush thread) | Background auto-flush thread (size + time dual trigger) |
| #8 (snapshot_view) | `snapshot_view()` epoch-guarded, lock-free |
| #8a | Snapshot export API cleanup — removed `at_slot` |
| #9 | MemTable map choice — `absl::btree_map` |
| #10 | C++ logging — `spdlog` (async, rotating, gated by `CROWTREE_HAVE_SPDLOG`) |
| #11 | Async FFI bridge — io_uring reactor (all 6 phases: reactor, C API async, Rust `Future` pump, zero-copy fast path, `KVFuture::Pending` for demand-load miss, full `PxLearner`/gRPC async conversion) |
| #12 | Lock-free EBR for `EpochManager` |
| #13 | `install_snapshot` epoch-safe (retire instead of immediate delete) |
| #14 | Mapping table redesign — packed slot word + segment struct + recycling (14a/14b); segment-image + directory + commit-anchor on-disk format replacing the manifest, two-pass segment-scan snapshot/recovery (14c/14d); `FaultyPageStore` + full test coverage (14e) |
| #15 | Reject oversized keys at `apply()` entry |
| #17 | Buffer pool live wiring — recency-ranked eviction via `last_touch_tick` |
| #19 | Terminology — `checkpoint`→`snapshot` (code) |
| #20 | `CrowtreeEngine` wired into `crowkv-server` (boot path, `resume_from_slot`, snapshot/GC maintenance loop, WAL GC) |
| #21 | GC sweep + dual watermark + `GcStats` |
| #22 | Raw block-device `PageStore` (`BlockPageStore` with `O_DIRECT`) |

---

## Overview Layer

### #8. Snapshot & Flush — Remaining Items `P1`

**Design:** `design-crowtree.md` §4.1 / D-Q11, core §6.2 / §9.

**Deviation (D-R1):** `snapshot_view()` still does an O(N) materialized
traversal (epoch-guarded, not `write_mutex_`-guarded), not the design's
zero-copy pinned `RootVersion`. Fixing that is blocked on the
`EpochManager::Guard` thread-bound issue (see Open Issues).

- [ ] Remove `write_mutex_` from `snapshot()`/`create_snapshot()` persistence
      phase — now unblocked (#11 async `PageStore` landed). Use
      `snapshot_write_next_async()` (already implemented in #11 Phase 2) to
      persist dirty pages without holding `write_mutex_`.
- [ ] Rename `flush()` → `create_snapshot()` — **deliberately skipped** (pure
      rename, high ripple risk, no functional benefit). Low priority.

### #20. Wire `CrowtreeEngine` into `crowkv` — Remaining Follow-ups `P2`

- [ ] New-member install streaming via `snapshot_export`/`import` wired into
      the reconfiguration/`SnapshotService` flow (design §§2/6).
- [ ] Dedicated cross-replica "durable on leader + >= 1 peer" watermark
      (today `set_gc_watermark`'s `snapshot_slot` is conservatively
      approximated by `group_safe_slot`).
- [ ] Configurable maintenance tick (hardcoded
      `group_maintenance::DEFAULT_MAINTENANCE_TICK` today).

---

## Memory Layer

### #5. Unified Buffer Design — Remaining Items

**Design:** [`design-crowtree-memory.md`](design/design-crowtree-memory.md), `design-crowtree.md` D-Q8.

**B2d — FFI boundary single-alloc** `P1`
- [ ] `ct_apply_*` allocs the key/cell `buffer`s once at the C boundary and
      moves them down (Option A); sets up B4 with no further call-site changes.

**B3 — `scan_view` zero-copy scan** `P2`
- [ ] `scan()`'s L1 resolution funnels through `resolve_chain_sorted` (owned
      `vector<leaf_entry>`, materializing to merge delta chains). A genuine
      zero-copy `scan_view` needs that resolver restructured to return
      borrowed views — larger blast radius (also used by GC/snapshot walks).
      `get_view()` covers the point-read case.

**B4 — Rust FFI** `P1`
- [ ] Step 1 (Option A): C API accepts raw ptrs, `buffer::alloc()`+copy at boundary
- [ ] Step 2 (Option B, future): `ct_alloc`/`ct_free` shared allocator, ownership yield, true end-to-end zero copy

**B5/B6 — future** `P2`
- [ ] Profile KV size distribution → size-classed memory pool behind the `buffer` seam
- [ ] RDMA-pinned allocation (with the RDMA backend)

---

## Async FFI Layer

### #11. Async FFI Bridge — io_uring Reactor `P1` — ✅ DONE (2026-07-09)

**Design:** [`design-crowtree-async.md`](design/design-crowtree-async.md) §13,
[`design-crowkv-async-kvengine.md`](design/design-crowkv-async-kvengine.md).

All 6 phases landed: `KVFuture<T>` trait shape → `Reactor` + `FileAsyncPageStore`
→ C API async variants (`ct_get_async`/`ct_flush_async`/`ct_snapshot_async`) →
Rust `async fn` + `Notify` pump (zero `spawn_blocking` for get/flush/snapshot) →
zero-copy fast-path `ct_future_poll` (borrows from `GetView`, no malloc) →
`KVFuture::Pending` for genuine demand-load miss + full `PxLearner`/gRPC async
conversion. Benchmark: ~20× speedup on resident-hit fast path vs. old
`spawn_blocking`. 248 C++ tests + full `cargo test --workspace` pass; ASan/TSan/UBSan clean.

**Remaining gap:** `scan`/`apply` stay `Ready`-only — no `ct_scan_async`/
`ct_apply_*_async` C API yet. Honest, documented gap, not an oversight.

---

## Core Layer (Tree & Epoch)

### #14. Mapping Table Redesign — Segment Recycling + Incremental Persistence `P1`

**Design:** [`design/design-crowtree-mappingtable.md`](design/design-crowtree-mappingtable.md). Workable spec: packed slot word, segment image + directory + A/B anchor, snapshot/recovery ordering.

**Key decisions:**
- PID recycling: **NO** — race condition risk too high
- Segment recycling: **YES** — free empty segments via epoch deleter
- Sparse segments: **acceptable** — 8 KB waste per segment
- Incremental persistence: **YES** — replace full manifest with segment-level persistence
- Backend abstraction: **YES** — all I/O via `PageStore` interface

**14a — Packed slot word + segment struct** `P1` — ✅ DONE (2026-07-09)
- [x] Packed 64-bit slot word (`mapping_slot.h`, `mapping_slot_test.cpp`)
- [x] Standalone `MappingSegment` struct (`mapping_segment.h`, `mapping_segment_test.cpp`) —
      `atomic<uint64_t> slots[]` (heap-allocated, runtime-sized) + `live_count`/
      `generation`/`dirty` atomics. Pure/standalone; live `MappingTable` adopts it in 14b.
- [x] `Options.mapping_segment_slots` (default 1024, fixed per tree)
- [x] **Cleanup:** removed the dead PID-recycling path (`MappingTable::free_page_id()` /
      `free_list_` in `mapping_table.h`/`.cpp`, plus its two tests) — contradicted
      "No PID recycling" (D1) and was unused. 243/243 `crowtree_tests` pass.

**14b — Segment recycling** `P1` — ✅ DONE (2026-07-10)
- [x] Epoch deleter clears slot → `live_count.fetch_sub` → CAS segment to nullptr + `epoch.retire` when 0 —
      `MappingTable::recycle_segment_if_empty` (`mapping_table.cpp`), called from both `store()` and
      `store_word()`/`clear()` on the empty-transition when `live_count` hits 0. `Crowtree` wires
      `mapping_.set_epoch_manager(&epoch_)` in its constructor; a table with no epoch wired (unit tests)
      `delete`s the segment immediately (no concurrent readers to protect against).
- [x] Reader loading nullptr segment / empty slot returns "gone" — already true of `get_word`'s existing
      nullptr-segment check; no change needed, covered by `MappingTable.SegmentRecycledWhenLastSlotCleared`.
- [~] Writer-owned dirty-set — deferred to 14d (snapshot needs to *drain* a dirty-segment set, not just the
      per-segment `dirty` bit that already exists on `MappingSegment`); no functional gap for 14b's in-memory
      recycling, which only needs the bit.
- Tests: `mapping_table_test.cpp` — `SegmentRecycledWhenLastSlotCleared`, `SegmentNotRecycledWhilePartiallyLive`,
  `RecycledSegmentFreedOnlyAfterEpochGuardDrains` (open `EpochManager::Guard` defers the free; `try_reclaim()`
  after release collects it). 258/258 `crowtree_tests` pass; ASan/TSan/UBSan clean.

**14c — On-disk format** `P1` — ✅ DONE (2026-07-10)
- [x] Segment image: header + `uint64_t packed[slot_count]` + CRC (≈8 KB) — `mapping_persist.h/.cpp`
      (`SegmentImageHeader`/`encode_segment_image`/`decode_segment_image`), separate header CRC + body CRC.
- [x] Segment directory image: `DirEntry{seg_idx, generation, image_addr, image_len, image_crc}[]` + CRC —
      `mapping_persist.h/.cpp` (`encode_segment_directory`/`decode_segment_directory`).
- [x] Commit anchor: tiny fixed A/B record — `persist.cpp`'s `CommitAnchor`/`encode_anchor`/`decode_anchor`
      (reuses the existing A/B slot machinery, renamed from `Superblock`). **Deviation** from the design's
      exact field list (documented in the struct's own comment and here): omits `leftmost_leaf_pid` (no
      leftmost-leaf fast path exists) and `page_alloc_root` (`SpaceAllocator` is rebuilt from live extents
      each `open()`, not itself a persistent structure with a root to save/restore).

**14d — Snapshot + recovery** `P1` — ✅ DONE (2026-07-10)
- [x] Snapshot order: dirty page frames → dirty segment images → directory → `flush()` → anchor → `flush()`
      → clear dirty. Implemented as a **two-pass segment scan**, not a reachable-page tree walk: pass 1
      enumerates every `is_dirty()` `MappingSegment` directly (bounded by `MappingTable::kMaxSegments`, not
      resident-tree size), folds any `BatchDelta` chain found there, and persists dirty page content; pass 2
      (after all pass-1 folding/retiring has settled) builds each still-dirty segment's final image and the
      full directory. `MappingTable::commit_segment_persist()` double-checks identity *and* `write_seq`
      before marking a segment's image durable (`PreparedSegmentWrite`), mirroring `PreparedPageWrite`'s
      identity check but needing the extra seq check since a segment's pointer, unlike a page's, doesn't
      change across a slot mutation (see `MappingSegment`'s doc comment).
- [x] Recovery: pick highest-valid anchor → read + CRC-check the directory → read + CRC-check each segment
      image → `MappingTable::install_recovered_segment()` installs the packed words directly (zero decode)
      → set root/next_page_id/last_applied_slot; pages demand-loaded lazily via the existing `resident()`
      cold path.
- [x] Old image cleanup: two-generation pending-free list — reuses the *same* `SpaceAllocator`/
      `build_allocator` mechanism pages already had (previous-generation extents protected via
      `collect_live_extents_from_directory`, everything else reusable), now uniformly covering page frames,
      segment images, and the directory itself.
- **Root-cause fix required to make the segment scan safe (not part of the original 14d checklist, found
  during implementation):** a merged-away leaf/inner's own PID was previously left "orphaned" — its mapping
  slot never cleared, never replaced (`crowtree.cpp`'s old comment: "the PID is leaked (acceptable in v1)").
  Harmless for the old manifest-walk snapshot (root→children only, never visits an unreachable PID) but a
  **use-after-free** for the segment-scan snapshot, which reads every slot regardless of tree-reachability,
  once the epoch reclaims the now-orphaned page. Fixed with `Crowtree::retire_orphaned_page(page_id, p)`:
  clears the mapping slot to empty *inside the epoch deleter*, at the same deferred point `p` itself becomes
  safe to delete (design §6's "slot clearing runs in the epoch deleter") — not immediately, which would race
  a straggler reader still walking in via a stale parent from before the retirement. Wired at all 4 orphan
  sites (`try_merge_leaf_locked`'s leaf + root-collapsed parent, `try_merge_inner_locked`'s merged-away inner
  + root-collapsed grandparent). This in turn exposed a **reentrancy hazard** in `EpochManager`: the deferred
  clear can itself trigger `MappingTable::recycle_segment_if_empty()` → `epoch.retire_object(seg)` on the
  *same* `EpochManager` instance, recursing into `retire()`/`reclaim_locked()` before the outer call returns.
  Fixed `reclaim_mu_` → `std::recursive_mutex`, and restructured `reclaim_locked()`/`~EpochManager()` to
  detach `retired_` into a local vector *before* running any deleter, so a nested `retire()` call can never
  mutate the vector an outer call is still iterating.

**14e — Tests** `P1` — ✅ DONE (2026-07-10)
- [x] Unit: packed-word round-trip (14a), image/directory CRC round-trip + tamper detection
      (`mapping_persist_test.cpp`). Anchor CRC round-trip has no *standalone* unit test (`CommitAnchor` is
      private to `persist.cpp`) but is exercised end-to-end by the crash-recovery tests below.
- [x] Crash recovery: before/after anchor, torn image (`Persist`/`CrashRecovery` corruption tests), highest-seq
      selection, two-generation fallback — all existing `crash_recovery_test.cpp`/`persist_test.cpp` tests
      pass unchanged against the new format (behavior-level, not byte-level, compatible: same 2-anchor-slot
      geometry and commit-ordering, just different slot/anchor *content*).
- [x] Segment recycling under split/merge churn (ASan/TSan/UBSan clean) — `MappingTable` unit tests (14b) +
      new `SplitMerge.SnapshotSucceedsAfterHeavyMergeAndRootCollapse` (heavy leaf+inner merge/root-collapse
      storm, then snapshot + reopen + verify). No dedicated concurrent "stale-reader-sees-empty" test with an
      overlapping live reader thread yet.
- [x] Incremental cost: page-level incrementality already verified (`incremental_checkpoint_test.cpp`'s
      `last_snapshot_pages_written()`); added the segment-level analogue, `last_snapshot_segments_written()`
      (`Crowtree`, backed by `snapshot_segments_written_`), plus `IncrementalCheckpoint.
      OnlyDirtySegmentsAreRewritten` (single-segment tree: 0 segments rewritten with no changes, exactly 1
      after a single-key edit) and `IncrementalCheckpoint.MultiSegmentTreeOnlyRewritesTouchedSegment`
      (3000-key tree spanning several segments: a single-key edit still re-images only 1 of them).
- [x] `FaultyPageStore` harness — `crowtree::FaultyPageStore` (`page_store.h`), a `PageStore` wrapper
      supporting one-shot `kDrop`/`kTear`/`kFail` faults armed on the Nth future `write_at`/`sync()` call.
      Unit tests in `page_store_test.cpp`. Two new `crash_recovery_test.cpp` integration tests:
      `FaultInjectedWriteFailureLeavesPreviousGenerationIntact` (a write fails mid-snapshot -> `snapshot()`
      itself reports the error, no anchor for that generation is ever written, previous generation intact)
      and `DroppedSegmentImageFailsReopenEvenThoughAnchorCommitted` (a segment image write is silently lost
      but the anchor still commits normally -- `open()` must fail out on the corrupt image, matching design
      §9's "a segment or directory image failing CRC while its anchor was committed indicates media
      corruption -> fail the node out", not silently fall back to an older generation).

**Sequencing:** 14a/14b need #5 B3 (lock-free readers + epoch retire) and #13
(epoch-safe slot clearing) — both done. 14c/14d need #17 + #18 (pool-owned
frames + durable per-frame `PageAddr`) and async PageStore (#11) — all done.
All dependencies are now satisfied; #14 is unblocked.

---

## Persistence Layer

### #17. Buffer Pool — Remaining `P2`

- [ ] **D3 (optional)** — extend eviction to inner/overflow bases (currently
      clean **leaf** bases only) if profiling shows it matters.

### #18. Incremental Snapshot — Remaining `P1`

**Design:** [`design-crowtree-persistence.md §4.3/§5A`](design/design-crowtree-persistence.md).

Durable per-page addr (`kNoAddr` = dirty) and write-only-dirty-pages are
already implemented and tested. Remaining:

- [x] **D4 — writer-owned dirty tracking** — ✅ DONE (2026-07-10), folded straight into #14d's
      segment-level `write_seq`/`is_dirty()` rather than a separate page-level tracker: `snapshot`
      no longer DFS-walks the reachable tree at all — `prepare_snapshot_locked` enumerates dirty
      `MappingSegment`s directly (bounded by `MappingTable::kMaxSegments`, not resident-tree size).
- [ ] **D5 (model reconciliation)** — likely a no-op given D1's frame model.
- [ ] **D6 — back-pressure test** under a write storm (eager snapshot).

**Sequencing:** D4 is done; D5/D6 remain, low priority.

### #22. Raw Block-Device `PageStore` — Remaining `P2`

`BlockPageStore` with `O_DIRECT` is done. Not yet wired into
`crowtree_ffi`/`CrowtreeOptions`/`crowkv-server` (those only select
`FilePageStore` vs. in-memory today).

- [ ] Expose `BlockPageStore` through `crowtree_ffi`/`CrowtreeOptions`/
      `crowkv-server` once a real SSD/SCM deployment target needs it.

---

## Snapshot & GC Layer

### #16. Native Frame Snapshot Format `P2`

**Files:** `snapshot_io.h/.cc`, `c_api.h/.cc`, `ffi/src/lib.rs`

A `kNative` format that directly streams page frame bytes would be
significantly faster for crowtree→crowtree transfers (Raft InstallSnapshot
production path). Currently only `kPortable` (key-value tuple serialization)
is supported.

- [ ] Define native format: leaf/inner frame images + remapped PID manifest
- [ ] Export: stream frame bytes directly (no tuple serialization)
- [ ] Import: load frames directly into mapping table (no entry-by-entry rebuild)
- [ ] Portable format remains available for testing and cross-engine scenarios
- [ ] Tests: native export/import round-trip, verify equivalence with portable

**Sequencing:** After #14 — native format shares the segment image concept.

---

## Test Layer

*No standalone tasks. Test requirements are embedded in each task above.
See [`design-crowtree-test.md`](design/design-crowtree-test.md) for the
overall test strategy.*

---

## Dependency Graph & Implementation Plan

### Dependency graph (✅ = done)

```
#1–#22 (core tasks) ......... ✅ (see Completed Tasks table)
#11 async FFI (all phases) .. ✅
#14 mapping table redesign .. ✅ done (14a-14e all complete)
#18 D4 DirtyTracker ......... ✅ done, folded into #14d
#5 B2d/B4/B5/B6 ............. P1/P2, profile-driven
#16 native frame snapshot ... unblocked (#14 done)
#20 follow-ups .............. P2, separable
#22 FFI/server wiring ....... P2, deployment-driven
#17 D3 ........................ P2, profiling-driven
```

### Recommended order for what's left

| Step | Item | Priority | Difficulty | Why here | Risk |
|-----:|------|:--------:|:----------:|----------|------|
| 1 | **#8 — remove `write_mutex_` from snapshot persistence** | P1 | **Low** | #11 Phase 2 already implemented `snapshot_write_next_async()`; just wire `snapshot()` to use it instead of the synchronous path under `write_mutex_` | Low |
| 2 | **#5 B2d + B4** — FFI boundary single-alloc + Rust FFI zero-copy | P1 | **Medium** | Profile-driven; B2d is additive (Option A copy at boundary), B4 Step 2 (shared allocator) is more involved | Med |
| 3 | **#16 native frame snapshot** | P2 | **Medium** | Unblocked (#14 done); shares segment image concept; performance optimization for crowtree→crowtree transfers | Low |
| 4 | **New-member install streaming** (`snapshot_export`/`import` ↔ `SnapshotService`) | P2 | **Medium** | Separable follow-up from #20; needs the reconfiguration flow | Med |
| 5 | **#5 B3 `scan_view`** — zero-copy scan | P2 | **Medium** | Needs `resolve_chain_sorted` restructured to return borrowed views; larger blast radius (also used by GC/snapshot walks); `get_view` covers point-read | Med |
| 6 | **#5 B5/B6** — size-classed memory pool, RDMA-pinned allocation | P2 | **High** | Profile-driven / backend-driven; future | Med |
| 7 | **#17 D3** — extend eviction to inner/overflow bases | P2 | **Low** | Only if profiling shows leaf-only eviction isn't enough | Low |
| 8 | **#22 FFI/server wiring** — expose `BlockPageStore` through FFI/server | P2 | **Low** | Only needed once a real SSD/SCM deployment target exists | Low |
| 9 | **#20 follow-ups** — configurable maintenance tick, cross-replica durability watermark | P2 | **Low** | Operational improvements; not on critical path | Low |

**Difficulty assessment criteria:**
- **Low:** Small, localized change; no new concurrency surfaces; existing patterns to follow.
- **Medium:** Multiple files/subsystems; some design decisions needed; moderate test surface.
- **High:** Large multi-session effort; new on-disk format or concurrency surface; significant test/recovery verification needed.

**Rationale:** All high-risk C++ concurrency work is done (#5 B1–B3, #3, #9, #12,
#13, #11), and #14 (the former largest remaining item, all of 14a-14e) is now
fully done. #8's `write_mutex_` removal from snapshot persistence is
low-hanging fruit since #11 already built the async machinery. Everything else
is P2 (profile-driven, deployment-driven, or optimization).

---

## Open Issues

- **`EpochManager::Guard` is thread-bound — blocks a naive zero-copy
  `RootVersion`.** `Guard::release()` mutates a per-thread, non-atomic
  `Participant::nest` counter — a `Guard` created on one thread and dropped on
  another would race, ruling out "pin = hold an open `Guard` for the object's
  whole lifetime". A real zero-copy `RootVersion` needs either (a) cross-thread
  `Guard` release support in `EpochManager`, or (b) a separate page-level
  refcount bumped under a short-lived guard and decremented from any thread on
  drop. Blocks: #8's true zero-copy snapshot, #21's deferred stale-`RootVersion`
  GC target.
