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
| #11 | Async FFI bridge — io_uring reactor (all 6 phases: reactor, C API async, Rust `Future` pump, zero-copy fast path, `KVFuture::Pending` for demand-load miss, full `PxLearner`/gRPC async conversion); `ct_scan_async`/`AsyncCrowtree::scan` follow-up |
| #12 | Lock-free EBR for `EpochManager` |
| #13 | `install_snapshot` epoch-safe (retire instead of immediate delete) |
| #14 | Mapping table redesign — packed slot word + segment struct + recycling (14a/14b); segment-image + directory + commit-anchor on-disk format replacing the manifest, two-pass segment-scan snapshot/recovery (14c/14d); `FaultyPageStore` + full test coverage (14e) |
| #15 | Reject oversized keys at `apply()` entry |
| #17 | Buffer pool live wiring — recency-ranked eviction via `last_touch_tick`; D3 separate inner-base eviction budget (`evict_clean_inner`) + a real `free_subtree`/teardown leak this exposed, fixed via `free_all_resident_pages`'s segment-scan |
| #19 | Terminology — `checkpoint`→`snapshot` (code) |
| #20 | `CrowtreeEngine` wired into `crowkv-server` (boot path, `resume_from_slot`, snapshot/GC maintenance loop, WAL GC); configurable `maintenance_tick_ms` |
| #21 | GC sweep + dual watermark + `GcStats` |
| #22 | Raw block-device `PageStore` (`BlockPageStore` with `O_DIRECT`), wired end-to-end through `crowtree_ffi`/`crowkv`/`crowkv-server` (`--kv-backend {file,block}`) |
| #16 | Native frame snapshot format (`kNative`): raw frame-byte export/import, no cell decode/tuple encode/tree-rebuild |
| #5 B2d/B4-1 | FFI boundary single-alloc: `Crowtree::apply_encoded` — `ct_apply_*` allocate key+cell once, no `Batch` re-encode |
| #18 D4–D6 | Incremental snapshot dirty-tracking folded into #14d; model reconciliation confirmed no-op; write-storm back-pressure test added (eager-snapshot trigger itself never existed — documented gap) |
| #5 B3 (partial) | `resolve_chain_sorted` copy avoidance — borrowed-`Slice` dedup map, only the final per-key winner is ever copied |

---

## Overview Layer

### #8. Snapshot & Flush — Remaining Items `P1`

**Design:** `design-crowtree.md` §4.1 / D-Q11, core §6.2 / §9.

**Deviation (D-R1):** `snapshot_view()` still does an O(N) materialized
traversal (epoch-guarded, not `write_mutex_`-guarded), not the design's
zero-copy pinned `RootVersion`. Blocked on the `EpochManager::Guard`
thread-bound issue (see Open Issues).

- [x] Remove `write_mutex_` from snapshot persistence I/O — already
      satisfied; only `prepare_snapshot_locked()`'s CPU-only walk holds it.
      Regression test: `Persist.WriteMutexNotHeldDuringSnapshotIo`.
- [ ] Rename `flush()` → `create_snapshot()` — **re-assessed 2026-07-10,
      still deliberately skipped, for a sharper reason than "pure rename":**
      `flush()` only drains L0 into the in-memory L1 tree
      (`drain_memtable_into_l1_locked`) -- it never touches `page_store`, so a
      plain rename to `create_snapshot()` would actively mislead callers into
      thinking it durably persists (it doesn't; only the separate `snapshot()`
      call does that, per `Crowtree::snapshot`/`persist.cpp`). Doing this
      *correctly* per the design doc's literal intent ("flush *is* snapshot
      creation") would mean **merging** flush+snapshot into one call -- a real
      behavioral change, not a rename, and a regression on the `apply()` hot
      path: `crowtree_engine.rs`'s `apply()` calls `flush()` on *every write*
      for immediate read visibility (cheap, in-memory only today); folding in
      `snapshot()`'s disk I/O would make every apply pay a durable-write cost.
      Confirmed: correctly deferred, not merely low-value.

### #20. Wire `CrowtreeEngine` into `crowkv` — Remaining Follow-ups `P2`

- [ ] **New-member install streaming** (`snapshot_export`/`import` ↔ a
      `SnapshotService`). The crowtree-side half already exists and is
      unused (`crowtree_ffi::Crowtree::snapshot_export`/`snapshot_import`),
      but genuinely nothing else does: no streaming RPC/proto exists at all
      (`pxos.proto`'s own comment: never-implemented), no "pull a snapshot
      then catch up on just the WAL tail" join flow exists in
      `crowkv-server` (new members today replay full Paxos history), and
      correlating the snapshot's `at_slot` with the joining replica's
      acceptor/learner state is a real cross-layer invariant. A genuine
      distributed-systems feature, not a wiring exercise — left as a
      separately-scoped follow-up.
- [ ] **Cross-replica "durable on leader + ≥1 peer" watermark** — today
      `group_safe_slot` conservatively approximates it (proven safe, never
      overstates; see `group_maintenance.rs`'s module doc). The real thing
      needs a new gossip channel piggybacked on Paxos heartbeats — touches
      a sensitive, well-tested protocol path for a storage-efficiency-only
      refinement with no correctness upside. Deferred.
- [x] Configurable maintenance tick — `PxElectionConfig::maintenance_tick_ms`
      replaces the hardcoded constant; test:
      `maintenance_loop_uses_configured_tick_interval`.

---

## Memory Layer

### #5. Unified Buffer Design — Remaining Items

**Design:** [`design-crowtree-memory.md`](design/design-crowtree-memory.md), `design-crowtree.md` D-Q8.

**B2d — FFI boundary single-alloc** `P1` — ✅ DONE (2026-07-10)
- [x] `Crowtree::encoded_op{key, cell}` + `apply_encoded()`: `ct_apply_*`
      allocate key+cell exactly once from the raw C bytes and move them
      straight to `MemTable::upsert` — no intermediate `Batch`/`batch_op`
      for `apply_batch` to re-encode. `Batch`/`apply()`/`apply_batch()`
      themselves untouched (no call-site ripple). Test:
      `CApi.OversizedKeyRejectedThroughEncodedPath`.

**B3 — `scan_view` zero-copy scan** `P2` — partial win landed 2026-07-10
- [x] Lock-freedom already done: `scan()` runs under only an
      `EpochManager::Guard`, one leaf at a time via `right_sibling`.
- [x] **Copy avoidance within `resolve_chain_sorted` — done.** Its dedup map
      is now keyed/valued by *borrowed* `Slice`s into the chain's own
      already-resident storage instead of `std::map<std::string,std::string>`
      — only the final winner per key is copied (in the output loop), not on
      every visit/every contested key. Purely internal: the function's
      signature (an owned `vector<leaf_entry>`) is unchanged, so
      `scan()`/`try_scan_no_load()`/`collect_in_order()`/GC's live walk
      needed no changes at all.
- [ ] **Full zero-copy (borrowed `Slice`s all the way out to those 3
      callers) remains deferred**, deliberately not attempted in the same
      pass: each caller would need to independently prove out how long its
      own borrowed result must stay valid (`scan()`'s multi-leaf walk holds
      one epoch guard for the *whole* call, so it's likely fine; GC's live
      walk and `collect_in_order`/`snapshot_view` would need the same
      argument worked through on their own terms) — a materially bigger,
      separately-scoped change touching 3 correctness-sensitive paths, not
      mechanical, still with no profiling data motivating the *extra* win
      over what the copy-avoidance above already captures.

**B4 — Rust FFI** `P1`
- [x] Step 1: done as part of B2d (`ffi/src/lib.rs` needed zero changes —
      already passed raw ptrs straight through).
- [ ] **Step 2, re-assessed 2026-07-10, still deferred:** `buffer::move_from`
      (the C++-side half of Option B) already exists (`buffer.h`) — the
      missing piece is exposing a `ct_alloc`/`ct_free` pair and a "yielding"
      `ct_apply_*` that wraps the Rust-allocated pointer via `move_from`
      instead of copying. The blocker isn't the allocator (`std::malloc`/
      `std::free` are already the same libc functions on both sides of the
      FFI, so sharing a heap is not itself risky) — it's that `buffer::alloc`'s
      `header_reserve` convention (cell header written into a reserved prefix
      *ahead of* the value bytes) would have to become a **stable, exposed
      ABI contract** Rust must replicate exactly (exact header size/layout)
      to pre-allocate correctly-shaped memory. That's a real design decision
      (what's the stable contract, and what breaks if the cell header ever
      changes shape), not a mechanical wire-up, for a win Step 1 already
      mostly captured (only the *one remaining* boundary copy is at stake).
      Left as future, matching the original assessment.

**B5/B6 — future** `P2` — re-assessed 2026-07-10, still genuinely blocked
- [ ] **Size-classed memory pool:** the design doc's own rollout plan (§6)
      is explicit — "Profile first... size classes are chosen from that
      histogram" — building one with unvalidated, guessed size classes
      would contradict the design's own stated methodology, not fulfill it.
      Confirmed still blocked on profiling data that doesn't exist in this
      repo (not a capability gap — a missing real-world input).
- [ ] RDMA-pinned allocation: blocked on an RDMA backend, which doesn't exist at all yet.

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

**Remaining gap, assessed and closed 2026-07-10:** `scan`/`apply` stayed
`Ready`-only. Split into two genuinely different cases:
- `apply()` is pure in-memory (`MemTable` upsert, no `page_store` I/O at
  all) — an `ct_apply_*_async` would have nothing to make async. Not a
  real gap; left as-is.
- `scan()` **did** call `resident()`, which synchronously blocks on
  `page_store->read_at()` for an unloaded page — the exact same cold-miss
  cost `get_async`/`KVFuture::Pending` already exists to avoid for point
  reads. **✅ Now done: `ct_scan_async`.**
  - `Crowtree::try_scan_no_load` (crowtree.h/.cpp): `scan()`'s logic
    verbatim, but the initial root→leaf descent and every leaf probe use
    a non-blocking check (mirrors `try_get_view_no_load`'s `probe`)
    instead of `resident()`'s demand-load; bails out the moment *any*
    page is unloaded, reporting which one.
  - `Crowtree::scan_async`/`scan_async_attempt`: mirrors
    `get_async`/`get_async_attempt`'s retry loop exactly, but since a
    scan can hit more than one cold page, a miss simply **retries the
    whole scan from scratch** once the blocking page resolves (documented
    trade-off: not maximally efficient, but always terminates — every
    retry either fully resolves or permanently loads one more page — and
    does no redundant I/O).
  - C API: `ct_scan_async` + a new `ct_future_impl::Kind::kScan` (packs
    into the same wire format `ct_scan` already uses; always an owned
    buffer, freed by `ct_future_poll` itself like flush/snapshot, never
    borrowed like get).
  - Rust FFI: `AsyncCrowtree::scan`, a new `FutureKind::Scan` (needed
    `take_buf` instead of `copy_buf` for its always-owned buffer — fixed
    a subtle leak-on-error-status edge case caught while implementing
    this, since the buffer must be freed even when the underlying op
    errored, not only inside the success branch).
  - Tests: `crowtree/tests/integration/async_scan_test.cpp`
    (`AsyncScan.*` — fast path, sync/async output equivalence including
    truncation, miss-after-eviction, abandon-before-completion, empty
    tree) + `crowtree/ffi/tests/ffi_test.rs`
    (`async_scan_fast_path_completes_on_first_poll`,
    `async_scan_slow_path_completes_after_eviction`,
    `async_scan_respects_limit_and_truncated_flag`). 294/294
    `crowtree_tests` + 15/15 `crowtree-ffi` tests + full
    `crowkv`/`crowkv-server` suites pass.

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

- [x] **D3 — inner bases — ✅ DONE (2026-07-10).** A first attempt shared
      `evict_clean_leaves`'s ranked budget between leaves and inner bases and
      broke `Eviction.RecentlyTouchedLeafSurvivesEvictionOverColderOnes` (a
      leaf's own recency protects it, but its just-visited ancestor chain
      could still be evicted from the same shared budget). Fixed with a
      genuinely **separate** budget/pass: `Crowtree::evict_clean_inner(_locked)`
      + `ct_evict_clean_inner` (C API) + `Crowtree::evict_clean_inner` (Rust
      FFI) — never evicts a leaf; `evict_clean_leaves` never evicts an inner
      base. Tests: `Eviction.EvictCleanInnerNeverTouchesLeaves`,
      `Eviction.RecentlyTouchedAncestorChainSurvivesInnerEvictionOverColderOnes`
      (measures a touched key's own ancestor-chain depth via
      `evict_clean_inner(0)`'s return value rather than assuming a fixed tree
      shape).
      - **Real bug found and fixed along the way (ASan-caught):**
        `free_subtree` (used by `~Crowtree`'s teardown and
        `install_snapshot(_native)`'s live-tree replacement) is a top-down
        root→children walk that bails out the moment it reaches an
        *unloaded* slot — always safe before (only a leaf, with no
        descendants, could ever be independently evicted) but not once
        `evict_clean_inner` can unload an *inner* ancestor while its leaf
        descendants stay fully resident underneath it: such a walk would
        never reach (or free/retire) those leaves at all. Fixed with
        `Crowtree::free_all_resident_pages`: a two-pass segment-scan (same
        technique `persist.cpp`'s snapshot uses to discover pages without a
        reachable-page walk) that also picks up overflow pages directly
        (their own mapping slot) instead of needing a leaf to discover them.
        Regression test: `Eviction.InstallSnapshotReclaimsResidentLeavesEven
        WhenInnerAncestorWasEvicted` (checkable via `BufferPool::Stats::used`,
        no sanitizer needed).
      - Independently evicting **overflow** frames still violates
        `evict_overflow_chain_locked`'s contiguous-whole-chain assumption —
        correctly out of scope for D3 (which was about inner bases), left as
        a separate, still-hypothetical follow-up if ever motivated.
      - 297/297 `crowtree_tests` across default/ASan/UBSan/TSan builds pass.

### #18. Incremental Snapshot — ✅ DONE (2026-07-10)

**Design:** [`design-crowtree-persistence.md §4.3/§5A`](design/design-crowtree-persistence.md).

- [x] **D4** writer-owned dirty tracking — folded into #14d's segment-level
      `write_seq`/`is_dirty()`; `snapshot()` no longer DFS-walks the tree,
      `prepare_snapshot_locked` enumerates dirty segments directly.
- [x] **D5** model reconciliation — confirmed no-op: `PageBase::durable_addr`
      already satisfies "a dirty frame is never evicted until written".
- [x] **D6** back-pressure test — **scoped down with a real finding**: no
      eager-snapshot-on-memory-pressure trigger exists anywhere in the engine
      at all (§5A's feature was never built, not a test gap) — building it
      would also need care around `write_mutex_` re-entrancy (calling
      `snapshot()` from inside `evict_clean_leaves_locked` would self-deadlock).
      Added `WritePath.WriteStormToBoundedKeySetKeepsDirtyMemoryBounded`
      instead: confirms dirty memory tracks distinct-key count, not write
      count, for the realistic case that doesn't need eager snapshot at all.
      The genuine eager-snapshot feature remains a documented, unbuilt gap.

### #22. Raw Block-Device `PageStore` — ✅ DONE (2026-07-10)

`BlockPageStore` (`O_DIRECT`) is now wired end-to-end: `ct_options.backend`
field (C ABI) → `crowtree_ffi::PageStoreBackend` → `crowkv::kv::CrowtreeBackend`
→ `crowkv-server --kv-backend {file,block}` CLI flag, threaded through
`create_group_with_wal`. No async twin for the block backend yet (falls back
to sync completion, matching the existing in-memory behavior). Tests:
`CApi.BlockDeviceCheckpointReopen`, `block_device_snapshot_reopen_smoke`,
`create_group_with_wal_crowtree_block_backend_persists_across_restart`.

---

## Snapshot & GC Layer

### #16. Native Frame Snapshot Format — ✅ DONE (2026-07-10)

**Files:** `snapshot_io.h/.cc`, `crowtree.h/.cc`

`kNative` streams raw leaf/inner/overflow frame bytes (`Crowtree::NativeFrame
{page_id, frame_bytes}`) instead of `kPortable`'s key-value tuple
serialization — no cell decode/encode, no entry-by-entry tree rebuild on
import. `collect_native_frames` walks the reachable tree, folding delta
chains into consolidated bases first (a real side-effect, unlike the
read-only `snapshot_view()`); `install_snapshot_native` wholesale-replaces
the tree via `from_frame_copy`, same as demand-load's own reconstruction.
`kPortable` untouched — still the cross-engine/oracle-comparable path.
Tests: `NativeExportImportRoundTrip`, `NativeEquivalentToPortable`,
`NativeEmptyTreeRoundTrip`, `NativeCrcTamperRejected`
(`snapshot_export_test.cpp`).

---

## Test Layer

*No standalone tasks. Test requirements are embedded in each task above.
See [`design-crowtree-test.md`](design/design-crowtree-test.md) for the
overall test strategy.*

---

## Remaining Work Summary (refined 2026-07-10)

Everything not listed here is done — see the Completed Tasks table. Every row
below has already been individually assessed (see its layer section for the
full reasoning); this table is the current, accurate "what's actually left"
view.

| # | Item | Priority | Status |
|--:|------|:--------:|--------|
| 1 | ~~`ct_scan_async`~~ (C API + Rust FFI, mirrors `get_async`'s retry pattern) | P1 | ✅ done (2026-07-10) |
| 2 | ~~`#17` D3 (separate inner-base eviction budget)~~ | P2 | ✅ done (2026-07-10), plus a real `free_subtree` teardown/leak bug found+fixed along the way |
| 3 | ~~`#5` B3 (partial): `resolve_chain_sorted` copy avoidance~~ | P2 | ✅ done (2026-07-10) |
| 4 | `#20` New-member install streaming (`SnapshotService` + join/catch-up flow) | P2 | Deferred — genuine new distributed-systems feature, needs its own session |
| 5 | `#20` Cross-replica durable watermark (leader + ≥1 peer, vs. today's safe `group_safe_slot` approximation) | P2 | Deferred — touches Paxos heartbeat protocol for a no-correctness-upside refinement |
| 6 | `#5` B3 (full): borrowed `Slice`s all the way out to `scan`/`collect_in_order`/GC | P2 | Deferred — each of the 3 callers needs its own borrowed-lifetime argument; no profiling data motivating the extra win over the copy-avoidance already landed |
| 7 | `#5` B4 Step 2 (`ct_alloc`/`ct_free` shared allocator, true zero-copy) | P2 | Re-assessed, still deferred — would need the cell `header_reserve` layout to become a stable, exposed ABI contract; Step 1 already captures most of the win |
| 8 | `#5` B5/B6 (size-classed pool, RDMA-pinned alloc) | P2 | Re-assessed, still blocked — design doc requires profiling-driven size classes (none exist); no RDMA backend exists at all |
| 9 | `#8` rename `flush()` → `create_snapshot()` | P2 | Re-assessed, still skipped — not a pure rename (flush never persists); doing it correctly means merging flush+snapshot, a real behavioral change that would regress `apply()`'s hot path |

**Rationale:** all high-risk C++ concurrency work is done (#5 B1–B3 partial,
#3, #9, #12, #13, #11 including its `ct_scan_async` follow-up), #14 (former
largest item) and #17 D3 are fully done, and every remaining open item
(#4–#9) has been concretely re-assessed this session and confirmed
correctly deferred/blocked — each for a specific, non-mechanical reason
(a real design decision needed, a missing real-world input like profiling
data, a missing backend, or a separately-scoped distributed-systems
feature) rather than simply unattempted.

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
