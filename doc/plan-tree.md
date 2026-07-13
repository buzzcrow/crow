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

**Completed:** #1 FFI migration, #2 API redesign, #6 STL rename, old-slot support,
clang-format, convenience methods (`put`/`del`/`batch_put`).

---

## Overview Layer

Tasks related to the engine abstraction, FFI boundary, and Rust adapter.

### #7. Epoch Ownership — Move into Crowtree `P0` — ✅ DONE (2026-07-01)

**Design:** `design-crowtree.md` D-Q9, core §10.
**Shipped:** `EpochManager epoch_` moved into `Crowtree` (declared last, so
destroyed first); `CrowtreeEnv` deleted entirely (`env.h`/`.cpp` removed;
ctor/`open()` no longer take it); C API + ~23 tests updated. 180 C++ + 6 FFI
tests pass; ASan + TSan clean.

### #8. Snapshot & Flush — Unified Design `P0`

**Design:** `design-crowtree.md` §4.1 / D-Q11, core §6.2 / §9.
**Files:** `crowtree.h`, `crowtree.cc`, `options.h`, `snapshot.h`

**Deviation (D-R1):** `snapshot_view()` still does an O(N) materialized
traversal (now epoch-guarded, not `write_mutex_`-guarded — see below), not the
design's zero-copy pinned `RootVersion`. Fixing that is blocked on the
`EpochManager::Guard` thread-bound issue (see Open Issues).

- [ ] Rename `flush()` → `create_snapshot()` — **deliberately skipped
      (2026-07-08):** pure rename with real ripple risk (C API, Rust FFI,
      every test) for no functional benefit; `flush()`/`snapshot()` stay
      separate, stable-named methods for now. See Open Issues.
- [x] `snapshot_view()` drops `write_mutex_` for an epoch guard — **DONE
      (2026-07-08).** `collect_in_order` walks the leaf chain via
      `right_sibling` (same technique/argument as #5 B3's `scan()` fix);
      `at_slot` is captured *before* the walk so a racing `flush()` can only
      make it see *more* than `at_slot` promises, never less. Still a
      materialized `vector<leaf_entry>` copy, not a true zero-copy pin (see
      Open Issues). New `Stress.ConcurrentSnapshotViewDuringChurnNoCorruption`.
      215/215 tests; ASan + TSan clean.
- [x] `compare()`/`iter_all()`/`snapshot_export()` — no change needed; all
      read through `snapshot_view()` and inherit the fix above.
- [ ] Remove `write_mutex_` from `snapshot()`/`create_snapshot()` persistence
      phase (needs async `PageStore`, #11).
- [x] Dual trigger (size + time) — **satisfied by the background auto-flush
      thread below**; `flush_interval_ms` is the time-based secondary trigger
      alongside the existing size thresholds (`memtable_flush_bytes`/`_entries`).
- [x] Background auto-flush thread — **DONE (2026-07-08).** Timer thread
      (`Options.background_flush`/`flush_interval_ms`) calls the existing
      synchronous `flush()` periodically (reuses its own locking). See
      `background_flush_test.cpp` (ASan + TSan clean). Does not yet remove
      the write-stall during flush (see item above).

### #8a. Snapshot Export API Cleanup — Remove `at_slot` `P0` — ✅ DONE (2026-07-01)

**Files:** `c_api.h/.cpp`, `snapshot_io.h/.cpp`, `ffi/src/lib.rs`, test files

Removed the unused `at_slot` parameter end-to-end (C API, C++
`snapshot_export_begin`/`snapshot_dump_to_file`, Rust `snapshot_export()`) —
historical export was never supported, so this was dead surface. Tests
updated across `snapshot_export_test.cpp`/`c_api_test.cpp`/`ffi_test.rs`.

### #15. Reject Oversized Keys at `apply()` Entry `P0` — ✅ DONE (2026-07-01)

**Files:** `crowtree.cpp` (`apply`), `options.h`, `crowtree.h`, tests

Key-size check (`Options.max_key_size`, default frame-dependent) funnels
through `apply()`, so `put`/`del`/`batch_put`/C API all inherit it. Tests:
`oversized_key_test.cpp` (5 cases). **Behavior change:** oversized keys are
now rejected instead of heap-falling-back (updated the old
`DurableEdgeCases.OversizedKeyHeapFallbackReopen` test accordingly).

### #20. Wire `CrowtreeEngine` into `crowkv` (plan.md P3 M1/M2) `P0`

**Design:** `design-crowtree.md §4` (async `KVEngine`/`EngineView`), §5a (Rust
adapter). `mem_kv::InMemKV` and `CrowtreeEngine` are the two `KVEngine`
implementations behind the same trait.
**Files:** root `Cargo.toml`, `crowkv/Cargo.toml`, `crowkv/src/kv/kv_engine.rs`,
`crowkv/src/kv/mem_kv.rs`, `crowkv/src/kv/crowtree_engine.rs`, the learner,
`crowtree/ffi/src/lib.rs`.

- [x] `crowtree/ffi` moved into the workspace `members` (was `exclude`) —
      DONE (2026-07-08); unifies `Cargo.lock` resolution.
- [x] `ct_apply_batch` C API + Rust `apply_batch`/`BatchOp` — DONE
      (2026-07-08, prereq); one atomic call into `Crowtree::apply` instead of
      looping single-key applies (which would break `KVEngine::apply`'s
      whole-batch-atomic contract).
- [x] `crowtree_engine.rs::CrowtreeEngine` — DONE (2026-07-08), **sync
      scope**: wraps the synchronous `crowtree_ffi::Crowtree`, implements the
      existing synchronous `KVEngine` trait; `PxLearner::with_engine(...)`
      added. Known caveat: `iter_all`/`compare` read `snapshot_view()`
      (L1-only), so a slot stuck behind an out-of-order gap is invisible to
      them until the gap fills (unlike `InMemKV`). `clear()` is
      `unimplemented!()` (no native wipe primitive yet; no real caller today).
- [x] Shared `KVEngine` conformance suite (`tests/kv/conformance.rs`) +
      `crowtree_engine_test.rs` + cross-engine parity test — DONE
      (2026-07-08). 27/27 `kv` tests pass; clippy + fmt clean.
- [ ] **Deferred:** upgrading `KVEngine` to the async trait (`design-
      crowtree.md §4`) — crowtree has no real async I/O yet (`#11`), so this
      would only add a `spawn_blocking` hop today for a large call-site
      ripple (Paxos learner + `PxKvStore` gRPC handlers). Revisit after #11.
- [x] **Wire `CrowtreeEngine` into the `crowkv-server` boot path — DONE
      (2026-07-08).** `--kv-engine {memory,crowtree}` (default `memory`) +
      `--data-root` CLI flags; `KvStoreRegistry.kv_engine`/`data_root` carry
      the selection to both the CLI bootstrap path (`main.rs`) and the
      management-API dynamic group-creation path (`mgmt_api.rs::add_group`)
      so they can't disagree. `startup::store_crowtree_path(data_root,
      store_id, group_id)` gives each group its own durable file
      (`{data_root}/store{N}/group{M}.ctdb`, parent dir created on first
      boot). New `PxLocalReplica::restore_from_replay_with_engine(id, role,
      replay, engine: Box<dyn KVEngine>)` — `restore_from_replay` is now a
      thin wrapper calling it with `Box::new(InMemKV::new())`, so all
      existing restore tests are an unchanged-behavior regression check.
      Tests: `crowkv/tests/wal/replay_tests.rs`'s new
      `restore_from_replay_with_engine_uses_injected_engine` (in-memory
      `CrowtreeEngine` injected, same assertions as the existing `InMemKV`
      restore test); `crowkv-server/tests/startup_test.rs`'s new
      `create_group_with_wal_crowtree_engine_persists_across_restart` (real
      on-disk file, drop + rebuild the group, KV state survives).
- [x] **`resume_from_slot()` — skip re-replaying the already-durable WAL
      prefix — DONE (2026-07-08).** New `KVEngine::resume_from_slot(&self)
      -> u64 { 0 }` default trait method: "every slot `<= S` is already
      durably reflected, safe to skip". `CrowtreeEngine` overrides it via
      `crowtree_ffi::Crowtree::last_applied_slot` — confirmed (reading
      `crowtree.cpp`'s `apply`/`flush`/`recompute_contiguous_locked`) this
      is a genuinely *contiguous* durable watermark (folds
      `received_slots_` forward from crowtree's own frontier at `flush()`
      time), not just "the max slot ever seen", so it's a safe resume floor.
      `restore_from_replay_with_engine` reads it once, up front (before any
      `apply` calls in this process — the trait doc spells out why that
      matters), then Pass 2 starts its walk at `resume_from + 1` instead of
      `1`, seeding the learner's frontier to `(resume_from,
      term-at-resume_from)` via new `PxLearner::seed_resume_frontier`
      (bypassing `update_frontier`'s sequential/out-of-order-map advance)
      using the just-Pass-1-rebuilt acceptor's entry at that exact slot for
      the term.
      **Found while implementing — no safe "fall back to full replay"
      exists once past the floor.** The initial design considered falling
      back to a full replay from slot 1 if the acceptor had no accepted
      entry at exactly `resume_from` (an assumed-impossible WAL/engine
      mismatch). Testing that fallback against a real `CrowtreeEngine`
      caught a real bug in the *test's assumption*, not the code: crowtree's
      `MemTable::durable_floor` (set from the exact `contiguous_slot_` value
      at `flush()` time) rejects **any** write at `slot <= floor`
      regardless of key — stronger than the per-key highest-slot-wins
      `KVEngine::apply` documents. So "fall back to replaying the skipped
      prefix" doesn't actually work for this engine: the write is silently
      dropped, not idempotently reapplied. Fixed by never re-attempting
      anything `<= resume_from` (always skip to `resume_from + 1`); if the
      acceptor lookup for the seed term comes up empty, the frontier is
      just left at the fresh learner's conservative default (`0`) instead
      of guessed — under-reporting `contiguous_chosen`/`last_chosen_term`
      only costs more conservative heartbeat catch-up / safe-read bounds,
      never incorrectness (see `candidate_log_up_to_date` in
      `local_replica.rs`, which gates actual vote-granting safety off the
      **acceptor's** `accepted_log_tip()`, not these learner watermarks —
      so this whole mechanism is a safe-read/catch-up-efficiency concern,
      not a Paxos safety one, contrary to this doc's note in the prior
      session).
      Tests: `crowkv/tests/wal/replay_tests.rs`'s new
      `restore_from_replay_with_engine_resumes_from_last_applied_slot`
      (pre-flush a real `CrowtreeEngine` past slot 1, verify the restored
      replica's full KV state *and* frontier watermarks exactly match what
      a full sequential replay would have produced) and
      `restore_from_replay_with_engine_falls_back_when_resume_slot_has_no_accepted_entry`
      (the defensive-mismatch case, verifying the skip-not-fallback
      behavior and the conservative under-reported frontier).
      Currently inert in production: nothing yet calls `snapshot()`
      periodically on a live `CrowtreeEngine` (`last_applied_slot()` only
      advances durably via an explicit `snapshot()` persisting the
      superblock — see `persist.cpp` — not just `flush()`), so a real
      restart still sees `resume_from_slot() == 0` until the item below
      lands.
      100 `crowkv` `wal` tests (was 97) + 2 `crowkv-server` `startup_test`
      tests (was 1) pass; full `cargo test --workspace` green (module-level
      and isolated reruns) aside from the same pre-existing flaky
      `e2e_three_node_cluster_kv_put_batch_delete` noted under #21 (passes
      100% in isolation; only flakes under heavy parallel-test-binary
      contention, confirmed unrelated to this change); `cargo clippy
      --workspace --all-targets` + `cargo fmt --check` clean.
- [x] **Snapshot/GC wiring — periodic engine durability + WAL GC — DONE
      (2026-07-08).** Per `design-crowtree-snapshot-gc.md` §§1/4/5. New
      `KVEngine` trait methods `persist_snapshot`/`set_gc_watermark`/
      `collect_garbage` (default no-ops, matching `resume_from_slot`'s
      pattern); `CrowtreeEngine` overrides delegate to
      `crowtree_ffi::Crowtree::snapshot`/`set_gc_watermark`/
      `collect_garbage`. New `cluster::group_maintenance` module: a
      per-group loop (`PxGroup::start_engine_maintenance_loop`, spawned
      alongside the election driver in `px_kv_store.rs`, sharing
      `tenure_cancel` so `PxGroup::shutdown` stops both) that every 30 s
      (1) persists a durable engine snapshot (purely local — every
      replica, leader or follower, does this independently), then (2)
      advances the engine's GC watermark / sweeps it and (3) runs a WAL
      segment GC pass, both gated on `PxGroup::group_safe_slot` (cross-
      replica-safety sensitive, `0` until established). Also fixed
      `wal::gc::run_gc_pass`'s long-standing `u64::MAX`-placeholder
      `safe_slot` (flagged with a `// TODO` in the code) to take the real
      value as a parameter (`spawn_gc_worker`'s own doc already promised a
      `safe_slot` provider closure — its signature just never actually had
      one; now it does).
      **Found while testing — two real, non-obvious gaps closed:**
      (1) `Crowtree::snapshot()` only persists the already-durable *L1*
      tree; it does not drain L0 itself the way `flush()` does, so calling
      it right after a burst of `apply()`s without an intervening `flush()`
      silently persisted a stale (often all-zero) `last_applied_slot` —
      `CrowtreeEngine::persist_snapshot` now flushes first. (2) group-wide
      `safe_slot` (`PxGroup::group_safe_slot`) is only ever computed on the
      **leader** (via heartbeat replies) — gating a replica's own
      `persist_snapshot` call on it would mean follower replicas never get
      periodic durability at all, defeating half the point; the local
      snapshot step is now unconditional and only the cross-replica-unsafe
      steps (GC watermark/sweep, WAL segment GC) stay `safe_slot`-gated.
      Tests: `crowkv/tests/wal/gc_tests.rs`'s new
      `gc_pass_is_bounded_by_safe_slot_not_just_snapshot_slot`;
      `crowkv/tests/group/maintenance_test.rs` (new file, 2 tests) —
      `maintenance_pass_persists_snapshot_and_gcs_wal_segments_once_safe`
      (real file-backed `CrowtreeEngine`, verifies the full
      snapshot→watermark→WAL-GC chain end to end) and
      `maintenance_pass_does_not_gc_wal_when_safe_slot_lags_snapshot` (a
      lagging voting peer holds WAL GC back even though this replica's own
      engine is far ahead). 48 `crowkv` `group` tests (was 46) + 101 `wal`
      tests (was 100) pass; full workspace build/test/clippy/fmt clean.
      **Not yet done** (larger, separable follow-ups, not required for the
      restart-efficiency win above): new-member install streaming via
      `snapshot_export`/`import` wired into the reconfiguration/
      `SnapshotService` flow (design §§2/6); a dedicated cross-replica
      "durable on leader + >= 1 peer" watermark (today `set_gc_watermark`'s
      `snapshot_slot` input is conservatively approximated by
      `group_safe_slot` itself — see `group_maintenance`'s module doc);
      configurable maintenance tick (hardcoded
      `group_maintenance::DEFAULT_MAINTENANCE_TICK` today).

**Sequencing:** independent of the C++-only concurrency/storage tasks in this
file; can be worked in parallel. High priority since it's the only path that
makes the substantial C++ engine work reachable from the running system.

---

## Memory Layer

Tasks related to `buffer` abstraction, zero-copy pipeline, and BufferPool.

### #5. Unified Buffer Design — Single Allocation, Zero-Copy Pipeline `P0`

**Design:** [`design-crowtree-memory.md`](design/design-crowtree-memory.md), `design-crowtree.md` D-Q8.
**Files:** new `buffer.h`, `cell.h`, `memtable.h`, `crowtree.cc`, `page.h`, `c_api.h/.cc`, `ffi/src/lib.rs`

**B1 — buffer core** `P0` — ✅ DONE (2026-07-02)
- Owned/borrowed `buffer` (move-only, `header_reserve`, `clone()`,
  glibc-malloc allocator seam) with small-buffer optimization (≤24 B inline,
  no malloc — needed so replacing `std::string` on the write path doesn't
  regress small keys/values). 16 unit tests (`buffer_test.cpp`); 209 Debug +
  ASan-clean.
- Note: threading it through the write path is **B2**, read path is **B3**.

**B2 — write path on buffer** (sequence with #9) `P0`

Split into independently-verifiable increments (each built + tested +
ASan-clean + committed separately):

- **B2a — buffer cell encoders (additive, zero churn).** `encode_cell_buf`/
  `encode_overflow_cell_buf` return a `buffer` with the header written in the
  reserved prefix + value copied after — one allocation. `CellView` already
  reads any byte range so it works unchanged on `buffer::slice()`.
- **B2b — MemTable → `absl::btree_map<std::string, buffer>`** ✅ DONE
  (2026-07-02). `mem_entry.cell` is a move-only `buffer`; key stays
  `std::string` (a B-tree relocates key slots on split/merge, which a
  move-only key can't satisfy, and `std::string` SSO already inlines small
  keys). `upsert`/`try_emplace`/`get`/`drain`/`snapshot` updated
  accordingly; `apply_batch` builds cells with `encode_cell_buf` (single
  alloc). 209 Debug + ASan + 6 FFI pass.
- **B2c — `leaf_entry.cell` → `buffer`** ✅ DONE (2026-07-02). `flush()`
  moves the drained cell buffer straight into `leaf_entry` (no string shim);
  the leaf frame copy is the only remaining cell copy (page construction
  itself). `buffer::operator Slice()` keeps `Slice(e.cell)` call sites
  unchanged. Updated `page_codec`, `snapshot_io`, `split_leaf` (compiler
  caught a real copy here), overflow spill/materialize. 209 Debug + ASan +
  TSan + 6 FFI pass.
- **B2d — FFI boundary single-alloc.** `ct_apply_*` allocs the key/cell
  `buffer`s once at the C boundary and moves them down (Option A); sets up
  B4 (shared-allocator ownership yield) with no further call-site changes.

**B3 — zero-copy read** (depends on #7 epoch-in-tree; subsumes #4) `P0`

**Lock scope change:** `get()`/`scan()` use no `write_mutex_` — epoch guard +
lock-free atomic mapping-table loads. Readers never block writers and vice
versa.

- [x] **`get()` returns borrowed `Slice` + slot for L1 hits, owned copy for
      L0/overflow — DONE (2026-07-08).** New `GetView`/`Crowtree::get_view()`
      (`crowtree.h`/`.cpp`): owns an `EpochManager::Guard` + a `Slice` that
      either borrows directly into a resident L1 leaf's frame (non-overflow
      cell) or points at an owned `buffer` (L0 hit — a MemTable cell isn't
      epoch-protected the same way a resident frame is; or an overflow
      value — assembled from multiple pages, no single frame to borrow
      from). `Crowtree::get()` is now a thin wrapper: `get_view()` + clone
      `value()` into the output `std::string` + let the guard drop,
      preserving its existing owned-copy contract for every other caller
      (`multi_get`, the C API's `ct_get`) unchanged.
      **Scope note:** `scan()`'s equivalent (`scan_view()`/borrowed
      `scan_entry_view`s) is *not* done here — `scan()`'s L1 resolution
      already funnels through `resolve_chain_sorted` (an owned
      `vector<leaf_entry>`, unavoidably materializing to merge delta chains
      / resolve highest-slot-wins across possibly multiple frames per key),
      so a genuine zero-copy `scan_view` would need that resolver itself
      restructured to return borrowed views — a larger, separate change
      with its own blast radius (also used by GC/snapshot walks) left as a
      follow-up rather than folded into this pass.
      Tests: new `ReadPath.GetView{NotFound,L0HitIsCorrect,
      L1HitBorrowsFrameSurvivingConcurrentEviction,OverflowValueIsMaterialized}`
      (`read_path_test.cpp`) — the `...SurvivingConcurrentEviction` case is
      the key safety proof: a `GetView` held open across an aggressive
      `evict_clean_leaves()` pass that unloads/retires the exact frame it
      borrows from must still read the correct value, since the epoch guard
      it owns keeps that frame's memory alive regardless. 230 C++ tests
      (was 226) pass; ASan + TSan clean; full `cargo test --workspace`
      (FFI relinks against the rebuilt `libcrowtree.a`, no Rust/API changes
      needed — `get()`'s signature and contract are unchanged) green aside
      from the pre-existing cross-binary e2e flake (confirmed unrelated,
      passes in isolation and in the full `crowkv-server` suite run alone).
- [ ] **`scan()` zero-copy (`scan_view`)** — deferred; see scope note above.
- [x] **Remove `write_mutex_` from `scan()` — DONE (2026-07-08).** Walks L1
      leaf-by-leaf via `right_sibling` (starting at `find_leaf_page_id`'s
      target for `prefix`), merge-cursored against an L0 snapshot, instead of
      a full-tree DFS under a lock. Correctness argument: `split_leaf_locked`/
      `try_merge_leaf_locked` always publish new content and repoint
      `right_sibling` in an order such that a chain walk under one epoch
      guard never skips or double-visits a live entry mid-SMO — full writeup
      in `crowtree.cpp::scan()`'s header comment. New
      `Stress.ConcurrentScanDuringChurnNoCorruption`. 213/213 tests; ASan +
      TSan clean.
- [x] `get()` — verified already lock-free (epoch guard only), no change needed.

**B4 — Rust FFI** `P1`
- [ ] Step 1 (Option A): C API accepts raw ptrs, `buffer::alloc()`+copy at boundary
- [ ] Step 2 (Option B, future): `ct_alloc`/`ct_free` shared allocator, ownership yield, true end-to-end zero copy

**B5/B6 — future** `P2`
- [ ] Profile KV size distribution → size-classed memory pool behind the `buffer` seam
- [ ] RDMA-pinned allocation (with the RDMA backend)

---

## Async FFI Layer

Tasks related to the io_uring reactor and completion-based async protocol.

### #11. Async FFI Bridge — io_uring Reactor `P1` — 🔎 SCOPED 2026-07-08

**Design:** [`design-crowtree-async.md`](design/design-crowtree-async.md) §13
(detailed phased plan + UT examples, added 2026-07-08). OQ8 resolved.
**Files:** `c_api.h/.cc`, `ffi/src/lib.rs`, new `reactor.h`, `reactor.cc`

**Found while scoping (2026-07-08) — no consumer without a companion
effort.** `AsyncCrowtree` (the `spawn_blocking` bridge this replaces) has
**zero production callers** — `crowkv`'s `CrowtreeEngine` deliberately
bypasses it because `KVEngine` is a synchronous trait used as `Box<dyn
KVEngine>`, and `PxLearner` calls it inline from already-async gRPC
handlers. Completing this task's 5 phases alone would not fix the (real,
already-live) problem of `CrowtreeEngine::get()` blocking a Tokio worker on
a synchronous `pread` during a demand-load miss — a companion plan,
[`design-crowkv-async-kvengine.md`](design/design-crowkv-async-kvengine.md),
scopes making `KVEngine`/`PxLearner`/the gRPC read path actually consume
this. Central tension resolved there: native `async fn` in a `dyn`-used
trait isn't compatible without `async-trait`-style per-call boxing (which
would regress the fast path's zero-overhead goal right back) — the
recommended fix is a `KVFuture<T>` enum (`Ready(T)` / `Pending(Pin<Box<dyn
Future>>)`) that keeps `KVEngine` plain-`fn`/`dyn`-compatible while still
letting the rare I/O path be genuinely async. See that doc's §7 for why it
should land *before* this task (independently valuable, de-risks this
larger effort).

- [x] **`KVFuture<T>` trait-shape landed (2026-07-09).** `KVEngine::apply/
      get/scan` return `KVFuture<T>` (new `crowkv/src/kv/kv_future.rs`);
      `InMemKV`/`CrowtreeEngine` construct `KVFuture::ready(...)` only (no
      `Pending` yet — no reactor exists). **Re-scoped mid-implementation:**
      the design doc's original caller-side plan (`Learner::learn` → async,
      `.await` through `PxLearner`/gRPC) turned out to have a **75+-call-site
      ripple** across 13 test files for a `Pending` case that's structurally
      impossible today — deferred (see
      [`design-crowkv-async-kvengine.md`](design/design-crowkv-async-kvengine.md)
      §5's revision). `PxLearner`'s three methods + `Learner::learn` stay
      synchronous via `KVFuture::into_ready()` (panics loudly if ever handed
      a real `Pending`, so the deferred conversion has a hard trigger).
      Ripple confined to `kv_engine.rs`/`mem_kv.rs`/`crowtree_engine.rs`
      + 4 test files needing `.into_ready()` on their direct `KVEngine`
      calls. 5 new tests (`kv_future_test.rs` + one `Ready`-variant
      regression guard each in `mem_kv_test.rs`/`crowtree_engine_test.rs`).
      Zero behavior change — full `crowkv`/`crowkv-server` suite (unchanged
      pass counts) + clippy + fmt clean.

**Lock scope (critical):** `AsyncPageStore` enables `create_snapshot()` (#8) to
persist dirty pages to disk **without holding `write_mutex_`**. The Flusher
submits io_uring write SQEs, then processes CQEs on the reactor thread. The tree
remains fully available for reads and new writes during the entire persistence
phase. This is the final piece that eliminates `write_mutex_` from the I/O path.

- [x] **Phase 0/1 landed (2026-07-09).** `liburing` added as a
      `[target.linux-64.dependencies]` pixi dep (Linux-only conda-forge
      package, no osx-arm64 build exists) + `crowtree/CMakeLists.txt`
      `find_path`/`find_library` probe gated behind `CROWTREE_HAVE_LIBURING`
      (source-excludes `reactor.cpp`/`file_async_page_store.cpp`/
      `reactor_test.cpp` entirely when not found, rather than compiling
      them to no-ops — matches design §10's macOS note). `Reactor`
      (`crowtree/include/crowtree/reactor.h` + `src/reactor.cpp`): a
      dedicated thread wrapping `io_uring_queue_init`, `submit_read/write/
      fsync` (mutex-guarded SQE production + callback map, any thread may
      call), `cancel` (best-effort map-erase), `run()` (bounded
      `io_uring_wait_cqe_timeout` poll loop, drains all ready CQEs per wake,
      `eventfd` notify once per batch). Non-throwing by design (matches
      the codebase's `Status`-based error convention over exceptions): a
      `valid_` flag gates a safe synchronous-error fallback if
      `io_uring_queue_init`/`eventfd()` fail in the constructor.
      `AsyncPageStore`/`FileAsyncPageStore` (`async_page_store.h` +
      `file_async_page_store.cpp`): `FilePageStore`'s async twin, bridging
      `Reactor`'s raw `int` CQE result to `Status` (single-shot per op, no
      short-read/write retry loop yet — documented as a Phase-later
      revisit if real usage needs it). 8 new tests (5 `Reactor` + 3
      `FileAsyncPageStore`, `crowtree/tests/unit/reactor_test.cpp`,
      matching design §13's 5 Reactor cases plus extra
      `FileAsyncPageStore` coverage). Fully additive — nothing else in
      crowtree constructs a `Reactor` yet. 243/243 `crowtree_tests` pass
      on the plain, ASan, and TSan builds; `ct-lint`/`ct-fmt` clean.
      **Recommended stopping point** if only one phase lands — Phase 2
      (C API async variants) needs this to exist.
- [x] **Phase 2 landed (2026-07-09).** C API async variants (`ct_get_async`/
      `ct_flush_async`/`ct_snapshot_async`/`ct_future_poll`/`ct_future_free`/
      `ct_reactor_eventfd`), plus their `Crowtree` engine-side twins
      (`get_async`/`flush_async`/`snapshot_async`). `get_async` reuses
      `get_view()`'s existing fast-path resolution (`#5 B3`) via a new
      `try_get_view_no_load`, only falling to a real reactor round trip
      (`get_async_attempt`, retry loop) on a demand-load miss. `ct_open`
      now constructs a `Reactor` + `FileAsyncPageStore` alongside the
      existing synchronous `FilePageStore` whenever a durable path is
      opened on a `CROWTREE_HAVE_LIBURING` build. `ct_future_impl`: a
      `std::shared_ptr`-owned handle with a single `std::atomic<bool> done`
      acquire/release handoff (one writer-then-reader pair per future, no
      mutex needed) — safe to `ct_future_free()` before the underlying I/O
      completes (the completion lambda captures the future by
      `shared_ptr`, so freeing the caller's handle just drops one
      refcount).
      `snapshot_async()` required refactoring `snapshot()`'s single
      inline walk-and-write into `prepare_snapshot_locked()` (synchronous
      DFS walk + delta-fold + encode, deferring every actual byte write
      into a new `PreparedSnapshot{page_writes, manifest_write,
      superblock_write}`) followed by `commit_prepared_snapshot()` (marks
      each page durable + bumps `version_`, once its specific write is
      confirmed on disk) — `snapshot()` and `snapshot_write_next_async()`
      (the new io_uring write-chain helper) both now call the same two
      functions, one synchronously and one via a recursive completion
      chain. Fixed a correctness bug found while doing this refactor:
      the original inline code set `PageBase::durable_addr` for a dirty
      page *before* its bytes were confirmed written, which would have let
      `evict_clean_leaves_locked()` (checks only `durable_addr != kNoAddr`)
      evict a page on the async path whose data hadn't landed yet — fixed
      by deferring that assignment to `commit_prepared_snapshot()`, keyed
      off page identity (not just page_id) so a concurrent consolidate/
      flush/split racing the same generation is handled safely (skip, not
      corrupt). Also replaced a `std::mutex` held across the async chain's
      completion (which can legitimately fire on a different thread than
      the one that locked it — undefined behavior for `std::mutex`) with
      `snapshot_inflight_`, a plain `std::atomic<bool>` spin-gate
      serializing overlapping `snapshot(_async)` generations against each
      other (the `SpaceAllocator`-reuse hazard this guards against is
      documented on `snapshot_async`'s doc comment in `crowtree.h`). 4 new
      tests (`async_get_test.cpp`, matching design §13 Phase 2's cases:
      fast-path hit, eviction-forced reactor round trip, a
      `ct_future_free`-before-completion ASan regression case, and flush/
      snapshot-async completion timing). 247/247 `crowtree_tests` pass on
      the plain, ASan, TSan, and UBSan builds; `ct-lint`/`ct-fmt` clean.
- [x] **Phase 3 landed (2026-07-09).** `AsyncCrowtree::get`/`flush`/
      `snapshot` now drive `ct_get_async`/`ct_flush_async`/
      `ct_snapshot_async` directly via a new `drive_ct_future` — zero
      `spawn_blocking` for these three (the exit criterion); `apply_put`/
      `apply_delete`/`put`/`del` keep `spawn_blocking` since Phase 2 has no
      async C API twin for them (re-scoped from the original "zero
      spawn_blocking calls" wording, which predates Phase 2's actual, more
      limited scope). **Re-scoped from the design's `CtGetFuture`/
      `CtVoidFuture` manual `Future` impls** to a plain `async fn` +
      `tokio::sync::Notify` fan-out, after two real bugs surfaced building
      the manual-`Future` version: (1) one `AsyncFd` registration per
      pending future double-registers the same eventfd with epoll
      (`EEXIST`, since epoll rejects adding a fd already added); (2) even a
      *single shared* `AsyncFd`, polled from multiple tasks via
      `poll_read_ready`, silently hangs all but the most-recently-polling
      task -- that method keeps only one reserved waker slot (its own doc
      comment says as much; `readable()`/`Notify`-based fan-out is the
      supported multi-waiter path). Fix: exactly one lazily-spawned pump
      task per `Crowtree` owns the sole `AsyncFd`, fanning out every
      eventfd wakeup to any number of concurrently-`.await`ing callers via
      `Notify::notify_waiters()` (constructing `notified()` *before*
      re-checking `ct_future_poll`, matching `Notify`'s documented
      race-free ordering); the pump is aborted in `Crowtree::drop` before
      `ct_close` tears down the Reactor. `RawFdView` wraps the eventfd
      without taking closing ownership (Reactor-owned, per the OQ8
      resolution in `design-crowtree-async.md`). Tests: 3 `ffi_test.rs`
      additions -- fast-path-resolves-on-first-poll (manual single poll
      with `Waker::noop()`, deterministic rather than timing-based),
      eviction-forced slow path, and N
      concurrently-`tokio::spawn`ed gets (the regression case for both bugs
      above; stress-verified 30/30 + 15 full-suite runs with no hangs).
- [x] **Phase 4 landed (2026-07-09).** `ct_get_async`'s fast path (the
      *first*, fully synchronous `try_get_view_no_load` attempt -- no I/O,
      same thread start to finish) now hands its resolved `GetView` straight
      through `Crowtree::get_async`/`ct_get_async`'s callback into
      `ct_future_impl`, guard and all, instead of materializing an owned
      `std::string` immediately. `ct_future_poll` returns a `ct_buf` that
      *borrows* directly from `GetView::value()` for that case (no malloc),
      and deliberately does **not** free the `ct_future` handle for any
      `kGet` future (found or not) -- the caller must always follow up with
      `ct_future_free` once done reading `out_value`, which is what finally
      releases the epoch guard. A genuine miss (resolved after crossing to
      the Reactor thread) still materializes an owned copy and releases its
      guard immediately via the new `materialize_owned()`, *before* handing
      off to `on_done` -- `EpochManager::Guard` is thread-bound (its own doc
      comment: "must be released on the thread that created it"), so a
      cross-thread resolution can never defer this the way the fast path
      does. `get_async_attempt` threads a new `same_thread` bool through
      every recursive call to track which case it's in.
      **Real bug found via ASan while landing this:** `GetView`'s defaulted
      move ctor/assignment blindly copied the `value_` `Slice` field
      byte-for-byte, but `owned_` (a `buffer`) relocates its bytes on move
      when small enough to be inline (SBO) -- so a moved-then-read `GetView`
      whose value was owned (not borrowed from a frame) could dangle
      pointing at the pre-move object's now-gone inline storage. Never
      manifested before Phase 4 because nothing previously moved a `GetView`
      itself across a boundary (every prior caller read `value()` and
      converted to `std::string` before any move could happen). Fixed with
      an explicit move ctor/assignment that re-derives `value_ =
      owned_.slice()` after relocating, whenever `owned_` is non-empty.
      Rust side: `try_poll_ct_future` gained a `FutureKind` parameter
      (Get/Flush/Snapshot) so it knows Get's different freeing contract;
      `copy_buf` (copy without `ct_free_buf`, unlike `take_buf`) plus an
      explicit `ct_future_free` call replace the implicit "poll already
      freed it" assumption for that one case.
      Tests: `async_get_test.cpp`'s two existing cases updated for the new
      contract (`ct_future_free` instead of `ct_free_buf` on the now-
      possibly-borrowed value -- calling `ct_free_buf` on it is exactly the
      bad-free ASan catches), plus a new
      `FastPathValueSurvivesRepeatedPollsUntilExplicitFree` regression case
      (polling a resolved kGet future repeatedly before freeing must be
      safe/idempotent). 248/248 `crowtree_tests` pass on plain, ASan, TSan,
      UBSan; `ffi_test.rs`'s existing 11 cases pass unchanged (the Rust-
      visible behavior/API didn't change, only what happens underneath);
      concurrent-gets stress-verified 30/30 again post-change.
- [x] **Phase 5 landed (2026-07-09).** ASan/TSan/UBSan: 247/247 (248/248
      after Phase 4 added one test) `crowtree_tests` pass on all three
      sanitizer builds, both before and after Phase 4. No Rust-side
      sanitizer run: this toolchain only has `stable`, and
      `-Zsanitizer=*` needs nightly; every concurrency/lifetime bug this
      session actually hit (Phase 3's eventfd hang/EEXIST, Phase 4's
      `GetView` move dangling-pointer) was caught by ASan (C++ side) or a
      stress loop (Rust side), which is the coverage that mattered here.
      **Benchmark** (`crowtree/ffi/examples/async_get_bench.rs`, N=2000,
      release build, resident-hit fast path vs. evict-before-every-call
      slow path, before/after Phase 4):
      | | fast (new) | fast (old, `spawn_blocking`) | speedup |
      |---|---|---|---|
      | Before Phase 4, 2 B value | 331–345 ns | 6.8–7.3 µs | ~20x |
      | After Phase 4, 16 B value | ~333 ns | ~6.75 µs | ~20x |
      | After Phase 4, 512 B value | ~354 ns | ~8.8 µs | ~25x |
      | After Phase 4, 8 KiB value | ~936 ns | ~11.5 µs | ~12x |

      Phase 4's own incremental contribution is real but modest at these
      sizes, honestly reported rather than cherry-picked: a small
      (SBO-inline, <=24 B) value's extra copies were already nearly free
      before Phase 4 (a `malloc`+memcpy of a few bytes is noise), and even
      at 8 KiB, removing 1-2 extra copies only costs on the order of a
      microsecond, dwarfed by the ~6-11 µs `spawn_blocking` hop that
      dominates the "old" path at every size tested. Phase 3 (removing that
      hop entirely for a resident hit) remains the overwhelming majority of
      the win; Phase 4 tightens the margin further but doesn't change the
      order of magnitude. Slow path (genuine demand-load miss, both paths
      wait on the same io_uring read either way) stays within noise of each
      other at every size, as expected -- neither phase changes how the
      slow path resolves.
- [x] **Phase 6 landed (2026-07-09)** (new, not in the original design —
      added once the consumer-gap above was found). `CrowtreeEngine::get`
      (already `KVFuture`-shaped by the companion plan) now constructs a
      real `KVFuture::Pending` for a genuine demand-load miss: a new
      `crowtree_ffi::AsyncCrowtree::try_get` does one synchronous
      `ct_future_poll` attempt via the existing (private)
      `try_poll_ct_future`/`FutureKind::Get` machinery and returns
      `GetOutcome::Ready` if that already resolved it (the fast path,
      zero allocation, same as before this phase) or `GetOutcome::Pending`
      -- a boxed future finishing the drive via `drive_ct_future` -- for a
      genuine miss. `CrowtreeEngine` was rewired to hold an `AsyncCrowtree`
      internally (was a bare `Crowtree`) so this new fast/slow split has
      the shared `Arc<Crowtree>`/eventfd-pump machinery Phase 3 already
      built to drive the slow path off the calling thread. `scan`/`apply`
      deliberately stay `Ready`-only: crowtree has no `ct_scan_async`/
      `ct_apply_*_async` C API yet (design-crowtree-async.md §4's table),
      so there is nothing for either to genuinely wait on today -- an
      honest, documented gap, not an oversight.

      **This is the phase where #11 starts mattering to `crowkv` in
      production**; everything before it was infrastructure. Landing it
      required the full caller-side ripple `design-crowkv-async-kvengine.md`
      §5 had deferred exactly until this moment: `Learner::learn` -> `async
      fn`, `PxLearner`'s three methods -> `async fn`, `PxLocalReplica`/
      `PxKvStore` call sites -> `.await`, and a 15-file test migration
      (`#[test]` -> `#[tokio::test]` + `.await`) found via a fresh grep, not
      reused from any stale estimate. User explicitly chose **full async
      conversion** over a bounded-synchronous-wait alternative once a real
      `Pending` case existed to decide with. Full details, including the
      real `GetView` move-ctor bug this phase's own Phase 4 dependency
      surfaced and the exact caller-side change list, are in
      `design-crowkv-async-kvengine.md` §7's updated sequencing log --
      not duplicated here.

      Two new regression tests close the design doc's own previously-
      deferred gaps: `kv/crowtree_engine_test.rs`'s
      `get_constructs_pending_for_genuine_demand_load_miss` (engine layer)
      and `paxos/learner_async_test.rs` (one layer up, through
      `PxLearner::engine_get`) -- both evict a durable engine's resident
      leaf and assert the resulting `KVFuture::Pending`/awaited value are
      both correct, not just that `Pending` gets constructed.

      Verification: full `cargo test --workspace` -- every `crowkv`/
      `crowtree-ffi` test passes consistently across repeated runs
      (confirmed via multiple full and isolated re-runs); `crowkv-web`'s
      network-integration tests show pre-existing, unrelated flakiness
      under parallel execution (a different test fails each full-workspace
      run, always passes alone) -- a real, pre-existing environment issue,
      not a regression introduced here. `cargo clippy --workspace
      --all-targets` clean (one new `type_complexity` lint on
      `GetOutcome::Pending`'s necessarily-nested `Pin<Box<dyn Future<...>>>`
      silenced the same way this codebase already silences it elsewhere).

**Sequencing:** land
[`design-crowkv-async-kvengine.md`](design/design-crowkv-async-kvengine.md)
first (independent, lower-risk, plumbing-only — establishes every
`KVFuture`/`.await` boundary with `Pending` never constructed yet), then
Phases 0–6 above in order. Each phase is its own session/PR; this is the
riskiest concurrency surface added since #12/#13 (a dedicated OS thread
doing kernel-level I/O completion dispatch), so no phase should skip
ASan/TSan before starting the next.

---

## Core Layer (Tree & Epoch)

Tasks related to MemTable, B+tree, epoch, and mapping table.

### #3. MemTable — Double Buffering (Active + Flushing) `P0` — ✅ DONE (2026-07-08)

**Design:** core §6 (MemTable), ties to #8 background flush.
**Files:** `crowtree.h` (`active_`/`frozen_`/`memtable_mutex_`, full design in
their member comment), `crowtree.cpp`, `options.h` (`max_memtable_count`),
`tests/integration/double_buffer_test.cpp`.

**Shipped:** replaced the single `MemTable memtable_` with
`shared_ptr<MemTable> active_` + `deque<shared_ptr<MemTable>> frozen_`
(oldest-first queue, not a single `flushing_` slot, so >2 buffers falls out
naturally) behind a dedicated `memtable_mutex_` held only for the pointer/
queue values, never while reading/writing/draining a table's contents.
`maybe_swap_active()` (was `maybe_flush()` — no longer drains) freezes
`active_` and installs a fresh one once size/entry thresholds trip — a fast
pointer swap, no B+tree work. `flush()` force-freezes whatever's in
`active_`, then drains every queued `frozen_` table's `slot <=
contiguous_slot_` entries into L1; any leftover non-contiguous entries are
re-`upserted` into the live `active_` (highest-slot-wins keeps this safe,
possibly bouncing through several freeze cycles under sustained
out-of-order delivery). `Options.max_memtable_count` (default 2) bounds
total buffer count; at capacity, a threshold-triggered freeze is skipped
(`active_` keeps growing) rather than stalling the writer — `flush()`'s
force-freeze always bypasses the cap. Tested up to 4 buffers.

**Key deviation (found necessary, not a shortcut):** out-of-order slot
delivery means a key can be live in more than one MemTable generation with
different slots, so `get()`/`scan()` check **every** live table and keep the
highest-slot cell (`cell_wins`), rather than assuming "newest table wins" as
originally sketched — see `DoubleBuffer.GetAndScanResolveHighestSlotAcross
OutOfOrderFreezeBoundary`. `write_mutex_` keeps its pre-#3 scope (drain +
tree mutation still serializes against itself); fully removing it from the
drain path is `#11` territory (async flush).

**Tests:** `tests/integration/double_buffer_test.cpp`, 5 cases incl. a
concurrent-readers-vs-frequent-freeze/drain stress test. 225 C++ tests (was
220); ASan + TSan clean; `crowtree_ffi` (8) + `crowkv` `kv` suite (27) pass
unchanged (public API didn't change).

### #9. MemTable — Map Choice: `absl::btree_map` `P0` — ✅ DONE (2026-07-01)

**Design:** `design-crowtree.md` D-Q10, core §1.

Replaced `std::map<...>` with `absl::btree_map<std::string, std::string,
std::less<>>` (keys/values stay `std::string`; `buffer` migration is #5
B1/B2). `emplace`/`erase(it)`/heterogeneous `find` verified. Benchmark
(`crowtree_bench`, `-DCROWTREE_BENCH=ON`) validated the choice:
`absl::btree_map` beats `std::map` 2.6× on ordered scan, 1.65× on get-hit at
100k; folly `ConcurrentSkipList` is slower single-threaded. 181 C++ + 6 FFI
tests pass.

### #12. Lock-Free EBR for `EpochManager` `P1` — ✅ DONE (2026-07-02)

**Design:** `design-crowtree-core.md §10.1`

Per-thread `Participant` slots (cache-line padded, lazily allocated, pushed
lock-free onto `participants_`). `enter()` = seq_cst load global epoch +
publish to the thread's slot (reentrant); `Guard::release()` = a
release-store 0 on outermost exit, no reclamation on the reader path.
`retire()`/`try_reclaim()` scan for the min active epoch and free retired
pages below it (writer-only, off the hot path). New
`ConcurrentReadersDerefRetiredNoUAF` stress test. 188 Debug + 188 ASan; all
8 Epoch tests TSan-clean. Landed ahead of #5 B3 since the read path already
took a guard on every `get()`/`scan()`.

### #13. Make `install_snapshot` Safe for Lock-Free Readers `P0` — ✅ DONE (2026-07-02)

**Files:** `crowtree.cpp` (`install_snapshot`, `free_subtree`), `crowtree.h`

Readers were already lock-free, so `install_snapshot`'s immediate `delete`
in `free_subtree()` was a live use-after-free. Fixed: `free_subtree(page_id,
retire)` epoch-retires (`retire=true`, clears the mapping slot first so new
readers see "gone") instead of deleting immediately; `retire=false` kept for
teardown/recovery (no concurrent readers). Test
`SnapshotExport.ConcurrentReadersDuringImportNoUAF` (4 readers vs. 5×
import). 189 Debug + 189 ASan + 189 TSan pass.

**Note:** a fully *consistent* swap (no transient-empty-tree window) needs a
staged `RootVersion` swap — deferred; this fix only guarantees safety
(no UAF/wrong data), not zero read disruption during the swap.

### #14. Mapping Table Redesign — Segment Recycling + Incremental Persistence `P1`

**Design:** [`design/design-crowtree-mappingtable.md`](design/design-crowtree-mappingtable.md). Workable spec: packed slot word, segment image + directory + A/B anchor, snapshot/recovery ordering.

**Key decisions:**
- PID recycling: **NO** — race condition risk too high
- Segment recycling: **YES** — free empty segments via epoch deleter
- Sparse segments: **acceptable** — 8 KB waste per segment
- Incremental persistence: **YES** — replace full manifest with segment-level persistence
- Backend abstraction: **YES** — all I/O via `PageStore` interface

**14a — Packed slot word + segment struct** `P1`
- [x] Packed 64-bit slot word: `0`=empty, `bit0=0`=resident `PageBase*`, `bit0=1`=unloaded `(iu_index, iu_count)`; pack/unpack helpers + unit tests — **DONE** (`mapping_slot.h`, `mapping_slot_test.cpp`; standalone, adopted by #14b)
- [ ] `Segment { atomic<uint64_t> slots[kSegSlots]; atomic<uint32_t> live_count; atomic<uint32_t> generation; atomic<bool> dirty; }`
- [ ] `Options.mapping_segment_slots` (default 1024, fixed per tree)
- [ ] **Cleanup (found in review, 2026-07-08):** remove the dead PID-recycling
      path (`MappingTable::free_page_id()` / `free_list_` in
      `mapping_table.h`/`.cpp`, plus its two tests `FreeAndRecycle` /
      `FreePidClearsUnloadedSlot` in `mapping_table_test.cpp`) before wiring
      14b. It contradicts D1 ("No PID recycling") above and is unused by
      `Crowtree` today (merged-away PIDs currently just leak, matching the
      design's "P1 — PID leak" problem statement) — don't let it get
      accidentally wired up once the table is otherwise rewritten.

**14b — Segment recycling (needs #5 B3 + #13)** `P1`
- [ ] Epoch deleter clears slot → `live_count.fetch_sub` → CAS segment to nullptr + `epoch.retire` when 0
- [ ] Writer-owned dirty-set + per-segment dirty bit; reader loading nullptr segment / empty slot returns "gone" and retries from root

**14c — On-disk format (needs #17 + #18)** `P1`
- [ ] Segment image: header + `uint64_t packed[slot_count]` + CRC (≈8 KB)
- [ ] Segment directory image: `DirEntry{seg_idx, generation, image_addr, image_len, image_crc}[]` + CRC
- [ ] Commit anchor: tiny fixed A/B record → `{seq, root_pid, leftmost_leaf_pid, last_applied_slot, next_page_id, segment_slots, segdir_addr/len/crc, page_alloc_root, crc}`

**14d — Snapshot + recovery** `P1`
- [ ] Snapshot order: dirty frames → dirty segment images → directory → `flush()` → anchor → `flush()` → clear dirty
- [ ] Recovery: pick highest-valid anchor → read directory → read segment images → memcpy packed words into slots → set root/next_page_id/last_applied_slot; pages demand-loaded lazily
- [ ] Old image cleanup: two-generation pending-free list

**14e — Tests** `P1`
- [ ] Unit: packed-word round-trip, image/directory/anchor CRC round-trip
- [ ] Crash recovery: before/after anchor, torn image, torn anchor A/B, highest-seq selection
- [ ] Segment recycling under split/merge churn (TSan/ASan); stale-reader-sees-empty
- [ ] Incremental cost: only dirty segments + directory written; backend parity (mem + file); demand-load after reopen
- [ ] `FaultyPageStore` harness (drop/tear/reorder writes at a chosen point) for crash-injection recovery tests

**Sequencing:** 14a/14b need #5 B3 (lock-free readers + epoch retire) and #13
(epoch-safe slot clearing). 14c/14d need #17 + #18 (pool-owned frames + durable
per-frame `PageAddr`) and async PageStore (#11).

---

## Persistence Layer

Tasks related to PageStore, snapshot, recovery, and on-disk format.

### #17. Buffer Pool — Live-Engine Wiring `P1` — ✅ DONE (2026-07-08, D3 optional/open)

**Design:** [`design-crowtree-persistence.md §4.5`](design/design-crowtree-persistence.md) (PT6c-5.1–5.4).
**Files:** `buffer_pool.h/.cpp`, `crowtree.cpp` (`resident`, `evict_clean_leaves_locked`,
`maybe_evict_locked`), `page.h` (`FrameStore`), `mapping_table.*`, `options.h`

**Audit result:** 5.1 (pool owns base frames), 5.2 (epoch-deferred frame
free), 5.3 (mapping slot tagging + demand load), 5.4 (clean-base eviction +
re-tag) are all already implemented, just via an interim `acquire_frame` +
Crowtree-level-eviction model rather than the pool's own `pin`/CLOCK engine.
Tests present: `eviction_test.cpp`, `buffer_pool_test.cpp`,
`incremental_checkpoint_test.cpp`.

- [x] **D1 — RESOLVED: keep the pin/CLOCK engine** (`buffer_pool.h`'s
      `pin`/`pin_new`/`mark_dirty`/`flush_dirty` + CLOCK write-back are the
      *designed* target engine, fully unit-tested — not dead code). Do not delete.
- [x] **D2 — RESOLVED: keep the deterministic 64 MiB default** for
      `Options.buffer_pool_bytes`; no auto-RAM sizing (keeps tests
      deterministic). Servers tune it up (e.g. ~25% RAM).
- [x] **Real #17 remaining — DONE (2026-07-08), via a deliberate deviation
      from the literal design (see below).** Eviction candidates are now
      ranked by genuine access recency instead of arbitrary DFS order:
      `PageBase` gained an atomic `last_touch_tick` (`page_types.h`),
      stamped with a single relaxed `fetch_add`/`store` on every
      `resident()` touch (both the hot resident-hit path and the cold
      demand-load path); `evict_clean_leaves_locked` sorts its existing
      DFS-gathered candidate set by that stamp ascending and evicts the
      genuinely-coldest pages first, instead of whichever DFS visited
      first.
      **Deviation from the literal ask ("migrate onto `BufferPool::pin`/
      CLOCK") — found while designing, confirmed by tracing the actual
      code:** `FrameStore::~FrameStore()` already calls
      `pool_->release_frame(...)`, so a resident page's frame is *already*
      pinned for its full object lifetime via `acquire_frame`/epoch-deferred
      delete — `BufferPool::pin`/`pin_new` (the page_id-keyed, CLOCK-`ref`-
      bit-tracked path) is genuinely unused by tree code today, only
      `acquire_frame` (anonymous frames) is. Making `pin`'s CLOCK `ref` bit
      meaningful would require every `resident()` hit to take `BufferPool`'s
      `mu_` mutex to set it — regressing the lock-free read path #5 B3/#12/
      #13 were built to establish. The atomic-touch-tick approach achieves
      the identical functional goal ("residency/eviction driven by real
      access recency, not arbitrary order") with a single relaxed atomic
      store per access instead — `BufferPool::pin`/CLOCK itself is
      unchanged (still available, still unit-tested, still not the tree's
      residency path) — this is the same kind of resolved deviation as D1/D2
      above, just found in this pass rather than the 2026-07-02 audit.
      Tests: new `Eviction.RecentlyTouchedLeafSurvivesEvictionOverColderOnes`
      (`eviction_test.cpp`) — a `CountingPageStore` wrapper proves a
      recently-re-touched leaf survives an aggressive eviction pass that a
      colder, untouched leaf does not (demand-load count only increases for
      the colder key on next access). 226 C++ tests (was 225) pass; ASan +
      TSan clean; full `cargo test --workspace` (FFI links against the
      rebuilt `libcrowtree.a`) green; `cargo clippy` + `cargo fmt --check`
      clean (no Rust changes needed — this is a pure C++ change).
- [ ] **D3 (optional)** — extend eviction to inner/overflow bases (currently
      clean **leaf** bases only) if profiling shows it matters.

### #18. Incremental Snapshot — Durable Frame Addrs + Dirty Tracking `P1` — 🔎 AUDITED 2026-07-02, partially DONE

**Design:** [`design-crowtree-persistence.md §4.3/§5A`](design/design-crowtree-persistence.md) (PT6d).
**Files:** `persist.cpp` (`snapshot` / `persist_one` / `walk`), `crowtree.cpp`,
`page.h` (`PageBase::durable_addr`/`durable_plen`).

**Audit result:** durable per-page addr (`kNoAddr` = dirty) and
write-only-dirty-pages are both already implemented and tested
(`incremental_checkpoint_test.cpp` asserts via `last_snapshot_pages_written()`).

- [ ] **D4 — writer-owned `DirtyTracker` (the real #18 work).** Snapshot still
      **DFS-walks the entire reachable tree** each time (checking `durable_addr` per
      page), so its cost is O(resident tree), not O(dirty). Add a writer-maintained
      dirty-page set (populated at consolidate/split/merge/apply) and have `snapshot`
      iterate that set instead of walking the tree. This dovetails with #14d
      (segment-level dirty bits) — decide whether to build a page-level tracker now
      or fold it straight into #14's segment dirty-set.
- [ ] **D5 (model reconciliation)** — "drop build pins so frames become evictable"
      does not apply as written: frames are anonymous+pinned until the page is
      retired by explicit clean-leaf eviction, not unpinned at snapshot. Reconcile
      with D1's chosen frame model; likely a no-op once D1 lands.
- [ ] **D6 — back-pressure test** under a write storm (eager snapshot) is not present.

**Sequencing:** D4 is the substantive item; align it with #14d rather than duplicating.

### #14 note

The current full-manifest snapshot/recovery is functional and remains as the
fallback until #14 replaces it with segment-level persistence (needs #17 + #18).

### #22. Raw Block-Device `PageStore` (`BlockPageStore`) `P1` — ✅ DONE (2026-07-08)

**Design:** `design-crowtree-persistence.md §2` (backend abstraction:
`FilePageStore` + `BlockPageStore`, one abstraction covering raw SSD/SCM via a
pluggable medium driver; RDMA deferred).
**Files:** `page_store.h` (unchanged interface), new
`block_page_store.h/.cpp` (glob-picked up by `CMakeLists.txt`, no build-file
edit needed).

**Shipped:** `BlockPageStore` opens a raw block device or a pre-allocated
regular file with `O_DIRECT` (macOS: `F_NOCACHE` instead, no `O_DIRECT` open
flag there). `write_at`/`read_at` check alignment of `off`, `len`, **and**
the buffer address (4096-byte, covers real device requirements) — an
aligned call goes straight to `pwrite`/`pread`; an unaligned one bounces
through a `posix_memalign`-allocated scratch buffer spanning the IU-aligned
range, read-modify-write for writes (correctly zero-filling only the
genuinely-never-written tail past current EOF, not clobbering real data —
found and fixed a short-read/EOF-vs-real-failure conflation bug here while
implementing). Geometry probe: `BLKGETSIZE64`/`BLKSSZGET` on a real Linux
block device (overrides the caller-supplied `iu_size` with the true logical
sector size); a regular file falls back to the caller-supplied `iu_size` and
`lseek`-based size. RDMA medium driver stays future work, deferred per the
design (unchanged).

Tests: `page_store_test.cpp`'s new `BlockDevice{AlignedRoundTripAcrossReopen,
UnalignedWriteReadBounces,UnalignedWritePreservesSurroundingBytes,
ReadPastEndFails}` (backed by a regular pre-allocated file — no real block
device available in this environment/CI, but exercises the identical
O_DIRECT alignment code path); `persist_test.cpp`'s new
`Persist.BlockDeviceBackendRoundTrip` (real `Crowtree` engine — apply/flush/
snapshot/reopen/recover — mirroring the existing `FileBackendRoundTrip` test
1:1 against `BlockPageStore` instead of `FilePageStore`, proving crowtree's
actual persist.cpp write/read pattern round-trips through it, not just
synthetic offsets). Confirmed this sandbox's `/tmp`/`/cjdata` are real XFS
(not tmpfs/overlayfs), so `O_DIRECT` is genuinely exercised, not silently
falling back. 235 C++ tests (was 230) pass; ASan + TSan clean; full `cargo
test --workspace` (FFI unaffected — this is a pure C++-internal addition,
no `PageStore` selection is wired through the FFI/Rust boundary yet) green.

**Sequencing:** independent; can land whenever a real deployment target
(SSD/SCM) needs it. Not on the critical path for #14/#17/#18 (backend-agnostic).
Not yet wired into `crowtree_ffi`/`CrowtreeOptions`/`crowkv-server` (those
only ever select `FilePageStore` vs. in-memory today) — a separate,
follow-up integration task once a real deployment target needs it.

---

## Snapshot & GC Layer

Tasks related to snapshot export/import and GC integration.

### #21. GC Sweep + Dual Watermark + `GcStats` `P1` — ✅ DONE (2026-07-08)

**Design:** `design-crowtree.md §4.1`, `design-crowtree-snapshot-gc.md §1/§4`.
**Files:** `crowtree.h/.cpp`, `c_api.h/.cpp`, `ffi/src/lib.rs`.

GC was opportunistic-only (tombstones dropped only as a side effect of
`consolidate()`'s delta-chain trigger or `snapshot()`'s dirty-page rebuild —
a leaf with no further writes could keep a tombstone past `gc_floor_`
forever); `set_gc_watermark` was single-param; no `GcStats`.

**Shipped:** `set_gc_watermark(snapshot_slot, safe_slot)` computes
`gc_floor_ = min(...)`, monotonic as before. New explicit
`Crowtree::collect_garbage()` sweep walks the *resident* tree (peeks
`MappingTable::get` directly — never demand-loads an evicted leaf just to
check GC eligibility, which would defeat #17's eviction) and
force-consolidates any leaf with a resolved tombstone `<= gc_floor_`,
independent of the delta-chain trigger. Returns `GcStats{tombstones_dropped,
pages_freed, bytes_freed}` end-to-end (C++ → C API → Rust). New
`Options.gc_interval_ms` runs it periodically on the existing
background-flush thread. Tests: `tests/unit/gc_test.cpp` (new) — watermark
min/monotonicity, no-op below floor, delete-then-no-writes regression,
skip-evicted-leaves, periodic trigger; new FFI test
`mem_gc_watermark_and_collect_garbage`. 220 C++ (was 215) + 8 FFI (was 7)
tests pass; ASan + TSan clean; full `cargo test --workspace` green.

**Deferred:** "stale root versions" GC (design's second target) — no
`RootVersion` exists yet (see #8's Open Issues entry on
`EpochManager::Guard`), so there's nothing to retire by refcount. `GcStats`
has no `versions_retired` field. The tombstone-sweep half has no real
dependency on a pinned `RootVersion` (it reclaims pages via the same
epoch-based reclamation `consolidate()`/`snapshot()` already use), so it was
safe to land without waiting.

### #16. Native Frame Snapshot Format `P2`

**Files:** `snapshot_io.h/.cc`, `c_api.h/.cc`, `ffi/src/lib.rs`

The streaming snapshot export API currently only supports `kPortable` format
(key-value tuple serialization). A `kNative` format that directly streams page
frame bytes would be significantly faster for crowtree→crowtree transfers (Raft
InstallSnapshot production path).

- [ ] Define native format: leaf/inner frame images + remapped PID manifest
- [ ] Export: stream frame bytes directly (no tuple serialization)
- [ ] Import: load frames directly into mapping table (no entry-by-entry rebuild)
- [ ] Portable format remains available for testing and cross-engine scenarios
- [ ] Tests: native export/import round-trip, verify equivalence with portable

**Sequencing:** After #14 — native format shares the segment image concept.

---

## Test Layer

*No standalone tasks. Test requirements are embedded in each task above as
checkbox items. See [`design-crowtree-test.md`](design/design-crowtree-test.md)
for the overall test strategy.*

---

## Infrastructure

### #19. Terminology — `checkpoint` → `snapshot` (code) `P1` — ✅ DONE (crowtree) (2026-07-01)

**Scope:** crowtree only (consensus/WAL `DedupCheckpoint` is a different,
unchanged subsystem).
**Files:** `c_api.h/.cpp`, `ffi/src/lib.rs`, `persist.cpp`, `crowtree.h/.cpp`, tests.

Renamed end-to-end: `ct_checkpoint`→`ct_snapshot`, `Crowtree::checkpoint()`→
`snapshot()`, `checkpoint_seq`→`snapshot_seq`, Rust `Crowtree::checkpoint()`/
`AsyncCrowtree::checkpoint()`→`snapshot()`. Residual `checkpoint` only in
test/file names (cosmetic). 180 C++ + 6 FFI tests pass.

**Deferred (Q3):** the main-workspace `KVEngine::persist_checkpoint` trait
was **not** renamed here — turned out `crowkv` has no such trait (see #20),
so this was a no-op, not a real deferral.

**Sequencing:** Independent; landed before #14 so new persistence code uses final names.

### #10. C++ Logging — `spdlog` `P0` — ✅ DONE (2026-07-02)

**Design:** `design-crowtree.md` D-Q12.
**Files:** `crowtree/include/crowtree/log.h`, `src/log.cpp`, `options.h`, `tests/integration/logging_test.cpp`

Added `spdlog` (async logger, 8192-entry ring buffer, rotating file 100 MiB
× 5) gated by `CROWTREE_HAVE_SPDLOG` so the Rust FFI `cc` build (no spdlog)
compiles the `CT_LOG_*` macros to no-ops. `Options.log_dir` (empty = off),
`log_level`, `log_max_file_mb`, `log_max_files`. `Crowtree::open()` calls
`init_logging()`; teardown is explicit via `shutdown_logging()` (logging is
process-global, so multiple `Crowtree` instances shouldn't tear down each
other's logger). 182 C++ + 6 FFI tests pass.

**TSan-safe teardown (2026-07-02):** `log.cpp` now owns the async logger +
its `thread_pool` directly (instead of spdlog's global registry/`shutdown()`)
so teardown joins the worker before sinks are freed — fixed a real spdlog
teardown race TSan caught.

**Remaining (deferred, low value):** post-rotate gzip compression, Rust-side
size rotation.

---

## Dependency Graph & Implementation Plan

### Dependency graph (✅ = done)

```
#1 FFI migration .............. ✅   #6 STL rename ................. ✅
#10 logging .................... ✅   #7 epoch-in-tree .............. ✅
#9 btree_map .................... ✅   #5 B1 buffer core ............. ✅
#5 B2 write-path-on-buffer ...... ✅ (B2a/B2d still open, non-blocking)
#5 B3 zero-copy read (get+scan) . ✅   #12 lock-free EBR ............. ✅
#13 install_snapshot epoch-safe . ✅   #3 double buffering ........... ✅
#8 background flush thread ...... ✅   #21 GC sweep + dual watermark . ✅
#8a snapshot export cleanup ..... ✅   #15 reject oversized keys ..... ✅
#19 checkpoint→snapshot ......... ✅

#20 wire CrowtreeEngine into crowkv-server boot path ...... ✅
#20 resume_from_slot() frontier-seeding ................... ✅
#20 snapshot/GC wiring (periodic persist_snapshot + WAL GC) ✅
#17 buffer pool live wiring (recency-ranked eviction) ...... ✅ (D3 optional)
#5 B3 get_view() zero-copy point read ...................... ✅ (scan_view
     deferred -- see #5 B3's scope note)
#18 incremental snapshot (D4) ──────── folds into #14d
#11 async FFI (io_uring reactor) ───── unblocked (#7 + #5 B3 + #3/#8 core
     done); removing write_mutex_ from flush drain / snapshot persist waits on it
#5 B4 FFI single-alloc ──► #5 B5/B6 (pool, RDMA) — P2, profile-driven
#14 mapping table redesign ──► needs #11 + #17 + #18 (14a done, 14b/c/d/e open)
#14 ───► #16 native frame snapshot format (shares segment image concept)
#22 raw block-device PageStore ........................... ✅ (not yet
     wired into crowtree_ffi/crowkv-server -- lands when a real SSD/SCM
     deployment target needs it)
```

### Recommended order for what's left

| Step | Item | Priority | Why here | Effort | Risk |
|-----:|------|:--------:|----------|--------|------|
| 1 | **#11** — io_uring async FFI reactor | P1 | Needs #7 + #5 B3 + #3/#8 (all done); highest FFI complexity | High | High |
| 2 | **#14 mapping table redesign** (folds in #18 D4's `DirtyTracker`) | P1 | Needs #11 + #17 + #18; largest remaining effort | High | High |
| 3 | **#5 B4/B5/B6** (FFI single-alloc, memory pool, RDMA) | P1/P2 | Profile-driven / backend-driven, future | — | — |
| 4 | **#16 native frame snapshot** | P2 | After #14; performance optimization, future | Med | Low |
| 5 | New-member install streaming (`snapshot_export`/`import` ↔ `SnapshotService`) | P2 | Separable follow-up noted under #20's snapshot/GC wiring; needs the reconfiguration flow | Med | Med |
| 6 | **#17 D3 (optional)** — extend eviction to inner/overflow bases | P2 | Only if profiling shows leaf-only eviction isn't enough | Low | Low |
| 7 | **#5 B3 `scan_view`** (deferred) — zero-copy scan, needs `resolve_chain_sorted` restructured | P2 | Larger blast radius (also used by GC/snapshot walks); `get_view` covers the point-read case | Med | Med |
| 8 | **#22 FFI/server wiring** (deferred) — expose `BlockPageStore` through `crowtree_ffi`/`CrowtreeOptions`/`crowkv-server` | P2 | Only needed once a real SSD/SCM deployment target exists | Low | Low |

Note: `#18` D4 (writer-owned `DirtyTracker`) isn't listed as its own step —
it's not safely implementable standalone (see `#18`'s note: a true
incremental dirty tracker needs an incremental on-disk manifest format,
which is `#14`'s territory), so it's folded into `#14`'s effort estimate
above rather than attempted in isolation.

Rationale: the C++ concurrency batch (buffer/epoch/double-buffering) that was
this plan's highest risk is now entirely done (#5 B1–B3 core, #3, #9, #12,
#13), #17's real remaining item is now done (recency-ranked eviction via a
deliberate, documented deviation from the literal pin/CLOCK design -- see
#17), #5 B3's `get_view()` zero-copy point read is done, and #20 (crowtree
engine wired into a real `crowkv-server` boot, the Paxos-frontier-seeding
resume optimization, and periodic snapshot/GC wiring) is now fully done end
to end. What's left splits into (a) the large, attended-session items (#11,
#14) that were always multi-session efforts, (b) the separable
new-member-install streaming piece of the snapshot/GC design that wasn't
needed for the restart-efficiency win, and (c) #22's raw block-device
`PageStore` is also now done (independent, contained, additive), leaving
only its own not-yet-needed FFI/server wiring as a deferred follow-up.

---

## Open Issues

- **`EpochManager::Guard` is thread-bound — blocks a naive zero-copy
  `RootVersion`** (found 2026-07-08 while fixing #8's `snapshot_view()`).
  `Guard::release()` mutates a per-thread, non-atomic `Participant::nest`
  counter — a `Guard` created on one thread and dropped on another (plausible
  for a long-lived `Snapshot`/`RootVersion` handed across FFI or a
  `spawn_blocking` pool) would race, ruling out "pin = hold an open `Guard`
  for the object's whole lifetime". A real zero-copy `RootVersion` needs
  either (a) cross-thread `Guard` release support in `EpochManager` (a change
  to a foundational, TSan-relied-upon subsystem), or (b) a separate
  page-level refcount bumped under a short-lived guard and decremented from
  any thread on drop, with `retire_page` consulting it. Blocks: #8's true
  zero-copy snapshot, #21's deferred stale-`RootVersion` GC target.

---

## Session Status (updated 2026-07-08)

**Completed this session** (see each task section above for full detail):
background flush thread (#8) · lock-free `scan()` (#5 B3) · `CrowtreeEngine`
wired into `crowkv` sync-first (#20) · lock-free `snapshot_view()` (#8) · GC
sweep + dual watermark + `GcStats` (#21) · MemTable double buffering (#3) ·
`CrowtreeEngine` wired into the `crowkv-server` boot path (#20, CLI/config
engine selection + per-group durable file) · `resume_from_slot()`
frontier-seeding so restore skips an already-durable WAL prefix instead of
always doing a full replay (#20) · periodic snapshot/GC wiring — new
`cluster::group_maintenance` loop persists a durable engine snapshot every
30s and drives `wal::gc` off the real group safe-slot, closing the
long-standing `u64::MAX` TODO and making #20's `resume_from_slot()` actually
non-zero on a real restart (#20, now fully done end to end) · #17's real
remaining item — recency-ranked eviction (`PageBase::last_touch_tick` +
`evict_clean_leaves_locked` sorting) instead of arbitrary DFS order, via a
deliberate, documented deviation from literal `BufferPool::pin`/CLOCK
migration (would have regressed the lock-free read path) · #5 B3's
`get_view()`/`GetView` zero-copy point read (`get()` is now a thin wrapper
over it; `scan()`'s equivalent deferred — see its scope note) · #22's
`BlockPageStore` — raw block-device / `O_DIRECT` `PageStore` backend with
alignment bounce-buffering and Linux geometry probing, completing the
backend matrix alongside `MemPageStore`/`FilePageStore` · **#11 scoping** —
found it has zero production consumer without a companion effort (`KVEngine`
is a synchronous, `dyn`-used trait; `AsyncCrowtree` has no real callers
today) and wrote two design docs: new
[`design-crowkv-async-kvengine.md`](design/design-crowkv-async-kvengine.md)
(the `KVFuture<T>` enum design resolving the `dyn KVEngine` vs. `async fn`
tension, plus the full `PxLearner`/gRPC caller-side plan) and
`design-crowtree-async.md` §13 (new: phased C++ reactor / C API / Rust
`Future` implementation plan with concrete unit test examples per phase) —
plan-doc cleanup (condensed every finished task's write-up) done alongside.

235 C++ tests (was 225) + 8 FFI tests + `crowkv`/`crowkv-server` workspace
tests (101 `wal`, 48 `group`, 2 `startup_test`, rest unchanged) pass; ASan +
TSan clean (TSan needs `setarch $(uname -m) -R` in this environment —
unrelated ASLR/mmap-mapping issue, not a regression); `cargo clippy
--workspace --all-targets` + `cargo fmt --check` clean; full `cargo test
--workspace` green aside from the pre-existing cross-binary e2e flake under
heavy parallel-test contention (100% pass rate in isolation and in the full
`crowkv-server` suite run alone, confirmed unrelated). #11's design/plan
work is docs-only this session — no code changes, nothing to test yet.

**Next up:** see "Recommended order for what's left" above and #11's
sequencing note — start with
[`design-crowkv-async-kvengine.md`](design/design-crowkv-async-kvengine.md)
(independent, lower-risk, plumbing-only), then `design-crowtree-async.md`
§13's Phase 0/1 (contract cleanup + standalone `Reactor`), then the rest of
#11's phases and the rest of the list in priority order.
