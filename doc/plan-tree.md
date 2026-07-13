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
- [ ] Snapshot/GC wiring per `design-crowtree-snapshot-gc.md` (restart
      resume, new-member install via `snapshot_export`/`import`) — plan.md
      P3 M4. The frontier-seeding mechanism above is what makes this safe to
      land now: periodic `snapshot()` calls will make `resume_from_slot()`
      actually non-zero on a real restart, which is the only thing missing
      for the boot-path wiring above to give a real O(1)-since-last-snapshot
      restore instead of always doing a full WAL replay in practice.

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

- [ ] `get()`/`scan()` return borrowed `buffer` + slot for L1 hits; owned copy for L0 hits
- [ ] Owning `get`/`multi_get`/`scan` become wrappers (zero-copy get + `clone` + release guard)
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

### #11. Async FFI Bridge — io_uring Reactor `P1`

**Design:** [`design-crowtree-async.md`](design/design-crowtree-async.md). OQ8 resolved.
**Files:** `c_api.h/.cc`, `ffi/src/lib.rs`, new `reactor.h`, `reactor.cc`

**Lock scope (critical):** `AsyncPageStore` enables `create_snapshot()` (#8) to
persist dirty pages to disk **without holding `write_mutex_`**. The Flusher
submits io_uring write SQEs, then processes CQEs on the reactor thread. The tree
remains fully available for reads and new writes during the entire persistence
phase. This is the final piece that eliminates `write_mutex_` from the I/O path.

- [ ] C++ reactor: single-thread io_uring event loop (`reactor.h/.cc`)
  - `Reactor` class: owns one io_uring instance, runs `io_uring_enter` / `peek_cqe` in a loop
  - Submit SQEs for demand-load reads and flush/snapshot writes
  - On CQE completion, invoke the registered callback for the corresponding `ct_future`
  - Runs on a dedicated C++ thread (not a pool — one reactor thread per `Crowtree`)
- [ ] C API: add async variants
  - `ct_future* ct_get_async(ct_tree*, const uint8_t* key, size_t klen)` — fast path returns `done=1`; slow path submits SQE, returns `done=0`
  - `ct_future* ct_flush_async(ct_tree*)` — always async
  - `ct_future* ct_snapshot_async(ct_tree*)` — always async
  - `ct_status ct_future_poll(ct_future*, int* done, ct_buf* out_value, uint64_t* out_slot)` — non-blocking poll
  - `void ct_future_free(ct_future*)` — cancel + free if not completed
  - Notification: reactor writes to an `eventfd` registered with Tokio's `AsyncFd`
- [ ] Rust FFI: replace `AsyncCrowtree` with true `Future` implementations
  - `CtGetFuture` implements `std::future::Future` — polls `ct_future_poll`; on `done=0` registers waker via `AsyncFd`
  - Remove all `spawn_blocking` calls from `AsyncCrowtree`
  - Fast path (in-memory hit): completes synchronously in first `poll()` — zero overhead
  - Slow path (I/O): pending → woken by eventfd → next `poll()` reads result
- [ ] Zero-copy fast-path value: `ct_get_async` returns borrowed pointer (into frame bytes) + epoch guard lifetime; Rust copies into `Vec<u8>` before dropping the guard
- [ ] Tests: verify fast-path completes without blocking; verify slow-path (cache miss) completes via reactor; verify flush/snapshot async completion

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

### #17. Buffer Pool — Live-Engine Wiring `P1` — 🔎 AUDITED 2026-07-02, mostly DONE

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
- [ ] **Real #17 remaining (unblocked — #5 B3 is done):** migrate
      `resident()` demand-load and `evict_clean_leaves` onto
      `BufferPool::pin`/`pin_new`/CLOCK so the pool owns residency (not
      `acquire_frame` + Crowtree-level eviction).
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

### #22. Raw Block-Device `PageStore` (`BlockPageStore`) `P1`

**Design:** `design-crowtree-persistence.md §2` (backend abstraction:
`FilePageStore` + `BlockPageStore`, one abstraction covering raw SSD/SCM via a
pluggable medium driver; RDMA deferred). **Status (found in review,
2026-07-08):** `FilePageStore` (local file, pread/pwrite + fdatasync) is
implemented and in production use; only a raw block-device backend is
missing — `MemPageStore` is explicitly a test placeholder ("the in-memory
`BlockPageStore` for tests"), not a real block-device driver.
**Files:** `page_store.h/.cpp`, new `block_page_store.h/.cpp`, `CMakeLists.txt`.

- [ ] `BlockPageStore`: open a raw block device (or a pre-allocated file with
      `O_DIRECT`) with IU alignment matching the device's logical sector size;
      `write_at`/`read_at` respect `O_DIRECT` alignment (offset/length/buffer
      alignment) with a bounce-buffer fallback for unaligned callers.
- [ ] `sync()` → `fdatasync`/`fsync` (or the appropriate raw-device barrier).
- [ ] Capacity/geometry probe (device size via `ioctl(BLKGETSIZE64)` on Linux
      block devices; a regular pre-allocated file falls back to `stat`).
- [ ] Tests: run the existing `PageStore` test matrix (`page_store_test.cpp`)
      against `BlockPageStore` backed by a loopback device or a large
      `O_DIRECT`-opened file; alignment edge cases (sub-sector writes).
- [ ] RDMA medium driver stays **future work** (no immediate task) — same
      `PageStore` interface, deferred per the design.

**Sequencing:** independent; can land whenever a real deployment target
(SSD/SCM) needs it. Not on the critical path for #14/#17/#18 (backend-agnostic).

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
#20 resume_from_slot() frontier-seeding ................... ✅ (inert until
     snapshot/GC wiring below lands -- nothing calls snapshot() yet)
#17 buffer pool live wiring ────────── unblocked (#5 B3 done)
#18 incremental snapshot (D4) ──────── folds into #14d
#11 async FFI (io_uring reactor) ───── unblocked (#7 + #5 B3 + #3/#8 core
     done); removing write_mutex_ from flush drain / snapshot persist waits on it
#5 B3 remaining (borrowed-buffer zero-copy value return) ── low-risk refinement
#5 B4 FFI single-alloc ──► #5 B5/B6 (pool, RDMA) — P2, profile-driven
#14 mapping table redesign ──► needs #11 + #17 + #18 (14a done, 14b/c/d/e open)
#14 ───► #16 native frame snapshot format (shares segment image concept)
#22 raw block-device PageStore ── independent, can land anytime
```

### Recommended order for what's left

| Step | Item | Priority | Why here | Effort | Risk |
|-----:|------|:--------:|----------|--------|------|
| 1 | **Snapshot/GC wiring** (`design-crowtree-snapshot-gc.md`) | P1 | Makes #20's `resume_from_slot()` non-zero on a real restart -- the frontier-seeding it needs is already done | Med | Med |
| 2 | **#17 real remaining** — migrate `resident()`/eviction onto pool `pin`/CLOCK | P1 | Unblocked (#5 B3 done); contained, C++-internal | High | High |
| 3 | **#5 B3 remaining** — borrowed-`buffer` zero-copy value return | P0 | Memory-efficiency refinement, not a correctness/locking concern | Med | Low |
| 4 | **#18 D4** — writer-owned `DirtyTracker` | P1 | Align with #14d rather than build standalone | Med | Med |
| 5 | **#11** — io_uring async FFI reactor | P1 | Needs #7 + #5 B3 + #3/#8 (all done); highest FFI complexity | High | High |
| 6 | **#22** — raw block-device `PageStore` | P1 | Independent; land whenever a real SSD/SCM target needs it | Med | Low |
| 7 | **#14 mapping table redesign** | P1 | Needs #11 + #17 + #18; largest remaining effort | High | High |
| 8 | **#5 B4/B5/B6** (FFI single-alloc, memory pool, RDMA) | P1/P2 | Profile-driven / backend-driven, future | — | — |
| 9 | **#16 native frame snapshot** | P2 | After #14; performance optimization, future | Med | Low |

Rationale: the C++ concurrency batch (buffer/epoch/double-buffering) that was
this plan's highest risk is now entirely done (#5 B1–B3 core, #3, #9, #12,
#13), and #20 (crowtree engine wired into a real `crowkv-server` boot, plus
the Paxos-frontier-seeding resume optimization) is now fully done. What's
left splits into (a) the snapshot/GC wiring that makes #20's resume
optimization actually fire on a real restart, (b) contained C++-internal
follow-ups (#17, #5 B3 remainder), and (c) the large, attended-session items
(#11, #14) that were always multi-session efforts.

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
always doing a full replay (#20, fully done — plan.md still gates the actual
runtime benefit on the snapshot/GC wiring item, since nothing calls
`snapshot()` periodically yet) — plan-doc cleanup (condensed every finished
task's write-up) done alongside.

225 C++ tests + 8 FFI tests + `crowkv`/`crowkv-server` workspace tests (100
`wal`, 2 `startup_test`, rest unchanged) pass; ASan + TSan clean (TSan needs
`setarch $(uname -m) -R` in this environment — unrelated ASLR/mmap-mapping
issue, not a regression); `cargo clippy --workspace --all-targets` + `cargo
fmt --check` clean; full `cargo test --workspace` green aside from the
pre-existing `e2e_three_node_cluster_kv_put_batch_delete` flake (100% pass
rate in isolation, confirmed unrelated).

**Next up:** see "Recommended order for what's left" above — snapshot/GC
wiring first (makes #20's resume optimization actually fire), then #17's
real remaining migration, then the rest in priority order.
