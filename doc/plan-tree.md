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

**Design:** `design-crowtree.md` D-Q9, core §10. Prerequisite for #5 B3.
**Files:** `crowtree.h`, `crowtree.cpp`, `persist.cpp`, `c_api.cpp`, `env.h`/`env.cpp` (deleted), all tests

- [x] Move `EpochManager epoch_` into `Crowtree` as a `mutable` member, **declared
      last so it is destroyed first** — `~Crowtree` retires the live tree, then
      `epoch_`'s dtor reclaims every retired page while `pool_`/`mapping_` are alive
- [x] `Crowtree` ctor + `Crowtree::open()`: dropped the `CrowtreeEnv& env` parameter
- [x] Deleted `env.h` / `env.cpp` entirely (removed `CrowtreeEnv`)
- [x] Updated the C API (`ct_tree` no longer holds an env) and all ~23 test files
- [x] Added a diagnostics `Crowtree::epoch()` accessor (mirrors `mapping()`) for tests
- [x] Verified: 180 C++ tests + 6 FFI tests pass; **ASan clean, TSan clean**
- Note: the `Env.DefaultSingleton` scaffold test was removed (the type no longer exists).

### #8. Snapshot & Flush — Unified Design `P0`

**Design:** `design-crowtree.md` §4.1 / D-Q11, core §6.2 / §9.
**Files:** `crowtree.h`, `crowtree.cc`, `options.h`, `snapshot.h`

**Current deviation (D-R1):** `snapshot_view()` currently does a full O(N)
materialized traversal under `write_mutex_` — it collects all entries into a
`vector<leaf_entry>`, blocking all writes during the traversal. The design
specifies a pinned `RootVersion` (O(1) refcount pin, zero-copy). This task fixes
the deviation as part of the unified snapshot redesign, together with #3 (double
buffering changes the flush lock scope).

- [ ] Rename `flush()` → `create_snapshot()` (drain L0→L1 + publish root + persist to disk). **Terminology:** crowtree uses **"snapshot"** as the single durability term — there is no "checkpoint". `create_snapshot()` is the whole operation; its durable persist phase is the "snapshot persist" (formerly "checkpoint"). See #19.
- [ ] `snapshot_view()` returns pinned `RootVersion` (refcount, zero-copy, O(1)) — replaces the current O(N) materialized traversal under `write_mutex_`; `Snapshot` class becomes a thin wrapper over a pinned root + epoch guard, not a materialized `vector<leaf_entry>`
- [ ] `compare()` / `iter_all()` / `snapshot_export()` updated to operate on the pinned root instead of the materialized copy
- [ ] Remove `write_mutex_` from `snapshot()`/`create_snapshot()` persistence phase (I/O goes through async PageStore #11; only MemTable swap holds lock #3)
- [ ] Dual trigger: keep size (`memtable_flush_bytes`/`_entries`, primary) + add time (`flush_interval_ms`, secondary; production default ~2 h)
- [ ] Background auto-flush thread (ties into #3 double buffering)

### #8a. Snapshot Export API Cleanup — Remove `at_slot` `P0` — ✅ DONE (2026-07-01)

**Files:** `c_api.h/.cpp`, `snapshot_io.h/.cpp`, `ffi/src/lib.rs`, test files

- [x] C API: remove `at_slot` from `ct_snapshot_export_begin`
- [x] C++ API: remove `at_slot` from `snapshot_export_begin()` and `snapshot_dump_to_file()`
- [x] `snapshot_io.cpp`: delete `at_slot` validation logic and historical pin comments
      (the snapshot's slot is still written into the stream header from the current view)
- [x] Rust FFI: `snapshot_export(&self)` — drop `at_slot` parameter
- [x] Tests: updated `snapshot_export_test.cpp`, `c_api_test.cpp`, `ffi_test.rs` call sites
- [x] Design docs: already state "no `at_slot`; historical export not supported"
      (`design-crowtree-core.md`, `design-crowtree-snapshot-gc.md`) — no change needed

### #15. Reject Oversized Keys at `apply()` Entry `P0` — ✅ DONE (2026-07-01)

**Files:** `crowtree.cpp` (`apply`), `options.h`, `crowtree.h`, tests

- [x] Add key size check in `apply()` (threshold = `frame_bytes / 2`)
- [x] Expose threshold via `Options.max_key_size` (default = frame-dependent)
- [x] Tests: verify rejection, verify normal-size keys unaffected
      (`oversized_key_test.cpp`, 5 cases)
- Note: the check funnels through `apply()`, so `put`/`del`/`batch_put` and the C
  API all inherit it (no separate `c_api.cc` change needed).
- **Behavior change (see Q1 in the questions section):** the old
  `DurableEdgeCases.OversizedKeyHeapFallbackReopen` test (oversized keys
  heap-fell-back and persisted) was updated to assert rejection instead.

---

## Memory Layer

Tasks related to `buffer` abstraction, zero-copy pipeline, and BufferPool.

### #5. Unified Buffer Design — Single Allocation, Zero-Copy Pipeline `P0`

**Design:** [`design-crowtree-memory.md`](design/design-crowtree-memory.md), `design-crowtree.md` D-Q8.
**Files:** new `buffer.h`, `cell.h`, `memtable.h`, `crowtree.cc`, `page.h`, `c_api.h/.cc`, `ffi/src/lib.rs`

**B1 — buffer core** `P0` — ✅ DONE (2026-07-02)
- [x] Create `buffer.h`: owned/borrowed modes, move-only, `header_reserve`, `clone()`,
      glibc-malloc allocator seam (`alloc`/`wrap`/`move_from`, `header(off)`,
      `set_size`, `slice()`, memcmp `operator<`/`==` so it can key an `absl::btree_map`)
- [x] **SBO (small-buffer optimization)** — owned buffers ≤ `kInlineCap` (24 B) live
      inline, no malloc (mirrors `std::string` SSO). *Correctness-for-performance:*
      without it, replacing `std::string` on the write path would regress small
      keys/values (forced malloc where SSO had none). `data()` is computed; moves
      relocate inline bytes. Design updated (`design-crowtree-memory.md §2`).
- [x] Unit tests (`buffer_test.cpp`, 16 cases): alloc/write/read, header-reserve
      layout, set_size, move (heap ptr stable / inline relocates), clone independence
      (heap + inline), wrap = borrowed (never frees — ASan-verified), move_from frees
      once, ordering/equality, slice, SBO inline/heap boundary. **209 Debug + ASan-clean.**
- Note: abstraction only; threading it through the write path (`cell.h`/MemTable/
  flush) is **B2** and the read path is **B3**.

**B2 — write path on buffer** (sequence with #9) `P0`

Split into safe, independently-verifiable increments (each built + tested +
ASan-clean + committed separately). **Do all of them now** (no deferral — the whole
point is the single-allocation write path):

- **B2a — buffer cell encoders (additive, zero churn).** Add `encode_cell_buf` /
  `encode_overflow_cell_buf` returning a `buffer` whose header is written in the
  reserved prefix and the value copied after it — one allocation. `CellView` already
  reads any byte range, so it works unchanged on `buffer::slice()`. Unit tests assert
  byte-equality with the `std::string` encoders. [foundation for B2b/B2c]
- **B2b — MemTable → `absl::btree_map<std::string, buffer>`** ✅ DONE (2026-07-02).
  `mem_entry.cell` is now a move-only `buffer`; the **key stays `std::string`** — a
  B-tree relocates its `const` key slots on split/merge, which a move-only `buffer`
  key can't satisfy (and `std::string` SSO already inlines small keys). `upsert(Slice,
  slot, buffer&&)` moves the pre-encoded cell in (no cell copy); `try_emplace` builds
  the move-only value in place; `get` copies out; `drain` moves the cell + copies the
  key; `snapshot` clones. `apply_batch` builds cells with `encode_cell_buf` (single
  alloc) and moves them into L0. Design §2 corrected (move-only-key limitation).
  **209 Debug + ASan + 6 FFI pass.** (Flush still shims cell→string for `leaf_entry`
  until B2c.)
- **B2c — `leaf_entry.cell` → `buffer`** ✅ DONE (2026-07-02). `flush()` now MOVES the
  drained cell buffer straight into `leaf_entry` (removed the string shim); the leaf
  frame copy is the only remaining cell copy (that copy *is* page construction). Key
  stays `std::string` (same reasoning as B2b). Enablers that kept churn low:
  `buffer::operator Slice()` (implicit view, so `Slice(e.cell)` sites are unchanged),
  `buffer::copy_of(Slice)`, `LeafBase::build` now takes `const&` (reads only). Updated
  `page_codec` (added `Reader::bytes(buffer*)`), `snapshot_io`, `split_leaf` (move
  iterators — the compiler *caught a real copy* here), overflow spill/materialize
  (`encode_overflow_cell_buf`). `buffer` is **move-only by design** → a
  braced-init-list can't hold it, so tests use a variadic `Entries(...)` helper.
  **209 Debug + 209 ASan + 209 TSan + 6 FFI pass.**
- **B2d — FFI boundary single-alloc.** `ct_apply_*` allocs the key/cell `buffer`s
  once at the C boundary and moves them down (Option A). Sets up B4 (shared-allocator
  ownership yield) with no further call-site changes.

**B3 — zero-copy read** (depends on #7 epoch-in-tree; subsumes #4) `P0`

**Lock scope change (critical):** Today `scan()` holds `write_mutex_` for the
entire scan (O(N) under lock). After this task, `get()` and `scan()` use **no
`write_mutex_`** — they acquire an epoch guard and do lock-free atomic loads of
mapping-table slots. Readers never block writers and writers never block readers.

- [ ] `get()`/`scan()` return borrowed `buffer` + slot for L1 hits; owned copy for L0 hits
- [ ] Owning `get`/`multi_get`/`scan` become wrappers (zero-copy get + `clone` + release guard)
- [ ] Remove `write_mutex_` from `scan()` — use epoch guard + atomic loads instead
- [ ] Remove `write_mutex_` from `get()` if any path still holds it (verify no regression)

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

### #3. MemTable — Double Buffering (Active + Flushing) `P0`

**Design:** core §6 (MemTable), ties to #8 background flush.
**Files:** `crowtree.h` (memtable_ field), `memtable.h`

**Lock scope change (critical):** Today `flush()` holds `write_mutex_` for the
entire duration (drain + tree mutation + publish). After this task, the
`write_mutex_` is held **only for the MemTable swap** (microseconds): `active_`
→ `flushing_`, install fresh `active_`, release lock. The Flusher then drains
`flushing_` into L1 as the sole tree writer — readers are not blocked because
they use epoch guard + atomic mapping-table loads (see #5 B3).

- [ ] Replace `memtable_` with `std::shared_ptr<MemTable> active_` + `std::shared_ptr<MemTable> flushing_` behind an atomic/`shared_mutex` swap
- [ ] Swap on `maybe_flush` threshold: move `active_` → `flushing_`, install fresh `active_`
- [ ] `get()`/`scan()` merge order: `active_` (newest) → `flushing_` → L1
- [ ] Non-contiguous slots in `flushing_` after a flush attempt: re-`upsert` them into `active_` (highest-slot-wins keeps this safe)
- [ ] Config: reuse `memtable_flush_bytes`/`memtable_flush_entries`; add optional `max_memtable_count` if >2 buffers are ever wanted
- [ ] Interacts with #8's background auto-flush thread
- [ ] Tests: stress test asserting reads see a consistent overlay while a flush swap is in flight

### #9. MemTable — Map Choice: `absl::btree_map` `P0` — ✅ DONE (2026-07-01)

**Design:** `design-crowtree.md` D-Q10, core §1. OQ2/OQ3 resolved.
**Files:** `memtable.h`, `CMakeLists.txt`, `pixi.toml`, `ffi/build.rs`

- [x] Add `absl` to `pixi.toml` (`libabseil`) + `CMakeLists.txt` (`find_package(absl REQUIRED)`, link `absl::btree`)
- [x] Replace `std::map<...>` with `absl::btree_map<std::string, std::string, std::less<>>` in `memtable.h`
      (kept `std::string` keys/values; `buffer` migration is #5 B1/B2, not yet done)
- [x] `emplace`/`erase(it)`/heterogeneous `find` verified; `get`/`drain_up_to`/`snapshot` unchanged and green (181 C++ tests, 6 ffi tests)
- [x] `ffi/build.rs` adds `$CONDA_PREFIX/include` so the standalone crate finds absl headers
- [x] Benchmark (Q2): added `crowtree_bench` (Google Benchmark, `-DCROWTREE_BENCH=ON`,
      `bench/memtable_bench.cpp`). `absl::btree_map` beats `std::map` on ordered
      scan (2.6×) and get-hit at 100k (1.65×); folly `ConcurrentSkipList` is slower
      single-threaded. Choice validated — full numbers in the Q2 answer below.

### #12. Lock-Free EBR for `EpochManager` `P1` — ✅ DONE (2026-07-02)

**Design:** `design-crowtree-core.md §10.1`
**Files:** `epoch.h`, `epoch.cpp`, `tests/unit/epoch_test.cpp`

- [x] Per-thread participant slot: cache-line-padded `Participant{atomic<uint64_t>
      local_epoch; next; nest}`, lazily allocated on a thread's first `enter()` and
      pushed lock-free onto `participants_`; keyed per-manager by a monotonic id in
      a process-global thread_local cache (no per-`enter` allocation on the hot path)
- [x] `enter()` = seq_cst load global epoch + seq_cst publish to the thread's slot
      (reentrant: only the outermost enter publishes; nested guards share the slot)
- [x] `Guard::release()` = release-store 0 on the outermost exit; **no reclamation on
      the reader path** (writer-driven, per design §10.1)
- [x] `retire()` / `try_reclaim()` scan participant slots for the min active epoch and
      free retired `< min`; the retired list stays under `reclaim_mu_` (writer-only,
      off the hot path)
- [x] Tests: existing invariants preserved; `MultipleGuardsHoldUntilAllExit` rewritten
      to two threads (per-thread EBR gives one epoch per thread); new
      `ConcurrentReadersDerefRetiredNoUAF` stress test (readers deref a shared node the
      writer swaps+retires). **188 Debug + 188 ASan pass; all 8 Epoch tests TSan-clean.**

**Note:** done ahead of #5 B3 (the read path already takes a guard on every
`get()`/`scan()`, so the mutex `enter`/`exit` was already the hot-path contention
point). The seq_cst enter-publish / retire-scan pairing gives the standard EBR
total-order safety; the participant list uses acquire/release (a brand-new reader can
only be missed for objects retired before it entered, which it cannot reference).

### #13. Make `install_snapshot` Safe for Lock-Free Readers `P0` — ✅ DONE (2026-07-02)

**Files:** `crowtree.cpp` (`install_snapshot`, `free_subtree`), `crowtree.h`,
`persist.cpp`, `tests/integration/snapshot_export_test.cpp`

Readers are already lock-free (`get()`/`scan()` take an epoch guard, no
`write_mutex_`), so `install_snapshot`'s immediate `delete` in `free_subtree()`
was a **live use-after-free** — fixed now (not deferred behind #5 B3).

- [x] `free_subtree(page_id, bool retire)`: `retire=true` epoch-retires each page
      (and overflow chain via `retire_overflow_chain_locked`) and clears the mapping
      slot first (new readers see "gone"); `retire=false` keeps immediate `delete`
      for teardown / recovery (no concurrent readers)
- [x] `install_snapshot` calls `free_subtree(root, /*retire=*/true)`; `~Crowtree` and
      the open()-recovery drop call `retire=false`
- [x] Slot cleared before retire so a reader that already loaded a page keeps it under
      its guard; the page frees only once that guard drains (epoch reclamation)
- [x] Test `SnapshotExport.ConcurrentReadersDuringImportNoUAF`: 4 reader threads walk
      B while A's snapshot is imported into B 5×. **189 Debug + 189 ASan + 189 TSan
      pass** (the new test is ASan/TSan-clean).

**Note:** a fully *consistent* swap (readers never observe a transient empty tree)
needs the staged RootVersion swap — deferred; the current fix guarantees safety
(no UAF / no wrong data), only allowing a transient miss during the swap window.

**Sequencing:** done alongside #12 (readers were already lock-free, so this was a
latent UAF rather than a future one). Install snapshot is uncommon (a corrupted
replica is typically removed from the group and re-added fresh, not waited on while
serving reads).

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

**Audit result — what already works (no action needed):**
- **5.1 pool owns base frames — DONE, differently than written.** Every resident
  base page's bytes live in a `BufferPool` frame via `FrameStore::alloc`/`adopt_copy`
  → `BufferPool::acquire_frame` (anonymous, pinned-resident); heap fallback when the
  pool is full so correctness is size-independent. `Crowtree` holds a
  `shared_ptr<BufferPool>` sized by `Options.buffer_pool_bytes`.
- **5.2 epoch-deferred frame free — DONE in effect.** Pages are epoch-retired
  (`retire_page`); the frame returns to the pool in `~FrameStore` (`release_frame`)
  only when the retired `PageBase` is actually reclaimed. No separate
  `FreeFrameDeferred` API is needed.
- **5.3 mapping slot tagging + demand load — DONE.** The mapping table already tags
  slots resident (`PageBase*`) vs unloaded (`unloaded_page*{addr,plen}`);
  `Crowtree::resident()` demand-loads an unloaded slot (read → decode → CRC/validate
  → publish), latching `io_failed_` on fault. (The *packed 64-bit word* form is
  #14a, not this task.)
- **5.4 clean-base eviction + re-tag — DONE at the Crowtree level.**
  `evict_clean_leaves_locked` selects clean, delta-free resident leaves, re-tags the
  slot unloaded, and epoch-retires the page; `maybe_evict_locked` runs it from the
  flush path to keep the cache bounded.
- **Tests present:** `eviction_test.cpp`, `buffer_pool_test.cpp`, `incremental_checkpoint_test.cpp`.

**True remaining delta:**
- [x] **D1 — RESOLVED (2026-07-02): keep the pin/CLOCK engine.** `BufferPool::pin`/
      `pin_new`/`mark_dirty`/`flush_dirty` + CLOCK write-back are **not** dead code —
      they are the *designed* pool-residency engine (demand-load + eviction +
      write-back), fully unit-tested in `buffer_pool_test.cpp`. The live engine
      currently uses the interim `acquire_frame` model; the remaining #17 work is to
      **migrate the engine onto the pin/CLOCK path** (pool-owned demand-load +
      eviction), which needs lock-free readers (**#5 B3**). Do not delete.
- [x] **D2 — RESOLVED: keep the deterministic 64 MiB default** for
      `Options.buffer_pool_bytes`; a server tunes it up (e.g. toward 25% RAM). No
      auto-RAM sizing (keeps tests deterministic and avoids platform code).
- [ ] **Real #17 remaining (needs #5 B3):** migrate `resident()` demand-load and
      `evict_clean_leaves` onto `BufferPool::pin`/`pin_new`/CLOCK so the pool owns
      residency (not `acquire_frame` + Crowtree-level eviction).
- [ ] **D3 (optional)** — extend eviction to inner/overflow bases (currently clean
      **leaf** bases only) if profiling shows it matters.

**Sequencing:** the real #17 migration is after #5 B3; D1/D2 are settled now.

### #18. Incremental Snapshot — Durable Frame Addrs + Dirty Tracking `P1` — 🔎 AUDITED 2026-07-02, partially DONE

**Design:** [`design-crowtree-persistence.md §4.3/§5A`](design/design-crowtree-persistence.md) (PT6d).
**Files:** `persist.cpp` (`snapshot` / `persist_one` / `walk`), `crowtree.cpp`,
`page.h` (`PageBase::durable_addr`/`durable_plen`).

**Audit result — what already works (no action needed):**
- **Durable per-page addr + record — DONE.** `PageBase::durable_addr`/`durable_plen`;
  `kNoAddr` marks a dirty (not-yet-durable) page. `persist_one` assigns a durable
  addr from the crash-safe append/reuse allocator and records `(pid, addr, len)` in
  the manifest.
- **Write only dirty pages — DONE.** `persist_one` writes a page's blob only when
  `durable_addr == kNoAddr`; clean pages keep their prior addr (no rewrite).
  `incremental_checkpoint_test.cpp` asserts only-dirty-pages-written via
  `last_snapshot_pages_written()`.

**True remaining delta:**
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

---

## Snapshot & GC Layer

Tasks related to snapshot export/import and GC integration.

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

**Scope:** crowtree only. Consensus/WAL `DedupCheckpoint` is a different subsystem
and stays unchanged. Docs are already renamed; this task carries it into code.
**Files:** `c_api.h/.cpp`, `ffi/src/lib.rs`, `persist.cpp`, `crowtree.h/.cpp`, tests.

- [x] C API: `ct_checkpoint` → `ct_snapshot` (no `_async` variant exists yet)
- [x] C++: `Crowtree::checkpoint()` → `Crowtree::snapshot()`; `checkpoint_seq` →
      `snapshot_seq`; `ckpt_pages_written_` → `snapshot_pages_written_`;
      `last_checkpoint_pages_written()` → `last_snapshot_pages_written()`
- [x] Rust FFI: `Crowtree::checkpoint()` / `AsyncCrowtree::checkpoint()` → `snapshot()`;
      `sys::ct_checkpoint` → `sys::ct_snapshot`
- [x] Updated all call sites, tests, comments, and error strings; residual
      `checkpoint` only in test/file names (`incremental_checkpoint_test.cpp`,
      `file_checkpoint_reopen_smoke`) which are cosmetic
- [x] Verified: 180 C++ + 6 FFI tests pass
- **Deferred (see Q3):** the main-workspace `KVEngine::persist_checkpoint` trait
      (crate `crowkv`) is **not** renamed here. `crowtree/ffi` is excluded from the
      root workspace and not yet wired into `crowkv`, so that trait rename is a
      separate main-workspace change (touches `InMemKV`, learner, consensus) and is
      out of scope for a crowtree-only, independently-verifiable commit.

**Sequencing:** Independent; can land anytime, but ideally before #14 so the new
persistence code is written with the final names.

### #10. C++ Logging — `spdlog` `P0` — ✅ DONE (2026-07-02)

**Design:** `design-crowtree.md` D-Q12. OQ6 resolved.
**Files:** `crowtree/include/crowtree/log.h` (new), `src/log.cpp` (new),
`CMakeLists.txt`, `pixi.toml`, `options.h`, `persist.cpp`, `crowtree.cpp`,
`tests/integration/logging_test.cpp` (new)

- [x] Added `spdlog >=1.17` to `pixi.toml` (`fmt` already present, pulled transitively)
- [x] `CMakeLists.txt`: `find_package(spdlog REQUIRED)` + `spdlog::spdlog` PRIVATE,
      gated by `CROWTREE_HAVE_SPDLOG` (LZ4-style) so the Rust FFI `cc` build (no
      spdlog) compiles the macros to no-ops; `SPDLOG_ACTIVE_LEVEL` = TRACE in Debug,
      INFO otherwise
- [x] `log.h`: `init_logging(dir, level, max_file_mb=100, max_files=5)`,
      `shutdown_logging()`, `logging_enabled()`, and `CT_LOG_{ERROR,WARN,INFO,DEBUG,TRACE}`
      macros; each macro checks a relaxed `atomic<bool>` gate (no output before init)
- [x] Async logger: 8192-entry ring buffer, block-on-overflow; rotating file
      `<log_dir>/crowtree.log`, 100 MiB × 5; pattern
      `%Y%m%d-%H%M%S.%e [%t] [%l] [%n] %v`
- [x] `Options`: `log_dir` (empty = off), `log_level="info"`, `log_max_file_mb=100`,
      `log_max_files=5`
- [x] `Crowtree::open()` calls `init_logging()` when `log_dir` is set; INFO on
      open/recover, INFO on snapshot commit, ERROR on demand-load I/O/CRC faults
- [x] Integration test `logging_test.cpp` (file created + format + content; disabled path)
- [x] Verified: 182 C++ tests + 6 FFI tests pass (FFI compiles the no-op logging path)
- **Deviation:** logging is process-global; init is done in `open()` and teardown is
  **explicit** via `shutdown_logging()` rather than in `~Crowtree` (so multiple
  `Crowtree` instances in one process don't tear down each other's logger).
- **TSan-safe teardown (2026-07-02):** `log.cpp` now **owns** the async logger and
  its `thread_pool` (instead of spdlog's global registry/`spdlog::shutdown()`).
  `shutdown_logging()` drops registry refs, flushes, releases the logger, then
  destroys the pool **last** — joining the worker before the sinks are freed. This
  fixes a spdlog teardown race (drop-loggers-before-join-pool) that TSan flagged
  once the sanitizer build was reconfigured with spdlog present.
- **Remaining (deferred, low value now):** post-rotate gzip compression (C++ +
  Rust `tracing-appender`) and Rust-side size rotation — tracked here, not blocking.

---

## Dependency Graph & Implementation Plan

### Dependency graph

```
#1 FFI migration ........................... ✅ done
#6 STL rename .............................. ✅ done

#10 logging (independent) ─────────────────┐  (helps debug everything below)
#7 epoch-in-tree ──────────► #5 B3 (zero-copy read) ─► (subsumes #4)
#9 btree_map ──┐
               ├─► #5 B2 (write path on buffer) ─► #5 B4 (FFI) ─► #5 B5/B6 (pool, RDMA)
#5 B1 buffer ──┘
                    (MemTable now stable on buffer+btree_map)
                                   │
                                   ▼
                         #3 double buffering ──► #8 background flush thread
#8 create_snapshot rename / snapshot_view (independent, low-risk) ── can land anytime
#8a snapshot export API cleanup (remove at_slot) ── independent, low-risk, can land anytime
#15 reject oversized keys (independent) ── can land anytime

#11 async FFI (io_uring reactor) ──► depends on #7 (epoch-in-tree) + #5 B3 (zero-copy read)
     │                                   for fast-path borrowed value return
     └─► after #3 + #8 (flush must be async-able for slow path)

#5 B3 ──► #12 lock-free EBR (after zero-copy read, guard frequency maximized)
#5 B3 ──► #13 install_snapshot epoch-safe (after lock-free readers)
#5 B3 ──► #17 buffer pool live wiring ──► #18 incremental snapshot (durable frame addrs)
#5 B3 + #13 ──► #14 mapping table (epoch-safe slot clearing + segment recycling)
#11 + #17 + #18 ──► #14c/#14d (segment-level persistence)
#14 ───► #16 native frame snapshot format (shares segment image concept)
```

### Recommended order

| Step | Item | Priority | Why here | Effort | Risk |
|-----:|------|:--------:|----------|--------|------|
| 1 | **#10 logging** | P0 | Independent; instruments all later work | Med | Low |
| 2 | **#7 epoch-in-tree** | P0 | Small, unblocks zero-copy read; removes `CrowtreeEnv` | Low | Low |
| 3 | **#8 + #8a + #15** | P0 | Independent, low-risk terminology/cleanup/safety | Low | Low |
| 4 | **#9 + #5 B1/B2 together** | P0 | One MemTable rewrite: `buffer` storage + `btree_map` container | High | Med |
| 5 | **#5 B3 (zero-copy read)** | P0 | Needs #7; subsumes #4; removes `write_mutex_` from read path | Med | Med |
| 6 | **#5 B4 (Rust FFI)** | P1 | After internal path is on `buffer` | Med | Med |
| 7 | **#3 double buffering + #8 background flush** | P0 | Needs stable MemTable (step 4); highest race risk | High | High |
| 8 | **#13 install_snapshot epoch-safe** | P0 | After #5 B3; safety fix for lock-free readers | Low | Low |
| 9 | **#11 async FFI (io_uring reactor)** | P1 | Needs #7 + #5 B3 + #3/#8; highest FFI complexity | High | High |
| 10 | **#12 lock-free EBR** | P1 | After #5 B3; reader path optimization | Med | Med |
| 11 | **#17 buffer pool live wiring** | P1 | After #5 B3; pool owns frames, demand load, eviction | High | High |
| 12 | **#18 incremental snapshot** | P1 | After #17; durable per-frame addrs + dirty tracking | Med | Med |
| 13 | **#14 mapping table redesign** | P1 | After #11 + #17 + #18; segment-level persistence + recycling | High | High |
| 14 | **#5 B5/B6 (memory pool, RDMA)** | P2 | Profile-driven / backend-driven, future | — | — |
| 15 | **#16 native frame snapshot** | P2 | After #14; performance optimization, future | Med | Low |

Rationale: keep **one** MemTable rewrite (step 4) instead of three; do the cheap,
independent, unblocking items first (#10, #7, #8a, #15); defer the highest-risk
concurrency work (#3) until the storage layer underneath it is stable; epoch
optimizations (#12, #13) follow #5 B3; the storage foundation (#17 pool wiring →
#18 incremental snapshot) precedes the mapping table redesign (#14), which is
the largest effort and depends on async I/O (#11) plus #17/#18.

---

## Pre-Implementation Gaps — Resolved (2026-07-01)

Gaps found while making the mapping table (#14) workable, now decided and folded
into the plan/design.

- **Gap A — Buffer-pool wiring + incremental snapshot untracked → RESOLVED.**
  Added **#17** (Buffer Pool Live Wiring, PT6c-5.1–5.4) and **#18** (Incremental
  Snapshot, PT6d); order `#11 → #17 → #18 → #14`. #14c/#14d depend on them.
- **Gap B — On-disk format migration → RESOLVED (clean break).** Nothing released;
  no compatibility required. Segment images + directory + A/B anchor replace the v1
  superblock/manifest layout; a `format_version` guard refuses to open old files.
  No converter. (`design-crowtree-mappingtable.md §13`.)
- **Gap C — Terminology → RESOLVED: one term, "snapshot".** "checkpoint" is
  eliminated across all crowtree docs/API/code (consensus `DedupCheckpoint` is a
  different subsystem and is unchanged). The durable persist is the persist phase
  of `create_snapshot`; C API `ct_checkpoint` → `ct_snapshot`; trait
  `persist_checkpoint` → `persist_snapshot`. Code rename tracked as **#19**.
- **Gap D — Crash-injection harness → RESOLVED.** Add a `FaultyPageStore`
  (drop/tear/reorder at a chosen point) and a **dedicated fault-injection (FI)
  test-case design** in `design-crowtree-test.md`; wired into #14e.
- **Gap E — Anchor region → RESOLVED.** Reserve IU 0 (A) and IU 1 (B) for the
  commit anchor at store-create; all else is normal allocation.
  (`design-crowtree-mappingtable.md §7.3 / §13`.)
- **Gap F — Ordering → RESOLVED.** Recommended order updated to
  `… #12 → #17 → #18 → #14`.

---

## Session Log (2026-07-01 / 2026-07-02, autonomous)

Each item below was built + tested green and committed separately. Baseline:
**182 C++ tests** + **6 Rust FFI tests**; #7 also ASan+TSan clean. (Under
`pixi run` the toolchain paths are set automatically.)

**Completed:** ffi `.cpp` glob build fix · #15 reject oversized keys ·
#9 `absl::btree_map` MemTable (+Q2 bench) · #8a drop snapshot `at_slot` ·
#7 epoch-in-tree (drop `CrowtreeEnv`, ASan+TSan clean) · #19 checkpoint→snapshot
(crowtree scope) · #19b crowkv rename (no-op — no such trait) · #10 spdlog logging ·
Q4 audit of #17/#18 · #14a packed slot-word helpers.

**Session 2 (2026-07-02) — concurrency foundation, all ASan+TSan clean:**
- **#12 lock-free EBR** — per-thread participant slots; reader `enter`/`exit` is
  lock-free; reclamation writer-driven. (+UAF stress test.)
- **#10 TSan fix** — own the spdlog async logger + thread pool so teardown joins the
  worker before sinks are freed (spdlog's global `shutdown()` had the race).
- **#13 epoch-safe `install_snapshot`** — `free_subtree(retire=true)` epoch-retires
  the old tree instead of `delete` (was a live UAF vs lock-free readers). (+concurrent
  import stress test.)
- **#5 B1 buffer core** — `buffer.h` owned/borrowed byte container + 10 unit tests.

**Remaining (large / attended — not safe to rush unattended):**
- **#5 B2** — thread `buffer` through the write path: move-only `buffer` in
  `leaf_entry`/`mem_entry`/MemTable ripples across ~32 files / 214 sites (scan-merge,
  `snapshot()` copies, `btree_map<buffer,buffer>` heterogeneous lookup, most tests).
  A focused, single-purpose session; modest immediate payoff (copies aren't the
  current bottleneck).
- **#5 B3** — lock-free `scan()` (drop `write_mutex_`): needs a proven scan-consistency
  argument under concurrent COW split/merge (get() is already lock-free). High risk.
- **#3 + #8** — MemTable double-buffering + background flush thread. Highest race risk.
- **#11** — io_uring async FFI reactor (large; Linux io_uring + Tokio `AsyncFd`).
- **#14b/c/d** — segment mapping-table + incremental persistence (prereqs #12/#13 now
  met; #14a helpers done). Large.
- **#17** — migrate residency onto the pool `pin`/CLOCK engine. **#16** native frame
  snapshot. **#18 D4** dirty tracker (folds into #14d).

**Resolved questions** (details now live in each task section):
- **Q1** — reject oversized keys, no heap-fallback (settled; see #15).
- **Q2** — added Google Benchmark bench; `absl::btree_map` validated (see #9).
- **Q3** — `crowkv` has no `persist_checkpoint` trait; rename was a no-op (see #19b).
- **Q4** — #17/#18 reduced to their true delta (see #17/#18).

## Roadmap Decision (2026-07-02)

Folding in the two directives ("design + schedule the concurrency batch"; "do
#17/#18, then #14 — change the order"):

**Concurrency batch — design captured, scheduled (not yet implemented).** The
design for #5 B1–B4, #3, #8, #11, #12, #13 lives in the design docs
(`design-crowtree-core.md`, `design-crowtree-async.md §…`) plus the dependency
graph and recommended-order table above (steps 4–10). These remain attended-session
work (highest race risk); no code lands unattended.

**Storage track is the chosen priority: #5 B3 → #17 → #18 → #14.** Honest
dependency note (this corrects the earlier optimistic #17/#18 audit): the *real*
#17 (pool-owned demand-load + CLOCK eviction — the already-tested `pin`/`pin_new`/
`flush_dirty` engine in `buffer_pool.*`) and #18's writer-owned `DirtyTracker` both
need **lock-free readers (#5 B3)** to be safe, and #14b/c/d additionally need
**#13**. So **#5 B3 is the gating prerequisite even in the reordered plan** and is
scheduled next.

- **#17 status:** the *interim* model is done (base frames via `acquire_frame` +
  heap fallback, tagged unloaded slots, demand-load in `resident()`, Crowtree-level
  clean-leaf eviction). The *designed* pool-based residency (migrate the engine onto
  the `pin`/CLOCK demand-load + write-back path — **not** dead code; it is the target
  engine, fully unit-tested in `buffer_pool_test.cpp`) is the remaining #17 work and
  waits on #5 B3. **D1 resolved:** keep the pin/CLOCK engine (do not delete). **D2:**
  keep the deterministic 64 MiB default; servers tune `Options.buffer_pool_bytes`.
- **#18 status:** per-page durable addr + write-only-dirty done. The `DirtyTracker`
  (**D4**) only pays off once the manifest itself is incremental (**#14d** segment
  images) — the current full manifest must enumerate every reachable page anyway —
  so D4 is folded into #14d rather than built standalone.
- **Done now (concurrency-independent): #14a** — packed 64-bit slot-word encode/
  decode helpers + `Segment` layout constants (`design-crowtree-mappingtable.md §4`).
  Pure data-structure work with unit tests; foundation the #14b/#14c wiring builds on
  once #5 B3 + #13 land.
