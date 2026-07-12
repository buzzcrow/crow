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

### #7. Epoch Ownership — Move into Crowtree `P0`

**Design:** `design-crowtree.md` D-Q9, core §10. Prerequisite for #5 B3.
**Files:** `crowtree.h`, `crowtree.cc`, `env.h`, `epoch.h`

- [ ] Move `EpochManager epoch_` from `CrowtreeEnv` into `Crowtree` (private member)
- [ ] `Crowtree::open()` signature: drop the `CrowtreeEnv& env` parameter
- [ ] Delete `env.h` / `env.cc` entirely
- [ ] Update all test call sites and C API (26 files reference `CrowtreeEnv`)

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

### #8a. Snapshot Export API Cleanup — Remove `at_slot` `P0`

**Files:** `c_api.h/.cc`, `snapshot_io.h/.cc`, `ffi/src/lib.rs`, test files

- [ ] C API: remove `at_slot` from `ct_snapshot_export_begin`
- [ ] C++ API: remove `at_slot` from `snapshot_export_begin()` and `snapshot_dump_to_file()`
- [ ] `snapshot_io.cc`: delete `at_slot` validation logic and historical pin comments
- [ ] Rust FFI: `snapshot_export(&self)` — drop `at_slot` parameter
- [ ] Tests: update `snapshot_export_test.cc` and `c_api_test.cc` call sites
- [ ] Design docs: verify no "historical snapshot export" references remain

### #15. Reject Oversized Keys at `apply()` Entry `P0`

**Files:** `crowtree.cc` (`apply`), `c_api.cc`

- [ ] Add key size check in `apply()` (threshold = `frame_payload / 2`)
- [ ] Expose threshold via `Options.max_key_size` (default = frame-dependent)
- [ ] Tests: verify rejection, verify normal-size keys unaffected

---

## Memory Layer

Tasks related to `buffer` abstraction, zero-copy pipeline, and BufferPool.

### #5. Unified Buffer Design — Single Allocation, Zero-Copy Pipeline `P0`

**Design:** [`design-crowtree-memory.md`](design/design-crowtree-memory.md), `design-crowtree.md` D-Q8.
**Files:** new `buffer.h`, `cell.h`, `memtable.h`, `crowtree.cc`, `page.h`, `c_api.h/.cc`, `ffi/src/lib.rs`

**B1 — buffer core** `P0`
- [ ] Create `buffer.h`: owned/borrowed modes, move-only, `header_reserve`, `clone()`, glibc-malloc backing
- [ ] Unit tests for `buffer` (alloc/wrap/move_from/clone, header region, free-iff-owned)

**B2 — write path on buffer** (sequence with #9) `P0`
- [ ] `cell.h` encode writes slot+flags into the reserved header (no second alloc)
- [ ] MemTable stores `buffer` key/value; `mem_entry`/`leaf_entry` carry `buffer` (moved end-to-end)
- [ ] `flush()` / `drain_up_to()` move buffers; `LeafFrameBuilder` takes `buffer` (final frame copy)

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

### #9. MemTable — Map Choice: `absl::btree_map` `P0`

**Design:** `design-crowtree.md` D-Q10, core §1. OQ2/OQ3 resolved.
**Files:** `memtable.h`, `memtable.cc`, `CMakeLists.txt`, `pixi.toml`

- [ ] Add `absl` to `pixi.toml` + `CMakeLists.txt` (`find_package(absl REQUIRED)`, link `absl::btree`)
- [ ] Replace `std::map<...>` with `absl::btree_map<buffer, buffer>` in `memtable.h`
- [ ] Use `try_emplace` / `emplace` for move-only insertion; verify `get`/`drain_up_to`/`snapshot` compile
- [ ] Benchmark point-get latency before/after

### #12. Lock-Free EBR for `EpochManager` `P1`

**Design:** `design-crowtree-core.md §10.1`
**Files:** `epoch.h`, `epoch.cc`

- [ ] Per-thread epoch slot registration (thread_local + dynamic slot pool, cache-padded)
- [ ] `enter()` = atomic acquire-load global epoch + atomic release-store local epoch
- [ ] `exit()` = atomic release-store 0 (no `ReclaimLocked` on reader path)
- [ ] `try_reclaim()` = scan per-thread local epochs for min active, free retired < min
- [ ] `retire()` keeps mutex (writer-only, no contention)
- [ ] Tests: TSan clean, high-concurrency `enter()`/`exit()` benchmark vs mutex

**Sequencing:** After #5 B3 — guard frequency increases then, maximizing payoff.

### #13. Make `install_snapshot` Safe for Lock-Free Readers `P0`

**Files:** `crowtree.cc` (`install_snapshot`, `free_subtree`)

After #5 B3 makes readers lock-free, `install_snapshot`'s `free_subtree()` must
change to epoch `retire` — immediate free would be use-after-free.

- [ ] Change `free_subtree()` to epoch-retire old root + reachable pages instead of immediate free
- [ ] Slot clearing via epoch deleter (deleter clears mapping slot to nullptr after all readers exit)

**Sequencing:** After #5 B3. Install snapshot is uncommon (a corrupted replica is
typically removed from the group and re-added fresh, not waited on while serving reads).

### #14. Mapping Table Redesign — Segment Recycling + Incremental Persistence `P1`

**Design:** [`design/design-crowtree-mappingtable.md`](design/design-crowtree-mappingtable.md). Workable spec: packed slot word, segment image + directory + A/B anchor, snapshot/recovery ordering.

**Key decisions:**
- PID recycling: **NO** — race condition risk too high
- Segment recycling: **YES** — free empty segments via epoch deleter
- Sparse segments: **acceptable** — 8 KB waste per segment
- Incremental persistence: **YES** — replace full manifest with segment-level persistence
- Backend abstraction: **YES** — all I/O via `PageStore` interface

**14a — Packed slot word + segment struct** `P1`
- [ ] Packed 64-bit slot word: `0`=empty, `bit0=0`=resident `PageBase*`, `bit0=1`=unloaded `(iu_index, iu_count)`; pack/unpack helpers + unit tests
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

### #17. Buffer Pool — Live-Engine Wiring `P1`

**Design:** [`design-crowtree-persistence.md §4.5`](design/design-crowtree-persistence.md) (PT6c-5.1–5.4).
**Files:** `buffer_pool.h/.cc`, `crowtree.cc`, `mapping_table.h`, `options.h`
**Prerequisite for #14c/#14d** (unloaded descriptors need pool-owned frames).

- [ ] 5.1 Pool owns live base frames: `Crowtree` gets a `BufferPool` sized by `Options.buffer_pool_bytes` (default `min(8 GiB, 25% RAM)`); bases built into `PinNew` frames, no eviction yet
- [ ] 5.2 Epoch-deferred frame free: `RetirePage` returns frame to pool free list via the epoch manager (`FreeFrameDeferred`), not `delete`
- [ ] 5.3 Mapping slot tagging + demand load: slot becomes the packed word (resident / unloaded `PageAddr`); `Get` of an unloaded slot demand-loads (CRC-checked) and publishes
- [ ] 5.4 CLOCK eviction of clean resident bases (skips anonymous/dirty); re-tags slot to unloaded; retire via epoch
- [ ] Tests: pool-stats residency, `stress_test` TSan/ASan, `lazy_load_test`, `eviction_test` (all per §4.5)

**Sequencing:** After #5 B3 (lock-free readers + epoch retire). 5.4 lands after #18.

### #18. Incremental Snapshot — Durable Frame Addrs + Dirty Tracking `P1`

**Design:** [`design-crowtree-persistence.md §4.3/§5A`](design/design-crowtree-persistence.md) (PT6d).
**Files:** `persist.cc`, `buffer_pool.h/.cc`
**Prerequisite for #14c/#14d** and for #17's 5.4 eviction.

- [ ] Snapshot assigns each dirty frame a durable `PageAddr` (append cursor) + records `pid→addr`, `page_len`
- [ ] `DirtyTracker` = set of dirty frames; snapshot walks it (not the whole tree)
- [ ] Write only dirty frames (optionally LZ4); drop build pins so frames become evictable
- [ ] Tests: incremental cost (only dirty frames written), eager-snapshot back-pressure under write storm

**Sequencing:** After #17 (5.1–5.3), before #14c.

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

### #19. Terminology — `checkpoint` → `snapshot` (code) `P1`

**Scope:** crowtree only. Consensus/WAL `DedupCheckpoint` is a different subsystem
and stays unchanged. Docs are already renamed; this task carries it into code.
**Files:** `c_api.h/.cc`, `ffi/src/lib.rs`, `persist.cc`, `crowtree.h/.cc`,
engine trait (`KVEngine`), tests.

- [ ] C API: `ct_checkpoint` → `ct_snapshot`, `ct_checkpoint_async` → `ct_snapshot_async`
- [ ] Trait/adapter: `persist_checkpoint` → `persist_snapshot`
- [ ] Internal: `persist_checkpoint()` / `checkpoint()` → `snapshot persist` naming; `checkpoint_every_slots` → `snapshot_every_slots`; `snapshot_seq` for the anchor sequence
- [ ] Update all call sites, tests, and Rust FFI bindings; grep for residual `checkpoint` (allow only `DedupCheckpoint`)

**Sequencing:** Independent; can land anytime, but ideally before #14 so the new
persistence code is written with the final names.

### #10. C++ Logging — `spdlog` `P0`

**Design:** `design-crowtree.md` D-Q12. OQ6 resolved.
**Files:** new `crowtree/include/crowtree/log.h`, `CMakeLists.txt`, `pixi.toml`

- [ ] Add `spdlog` + `fmt` to `pixi.toml` (conda-forge package names: `spdlog`, `fmt`)
- [ ] Add to `CMakeLists.txt`: `find_package(spdlog REQUIRED)`, `target_link_libraries(crowtree PRIVATE spdlog::spdlog)`
- [ ] Create `crowtree/include/crowtree/log.h`:
  - `void init_logging(const std::string& log_dir, const std::string& level, size_t max_file_mb, size_t max_files);`
  - `void shutdown_logging();` (flush + join async thread)
  - Thin macros: `CT_LOG_ERROR(...)`, `CT_LOG_WARN(...)`, `CT_LOG_INFO(...)`, `CT_LOG_DEBUG(...)`, `CT_LOG_TRACE(...)`
  - No-op when logging not initialized (zero overhead — check a `std::atomic<bool>` flag)
  - Async logger: ring buffer (configurable, default 8192 entries), overflow policy = block
  - Format: `YYYYMMDD-HHMMSS.mmm [tid] [level] [crowtree] message` (align with Rust `tracing` format)
  - Rotating file: default 100MB × 5 files
- [ ] Add to `Options`: `std::string log_dir;` (empty = no logging), `std::string log_level = "info";`
- [ ] Initialize logging in `Crowtree::open()` when `opt.log_dir` is non-empty
- [ ] Shutdown logging in `~Crowtree()` destructor (flush + join)
- [ ] Add log calls per the level design table (error/warn/info/debug/trace)
- [ ] Compile-time level guard: `SPDLOG_ACTIVE_LEVEL=SPDLOG_LEVEL_INFO` in release builds
- [ ] Tests: integration test that enables file logging, runs a few ops, verifies log file
- [ ] Log rotation + auto-compression (C++ spdlog + Rust tracing-appender):
  - C++: Use `rotating_file_sink` (size-based, 100MB × 5). Add post-rotate gzip compression.
  - Rust: Replace `rolling::never` with size-based rotation. Add gzip compression.
  - Both: configurable via `Options` / CLI args: `max_file_size_mb` (default 100), `max_files` (default 5), `compress_rotated` (default true)

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