# CrowKV - Plan: crowtree Persistence (libcrowtree, C++)

Implements [`design/design-crowtree-persistence.md`](design/design-crowtree-persistence.md).
Builds on the completed core (`design/design-crowtree-core.md`; tasks CT1–CT14).

Scope: make crowtree's materialized L1 state durable and recoverable —
`PageStore` backends, the on-disk page codec, checkpoint (superblock A/B), and
crash recovery — so a reopened tree restores `root`, all pages, and
`last_applied_slot`. Snapshot export/import, GC of durable pages, the C ABI, and
the Rust FFI adapter are later phases.

## Conventions

- C++20, `-Wall -Wextra -Werror`; ASan/TSan/UBSan jobs as in the core.
- GoogleTest, integration-style under `crowtree/tests/` (one file per component).
- Public API returns `Status` (no exceptions across the future C ABI boundary).
- Oracle for parity: re-open round-trips compared against the pre-checkpoint
  `SnapshotView()` / `std::map`.

---

## Milestone PT-A — Storage backend & codec

- [x] **PT1 — `PageStore` abstraction + backends.** `PageStore` interface
  (`allocate`/`free`/`read_page`/`write_page`/`flush`/`iu_size`/`capacity`),
  a `MemPageStore` (test/in-mem block backend) and a `FilePageStore`
  (`pread`/`pwrite` + `fdatasync`). v1 is **synchronous** (the async signature in
  the design + tokio bridging is deferred to the FFI phase; see Issues).
  - Tests (`unit/page_store_test.cc`): allocate/write/read round-trip; durability
    across reopen (file); IU geometry; out-of-range read.
  - Deps: core.
- [x] **PT2 — On-disk page codec.** `PageCodec` serialize/deserialize for
  `LeafBase` and `InnerBase`: `PageDiskHeader` (type, version, self_pid,
  logical_len, last_applied_slot_hint, flags), packed body (offset-array layout
  for leaves; separators+child PIDs for inner), zero-pad to IU, `CRC32C` trailer
  with `logical_len`. CRC mismatch ⇒ `Corruption`.
  - Tests (`unit/page_codec_test.cc`): leaf/inner round-trip; tombstone cells;
    empty page; IU padding ignored via logical_len; bit-flip ⇒ CRC failure;
    binary keys with NULs.
  - Deps: PT1.

## Milestone PT-B — Checkpoint & recovery

- [x] **PT3 — Checkpoint.** `Crowtree::Checkpoint(uint64_t* out_last_applied)`:
  under the write lock, consolidate dirty leaves, freeze the reachable page set
  from `root_pid`, fold each leaf chain, serialize every base page, **append**
  the image past EOF (never overwriting the committed image), write a framed
  manifest `(pid, addr, len)*`, then commit by writing the inactive
  **superblock A/B** slot (magic, format version, checkpoint_seq, root_pid,
  last_applied_slot, next_pid, manifest addr/len, page_count, CRC) and `Sync()`.
  The superblock write is the atomic commit point (A/B chosen by seq parity).
  - Tests (`integration/persist_test.cc`): checkpoint advances durable slot;
    reopen restores keys; corrupt newest superblock falls back to previous.
  - Deps: PT2.
- [x] **PT4 — Recovery.** `Crowtree::Open(env, opts)` chooses the
  highest-seq superblock with a valid CRC, loads the manifest, rebuilds the
  mapping table (v1: eager full load; lazy demand-load deferred), restores
  `root_pid`, `next_pid`, and `last_applied_slot`. Empty/none-valid ⇒ fresh tree
  at slot 0.
  - Tests (`integration/persist_test.cc`): write → checkpoint → reopen restores
    all keys + `last_applied_slot`; multi-level tree survives; corrupt newest
    superblock falls back to the previous. (Torn-page CRC covered by
    `unit/page_codec_test.cc`.)
  - Deps: PT3.
- [x] **PT5 — Engine wiring + durable round-trip.** Plumb a `PageStore` through
  `Options`/`CrowtreeEnv`; `Checkpoint()` reachable from the engine; reopen path
  validated against a pre-checkpoint `SnapshotView()` (empty diff). Re-applying
  slots `<= last_applied_slot` after recovery is a no-op (highest-slot-wins).
  - Tests (`integration/persist_test.cc`): multi-level reopen `Compare` empty;
    re-apply old slots no-op; file-backed randomized round-trip.
  - Deps: PT4.

## Milestone PT-D — Frame-based buffer pool (zero-copy)

Implements `design-crowtree-persistence.md` §3 (zero-copy frame format), §4
(buffer pool), §5A (high-perf L0→L1→disk pipeline). Decision: in-memory == on-disk
frame, no encode/decode; base pages become views over frames. This is a core
rearchitecture, sequenced foundation-first so each step builds + tests green.

- [ ] **PT6a — Slotted frame format + zero-copy views.** `FrameHeader`,
  `LeafFrameView`/`LeafFrameBuilder`, `InnerFrameView`/`InnerFrameBuilder` over a
  fixed `page_bytes` frame; sorted slot dir + records-from-end; `logical_len` +
  CRC32C trailer. In-place binary search / `Find` / `LowerBound` / `ChildIndexFor`
  returning `Slice`s into the frame.
  - Tests (`unit/frame_page_test.cc`): build→view round-trip; sorted insert;
    binary search; tombstone cells; capacity (`TryAppend` fails when full); inner
    separators+children; CRC bit-flip; binary keys with NULs.
  - Deps: PT2 (CRC).
- [ ] **PT6b — Buffer pool manager.** Contiguous frame arena sized by
  `capacity_bytes` (4/8 GiB knob); open-addressing `pid→frame` page table (no
  `unordered_map`); `Pin`/`PinNew`/`MarkDirty`/release (RAII `FrameRef`); CLOCK
  eviction skipping pinned+dirty; demand-load miss via `PageStore`; `Stats`.
  - Tests (`unit/buffer_pool_test.cc`): pin/unpin residency; miss loads from
    store; eviction under pressure respects pins/dirty; dirty flush writes back;
    page-table collisions; capacity cap honored.
  - Deps: PT6a, PT1.
- [ ] **PT6c — Mapping table over frames + core L1 migration.** Mapping slot holds
  tagged `frame-id | unloaded PageAddr`; `Get` demand-loads. Migrate leaf/inner
  base pages from `LeafBase`/`InnerBase` objects to frame views; flush/consolidate/
  split/merge build new frames (COW) via builders; epoch-retire old frames.
  - Tests: re-run full core suite (write/read/split-merge/parity/stress) on frames;
    `bench` rebuild cost vs old path.
  - Deps: PT6b. **Largest task; gates incremental checkpoint.**
- [ ] **PT6d — Incremental checkpoint + lazy recovery + durable-page GC.** Replace
  the PT3 full-rewrite walk with a DirtyTracker walk (write only dirty frames,
  remap `pid→PageAddr`); recovery loads superblock + page-allocation map and
  demand-loads frames; reclaim durable pages no longer reachable.
  - Tests (`integration/incremental_checkpoint_test.cc`): only-dirty-written;
    reopen equals; space reused; lazy-load on first access.
  - Deps: PT6c.

## Milestone PT-E — Compression, export/import

- [ ] **PT10 — Page compression (LZ4 default).** Vendor single-file LZ4 under
  `crowtree/third_party/lz4`; compress frame body at `write_page`, decompress into
  a full frame at `read_page`; per-page `flags.compressed` + algo id; store
  uncompressed if it doesn't shrink. `Options.compression = kLz4` default.
  - Tests (`unit/page_compression_test.cc`): compress/decompress round-trip;
    incompressible page stored raw; CRC over stored bytes; mixed pages in one tree.
  - Deps: PT6a (+ PT6d for the write path).
- [ ] **PT7 — Snapshot export/import.** Portable tuple stream (versioned header,
  key-sorted `(klen,key,slot,kind,vlen,value)` ≤1 MiB chunks, whole-stream CRC32C)
  as the primitive; `SnapshotDumpToFile` / `SnapshotLoadFromFile` wrappers
  (`.ctsnap`). Export pins a `RootVersion`; import bulk-loads into staging then
  atomically swaps. Native frame-dump format deferred.
  - Tests (`integration/snapshot_export_test.cc`): export→import→`Compare` empty;
    file dump/load round-trip; chunk-boundary determinism; CRC tamper rejected;
    cross-engine parity vs in-mem oracle.
  - Deps: PT6c (RootVersion pinning).

## Milestone PT-F — Deferred (tracked, not in this pass)

- [ ] **PT8 — C ABI** (`ct_*`) + Rust FFI adapter; async I/O bridging.
- [ ] **PT9 — Block alignment modes + readable debug codec** (file-debug text
  encode/decode; aligned vs unaligned block devices).
- [ ] **PT11 — Overflow pages** for entries larger than a frame.
- [ ] **PT12 — In-frame delta region** (optional flush-rebuild optimization, §5A).

---

## Issues / Risks To Track

Carried from the core review plus risks introduced by persistence. Update as
phases land.

### From core review

- **Flush/SMO routing.** Stale-root grouping in `Flush()` was fixed (route each
  per-leaf group against the current tree); a regression test
  (`SplitMerge.LargeFlushSpanningLeavesSplitsMidFlush`) guards it. Split/merge
  publish ordering remains delicate — keep exercising it under checkpoint churn.
- **Epoch manager not scalable.** `EpochManager` serializes `Enter`/`Exit`/
  reclaim on one mutex; fine for v1 correctness, a throughput bottleneck under
  heavy readers. Checkpoint adds another writer-lock holder — watch contention.
- **Merge leaks merged-away PIDs.** Leaf merge retires the page but never
  recycles the PID (avoids a nullptr race). Durable PID/page-address lifecycle
  must be defined so checkpoints don't persist dangling references and so the
  free list is recoverable (see PT3/PT4 manifest).
- **`SnapshotView()` is O(N) under the write lock.** Acceptable for v1; the
  durable `RootVersion` + path-copy COW (persistence) should later let snapshots
  pin a version instead of materializing.
- **Inner-node underflow deferred.** Long delete-heavy workloads can leave a
  sparse upper tree; checkpoints will then persist underfull inner pages.

### Introduced by persistence

- **Synchronous PageStore vs async design.** v1 implements blocking I/O; the
  design's async `read_page/write_page` + tokio bridging is deferred to PT8. The
  interface keeps the callback-free shape minimal; revisit before FFI.
- **Append-only full-rewrite checkpoint (no incremental).** v1 appends a fresh
  full image past EOF each checkpoint and never reclaims old images — crash-safe
  (committed superblock keeps pointing at an intact image) but unbounded file
  growth and O(tree) write amplification. Incremental dirty-only flush + a
  durable-page free list / image reuse is PT6.
- **Eager recovery load.** v1 loads all pages at open; large trees pay full read
  cost at restart. Lazy demand-load is PT6.
- **Persisted tombstones not reclaimed.** Checkpoint folds chains but keeps
  tombstones with `slot > gc_floor`; a recovered `SnapshotView()` still contains
  them (live `Get` skips them). Durable-page GC + tombstone reclaim below the
  watermark is PT6.
- **Checkpoint holds the write lock for the whole walk + I/O.** O(tree)
  serialization + blocking writes block flush/SMO for the duration. Acceptable
  for v1; revisit with incremental checkpoint (PT6) and async I/O (PT8).
- **No durable redo of the delta tail.** By design (checkpoint + consensus
  replay). A crash between checkpoints loses in-memory deltas; the learner
  re-applies slots `> last_applied_slot`. Revisit only if checkpoint cadence
  proves too costly (persistence §6 TODO-CONFIRM).
