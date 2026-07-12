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

- [x] **PT6a — Slotted frame format + zero-copy views.** `FrameHeader`,
  `LeafFrameView`/`LeafFrameBuilder`, `InnerFrameView`/`InnerFrameBuilder` over a
  fixed `page_bytes` frame; sorted slot dir + records-from-end; `logical_len` +
  CRC32C trailer. In-place binary search / `Find` / `LowerBound` / `ChildIndexFor`
  returning `Slice`s into the frame.
  - Tests (`unit/frame_page_test.cc`): build→view round-trip; sorted insert;
    binary search; tombstone cells; capacity (`TryAppend` fails when full); inner
    separators+children; CRC bit-flip; binary keys with NULs.
  - Deps: PT2 (CRC).
- [x] **PT6b — Buffer pool manager.** Contiguous frame arena sized by
  `capacity_bytes` (4/8 GiB knob); open-addressing `pid→frame` page table (no
  `unordered_map`); `Pin`/`PinNew`/`MarkDirty`/release (RAII `FrameRef`); CLOCK
  eviction skipping pinned+dirty; demand-load miss via `PageStore`; `Stats`.
  - Tests (`unit/buffer_pool_test.cc`): pin/unpin residency; miss loads from
    store; eviction under pressure respects pins/dirty; dirty flush writes back;
    page-table collisions; capacity cap honored.
  - Deps: PT6a, PT1.
- **PT6c — Core L1 migration to frames.** Migrate leaf/inner base pages to the
  zero-copy frame format and unify the durable format. Done incrementally:
  - [x] **PT6c-1** widen frame slot offsets to u32 (remove 64 KiB cap so a
    consolidated leaf fits one frame during build-then-split).
  - [x] **PT6c-2** back `LeafBase` with a frame; zero-copy `key/cell/Find/
    Lookup/LowerBound` via `LeafFrameView`; chain resolution reads the frame.
  - [x] **PT6c-3** back `InnerBase` with a frame; route via `InnerFrameView`.
  - [x] **PT6c-4** durable format == in-memory frame: checkpoint writes frame
    bytes, recovery rebuilds via `FromFrameCopy` (PageCodec/PT2 superseded).
  - **PT6c-5** buffer-pool residency (design §4.5; resolves lock-free-reads vs
    pin/evict via epoch-deferred frame reuse, and anonymous dirty frames). Reads
    stay lock-free; only the writer/checkpoint/demand-load pin. **Gates PT6d.**
    - [x] **PT6c-5.1** `Crowtree` owns a shared `BufferPool` (`Options.buffer_pool_bytes`,
      `frame_bytes`); `LeafBase`/`InnerBase` build into an anonymous pinned frame
      (`FrameStore` via `AcquireFrame`), heap fallback when oversized/pool full.
    - [x] **PT6c-5.2** frames freed back to the pool in `~FrameStore`, which runs
      only on epoch reclaim of the retired page (epoch-deferred reuse); pages
      co-own the pool via `shared_ptr`. Verified ASan + stress/SMO TSan.
    - [x] **PT6c-5.3** tagged mapping slot (`UnloadedPage*` low-bit tag | real
      `PageBase*`); `Crowtree::Resident(pid)` demand-loads the cold path under
      `load_mutex_` (hot path stays lock-free) and publishes into a pool frame;
      recovery stores `pid→addr` tags only (lazy). Descent (`FindLeafPID`,
      `CollectInOrder`) takes a resolver. Tests: `Persist.LazyRecovery*`,
      `Stress.ConcurrentDemandLoadAfterRecovery` (TSan-clean, ASan-clean).
    - [x] **PT6c-5.4** writer-driven eviction of clean delta-free leaf bases
      (`EvictCleanLeaves`/`EvictCleanLeavesLocked`): re-tag the slot `unloaded`
      and epoch-retire the page (design §4.6); `MaybeEvictLocked` auto-triggers at
      flush over an 85%→70% high/low-water. Unblocked once PT6d gave live pages a
      durable `addr`. Anonymous/dirty pages and pages with deltas are skipped;
      evicted pages demand-load on next access. Tests:
      `integration/eviction_test.cc` (memory drops + reload-correct; idempotent;
      concurrent readers-while-evicting under TSan). Policy is a simple sweep;
      a CLOCK/registry over frames is a tracked refinement. 128 tests pass,
      ASan + TSan clean.
  - Tests: full core suite (write/read/split-merge/parity/stress) green on
    frame-backed pages (PT6c-1..4 done, 117 pass, ASan-clean).
  - Deps: PT6b.
- **PT6d — Incremental checkpoint + lazy recovery + durable-page GC.**
  - [x] **PT6d-1/2 — durable addrs + incremental checkpoint.** `PageBase` carries
    `durable_addr`/`durable_plen` (`~0ull` == dirty/anonymous); `Resident` marks
    demand-loaded pages clean. `Checkpoint` folds any delta chain into a fresh
    base, then writes a page's **live frame** only when dirty and retains clean
    pages' prior addr in the manifest (append-only, no overwrite ⇒ crash-safe).
    `last_checkpoint_pages_written()` exposes the write count. Lazy recovery was
    already done in PT6c-5.3. This also satisfies the **addr precondition that
    unblocks PT6c-5.4** (design §4.6). Tests:
    `integration/incremental_checkpoint_test.cc` (no-change ⇒ 0 written;
    single-key edit rewrites only its path; reopen-after-incremental equals).
    125 tests pass, ASan + TSan clean.
  - [x] **PT6d-3 — durable-page GC / free-space reuse.** `Checkpoint` builds a
    crash-safe `SpaceAllocator` from the committed superblock's live extents
    (`CollectLiveExtents` = all reachable page frames + the manifest) and writes
    dirty pages/manifest into the *complement* (dead-w.r.t.-committed gaps), else
    appends past EOF. It never overwrites the committed image, so the crash
    fallback stays intact (two-generation safety: space freed by the committed
    checkpoint is reusable only after the next one commits). Fixed-size frames
    reuse exactly-sized gaps, so steady rewriting keeps the file flat. Test:
    `IncrementalCheckpoint.SpaceIsReusedAcrossManyCheckpoints` (file flat over 50
    rewriting rounds; reopen equals). 129 tests pass, ASan + TSan clean.
  - Deps: PT6c.

## Milestone PT-E — Compression, export/import

- [ ] **PT10 — Page compression (LZ4 default, end-to-end).** The codec core is
  implemented (`compressor.{h,cc}`; durable blob `[algo][raw_len][stored_len]`
  `[crc][stored]`) and unit-tested, but **review found it is not yet wired into
  the durable path**: `Checkpoint` still writes raw frame bytes and
  `Resident`/`Open` still read/validate raw frames directly. `Options` also has
  no compression selector yet, so the feature is not active in production.
  - [x] Codec core + tests (`unit/page_compression_test.cc`): round-trip;
    compressible shrinks; incompressible stored raw; CRC tamper rejected;
    short-blob rejected. ASan-clean.
  - [ ] **10.1** Plumb `Options.compression` / `CompressAlgo` into the engine.
  - [ ] **10.2** Wire `EncodeDurablePage` into `Checkpoint` and
    `DecodeDurablePage` into demand-load / recovery (`Resident`, `Open`).
  - [ ] **10.3** Add integration coverage: checkpoint→reopen with compressed
    pages, eviction-reload of compressed pages, CRC tamper rejection on reopen.
  - **Build note:** current CMake links the runtime `liblz4.so.1` by path and
    compiles an identity fallback when absent. Replace with vendored source in
    PT13.
  - Deps: PT6a (+ PT6d for the write/read path).
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
  - [ ] **8.1** Define the stable C surface in `include/crowtree/c_api.h`:
    opaque handles (`ct_tree`, `ct_view`, `ct_iter`), `ct_status`, `ct_buf`,
    `ct_options`, owned-buffer free functions, and status-code mapping from
    `Status`.
  - [ ] **8.2** Implement engine lifecycle + durability entry points in
    `src/c_api.cc`: `ct_open/close`, `ct_checkpoint`, `ct_last_applied_slot`,
    `ct_set_gc_watermark`, `ct_collect_garbage`.
  - [ ] **8.3** Implement data/snapshot APIs: `ct_apply`,
    `ct_advance_contiguous`, `ct_get`, `ct_scan`, `ct_snapshot_view`,
    `ct_view_iter`, `ct_snapshot_export_*`, `ct_snapshot_import_*`.
  - [ ] **8.4** Rust FFI adapter: safe wrapper types, owned-buffer translation,
    error mapping, and `spawn_blocking` / completion-channel bridge for the still-
    synchronous v1 `PageStore`.
  - [ ] **8.5** C ABI / Rust integration tests: open/apply/get/scan/checkpoint/
    reopen, snapshot export/import round-trip, and kill/restart smoke through the
    Rust adapter.
- [ ] **PT13 — Vendor LZ4 source** `third_party/lz4/{lz4.c,lz4.h}` to replace the
  runtime-`.so.1`-by-path link (portability; no dev package needed).
  - [ ] **13.1** Add vendored LZ4 source + license under
    `crowtree/third_party/lz4/` (user-provided library/source is acceptable).
  - [ ] **13.2** Update `CMakeLists.txt` to prefer vendored source, stop linking
    a distro-specific runtime soname path, and keep the identity fallback only
    when vendored LZ4 is intentionally absent.
  - [ ] **13.3** Switch `compressor.cc` to include/use the vendored header
    instead of local manual prototypes.
  - [ ] **13.4** Add a portability/build test path: build with vendored LZ4 and
    without any system LZ4 package; ensure page-compression tests still pass.

### PT11 — Overflow pages (entries larger than a frame) — DESIGN

Today a leaf entry whose `key+cell` exceeds `frame_bytes` falls back to an
*oversized heap page* (`FrameStore::Alloc` tight buffer): correct but
non-pool-resident (can't be evicted ⇒ unbounded memory) and produces
variable-size durable pages. Overflow pages remove the oversized base page: a
large value is split across a chain of fixed `frame_bytes` **overflow frames**
and the leaf stores a small fixed **overflow pointer cell**, so every base page
stays ≤ `frame_bytes` (pool-resident + evictable + alignment-friendly, feeds PT9).

- **On-disk format.** New `PageType::kOverflowFrame`; frame =
  `[FrameHeader(magic 'CTOV')][next_pid u64 @ header reserved][payload chunk]
  [trailer]`. `FrameValidate` extended for the new magic/type. A value of `N`
  bytes ⇒ `ceil(N / chunk_cap)` frames linked by `next_pid`.
- **Cell format (cell.h).** Add `flags bit1 = kFlagOverflow`. An overflow cell =
  `[slot u64][flags u8][head_overflow_pid u64][total_value_len u64]` (fixed 25 B).
  `CellView::is_overflow()`; inline `value()` stays for normal cells. Overflow
  value resolution needs I/O, so it is done by the engine via a resolver
  (`AssembleOverflowValue(head_pid, len)` walking the chain through `Resident`).
- **Size policy.** `Options.max_inline_value` (default ≈ `frame_bytes/4`): values
  above it spill to overflow at flush/consolidate build time.
- **Reachability (critical).** Overflow frames are referenced by *leaf cells*, not
  child PIDs, so the checkpoint DFS and GC liveness must additionally walk each
  leaf's overflow-cell chains and include those PIDs in the manifest/live set.
  Recovery + eviction treat overflow frames as ordinary clean pages.
- **Sub-tasks:**
  - [ ] **11.1** Overflow frame format: `PageType::kOverflowFrame` + magic
    `'CTOV'`; `next_pid`/`chunk_len` header fields; `OverflowFrameView` +
    `OverflowFrameBuild` in `frame_page.{h,cc}`; extend `FrameValidate` for the
    new magic/type. Unit test in `frame_page_test.cc`.
  - [ ] **11.2** Cell overflow flag: `kFlagOverflow = 0x2` in `cell.h`; overflow
    cell encode `[slot u64][flags u8][head_pid u64][total_len u64]`;
    `CellView::is_overflow()/overflow_head()/overflow_len()`. Unit test in
    `cell`/leaf tests.
  - [ ] **11.3** Write path: `Options.max_inline_value` (default `frame_bytes/4`);
    at flush/consolidate/split build, spill big values into an overflow chain
    (allocate overflow PIDs via the mapping table) and store an overflow pointer
    cell in the leaf. Old chain epoch-retired on rewrite.
  - [ ] **11.4** Read path: `Crowtree::AssembleOverflowValue(head_pid, len)` walks
    the chain via `Resident`; `Get`/`Scan`/`iter`/`Snapshot` materialize overflow
    values (under the read epoch guard).
  - [ ] **11.5** Reachability: checkpoint DFS and GC `CollectLiveExtents`/liveness
    additionally include every leaf overflow-cell chain PID in the manifest +
    live set (overflow frames are not child PIDs).
  - [ ] **11.6** Recovery + eviction: overflow frames demand-load + are evictable
    as ordinary clean pages (verify `Resident`/`EvictCleanLeaves` cover them, or
    add an overflow eviction pass).
  - [ ] **11.7** Tests (`integration/overflow_test.cc`): multi-MiB put/get/scan/
    delete; multi-frame chain boundaries; reopen-equals; eviction-reload; parity
    vs in-mem oracle. Run ASan + TSan.
- **Deps:** PT6c/PT6d. Foundational for PT9 alignment.

### PT9 — Block alignment modes + readable debug codec — DESIGN

**A. Alignment (aligned block device vs byte-addressable).** Decouple *logical*
frame size from *physical* extent size:
- Contract: `durable_plen` = logical frame size (drives `FrameValidate`);
  physical write/read length = `RoundUp(plen, iu)`; every durable extent is
  allocated IU-aligned and IU-sized.
- `SpaceAllocator` rounds gap starts + lengths up to `iu`; write path pads the
  buffer to `RoundUp(plen, iu)` with zeros (no copy when already aligned — the
  common pooled `frame_bytes % iu == 0` case); read path (`Resident`,
  `CollectLiveExtents`, recovery) reads `RoundUp(plen, iu)` but validates over
  `plen`.
- Open-time geometry validation: `frame_bytes % iu == 0` and `iu` divides
  `kSuperblockBytes` (v1 supports `iu ≤ 4096`; 16K/64K SSD needs superblock-slot
  resizing — tracked follow-up).
- **Sub-tasks:**
  - [ ] **9.1** Alignment contract: `RoundUp(plen, iu)` helper; `SpaceAllocator`
    rounds gap starts + allocation lengths up to `iu`; checkpoint write pads the
    page/manifest buffer to `RoundUp(plen, iu)` (zero-fill, copy only when
    padding is needed); read sites (`Resident`, `CollectLiveExtents`, recovery)
    read `RoundUp(plen, iu)` bytes but `FrameValidate`/decode over `plen`.
  - [ ] **9.2** Open-time geometry validation: reject `frame_bytes % iu != 0` or
    `kSuperblockBytes % iu != 0` (v1: `iu ≤ 4096`); document the 16K/64K
    superblock-slot-resizing follow-up.
  - [ ] **9.3** Tests: `iu=4096` `MemPageStore` checkpoint round-trip + reopen-
    equals; oversized/heap pages aligned; torn-tail (truncated IU pad) detected
    via CRC; allocator reuse stays IU-aligned over many checkpoints.

**B. Readable debug codec.** `debug_codec.{h,cc}`: `EncodeFrameText(frame, plen)`
→ human-readable text (header fields + per-slot `key`/`cell` as escaped/hex),
`DecodeFrameText` → exact bytes (round-trip). For unaligned/file-debug media
(`iu=1`, variable length via manifest `plen`). Optional `DebugPageStore` wrapper
selected by `Options.debug_codec`.
- **Sub-tasks:**
  - [ ] **9.4** `debug_codec.{h,cc}`: `EncodeFrameText(frame, plen) -> string`
    (header fields + per-slot key/cell escaped or hex) and `DecodeFrameText ->
    bytes`; exact byte round-trip. Unit test `unit/debug_codec_test.cc`.
  - [ ] **9.5** Optional `DebugPageStore` wrapper (or `Options.debug_codec`):
    stores each page as text (`iu=1`, variable length via manifest `plen`);
    checkpoint→reopen-equals test in debug mode.
- **Deps:** PT11 (so all base pages are bounded → clean alignment story).

### PT12 — In-frame delta region (§5A optimization) — DESIGN

crowtree already has an *out-of-frame* overlay: `BatchDelta` heap nodes chained
in front of a `LeafBase`, folded into a fresh frame at consolidate/checkpoint.
PT12 is the *in-frame* variant — store a bounded delta region inside the leaf
frame's free space so tiny batches avoid a full frame rebuild and the deltas are
durable within the page.
- **Format.** Reserve a delta area between `free_lo`/`free_hi`; header gains
  `delta_count`/`delta_lo`. Search overlays in-frame deltas (newest-wins) atop the
  sorted slots; capped at `Options.max_inframe_delta`.
- **Mutation model.** Frames are immutable for lock-free readers, so appending a
  delta COWs a new frame (cheap memcpy, source stays pool-resident) with the delta
  appended — or, if proven safe, an epoch-guarded in-place append. Gated behind
  `Options.inframe_delta` and **measured against plain COW-rebuild** (default off).
- **Sub-tasks:**
  - [ ] **12.1** In-frame delta format: reserve a delta area between `free_lo`/
    `free_hi`; header `delta_count`/`delta_lo`; `LeafFrameView` overlays in-frame
    deltas (newest-wins) in `Find`/`Lookup`/scan; cap `Options.max_inframe_delta`.
  - [ ] **12.2** Append-delta builder: COW a new frame (memcpy source + append
    delta) — or an epoch-guarded in-place append if proven safe.
  - [ ] **12.3** Flush integration behind `Options.inframe_delta` (default off);
    fold to a fresh base on cap overflow / at checkpoint.
  - [ ] **12.4** Tests + microbenchmark vs plain COW-rebuild (the default).
- **Deps:** PT11. Lowest priority (optimization; existing overlay already works).

---

## Issues / Risks To Track

Carried from the core review plus risks introduced by persistence. Update as
phases land.

### From core review

- [ ] **Flush/SMO routing.** Stale-root grouping in `Flush()` was fixed (route
  each per-leaf group against the current tree); a regression test
  (`SplitMerge.LargeFlushSpanningLeavesSplitsMidFlush`) guards it. Split/merge
  publish ordering remains delicate — keep exercising it under checkpoint churn.
- [ ] **Epoch manager not scalable.** `EpochManager` serializes `Enter`/`Exit`/
  reclaim on one mutex; fine for v1 correctness, a throughput bottleneck under
  heavy readers. Checkpoint adds another writer-lock holder — watch contention.
- [ ] **Merge leaks merged-away PIDs.** Leaf merge retires the page but never
  recycles the PID (avoids a nullptr race). Durable PID/page-address lifecycle
  must be defined so checkpoints don't persist dangling references and so the
  free list is recoverable (see PT3/PT4 manifest).
- [ ] **`SnapshotView()` is O(N) under the write lock.** Acceptable for v1; the
  durable `RootVersion` + path-copy COW (persistence) should later let snapshots
  pin a version instead of materializing.
- [ ] **Inner-node underflow deferred.** Long delete-heavy workloads can leave a
  sparse upper tree; checkpoints will then persist underfull inner pages.

### Introduced by persistence

- [ ] **Synchronous PageStore vs async design.** v1 implements blocking I/O; the
  design's async `read_page/write_page` + tokio bridging is deferred to PT8. The
  interface keeps the callback-free shape minimal; revisit before FFI.
- [ ] **Demand-load failure is silent to reads.** `Resident()` returns `nullptr`
  on `ReadAt` or CRC failure; callers like `Get()` then treat that as an
  ordinary miss. This hides media corruption / I/O faults instead of surfacing a
  hard error. Follow-up: a status-returning resolver path or a latched fatal
  engine error that read APIs expose.
- [ ] **Compression codec is not wired into the durable path yet.** PT10's
  `EncodeDurablePage` / `DecodeDurablePage` are unit-tested, but checkpoint/open
  still persist and read raw frames. The plan above now tracks the missing
  end-to-end wiring as open work.
- [ ] **IU alignment is still deferred.** Checkpoint writes `plen` bytes directly
  and recovery/demand-load read exactly `plen`; extents are not yet rounded to
  the store IU. Safe today for `iu=1` / aligned-by-construction cases; PT9 must
  make extent allocation, writes, and reads `RoundUp(plen, iu)` aware.
- [ ] **Superblock geometry assumes `iu <= 4096`.** `kSuperblockBytes` is 4 KiB,
  so larger-IU stores (16/64 KiB) need a different superblock-slot strategy.
  Track this with PT9's alignment work.
- [ ] **Persisted tombstones not reclaimed.** Checkpoint folds chains but keeps
  tombstones with `slot > gc_floor`; a recovered `SnapshotView()` still contains
  them (live `Get` skips them). Durable-page GC + tombstone reclaim below the
  watermark is PT6.
- [ ] **Checkpoint still holds the write lock for the whole walk + I/O.** PT6d
  made it incremental, but the DFS + writes still run under `write_mutex_`, so
  slow stores serialize flush/SMO for the duration. PT8 async I/O (and later
  version-pinned snapshots) should reduce that stall window.
- [ ] **LZ4 linked by runtime soname, not vendored.** The build env has no LZ4
  dev package/header/source, so CMake links `/usr/lib/.../liblz4.so.1` by path
  and falls back to an identity codec when absent. Portable builds need the
  vendored single-file source (PT13). Also note the repo-wide `*.a` ignore added
  in review can hide a vendored static archive unless scoped to build outputs.
- [ ] **Oversized base pages still fall back to heap.** Until PT11 overflow
  pages land, a very large value can still build a heap-backed leaf that is not
  pool-resident/evictable and complicates PT9 alignment assumptions.
- [ ] **No durable redo of the delta tail.** By design (checkpoint + consensus
  replay). A crash between checkpoints loses in-memory deltas; the learner
  re-applies slots `> last_applied_slot`. Revisit only if checkpoint cadence
  proves too costly (persistence §6 TODO-CONFIRM).
