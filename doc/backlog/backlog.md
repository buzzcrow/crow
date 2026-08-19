<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# New Requirements — Backlog & Analysis

Forward-looking implementation items. Each item is classified by priority,
complexity, and dependency. Before implementation, follow the
[Implementation Process](#implementation-process) below.

---

## Item Index

**Next R number: R109** — Bump this line in the same commit when adding a new item.

### High Priority

- **[R108](R108-rpc-multi-engine-io.md)** — rpc multi-engine I/O
  worker model — Area: rpc — Split `io_workers` into `io_engines`
  (N independent epoll/kqueue instances) × `io_workers_per_engine`
  (M workers per engine, ONESHOT only when M>1) so the single-fd
  event-queue serialization bottleneck (~332K ops/s ceiling on M5
  Pro) can be broken by parallelizing across independent kernel
  event queues. `io_dispatch_threads` (Rust handler thread pool) is
  preserved as a third independent axis. Every point is
  independently tunable for per-platform profiling (macOS kqueue
  vs Linux epoll may scale differently). Backward-compatible:
  `with_workers(N)` maps to `(1, N)`.
- **[R104](R104-kv-server-group0-authoritative-restore.md)** — group-0
  authoritative restore (toml bootstrap-only) — Area: server / kv —
  Make the toml optional: on restart the server scans
  `<root>/waldata` for local stores/groups, loads them from disk via
  `create_group_with_wal`, then reads group 0 `/kv/replica/` records to
  wire remote replicas. The toml is only needed for first-boot tunables
  before group 0 exists. Implements the Phase 2 cutover from
  `design-crow-kv-group0.md` §5.1. Adds `--root` CLI (fixed subdir
  layout) and persists the node root to group 0 via `KvServerExtra`.
- **[R103](R103-chunkdb-range-migration.md)** — chunkdb range ownership
  migration — Area: chunkdb / kv — Implement the full
  `Copying`/`Cutover`/`Complete` migration flow for transferring chunkdb
  instance range ownership. Dual-serve reads during cutover, new-owner-only
  writes, background metadata verification, graceful client redirect.
  Distinct from R102: R103 transfers which chunkdb instance serves a hash
  range; R102 rebinds which paxos group stores a disk-group's data. Both
  reuse the common `BindingStrategy` framework
  (`doc/design/chunkdb/design-crow-chunkdb-range-binding.md` §5).
- **[R102](R102-diskdb-dynamic-binding-migration.md)** — diskdb dynamic
  disk-group binding migration — Area: diskdb / kv — Reuse the common
  `BindingStrategy` framework
  (`doc/design/chunkdb/design-crow-chunkdb-range-binding.md` §5) to
  dynamically rebind diskdb disk-groups to paxos groups, replacing the
  operator-manual `BindMapValue` write with automatic monitoring +
  rebinding. Monitor detects instance join/leave, rebalances disk-group
  assignments, migrates data during rebinding.
- **[R101](R101-kv-put-cas.md)** — KV compare-and-set on Put — Area: kv — Add `expected_revision` to `KvSetRequest` for optimistic concurrency; leader checks key revision before propose (lease-protected). Defense-in-depth for the chunkdb per-chunk lock (`doc/design/chunkdb/design-crow-chunkdb.md` §10); enables cross-instance CAS on `put_chunk` if range ownership is ever bypassed.
- **[R79](R79-diskdb-free-batch.md)** — diskdb free batch
  (size-threshold, no timer) — Area: diskdb — Group frees into a
  batch and flush via one `batch_write` when the batch reaches a
  configurable size (default 256). No timer — the flush is
  synchronous on the free path, not a background loop. v1 ships with
  immediate free (R72); this is a follow-up for high-free-throughput
  workloads.
- **[R80](R80-diskdb-rebalance.md)** — diskdb space rebalance across
  disks + disk-groups — Area: diskdb — New/recovered disks enter
  `allocating_disks` empty while peers stay near-full; the round-robin
  allocator is load-unaware so imbalance persists. Add imbalance
  gauges (per-disk-group `used_pct` spread), load-aware allocation
  skewing (weight new allocates by free space — passive convergence,
  no data move), and a per-disk-group rebalance planner that emits
  `RebalancePlanValue` (source busy blocks + `owner_chunk` + target
  disk) with placeholder relocation (`LogOnly`, no `diskio` — same
  envelope as the disk failure recovery scan). Disk-group-level
  rebalance is a caller concern
  (§3.2 — caller picks `disk_group_id`); diskdb contributes a
  `GetRebalanceHint` RPC + keepalive summary, not cross-instance
  moves. Real data relocation deferred to a future `diskio` service.
- **[R82](R82-kv-watch-notify-coalescing.md)** — watch/notify
  coalescing (debounce) — Area: kv / diskdb — the watch/notify
  extension ships without coalescing: one notify per changed key per
  matching prefix. Burst writes to a watched prefix (e.g. diskdb
  `batch_write` touching 10 disks) generate 10 separate notifies,
  amplifying subscriber wakeups + re-read load. Add a per-prefix
  debounce coalescer with timer-task flush between the apply-path hook
  and `WatchRegistry::emit`. The original coalescer was removed because
  the timer task captured no registry/coalescer refs (buffered keys
  were silently dropped); R82 must wire the `Weak` refs properly. Load
  optimization, not correctness — the safety-net poller covers missed
  notifies.

- **[R66](R66-kv-wal-io-uring.md)** — WAL io_uring backend — eliminate
  `spawn_blocking` on the durability path. The WAL's production I/O
  backend (`File` / `BlockDevice`) routes `fdatasync` and file writes
  through `tokio::fs` / `std::fs`, both of which use `spawn_blocking`
  internally (thread hop + blocking pool saturation under burst load).
  Add `IoBackend::Uring` variant that reuses the crow-tree C++ reactor
  (`lib/crow-tree/src/reactor.cpp`, already proven for B-tree page I/O)
  for WAL segment I/O via `io_uring` SQE/CQE. Expose the reactor's
  submit API (`submit_read`/`submit_write`/`submit_fsync`) via FFI as
  Rust async functions. `WalFileInner::Uring` implements all `WalFile`
  operations via reactor SQEs — no `spawn_blocking`, no thread hop.
  Fallback to `File` on non-Linux / no-liburing. `O_DIRECT` aligned
  writes. No `pipeline_writer` or `segment` API changes (drop-in async
  fn replacement). Linux + liburing only; tests skip on other platforms.

### Data Path (diskio + chunk object writers + read flow)

Dependency order: R105 → R93 → R94 → R106, R107. R32 depends
on the RPC library but is in a separate area (KV consensus).

- **[R105](R105-diskio-disk-io-engine.md)** — Disk IO engine — Area:
  diskio — Per-node disk IO server (`crow-diskio`) using io_uring on
  Linux (SQE/CQE for read/write/fsync, no `spawn_blocking`) with
  `pwrite`/`pread` fallback on macOS for dev/testing. Uses R104 RPC
  for control+data framing (control message = disk/zone/offset/size,
  followed by raw data payload). The missing data-I/O component that
  chunkdb, the writers (R94, R106), the read flow (R107), and recovery
  (R83) all depend on. Reference: the reference's disk I/O engine
  (libaio/SPDK engine shapes).
- **[R93](R93-chunkdb-mirror-to-ec-conversion.md)** — Mirror-to-EC
  conversion — Area: chunkdb — Background conversion of mirror strips
  to EC strips in shared chunks. Reads mirror data via diskio (R105),
  EC-encodes via isa-l, allocates EC strip blocks, writes via diskio,
  and atomically swaps via `update_chunk_strip`. Reclaims 3×→1.5×
  storage (8+4 EC) on shared chunks. Configurable policy (seal age,
  strip count, manual trigger) + bandwidth throttling. Foundation
  for R106's mirror-first write strategy.
- **[R94](R94-chunkdb-large-object-writer.md)** — Large object writer
  + chunk IO interface + Location — Area: chunkdb — Dedicated chunk
  per large object (> EC strip size, e.g. > 8 MB for 8+4). Direct EC
  strip writes, producer-consumer strip preallocation (object size
  known upfront → strip count known), ~1 GB max chunk size with chunk
  rotation for very large objects. **Defines the shared `ChunkIoWriter`
  async interface** (`on_data`/`on_finish`/`on_error` + completion)
  and the `Location` type (`chunk_id [offset, end)` + logical
  offset/length, array for multi-chunk) that R106 and R107 depend on.
  All writer code lives in `crow-chunkdb-client`. Reference: the reference's
  `SObjSChunkWriter` / `SObjMChunkWriter`.
- **[R106](R106-chunkdb-small-object-writer.md)** — Small object
  shared chunk writer — Area: chunkdb — Shared 256 MB chunks for
  small objects (< EC strip threshold). Dynamic pool of write
  pipelines, each with a worker task that fetches queued buffers and
  writes batches to shared chunks (aggregation for max TPS). Write
  to 3 mirror strips first → return success → background mirror→EC
  conversion (R93). Dynamic pipeline scale in/out based on queue
  depth for max BW + aggregation. Implements `ChunkIoWriter` (R94).
  Reference: the reference's `SharedObjWriter` + `Write2M1ECChunkHandler`.
- **[R107](R107-chunkdb-chunk-read-flow.md)** — Chunk object read
  flow — Area: chunkdb — Reconstructs object bytes from a `Location`
  array (R94). Queries chunk strip layout via `query_chunk`, maps
  offsets to strips, reads blocks via diskio (R105). Handles EC
  decode (for missing blocks, ≤ `code_num`) and mirror fallback (for
  failed replicas). Multi-chunk assembly in `logical_offset` order.
  Partial range reads (`read_range`). Streaming read for large
  objects (memory-bounded `ChunkReadStream`). Transparent across
  mirror→EC conversion (R93).

### Medium Priority

- **[R83](R83-chunkdb-complete-recovery-flow.md)** — chunkdb
  complete recovery flow (real data recovery + speed control) —
  Area: chunkdb / diskdb / diskio — diskdb's recovery is disk-layer
  only: the R76 `RecoveryScanTask` lists impacted busy blocks +
  `owner_chunk` but the repair step is a placeholder
  (`RecoveryAction::LogOnly`, no data rebuild). There is no chunkdb
  yet (only a reserved proto surface), so when a disk goes `Bad` the
  impacted blocks are handed to a "future recovery/relocation path"
  (§8) that does not exist — no surviving replica/parity is read, no
  rebuilt data is written, no strip is updated. Full data recovery
  needs chunkdb (the chunk→strip→segment owner) to rebuild lost
  mirror replicas / EC data+parity from surviving strips via the
  `diskio` service, `UpdateChunkStrip` to new segments, and free the
  old `Bad`-disk segments. Recovery speed must be throttled at the
  chunkdb layer (configurable bandwidth/IOps/concurrency) so
  foreground traffic is not starved. Blocked on the chunkdb server
  component + the `diskio` service (both unlanded; must be filed as
  their own backlog items first). Replaces R76's `LogOnly` with
  `Relocate` / `RebuildFromEc`.
- **[R84](R84-chunkdb-post-disk-move-placement-scanner.md)** —
  chunkdb post-disk-move placement scanner — Area: chunkdb / diskdb —
  R81 Part 2 adds disk move with a stable `DiskId` (record copy
  during Maintenance, no full scan). The move is placement-only and
  the data is intact, but there is no verification that chunk
  placement is still consistent after a move: chunks reference blocks
  via `Segment { disk_id, ... }` (in `MirrorStrip` / `EcStrip`), and
  every chunk with a segment on the moved disk must still reach that
  segment via the disk's new placement. Add a placement-integrity
  scanner (chunkdb-side, following diskdb's `ScannerTask` /
  `BgRunner` pattern, §10) that walks chunk→strip→segment after a
  move (and periodically), resolves each segment's `DiskId` to its
  current group-0 placement, and reports unreachable / orphaned
  segments — handing `Bad`/`Missing`-disk segments to R83 for
  rebuild. Triggered on move via watch/notify (R78) with a periodic
  safety net. Blocked on the chunkdb server component (unlanded) and
  R81 Part 2.
- **[R32](R32-kv-custom-rust-rpc.md)** — KV consensus hot path →
  `crow-rpc` — Area: kv / RPC — Migrate the internal replica-to-replica
  Paxos path from gRPC/tonic to the `crow-rpc` flatbuffer RPC library.
  Recovers the ~17% h2-lock throughput loss at 2T:1C
  (measured in `kv-read-flow-analysis.md`). Protocol semantics
  preserved (same request/response shapes, `NotLeaderHint`, error
  codes); only the transport changes. Depends on `crow-rpc` (RPC lib).
  Management API stays on Axum/HTTP. Reference: the reference's RPC engine.
- **[R68](R68-kv-write-largeval-bench.md)** — Large-value write
  benchmark — Area: cluster / maintenance / bench — R67 fixed the 16 KiB
  scan error spike by wrapping the maintenance loop's `flush` /
  `persist_snapshot` / `collect_garbage` in `spawn_blocking`, but
  verified it only on the scan path. The maintenance loop runs
  identically under write load, yet the write regression sentinel
  (`bench-write-regression.sh`) only exercises 512 B values — there is
  no large-value write config. Add a `largeval_16k` write config
  (`--value-size 16384`, 100k keys, 10s mem mode) and verify 0 write
  errors across 3 consecutive runs on Linux. If errors appear, RCA into
  whether the R67 fix has a write-path gap and file a follow-up
  requirement. Low complexity; verifies R67's coverage extends to
  writes.
- **[R33](R33-crow-tree-rename.md)** — Extract crow-tree to separate repo and rename — Area:
  workspace — Move `crowtree/` into its own git repository (preserving
  history), wire `crow-kv` to depend on `crow-tree-ffi` as an external
  dependency, and rename the crate/namespace/macros from `crowtree` to
  `crow-tree` / `crow::tree` / `CROW_TREE_*`. Establishes the `crow-kv` →
  `crow-tree` dependency boundary analogous to `crow-kv` → `crow-common`.
  Most naturally done after R12.
- **[R50](R50-epoch-protected-memtable.md)** — Epoch-protected
  lock-free MemTable — Area: scan / get / crow-tree engine —
  **Done.** `MemTable::snapshot()` deep-copied every live L0 entry
  (key + full cell payload) on every scan regardless of range or
  `limit`, and an L0 `get` hit copied twice. Root cause: L0 was the
  only reader-visible structure outside the engine's EBR scheme.
  Replaced the `absl::btree_map` under `mu_` with a
  `ConcurrentSkipList` (inline keys, versioned cell pointers,
  epoch-deferred reclamation). Readers now traverse L0 lock-free
  under their existing epoch guard with zero copy; the cursor seeks
  directly (no `upper_bound` skip pass); `get_view` borrows the
  cell directly off the node. Closes the known gap at
  `crow-tree.h:81`. All 383 `test-tree-ct` tests pass.

### Low Priority

**Complexity — Low (placeholder):**
- **[R5](R5-rdma-alloc.md)** — RDMA-pinned allocation — Blocked by: RDMA backend — Area: crowtree
  engine — `buffer::allocate` seam is designed for RDMA-pinned memory but no
  RDMA backend exists yet; placeholder only.

**Complexity — Medium:**
- **[R4](R4-bounded-mempool.md)** — Bounded memory pool — Area: crowtree engine — `buffer::allocate` uses
  unbounded `std::malloc`; a burst of large writes can spike RSS without
  backpressure.
- **[R52](R52-reverse-scan.md)** — Reverse scan — Area: scan / crow-tree
  engine — `scan` is forward-only today (ascending key order). Reverse
  scan (descending order, `start_before` instead of `start_after`) is a
  distinct cost shape: the B+tree descent targets the leaf containing
  `start_before`, the merge loop walks cursors backward, and the
  `LeafChainCursor` needs a reverse seek/advance. The skip-list L0
  cursor (R50) is forward-only — a reverse cursor would need
  `prev()` links or a separate reverse traversal path. Client API:
  `KvScanRequest` gains a `direction` field; the S3-style pagination
  uses the first key of each page as the next `start_before`. Needs
  its own scan perf baseline (reverse scans have different cache
  behavior — backward leaf traversal touches pages in reverse
  allocation order).
- **[R54](R54-kv-scan-engine-profiling.md)** — Scan engine profiling —
  Area: scan / crow-tree engine — both read modes saturate near ~38k
  scans/s at 32T:32C; the bottleneck moved to the C++ crow-tree merge
  loop (L0 skip-list + L1 B+tree cursor) but the specific hot spot is
  unknown. Add `tools/profile-scan.sh` (mirroring
  `tools/profile-write.sh`), profile the 32T:32C scan bench, and
  document the top hot stacks. Investigation only — no scan-path code
  changes. If a clear optimization target emerges, file a follow-up
  requirement with the profiling evidence. Low complexity.
- **[R60](R60-tree-scan-sibling-leaf-readahead.md)** — Sibling-leaf
  readahead on cold scans — Area: scan / crow-tree engine — the scan
  path demand-loads each L1 leaf inline (sync) or one pending page per
  reactor round trip (async), so a cold multi-leaf range pays one
  stall/round-trip per leaf, serialized with merge work on prior
  leaves. The scan knows `right_sibling` (`crow-tree.cpp:1822/2074`)
  before finishing the current leaf — issue a readahead for the next
  leaf to overlap I/O with merging. Sync path: prefetch the
  right-sibling page id via a page-cache async-resolve seam. Async
  path: batch the right-sibling read with the current leaf's read in
  the reactor submission (small readahead window, default 1). Win is
  zero on mem-mode (leaves resident); needs a cold/disk bench config to
  validate. Medium complexity.
- **[R68](R68-kv-write-largeval-bench.md)** — Large-value write
  benchmark — Area: cluster / maintenance / bench — R67 fixed the 16 KiB
  scan error spike by wrapping the maintenance loop's `flush` /
  `persist_snapshot` / `collect_garbage` in `spawn_blocking`, but
  verified it only on the scan path. The maintenance loop runs
  identically under write load, yet the write regression sentinel
  (`bench-write-regression.sh`) only exercises 512 B values — there is
  no large-value write config. Add a `largeval_16k` write config
  (`--value-size 16384`, 100k keys, 10s mem mode) and verify 0 write
  errors across 3 consecutive runs on Linux. If errors appear, RCA into
  whether the R67 fix has a write-path gap and file a follow-up
  requirement. Low complexity; verifies R67's coverage extends to
  writes.

---

## Implementation Process

Each item follows the lifecycle defined in the
[`/implement-requirement` workflow](../../.devin/workflows/implement-requirement.md):
understand → design → plan → implement → merge design → cleanup.

After the PR is merged, all obsolete working docs (design draft, plan doc)
must be deleted — see the workflow's Post-merge cleanup section.

---

<!-- Reference implementation details: see ~/.codeium/windsurf/memories/global_rules.md -->
