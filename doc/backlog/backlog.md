<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# New Requirements — Backlog & Analysis

Forward-looking implementation items. Each item is classified by priority,
complexity, and dependency. Before implementation, follow the
[Implementation Process](#implementation-process) below.

---

## Item Index

**Next R number: R126** — Bump this line in the same commit when adding a new item.

### High Priority

- **[R118](R118-cluster-unify-port-usage.md)** — unify port usage &
  test port prober — Area: cluster / protocol / server —
  `crowdb-protocol/src/ports.rs` already defines base ports + stride +
  `ServicePort` for all services, but adoption is incomplete:
  `crowdb-kv-server` has no CLI flag for the consensus (`KV_RPC_BASE`) or
  client-facing (`KV_CLIENT_RPC_BASE`) RPC ports; `crowdb-diskdb` takes
  listen addrs from TOML config (not CLI flags with `ports.rs` defaults);
  and `KvServer::start` still supports port 0 (OS-assigned), contradicting
  the project flow. Wire every server to accept explicit per-listener
  port flags (defaults from `crowdb-protocol::ports`), reject port 0, and
  add an in-process port-prober + flock-coordinated claim file (library
  + `crowdb-port-alloc` CLI binary, no daemon) that is the single place
  picking ports, so tests and cluster bootstrap run in parallel without
  `Address already in use`. Console UI E2E shells out to the CLI binary
  to replace its private `freePort()` counter. Open questions: claim-to-
  bind TOCTOU mitigation, claim-file path/format, probe port range,
  cross-host scope, paired-port override semantics.
- **[R119](R119-cluster-log-file-usage-review.md)** — log file usage
  review & unification — Area: cluster / observability — CROWDB has
  two logging stacks (Rust `tracing` in `crowdb-common/rust`, C++
  `spdlog` in `crowdb-common/cpp`) and four servers + `crowdb-cli`, but
  only `crowdb-kv-server` wires up file logging with rotation +
  compression; `crowdb-diskdb`, `crowdb-chunkdb`, and `crowdb-web`
  initialize console-only `tracing_subscriber::fmt().init()` and
  lose every log line when daemonized. The `crowdb-rpc` C++ library
  ships a `crowdb_rpc_init_logging` C API that no Rust caller ever
  invokes (no FFI wrapper, no call site), so the consensus
  transport layer is silent. Log directories diverge (`"log"`,
  `~/.crowdb-kv/log`, temp paths), log formats differ between Rust
  and C++, and no audit has been done of whether log lines are
  meaningful and self-explaining. R119 does a two-prong audit
  (code review of every logging call site + run each service's
  e2e test and read the real log output), then unifies every
  server on the shared rotating-file logging stack, wires the
  crowdb-rpc C++ logging through an FFI bridge, adopts one log
  directory + format convention, adds a "Logging" section to
  `design-crowdb-kv-observability.md` (current design gap — the doc
  covers metrics only), and fixes log lines that are opaque,
  noisy, or missing context. Verification: each service's e2e
  test asserts its log file exists, is non-empty, and contains
  self-explaining key lines.
- **[R103](R103-chunkdb-range-migration.md)** — chunkdb range ownership
  migration — Area: chunkdb / kv — Implement the full
  `Copying`/`Cutover`/`Complete` migration flow for transferring chunkdb
  instance range ownership. Dual-serve reads during cutover, new-owner-only
  writes, background metadata verification, graceful client redirect.
  Distinct from R102: R103 transfers which chunkdb instance serves a hash
  range; R102 rebinds which paxos group stores a disk-group's data. Both
  reuse the common `BindingStrategy` framework
  (`doc/design/chunkdb/design-crowdb-chunkdb-range-binding.md` §5).
- **[R102](R102-diskdb-dynamic-binding-migration.md)** — diskdb dynamic
  disk-group binding migration — Area: diskdb / kv — Reuse the common
  `BindingStrategy` framework
  (`doc/design/chunkdb/design-crowdb-chunkdb-range-binding.md` §5) to
  dynamically rebind diskdb disk-groups to paxos groups, replacing the
  operator-manual `BindMapValue` write with automatic monitoring +
  rebinding. Monitor detects instance join/leave, rebalances disk-group
  assignments, migrates data during rebinding.
- **[R101](R101-kv-put-cas.md)** — KV compare-and-set on Put — Area: kv — Add `expected_revision` to `KvSetRequest` for optimistic concurrency; leader checks key revision before propose (lease-protected). Defense-in-depth for the chunkdb per-chunk lock (`doc/design/chunkdb/design-crowdb-chunkdb.md` §10); enables cross-instance CAS on `put_chunk` if range ownership is ever bypassed.
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
  Add `IoBackend::Uring` variant that reuses `DiskIOUring` in
  `crowdb-common` (already proven for B-tree page I/O) for WAL segment
  I/O via `io_uring` SQE/CQE. Expose `DiskIOUring`'s submit API
  (`submit_read`/`submit_write`/`submit_fsync`) via FFI as Rust async
  functions. `WalFileInner::Uring` implements all `WalFile` operations
  via `DiskIOUring` SQEs — no `spawn_blocking`, no thread hop.
  Fallback to `File` on non-Linux / no-liburing. `O_DIRECT` aligned
  writes. No `pipeline_writer` or `segment` API changes (drop-in async
  fn replacement). Linux + liburing only; tests skip on other platforms.

### Data Path (diskio + chunk object writers + read flow)

Dependency order: R93 → R106, R107 → R110, R111, R112
(R110/R112 reuse R110's negative list; R111 reuses R110's negative
list + degraded-strip tracking). The RPC migration items (R115,
R116, R117) are in a separate area (see RPC Migration section
below); R32 depends on R115.

- **[R93](R93-chunkdb-mirror-to-ec-conversion.md)** — Mirror-to-EC
  conversion — Area: chunkdb — Background conversion of mirror strips
  to EC strips in shared chunks. Reads mirror data via diskio (R105),
  EC-encodes via isa-l, allocates EC strip blocks, writes via diskio,
  and atomically swaps via `update_chunk_strip`. Reclaims 3×→1.5×
  storage (8+4 EC) on shared chunks. Configurable policy (seal age,
  strip count, manual trigger) + bandwidth throttling. Foundation
  for R106's mirror-first write strategy.
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

- **[R110](R110-chunkdb-chunkio-error-handling.md)** — Large-write
  IO error handling (write path) — Area: chunkdb / diskdb / diskio
  — In-line error handler for the large-write data path (R94),
  spanning three services: chunkdb (strip metadata,
  `update_chunk_strip`), diskdb (block allocation with disk
  exclusion), diskio (write/fsync error detection). Single-block
  replacement on write failure (not whole-strip retry): keep
  successful blocks, re-allocate the failed block on a healthy
  disk via diskdb, `update_chunk_strip` to replace the segment in
  chunkdb. Negative list (TTL-based) temporarily blocks bad disks
  from new allocations across diskdb — shared with R111 (read) and
  R112 (small-write). Degraded strip tracking (parity missing,
  data durable). Escalation to R83 recovery when inline retries
  are exhausted. Read-path error handling is a separate requirement
  (R111); R110 defines the negative list and degraded-strip
  tracking that R111 and R112 reuse.
- **[R111](R111-chunkdb-read-io-error-handling.md)** — Chunk read
  IO error handling (unified read path) — Area: chunkdb / diskdb /
  diskio — In-line error handler for the read path (R107), which
  is unified across large objects (EC strips, R94) and small
  objects (mirror strips, R106, before R93 conversion). EC decode
  fallback for failed EC blocks (read surviving data + parity,
  isa-l decode the missing block, within `code_num` tolerance);
  mirror replica fallback for failed mirror strips (read next
  replica). Background rebuild + replace after a successful
  fallback (allocate new block via diskdb, write reconstructed
  data via diskio, `update_chunk_strip` to repair the strip so
  future reads don't pay the fallback cost). Degraded strip read
  tolerance (parity missing — readable for full-data, partial
  result + R83 escalation on data block failure). Partial read
  results with explicit failed byte ranges (no silent corruption —
  applies to both `read_range` and `ChunkReadStream`). Escalation
  to R83 when inline fallback is unrecoverable. Reuses R110's
  negative list and degraded-strip tracking.
- **[R112](R112-chunkdb-small-write-io-error-handling.md)** —
  Small-write IO error handling (multi-service cooperation) — Area:
  chunkdb / diskdb / diskio — In-line error handler for the
  small-write data path (R106), spanning the same three services
  as R110. Reuses R110's negative list and escalation reporting,
  but adds batch-aware per-object retry (a single diskio write
  carries a batch of N small objects — a partial failure must
  track which objects were written and retry only the unwritten
  ones) and mirror-replica replacement specific to the shared-
  chunk writer (R106 writes 3 mirror replicas first, then R93
  converts to EC in the background — a single replica failure is
  tolerated but must be re-allocated + `update_chunk_strip` to
  restore 3-replica durability). Shared chunk rotation safety
  (mid-rotation failure must not corrupt the sealed portion or
  span a corrupted boundary). Clear boundary with R93's mirror→EC
  conversion (R112 handles write-path failures; R93 handles
  conversion-path failures). Escalation to R83 when inline retries
  are exhausted.
- **[R113](R113-chunkio-batch-strip-allocation.md)** — Batch strip
  allocation + deferred chunkdb confirm — Area: chunkio / chunkdb /
  diskdb — Optimize the large-write strip allocation path (R94) to
  reduce `append_chunk` RPC count. Current flow: one `append_chunk`
  per strip (250K RPCs for a 1 TB object). Two candidate approaches:
  (1) batch `append_chunk(strip_count=N)` — chunkdb allocates N
  strips in parallel, persists once, returns `Chunk` with N strips.
  Simple, safe, but first strip waits for all N. (2) Direct diskdb
  allocation + deferred chunkdb confirm — client allocates blocks
  from diskdb directly (TENTATIVE), writes immediately, batch-
  confirms to chunkdb later. Maximum overlap but requires client-
  side placement, a new confirm RPC, and a TENTATIVE block reaper.
  Key design tension: the chunk allocate confirm flow
  (`BusyBlockValue.commit_state: TENTATIVE → COMMITTED`) must
  guarantee crash safety — TENTATIVE blocks with written data that
  are never confirmed must be reclaimable. Blocked on the chunk-
  layer refactor (`doc/working/design-chunk-layer-refactor.md`).

### Medium Priority

- **[R121](R121-tree-cpp-lock-review.md)** — C++ mutex/lock review
  fixes — Area: tree / rpc / common — Full review of all mutex/lock
  usage across the C++ codebase (37 files) found 3 critical hot-path
  findings, 5 medium, 17 OK. Highest-impact: `BufferPool::pin` holds
  the global mutex during synchronous disk I/O on a cache miss — every
  cache hit blocks behind every miss, making the buffer pool mutex a
  global serialization point. Also critical: `ConcurrentSkipList`
  spinlock (no backoff/fairness) on the `apply_batch` → `MemTable::upsert`
  write path, and `HandlerRegistry::get_handler` mutex on every RPC
  frame dispatch (handlers registered once at startup, never change).
  Medium: `MetricsRegistry::register_*` data race (unsynchronized
  `push_back` vs. `flush_to` iteration), `thread_name_flag` mutex on
  every log line, `ConnectionPool` mutex on every connection acquire,
  `Crowdbtree::resident` cold-path load serialization, `slot_mutex_` set
  insert on the apply path. R121 tracks the fixes; OK findings document
  correct patterns and need no action.
- **[R122](R122-kv-rust-lock-review.md)** — Rust mutex/lock review
  fixes — Area: kv / client / diskdb / server — Full review of all
  mutex/`RwLock`/`parking_lot`/`tokio::sync` lock usage across the Rust
  crates (75 files) found 3 critical hot-path findings, 7 medium, ~30
  OK. The codebase leans heavily on lock-free patterns (`DashMap`,
  `Atomic*`, `arc_swap`, `OnceLock`, per-bit CAS), so findings are
  fewer/milder than C++ (R121). Highest-impact:
  `PxLearner::out_of_order` `Mutex<BTreeMap>` taken on every accepted
  slot on a follower for the chosen-frontier advance — serializes
  concurrent out-of-order applies. Also critical: `WalEngine::index`
  `Mutex<SegmentIndex>` held during every flush batch insert (multi-
  pipeline flush completions serialize; GC stalls flushes), and
  `ClientMetrics::window_lat` `std::Mutex` on every client RPC
  completion. Medium: `RangeBindingClient` `RwLock` read on every route
  (should use `arc_swap`), `MetricsRunner` collector inside
  `registry.lock()` (same root cause as R121 #4), `PxGroup` coalescer/
  peer-watermark mutexes, `PxLocalReplica` gap-slots/election-state
  locked reads. R122 tracks the fixes; OK findings document notably
  good lock-free design.
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
  `crowdb-rpc` — Area: kv / RPC — Migrate the internal replica-to-replica
  Paxos path from the legacy tonic/h2 stack to the `crowdb-rpc` flatbuffer RPC library.
  Recovers the ~17% h2-lock throughput loss at 2T:1C
  (measured in `kv-read-flow-analysis.md`). Protocol semantics
  preserved (same request/response shapes, `NotLeaderHint`, error
  codes); only the transport changes. Depends on R104 (finished) +
  R114 (finished — bidirectional request-response for LearnerStream +
  StreamSnapshot). Management
  API stays on Axum/HTTP. Open Question resolved: full `.fbs`
  conversion (no prost bridge — the rejected approach), consistent with R105/diskio.

### RPC Migration (legacy → crowdb-rpc)

Dependency order: R115 → R116 (unary); R117 (streaming) depends on
R114 (finished) + R32. R115 lands first to validate the
migration pattern (schema, server, client, error mapping, mixed
rollout) before the streaming services. All four items follow the
zero-copy wrapper convention (`design-crowdb-rpc.md` §6): `FB`-prefixed
flatbuffer types, wrapper classes in `crowdb-protocol`, no owned
intermediate structs, no per-field copy. All four items (R115 diskdb,
R32 KV consensus, R117 KV client-facing, R116 chunkdb) are DONE.

- **[R68](R68-kv-write-largeval-bench.md)** — Large-value write
  benchmark — Area: cluster / maintenance / bench — R67 fixed the 16 KiB
  scan error spike by wrapping the maintenance loop's `flush` /
  `persist_snapshot` / `collect_garbage` in `spawn_blocking`, but
  verified it only on the scan path. The maintenance loop runs
  identically under write load, yet the write regression sentinel
  (`bench-kv-write-regression.sh`) only exercises 512 B values — there is
  no large-value write config. Add a `largeval_16k` write config
  (`--value-size 16384`, 100k keys, 10s mem mode) and verify 0 write
  errors across 3 consecutive runs on Linux. If errors appear, RCA into
  whether the R67 fix has a write-path gap and file a follow-up
  requirement. Low complexity; verifies R67's coverage extends to
  writes.
- **[R33](R33-crowdb-tree-rename.md)** — Extract crowdb-tree to separate repo and rename — Area:
  workspace — Move `crowdbtree/` into its own git repository (preserving
  history), wire `crowdb-kv` to depend on `crowdb-tree-ffi` as an external
  dependency, and rename the crate/namespace/macros from `crowdbtree` to
  `crowdb-tree` / `crow::tree` / `CROWDB_TREE_*`. Establishes the `crowdb-kv` →
  `crowdb-tree` dependency boundary analogous to `crowdb-kv` → `crowdb-common`.
  Most naturally done after R12.
- **[R50](R50-epoch-protected-memtable.md)** — Epoch-protected
  lock-free MemTable — Area: scan / get / crowdb-tree engine —
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
  `crowdb-tree.h:81`. All 383 `test-tree-ct` tests pass.

### Low Priority

**Complexity — Low (placeholder):**
- **[R5](R5-rdma-alloc.md)** — RDMA-pinned allocation — Blocked by: RDMA backend — Area: crowdbtree
  engine — `buffer::allocate` seam is designed for RDMA-pinned memory but no
  RDMA backend exists yet; placeholder only.

**Complexity — Medium:**
- **[R4](R4-bounded-mempool.md)** — Bounded memory pool — Area: crowdbtree engine — `buffer::allocate` uses
  unbounded `std::malloc`; a burst of large writes can spike RSS without
  backpressure.
- **[R52](R52-reverse-scan.md)** — Reverse scan — Area: scan / crowdb-tree
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
  Area: scan / crowdb-tree engine — both read modes saturate near ~38k
  scans/s at 32T:32C; the bottleneck moved to the C++ crowdb-tree merge
  loop (L0 skip-list + L1 B+tree cursor) but the specific hot spot is
  unknown. Add `tools/profile-scan.sh` (mirroring
  `tools/profile-write.sh`), profile the 32T:32C scan bench, and
  document the top hot stacks. Investigation only — no scan-path code
  changes. If a clear optimization target emerges, file a follow-up
  requirement with the profiling evidence. Low complexity.
- **[R60](R60-tree-scan-sibling-leaf-readahead.md)** — Sibling-leaf
  readahead on cold scans — Area: scan / crowdb-tree engine — the scan
  path demand-loads each L1 leaf inline (sync) or one pending page per
  reactor round trip (async), so a cold multi-leaf range pays one
  stall/round-trip per leaf, serialized with merge work on prior
  leaves. The scan knows `right_sibling` (`crowdb-tree.cpp:1822/2074`)
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
  (`bench-kv-write-regression.sh`) only exercises 512 B values — there is
  no large-value write config. Add a `largeval_16k` write config
  (`--value-size 16384`, 100k keys, 10s mem mode) and verify 0 write
  errors across 3 consecutive runs on Linux. If errors appear, RCA into
  whether the R67 fix has a write-path gap and file a follow-up
  requirement. Low complexity; verifies R67's coverage extends to
  writes.
- **[R120](R120-kv-ignored-test-migration.md)** — revive crowdb-rpc migrated
  ignored tests — Area: kv / tests — Two `#[ignore]`d test stubs in
  `group_test.rs` cover real contracts with no other coverage at the
  `crowdb-kv` layer: the forwarded-flag loop guard on `Get`/`Scan` (a
  follower must not re-forward an already-forwarded request) and malformed
  `Accept` rejection on the LearnerStream bidi path. Both have empty
  bodies pending crowdb-rpc migration; the infrastructure now exists — write
  the test bodies and un-ignore. Low complexity; no dependencies.
---

## Implementation Process

Each item follows the lifecycle defined in the
[`/implement-requirement` workflow](../../.devin/workflows/implement-requirement.md):
understand → design → plan → implement → merge design → cleanup.

After the PR is merged, all obsolete working docs (design draft, plan doc)
must be deleted — see the workflow's Post-merge cleanup section.

---

<!-- Reference implementation details: see ~/.codeium/windsurf/memories/global_rules.md -->
