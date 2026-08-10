<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# New Requirements — Backlog & Analysis

Forward-looking implementation items. Each item is classified by priority,
complexity, and dependency. Before implementation, follow the
[Implementation Process](#implementation-process) below.

---

## Item Index

**Next R number: R78** — Bump this line in the same commit when adding a new item.

### High Priority

- **[R69](R69-diskdb-disk-block-allocator.md)** — diskdb: distributed
  disk-block allocator on CROW — Area: new component / diskdb —
  Redesign and implement a distributed disk-block allocator as a new
  CROW component, referencing the original design in another project
  (aioss `server/diskdb`). diskdb is a lightweight, stateless server
  that owns some disk-groups, allocates 1 MB blocks from per-zone
  append-only allocators (CAS-serialized in-memory), and persists all
  state to CROW KV (group 0 for sysdata, paxos data groups for zone
  journals) — no local WAL. **Scoping + foundation requirement** —
  defines the full scope, design decisions (D1–D8), writes the
  high-level design doc (`doc/design/diskdb/design-crow-diskdb.md`),
  splits implementation into R70–R77, and sets up the project skeleton
  (`lib/protocol`, `app/crow-diskdb`, `lib/crow-diskdb-client`)
  with correct dependencies and a clean build.

- **[R70](R70-diskdb-protocol-core-types.md)** — diskdb protocol + core
  types — Area: diskdb — Fill in the protobuf services
  (allocate/free/active_zone/query) in `lib/protocol`, core types
  (Segment, ZoneRecord, BusyRecord, FreeRecord, ZoneSnapshot), and
  config validation. Foundation for R71–R76.
- **[R71](R71-diskdb-group0-sysdata-sync.md)** — diskdb group-0 sysdata
  + sync — Area: diskdb — Implement the group-0 sysdata schema
  (node/disk/disk-group metadata, ownership map, binding map), periodic
  sync loop, ownership/binding map read/write, instance heartbeat
  registration. Disk status management component.
- **[R72](R72-diskdb-zone-allocator-journal.md)** — diskdb zone
  allocator + journal persistence — Area: diskdb — Implement the zone
  CAS allocator (Active→Busy→Active), active zone deque, disk-level
  round-robin, two-phase async allocation (sync CAS claim + async KV
  persist of BusyRecord), free batch flush (FreeRecord), `active_zone`
  API. Block allocate/free component.
- **[R73](R73-diskdb-crash-recovery-snapshot.md)** — diskdb crash
  recovery + snapshot compaction — Area: diskdb — Implement journal
  replay (reconstruct in-memory bitmap + allocate_pos from
  BusyRecord/FreeRecord + ZoneSnapshot), snapshot compaction (write new
  snapshot, batch-delete expired records). Crash safety.
- **[R74](R74-diskdb-space-metrics-query.md)** — diskdb space metrics +
  query API — Area: diskdb — Implement per-disk / per-disk-group /
  per-zone space metrics with accurate accounting and a recalculation
  path (replay journal to verify). `query_disk_usage` gRPC API. Space
  metrics component.
- **[R75](R75-diskdb-background-scanner.md)** — diskdb background
  scanner — Area: diskdb — Implement ghost-allocation detection,
  allocate_pos drift detection, record integrity (CRC). Leak detection
  deferred (needs caller registries).
- **[R76](R76-diskdb-disk-discovery-health.md)** — diskdb disk discovery
  + health probing — Area: diskdb — Implement config-driven disk
  discovery, health probing (existence, size, basic read/write test),
  disk failure detection + recovery flow.
- **[R77](R77-diskdb-console-cli.md)** — diskdb console + CLI
  integration — Area: diskdb / console — Disk/disk-group management UI,
  zone busy/free visualization (block array chart), CLI command design
  (`crow diskdb` subcommands vs sub-wrapper binaries). Follow-up after
  core diskdb is functional.

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

### Medium Priority

**Complexity — Medium:**
- **[R32](R32-kv-custom-rust-rpc.md)** — Custom Rust RPC library to replace gRPC on the hot path — Area:
  RPC / consensus — gRPC (tonic + h2) serializes concurrent writers on a
  connection-level userspace lock (HPACK table, frame buffer,
  flow-control windows); measured cost is ~17% at 2T:1C, zero at
  1T:1C. A custom `[len][req_id][protobuf]`-over-raw-TCP transport
  removes the userspace funnel — the kernel TCP lock is the only
  serialization point. **Deferred until** read throughput is the
  primary constraint AND the h2 lock is profiled as the hot spot; until
  then write-path (R16a/R17) and disk-I/O work take precedence.
  High complexity (2–4K lines: framing, pool, reconnect, timeout,
  cancellation, backpressure, TLS). Scope is the internal
  replica-to-replica path only; management API stays on Axum/HTTP.
  Reference implementations: protosocket (Momento), Volo (CloudWeGo),
  Cap'n Proto RPC.
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
