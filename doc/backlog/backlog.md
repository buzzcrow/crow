<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# New Requirements — Backlog & Analysis

Forward-looking implementation items. Each item is classified by priority,
complexity, and dependency. Before implementation, follow the
[Implementation Process](#implementation-process) below.

---

## Item Index

**Next R number: R67** — Bump this line in the same commit when adding a new item.

### High Priority

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
- **[R57](R57-tree-scan-zero-copy-staging.md)** — Zero-copy engine scan
  result staging — Area: scan / crow-tree engine — each scan page is
  copied 3 times before the FFI boundary: `consider` lambda stages into
  `std::vector<scan_entry>` (`crow-tree.cpp:1853/1868`), `ct_scan`
  re-packs into `std::string packed` (`c_api.cpp:912-920`), `make_buf`
  mallocs+memcpys again (`c_api.cpp:43/921`). ~10.5 MiB memcpy per full
  3.5 MiB page. Fix: pack the wire format directly in `consider` (single
  growing buffer) and transfer ownership across the FFI via the
  `make_borrowed_buf` pattern already used by the get fast path. Collapses
  3 copies to 1 (the unavoidable wire-format assembly). Design-level
  redundancy, not profiling-guided. Medium complexity.
- **[R58](R58-tree-scan-merge-loop-fast-path.md)** — Merge loop 2-source
  fast path + loser tree — Area: scan / crow-tree engine — the merge loop
  does 2 × N_sources byte-wise compares per output entry
  (`crow-tree.cpp:1890-1934`): a min-key scan then a winner pass. The
  common case (1 active L0 + L1, no frozen memtables) is a trivial 2-way
  min needing 1 compare, not a 2-pass vector scan. Add a 2-source fast
  path branch; for k > 2 (several frozen memtables) use a loser tree
  (O(log k) per merge). Add `__builtin_prefetch` for the next skip-list
  node and right-sibling leaf. Design-level redundancy (k-way merge has a
  known O(log k) structure). Medium complexity.
- **[R59](R59-kv-snapshot-scan.md)** — Two scan modes + snapshot
  versioning API — Area: scan / kv / crow-tree engine — the current
  `scan` is the only range-read surface (S3-list semantics: per-page
  consistent, not cross-page). R59 formalizes two modes: (1) **list
  scan** — the existing `scan`, fast, latest values, for interactive
  listing; (2) **snapshot versioning API** — flush + `snapshot_view()`
  (already built, pins L1 at `last_applied_slot`, zero-copy page
  refcounts) + iterate the frozen vector with prefix/pagination. New
  RPCs: `CreateSnapshot`/`ListSnapshots`/`SnapshotScan`/
  `ReleaseSnapshot` + management API for `SetGcWatermark`. No new engine
  machinery (no version chain, no L0 pinning — flush drains L0 first).
  Active snapshots protect pinned pages from GC via refcount. Medium
  complexity.
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

---

## Implementation Process

Each item follows the lifecycle defined in the
[`/implement-requirement` workflow](../../.devin/workflows/implement-requirement.md):
understand → design → plan → implement → merge design → cleanup.

After the PR is merged, all obsolete working docs (design draft, plan doc)
must be deleted — see the workflow's Post-merge cleanup section.
