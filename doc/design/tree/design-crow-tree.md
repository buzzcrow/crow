<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROW - Design: crow-tree Storage Engine (Overview)

Depends on: [`../kv/design-crow-kv.md`](../kv/design-crow-kv.md), [`../kv/design-crow-kv-state-machine.md`](../kv/design-crow-kv-state-machine.md)
Satisfies: [`../kv/design-crow-kv.md`](../kv/design-crow-kv.md) §8.3 (learner storage)

This is the parent document for **crow-tree**, the production storage engine that
backs CROW learners: an embeddable, ordered key-value engine implementing the
CROW `KVEngine` contract, built as a standalone C++ library (`libcrow-tree`) and
consumed from the Rust `crow-kv` crate over a C ABI. It records the decisions
behind that design and maps the sub-design documents. `libcrow-tree` is fully
implemented, wired into `crow-kv` (`CrowTreeEngine`), and shipped; this document
set is the durable record of *why* it looks the way it does, not a build plan.
See [`todo_code.md`](../todo_code.md) for anything still open.

## Table of Contents

- [1. Goals and Non-Goals](#1-goals-and-non-goals)
- [2. Architecture](#2-architecture)
- [3. Engine Abstraction](#3-engine-abstraction)
- [4. FFI Boundary](#4-ffi-boundary)
- [5. Sub-Design Document Map](#5-sub-design-document-map)
- [6. Decision Log](#6-decision-log)

---

## 1. Goals and Non-Goals

**Goals**

- One ordered, **single version per key** KV engine that serves all learner
  reads. Only the latest write per key is kept (no multi-version / time-travel).
- **Slot-aware storage:** every live key carries the consensus slot (= WAL
  position) of the write that produced it, stored **in the value cell, not the
  key**. Putting it in the key would make each key sort into multiple ordered
  versions — multi-version storage — which is explicitly not wanted.
- Pluggable persistence behind one page-granular backend: a **file** store and a
  **block-device** store. The block-device store covers raw SSD, SCM, an
  in-memory store for tests, and RDMA-remote (a remote block device); it is
  parameterized by an **IU (indivisible-unit) alignment** as small as 1 byte
  (mem / SCM) or a flash page (SSD).
- Consistent point-in-time read snapshots (`scan`, `compare`, snapshot export)
  without stopping writes: an immutable COW B+tree root tagged with the slot it
  reflects (§3.1).
- Compose with the **external** durability/GC flow: crow-tree exposes
  `last_applied_slot` and accepts a GC watermark from the slot/WAL layer; it
  keeps **no operation log of its own**. There are two distinct GCs:
  1. **Data-retention GC — logical, slot-driven.** Dropping tombstones and
     superseded values once the slot/WAL layer says they are durable
     everywhere. crow-tree is *told* the floor via `set_gc_watermark`; the
     *policy* (which slot is safe) is owned by the learner/WAL layer.
  2. **Page reclamation — physical, internal.** Freeing B+tree pages and
     retired root versions no longer referenced by any reader.
     Crowtree-internal (epoch-based), automatic, **not** slot-driven.
- A structure the team fully implements and controls — no third-party storage
  library.

**Non-Goals**

- **Multi-version / MVCC time-travel reads.** Single version per key only —
  not worth the cost for CROW.
- **Tree-level range split/merge in v1.** This is *tree*-level (splitting a
  whole crow-tree into two), distinct from internal B+tree *page* split/merge
  (always done). One crow-tree per consensus group; partitioning happens at the
  consensus-group layer. **The design must not preclude it** — a future
  large-KV-cluster sharding feature will split/merge whole trees.
- **A second operation log.** crow-tree has no redo/replay log of its own.
  Durability of consensus output is the consensus WAL; crow-tree persists only
  its materialized state (a snapshot = immutable root + `last_applied_slot`).
  This lets crow-tree compose with the slot/WAL system (or any other log) for
  recovery.
- **Lock-free multi-writer B+tree.** The B+tree has a single writer (the flush
  thread). Write concurrency is provided *in front of* the tree by a
  concurrent MemTable; a single Flusher merges the contiguous-applied prefix
  into the tree ("flush = the persistent write").

The pagetree design (page layout, delta records, consolidation, epoch
GC, bloom filters, IU alignment) was an **algorithmic reference only**
during design. It is not linked and not a dependency. What crow-tree
reused vs. dropped vs. simplified:

| Mechanism | Decision |
| --- | --- |
| Leaf page layout + bloom + IU alignment + CRC32C | Reuse |
| Delta records (batch upsert/delete) + consolidation by length/bytes | Reuse (deltas carry `slot`) |
| Mapping table (PID indirection) | Reuse, **drop CAS retry** (single writer stores directly) |
| Epoch-based GC | Reuse (readers lightweight enter/exit) |
| Page-level split / merge | Simplify: **writer-exclusive**, drop multi-phase cooperative help-along |
| Immutable versioned root table | **New** (MVCC snapshot + export + recovery anchor) |
| Inline `(slot, cell)` per value | **New** (slot single-version semantics) |
| Tree-level split / merge | **Dropped in v1** (partitioning is in the consensus layer); design must not preclude future sharding |
| Internal WAL | **Dropped** — no redo log; recovery = snapshot + external-WAL replay from `last_applied_slot+1` |

---

## 2. Architecture

```
crow-kv (Rust)
  PxLearner ──drives──► dyn KVEngine
                          ├─ InMemKV            (Rust, tests)
                          └─ CrowTreeEngine     (Rust, FFI adapter; §4)
                                │  C ABI: ct_open / ct_apply / ct_get / ct_scan / ct_get_async / ...
                                ▼
  libcrow-tree (C++)
  └─ Crowtree (one per consensus group)
        ├─ EpochManager        page reclamation, tree-private
        ├─ MemTable (L0)       concurrent ordered buffer; absorbs concurrent/out-of-order apply
        ├─ Flusher (1 thread)  flushes the contiguous-applied prefix  L0 → B+tree
        ├─ MappingTable        PID → resident page | unloaded durable address
        ├─ BufferPool          tree-private frame cache
        ├─ Reactor (1 thread)  io_uring event loop for async I/O
        ├─ root_pid / leftmost_leaf_pid
        ├─ RootVersion         versioned root + refcount for consistent snapshots
        └─ PageStore (backend)  FilePageStore | BlockPageStore
                                 (raw SSD / SCM / mem-for-test / RDMA-remote; IU-aligned, IU ≥ 1B)
```

- **One crow-tree per consensus group.** A node hosting many groups owns many
  lightweight `Crowtree` instances. Each owns its epoch manager, buffer pool,
  and reactor; no shared process-wide state.
- **Two-level write path.** `apply` writes into the concurrent **MemTable
  (L0)**; a single **Flusher** thread merges the contiguous-applied prefix
  into the COW **B+tree (L1)**. The B+tree therefore has exactly one writer;
  ingestion can be concurrent and out-of-order. Full design in
  [`design-crow-tree-engine.md`](design-crow-tree-engine.md).
- **Concurrent readers.** Reads take an epoch guard, overlay the MemTable on
  the B+tree (read amplification 2), and walk immutable pages with lock-free
  atomic pointer loads.
- **Async I/O.** A single-thread reactor per `Crowtree` handles demand-load
  reads and flush/snapshot writes via io_uring
  ([`design-crow-tree-engine.md §3`](design-crow-tree-engine.md#3-async-ffi-bridge)).
  Fast path (in-memory hit) completes synchronously; slow path (I/O) submits
  an SQE and returns pending.

---

## 3. Engine Abstraction

The Rust `KVEngine` trait exposes durability, snapshots, GC, and consistent
views; both `InMemKV` and `CrowTreeEngine` implement it:

```rust
#[async_trait]
pub trait KVEngine: Send + Sync {
    async fn apply(&self, slot: u64, batch: &Batch) -> Result<(), EngineError>;
    async fn get(&self, key: &[u8]) -> Result<Option<(u64, Vec<u8>)>, EngineError>;
    async fn scan(&self, prefix: &[u8], limit: usize)
        -> Result<(Vec<(Vec<u8>, u64, Vec<u8>)>, bool), EngineError>;

    fn snapshot_view(&self) -> Arc<dyn EngineView>;   // pin a consistent point-in-time version

    fn last_applied_slot(&self) -> u64;
    async fn persist_snapshot(&self) -> Result<u64, EngineError>;
    fn set_gc_watermark(&self, snapshot_slot: u64, safe_slot: u64);
    async fn collect_garbage(&self) -> Result<GcStats, EngineError>;

    fn snapshot_export(&self) -> BoxStream<'_, Result<Chunk, EngineError>>;
    async fn snapshot_import(&self, chunks: BoxStream<'_, Chunk>) -> Result<(), EngineError>;
    async fn clear(&self) -> Result<(), EngineError>;
}

pub trait EngineView: Send + Sync {   // compare / iter_all / range read on a fixed version
    fn get(&self, key: &[u8]) -> Option<(u64, Vec<u8>)>;
    fn iter_all(&self) -> Box<dyn Iterator<Item = (Vec<u8>, u64, Cell)> + '_>;
    fn compare(&self, other: &dyn EngineView) -> Vec<EngineDiff>;
    fn at_slot(&self) -> u64;
}
```

- **Async + fallible.** Persistence is async and can fail; the in-memory
  engine implements the methods trivially (always `Ok`).
- **`snapshot_view()`** removes the "stop client traffic and wait for
  quiescence" requirement `compare`/`iter_all`/range reads previously needed —
  they run on a pinned version instead.
- **`last_applied_slot` / `persist_snapshot` / `set_gc_watermark` /
  `collect_garbage`** make the state-machine self-persistence and the logical
  retention-GC policy explicit interface methods (semantics in §3.1).

### 3.1 Out-of-order apply, snapshots, and the two GCs

**Out-of-order apply is required.** `learn()` applies each chosen entry to the
engine immediately, so `apply(slot, batch)` can arrive out of slot order, e.g.
slot 7 before slot 6 in the parallel-slot window. The final materialized state
is **order-independent and idempotent** thanks to per-key highest-slot-wins.
The **learner**, not the engine, tracks the contiguous applied frontier. So the
engine contract is: accept `apply` at any slot; never corrupt; converge to
highest-slot-wins. *How* it absorbs out-of-order writes internally is the
two-level write path: `apply` lands in the concurrent MemTable; a single
Flusher merges the *contiguous-applied prefix* into the B+tree, so the B+tree
only ever sees ordered, contiguous, single-writer batches.

**Contiguous frontier comes from the learner.** A NoOp / repair-fill slot
carries an empty batch and leaves no MemTable entry, so crow-tree must **not**
infer the contiguous prefix from MemTable contents. It would block forever at
the gap left by a NoOp. Instead the learner passes its `contiguous_applied`
watermark down (`ct_advance_contiguous`), and the Flusher flushes only
MemTable entries with `slot ≤ contiguous_slot`.

**Flush trigger is dual.** The Flusher runs when the MemTable crosses a byte /
entry limit (**primary**) **or** a long time interval elapses since the last
flush (**secondary safety net**, default ~2 h). The size trigger keeps
per-flush work roughly constant; the time trigger bounds how long a
slow-write workload can leave L0 un-flushed (unbounded L0 ⇒ unbounded
crash-recovery replay). Each flush produces a new immutable COW root tagged
with the flushed slot = a snapshot, so `flush` *is* snapshot creation; the
public surface is unified under snapshot terminology (`create_snapshot`).

**`last_applied_slot` ownership is split.** The *learner* owns the in-memory
applied frontier (`contiguous_applied`); the *engine* owns only the
**durable** one. `last_applied_slot` = the highest contiguous slot whose
MemTable entries have been flushed and whose root is persisted
(`last_applied_slot ≤ contiguous_applied`). A snapshot = flush + persist root;
it needs only this single watermark plus a per-root slot tag, **not** a
per-key slot index. Because the B+tree only ever holds a clean contiguous
prefix, a snapshot is an exact point-in-time state and new-member install is
trivial: the receiver imports the root at `S`, then replays the WAL from
`S+1`.

**The two GCs, concretely.**

1. *Data-retention GC.* The learner/replicator advances `safe_slot` (every
   member applied through here) and `snapshot_slot` (state durable on a
   quorum) and calls `set_gc_watermark(snapshot_slot, safe_slot)`. crow-tree
   computes `gc_slot = min(safe_slot, snapshot_slot)` and, on the next
   `collect_garbage()` / consolidation, physically drops tombstones and
   superseded cells whose slot `< gc_slot`. **Policy is owned by the WAL/slot
   layer; crow-tree only enforces the floor.** This is the same `gc_slot` the
   consensus-WAL GC uses ([`design-crow-tree-storage.md §7`](design-crow-tree-storage.md#7-interaction-with-consensus-wal-gc)).
2. *Page reclamation.* Freeing B+tree pages and retired root versions once no
   reader epoch references them — epoch-based, automatic, not slot-driven, no
   external policy.

---

## 4. FFI Boundary

The boundary is **coarse** (engine-level). The Rust `CrowTreeEngine`
(`lib/crow-kv/src/kv/crow_tree_engine.rs`) is a thin adapter that:

- Owns an opaque `*mut ct_tree` handle returned by `ct_open`.
- Translates `Batch` / keys / ranges to `(ptr, len)` pairs across the C ABI.
- Bridges async↔sync via the io_uring reactor + completion-based `ct_future`
  protocol (fast path completes synchronously with zero scheduling overhead;
  slow path parks on `AsyncFd` until the C++ reactor signals completion).
  Full protocol in [`design-crow-tree-engine.md §3`](design-crow-tree-engine.md#3-async-ffi-bridge).
- Maps C status codes to `EngineError`.

Ownership rules (enforced by convention, documented at the C API in
`lib/crow-tree/include/lib/crow-tree/c_api.h`, the single source of truth for exact
signatures):

- Buffers passed *into* C are borrowed for the call's duration only.
- Buffers returned *from* C are either copied immediately by Rust, or
  returned as an opaque owned handle freed by a matching `ct_free_*`.
- No C++ exceptions cross the boundary; everything is `noexcept` + status
  code.

---

## 5. Sub-Design Document Map

The crow-tree design is split into two self-contained documents:

| Doc | Covers |
| --- | --- |
| `design-crow-tree.md` (this) | Goals, architecture, `KVEngine` trait, FFI boundary, decision log. |
| [`design-crow-tree-engine.md`](design-crow-tree-engine.md) | **In-memory engine.** MemTable (L0) + COW B+tree (L1), slot-aware value cell, delta records + consolidation, split/merge, versioned root (MVCC snapshots), epoch-based reclamation, read path; the `buffer` memory-ownership model (zero-copy write/read pipelines); the io_uring async FFI bridge. |
| [`design-crow-tree-storage.md`](design-crow-tree-storage.md) | **Durable storage.** `PageStore` backends, on-disk zero-copy frame format, buffer pool (frame cache) + eviction safety, snapshot + internal-WAL decision + recovery, snapshot export/import; the mapping table (PID indirection, segment persistence, recycling); snapshot/GC flow integration with the learner and consensus WAL. |

Test strategy for crow-tree (C++ unit, integration, crash/recovery, Rust FFI,
cross-engine parity, sanitizer) is documented in [`../kv/design-crow-kv-test.md`](../kv/design-crow-kv-test.md) §
"crow-tree C++ Test Layers".

---

## 6. Decision Log

| # | Decision | Rationale |
| --- | --- | --- |
| D1 | **Build crow-tree in C++** as `libcrow-tree`, consume from Rust over a C API. | The lowest storage layer (block device / RDMA) is C++. Placing the FFI boundary at the *top* (engine level) keeps the page-I/O hot path entirely in C++ and crosses FFI only at coarse `apply`/`get`/`scan`/`snapshot` calls. |
| D2 | **B+tree mutation is single-writer (the flush thread); no bw-tree lock-free CAS.** | The B+tree has exactly one mutator — the flush thread — holding the writer lock; readers do lock-free atomic loads. A bw-tree's lock-free multi-writer CAS adds large complexity that conflicts with COW + persistence + snapshots. Ingestion concurrency is absorbed *in front of* the tree by the MemTable (D3). |
| D3 | **Bounded 2-level: a concurrent memtable (L0) over one COW B+tree (L1); no multi-run LSM, no global compaction.** | Concurrent / out-of-order `apply` lands in the memtable; a single flush thread merges the *contiguous-applied prefix* into the B+tree ("flush = the persistent write"). Read amplification is bounded at **2** (memtable + tree). A classic multi-run LSM (many runs + per-run bloom + global compaction) is rejected for hurting linearizable `get`/`scan`. |
| D4 | **Tree family = COW B+tree + per-leaf delta chain + local consolidation.** | A "mini bw-tree without the lock-free machinery": leaf-level deltas give LSM-like write batching with low write amplification, while a single B+tree locate keeps reads cheap and snapshots clean. |
| D5 | **Slot is inlined into the value cell.** | Single-version, highest-slot-wins semantics require each entry to carry `(slot, kind, value)`. Slot is **not** in the key (that would create multiple versions). A batch at slot `S` fans out into one cell per key, scattered across possibly many leaves/pages, all carrying the same `S` — this many-cells-share-one-slot case is the *common* case (§3.1). |
| D6 | **Page lifecycle via epoch-based reclamation, owned per-`Crowtree`.** | This is the *physical* page-reclamation GC, internal and automatic: readers do a nanosecond-scale epoch enter/exit; the writer retires replaced pages; a GC thread reclaims after readers drain. The epoch manager is per-tree because the buffer pool (and therefore all retired pages) is already tree-private — there is no cross-tree page sharing, so a per-tree epoch is simpler and makes zero-copy borrowed reads fully tree-scoped. There is no shared cross-tree environment object. |
| D7 | **Engine abstraction is async and adds snapshot/retention-GC/`last_applied_slot`/consistent views.** | Persistence can fail and is async (io_uring / RDMA); `compare`/`iter_all` move onto a pinned consistent view instead of a quiescent global stop. `set_gc_watermark`/`collect_garbage` drive the **logical, slot-driven data-retention GC** (tombstone drop), *not* internal page reclamation (§3.1). |
| D8 | **Unified `buffer` memory model; single-allocation zero-copy pipeline.** | Key/value bytes are allocated once at the API boundary and moved down to the MemTable and into the frame; reads return borrowed views into resident frames (L1) or copies (L0). Replaces per-write `std::string`. Full design in [`design-crow-tree-engine.md §2`](design-crow-tree-engine.md#2-memory-and-buffer-management). |
| D9 | **MemTable ordered map = `absl::btree_map`.** | Chosen over `std::map` (poor cache locality) and skip list (cache-miss-heavy at MemTable scale). B-tree fanout gives 2–3× faster point lookups; ordered iteration is preserved for drain/snapshot. |
| D10 | **Snapshot/flush unified terminology + dual flush trigger.** | `flush` (drain L0→L1 + publish a new COW root) *is* snapshot creation. Trigger = MemTable size (primary) **OR** a long time interval (secondary safety net, default ~2 h) so a slow-write workload cannot leave L0 un-flushed and make crash recovery replay unbounded (§3.1). |
| D11 | **C++ logging via `spdlog`, hot-path-silent.** | Async ring-buffer file logger matching the Rust `tracing` file format. Hot paths (`apply`/`get`/`scan`) emit no info/warn logs; only structural events (flush/snapshot/recover) and errors log at info+. Off by default (`Options.log_dir`). |
| D12 | **Async FFI via io_uring + completion-based futures; no `spawn_blocking`, no large thread pools.** | Fast path (in-memory hit) completes synchronously with zero scheduling overhead; slow path (I/O) submits an io_uring SQE and returns pending. A single-thread C++ reactor per `Crowtree` processes completions and notifies the Rust `Future` via `eventfd` + Tokio `AsyncFd`. Full design in [`design-crow-tree-engine.md §3`](design-crow-tree-engine.md#3-async-ffi-bridge). |
| D13 | **Flush trigger = MemTable byte/entry limit OR time limit; flush the contiguous prefix only.** | The contiguous frontier is supplied by the learner (NoOp / repair-fill slots leave no MemTable entry, so the frontier must not be inferred from MemTable contents — otherwise the Flusher blocks at a NoOp gap). Flush entries with `slot ≤ contiguous_slot` (§3.1). |
| D14 | **One block-device backend covers raw SSD / SCM / mem-for-test / RDMA-remote; RDMA is not a separate backend.** | All are IU-aligned with a configurable IU that can be **1 byte** (mem / SCM) up to a flash page (SSD). RDMA-remote is just a remote block device; its cache/eviction details are deferred with the rest of the block backend. Full design in [`design-crow-tree-storage.md §2`](design-crow-tree-storage.md#2-backends). |
| D15 | **No multi-version; slot in the value cell, not the key.** | Single version per key, highest-slot-wins (§1, D5). |
| D16 | **Tree-level split/merge: not in v1, but not precluded.** | Designed so a future large-cluster *sharding* feature can split/merge whole trees (§1). |
| D17 | **No internal redo-WAL; snapshot-only recovery.** | crow-tree persists a snapshot = immutable root + `last_applied_slot`. On restart it composes with the external WAL: replay starts from `last_applied_slot+1`. Full rationale in [`design-crow-tree-storage.md §5`](design-crow-tree-storage.md#5-internal-wal-decision). |
| D18 | **Snapshot is implicit (a COW root version).** | Every flush/snapshot yields a new immutable root tagged with its slot = a snapshot, no explicit "create snapshot" API; callers obtain `(version, root, slot)` via `snapshot_view()`. `snapshot_export` iterates a pinned root (§3.1). |
| D19 | **Compression implemented (LZ4), off by default.** | LZ4 on-disk page compression is opt-in (`Options.compression = kLz4`); see [`design-crow-tree-storage.md §3.6`](design-crow-tree-storage.md#36-compression-details). |
