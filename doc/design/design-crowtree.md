# CrowKV - Design: crowtree Storage Engine (Overview)

Depends on: [`requirement.md`](../requirement.md), [`design.md`](../design.md), [`design-state-machine.md`](design-state-machine.md)
Satisfies: [requirement.md §8.3 learner storage](../requirement.md#83-learner-storage), [plan.md P3 — Storage Engine](../plan.md#p3--storage-engine)

This is the parent document for **crowtree**, the production storage engine that
backs CrowKV learners. It records the decisions taken during the P3 redesign
(language, FFI boundary, tree family), the redefined engine abstraction, and the
map of the crowtree sub-design documents.

This document set replaces the placeholder "crowtree, already developed" wording
in [`design-state-machine.md §9.3`](design-state-machine.md#93-crowtree). The
abstraction that crowtree implements still lives, conceptually, with the state
machine; the *implementation* is specified here.

## Table of Contents

- [1. Goals and Scope](#1-goals-and-scope)
- [2. Decisions (Round 1–3)](#2-decisions-round-13)
- [3. Architecture](#3-architecture)
- [4. Redefined Engine Abstraction](#4-redefined-engine-abstraction)
- [5. FFI Boundary](#5-ffi-boundary)
- [6. Sub-Design Document Map](#6-sub-design-document-map)
- [7. Open Questions](#7-open-questions)

---

## 1. Goals and Scope

crowtree is an **embeddable, ordered key-value storage engine** implementing the
CrowKV `KVEngine` contract. It is built as a standalone C++ library
(`libcrowtree`) so it can be reused outside CrowKV, and consumed from the Rust
`crowkv` crate over a C ABI.

**Goals**

- One ordered, **single version per key** KV engine that serves all learner
  reads. Only the latest write per key is kept (no multi-version / time-travel).
- **Slot-aware storage:** every live key carries the consensus slot (= WAL
  position) of the write that produced it. The slot is stored **in the value
  cell, not in the key**. Putting it in the key would make each key sort into
  multiple ordered versions — i.e. multi-version storage — which we explicitly do
  not want (single version, highest-slot-wins). See Non-Goals.
- Pluggable persistence behind one page-granular backend: a **file** store and a
  **block-device** store. The block-device store covers raw SSD, SCM, an
  in-memory store for tests, and RDMA-remote (which is just a remote block
  device); it is parameterized by an **IU (indivisible-unit) alignment** that can
  be as small as 1 byte (mem / SCM) or a flash page (SSD).
- Consistent point-in-time read snapshots (for `scan`, `compare`, snapshot
  export) without stopping writes. A snapshot is simply an immutable COW B+tree
  root, tagged with the slot it reflects (§4.1).
- Compose with the **external** durability/GC flow: crowtree exposes
  `last_applied_slot` and accepts a GC watermark from the slot/WAL layer; it keeps
  **no operation log of its own**. (Two distinct GCs — see box below.)
- A structure the team can fully implement and control — no third-party storage
  library.

> **Two distinct GCs (clarification, see §4.1).**
> 1. **Data-retention GC — logical, slot-driven.** Dropping tombstones and
>    superseded values once the slot/WAL layer says they are durable everywhere.
>    crowtree is *told* the floor via `set_gc_watermark`; the *policy* (which slot
>    is safe) is owned by the learner/WAL layer. This is the "watermark-driven
>    GC" mentioned above.
> 2. **Page reclamation — physical, internal.** Freeing B+tree pages and retired
>    root versions no longer referenced by any reader. Crowtree-internal
>    (epoch-based, D6), automatic, **not** slot-driven; nobody manages it by hand.

**Non-Goals**

- **Multi-version / MVCC time-travel reads.** Single version per key only.
  (Decided in review — not worth the cost for CrowKV.)
- **Tree-level range split/merge in v1.** This is *tree*-level (splitting a whole
  crowtree into two), distinct from internal B+tree *page* split/merge (which we
  always do). One crowtree per consensus group; partitioning is done at the
  consensus-group layer, so v1 does not implement tree-level split/merge.
  **However the design must not preclude it** — a future large-KV-cluster
  *sharding* feature will split/merge whole trees.
- **A second operation log.** crowtree has no redo/replay log of its own.
  Durability of consensus output is the consensus WAL; crowtree persists only its
  materialized state (a checkpoint = immutable root + `last_applied_slot`). This
  lets crowtree compose with the slot/WAL system (or any other log) for recovery.
- **Lock-free multi-writer B+tree.** The B+tree has a single writer (the flush
  thread). Write concurrency is provided *in front* of the tree by a concurrent
  MemTable; a single Flusher merges the contiguous-applied prefix into the tree
  ("flush = the persistent write"). The Flusher must be efficient or it becomes
  the write-IOPS bottleneck (D2/D3, §4.1).

---

## 2. Decisions (Round 1–3)

| # | Decision | Rationale |
| --- | --- | --- |
| D1 | **Build crowtree in C++** as `libcrowtree`, consume from Rust over a C API. | The lowest storage layer (block device / RDMA, in `aioss` `chunkio`/`diskio`/`rdmaio`) is C++. Placing the FFI boundary at the *top* (engine level) keeps the page-I/O hot path entirely in C++ and crosses FFI only at coarse `apply`/`get`/`scan`/`snapshot` calls. |
| D2 | **B+tree mutation is single-writer (the flush thread); no bw-tree lock-free CAS.** | The B+tree has exactly one mutator — the flush thread — holding the writer lock; readers do lock-free atomic loads. A bw-tree's lock-free multi-writer CAS adds large complexity that conflicts with COW + persistence + snapshots. Ingestion concurrency is absorbed *in front* of the tree by the memtable (D3). |
| D3 | **Bounded 2-level: a concurrent memtable (L0) over one COW B+tree (L1); no multi-run LSM, no global compaction.** | Decided (was §7 Q1 → option B). Concurrent / out-of-order `apply` lands in the memtable; a single flush thread merges the *contiguous-applied prefix* into the B+tree ("flush = the persistent write"). Read amplification is bounded at **2** (memtable + tree). A classic multi-run LSM (many runs + per-run bloom + global compaction) is rejected for hurting linearizable `get`/`scan`. |
| D4 | **Tree family = COW B+tree + per-leaf delta chain + local consolidation.** | A "mini bw-tree without the lock-free machinery": leaf-level deltas give LSM-like write batching with low write amplification, while a single B+tree locate keeps reads cheap and snapshots clean. |
| D5 | **Slot is inlined into the value cell.** | Single-version, highest-slot-wins semantics (`design-state-machine.md §3, §4`) require each entry to carry `(slot, kind, value)`. Slot is **not** in the key (that would create multiple versions). A batch at slot `S` **fans out into one cell per key**, scattered across possibly many leaves / pages, all carrying the same `S` — this many-cells-share-one-slot case is the *common* case (see §4.1, core doc §2 / §6). |
| D6 | **Page lifecycle via epoch-based reclamation** (reuse the pagetree `EpochManager` idea). | This is the *physical page-reclamation* GC, internal and automatic: readers do a nanosecond-scale epoch enter/exit; the writer retires replaced pages; a GC thread reclaims after readers drain. |
| D7 | **Engine abstraction becomes async + adds snapshot/retention-GC/`last_applied_slot`/consistent views.** | Persistence can fail and is async (io_uring / RDMA); `compare`/`iter_all` move onto a pinned consistent view instead of a quiescent global stop. The `set_gc_watermark`/`collect_garbage` here drive the **logical, slot-driven data-retention GC** (tombstone drop), *not* internal page reclamation. See §4.1. |

The pagetree implementation at `/cjdata/cpp/aioss/libs/pagetree` is an
**algorithmic reference only** (page layout, delta records, consolidation, epoch
GC, bloom filters, IU alignment). It is not linked and not a dependency.

### What is reused vs dropped vs simplified from the pagetree reference

| Mechanism | Decision |
| --- | --- |
| Leaf page layout + bloom + IU alignment + CRC32C | Reuse |
| Delta records (Batch / Put / Delete) + consolidation by length/bytes | Reuse (deltas carry `slot`) |
| Mapping table (PID indirection) | Reuse, **drop CAS retry** (single writer stores directly) |
| Epoch-based GC | Reuse (readers lightweight enter/exit) |
| Page-level split / merge | Simplify: **writer-exclusive**, drop multi-phase cooperative help-along |
| Immutable versioned root table | **New** (MVCC snapshot + export + recovery anchor) |
| Inline `(slot, cell)` per value | **New** (slot single-version semantics) |
| Tree-level split / merge | **Drop in v1** (partitioning is in the consensus layer); design must not preclude future sharding use |
| Internal WAL | **Drop** — no redo log; recovery = checkpoint (immutable root + `last_applied_slot`) + external-WAL replay from `last_applied_slot+1` |

---

## 3. Architecture

```
crowkv (Rust)
  PxLearner ──drives──► dyn KVEngine
                          ├─ InMemKV            (Rust, tests)
                          └─ CrowtreeEngine     (Rust, thin FFI adapter; spawn_blocking bridge)
                                │  C API: ct_open / ct_apply / ct_get / ct_scan
                                │         ct_snapshot_view / ct_snapshot_export / ct_snapshot_import
                                │         ct_checkpoint / ct_set_gc_watermark / ct_collect_garbage
                                ▼
  libcrowtree (C++)
  ├─ CrowtreeEnv (process-wide)  : EpochManager · GC pool · Consolidation pool
  └─ Crowtree (one per consensus group)
        ├─ MemTable (L0)       concurrent ordered buffer; absorbs concurrent/out-of-order apply
        ├─ Flusher (1 thread)  flushes the contiguous-applied prefix  L0 → B+tree
        ├─ MappingTable        PID → atomic<PageBase*>
        ├─ root_pid / leftmost_leaf_pid
        ├─ VersionTable        version → (root snapshot, last_applied_slot)
        ├─ DirtyTracker
        └─ PageStore (backend)  FilePageStore | BlockPageStore
                                 (raw SSD / SCM / mem-for-test / RDMA-remote; IU-aligned, IU ≥ 1B)
```

- **One crowtree per consensus group.** A node hosting many groups owns many
  lightweight `Crowtree` instances sharing one `CrowtreeEnv`.
- **Two-level write path (decided).** `apply` writes into the concurrent
  **MemTable (L0)**; a single **Flusher** thread merges the contiguous-applied
  prefix into the COW **B+tree (L1)**. The B+tree therefore has exactly one writer
  (the flusher); ingestion can be concurrent and out-of-order.
- **Concurrent readers.** Reads take an epoch guard, overlay the MemTable on the
  B+tree (read amplification 2), and walk immutable pages with lock-free atomic
  pointer loads.

---

## 4. Redefined Engine Abstraction

The Rust `KVEngine` contract is upgraded from the current synchronous trait
(`crowkv/src/kv/kv_engine.rs`) to an async surface that exposes durability,
snapshots, GC, and consistent views. `InMemKV` and `CrowtreeEngine` both
implement it. This is the integration contract crowtree satisfies; the current
synchronous trait remains documented in `design-state-machine.md` until the P3
M1 migration lands.

```rust
#[async_trait]
pub trait KVEngine: Send + Sync {
    // Write — single-writer, serialized by slot; highest-slot-wins idempotent.
    async fn apply(&self, slot: u64, batch: &Batch) -> Result<(), EngineError>;

    // Reads — concurrent with apply.
    async fn get(&self, key: &[u8]) -> Result<Option<(u64, Vec<u8>)>, EngineError>;
    async fn scan(&self, prefix: &[u8], limit: usize)
        -> Result<(Vec<(Vec<u8>, u64, Vec<u8>)>, bool), EngineError>;

    // Pin a consistent point-in-time version (scan-at / compare / export).
    fn snapshot_view(&self) -> Arc<dyn EngineView>;

    // Durability + GC watermarks (replaces the former DurableCommitWatermark).
    fn last_applied_slot(&self) -> u64;
    async fn persist_checkpoint(&self) -> Result<u64, EngineError>;
    fn set_gc_watermark(&self, snapshot_slot: u64, safe_slot: u64);
    async fn collect_garbage(&self) -> Result<GcStats, EngineError>;
    // NOTE: apply() may be called out of slot order (parallel window) — see §4.1.
    // set_gc_watermark / collect_garbage drive the logical, slot-driven retention
    // GC (tombstone drop), NOT internal page reclamation. Flow in §4.1.

    // Snapshot transfer (new-member install). A snapshot is implicitly an
    // immutable COW root tagged with a slot; export pins a root version and
    // streams it. Out-of-order handling: §4.1 and §7 Q1.
    fn snapshot_export(&self, at_slot: u64) -> BoxStream<'_, Result<Chunk, EngineError>>;
    async fn snapshot_import(&self, chunks: BoxStream<'_, Chunk>) -> Result<(), EngineError>;

    async fn clear(&self) -> Result<(), EngineError>;
}

// Immutable consistent view: compare / iter_all / range read on a fixed version.
pub trait EngineView: Send + Sync {
    fn get(&self, key: &[u8]) -> Option<(u64, Vec<u8>)>;
    fn iter_all(&self) -> Box<dyn Iterator<Item = (Vec<u8>, u64, Cell)> + '_>;
    fn compare(&self, other: &dyn EngineView) -> Vec<EngineDiff>;
    fn at_slot(&self) -> u64;
}
```

Changes from the current trait:

- **Async + `EngineError`.** Persistence is fallible and async; the in-memory
  engine implements the methods trivially (always `Ok`).
- **`snapshot_view()` consistent view.** `compare` / `iter_all` / range reads run
  on a pinned version, removing the "stop client traffic and wait for quiescence"
  requirement from `design-state-machine.md §8.4`.
- **`last_applied_slot` / `persist_checkpoint` / `set_gc_watermark` /
  `collect_garbage`.** Make the state-machine self-persistence and the logical
  retention-GC policy explicit interface methods. Semantics and flows in §4.1.
- **`snapshot_export/import`** promoted into the trait (P3 M4).

### 4.1 Out-of-order apply, checkpoints, and the two GCs

**Out-of-order apply is required.** `learn()` applies each chosen entry to the
engine immediately, so `apply(slot, batch)` can arrive out of slot order — e.g.
slot 7 before slot 6 in the parallel-slot window
(`crowkv/src/paxos/learner.rs::learn` → `apply_entry`). The final materialized
state is **order-independent and idempotent** thanks to per-key highest-slot-wins
(`design-state-machine.md §4`). The **learner**, not the engine, tracks the
contiguous applied frontier (`contiguous_applied`). So the engine contract is:
accept `apply` at any slot; never corrupt; converge to highest-slot-wins. *How*
it absorbs out-of-order writes internally is the **two-level write path (decided,
§7)**: `apply` lands in the concurrent MemTable; a single Flusher merges the
*contiguous-applied prefix* into the B+tree, so the B+tree only ever sees ordered,
contiguous, single-writer batches.

**Contiguous frontier comes from the learner.** A NoOp / repair-fill slot carries
an empty batch and leaves no MemTable entry, so crowtree must **not** infer the
contiguous prefix from MemTable contents — it would block forever at the gap left
by a NoOp. Instead the learner passes its `contiguous_applied` watermark down
(`ct_apply`'s `contiguous_slot` argument, or a separate `ct_advance_contiguous`),
and the Flusher flushes only MemTable entries with `slot ≤ contiguous_slot`.

**Flush trigger.** The Flusher runs when the MemTable crosses a byte / entry limit
**or** a time limit, then flushes the current contiguous prefix. Each flush
produces a new immutable COW root tagged with the flushed slot = a snapshot (R5).

**`last_applied_slot` / checkpoints.** Ownership is split (Q4 decided): the
*learner* owns the in-memory applied frontier (`contiguous_applied`); the *engine*
owns only the **durable** one — `last_applied_slot` = the highest contiguous slot
whose MemTable entries have been flushed and whose root is persisted
(`last_applied_slot ≤ contiguous_applied`). A checkpoint = flush + persist root; it
needs only this single watermark plus a per-root slot tag — **not** a per-key slot
index. Because the B+tree only ever holds a clean contiguous prefix, a snapshot is
an exact point-in-time state and new-member install is trivial: the receiver
imports the root at `S`, then replays the WAL from `S+1`.

**The two GCs (concrete flow).**

1. *Data-retention GC — logical, slot-driven.* The learner/replicator advances
   `safe_slot` (every member applied through here) and `snapshot_slot` (state
   durable on a quorum) and calls `set_gc_watermark(snapshot_slot, safe_slot)`.
   crowtree computes `gc_slot = min(safe_slot, snapshot_slot)` and, on the next
   `collect_garbage()` / consolidation, physically drops tombstones and
   superseded cells whose slot `< gc_slot`. **Policy is owned by the WAL/slot
   layer; crowtree only enforces the floor.** This is the same `gc_slot` the
   consensus-WAL GC uses (`design-crowtree-snapshot-gc.md §5`).
2. *Page reclamation — physical, internal.* Freeing B+tree pages and retired root
   versions once no reader epoch references them — epoch-based (D6), automatic,
   not slot-driven, no external policy.

---

## 5. FFI Boundary

The boundary is **coarse** (engine-level). The Rust `CrowtreeEngine` is a thin
adapter that:

- Owns an opaque `*mut Crowtree` handle returned by `ct_open`.
- Translates `Batch` / keys / ranges to `(ptr, len)` pairs across the C ABI.
- Bridges async↔sync via `tokio::task::spawn_blocking` — `apply` enqueues into the
  MemTable (cheap, may be concurrent) and reads run on the blocking pool; the
  Flusher is a crowtree-owned C++ thread, not a Rust task.
- Maps C status codes to `EngineError`.

Ownership rules (enforced by convention, documented at the C API):

- Buffers passed *into* C are borrowed for the call's duration only.
- Buffers returned *from* C are either copied immediately by Rust, or returned as
  an opaque owned handle freed by a matching `ct_free_*`.
- No C++ exceptions cross the boundary; everything is `noexcept` + status code.

The detailed C API signatures are specified in
[`design-crowtree-persistence.md`](design-crowtree-persistence.md) and the
per-call data shapes in [`design-crowtree-core.md`](design-crowtree-core.md).

---

## 6. Sub-Design Document Map

| Doc | Covers |
| --- | --- |
| `design-crowtree.md` (this) | Decisions, architecture, engine abstraction, FFI boundary, doc map. |
| [`design-crowtree-core.md`](design-crowtree-core.md) | MemTable (L0), pages, mapping table, delta records, slot cell encoding, two-level write path (apply→MemTable, flush→delta→consolidate→split/merge), versioned root, epoch GC, read path (L0 overlay). |
| [`design-crowtree-persistence.md`](design-crowtree-persistence.md) | `PageStore` backend abstraction (file + block-device; RDMA = remote block), on-disk page format & IU alignment, checkpoint, recovery, internal-WAL decision, C API. |
| [`design-crowtree-snapshot-gc.md`](design-crowtree-snapshot-gc.md) | Snapshot export/import format, `last_applied_slot`, GC watermarks, integration with learner / consensus WAL / new-member install. |
| [`design-crowtree-test.md`](design-crowtree-test.md) | Test cases, scope, layers (C++ unit, C++ integration, Rust FFI, cross-engine parity, crash/recovery). |

---

## 7. Decision Log

All round-1–3 questions are decided and folded into the body above; kept here for
the record.

### Round 1–2 (data model & scope)

- **R1 — No multi-version; slot in the value cell, not the key.** Single version
  per key, highest-slot-wins. (§1 Goals/Non-Goals, D5.)
- **R2 — Tree-level split/merge: not in v1, but not precluded.** Designed so a
  future large-cluster *sharding* feature can split/merge whole trees. (§1.)
- **R3 — No internal redo-WAL; checkpoint-only recovery.** crowtree persists a
  checkpoint = immutable root + `last_applied_slot`. On restart it composes with
  the external WAL: replay starts from `last_applied_slot+1`. This is exactly the
  "snapshot + slot, replay from there" model. (§1, D-Internal-WAL,
  `design-crowtree-persistence.md §6–7`.)
- **R4 — Two distinct GCs, clearly separated.** Logical slot-driven retention GC
  (tombstone drop, policy owned by the WAL/slot layer, enforced via
  `set_gc_watermark`) vs internal epoch-based page reclamation (automatic). (§1
  box, §4.1, D6/D7.)
- **R5 — Snapshot is implicit (a COW root version).** Every flush/checkpoint
  yields a new immutable root tagged with its slot = a snapshot, no explicit
  "create snapshot" API; callers just obtain `(version, root, slot)` via
  `snapshot_view()`. `snapshot_export` iterates a pinned root. (§4.1.)
- **R6 — Compression deferred.** LZ4/zstd on leaf base pages is easy to add later
  as a backend option; not in the initial scope.

### Round 3 (write path & backends)

- **D-Q1 — Write concurrency = bounded 2-level (memtable + single flusher).**
  `apply` lands in a concurrent **MemTable (L0)**; a single **Flusher** thread
  merges the *contiguous-applied prefix* into the COW B+tree (L1). The B+tree is
  single-writer; ingestion is concurrent / out-of-order; read amplification = 2.
  (D2, D3, §3, §4.1.) Lock-free multi-writer B+tree rejected.
- **D-Q2 — Flush trigger = MemTable byte/entry limit OR time limit; flush the
  contiguous prefix only.** The contiguous frontier is supplied by the learner
  (NoOp / repair-fill slots leave no MemTable entry, so the frontier must not be
  inferred from MemTable contents — otherwise the Flusher blocks at a NoOp gap).
  Flush entries with `slot ≤ contiguous_slot`. (§4.1.)
- **D-Q3 — One block-device backend covers raw SSD / SCM / mem-for-test /
  RDMA-remote; RDMA is not a separate backend.** All are IU-aligned with a
  configurable IU that can be **1 byte** (mem / SCM) up to a flash page (SSD).
  RDMA-remote is just a remote block device; its cache/eviction details are
  deferred with the rest of the block backend. (§1, §3,
  `design-crowtree-persistence.md`.)
- **D-Q4 — `last_applied_slot` ownership split confirmed.** Learner owns the
  in-memory applied frontier (`contiguous_applied`); engine owns only the durable
  one (`last_applied_slot ≤ contiguous_applied`). (§4.1.)
