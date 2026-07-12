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

- One ordered, single-version (per key) KV engine that serves all learner reads.
- Slot-aware storage: every live key carries its resolved consensus slot.
- Pluggable persistence: local file, raw block device, and remote (RDMA) page
  stores behind one page-granular backend abstraction.
- Consistent point-in-time read snapshots (for `scan`, `compare`, snapshot
  export) without stopping writes.
- Streamable snapshot export/import and watermark-driven GC that plug into the
  existing learner / consensus-WAL / new-member-install flows.
- A structure the team can fully implement and control — no third-party storage
  library.

**Non-Goals**

- MVCC time-travel reads (single version per key; see `design-state-machine.md §3.2`).
- Tree-level range split/merge. Partitioning is done at the consensus **group**
  layer (one crowtree per group); the pagetree reference's tree-split/merge is
  **dropped**.
- A second operation log. Durability of consensus output is the consensus WAL;
  crowtree persists only its own materialized state (snapshot + `last_applied_slot`).
- Lock-free multi-writer concurrency. Writes are serialized by the learner (one
  writer per group); see §2.

---

## 2. Decisions (Round 1–3)

| # | Decision | Rationale |
| --- | --- | --- |
| D1 | **Build crowtree in C++** as `libcrowtree`, consume from Rust over a C API. | The lowest storage layer (block device / RDMA, in `aioss` `chunkio`/`diskio`/`rdmaio`) is C++. Placing the FFI boundary at the *top* (engine level) keeps the page-I/O hot path entirely in C++ and crosses FFI only at coarse `apply`/`get`/`scan`/`snapshot` calls. |
| D2 | **Do not use a bw-tree's lock-free machinery.** | Writes are serialized by consensus slot order (single writer per group); reads are concurrent. The bw-tree's reason for existing (lock-free multi-writer CAS) is wasted here. crowtree keeps a single-writer simplification: writer does plain pointer stores, readers do lock-free atomic loads. |
| D3 | **Do not use a 2-level LSM + global merge.** | LSM read amplification (memtable + multiple runs + per-run bloom) hits CrowKV's latency-sensitive linearizable `get`/`scan`, and slot reconciliation across runs is awkward. |
| D4 | **Tree family = COW B+tree + per-leaf delta chain + local consolidation.** | A "mini bw-tree without the lock-free machinery": leaf-level deltas give LSM-like write batching with low write amplification, while a single B+tree locate keeps reads cheap and snapshots clean. |
| D5 | **Slot is inlined into the value cell.** | Single-version, highest-slot-wins semantics (`design-state-machine.md §3, §4`) require each entry to carry `(slot, kind, value)`. |
| D6 | **Page lifecycle via epoch-based reclamation** (reuse the pagetree `EpochManager` idea). | Answers page deletion / references / concurrent-read performance with one mechanism: readers do a nanosecond-scale epoch enter/exit; the writer retires replaced pages; a GC thread reclaims after readers drain. |
| D7 | **Engine abstraction becomes async + adds snapshot/GC/`last_applied_slot`/consistent views.** | Persistence can fail and is async (io_uring / RDMA); snapshot/GC are first-class; `compare`/`iter_all` move onto a pinned consistent view instead of a quiescent global stop. See §4. |

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
| Tree-level split / merge | **Drop** (partitioning is in the consensus layer) |
| Internal WAL | See `design-crowtree-persistence.md` (checkpoint-anchored; minimal redo) |

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
        ├─ MappingTable        PID → atomic<PageBase*>
        ├─ root_pid / leftmost_leaf_pid
        ├─ VersionTable        version → (root snapshot, last_applied_slot)
        ├─ DirtyTracker
        └─ PageStore (backend)  FilePageStore | BlockDevicePageStore | RdmaPageStore
```

- **One crowtree per consensus group.** A node hosting many groups owns many
  lightweight `Crowtree` instances sharing one `CrowtreeEnv`.
- **Single writer per tree.** The learner applies chosen entries in slot order;
  `apply` is the only mutator and holds the tree's writer lock.
- **Concurrent readers.** Reads take an epoch guard and walk immutable pages with
  lock-free atomic pointer loads.

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

    // Snapshot transfer (new-member install).
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
  `collect_garbage`.** Make the state-machine self-persistence (§2.1) and the GC
  policy (§7) explicit interface methods.
- **`snapshot_export/import`** promoted into the trait (P3 M3).

---

## 5. FFI Boundary

The boundary is **coarse** (engine-level). The Rust `CrowtreeEngine` is a thin
adapter that:

- Owns an opaque `*mut Crowtree` handle returned by `ct_open`.
- Translates `Batch` / keys / ranges to `(ptr, len)` pairs across the C ABI.
- Bridges async↔sync via `tokio::task::spawn_blocking` — acceptable because
  `apply` is already serialized, and reads run on the blocking pool.
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
| [`design-crowtree-core.md`](design-crowtree-core.md) | Pages, mapping table, delta records, slot cell encoding, write path (apply→delta→consolidate→split/merge), versioned root, epoch GC, read path. |
| [`design-crowtree-persistence.md`](design-crowtree-persistence.md) | `PageStore` backend abstraction (file/block/RDMA), on-disk page format & alignment, checkpoint, recovery, internal-WAL decision, C API. |
| [`design-crowtree-snapshot-gc.md`](design-crowtree-snapshot-gc.md) | Snapshot export/import format, `last_applied_slot`, GC watermarks, integration with learner / consensus WAL / new-member install. |
| [`design-crowtree-test.md`](design-crowtree-test.md) | Test cases, scope, layers (C++ unit, C++ integration, Rust FFI, cross-engine parity, crash/recovery). |

---

## 7. Open Questions

Resolved in later rounds (detailed plans written before implementation).

- **TODO-CONFIRM:** Internal redo-WAL vs checkpoint-only recovery (see persistence doc §recovery).
- **TODO-CONFIRM:** RDMA page-cache eviction policy and pin accounting.
- **TODO-CONFIRM:** Compression (LZ4/zstd) on leaf base pages — keep as a backend option or defer.
- **TODO-CONFIRM:** Whether `snapshot_export` uses native page dump or the portable `(key, slot, cell)` format first (parity tests need the portable path).
