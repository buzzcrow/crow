# CrowKV - Design: crowtree Test Strategy

Parent: [`design-crowtree.md`](design-crowtree.md)
Depends on: [`test.md`](../test.md), [`design-crowtree-core.md`](design-crowtree-core.md), [`design-crowtree-persistence.md`](design-crowtree-persistence.md), [`design-crowtree-snapshot-gc.md`](design-crowtree-snapshot-gc.md)

This document defines the test scope and cases for crowtree across its layers:
C++ unit, C++ integration (with sanitizers), Rust FFI adapter, cross-engine
parity, and crash/recovery. It mirrors the layer-scope philosophy of `test.md`.

## Table of Contents

- [1. Test Layers and Scope](#1-test-layers-and-scope)
- [2. C++ Unit Tests](#2-c-unit-tests)
- [3. C++ Integration Tests](#3-c-integration-tests)
- [4. Crash / Recovery Tests](#4-crash--recovery-tests)
- [5. Rust FFI Adapter Tests](#5-rust-ffi-adapter-tests)
- [6. Cross-Engine Parity Tests](#6-cross-engine-parity-tests)
- [7. Concurrency / Sanitizer Tests](#7-concurrency--sanitizer-tests)
- [8. Benchmarks](#8-benchmarks)
- [9. Test Tooling](#9-test-tooling)

---

## 1. Test Layers and Scope

| Layer | Where | What it proves | Out of scope |
| --- | --- | --- | --- |
| C++ unit | `libcrowtree/tests/unit` | Single component correctness (cell encoding, page build, delta replay, mapping table, epoch). | I/O, FFI, consensus. |
| C++ integration | `libcrowtree/tests/integration` | Multi-component flows (apply→consolidate→split/merge, scan, snapshot, GC) over a `PageStore`. | Consensus, network. |
| Crash/recovery | `libcrowtree/tests/recovery` | Durability: checkpoint + recover, torn-page, superblock A/B. | Multi-node. |
| Rust FFI | `crowkv` `tests/` | `CrowtreeEngine` implements `KVEngine` correctly across the C ABI; async bridge. | Tree internals (covered in C++). |
| Cross-engine parity | `crowkv` `tests/` | `InMemKV` and `CrowtreeEngine` produce identical state via `compare()`. | — |
| Concurrency | sanitizer CI | No data races / UAF under reader+writer load (TSan/ASan). | — |

The authoritative correctness oracle is **`compare()` against `InMemKV`**: for any
op sequence, the two engines' `EngineView::iter_all` must be byte-for-byte equal
(same key set, same `(slot, cell)`).

---

## 2. C++ Unit Tests

- **Cell encoding** — round-trip `[slot][flags][value]`; tombstone flag; empty
  value; highest-slot-wins comparator.
- **Leaf page build/read** — build from sorted entries; binary search hit/miss;
  bloom true-negative and false-positive accounting; CRC validate; IU padding +
  `logical_len` trailer.
- **Delta replay** — chain of `BatchDelta`s over a base; newest/highest-slot wins;
  tombstone shadows value; `FindKey` binary search within a delta.
- **Consolidation triggers** — fires at `max_delta_len` and at `max_delta_bytes`;
  preserves tombstones; drops tombstones only with GC hint below watermark.
- **Mapping table** — allocate/free/recycle PID; segment growth; atomic
  load/store; `kInvalidPID` boundaries.
- **Epoch manager** — retire not freed while a guard is open; freed after all
  guards in the epoch drop; advance/reclaim.
- **Split point** — by-bytes split index; fallback to count median; hysteresis
  (split at target, merge at target/4) prevents oscillation.

---

## 3. C++ Integration Tests

Run over `InMemoryPageStore` and `FilePageStore`.

- **basic_crud** — put/get/delete/exists; get after delete → NotFound; overwrite
  raises slot.
- **batch_apply** — multi-leaf batch; intra-batch duplicate last-wins; idempotent
  re-apply of a lower slot is a no-op.
- **scan** — prefix scan order, `limit` + `truncated`; scan across `right_sibling`
  boundaries; scan excludes tombstones.
- **split_merge** — drive enough data to split leaves and grow an inner level;
  delete down to trigger merges and root collapse; tree stays ordered and
  searchable throughout.
- **consistent_view** — pin a `snapshot_view`, mutate concurrently, verify the
  view is unchanged; `iter_all` includes tombstones; `at_slot` correct.
- **snapshot_roundtrip** — export (portable) → import into a fresh tree →
  `compare` empty; resume export from a chunk offset reproduces identical bytes.
- **gc** — tombstones below `gc_slot` removed after `collect_garbage`; stale
  unpinned root versions reclaimed; pinned versions survive.

---

## 4. Crash / Recovery Tests

Use a fault-injecting `PageStore` wrapper that can drop the tail of writes / flip
bytes / fail mid-flush.

- **checkpoint_recover** — apply N slots, checkpoint, simulate crash, recover →
  `last_applied_slot` correct, state equals pre-crash `compare`.
- **crash_between_checkpoints** — apply past a checkpoint, crash before the next →
  recover to the last checkpoint; slots above replayed by the harness reproduce
  full state (models consensus re-apply).
- **torn_page** — corrupt one flushed page's CRC → first read of it errors;
  superblock still references prior good checkpoint when its write was incomplete.
- **superblock_AB** — crash during superblock swap → recover picks the latest
  valid of A/B; never a partially-written superblock.
- **double_apply_idempotent** — replay slots `<= last_applied_slot` after recovery
  → no-ops (highest-slot-wins).

These back global milestone **G2** (persistent core, `kill -9` no data loss) and
**G3** (engine parity).

---

## 5. Rust FFI Adapter Tests

In the `crowkv` crate, exercising `CrowtreeEngine` through the C ABI.

- **trait_conformance** — `CrowtreeEngine` passes the same `KVEngine` test suite
  as `InMemKV` (shared parametrized tests).
- **async_bridge** — concurrent `get` calls during a long `apply` do not deadlock
  the `spawn_blocking` pool; cancellation safety.
- **buffer_ownership** — values/scan results survive after the C buffer is freed
  (Rust copied); no leaks under `valgrind`/`ASan` on the C side.
- **error_mapping** — backend errors surface as `EngineError`, not panics.

---

## 6. Cross-Engine Parity Tests

The strongest correctness gate (G3). Property/randomized:

- Generate a random op stream (puts/deletes/batches with increasing slots, random
  keys including duplicates and prefixes).
- Apply the identical stream to `InMemKV` and `CrowtreeEngine`.
- After every K ops: `view_a.compare(view_b)` must be empty.
- Include snapshot export(in-mem)→import(crowtree) and vice versa via the portable
  format → `compare` empty.
- Include restart-in-the-middle for `CrowtreeEngine` (checkpoint, drop, recover,
  re-apply tail) → still parity.

---

## 7. Concurrency / Sanitizer Tests

- **reader_writer_stress** — one writer applying batches while many readers
  `get`/`scan`/hold `snapshot_view`; run under **TSan** (no races) and **ASan**
  (no UAF, validates epoch reclamation) and **UBSan**.
- **epoch_reclaim_under_load** — assert retired pages are never freed while a
  reader guard could reference them (instrumented counters).
- **version_pin_gc** — long reader pins an old version while many checkpoints
  churn; memory bounded; version freed promptly after the reader drops.

---

## 8. Benchmarks

Google Benchmark (C++) + criterion (Rust end-to-end), to validate D2/D4 choices.

- Point read (hot in-memory / cold from `FilePageStore`).
- Batch apply throughput vs delta-chain length and consolidation policy.
- Range scan rate (sequential leaf traversal).
- Checkpoint cost vs dirty-page count.
- Comparison knob: delta+consolidate vs pure COW path-copy (validates the mini-
  bw-tree write-amplification claim).

---

## 9. Test Tooling

- **Build/CI** — CMake + GoogleTest/Benchmark for C++; ASan/TSan/UBSan jobs; the
  Rust side links `libcrowtree` and runs `cargo test` + `cargo bench`.
- **Fault injection** — `FaultyPageStore` decorator (drop-tail, flip-bytes,
  fail-flush) for recovery tests.
- **Deterministic harness** — seeded RNG op-stream generator shared by parity and
  property tests, printable for repro on failure.
- **Oracle** — `InMemKV` is the reference model for `compare()`-based equivalence.
