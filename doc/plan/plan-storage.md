# CrowKV - Plan: Storage Engine Implementation

Depends on: [`plan.md`](plan/plan.md), [`design-storage-engine.md`](design/design-storage-engine.md), [`plan-consensus.md`](plan/plan-consensus.md)
Satisfies: [requirement.md §8.3](requirement.md#83-learner-storage), [requirement.md §8.4](requirement.md#84-snapshot-and-install)

Phase 3 implementation: engine trait formalization, ordered-file backend, snapshot skeleton.

## 1. Milestones

### M1 — Engine trait freeze

The trait was introduced in P1 M4 with `InMemoryEngine`. P3 M1 freezes the surface and removes any P1 placeholder methods.

- Final trait surface (all methods `async`, per [`plan.md`](plan/plan.md) §7): `apply(slot, batch)`, `get(k) → Option<(slot, value)>`, `scan(range, limit) → impl Stream<Item=(K,V)>`, `snapshot_export() → impl Stream<Item=Chunk>`, `snapshot_import(stream)`, `compare(other) → Diff`, `iter_all()` (for `compare` two-cursor merge per [`design-storage-engine.md`](design/design-storage-engine.md) §8.3).
- Trait lives in `crowkv::engine`; `crowkv::consensus` code is generic over `E: Engine`. Use `async_trait` (or RPITIT once stable on toolchain) to express async methods on the trait.
- Disk-touching backends (ordered-file, future crowtree) use the project async I/O facade ([`design-async-io.md`](design/design-async-io.md)) — same one as the WAL. No direct syscalls in engine code.

**Acceptance:** compile-time check that `InMemoryEngine`, `OrderedFileEngine`, and `CrowtreeEngine` (placeholder) all implement trait without warnings.

### M2 — Ordered-file backend

- Single sorted key-value file with small index
- `apply` writes to staging area, fsync, atomic swap into active file
- `scan` via btree cursor over sorted file

**Acceptance:** `compare()` between in-memory and ordered-file engines after identical ops returns empty diff.

### M3 — Snapshot export/import skeleton

- `snapshot_export` serializes `(key, slot, value_or_tombstone)` tuples in key order with a versioned header
- `snapshot_import` consumes same format, builds engine state atomically (no partial-state visibility per [`design-storage-engine.md`](design/design-storage-engine.md) §6.5)
- Resumable chunk boundaries marked in stream; default chunk size 1 MiB ([`design-storage-engine.md`](design/design-storage-engine.md) §6.2)
- Snapshot format: portable interchange format in P3 (engine-agnostic, used for cross-engine testing). Native engine-specific formats may be added per-engine later if performance requires.

**Acceptance:** export → import round-trip produces engine `compare()` equal to original; resume-from-offset reproduces same final state.

### M4 — crowtree placeholder

- `CrowtreeEngine` struct implements trait, delegates all methods to `InMemoryEngine`
- `snapshot_export` contains `todo!("crowtree integration")`

**Acceptance:** compiles, all crowtree tests skipped with `#[ignore]`.

## 2. Module Breakdown

Module: **`engine`** inside `crowkv` (introduced in P1 M4 with `Engine` trait + `InMemoryEngine`; P3 adds the rest).

| Rust path (in `crowkv/src/engine`) | Responsibility | Phase |
|---|---|---|
| `mod.rs` | `Engine` trait definition (FROZEN end of P1 M4 / P3 M1) | P1 M4 |
| `memory.rs` | `InMemoryEngine` (btree-based) | P1 M4 |
| `ordered_file.rs` | `OrderedFileEngine` (sorted file + small index) | P3 M2 |
| `snapshot.rs` | Portable snapshot format, chunk framing | P3 M3 |
| `crowtree.rs` | `CrowtreeEngine` placeholder delegating to in-memory | P3 M4 |

`crowkv::engine` depends on `crowkv::io` for disk-touching backends (`ordered_file`, future `crowtree`). `crowkv::consensus` depends on `crowkv::engine`.

## 3. Freeze Checklist

Before P4 (RPC) starts (P2 WAL proceeds independently):
- [ ] Engine trait surface frozen (no method additions in P4 without explicit version bump)
- [ ] `compare()` deterministic and cross-engine (in-memory vs ordered-file equivalence)
- [ ] Snapshot format versioned and self-describing (`magic`, `version` header)
- [ ] G3 milestone passes: engine swap-in without consensus changes
