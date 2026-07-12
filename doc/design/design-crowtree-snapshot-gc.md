# CrowKV - Design: crowtree Snapshot and GC Flow Integration

Parent: [`design-crowtree.md`](design-crowtree.md)
Depends on: [`design-crowtree-core.md`](design-crowtree-core.md), [`design-crowtree-persistence.md`](design-crowtree-persistence.md), [`design-state-machine.md`](design-state-machine.md), [`design-wal.md`](design-wal.md), [`design-reconfiguration.md`](design-reconfiguration.md)

This document wires crowtree's snapshot and GC primitives into the existing
CrowKV flows: learner apply / restart, the consensus WAL, GC watermarks, and
new-member snapshot install.

## Table of Contents

- [1. Watermarks](#1-watermarks)
- [2. Snapshot Export / Import](#2-snapshot-export--import)
- [3. Restart and last_applied_slot](#3-restart-and-last_applied_slot)
- [4. Garbage Collection](#4-garbage-collection)
- [5. Interaction with Consensus WAL GC](#5-interaction-with-consensus-wal-gc)
- [6. New-Member Install Flow](#6-new-member-install-flow)
- [7. Sequence Summaries](#7-sequence-summaries)

---

## 1. Watermarks

crowtree consumes two slot watermarks the learner already tracks, plus its own
`last_applied_slot`:

| Watermark | Source | Meaning |
| --- | --- | --- |
| `last_applied_slot` | crowtree (per snapshot) | Highest slot whose effects are durable in the engine's last snapshot. |
| `snapshot_slot` | learner / replicator | State at this slot is durable on leader + ≥1 peer. |
| `safe_slot` | learner (`contiguous_applied` min across members) | Every learner has applied through here. |

`set_gc_watermark(snapshot_slot, safe_slot)` is called by the learner whenever
these advance. crowtree stores them and uses `gc_slot = min(snapshot_slot,
safe_slot)` to gate tombstone and version reclamation (§4), matching
`design-state-machine.md §7`.

---

## 2. Snapshot Export / Import

Export/import back the `KVEngine::snapshot_export/import` methods
(`design-crowtree.md §4`) and feed the `SnapshotService` streaming transfer
(`design-reconfiguration.md`, `plan.md P5 M1`).

### Export

`snapshot_export()`:

1. The engine pins the current `RootVersion` (core doc §9) — always the latest
   durable state. There is no `at_slot` parameter; historical snapshot export is
   not supported (Raft InstallSnapshot always installs the current state).
2. It streams chunks over the pinned, immutable tree. Two formats:
   - **Portable** `(key, slot, kind, value)` tuples in key order, versioned
     header, default 1 MiB chunks. Deterministic byte boundaries → resumable.
     Required for cross-engine parity tests (in-memory ↔ crowtree).
   - **Native** page dump (faster) for crowtree↔crowtree production transfer.
3. The pin is released when the export ends. The export holds the version alive
   even as newer snapshots supersede it (refcount in core doc §9).

The first implementation ships the **portable** format (P3 M3); the native dump
is an optimization (TODO-CONFIRM in `design-crowtree.md §7`).

### Import

`snapshot_import(stream)`:

1. Builds a fresh tree (bulk-loaded, bottom-up, fully consolidated immutable base
   pages) into staging, never touching the live tree.
2. Verifies the end-to-end CRC after the last chunk.
3. Atomically swaps the staged tree in as a new `RootVersion` with
   `last_applied_slot` = the slot from the export stream header; old version retired via epoch.

Atomic to readers: until the swap, reads serve the previous state (core doc §9,
state-machine §6.5).

---

## 3. Restart and last_applied_slot

This replaces the former `DurableCommitWatermark` WAL record (state-machine §2.1).

```
node restart:
    1. consensus WAL replay rebuilds ACCEPTOR state only
       (Promised / Accepted / VoteGranted)  — design-wal.md
    2. crowtree.recover() loads superblock -> last_applied_slot = L  (persistence §7)
    3. learner sets contiguous_applied = L
    4. slots (L, max_chosen] are re-learned:
         - steady-state heartbeat catch-up, or
         - new-leader bulk Phase 1
       and re-applied to crowtree via apply(slot, batch)
```

Re-applying slots `<= L` is harmless: highest-slot-wins makes them no-ops (core
doc §6). So the consensus WAL need only retain entries `> min(last_applied_slot
across members)`; see §5.

---

## 4. Garbage Collection

`collect_garbage()` reclaims two kinds of space, both gated by `gc_slot =
min(snapshot_slot, safe_slot)`:

1. **Tombstones.** A tombstone cell written at slot `t` is dropped during
   consolidation when `t < gc_slot` (state-machine §7.1). The sweeper passes
   crowtree a "drop tombstones below `gc_slot`" hint; consolidation honors it.
2. **Stale root versions.** A `RootVersion` with `refcount == 0` and
   `last_applied_slot < gc_slot` is retired, and the pages reachable only from it
   are freed (page-allocation map entries returned to the free list).

Triggers (state-machine §7.3):

- **Periodic** — background sweep every `compaction_tick` (default 5 min).
- **Pressure** — backend reports low free space → focused sweep / eager
  snapshot+GC.
- **Post-snapshot** — after a snapshot, eligible tombstones below the new
  `snapshot_slot` are swept.

`GcStats` reports tombstones dropped, versions retired, pages/bytes freed.

---

## 5. Interaction with Consensus WAL GC

The consensus WAL's GC (`design-wal.md`, `plan.md P2 M5`) uses `gc_slot =
min(safe_slot, snapshot_slot)`. crowtree's `last_applied_slot` participates:

- A segment of the consensus WAL may be unlinked only when all its records have
  `slot < min(last_applied_slot across members, safe_slot, snapshot_slot)`.
- Because a restarting node resumes at its own `last_applied_slot` and re-learns
  the rest, the consensus WAL must retain entries above the **minimum**
  `last_applied_slot` of any member that might restart and replay locally.

This couples the two GC watermarks: crowtree advancing `last_applied_slot` (via
snapshots) is what eventually lets the consensus WAL drop old segments.

---

## 6. New-Member Install Flow

For a new or far-lagging member (`design-reconfiguration.md`, `plan.md P5 M1`):

```
1. Leader picks a snapshot slot S (>= its last_applied_slot).
2. Leader engine: snapshot_export() streams chunks via SnapshotService.
   S = leader's last_applied_slot at export time (always the latest durable state).
3. New member engine: snapshot_import(stream) builds + swaps in the tree at S.
4. New member sets contiguous_applied = S, then streams consensus WAL
   (S+1, current_max_chosen] and applies via apply(slot, batch).
5. compare(view) against an existing learner -> empty diff (parity gate G3/G4).
```

Resumability and throttling are handled by the snapshot module above the engine;
crowtree only provides deterministic, chunk-boundary-stable export and atomic
import (persistence §8 C API: `ct_snapshot_export_*` / `ct_snapshot_import*`).

---

## 7. Sequence Summaries

**Steady-state write + periodic durability**

```
learner.learn(entry) -> engine.apply(slot, batch)         // in-memory deltas
... every snapshot_every_slots / dirty threshold ...
engine.persist_snapshot() -> last_applied_slot advances  // durable + new RootVersion
learner observes safe_slot/snapshot_slot advance -> engine.set_gc_watermark(...)
background -> engine.collect_garbage()                      // tombstones + stale versions
```

**Consistent read / verification**

```
view = engine.snapshot_view()      // pin RootVersion
view.scan / view.iter_all / view.compare(other_view)   // stable, no global stop
drop(view)                         // release pin -> version GC-eligible
```

**Crash / restart** — see §3.

These flows require no change to the learner's public contract beyond the
redefined async `KVEngine` surface (`design-crowtree.md §4`); `InMemKV`
implements the same methods (snapshot/GC are near-no-ops in memory) so tests
exercise the same code paths.
