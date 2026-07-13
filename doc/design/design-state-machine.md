# CrowKV - Design: State Machine

Depends on: [`requirement.md`](../requirement.md), [`design.md`](../design.md)
Satisfies: [requirement.md §8.3](../requirement.md#83-learner-storage), [requirement.md §8.4 import/export](../requirement.md#84-snapshot-and-install), implementation prerequisites of [requirement.md §14.1 crowbench `compare`](../requirement.md#141-correctness-criteria-for-crowbench)

This document specifies the storage engine abstraction used by CrowKV learners. The engine is the **only** consumer of consensus output; it owns the materialized key-value state and serves all reads. The WAL is the durable log; the engine is the materialized projection.

> **P3 redesign note.** The production engine `crowtree` and the redefined
> (async, snapshot/GC-aware) `KVEngine` abstraction are specified in the crowtree
> sub-design set: [`design-crowtree.md`](design-crowtree.md) (overview + engine
> abstraction + language/FFI decisions), [`design-crowtree-engine.md`](design-crowtree-engine.md)
> (in-memory engine + memory model + async FFI), [`design-crowtree-storage.md`](design-crowtree-storage.md)
> (durable storage + mapping table + snapshot/GC flow), and
> [`design-crowtree-test.md`](design-crowtree-test.md). This document remains the
> source of truth for the *semantics* (per-key slot, apply, compare, compaction);
> crowtree docs own the *implementation*.

## Table of Contents

- [1. Goals and Non-Goals](#1-goals-and-non-goals)
- [2. Engine Responsibilities](#2-engine-responsibilities)
- [3. Per-Key Slot Tracking and Single Versioning](#3-per-key-slot-tracking-and-single-versioning)
- [4. Apply Semantics](#4-apply-semantics)
- [5. Read Surface](#5-read-surface)
- [6. Snapshot Export and Import](#6-snapshot-export-and-import)
- [7. Compaction Policy](#7-compaction-policy)
- [8. Compare for Cross-Learner Validation](#8-compare-for-cross-learner-validation)
- [9. Engine Implementations](#9-engine-implementations)
- [10. Tunables and Defaults](#10-tunables-and-defaults)

---

## 1. Goals and Non-Goals

**Goals:**

- Define one engine surface so the same learner code drives any backend (in-memory, file, crowtree).
- Encode the per-key resolved-slot semantics that make parallel-slot consensus correct.
- Provide a deterministic state-equality check (`compare`) for cross-learner test verification.
- Provide a streamable snapshot import/export usable by snapshot install.

**Non-goals:**

- Multi-version (MVCC) reads. CrowKV is single-version per key; time-travel is not supported.
- Cross-engine queries. The engine is per-group.
- Transactions across keys beyond a single batch. Batch-level atomicity only.
- Pluggable compression/encryption. Each engine handles its own internals.

---

## 2. Engine Responsibilities

The engine encapsulates everything below the consensus / learner layer:

| Responsibility | Notes |
| --- | --- |
| Persist (or hold in memory) the current `(slot, value)` for every live key | Single version, tombstones for deletions |
| Apply a `(slot, batch)` atomically | Idempotent for slots ≤ resolved-slot of each affected key |
| Serve point reads with their resolved-slot | Used by leader linearizable reads, follower RYW |
| Serve ordered range scans | Required for `Scan` API |
| Export and import snapshots | Streamable; resumable on the consumer side |
| Provide a deterministic state digest or comparison | For test-time validation |
| Track watermarks for compaction | `snapshot_slot`, `safe_slot` (provided by learner) |
| Persist snapshot with slot index | The engine persists its KV state and `last_applied_slot` to a snapshot file; on restart, it loads this snapshot to skip re-applying already-applied slots |

The engine does **not** know about Paxos, terms, ballots, leaders, or the network. It receives `(slot, batch)` and applies; that is the entire write contract.

### 2.1 State Machine Restart: Engine Durability + WAL Replay

The state machine's persistence boundary is separate from the WAL — but it is
not a dedicated "state-machine snapshot file" either. It is whatever the
*engine itself* durably persists (crowtree's own snapshot pipeline,
[`design-crowtree-storage.md §6`](design-crowtree-storage.md#6-snapshot-recovery-and-exportimport)),
queried through `KVEngine::resume_from_slot()`. This replaces the former
`DurableCommitWatermark` WAL record design.

**Restart behavior** (`PxLocalReplica::restore_from_replay_with_engine`,
`crowkv/src/cluster/local_replica.rs`):

1. WAL replay rebuilds acceptor state (`Promised`, `Accepted`, `VoteGranted`) — Pass 1, unconditional.
2. The engine reports `resume_from = resume_from_slot()`: the highest slot it
   already durably reflects (always `0` for `InMemKV`; crowtree's
   `last_applied_slot`, restored from its on-disk commit anchor, for a
   durable engine that was cleanly snapshotted before the crash/restart).
3. Pass 2 `learn()`s every WAL-accepted slot in `(resume_from, highest_local]`
   into the engine, in order, and fast-forwards the learner's frontier
   (`contiguous_chosen`/`contiguous_applied`/`last_chosen_term`) directly to
   `resume_from` via `seed_resume_frontier` — skipping the now-redundant
   re-`learn()` of the already-durable prefix.
4. Slots above the local WAL's own highest accepted slot are re-learned via
   new-leader recovery (bulk Phase 1) or steady-state heartbeat catch-up (§6
   of this doc's parent flows, `design-parallel-slots.md`).

Step 3's skip is a pure **optimization**, not a correctness requirement:
`KVEngine::apply` is idempotent (highest-slot-wins per key) and
`update_frontier` tolerates out-of-order slots, so replaying the *entire*
local WAL through `learn()` (`resume_from = 0`, `InMemKV`'s permanent case)
always converges to the same correct state — it's just extra, safe, no-op
work. `persist_snapshot()` (called periodically by the group's maintenance
loop, `crowkv/src/cluster/group_maintenance.rs::run_pass`) is what advances
the durable floor `resume_from_slot()` will report on the *next* restart.

---

## 3. Per-Key Slot Tracking and Single Versioning

### 3.1 What we store

For every live key `k`, the engine stores exactly one tuple:

```
   (k → resolved_slot(k), value_or_tombstone(k))
```

- `resolved_slot(k)` is the highest slot that has touched key `k` and been applied.
- `value_or_tombstone(k)` is the value from that slot, or a tombstone marker if that slot was a `Delete`.

Tombstones occupy space until compacted away (§7).

### 3.2 Why single-version

CrowKV does not provide repeatable reads or time-travel queries. Snapshot reads use the `AtSlot(N)` mode by waiting for the engine's contiguous-applied to reach `N`, then reading the current single version.

`Scan(AtSlot(N))` returns the engine state *after* applying everything up through the contiguous-applied frontier of the serving replica, which the replica advances to ≥ `N` before serving. If a slot `M > N` has already been applied for some key `k`, the value returned for `k` is the value at `M`, not the value at `N`. This still satisfies linearizability: slot `M` linearizes after slot `N`, so the read at "logical instant `N`" is consistent with reading at the later linearization point `M` — both are valid linearization points for a single point in real time. `AtSlot(N)` is therefore a *lower bound on freshness*, not a snapshot pin: single-version reads always reflect the latest applied value.

If true historical snapshots are ever required, MVCC is a future extension. The single-version restriction comes from [requirement.md §1 / §5.2](../requirement.md#5-data-model-and-client-api).

### 3.3 Resolved-slot is monotone per key

The engine never accepts a write at slot `s` for key `k` if `s ≤ resolved_slot(k)`. This is the runtime expression of [Invariant I5 in `design-parallel-slots.md`](design-parallel-slots.md#2-concepts-and-invariants).

Implication: replays and out-of-order applies are naturally idempotent. If WAL replay tries to apply slot 7 for key `k` and `resolved_slot(k)` is already 9, the apply is a no-op for `k` — consistent with the parallel-slot semantics.

---

## 4. Apply Semantics

The engine's `apply(slot, batch)` is the only mutator. It must be atomic and idempotent.

### 4.1 Inputs

- `slot` — the slot of this batch. All operations in `batch` carry this same slot.
- `batch` — an ordered list of `(key, op, value?)` tuples, where `op ∈ { Put, Delete }`.

### 4.2 Procedure

For each `(k, op, v?)` in batch order:

1. Read current `(resolved_slot(k), current)` if any.
2. If `slot ≤ resolved_slot(k)` → skip (idempotent no-op).
3. Else write `(slot, value)` for `Put`, or `(slot, tombstone)` for `Delete`.
4. Update `resolved_slot(k) = slot`.

After all tuples are processed, advance the engine's `max_applied = max(max_applied, slot)` and update the contiguous-applied frontier if `slot == contiguous_applied + 1`, possibly cascading.

### 4.3 Atomicity

The whole `apply` is atomic with respect to readers: a reader either sees all of the batch's effects or none. This is required so that a `Scan(Linearizable)` does not observe a partial batch as the "current state".

In-memory engines can hold a write lock for the duration of the batch. File-based engines can use a transactional write group. crowtree's btree operations on a single batch can ride on its own concurrency control.

### 4.4 Intra-batch order

For a key `k` appearing multiple times in a batch (rare but legal — see [requirement.md §7.3.1](../requirement.md#731-correctness-analysis-for-parallel-slot-writes)), the *last* occurrence in batch order wins. The earlier ones are folded into the apply procedure naturally (each tuple in turn updates `current`; the loop's final state is what persists).

### 4.5 Failure during apply

`apply` either completes or returns an error. On error, the engine must leave its state unchanged (no partial apply). The learner treats engine apply errors as fatal — they indicate disk corruption or out-of-space — and fails the node out of the group.

---

## 5. Read Surface

The engine exposes three read primitives. All are non-mutating and may be served concurrently with writes.

### 5.1 `get(k) → Option<(slot, value)>`

Returns `None` if `k` is unset or tombstoned (callers may distinguish if they care, e.g. for range scans excluding tombstones). Returns `Some((resolved_slot(k), value))` if a live value exists.

The slot is returned because callers (the learner) need it to assemble responses to `Get` with read-mode semantics.

### 5.2 `scan(range, limit) → iterator<(key, slot, value)>`

Returns an iterator of live entries (no tombstones) within `range`, in key order, up to `limit` items. The iterator may be backed by a btree cursor (in crowtree) or a sorted-tree iterator (in-memory).

The iterator must reflect a consistent point-in-time view of the engine. In-memory engines use a snapshot-on-iterator-create. File engines and crowtree use their natural snapshot or copy-on-write semantics. The point in time corresponds to "after some `apply` calls and before others" — which is always a valid linearization point given the consensus layer's slot ordering.

### 5.3 `multi_get(keys) → map<key, (slot, value)>`

A batched point read. Returned as a map; missing keys are simply absent.

### 5.4 What the engine does **not** expose

- No range-write API. All writes go through `apply(slot, batch)`.
- No "delete-range" API. Range deletes are decomposed by the consensus layer into per-key deletes inside a batch.
- No "wait-for-slot" primitive. Wait conditions (RYW, SafeSlot, AtSlot) are implemented in the learner using the engine's `contiguous_applied` watermark, not inside the engine.

This minimal surface keeps multi-engine compatibility easy.

---

## 6. Snapshot Export and Import

### 6.1 Purpose

Snapshot install ([§8.4 of design.md](../design.md#84-snapshot-and-install)) is the bootstrap path for new or far-lagging members. The engine provides export/import primitives that the snapshot module wraps in a chunked, resumable, throttled transfer.

In addition to peer transfer, the engine uses snapshot export to persist its own state locally (see §2.1). The local snapshot file stores the KV state and `last_applied_slot`, enabling fast restart without re-applying the entire WAL.

### 6.2 Export

`snapshot_export() → stream<chunk>`:

- Returns a stream of opaque chunks that, when imported by a peer engine, produce the same logical state as the exporter's current `last_applied_slot`.
- The export reflects the engine state *after* applying every slot ≤ `last_applied_slot` (always the latest durable state).
- Chunks have stable ordering and stable boundaries: the same state always produces the same byte sequence, modulo any internal pagination. This determinism is what makes resumption possible.
- Chunk size is engine-defined; 1–4 MiB is a reasonable default.

The chunk format is **engine-specific** — the in-memory tree might serialize key/value pairs in btree order; crowtree might dump native pages directly. The snapshot module treats chunks as opaque.

### 6.3 Import

`snapshot_import(stream<chunk>) → ()`:

- Consumes a stream of chunks produced by the same engine type.
- On success, the engine's state is exactly what `snapshot_export()` produced, with `contiguous_applied` and `max_applied` set to the exported slot.
- Resumable: an import that fails part-way may be restarted from a known chunk offset (the snapshot module records the last successful offset).
- Verified: the snapshot module checks an end-to-end CRC after the last chunk before activating the import.

### 6.4 Cross-engine snapshots

The default rule: **snapshot exported by engine X can only be imported by engine X.** A 3-node group all running crowtree can snapshot-install crowtree-formatted snapshots. A test cluster mixing in-memory and file engines must export/import via a portable format.

A portable interchange format (e.g. sorted key-value pairs with slots) is provided as an optional path for testing and operations, at the cost of being slower than the native path. Production deployments should use a single engine type per group.

### 6.5 Atomicity of import

The import is atomic to readers: until the last chunk is processed and the import is activated, the engine continues serving its previous state. Only at the activation step does the engine swap to the imported state. This avoids serving a partial snapshot.

---

## 7. Compaction Policy

Tombstones and replaced values can be removed once they are no longer needed. The two watermarks that gate compaction:

- `snapshot_slot` — the engine state at this slot is durable on at least leader + one peer.
- `safe_slot` — every learner has applied every slot up to here.

### 7.1 When can a tombstone be GC'd?

A tombstone for key `k` placed at slot `t` may be GC'd when `t < min(snapshot_slot, safe_slot)`. Both must have advanced past `t` because:

- Until `safe_slot` passes `t`, some learner might still be applying earlier slots that could resurrect `k`. (Cannot happen for blind ops, but could complicate snapshot install if the tombstone is GC'd too early.)
- Until `snapshot_slot` passes `t`, a snapshot install must reproduce the tombstone — otherwise the receiving peer would think `k` was never deleted.

The conservative "both must pass" rule prevents observability holes.

### 7.2 When can an old value be GC'd?

When a key `k` is overwritten with a higher slot value, the old value is immediately overwritten in single-version storage; there is nothing to GC. The complication is only with tombstones (representing deletes) and with engine-internal versioning (e.g. crowtree's MVCC of pages, which it manages internally).

### 7.3 Compaction trigger

- **Periodic.** A background sweeper runs every `compaction_tick` (default 5 min) and removes eligible tombstones in batches.
- **Pressure.** If the engine reports memory or disk pressure, a focused sweep runs.
- **Snapshot-side-effect.** When a snapshot completes, eligible tombstones for slots ≤ that snapshot may be swept as part of the post-snapshot bookkeeping.

### 7.4 Engine-specific compaction

- In-memory: tombstones are simply removed from the map.
- Ordered file: the file is rewritten without the tombstones. The rewrite is atomic via swap.
- crowtree: leverages crowtree's internal compaction. The sweeper hands crowtree a "tombstones below slot S are safe to drop" hint and crowtree merges that into its compaction policy.

---

## 8. Compare for Cross-Learner Validation

A `compare(other) -> diff` operation is required so that `crowbench` can verify state equality across learners after a test run ([requirement.md §14.1](../requirement.md#141-correctness-criteria-for-crowbench)).

### 8.1 Semantics

`compare(other)` returns a diff describing the keys that differ in `(slot, value)` between `self` and `other`. If the engines are in the same logical state (same set of live keys, same `(slot, value)` per key), the diff is empty.

The compare is **logical**, not byte-level. Two engines may have different physical layouts (e.g. one in-memory tree and one ordered file) yet be logically equal. Compare must succeed across engine types.

### 8.2 Required behavior

- Order: results sorted by key.
- Tombstones: included if they differ. Two engines that both have `k` tombstoned at the same slot are equal.
- Resolved-slot: compared exactly. Two engines with the same value for `k` but different `resolved_slot(k)` are *not* equal — this would indicate that one of them missed an apply.

### 8.3 Implementation strategy

For testing, a simple two-cursor merge over both engines' ordered key streams is sufficient. The engines provide a `iter_all() → iterator<(key, slot, value_or_tombstone)>` interface to support this. The cursors advance the smaller key first; when both are at the same key, compare `(slot, value)`; emit a diff if they disagree.

For very large states, a Merkle-tree-style digest could be added later; not required for the initial implementation.

### 8.4 Compare under in-flight writes

`compare` is a snapshot-time operation. It assumes the engines are quiescent (no apply in flight) or that the iterator semantics provide a consistent point-in-time view. The test harness ensures quiescence by stopping client traffic and waiting for `safe_slot == max_chosen` before invoking compare.

---

## 9. Engine Implementations

Three engine implementations satisfy the surface above. Each is appropriate for a specific phase of development or workload.

### 9.1 In-Memory Tree

- Backing store: a sorted in-memory map (e.g. a btree or skiplist).
- Persistence: none.
- Use cases: unit tests, integration tests where startup and teardown are cheap, behavior-validation runs of `crowbench`.
- Snapshot format: serialized sorted (key, slot, value-or-tombstone) tuples.
- Concurrency: read-write lock over the whole map for `apply`; concurrent reads via a snapshot pointer.

### 9.2 Ordered File

- Backing store: a single sorted key-value file with a small index.
- Persistence: yes (via the engine's own fsync, separate from the WAL).
- Use cases: testing without WAL replay surprises, debugging on a single machine, lightweight production use cases that don't need crowtree's features.
- Snapshot format: a copy of the file plus its index.
- Concurrency: write-aside-then-swap; readers see a stable file pointer.

### 9.3 crowtree

- Backing store: the production btree library `crowtree` — a C++ `libcrowtree`
  (single-writer COW B+tree with per-leaf delta chains, epoch GC, versioned root)
  consumed from Rust over a coarse C ABI. Full design:
  [`design-crowtree.md`](design-crowtree.md) and its sub-docs.
- Persistence: yes, via a pluggable `PageStore` (local file, raw block device,
  remote/RDMA); snapshot + consensus replay for recovery (no second op-log).
- Use cases: production.
- Snapshot format: portable `(key, slot, cell)` tuples first; native page dump as
  an optimization.
- Concurrency: single writer (learner apply) + lock-free immutable-page reads,
  epoch-based reclamation.

The trait surface is engine-agnostic; switching engines is a configuration choice. All three pass the same `compare`-based equivalence tests.

---

## 10. Tunables and Defaults

| Parameter | Default | Range | Notes |
| --- | --- | --- | --- |
| `compaction_tick` | 5 min | 10 s – 24 h | Background tombstone sweep cadence |
| `snapshot_chunk_bytes` | 1 MiB | 64 KiB – 64 MiB | Streaming snapshot chunk size |
| `engine_apply_concurrency` | 1 | 1 – ∞ | Apply is serialized by slot anyway; >1 makes sense only if apply is non-overlapping per-key |
| `engine_read_concurrency` | unbounded | — | Reads do not contend with apply at the engine level |
| `tombstone_grace_slots` | 0 | 0 – ∞ | Optional minimum number of slots to keep a tombstone past the GC watermark, for forensics |

**Engine choice guidance:**

- Unit and integration tests → in-memory.
- Manual debugging or operations exercises → ordered file.
- Production → crowtree.

**Per-key memory cost considerations:** the per-key resolved-slot adds 8 bytes per live key. For 10⁹ live keys this is 8 GiB on every learner, accepted in the requirement ([§7.3.1](../requirement.md#731-correctness-analysis-for-parallel-slot-writes)). If memory pressure dictates, a future optimization could compress recently-applied resolved-slots into a "below safe-slot" bit (a single bit replacing the 8 bytes when the per-key slot is no longer needed for read-your-writes).
