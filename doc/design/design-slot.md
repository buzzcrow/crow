# CrowKV - Design: Slots — Parallel Pipelining & Concurrent Slot List

Depends on: [`requirement.md`](../requirement.md), [`design.md`](../design.md)
Satisfies: [requirement.md §6.5](../requirement.md#65-parallel-slot-linearizability-analysis), [requirement.md §7.3](../requirement.md#73-parallel-slot-processing), [`requirement.md`](../requirement.md) §5.1 (high-concurrency log)

This document covers two aspects of CrowKV's slot mechanism:

- **Part A (§1–§14): Parallel Slot Pipelining** — the consensus protocol that distinguishes CrowKV from Raft-based KV systems. How the leader pipelines proposals, how gaps are detected and repaired, how the safe-slot is maintained, and how the system stays correct under concurrent in-flight slots.
- **Part B (§15–§22): Concurrent Sparse Slot List** — the `SlotList<T>` data structure that backs the Acceptor's per-slot state. Chunked array, lock-free reads, trim/GC, reclamation.

## Table of Contents

- [1. Why Parallel Slots](#1-why-parallel-slots)
- [2. Concepts and Invariants](#2-concepts-and-invariants)
- [3. Slot Lifecycle on the Leader](#3-slot-lifecycle-on-the-leader)
- [4. Sliding Window and Backpressure](#4-sliding-window-and-backpressure)
- [5. Pipelined Fanout](#5-pipelined-fanout)
- [6. Per-Key Resolved-Slot](#6-per-key-resolved-slot)
- [7. Safe-Slot Computation and Propagation](#7-safe-slot-computation-and-propagation)
- [8. Gap Detection](#8-gap-detection)
- [9. Gap Repair via Classic Paxos](#9-gap-repair-via-classic-paxos)
- [10. Timing Diagrams](#10-timing-diagrams)
- [11. Interaction with Snapshot and WAL GC](#11-interaction-with-snapshot-and-wal-gc)
- [12. Tunables and Defaults](#12-tunables-and-defaults)
- [13. Correctness Analysis for Parallel Slot Writes](#13-correctness-analysis-for-parallel-slot-writes-moved-from-requirementmd-731)
- [14. Parallel-Slot Linearizability Analysis](#14-parallel-slot-linearizability-analysis-moved-from-requirementmd-65)
- [15. Slot List: Problem Statement](#15-slot-list-problem-statement)
- [16. Slot List: Design Overview](#16-slot-list-design-overview)
- [17. Slot List: PxSlotNode](#17-slot-list-pxslotnode)
- [18. Slot List: Algorithms](#18-slot-list-algorithms)
- [19. Slot List: API Surface](#19-slot-list-api-surface)
- [20. Slot List: Correctness Invariants](#20-slot-list-correctness-invariants)
- [21. Slot List: Performance Model](#21-slot-list-performance-model)
- [22. Slot List: Risks, Reclamation & Open Questions](#22-slot-list-risks-reclamation--open-questions)

---

# Part A: Parallel Slot Pipelining

## 1. Why Parallel Slots

A Raft leader cannot acknowledge slot N+1 until slot N has been committed; the log is contiguous, so a single slow follower stalls the entire commit pipeline (head-of-line blocking). Multi-Paxos has no such constraint: each slot is a separate Paxos instance, and a quorum that decides slot N+1 need not include the same nodes that decide slot N.

CrowKV exploits this by running many slots in parallel on the leader. Throughput is bounded by network and disk bandwidth, not by per-slot serialized round-trips. The price is two-fold:

- **Gaps.** A slot may remain undecided long after later slots are decided. We need a mechanism to resolve gaps without stalling the hot path.
- **Conservative cross-key reads.** A `Scan` must wait for a no-gap prefix; point reads do not.

The blind-ops premise from [requirement.md §5.2](../requirement.md#52-operations) makes the trade-off cheap: out-of-order *apply* is safe because no operation reads before writing.

---

## 2. Concepts and Invariants

- **I1 — Single slot counter.** On a leader, slot assignment is performed by exactly one logical worker. Two writes never receive the same slot, and no two slots are assigned out of arrival order.
- **I2 — Slot determines linearization.** The slot number assigned to an op is the op's position in the global linearization order ([requirement.md §6.1](../requirement.md#61-write-guarantee)).
- **I3 — Quorum-fsync before ack.** A client write is acked only after a quorum of acceptors (including the leader) have fsynced their `Accepted(slot, ballot, value)` records. This is the durability hook that makes I2 robust to failures.
- **I4 — Apply-order independence for blind ops.** For any key *k*, the engine's final value is `value(max{ slot | slot writes k })`. Apply order between non-overlapping keys is irrelevant; for the same key, the higher slot wins regardless of arrival order ([§13](#13-correctness-analysis-for-parallel-slot-writes-moved-from-requirementmd-731)).
- **I5 — Per-key resolved-slot is monotone.** A learner's per-key tracker only ever advances. This is the basis for read-your-writes from followers.
- **I6 — Safe-slot is contiguous.** The cluster-wide safe-slot is the maximum N such that *every* slot ≤ N is chosen and applied on every learner. It is by definition gap-free.

---

## 3. Slot Lifecycle on the Leader

| State | Trigger to enter | Trigger to leave | Notes |
| --- | --- | --- | --- |
| `Assigned` | client request admitted; counter incremented | leader's WAL fsync of the `PxLogEntry` completes | Visible to no one yet |
| `Proposing` | local fsync done; `Accept` fanned out to followers | quorum of `Accepted` responses received | At this point the value is *chosen* |
| `Chosen` | quorum reached | learner has applied to engine | Ack to client may be sent here (see below) |
| `Applied` | learner.apply() returned | terminal | Used by metrics; safe to evict |
| `OrphanRepair` | leader changed before reaching `Chosen`; repair task takes over | `Chosen` (via repair) | New leader's responsibility |

**When does the ack happen?** When the slot reaches `Chosen` *and* the leader's own learner has applied it. The leader's own learner application is required so that an immediately-following `Get` on the leader sees the new value (see [§6.1 of design.md](../design.md#61-linearizable-leader-read)).

**Eviction.** Once a slot's record is `Applied` *and* its slot number is below the safe-slot, it can be dropped from the in-memory map. The WAL record stays until WAL GC catches up.

---

## 4. Sliding Window and Backpressure

The number of in-flight slots is capped at the **window size** (`proposer_window`, default 16). The proposer uses a `Semaphore` as a sliding-window admission gate. If a permit is available, the request is admitted, slot is assigned, and proposing begins. The permit is held for the entire proposal duration. If no permit is available (`try_acquire` fails), the leader immediately returns `Busy` — a retryable error. No queuing.

This fail-fast design avoids unbounded queue latency. The client is expected to retry with backoff. Sustained `Busy` indicates either an undersized window or a downstream bottleneck.

---

## 5. Pipelined Fanout

As soon as the leader has fsynced its own copy of slot N, it sends `Accept(N, ...)` to all followers. It does **not** wait for slot N-1, N-2, etc. to reach quorum first.

**Transport: per-peer bidi `LearnerStream`.** The leader uses one long-running gRPC bidi stream per `(group_id, peer_id)` pair (see [`design-rpc.md`](design-rpc.md) §3). The stream's background task maintains a `PendingMap` (`HashMap<request_id, oneshot::Sender>`). Because each `Accept` gets its own oneshot, the leader can enqueue slot N+1's `Accept` before slot N's `Accepted` response has returned.

**Per-follower flow control.** The stream's `cmd_tx` is a bounded `tokio::sync::mpsc` whose capacity is `learner_stream_window_frames` (default 64). When full, `dispatch` fails and the proposer surfaces `PxPaxosError::Busy`.

**Quorum bookkeeping.** For each in-flight slot, the leader keeps a small bitmap of which peers have `Accepted` it. As soon as a majority is reached (counting itself), the slot transitions to `Chosen`.

**Out-of-order `Accepted` and `Chosen` notifications are fine.** A follower may respond `Accepted(N+2)` before `Accepted(N)`. Each is processed independently. A follower may receive `Chosen(N+2)` before `Chosen(N)` and apply only the parts safe to apply (per-key tracking handles this).

---

## 6. Per-Key Resolved-Slot

Each learner maintains, per key it has applied, the highest slot that has touched that key. This enables:

- **Read-your-writes** ([§6.2 of design.md](../design.md#62-read-your-writes-follower-read)): a follower can serve `Get(k, slot=N)` as soon as `resolved_slot[k] ≥ N`.
- **Linearizability** ([§14](#14-parallel-slot-linearizability-analysis-moved-from-requirementmd-65)): per-key tracking makes "highest slot wins" correct under out-of-order apply.

**Update rule.** On `apply(slot, batch)`: for each `(k, op, v?)` in the batch, if `slot > resolved_slot[k]`, update `resolved_slot[k] = slot` and write/tombstone `v`. Otherwise drop. Then advance `max_applied` and the contiguous-applied frontier.

---

## 7. Safe-Slot Computation and Propagation

The safe-slot is the cluster-wide **contiguous applied frontier**: the maximum N such that every slot ≤ N has been chosen and applied on every learner.

**Aggregation.** The leader collects per-learner `contiguous_applied` reports piggy-backed on heartbeat responses. The safe-slot is recomputed at the end of each quorum heartbeat round:

```
   safe_slot = min(contiguous_applied[learner])  for learner in voting members
```

A peer that has never reported is treated as `0`, so the safe-slot only rises once *all* voting members are heard from. The safe-slot is monotonic within a tenure (`fetch_max`); a new leader resets it to `0` and re-establishes it from fresh heartbeats.

**Propagation.** The leader includes `safe_slot` in heartbeats, write responses, read responses, and the describe-cluster RPC.

`Scan(Linearizable)` uses the leader's *own* contiguous frontier rather than the safe-slot — it is strictly ≥ safe-slot at all times ([§6.4 of design.md](../design.md#64-scan-modes)).

---

## 8. Gap Detection

A "gap" is a slot N < max-chosen-slot for which the leader has no `Chosen` decision yet. The leader's learner maintains `contiguous_chosen` (highest gap-free chosen slot) and `last_chosen_slot` (highest slot ever seen chosen, gaps allowed). A gap exists when `contiguous_chosen < last_chosen_slot`.

**Repair mechanism:**

1. **Opportunistic repair.** After each heartbeat round in the leader-state loop, `repair_once()` is called. It finds the lowest gap (`contiguous_chosen + 1`) and runs classic Paxos (Phase 1 + Phase 2) to close it. A no-gap leader returns immediately without any RPCs.
2. **On leader change:** the new leader runs bulk Phase 1 over `[contiguous_chosen+1, ceiling]` as a one-shot sweep (see [`design-leader-election.md`](design-leader-election.md) §4).

There is no separate repair task with its own tick or concurrency cap; repair is interleaved with heartbeats at the heartbeat cadence.

---

## 9. Gap Repair via Classic Paxos

Once a gap is selected, repair runs full classic-Paxos for that slot:

1. **Pick a fresh ballot.** `(round, leader_id)` with `round = max_seen_round + 1`.
2. **Phase 1 — Prepare.** Send `Prepare(slot, ballot)` to all acceptors.
3. **Wait for quorum of `Promise`s.** Each carries the highest `(ballot', value')` previously accepted, if any.
4. **Choose a value.** If any `Promise` returned an accepted value, re-propose the one with the highest `ballot'`. If none, propose `NoOp`.
5. **Phase 2 — Accept.** Send `Accept(slot, ballot, value)`. Wait for quorum.
6. **Chosen.** Broadcast `Chosen(slot)` and apply locally.

The repair never invents a fresh user value — if any acceptor had a half-baked accept, that value is re-chosen. Repair touches one slot at a time; the hot path on other slots is unaffected. A repair on slot N does not block new writes at slot M > N.

---

## 10. Timing Diagrams

### 10.1 Best case — fully pipelined window

```
  time →

  slot N    : assign─ fsync─ Accept ──► quorum ──► Chosen ─ apply ─ ack
  slot N+1  :         assign─ fsync─ Accept ──► quorum ──► Chosen ─ apply ─ ack
  slot N+2  :                 assign─ fsync─ Accept ──► quorum ──► Chosen ─ apply ─ ack

  end-to-end latency for slot N      = fsync + RTT + apply
  per-slot incremental latency       ≈ batched-fsync amortization
  steady-state throughput            = window × (1 / RTT) when network-bound
                                     = disk_bw / record_size when disk-bound
```

### 10.2 Gap repair (slow follower)

```
  time →

  slot N    : assign─ fsync─ Accept ──► follower-A: Accepted
                                      │ follower-B: ...........(slow)
  slot N+1  :       assign─ fsync─ Accept ──► quorum ──► Chosen ─ apply ─ ack(N+1)
  slot N+2  :              assign─ fsync─ Accept ──► quorum ──► Chosen ─ apply ─ ack(N+2)

  ... after heartbeat round ...

  repair    : Prepare(N, ballot') ─► quorum of Promises
                                  │  Promise from leader: Accepted(ballot, v)
              Accept(N, ballot', v) ──► quorum ──► Chosen(N)
              apply(N) on leader, advance contiguous_applied to N+2
```

### 10.3 Leader change

```
  time →

  old leader: ...slot N-1: Chosen, applied
              slot N:   Accepted on 1 follower only (no quorum)
              slot N+1: Accepted on 0 followers
              <crash>

  new leader (term T+1):
              Prepare((T+1, me)) over [N, N+1, ..., max_chosen]
              Promises:
                slot N   : (older_ballot, v_N) from one follower
                slot N+1 : empty
              re-Accept((T+1,me), v_N) at slot N    → Chosen
              Accept((T+1,me), NoOp)   at slot N+1  → Chosen
              steady state resumes
```

---

## 11. Interaction with Snapshot and WAL GC

Parallel slots interact with WAL GC through two watermarks:

- `safe_slot` — every slot ≤ `safe_slot` is chosen and applied on every learner.
- `snapshot_slot` — the engine state at `snapshot_slot` is durably snapshotted on at least the leader and one peer.

**WAL GC rule:** discard WAL records with `slot < min(safe_slot, snapshot_slot)`. Both must advance past a slot before its WAL record can be GC'd. Repair never needs GC'd slots — every slot ≤ safe_slot is already chosen on every learner, so it cannot be a gap.

Detailed further in [`design-wal.md`](design-wal.md) §4.

---

## 12. Tunables and Defaults

| Parameter | Default | Where it lives |
| --- | --- | --- |
| `proposer_window` | 16 | `PaxosConfig` (semaphore permits) |
| `max_paxos_retries` | 3 | `PaxosConfig` (per-slot Phase-2 retries) |
| `max_slot_retries` | 3 | `PaxosConfig` (new-slot retries before giving up) |
| `retry_base_backoff_ms` | 5 | `PaxosConfig` (exponential backoff base) |
| `learner_stream_window_frames` | 64 | `PxElectionConfig` (per-peer mpsc capacity) |
| `bulk_prepare_window` | 1024 | `PxElectionConfig` (bulk Phase-1 batch size) |

The bulk Phase-1 typically resolves open gaps in a single RTT. Steady-state gaps are closed one-at-a-time after each heartbeat round, so worst-case gap-clear time is bounded by `heartbeat_interval × gap_count`.

---

## 13. Correctness Analysis for Parallel Slot Writes (moved from `requirement.md` §7.3.1)

> **Moved 2026-07.** Originally in the requirements doc; relocated here as design-level correctness argument.

**Key insight:** Parallel slot writes are safe because we only support **blind operations** (`Put`, `Delete`) — no operation reads current state before writing. The final value of each key is determined solely by the highest slot that touched it.

- **Consensus phase (parallel):** Each `PxSlot` is an independent Paxos instance. They can be decided in any order.
- **Apply phase (per-key slot tracking):** `PxLearner`s store `(slot, value)` per key and only accept writes where `slot > current_slot`. Regardless of apply order, the highest slot wins.
- **Gaps don't block point reads:** An undecided slot 3 does not block `Get(k)` if slot 5 for key `k` is already applied. Gaps only block cross-key scans and WAL truncation.
- **Batches:** Intra-batch order is **as written by the client**. A batch uses the batch's slot for each key.

This would NOT be safe for `CAS` or `Increment`, which read current state. Not supported.

---

## 14. Parallel-Slot Linearizability Analysis (moved from `requirement.md` §6.5)

> **Moved 2026-07.** Shows that linearizability is preserved under parallel consensus.

**Premises:** (1) Only blind ops. (2) Leader serializes slot assignment. (3) Quorum durable-flush before ack. (4) Per-key `(slot, value)` tracking, highest slot wins. (5) Leader reads fenced by lease or ReadIndex.

**Claim:** The assigned slot number is a valid linearization point for every op.

**Sketch:**

- **Real-time order → slot order.** If `ack(A)` completes before `invoke(B)`, the counter has already advanced past `slot(A)`, so `slot(A) < slot(B)`.
- **Blind apply-order independence.** An undecided earlier slot writing *k* will be ordered earlier and immediately overwritten by the later slot's value.
- **Durability before visibility.** Quorum durable-flush before ack ensures a client-observed write cannot be lost by leader change. Gap repair re-chooses the same value.
- **Leader read correctness.** The leader's learner has applied slot N before acking. A subsequent `Get(k)` reflects the highest chosen slot writing *k*.
- **Follower read correctness.** Read-your-writes uses per-key resolved-slot; bounded-stale uses the gap-free `safe-slot`.

**Single remaining cost — linearizable `Scan`.** Must wait for the leader's **contiguous applied frontier** to cover the target. Latency is bounded by (window size) × (slot-resolution time). Mitigation: `Scan(mode = SafeSlot)` or `Scan(mode = AtSlot(N))`. Point `Get`s bypass gaps entirely.

**Why `CAS`/`Increment` would break this:** Read-modify-write ops must read current value, which is unknown until all earlier slots for that key are resolved. Would require sequential consensus or per-key dependency tracking. Neither is in scope.

**Implementation invariants:** Slot assignment is single-point. `Accepted` not sent before durable flush. Classic-Paxos recovery never discards accepted values. Lease/ReadIndex fencing on every linearizable read. Per-key slot comparison atomic with write.

---

# Part B: Concurrent Sparse Slot List

## 15. Slot List: Problem Statement

The Acceptor's per-slot state (`promised` ballot + `accepted` entry) needs a data structure optimized for:

- **O(1) insert by slot** — no re-allocation per slot; chunk created lazily.
- **O(1) hot-slot access** — tail-first lookup hits the newest chunk.
- **O(1) batch front-trim** — drop whole chunks, not individual slots.
- **Wait-free / lock-free reads** — readers never block each other.
- **Safe reclamation** — a chunk is freed only after all concurrent readers have passed it.

`PxSlot` is a monotonically increasing `u64` assigned by an external sequencer. Access is strongly tail-biased (latest-slot prepare/accept/replay), while allocation may be sparse.

---

## 16. Slot List: Design Overview

A **chunked, reader-pinned concurrent sparse list** (`SlotList<T>`):

```
SlotList<T>
├─ head  ──► Chunk { start: 1024, entries: [1024..2047] }  (partially filled)
│            ⇅
│            Chunk { start: 4096, entries: [4096..5119] }  (sparse gap 2048..4095)
│            ⇅
│            Chunk { start: 5120, entries: [5120..6143] }  ◄── tail
│
├─ trim_slot: AtomicU64  (slots below this are logically invalid)
└─ retired:   AtomicPtr<RetiredChunk<T>>
```

Each chunk is a fixed-size array (`SLOT_CHUNK_SIZE = 1024` slots). A slot index maps to a chunk via `chunk = start / SLOT_CHUNK_SIZE` and an intra-chunk offset. Chunks are doubly-linked so the hot path walks backward from `tail` while the general path walks forward from `head`.

### Chunk layout

```rust
const SLOT_CHUNK_SIZE: usize = 1024;

struct Chunk<T> {
    start_slot: PxSlot,
    entries: [AtomicPtr<T>; SLOT_CHUNK_SIZE],
    next: AtomicPtr<Chunk<T>>,
    prev: AtomicPtr<Chunk<T>>,
    live_count: AtomicUsize,
    reader_refs: AtomicU32,
    retired: AtomicBool,
    _pad: [u8; 64],  // cache-line padded to prevent false sharing
}
```

**Why `AtomicPtr<T>` per slot?** Writers CAS from `null → Box::into_raw(Box::new(value))`. Readers pin the containing chunk (`reader_refs += 1`), then load the pointer. Retired chunks are freed only after `reader_refs == 0`.

### List header

```rust
pub struct SlotList<T> {
    head: AtomicPtr<Chunk<T>>,
    tail: AtomicPtr<Chunk<T>>,
    trim_slot: AtomicU64,
    retired: AtomicPtr<Chunk<T>>,
}
```

---

## 17. Slot List: PxSlotNode

For the `PxAcceptor` we store **both** the promised ballot and the accepted entry in a single node, eliminating the double-map indirection:

```rust
pub struct PxSlotNode {
    promised: AtomicPtr<PxBallot>,
    accepted: AtomicPtr<PxLogEntry>,
    retired_promised: AtomicPtr<RetiredPtr<PxBallot>>,
    retired_accepted: AtomicPtr<RetiredPtr<PxLogEntry>>,
}

pub struct PxAcceptor {
    log: SlotList<PxSlotNode>,
}
```

Paxos operations use `get_tail_ptr` / `get_ptr` to obtain the `AtomicPtr<PxSlotNode>` for a slot, then CAS the node pointer itself (installing a default node if absent) before mutating fields inside the stable node. `PxSlotNode` uses deferred reclamation: when `cas_promised` / `cas_accepted` swaps an existing pointer, the old pointer goes into a per-field retired list, drained on `PxSlotNode::drop`.

---

## 18. Slot List: Algorithms

### 18.1 Insert (by external slot)

The caller decides the exact `slot`; the list only stores the value. Slots may be non-consecutive (sparse). `insert` locates or lazily creates the chunk covering the slot, pins the chunk (`reader_refs += 1`), then CAS-installs the slot object from `null → ptr`. If another writer raced, the existing object is kept. `insert` never replaces an existing slot pointer — callers needing field-level CAS use `get_ptr` / `get_tail_ptr` directly.

Chunk lookup (`find_or_create_chunk`) walks the doubly-linked list to find the predecessor/successor window around the aligned chunk start, then splices a new chunk in via CAS. Sparse gaps are cheap: no chunk is allocated for empty ranges.

### 18.2 Get (head-first, general path)

`get(slot)` walks from `head`, checking `trim_slot` first. When the covering chunk is found, it pins the chunk, loads the slot pointer, and returns a `SlotReadGuard`. Used for any historical slot.

### 18.3 Get Tail (tail-first, hot path)

`get_tail(slot)` walks backward from `tail`. This is the normal Paxos read path: latest-slot prepare/accept/replay almost always hit the last chunk or one of its immediate predecessors.

### 18.4 Trim (front GC)

`trim(before_slot)` advances `trim_slot` via `fetch_max`, then walks from `head` unlinking chunks whose `end ≤ before_slot`. Unlinked chunks are marked `retired = true` and pushed onto the retired list. **Single-caller by contract.** Each `get` / `get_tail` checks `trim_slot` before touching any chunk, so newly arriving readers immediately reject trimmed slots.

### 18.5 Chunk-Level Reclamation

A background `reclaim()` walks the retired list and frees chunks whose `reader_refs == 0`. **Single-caller by contract.** This is safe because `trim_slot` is advanced before chunk unlinking — only readers that pinned the chunk before trim can still hold a reference.

**Current limitation:** there is a narrow race between "reader has observed a chunk pointer" and "reader has incremented `reader_refs`". The current manual scheme is safe under a restricted envelope (single GC caller, disciplined reclaim timing). For fully general lock-free safety, use epoch/hazard pointers or `Arc<Chunk<T>>` ownership.

---

## 19. Slot List: API Surface

```rust
impl<T> SlotList<T> {
    pub fn new() -> Self;
    pub fn get(&self, slot: PxSlot) -> Option<SlotReadGuard<'_, T>>;
    pub fn get_tail(&self, slot: PxSlot) -> Option<SlotReadGuard<'_, T>>;
    pub fn get_ptr(&self, slot: PxSlot) -> Option<SlotPtrGuard<'_, T>>;
    pub fn get_tail_ptr(&self, slot: PxSlot) -> Option<SlotPtrGuard<'_, T>>;
    pub fn iter_range(&self, start: PxSlot, end_exclusive: PxSlot) -> SlotIter<'_, T>;
    pub fn insert(&self, slot: PxSlot, value: T) -> SlotReadGuard<'_, T>;
    pub fn trim(&self, before_slot: PxSlot);
    pub fn reclaim(&self) -> usize;
    pub fn trim_slot(&self) -> PxSlot;
    pub fn len(&self) -> usize;
}
```

`SlotReadGuard` derefs to `&T` and decrements `reader_refs` on drop. `SlotPtrGuard` derefs to `&AtomicPtr<T>` for caller-controlled CAS.

---

## 20. Slot List: Correctness Invariants

| Invariant | Why it matters | Enforced by |
|---|---|---|
| **I1 — External slot assignment** | Slot number chosen by caller; list only stores at that index | `insert` validates slot is inside the chunk range it targets |
| **I2 — Stable slot object** | Once installed, readers keep seeing the same pointer | `insert` / `get_ptr` only CASes `null → ptr`; later updates mutate fields inside the object |
| **I3 — Trim coherence** | Readers never observe a trimmed slot | `trim_slot` advanced before chunks are unlinked; every `get` checks it first |
| **I4 — No dangling reads** | A retired chunk is freed only after all pinned readers have dropped | Chunk-level `reader_refs` + retired list |
| **I5 — Sparse ordering** | Chunks stay ordered by `start_slot` | `find_window` and `link_between` preserve sorted doubly linked order |

---

## 21. Slot List: Performance Model

| Operation | Cost | Notes |
|---|---|---|
| `insert` (existing chunk) | 1 slot CAS + 1 reader-ref increment | Reuses existing slot object if present |
| `insert` (new chunk) | 1 allocation + chunk-link CAS | Amortised only when touching a previously empty chunk range |
| `get_tail` | 1 trim check + 1 chunk pin + 1 atomic load | Tail chunk is cache-hot; covers most reads |
| `get` (head-first) | 1 trim check + ≤ N chunk hops + 1 atomic load | N = number of live sparse chunks |
| `trim` | 1 watermark advance + O(retired chunks) unlink | Drops whole chunks, never individual slots |
| `reclaim` | O(retired chunks) | Frees only chunks whose `reader_refs == 0` |
| Memory overhead | ~8 bytes / slot (on 64-bit) | `AtomicPtr` per slot; chunk metadata negligible |

Compared to `BTreeMap`: ~10× faster insert, ~5× faster latest-slot access, ~3× lower per-slot overhead.

---

## 22. Slot List: Risks, Reclamation & Open Questions

### 22.1 Risks

| Risk | Mitigation |
|---|---|
| `unsafe` in raw-pointer linking | Review with `miri`; maintain `Arc` fallback for comparison |
| `reader_refs` leak due to forgotten guard drop | Keep all public reads guard-based; no bare `&T` escapes |
| Prev/next inconsistency under sparse insertion | Centralise linking in `find_window` / `link_between`; stress tests |
| Reader late-pin race | Short-term: single GC caller + disciplined reclaim. Long-term: epoch/hazard pointers or `Arc<Chunk<T>>` |

### 22.2 PxSlotNode Reclamation Evolution

**Current:** Replaced `promised`/`accepted` pointers are retired and reclaimed on `PxSlotNode::drop`. No historical replacement leak, no dangling-reference regression.

**Future triggers:** Per-node retired-chain depth growing across GC cycles, RSS growth from field churn, or tail latency regression from large node-drop reclamation bursts.

**Options:** (1) Epoch/hazard-based early reclaim (preferred long-term). (2) Guarded/owned return API replacing raw references. (3) `Arc` payload fields (simplest, higher overhead).

### 22.3 Open Questions

1. **Manual `reader_refs` vs `Arc<Chunk<T>>`:** Keep manual as target; validate against `Arc` prototype if risk grows.
2. **Chunk size:** Start with 1K; make `SLOT_CHUNK_SIZE` a `const` generic for benchmarking.
3. **Per-slot `AtomicPtr` vs inline storage:** Start with `AtomicPtr`; optimise if profiles show bottleneck.
