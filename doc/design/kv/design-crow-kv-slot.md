<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROW - Design: Slots — Parallel Pipelining & Concurrent Slot List

Depends on: [`design-crow-kv.md`](design-crow-kv.md) §5.1, §6.5, §7.3
Satisfies: [`design-crow-kv.md`](design-crow-kv.md) §5.1, §6.5, §7.3

This document covers two aspects of CROW's slot mechanism:

- **Part A (§1–§14): Parallel Slot Pipelining** — the consensus protocol that distinguishes CROW from Raft-based KV systems. How the leader pipelines proposals, how gaps are detected and repaired, how the safe-slot is maintained, and how the system stays correct under concurrent in-flight slots.
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
- [9A. Follower-Side Apply and Catch-up](#9a-follower-side-apply-and-catch-up)
- [10. Timing Diagrams](#10-timing-diagrams)
- [11. Interaction with Snapshot and WAL GC](#11-interaction-with-snapshot-and-wal-gc)
- [12. Tunables and Defaults](#12-tunables-and-defaults)
- [13. Correctness Analysis for Parallel Slot Writes](#13-correctness-analysis-for-parallel-slot-writes)
- [14. Parallel-Slot Linearizability Analysis](#14-parallel-slot-linearizability-analysis)
- [15. Slot List: Problem Statement](#15-slot-list-problem-statement)
- [16. Slot List: Design Overview](#16-slot-list-design-overview)
- [17. Slot List: PxSlotNode](#17-slot-list-pxslotnode)
- [18. Slot List: Algorithms](#18-slot-list-algorithms)
- [19. Slot List: API Surface](#19-slot-list-api-surface)
- [20. Slot List: Correctness Invariants](#20-slot-list-correctness-invariants)
- [21. Slot List: Performance Model](#21-slot-list-performance-model)
- [22. Slot List: Risks, Reclamation & Open Questions](#22-slot-list-risks-reclamation--open-questions)
- [23. Server-side Proposal Coalescing](#23-server-side-proposal-coalescing)

---

# Part A: Parallel Slot Pipelining

## 1. Why Parallel Slots

A Raft leader cannot acknowledge slot N+1 until slot N has been committed; the log is contiguous, so a single slow follower stalls the entire commit pipeline (head-of-line blocking). Multi-Paxos has no such constraint: each slot is a separate Paxos instance, and a quorum that decides slot N+1 need not include the same nodes that decide slot N.

CROW exploits this by running many slots in parallel on the leader. Throughput is bounded by network and disk bandwidth, not by per-slot serialized round-trips. The price is two-fold:

- **Gaps.** A slot may remain undecided long after later slots are decided. We need a mechanism to resolve gaps without stalling the hot path.
- **Conservative cross-key reads.** A `Scan` must wait for a no-gap prefix; point reads do not.

The blind-ops premise from design-crow-kv.md §5.2](design-crow-kv.md) makes the trade-off cheap: out-of-order *apply* is safe because no operation reads before writing.

---

## 2. Concepts and Invariants

- **I1 — Single slot counter.** On a leader, slot assignment is performed by exactly one logical worker. Two writes never receive the same slot, and no two slots are assigned out of arrival order.
- **I2 — Slot determines linearization.** The slot number assigned to an op is the op's position in the global linearization order (design-crow-kv.md §6.1](design-crow-kv.md)).
- **I3 — Quorum-fsync before ack.** A client write is acked only after a quorum of acceptors (including the leader) have fsynced their `Accepted(slot, ballot, value)` records. This is the durability hook that makes I2 robust to failures.
- **I4 — Apply-order independence for blind ops.** For any key *k*, the engine's final value is `value(max{ slot | slot writes k })`. Apply order between non-overlapping keys is irrelevant; for the same key, the higher slot wins regardless of arrival order ([§13](#13-correctness-analysis-for-parallel-slot-writes)).
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

**When does the ack happen?** When the slot reaches `Chosen` *and* the leader's own learner has applied it. The leader's own learner application is required so that an immediately-following `Get` on the leader sees the new value (see [§6.1 of design-crow-kv.md](design-crow-kv.md#61-linearizable-leader-read)).

**Eviction.** Once a slot's record is `Applied` *and* its slot number is below the safe-slot, it can be dropped from the in-memory map. The WAL record stays until WAL GC catches up.

---

## 4. Sliding Window and Backpressure

The number of in-flight slots is capped at the **window size** (`max_inflight_proposals`, default 32). The proposer uses an `InflightAdmission` gate backed by one or more `Semaphore`s. If a permit is available, the request is admitted, slot is assigned, and proposing begins. The permit is held for the entire proposal duration.

**Admission policy** (`AdmissionPolicy`, internal config, not exposed via CLI):

- **`Queue` (default)** — If no permit is available, the caller blocks on `acquire().await` until a permit is freed. This eliminates client-side reject-retry storms under contention (e.g., window=1 with 16 writers). Wait time and queue depth are tracked as metrics (`inflight_total_enqueued`, `inflight_total_wait_us`, `inflight_queue_depth`).
- **`Reject`** — If no permit is available (`try_acquire` fails), the leader immediately returns `Busy` — a retryable error. No queuing. Used only in tests that need to verify fail-fast behavior.

**Multi-queue routing** (`--inflight-queues`, default 1): The window is split across N semaphores, each sized `ceil(max_inflight / N)`. Proposals are routed round-robin. Multiple queues reduce semaphore contention under high concurrency without affecting correctness (each slot is an independent Paxos instance; admission ordering does not influence consensus safety).

---

## 5. Pipelined Fanout

As soon as the leader has fsynced its own copy of slot N, it sends `Accept(N, ...)` to all followers. It does **not** wait for slot N-1, N-2 to reach quorum first.

**Transport: per-peer bidi `LearnerStream`.** The leader uses one long-running crow-rpc bidi stream per `(group_id, peer_id)` pair (see [`design-crow-kv-rpc.md`](design-crow-kv-rpc.md) §3). The stream's background task maintains a `PendingMap` (`HashMap<request_id, oneshot::Sender>`). Because each `Accept` gets its own oneshot, the leader can enqueue slot N+1's `Accept` before slot N's `Accepted` response has returned.

**Per-follower flow control.** The stream's `cmd_tx` is a bounded `tokio::sync::mpsc` whose capacity is `learner_stream_window_frames` (default 64). When full, `dispatch` fails and the proposer surfaces `PxPaxosError::Busy`.

**Quorum bookkeeping.** For each in-flight slot, the leader keeps a small bitmap of which peers have `Accepted` it. As soon as a majority is reached (counting itself), the slot transitions to `Chosen`.

**Out-of-order `Accepted` and `Chosen` notifications are fine.** A follower may respond `Accepted(N+2)` before `Accepted(N)`. Each is processed independently. A follower may receive `Chosen(N+2)` before `Chosen(N)` and apply only the parts safe to apply (per-key tracking handles this).

**Zero-copy accept path.** `Acceptor::accept` and `ReplicaHandler::on_accept` take `&PxLogEntry` instead of owned `PxLogEntry`. The caller (proposer's `run_accept_phase`, crow-rpc `handle_accept_inner`, WAL replay) passes a reference; no clone for the acceptor call. The only clone is inside `inner_accept` for `cas_accepted`, where the slot node must own its copy. `WALRecord::from_accepted` already borrows, so the WAL encode after accept is also zero-copy. With `Bytes` payloads these clones were O(1) ref-count bumps; the signature change makes the zero-copy intent explicit and avoids redundant bumps.

---

## 6. Per-Key Resolved-Slot

Each learner maintains, per key it has applied, the highest slot that has touched that key. This enables:

- **Read-your-writes** ([§6.2 of design-crow-kv.md](design-crow-kv.md#62-read-your-writes-follower-read)): a follower can serve `Get(k, slot=N)` as soon as `resolved_slot[k] ≥ N`.
- **Linearizability** ([§14](#14-parallel-slot-linearizability-analysis)): per-key tracking makes "highest slot wins" correct under out-of-order apply.

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

`Scan(Linearizable)` uses the leader's *own* contiguous frontier rather than the safe-slot. It is strictly ≥ safe-slot at all times ([§6.4 of design-crow-kv.md](design-crow-kv.md#64-scan-modes)).

---

## 8. Gap Detection

A "gap" is a slot N < max-chosen-slot for which the leader has no `Chosen` decision yet. The leader's learner maintains `contiguous_chosen` (highest gap-free chosen slot) and `last_chosen_slot` (highest slot ever seen chosen, gaps allowed). A gap exists when `contiguous_chosen < last_chosen_slot`.

**Repair mechanism:**

1. **Opportunistic repair.** After each heartbeat round in the leader-state loop, `repair_once()` is called. It finds the lowest gap (`contiguous_chosen + 1`) and runs classic Paxos (Phase 1 + Phase 2) to close it. A no-gap leader returns immediately without any RPCs.
2. **On leader change:** the new leader runs bulk Phase 1 over `[contiguous_chosen+1, ceiling]` as a one-shot sweep (see [`design-crow-kv-leader-election.md`](design-crow-kv-leader-election.md) §4).

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

## 9A. Follower-Side Apply and Catch-up

The leader-side repair in §9 closes gaps in the leader's *chosen* frontier. Followers face a symmetric but distinct problem: they learn about chosen slots via `ChosenNotice` (a fire-and-forget notification the leader broadcasts after quorum), and they may miss or receive stale versions of those slots. This section covers the follower-side apply path and catch-up mechanism.

### 9A.1 Accept Path Stores Only

A follower's `handle_accept` stores the accepted value in its acceptor and persists it to the WAL, but does **not** advance `known_commit_slot` or wake the apply loop. Accept is a storage event, not a commit event. The leader may crash before reaching quorum, in which case the accepted value may never be chosen (or may be overwritten by a higher-ballot value during new-leader recovery).

Advancing the apply frontier on Accept would let a follower apply an un-chosen value, violating Paxos safety, the same class of bug as applying before `leaderCommit` in Raft.

The one exception is the new-leader transition: after a follower wins election and completes bulk Phase 1 over `[contiguous_chosen+1, ceiling]`, it advances `known_commit_slot` to its new `contiguous_chosen` and wakes the apply loop. This is safe because bulk Phase 1 has resolved (chosen or NoOp-filled) every slot in that range under the new leader's ballot.

### 9A.2 ChosenNotice — Ballot-Verified Out-of-Order Apply

When the leader chooses a slot, it broadcasts a `ChosenNotice` carrying `(slot, term, leader_id, ballot_round)` to all followers. The follower's ChosenNotice handler:

- If the follower's acceptor has an accepted value at `slot` with `ballot == chosen_ballot` → the value matches what was chosen. The follower calls `update_chosen_frontier` and wakes the apply loop. Apply can proceed out-of-order; the per-key slot tracking in §3 makes this safe (highest slot wins per key).
- If the follower's accepted ballot at `slot` is **lower** than the chosen ballot (stale) → the follower has an outdated value. It records a gap for `slot` (driven by FetchGap, §9A.3) and does not apply.
- If the follower has no accepted value at `slot` → it records a gap and does not apply.

The `ballot_round` field in the ChosenNotice flatbuffer enables this verification. Without it, the follower cannot distinguish a fresh chosen value from a stale re-delivery.

### 9A.3 Follower-Driven FetchGap Catch-up

When a follower detects a gap (missing or stale slot in the chosen range), it sends a `FetchGap(slot)` request to the leader via the LearnerStream. The leader:

- Has the chosen value locally → replies with the full entry (payload + ballot + term).
- Does not have it → runs classic Paxos (`repair_once`, §9) to resolve the slot, then replies with the resolved value (or NoOp).

On receipt, the follower overwrites any stale accepted value with the chosen value, updates its chosen frontier, and wakes the apply loop. FetchGap is bounded by `MAX_INFLIGHT_FETCHGAP` (default 16) to avoid flooding the leader.

This replaces the previous leader-driven catch-up that ran inline in `run_heartbeat_round` (up to 64 synchronous `send_accept().await` calls per lagging follower). That inline catch-up could exceed `heartbeat_interval` and trigger spurious elections. The heartbeat round is now pure liveness + lease: one RPC round-trip, no catch-up work.

### 9A.4 Snapshot Fallback

If a follower's gap count exceeds `catchup_snapshot_threshold` (default `bulk_prepare_window` = 1024), the follower stops issuing FetchGap requests and logs a warning. The full snapshot-install path for running replicas is deferred; the threshold gate prevents FetchGap storms against the leader when a follower is severely lagging (e.g. after a long network partition).

### 9A.5 Apply Loop

The follower's apply loop targets `max(known_commit_slot, last_chosen_slot)`. For each slot in the apply range:

- If `slot ≤ known_commit_slot` → apply (committed by definition).
- Else if `learner.is_chosen(slot)` → apply (out-of-order chosen slot).
- Else (accepted but not chosen) → skip and record a gap for FetchGap.

This ensures accepted-but-not-chosen slots are never applied, while allowing out-of-order chosen slots to apply immediately without waiting for lower slots to be resolved.

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

**WAL GC rule:** discard WAL records with `slot < min(safe_slot, snapshot_slot)`. Both must advance past a slot before its WAL record can be GC'd. Repair never needs GC'd slots; every slot ≤ safe_slot is already chosen on every learner, so it cannot be a gap.

Detailed further in [`design-crow-kv-wal.md`](design-crow-kv-wal.md) §4.

---

## 12. Tunables and Defaults

| Parameter | Default | Where it lives |
| --- | --- | --- |
| `max_inflight_proposals` | 32 | `PaxosConfig` (total semaphore permits) |
| `inflight_admission` | `Queue` | `PaxosConfig` (internal, not CLI-exposed) |
| `max_paxos_retries` | 3 | `PaxosConfig` (per-slot Phase-2 retries) |
| `max_slot_retries` | 3 | `PaxosConfig` (new-slot retries before giving up) |
| `retry_base_backoff_ms` | 5 | `PaxosConfig` (exponential backoff base) |
| `learner_stream_window_frames` | 64 | `PxElectionConfig` (per-peer mpsc capacity) |
| `bulk_prepare_window` | 1024 | `PxElectionConfig` (bulk Phase-1 batch size) |
| `catchup_snapshot_threshold` | 1024 | `PxElectionConfig` (gap count above which FetchGap is skipped in favor of snapshot fallback) |

The bulk Phase-1 typically resolves open gaps in a single RTT. Steady-state gaps are closed one-at-a-time after each heartbeat round, so worst-case gap-clear time is bounded by `heartbeat_interval × gap_count`. Follower-side gaps (missing or stale chosen slots) are closed by FetchGap at the follower's own pace, bounded by `MAX_INFLIGHT_FETCHGAP` (default 16) concurrent requests.

---

## 13. Correctness Analysis for Parallel Slot Writes

**Key insight:** Parallel slot writes are safe because we only support **blind operations** (`Put`, `Delete`). No operation reads current state before writing. The final value of each key is determined solely by the highest slot that touched it.

- **Consensus phase (parallel):** Each `PxSlot` is an independent Paxos instance. They can be decided in any order.
- **Apply phase (per-key slot tracking):** `PxLearner`s store `(slot, value)` per key and only accept writes where `slot > current_slot`. Regardless of apply order, the highest slot wins.
- **Gaps don't block point reads:** An undecided slot 3 does not block `Get(k)` if slot 5 for key `k` is already applied. Gaps only block cross-key scans and WAL truncation.
- **Batches:** Intra-batch order is **as written by the client**. A batch uses the batch's slot for each key.

This would NOT be safe for `CAS` or `Increment`, which read current state. Not supported.

---

## 14. Parallel-Slot Linearizability Analysis

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

The caller decides the exact `slot`; the list only stores the value. Slots may be non-consecutive (sparse). `insert` locates or lazily creates the chunk covering the slot, pins the chunk (`reader_refs += 1`), then CAS-installs the slot object from `null → ptr`. If another writer raced, the existing object is kept. `insert` never replaces an existing slot pointer; callers needing field-level CAS use `get_ptr` / `get_tail_ptr` directly.

Chunk lookup (`find_or_create_chunk`) walks the doubly-linked list to find the predecessor/successor window around the aligned chunk start, then splices a new chunk in via CAS. Sparse gaps are cheap: no chunk is allocated for empty ranges.

### 18.2 Get (head-first, general path)

`get(slot)` walks from `head`, checking `trim_slot` first. When the covering chunk is found, it pins the chunk, loads the slot pointer, and returns a `SlotReadGuard`. Used for any already-known slot.

### 18.3 Get Tail (tail-first, hot path)

`get_tail(slot)` walks backward from `tail`. This is the normal Paxos read path: latest-slot prepare/accept/replay almost always hit the last chunk or one of its immediate predecessors.

### 18.4 Trim (front GC)

`trim(before_slot)` advances `trim_slot` via `fetch_max`, then walks from `head` unlinking chunks whose `end ≤ before_slot`. Unlinked chunks are marked `retired = true` and pushed onto the retired list. **Single-caller by contract.** Each `get` / `get_tail` checks `trim_slot` before touching any chunk, so newly arriving readers immediately reject trimmed slots.

### 18.5 Chunk-Level Reclamation

A background `reclaim()` walks the retired list and frees chunks whose `reader_refs == 0`. **Single-caller by contract.** This is safe because `trim_slot` is advanced before chunk unlinking; only readers that pinned the chunk before trim can still hold a reference.

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

**Current:** Replaced `promised`/`accepted` pointers are retired and reclaimed on `PxSlotNode::drop`. No replacement leak, no dangling-reference regression.

**Future triggers:** Per-node retired-chain depth growing across GC cycles, RSS growth from field churn, or tail latency regression from large node-drop reclamation bursts.

**Options:** (1) Epoch/hazard-based early reclaim (preferred long-term). (2) Guarded/owned return API replacing raw references. (3) `Arc` payload fields (simplest, higher overhead).

### 22.3 Open Questions

1. **Manual `reader_refs` vs `Arc<Chunk<T>>`:** Keep manual as target; validate against `Arc` prototype if risk grows.
2. **Chunk size:** Start with 1K; make `SLOT_CHUNK_SIZE` a `const` generic for benchmarking.
3. **Per-slot `AtomicPtr` vs inline storage:** Start with `AtomicPtr`; optimise if profiles show bottleneck.

## 23. Server-side Proposal Coalescing

### 23.1 Problem

Each client `PUT`/`DELETE` is its own Paxos proposal: one slot, one
quorum RPC round, one WAL record, one learner apply. WAL batch
aggregation already amortizes `fsync` across concurrent proposals, and
the leader's local `fsync` is off the critical path, so the
remaining per-proposal fixed cost is the **quorum RPC round**. The
write-flow sweep shows throughput plateaus at ~29K (Intel) / ~48K
(M5 Pro) once the consensus pipeline saturates, independent of the
inflight window above MI=16. The bottleneck is the per-proposal quorum
RPC rate, not `fsync`.

The `Batch` payload format already supports multiple ops per slot and
`kv_batch_write` exposes it, but there was no server-side coalescer:
concurrent single-key proposes each took a distinct slot and paid the
full quorum round.

### 23.2 Drain Threshold

The first op starts a 1-op slot-task **immediately** (no timer). Ops
arriving during the round join a pending batch. On round completion,
`coalesce_drain_after_round` checks the in-flight slot-task count
(`occupied`, permits held) before taking the pending batch. If
`occupied >= coalesce_drain_threshold`, the drain is skipped; the
`max_keys` overflow path handles high load with full batches. If
`occupied < coalesce_drain_threshold`, the pending batch is taken and
the next slot-task is started (if non-empty) or the coalescer goes
idle (if empty).

At high load, drains almost never fire (many slot-tasks in flight);
the `max_keys` overflow path produces full batches. At low load,
drains always fire (few slot-tasks, threshold not exceeded), so there
is no latency floor; a lone op flushes immediately. CLI default
`coalesce_drain_threshold = max_inflight / 4` (see §23.5).

### 23.3 Coalescer Design

Per-group state on `PxGroup`:

- `coalescer: parking_lot::Mutex<Option<PendingBatch>>` — the
  accumulating batch, `None` when idle.
- `self_weak: OnceLock<Weak<PxGroup>>` — set once the group is wrapped
  in `Arc`, so spawned slot-tasks can upgrade without holding a strong
  self-reference.
- `coalesce_last_activity_us: AtomicU64` — last coalescer activity
  timestamp (updated on every enqueue and drain). Used by the
  activity-based watchdog.

`PendingBatch` holds:
- `op_bodies: Vec<u8>` — concatenated op bodies (each single-op
  payload's leading count bytes dropped; op bodies are self-delimited).
- `op_count: u16` — number of ops accumulated.
- `tags: Vec<DedupTag>` — one `(client_id, seq)` dedup tag per client
  op, all mapping to the shared slot.
- `waiters: Vec<oneshot::Sender<ProposeResult>>` — one per coalesced
  caller; each receives the shared `ProposeResult` on flush.
- `timer: Option<JoinHandle<()>>` — reserved (unused in event mode;
  the watchdog is activity-based, not per-batch).

`propose` flow (refactored into `propose` + `propose_inner`):

1. Leadership gate (as before).
2. Dedup lookup (as before) — a hit returns the cached slot
   immediately, never enters a batch.
3. If `coalesce_max_keys == 0` (disabled) or `self_weak` is unset:
   call `propose_inner(payload, &[tag])` directly; bit-identical to
   the fallback path.
4. Else (coalescing on): `coalesce_enqueue(payload, tag)`:
   - Coalescer is `None` (idle) → create a batch with this op, start a
     1-op slot-task **immediately**. Open a fresh empty batch for ops
     arriving during this round.
   - Coalescer is `Some` (batch exists) → append op body + tag +
     waiter. If `op_count >= max_keys` → overflow: take the batch,
     spawn a concurrent slot-task, open a new empty batch.
5. `await` the waiter's `ProposeResult`.

`coalesce_flush_batch` (called by overflow and drain):

1. Build merged payload: `[op_count as u16 LE][op_bodies]`.
2. Open a fresh empty batch in `coalescer` (for ops during the next
   round).
3. `tokio::spawn` `propose_inner(payload, tags)`. On completion, fan
   the `ProposeResult` (now `Clone`) to all waiters, then call
   `coalesce_drain_after_round`.

`coalesce_drain_after_round` (called after each slot-task finishes):

1. Touch activity timestamp.
2. If `occupied >= coalesce_drain_threshold` → return (skip drain;
   overflow handles it).
3. Take the pending batch from `coalescer`.
4. If `op_count == 0` → go idle (return).
5. If `op_count > 0` → `coalesce_flush_batch(batch)` (start next round).

**Watchdog**: A single background task per group sleeps 1000ms, then
checks if there's been no coalescer activity for 1000ms. If so, it
flushes any stuck non-empty batch. Safety net for edge cases (drain
panic, spawn failure). Zero overhead during normal operation.

### 23.4 Dedup Tag Threading

A coalesced batch carries K `(client_id, seq)` tags but one slot. To
preserve the existing dedup-on-all-replicas invariant (so a follower
that becomes leader can return cached slots for retried coalesced ops),
all K tags must reach every replica that accepts the batch.

The `Accept` RPC flatbuffer is extended with a repeated `dedup_tags` field:

```fbs
message DedupTag { uint64 client_id = 1; uint64 seq = 2; }
// in AcceptRequest:
repeated DedupTag dedup_tags = 13;
```

The `client_id`/`seq` fields (9/10) are kept populated with the
first tag (or 0) for backward-compat with older followers during a
rolling upgrade. New followers prefer `dedup_tags`; older followers
fall back to the single tag.

The `Learner::learn` trait signature changed from `(entry,
client_id: Option<u64>, seq: Option<u64>)` to `(entry, dedup_tags:
&[DedupTag])`. `PxLearner::record_dedup_tags` records each tag against
the slot (skipping `client_id == 0` sentinels). Repair/election/restore
paths pass `&[]` (no tags → no dedup recording, identical to the old
`None, None`).

### 23.5 Config

`PaxosConfig` gains:

- `coalesce_max_keys: usize` — max ops per batch (cap 65535, the
  payload count field is `u16`). `0` disables coalescing (default).
- `coalesce_drain_threshold: usize` — skip drain when in-flight
  slot-task count >= this. Library default `1`; the `crow-kv-server`
  CLI derives `max_inflight / 4` when `--coalesce-drain-threshold` is
  omitted (skip drain once the pipeline is a quarter full; the last
  finisher always drains). `0` = always drain (pure event mode);
  higher values skip the drain at high load so the `max_keys` overflow
  path produces full batches.

CLI: `--coalesce-max-keys`, `--coalesce-drain-threshold` on
`crow-kv-server`, applied in `main.rs` into `config.paxos`. Wired into
the group via `set_from_config` (the coalescer reads
`self.config.paxos.*`).

### 23.6 Correctness

- **Dedup**: each coalesced tag is recorded on leader + all accepting
  followers → a retried `(client_id, seq)` returns the shared slot on
  any replica that has it; outside the window, safe to re-propose
  (per-key highest-slot-wins makes a re-propose idempotent at the
  engine level). Identical guarantee shape as before.
- **Per-key ordering**: unchanged; all ops in a batch share one slot;
  across batches, per-key highest-slot-wins applies as before.
- **`ProposeResult::Chosen { slot }` contract**: every coalesced
  waiter receives the same slot. `ProposeResult` gains `Clone`.
- **`coalesce_max_keys = 0`**: `propose` calls `propose_inner` with a
  1-tag slice; the paxos loop is unchanged; the only difference is
  the `&[DedupTag]` vs `(Option, Option)` plumbing, which records the
  same single dedup entry. No behavior change.
- **Leadership**: re-checked inside `propose_inner`; a step-down
  between batch collection and flush surfaces as `NotLeader` to all
  waiters.
- **Shutdown**: slot-tasks hold a `Weak<PxGroup>`; on shutdown the
  pending batch is dropped (waiters get a closed oneshot → mapped
  to `Err`).
- **Drain threshold liveness**: the inflight permit is released (on
  permit drop in `propose_inner`) **before** `coalesce_drain_after_round`
  is called. So the last finisher always sees `occupied = 0 < threshold`
  and takes the batch. The batch is never stuck. The 1000ms watchdog
  is a backstop for edge cases (drain panic, spawn failure).

### 23.7 Benchmark Results (10s mem mode, 3-node cluster, max_keys=32)

| Threads | Baseline TPS | Timer TPS | Event TPS | Drain TPS | Timer WAL | Drain WAL |
|---|---|---|---|---|---|---|
| 32 | 27,787 | 33,029 | 48,346 | 47,485 | 31,090 | 139,404 |
| 64 | 28,062 | 64,145 | 68,201 | 68,741 | 60,498 | 106,926 |
| 128 | 28,260 | 97,554 | 86,759 | 101,537 | 92,752 | 101,350 |
| 256 | 27,804 | 113,671 | 97,865 | 118,377 | 110,034 | 111,944 |

Drain beats Timer at high load (128: 102K vs 98K, 256: 118K vs 114K) with
WAL counts close to Timer. At 64 threads it matches Event mode and beats
Timer. At 32 threads it matches Event mode (no regression). The default
threshold of 1 still allows drains at low load, preserving the zero-
latency-floor behavior.
