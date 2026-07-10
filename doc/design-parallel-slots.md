# CrowKV - Design: Parallel Slot Pipelining

Depends on: [`requirement.md`](requirement.md), [`design.md`](design.md)
Satisfies: [requirement.md §6.5](requirement.md#65-parallel-slot-linearizability-analysis), [requirement.md §7.3](requirement.md#73-parallel-slot-processing), [requirement.md §7.3.1](requirement.md#731-correctness-analysis-for-parallel-slot-writes)

This document specifies the design of parallel-slot Multi-Paxos consensus — the feature that distinguishes CrowKV from Raft-based KV systems. It defines how the leader pipelines proposals, how gaps are detected and repaired, how the safe-slot is maintained, and how the system stays correct under concurrent in-flight slots.

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

---

## 1. Why Parallel Slots

A Raft leader cannot acknowledge slot N+1 until slot N has been committed; the log is contiguous, so a single slow follower stalls the entire commit pipeline (head-of-line blocking). Multi-Paxos has no such constraint: each slot is a separate Paxos instance, and a quorum that decides slot N+1 need not include the same nodes that decide slot N.

CrowKV exploits this by running many slots in parallel on the leader. Throughput is bounded by network and disk bandwidth, not by per-slot serialized round-trips. The price is two-fold:

- **Gaps.** A slot may remain undecided long after later slots are decided. We need a mechanism to resolve gaps without stalling the hot path.
- **Conservative cross-key reads.** A `Scan` must wait for a no-gap prefix; point reads do not.

The blind-ops premise from [requirement.md §5.2](requirement.md#52-operations) makes the trade-off cheap: out-of-order *apply* is safe because no operation reads before writing.

Mature inspirations:

- **Multi-Paxos pipelining** as analyzed in *Paxos Made Live* (Chandra et al., 2007) and *Mencius* (Mao et al., 2008).
- **Closed-timestamp / safe-ts** patterns from CockroachDB and TiKV, the closest analogue of our safe-slot.
- **Repair via classic Paxos** as the canonical gap-resolution pattern, recognized since *The Part-Time Parliament*.

EPaxos generalizes pipelining further (per-command dependency tracking) but at considerable complexity. CrowKV deliberately stays with the simpler "leader assigns slots, blind ops, classic-Paxos repair" formula.

---

## 2. Concepts and Invariants

The design depends on a small set of invariants. Every other rule in this document follows from them.

- **I1 — Single slot counter.** On a leader, slot assignment is performed by exactly one logical worker. Two writes never receive the same slot, and no two slots are assigned out of arrival order.
- **I2 — Slot determines linearization.** The slot number assigned to an op is the op's position in the global linearization order ([requirement.md §6.1](requirement.md#61-write-guarantee)).
- **I3 — Quorum-fsync before ack.** A client write is acked only after a quorum of acceptors (including the leader) have fsynced their `Accepted(slot, ballot, value)` records. This is the durability hook that makes I2 robust to failures.
- **I4 — Apply-order independence for blind ops.** For any key *k*, the engine's final value is `value(max{ slot | slot writes k })`. Apply order between non-overlapping keys is irrelevant; for the same key, the higher slot wins regardless of arrival order ([requirement.md §7.3.1](requirement.md#731-correctness-analysis-for-parallel-slot-writes)).
- **I5 — Per-key resolved-slot is monotone.** A learner's per-key tracker only ever advances. This is the basis for read-your-writes from followers.
- **I6 — Safe-slot is contiguous.** The cluster-wide safe-slot is the maximum N such that *every* slot ≤ N is chosen and applied on every learner. It is by definition gap-free.

Anything that would violate any of these invariants is rejected by design.

---

## 3. Slot Lifecycle on the Leader

The proposer maintains a small in-memory record per in-flight slot. It transitions through a fixed state machine.

| State | Trigger to enter | Trigger to leave | Notes |
| --- | --- | --- | --- |
| `Assigned` | client request admitted; counter incremented | leader's WAL fsync of the `PxLogEntry` completes | Visible to no one yet |
| `Proposing` | local fsync done; `Accept` fanned out to followers | quorum of `Accepted` responses received | At this point the value is *chosen* |
| `Chosen` | quorum reached | learner has applied to engine | Ack to client may be sent here (see below) |
| `Applied` | learner.apply() returned | terminal | Used by metrics; safe to evict |
| `OrphanRepair` | leader changed before reaching `Chosen`; repair task takes over | `Chosen` (via repair) | New leader's responsibility |

**When does the ack happen?** When the slot reaches `Chosen` *and* the leader's own learner has applied it. The leader's own learner application is required so that an immediately-following `Get` on the leader sees the new value (see [§6.1 of design.md](design.md#61-linearizable-leader-read)).

**Eviction.** Once a slot's record is `Applied` *and* its slot number is below the safe-slot, it can be dropped from the in-memory map. The WAL record stays until WAL GC catches up.

---

## 4. Sliding Window and Backpressure

The number of in-flight slots (states `Assigned`, `Proposing`, `Chosen` that have not yet been `Applied` *and* eligible for eviction) is capped at the **window size**. Default: 16; configurable in `[1, 1024]`.

Why a window?

- Too small → throughput is bounded by per-slot RTT.
- Too large → gap-repair worst case grows; tail latency for `Scan(Linearizable)` grows; recovery time after leader change grows.

**Backpressure protocol:**

1. The proposer maintains a small **admission queue** in front of the slot counter.
2. If the in-flight set has space, the queued request is admitted, slot is assigned, and proposing begins.
3. If the in-flight set is full, requests sit in the queue up to `admit_queue_depth` (default 1 × window).
4. Beyond that, the leader returns `Busy` to the client, which is a retryable error.

**The leader never blocks indefinitely.** No client request waits longer than `admit_queue_timeout` (default 100 ms) before either being admitted or being rejected with `Busy`. This bounds tail latency under overload.

`Busy` and queue-fullness are first-class metrics ([§13.2 of requirement.md](requirement.md#132-mandatory-observability-signals)). Sustained `Busy` indicates either an undersized window or a downstream bottleneck (slow follower, full disk).

---

## 5. Pipelined Fanout

Fanout means: as soon as the leader has fsynced its own copy of slot N, it sends `Accept(N, ...)` to all followers. It does **not** wait for slot N-1, N-2, etc. to reach quorum first.

**Per-follower flow control.** Each follower has a per-peer in-flight cap (default = window size). The replicator keeps a per-peer queue and reuses one gRPC bidi stream per group→peer pair so the messages travel in order without per-message TCP setup. If a peer falls behind beyond a configured slack (default 4 × window), the leader stops sending new `Accept`s to that peer and switches to **catch-up mode** for that peer (streaming missed slots in batched form). The peer remains a member of the group, just not part of the current quorum.

**Quorum bookkeeping.** For each in-flight slot, the leader keeps a small bitmap of which peers have `Accepted` it. As soon as a majority is reached (counting itself), the slot transitions to `Chosen`. Late `Accepted` messages are still useful: they advance the per-peer learned-slot for safe-slot computation and let other peers GC their WAL prefix sooner.

**Out-of-order `Accepted` is fine.** A follower may respond `Accepted(N+2)` before `Accepted(N)`. Each is processed independently against its own slot's bitmap.

**Out-of-order `Chosen` notifications are fine.** The leader broadcasts `Chosen(slot)` to followers as soon as quorum is reached. A follower may receive `Chosen(N+2)` before `Chosen(N)` and apply only the parts safe to apply (per-key tracking handles this).

---

## 6. Per-Key Resolved-Slot

Each learner maintains, per key it has applied, the highest slot that has touched that key. This is exposed to read traffic in two ways:

- **Read-your-writes** ([§6.2 of design.md](design.md#62-read-your-writes-follower-read)): a follower can serve a `Get(k, slot=N)` as soon as `resolved_slot[k] ≥ N`, even if other slots in the gap are still pending.
- **Linearizability sketch** ([§6.5 of requirement.md](requirement.md#65-parallel-slot-linearizability-analysis)): per-key tracking is what makes "highest slot wins" correct in the presence of out-of-order apply.

**Storage.** The engine stores `(slot, value)` per live key (one version, since CrowKV is single-version per key — [§8.7 of design.md](design.md#87-storage-engine-plug-in)). Tombstones for deletions also carry their slot.

**Memory cost.** O(live keys) per learner. Accepted as a design cost in [requirement.md §7.3.1](requirement.md#731-correctness-analysis-for-parallel-slot-writes).

**Update rule.** On `apply(slot, batch)`:

1. For each `(k, op, v?)` in the batch, if `slot > resolved_slot[k]`, update `resolved_slot[k] = slot` and write/tombstone `v`. Otherwise drop the update — it is from a slot earlier than what the engine already has.
2. Update the engine's `max_applied` watermark.
3. Update the contiguous-applied frontier if `slot == contiguous_applied + 1`, then advance through any cached higher slots that are now contiguous.

This three-step apply is the only place out-of-order slots collapse into a deterministic per-key state.

---

## 7. Safe-Slot Computation and Propagation

The safe-slot is the cluster-wide **contiguous applied frontier**: the maximum N such that every slot ≤ N has been chosen and applied on every learner. It is the foundation of follower reads ([§6.3 of design.md](design.md#63-bounded-stale-follower-read)).

**Per-learner contribution.** Each learner maintains its own `contiguous_applied` watermark — the largest N such that every slot in `[1, N]` has been applied. A learner advances this watermark whenever apply completes for the slot just above it, possibly cascading through cached higher slots.

**Aggregation.** The leader collects per-learner `contiguous_applied` reports (lightweight piggyback on the heartbeat / `Chosen` broadcast, or an explicit `LearnerReport` message every `safe_slot_report_interval`, default 50 ms).

```
   safe_slot = min(contiguous_applied[learner])  for learner in voting members
```

Followers that are persistently lagging beyond `lagging_threshold` (default 4 × window) are temporarily excluded from the `min` so a single slow node cannot freeze the safe-slot. Excluded followers are flagged as `lagging` in metrics; if they stay lagging too long, an admin alert fires.

**Propagation.** The leader includes `safe_slot` in:

- Heartbeats to followers.
- Every write response back to the client.
- Every read response that returns a slot.
- The describe-cluster RPC (so a freshly-arriving client gets a starting point).

**Properties:**

- Monotone non-decreasing under steady operation (never goes backward).
- May pause (not advance) while a gap is being repaired.
- Always ≤ leader's own contiguous frontier (because the leader is one of the learners).

This is why `Scan(Linearizable)` uses the leader's *own* contiguous frontier rather than the safe-slot — it is strictly ≥ safe-slot at all times ([§6.4 of design.md](design.md#64-scan-modes)).

---

## 8. Gap Detection

A "gap" is a slot N < max-chosen-slot for which the leader has no `Chosen` decision yet. Gaps appear when:

- A follower temporarily fails after sending `Accept` for some slots but not others.
- A network reorders/drops `Accept` messages (the leader retransmits, but eventually a slot crosses an age threshold).
- A leader change occurred while slot N was `Assigned` but not yet `Chosen`.

**Tracking.** The proposer maintains a **gap set** = `{ slots in [contiguous_chosen+1, max_chosen] that are not Chosen }`. On every state transition into `Chosen` for some slot M, the gap set is recomputed for the prefix.

**Triggers for repair:**

1. Slot's age (time since `Assigned`) exceeds `gap_age_threshold` (default 200 ms — slow enough that normal RTT noise doesn't trigger, fast enough that recovery happens).
2. The gap set's size exceeds `gap_count_threshold` (default 0.5 × window) — proactive batch repair to keep the safe-slot moving.
3. Manual: admin RPC.
4. On leader change: the new leader runs a single Phase-1 round over the entire `[contiguous_chosen+1, max_chosen]` interval; this is one bulk repair, not many small ones.

The repair task (a single async loop, see [`plan.md`](plan/plan.md) §7) runs at a configurable cadence (default 50 ms tick) and respects a `max_concurrent_repairs` cap (default 4) so it does not contend with the hot path.

---

## 9. Gap Repair via Classic Paxos

Once a gap is selected, repair runs full classic-Paxos for that slot. This is the canonical safety procedure: any value that any acceptor has already accepted at this slot will be re-chosen; only if no acceptor has any value can a `NoOp` be filled in.

**Procedure (per-gap-slot):**

1. **Pick a fresh ballot.** `(round, leader_id)` with `round = max_seen_round + 1` for that slot. `leader_id` is the current leader.
2. **Phase 1 — Prepare.** Send `Prepare(slot, ballot)` to all acceptors.
3. **Wait for quorum of `Promise`s.** Each `Promise` carries the highest `(ballot', value')` the acceptor had previously `Accepted` for this slot, if any.
4. **Choose a value.**
   - If any `Promise` returned an accepted value, take the value from the `Promise` with the highest `ballot'`. **Re-propose that value** at the new ballot. (This is the safety property of Paxos; it is what makes Phase 1 mandatory.)
   - If no `Promise` returned a value, propose `NoOp` for this slot.
5. **Phase 2 — Accept.** Send `Accept(slot, ballot, value)` to acceptors. Wait for quorum.
6. **Chosen.** Broadcast `Chosen(slot)` and apply locally.

**Important properties:**

- The repair never invents a fresh user value at an undecided slot. If any acceptor had a half-baked accept, that accept's value is re-chosen — preserving Invariant I3.
- Repair touches *one slot at a time*; the hot path on other slots is unaffected.
- After leader change, the bulk Phase-1 over `[contiguous_chosen+1, max_chosen]` is exactly the same procedure folded into one round-trip. Each open slot is resolved independently using its own `Promise`-derived value (or `NoOp`).

**Concurrent repair vs new writes.** A repair running on slot N does not block new writes at slot M > N; the proposer continues to assign new slots and pipeline them. The repair only contends for the network channel with normal traffic, which is governed by the per-peer flow control.

**Dueling proposers.** If the new leader's repair race-overlaps with a stale leader's last accepts, the higher-ballot prepare wins by Paxos rules; the older accepts are either superseded or, if any acceptor had already accepted them, re-proposed by the new leader (which is correct).

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

  ... after gap_age_threshold ...

  repair    : Prepare(N, ballot') ─► quorum of Promises
                                  │  Promise from leader: Accepted(ballot, v)
              Accept(N, ballot', v) ──► quorum ──► Chosen(N)  ◄─ slow follower may
                                                                   still be down
              apply(N) on leader, advance contiguous_applied to N+2

  Note: clients of slot N saw an ack as soon as the leader reached Chosen(N).
        Slot N's "gap" was internal to acceptor B.
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

The bulk Phase-1 round is what made this recovery cheap — one RTT, not one-RTT-per-gap.

---

## 11. Interaction with Snapshot and WAL GC

Parallel slots interact with WAL GC through two watermarks:

- `safe_slot` — every slot ≤ `safe_slot` is chosen and applied on every learner. WAL records below `safe_slot` are **decision-safe** to discard (no acceptor needs to vote on them again).
- `snapshot_slot` — the engine state at `snapshot_slot` is durably snapshotted on at least the leader and one peer.

**WAL GC rule:** discard WAL records with `slot < min(safe_slot, snapshot_slot)`. Both watermarks must have advanced past a slot before its WAL record can be GC'd:

- If `snapshot_slot < safe_slot`: we have decisions for slots ≤ safe_slot but no engine snapshot covering them; GC up to `snapshot_slot`. Replay reads the WAL above the snapshot to catch up.
- If `safe_slot < snapshot_slot`: we have a snapshot but some learner has not caught up; GC up to `safe_slot`. The lagging learner can still replay from WAL; otherwise it would need a snapshot install.

**Why both?**

- GC'ing past `snapshot_slot` would mean a node restart could not rebuild engine state without snapshot install, even if it had its own WAL.
- GC'ing past `safe_slot` would risk discarding a record some learner still needs.

**Repair-time safety:** repair never needs to read GC'd slots, because by definition every slot ≤ safe_slot is *already chosen* on every learner, so it cannot be a gap.

This is detailed further in [`design-wal.md`](design-wal.md) §4.

---

## 12. Tunables and Defaults

Numbers below are starting points based on [requirement.md §12.1](requirement.md#121-performance-targets) and standard practice. Real values will be fine-tuned in operations.

| Parameter | Default | Range | Where it lives |
| --- | --- | --- | --- |
| `window_size` | 16 | 1 – 1024 | Proposer |
| `admit_queue_depth` | 1 × window | 0 – 16 × window | Proposer |
| `admit_queue_timeout` | 100 ms | 1 ms – 10 s | Proposer |
| `peer_in_flight_cap` | 1 × window | per peer | Replicator |
| `peer_lagging_threshold` | 4 × window | per peer | Replicator |
| `safe_slot_report_interval` | 50 ms | 5 ms – 1 s | Learner |
| `gap_age_threshold` | 200 ms | 10 ms – 10 s | Repair |
| `gap_count_threshold` | 0.5 × window | 0 – ∞ | Repair |
| `repair_tick` | 50 ms | 10 ms – 1 s | Repair |
| `max_concurrent_repairs` | 4 | 1 – 64 | Repair |

**Window-size guidance:**

- Latency-sensitive deployment, low write rate → window = 8 or 16.
- Throughput-sensitive deployment, batching client → window = 64–128.
- WAN deployment (high RTT) → larger window to keep the pipe full.

The product (`window × max_concurrent_repairs × repair_tick`) bounds the worst-case time-to-resolve-all-gaps after a leader change. With defaults: `16 × 4 × 50 ms = 3.2 s` worst case to fully clear gaps after a recovery, and that is a pessimistic upper bound; the bulk Phase-1 typically resolves them in a single RTT.
