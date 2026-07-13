<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CrowKV - Design: Leader Election, Term, and Lease

Depends on: [`requirement.md`](../requirement.md), [`design.md`](../design.md)
Satisfies: [requirement.md §3 Dependencies](../requirement.md#3-dependencies-and-assumptions), [requirement.md §4.2](../requirement.md#42-paxos-core), [requirement.md §6.2](../requirement.md#62-leader-read-fencing), implicit prerequisites of [requirement.md §7](../requirement.md#7-consensus-architecture)

This document specifies leader election, term management, the `PxBallot`/`PxTerm` separation, and the leader lease used for fast linearizable reads. The design follows Raft very closely; only the per-slot Paxos parts differ.

## Table of Contents

- [1. Why Raft-Style Election on a Paxos Log](#1-why-raft-style-election-on-a-paxos-log)
- [2. PxTerm vs PxBallot](#2-pxterm-vs-pxballot)
- [3. Election Protocol](#3-election-protocol)
- [4. New-Leader Bulk Phase 1](#4-new-leader-bulk-phase-1)
- [5. Heartbeats and Liveness](#5-heartbeats-and-liveness)
- [6. Leader Lease](#6-leader-lease)
- [7. ReadIndex Fallback](#7-readindex-fallback)
- [8. Step-Down Triggers](#8-step-down-triggers)
- [9. Safety Argument](#9-safety-argument)
- [10. Tunables and Defaults](#10-tunables-and-defaults)

---

## 1. Why Raft-Style Election on a Paxos Log

Classical Multi-Paxos elects a "distinguished proposer" through Paxos itself: an aspiring leader runs Phase 1 over the entire log. This works but conflates election with proposal and complicates clean step-down, lease management, and observability.

Raft's contribution was to factor election out of the log — a separate election RPC, a per-term vote, randomized timeouts. CrowKV adopts that factoring exactly, and lets per-slot Paxos handle just the per-slot work. The separation maps onto two distinct identifiers: `PxTerm` for elections, `PxBallot` for slot proposals.

This design choice is consistent with *How to Build a Highly Available System Using Consensus* (Lampson, 1996) and is the same pattern used in Spinnaker, Spanner's Paxos groups, and TiKV's region leadership.

---

## 2. PxTerm vs PxBallot

The two identifiers serve disjoint purposes; conflating them is a common bug source.

### 2.1 `PxTerm`

- Type: monotonic non-negative integer per group.
- Persistence: durable. Every term increment is fsynced before being acted on.
- Scope: one term covers all writes by a single leader, across many slots.
- Used by:
  - Election RPCs (`RequestVote`, `Vote`).
  - Heartbeats and `Accept` messages (carried for fencing).
  - Lease validity (a lease is associated with a term).

### 2.2 `PxBallot`

- Type: lexicographically ordered pair `(round, leader_id)`.
- Persistence: durable on every acceptor (in WAL with the `PxLogEntry`).
- Scope: one ballot covers proposals at a single slot. Different slots may simultaneously be at different ballots.
- Used by:
  - Phase 1 (`Prepare`) and Phase 2 (`Accept`) messages of classical Paxos.
  - Repair: a higher round at the same leader_id supersedes a lower round at that slot.

### 2.3 The bridge

A new leader at term `T` proposes new slots with ballot `(round=0, leader_id=me)`. The implicit ordering is "this leader's ballots are 'newer' than any leader before it" because acceptors fence on `term` first: an `Accept` carrying `term=T` is rejected by any acceptor whose `current_term > T` regardless of ballot. Conversely a same-term `Accept` is compared by ballot rules.

This means:

- Acceptors keep `(current_term, slot_state[slot])` where `slot_state` records `(promised_ballot, accepted_ballot, accepted_value)`.
- Election bumps `current_term`, invalidating any in-flight `Accept` from older terms.
- Repair within a term bumps `round` for a single slot, invalidating any in-flight `Accept` for that slot only.

Both fences are necessary, and they are independent.

---

## 3. Election Protocol

The protocol matches Raft's leader election with adaptations for the bulk Phase-1 step (§4).

### 3.1 Roles

Each member is exactly one of:

- **Follower** — accepts heartbeats and `Accept`s from a leader.
- **PreCandidate** — running a `PreVote` round (see §3.2a); has not yet bumped `current_term`.
- **Candidate** — has bumped `current_term` and is collecting votes.
- **Leader** — won the latest election; serves writes.

### 3.2 Election trigger

A follower starts an election when its **election timer** expires. The timer is reset on every legitimate heartbeat or `Accept` from the current leader. The timeout is randomized in `[election_min, election_max]` (defaults 4000 ms – 8000 ms; see §10), avoiding split votes.

When the timer fires:

1. (PreVote, enabled by default) Follower transitions to PreCandidate and sends `PreVote(proposed_term, candidate_id, accepted_log_tip_slot, accepted_log_tip_term)` to all peers *without* bumping `current_term`. If a quorum grants, proceeds to step 2. If a peer reports a higher term, reverts to follower. If quorum is not reached, reverts to follower and waits for the next deadline.
2. Follower transitions to Candidate, bumps `current_term` to `proposed_term`, persists (`VoteGranted` WAL record).
3. Votes for itself.
4. Sends `RequestVote(term, candidate_id, accepted_log_tip_slot, accepted_log_tip_term)` to all peers.

> **PreVote** (Raft optimization, ON by default): avoids spurious term increments from a partitioned-and-rejoined node. A node that was partitioned can rejoin and ask "would you vote for me?" without disrupting the current leader's term.

### 3.3 Vote rules (matching Raft)

A peer grants its vote iff:

- The request's `term` is ≥ peer's `current_term`. If higher, peer adopts the new term and reverts to follower first.
- The peer has not already voted in this term, or already voted for the same candidate.
- The candidate's acceptor log is **at least as up-to-date** as the peer's, where up-to-date is the lexicographic comparison `(accepted_log_tip_term, accepted_log_tip_slot)`.
- The peer's `vote_lockout_until` has passed (no active lease promise from a heartbeat).

The "log up-to-date" check is the safety property that prevents a stale leader from being elected.

### 3.4 Outcome

- **Win:** received votes from a quorum within the election timeout. Becomes leader; immediately runs the bulk Phase 1 (§4) and then sends initial heartbeats.
- **Loss / split:** sees a higher term, or election timer fires again with no quorum. Reverts to follower and waits for a new randomized timer.
- **Conversion:** if the candidate sees a `Heartbeat` or `Accept` with `term ≥ current_term`, it accepts that node as leader and reverts to follower.

---

## 4. New-Leader Bulk Phase 1

A freshly elected leader does not immediately serve client writes. It first runs **one** Phase 1 over the open slot prefix to discover any in-flight values from the previous leader.

### 4.1 What is the open prefix?

When the leader takes office, it computes:

- `floor = its own contiguous_chosen` (deliberately NOT maxed with peers' `contiguous_chosen`). After a restart, a replica can win election while missing committed slots it never received (the value lived only as a `ChosenNotice` watermark, which carries no payload). Maxing with peers' commit point would skip exactly those slots, leaving the new leader serving a stale value for a committed key. Sweeping from the leader's own frontier forces Phase 1 to re-derive every higher slot from the quorum.
- `ceiling = max(local highest_seen_slot, next_slot - 1, peers' highest_seen_slot from election replies)`.

The open prefix is `[floor + 1, ceiling]`. If a previous leader had assigned slots beyond what any current acceptor knows about, those slots are simply not in this set; their values are unrecoverable but also un-acked, so no client expects them to exist.

### 4.2 Bulk Prepare

The leader runs Phase-1 `Prepare` **per slot** over `[floor+1, ceiling]`, batched by `bulk_prepare_window` (default 1024) with a `yield_now()` between batches to avoid starving other tasks. For each slot, `Prepare(ballot=(0, me), term=T)` is sent to all peers. Each acceptor responds with `Promise` carrying the highest `(accepted_ballot, accepted_value)` if any.

The ballot used here is `(0, me)`. Because acceptors fence on `term` first, this ballot is "fresh" for the new term; we don't need a higher round.

### 4.3 Adopt and re-Accept

For each slot in the range:

- If any peer's `Promise` returned an accepted value, choose the value with the highest `accepted_ballot` and re-Accept it at `(0, me)` under term T.
- If no peer returned a value, propose `NoOp` and re-Accept at `(0, me)` under term T.

These re-Accepts are pipelined; the new leader does not wait for any one to be chosen before proposing the next.

### 4.4 Steady state begins

Once the bulk Phase 1 has been *issued* (not necessarily completed), the leader is free to start assigning new slots starting at `ceiling + 1`. The repair work for `[floor+1, ceiling]` proceeds in parallel and reuses the same machinery as routine gap repair (see [`design-slot.md`](design-slot.md) §9).

---

## 5. Heartbeats and Liveness

Heartbeats serve two purposes: liveness signaling (reset followers' election timers) and lease maintenance (extend the leader's lease).

### 5.1 Heartbeat content

Every heartbeat carries:

- `term`, `leader_id`.
- `committed_safe_slot` (latest known safe-slot).
- `lease_grant_until` (monotonic-time deadline; see §6).
- `prev_log_slot, prev_log_term` (Raft-style consistency check).
- `t_send_ms_mono` (monotonic timestamp for lease grant calculation).

`Chosen(slot)` notifications and `Accept`s are sent through separate `learner_stream` paths, not piggy-backed on heartbeats.

### 5.2 Heartbeat cadence

- Default `heartbeat_interval = 500 ms` (see §10).
- A follower's election timer must be ≫ `heartbeat_interval` to allow for occasional jitter; with defaults, election timeouts are 8–16× the heartbeat interval.

### 5.3 Heartbeat response

Followers respond with their own `term`, `success` (false if the follower's term is higher), `contiguous_chosen`, `last_chosen_term`, `contiguous_applied`, `highest_seen_slot`, and `durable_snapshot_slot`. The leader uses these to:

- Detect a stale leader's continued existence (if any response carries a higher term, the leader steps down — §8).
- Maintain peer state (used by replicator and gap detection).
- Refresh the safe-slot computation.

---

## 6. Leader Lease

A lease lets the leader serve `Get(mode=Linearizable)` without a per-read quorum round-trip ([§6.1 of design.md](../design.md#61-linearizable-leader-read)). The lease is the standard Raft-style approach with a clock-skew bound.

### 6.1 What the lease grants

While its lease is valid, a leader may serve linearizable reads from local state under two assumptions:

- No other node was elected leader during the lease window.
- The leader's `contiguous_applied` slot reflects all writes acked through this leader (which is true by Invariant I3 from [`design-slot.md`](design-slot.md) §2).

The first assumption is enforced by:

- Acceptors **promise not to vote for any other candidate** for at least `lease_duration` after granting a heartbeat (this is the Raft "PreVote + lease" pattern).
- The leader treats its lease as expired at `lease_grant_time + lease_duration - max_clock_skew` for safety (see §6.3).

### 6.2 Lease grant and renewal

Each heartbeat round-trip is also a lease grant:

1. Leader sends heartbeat at monotonic time `T_send`.
2. Follower receives, records "I will not vote for any candidate before `T_recv + lease_duration`", and replies.
3. Leader receives the response at `T_recv_reply`. The lease is valid through `T_send + lease_duration` on the leader's clock — the leader uses `T_send` (not `T_recv_reply`) as the start so that any clock skew works in its favor.
4. The leader treats the lease as effective until `T_send + lease_duration - max_clock_skew`. This is conservative; it gives a safety margin equal to the assumed skew bound.

With the default `heartbeat_interval = 500 ms` and `lease_duration = 9 × heartbeat_interval = 4500 ms` (see §10), the leader is essentially always within an active lease in steady state. A short network blip costs the leader its fast-read privilege but not its leadership.

### 6.3 Clock-skew assumption

[requirement.md §3](../requirement.md#3-dependencies-and-assumptions) caps clock skew at `max_clock_skew` (default `500 ms`, see §10) per heartbeat interval. The lease formula is:

```
  effective_lease = lease_duration - max_clock_skew
```

Concretely with defaults: `4500 ms - 500 ms = 4000 ms` of "fast-read" coverage between successful heartbeat round-trips. If the round-trip exceeds the effective lease, the lease has *technically* expired and the leader downgrades to ReadIndex for the next linearizable read until the next successful heartbeat refreshes the lease.

### 6.4 Why monotonic-only

All lease math uses the **monotonic clock**, not wall-clock. Wall-clock can jump (NTP step, manual operator change); monotonic cannot. This is the same discipline as Raft's reference implementation.

### 6.5 What goes wrong if lease misuse occurs

If a leader serves a linearizable read after its lease has *truly* expired and a new leader has been elected, the read can return stale data. The read would not have observed a write committed by the new leader. This is the precise correctness reason for the conservative `effective_lease`.

The fallback is ReadIndex, which gives the same correctness guarantee without the clock assumption.

---

## 7. ReadIndex Fallback

ReadIndex is the lease-free path for linearizable reads. Always available; used automatically when the lease is not effective; available to clients on a per-read basis.

### 7.1 Procedure

1. The leader records its current `contiguous_applied` slot, call it `R`.
2. The leader broadcasts a heartbeat to a quorum, awaits responses.
3. As soon as a quorum has responded *with the leader's own term*, the leader is confirmed to still be leader at this point in time.
4. The leader serves the read from local state, ensuring the engine's `contiguous_applied >= R`.

The cost is one network round-trip per ReadIndex (or per batch of reads). No fsync, no extra log records.

### 7.2 Batching

> **Not yet implemented.** The current `linearizable_read_barrier` serves one read at a time. Batching multiple reads into a single heartbeat-quorum round is a planned optimization.

### 7.3 When to choose ReadIndex over lease

Per-read settings:

- **Default:** lease if effective, ReadIndex if not.
- **Force ReadIndex:** for environments without a clock-skew guarantee, or for workloads requiring belt-and-suspenders correctness. Higher latency but no clock dependence.

The choice is exposed as a per-request option in the read RPC and as a default at the server config.

---

## 8. Step-Down Triggers

A leader steps down (transitions to follower) on any of:

- **Higher term seen.** Any RPC response carrying `term > current_term` causes the leader to adopt the new term and revert to follower.
- **Lease unrenewable.** The leader has been unable to obtain a heartbeat-quorum response for longer than `lease_duration`. It cannot refresh its lease and assumes a partition or majority loss.
- **Admin step-down RPC.** Operator forces it for testing, planned maintenance, or rolling upgrade.
- **Removed from group.** Designed but not yet implemented (`ConfigChange` log entries are not yet supported).

On step-down:

1. Cancel the per-tenure `CancellationToken` — aborts in-flight bulk Phase 1 and any tenure-bound work.
2. Stop heartbeats (the leader-state loop returns on step-down).
3. Set `role = Follower`; adopt the higher term if the trigger was `HigherTerm`.
4. Expire `LeaseState` (`become_follower` clears leader id).
5. In-flight proposals see `NotLeader` via the propose leadership gate (proposing_term check).

---

## 9. Safety Argument

The election + bulk-Phase-1 + lease design must satisfy three properties.

### 9.1 At most one leader per term per group

Standard Raft argument:

- A candidate needs majority votes within a term.
- A peer votes at most once per term.
- Two majorities over the same membership intersect (by quorum overlap).
- Therefore at most one candidate can collect a majority in any one term.

CrowKV inherits this directly.

### 9.2 No chosen value is lost across leader change

By Paxos's classical safety: if a value `v` was *chosen* at slot `s` (≥ majority of acceptors `Accepted` it), then any future Phase-1 over slot `s` will see `v` from at least one acceptor and re-propose it.

The new-leader bulk Phase-1 (§4) executes this exact procedure over the open prefix, so any chosen value is preserved.

A value that was `Accepted` by some but not a majority of acceptors may be either re-chosen or lost, but it was never acknowledged to the client (Invariant I3); the client knows its outcome is unknown and retries.

### 9.3 No stale leader serves a stale linearizable read

Two enforcement mechanisms run in series:

- **Term fencing:** every RPC carries `term`. Any acceptor with a higher term refuses the request and informs the sender, which steps down.
- **Lease conservatism:** even when no fencing has happened, the leader self-expires its lease at `effective_lease = lease_duration - max_clock_skew`. If the clock-skew assumption holds, no other leader can have been elected within this window (because acceptors won't vote against the lease).

If the clock-skew assumption is *violated*, lease-based reads can return stale data — this is documented and the operator can force ReadIndex for stronger guarantees.

---

## 10. Tunables and Defaults

| Parameter | Default | Range | Notes |
| --- | --- | --- | --- |
| `heartbeat_interval` | 500 ms | 10 ms – 30 s | Should be ≪ `lease_duration` |
| `lease_duration` | 4500 ms | 100 ms – 60 s | Should be ≫ `heartbeat_interval` + `max_clock_skew` (rule of thumb: `9 × heartbeat_interval`) |
| `effective_lease` | derived | — | `= lease_duration - max_clock_skew` |
| `max_clock_skew` | 500 ms | 1 ms – 5 s | Architectural bound from [requirement.md §3](../requirement.md#3-dependencies-and-assumptions) |
| `election_min` | 4000 ms | ≥ 8 × `heartbeat_interval` | Avoid spurious elections |
| `election_max` | 8000 ms | ≤ 60 s | Bounds time to elect after leader loss |
| `prevote_enabled` | true | bool | Reduces disruption from rejoining nodes |
| `bulk_prepare_window` | 1024 | 1 – 65536 | Slots per yield batch in bulk Phase-1 repair |
| `learner_stream_window_frames` | 64 | 8 – 1024 | Per-peer `PxLearnerStream` outbound mpsc capacity; full mpsc surfaces as `Busy` |
| `maintenance_tick_ms` | 30 000 | 1 000 – 300 000 | Engine-durability + WAL-GC maintenance loop interval |

Notes:

- **PreVote** (Raft optimization, ON by default): a candidate first asks "would you vote for me?" without bumping the term. This avoids spurious term increments from a partitioned-and-rejoined node.
- **Pre-emptive step-down on heartbeat loss** is governed by `lease_duration`, not `election_min`. A leader that loses contact with quorum gives up its lease at `lease_duration` and stops serving fast reads even before any follower starts an election.
- **Default choice rationale.** The defaults above are tuned for general-purpose deployments (mix of single-DC and modest cross-AZ latency). They give ~5–8 s failover detection, which matches the operational expectations of most online KV workloads while keeping heartbeat chatter low. See [`design.md`](../design.md) §13 (Open Design Questions) for the analysis.
- **Operational profiles:**
  - *Low-latency single datacenter:* `heartbeat_interval = 100 ms`, `election_min/max = 800/1500 ms`, `lease_duration = 900 ms`, `max_clock_skew = 100 ms`. Sub-second failover; higher message rate.
  - *Cross-region / WAN:* `heartbeat_interval = 3 s`, `election_min/max = 24/48 s`, `lease_duration = 27 s`. Matches CockroachDB-style geo-replicated tunings; tolerates wider RTT variance at the cost of failover latency.
  - *Tests:* `PxElectionConfig::for_tests()` produces `heartbeat = 5 ms`, `election = 30–60 ms`, `lease = 25 ms` for use under `tokio::time::pause()`. Not exposed on `crowkv-server` CLI.
