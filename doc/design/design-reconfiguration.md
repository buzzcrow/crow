# CrowKV - Design: Reconfiguration

Depends on: [`requirement.md`](../requirement.md), [`design.md`](../design.md), [`design-leader-election.md`](design-leader-election.md)
Satisfies: [requirement.md §9.1](../requirement.md#91-reconfiguration), prerequisites of [requirement.md §9.2](../requirement.md#92-rolling-upgrade)

This document specifies how a CrowKV group safely changes its membership while preserving consensus safety. The design is Raft-style joint consensus, adapted to CrowKV's Multi-Paxos log.

## Table of Contents

- [1. Scope and Supported Transitions](#1-scope-and-supported-transitions)
- [2. The Joint-Consensus Pattern](#2-the-joint-consensus-pattern)
- [3. Reconfiguration State Machine](#3-reconfiguration-state-machine)
- [4. New-Member Bootstrap](#4-new-member-bootstrap)
- [5. Member Removal](#5-member-removal)
- [6. Leader Transfer](#6-leader-transfer)
- [7. Group-0 Special Cases](#7-group-0-special-cases)
- [8. Quorum-Overlap Safety Argument](#8-quorum-overlap-safety-argument)
- [9. Failure During Reconfiguration](#9-failure-during-reconfiguration)
- [10. Tunables and Defaults](#10-tunables-and-defaults)

---

## 1. Scope and Supported Transitions

CrowKV supports membership changes within a single group. Specifically:

- **Add or remove voting members.** Adding a member when going 3 → 5 → 7 or removing a member when going 7 → 5 → 3.
- **Replace a member.** Implemented as add-then-remove (or vice versa), each as a single-member change.
- **Change the leadership of the group.** Triggered as a side effect when removing the current leader.

Out of scope ([requirement.md §2](../requirement.md#2-non-goals-out-of-scope)):

- Changing `num_groups` (the total number of groups in the cluster) — fixed at cluster creation.
- Splitting or merging groups — not supported.
- Going below 3 voting members — not supported (a 1-member group has no fault tolerance).
- Going above 7 voting members — not in the initial scope; quorum size grows linearly with membership and the marginal availability gain past 7 is small.

**Granularity:** every reconfiguration moves *exactly one member* in or out at a time. To go 3 → 5, do two single-member additions in sequence. This keeps the safety argument simple and matches Raft 2014's recommendation.

---

## 2. The Joint-Consensus Pattern

The naive idea — "the leader writes a single `ConfigChange` log entry that swaps the members" — is unsafe. Between the moment the new config is *chosen* and the moment every member has *applied* it, there is a window where different members disagree about who is in the quorum, and two disjoint majorities could both make decisions.

Joint consensus closes this window by passing through an intermediate **joint configuration** `C_old ∪ C_new`:

```
   C_old ────► C_old ∪ C_new ────► C_new
              (joint config)
```

While the joint config is in effect, **every** decision (write, leader election) requires:

- A quorum from `C_old`, **and**
- A quorum from `C_new`.

This is "two quorums in series" and it is what guarantees safety. Two majorities under `C_old ∪ C_new` always intersect with both `C_old`-majorities and `C_new`-majorities, so no two disjoint decisions can be made even during the transition.

CrowKV uses exactly this pattern, encoded as two `ConfigChange` entries in the log:

1. `ConfigChange(joint = C_old ∪ C_new)` — enters joint mode.
2. `ConfigChange(C_new)` — exits joint mode, only the new config is active.

Both entries flow through normal consensus (i.e. each is a slot in the WAL and goes through Phase 2). The reconfiguration is *complete* when entry 2 is **applied**, not when it is *chosen* — see §8.

---

## 3. Reconfiguration State Machine

The leader drives a small state machine for each in-progress reconfiguration. Followers simply observe the log entries and react.

| State | Triggered by | Allowed actions | Exit on |
| --- | --- | --- | --- |
| `Idle` | initial / steady state | Normal writes only | `ProposeJoint` (admin RPC) |
| `Proposing Joint` | admin issues `ConfigChange(joint)` | Bulk Phase-1 if needed; then Accept the joint entry | Joint entry applied → `JointActive` |
| `JointActive` | joint entry applied | New writes require both-quorums; new members may be added as non-voting catch-up readers | Catch-up done → `ProposeFinal` |
| `Proposing Final` | leader proposes `ConfigChange(C_new)` | Same as `Proposing Joint` but with `C_new` payload | Final entry applied → `Idle` |

**Why do we wait for the joint entry to be *applied* (not just *chosen*) before accepting catch-up of new members?**

Because applying the joint entry is the moment the leader's local state machine starts requiring both-quorums for subsequent decisions. If we let the new member become a voting peer earlier, a quorum could be formed using new-side voters before old-side voters had observed the joint config.

Application happens automatically after `Chosen`, with no special handling: the learner sees a `ConfigChange` payload and switches its quorum-eval logic.

---

## 4. New-Member Bootstrap

A new member joining a group has empty WAL and empty engine state. It must be brought up to date before it can vote.

### 4.1 Phases

```
   1. ConfigChange(joint) is chosen and applied on the leader.
   2. New member starts as non-voting "learner" (not part of either quorum yet).
   3. Snapshot install: leader sends a snapshot at slot S.
   4. WAL catch-up: leader streams WAL records [S+1, current_max_chosen].
   5. New member reaches `contiguous_applied = current_max_chosen`.
   6. Leader proposes `ConfigChange(C_new)`. New member is now in `C_new` and voting.
   7. C_new entry is chosen with both-quorums and applied.
   8. Reconfiguration complete; group is now in C_new.
```

Step 6 is allowed because the new member is in `C_new` *but not yet `C_old`*. The both-quorum rule for the C_new entry itself uses the joint config, which includes the new member only on the C_new side.

### 4.2 Why pre-load before adding to C_new

If a new member were added directly to `C_new` while still empty, then a quorum of `C_new` could form with the new member's vote, even though the new member has none of the previously-decided values. A subsequent leader election under that quorum could elect a leader that has not seen older committed writes — violating Paxos safety.

By requiring catch-up *before* proposing the final `ConfigChange`, we ensure that when the new member first becomes a voting peer, it already has all chosen values up to `current_max_chosen`.

### 4.3 Snapshot install protocol

Defined in [§8.4 of design.md](../design.md#84-snapshot-and-install) and `design-state-machine.md` §6 (snapshot import). Resumable, throttled, end-to-end CRC.

### 4.4 Catch-up termination criterion

The leader continues streaming WAL until the new member's `contiguous_applied` is within `catchup_slack` (default 100 slots) of the leader's `max_chosen`. At that point the leader proposes `ConfigChange(C_new)` and the new member is expected to keep up via normal replication once it becomes voting.

If the new member cannot keep up (e.g. slow disk), reconfiguration aborts (§9) and the operator is alerted.

---

## 5. Member Removal

Removing a member is simpler than adding one because there is no catch-up to wait for.

### 5.1 Procedure

```
   1. ConfigChange(joint = C_old ∪ C_new) where C_new = C_old \ { member_X }.
   2. Joint applied.
   3. Member X stops counting in C_new but still counts in C_old; both-quorums still reach without X for any C_new vote.
   4. ConfigChange(C_new) proposed with both-quorums.
   5. C_new applied. Member X is no longer in any active config.
   6. Member X stops sending Accepts, drains in-flight responsibilities, sends step-out RPC to leader.
   7. Member X may be powered off.
```

### 5.2 If the removed member is the leader

Leader transfer must happen before the final `ConfigChange(C_new)` is applied; see §6. Otherwise the group can find itself momentarily without a leader at the worst possible moment.

### 5.3 If the removed member is unreachable

Removal works regardless of reachability: the joint entry only requires a quorum *of those still reachable*. As long as `C_old` still has a quorum without member X (which is true when removing one member from 5 → 4 → ...), reconfiguration proceeds. After C_new applies, member X is irrelevant.

This is how "remove a dead node" is implemented operationally.

---

## 6. Leader Transfer

Transferring leadership is needed in two scenarios:

- The current leader is being removed by reconfiguration.
- An admin wants to drain a node for maintenance.

### 6.1 Procedure

1. Leader identifies a target follower `T` that is fully caught up (`contiguous_applied = leader's max_chosen`).
2. Leader sends a special `TimeoutNow` RPC to `T`. This tells `T` to start an election immediately at `term + 1`.
3. Old leader stops accepting new client writes; responds `NotLeader { hint = T }` for in-flight requests.
4. `T` runs election, wins (since it is up-to-date and others see its higher term), runs bulk Phase-1, becomes leader.
5. Old leader, on observing the new term in any RPC response, steps down ([§8 of design-leader-election.md](design-leader-election.md#8-step-down-triggers)).

This is the same `TransferLeadership` pattern as in Raft. It avoids the latency of a normal randomized-timeout election.

### 6.2 During reconfiguration

When removing the current leader:

1. Joint config is chosen and applied as usual.
2. Before proposing `ConfigChange(C_new)`, the leader runs leader transfer to a member that will remain in `C_new`.
3. The new leader proposes and finalizes `ConfigChange(C_new)`.

This way, the moment `C_new` is applied, the new leader is already a regular voting member in `C_new` and the old leader can cleanly retire.

---

## 7. Group-0 Special Cases

> **Decision record (2026-07, see `plan-client.md` §6 Issue 1):** `Group-0` as
> described in this section was never implemented. Topology (per-group
> memberships, cluster inventory) is operator-managed via the HTTP management
> API and persisted to per-group config files, not self-hosted in a
> Paxos-replicated system group — see [`requirement.md` §7.1](../requirement.md#71-groups-and-cluster-topology).
> There is therefore no cluster-wide `config_version`, no recursive
> reconfiguration case, and no "Group-0 pauses data-group reconfiguration"
> rule in the shipped system. Kept below for history/reference only, in case
> a system embedding `crowkv`'s primitives wants to build its own such
> metadata group on top.

Group-0 holds the cluster topology (per-group memberships, partitioning rule, `config_version`). Reconfiguring Group-0 itself looks like reconfiguring any other group, but it has two extra constraints.

### 7.1 Topology vs Group-0 membership

A `ConfigChange(joint)` for a *data* group is a write to *Group-0's log* (because Group-0 owns topology). So:

- Reconfiguring data group K → committing two log entries in **Group-0**.
- Reconfiguring **Group-0 itself** → committing two log entries in Group-0 *and* using joint-consensus for those very entries.

The recursion is well-defined: Group-0's joint-consensus operates on its own membership, exactly like any other group. There is no infinite regress because Group-0 always has *some* current membership; only Group-0 modifies that membership.

### 7.2 Serializing topology changes with Group-0 membership change

If we are simultaneously (a) changing data group K's membership and (b) changing Group-0's own membership, we must serialize these so that observers cannot see an inconsistent topology.

The rule: **Group-0 membership changes pause topology changes.** While Group-0 is in joint mode for its own membership, no `ConfigChange(joint)` for any data group is admitted to its log. Pending data-group reconfigurations wait until Group-0 has exited its own joint mode.

This is conservative and easy to reason about. Throughput cost: minor — Group-0 reconfigurations are rare.

### 7.3 `config_version`

Each topology change increments `config_version` in Group-0. Clients piggy-back this version on every request; if a server sees an older version from a client (or vice versa), it returns the up-to-date one in the response so the client refreshes its cache.

This prevents clients from acting on a stale view of the cluster while topology is changing.

---

## 8. Quorum-Overlap Safety Argument

The safety property we must preserve through reconfiguration is:

> **No two values can be chosen at the same slot.**

Standard Paxos guarantees this when all decisions are evaluated against the same membership. Reconfiguration introduces the risk that two decisions are evaluated against *different* memberships, which could both succeed without overlap. Joint consensus closes that risk.

### 8.1 The single-membership case

For a single fixed membership `M`, Paxos safety says: any two majorities of `M` intersect. Therefore no two distinct values can both be chosen at the same slot.

### 8.2 The transition case

Consider a slot `s` and two candidate values `v` and `v'`. Suppose `v` is chosen under `C_old` and `v'` under `C_new`. We must show this cannot happen.

The key observation: between `C_old` being the *active* config and `C_new` being the *active* config, the **joint** config is the active config. Every decision during the joint phase requires a both-majorities quorum.

Consider the following exhaustive cases for slot `s`:

- **Both `v` and `v'` chosen under `C_old`.** Impossible by `C_old` quorum overlap.
- **Both chosen under `C_new`.** Impossible by `C_new` quorum overlap.
- **Both chosen under joint.** Impossible because joint requires both `C_old` and `C_new` majorities; the `C_old` overlap kills it.
- **`v` under `C_old`, `v'` under `C_new`.** For `v'` to be chosen under `C_new`, the `ConfigChange(C_new)` entry must already have been chosen. Per the joint-consensus rule, that entry was chosen under both-quorums, which means a `C_old`-majority was reached at the moment of the joint→C_new transition. This `C_old`-majority intersects the `C_old`-majority that chose `v`. The intersecting acceptor would have had to accept both `v` (at slot `s`) and the eventual no-op-or-replay at slot `s` under the new term — Paxos's per-slot rule (highest-ballot-accepted-value wins) requires that the new leader re-propose `v`, not `v'`. Contradiction.
- **`v` under `C_old`, `v'` under joint.** Similar reasoning; the joint phase requires `C_old`-quorum, which intersects the `C_old`-quorum that chose `v`.
- **`v` under joint, `v'` under `C_new`.** Both `C_new`-majorities; quorum overlap.

In every case, two disjoint chosen values are impossible.

The argument is the same as Raft 2014's. CrowKV's per-slot Paxos and Raft's monotonic log are equivalent at the membership-change layer.

### 8.3 Asymmetric transitions and "open question"

The case 3 → 5 with the new two members not yet caught up was flagged as an open question in [§11 of design.md](../design.md#11-open-design-questions). The answer above resolves it: catch-up happens during the joint phase, while the new members are non-voting; only after catch-up is the final `ConfigChange(C_new)` proposed, at which point new members are voting and the both-quorum invariant has been preserved throughout.

---

## 9. Failure During Reconfiguration

Reconfiguration is itself a sequence of Paxos decisions, so it is robust to failure at any step. Recovery rules:

- **Leader crashes during `Proposing Joint`.** New leader sees no `ConfigChange(joint)` in its log; no joint mode is active. Operator may retry the reconfiguration. (If the joint entry was *Accepted* but not yet *Chosen*, the new leader's bulk Phase-1 will resolve it like any other open slot.)
- **Leader crashes during `JointActive`.** The new leader runs election under the joint config (both-quorums needed for the election itself). Reconfiguration resumes from `JointActive`, awaiting catch-up or final propose.
- **Leader crashes during `Proposing Final`.** Same as above; if the entry is not chosen, the new leader either retries or, if it determines the operation should be aborted, proposes `ConfigChange(C_old)` to revert.
- **Catch-up of new member fails (timeout).** Reconfiguration aborts: the leader proposes `ConfigChange(C_old)` to revert from joint mode. The new member is removed from any cached state and the operator is alerted.
- **Network partition during reconfiguration.** Joint mode requires both-quorums. If either side of the prospective new membership cannot form a quorum, reconfiguration stalls until the partition heals.
- **Group-0 reconfiguration interrupts data-group reconfiguration.** Per §7.2, data-group reconfigurations pause; they resume after Group-0 settles.

In all cases, the group does not lose data; the worst outcome is a stalled or rolled-back reconfiguration with operator intervention.

---

## 10. Tunables and Defaults

| Parameter | Default | Range | Notes |
| --- | --- | --- | --- |
| `catchup_slack` | 100 slots | 10 – 10 000 | How close to leader's max_chosen the new member must be |
| `catchup_timeout` | 10 min | 1 min – 24 h | Abort threshold for catch-up |
| `reconfig_grace` | 5 s | 0 – 60 s | Wait between joint-applied and final-propose, useful for test reproducibility |
| `leader_transfer_timeout` | 5 s | 100 ms – 60 s | Max wait for `TimeoutNow` target to win election |

**Operational guidance:**

- Add members one at a time. Wait for `JointActive → Idle` before starting the next.
- For 3 → 5: do two single-add reconfigurations.
- For 7 → 3: do four single-remove reconfigurations.
- Always leader-transfer off a node before removing it (the system will do this automatically via §6.2 but admins can do it explicitly to control the new leader choice).
- Monitor `safe_slot` during reconfiguration — a stalled `safe_slot` is a sign that catch-up is not progressing.
