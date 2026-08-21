<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROW - Design: Reconfiguration

Depends on: [`design-crow-kv.md`](design-crow-kv.md) §9.1, §9.2, [`design-crow-kv-leader-election.md`](design-crow-kv-leader-election.md)
Satisfies: [`design-crow-kv.md`](design-crow-kv.md) §9.1, §9.2

This document specifies how a CROW group safely changes its membership while preserving consensus safety. The mechanism is direct per-node HTTP mutation of each replica's remote-replica list, persisted to the local `GroupConfigStore`, with a `membership_epoch` exact-match fence. This model applies to all groups including the system group (group 0, which stores cluster topology metadata, see `design-crow-kv.md` §3.3).

## Table of Contents

- [1. Scope and Supported Transitions](#1-scope-and-supported-transitions)
- [2. Direct Per-Node Mutation Model](#2-direct-per-node-mutation-model)
- [3. New-Member Bootstrap](#3-new-member-bootstrap)
- [4. Member Removal](#4-member-removal)
- [5. Leader Transfer](#5-leader-transfer)
- [6. The `membership_epoch` Fence](#6-the-membership_epoch-fence)
- [7. Group-0 Special Cases](#7-group-0-special-cases)
- [8. Safety Argument](#8-safety-argument)
- [9. Failure During Reconfiguration](#9-failure-during-reconfiguration)
- [10. Tunables and Defaults](#10-tunables-and-defaults)

---

## 1. Scope and Supported Transitions

CROW supports membership changes within a single group. Specifically:

- **Add or remove voting members.** Adding a member when going 3 → 5 → 7 or removing a member when going 7 → 5 → 3.
- **Replace a member.** Implemented as add-then-remove (or vice versa), each as a single-member change.
- **Change the leadership of the group.** Triggered as a side effect when removing the current leader.

Out of scope (design-crow-kv.md §2](design-crow-kv.md)):

- Changing `num_groups` (the total number of groups in the cluster) — fixed at cluster creation.
- Splitting or merging groups — not supported.
- Going below 3 voting members — not supported (a 1-member group has no fault tolerance).
- Going above 7 voting members — not in the initial scope; quorum size grows linearly with membership and the marginal availability gain past 7 is small.

**Granularity:** every reconfiguration moves *exactly one member* in or out at a time. To go 3 → 5, do two single-member additions in sequence. Under the exact-match `membership_epoch` fence (§6) this single-member-at-a-time rule is no longer required for safety, but it is still recommended because it minimizes the propagation window during which writes stall.

---

## 2. Direct Per-Node Mutation Model

The design does not use `ConfigChange` log entries or a joint configuration. Instead, the operator (or `crow-console`) mutates the remote-replica list on each node independently through the HTTP management API:

- `POST /stores/:sid/groups/:gid/remotes` — add one or more remote replicas.
- `DELETE /stores/:sid/groups/:gid/remotes/:rid` — remove a remote replica.

Each call is atomically persisted to the local `GroupConfigStore` config file (atomic `tmp` + `fsync` + rename) and applied synchronously to the in-memory `PxGroup`. `membership_epoch` is bumped by exactly one when the call changes the voting set. A non-voting member addition/removal does **not** change the epoch or the quorum size.

Because there is no cluster-wide `ConfigChange` log, the quorum view is local to each node. This is safe because the `membership_epoch` fence (§6) forces all `Prepare`/`Accept` RPCs to be evaluated at the same epoch before their replies can count toward a quorum. A leader whose local epoch differs from a peer's rejects that peer's reply and converges upward (§6.3).

### 2.1 Propagation window and write stalls

A membership mutation is applied to one node per HTTP call. Between the first node's call and the last node's call, different nodes transiently disagree on the `membership_epoch`. During this window a leader that has already adopted the new epoch will see `epoch_mismatch` from nodes that have not yet adopted it. Writes are rejected by the acceptors and the leader's `propose` loop self-heals by adopting the responder's higher epoch (up to `max(own, peer)`) and retrying the same slot.

This is a deliberate availability/safety trade-off: writes stall for the cluster-wide duration of the fan-out (one HTTP round-trip per node), then resume immediately. The stall is bounded, self-healing, and loudly visible in the `epoch_mismatch` response bit rather than silently using a stale quorum.

---

## 3. New-Member Bootstrap

A new member joining a group has empty WAL and empty engine state. It must be brought up to date before it can vote.

### 3.1 Phases

```
   1. Console ensures the new node hosts the store and places the group on it.
   2. The new member is added as `voting: false` on every existing node.
   3. The new member is added as `voting: false` on itself (so it learns remotes).
   4. Snapshot install: a caught-up peer streams a snapshot to the new member.
   5. WAL catch-up: the new member follows the leader via normal replication.
   6. The new member is re-registered with `voting: true` on every node.
   7. `membership_epoch` is bumped on every node; the new member is now a voting peer.
```

### 3.2 Why pre-load before voting

If a new member were added directly with `voting: true` while still empty, then a quorum could form with its empty-state vote. A subsequent leader election under that quorum could elect a leader that has not seen older committed writes — violating Paxos safety.

By requiring catch-up while the member is non-voting, we ensure that when it first becomes a voting peer it already has all chosen values up to the current `max_chosen`.

### 3.3 Snapshot install protocol

Defined in [§8.4 of design-crow-kv.md](design-crow-kv.md#84-snapshot-and-install) and `design-crow-kv-state-machine.md` §6 (snapshot import). Resumable, throttled, end-to-end CRC.

### 3.4 Catch-up termination criterion

The leader continues streaming WAL until the new member's `contiguous_applied` is within `catchup_slack` (default 100 slots) of the leader's `max_chosen`. At that point the operator re-adds the member as `voting: true` and the new member is expected to keep up via normal replication once it becomes voting.

If the new member cannot keep up (e.g. slow disk), the operator can abort by removing it before flipping the `voting` flag.

---

## 4. Member Removal

Removing a member is simpler than adding one because there is no catch-up to wait for.

### 4.1 Procedure

```
   1. If the removed member is the current leader, ask it to StepDown first.
   2. Delete the removed replica as a remote from every surviving peer.
   3. Delete the local `PxGroup` on the removed member's node.
   4. The `membership_epoch` is bumped on every node where the voting set changed.
```

### 4.2 If the removed member is the leader

The console's `DELETE /api/stores/:sid/groups/:gid/replicas/:rid` handler calls `POST /stores/:sid/groups/:gid/step-down` on the leader node first. The leader rejects further proposals, the survivors run a normal election, and only after a new leader is observed does the removal proceed. If the step-down call fails (network partition, crashed process), the console proceeds anyway and the survivors' lease-unrenewable logic eventually elects a new leader.

### 4.3 If the removed member is unreachable

Removal works regardless of reachability. The surviving peers delete the dead replica from their remote lists, and the dead replica's local group is removed if the node is still reachable. The `membership_epoch` bump on each survivor ensures the remaining voters cannot be confused by delayed heartbeat/accept traffic from the removed node once it comes back. The removed node is then absent from every survivor's `remote_replica_info()`.

---

## 5. Leader Transfer

Transferring leadership is needed when:

- The current leader is being removed by reconfiguration.
- An admin wants to drain a node for maintenance.

### 5.1 Procedure

1. Admin or console calls `POST /stores/:sid/groups/:gid/step-down` on the leader node.
2. The leader strictly-fences the request (`self.id == target_leader_id && current term`) and calls `handle_step_down`, becoming a follower.
3. The old leader stops accepting new client writes; in-flight requests are handled by the new leader once elected.
4. Followers run a normal randomized-timeout election. The first up-to-date candidate to reach a quorum of the current voting set wins.
5. The old leader, on observing the new term in any RPC response, steps down.

This is not Raft's `TimeoutNow` fast transfer. It avoids the complexity of target-precondition checking and is accepted as sufficient because membership changes are operator-driven.

### 5.2 During reconfiguration

When removing the current leader:

1. The console calls step-down on the leader before issuing the removal.
2. The console waits (bounded, 5 s) for a survivor to report a leader *other than* the removed replica.
3. The removal proceeds; the new leader's `membership_epoch` now governs the group.

---

## 6. The `membership_epoch` Fence

Every `Prepare`/`Accept` RPC carries the proposer's current `membership_epoch`. The acceptor compares it to its own local `membership_epoch`. If they do not match exactly, the acceptor rejects the request with `epoch_mismatch: true` and attaches its own epoch. The proposer then adopts `max(own_epoch, responder_epoch)` and retries the same slot.

### 6.1 Why exact match, not "within 1"

Single-degree membership changes (old/new configs differing by exactly one voting member) have a proven quorum-overlap guarantee: any majority of `N` and any majority of `N±1` always share at least one member. That guarantee does **not** chain transitively across two sequential changes unless the first one fully converges everywhere. A counter-example: `C1 = {1,2,3}` (quorum 2) and `C3 = {1,2,3,4,5}` (quorum 3) have majorities `{2,3}` and `{1,4,5}` that do not intersect. So "one epoch behind is safe" is false in general. The only airtight rule is **exact epoch match**.

### 6.2 What exact match buys

Since a quorum decision can never straddle two epochs, the single-degree-at-a-time restriction stops being a safety requirement. A batch membership change (same voting set delta applied to multiple nodes in one fan-out) is just as safe as a single-member change; it only takes longer to converge.

### 6.3 Convergence and self-healing

Both the proposer and the acceptor converge upward to `max(own, peer)`:

- **Proposer (`PxGroup::propose`):** on `MembershipEpochMismatch`, adopts `responder_epoch` and retries the same slot.
- **Acceptor (`PxLocalReplica::on_prepare`/`on_accept`, heartbeat catch-up):** on `epoch_mismatch`, adopts `max(own, proposer_epoch)` before returning the rejection.

This bidirectional convergence ensures that even if the leader starts behind, it catches up within the same `propose()` call and the write succeeds as soon as the last straggler adopts the new epoch. The only cost is the bounded fan-out stall (§2.1).

### 6.4 Non-voting members must not count toward quorum

A non-voting catch-up member physically accepts and promises so it can follow the log, but its reply must not count toward the voting quorum. `PxGroup::run_prepare_phase` and `PxGroup::run_accept_phase` only increment `promised`/`accepted` for `RemoteReplicaKind::Real(remote)` when `remote.voting` is true. The local self-count is gated on `replica.voting()`. `PxGroup::propose` and `run_bulk_phase1` reuse `self.quorum()` (voting-only) rather than computing a voting-agnostic threshold.

---

## 7. Group-0 Special Cases

Group 0 (store 0, group 0) is the system group that stores cluster
topology metadata. It uses the same direct HTTP mutation +
`membership_epoch` fence reconfiguration model (§2) as all other
groups. No joint-consensus primitive is needed. Group 0 stores
cluster topology as KV entries under text-path keys (`/hw/rack/...`,
`/hw/node/...`, `/kv/store/...`, `/kv/group/...`) with JSON
values, written by `HardwareClient` and `KVClusterMetaClient` in
`crow-kv-client`. It is created via `POST /system/init`. See
`design-crow-kv.md` §3.3, `design-crow-kv-group0.md`, and
`../console/design-crow-console.md` §4.3 for details.

A system embedding `crow-kv`'s primitives may choose to build a
joint-consensus layer on top of the `membership_epoch` fence.

---

## 8. Safety Argument

The safety property we must preserve through reconfiguration is:

> **No two values can be chosen at the same slot.**

### 8.1 The single-epoch case

For a single fixed `membership_epoch`, standard Paxos safety applies: any two majorities of the voting set intersect. Therefore no two distinct values can both be chosen at the same slot.

### 8.2 The transition case

The only way two different values could be chosen at the same slot is if two disjoint quorums from different epochs both accepted. The `membership_epoch` fence prevents this: a `Prepare`/`Accept` reply is only counted toward quorum if the request's epoch exactly matches the acceptor's epoch. Two quorums can never be formed at different epochs for the same slot, because at least one of them would have required an epoch-mismatched reply and that reply would be rejected.

### 8.3 Why the fan-out window is safe

During the fan-out, nodes may transiently disagree on the epoch. A leader whose epoch is ahead sends `Prepare`/`Accept` with the higher epoch. Acceptor nodes still on the old epoch reject with `epoch_mismatch` and adopt the higher epoch. The leader's quorum count is therefore stalled until enough nodes have adopted the new epoch. The moment a quorum with the new epoch is reached, all nodes in that quorum share the same membership view, and Paxos safety holds.

### 8.4 Asymmetric transitions

The single-degree-at-a-time recommendation is not a safety requirement under the exact-match fence. Even a batch change (multiple voting members added or removed in one fan-out) is safe because the fence only permits quorums within one epoch. The trade-off is a longer propagation window and a correspondingly longer write stall.

---

## 9. Failure During Reconfiguration

Because membership is persisted per node and not via a consensus log, failure recovery is simpler:

- **Leader crashes after the first node has been mutated but before the fan-out completes.** The remaining nodes may have different epochs. The new leader, once elected, adopts the highest epoch it observes from any peer and continues the fan-out from the console. Writes self-heal as epochs converge.
- **A new member fails during catch-up.** The operator removes it before flipping `voting` to `true`. Because the member is non-voting, its failure cannot affect quorum safety.
- **Network partition during reconfiguration.** The partition side that cannot reach a quorum of the current voting set cannot choose values. The side with the newer epoch may stall until enough nodes adopt it. The partition heals; no split-brain is possible because the epoch fence prevents cross-epoch quorums.
- **Console crashes mid-fan-out.** The operator re-runs the same mutation on the remaining nodes. The `membership_epoch` bump is idempotent because it is a synchronous local increment; applying the same mutation twice leaves the epoch at the same value.

---

## 10. Tunables and Defaults

| Parameter | Default | Range | Notes |
| --- | --- | --- | --- |
| `catchup_slack` | 100 slots | 10 – 10 000 | How close to leader's `max_chosen` the new member must be before flipping `voting` |
| `catchup_timeout` | 10 min | 1 min – 24 h | Operator abort threshold for catch-up |
| `lease_duration_ms` | election-profile dependent | — | How long a removed/unreachable leader can remain leader before survivors re-elect |
| `step_down_timeout` | 5 s | 1 s – 60 s | How long the console waits for a new leader after step-down before proceeding |

**Operational guidance:**

- Add members one at a time. Wait for the catch-up to finish and `voting` to flip before starting the next member.
- For 3 → 5: do two single-add reconfigurations.
- For 7 → 3: do four single-remove reconfigurations.
- Always step-down a leader before removing it. If the leader is unreachable, proceed and rely on the lease-unrenewable fallback.
- Monitor `epoch_mismatch` responses and `membership_epoch` values during reconfiguration — a sustained `epoch_mismatch` is a sign that the fan-out is still in progress.
