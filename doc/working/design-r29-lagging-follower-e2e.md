<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# R29 Design — Lagging-follower e2e for MinSlot fallback

## Problem

R26 shipped `read_endpoint_policy = AnyReplica` for `MinSlot` reads,
including a `NotLeader`-hint fallback: when a chosen follower's
`contiguous_applied` has not reached the client's `min_slot`, the server
returns `NotLeader { hint = leader_endpoint }` and the client follows the
hint, incrementing `read_endpoint_fallback`. The existing
`e2e_follower_read_test.rs` (a 2-node pinned cluster copied from
`e2e_retry_test.rs`) confirms distribution and linearizable isolation but
**cannot trigger the fallback path**: in a 2-node cluster both nodes must
vote for quorum, so both run their election driver and both apply on
accept — both replicas always share the same `contiguous_applied`. No
single e2e test proves the end-to-end flow: *distributed read → lagging
follower → NotLeader redirect → leader retry → read succeeds → counter
increments*.

## Current behavior (why the 2-node cluster can't lag)

`px_service.rs::on_accept` (the gRPC Accept handler) calls
`replica.learn_chosen(&entry, ...)` directly when the acceptor replies
`Accepted` (px_service.rs:547). `learn_chosen` advances
`contiguous_applied`. So any replica that receives an Accept RPC applies
immediately — regardless of whether its election driver is running.

## Why R29's stated mechanism does not work as written

R29 proposed: "C is a non-voting remote on A, election driver disabled via
`add_group_without_election`, so its learner never applies." This does not
hold, for two reasons:

- The proposer's Accept fan-out (`group.rs::run_accept_phase`, ~line 1623)
  sends `send_accept` to **every** real remote — the `voting` flag only
  gates quorum counting (~line 1647), not fan-out. A non-voting C still
  receives Accepts.
- `fan_out_chosen_notice` (group.rs:1741) likewise fans out to **every**
  real remote with no voting filter.
- Either path lands in `learn_chosen` on C, advancing `contiguous_applied`.

So a non-voting C wired as a remote on A would apply just like B and would
not lag. Disabling C's election driver does not help, because the apply
happens in the gRPC Accept handler, not in the election driver.

## Proposed approach (test-only, no production changes)

Build a 3-node cluster where the lagging follower is **not in the leader's
accept/notice fan-out at all**, so it deterministically never applies —
mirroring the real production shape of a learner or recently-rejoined node
that catches up via snapshot + WAL tail (not via the Accept fan-out).

- **A (id 1)** — `Leader`, voting. Group remotes = `[B]`. Election driver
  runs. A + B form the 2-voter quorum.
- **B (id 2)** — `Follower`, voting, believes A is leader. Group remotes =
  `[A]`. Election driver runs. B applies on accept (existing 2-node
  behavior).
- **C (id 3)** — `Follower`, **non-voting** (`with_voting(false)`), believes
  A is leader. Group remotes = `[A]` (only so C can resolve A's endpoint
  for the `NotLeader` hint via `leader_endpoint()`). Election driver
  **disabled** via `add_group_without_election` so C stays quiet (a
  non-voting follower with no heartbeats would otherwise time out and spin
  up elections). C is **not** a remote on A's group, so A never sends
  Accepts or chosen notices to C → C never applies → `contiguous_applied`
  stays 0, engine empty.

### Topology discovery

The client's `AnyReplica` selector round-robins over the replica list from
`/topology`. The replica list is built in `topology.rs::merge` from a
store's `listen_addr` (local) + `group.remotes` (remote endpoints). C is
not wired on A, so A's real `status()` lists only B as a remote — C would
be invisible to the selector.

To include C in the replica list without wiring C into A's group (which
would make C apply), the test's topology server serves a **hand-crafted
`StoreStatus`**: A's real `status()` with C appended to group 1's `remotes`
list (`RemoteStatus { id: 3, endpoint: C's bound addr, voting: false, .. }`).
The replica list becomes `[A, B, C]`. This is purely a test-harness
discovery artifact; A's actual group membership is unchanged, so no Accept
ever reaches C.

### Read flow against the lagging follower

- `get(k1, MinSlot, min_slot = 0)` hitting C: `contiguous_applied(0) >= 0`
  → `Serve` → empty engine → `NotFound`. (A/B return `Found`.)
- `get(k1, MinSlot, min_slot = write.revision)` hitting C:
  `contiguous_applied(0) < revision` → `NotLeader { hint = A's endpoint }`
  → client `follow_not_leader` → retry at A → `Found`;
  `read_endpoint_fallback` increments.
- `scan` mirrors get: `min_slot = 0` on C → 0 items; `min_slot = revision`
  on C → `"not leader; retry scan at {A}"` → `follow_scan_not_leader` →
  retry at A → 1 item; `read_endpoint_fallback` increments.

### Determinism

The round-robin cursor cycles `[A, B, C]` by index. Over 6 reads each
replica is hit exactly twice, so the lagging branch (C) fires
deterministically — no flakiness from random selection or election timing.

## Alternatives considered

- **Non-voting remote on A (R29's original proposal)** — rejected: Accept
  and chosen-notice fan-out ignore `voting`, so C would apply and not lag.
  Would require production changes (voting-gated fan-out) to make C lag,
  violating the test-only constraint.
- **C's gRPC server not started / unreachable endpoint on A** — rejected:
  reads routed to C would fail with a transport error, not a `NotLeader`
  redirect, so the fallback path would not be exercised.
- **C's group absent on C's store** — rejected: reads to C fail with "group
  not found", again not a `NotLeader` redirect.
- **Randomized replica selection** — rejected: round-robin is already
  deterministic and gives exact hit counts; no need for randomness.

## Acceptance

- `any_replica_falls_back_to_leader_when_follower_lags` (get): 6 reads over
  `[A, B, C]`, every read returns `Found`, `read_endpoint_distributed >= 6`,
  `read_endpoint_fallback >= 1`.
- `any_replica_scan_falls_back_when_follower_lags` (scan): 6 scans over
  `[A, B, C]`, every scan returns 1 item, `read_endpoint_distributed >= 6`,
  `read_endpoint_fallback >= 1`.
- `any_replica_distributes_minslot_reads_with_lagging_follower`
  (`min_slot = 0`): reads over `[A, B, C]` return `Found` (A, B) or
  `NotFound` (C, empty engine); both branches fire;
  `read_endpoint_fallback == 0`.
- `any_replica_scan_distributes_with_lagging_follower` (scan,
  `min_slot = 0`): scans return 1 item (A, B) or 0 items (C); both
  branches fire; `read_endpoint_fallback == 0`.
- Linearizable and `Leader`-policy tests still pass unchanged.

## Files

- `crowkv-client/tests/e2e_follower_read_test.rs` — generalized 3-node
  cluster helper, hand-crafted topology server, adapted distribution tests,
  two new fallback tests. No production code changes.
