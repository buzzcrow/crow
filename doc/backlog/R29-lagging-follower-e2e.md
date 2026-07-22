<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R29: Lagging-follower e2e for MinSlot fallback

**Problem**: R26 shipped follower read distribution for `MinSlot` reads
(`read_endpoint_policy = AnyReplica`), including a fallback path: when a
chosen follower's `contiguous_applied` has not reached the client's
`min_slot`, the server returns `NotLeader { hint = leader_endpoint }`
and the client follows the hint to the leader, incrementing
`read_endpoint_fallback`. The existing e2e tests
(`e2e_follower_read_test.rs`) confirm distribution and linearizable
isolation, but **cannot trigger the fallback path**: in a 2-node pinned
cluster the follower applies the chosen value inside the `on_accept`
gRPC handler (`px_service.rs:547`) *before* the leader reaches quorum,
so both replicas always share the same `contiguous_applied`. The
fallback counter (`read_endpoint_fallback`) is therefore only exercised
indirectly — its increment is covered by the `follow_not_leader` branch
in `e2e_retry_test.rs`, and the scan-specific
`follow_scan_not_leader` parser is tested standalone, but no single e2e
test proves the end-to-end flow: *distributed read → lagging follower
→ NotLeader redirect → leader retry → read succeeds → counter
increments*.

**Use case (concrete example)**: A 3-node group (nodes A=leader, B, C)
serves a read-heavy workload with `read_endpoint_policy = AnyReplica`.
Node C is a non-voting learner that joined recently via snapshot + WAL
tail; its `contiguous_applied` is still catching up to the group's
`contiguous_chosen`. A client writes `k1 = v1` at revision 100, then
issues `get(k1, MinSlot, min_slot = 100)` to read its own write. The
round-robin selector picks node C. Node C's `contiguous_applied` is 80
(< 100), so `resolve_read_point` returns `NotLeader { hint = A }`. The
client follows the hint to node A, which has `contiguous_applied = 100`
and serves the read. `read_endpoint_distributed` increments (the
selector fired) and `read_endpoint_fallback` increments (the redirect
fired). The read returns `Found { value = v1 }` — the fallback is
transparent to the caller. Without this test, a regression that breaks
the redirect-follow (e.g. a stale hint, a missing counter increment, or
a scan parser that no longer matches the server's error string) would
only be caught in production.

**Approach**: Test-only change — no production code modification. Build
a 3-node pinned cluster where one node is a non-voting learner that
does not apply chosen values, creating a deterministic
`contiguous_applied` gap.

- 3 nodes: A (id 1, `Leader`, voting), B (id 2, `Follower`, voting), C
  (id 3, `Follower`, **non-voting**, election driver disabled via
  `add_group_without_election` so its learner never applies). A and B
  form a 2-voter quorum; C accepts proposals (its acceptor still runs)
  but its `contiguous_applied` stays 0.
- Topology server serves A's `/topology`, which lists all 3 endpoints
  (A local + B, C remotes) — the replica list the `AnyReplica` selector
  round-robins over.
- Client writes `k1 = v1` via `put` (quorum = A + B; C's acceptor
  accepts but does not apply). Capture `write.revision` as `min_slot`.
- Issue `get(k1, MinSlot, min_slot = 0)` in a loop. Reads that hit A or
  B return `Found`; reads that hit C return `NotFound` (C's engine is
  empty, `min_slot = 0` is served locally). Assert both branches fire
  and `read_endpoint_distributed >= N` with `read_endpoint_fallback == 0`.
- Issue `get(k1, MinSlot, min_slot = write.revision)` in a loop. Reads
  that hit A or B return `Found`; reads that hit C redirect to A via
  `NotLeader` hint and then return `Found`. Assert every read returns
  `Found`, `read_endpoint_distributed >= N`, and
  `read_endpoint_fallback >= 1` (at least one read hit C and fell back).
- Repeat both loops for `scan` with the same `min_slot` values. The
  scan fallback parses the server's
  `"not leader; retry scan at {endpoint}"` error string. Assert the
  same counter behavior.

**Why 3 nodes, not 2**: In a 2-node cluster both nodes must vote for
quorum, so both must run their election driver, so both apply on
accept — no lag. A 3-node cluster with one non-voting learner lets the
leader reach quorum (A + B) while C deterministically lags. This is
also the realistic production shape: a learner or a recently-rejoined
node catching up via snapshot + WAL tail.

**Concept change**: none — purely a test harness extension. Reuses the
existing `start_two_node_cluster` helper from
`e2e_follower_read_test.rs`, generalized to N nodes with per-node
`voting` and `spawn_driver` flags.

**Priority**: Low — the fallback code path is already covered by
`e2e_retry_test.rs` (get) and the standalone parser test (scan). This
test adds end-to-end confidence that the *combination* of distribution
+ fallback + counter increment works correctly, and would catch a
regression that only manifests when the selector picks a lagging
replica. Nice-to-have for test completeness, not blocking any feature.

**Complexity**: Low — test-only, ~150 lines, reuses existing cluster
helpers. The main work is generalizing `start_two_node_cluster` to
support a non-voting learner (per-node `voting` flag on
`PxLocalReplica`, per-node `add_group` vs
`add_group_without_election`).

**Dependencies**: R26 (shipped) — the `AnyReplica` policy, the
`read_endpoint_distributed` / `read_endpoint_fallback` counters, and
the `follow_scan_not_leader` parser must all exist.

**Files**: `crowkv-client/tests/e2e_follower_read_test.rs` (new test
functions + generalized cluster helper). No production code changes.

**Acceptance**:
- `any_replica_falls_back_to_leader_when_follower_lags` (get): 6 reads
  over [A, B, C], every read returns `Found`,
  `read_endpoint_distributed >= 6`, `read_endpoint_fallback >= 1`.
- `any_replica_scan_falls_back_when_follower_lags` (scan): 6 scans over
  [A, B, C], every scan returns 1 item,
  `read_endpoint_distributed >= 6`, `read_endpoint_fallback >= 1`.
- `any_replica_distributes_minslot_reads_with_lagging_follower`
  (`min_slot = 0`): reads over [A, B, C] return `Found` (A, B) or
  `NotFound` (C, empty engine); both branches fire;
  `read_endpoint_fallback == 0` (min_slot = 0 never redirects).
- Linearizable and `Leader`-policy tests still pass unchanged.
