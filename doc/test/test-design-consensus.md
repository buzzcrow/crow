# CrowKV - Test Design: Consensus Core

Depends on: [`test-design.md`](test/test-design.md), [`design.md`](design.md) §3–5, [`design-leader-election.md`](design-leader-election.md), [`design-parallel-slots.md`](design-parallel-slots.md)
Satisfies: [requirement.md §14.1](requirement.md#141-correctness-criteria-for-crowbench), [requirement.md §14.2](requirement.md#142-failure-scenarios-must-be-covered-in-test-designmd)

Enumerates invariants, unit tests, and integration scenarios for the Phase 1 consensus core.

## 1. Invariants

| ID | Claim | Trigger | Ref |
|---|---|---|---|
| C1 | At most one leader per term | After every `RequestVote` / heartbeat | [`design-leader-election.md`](design-leader-election.md) §9.1 |
| C2 | Ballot monotonic per slot | Every `Prepare`/`Accept` handler | [`design.md`](design.md) §4.2 |
| C3 | Chosen value immutable | Slot transitions to `Chosen` | [`design-leader-election.md`](design-leader-election.md) §9.2 |
| C4 | Slot order = real-time ack order | Every client write ack | [`design-parallel-slots.md`](design-parallel-slots.md) §2 I1 |
| C5 | Safe-slot contiguous | Every heartbeat aggregation | [`design-parallel-slots.md`](design-parallel-slots.md) §7 |
| C6 | Per-key resolved-slot monotone | Every `apply` | [`design-parallel-slots.md`](design-parallel-slots.md) §6 |
| C7 | Dedup cache idempotent | Every write request | [`design.md`](design.md) §8.6 |
| C8 | Liveness under quorum | Heartbeats + write attempts | [`design-leader-election.md`](design-leader-election.md) §3 |
| C9 | Window backpressure bounded | Every admission decision | [`design-parallel-slots.md`](design-parallel-slots.md) §4 |
| C10 | Lease never overlaps | Two leaders coexist in time | [`design-leader-election.md`](design-leader-election.md) §6 |
| C11 | Lease-invalid → ReadIndex fallback | Linearizable read on leader without valid lease | [`design-leader-election.md`](design-leader-election.md) §7 |
| C12 | Lease-unrenewable → step-down | Leader cannot reach quorum to renew | [`design-leader-election.md`](design-leader-election.md) §8 |

**Liveness (C8):** with a stable quorum and bounded message delay, every admitted client write reaches `Chosen` within bounded time. Tested by injecting bounded delays and asserting completion within `delay + window × RTT + repair_tick`.

**Backpressure (C9):** with the in-flight window full, the admission queue accepts up to `admit_queue_depth` more requests then returns `Busy`; no request waits longer than `admit_queue_timeout`.

## 2. Unit Test Matrix

| Module | Test | Setup | Expected |
|---|---|---|---|
| `acceptor` | `prepare_promise` | Fresh, `Prepare(b=1)` | Records promise, returns `None` |
| `acceptor` | `reject_lower_ballot` | Promised b=2, `Prepare(b=1)` | Rejected |
| `acceptor` | `accept_after_promise` | Promised b=1, `Accept(b=1, v=X)` | Records accepted |
| `election` | `elect_single_leader` | 3 nodes, randomized timeouts | Exactly 1 leader |
| `election` | `step_down_higher_term` | Leader term=2, hb term=3 | Reverts to follower |
| `election` | `bulk_phase1_adopts` | Crash after 1 `Accept` | New leader re-Accepts same value |
| `proposer` | `parallel_window` | Window=16, 20 writes | 16 immediate, 17th queued, 21st `Busy` |
| `proposer` | `quorum_bitmap` | 5 nodes, 3 `Accepted` | Slot `Chosen` |
| `repair` | `gap_repair_noop` | No accepted value at slot | Fills `NoOp` |
| `repair` | `gap_repair_preserve` | Accepted on 1 acceptor | Re-Accepts same value |
| `learner` | `apply_out_of_order` | Apply slot 5 then 3 for key K | Final value from slot 5 |
| `learner` | `contiguous_applied` | Chosen [1,3,2] | Advances 1→2→3 |
| `dedup` | `repeat_same_seq` | Retry (cid=1,seq=5) | Same slot, no new assignment |
| `lease` | `valid_lease_fast_read` | Heartbeats acked, time within `lease_duration` | Linearizable read served locally, no quorum round-trip |
| `lease` | `expired_lease_readindex` | `TestTimer::advance(>lease_duration)`, no new heartbeats | Linearizable read triggers ReadIndex (quorum heartbeat) |
| `lease` | `unrenewable_step_down` | Partition leader from quorum, advance past `step_down_threshold` | Leader transitions to follower; subsequent reads return `NotLeader` |
| `lease` | `clock_skew_bound` | `TestTimer::skew(follower, +max_skew)` | Effective lease shortens; correctness preserved |
| `engine` | `compare_equal` | Identical ops on two engines | Empty diff |
| `engine` | `compare_divergent` | One missed slot 3 | Reports mismatch |

Note: `engine.compare_equal` / `compare_divergent` overlap with [`test-design-storage.md`](test/test-design-storage.md) §2. Owned by consensus (P1) for the in-memory engine; storage (P3) extends to ordered-file. Avoid duplication when implementing — share test fixtures.

## 3. Integration Scenarios

**S1 — Leader change mid-write:**
1. L1 assigns N, sends `Accept`, only 1 acceptor receives it, L1 crashes.
2. L2 elected, bulk Phase 1 discovers (N,v).
3. L2 re-Accepts v; client retry sees same result.
4. All learners `compare()` equal.

**S2 — Parallel slots with gap:**
1. Window=16, drop `Accept` for slot 5 on 2 acceptors.
2. Slots 6–21 chosen, safe-slot stalls at 4.
3. Repair detects gap, resolves slot 5, safe-slot advances to 21.
4. `compare()` equal.

**S3 — Partition minority:**
1. Majority (2 nodes) continue, minority (1 node) partitioned.
2. Majority elects new leader, serves writes.
3. Minority stops serving, on heal catches up via repair.
4. `compare()` equal after heal.

## 4. Failure Injection

Method names match `test-design.md` §4 and `plan-consensus.md` §4.

| Failure | Unit sim | Invariant tested | Assertion |
|---|---|---|---|
| Node crash + restart | `TestNode::crash()` + `restart()` | C3, C8 | Restarted node catches up; state equal across all learners |
| Partition | `TestRouter::partition()` / `heal()` | C1, C8 | Minority rejects writes; majority continues; on heal, minority catches up |
| Delay | `TestRouter::delay(from, to, ms)` | C5, C8 | Gaps eventually repaired; no deadlock; safe-slot resumes advance |
| Loss | `TestRouter::drop(from, to, pct)` | C8 | Retry + repair restore liveness within bounded time |
| Forced step-down | `TestNode::force_step_down()` | C1, C3 | New leader elected; no split-brain; chosen values preserved |
| Clock skew | `TestTimer::skew(node, ms)` | C1, C10 | Lease never overlaps; ReadIndex fallback engages |
| Lease unrenewable | partition + `TestTimer::advance(>step_down_threshold)` | C12 | Leader steps down; new election proceeds |

## 5. Resolved Decisions

- **Op generation:** fixed deterministic sequences for M1–M4 unit tests; `proptest` for the M5 dedup invariant and the G1 random-1000-ops gate.
- **2-group smoke:** include a single 2-group integration scenario in P1 to exercise Group Manager dispatch (`integration_two_group_smoke`).
