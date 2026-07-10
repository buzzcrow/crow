# CrowKV - Test Plan: Consensus Core

Depends on: [`test-plan.md`](test/test-plan.md), [`test-design-consensus.md`](test/test-design-consensus.md), [`plan-consensus.md`](plan/plan-consensus.md), [`design-rpc.md`](design/design-rpc.md)
Satisfies: [requirement.md §14.1](requirement.md#141-correctness-criteria-for-crowbench), [requirement.md §14.2](requirement.md#142-failure-scenarios-must-be-covered-in-test-designmd)

Execution plan for Phase 1 consensus core tests. Most tests run in-process via `test_harness` (no gRPC, no real timers). M2 adds a loopback-server flavor: tests spawn `Node` tasks bound to `127.0.0.1:0` and exercise the minimal Paxos RPC service over real gRPC. The `crowkv-server` binary is never invoked by tests.

## 1. Milestone Test Gates

| Plan Milestone | Test Set | Target Duration | Gate |
|---|---|---|---|
| M1 — Core types + acceptor | `acceptor.rs` unit tests + C2 invariant | < 5 s | All pass |
| M2 — Wire protocol + three Paxos flows | `rpc` unit + S0-A/B/C/D loopback scenarios (classic, optimized, multi, rejection) | < 15 s | All pass |
| M3 — Election + bulk Phase 1 | `election.rs` unit + S1 scenario | < 10 s | All pass |
| M4 — Proposer + pipeline | `proposer.rs` + `repair.rs` unit + S2 scenario | < 15 s | All pass |
| M5 — Learner + engine + lease + reads | `learner.rs` + `engine.rs` + `lease.rs` unit + S3 scenario + lease-skew/expiry/step-down tests (C10–C12) | < 15 s | All pass |
| M6 — Dedup cache | `dedup.rs` unit + C7 invariant | < 5 s | All pass |
| **G1 — Core linearizable** | All M1–M6 + 1000 random ops harness scenario | < 30 s | **All pass; gates start of P4 (P2/P3 may proceed in parallel)** |

## 2. Test Execution

1. **Unit tests** run by `cargo test --lib`. Test order is not guaranteed by `cargo test`; per-module groupings are organizational only:
   - `types`, `messages`
   - `acceptor`, `election`
   - `proposer`, `replicator`, `repair`
   - `learner`, `engine`, `lease`, `dedup`, `group`
2. **Integration tests** run by `cargo test --test integration` using `test_harness.rs`:
   - `integration_loopback_classic_paxos` (S0-A, M2 gate; 3-node, full Phase 1+2)
   - `integration_loopback_optimized_paxos` (S0-B, M2 gate; 3-node, skip Phase 1)
   - `integration_loopback_multi_paxos` (S0-C, M2 gate; 5-node, ballot reuse 10 slots)
   - `integration_loopback_quorum_rejection` (S0-D, M2 gate; 5-node, 2 pre-promised higher)
   - `integration_election_3node`
   - `integration_leader_change_preserve` (S1)
   - `integration_parallel_slots_with_gap` (S2)
   - `integration_partition_minority` (S3)
   - `integration_random_ops_1000` (G1 gate)
3. **Deterministic seeds:** every integration test uses a fixed `seed: u64` logged on failure for reproduction.

## 3. Test Commands

```bash
# Suite A — unit only
cargo test --lib

# Suite B — integration only
cargo test --test integration

# Suite A+B — full P1 gate
cargo test
```

## 4. Failure Reproduction

On any integration test failure, the harness prints:
- Seed value
- Message delivery log (ordered by simulated time)
- Final state of every node (`contiguous_applied`, `current_term`, `is_leader`)
- `compare()` diff if state divergence detected

This is sufficient to reproduce the exact execution trace in a standalone script.

## 5. Resolved Decisions

- **2-group smoke:** add `integration_two_group_smoke` to the P1 integration suite.
- **Random-ops gate:** fixed deterministic seed sweep (seeds 0–999) for the G1 random-1000-ops gate — reproducible without an extra `proptest` dependency. (`proptest` is reserved for the M5 dedup invariant per `test-design-consensus.md` §5.)
