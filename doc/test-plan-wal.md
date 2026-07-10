# CrowKV - Test Plan: WAL

Depends on: [`test-plan.md`](test-plan.md), [`test-design-wal.md`](test-design-wal.md), [`plan-wal.md`](plan-wal.md)
Satisfies: [requirement.md §8.1](requirement.md#81-wal-write-ahead-log)

Execution plan for Phase 2 WAL tests.

## 1. Milestone Test Gates

| Plan Milestone | Test Set | Gate |
|---|---|---|
| M1 — Segment layout | `segment.rs` unit | All pass |
| M2 — Batched fsync | `fsync_worker.rs` unit + throughput benchmark | All pass |
| M3 — Multi-disk | `manager.rs` unit + scale benchmark | All pass |
| M4 — Replay | `replay.rs` unit + `kill9_recovery` script | All pass |
| M5 — GC | `manager.rs` GC unit | All pass |
| **G2 — Persistent core** | All M1–M5 + integration crash test | **All pass; gates start of P4 (P3 may run in parallel with P2)** |

## 2. Test Commands

```bash
cargo test --lib wal
cargo test --test wal_integration
cargo bench -- wal_throughput   # if criterion added
```

## 3. Crash Test Script

```bash
# run node, send 1k writes, SIGKILL, restart, verify state
./scripts/crash_recovery_test.sh --records 1000 --nodes 3
```
