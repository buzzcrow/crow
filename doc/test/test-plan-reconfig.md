# CrowKV - Test Plan: Reconfiguration

Depends on: [`test-plan.md`](test/test-plan.md), [`test-design-reconfig.md`](test/test-design-reconfig.md), [`plan-reconfig.md`](plan/plan-reconfig.md)
Satisfies: [requirement.md §9.1](requirement.md#91-reconfiguration), [requirement.md §9.2](requirement.md#92-rolling-upgrade)

Execution plan for Phase 5 reconfiguration tests.

## 1. Milestone Test Gates

| Plan Milestone | Test Set | Gate |
|---|---|---|
| M1 — Snapshot install | Chunked transfer integration | All pass |
| M2 — Joint consensus | 3→5→3 membership integration | All pass |
| M3 — Leader transfer | `TimeoutNow` integration | All pass |
| M4 — Rolling upgrade | Mixed-version integration | All pass |
| **G5 — Elastic membership** | crowbench during reconfig, zero divergence | **All pass** |

## 2. Test Commands

```bash
cargo test --test reconfig_integration
cargo run --bin crowbench -- --scenario reconfig_3_to_5
```
