# CrowKV - Test Plan: RPC and Client

Depends on: [`test-plan.md`](test/test-plan.md), [`test-design-rpc.md`](test/test-design-rpc.md), [`plan-rpc.md`](plan/plan-rpc.md)
Satisfies: [requirement.md §10](requirement.md#10-client-interaction)

Execution plan for Phase 4 RPC tests.

## 1. Milestone Test Gates

| Plan Milestone | Test Set | Gate |
|---|---|---|
| M1 — Protobuf schema | `protobuf_roundtrip` unit | All pass |
| M2 — Node-to-node gRPC | 3-node loopback integration | All pass |
| M3 — Client library | `client.rs` unit + retry tests | All pass |
| M4 — Read mode routing | Mixed follower-read integration | All pass |
| **G4 — Networked cluster** | crowbench 10k ops zero divergence | **All pass** |

## 2. Test Commands

```bash
cargo test --lib rpc
cargo test --test rpc_integration
cargo run --bin crowbench -- --ops 10000 --nodes 3
```
