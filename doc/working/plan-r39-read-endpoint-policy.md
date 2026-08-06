<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R39 Plan: Least-conn / latency read-endpoint policy

## Task breakdown

- [ ] T1: Add `LeastConnections`, `Latency` variants to
      `ReadEndpointPolicy` + `is_distributed()` helper (`config.rs`)
- [ ] T2: Add `EndpointStats` struct + `InFlightGuard` RAII +
      per-endpoint `DashMap` field on `CrowkvClient` (`client.rs`)
- [ ] T3: Implement selection logic in `resolve_read_endpoint` for
      the new policies (`client.rs`)
- [ ] T4: Add in-flight guard + RTT recording in `get` retry loop
      (`client.rs`)
- [ ] T5: Add in-flight guard + RTT recording in `scan` and
      `scan_stream` retry loops (`client.rs`)
- [ ] T6: Replace `== AnyReplica` checks with `is_distributed()` for
      `read_endpoint_fallback` counter (`client.rs`)
- [ ] T7: Update metrics doc comments (`metrics.rs`)
- [ ] T8: Add `test-util` get delay seam to `PxKvStore` (`px_kv_store.rs`)
- [ ] T9: Add `test-util` feature to `crow-kv-client` Cargo.toml +
      self dev-dependency
- [ ] T10: Update bench CLI parsing for new policy strings (`bench.rs`)
- [ ] T11: Write e2e tests — distribution, slow-replica bias, fallback,
      linearizable no-effect (`e2e_follower_read_test.rs`)
- [ ] T12: Run `cargo test -p crow-kv-client --all-targets` and fix
      failures
- [ ] T13: Run lint gate — `cargo fmt --check`, `cargo clippy --
      -D warnings`
- [ ] T14: Commit implementation + design + plan docs
- [ ] T15: Run full test suite (`pixi run test-suite`)
- [ ] T16: Merge design into `design-crow-kv.md` §10
- [ ] T17: Delete working docs + R39 backlog entry, commit cleanup
- [ ] T18: Run local CI check

## File changes

| File | Change |
| --- | --- |
| `lib/crow-kv-client/src/config.rs` | Add `LeastConnections`, `Latency` variants; `is_distributed()` method |
| `lib/crow-kv-client/src/client.rs` | `EndpointStats`, `InFlightGuard`, per-endpoint state, selection logic, in-flight/RTT in get/scan/scan_stream, `is_distributed()` for fallback counter |
| `lib/crow-kv-client/src/metrics.rs` | Doc comment updates for `read_endpoint_distributed`/`read_endpoint_fallback` |
| `lib/crow-kv-client/Cargo.toml` | `test-util` feature + self dev-dependency |
| `lib/crow-kv/src/cluster/px_kv_store.rs` | `#[cfg(feature = "test-util")]` get delay seam |
| `app/crow-cli/src/commands/bench.rs` | Parse `least-connections` / `latency` policy strings |
| `lib/crow-kv-client/tests/e2e_follower_read_test.rs` | New tests for `LeastConnections` and `Latency` |
| `doc/design/kv/design-crow-kv.md` | §10 update with new policies (merge step) |

## Test checklist

- [ ] `least_connections_distributes_minslot_reads` — distribution
- [ ] `latency_distributes_minslot_reads` — distribution
- [ ] `least_connections_routes_away_from_slow_replica` — bias
- [ ] `latency_routes_away_from_slow_replica` — bias
- [ ] `least_connections_falls_back_to_leader` — fallback
- [ ] `latency_falls_back_to_leader` — fallback
- [ ] `new_policies_linearizable_still_targets_leader` — no-effect
- [ ] Existing `any_replica_*` tests still pass (no regression)
