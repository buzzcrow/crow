<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# R26 Plan — Follower Read Distribution for MinSlot

Design: `doc/working/design-r26.md`. Backlog entry:
`doc/backlog/R26-follower-read.md`.

## Task breakdown

- [ ] T1 — `crowkv-client/src/config.rs`: add `ReadEndpointPolicy` enum
      (`Leader`, `AnyReplica`, `Default = Leader`), `ClientConfig` field
      `read_endpoint_policy`, default in `ClientConfig::new`.
- [ ] T2 — `crowkv-client/src/topology.rs`: add `replicas:
      DashMap<(u64, u64), Vec<String>>`; populate in `merge` from
      `store.listen_addr` (local) + `group.remotes[*].endpoint`;
      accessor `replicas(store_id, group_id) -> Option<Vec<String>>`;
      extend the existing `#[cfg(test)]` sample-topology helper to
      cover the multi-replica case.
- [ ] T3 — `crowkv-client/src/metrics.rs`: add
      `read_endpoint_distributed` / `read_endpoint_fallback` atomics,
      `record_*` helpers, snapshot fields (`#[serde(default)]`).
- [ ] T4 — `crowkv-client/src/client.rs`:
      - store `read_endpoint_policy` on `CrowkvClient`;
      - add `read_rr: DashMap<(u64, u64), AtomicU64>`;
      - `resolve_read_endpoint(store_id, group_id, read_mode)`;
      - `get` / `scan` use it for the initial endpoint;
      - `follow_scan_not_leader` helper + scan retry branch;
      - increment `read_endpoint_distributed` on a distributed pick,
        `read_endpoint_fallback` on a followed fallback.
- [ ] T5 — `crowkv-client/src/lib.rs`: re-export `ReadEndpointPolicy`.
- [ ] T6 — Tests under `crowkv-client/tests/`:
      - `e2e_follower_read_test.rs` covering: default `Leader` policy
        unchanged; `AnyReplica` single-replica falls back to leader;
        `AnyReplica` two-replica MinSlot distributes; `AnyReplica`
        MinSlot fallback to leader on a not-caught-up follower;
        `AnyReplica` Linearizable still targets leader; scan
        distribution + fallback via the parsed error.
- [ ] T7 — Lint + relevant tests pass (`cargo fmt --check`,
      `cargo clippy -- -D warnings`, `cargo test -p crowkv-client`).

## File list

- `crowkv-client/src/config.rs` — new enum + field.
- `crowkv-client/src/topology.rs` — replica cache + accessor.
- `crowkv-client/src/metrics.rs` — two counters + snapshot fields.
- `crowkv-client/src/client.rs` — selector, branching, scan fallback.
- `crowkv-client/src/lib.rs` — re-export.
- `crowkv-client/tests/e2e_follower_read_test.rs` — new tests.

## Test checklist

- [ ] `cargo test -p crowkv-client` green.
- [ ] `e2e_single_node_test` / `e2e_retry_test` still green (default
      policy = `Leader` ⇒ no behavior change).
- [ ] New `e2e_follower_read_test` covers all six acceptance scenarios.
- [ ] `cargo fmt --check`, `cargo clippy -- -D warnings` clean.

## Post-merge cleanup

- Delete `doc/backlog/R26-follower-read.md` and its `backlog.md` entry.
- Delete `doc/working/design-r26.md` and `doc/working/plan-r26.md`.
- Fold the design into `design/design.md` §10 (Client Interaction) and
  `design/design-observability.md` (client metrics).
