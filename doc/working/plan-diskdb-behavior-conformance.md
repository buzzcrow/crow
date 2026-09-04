<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Diskdb Behavioral Conformance Plan

This plan implements
[`design-diskdb-behavior-conformance.md`](design-diskdb-behavior-conformance.md)
and [R130](../backlog/R130-diskdb-behavior-conformance.md): make diskdb
behavior feature-verifiable, assign immutable owners correctly, and add
the console CLI benchmark.

## Owner Creation

- [~] **Choose the group-0 linearization primitive**: decide conditional
  write dependency, dedicated group-0 operation, or distributed
  single-writer coordinator. Files:
  `doc/working/design-diskdb-behavior-conformance.md`.
- [ ] **Add atomic owner creation**: commit a disk-group with one
  least-loaded immutable owner and expose conflict/failure semantics.
  Files: `lib/crowdb-kv-client/src/hardware.rs`, selected KV protocol and
  server files.
- [ ] **Make creation mandatory**: return management failure when no owner
  is eligible or assignment fails; commit local configuration consistently.
  Files: `app/crowdb-web/src/lifecycle.rs`,
  `lib/crowdb-console-shared/src/ops/hardware.rs`.
- [ ] **Reject owner replacement**: replace unconditional owner mutation
  with create and same-owner renewal surfaces. Files:
  `app/crowdb-web/src/diskdb.rs`, `lib/crowdb-kv-client/src/hardware.rs`,
  `lib/crowdb-console-shared/src/clients/console.rs`.

## Group-0 Reconciliation

- [ ] **Build complete observed views**: apply no owner/bind/hardware delta
  after a partial read. Files:
  `app/crowdb-diskdb/src/liveness/keepalive.rs`.
- [ ] **Fence stale zone loads by generation**: publish loaded state only
  when owner and bind still match the observed generation. Files:
  `app/crowdb-diskdb/src/main.rs`,
  `app/crowdb-diskdb/src/liveness/keepalive.rs`.

## Feature Tests

- [ ] **Reorganize server integration tests**: group internal coverage by
  the documented feature vocabulary. Files: `app/crowdb-diskdb/tests/`,
  `app/crowdb-diskdb/Cargo.toml`.
- [ ] **Build client component E2E tests**: cover startup/sync, ownership,
  allocation lifecycle, recovery/compaction, hardware lifecycle,
  query/metrics, scanner/admin, and endpoint retry. Files:
  `lib/crowdb-diskdb-client/tests/`,
  `lib/crowdb-test-harness/src/diskdb.rs`.

## Console CLI Benchmark

- [ ] **Add diskdb benchmark verbs**: define allocate and mix commands,
  memory/block modes, and common workload arguments. Files:
  `app/crowdb-cli/src/commands/bench/verb.rs`,
  `app/crowdb-cli/src/commands/bench.rs`.
- [ ] **Add benchmark topology lifecycle**: provision and tear down three
  nodes, three disk-groups, and 12 disks. Files:
  `app/crowdb-cli/src/commands/bench/diskdb.rs`,
  `lib/crowdb-test-harness/src/diskdb.rs`.
- [ ] **Implement allocate workload**: stop on cluster-wide exhaustion or
  deadline and drain in-flight requests. Files:
  `app/crowdb-cli/src/commands/bench/diskdb_allocate.rs`.
- [ ] **Implement mixed workload**: issue deterministic 70/30 allocate/free
  operations against a non-duplicated live set. Files:
  `app/crowdb-cli/src/commands/bench/diskdb_mix.rs`.
- [ ] **Implement verification and reporting**: compare live segments,
  durable records, and compacted/recalculated capacity statistics. Files:
  `app/crowdb-cli/src/commands/bench/diskdb_verify.rs`,
  `app/crowdb-cli/src/commands/bench/result.rs`.

## File List

- `lib/crowdb-kv-client/src/hardware.rs` — owner creation and renewal.
- Selected KV protocol/server files — chosen linearization primitive.
- `app/crowdb-web/src/lifecycle.rs` — mandatory balanced assignment.
- `app/crowdb-web/src/diskdb.rs` — immutable owner handler.
- `lib/crowdb-console-shared/src/ops/hardware.rs` — creation outcome.
- `lib/crowdb-console-shared/src/clients/console.rs` — owner API surface.
- `app/crowdb-diskdb/src/liveness/keepalive.rs` — complete-view sync.
- `app/crowdb-diskdb/src/main.rs` — load generation.
- `app/crowdb-diskdb/tests/` — feature integration tests.
- `lib/crowdb-diskdb-client/tests/` — component E2E tests.
- `app/crowdb-cli/src/commands/bench/` — benchmark.
- `lib/crowdb-test-harness/src/diskdb.rs` — topology fixture.
- `doc/design/diskdb/design-crowdb-diskdb.md` — final folded design.

## Test Checklist

### Unit and integration

- [ ] Least-loaded selection produces 5/4/4 for 13 sequential creates.
- [ ] Concurrent create coordination keeps owner spread at most one.
- [ ] Owner replacement conflicts; same-owner renewal changes expiry only.
- [ ] Partial group-0 views do not alter last-known-good runtime state.
- [ ] Stale zone-load generation cannot publish.
- [ ] Mixed selector maintains live-set and accounting invariants.

### End to end

- [ ] Three owners and 13 management creates persist one owner each at
  5/4/4.
- [ ] Missing owner and owner-write failure reject disk-group creation.
- [ ] Complete startup becomes writable only after zone load.
- [ ] Allocate benchmark covers 12 disks and verifies exhaustion/deadline.
- [ ] Mixed benchmark verifies 70/30 operation selection, durable live set,
  and capacity totals.

### Commands

- [ ] `pixi run cargo fmt --all -- --check`
- [ ] `pixi run cargo clippy --all-targets -- -D warnings`
- [ ] `pixi run test-protocol`
- [ ] `pixi run test-kv-client`
- [ ] `pixi run clean-env && pixi run test-console-server`
- [ ] `pixi run clean-env && pixi run test-diskdb`
- [ ] `pixi run clean-env && pixi run test-diskdb-client`
- [ ] `pixi run clean-env && pixi run test-console-cli`

## Blocked

Concurrent least-loaded owner assignment needs a distributed
linearization point. The existing group-0 client provides blind
put/delete and atomic batches, but no conditional create or revision
guard. Atomic batching prevents a partial disk-group/owner pair but does
not prevent concurrent creators from selecting the same least-loaded
instance from identical snapshots.

The choices are:

- Depend on R101 conditional writes and use a guarded assignment counter
  or reservation. This is general and lock-free, but expands the critical
  path to an unfinished requirement.
- Add a dedicated group-0 `CreateDiskGroupWithOwner` operation. This is
  the narrowest correct behavior and gives a single linearization point,
  but adds protocol, server, and client code for one sysdata workflow.
- Require a distributed single-writer console coordinator. This avoids a
  KV primitive but adds leadership/failover machinery; a process-local
  mutex is both prohibited and insufficient.

The accepted concurrent balance and immutable-owner requirements cannot
be implemented correctly with the current blind-put surface. A user
decision is required before production code changes begin.
