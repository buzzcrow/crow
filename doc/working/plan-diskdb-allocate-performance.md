<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# DiskDB Allocation Throughput Plan

Upstream: [R131](../backlog/R131-diskdb-allocate-performance.md) and
[working design](design-diskdb-allocate-performance.md).

Goal: attribute and tune the durable allocation path to 400K one-unit
allocations/s while retaining latency, correctness, and cross-process logs.

## Phase 1: Review Checkpoint

- [x] **Review measurement design**: agree on the three transport legs,
  proposed counters, data-group comparison, and KV-sentinel-centered sweep
  before code changes.
  Files: `doc/working/design-diskdb-allocate-performance.md`.

## Phase 2: Parameter Plumbing

- [x] **Expose benchmark client transport**: add DiskDB connection-pool and
  client RPC-worker controls. Files: `app/crowdb-cli/src/commands/bench/verb.rs`,
  `app/crowdb-cli/src/commands/bench/diskdb.rs`,
  `lib/crowdb-diskdb-client/src/rpc_transport.rs`.
- [x] **Expose DiskDB server transport**: pass RPC workers through local
  deployment. Files: `app/crowdb-cli/src/commands/cluster.rs`,
  `lib/crowdb-console-shared/src/ops/cluster.rs`,
  `lib/crowdb-console-shared/src/lifecycle.rs`.
- [x] **Expose DiskDB-to-KV transport**: add static pool/worker configuration
  and construct the shared KV client from it. Files:
  `app/crowdb-diskdb/src/ddb_config.rs`, `app/crowdb-diskdb/src/main.rs`.

## Phase 3: Workflow Metrics

- [ ] **Instrument benchmark client**: add schedule, inflight, result, and
  unit-magnitude metrics without duplicating C++ RPC counters. Files:
  `app/crowdb-cli/src/commands/bench/{diskdb.rs,metrics.rs}`.
- [x] **Instrument DiskDB RPC methods**: add uniform per-method latency/count,
  inflight, and error metrics for every registered request type. Files:
  `app/crowdb-diskdb/src/metrics.rs`,
  `app/crowdb-diskdb/src/service/diskdb_rpc_service/{service.rs,mutations.rs,queries.rs,admin.rs}`.
- [x] **Instrument DiskDB allocation stages**: add record-build,
  response-build, rollback, partial, and error-reason metrics. Files:
  `app/crowdb-diskdb/src/metrics.rs`,
  `app/crowdb-diskdb/src/model/alloc.rs`,
  `app/crowdb-diskdb/src/service/diskdb_rpc_service/mutations.rs`.
- [~] **Instrument KV boundary**: add DiskDB-originated KV latency, inflight,
  record/byte magnitude, retry, and error metrics. Files:
  `app/crowdb-diskdb/src/ddb_kv_client.rs`,
  `app/crowdb-diskdb/src/metrics.rs`.

## Phase 4: Regression Harness and Experiments

- [ ] **Extend retained artifacts**: record resolved controls, workload window,
  RPC logs, manifest, and attribution TSV. Files:
  `tools/bench-diskdb-regression.sh`.
- [x] **Run discovery matrix**: compare the one-data-group diagnostic ceiling
  with the three-data-group fixture, then run the reviewed five-second narrow
  connection/worker sweep around the winning KV regression configuration;
  reject incorrect cases and select the best region. Files: `bench-log/`
  generated artifacts.
- [ ] **Run confirmation matrix**: run three 20-second repetitions plus
  one-worker, four-block, and mixed sentinels. Files: `bench-log/` generated
  artifacts.
- [x] **Record flow analysis**: add exact commands, hardware, results, metrics
  excerpts, log roots, attribution, and bottleneck conclusion. Files:
  `doc/design/diskdb/diskdb-allocate-flow-analysis.md`, `doc/doc_index.md`.

## Phase 5: Evidence-Driven Optimization

- [ ] **Implement measured bottleneck fix**: optimize only the dominant stage;
  update this plan with exact files after the experiment selects it.
- [ ] **Verify acceptance separately**: run unit, integration, layer-isolation,
  regression, formatting, and clippy gates through `pixi run`.
- [ ] **Review changes**: apply `/review`, resolve correctness and hot-path
  findings, and rerun affected gates.
- [ ] **Fold and clean up**: merge stable intent into permanent design, remove
  R131/backlog entry and working documents, and commit cleanup separately.

## Files

- `app/crowdb-cli/src/commands/bench/{verb.rs,diskdb.rs,metrics.rs}`
- `app/crowdb-cli/src/commands/cluster.rs`
- `lib/crowdb-diskdb-client/src/rpc_transport.rs`
- `lib/crowdb-console-shared/src/{lifecycle.rs,ops/cluster.rs}`
- `app/crowdb-diskdb/src/{main.rs,ddb_config.rs,ddb_kv_client.rs,metrics.rs}`
- `app/crowdb-diskdb/src/model/alloc.rs`
- `app/crowdb-diskdb/src/service/diskdb_rpc_service/{service.rs,mutations.rs,queries.rs,admin.rs}`
- `tools/bench-diskdb-regression.sh`
- `doc/design/diskdb/diskdb-allocate-flow-analysis.md`
- `doc/doc_index.md`

## Tests

- Unit: metric registration/snapshot for every RPC method, pool round-robin,
  argument validation, rollback and partial-response counters.
- Integration: connection-pool concurrency, persistence failure rollback,
  per-method success/error/inflight accounting, controlled inflight
  saturation, stage attribution.
- E2E: retained-log manifest, complete parameter rows, three-run durable target,
  one-worker latency guard, four-block throughput, mixed exact accounting.
- Gates: `pixi run -- cargo fmt --all -- --check`, affected crate clippy/tests,
  each affected task from `pixi task list`, and the R131 regression command.

## Blocked

The 400K one-unit durable allocation/s acceptance target is blocked by the
one-KV-write-per-allocation contract and the measured upstream KV ceiling. Six
root-cause-driven configurations were run on the reference host:

- three groups at 128/256/512/768 workers: 128,559 / 156,741 / 183,310 /
  193,977 ops/s;
- one group at 512 workers with 16/8 and 4/4 client/KV connections: 205,167
  and 206,266 ops/s for five seconds;
- the 4/4 configuration confirmed at 191,971 ops/s for 20 seconds.

All cases had zero errors, deadline stop, and exact space accounting. Metrics
attribute nearly all DiskDB handler time to KV persistence; bitmap scan is
below one microsecond. The direct KV write sentinel peaks near 264K writes/s,
also below 400K, so more DiskDB worker/connection tuning cannot satisfy the
target.

Continuation requires an architecture choice:

- raise the direct KV write ceiling above 400K before resuming DiskDB tuning;
- coalesce multiple DiskDB allocation operations into one DiskDB-to-KV write,
  with reviewed durability, response, and rollback semantics;
- redefine the target as allocated units/s using multi-block requests, which
  already exceeds 400K units/s but changes the acceptance meaning.
