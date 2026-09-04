<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R131: diskdb — Allocation Throughput

## Problem

The production-path DiskDB allocation benchmark currently reaches about 61K
one-unit allocations/s at 64 workers on the Linux reference host. A
four-block request reaches about 136K allocated units/s at 16 workers. The allocator's
bitmap claim is lock-free, but a successful client request also crosses
RPC dispatch, async scheduling, record construction, KV batch persistence,
Paxos, WAL, storage apply, and response scheduling. We have not isolated
these costs or tuned the factors that control them.

Without a layer-by-layer profile, changing concurrency or batching can move
the bottleneck and hide tail-latency, durability, or correctness regressions.
The benchmark fixture also needs enough logical capacity to sustain the
target rate for the complete measurement window without entering the
exhaustion and compaction paths.

Design pointers: allocation behavior is defined by
`doc/design/diskdb/design-crowdb-diskdb-zone-management.md` §4 and its
lock-free concurrency contract by §8. The server concurrency model and
tunables are in `doc/design/diskdb/design-crowdb-diskdb.md` §12 and §14.
RPC scheduling and backpressure are defined in
`doc/design/rpc/design-crowdb-rpc.md` §4.

Use scenarios:

- An engineer runs the 20-second allocation regression on the Linux
  reference host. The workload sustains at least 400K allocations/s without
  exhausting logical space, reports zero errors, and verifies exact busy
  space.
- An engineer compares an optimization with the recorded baseline. Stage
  timings and saturation counters identify which layer improved and expose
  any bottleneck that moved downstream.
- An engineer sweeps request size, client concurrency, RPC workers, KV/WAL
  settings, and allocator tunables. Results are reproducible and retain
  latency and correctness data instead of selecting a TPS number alone.
- A change improves an isolated layer but reduces full-path throughput or
  worsens p99 latency. The regression history preserves both measurements
  and rejects the change as a full-path improvement.
- An engineer opens one timestamped regression root after a run and compares
  CLI, DiskDB, KV, RPC, WAL, storage, CPU, memory, and network counters for
  the same workload window.

## Solution

Instrument and benchmark each allocation stage, tune only measured
bottlenecks, and retain the results in a permanent allocation-flow analysis.

1. **Allocation flow analysis** — add
   `doc/design/diskdb/diskdb-allocate-flow-analysis.md`, following the KV
   write-flow analysis format. Trace the client, crowdb-rpc, diskdb bitmap
   claim, KV batch write, consensus/WAL/apply, and response path; record
   reference hardware, exact commands, results, flamegraphs or profiles,
   bottleneck evidence, and change history.

2. **Stage observability** — extend existing metrics in
   `crowdb-diskdb-client`, `app/crowdb-diskdb/src/service/`,
   `app/crowdb-diskdb/src/model/alloc.rs`, and the KV client path so the
   benchmark can attribute request time to client scheduling, transport,
   server scheduling, bitmap claim, persistence, and response delivery.
   Add counters for queue pressure, CAS retries, partial/rolled-back batches,
   and KV errors. Hot-path collection remains lock-free.

3. **Layer-isolation benchmarks** — extend `crowdb-cli bench diskdb` with
   diagnostic modes or repository-owned fixtures that measure bitmap claim,
   diskdb RPC without persistence, direct KV persistence, and the complete
   durable path using comparable request shapes. Diagnostic modes must be
   explicit and cannot be reported as durable allocation throughput.

4. **Tunables sweep** — extend `tools/bench-diskdb-regression.sh` to cover
   the confirmed performance controls: client concurrency and connections,
   request block count, crowdb-rpc client/server workers and engines,
   response write mode, allocator zone rotation and CAS retry limits, KV
   proposal coalescing/inflight limits, WAL settings, and storage mode.
   Preserve single-worker latency and multi-worker saturation cases.

5. **Bottleneck fixes** — optimize the stages proven dominant by the
   isolation results. Candidate directions include reducing per-request
   allocation and serialization, batching busy-record persistence, avoiding
   redundant task hops, increasing safe parallelism, and aligning DiskDB
   request batches with KV proposal coalescing. Each accepted change needs
   an A/B result and must preserve allocation durability and rollback.

6. **Regression sentinel** — keep a 20-second, one-unit, full durable-path
   case with at least 8,000,000 units of usable capacity. Record throughput,
   p50/p99 latency, errors, stop reason, allocated units, and exact busy-space
   delta in the regression script history.

7. **Production lifecycle and retained metrics** — keep
   `tools/bench-diskdb-regression.sh` on the `crowdb-cli cluster local-deploy`
   path. Each case creates three memory-backed KV nodes, group 0 plus one or
   more data groups, three DiskDB instances, and twelve 1-TiB logical disks.
   All commands share one run root while each deploy, group operation,
   benchmark, and destroy invocation gets its own command-and-datetime
   folder. Retain KV-server, DiskDB-server, RPC, and CLI metrics logs.

8. **Compaction correctness prerequisite** — fix the snapshot/watermark race
   exposed by high-concurrency mix runs before accepting throughput changes.
   A compaction scan must not advance its watermark past durable free records
   omitted by that scan; later compaction must reclaim them rather than mark
   them stale.

```text
crowdb-cli workers
        |
        v
client encode/schedule -> crowdb-rpc transport -> diskdb RPC dispatch
                                                |
                                                v
                                    lock-free bitmap claim
                                                |
                                                v
                                      KV batch persistence
                                                |
                              Paxos -> WAL -> storage apply
                                                |
                                                v
                               response encode/schedule -> client

Isolation: bitmap-only | RPC-only | KV-only | full durable path
```

Edge cases at a glance:

- Fixture reaches `NoSpace` before the deadline → invalidate the throughput
  sample and increase benchmark-only logical capacity.
- An allocation persistence failure occurs → roll back every bitmap claim
  and count the request as an error.
- A batch returns fewer segments than requested → fail correctness
  verification; do not count it as successful throughput.
- Throughput rises while p99 or error count regresses → retain the result but
  do not promote it as the new reference.
- A diagnostic non-durable mode outperforms the full path → label it only as
  an isolation result.
- CAS retries or queues saturate → report the saturation counter with the
  corresponding stage latency.
- A compaction scan omits a concurrent durable free → a later scan reclaims
  it; the prior watermark cannot turn an unprocessed free into a stale one.

Current production-path baseline on the AMD Ryzen 9 5950X, 2026-09-04:

- One-block allocate: 2,579 ops/s at 1 worker, 10,932 at 4, 38,962 at
  16, and 60,642 at 64. All four cases ran for 20 seconds with zero errors
  and exact space accounting.
- At one worker, DiskDB metrics attribute about 297 us to KV persistence,
  299 us to the complete server RPC handler, and 1 us to bitmap scanning.
  The first optimization pass therefore belongs in persistence, batching,
  and KV proposal flow rather than the lock-free bitmap claim.
- Four-block allocate at 16 workers: 135,511 allocated units/s, p50 453 us,
  p99 801 us, zero errors, exact 2,842,324,631,552-byte busy delta.
- One-block mix: 2,593 ops/s at 1 worker, 11,386 at 4, and 37,572 at 16,
  with zero errors and exact accounting.
- The 64-worker one-block mix and 16-worker four-block mix are invalid
  throughput baselines. They reproduced leaked busy bits after acknowledged
  frees: 2,189 and 304 units respectively. Their logs show compaction scans
  later treating missed free records as stale.
- Verified run roots:
  `bench-log/diskdb-regression-20260904-135217` and
  `bench-log/diskdb-regression-20260904-140333`. Generated logs and TSV files
  stay untracked; the script comment is the retained history.

## Dependencies

- Depends on R130's `crowdb-cli bench diskdb`, self-contained fixture,
  regression script, and space-accounting verification.
- Uses the existing crowdb-rpc latency metrics and KV benchmark methodology;
  no RPC or KV semantic change is required to begin analysis.
- If busy-record batching changes durability or allocation response
  semantics, its design must be reviewed before implementation. R79 covers
  free batching only and does not provide allocate batching.
- R113 may later reuse allocation batching results, but it is not required
  for this work.

## Acceptance

**Capacity and correctness**:

- Start the benchmark fixture with 3 disk groups, 4 disks per group, 4 zones
  per disk, 262,144 one-MiB units per zone → query topology and verify
  12,582,912 units (12 TiB logical) of capacity. E2E test.
- Run one-unit allocation for 20 seconds at up to 400K allocations/s → the
  workload reaches the deadline rather than `NoSpace`, returns zero errors,
  and reports `busy_delta == expected_delta`. E2E test.
- Inject a KV persistence failure after bitmap claims → verify all claims are
  rolled back, no busy record is leaked, and the rollback counter matches the
  failed batch. Integration test.
- Return fewer segments than requested from a diagnostic fixture → verify the
  benchmark exits non-zero instead of including a partial allocation in TPS.
  Integration test.
- Persist frees concurrently with a compaction scan, including records that
  fall behind the scan watermark → compact again and verify every
  acknowledged free clears its bitmap range and exact busy-space accounting
  holds. Integration test.

**Flow attribution**:

- Run bitmap-only, RPC-only, KV-only, and full-path cases with the same
  one-unit request shape → record throughput and stage latency for each, with
  non-durable cases clearly labeled. Integration test.
- Saturate one controlled stage at a time → verify its queue/CAS/error counter
  increases and its latency accounts for the full-path slowdown. Integration
  test.
- Compare summed stage timings with end-to-end latency → verify the unexplained
  remainder is reported rather than silently attributed to a layer.
  Integration test.

**Tuning and regression**:

- Sweep single-worker and saturation concurrency, connection/worker counts,
  one- and multi-block requests, allocator controls, and KV/WAL controls →
  save exact parameters with TPS, p50, p99, errors, and correctness fields.
  E2E test.
- On the AMD Ryzen 9 5950X Linux reference host, run the selected release
  configuration for 20 seconds → sustain at least 400K one-unit durable
  allocations/s, zero errors, deadline stop, and exact space accounting.
  E2E test.
- Repeat the selected configuration three times → every run remains within
  10% of the median throughput, has zero errors, and passes exact accounting.
  E2E test.
- Run the single-worker case before and after each accepted optimization → p99
  does not regress by more than 10% unless the flow analysis records and
  justifies the trade-off. E2E test.
- Run one complete regression case → verify its shared timestamped root has
  separate timestamped KV deploy, group-create, DiskDB deploy, benchmark, and
  destroy folders; verify three KV metrics logs, three DiskDB metrics logs,
  and one CLI metrics log contain samples for the workload. E2E test.
- Run all cases when one mix case fails accounting → verify later cases still
  execute, results are retained, and the script exits non-zero after the
  complete matrix. E2E test.

Run `pixi run -- cargo test -p crowdb-diskdb-client --tests`, the added
DiskDB layer-isolation test targets, and
`DISKDB_BENCH_DURATION=20 bash tools/bench-diskdb-regression.sh`.
Run `pixi run -- cargo fmt --all -- --check` and
`pixi run -- cargo clippy --all-targets -- -D warnings` before completion.
