<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# DiskDB Allocation Throughput (R131)

This draft implements [R131](../backlog/R131-diskdb-allocate-performance.md)
within the allocation and concurrency contracts in
[the DiskDB root design](../design/diskdb/design-crowdb-diskdb.md),
[zone management](../design/diskdb/design-crowdb-diskdb-zone-management.md),
and [crowdb-rpc](../design/rpc/design-crowdb-rpc.md). R130's production
benchmark fixture and retained-log lifecycle have landed. R132's compaction
fix has landed; the high-concurrency mixed workload remains a correctness
sentinel until it is rerun.

## 1. Measurement Model

Each successful allocation performs one DiskDB RPC and one KV `batch_write`.
`blocks_per_request` changes the number of busy records in that batch, not the
number of KV proposals. The current one-worker result attributes about 297 us
of a 299 us DiskDB handler to KV persistence and about 1 us to bitmap claim.
The first experiment therefore changes concurrency around the existing KV
write, not the allocation data flow. The starting configuration comes from
`tools/bench-kv-write-regression.sh`; R131 does not repeat its broad KV sweep.

On the same AMD Ryzen 9 5950X reference host, the KV sentinel's best recorded
run reached about 264K writes/s at 512 tasks and 16 client connections. It used
four client RPC workers, four KV server RPC workers, `event_write=true`, peer
pool 4, `max_inflight=64`, `coalesce_max_keys=64`, and
`coalesce_drain_threshold=1`. It observed about 58 client writes per WAL append
and no inflight-window enqueue events. This is evidence that native RPC and KV
aggregation work and that raising the window alone is not the first lever.

One DiskDB allocation produces one KV client call even when KV coalescing later
combines several calls into one Paxos slot. The measured single-data-group
raw-KV peak is below R131's 400K allocation/s target. The discovery matrix
compares one and three data groups because all groups share the same three KV
processes and CPUs; capacity is not assumed to scale linearly. The measured
comparison selects one group for the performance sentinel because three groups
reduce throughput on the reference host.

Keep the following controls distinct:

1. `workload_concurrency`: Tokio tasks in `crowdb-cli bench diskdb`; each task
   keeps one request outstanding.
2. `diskdb_connections` and `diskdb_client_rpc_workers`: connections and C++
   I/O workers on the CLI-to-DiskDB leg.
3. `diskdb_rpc_workers`: C++ I/O workers accepting DiskDB requests. The handler
   immediately spawns the async allocation on DiskDB's Tokio runtime.
4. `kv_connections` and `kv_client_rpc_workers`: per-endpoint connection pool
   and C++ I/O workers shared by DiskDB's KV client.
5. `kv_rpc_workers`, `peer_pool_size`, `max_inflight`,
   `coalesce_max_keys`, and `coalesce_drain_threshold`: KV server admission,
   proposal aggregation, and consensus transport controls.

One connection can aggregate concurrent frames in crowdb-rpc. Additional
DiskDB-layer batching is not introduced merely to create aggregation. A new
batching mechanism is considered only if the measured KV proposal path stays
dominant after connection, worker, inflight, and native KV coalescing sweeps.

The durable-path latency decomposition is:

```text
bench.e2e
  = bench.schedule + CLI rpc.request.e2e
  = network/dispatch + diskdb.allocate.rpc
  = bitmap_claim + record_build + diskdb.kv_persist + response_schedule
  = KV client RPC + KV batch_write + Paxos/WAL/apply + response
```

Every report includes `unattributed_us = parent_us - sum(child_us)` for each
parent with measurable children. Negative values beyond clock/histogram
rounding invalidate that attribution row.

## 2. Tunable Plumbing and Sweep

Extend `DiskdbArgs` with these flags and pass them to real constructors:

```text
--diskdb-connections <usize>          default 1
--diskdb-client-rpc-workers <u32>     default 2
--kv-connections <usize>              default 1
--kv-client-rpc-workers <u32>         default 2
```

`DiskdbRpcTransport::with_pool_size(pool_size, workers)` mirrors
`KvRpcTransport`: it keeps a vector per endpoint and selects round-robin.
`crowdb-diskdb` adds static config fields `server.kv_pool_size` and
`server.kv_rpc_workers`, constructs `ClientConfig` with them, and exposes
`--rpc-workers` through `LocalDiskdbDeployConfig`. These are startup-only
controls; config reload logs that a restart is required.

`tools/bench-diskdb-regression.sh` accepts named environment controls for all
five groups above, writes every resolved value into `results.tsv`, and passes
KV controls to `cluster local-deploy -t kv`, DiskDB server controls to
`cluster local-deploy -t diskdb`, and client controls to `bench diskdb`. Its
performance fixture defaults to one data group; the three-data-group case is a
diagnostic parallelism comparison.

The first focused one-block allocate sweep uses five-second discovery runs,
then 20-second confirmation runs for the best region:

1. Establish a one-data-group ceiling with the KV winner:
   `event_write=true`, peer pool 4, inflight 64, coalesce 64, drain 1, and four
   KV RPC workers. This separates sharding gain from per-request overhead.
2. Compare three data groups while sweeping workload concurrency `128, 256, 512, 768`.
   Start with 16 CLI-to-DiskDB connections and four client RPC workers, matching
   the KV winner's total client-side shape.
3. At the best workload concurrency, compare CLI-to-DiskDB connections
   `8, 16, 32`; compare DiskDB server RPC workers `2, 4`; and compare each
   DiskDB process's KV connections `4, 8, 16` with four KV-client RPC workers.
4. Keep the KV winner fixed, then run only its useful neighbors: inflight 32
   with coalesce 16, and inflight 64 with coalesce 64. Drain threshold remains
   1 because that is part of the measured winning aggregation behavior.
5. Confirm the best configuration three times for 20 seconds.
   Then rerun one worker, four blocks/request, and the mixed correctness
   sentinel.

Change one parameter group at a time. Promotion requires zero errors, deadline
stop, exact space accounting, no more than 10% one-worker p99 regression, and
three-run throughput spread within 10% of the median. A higher request count
per proposal is recorded as KV/crowdb-rpc aggregation, not DiskDB batching.
The report includes total throughput and per-data-group KV request/proposal
rates so uneven disk-group routing cannot masquerade as DiskDB overhead.

## 3. DiskDB Workflow Metrics

Metrics use `crowdb-common` handles, are registered once, and are updated with
atomics on hot paths. Existing histograms are renamed only when the permanent
design is folded; during implementation use the established names where a
rename would break consumers.

### 3.1 CLI client

- `bench.diskdb.allocate.e2e.lh`: client-observed successful allocation.
- `bench.diskdb.free.e2e.lh`: client-observed successful free.
- `bench.diskdb.schedule_delay.lh`: task-ready to request-submit delay.
- `bench.diskdb.inflight.g`: requests awaiting a response.
- `bench.diskdb.completed.c`: successful operations; a distinct result count.
- `bench.diskdb.errors.c`: failed operations.
- `bench.diskdb.partial_response.c`: successful RPC with fewer segments than
  requested.
- `bench.diskdb.no_space.c`: explicit exhaustion responses.
- `bench.diskdb.allocated_units.c` and `freed_units.c`: magnitude counters.
- `bench.diskdb.live_units.g`: client-side expected live allocation count.
- `bench.diskdb.group_requests.c@{disk_group_id}`: request distribution across
  the three data groups, needed to interpret aggregate throughput.

The existing C++ RPC block supplies `rpc.request.e2e`,
`rpc.submit_to_writev`, `rpc.read_to_parse`, `rpc.read_handle`,
`rpc.write_handle`, `rpc.writev`, `rpc.request.response_schedule`,
`rpc.response.inline`, socket bandwidth, read/write errors,
`rpc.send.queue.full.c`, submit retries, missed responses, reaped requests,
and `rpc.client.connections`. No duplicate Rust counters are added.

### 3.2 DiskDB server

Add one uniform request family for every registered DiskDB RPC method:

```text
request.{method}.lh          # completed requests: count, TPS, avg/max and latency buckets
request.{method}.inflight.g  # requests currently executing
request.{method}.errors.c    # responses whose DiskDB return code is not Success
```

`method` is one of `allocate_blocks`, `free_blocks`, `commit_blocks`,
`query_capacity_stats`, `get_disk_group_info`, `get_disk_info`,
`rebuild_zone_bitmap`, `recalc_disk_usage`, `compact_zone`, `trigger_scan`, or
`get_scan_status`. The latency begins when the registered handler receives the
frame and ends when its response is submitted. It therefore includes request
validation, async task queueing, domain work, response construction, and
response scheduling, but not socket delivery after submission.

The latency histogram's count and window rate are the authoritative completed
request count and TPS for that method; do not add a redundant success counter.
The per-method inflight gauge shows the current load mix, including slow
requests that have not completed. The error counter is separate because it is
a different outcome population. Validation rejects, unavailable/degraded
rejects, `NoSpace`, persistence failures, and internal failures all complete
the latency histogram and increment the method error counter. Where diagnosis
needs it, the existing domain-specific counters below split the error reason.

Replace the current allocation/free whole-handler metrics with this family and
update their consumers; do not record the same handler in both an old and new
histogram. Keep the allocation stage metrics and add the missing boundaries:

- `allocate.bitmap_scan.latency_us`: bitmap claim and zone selection.
- `allocate.record_build.latency_us`: busy-key/value construction and
  serialization.
- `allocate.kv_persist.latency_us`: `DdbKvClient::persist_busy_batch` await.
- `allocate.response_build.latency_us`: response serialization.
- `allocate.inflight.g`: async allocate handlers currently active.
- `allocate.claimed_units.c`: units claimed, a magnitude counter.
- `allocate.rollback_units.c`: claimed units rolled back after persistence
  failure.
- `allocate.partial_batches.c`: internal batches that cannot return all
  requested segments.
- `allocate.errors.total`: all allocation failures, split additionally into
  `allocate.errors.no_space.c`, `allocate.errors.kv.c`, and
  `allocate.errors.invalid.c`.
- `zone.allocate.retry.cms.bit`: existing CAS retry count.
- `allocate.zone_rotate.latency_us` and `allocate.zone_rotate.c`: rotation
  latency and outcome count; the counter is retained only because rotations
  can occur outside successful allocations.

The existing free stage, compaction, scanner, sync, recovery, space, and C++
RPC metrics remain in the same DiskDB metrics log. Add `free.errors.kv.c` so
mixed runs expose persistence errors; request-level free inflight and latency
come from the uniform family.

### 3.3 DiskDB-to-KV and KV server

Add client-side `DdbKvClient` boundaries to the DiskDB registry:

- `kv_client.batch_write.e2e.lh`: call entry through parsed response.
- `kv_client.batch_write.ops.c`: busy records carried, a magnitude counter.
- `kv_client.batch_write.bytes.bw`: encoded key/value bytes.
- `kv_client.inflight.g`: outstanding DiskDB-originated KV calls.
- `kv_client.errors.c` and `kv_client.retries.c`: failed attempts and retries.
- `kv_client.connections.g`: live connections, sourced from crowdb-rpc rather
  than duplicated if the registered C++ gauge is available in the block.
- `kv_client.batch_write.c@{group_id}`: calls by data group, used to verify
  that the 400K aggregate workload is distributed rather than bottlenecked on
  one Paxos group.

Retain the KV server's existing `kv.batch_write.lh`,
`write.inflight_enqueued.c`, `write.inflight_wait.l`,
`paxos.inflight_slots.g`, `paxos.propose.e2e.l`, prepare and accept-quorum
latencies, `paxos.learn.apply.l`, per-peer RPC latency/errors, WAL append/fsync
latency and bandwidth, tree apply/flush/page-write metrics, system CPU/RSS/TCP
counters, and the full C++ RPC block. These are referenced by the flow report;
they are not re-registered in DiskDB.

## 4. Log Collection and Report

All commands for one case share a timestamped run root. Teardown preserves:

- three `crowdb-kv-server-metrics-*.log` files and each KV server's
  `crowdb-kv-server-rpc-*.log`;
- three `crowdb-diskdb-metrics-*.log` files and each DiskDB server's
  `crowdb-diskdb-rpc-*.log`;
- one `crowdb-cli-metrics-*.log` and its `crowdb-cli-rpc-*.log`;
- command stdout/stderr, resolved configuration, `results.tsv`, and a derived
  per-window attribution TSV.

The regression script validates non-empty files and at least one metrics
window overlapping the benchmark interval. It writes exact paths into a
manifest instead of relying only on recursive discovery. RPC transport logs
are retained for connection lifecycle and errors; numeric RPC performance
comes from each process's `cpp-rpc` metrics block.

The permanent `doc/design/diskdb/diskdb-allocate-flow-analysis.md` records the
hardware, commit, commands, resolved tunables, result rows, relevant metrics
windows, bottleneck conclusion, and accepted A/B changes. Generated raw logs
remain untracked under `bench-log/`.

## 5. Failure and Fallback Behavior

Backpressure, timeout, connection close, KV error, rollback, partial response,
and `NoSpace` are separate outcomes. A failed KV persist rolls back every
claimed unit before responding and increments the exact rollback magnitude.
Metrics failure never changes request behavior. If a C++ RPC metric cannot be
included in a process's combined metrics log, retain its RPC log and mark the
counter unavailable; do not infer zero. Diagnostic non-durable modes carry a
mode field in every output row and cannot become the durable baseline.

## Scope

- `app/crowdb-cli/src/commands/bench/{verb.rs,diskdb.rs,metrics.rs}`: expose
  transport controls and client workflow metrics.
- `lib/crowdb-diskdb-client/src/rpc_transport.rs`: connection pool and RPC
  worker constructor.
- `app/crowdb-diskdb/src/{main.rs,ddb_config.rs,ddb_kv_client.rs,metrics.rs}`:
  configure the KV client and register KV-boundary metrics.
- `app/crowdb-diskdb/src/service/diskdb_rpc_service/{service.rs,mutations.rs,queries.rs,admin.rs}`
  and `app/crowdb-diskdb/src/model/alloc.rs`: uniform per-method request
  metrics plus allocation stage, outcome, and rollback instrumentation.
- `lib/crowdb-console-shared/src/{lifecycle.rs,ops/cluster.rs}` and
  `app/crowdb-cli/src/commands/cluster.rs`: DiskDB deployment tunables.
- `tools/bench-diskdb-regression.sh`: parameter matrix, manifest, log checks,
  and attribution output.
- `lib/crowdb-diskdb-client/tests/` and affected CLI/metrics tests: correctness
  and metric assertions.
- `doc/design/diskdb/diskdb-allocate-flow-analysis.md`: measured permanent
  analysis after implementation.

## Complexity

Medium. The metric handles and CLI plumbing are direct. The main difficulty is
keeping three connection/worker layers unambiguous, aligning independent log
windows, and preventing short discovery runs from being mistaken for accepted
results. No new lock or DiskDB data-flow batching is proposed.

## Test Design

1. Configure two DiskDB connections and one client RPC worker, issue concurrent
   requests, and assert both connections are established and all responses
   correlate exactly once. Invariant: pool selection does not alter semantics.
2. Inject KV failure after a multi-unit claim and assert the request fails,
   every claim is rolled back, `rollback_units` equals claimed units, and the
   KV error count increments. Invariant: failed persistence leaks no space.
3. Return fewer segments than requested and assert the benchmark exits
   non-zero and increments `partial_response`. Invariant: partial success is
   never counted as throughput.
4. Saturate KV admission with a small inflight window and assert
   `write.inflight_enqueued`, `write.inflight_wait`, DiskDB KV latency, and
   client e2e latency rise together. Invariant: controlled pressure is visible
   at its owning layer.
5. Invoke every registered DiskDB RPC once successfully and once with a
   rejected or injected-error outcome; assert its request histogram count is
   two, error count is one, inflight returns to zero, and latency covers through
   response submission. Invariant: method load, latency, and outcome are
   visible without double counting.
6. Hold one async admin request open while issuing allocations and assert both
   per-method inflight gauges show their respective live counts. Invariant:
   current load mix is visible before requests complete.
7. Run one production fixture case and assert the manifest names three KV
   metrics/RPC pairs, three DiskDB metrics/RPC pairs, and one CLI metrics/RPC
   pair with an overlapping workload window. Invariant: a result is auditable
   from one retained root.
8. Sweep connections/workers/inflight/coalescing and assert every TSV row has
   resolved parameters, throughput, p50/p99, errors, stop reason, exact space,
   and attribution remainder. Invariant: configurations are reproducible.
9. Run one and three data-group cases with the same winning KV configuration;
   assert per-group request counters sum to the client total and record each
   group's proposal rate. Invariant: sharding gain and DiskDB overhead are
   separately measurable.
10. Run the best durable case three times and assert zero errors, exact space,
   deadline stop, throughput within 10% of the median, and the R131 target.
11. Rerun one worker and assert p99 is within 10% of baseline or the analysis
   records the rejected trade-off. Invariant: saturation gains do not silently
   damage low-concurrency latency.
12. Run the mixed sentinel after the R132 fix and assert repeated compaction
   reclaims every acknowledged free. Invariant: tuning preserves durability
   and exact space accounting.

## Module Structure

```text
app/crowdb-cli/src/commands/bench/
  verb.rs                 # DiskDB client transport flags
  diskdb.rs               # workload construction and stage recording
  metrics.rs              # benchmark registry and log writer
lib/crowdb-diskdb-client/src/
  rpc_transport.rs        # per-endpoint connection pool
app/crowdb-diskdb/src/
  ddb_config.rs           # DiskDB-to-KV static transport settings
  ddb_kv_client.rs        # KV boundary instrumentation
  metrics.rs              # DiskDB workflow handles
  model/alloc.rs          # claim/build/persist/rollback stages
  service/diskdb_rpc_service/ # per-method latency/inflight/outcomes
tools/
  bench-diskdb-regression.sh # lifecycle, sweep, retention, attribution
doc/design/diskdb/
  diskdb-allocate-flow-analysis.md # permanent measured result
```

## Config Extensions

Product defaults preserve current behavior: one connection and two client RPC
workers on both client legs, two DiskDB server RPC workers, KV max inflight 32,
and KV coalescing off. The regression performance profile explicitly selects
the KV sentinel baseline: one data group, event-write, peer pool 4, inflight
64, coalesce 64, drain 1, four KV server workers, 16 CLI connections, and four
CLI RPC workers. All values are recorded after resolution.

## Server Wiring

`local-deploy` passes resolved KV controls to every KV node and DiskDB controls
to every DiskDB node. Each DiskDB process applies its KV pool/worker settings
before constructing the shared `CrowdbKvClient`. Metrics runners start before
readiness and stop after RPC serving stops so the retained logs cover the full
benchmark window.

## Open Questions

- None. The existing KV regression resolves the initial KV configuration;
  R131 measures DiskDB overhead and the narrow neighboring settings rather
  than reopening the full KV tuning search.
