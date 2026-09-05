<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB - Design: DiskDB Allocation Flow Analysis

Allocation flow, observability, tuning controls, and reference performance for
the durable DiskDB path.

Depends on: [DiskDB overview](design-crowdb-diskdb.md),
[zone management](design-crowdb-diskdb-zone-management.md),
[RPC design](../rpc/design-crowdb-rpc.md), and
[KV write flow](../kv/kv-write-flow-analysis.md).

## Table of Contents

- [1. Flow](#1-flow)
- [2. Metrics](#2-metrics)
- [3. Tunables](#3-tunables)
- [4. Reference Results](#4-reference-results)
- [5. Bottleneck](#5-bottleneck)
- [6. Retained Logs](#6-retained-logs)

## 1. Flow

```text
crowdb-cli workload task
  -> DiskdbRpcTransport connection pool
  -> crowdb-rpc DiskDB server worker
  -> Tokio allocation task
  -> lock-free bitmap claim
  -> busy-record construction
  -> DdbKvClient::persist_busy_batch
  -> KvRpcTransport connection pool
  -> KV batch_write
  -> proposal coalescing and inflight admission
  -> Paxos accept quorum
  -> WAL and engine apply
  -> DiskDB response construction and crowdb-rpc submission
  -> client completion
```

One allocation RPC produces one KV `batch_write`. Multiple blocks in an
allocation RPC become multiple busy records in that batch. Independent
allocation RPCs can be combined by KV proposal coalescing; DiskDB does not add
a second flow-level batcher.

## 2. Metrics

Every DiskDB request method has this family:

```text
request.{method}.lh
request.{method}.inflight.g
request.{method}.errors.c
```

The histogram count and rate identify the completed workload mix. The gauge
identifies the current live mix, including slow requests. The error counter is
the non-success response population. Timing begins at Rust handler dispatch
and ends at response submission, including async task scheduling.

Allocation adds bitmap scan, record build, KV persist, response build,
claimed-unit, rollback-unit, partial-batch, and error-reason metrics. The
DiskDB-to-KV boundary reports batch-write latency, record magnitude, inflight,
and errors. Each process's `cpp-rpc` block supplies transport scheduling,
writev/read/parse, bandwidth, queue pressure, connection, and transport-error
metrics without duplicate Rust counters.

KV-server logs supply `kv.batch_write.lh`, proposal inflight wait, Paxos phase
latencies, WAL append/fsync, engine apply/flush, inter-replica RPC, CPU, RSS,
and network counters.

## 3. Tunables

The three connection and worker layers remain independent:

- benchmark client: workload concurrency, connections per DiskDB endpoint,
  and client RPC workers;
- DiskDB: server RPC workers, KV connections per endpoint, and KV-client RPC
  workers;
- KV: server RPC workers, peer pool, proposal inflight window, coalesce size,
  drain threshold, event-write mode, WAL, and storage backend.

The high-throughput memory profile starts from the KV write sentinel. The
current matrix enables event-write and uses inflight 32 and coalesce 32 without
an explicit drain-threshold override. The 1/16-thread cases use two
connections and RPC workers on every layer; 128/256-thread cases use four.
Connection count is recorded explicitly because a per-process, per-endpoint
pool is not comparable to the direct KV benchmark's total count.

## 4. Reference Results

AMD Ryzen 9 5950X, 16c/32t, Linux 6.8, x86_64, memory KV/WAL, one block per
request, and 4 TiB per logical disk. Every case ran for 20 seconds, stopped at
its deadline with zero errors, and reported exact busy-space accounting.

| Workload | Grp | Thread | Block | Client connection | DiskDB connection | KV internal connection | Epoll worker | Window | Coalesce | ops/s | p50 us | p99 us | Duration | Errors | Space |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| allocate | 3 | 1 | 1 | 2 | 2 | 2 | 2 | 32 | 32 | 2,481 | 410 | 504 | 20 s | 0 | exact |
| allocate | 3 | 16 | 1 | 2 | 2 | 2 | 2 | 32 | 32 | 42,959 | 362 | 582 | 20 s | 0 | exact |
| allocate | 3 | 128 | 1 | 4 | 4 | 4 | 4 | 32 | 32 | 130,863 | 930 | 1,863 | 20 s | 0 | exact |
| allocate | 3 | 256 | 1 | 4 | 4 | 4 | 4 | 32 | 32 | 159,418 | 1,514 | 3,294 | 20 s | 0 | exact |
| allocate | 1 | 256 | 1 | 4 | 4 | 4 | 4 | 32 | 32 | 171,206 | 1,423 | 2,883 | 20 s | 0 | exact |
| mix | 3 | 1 | 1 | 2 | 2 | 2 | 2 | 32 | 32 | 2,494 | 405 | 500 | 20 s | 0 | exact |
| mix | 3 | 16 | 1 | 2 | 2 | 2 | 2 | 32 | 32 | 43,106 | 361 | 568 | 20 s | 0 | exact |
| mix | 3 | 128 | 1 | 4 | 4 | 4 | 4 | 32 | 32 | 128,013 | 948 | 1,957 | 20 s | 0 | exact |
| mix | 3 | 256 | 1 | 4 | 4 | 4 | 4 | 32 | 32 | 154,624 | 1,556 | 3,456 | 20 s | 0 | exact |
| mix | 1 | 256 | 1 | 4 | 4 | 4 | 4 | 32 | 32 | 171,419 | 1,419 | 2,915 | 20 s | 0 | exact |

`Grp` is the number of KV data groups bound round-robin to DiskDB disk groups;
three groups in the default three-node topology gives each node's DiskDB disk
group a distinct KV group. `Epoll worker` applies uniformly to the client,
DiskDB server, DiskDB KV-client, and KV-server RPC layers. The fixture uses
4 TiB per logical disk so a 20-second high-concurrency run does not approach a
single node's available space.

The active regression matrix runs allocate and 70/30 allocate/free mix at 1,
16, 128, and 256 threads. The 1/16-thread cases use two connections and two
epoll workers on every RPC layer; 128/256 threads use four of each. A separate
256-thread case binds all DiskDB disk groups to one KV data group to measure
the single-group ceiling. Other cases bind one KV data group per node.
Environment overrides can replace the group, connection, or worker count for
experiments.

The 256-thread three-group allocation completed 3,191,195 allocations and
reported an exact 3,346,210,488,320-byte busy-space delta. The one-group case
completed 3,427,099 allocations with an exact 3,593,573,761,024-byte delta.

The direct KV write sentinel peaks near 264K writes/s at 512 tasks with the
larger 64/64 window/coalesce profile. The closest 32-slot direct-KV references
reach about 181–191K writes/s at 128/256 threads. Against that range, the
one-group DiskDB allocation result is within about 6%, while the three-group
result is within about 12% of the 256-thread KV result.

## 5. Bottleneck

At 256 threads, DiskDB request latency is dominated by KV persistence. In
representative saturated allocation windows, whole-handler average latency is
about 1.25–1.76 ms while KV persistence is about 1.08–1.50 ms. Bitmap scan and
record construction average at or below one microsecond. KV batch-write
service averages about 0.5–0.7 ms, and KV processes consume several cores in
both user and system time. Inflight slots remain around one to three, so the
32-slot proposal window is not saturated.

Three data groups are about 7% slower than one at the same 256-thread settings
for both allocate and mix. All groups are replicated across the same three KV
processes, so they contend for shared accept-round CPU and consensus transport;
adding groups does not provide independent hardware capacity.

The current bottleneck is KV accept-round and RPC CPU capacity, not DiskDB
bitmap allocation, the 32-slot inflight window, or disk capacity. The durable
one-unit path cannot reach 400K operations/s through DiskDB tuning alone while
it requires one KV client write per operation and the measured direct KV
ceiling is about 264K operations/s. Reaching a higher rate requires raising the
underlying KV ceiling or changing the number of DiskDB operations represented
by one KV client write. Any such batching must preserve durable acknowledgement
and exact rollback semantics.

## 6. Retained Logs

Reference run roots:

- `bench-log/diskdb-regression-20260905-133409`: complete current allocation
  and mixed-workload matrix.

Each root retains command output and configuration plus three KV metrics/RPC
log pairs, three DiskDB metrics/RPC log pairs, and one CLI metrics/RPC pair per
case. `results.tsv` records the resolved parameters and correctness fields.
Generated logs remain untracked.
