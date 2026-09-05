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

The high-throughput memory profile starts from the KV write sentinel:
event-write enabled, peer pool 4, inflight 64, coalesce 64, drain threshold 1,
and four KV server RPC workers. DiskDB uses four RPC workers on each client and
server leg. Connection count is recorded explicitly because a per-process,
per-endpoint pool is not comparable to the direct KV benchmark's total count.

## 4. Reference Results

AMD Ryzen 9 5950X, 16c/32t, Linux 6.8, x86_64, memory KV/WAL, one block per
request. Discovery cases ran for five seconds; the confirmation ran for 20
seconds. Every case stopped at its deadline with zero errors and exact busy
space accounting.

| Workload | Grp | Thread | Block | Client connection | DiskDB connection | KV internal connection | Epoll worker | Window | Coalesce | ops/s | p50 us | p99 us | Duration | Errors | Space |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| allocate | 3 | 128 | 1 | 16 | 8 | 4 | 4/4/4/4 | 64 | 64 | 128,559 | 951 | 1,848 | 5 s | 0 | exact |
| allocate | 3 | 256 | 1 | 16 | 8 | 4 | 4/4/4/4 | 64 | 64 | 156,741 | 1,541 | 3,332 | 5 s | 0 | exact |
| allocate | 3 | 512 | 1 | 16 | 8 | 4 | 4/4/4/4 | 64 | 64 | 183,310 | 2,635 | 5,872 | 5 s | 0 | exact |
| allocate | 3 | 768 | 1 | 16 | 8 | 4 | 4/4/4/4 | 64 | 64 | 193,977 | 3,703 | 8,455 | 5 s | 0 | exact |
| allocate | 1 | 512 | 1 | 16 | 8 | 4 | 4/4/4/4 | 64 | 64 | 205,167 | 2,382 | 4,790 | 5 s | 0 | exact |
| allocate | 1 | 512 | 1 | 4 | 4 | 4 | 4/4/4/4 | 64 | 64 | 206,266 | 2,362 | 4,818 | 5 s | 0 | exact |
| allocate | 1 | 512 | 1 | 4 | 4 | 4 | 4/4/4/4 | 64 | 64 | 191,971 | 2,508 | 5,513 | 20 s | 0 | exact |

`Grp` is the number of KV data groups bound round-robin to DiskDB disk groups;
three groups in the default three-node topology gives each node's DiskDB disk
group a distinct KV group. `Epoll worker` is shown as client, DiskDB server,
DiskDB KV-client, and KV-server RPC workers. The regression fixture now uses
4 TiB per logical disk so a 20-second high-concurrency run does not approach a
single node's available space.

The active regression matrix runs allocate and 70/30 allocate/free mix at 1,
16, 128, and 512 threads. The 1/16/128-thread cases use two connections and
two epoll workers on every RPC layer; 512 threads uses four of each. A separate
512-thread case binds all DiskDB disk groups to one KV data group to measure
the single-group ceiling. Other cases bind one KV data group per node.
Environment overrides can replace the group, connection, or worker count for
experiments.

The 20-second confirmation completed 3,842,826 allocations and reported an
exact 4,029,495,115,776-byte busy-space delta.

The direct KV write sentinel peaks near 264K writes/s at 512 tasks and 16
connections with the same KV server profile. That workload observed about 58
client writes per WAL append and no inflight-window saturation.

## 5. Bottleneck

At 512 workers, DiskDB request latency is dominated by KV persistence. In
representative one-second windows, whole-handler average latency is about
1.8–2.7 ms while KV persistence is about 1.6–2.4 ms. Bitmap scan averages below
one microsecond. Connection reduction from 16/8 to 4/4 changes throughput by
less than one percent, so excess connection count is not the active limit.

Three data groups are slower than one on the same three KV processes. The
groups contend for shared CPU and consensus transport; adding groups does not
provide independent hardware capacity.

The durable one-unit path cannot reach 400K operations/s through DiskDB tuning
alone while it requires one KV client write per operation and the measured
direct KV ceiling is about 264K operations/s. Reaching a higher rate requires
raising the underlying KV ceiling or changing the number of DiskDB operations
represented by one KV client write. Any such batching must preserve durable
acknowledgement and exact rollback semantics.

## 6. Retained Logs

Reference run roots:

- `bench-log/diskdb-regression-20260905-101733`: 128-worker discovery.
- `bench-log/diskdb-regression-20260905-101918`: 256/512-worker discovery.
- `bench-log/diskdb-regression-20260905-102109`: 768-worker discovery.
- `bench-log/diskdb-regression-20260905-102201`: one-group comparison.
- `bench-log/diskdb-regression-20260905-102241`: four-connection comparison.
- `bench-log/diskdb-regression-20260905-102330`: 20-second confirmation.

Each root retains command output and configuration plus three KV metrics/RPC
log pairs, three DiskDB metrics/RPC log pairs, and one CLI metrics/RPC pair per
case. `results.tsv` records the resolved parameters and correctness fields.
Generated logs remain untracked.
