<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# KV Write Flow Analysis

Write flow from client request through proposal admission, Paxos, WAL, and
engine apply. The benchmark sentinel is
`tools/bench-kv-write-regression.sh`.

## 1. Flow

```text
PUT/DELETE/BatchWrite
  -> encode key/value payload
  -> PxKvStore::propose_and_respond
  -> PxGroup::propose
     leadership and client-sequence deduplication
     optional proposal coalescing
     inflight admission and slot allocation
  -> prepare phase when required
     local acceptor + concurrent follower RPCs
  -> accept phase
     local CAS + follower RPCs
     strict mode waits for local WAL fdatasync
     early-ack mode defers local persist after quorum
  -> chosen
     record deduplication and chosen frontier
     async mode applies the entry off the proposal task
     Linearizable reads wait for the apply fence when necessary
  -> fan out chosen notice
  -> KvResponse { chosen slot }
```

Payloads become ref-counted `Bytes` after the initial encode. Retries and
fan-out clone references rather than payload data. Batch decoding uses slices
of the same buffer, and the FFI passes key/value pointer-length pairs to the
engine. The remaining large copies are request encoding, WAL recovery, the
engine's internal memtable apply, and socket serialization.

The critical path in production is the quorum RPC round trip. Proposal
coalescing reduces rounds per key, while the inflight window provides
backpressure. The queue admission policy avoids `Busy` responses and retry
storms.

## 2. Latest Benchmark Results

The Linux reference is the current run: 3-node cluster, 512B values, 1M key
space, 10s mem mode, coalescing enabled, and zero-copy crow-rpc handlers. The
macOS run is an older gRPC baseline with different coalescing and inflight
settings, so it shows platform and transport context rather than a strict
A/B comparison.

### Linux — 2026-08-26

AMD Ryzen 9 5950X, 16c/32t, x86_64, Linux. `win` is the inflight window;
`co` is observed average coalesced keys over the configured maximum.

| Threads | Conn | Workers | win | co | ops/s | WAL/node | p50 us | p99 us | Errors |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 1 | 2 | 32 | 1.0/16 | 3,770 | 37,722 | 273 | 366 | 0 |
| 16 | 2 | 2 | 32 | 6.7/16 | 63,393 | 94,137 | 231 | 625 | 0 |
| 64 | 4 | 2 | 32 | 14.7/16 | 171,582 | 116,476 | 339 | 858 | 0 |
| 128 | 4 | 4 | 32 | 15.3/16 | 191,411 | 124,957 | 582 | 1,448 | 0 |
| 256 | 8 | 4 | 32 | 15.4/16 | 190,769 | 123,974 | 1,173 | 2,970 | 0 |
| 512 | 16 | 4 | 64 | 35.0/64 | 178,024 | 50,815 | 2,738 | 5,444 | 0 |
| 1,000 | 16 | 4 | 64 | 27.5/32 | 182,541 | 66,376 | 5,204 | 12,832 | 0 |

The peak is 191,411 ops/s at 128 threads. Throughput plateaus at high
concurrency because accept-round processing, not the inflight window, is the
limit. The window was never full and all configurations completed without
errors.

### macOS — 2026-08-19

Apple M5 Pro, 18c, arm64, macOS 26.5. This run used gRPC transport,
coalescing up to 32 keys, and `max_inflight=128`.

| Threads | Conn | ops/s | WAL append | p50 us | p99 us | p999 us | Errors |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 1 | 10,144 | 304,358 | 95 | 153 | 211 | 0 |
| 4 | 2 | 21,879 | 449,508 | 178 | 307 | 380 | 0 |
| 16 | 4 | 47,260 | 276,795 | 330 | 523 | 619 | 0 |
| 32 | 16 | 57,889 | 170,600 | 537 | 894 | 1,046 | 0 |
| 64 | 32 | 69,908 | 104,777 | 888 | 1,440 | 1,745 | 0 |
| 128 | 32 | 78,155 | 86,840 | 1,590 | 2,654 | 3,794 | 0 |
| 256 | 32 | 87,448 | 86,619 | 2,870 | 4,704 | 7,004 | 0 |

The macOS gRPC run peaks at 87,448 ops/s. The current Linux crow-rpc run
reaches 190,769 ops/s at the matching 256-thread load, but the transport and
benchmark settings differ.

## 3. Change History

### Concurrent Paxos phases

Local and follower prepare/accept work now runs concurrently via
`tokio::join!`, with quorum short-circuiting and bounded retries.

Perf: the proposal critical path is now one quorum RPC round-trip instead of
sequential local + remote phases.

### Early acknowledgement and asynchronous apply

Follower quorum can return before local fsync (`wal_early_ack = true`,
production default), and engine apply runs outside the proposal task behind a
Linearizable apply fence. Strict mode remains available where synchronous
durability is required.

Perf: 48T:48C Linux +7.7% throughput (27,663 → 29,790 ops/s) and −7.2% avg
latency; on M5 Pro all percentiles improve (p99 −3.2% at 48T across 3 runs).

### Proposal coalescing

Multiple client writes share one Paxos slot and quorum round. At 256 threads
in the current run, batches average 15.4 of 16 keys.

Perf: throughput reaches 190,769 ops/s at 256T; accept rounds drop from
~13.4K/s (one per key) to ~12.5K/s (one per batch of ~15 keys).

### WAL flush tuning

An explicit flush coalescing delay was tested from 0 to 200us at 48T:48C.
Throughput stayed flat at ~29.2K ops/s (±1% noise) with no winner, so the
extra delay was removed and wake-drain-flush remains the baseline.

Perf: no change — the coalesce delay was a no-op; `wal_flush_coalesce_us` and
the coalesce arm in `pipeline_writer.rs` were deleted.

### Transport migration

Zero-copy crow-rpc replaced the gRPC serialization and thread-pool handoff on
the Linux path. C++ Frame ownership transfers to Rust; flatbuffers are parsed
zero-copy in the tokio task; responses use `FlatBufferBuilder::collapse()` +
external C++ Buffer.

Perf: the 256T Linux result rose from ~124K to ~191K ops/s, a 1.5x
improvement. The macOS gRPC baseline (87K at 256T) is retained for context.

### Benchmark update (2026-08-26)

Replaced the Linux reference with the current crow-rpc/coalescing run and
retained the macOS gRPC baseline. The current run has zero errors and reaches
191K ops/s before the high-load accept-round ceiling. The previous Linux
baseline used gRPC transport without coalescing; positive throughput deltas
are improvements.

| Threads | Conn | Old ops/s (gRPC) | New ops/s (crow-rpc) | Δ ops/s |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 1 | 3,249 | 3,770 | +16.0% |
| 16 | 2 | 25,802 | 63,393 | +145.7% |
| 64 | 4 | 28,761 | 171,582 | +496.4% |
| 128 | 4 | 28,898 | 191,411 | +562.7% |
| 256 | 8 | 28,898 | 190,769 | +560.3% |
| 512 | 16 | 28,898 | 178,024 | +516.1% |
| 1,000 | 16 | 28,898 | 182,541 | +531.7% |

The gRPC baseline plateaued at ~29K ops/s across all high-thread configs;
crow-rpc + coalescing scales to 191K at 128T before the accept-round ceiling.
