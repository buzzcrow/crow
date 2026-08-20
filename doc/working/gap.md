<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# RPC Echo Perf Gap Analysis — CROW vs buzz-cpp

Gap analysis between CROW's RPC echo bench (~520K TPS, 2-process +
C++ coroutines) and buzz-cpp's coroutine loader bench (2.03M TPS, same
platform). Both use the same inline-resume technique; the gap is in
the surrounding architecture, not the resume model.

Companion: `rpc-echo-flow-analysis.md` (CROW flow detail).

---

## Benchmark Comparison

Both measured on the same AMD Ryzen 9 5950X (16c/32t), 128B values,
epoll + TCP loopback.

| Metric | buzz-cpp | CROW (current) | Ratio |
| --- | --- | --- | --- |
| Peak TPS | 2,029,024 | ~520,000 | 3.9× |
| Per-worker TPS | 63K (32 workers) | ~33K (16 workers) | 0.52× (CROW wins per-worker) |
| Workers at peak | 32 client + 16 server | 16 client + 16 server | — |
| Epoll fds | 2 (client + server, separate processes) | 2 (client + server, separate processes) | — (matched) |
| Process model | 2 processes | 2 processes | — (matched) |
| Client model | C++ coroutines | C++ coroutines | — (matched) |
| Avg latency at peak | 592µs | 242µs (128 coros) / 2ms (1000 coros) | — |
| Errors | 0 | 0 | — |

Source: `buzz-cpp/logs/bench.log.20260820-100050` (2,029,024 TPS),
CROW bench runs (128 coros, 16 conns, 16+16 workers, 128B, 5s).

---

## Same Technique, Different Architecture

Both use the buzz model: the C++ I/O worker directly resumes the next
iteration on the response thread — no scheduler, no thread switch.

CROW's slab pool (pre-allocated array indexed by `request_id & mask`,
O(1), zero heap alloc) is better than buzz-cpp's `mt_hash_map` (lock-free
concurrent hash map with hazard pointers + heap alloc per node).

The lock path is fully clean — zero `std::mutex` acquisitions on the hot
path (send queue is lock-free MPSC, epoll re-arm calls `epoll_ctl`
directly, buffer pool uses direct heap alloc via glibc per-thread
arenas). Details in `rpc-echo-flow-analysis.md`.

---

## Remaining Gaps (3.9×)

### Gap 1: Worker count — 48 vs 32 (~1.5×)

buzz-cpp: 32 client I/O workers + 16 server I/O workers = 48 threads
covering 2 epoll fds. More workers keep `epoll_wait` covered while
others do callback work.

CROW: 16 client I/O workers + 16 server I/O workers = 32 threads.
Scaling beyond 16 workers per engine hits diminishing returns due to
`EPOLLONESHOT` re-arm contention within a single epoll fd. buzz-cpp
scales to 32 client workers because its per-worker cost is lower (no
flatbuffer, no FFI callback).

CROW's workers are individually more efficient at low concurrency
(~33K vs 63K per worker at peak) but **cannot scale to as many
workers** due to higher per-op cost (flatbuffer build + FFI + malloc).

### Gap 2: Per-op cost — flatbuffer + FFI vs custom codec (~1.3×)

buzz-cpp: custom binary codec (`msg_bench_write_request`), direct
`recv()` into message buffer, no heap alloc per frame. Per-op cost
is dominated by the syscall + epoll re-arm, not the codec.

CROW: flatbuffer build per op (`FlatBufferBuilder::new()` + create +
finish), FFI callback overhead (~20ns/op). The flatbuffer rebuild is
the biggest remaining contributor — it allocates a new buffer every op.

---

## Open Recommendations

### Flatbuffer template reuse (low effort, ~1.2×)

The `ConnectionPingRequest` is rebuilt per op via
`FlatBufferBuilder::new()`. Pre-build a template and patch the `id`
field. The server's `build_ping_response` has the same issue.

Expected: ~5µs saved per op (significant at low concurrency where
per-op cost is 12µs; less impact at high concurrency where queueing
dominates).

### More workers via multi-engine (medium effort, ~1.3×)

CROW currently uses 1 engine per process (1 epoll fd per side). Adding
`--io-engines 2` on the client side would give 2 client epoll fds,
allowing 32 client workers without ONESHOT re-arm contention on a
single fd. This matches buzz-cpp's 32 client workers.

The server side can also use multi-engine to scale beyond 16 workers.

---

## Projected TPS After Fixes

Rough estimates based on bottleneck reasoning, not measurements.
Treat as ordering of impact, not precise predictions.

| Fix | Estimated impact | Basis |
| --- | --- | --- |
| Current (2-process + C++ coroutines) | 520K (measured) | 128 coros, 16 conns, 16+16 workers, 128B, 5s |
| + Flatbuffer reuse | ~1.2× | eliminates per-op `FlatBufferBuilder::new()` alloc + build |
| + Multi-engine (more workers) | ~1.3× | 2 client epoll fds → 32 client workers, matching buzz-cpp |

Cumulative multiplier (if independent): ~1.5× → ~780K. Realistic
range: 700K–850K. The fixes likely overlap (reducing per-op cost also
reduces the benefit of more workers).

**RDMA (R32) is untested.** The "2M+" target is based on buzz-cpp
hitting 2M with a similar coroutine technique, but RDMA bypasses the
kernel socket path entirely — the bottleneck shifts from epoll/syscall
overhead to NIC + PCIe latency.

---

## Current Measured Results (128B, 5s, epoll + TCP loopback)

| Config | TPS | Avg lat | Notes |
| --- | --- | --- | --- |
| 128 coros, 16 conns, 16+16 workers | 518K | 242µs | peak per-worker efficiency |
| 1000 coros, 32 conns, 16+16 workers | 489K | 2.0ms | scales to 1000, latency rises |
