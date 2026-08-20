<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# RPC Echo Perf Gap Analysis — CROW vs buzz-cpp

Gap analysis between CROW's RPC echo bench (~2.34M TPS, 2-process +
C++ coroutines) and buzz-cpp's coroutine loader bench (2.03M TPS, same
platform). Both use the same inline-resume technique; CROW now exceeds
buzz-cpp by 1.15× after the CAS slab fix eliminated silent callback
loss under high worker contention.

Companion: `rpc-echo-flow-analysis.md` (CROW flow detail).

---

## Benchmark Comparison

Both measured on the same AMD Ryzen 9 5950X (16c/32t), 128B values,
epoll + TCP loopback.

| Metric | buzz-cpp | CROW (current) | Ratio |
| --- | --- | --- | --- |
| Peak TPS | 2,029,024 | ~2,339,116 | 0.87× (CROW wins) |
| Per-worker TPS | 63K (32 workers) | ~73K (32 workers) | 0.86× (CROW wins per-worker) |
| Workers at peak | 32 client + 16 server | 16 client + 16 server | — |
| Epoll fds | 2 (client + server, separate processes) | 2 (client + server, separate processes) | — (matched) |
| Process model | 2 processes | 2 processes | — (matched) |
| Client model | C++ coroutines | C++ coroutines | — (matched) |
| Avg latency at peak | 592µs | 425µs | — |
| Errors | 0 | 0 | — |

Source: `buzz-cpp/logs/bench.log.20260820-100050` (2,029,024 TPS),
CROW bench run 2026-08-20 (2e16w, 1000 coros, 32 conns, 16+16
workers, 128B, 20s). See `tools/bench-rpc-regression.sh` reference B3.

---

## Same Technique, Different Architecture

Both use the buzz model: the C++ I/O worker directly resumes the next
iteration on the response thread — no scheduler, no thread switch.

CROW's slab pool (pre-allocated array indexed by `request_id & mask`,
O(1), zero heap alloc, CAS `DONE→PENDING` for slot reuse) is better
than buzz-cpp's `mt_hash_map` (lock-free concurrent hash map with
hazard pointers + heap alloc per node). The CAS ensures clean slot
reuse under high worker contention — no silent callback loss.

The lock path is fully clean — zero `std::mutex` acquisitions on the hot
path (send queue is lock-free MPSC, epoll re-arm calls `epoll_ctl`
directly, buffer pool uses direct heap alloc via glibc per-thread
arenas). Details in `rpc-echo-flow-analysis.md`.

---

## Gap Closed — CROW Exceeds buzz-cpp (1.15×)

The 3.9× gap from the initial measurement (~520K) was caused by silent
callback loss under high worker contention, not architecture
differences. B2's `call_callback` unconditionally overwrote the slab
slot (no CAS), so concurrent `on_response` calls for the same
connection could stomp on each other's slots — `resp_wrong_id` would
tick, the response was silently dropped, and the coroutine hung until
the 30s force-exit. This capped effective concurrency at ~4 workers
(1e4w was the peak at 585K; 1e16w degraded to 497K).

The CAS fix (`DONE→PENDING` in `call_callback`, `PENDING→DONE` in
`on_response`) eliminated the silent drops. All coroutines are properly
resumed, so effective concurrency matches configured concurrency.
1e16w jumped from 497K → 2.27M (4.6×), and 2e16w reached 2.34M —
1.15× buzz-cpp's 2.03M.

### Remaining per-op cost difference

CROW still has higher per-op cost than buzz-cpp (flatbuffer build +
FFI callback vs custom codec), but this is offset by CROW's better
slab pool (zero heap alloc + CAS vs `mt_hash_map` hazard pointers +
heap alloc per node). At 16 workers, CROW achieves ~73K TPS per worker
vs buzz-cpp's ~63K per worker at 32 workers.

---

## Open Recommendations

### Flatbuffer template reuse (low effort, latency improvement)

The `ConnectionPingRequest` is rebuilt per op via
`FlatBufferBuilder::new()`. Pre-build a template and patch the `id`
field. The server's `build_ping_response` has the same issue.

Expected: ~5µs saved per op (improves low-concurrency latency; less
impact at high concurrency where queueing dominates). TPS ceiling is
now bounded by socket buffer throughput, not per-op cost.

---

## Current Measured Results (128B, 20s, epoll + TCP loopback)

| Config | TPS | Avg lat | Notes |
| --- | --- | --- | --- |
| 1e1w 1l 1c | 52,759 | 18µs | single-thread baseline |
| 1e1w 256l 8c | 345,745 | 740µs | sentinel config |
| 1e2w 512l 8c | 606,933 | 842µs | multi-worker, 512 coros |
| 1e4w 1000l 32c | 1,084,619 | 920µs | 4 workers |
| 1e16w 1000l 32c | 2,274,259 | 438µs | 16 workers, peak per-engine |
| 2e16w 1000l 32c | 2,339,116 | 425µs | 2 engines × 16 workers, peak |

Full 9-config sweep: `tools/bench-rpc-regression.sh` reference B3.
