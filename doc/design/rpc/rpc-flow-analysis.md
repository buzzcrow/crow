<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# RPC Flow Analysis

End-to-end analysis of the standalone RPC echo benchmark. It measures
crowdb-rpc transport only: epoll/kqueue, framing, request correlation, and the
request/response callback. There is no KV or storage layer.

## 1. Flow

```text
Rust benchmark worker
  -> build flatbuffer control and pooled data buffers
  -> RpcClient::send(..., callback)
     claim a preallocated completion slab slot
     enqueue the frame and writev on the caller's I/O worker
  -> standalone server readable event
     read into a 256KiB worker buffer
     parse all frames, copy the request into an echo response
     enqueue the response and batch writev per connection
  -> client readable event
     parse response and claim the slab slot DONE
     invoke bench_on_complete inline on the I/O worker
     release buffers and submit the next request
  -> worker unparks after its in-flight requests drain
```

The callback mode avoids a tokio scheduler round trip. The tokio mode uses
`RpcClient::call`, an oneshot channel, and a task wake for each response. Both
models share `RpcClient`, `Connection`, `BufferPool`, and `RpcServer`.

The hot path uses connection pointers in epoll/kqueue user data, per-worker
receive buffers, caller-thread `writev`, batched server responses, and a
slab completion pool with a map fallback. The timeout reaper handles lost
responses outside the hot path. Reference-count operations are O(1); frame
construction, the echo memcpy, flatbuffer serialization, and kernel socket
copies are unavoidable.

## 2. Latest Benchmark Results

Both runs use 128B values, a standalone server, loopback, and a 20s duration.
The Linux run is the current regression reference. The macOS run is the latest
retained standalone baseline. `nagle` means TCP coalescing is enabled.

### Linux — 2026-08-27

AMD Ryzen 9 5950X, 16c/32t, x86_64, Linux 6.8. Single engine; coroutine
results use direct callbacks. The non-Nagle run is the reference mode.

| Workers | Load | Mode | Nagle | ops/s | avg us | p50 us | p99 us | p999 us | Errors |
| ---: | ---: | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 1T:1C | coroutine | off | 52,759 | 17 | 17 | 25 | 31 | 0 |
| 4 | 64T:4C | coroutine | off | 514,844 | 122 | 108 | 186 | 326 | 0 |
| 8 | 512T:8C | coroutine | off | 820,424 | 621 | 612 | 842 | 1,181 | 0 |
| 16 | 1,000T:32C | coroutine | off | 1,246,622 | 798 | 401 | 6,148 | 10,416 | 0 |
| 4 | 64T:4C | coroutine | on | 983,245 | 63 | 59 | 162 | 520 | 0 |
| 8 | 512T:8C | coroutine | on | 1,744,728 | 291 | 261 | 540 | 5,248 | 0 |
| 16 | 1,000T:32C | coroutine | on | 2,023,369 | 490 | 418 | 1,550 | 2,282 | 0 |
| 1 | 1T:1C | tokio | off | 23,564 | 41 | 42 | 69 | 102 | 0 |
| 4 | 64T:4C | tokio | off | 557,362 | 113 | 105 | 265 | 425 | 6 |
| 16 | 1,000T:32C | tokio | off | 849,575 | 1,156 | 1,063 | 2,652 | 3,838 | 591 |

Nagle improves coroutine throughput by 91%, 113%, and 62% at 64T, 512T,
and 1,000T. It also reduces p99 by 13%, 36%, and 75%. Tokio is slower and
produces queue-full errors under bursty high concurrency.

### Linux — 2026-08-31

AMD Ryzen 9 5950X, 16c/32t, x86_64, Linux 6.8. Same hw as 2026-08-27.
Fix: per-call control flatbuffer — `request_id` now embedded in
`ConnectionPingRequest.id` per call (was fixed `id=0`), enabling correct
slab slot correlation. 6 of 10 configs strictly better than 2026-08-27;
4 retained (1L/1C coroutine ops/s slightly lower, 64T nagle-on p999
worse, tokio 64T ops/s lower, tokio 1000T errors worse).

| Workers | Load | Mode | Nagle | ops/s | avg us | p50 us | p99 us | p999 us | Errors |
| ---: | ---: | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 1T:1C | coroutine | off | 52,134 | 18 | 17 | 17 | 31 | 0 |
| 4 | 64T:4C | coroutine | off | 517,687 | 122 | 94 | 96 | 260 | 0 |
| 8 | 512T:8C | coroutine | off | 830,174 | 614 | 486 | 505 | 1,074 | 0 |
| 16 | 1,000T:32C | coroutine | off | 1,307,488 | 762 | 302 | 324 | 9,645 | 0 |
| 4 | 64T:4C | coroutine | on | 994,296 | 63 | 44 | 46 | 578 | 0 |
| 8 | 512T:8C | coroutine | on | 1,869,083 | 271 | 209 | 219 | 3,993 | 0 |
| 16 | 1,000T:32C | coroutine | on | 2,213,182 | 448 | 157 | 171 | 1,999 | 0 |
| 1 | 1T:1C | tokio | off | 29,367 | 32 | 24 | 25 | 71 | 0 |
| 4 | 64T:4C | tokio | off | 487,362 | 127 | 63 | 71 | 519 | 4 |
| 16 | 1,000T:32C | tokio | off | 995,573 | 993 | 327 | 408 | 5,540 | 895 |

The per-call control fix dramatically improves p99 at high concurrency:
coroutine 1,000T p99 drops from 6,148us to 324us (−95%) without nagle,
and from 1,550us to 171us (−89%) with nagle. The 1,000T nagle-on peak
rises to 2.21M ops/s (+9.4%). Tokio 1T:1C improves 24.6% (29,367 vs
23,564) — the fixed correlation eliminates resp_missed overhead.

### Linux — 2026-09-02

AMD Ryzen 9 5950X, 16c/32t, x86_64, Linux 6.8. Same hw as 2026-08-31.
The crowdb-tree engine changes (page-count metrics + flush re-check
loop) do not touch the RPC transport — this run confirms no
transport-level regression. Results are within noise (−1% to −5% vs
2026-08-31). p50/p99 use coarser histogram buckets (100/500/1000/5000us)
than the 2026-08-31 exact values. Not strictly better — reference
retained per regression policy.

| Workers | Load | Mode | Nagle | ops/s | avg us | p50 us | p99 us | Errors |
| ---: | ---: | --- | --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 1T:1C | coroutine | off | 50,663 | 18 | 100 | 100 | 0 |
| 4 | 64T:4C | coroutine | off | 514,318 | 123 | 500 | 500 | 0 |
| 8 | 512T:8C | coroutine | off | 814,807 | 626 | 1,000 | 1,000 | 0 |
| 16 | 1,000T:32C | coroutine | off | 1,271,548 | 783 | 500 | 10,000 | 0 |
| 4 | 64T:4C | coroutine | on | 973,317 | 64 | 100 | 500 | 0 |
| 8 | 512T:8C | coroutine | on | 1,775,602 | 286 | 500 | 500 | 0 |
| 16 | 1,000T:32C | coroutine | on | 2,130,032 | 466 | 500 | 5,000 | 0 |
| 1 | 1T:1C | tokio | off | 28,062 | 34 | 100 | 100 | 0 |
| 4 | 64T:4C | tokio | off | 492,924 | 126 | 500 | 500 | 0 |
| 16 | 1,000T:32C | tokio | off | 988,956 | 999 | 1,000 | 5,000 | 0 |

### macOS — 2026-08-21

Apple M5 Pro, 18c, arm64, macOS 26/Darwin 25.5. kqueue, 128B values, and
Nagle disabled.

| Engines | Workers | Load | ops/s | avg us | p50 us | p99 us | p999 us | Errors |
| ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 1 | 1T:1C | 49,445 | 19 | 19 | 29 | 49 | 0 |
| 1 | 4 | 64T:4C | 558,326 | 112 | 106 | 231 | 299 | 0 |
| 1 | 8 | 512T:8C | 900,017 | 564 | 503 | 1,446 | 3,938 | 0 |
| 2 | 8 | 512T:8C | 927,537 | 547 | 521 | 951 | 3,630 | 0 |
| 1 | 16 | 1,000T:32C | 722,644 | 1,372 | 1,009 | 5,484 | 14,384 | 0 |
| 2 | 16 | 1,000T:16C | 900,252 | 1,099 | 851 | 4,012 | 9,056 | 0 |

The macOS peak is 927,537 ops/s at two engines and eight workers. Linux is
faster at comparable high-concurrency loads, reaching 2.02M ops/s with Nagle
and 1.25M without it. The platforms use different kernels and socket
transports, so this is directional rather than a controlled A/B result.

## 3. Change History

### Shared connections and caller-thread writes

Removed the notify queue from submit and moved `writev` to the submitting
I/O worker thread. The `in_send_` CAS serializes concurrent senders; EAGAIN
arms write on the owning engine.

Perf: macOS throughput rose from ~132K to ~317K ops/s.

### Direct callback completion

Added the callback client model (`send()`) with a preallocated slab
completion pool, removing the oneshot channel allocation and scheduler wake
from the benchmark path.

Perf: peak throughput reached ~585K ops/s (2.1x the previous baseline).

### Slab fallback and reaper

Added CAS-based slot ownership (`FREE/DONE→PENDING` claim), map fallback for
occupied slots, timeout reclamation, and race fixes (init mutex, two-phase
PENDING, read-before-CAS in `on_response`).

Perf: peak Linux throughput reached ~2.29M ops/s (8.3x baseline) with zero
errors. Fixed silent callback loss under high worker contention.

### Standalone server and worker scaling

The same transport was measured in separate client/server processes instead
of in-process echo. Multi-engine and multi-worker configurations were swept.

Perf: Linux reached 2.40M ops/s (2e16w 1000t16c); macOS reached 928K ops/s
(2e8w 512t8c). Linux is 2.6x macOS under the same codebase and config.

### Tokio comparison

The async `call()` path (oneshot channel + scheduler wake per op) was
compared against the coroutine path (direct callback on I/O thread).

Perf: tokio runs at 45–69% of coroutine throughput and 1.5–2.2x the latency.
Per-call heap alloc (2 allocs/op) and scheduler round-trip account for most
of the gap; high-load bursts fill the send queue (30–879 errors at 64+
loaders).

| Config | Coroutine ops/s | Tokio ops/s | Tokio % of coroutine |
| --- | ---: | ---: | ---: |
| 1e1w 1T:1C | 52,759 | 23,564 | 45% |
| 1e4w 64T:4C | 514,844 | 557,362 | 108% |
| 1e16w 1000T:32C | 1,246,622 | 849,575 | 68% |

### Nagle comparison (2026-08-27)

Enabling TCP coalescing (`--enable-nagle`) allows the kernel to batch
multiple small 128B frames into a single TCP segment, reducing syscall
overhead for bursty coroutine workloads.

Perf:

| Load | Nagle off ops/s | Nagle on ops/s | Speedup | p99 off us | p99 on us |
| --- | ---: | ---: | ---: | ---: | ---: |
| 64T:4C | 514,844 | 983,245 | +91% | 186 | 162 |
| 512T:8C | 820,424 | 1,744,728 | +113% | 842 | 540 |
| 1000T:32C | 1,246,622 | 2,023,369 | +62% | 6,148 | 1,550 |

The result shows syscall pressure in the small-frame, bursty workload;
application-level send batching is the longer-term alternative.

### Benchmark update (2026-08-27)

Replaced the Linux reference with the current single-engine sweep and
retained the macOS standalone baseline. Metrics shutdown now wakes its
condition variable, reducing shutdown delay from ~5s to 60ms; it does not
affect request throughput.

### Per-call control flatbuffer (2026-08-31)

The `bench rpc` echo client was building a fixed `ConnectionPingRequest`
control flatbuffer with `id=0` and reusing it for every request. The
echo handler extracts `request_id` from the control's `id` field during
parse and echoes it back in the response. With `id=0`, every response
arrived with `request_id=0`, which never matched the slab slot's
per-call `request_id` — causing `resp_missed` and indefinite hangs.

Fix: `build_control(request_id)` builds a fresh `ConnectionPingRequest`
with `id=request_id` per call. Both tokio and coroutine modes now
correlate responses correctly.

Perf: 6 of 10 configs strictly better than 2026-08-27. The most dramatic
improvement is p99 at 1,000T: 6,148us→324us (−95%) without nagle,
1,550us→171us (−89%) with nagle. Peak nagle-on throughput rises to
2.21M ops/s (+9.4%). Tokio 1T:1C improves 24.6% (29,367 vs 23,564).

### B-tree page-count metrics + flush re-check loop (2026-09-02)

The crowdb-tree engine changes (page-count gauges + flush re-check loop)
do not touch the RPC transport layer. This run confirms no
transport-level regression: results are within noise (−1% to −5% vs
2026-08-31). Reference retained per regression policy. See the
"Linux — 2026-09-02" subsection above for the full table.
