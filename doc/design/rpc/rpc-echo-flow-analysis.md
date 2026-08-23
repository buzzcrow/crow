<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# RPC Echo Flow Analysis

End-to-end trace of the CROW RPC echo path (standalone-server loopback
benchmark). Mirrors the structure of
[`kv-write-flow-analysis.md`](../kv/kv-write-flow-analysis.md).

The echo benchmark measures raw RPC transport throughput (epoll/kqueue
+ framing + request/response correlation) with no KV/storage layer. The
CLI starts a standalone echo-server process and connects over loopback.
The echo handler copies request data to response data, so the benchmark
is purely I/O-bound.

---

## Flow

The callback model uses an inline callback chain that runs entirely on
C++ I/O worker threads. 2 epoll events per op (down from 5 in the
original oneshot design).

Key optimizations:

- **udata dispatch**: epoll stores `Connection*` in udata — no
  per-event mutex + map lookup.
- **Caller-thread writev**: `submit` does `writev` directly on the
  caller's thread (no notify queue, no cross-thread round-trip). The
  `in_send_` CAS serializes concurrent senders; EAGAIN arms write on
  the owning engine. Per-connection MPSC send queue + batch writev is
  already the shipped design — the residual contention is the
  single-writer CAS itself.
- **submit_inline for server responses**: server dispatch enqueues
  only (no writev). Responses are batched into one `writev` per
  connection after all events in the batch (send aggregation).
- **Per-worker receive buffer**: one `read()` into a 256KB per-worker
  buffer, then `feed_data` parses all frames it contains (recv
  aggregation).
- **Slab completion pool**: O(1) bitmask index (`request_id &
  pool_mask`), zero per-call heap alloc. CAS `FREE/DONE→PENDING`
  claims slot; CAS `PENDING→DONE` in `on_response` prevents
  double-invoke with the reaper. Falls back to
  `folly::ConcurrentHashMap` only if slot is occupied.
- **Inline callback resume**: C++ I/O worker invokes the C ABI
  callback directly on the response thread — no oneshot channel, no
  scheduler round-trip, no thread switch.
- **Timeout reaper**: background thread scans slab + map every
  `scan_interval_ns` for timed-out entries. Prevents leaked slots
  from lost responses.

```
Rust worker thread (std::thread::spawn, NOT tokio)
  → submit_next(ctx)  ← initial kickoff
    1. Build flatbuffer control + allocate data payload from BufferPool
    2. RpcClient::send(server, conn, request_id, ctrl, data,
       ECHO_MSG_TYPE, bench_on_complete, slot_ptr)
       → slab pool: CAS FREE/DONE→PENDING to claim slot
       → transport->submit: enqueue_send + caller-thread writev
    3. thread::park()  ← wait for all in-flight to drain

  For callback-driven submits (bench_on_complete → submit_next),
  the caller IS the C++ I/O worker — writev happens on the I/O worker
  thread, no cross-thread notify at all.

  ── Server-side (standalone process, server C++ I/O worker) ──

  Event 1: Readable — read + echo + enqueue response
    → read(fd, recv_buf, 256KB)  ← one read for multiple frames
    → parser.feed_data → conn->on_frame → echo_handler
      → build response (pool alloc + memcpy)
      → submit_inline(conn, response)  ← enqueue only
    → after all events: batch writev per connection

  ── Response arrives back on client connection ──

  Event 2: Readable — read response + callback
    → on_readable_impl → conn->on_frame → RpcClient::on_response
      → slab pool: CAS PENDING→DONE
      → invoke callback: bench_on_complete
        → record latency, release response buffers
        → if before deadline: submit_next (next request, same slot)
        → else: drain_in_flight; if 0: thread::unpark(worker)
```

**Events per op: 2** — server Readable (read + echo + batch writev)
and client Readable (read response + inline callback + caller-thread
writev for next request). The two writevs happen on the caller thread,
not as separate epoll events.

### Thread Model

- **Rust worker threads** (N = `--threads`) — build requests, submit
  via FFI, then `thread::park()` until in-flight drain. The callback
  chain builds the next request on the C++ I/O worker thread — no
  tokio scheduler involvement.
- **C++ I/O worker threads** (`io_workers` total per process;
  per-engine = `io_workers / io_engines`) — the client and standalone
  server use the same configuration. Each engine owns an independent
  epoll/kqueue fd and connections are partitioned round-robin. On Linux,
  workers sharing one fd use `EPOLLONESHOT`; the single-worker default
  uses the level-triggered fast path.
- **C++ acceptor thread** (1) — accepts new connections. Not on the
  hot path after setup.
- **C++ reaper thread** (0 or 1, opt-in) — scans slab + map for
  timed-out entries. Not on the hot path.

### Rust API

The `crow-rpc-ffi` crate exposes two client models:

- **Oneshot (async, tokio-friendly)** — `call()` returns a `Future`
  that resolves on response. Uses `oneshot::channel` (2 heap allocs
  per call). This is the path used by the KV client and consensus
  code. Drop-safe, `Send + Sync`, no unsafe code needed.
- **Callback (sync, zero-alloc hot path)** — `send()` stores
  the callback in a pre-allocated slab slot. The callback runs on the
  C++ I/O worker thread (must be non-blocking). Caller manages
  `user_data` lifetime. Used by the RPC bench target's coroutine mode;
  ~2x TPS vs oneshot. Not tokio-specific.

Both models share the same `RpcClient`, `Connection`, `BufferPool`,
and `RpcServer`.

The bench CLI supports both via `--mode`:
- `coroutine` (default) — C++ coroutines on I/O worker threads, direct
  callback dispatch via `send()`. No tokio scheduler involvement.
- `tokio` — Rust tokio tasks calling `call()` in a closed loop. Each
  op: `Box<oneshot::Sender>` heap alloc → slab submit → tokio
  scheduler wake → `CallFuture` poll. Measures the async FFI path
  overhead vs the coroutine path.

### Buffer Lifecycle

- **Request control + data**: allocated by Rust → `ref_clone` in C
  API → released after `writev` sends → recycles to pool.
- **Response control + data**: allocated by echo handler from pool →
  released after `writev` send → recycles to pool.
- **Response arrival on client**: parser malloc's control + data →
  wrapped as `ref=nullptr` Buffer → callback releases (no-op) →
  wrapper struct freed.

Memory copy summary:

- O(n) unavoidable: flatbuffer build, data payload fill, kernel
  socket copy (both directions), echo handler memcpy, header serialize.
- O(1) ref-count bumps: `ref_clone` on request, `release` after send.
- Zero-copy: `OutFrame` holds `Buffer*` pointers; `writev` iovecs
  point into Buffer data; `extract_request_id` reads in-place;
  response wrapped as `ref=nullptr` (no pool round-trip).

---

## Current Data

Regression sentinel: `tools/bench-rpc-regression.sh`.
Raw TSV: `doc/working/bench-rpc-regression.tsv`.

### 2026-08-21 (Standalone Server, macOS)

Platform: **Apple M5 Pro** (18c, arm64, macOS 26/Darwin 25.5).
Config: 128B values, 20s duration, standalone echo server, kqueue
loopback, pipeline_depth=1. After `send()` unification + global static
counters (removed per-instance atomics from the hot path).

#### Full sweep (6 configs)

| Eng | Wkr | T | C | ops/s | avg | p50 | p99 | p999 | raggr | saggr | err |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | 1 | 1 | 1 | 49,445 | 19 | 19 | 29 | 49 | 1.0 | 1.0 | 0 |
| 1 | 4 | 64 | 4 | 558,326 | 112 | 106 | 231 | 299 | 2.2 | 6.8 | 0 |
| 1 | 8 | 512 | 8 | 900,017 | 564 | 503 | 1446 | 3938 | 2.9 | 12.5 | 0 |
| 2 | 8 | 512 | 8 | 927,537 | 547 | 521 | 951 | 3630 | 2.9 | 11.1 | 0 |
| 1 | 16 | 1000 | 32 | 722,644 | 1372 | 1009 | 5484 | 14384 | 7.2 | 9.3 | 0 |
| 2 | 16 | 1000 | 16 | 900,252 | 1099 | 851 | 4012 | 9056 | 6.3 | 13.0 | 0 |

Eng=io_engines, Wkr=total io_workers per process (Eng × per-engine;
client and server use the same config), T=client dispatch threads, and
C=connections. raggr=frames per read and saggr=frames per writev on the
server.

Peak throughput is **928K ops/s** at 2e8w 512t8c. All six configurations
completed with zero errors. The 16-worker configs improved +27% (1e16w)
and +56% (2e16w) versus the pre-unification M5 Pro standalone run
earlier on 2026-08-21 — removing per-instance counter atomics cut
hot-path contention under high dispatch-thread counts.

#### Multi-worker scaling

- 1w→4w: **11.3x** (49K→558K). More dispatch threads, connections, and
  I/O workers expose send and receive aggregation.
- 4w→1e8w: **+61%** (558K→900K).
- 1e8w→2e8w at 512T:8C: **+3.1%** (900K→928K). Splitting eight workers
  across two kqueue engines improves tail latency (p99 1446→951).
- 2e16w 1000t16c holds at 900K — no longer regresses below the 8-worker
  configs. The 1,000 dispatch threads still raise average latency above
  1ms, but the global counters eliminate the per-instance atomic
  contention that capped the 16-worker configs at 575K before.

### 2026-08-21 (Standalone Server, Linux)

Platform: **AMD Ryzen 9 5950X** (16c/32t, x86_64, Linux 6.8).
Config: 128B values, 20s duration, standalone echo server, epoll
loopback, pipeline_depth=1. After `send()` unification + global static
counters (removed per-instance atomics from the hot path) — same
codebase state as the 2026-08-21 macOS run, different platform.

#### Full sweep (6 configs)

| Eng | Wkr | T | C | ops/s | avg | p50 | p99 | p999 | raggr | saggr | err |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | 1 | 1 | 1 | 54,503 | 17 | 17 | 22 | 31 | 1.0 | 1.0 | 0 |
| 1 | 4 | 64 | 4 | 1,036,835 | 60 | 55 | 141 | 580 | 6.0 | 6.1 | 0 |
| 1 | 8 | 512 | 8 | 1,990,216 | 255 | 235 | 391 | 2632 | 11.0 | 11.8 | 0 |
| 2 | 8 | 512 | 8 | 1,839,681 | 276 | 250 | 360 | 420 | 7.5 | 7.8 | 0 |
| 1 | 16 | 1000 | 32 | 2,213,973 | 448 | 330 | 1778 | 2646 | 9.3 | 9.6 | 0 |
| 2 | 16 | 1000 | 16 | 2,404,670 | 412 | 362 | 1254 | 5012 | 8.8 | 9.9 | 0 |

Eng=io_engines, Wkr=total io_workers per process (Eng × per-engine;
client and server use the same config), T=client dispatch threads, and
C=connections. raggr=frames per read and saggr=frames per writev on the
server.

Peak throughput is **2.40M ops/s** at 2e16w 1000t16c. All six
configurations completed with zero errors. Cross-platform, the AMD
epoll standalone server hits **2.6x** the M5 Pro kqueue result (928K)
under the same codebase and config — epoll loopback on the 5950X
sustains higher frame aggregation (raggr 11.0 vs 2.9 at 1e8w) and
lower tail latency (p99 391 vs 1446 at 1e8w).

#### Multi-worker scaling

- 1w→4w: **19.0x** (55K→1.04M). The 5950X's 32 hardware threads keep
  dispatch and I/O workers off each other's backs; aggregation kicks in
  immediately (raggr 6.0, saggr 6.1).
- 4w→1e8w: **+92%** (1.04M→1.99M). Eight workers on one epoll fd
  saturate recv aggregation (raggr 11.0) — one `read()` pulls ~11
  frames.
- 1e8w→2e8w at 512T:8C: **-7.6%** (1.99M→1.84M). Splitting eight
  workers across two engines costs aggregation (raggr 11.0→7.5); the
  5950X has enough cores that a single epoll fd at 512T isn't the
  bottleneck.
- 2e16w 1000t16c reaches the 2.40M peak, **+8.6%** over 1e16w
  1000t32c. At 1,000 dispatch threads the second engine finally pays
  off — per-engine worker count drops to 8, matching the sweet spot.

### 2026-08-21 (Coroutine vs Tokio Mode, Linux)

Platform: **AMD Ryzen 9 5950X** (16c/32t, x86_64, Linux 6.8).
Config: 128B values, 20s duration, standalone echo server, epoll
loopback, pipeline_depth=1. Slab completion pool with two-phase
PENDING (CLAIMED→READY) + read-before-CAS in on_response (no
PROCESSING state). Coroutine mode uses send_queue=256 (same-thread
submit+drain); tokio mode uses send_queue=1024 (burst-submit needs
larger queue). Each config runs in both `--mode coroutine` (default)
and `--mode tokio`.

#### Full sweep (12 configs)

| Eng | Wkr | T | C | Mode | ops/s | avg | p50 | p99 | p999 | raggr | saggr | err |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | 1 | 1 | 1 | coroutine | 53,644 | 17 | 17 | 24 | 29 | 1.0 | 1.0 | 0 |
| 1 | 1 | 1 | 1 | tokio | 24,849 | 39 | 44 | 59 | 71 | 1.0 | 1.0 | 0 |
| 1 | 4 | 64 | 4 | coroutine | 964,072 | 65 | 61 | 145 | 613 | 6.0 | 6.0 | 0 |
| 1 | 4 | 64 | 4 | tokio | 662,696 | 95 | 91 | 196 | 297 | 5.5 | 5.5 | 30 |
| 1 | 8 | 512 | 8 | coroutine | 1,749,146 | 290 | 271 | 422 | 2568 | 10.1 | 10.7 | 0 |
| 1 | 8 | 512 | 8 | tokio | 1,014,057 | 499 | 255 | 1446 | 41216 | 13.8 | 14.7 | 194 |
| 2 | 8 | 512 | 8 | coroutine | 1,803,255 | 281 | 247 | 357 | 415 | 7.9 | 8.2 | 0 |
| 2 | 8 | 512 | 8 | tokio | 1,038,462 | 485 | 278 | 1407 | 41152 | 11.9 | 12.3 | 295 |
| 1 | 16 | 1000 | 32 | coroutine | 2,217,250 | 447 | 340 | 1707 | 2572 | 9.4 | 9.8 | 0 |
| 1 | 16 | 1000 | 32 | tokio | 1,063,014 | 925 | 578 | 1832 | 41984 | 4.9 | 5.2 | 822 |
| 2 | 16 | 1000 | 16 | coroutine | 2,348,192 | 422 | 363 | 1399 | 5584 | 9.2 | 10.3 | 0 |
| 2 | 16 | 1000 | 16 | tokio | 1,071,921 | 917 | 713 | 1880 | 41632 | 5.8 | 6.4 | 879 |

Mode=worker execution model (`--mode coroutine|tokio`). Coroutine =
C++ coroutines on I/O threads (direct callback, zero tokio
involvement). Tokio = Rust tokio tasks calling `call()` (oneshot
channel + scheduler wake per op). Other columns same as above.

#### Analysis

The tokio mode (call() path) runs at **45-69% of coroutine throughput**
and **1.5-2.2x latency** across all configs. The gap has three causes:

- **Per-call heap alloc**: `Box<oneshot::Sender>` + `oneshot::channel()`
  = 2 heap allocs/op that the coroutine path doesn't do. At 1M ops/s
  that's 2M allocs/s.
- **Scheduler round-trip**: each response goes C++ callback →
  `Box::from_raw` → `tx.send()` → tokio wake → task re-schedule →
  poll `Receiver`. The coroutine path dispatches the callback inline
  on the I/O thread — no scheduler involvement.
- **yield_now contention**: 512+ tokio tasks yield every 64 iterations,
  contending with 511 other tasks for 32 worker threads.

The tokio mode produces SendQueueFull errors (30-879) at 64+ loaders —
the per-connection send queue (1024 frames) fills when 512+ tokio tasks
burst-submit on scheduler wake. The coroutine mode has zero errors
across all configs (same-thread submit+drain, queue=256 is sufficient).

The slab completion pool provides 7-13% throughput benefit over a
map-only path for `call()` (measured separately), so it's kept for
both modes. The slab race fixes (init mutex, CAS-first claim with
two-phase PENDING) eliminated the SIGSEGV that previously occurred at
2+ engines with 4+ connections. The read-before-CAS pattern in
on_response (read fields while PENDING_READY, then CAS directly to
DONE) eliminates the PROCESSING state and saves 1 atomic store per
response. The two-phase PENDING adds 1 atomic store per submit (the
irreducible cost of the write-before-CAS race fix) — this accounts for
the ~2-4% gap vs the pre-fix baseline. Send queue capacity is tuned
per mode: 256 for coroutine (minimizes cache pressure, same-thread
drain), 1024 for tokio (absorbs scheduler-burst submits).

### 2026-08-20 (Slab Fallback + Reaper, Linux)

Platform: **AMD Ryzen 9 5950X** (16c/32t, x86_64, Linux 6.8).
Config: 128B values, 20s duration, in-process echo, epoll loopback,
pipeline_depth=1.

#### Full sweep (6 configs)

| Eng | Wkr | T | C | ops/s | avg | p50 | p99 | p999 | raggr | saggr | err |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | 1 | 1 | 1 | 52,790 | 17 | 17 | 23 | 32 | 1.0 | 1.0 | 0 |
| 1 | 4 | 64 | 4 | 938,568 | 66 | 63 | 155 | 614 | 6.0 | 6.0 | 0 |
| 1 | 8 | 512 | 8 | 1,780,802 | 285 | 264 | 457 | 4112 | 11.0 | 11.7 | 0 |
| 2 | 8 | 512 | 8 | 1,744,829 | 291 | 270 | 387 | 438 | 7.6 | 7.8 | 0 |
| 1 | 16 | 1000 | 32 | 2,197,231 | 452 | 362 | 1607 | 2404 | 9.7 | 10.0 | 0 |
| 2 | 16 | 1000 | 16 | 2,293,581 | 432 | 379 | 1282 | 6092 | 9.1 | 10.0 | 0 |

Eng=io_engines, Wkr=total io_workers (Eng × per-engine; client uses
same config), T=client dispatch threads, C=connections. raggr=frames per
read and saggr=frames per writev on the server.

Peak **~2.29M** (2e16w 1000t16c), 8.3x the 276K baseline. Zero errors
across all six configs. `slab_fallback=0` and `map_in_flight=0` across
all configs.

#### Multi-worker scaling

- 1w→4w: **17.8x** (53K→939K).
- 4w→8w: **+90%** (939K→1.78M).
- 1e8w→2e8w at 512T:8C: **-2%** (1.78M→1.74M).
- 2e16w 1000t16c reaches the 2.29M peak, ahead of the 2.20M single-engine
  result at 1e16w 1000t32c.

---

## History

- **2026-08-19 (M5 Pro, 64B/5s)**: shared connections + caller-thread
  writev lifted ceiling from ~132K to ~317K. Multi-engine/multi-worker
  did not help for loopback. Zero errors across 10 configs.
- **2026-08-20 (AMD 5950X, Gap4+Gap1)**: ONESHOT zero-lock re-arm +
  `folly::ConcurrentHashMap`. Peak ~276K. Timeout errors at 2e1w/1e2w
  from `retry_send` busy-loop under high contention.
- **2026-08-20 (Gap2+Gap3)**: callback-based client model + slab
  completion pool. Peak ~585K (2.1x baseline), zero errors.
- **2026-08-20 (slab fallback + reaper)**: CAS-based slab claim + map
  fallback + timeout reaper. Peak ~2.29M (8.3x baseline), zero
  errors. Fixed silent callback loss under high worker contention
  (CAS `DONE→PENDING` in `send()`, `PENDING→DONE` in
  `on_response`). `slab_fallback=0` across all configs. Per-op
  latency histogram fix for accurate p50/p99/p999. CLI flag renamed
  from `--io-workers-per-engine` to `--io-workers` (total).
- **2026-08-21 (M5 Pro, standalone server)**: 128B/20s two-process
  kqueue sweep peaked at 956K ops/s with 2 engines and 8 total workers.
  All six configurations completed without errors.
- **2026-08-21 (M5 Pro, send() unification + global counters)**:
  unified the client call path to `send()` (removed `call()` /
  `call_one_way()` / `call_callback`), replaced 12 per-instance atomic
  counters with 6 global static `crow-common::metrics::Counter`
  instances. Peak 928K (2e8w). 16-worker configs improved +27% (1e16w)
  and +56% (2e16w) — removing per-instance atomics cut hot-path
  contention under 1,000 dispatch threads.
- **2026-08-21 (AMD 5950X, standalone server)**: 128B/20s two-process
  epoll sweep peaked at 2.40M ops/s (2e16w 1000t16c), 2.6x the M5 Pro
  kqueue result under the same codebase. All six configs zero errors.
- **2026-08-21 (AMD 5950X, slab race fixes + tokio mode)**: fixed three
  slab completion pool races — init race (mutex), write-before-CAS
  (two-phase PENDING_CLAIMED→PENDING_READY), slot reuse during callback
  (read-before-CAS: read fields while PENDING_READY, CAS directly to
  DONE, no PROCESSING state). Added `--mode coroutine|tokio` to bench
  rpc. Coroutine peak 2.35M (2e16w), tokio peak 1.07M (45% of
  coroutine). Tokio mode: 45-69% throughput, 1.5-2.2x latency,
  SendQueueFull errors at 64+ loaders from burst-submit. Slab provides
  7-13% benefit over map-only for call(), kept for both modes. Send
  queue tuned per mode: 256 for coroutine (cache-friendly, same-thread
  drain), 1024 for tokio (absorbs scheduler bursts). Two-phase PENDING
  adds 1 store/submit (irreducible cost of write-before-CAS fix) —
  ~2-4% gap vs pre-fix baseline.
