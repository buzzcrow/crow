<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# RPC Echo Flow Analysis

End-to-end trace of the CROW RPC echo path (in-process loopback
benchmark). Mirrors the structure of
[`kv-write-flow-analysis.md`](../design/kv/kv-write-flow-analysis.md).

The echo benchmark measures raw RPC transport throughput (epoll/kqueue
+ framing + request/response correlation) with no KV/storage layer.
The echo handler copies request data to response data, so the
benchmark is purely I/O-bound.

---

## Echo Flow — Callback Model (current)

The callback model uses an inline callback chain that runs entirely on
C++ I/O worker threads. 2 epoll events per op (down from 5 in the
original oneshot design).

Key optimizations:

- **udata dispatch**: epoll stores `Connection*` in udata — no
  per-event mutex + map lookup.
- **Caller-thread writev**: `submit` does `writev` directly on the
  caller's thread (no notify queue, no cross-thread round-trip). The
  `in_send_` CAS serializes concurrent senders; EAGAIN arms write on
  the owning engine.
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
    2. RpcClient::call_callback(server, conn, request_id, ctrl, data,
       ECHO_MSG_TYPE, bench_on_complete, slot_ptr)
       → slab pool: CAS FREE/DONE→PENDING to claim slot
       → transport->submit: enqueue_send + caller-thread writev
    3. thread::park()  ← wait for all in-flight to drain

  For callback-driven submits (bench_on_complete → submit_next),
  the caller IS the C++ I/O worker — writev happens on the I/O worker
  thread, no cross-thread notify at all.

  ── Server-side (same C++ I/O worker, same process) ──

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

---

## Thread Model

- **Rust worker threads** (N = `--threads`) — build requests, submit
  via FFI, then `thread::park()` until in-flight drain. The callback
  chain builds the next request on the C++ I/O worker thread — no
  tokio scheduler involvement.
- **C++ I/O worker threads** (`io_workers` total; per-engine =
  `io_workers / io_engines`) — each engine owns an independent epoll
  fd; connections are partitioned round-robin. When per-engine>1,
  workers share one fd with `EPOLLONESHOT`. Default 1 worker, no
  ONESHOT (level-triggered fast path).
- **C++ acceptor thread** (1) — accepts new connections. Not on the
  hot path after setup.
- **C++ reaper thread** (0 or 1, opt-in) — scans slab + map for
  timed-out entries. Not on the hot path.

---

## Rust API

The `crow-rpc-ffi` crate exposes two client models:

- **Oneshot (async, tokio-friendly)** — `call()` returns a `Future`
  that resolves on response. Uses `oneshot::channel` (2 heap allocs
  per call). This is the path used by the KV client and consensus
  code. Drop-safe, `Send + Sync`, no unsafe code needed.
- **Callback (sync, zero-alloc hot path)** — `call_callback()` stores
  the callback in a pre-allocated slab slot. The callback runs on the
  C++ I/O worker thread (must be non-blocking). Caller manages
  `user_data` lifetime. Used by the RPC bench target; ~2x TPS vs
  oneshot. Not tokio-specific.

Both models share the same `RpcClient`, `Connection`, `BufferPool`,
and `RpcServer`.

---

## Buffer Lifecycle

- **Request control + data**: allocated by Rust → `ref_clone` in C
  API → released after `writev` sends → recycles to pool.
- **Response control + data**: allocated by echo handler from pool →
  released after `writev` send → recycles to pool.
- **Response arrival on client**: parser malloc's control + data →
  wrapped as `ref=nullptr` Buffer → callback releases (no-op) →
  wrapper struct freed.

**Memory copy summary:**

- O(n) unavoidable: flatbuffer build, data payload fill, kernel
  socket copy (both directions), echo handler memcpy, header serialize.
- O(1) ref-count bumps: `ref_clone` on request, `release` after send.
- Zero-copy: `OutFrame` holds `Buffer*` pointers; `writev` iovecs
  point into Buffer data; `extract_request_id` reads in-place;
  response wrapped as `ref=nullptr` (no pool round-trip).

---

## Benchmark Results — 2026-08-20 (Slab Fallback + Reaper, Linux)

Platform: **AMD Ryzen 9 5950X** (16c/32t, x86_64, Linux 6.8).
Config: 128B values, 20s duration, in-process echo, epoll loopback,
pipeline_depth=1.

Regression sentinel: `tools/bench-rpc-regression.sh`.
Raw TSV: `doc/working/bench-rpc-regression.tsv`.

### Full sweep (6 configs)

| Eng | Wkr | T | C | ops/s | avg | p50 | p99 | p999 | raggr | saggr | err |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | 1 | 1 | 1 | 52,790 | 17 | 17 | 23 | 32 | 1.0 | 1.0 | 0 |
| 1 | 4 | 64 | 4 | 938,568 | 66 | 63 | 155 | 614 | 6.0 | 6.0 | 0 |
| 1 | 8 | 512 | 8 | 1,780,802 | 285 | 264 | 457 | 4112 | 11.0 | 11.7 | 0 |
| 2 | 8 | 512 | 8 | 1,744,829 | 291 | 270 | 387 | 438 | 7.6 | 7.8 | 0 |
| 1 | 16 | 1000 | 32 | 2,197,231 | 452 | 362 | 1607 | 2404 | 9.7 | 10.0 | 0 |
| 2 | 16 | 1000 | 16 | 2,293,581 | 432 | 379 | 1282 | 6092 | 9.1 | 10.0 | 0 |

Eng=io_engines, Wkr=total io_workers (Eng × per-engine; client uses
same config), T=client dispatch threads (coroutines), C=connections.
raggr=frames per read, saggr=frames per writev (server-side).

Peak **~2.29M** (2e16w 1000t16c) — 8.3x vs Gap4+Gap1 baseline (276K).
Zero errors across all 6 configs. `slab_fallback=0`, `map_in_flight=0`
across all configs (coroutine model pins fixed slots — CAS always
succeeds).

### Multi-worker scaling

- 1w→4w: **+17x** (53K→939K). 4 workers parallelize the callback
  chain; more coroutines + connections saturate the event loop.
- 4w→8w: **+1.9x** (939K→1.78M). 8 workers with 512 coroutines scale
  near-linearly.
- 1e8w vs 2e8w (512T:8C): **-2%** (1.78M→1.74M). 2 engines × 4
  workers/engine slightly underperforms 1 × 8 — engine overhead
  exceeds parallelism benefit at this concurrency.
- 1e16w 32c: 2.20M. Peak single-engine (16 workers on one epoll fd).
- 2e16w 16c: **2.29M (peak)**. 2 engines × 8 workers/engine beats
  1e16w by spreading workers across 2 epoll fds, reducing per-engine
  ONESHOT re-arm contention.

The slab fallback ceiling (~2.29M) is ~18.5× the KV write ceiling
(~124K, consensus quorum bound).

---

## Enhancement Ideas

- **Caller-thread writev contention** — the `in_send_` CAS serializes
  senders on the same connection. Per-connection send queues with
  lock-free MPSC enqueue + batch writev could reduce contention.
- **RDMA transport (R32)** — bypasses the kernel socket path
  entirely, eliminating `writev`/`read` copies and epoll event loop
  overhead.
- **Flatbuffer reuse** — `ConnectionPingRequest` is rebuilt per op.
  Pre-building a template and patching the `id` field would save
  ~5µs per op (significant at 1T:1C where per-op cost is 12µs).

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
  (CAS `DONE→PENDING` in `call_callback`, `PENDING→DONE` in
  `on_response`). `slab_fallback=0` across all configs. Per-op
  latency histogram fix for accurate p50/p99/p999. CLI flag renamed
  from `--io-workers-per-engine` to `--io-workers` (total).
