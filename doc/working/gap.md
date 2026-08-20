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
| Connections per epoll fd | 32 (one side) | 16–32 (one side) | — (matched) |
| Process model | 2 processes | 2 processes | — (matched) |
| Client model | C++ coroutines | C++ coroutines | — (matched) |
| Avg latency at peak | 592µs | 242µs (128 coros) / 2ms (1000 coros) | — |
| Errors | 0 | 0 | — |

Source: `buzz-cpp/logs/bench.log.20260820-100050` (2,029,024 TPS),
CROW bench runs (128 coros, 16 conns, 16+16 workers, 128B, 5s).

---

## Same Technique, Different Architecture

### The inline-resume technique (identical in both)

Both use the buzz model: the C++ I/O worker directly resumes the next
iteration on the response thread — no scheduler, no thread switch.

- **buzz-cpp**: `coroutine_remote_caller::complete()` →
  `requests_.remove(id, pap)` → `handle.resume()`. The coroutine
  resumes inline, runs `on_run_resume()` (records metrics), loops back
  to `co_await co_feed()` → `operation->run()` → `co_call()` →
  `post_send()` — building and submitting the next request on the I/O
  worker thread.
  Source: `buzz-cpp/src/lib/buzz-rpc/send/coroutine_remote_caller.cpp`
  lines 81-95.

- **CROW**: C++ coroutine suspends at `co_await` → I/O worker receives
  response → `on_response` → `co_on_complete` → `handle.resume()`
  (direct function call, ~10ns). Rust `co_on_response()` runs via FFI
  on the I/O worker thread, records stats, then the coroutine loops
  back to `co_build_request()` → submit → suspend.
  Source: `app/crow-cli/src/bench/targets/rpc.rs` lines 309-420,
  `lib/crow-rpc/src/client/co_client.cpp`.

### CROW's slab pool is better than buzz-cpp's hash map

CROW uses a pre-allocated slab pool indexed by `request_id & pool_mask`
(O(1) array index, zero heap allocation). buzz-cpp uses `mt_hash_map`
—a lock-free concurrent hash map with hazard pointers and a heap
allocation per node (`new node_type(...)` on every `add`).

---

## The Remaining Architectural Diffs (3.9× gap)

### Diff 1: Worker count — 48 vs 32 (~1.5×)

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

### Diff 2: Per-op cost — flatbuffer + malloc vs custom codec (~1.3×)

buzz-cpp: custom binary codec (`msg_bench_write_request`), direct
`recv()` into message buffer, no heap alloc per frame. Per-op cost
is dominated by the syscall + epoll re-arm, not the codec.

CROW (after zero-copy parse refactor): flatbuffer build per op
(`FlatBufferBuilder::new()` + create + finish), FFI callback overhead
(~20ns/op). The `FrameParser` no longer mallocs per frame — control
uses a reused `std::vector`, data uses a pool `Buffer`. The flatbuffer
rebuild is the biggest remaining contributor — it allocates a new
buffer every op.

### Diff 3: Read path — direct-to-message-buffer vs copy-through-worker-buffer (resolved)

buzz-cpp: for the data phase, `recv()` reads directly into the message
object's data buffer (`transfer_socket_data`), avoiding one copy.
Source: `buzz-cpp/src/app/buzz-bench-server/rpc/proto/msg_bench_write_request.cpp`
lines 44-61.

CROW (after zero-copy parse refactor): when the parser is in
`ReadingData` state, `read()` goes directly into the pool-allocated
`data_buf_` — zero copy, matching buzz-cpp. Header + control still
read into the per-worker `recv_buf` (small, fixed-size, batched).
One residual copy remains: when header + control + data arrive in a
single `read()` batch, the data bytes in `recv_buf` are `memcpy`'d
to `data_buf_`. This is unavoidable (bytes already consumed from the
socket) and bounded by `data_size` (128B in bench — negligible).
For large payloads, most data arrives via the direct-read path.

---

## Lock Analysis — Full Hot Path

### Rust side (C++ coroutine FFI callbacks)

| Location | Lock type | Contended? | Hot path? |
| --- | --- | --- | --- |
| `CoCtx.stats` (atomics) | `AtomicU64` | No (Relaxed) | YES — every `co_on_response` |
| `request_id_counter` (AtomicU64) | atomic | No (per-coroutine slot) | No (C++ assigns slots) |
| `WorkerCounters` | lock-free atomics | No (per-worker) | YES — every `co_on_response` |
| `next_conn` (AtomicUsize) | atomic | No (only at spawn) | No |

**No contended locks on the Rust hot path.**

### C++ side (echo round-trip)

| Location | Lock type | Contended? | Hot path? |
| --- | --- | --- | --- |
| `Connection::send_mu_` (Mutex) | `std::mutex` | **YES** — every `enqueue_send` + `drain_send_queue` + `has_pending_send` | **YES** — 3× per op (request submit, server response enqueue, server batch flush) |
| `Connection::in_send_` (atomic bool) | CAS | Low (serializes writev, not the whole op) | YES — every writev |
| Slab pool `slot.state` (atomic u8) | atomic | No (per-slot, no cross-slot) | YES — every submit + response |
| `EpollEngine::mask_mu_` (Mutex) | `std::mutex` | Only in level-triggered mode | No (ONESHOT fast path skips it) |
| `EpollEngine::conn_mu_` (Mutex) | `std::mutex` | Only add/remove connection | No (setup only) |
| `FrameParser` pool alloc | `BufferPool` | Per-frame data `Buffer` alloc (pool-reused) | **YES** — 1 pool alloc + 1 release per op (server + client) |
| `RpcClient::pending_mu_` (Mutex) | `std::mutex` | No — slab path bypasses it | No (only oneshot path) |

**The `send_mu_` mutex is the contended C++ lock.** It's held during
`enqueue_send` (push to deque), `drain_send_queue` (pop from deque),
and `has_pending_send` (check if empty). With the 2-process model,
the same connection's send queue is touched by the submitter thread
(request) and the I/O worker (response batch flush), creating
cross-thread contention on the same mutex.

Source: `lib/crow-rpc/src/connection.cpp` lines 26-48, 57-223;
`lib/crow-rpc/include/crow-rpc/transport.h` lines 160-166.

### Per-op lock count (2-process, 16+16 workers)

Per echo round-trip (1 request + 1 response):

| Lock | Count | Where |
| --- | --- | --- |
| C++ `send_mu_` | 3 | request `enqueue_send`, response `enqueue_send`, response batch `has_pending_send` + `drain_send_queue` |
| C++ `in_send_` CAS | 2 | request writev, response writev |
| Slab `state` atomic | 2 | submit (PENDING), response (DONE) |
| `BufferPool` alloc/release | 2 | data `Buffer` alloc (server parse), data `Buffer` release (client callback) |

**Total: 3 C++ mutex acquisitions + 2 CAS + 2 pool ops per op.**
The C++ `send_mu_` is the only contended lock; the rest are
low-contention or per-slot.

---

## Recommendations (Next Optimizations)

### Flatbuffer template reuse (low effort, ~1.2×)

The `ConnectionPingRequest` is rebuilt per op via
`FlatBufferBuilder::new()`. Pre-build a template and patch the `id`
field. The server's `build_ping_response` has the same issue.

Expected: ~5µs saved per op (significant at low concurrency where
per-op cost is 12µs; less impact at high concurrency where queueing
dominates).

### Parser buffer pooling (done — zero-copy parse refactor)

`FrameParser` now uses a reused `std::vector` for control and a
`BufferPool`-allocated `Buffer` for data. No per-frame `malloc`/`free`
on the receive path. The `Frame` struct holds a `Buffer*` (ref-counted)
instead of raw pointers.

### More workers via multi-engine (medium effort, ~1.3×)

CROW currently uses 1 engine per process (1 epoll fd per side). Adding
`--io-engines 2` on the client side would give 2 client epoll fds,
allowing 32 client workers without ONESHOT re-arm contention on a
single fd. This matches buzz-cpp's 32 client workers.

The server side can also use multi-engine to scale beyond 16 workers.

### Lock-free send queue (high effort, ~1.1×)

Replace `Connection::send_mu_` + `std::deque` with a lock-free MPSC
queue. The send pattern is MPSC (multiple submitters, one writer
thread due to `in_send_`), so an MPSC ring buffer fits. This removes
the 3 mutex acquisitions per op.

This is the design's stated future optimization ("the design's
lock-free MPSC is a future optimization" —
`lib/crow-rpc/include/crow-rpc/transport.h` line 158).

---

## Projected TPS After Fixes

These are **rough estimates** based on bottleneck reasoning, not
measurements. Each multiplier is a guess; stacking them compounds
error. Treat as ordering of impact, not precise predictions.

| Fix | Estimated impact | Basis |
| --- | --- | --- |
| Current (2-process + C++ coroutines) | 520K (measured) | 128 coros, 16 conns, 16+16 workers, 128B, 5s |
| + Flatbuffer reuse | ~1.2× | eliminates per-op `FlatBufferBuilder::new()` alloc + build |
| + Parser buffer pooling (done) | ~1.1× | eliminated 4 malloc + 4 free per op via zero-copy parse |
| + Multi-engine (more workers) | ~1.3× | 2 client epoll fds → 32 client workers, matching buzz-cpp |
| + Lock-free send queue | ~1.1× | removes 3 mutex acquisitions per op |

Cumulative multiplier (if independent): ~1.9× → ~1.0M. This is
optimistic — the fixes likely overlap (e.g. reducing per-op cost
also reduces the benefit of more workers). Realistic range: 700K–1.0M.

**RDMA (R32) is untested.** The "2M+" target is based on buzz-cpp
hitting 2M with a similar coroutine technique, but RDMA bypasses the
kernel socket path entirely — the bottleneck shifts from epoll/syscall
overhead to NIC + PCIe latency. Whether CROW's flatbuffer + FFI
overhead matters at that level is unknown until measured.

---

## C++ Coroutine Client — Implementation Details

### Architecture

```
Rust (domain logic)          C++ I/O worker (epoll)
───────────────────          ──────────────────────
co_build_request() ───►      coroutine: build → submit → co_await (suspend)
  (alloc flatbuffer)         ...epoll_wait → read → parse → on_response...
                             co_on_complete → handle.resume() (inline)
co_on_response() ◄───        coroutine: process → loop back to build
  (record stats)
```

- C++20 coroutine suspends at `co_await`, yields I/O thread to epoll.
- Response arrives → `on_response` → `co_on_complete` → `handle.resume()`
  (direct function call, ~10ns — no scheduler, no channel).
- Rust domain logic (build request, record stats) runs via FFI on the
  I/O worker thread.
- Per-coroutine slab slot (no collision): `request_id = slot_index +
  N * pool_size`, so each coroutine always uses the same slot.

### Per-op overhead breakdown

| Component | cpp-coroutine |
| --- | --- |
| Submit | ~50ns |
| Resume | ~10ns (handle.resume) |
| Channel | 0 |
| FFI | ~20ns (build + on_response) |
| **Total** | **~80ns** |

The C++ coroutine has ~20ns FFI overhead per op (two `extern "C"`
calls), but no scheduler round-trip. This is 5× less overhead than
Rust coroutines (~400ns with tokio scheduler + oneshot channel).

### Current measured results (128B values, 5s, epoll + TCP loopback)

| Config | TPS | Avg lat | Notes |
| --- | --- | --- | --- |
| 128 coros, 16 conns, 16+16 workers | 518K | 242µs | peak per-worker efficiency |
| 1000 coros, 32 conns, 16+16 workers | 489K | 2.0ms | scales to 1000, latency rises |
