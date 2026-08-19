<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# RPC Echo Flow Analysis

End-to-end trace of the CROW RPC echo path (in-process loopback
benchmark). Mirrors the structure of
[`kv-write-flow-analysis.md`](../design/kv/kv-write-flow-analysis.md).
Focuses on flow, conclusions, and data — not rationale prose.

The echo benchmark measures raw RPC transport throughput (epoll/kqueue
+ framing + request/response correlation) with no KV/storage layer in
the path. The echo handler copies request data to response data, so
the benchmark is purely I/O-bound.

---

## Echo Flow — Single Request/Response

The flow below shows the optimized path (3 event-loop iterations per
op, down from 5 in the original design). The optimizations:
- **udata dispatch**: kqueue/epoll stores `Connection*` in udata,
  eliminating per-event mutex + map lookup in `wait()`.
- **Direct-write on notify**: when draining pending submits, try
  `writev` immediately instead of arming write + waiting for a
  Writable event. Eliminates 1 event per op (the Writable event for
  the request).
- **submit_inline for server responses**: the server dispatch calls
  `submit_inline` (direct enqueue + writev) instead of `transport->
  submit` (cross-thread notify). Eliminates 1 event per op (the
  Notify event for the response).

```
Rust worker (tokio task)
  → RpcBenchClient::issue_op
    1. request_id = global AtomicU64::fetch_add
       [global counter: unique IDs across workers — the C API uses
        the flatbuffer `id` field for response correlation]
    2. Build ConnectionPingRequest flatbuffer (control message)
       - FlatBufferBuilder::create + finish
         [copy: fbb internal buffer → pool Buffer via ctrl.write()]
    3. Allocate data payload from BufferPool (if value_size > 0)
       - pool.alloc_buffer(value_size)
       - Fill with deterministic pattern (iter % 256 per byte)
         [copy: Vec<u8> → pool Buffer via buf.write()]
    4. RpcClient::call(server, conn, ctrl, data, ECHO_MSG_TYPE)
       → Rust FFI: crow_rpc_client_call(...)
         → C API (c_api.cpp):
           a. ref_clone() on ctrl + data buffers
              [O(1) ref-count bump: C++ RpcClient holds its own ref]
           b. attach(client, conn) — set on_frame callback for response
              routing (idempotent)
           c. extract_request_id(ctrl_buf) — flatbuffer `id` field
              [zero-copy: reads directly from the control buffer]
           d. RpcClient::call(transport, conn, req_id, ctrl, data,
              msg_type, OnCompleteAdapter callback)
              → insert into pending_ map (req_id → callback)
                [mutex lock: pending_mu_]
              → build_frame: new OutFrame { request_id, header, ctrl, data }
                [zero-copy: OutFrame holds Buffer pointers, no copy]
              → transport->submit(conn, frame)
                → SocketTransport::submit: lock submit_mu_, push to
                  pending_submits_ → engine->notify_worker()
                    [cross-thread: tokio worker → C++ I/O worker]
           e. crow_rpc_buffer_release(control + data)
              [decrements the ref bumped in (a), frees wrapper struct]
         → returns CallFuture { oneshot::Receiver }
    5. tokio::time::timeout(2s, CallFuture).await
       [yields tokio worker thread while waiting for response]

  ── C++ I/O worker thread (kqueue/epoll loop) ──

  Worker::run_loop (event loop)

  Event 1: Notify (EVFILT_USER) — drain submits + direct write
    → kevent wait → Notify event
      → drain_pending_submits():
        → swap pending_submits_ under submit_mu_
        → for each (conn, frame):
            conn->enqueue_send(frame)
            on_writable_impl(conn, fd)  ← DIRECT WRITE (no Writable event)
              → drain_send_queue + build iovecs + writev
                [kernel copy: user → socket buffer (unavoidable)]
              → release sent frames (buffer refcount decrement)
            if partial write: engine->arm_write(fd)  ← only on EAGAIN

  ── Server-side (same C++ I/O worker, same process) ──

  Event 2: Readable (EVFILT_READ) — read + echo + direct write response
    → kevent wait → Readable event (udata = Connection*, no map lookup)
      → on_readable_impl(conn, fd)
        → FrameParser::advance: parse header → alloc control → alloc data
          [copy: ::read(fd, target.ptr, target.len) → parser buffers]
        → conn->on_frame(frame) → RpcServer::dispatch(frame, conn)
          → echo_handler:
            → extract_request_id(request->control)
            → build_ping_response(pool, req_id, 0)
              [pool alloc + memcpy: response control buffer]
            → pool->alloc + memcpy(request->data)
              [copy: request data → response data buffer]
            → delete request
            → return OutFrame { response_ctrl, response_data }
          → submit_inline(conn, response)  ← NO Notify event
            → conn->enqueue_send(response)
            → on_writable_impl(conn, fd)  ← DIRECT WRITE response
              → writev response frame to socket

  ── Response arrives back on client connection ──

  Event 3: Readable (EVFILT_READ) — read response + callback
    → kevent wait → Readable event (udata = Connection*)
      → on_readable_impl → FrameParser::advance → conn->on_frame
        → RpcClient::on_response(req_id, frame)
          → lock pending_mu_, find + erase callback for req_id
          → callback(frame, RpcError::Ok)
            → OnCompleteAdapter::operator()(frame, Ok)
              → wrap response control/data in crow_rpc_buffer_s handles
                [zero-copy: Buffer wraps raw parser pointer, ref=nullptr]
              → crow_rpc_on_complete(req_id, ctrl_handle, data_handle,
                CROW_RPC_OK, user_data)
                → Rust: on_complete_cb
                  → Box::from_raw(user_data) → oneshot::Sender
                  → Buffer::from_raw(ctrl_handle + data_handle)
                  → tx.send(Ok(Response { control, data }))
                    [wakes the tokio task awaiting CallFuture]

  ── Back on tokio worker thread ──

    6. CallFuture resolves → OpOutcome { ok: true }
       [Buffer Drop: releases response control + data]
```

**Events per op: 3** (down from 5 in the original design):
1. Notify — drain submits + direct write request
2. Readable (server) — read + echo + direct write response
3. Readable (client) — read response + callback

The original design had 5 events: Notify → Writable (request) →
Readable (server) → Notify → Writable (response) → Readable (client).

---

## Thread Model

- **Tokio worker threads** (N = `--threads`) — build flatbuffer control
  messages, allocate pool buffers, submit via FFI, await response
  futures. Each worker holds a cloned `RpcBenchClient` (Arc handles).
- **C++ I/O worker thread** (1, single worker) — owns all connections,
  runs the kqueue/epoll event loop, handles read/write/notify events
  for both client-side and server-side sockets. This is the single
  bottleneck (see Benchmark Results).
- **C++ acceptor thread** (1) — accepts new connections on the listen
  socket. Not on the hot path after setup.

All request/response correlation happens via the C++ `RpcClient`'s
`pending_` map (mutex-protected `unordered_map<req_id, callback>`).
The C++ I/O thread calls the callback directly when a response frame
arrives, which sends into a tokio oneshot channel.

---

## Buffer Lifecycle

Pool-allocated buffers (BufferPool, max 4096):

- **Request control + data**: allocated by Rust → `ref_clone` in C API
  (C++ holds a ref) → caller's wrapper released after submit → C++
  releases its ref after `writev` sends the frame → buffer recycles
  to pool.
- **Response control + data**: allocated by the echo handler from the
  pool → sent via `writev` → released after send → recycles to pool.
- **Response arrival on client**: parser malloc's control + data (not
  pool-allocated) → wrapped in `crow_rpc_buffer_s` with `ref=nullptr`
  → Rust `Buffer::from_raw` → Drop calls `crow_rpc_buffer_release`
  (no-op for ref=nullptr) → `crow_rpc_buffer_s` struct freed.

The buffer leak fix (2026-08-19): the C API's `crow_rpc_client_call`
was bumping refcounts via `ref_clone()` but never releasing the
caller's `crow_rpc_buffer_s` wrapper handles — each request leaked 2
buffer refs + 2 wrapper structs, exhausting the 4096-buffer pool
after ~2048 ops. Fix: call `crow_rpc_buffer_release()` on the
caller's handles after submit.

---

## Benchmark Results — 2026-08-19

Scaling sweep. In-process echo server (kqueue loopback, no network),
64-byte values, 1000 key space (unused by echo), 5-second duration,
`io_workers=1` (single-worker fast path). Platform: **Apple M5 Pro**
(18 cores, arm64, macOS 26/Darwin 25.5). 7 runs, zero errors across
all configs.

Regression sentinel: `tools/bench-rpc-regression.sh`.
Raw TSV: `doc/working/bench-rpc-regression.tsv`.

### Full sweep (after single-epoll optimizations)

| Threads | Conn | Pipeline | Throughput (ops/s) | avg (µs) | p50 (µs) | p99 (µs) | p999 (µs) | Errors |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | 1 | 1 | 37,363 | 26 | 25 | 39 | 76 | 0 |
| 8 | 4 | 32 | 105,483 | 75 | 74 | 106 | 169 | 0 |
| 16 | 8 | 128 | 114,916 | 138 | 135 | 268 | 379 | 0 |
| 64 | 8 | 512 | 122,264 | 522 | 516 | 720 | 1,045 | 0 |
| 128 | 16 | 2048 | 126,014 | 1,015 | 1,003 | 1,286 | 1,848 | 0 |
| 256 | 16 | 4096 | 125,622 | 2,037 | 1,987 | 3,388 | 4,900 | 0 |
| 512 | 32 | 16384 | 124,087 | 4,127 | 4,066 | 4,920 | 5,372 | 0 |

### Before vs after single-epoll optimizations

The optimizations (udata dispatch, direct-write on notify, submit_inline
for server responses) lifted the ceiling from ~104K to ~126K — a 22%
improvement. Per-op latency at 1T:1C dropped from 29µs to 26µs.

| Config | Before (ops/s) | After (ops/s) | Improvement |
| --- | --- | --- | --- |
| 1T:1C | 33K | 37K | 12% |
| 8T:4C | 84K | 105K | 25% |
| 16T:8C | 90K | 115K | 28% |
| 128T:16C | 99K | 126K | 27% |
| 256T:16C | 103K | 126K | 22% |

### Multi-worker (EV_ONESHOT) — does not help for loopback

| io_workers | TPS (ops/s) | avg (µs) | Errors |
| --- | --- | --- | --- |
| 1 | 126K | 1,015 | 0 |
| 2 | 125K | 2,047 | 0 |
| 4 | 117K | 2,194 | 0 |
| 8 | 62K | 4,109 | 0 |

Multi-worker with `EV_ONESHOT` re-arm is *worse* than single-worker for
the in-process loopback. The re-arm overhead (1 extra `kevent` syscall
per event) + kernel-level contention on the shared kqueue fd exceeds
any parallelism benefit. The single I/O worker is NOT CPU-bound (78%
idle at saturation) — the bottleneck is event queueing latency, not
compute. Multi-worker would help with real network I/O where the I/O
thread is CPU-bound on socket operations.

### Conclusions

- **Single C++ I/O worker is the serialization bottleneck** — TPS
  plateaus at ~126K at 128T+. Beyond 128T, latency doubles (1ms →
  2ms → 4ms avg) without TPS gain. The single worker thread's event
  loop serializes all event processing across all connections.
- **NOT CPU-bound — latency-bound by event queueing** — at 256T:16C,
  CPU is 78% idle. The 78× latency increase from 1T:1C (26µs) to
  256T:16C (2037µs) is pure queueing delay on the single I/O thread's
  event queue, not compute.
- **Single-epoll optimizations gave 22% TPS gain** — udata dispatch
  (eliminate per-event mutex), direct-write on notify (eliminate
  Writable event for requests), submit_inline for server responses
  (eliminate Notify event for responses) reduced events per op from
  5 to 3 on the fast path.
- **Multi-worker (EV_ONESHOT) does not help for loopback** — the
  re-arm overhead exceeds parallelism benefit when the I/O thread is
  not CPU-bound. Would help with real network I/O.
- **Value size has minimal effect** — at 256T:16C, 0B→4096B (64×
  larger payload) only drops TPS by 16%. Flat TPS across payload sizes
  confirms the bottleneck is event loop overhead, not data movement.
- **Zero errors across all 7 configs** — the 2s response timeout
  never fires under normal load; the in-process loopback is reliable.
- **Per-op latency at 1T:1C is 26µs** — this is the round-trip cost
  of: flatbuffer build + pool alloc + FFI call + kqueue notify +
  writev + read + echo handler + writev + read + oneshot wake.

### Scaling ceiling comparison

| Target | Ceiling (ops/s) | Bottleneck | Platform |
| --- | --- | --- | --- |
| RPC echo (in-process, optimized) | ~126K | Single C++ I/O worker thread | M5 Pro |
| KV write (coalesced, 3-node) | ~48K | Consensus quorum RPC | M5 Pro |
| KV write (coalesced, 3-node) | ~124K | Consensus quorum RPC | AMD 5950X |

The RPC echo ceiling is ~2.6× the KV write ceiling on the same M5 Pro
platform — the KV path adds Paxos consensus (2-phase quorum round +
WAL persist + engine apply) on top of the RPC transport.

---

## Memory Copy Summary

Copy points are annotated inline in the flow diagram above. Summary:

- **O(n) unavoidable** — flatbuffer build (FBB internal → pool Buffer);
  data payload fill (Vec<u8> → pool Buffer); kernel socket copy
  (user → socket buffer on `writev`, socket → user on `read`); echo
  handler data copy (request → response Buffer via `memcpy`); header
  serialize (12 bytes → stack buffer per frame).
- **O(1) ref-count bumps (negligible)** — `ref_clone` on request
  control + data in C API; `release` after `writev` send; `release`
  of caller's wrapper handles after submit.
- **Zero-copy (pointer pass)** — `OutFrame` holds `Buffer*` pointers
  (no copy on submit); `writev` iovecs point directly into Buffer
  data (no copy on send); `extract_request_id` reads flatbuffer `id`
  in-place; response control/data wrapped as `ref=nullptr` Buffers
  (no copy, no pool round-trip).

---

## Enhancement Ideas

- **Multi-worker I/O (for real network, not loopback)** — the single
  C++ I/O worker thread serializes all event processing. Multi-worker
  with `EV_ONESHOT` is implemented (`--io-workers N`) but does not
  help for the in-process loopback (re-arm overhead > parallelism
  benefit when I/O thread is not CPU-bound). Would help with real
  network I/O where the I/O thread is CPU-bound on socket operations.
  The transport supports shared-engine multi-worker with `EV_ONESHOT`
  re-arm, following the reference's design.
- **RDMA transport (R32)** — bypasses the kernel socket path entirely,
  eliminating the `writev`/`read` copies and the kqueue/epoll event
  loop overhead. The reference's RDMA path achieves ~2× the TCP path.
- **Buffer pool sizing** — the current 4096-buffer pool limits
  pipeline depth. At 512T:32C with pipeline=16384, the pool is the
  constraint (alloc returns None → op dropped). A larger pool or
  dynamic growth would remove this limit.
- **Flatbuffer reuse** — the `ConnectionPingRequest` is rebuilt per
  op (FlatBufferBuilder is fresh each time). Pre-building a template
  and patching the `id` field would save ~5µs per op (significant at
  1T:1C where per-op cost is 26µs).
