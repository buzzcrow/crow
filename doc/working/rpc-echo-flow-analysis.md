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
                → Worker::submit: lock submit_mu_, push to pending_submits_
                  → engine->notify_worker() (EVFILT_USER trigger)
                    [cross-thread: tokio worker → C++ I/O worker]
           e. crow_rpc_buffer_release(control + data)
              [decrements the ref bumped in (a), frees wrapper struct]
         → returns CallFuture { oneshot::Receiver }
    5. tokio::time::timeout(2s, CallFuture).await
       [yields tokio worker thread while waiting for response]

  ── C++ I/O worker thread (kqueue/epoll loop) ──

  Worker::run_loop (event loop)
    → kevent wait → Notify event (EVFILT_USER)
      → drain pending_submits_ (swap under submit_mu_)
      → for each (conn, frame):
          conn->enqueue_send(frame)  [push to connection send queue]
          engine->arm_write(fd)      [EVFILT_WRITE add]

    → kevent wait → Writable event (EVFILT_WRITE)
      → on_writable_impl(conn, fd)
        → drain_send_queue(batch, BATCH_MAX=64)
        → build iovecs: [header_buf][control][data] per frame
          [copy: serialize_header → stack header_bufs (12 bytes)]
          [zero-copy: control + data pointers go directly to writev]
        → writev(fd, iov, iov_count)
          [kernel copy: user → socket buffer (unavoidable)]
        → release fully-sent frames:
          frame->control->release()  [decrement ref, recycle to pool]
          frame->data->release()
          delete frame

  ── Server-side (same C++ I/O worker, same process) ──

    → kevent wait → Readable event (EVFILT_READ on client conn)
      → on_readable_impl(conn, fd)
        → FrameParser::advance: parse header → alloc control → alloc data
          [copy: ::read(fd, target.ptr, target.len) → parser buffers]
          [malloc: control + data are malloc'd by the parser]
        → conn->on_frame(frame) → RpcServer::dispatch(frame, conn)
          → handlers_.get_handler(msg_type=100) → echo_handler
            → extract_request_id(request->control)
              [zero-copy: reads flatbuffer `id` from control buffer]
            → build_ping_response(pool, req_id, 0)
              [pool alloc + memcpy: response control buffer]
            → pool->alloc(request->data_len) + memcpy(request->data)
              [copy: request data → response data buffer]
            → delete request (frees parser malloc'd control/data)
            → return OutFrame { response_ctrl, response_data }
          → transport->submit(conn, response)
            [same cross-thread submit path as the request]

  ── Response arrives back on client connection ──

    → kevent wait → Readable event (EVFILT_READ on client conn)
      → on_readable_impl → FrameParser::advance → conn->on_frame
        → RpcClient::on_response(req_id, frame)
          → lock pending_mu_, find + erase callback for req_id
          → callback(frame, RpcError::Ok)
            → OnCompleteAdapter::operator()(frame, Ok)
              → wrap response control/data in crow_rpc_buffer_s handles
                [zero-copy: Buffer wraps raw parser pointer, ref=nullptr
                 (release is a no-op; Frame destructor already nulled)]
              → crow_rpc_on_complete(req_id, ctrl_handle, data_handle,
                CROW_RPC_OK, user_data)
                → Rust: on_complete_cb
                  → Box::from_raw(user_data) → oneshot::Sender
                  → Buffer::from_raw(ctrl_handle + data_handle)
                  → tx.send(Ok(Response { control, data }))
                    [wakes the tokio task awaiting CallFuture]

  ── Back on tokio worker thread ──

    6. CallFuture resolves → OpOutcome { ok: true }
       [Buffer Drop: releases response control + data (no-op for
        malloc'd parser buffers, frees the crow_rpc_buffer_s wrapper)]
```

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
64-byte values, 1000 key space (unused by echo), 5-second duration.
Platform: **Apple M5 Pro** (18 cores, arm64, macOS 26/Darwin 25.5).
7 runs, zero errors across all configs.

Regression sentinel: `tools/bench-rpc-regression.sh`.
Raw TSV: `doc/working/bench-rpc-regression.tsv`.

### Full sweep

| Threads | Conn | Pipeline | Throughput (ops/s) | avg (µs) | p50 (µs) | p99 (µs) | p999 (µs) | Errors |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | 1 | 1 | 32,736 | 29 | 29 | 50 | 94 | 0 |
| 8 | 4 | 32 | 83,850 | 94 | 94 | 132 | 209 | 0 |
| 16 | 8 | 128 | 89,994 | 177 | 175 | 233 | 398 | 0 |
| 64 | 8 | 512 | 96,508 | 662 | 657 | 789 | 1,332 | 0 |
| 128 | 16 | 2048 | 99,301 | 1,288 | 1,275 | 1,532 | 2,344 | 0 |
| 256 | 16 | 4096 | 103,058 | 2,483 | 2,454 | 3,192 | 4,320 | 0 |
| 512 | 32 | 16384 | 103,650 | 4,939 | 4,884 | 5,804 | 6,864 | 0 |

### Conclusions

- **Single C++ I/O worker is the serialization bottleneck** — TPS
  plateaus at ~104K at 256T+. Beyond 256T, latency doubles (2.5ms →
  4.9ms avg) without TPS gain. The single worker thread's event loop
  (kqueue wait + drain submits + readv/writev + dispatch) serializes
  all 5 event-loop iterations per op across all connections.
- **NOT CPU-bound — latency-bound by event queueing** — at 256T:16C,
  CPU is 78% idle. The machine has plenty of headroom. The 86×
  latency increase from 1T:1C (29µs) to 256T:16C (2500µs) is pure
  queueing delay on the single I/O thread's event queue, not compute.
  Little's Law: 256 in-flight × 5 events/op = ~1280 events queued at
  ~2µs/event = 2.5ms round-trip — exactly what we observe.
- **Value size has minimal effect** — at 256T:16C, 0B→4096B (64×
  larger payload) only drops TPS by 16% (114K→96K). If the bottleneck
  were data copy/memcpy, we'd expect a steep drop. Flat TPS across
  payload sizes confirms the bottleneck is event loop overhead, not
  data movement.
- **Threads scale well to 16T** — 1T→16T gives 2.8× (33K→90K). Beyond
  16T the curve flattens (90K→104K from 16T→512T, only 1.15×).
- **T:C ratio has minimal effect** — 128T:16C and 128T:8C both ~99K
  (tested separately). The bottleneck is the I/O thread, not the
  number of sockets.
- **Zero errors across all 7 configs** — the 2s response timeout
  never fires under normal load; the in-process loopback is reliable.
- **Per-op latency at 1T:1C is 29µs** — this is the round-trip cost
  of: flatbuffer build + pool alloc + FFI call + kqueue notify +
  writev + read + echo handler + writev + read + oneshot wake. The
  kernel socket loopback adds ~10-15µs; the rest is framing + FFI +
  flatbuffer overhead.

### Scaling ceiling comparison

| Target | Ceiling (ops/s) | Bottleneck | Platform |
| --- | --- | --- | --- |
| RPC echo (in-process) | ~104K | Single C++ I/O worker thread | M5 Pro |
| KV write (coalesced, 3-node) | ~48K | Consensus quorum RPC | M5 Pro |
| KV write (coalesced, 3-node) | ~124K | Consensus quorum RPC | AMD 5950X |

The RPC echo ceiling is ~2× the KV write ceiling on the same M5 Pro
platform — the KV path adds Paxos consensus (2-phase quorum round +
WAL persist + engine apply) on top of the RPC transport. Closing this
gap requires either multi-worker I/O (parallel kqueue/epoll loops) or
RDMA transport (R32, planned).

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

- **Multi-worker I/O** — the single C++ I/O worker thread serializes
  all event processing. With 78% idle CPU at saturation, adding
  per-connection worker assignment (round-robin or hash-based) with
  separate kqueue/epoll loops would reduce per-worker event queue
  depth, directly cutting queueing latency and lifting TPS. The
  transport already has a `Worker` abstraction with `get_worker()`
  (round-robin), but only 1 worker is created (`SocketTransport(1,
  pool_)`). 4-8 workers would likely scale TPS 3-5× on 18 cores.
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
  1T:1C where per-op cost is 29µs).
