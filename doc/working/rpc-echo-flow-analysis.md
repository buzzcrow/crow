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

## Echo Flow — Callback Model (Gap2+Gap3, current)

The callback model replaces the tokio oneshot + scheduler path with
an inline callback chain that runs entirely on the C++ I/O worker
threads. 2 epoll events per op (down from 5 in the original design).
Key optimizations:

- **udata dispatch**: epoll stores `Connection*` in udata, eliminating
  per-event mutex + map lookup.
- **Direct-write on notify**: when draining pending submits, try
  `writev` immediately instead of arming write + waiting for a
  Writable event. Eliminates 1 event per op.
- **submit_inline for server responses**: server dispatch calls
  `submit_inline` (enqueue only) instead of `transport->submit`
  (cross-thread notify). Eliminates 1 event per op.
- **Per-worker receive buffer**: one big `read()` into a 256KB
  per-worker buffer, then `feed_data` parses all frames it contains.
- **Send aggregation**: after processing all readable events in one
  event-loop batch, `writev` all pending responses per connection in
  one syscall.
- **Slab completion pool**: O(1) bitmask index (`request_id &
  pool_mask`), no hash map lookup, zero per-call heap allocation.
  CAS `FREE/DONE→PENDING` claims the slot; if occupied, falls back to
  `folly::ConcurrentHashMap` (one heap alloc). CAS `PENDING→DONE` in
  `on_response` prevents double-invoke with the reaper.
- **Inline callback resume**: C++ I/O worker invokes the C ABI
  callback directly on the response thread — no oneshot channel, no
  tokio scheduler round-trip, no thread switch.
- **Timeout reaper**: background thread scans slab + map every
  `scan_interval_ns` for entries past their deadline. Timed-out
  entries are failed with `RpcError::Timeout` and reclaimed. Prevents
  leaked slots/entries from lost responses (server crash, network
  black hole).

```
Rust worker thread (std::thread::spawn, NOT tokio)
  → submit_next(ctx, None)  ← initial kickoff
    1. request_id = global AtomicU64::fetch_add
    2. Build ConnectionPingRequest flatbuffer (control message)
       [copy: fbb internal buffer → pool Buffer via ctrl.write()]
    3. Allocate data payload from BufferPool (if value_size > 0)
       [copy: Vec<u8> → pool Buffer via buf.write()]
    4. RpcClient::call_callback(server, conn, request_id, ctrl, data,
       ECHO_MSG_TYPE, bench_on_complete, slot_ptr)
       → Rust FFI: crow_rpc_client_call_callback(...)
         → C API (c_api.cpp):
           a. ref_clone() on ctrl + data buffers
              [O(1) ref-count bump: C++ RpcClient holds its own ref]
           b. RpcClient::call_callback(transport, conn, request_id,
              ctrl, data, msg_type, cb, user_data)
              → slab pool: CAS FREE/DONE→PENDING to claim slot
                slot[idx].request_id = request_id
                slot[idx].cb = bench_on_complete
                slot[idx].user_data = slot_ptr
                slot[idx].state = PENDING (atomic acq_rel)
                [O(1) bitmask index, no hash map lookup]
                [if CAS fails: fall back to folly map (one heap alloc)]
              → build_frame: new OutFrame { request_id, header, ctrl, data }
                [zero-copy: OutFrame holds Buffer pointers, no copy]
              → transport->submit(conn, frame)
                → SocketTransport::submit: enqueue_send(frame) +
                  caller-thread writev (buzz model — no notify queue,
                  no cross-thread round-trip). The in_send_ CAS
                  serializes concurrent senders; if writev hits EAGAIN,
                  arm write on the owning engine.
                    [kernel copy: user → socket buffer (unavoidable)]
           c. crow_rpc_buffer_release(control + data)
              [decrements the ref bumped in (a), frees wrapper struct]
    5. thread::park()  ← wait for all in-flight to drain

  Note: for callback-driven submits (bench_on_complete → submit_next),
  the caller thread IS the C++ I/O worker (the callback runs inline on
  the I/O worker thread), so the writev happens on the I/O worker
  thread — no cross-thread notify at all. notify_worker() is only used
  for shutdown wake (Worker::stop), not on the hot path.

  ── Server-side (same C++ I/O worker, same process) ──

  Event 1: Readable — read + echo + enqueue response
    → epoll_wait → Readable event (udata = Connection*, no map lookup)
      → on_readable_impl(conn, fd, recv_buf, recv_buf_size, pending_writes)
        → ::read(fd, recv_buf, 256KB)  ← ONE big read for multiple frames
        → parser.feed_data(recv_buf, n, on_frame callback)
          → copies bytes into parser buffers, yields complete frames
          → for each frame: conn->on_frame(frame)
            → RpcServer::dispatch → echo_handler
              → build response (pool alloc + memcpy)
              → submit_inline(conn, response)  ← ENQUEUE ONLY (no writev)
        → if conn has pending sends: add to pending_writes

  After all events in this batch:
    → for each conn in pending_writes:
        → on_writable_impl(conn, fd)  ← ONE writev for all responses

  ── Response arrives back on client connection ──

  Event 2: Readable — read response + callback
    → epoll_wait → Readable event (udata = Connection*)
      → on_readable_impl → FrameParser::advance → conn->on_frame
        → RpcClient::on_response(req_id, frame)
          → slab pool: idx = request_id & pool_mask_
            slot[idx].state == PENDING && slot[idx].request_id == req_id
            → CAS PENDING→DONE (prevents double-invoke with reaper)
            → invoke_c_complete(cb, user_data, req_id, frame, Ok)
              → frame_to_c_handles: wrap control/data in buffer handles
                [zero-copy: Buffer wraps raw parser pointer, ref=nullptr]
              → cb(req_id, ctrl_handle, data_handle, CROW_RPC_OK, user_data)
                → bench_on_complete (Rust, unsafe extern "C"):
                  → record latency + outcome (if past measure_start)
                  → release response buffer handles
                  → if before deadline: submit_next(ctx, Some(req_id))
                    → request_id = req_id + pool_size  ← SAME slot
                    → build next request + call_callback
                  → else: drain_in_flight (atomic fetch_sub)
                    → if in_flight == 0: thread::unpark(worker)

  ── Back on Rust worker thread ──

    6. thread::park() returns → collect stats → return
```

**Events per op: 2**:
1. Readable (server) — read + echo + enqueue response + batch writev
2. Readable (client) — read response + inline callback + caller-thread
   writev (next request)

The request writev happens on the caller thread (Rust worker for the
initial kickoff, C++ I/O worker for callback-driven submits) — not an
epoll event. The server response writev happens in the post-event
batch flush (send aggregation) — also not a separate epoll event.

**What the callback model eliminated (vs the old oneshot path):**
- `oneshot::channel()` per call (2 allocs + 1 free)
- `Box::into_raw`/`from_raw` for user_data (slot pointer is
  pre-allocated in a `Box<[BenchSlot]>`)
- tokio reactor wake + thread switch (callback runs inline on the
  I/O worker thread)
- `folly::ConcurrentHashMap` lookup/erase on the hot path (O(1) bitmask
  index; map is only used as slab fallback when a slot is occupied)
- Slot reuse: callback advances request_id by pool_size (not +1) to
  stay in the same slab slot, preventing out-of-order response
  collisions
- Silent callback loss under worker contention (CAS PENDING→DONE in
  on_response prevents double-invoke; CAS FREE/DONE→PENDING in
  call_callback prevents slot corruption)

---

## Thread Model

- **Rust worker threads** (N = `--threads`, `std::thread::spawn`) —
  build flatbuffer control messages, allocate pool buffers, submit via
  FFI, then `thread::park()` until all in-flight drain. The callback
  chain (invoked on the C++ I/O worker thread) builds the next request
  and submits it — no tokio scheduler involvement.
- **C++ I/O worker threads** (`io_engines` × `workers_per_engine`,
  default 1×1) — each engine owns an independent epoll fd; connections
  are partitioned round-robin across engines. When
  `workers_per_engine>1`, workers share one engine fd with
  `EPOLLONESHOT`. The default 1×1 config uses a single worker with no
  ONESHOT (level-triggered fast path).
- **C++ acceptor thread** (1) — accepts new connections. Not on the
  hot path after setup.
- **C++ reaper thread** (0 or 1, opt-in via `start_reaper`) — scans
  the slab pool + pending map every `scan_interval_ns` for timed-out
  entries. Not on the hot path; only active when timeouts are enabled.

Request/response correlation: the callback model uses a pre-allocated
slab pool (`CompletionSlot[]`, indexed by `request_id & pool_mask`).
CAS `FREE/DONE→PENDING` claims a slot; if occupied, falls back to
`folly::ConcurrentHashMap` (one heap alloc). CAS `PENDING→DONE` in
`on_response` prevents double-invoke with the reaper. The oneshot
model (still available for non-bench callers) uses the same
`folly::ConcurrentHashMap` directly.

---

## Rust API

The `crow-rpc-ffi` crate exposes two client models:

### Oneshot model (async, tokio-friendly)

```rust,ignore
let client = RpcClient::new();
client.attach(&conn);
let fut = client.call(&server, &conn, ctrl, data, msg_type)?;
let response = tokio::time::timeout(Duration::from_secs(2), fut).await??;
// response.control, response.data → Option<Buffer>
```

- `call()` returns a `CallFuture` (implements `Future`) that resolves
  when the response arrives. Internally uses `oneshot::channel` +
  `Box::into_raw` for the sender — 2 heap allocs per call.
- Drop-safe: `RpcClient`, `CallFuture`, `Buffer` all implement
  `Drop` correctly. No manual cleanup needed.
- `Send + Sync`: `RpcClient` is safe to share across tokio tasks.
- This is the path used by the KV client and consensus code.

### Callback model (sync, zero-alloc hot path)

```rust,ignore
let client = RpcClient::new();
client.attach(&conn);
client.set_completion_pool_size(max_in_flight);
client.call_callback(
    &server, &conn, request_id, ctrl, data, msg_type,
    Some(my_callback), user_data_ptr,
)?;
// callback fires on the C++ I/O worker thread when response arrives
```

- `call_callback()` stores the callback + `user_data` in a pre-allocated
  slab slot and submits. Zero heap allocation per call.
- The callback (`unsafe extern "C" fn`) runs on the C++ I/O worker
  thread — must be non-blocking. It receives the response buffers as
  C handles (`crow_rpc_buffer_t`), which must be released via
  `crow_rpc_buffer_release`.
- The caller manages `user_data` lifetime (typically a pointer into a
  pre-allocated array, not a per-call `Box`).
- The caller must ensure at most `max_in_flight` requests are
  in-flight (so no two share a slab slot).
- This is the path used by the RPC bench target. It is **not**
  tokio-specific — works from any thread, including `std::thread`.

### Is it easy for Rust code to use this RPC lib?

**Yes, for the oneshot model** — it's a standard async Rust API:
`call()` returns a `Future`, you `.await` it under tokio. The KV
client and consensus code already use this path. Buffer management is
automatic (Drop). No unsafe code needed from the caller.

**The callback model is more involved** — it's a low-level C ABI
callback with manual `user_data` lifetime management and raw buffer
handle release. It's designed for hot paths where you want to bypass
tokio's scheduler entirely (benchmarks, high-throughput pipelines).
Most application code should use the oneshot model; the callback model
is opt-in for cases where the ~2x TPS improvement matters.

Both models share the same `RpcClient`, `Connection`, `BufferPool`,
and `RpcServer` — you can mix them on the same client (though a
single call uses one model or the other, not both).

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
  → callback releases via `crow_rpc_buffer_release` (no-op for
  ref=nullptr) → `crow_rpc_buffer_s` struct freed.

---

## Benchmark Results — 2026-08-20 (Slab Fallback + Reaper, Linux)

Platform: **AMD Ryzen 9 5950X** (16c/32t, x86_64, Linux 6.8).
Config: 128B values, 20s duration, in-process echo, epoll loopback,
pipeline_depth=1.

Regression sentinel: `tools/bench-rpc-regression.sh`.
Raw TSV: `doc/working/bench-rpc-regression.tsv`.

### Full sweep (9 configs)

| Eng | Wkr | T | C | ops/s | avg | p50 | p99 | p999 | err |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | 1 | 1 | 1 | 52,759 | 18 | 18 | 18 | 18 | 0 |
| 1 | 1 | 64 | 4 | 312,816 | 204 | 204 | 204 | 204 | 0 |
| 1 | 1 | 256 | 8 | 345,745 | 740 | 740 | 740 | 740 | 0 |
| 2 | 1 | 512 | 8 | 571,031 | 895 | 895 | 895 | 895 | 0 |
| 1 | 2 | 512 | 8 | 606,933 | 842 | 842 | 842 | 842 | 0 |
| 1 | 1 | 1000 | 32 | 307,580 | 3247 | 3246 | 3246 | 3246 | 0 |
| 1 | 4 | 1000 | 32 | 1,084,619 | 920 | 920 | 920 | 920 | 0 |
| 1 | 16 | 1000 | 32 | 2,274,259 | 438 | 438 | 438 | 438 | 0 |
| 2 | 16 | 1000 | 32 | 2,339,116 | 425 | 425 | 425 | 425 | 0 |

Peak **~2.34M** (2e16w 1000t32c) — 4.0x vs B2 (585K), 8.5x vs Gap4+Gap1
baseline (276K). Sentinel (1e1w 256t8c): 346K vs 358K B2 = -3% (CAS
acquire-load overhead on the uncontended hot path). Zero errors across
all 9 configs.

### Slab vs map usage

Client counters across all configs: `slab_fallback=0`,
`map_in_flight=0`, `slab_in_flight=0` (at snapshot). The coroutine
model pins a fixed `slot_index` per coroutine (`req_id = slot_index +
N * pool_size`), so the CAS `DONE→PENDING` always succeeds — no two
coroutines target the same slot. The map fallback exists for the
general `call_callback` path (non-coroutine callers with variable
latency), not for the bench.

### Multi-worker scaling

- 1e1w → 1e4w: **+253%** (308K → 1.08M). Without the tokio scheduler
  bottleneck, 4 epoll workers fully parallelize the callback chain.
- 1e4w → 1e16w: **+110%** (1.08M → 2.27M). Unlike B2 (where 1e16w
  degraded -15% from worker contention), the CAS fix eliminated the
  silent callback loss that capped B2's high-worker configs. 16 workers
  now scale linearly — each worker processes responses independently
  without stomping on each other's slots.
- 1e16w → 2e16w: **+3%** (2.27M → 2.34M). Two epoll engines provide
  marginal improvement — the bottleneck shifts to socket buffer
  throughput, not epoll contention.
- 1e2w at 512T:8C: **607K** (3x vs B2's 203K). The CAS eliminated the
  contention that capped B2's multi-worker configs at 512T.

### Why 1e16w jumped 4.6x vs B2

B2's 1e16w was 497K — slower than 1e4w (585K), attributed to "ONESHOT
re-arm overhead + worker contention." The real cause: B2's
`call_callback` unconditionally overwrote the slot (no CAS), so under
high worker contention (16 workers sharing one epoll fd), concurrent
`on_response` calls for the same connection could stomp on each other's
slots — `resp_wrong_id` would tick, the response was silently dropped,
and the coroutine hung until the 30s force-exit. This reduced effective
concurrency. The CAS fix (`DONE→PENDING` in `call_callback`,
`PENDING→DONE` in `on_response`) ensures clean slot reuse — no double
invoke, no silent drops. All coroutines are properly resumed, so
effective concurrency matches configured concurrency.

### Scaling ceiling comparison

| Target | Ceiling (ops/s) | Bottleneck | Platform |
| --- | --- | --- | --- |
| RPC echo (slab fallback + reaper, 128B/20s) | ~2.34M | Socket buffer throughput | AMD 5950X |
| RPC echo (Gap2+Gap3, 128B/20s) | ~585K | Silent callback loss under worker contention | AMD 5950X |
| RPC echo (Gap4+Gap1, 128B/20s) | ~276K | Tokio scheduler round-trip + per-op alloc | AMD 5950X |
| RPC echo (in-process, optimized) | ~317K | Caller-thread writev contention + per-op alloc | M5 Pro |
| KV write (coalesced, 3-node) | ~124K | Consensus quorum RPC | AMD 5950X |

The slab fallback ceiling (~2.34M) is ~18.9× the KV write ceiling and
~1.15× the 2M TPS buzz-cpp reference. The CAS fix closed the remaining
gap to the buzz-cpp coroutine loader technique — the inline resume model
was always correct, but the slot management wasn't.

---

## Memory Copy Summary

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

- **Slab fallback + reaper (done, 2026-08-20)** — CAS-based slab claim
  + map fallback + timeout reaper. Peak TPS 4.0x vs B2 (585K → 2.34M),
  zero errors. Fixed silent callback loss under high worker contention.
- **Gap2+Gap3 (done, 2026-08-20)** — callback-based client model +
  slab completion pool. Peak TPS 2.1x (276K → 585K), zero timeout
  errors. Plan: `doc/working/plan-rpc-perf-gap2-3.md`.
- **Multi-engine I/O (R108, measured — does not help for loopback)** —
  the transport supports N independent epoll/kqueue instances
  (`--io-engines N --io-workers-per-engine M`). Multi-engine (2×1)
  does not improve TPS for the in-process loopback because the single
  I/O worker is not the bottleneck. Would help with real network I/O.
- **Caller-thread writev contention** — the `in_send_` CAS serializes
  senders on the same connection. Per-connection send queues with
  lock-free MPSC enqueue + batch writev could reduce contention.
- **RDMA transport (R32)** — bypasses the kernel socket path entirely,
  eliminating the `writev`/`read` copies and the epoll event loop
  overhead. The reference's RDMA path achieves ~2× the TCP path.
- **Flatbuffer reuse** — the `ConnectionPingRequest` is rebuilt per
  op. Pre-building a template and patching the `id` field would save
  ~5µs per op (significant at 1T:1C where per-op cost is 12µs).

---

## History (brief)

- **2026-08-19 (M5 Pro, 64B/5s)**: shared connections + caller-thread
  `in_send_` writev lifted ceiling from ~132K to ~317K. Multi-engine
  (2×1) and multi-worker (EV_ONESHOT) did not help for loopback.
  Zero errors across 10 configs. Per-op latency at 1T:1C: 23µs.
- **2026-08-20 (AMD 5950X, 128B/20s, Gap4+Gap1)**: ONESHOT zero-lock
  re-arm + `folly::ConcurrentHashMap`. Peak ~276K (1e4w 1000t32c).
  2e1w/1e2w at 512T:8C had 1452/1762 timeout errors from
  `connection.cpp` `retry_send` busy-loop under high contention.
- **2026-08-20 (AMD 5950X, 128B/20s, Gap2+Gap3)**: callback-based
  client model + slab completion pool. Peak ~585K (2.1x baseline),
  zero errors. See sections above for full detail.
- **2026-08-20 (AMD 5950X, 128B/20s, slab fallback + reaper)**: CAS-
  based slab claim + map fallback + timeout reaper. folly made
  unconditional. Peak ~2.34M (4.0x B2, 8.5x baseline), zero errors.
  Fixed silent callback loss under high worker contention (B2's 1e16w
  was 497K due to unconditional slot overwrite; CAS fix → 2.27M).
  Low-concurrency configs slightly below B2 from CAS acquire-load
  overhead. slab_fallback=0 across all configs (coroutine model pins
  fixed slots).
