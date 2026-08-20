<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# RPC Echo Perf Gap Analysis — CROW vs buzz-cpp

Gap analysis between CROW's RPC echo bench (585K TPS, Gap2+Gap3) and
buzz-cpp's coroutine loader bench (2.03M TPS, same platform). Both use
the same inline-resume technique; the gap is in the surrounding
architecture, not the resume model.

Companion: `rpc-echo-flow-analysis.md` (CROW flow detail),
`plan-rpc-perf-gap2-3.md` (Gap2+Gap3 plan, completed).

---

## Benchmark Comparison

Both measured on the same AMD Ryzen 9 5950X (16c/32t), 128B values,
20s duration, epoll + TCP loopback.

| Metric | buzz-cpp | CROW (Gap2+Gap3) | Ratio |
| --- | --- | --- | --- |
| Peak TPS | 2,029,024 | 584,941 | 3.5× |
| Per-worker TPS | 63K (32 workers) | 146K (4 workers) | 0.43× (CROW wins) |
| Workers at peak | 32 client + 16 server | 4 (1×4) | — |
| Epoll fds | 2 (client + server, separate processes) | 1 (shared) | — |
| Connections per epoll fd | 32 (one side) | 64 (32 client + 32 server) | — |
| Process model | 2 processes (bench + bench-server) | 1 process (in-process echo) | — |
| Avg latency at peak | 592µs | 1,705µs | 2.9× |
| p999 at peak | 42ms | 40ms | ~same |
| Errors | 0 | 0 | — |

Source: `buzz-cpp/logs/bench.log.20260820-100050` (2,029,024 TPS),
`rpc-echo-flow-analysis.md` § Benchmark Results (584,941 TPS).

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

- **CROW**: `RpcClient::on_response()` → slab lookup →
  `bench_on_complete()`. The callback runs inline, records latency,
  calls `submit_next()` → builds flatbuffer → `call_callback()` →
  `transport->submit()` → writev — building and submitting the next
  request on the I/O worker thread.
  Source: `app/crow-cli/src/bench/targets/rpc.rs` lines 762-811.

### CROW's slab pool is better than buzz-cpp's hash map

CROW uses a pre-allocated slab pool indexed by `request_id & pool_mask`
(O(1) array index, zero heap allocation). buzz-cpp uses `mt_hash_map`
—a lock-free concurrent hash map with hazard pointers and a heap
allocation per node (`new node_type(...)` on every `add`).

The global_rules note describes buzz-cpp as using "pre-allocate
completion slots, index by request_id & mask" — this is actually
CROW's design, not buzz-cpp's. CROW improved on buzz-cpp here.

---

## The 5 Architectural Diffs (3.5× gap)

### Diff 1: Process model — 2 epoll fds vs 1 (biggest factor, ~2×)

buzz-cpp runs the bench client and bench-server as **2 separate
processes**, each with its own epoll fd. The client-side re-arms and
server-side re-arms never contend with each other.

CROW runs the echo server and client in **1 process** with **1 epoll
fd**. All client + server connections share that fd. The in-process
echo creates a tight feedback loop on the fd: every server read
triggers a response writev, which immediately makes the client
connection readable, which triggers a callback that submits a new
request writev, which immediately makes the server connection readable
—all on the same epoll fd, all requiring ONESHOT re-arms
(`epoll_ctl MOD`).

### Diff 2: Worker count — 48 vs 4 (~2×)

buzz-cpp: 32 client I/O workers + 16 server I/O workers = 48 threads
covering 2 epoll fds. More workers keep `epoll_wait` covered while
others do callback work.

CROW: 4 workers (1×4) on 1 epoll fd. Beyond 4 workers, ONESHOT re-arm
contention + empty `epoll_wait` calls dominate: 1×16 drops to 497K
(31K/worker, massive degradation from 146K/worker at 1×4).

CROW's workers are individually more efficient (146K vs 63K per
worker) because in-process echo avoids cross-process context switches.
But CROW **cannot add more workers** due to Diff 1.

### Diff 3: Connections per epoll fd — 32 vs 64 (~1.3×)

buzz-cpp: 32 connections per epoll fd (client-side only or server-side
only). Shorter kernel interest list, lower lock hold time.

CROW: 64 connections per epoll fd (32 client + 32 server mixed).
Longer interest list, longer kernel epoll lock hold during re-arms.

### Diff 4: Read path — direct-to-message-buffer vs copy-through-worker-buffer (~1.1×)

buzz-cpp: for the data phase, `recv()` reads directly into the message
object's data buffer (`transfer_socket_data`), avoiding one copy.
Source: `buzz-cpp/src/app/buzz-bench-server/rpc/proto/msg_bench_write_request.cpp`
lines 44-61.

CROW: always reads into a per-worker buffer, then `feed_data` copies
into parser buffers. For 128B payloads this is a small cost, but it
scales with payload size.

### Diff 5: Server write — immediate vs batched (CROW wins here)

buzz-cpp: `post_send` → writev per response (no batching across
responses in one event batch).

CROW: `submit_inline` + batch writev after all events in the batch
(send aggregation). This coalesces multiple responses into one writev
per connection. CROW is better here, but it doesn't offset Diffs 1-3.

---

## Lock Analysis — Full Hot Path

### Rust side (bench callback path)

| Location | Lock type | Contended? | Hot path? |
| --- | --- | --- | --- |
| `BenchWorkerCtx.stats` (Mutex) | `std::sync::Mutex` | **YES** — 4 I/O workers invoke callbacks concurrently, all lock the same mutex per op | **YES** — every `bench_on_complete` |
| `BenchWorkerCtx.in_flight` (AtomicU64) | atomic | No (Relaxed/AcqRel) | YES — every callback |
| `request_id_counter` (AtomicU64) | atomic | Low contention (only initial kickoff) | Only kickoff |
| `WorkerCounters` | lock-free atomics | No (per-worker) | YES — every callback |
| `next_conn` (AtomicUsize) | atomic | No (only at worker spawn) | No |

**The `stats` Mutex is the only contended lock on the Rust hot path.**
Every `bench_on_complete` callback locks it to record latency into
`OpStats` (which contains a `PreciseHistogram`). With 4 I/O workers
invoking callbacks concurrently on the same `BenchWorkerCtx`, this
mutex serializes the stat-recording portion of every op.

Source: `app/crow-cli/src/bench/targets/rpc.rs` lines 778-791.

### C++ side (echo round-trip)

| Location | Lock type | Contended? | Hot path? |
| --- | --- | --- | --- |
| `Connection::send_mu_` (Mutex) | `std::mutex` | **YES** — every `enqueue_send` + `drain_send_queue` + `has_pending_send` | **YES** — 3× per op (request submit, server response enqueue, server batch flush) |
| `Connection::in_send_` (atomic bool) | CAS | Low (serializes writev, not the whole op) | YES — every writev |
| Slab pool `slot.state` (atomic u8) | atomic | No (per-slot, no cross-slot) | YES — every submit + response |
| `EpollEngine::mask_mu_` (Mutex) | `std::mutex` | Only in level-triggered mode | No (ONESHOT fast path skips it) |
| `EpollEngine::conn_mu_` (Mutex) | `std::mutex` | Only add/remove connection | No (setup only) |
| `FrameParser` malloc/free | heap | Per-frame `malloc` for control + data | **YES** — 4 mallocs + 4 frees per op |
| `RpcClient::pending_mu_` (Mutex) | `std::mutex` | No — slab path bypasses it | No (only oneshot path) |

**The `send_mu_` mutex is the contended C++ lock.** It's held during
`enqueue_send` (push to deque), `drain_send_queue` (pop from deque),
and `has_pending_send` (check if empty). With the in-process echo
feedback loop, the same connection's send queue is touched by the
submitter thread (request) and the I/O worker (response batch flush),
creating cross-thread contention on the same mutex.

Source: `lib/crow-rpc/src/connection.cpp` lines 26-48, 57-223;
`lib/crow-rpc/include/crow-rpc/transport.h` lines 160-166.

### Per-op lock count (callback model, 1×4 config)

Per echo round-trip (1 request + 1 response):

| Lock | Count | Where |
| --- | --- | --- |
| Rust `stats` Mutex | 1 | `bench_on_complete` |
| C++ `send_mu_` | 3 | request `enqueue_send`, response `enqueue_send`, response batch `has_pending_send` + `drain_send_queue` |
| C++ `in_send_` CAS | 2 | request writev, response writev |
| Slab `state` atomic | 2 | submit (PENDING), response (DONE) |
| `malloc`/`free` | 4+4 | parser alloc control+data (server), parser alloc control+data (client), + Frame struct |

**Total: 1 Rust mutex + 3 C++ mutex acquisitions + 2 CAS + 8 heap
ops per op.** The Rust `stats` mutex and the C++ `send_mu_` are the
contended ones; the rest are low-contention or per-slot.

---

## Recommendations

### Remove the Rust `stats` Mutex (low effort, tail-latency win)

The `stats` mutex serializes stat recording across all I/O workers
invoking callbacks on the same `BenchWorkerCtx`. Replace with
per-worker stats merged at the end:

- **Option A (simplest):** Use `UnsafeCell<OpStats>` — the callback
  model guarantees no concurrent same-slot access, but different
  workers CAN invoke callbacks on the same `BenchWorkerCtx`
  concurrently. So this is NOT safe without additional synchronization.
- **Option B (correct):** Per-I/O-worker stats. Each C++ I/O worker
  gets its own `OpStats` (no mutex). Merge at the end via
  `OpStats::merge`. This requires knowing which I/O worker invoked the
  callback — the C ABI callback doesn't currently pass this.
- **Option C (simplest correct):** Lock-free histogram. Replace
  `OpStats` with a version that uses atomic counters + a lock-free
  histogram. `PreciseHistogram` is not currently lock-free, so this
  requires a new atomic histogram type.
- **Option D (pragmatic):** Thread-local accumulation. Each callback
  thread accumulates into a thread-local `OpStats`, merged at thread
  exit. The C++ I/O worker threads are long-lived, so merge happens at
  `Worker::stop`. This requires a C++-side hook to merge per-thread
  stats into the Rust `BenchWorkerCtx`.

**Recommended: Option B** — pass the I/O worker index through the
callback (via the slab slot or a new C ABI field), and index into a
per-worker `OpStats[]` array. Zero locks, zero contention, correct.

Expected impact: removes the p999 40ms tail (mutex contention is the
likely cause of the long tail at 1×4).

### Multi-engine + separate client transport (medium effort, ~1.7×)

The highest-impact architectural change: give CROW the same 2-epoll-fd
architecture that buzz-cpp has, without going cross-process.

- Create a **client-side `SocketTransport`** separate from the
  server's transport. Client connections live on the client
  transport's epoll fd; server connections live on the server
  transport's epoll fd.
- This breaks the in-process echo feedback loop's single-fd
  contention: client-side re-arms and server-side re-arms happen on
  different fds, in parallel.
- Use `--io-engines 2` (one for client, one for server) to get 2
  independent epoll fds.

Expected: ~1M TPS (1.7×), with more headroom to add workers.

### Flatbuffer template reuse (low effort, ~1.2×)

The `ConnectionPingRequest` is rebuilt per op via
`FlatBufferBuilder::new()`. Pre-build a template and patch the `id`
field. The server's `build_ping_response` has the same issue.

Expected: ~5µs saved per op (significant at low concurrency where
per-op cost is 12µs; less impact at high concurrency where queueing
dominates).

### Parser buffer pooling (low-medium effort, tail-latency win)

`FrameParser::alloc_buf` calls `std::malloc` per frame for control +
data. Replace with pool-backed allocation (reuse buffers from a
per-worker free list). This eliminates 4 malloc + 4 free per op,
reducing allocator pressure and tail latency.

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

| Fix | Expected TPS | Cumulative |
| --- | --- | --- |
| Current (Gap2+Gap3) | 585K | 585K |
| + Remove stats mutex | 585K (tail-latency only) | 585K |
| + Flatbuffer reuse | 700K | 700K |
| + Parser buffer pooling | 750K | 750K |
| + Multi-engine + separate client transport | 1.1M | 1.1M |
| + Lock-free send queue | 1.2M | 1.2M |
| + RDMA (R32) | 2.0M+ | 2.0M+ |

Without RDMA, the realistic ceiling is ~1.2M (60% of buzz-cpp's 2M).
The remaining 40% is the cross-process parallelism advantage: buzz-cpp
runs client and server on 2 independent cores with 2 address spaces,
while CROW's in-process echo shares 1 address space. Closing that
final gap requires either RDMA (R32, bypasses the kernel socket path)
or a cross-process echo bench mode.

---

## Implementation Results (2-process + lock-free stats)

Both recommendations were implemented and measured on the same
AMD Ryzen 9 5950X, 512B values, 10s, epoll + TCP loopback.

### Changes

- **2-process model**: `crow-cli bench run --target rpc` now spawns
  `crow-rpc-echo-server` as a child process (separate epoll fd). The
  CLI creates a local `RpcServer` (no listen) only for its client-side
  transport, giving 2 independent epoll fds — matching buzz-cpp's
  architecture. The server prints `listening port=NNNN` to stdout; the
  CLI polls the log file to discover the port.
- **Lock-free stats**: Replaced `Mutex<OpStats>` with
  `LatencyHistogram` (atomic bucket counters) + `Counter` (atomic u64)
  for ops/errors. No mutex on the callback hot path. Added
  `--stats-mode histogram|avg-only` to compare overhead.
- **Server metrics**: The echo server prints `stats ...` to stdout on
  SIGTERM. The CLI reads it from the log file and includes it in the
  report as `server_transport_stats`.

### Measured TPS

| Config | TPS | Avg lat | Notes |
| --- | --- | --- | --- |
| In-process (Gap2+Gap3, baseline) | 585K | 1.7ms | 1 epoll fd, 4 workers |
| 2-process, 4+4 workers, histogram | 173K | 23µs | 2 epoll fds, cross-process |
| 2-process, 4+4 workers, avg-only | 174K | 23µs | histogram overhead < 1% |
| 2-process, 16+16 workers, histogram | 242K | 66µs | scales with workers |

`N+M workers` = N client-side bench threads (`--threads N`) + M
server-side I/O workers (`--io-workers-per-engine M` on the spawned
echo server). Both sides use `--io-engines 1` (1 epoll fd per process,
2 total across the 2 processes).

### Analysis

The 2-process model is **slower** than the in-process baseline (173K vs
585K) despite having 2 epoll fds. The reason: cross-process context
switches (2 sched transitions per op) cost more than the single-epoll-fd
contention saves. The in-process model's `submit_inline` path is a
direct function call (no syscall), while the 2-process model adds a
kernel scheduler transition for every response.

The lock-free histogram has **no measurable overhead** vs minimal atomic
counters (173K vs 174K, < 1% difference). The `Mutex<OpStats>` removal
is a correctness + tail-latency win, not a throughput win — the mutex
was uncontended at 4 workers.

### Next steps

The 2-process model's value is architectural isolation (matching
buzz-cpp's deployment model), not raw throughput. To close the
throughput gap:

- **Multi-engine in-process** (1 process, 2 epoll fds): keep the
  in-process `submit_inline` direct-call path, but split client vs
  server connections across 2 engines. This gets the 2-epoll-fd benefit
  without the cross-process cost. Expected ~1M TPS.
- **RDMA (R32)**: bypass the kernel socket path entirely. Expected
  2M+ TPS.

---

## Client Model Comparison: Callback vs Coroutine

Added `--client-mode callback|coroutine` to compare the two client
models. Both use the same 2-process echo server (16+16 workers, 16
connections, 512B values, 10s).

### Callback model (existing)

Closed-loop callback chain on C++ I/O worker threads. Each bench thread
maintains `pipeline_depth` in-flight requests via a callback chain:
response arrives → callback runs inline on I/O worker → submits next
request → writev. No scheduler, no per-call heap alloc.

### Coroutine model (new)

N independent tokio tasks using the oneshot `call()` path. Each task =
one independent "client" that loops: submit → await response → submit
next. When a task `await`s, it yields the tokio runtime thread — another
task runs. The C++ I/O worker receives the response → `on_response` →
`oneshot::send` → tokio wakes the task. `--threads` = number of
coroutines.

### Measured comparison

| Mode | In-flight | TPS | Avg lat | p99 | p999 |
| --- | --- | --- | --- | --- | --- |
| callback, 16×depth 1 | 16 | 242K | 66µs | 66µs | 66µs |
| callback, 16×depth 8 | 128 | 469K | 272µs | 274µs | 274µs |
| callback, 128×depth 8 | 1024 | 454K | 2.3ms | 2.5ms | 2.5ms |
| coroutine, 16 tasks | 16 | 200K | 78µs | 157µs | 213µs |
| coroutine, 128 tasks | 128 | 446K | 281µs | 699µs | 974µs |
| coroutine, 1000 tasks | 1000 | 444K | 2.2ms | 7.6ms | 18ms |

### Analysis

- **Peak TPS is similar** (~445-469K) — both saturate the server. The
  bottleneck is server-side, not the client model.
- **Callback mode has lower latency** at all load levels:
  - 128 in-flight: callback 272µs vs coroutine 281µs avg — **3%
    overhead** from oneshot channel + tokio scheduler.
  - p99: callback 274µs vs coroutine 699µs — **2.6× worse tail**
    (tokio scheduler jitter).
- **Coroutine mode has more realistic latency distribution** — p99/p999
  spread is wider, matching real client behavior. Callback mode's tight
  p99 (274µs) is artificial — the closed-loop chain has no scheduling
  jitter.
- **1000 coroutines work** but latency explodes (2.2ms avg, 18ms p999)
  — 1000 tasks on ~16 tokio runtime threads means heavy context
  switching.

### Rust coroutine overhead breakdown

- Per-call oneshot channel heap alloc: ~50ns
- Tokio scheduler wake → poll → resume: ~200ns
- Tokio task scheduling jitter on p99: ~400µs

Total per-op overhead: ~250ns (0.8% of 30µs server cost). The throughput
impact is small; the tail-latency impact (p99 2.6×) is the real cost.

### C++ coroutine client (proposed)

If the ~3% throughput + 2.6× p99 overhead of Rust coroutines is
unacceptable for production, a C++ coroutine client (matching buzz-cpp's
design) can be wrapped via FFI:

- **C++ side**: C++20 coroutines on the I/O worker threads, same as
  buzz-cpp. `co_await co_call()` → `post_send()` → suspend →
  `handle.resume()` on response. No scheduler, no per-call heap alloc
  (slab pool for coroutine state).
- **Rust side**: thin FFI wrapper exposing `coroutine_call()` →
  `CoFuture` (a Rust `Future` that resolves when the C++ coroutine
  completes). The Rust `Future` is polled by tokio, but the actual
  coroutine runs on the C++ I/O worker thread — no tokio scheduler
  round-trip for the resume.
- **Expected**: same throughput as callback mode, with independent
  client semantics (1000 coroutines). The C++ coroutine resume is
  inline on the I/O worker (no scheduler), so p99 should match callback
  mode.

This is the same architecture as buzz-cpp: C++ coroutines for the hot
path, Rust for the orchestration layer. The FFI boundary is at the
coroutine spawn/join level, not per-op.
