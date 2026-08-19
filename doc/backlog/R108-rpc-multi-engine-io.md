<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R108: rpc — Multi-Engine I/O Worker Model

**Problem**

The CROW RPC echo benchmark plateaus at ~332K ops/s on Apple M5 Pro
(18c) with a single C++ I/O worker thread (`io_workers=1`). The
bottleneck is not CPU — CPU usage stays low — but event-queue
serialization on the single epoll/kqueue fd: every read, write, and
notify event across all connections funnels through one
`kevent`/`epoll_wait` call. Latency climbs from 24µs (1T:1C) to
1,541µs (512T:8C) purely from queueing delay on that one fd.

The existing multi-worker mode (`io_workers>1`) does NOT help: it
shares one epoll/kqueue fd across all workers with
`EV_ONESHOT`/`EPOLLONESHOT` re-arm, and is 13-18% *slower* than
single-worker for the in-process loopback. The re-arm overhead (one
extra `kevent`/`epoll_ctl` syscall per event) plus kernel-level
contention on the shared fd exceeds any parallelism benefit when the
I/O thread is not CPU-bound. See
`doc/working/rpc-echo-flow-analysis.md` "Multi-worker (EV_ONESHOT)
— does not help for loopback" and "Conclusions".

There is no way to run multiple independent epoll/kqueue instances
today. The transport hardcodes a binary choice: one engine owned by
one worker (`num_workers<=1`), or one shared engine with N workers
on ONESHOT (`num_workers>1`). An operator cannot answer the basic
question "does N independent engines beat 1 shared engine on my
platform?" without code changes. macOS (kqueue) and Linux (epoll)
have different scaling characteristics for shared-fd contention vs
independent-fd parallelism, and the only way to find the right
config is to make every axis independently tunable and measure.

A second, orthogonal axis is also under-exposed: where the request
handler runs. Today the handler either runs inline on the C++ I/O
worker thread (the built-in echo handler) or, when
`io_dispatch_threads>0`, is handed off to a Rust thread pool via
the dispatch callback. This dispatch model exists but is not
documented as a first-class tuning knob in the bench CLI help, and
its interaction with the engine/worker model is not measurable
independently.

**Current behavior + impact**: `SocketTransport` ctor
(`src/transport/socket_transport.cpp:304`) branches on
`num_workers<=1` (single owned engine) vs `num_workers>1` (one
shared engine + ONESHOT). There is no path for N independent
engines. The bench CLI exposes `--io-workers` (single integer) and
`--io-dispatch-threads`, but `--io-workers` conflates "number of
engines" with "workers per engine" — they cannot be tuned
separately. Impact: the TPS ceiling is pinned at ~332K with no
knob to break it; platform-specific tuning is impossible; the
dispatch-threadpool interaction with engine count is unmeasurable.

**Design pointers**: TCP transport design
`doc/design/rpc/design-crow-rpc-tcp.md` §1 (Worker Loop — "each
connection is owned by one worker, so no cross-worker locking"),
§4 (EpollEngine), §5 (KqueueEngine). Root RPC design
`doc/design/rpc/design-crow-rpc.md` §2 ("Per-connection writer, no
connection-level lock — each connection is owned by one worker
thread"), §4.4 (Server Side — "Handlers run on the worker thread.
Fast handlers return inline; slow handlers offload to
`RpcServer`'s `offload_pool`"). The echo flow analysis
`doc/working/rpc-echo-flow-analysis.md` "Thread Model" and
"Conclusions" document the single-worker bottleneck and the
ONESHOT regression.

**Use scenarios**:

- **Single-engine baseline (regression check)**: Operator runs the
  echo bench with 1 engine × 1 worker (the current default fast
  path). Expected: TPS matches the existing ~332K baseline; no
  regression from the multi-engine refactor.

- **Multi-engine on loopback (the target)**: Operator runs the echo
  bench with 2 engines × 1 worker each (2 independent kqueue fds,
  no ONESHOT). Connections are partitioned round-robin across the 2
  engines. Expected: TPS rises above the ~332K single-engine
  ceiling because event processing is parallelized across 2
  independent kernel event queues with no re-arm overhead.

- **Multi-engine × multi-worker (real-network shape)**: Operator
  runs with 2 engines × 2 workers per engine (4 total workers; each
  engine's 2 workers share that engine's fd with ONESHOT). Expected:
  useful when the I/O thread becomes CPU-bound on real network I/O;
  the ONESHOT overhead is paid only within each engine, not across
  all workers on one fd. Measurable independently of the
  loopback case.

- **Dispatch threadpool + multi-engine**: Operator runs with 2
  engines × 1 worker + 4 dispatch threads (handler runs on the
  Rust thread pool, not the C++ I/O worker). Expected: the I/O
  worker only reads/parses; handler execution overlaps on the
  thread pool. This isolates the I/O-parallelism gain from the
  handler-parallelism gain — the operator can compare
  inline-handler vs threadpool-handler at each engine count.

- **Platform comparison**: Operator runs the same config matrix
  (1×1, 2×1, 4×1, 1×2, 2×2, 1×4) on macOS (kqueue) and Linux
  (epoll). Expected: the optimal config may differ by platform
  (e.g. macOS kqueue may scale better with independent fds than
  Linux epoll, or vice versa). The bench records the config in
  the report so the comparison is reproducible.

- **Connection partitioning correctness**: 8 connections are
  created with 4 engines. Expected: each engine owns exactly 2
  connections (round-robin); a read event on engine 0's connection
  is processed by engine 0's worker only — no cross-engine
  dispatch, no double-read race.

**Solution**

Replace the binary single-engine-vs-shared-engine model with a
two-dimensional config: `io_engines` (N independent epoll/kqueue
instances) × `io_workers_per_engine` (M workers per engine). Total
workers = N × M. Each engine owns its own fd and its own set of
connections (round-robin partitioned at accept/connect time). When
M=1, the single worker owns the engine with no ONESHOT (the fast
path). When M>1, the M workers share that one engine's fd with
ONESHOT (re-arm only within that engine). The existing
`io_dispatch_threads` knob (Rust handler thread pool) is preserved
and documented as a third independent axis.

**One-line summary**: Split `io_workers` into `io_engines` ×
`io_workers_per_engine` so N independent epoll/kqueue instances can
each run M workers, making engine count, workers-per-engine, and
dispatch-threadpool size independently tunable for per-platform
profiling.

**Numbered work items**:

1. **`Connection::io_engine` back-pointer**
   (`include/crow-rpc/transport.h`) — add a `void *io_engine` field
   to `Connection`, set by `Worker::add_connection` to the owning
   `SocketEngine*`. `SocketTransport::submit` (caller-thread
   writev path) uses this to arm write on the correct engine when
   writev hits EAGAIN. Without this, submit cannot route the
   `arm_write` to the right engine now that connections are
   partitioned across multiple engines. Type-erased as `void*` to
   avoid a layering dependency on `socket_transport.h` from
   `transport.h`.

2. **`SocketTransport` multi-engine ctor**
   (`src/transport/socket_transport.cpp`,
   `include/crow-rpc/transport/socket_transport.h`) — replace the
   current ctor with one that takes `(io_engines, workers_per_engine,
   pool)`. Create `io_engines` independent `SocketEngine` instances.
   For each engine, create `workers_per_engine` `Worker`s. When
   `workers_per_engine>1`, that engine's workers share its fd with
   ONESHOT (`set_oneshot(true)`); when `=1`, the single worker owns
   the engine with no ONESHOT. Remove the dead shared-submit-queue
   machinery (`shared_engine_`, `shared_submit`, `shared_pending_
   submits_`, `drain_shared_submits`, the `Worker(id, shared_engine,
   transport)` ctor, `Worker::pending_submits_`) — the cross-thread
   submit path is now caller-thread writev (the buzz model), not a
   notify queue. `get_worker` round-robins across all N×M workers;
   `create_connection` assigns the connection to the chosen worker's
   engine and sets `conn->io_engine`.

3. **`SocketTransport::submit` engine routing**
   (`src/transport/socket_transport.cpp`) — the caller-thread writev
   path currently hardcodes `workers_[0]->engine_->arm_write(fd)` on
   EAGAIN. Change to `static_cast<SocketEngine*>(conn->io_engine)->
   arm_write(fd)` so the re-arm hits the engine that actually owns
   the connection's fd. This is the correctness fix that makes
   multi-engine safe: arming write on the wrong engine's fd is a
   no-op (the fd isn't registered there) and the partial send would
   stall forever.

4. **`Worker::run_loop` simplification**
   (`src/transport/socket_transport.cpp`) — remove the
   `transport_ != nullptr` branches that distinguished shared-engine
   multi-worker from single-worker. With per-engine ownership, every
   worker uses the same logic: Readable → `on_readable_impl` →
   re-arm read only if `oneshot` (engine-level flag, not
   transport-level); Writable → `on_writable_impl` → re-arm/disarm
   based on `oneshot`. The Notify event becomes a no-op (the
   cross-thread submit queue is gone; submits are caller-thread
   writev). `drain_pending_submits` is removed. The per-engine
   `oneshot` flag is exposed to the worker (e.g. via the engine
   pointer) so the re-arm logic is engine-local, not transport-global.

5. **FFI + CLI config surface**
   (`lib/crow-rpc/ffi/src/server.rs`,
   `lib/crow-rpc/ffi/src/sys.rs`,
   `include/crow-rpc/c_api.h`,
   `src/c_api.cpp`,
   `app/crow-cli/src/commands/bench.rs`,
   `app/crow-cli/src/bench/runner.rs`,
   `app/crow-cli/src/bench/targets/rpc.rs`) — add a new C ABI
   function `crow_rpc_server_create_with_engines(pool, io_engines,
   workers_per_engine)` and a Rust wrapper
   `RpcServer::with_engines(pool, io_engines, workers_per_engine)`.
   Keep `with_workers` as a deprecated alias that maps to
   `(io_engines=1, workers_per_engine=num_workers)` for backward
   compatibility. In the bench CLI, replace `--io-workers` with
   `--io-engines` (default 1) and `--io-workers-per-engine` (default
   1); keep `--io-dispatch-threads` (default 0). `BenchConfig` gains
   `io_engines` and `io_workers_per_engine` fields; `io_workers` is
   removed. The RPC bench target calls `with_engines`. The bench
   report records all three axes so runs are reproducible.

6. **Bench regression script update**
   (`tools/bench-rpc-regression.sh`) — update the sentinel to use
   the new `--io-engines` / `--io-workers-per-engine` flags. Keep
   the 1×1 baseline runs (regression check). Add a 2×1 run (2
   engines, 1 worker each) to track the multi-engine gain. The
   reference results table in the script header is updated with the
   new baseline + 2×1 numbers after the first run.

7. **Echo flow analysis doc update**
   (`doc/working/rpc-echo-flow-analysis.md`) — update "Thread Model"
   to describe the N-engines × M-workers model. Update "Benchmark
   Results" with the multi-engine sweep. Update "Conclusions" and
   "Enhancement Ideas" (the "Multi-worker I/O" bullet is replaced
   by the multi-engine finding). Update "Scaling ceiling
   comparison" with the new ceiling.

8. **TCP transport design doc update**
   (`doc/design/rpc/design-crow-rpc-tcp.md`) — update §1 (Worker
   Loop) to document the multi-engine model: N independent engines,
   M workers per engine, connection partitioning, ONESHOT only when
   M>1. Add a §6 (Multi-Engine Scaling) covering the config matrix
   and the per-platform tuning rationale.

**Flow diagram**:

```
                       SocketTransport(io_engines=N, workers_per_engine=M)
                       │
                       ├── Engine 0 (epoll_fd/kq_fd #0)
                       │     ├── Worker 0  ┐  (M=1: owns engine, no ONESHOT)
                       │     ├── Worker 1  │  (M>1: share engine, ONESHOT)
                       │     └── ...       ┘
                       │     owns connections: conn[0], conn[N], conn[2N], ...
                       │
                       ├── Engine 1 (epoll_fd/kq_fd #1)
                       │     ├── Worker M
                       │     ├── Worker M+1
                       │     └── ...
                       │     owns connections: conn[1], conn[N+1], conn[2N+1], ...
                       │
                       └── ... Engine N-1

  Caller thread (tokio worker):
    RpcClient::call → transport->submit(conn, frame)
      → conn->enqueue_send(frame)
      → conn->try_send(fd)              [caller-thread writev, buzz model]
         → if EAGAIN: conn->io_engine->arm_write(fd)   [route to owner engine]
         → if ok:     done (no wake needed)

  Engine K's worker:
    kevent/epoll_wait → Readable(conn on engine K)
      → on_readable_impl → parser.feed_data → conn->on_frame
        → server dispatch → echo handler → submit_inline (enqueue only)
      → re-arm read only if engine K is oneshot (M>1)
    [post-event flush]
      → for each conn with pending sends: on_writable_impl (batch writev)
```

**Edge cases at a glance**:

- `io_engines=0` or `workers_per_engine=0` → invalid config,
  rejected at `BenchConfig::validate` (and in the C++ ctor as a
  defensive check); returns error / asserts.
- `io_engines=1, workers_per_engine=1` → identical to the current
  single-worker fast path; no ONESHOT, no regression.
- `io_engines=1, workers_per_engine>1` → identical to the current
  shared-engine multi-worker (ONESHOT) mode; preserved for
  comparison, not removed.
- Connection's owning engine is destroyed before the connection
  → cannot happen: engines live for the transport's lifetime;
  connections are destroyed before the transport in `RpcServer::stop`
  order.
- `submit` called on a connection whose `io_engine` is null →
  defensive: treat as "no engine to arm", return true (the
  caller-thread writev already sent the data or hit EAGAIN with no
  retry path — same as a closed connection).
- Partial write on engine K's connection → `arm_write` on engine K
  only; engine K's worker picks up the Writable event; no other
  engine sees it (fd is registered only on engine K).
- Acceptor assigns a connection to a worker on engine K; a
  concurrent `submit` from a tokio thread races with the
  `add_connection` → safe: `enqueue_send` is mutex-protected;
  `io_engine` is set before `add_connection` returns and is never
  mutated after; `try_send` reads `io_engine` only on the EAGAIN
  path (rare), and a stale-but-non-null `io_engine` still points to
  a valid engine (engines outlive connections).

**Dependencies**

- **Depends on**: nothing unlanded. The RPC library
  (`lib/crow-rpc`) is stable; the buzz-model caller-thread writev
  (`Connection::try_send`, `in_send_` CAS) is already in place. The
  dispatch-threadpool model (`io_dispatch_threads`) is already
  implemented in the bench target. This requirement restructures the
  engine/worker wiring and exposes existing axes as config, with no
  new external dependencies.
- **Depended on by**: **R32** (KV consensus hot path → crow-rpc) —
  R32 will consume the multi-engine model for the consensus
  transport; the per-platform tuning results from this requirement
  inform R32's default config. No other item depends on it directly.

**Acceptance**

**Config validation**:
- `--io-engines 0` → bench CLI rejects with a config error
  ("--io-engines must be >= 1"). Integration test (CLI parse).
- `--io-workers-per-engine 0` → bench CLI rejects with a config
  error. Integration test (CLI parse).
- `--io-engines 1 --io-workers-per-engine 1` → runs successfully,
  produces a valid bench report. Integration test.

**Single-engine regression (no behavior change)**:
- `io_engines=1, workers_per_engine=1` echo bench (1T:1C, 256T:4C,
  512T:8C) → TPS within ±5% of the pre-R108 baseline (~40K / ~322K
  / ~332K on M5 Pro). Integration test (bench run + comparison).
- All existing `lib/crow-rpc` tests (`loopback_test`,
  `transport_test`, `server_test`, `client_pool_test`,
  `framing_test`, `buffer_test`, `load_test`) pass unchanged.
  Integration test.

**Multi-engine correctness**:
- `io_engines=2, workers_per_engine=1` with 8 connections → each
  engine owns exactly 4 connections (round-robin); verify via a
  debug counter or test hook that exposes per-engine connection
  counts. Unit test (add a `transport->engine_conn_count(i)` test
  hook under `test-util`).
- `io_engines=2, workers_per_engine=1` echo bench → 0 errors across
  1T:1C, 256T:4C, 512T:8C configs (5s each). Integration test
  (bench run).
- `io_engines=4, workers_per_engine=2` echo bench → 0 errors; all
  8 workers across 4 engines process events without cross-engine
  dispatch races. Integration test (bench run).

**Multi-engine performance (the target)**:
- `io_engines=2, workers_per_engine=1` echo bench at 256T:4C or
  512T:8C → TPS is measured and recorded in
  `doc/working/rpc-echo-flow-analysis.md`. The run documents
  whether 2 independent engines beat the ~332K single-engine
  ceiling on macOS (kqueue). Integration test (bench run + doc
  update). Note: this is a measurement, not a pass/fail threshold —
  the requirement is that the config *runs* and the number is
  *recorded*; whether it beats the ceiling is a finding, not an
  acceptance gate.

**Engine routing correctness**:
- `SocketTransport::submit` on a connection owned by engine K,
  forced EAGAIN (small socket buffer + large burst) → `arm_write`
  is called on engine K (not engine 0); the Writable event fires
  on engine K's worker and drains the send queue. Unit test
  (inject EAGAIN via a test hook that fills the socket buffer, or
  a mock engine that records `arm_write` calls).
- `Connection::io_engine` is set exactly once (at
  `add_connection`) and never mutated → verify via a debug
  assertion or test hook. Unit test.

**Dispatch threadpool interaction**:
- `io_engines=2, workers_per_engine=1, io_dispatch_threads=4`
  echo bench → 0 errors; the handler runs on the Rust thread pool
  (verify via the dispatch callback path, not the inline C++
  handler). Integration test (bench run).
- `io_engines=2, workers_per_engine=1, io_dispatch_threads=0`
  echo bench → 0 errors; the handler runs inline on the C++ I/O
  worker (the built-in echo handler). Integration test (bench run).

**Backward compatibility**:
- `RpcServer::with_workers(pool, N)` still compiles and maps to
  `(io_engines=1, workers_per_engine=N)` → existing callers
  (R32 prep, tests) are not broken. Unit test (compile + behavior).

**Bench report reproducibility**:
- The bench JSON report includes `io_engines`,
  `io_workers_per_engine`, and `io_dispatch_threads` fields so a
  run can be reproduced from the report. Integration test (bench
  run + check JSON fields).

**Test commands**: `pixi run cargo test -p crow-rpc-ffi --test
ffi_loopback`, `pixi run cargo test -p crow-cli --test bench_rpc`
(if exists; otherwise the bench run itself is the integration
check), `pixi run cargo fmt --all -- --check`,
`pixi run cargo clippy --all-targets -- -D warnings`,
`pixi run -- clang-format --dry-run --Werror` (changed `.cpp`/`.h`),
`pixi run -- tree-lint` (changed C++).

**Open Questions**

- **Should `with_workers` be fully removed or kept as a deprecated
  alias?** Keeping it as a deprecated alias mapping to
  `(1, num_workers)` preserves backward compat for any external
  caller and for R32 prep work. Removing it forces all callers to
  the new API. Trade-off: compat vs API cleanliness. Current
  lean: keep as deprecated alias (cheap, no risk). Confirm.

  ai-todo: no, keep it as config

- **Should the default bench config change from `io_workers=1` to
  `io_engines=1, workers_per_engine=1`?** This is the same
  behavior (1×1 = the current fast path), so the default is
  unchanged in effect. The question is whether to also change the
  *recommended* config in the regression script to 2×1 if it
  wins. Trade-off: regression stability (keep 1×1 as the sentinel
  baseline) vs showcasing the win. Current lean: keep 1×1 as the
  baseline sentinel, add 2×1 as an extra row. Confirm after the
  first measurement.

  ai-todo: yes

- **Per-platform default**: should the C++ ctor pick a default
  `io_engines` based on platform (e.g. `std::thread::hardware_
  concurrency()` on macOS) when the caller passes 0, or always
  require an explicit value? An auto-default would help casual
  callers but hides the tuning knob from operators who need to
  profile. Trade-off: convenience vs measurability. Current lean:
  require explicit (the bench CLI always passes explicit values;
  the C++ ctor treats 0 as invalid). Confirm.

  ai-todo: keep 1