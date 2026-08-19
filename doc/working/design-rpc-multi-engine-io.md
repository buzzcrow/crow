<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Multi-Engine I/O Worker Model (R108)

Implementation design draft for
[`doc/backlog/R108-rpc-multi-engine-io.md`](../backlog/R108-rpc-multi-engine-io.md).
Root design: [`doc/design/rpc/design-crow-rpc.md`](../design/rpc/design-crow-rpc.md)
§2 (per-connection writer) and
[`doc/design/rpc/design-crow-rpc-tcp.md`](../design/rpc/design-crow-rpc-tcp.md)
§1 (Worker Loop). Architecture decisions and rationale are in the
root design; this doc does not repeat them.

What is already landed: the buzz-model caller-thread writev
(`Connection::try_send` with `in_send_` CAS, in
`src/connection.cpp`), the `submit_inline` server-dispatch path
(`src/transport/socket_transport.cpp:408`), the per-worker receive
buffer + send aggregation, the dispatch-callback executor model
(`io_dispatch_threads`, in `app/crow-cli/src/bench/targets/rpc.rs`),
and the `EV_ONESHOT`/`EPOLLONESHOT` shared-engine multi-worker path
(`src/transport/socket_transport.cpp:316`). The single-engine
ceiling is documented in
`doc/working/rpc-echo-flow-analysis.md` (~332K ops/s on M5 Pro).

---

## 1. Connection Engine Back-Pointer

### 1.1 Why

`SocketTransport::submit` (the caller-thread writev path,
`src/transport/socket_transport.cpp:353`) currently hardcodes
`workers_[0]->engine_->arm_write(fd)` on EAGAIN. This is correct
only when there is one engine. With N engines, a connection's fd
is registered on exactly one engine's epoll/kqueue fd; arming
write on a different engine's fd is a silent no-op (the fd is not
in that engine's interest set) and the partial send stalls until
the next read event happens to re-trigger a write. The submit path
needs to know which engine owns the connection's fd.

### 1.2 How

Add a `void *io_engine` field to `Connection`
(`include/crow-rpc/transport.h`), type-erased as `void*` to avoid
a layering dependency on `socket_transport.h` from `transport.h`
(`transport.h` is included by `client.h` and `server.h`; pulling
`SocketEngine` into it would couple every header to the engine
hierarchy).

`Worker::add_connection`
(`src/transport/socket_transport.cpp:78`) sets
`conn->io_engine = engine_` before calling
`engine_->add_connection(fd, conn.get())`. The field is written
once (at add time) and read only on the EAGAIN retry path in
`submit`. No mutex needed — `add_connection` happens-before any
`submit` on that connection (the connection is not visible to
callers until `create_connection` returns, which calls
`add_connection`).

`SocketTransport::submit` changes from:
```cpp
auto &w = workers_[0];
w->engine_->arm_write(fd);
```
to:
```cpp
auto *engine = static_cast<SocketEngine *>(conn->io_engine);
if (engine != nullptr) {
    engine->arm_write(fd);
}
```

Edge cases:
- `io_engine == nullptr` → connection was never added to a worker
  (should not happen in normal flow); defensive null check skips
  the arm. The caller-thread writev already attempted and hit
  EAGAIN; without an engine to retry, the frame stays queued and
  will be sent on the next read-triggered write. Same behavior as
  a connection that closed mid-submit.
- `io_engine` points to a destroyed engine → cannot happen:
  engines are owned by `SocketTransport` and live for the
  transport's lifetime; connections are destroyed before the
  transport in `RpcServer::stop` (transport_->stop() joins workers
  but engines are destroyed in `~SocketTransport`, after all
  connections are released).

---

## 2. SocketTransport Multi-Engine Constructor

### 2.1 Why

The current ctor
(`src/transport/socket_transport.cpp:304`) branches on
`num_workers <= 1` (single owned engine) vs `num_workers > 1` (one
shared engine + ONESHOT). There is no path for N independent
engines. The shared-submit-queue machinery
(`shared_engine_`, `shared_submit`, `shared_pending_submits_`,
`drain_shared_submits`, the `Worker(id, shared_engine, transport)`
ctor, `Worker::pending_submits_`) is dead code — the buzz-model
caller-thread writev replaced the cross-thread notify-queue submit
path, but the old code was never removed. This requirement
replaces the binary branch with a 2D config and removes the dead
code.

### 2.2 How

New `SocketTransport` ctor signature
(`include/crow-rpc/transport/socket_transport.h`):
```cpp
SocketTransport(uint32_t io_engines,
                uint32_t workers_per_engine,
                BufferPool *pool = nullptr);
```

Construction logic (`src/transport/socket_transport.cpp`):
```
a. Validate: io_engines >= 1, workers_per_engine >= 1
   (defensive; the bench CLI validates earlier).
b. For each engine e in [0, io_engines):
     engine = create_engine(); engine->init();
     if workers_per_engine > 1:
         engine->set_oneshot(true);
     for each worker w in [0, workers_per_engine):
         workers_.push_back(make_unique<Worker>(
             e * workers_per_engine + w,
             engine.get(),   // shared within this engine
             engine_owned_by_transport));
     engines_.push_back(std::move(engine));
```

`Worker` retains a single ctor shape: `Worker(id, SocketEngine*
engine, TransportStats* stats)`. The engine pointer is non-owning
(the transport owns all engines in `engines_`). The
`owned_engine_` / `transport_` distinction is removed — every
worker just holds `engine_` (non-owning) + `stats_`. The
`multi_worker_` flag is removed; oneshot-ness is per-engine (the
engine's own `oneshot_` flag, already set via `set_oneshot`).

`get_worker` round-robins across all `io_engines *
workers_per_engine` workers (unchanged logic, just more workers).
`create_connection` assigns the chosen worker's engine to the
connection: `conn->io_engine = worker->engine()`.

Removed fields/methods from `SocketTransport`:
- `shared_engine_` (replaced by `engines_` vector)
- `multi_worker_` (replaced by per-engine `oneshot_`)
- `shared_submit_mu_`, `shared_pending_submits_`
- `shared_submit`, `drain_shared_submits`

Removed from `Worker`:
- `owned_engine_` (transport owns all engines)
- `transport_` (no shared submit queue)
- `pending_submits_`, `submit_mu_` (no cross-thread notify queue)
- `drain_pending_submits` method
- The `Worker(id, SocketEngine*, SocketTransport*)` ctor

### 2.3 Backward Compatibility

`SocketTransport(uint32_t num_workers, BufferPool* pool)` is kept
as a deprecated overload that delegates to
`SocketTransport(1, num_workers, pool)`. This preserves the
existing `RpcServer(pool, num_workers)` ctor and
`crow_rpc_server_create_with_workers` C ABI function — no external
caller breaks. The deprecation is via a doc comment only (no
`[[deprecated]]` attribute, to avoid build warnings on existing
callers during the transition).

Edge cases:
- `io_engines=0` or `workers_per_engine=0` → defensive assert in
  the ctor (`assert(io_engines >= 1 && workers_per_engine >= 1)`).
  The bench CLI rejects this earlier with a config error.
- `io_engines=1, workers_per_engine=1` → one engine, one worker,
  no ONESHOT. Identical to the current single-worker fast path.
- `io_engines=1, workers_per_engine>1` → one engine, M workers,
  ONESHOT. Identical to the current shared-engine multi-worker
  mode. Preserved for comparison.

---

## 3. Worker Run Loop Simplification

### 3.1 Why

`Worker::run_loop`
(`src/transport/socket_transport.cpp:121`) has
`transport_ != nullptr` branches that distinguished shared-engine
multi-worker from single-worker. With the per-engine model, every
worker uses the same logic — the oneshot-ness is an engine
property, not a transport property. The Notify event handler
(`drain_pending_submits`) is dead code (the cross-thread submit
queue is gone; submits are caller-thread writev). Removing these
simplifies the hot path and eliminates dead branches.

### 3.2 How

`Worker::run_loop` changes:
```
a. Notify event: no-op (remove drain_pending_submits call).
   The notify_fd/EVFILT_USER is still registered (engines create
   it in init()) but no longer woken by submit — it remains for
   future use (scheduled executor, shutdown wake). The stop()
   path still calls notify_worker() to wake a blocked wait().
b. Readable event: on_readable_impl → re-arm read if
   engine_->oneshot(). The engine exposes a `bool oneshot() const`
   accessor (new, trivial). In single-worker-per-engine mode
   (oneshot=false, level-triggered), re-arm is a no-op (read stays
   armed). In multi-worker-per-engine mode (oneshot=true), re-arm
   is required.
c. Writable event: on_writable_impl → if queue empty and not
   oneshot, disarm_write; if queue non-empty and oneshot, arm_write.
   Same logic as today, just keyed off engine_->oneshot() instead
   of transport_ != nullptr.
d. Post-event send aggregation: unchanged (pending_write_conns_
   flush).
```

New `SocketEngine::oneshot()` accessor:
```cpp
// In SocketEngine (or a base method if added):
bool oneshot() const { return oneshot_; }
```
Both `KqueueEngine` and `EpollEngine` already have a private
`oneshot_` field — promote to protected or add a public const
accessor. (The field is set once in `set_oneshot` before any
worker starts, so no synchronization needed.)

Edge cases:
- Worker wakes on Notify during shutdown → `running_` is false,
  loop exits. No submit drain needed (no pending submits).
- oneshot() true but read event was not consumed (spurious wake)
  → `arm_read` is idempotent (EV_ADD on an already-armed filter is
  a no-op on kqueue; epoll MOD with same events is a no-op).

---

## 4. FFI + CLI Config Surface

### 4.1 Why

The bench CLI exposes `--io-workers` (single integer) which
conflates engines with workers-per-engine. To profile the 2D
matrix, the CLI needs two separate flags. The C ABI and Rust FFI
need a new function that takes both parameters; the existing
`with_workers` is kept as a deprecated alias for backward compat.

### 4.2 How

**C ABI** (`include/crow-rpc/c_api.h`, `src/c_api.cpp`):
```c
crow_rpc_server_t crow_rpc_server_create_with_engines(
    crow_rpc_pool_t pool,
    uint32_t io_engines,
    uint32_t workers_per_engine);
```
Implementation: `new RpcServer(bp, io_engines, workers_per_engine)`.
`crow_rpc_server_create_with_workers(pool, num_workers)` is kept
unchanged — it now delegates to `RpcServer(bp, 1, num_workers)`.

**RpcServer** (`include/crow-rpc/server/server.h`,
`src/server/server.cpp`):
```cpp
RpcServer(BufferPool *pool,
          uint32_t io_engines,
          uint32_t workers_per_engine);
// Deprecated alias: RpcServer(BufferPool *pool, uint32_t num_workers);
// maps to (1, num_workers).
```
The old `RpcServer(BufferPool *pool, uint32_t num_workers)` ctor
is kept but delegates to the new one with `io_engines=1`.

**Rust FFI** (`lib/crow-rpc/ffi/src/sys.rs`,
`lib/crow-rpc/ffi/src/server.rs`):
```rust
// sys.rs
pub fn crow_rpc_server_create_with_engines(
    pool: crow_rpc_pool_t,
    io_engines: u32,
    workers_per_engine: u32,
) -> crow_rpc_server_t;

// server.rs
pub fn with_engines(
    pool: Option<&BufferPool>,
    io_engines: u32,
    workers_per_engine: u32,
) -> Self;
```
`with_workers` is kept, delegates to `with_engines(pool, 1, num_workers)`.

**Bench CLI** (`app/crow-cli/src/commands/bench.rs`):
- Remove `--io-workers` flag.
- Add `--io-engines` (default 1) and `--io-workers-per-engine`
  (default 1).
- Keep `--io-dispatch-threads` (default 0).

**BenchConfig** (`app/crow-cli/src/bench/runner.rs`):
- Remove `io_workers: u32`.
- Add `io_engines: u32` (default 1) and `io_workers_per_engine: u32`
  (default 1).
- `validate()`: assert `io_engines >= 1`, `io_workers_per_engine >= 1`.

**RPC bench target** (`app/crow-cli/src/bench/targets/rpc.rs`):
- `provision`: `RpcServer::with_engines(Some(&pool),
  cfg.io_engines, cfg.io_workers_per_engine)`.

**Bench report**: the JSON output already includes config fields;
add `io_engines` and `io_workers_per_engine` to the report so runs
are reproducible. (Check the report struct — if it serializes
`BenchConfig` fields, the new fields appear automatically.)

Edge cases:
- `--io-engines 0` → `validate()` returns
  `Error::Config("--io-engines must be >= 1")`.
- `--io-workers-per-engine 0` → `validate()` returns
  `Error::Config("--io-workers-per-engine must be >= 1")`.
- Old scripts using `--io-workers` → clap rejects the unknown flag
  with a helpful error. The regression script is updated in the
  same commit, so no internal caller breaks.

---

## 5. Bench Regression Script

### 5.1 Why

`tools/bench-rpc-regression.sh` uses `--io-workers`. It must
switch to the new flags and add a 2×1 run to track the
multi-engine gain.

### 5.2 How

Update `run_bench` to take `io_engines` and `workers_per_engine`
instead of `io_workers`:
```bash
run_bench() {
    local threads="$1" conn="$2" label="$3" \
          io_engines="${4:-1}" workers_per_engine="${5:-1}"
    pixi run -- cargo run --release -p crow-cli -- bench run \
        --target rpc --workload write --duration-secs "$DURATION" \
        --threads "$threads" --connections "$conn" \
        --key-space "$KEYSPACE" --value-size "$VALUE_SIZE" \
        --io-engines "$io_engines" \
        --io-workers-per-engine "$workers_per_engine" \
        --json 2>&1
}
```

Runs:
- Baseline (1×1): `1t_1c`, `8t_4c`, `64t_4c`, `256t_4c`, `256t_8c`,
  `512t_8c` — regression check against the existing ~332K ceiling.
- Multi-engine (2×1): `256t_4c`, `512t_8c` — the target measurement.
- Shared-engine multi-worker (1×2): `256t_4c`, `512t_8c` —
  preserved for comparison (the old `io_workers=2` rows).

The header reference-results table is updated after the first run
with the new numbers.

---

## 6. Documentation Updates

### 6.1 Echo Flow Analysis

`doc/working/rpc-echo-flow-analysis.md`:
- **Thread Model**: replace "C++ I/O worker thread (1, single
  worker)" with the N-engines × M-workers model.
- **Benchmark Results**: add the multi-engine sweep (2×1, 1×2
  comparison rows).
- **Conclusions**: update the "Single C++ I/O worker is still the
  serialization bottleneck" bullet with the multi-engine finding.
- **Enhancement Ideas**: replace the "Multi-worker I/O" bullet
  (which said ONESHOT doesn't help) with the multi-engine result.
- **Scaling ceiling comparison**: update the RPC echo ceiling row.

### 6.2 TCP Transport Design

`doc/design/rpc/design-crow-rpc-tcp.md`:
- **§1 Worker Loop**: update to describe N engines × M workers,
  connection partitioning, ONESHOT only when M>1.
- **New §6 Multi-Engine Scaling**: the config matrix, per-platform
  tuning rationale (macOS kqueue vs Linux epoll), the
  `io_engine` back-pointer mechanism.

### 6.3 Doc Index

`doc/doc_index.md`: update the `design-crow-rpc-tcp.md` row to
mention multi-engine scaling (§6).

---

## Scope

- `lib/crow-rpc/include/crow-rpc/transport.h` — add `io_engine`
  field to `Connection`.
- `lib/crow-rpc/include/crow-rpc/transport/socket_transport.h` —
  new ctor signature, remove shared-engine fields/methods, add
  `SocketEngine::oneshot()` accessor, simplify `Worker`.
- `lib/crow-rpc/src/transport/socket_transport.cpp` — multi-engine
  ctor, `submit` engine routing, `run_loop` simplification, remove
  dead shared-submit code.
- `lib/crow-rpc/include/crow-rpc/transport/kqueue/kqueue_engine.h`
  — expose `oneshot()` accessor.
- `lib/crow-rpc/include/crow-rpc/transport/epoll/epoll_engine.h`
  — expose `oneshot()` accessor.
- `lib/crow-rpc/include/crow-rpc/server/server.h` — new
  `RpcServer` ctor signature.
- `lib/crow-rpc/src/server/server.cpp` — new ctor, old ctor
  delegates.
- `lib/crow-rpc/include/crow-rpc/c_api.h` — new
  `crow_rpc_server_create_with_engines` declaration.
- `lib/crow-rpc/src/c_api.cpp` — new function impl, old
  `create_with_workers` delegates.
- `lib/crow-rpc/ffi/src/sys.rs` — new FFI declaration.
- `lib/crow-rpc/ffi/src/server.rs` — `with_engines` wrapper,
  `with_workers` delegates.
- `app/crow-cli/src/commands/bench.rs` — replace `--io-workers`
  with `--io-engines` + `--io-workers-per-engine`.
- `app/crow-cli/src/bench/runner.rs` — `BenchConfig` field
  changes + `validate()`.
- `app/crow-cli/src/bench/targets/rpc.rs` — call `with_engines`.
- `tools/bench-rpc-regression.sh` — new flags + 2×1 runs.
- `doc/working/rpc-echo-flow-analysis.md` — benchmark results +
  thread model + conclusions update.
- `doc/design/rpc/design-crow-rpc-tcp.md` — §1 update + new §6.
- `doc/doc_index.md` — update `design-crow-rpc-tcp.md` row.

---

## Complexity

**Medium.** The core refactor (multi-engine ctor, `io_engine`
back-pointer, `submit` routing, `run_loop` simplification) is
mechanical — the buzz-model caller-thread writev is already in
place, so there is no new I/O logic, just wiring. The main
challenges are: (1) removing the dead shared-submit-queue code
without breaking the `Worker` class invariant (careful field
removal), (2) ensuring the `oneshot()` accessor is available on
both engine subclasses without duplicating logic, and (3) the CLI
flag rename (`--io-workers` → two flags) which touches the config
struct, validation, and the bench target in one coordinated
change. No new external dependencies; no protocol changes; no
storage layer interaction.

---

## Test Design

### Unit Tests (UT)

**Engine back-pointer**:
- `UT-1`: Create a `SocketTransport(2, 1, pool)`, add 4
  connections, verify each connection's `io_engine` is non-null
  and matches the engine of the worker it was assigned to (via a
  `test-util` accessor exposing `transport->worker_for_conn(fd)->
  engine()`). Guards: `io_engine` is set at `add_connection` time.
- `UT-2`: `SocketTransport::submit` on a connection owned by
  engine 1, with a mock engine that records `arm_write` calls →
  verify `arm_write` is called on engine 1, not engine 0. Guards:
  EAGAIN retry routes to the owning engine.

**Config validation**:
- `UT-3`: `BenchConfig::validate()` with `io_engines=0` → returns
  `Error::Config("--io-engines must be >= 1")`. Guards: invalid
  config rejected at CLI.
- `UT-4`: `BenchConfig::validate()` with `workers_per_engine=0`
  → returns `Error::Config("--io-workers-per-engine must be
  >= 1")`. Guards: invalid config rejected at CLI.

**Connection partitioning**:
- `UT-5`: `SocketTransport(4, 1, pool)`, add 8 connections → each
  engine owns exactly 2 connections (round-robin). Verify via a
  `test-util` accessor `transport->engine_conn_count(engine_idx)`.
  Guards: connections are partitioned, not duplicated.

**Backward compat**:
- `UT-6`: `SocketTransport(num_workers=4, pool)` (old ctor) →
  creates 1 engine + 4 workers with ONESHOT. Verify via
  `transport->engine_count() == 1` and
  `transport->worker_count() == 4`. Guards: old API maps to
  `(1, num_workers)`.

### End-to-End Tests (E2E)

**Single-engine regression (no behavior change)**:
- `E2E-1`: `io_engines=1, workers_per_engine=1` echo bench
  (1T:1C, 256T:4C, 512T:8C, 5s each) → 0 errors; TPS within ±5%
  of pre-R108 baseline. Proves: the refactor doesn't regress the
  fast path.
- `E2E-2`: All existing `lib/crow-rpc` tests
  (`loopback_test`, `transport_test`, `server_test`,
  `client_pool_test`, `framing_test`, `buffer_test`,
  `load_test`) pass unchanged. Proves: no behavioral break.

**Multi-engine correctness**:
- `E2E-3`: `io_engines=2, workers_per_engine=1` echo bench
  (1T:1C, 256T:4C, 512T:8C, 5s each) → 0 errors. Proves: 2
  independent engines run without cross-engine races.
- `E2E-4`: `io_engines=4, workers_per_engine=2` echo bench
  (256T:4C, 5s) → 0 errors. Proves: 8 workers across 4 engines
  with ONESHOT run correctly.

**Multi-engine performance (measurement, not pass/fail)**:
- `E2E-5`: `io_engines=2, workers_per_engine=1` echo bench at
  256T:4C and 512T:8C → TPS recorded in
  `doc/working/rpc-echo-flow-analysis.md`. Proves: the config
  runs and the number is captured for analysis.

**Dispatch threadpool interaction**:
- `E2E-6`: `io_engines=2, workers_per_engine=1,
  io_dispatch_threads=4` echo bench (256T:4C, 5s) → 0 errors.
  Proves: multi-engine + Rust handler threadpool compose.
- `E2E-7`: `io_engines=2, workers_per_engine=1,
  io_dispatch_threads=0` echo bench (256T:4C, 5s) → 0 errors.
  Proves: multi-engine + inline C++ handler compose.

**Bench report reproducibility**:
- `E2E-8`: Run bench with `--io-engines 2 --io-workers-per-engine
  1` → JSON report includes `io_engines: 2`,
  `io_workers_per_engine: 1`, `io_dispatch_threads: 0`. Proves:
  config is captured for reproduction.

---

## Module Structure

```
lib/crow-rpc/
  include/crow-rpc/
    transport.h                    # +io_engine field on Connection
    c_api.h                        # +crow_rpc_server_create_with_engines
    server/server.h                # +RpcServer(io_engines, wpe, pool) ctor
    transport/
      socket_transport.h           # multi-engine ctor, remove shared fields,
                                    #  +SocketEngine::oneshot(), simplify Worker
      kqueue/kqueue_engine.h       # +oneshot() accessor
      epoll/epoll_engine.h         # +oneshot() accessor
  src/
    c_api.cpp                      # +create_with_engines, old delegates
    server/server.cpp              # +new ctor, old delegates
    transport/socket_transport.cpp # multi-engine ctor, submit routing,
                                    #  run_loop simplification, remove dead code
  ffi/src/
    sys.rs                         # +crow_rpc_server_create_with_engines
    server.rs                      # +with_engines, with_workers delegates

app/crow-cli/src/
  commands/bench.rs                # --io-engines + --io-workers-per-engine
  bench/
    runner.rs                      # BenchConfig: io_engines, io_workers_per_engine
    targets/rpc.rs                 # call with_engines

tools/
  bench-rpc-regression.sh          # new flags + 2×1 runs

doc/
  working/rpc-echo-flow-analysis.md  # results + thread model + conclusions
  design/rpc/design-crow-rpc-tcp.md  # §1 update + new §6
  doc_index.md                       # update rpc-tcp row
```

---

## Config Extensions

`BenchConfig` (`app/crow-cli/src/bench/runner.rs`):
- Removed: `io_workers: u32` (default 1).
- Added: `io_engines: u32` (default 1).
- Added: `io_workers_per_engine: u32` (default 1).
- `validate()`: `if self.io_engines == 0 { return Err("--io-engines must be >= 1"); }`
  and same for `io_workers_per_engine`.

CLI (`app/crow-cli/src/commands/bench.rs`):
- Removed: `--io-workers` flag.
- Added: `--io-engines` (default 1), `--io-workers-per-engine` (default 1).
- `io_dispatch_threads` unchanged (default 0).

---

## Server Wiring

No `main.rs` / `sync.rs` changes — R108 affects only the RPC
library internals and the bench CLI. The `crow-kv-server` binary
does not use `io_engines` yet (it uses the default `RpcServer(pool)`
which maps to `(1, 1)`). R32 (KV consensus → crow-rpc) will wire
the multi-engine config into the server binary when it migrates
the consensus hot path.

Bench target wiring (`app/crow-cli/src/bench/targets/rpc.rs::provision`):
```
a. let server = Arc::new(RpcServer::with_engines(
       Some(&pool), cfg.io_engines, cfg.io_workers_per_engine));
b. server.listen("127.0.0.1", 0);
c. (dispatch threadpool setup unchanged — keyed off
   cfg.io_dispatch_threads)
d. server.start();
e. (connection setup unchanged)
```

---

## Open Questions

All resolved per user review of the backlog doc:

- **`with_workers` alias** — kept (not removed). Maps to
  `(1, num_workers)`. No `[[deprecated]]` attribute (avoid build
  warnings on existing callers).
- **Default bench config** — default is `io_engines=1,
  workers_per_engine=1` (same behavior as the old `io_workers=1`).
  The regression script keeps 1×1 as a baseline row; if the 2×1
  measurement wins, 2×1 becomes the recommended/highlighted config
  in the script header.
- **Per-platform default** — no auto-default. The C++ ctor treats
  0 as invalid (assert); the bench CLI always passes explicit
  values; `BenchConfig` defaults to `io_engines=1,
  workers_per_engine=1`.
