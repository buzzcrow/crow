<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Multi-Engine I/O Worker Model Plan

Design: [`doc/working/design-rpc-multi-engine-io.md`](design-rpc-multi-engine-io.md).
Backlog: [`doc/backlog/R108-rpc-multi-engine-io.md`](../backlog/R108-rpc-multi-engine-io.md).
Goal: split `io_workers` into `io_engines` × `io_workers_per_engine`,
remove dead shared-engine code, and make every tuning axis
independent for per-platform profiling.

## C++ Engine + Transport Core

- [x] **Add `Connection::io_engine` field**: add `void *io_engine`
  to `Connection` in `transport.h` (after `transport_handle`).
  Files: `lib/crow-rpc/include/crow-rpc/transport.h`.
- [x] **Expose `SocketEngine::oneshot()` accessor**: add a public
  `bool oneshot() const` to `SocketEngine` (or promote `oneshot_`
  to protected + add accessor). Both `KqueueEngine` and
  `EpollEngine` already have the private `oneshot_` field.
  Files: `lib/crow-rpc/include/crow-rpc/transport/socket_transport.h`,
  `lib/crow-rpc/include/crow-rpc/transport/kqueue/kqueue_engine.h`,
  `lib/crow-rpc/include/crow-rpc/transport/epoll/epoll_engine.h`.
- [x] **Simplify `Worker` class**: remove `owned_engine_`,
  `transport_`, `pending_submits_`, `submit_mu_`,
  `drain_pending_submits`, and the
  `Worker(id, SocketEngine*, SocketTransport*)` ctor. Keep one
  ctor: `Worker(id, SocketEngine* engine, TransportStats* stats)`.
  `engine_` is non-owning (transport owns all engines).
  Files: `lib/crow-rpc/include/crow-rpc/transport/socket_transport.h`,
  `lib/crow-rpc/src/transport/socket_transport.cpp`.
- [x] **Simplify `Worker::run_loop`**: remove `transport_ != nullptr`
  branches; key re-arm logic off `engine_->oneshot()`. Notify event
  becomes no-op (remove `drain_pending_submits` call). Readable:
  re-arm read only if `engine_->oneshot()`. Writable: re-arm/disarm
  based on `engine_->oneshot()`.
  Files: `lib/crow-rpc/src/transport/socket_transport.cpp`.
- [x] **Rewrite `SocketTransport` ctor for multi-engine**: new
  signature `(io_engines, workers_per_engine, pool)`. Create N
  engines, M workers per engine. ONESHOT when M>1. Keep old
  `(num_workers, pool)` ctor as deprecated alias → `(1, num_workers)`.
  Remove `shared_engine_`, `multi_worker_`, `shared_submit_mu_`,
  `shared_pending_submits_`, `shared_submit`, `drain_shared_submits`.
  Add `engines_` vector (transport owns all engines).
  Files: `lib/crow-rpc/include/crow-rpc/transport/socket_transport.h`,
  `lib/crow-rpc/src/transport/socket_transport.cpp`.
- [x] **Fix `SocketTransport::submit` engine routing**: change
  `workers_[0]->engine_->arm_write(fd)` to
  `static_cast<SocketEngine*>(conn->io_engine)->arm_write(fd)`
  with null check.
  Files: `lib/crow-rpc/src/transport/socket_transport.cpp`.
- [x] **Set `io_engine` in `Worker::add_connection`**: set
  `conn->io_engine = engine_` before `engine_->add_connection`.
  Files: `lib/crow-rpc/src/transport/socket_transport.cpp`.

## C++ Server + C ABI

- [x] **New `RpcServer` ctor**: add
  `RpcServer(pool, io_engines, workers_per_engine)`. Old
  `RpcServer(pool, num_workers)` delegates to `(pool, 1, num_workers)`.
  Files: `lib/crow-rpc/include/crow-rpc/server/server.h`,
  `lib/crow-rpc/src/server/server.cpp`.
- [x] **New C ABI function**: add
  `crow_rpc_server_create_with_engines(pool, io_engines,
  workers_per_engine)`. Old `crow_rpc_server_create_with_workers`
  delegates to `(pool, 1, num_workers)`.
  Files: `lib/crow-rpc/include/crow-rpc/c_api.h`,
  `lib/crow-rpc/src/c_api.cpp`.

## Rust FFI + Bench CLI

- [x] **FFI sys binding**: add
  `crow_rpc_server_create_with_engines` to `sys.rs`.
  Files: `lib/crow-rpc/ffi/src/sys.rs`.
- [x] **FFI safe wrapper**: add `RpcServer::with_engines(pool,
  io_engines, workers_per_engine)`. `with_workers` delegates to
  `with_engines(pool, 1, num_workers)`.
  Files: `lib/crow-rpc/ffi/src/server.rs`.
- [x] **BenchConfig fields**: remove `io_workers`, add `io_engines`
  (default 1) + `io_workers_per_engine` (default 1). Update
  `validate()`.
  Files: `app/crow-cli/src/bench/runner.rs`.
- [x] **Bench CLI flags**: remove `--io-workers`, add `--io-engines`
  + `--io-workers-per-engine`. Update the arg-parsing that maps to
  `BenchConfig`.
  Files: `app/crow-cli/src/commands/bench.rs`.
- [x] **RPC bench target**: call `RpcServer::with_engines` in
  `provision`.
  Files: `app/crow-cli/src/bench/targets/rpc.rs`.

## Bench Script + Docs

- [x] **Update regression script**: switch to `--io-engines` +
  `--io-workers-per-engine`, add 2×1 runs.
  Files: `tools/bench-rpc-regression.sh`.
- [x] **Run bench + update echo flow analysis**: run the regression
  script, record results in `rpc-echo-flow-analysis.md` (Thread
  Model, Benchmark Results, Conclusions, Scaling ceiling).
  Files: `doc/working/rpc-echo-flow-analysis.md`.
- [x] **Update TCP transport design doc**: §1 worker loop update +
  new §6 Multi-Engine Scaling.
  Files: `doc/design/rpc/design-crow-rpc-tcp.md`.
- [x] **Update doc index**: update the `design-crow-rpc-tcp.md` row.
  Files: `doc/doc_index.md`.

## Lint + Tests

- [x] **Quality gate**: `cargo fmt --check`, `cargo clippy -- -D
  warnings`, `clang-format --dry-run --Werror` (changed .cpp/.h),
  `tree-lint` (changed C++). Fix up to 3 times.
- [x] **Affected tests**: `pixi run test-rpc-ct`, `pixi run
  test-rpc-ffi`. Both must pass.
- [x] **Bench smoke**: run a quick 2×1 bench (256T:4C, 5s) to
  verify 0 errors + record TPS.

## File List

- `lib/crow-rpc/include/crow-rpc/transport.h` — +io_engine field
- `lib/crow-rpc/include/crow-rpc/transport/socket_transport.h` — multi-engine ctor, remove shared fields, simplify Worker, +oneshot()
- `lib/crow-rpc/include/crow-rpc/transport/kqueue/kqueue_engine.h` — +oneshot() accessor
- `lib/crow-rpc/include/crow-rpc/transport/epoll/epoll_engine.h` — +oneshot() accessor
- `lib/crow-rpc/include/crow-rpc/server/server.h` — +new RpcServer ctor
- `lib/crow-rpc/include/crow-rpc/c_api.h` — +create_with_engines
- `lib/crow-rpc/src/transport/socket_transport.cpp` — multi-engine ctor, submit routing, run_loop, remove dead code
- `lib/crow-rpc/src/server/server.cpp` — +new ctor, old delegates
- `lib/crow-rpc/src/c_api.cpp` — +create_with_engines, old delegates
- `lib/crow-rpc/ffi/src/sys.rs` — +FFI binding
- `lib/crow-rpc/ffi/src/server.rs` — +with_engines, with_workers delegates
- `app/crow-cli/src/commands/bench.rs` — flag rename
- `app/crow-cli/src/bench/runner.rs` — BenchConfig fields + validate
- `app/crow-cli/src/bench/targets/rpc.rs` — call with_engines
- `tools/bench-rpc-regression.sh` — new flags + 2×1 runs
- `doc/working/rpc-echo-flow-analysis.md` — results + analysis
- `doc/design/rpc/design-crow-rpc-tcp.md` — §1 + §6
- `doc/doc_index.md` — rpc-tcp row update

## Test Checklist

**Unit**:
- [ ] UT-1: `io_engine` set at `add_connection` (test-util accessor)
- [ ] UT-2: `submit` routes `arm_write` to owning engine (mock)
- [ ] UT-3: `io_engines=0` rejected by `validate()`
- [ ] UT-4: `workers_per_engine=0` rejected by `validate()`
- [ ] UT-5: 4 engines × 8 conns → each engine owns 2 (round-robin)
- [ ] UT-6: old `SocketTransport(4, pool)` → 1 engine, 4 workers, ONESHOT

**Integration (bench runs)**:
- [ ] E2E-1: 1×1 bench (1T:1C, 256T:4C, 512T:8C) → 0 errors, ±5% baseline
- [ ] E2E-2: all existing rpc tests pass (`test-rpc-ct`, `test-rpc-ffi`)
- [ ] E2E-3: 2×1 bench (1T:1C, 256T:4C, 512T:8C) → 0 errors
- [ ] E2E-4: 4×2 bench (256T:4C) → 0 errors
- [ ] E2E-5: 2×1 bench TPS recorded in echo flow analysis
- [ ] E2E-6: 2×1 + 4 dispatch threads → 0 errors
- [ ] E2E-7: 2×1 + 0 dispatch threads → 0 errors
- [ ] E2E-8: bench JSON includes io_engines + io_workers_per_engine
