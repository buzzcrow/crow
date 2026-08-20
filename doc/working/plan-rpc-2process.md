<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# RPC 2-Process Bench + Lock-Free Stats Plan

Goal: split the RPC echo bench into 2 processes (standalone echo
server binary + CLI client) following the same pattern as the KV bench,
and replace the `Mutex<OpStats>` in the callback hot path with the
existing lock-free `LatencyHistogram` + `Counter`.

Context: `doc/working/gap.md` identified the single-epoll-fd
in-process echo as the biggest architectural diff vs buzz-cpp (3.5×
gap). The `Mutex<OpStats>` in `bench_on_complete` is the only
contended Rust lock on the hot path. The KV bench already uses the
2-process pattern (`deploy_local` spawns `crow-kv-server` as a child
process, collects metrics from log files, stops via
`stop_pid_with_timeout`). The RPC bench should follow the same
pattern.

## Echo server example binary

- [ ] **Add echo server example**: create `examples/echo_server.cpp`
  in crow-rpc — a standalone binary that creates an `RpcServer`,
  registers the built-in echo handler, listens on a configurable
  port (CLI arg), runs until SIGTERM/SIGINT, and writes transport
  stats (read_calls, writev_calls, dispatch latency) to a log file
  on shutdown. This is an example of how to use the crow-rpc library
  to build a server — not a bench-specific tool.
  Files: `lib/crow-rpc/examples/echo_server.cpp`.

- [ ] **Add CMake target**: add `add_executable(crow-rpc-echo-server
  examples/echo_server.cpp)` to the crow-rpc CMakeLists.txt, linked
  against `crow-rpc`.
  Files: `lib/crow-rpc/CMakeLists.txt`.

## CLI: spawn server + 2-process bench (follow KV pattern)

- [ ] **Spawn echo server as child process**: in `RpcTarget::provision`,
  spawn the `crow-rpc-echo-server` binary as a child process via
  `std::process::Command` (same pattern as `deploy_local` in
  `crow-console-shared/src/lifecycle.rs`). Pass the port + log file
  path as args. Redirect stdout/stderr to a log file. Wait for
  readiness (poll connect). Store the pid + log path for cleanup +
  metrics collection.
  Files: `app/crow-cli/src/bench/targets/rpc.rs`.

- [ ] **Connect to external server**: change `RpcTarget::provision`
  to connect to the spawned server's port instead of creating an
  in-process server. The client-side transport stays in-process
  (the CLI owns the client connections + I/O workers). This gives
  2 independent epoll fds: one in the server process, one in the
  CLI client process — matching buzz-cpp's architecture.
  Files: `app/crow-cli/src/bench/targets/rpc.rs`.

- [ ] **Stop server + read metrics from log file**: in
  `RpcTarget::cleanup`, stop the server via
  `stop_pid_with_timeout` (from `crow-console-shared::lifecycle`).
  In `RpcTarget::collect_artifacts` (or equivalent), read the
  server's transport stats from the log file and include in the
  bench report.
  Files: `app/crow-cli/src/bench/targets/rpc.rs`,
  `app/crow-cli/src/bench/report.rs`.

## Lock-free stats (remove Mutex)

- [ ] **Replace Mutex<OpStats> with LatencyHistogram + Counter**: in
  `BenchWorkerCtx`, replace `stats: Mutex<OpStats>` with
  `latency: LatencyHistogram` (lock-free, from `crow_common::metrics`
  — gives p50/p99/avg/max via AtomicU64 + Relaxed, no locks) +
  `ops: Counter` (lock-free) + `errors: Counter` (lock-free). The
  callback `bench_on_complete` calls `latency.observe(ns)` +
  `ops.inc()` — no mutex, no lock contention.
  Files: `app/crow-cli/src/bench/targets/rpc.rs`.

- [ ] **Merge per-worker metrics at end**: after the bench, snapshot
  each worker's `LatencyHistogram` + `Counter` and merge into the
  final `BenchReport`. The `LatencyHistogram::snapshot()` returns
  count/avg/p50/p99/max; merge by summing counts and taking the
  weighted avg.
  Files: `app/crow-cli/src/bench/targets/rpc.rs`,
  `app/crow-cli/src/bench/report.rs`.

- [ ] **Add avg-only mode for comparison**: add a `--stats-mode`
  flag (`histogram` default, `avg-only` for comparison). In
  `avg-only` mode, the callback only does `sum.fetch_add(ns)` +
  `count.fetch_add(1)` — no histogram buckets, no binary search.
  This lets us measure the overhead of percentile computation.
  Files: `app/crow-cli/src/commands/bench.rs`,
  `app/crow-cli/src/bench/targets/rpc.rs`.

## File list

- `lib/crow-rpc/examples/echo_server.cpp` — new standalone echo
  server example binary
- `lib/crow-rpc/CMakeLists.txt` — add echo-server target
- `app/crow-cli/src/commands/bench.rs` — `--stats-mode` flag
- `app/crow-cli/src/bench/targets/rpc.rs` — spawn server child
  process, connect to external server, replace Mutex<OpStats> with
  LatencyHistogram + Counter, stop + read metrics from log
- `app/crow-cli/src/bench/report.rs` — merge per-worker histogram
  snapshots, add server transport stats from log file

## Test checklist

- [ ] **Unit**: echo server binary starts, listens, and responds to
  a ping (manual smoke test)
- [ ] **Bench**: run `crow-cli bench run --target rpc` with the
  2-process model, verify TPS + zero errors
- [ ] **Bench**: run with `--stats-mode histogram` vs
  `--stats-mode avg-only`, compare TPS to measure histogram overhead
- [ ] **Bench**: run `tools/bench-rpc-regression.sh` and compare
  TPS vs the in-process baseline (585K)
