<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CLI Bench: Multi-Target (RPC / KV / DiskDb / ChunkDb) Plan

Goal: extend `crow-cli bench` to benchmark the crow-rpc server (echo
workload) in addition to the existing KV cluster bench, with a trait-based
abstraction that supports future diskdb/chunkdb targets.

Design draft: `doc/working/design-bench-multi-target.md` (to be written
alongside this plan if the user approves).

## Background

The existing bench (`app/crow-cli/src/bench/`) is tightly coupled to
`CrowkvClient` — the runner creates a `CrowkvClient`, the worker calls
`kv.get/put/delete/scan`, and the report assumes KV-specific fields
(`store_id`, `group_id`, `read_mode`, etc.).

The RPC bench needs a fundamentally different execution model:
- **No cluster provisioning** — a single in-process `RpcServer` with an
  echo handler (no Paxos, no WAL, no consensus overhead).
- **Pipelined sends** — multiple worker threads share C connections,
  fire requests without waiting for responses (the pattern from
  `load_test.cpp`), giving the transport a chance to batch `writev` calls.
- **Different op semantics** — the only op is "echo" (send N bytes, receive
  N bytes back); there is no read/write/list/mix distinction.

Future targets (diskdb, chunkdb) use gRPC (tonic), have stateful ops
(allocate/free, append/seal), and need cluster provisioning similar to KV.

## Architecture: BenchTarget + BenchClient traits

Introduce two traits that abstract the target-specific parts. The runner,
worker, report, and progress/metrics infrastructure stay shared.

```
bench/
  bench.rs              — pub mod index (existing)
  target.rs             — BenchTarget + BenchClient traits (shared interface)
  target/
    kv.rs               — KvTarget + KvBenchClient + KvFixture (existing KV code)
    rpc.rs              — RpcTarget + RpcBenchClient (new)
  runner.rs             — run_bench<T: BenchTarget>() (generic, shared)
  worker.rs             — run_worker<C: BenchClient>() (generic, shared)
  workload.rs           — WorkloadKind, OpGen, OpKind (shared, existing)
  report.rs             — BenchReport (shared, +target field)
  report_format.rs      — (shared, +target label in output)
  metrics_flusher.rs    — (shared, existing)
  metrics_log.rs        — (shared, existing, unchanged)
```

The split is:
- **Top level** = the shared bench engine (runner, worker, workload,
  report, metrics). These are target-agnostic — they drive the closed-loop
  or pipelined op loop, collect latency histograms, and write the report.
- **`target/`** = one module per target engine. Each module is
  self-contained: it provisions the server, builds clients, and implements
  `issue_op`. Adding a new target (diskdb, chunkdb) = one new module
  under `target/` + one `pub mod` line in `target.rs`.

`target.rs` is a pure Rust 2018 index: `pub mod kv; pub mod rpc;` +
`pub use` re-exports of the traits. No logic.

Each target module starts as a single file (`kv.rs`, `rpc.rs`). If the
code grows past the 300-line healthy threshold, split into a directory
following Rust 2018 style — e.g. `kv.rs` + `kv/` with sub-files for
fixture, client, and target impl. Same for `rpc.rs` → `rpc.rs` + `rpc/`.
Future targets (diskdb, chunkdb) each get their own module from the
start, independent of the others.

### BenchClient trait

```rust
/// A client that can issue bench ops. One instance per worker.
/// Closed-loop: `issue_op` is awaited before the next call.
/// Pipelined: the runner fires up to `pipeline_depth` concurrent
/// `issue_op` futures per worker.
#[async_trait::async_trait]
pub trait BenchClient: Send + Sync + 'static {
    /// Issue one op. Returns the outcome + latency is measured by caller.
    async fn issue_op(
        &self,
        kind: OpKind,
        gen: &mut OpGen,
        cfg: &BenchConfig,
    ) -> OpOutcome;
}
```

### BenchTarget trait

```rust
/// A benchmark target: provisions the server, builds clients, cleans up.
pub trait BenchTarget: Send {
    type Client: BenchClient;

    /// Short label for the report: "kv", "rpc", "diskdb", "chunkdb".
    fn label(&self) -> &'static str;

    /// Provision the server(s). Called before measurement.
    async fn provision(&mut self, cfg: &BenchConfig) -> Result<()>;

    /// Build a client for one worker. Called `cfg.threads` times.
    async fn build_client(&self) -> Result<Self::Client>;

    /// Pre-populate the key space (KV only; RPC/diskdb return (0, 0)).
    async fn pre_populate(
        &self,
        client: &Self::Client,
        cfg: &BenchConfig,
    ) -> Result<(u64 /* ms */, u64 /* errors */)>;

    /// Cleanup: stop servers, kill processes, etc.
    async fn cleanup(&mut self);

    /// Whether this target supports pipelined sends.
    /// KV/diskdb/chunkdb = false (closed-loop); RPC = true.
    fn supports_pipeline(&self) -> bool { false }

    /// Default pipeline depth when `--pipeline-depth` is not set.
    /// RPC: `connections * threads`; others: 1.
    fn default_pipeline_depth(&self, cfg: &BenchConfig) -> usize { 1 }
}
```

### Runner changes

`run_bench` becomes generic: `run_bench<T: BenchTarget>(target: &mut T, cfg: &BenchConfig)`.

The runner:
1. Calls `target.provision(cfg)`.
2. Builds `cfg.threads` clients via `target.build_client()`.
3. Runs pre-population via `target.pre_populate()`.
4. Spawns worker tasks. Each worker calls `run_worker::<T::Client>(client, gen, cfg, ...)`.
5. If `pipeline_depth > 1`, the worker uses a `tokio::sync::Semaphore` to
   fire up to `pipeline_depth` concurrent `issue_op` futures, collecting
   results as they complete (not fire-all-then-collect — bounded
   concurrency is more realistic and avoids send-queue overflow).
6. Reduces stats, writes report (with `target.label()` in the report).

### Worker changes

`run_worker` becomes generic over `C: BenchClient`. The op dispatch
(`get/put/delete/scan` for KV, `echo` for RPC) moves into the
`BenchClient::issue_op` implementation. The worker loop handles:
- Deadline checking
- Warmup window
- Latency recording
- Counter bumping
- Pipeline depth (semaphore-bounded concurrent ops)

### KV target (wrapping existing code)

`KvTarget` wraps the existing `BenchFixture` + `CrowkvClient`. The
`KvBenchClient` wraps a `CrowkvClient` clone and implements `issue_op`
with the existing get/put/delete/scan dispatch (moved from `worker.rs`).
Pre-population moves from `runner.rs` into `KvTarget::pre_populate()`.

This is a refactor, not new logic — the existing KV bench behavior is
preserved exactly.

### RPC target (new)

`RpcTarget` provisions an in-process `RpcServer`:
1. Create `RpcServer` (with a `BufferPool` sized for the bench).
2. Register the built-in echo handler via a one-line FFI call.
3. Listen on `127.0.0.1:0` (ephemeral port).
4. Start the server.
5. `build_client()` creates `RpcBenchClient` — connects to the server,
   creates an `RpcClient`, and stores the `Connection` + `BufferPool`.

`RpcBenchClient::issue_op`:
1. Build a `ConnectionPingRequest` flatbuffer (control buffer).
2. Allocate a data buffer of `cfg.value_size` bytes (deterministic payload).
3. Call `rpc_client.call(server, conn, ctrl, data, ECHO_MSG_TYPE)`.
4. Await the `CallFuture`.
5. Verify the response data matches the request data (correctness check).
6. Return `OpOutcome { ok: true, .. }`.

The RPC target sets `supports_pipeline() = true` and
`default_pipeline_depth() = cfg.connections * cfg.threads` (or a
configurable cap, e.g. 256).

## C API: built-in echo handler

Since the CLI is Rust and links `crow-rpc-ffi` directly, we skip the
generic C callback handler registration entirely. No `crow_rpc_handler_fn`
typedef, no `user_data` marshalling, no C-to-Rust callback bridge.

Instead, add a built-in echo handler in C++ and a one-line C API call to
register it:

```c
// Register the built-in echo handler for the given msg_type.
// The echo handler returns the request data as the response data,
// with a ConnectionPingResponse control buffer echoing the request_id.
void crow_rpc_server_register_echo_handler(
    crow_rpc_server_t server, uint16_t msg_type);
```

The C++ implementation in `c_api.cpp` calls `server->register_handler(
msg_type, echo_handler_fn)` where `echo_handler_fn` is a C++ function
that allocates response buffers from the connection's pool, copies the
request data, builds the `OutFrame`, and returns it. This is the same
echo handler logic used in `load_test.cpp`, just compiled into the
library.

## FFI: expose echo handler registration

In `crow-rpc-ffi/src/server.rs`:
- Add `RpcServer::register_echo_handler(msg_type: u16)` — a thin safe
  wrapper over `crow_rpc_server_register_echo_handler`. One line of
  unsafe FFI, no callback glue.

## CLI changes

### New `--target` flag

```
crow-cli bench run --target rpc [--workload echo] [--value-size 512] ...
crow-cli bench run --target kv  [--workload mix]  [--value-size 64]  ...
```

- `--target` defaults to `kv` (back-compat).
- `--target rpc` selects the RPC echo bench.
- `--pipeline-depth N` (default: target-specific) controls pipelining.
- `--workload echo` is the only RPC workload (alias for the echo pattern).

### RunArgs additions

```rust
/// Bench target: "kv" (default) or "rpc".
#[arg(long, default_value = "kv")]
pub target: String,

/// Pipeline depth: max concurrent in-flight ops per worker.
/// Default: 1 (closed-loop). RPC target defaults to connections*threads.
#[arg(long)]
pub pipeline_depth: Option<usize>,
```

### Dispatch in bench_benchmark()

```rust
match target {
    "kv" => {
        let mut target = KvTarget::new();
        run_bench(&mut target, cfg).await
    }
    "rpc" => {
        let mut target = RpcTarget::new();
        run_bench(&mut target, cfg).await
    }
    "diskdb" => Err("diskdb bench not yet implemented"),
    "chunkdb" => Err("chunkdb bench not yet implemented"),
}
```

## Report changes

Add `target: String` to `BenchReport` (default `"kv"` for back-compat).
The human summary and markdown report show the target label. The JSON
report includes it for filtering/comparison.

## Future: diskdb / chunkdb targets

The trait is designed so these plug in without runner/worker changes:
- `DiskDbTarget`: provisions a diskdb cluster (via `BenchFixture`-like
  fixture), `DiskDbBenchClient::issue_op` calls `allocate_blocks` /
  `free_blocks` in a loop. Workload = "allocate" / "free" / "mix".
- `ChunkDbTarget`: provisions a chunkdb cluster, `ChunkDbBenchClient::
  issue_op` calls `allocate_chunk` / `append_chunk` / `seal_chunk` /
  `delete_chunk`. Workload = "lifecycle" / "append" / "mix".

These are out of scope for this task — the trait + `--target` flag just
need to exist so the future work is additive.

## Task breakdown

### Phase 1: C API + FFI echo handler

- [ ] **Add built-in echo handler to C API**: `crow_rpc_server_register_
  echo_handler(server, msg_type)` in `c_api.h` + `c_api.cpp`. The C++
  implementation registers a `HandlerFn` that echoes request data back
  as response data (same logic as `load_test.cpp`'s echo handler, but
  compiled into the library). No generic C callback — just the built-in.
  Files: `lib/crow-rpc/include/crow-rpc/c_api.h`,
  `lib/crow-rpc/src/c_api.cpp`.

- [ ] **Expose echo handler in FFI**: add
  `RpcServer::register_echo_handler(msg_type: u16)` safe wrapper in
  `crow-rpc-ffi/src/server.rs`. One line of unsafe FFI over the C
  function. Add the binding to `sys.rs`.
  Files: `lib/crow-rpc/ffi/src/server.rs`, `lib/crow-rpc/ffi/src/sys.rs`.

- [ ] **Build + test**: verify the FFI loopback test still passes. Add
  an FFI test that registers the echo handler and verifies data
  round-trip.
  Files: `lib/crow-rpc/ffi/tests/ffi_loopback.rs`.

### Phase 2: Bench trait abstraction

- [ ] **Add `target.rs`**: `BenchTarget` trait, `BenchClient` trait,
  `TargetKind` enum. No logic — just the trait definitions.
  Files: `app/crow-cli/src/bench/target.rs`, `app/crow-cli/src/bench.rs`.

- [ ] **Add `async-trait` dependency**: the traits need
  `#[async_trait::async_trait]` for async fn in traits.
  Files: `app/crow-cli/Cargo.toml`.

- [ ] **Refactor runner to be generic**: `run_bench<T: BenchTarget>`.
  Move KV-specific pre-population into `KvTarget::pre_populate()`.
  Move KV-specific client creation into `KvTarget::build_client()`.
  The runner calls `target.provision()`, `target.build_client()`,
  `target.pre_populate()`, spawns workers, reduces stats.
  Files: `app/crow-cli/src/bench/runner.rs`.

- [ ] **Refactor worker to be generic**: `run_worker<C: BenchClient>`.
  Move the KV op dispatch (get/put/delete/scan) into `KvBenchClient::
  issue_op()`. The worker loop handles deadline, warmup, latency,
  counters, and pipeline depth (semaphore-bounded concurrency).
  Files: `app/crow-cli/src/bench/worker.rs`.

- [ ] **Extract KvTarget + KvBenchClient**: move existing KV bench
  logic into `target/kv.rs` (from `provision.rs`). `KvTarget`
  wraps `BenchFixture`, `KvBenchClient` wraps `CrowkvClient`.
  Files: `app/crow-cli/src/bench/target/kv.rs` (new, from
  `provision.rs`), `app/crow-cli/src/bench/target.rs`,
  `app/crow-cli/src/bench.rs`.

- [ ] **Add pipeline depth to BenchConfig**: `pipeline_depth: usize`
  (default 1). Worker uses `Semaphore` when > 1.
  Files: `app/crow-cli/src/bench/runner.rs`,
  `app/crow-cli/src/bench/worker.rs`.

- [ ] **Add `target` field to BenchReport**: `target: String` with
  `#[serde(default = "default_kv")]` for back-compat. Update
  `human_summary()` and `markdown_report()` to show the target.
  Files: `app/crow-cli/src/bench/report.rs`,
  `app/crow-cli/src/bench/report_format.rs`.

- [ ] **Verify KV bench still passes**: run `pixi run test-console-cli`
  (includes `bench_benchmark_test`) and confirm no regression.
  Files: (no file changes — verification only).

### Phase 3: RPC target

- [ ] **Add `crow-rpc-ffi` dependency to crow-cli**:
  Files: `app/crow-cli/Cargo.toml`.

- [ ] **Implement `RpcTarget`**: provisions an in-process `RpcServer`
  with echo handler, listens on ephemeral port, starts server.
  `build_client()` connects to the server, creates `RpcClient` +
  `BufferPool`. `cleanup()` stops the server.
  Files: `app/crow-cli/src/bench/target/rpc.rs` (new),
  `app/crow-cli/src/bench/target.rs`, `app/crow-cli/src/bench.rs`.

- [ ] **Implement `RpcBenchClient::issue_op`**: builds a
  `ConnectionPingRequest` flatbuffer, allocates data buffer of
  `cfg.value_size` bytes, calls `rpc_client.call()`, awaits response,
  verifies data round-trip. Returns `OpOutcome`.
  Files: `app/crow-cli/src/bench/target/rpc.rs`.

- [ ] **Wire `--target rpc` in CLI**: parse `--target`, dispatch to
  `RpcTarget` vs `KvTarget`. Add `--pipeline-depth` flag. Set
  `cfg.pipeline_depth` from CLI or target default.
  Files: `app/crow-cli/src/commands/bench.rs`.

- [ ] **Add RPC bench integration test**: `tests/rpc_bench_test.rs` —
  runs `crow-cli bench run --target rpc --duration-secs 2 --threads 4
  --connections 2 --value-size 512` and verifies the report has
  `total_ops > 0`, `error_rate == 0.0`, `target == "rpc"`.
  Files: `app/crow-cli/tests/rpc_bench_test.rs` (new),
  `app/crow-cli/Cargo.toml` (add `[[test]]` entry).

### Phase 4: Tools scripts + polish + docs

- [ ] **Update tools bench scripts for `--target` flag**: the existing
  scripts (`tools/bench-write-regression.sh`,
  `tools/bench-read-regression.sh`, `tools/bench-scan-regression.sh`,
  `tools/profile-write.sh`) call `crow-cli bench run` without
  `--target` — they default to `kv` so they keep working unchanged.
  Add a new `tools/bench-rpc-regression.sh` script that runs the RPC
  echo bench across a thread/connection sweep (1T:1C → 64T:4C) with
  pipelining, recording ops/s + latency into
  `doc/working/bench-rpc-regression.tsv`. Mirror the structure of
  `bench-write-regression.sh` (run_bench helper, jq parse, TSV output).
  Files: `tools/bench-rpc-regression.sh` (new).

- [ ] **Update bench help text**: `--target` description, `--workload
  echo` for RPC, `--pipeline-depth` description.
  Files: `app/crow-cli/src/commands/bench.rs`.

- [ ] **Update AGENTS.md**: note the `--target` flag in the bench
  section if one exists, or add a brief note.
  Files: `AGENTS.md`.

- [ ] **Full test suite (regression check)**: after all code changes
  are complete, rerun the full test suite to confirm no regressions
  beyond the targeted tests. Run every test task in `pixi.toml`:
  `test-tree-ct`, `test-tree-ffi`, `test-rpc-ct`, `test-rpc-ffi`,
  `test-common`, `test-protocol`, `test-kv-core`, `test-kv-client`,
  `test-diskdb-client`, `test-chunkdb-client`, `test-kv-server`,
  `test-diskdb`, `test-chunkdb`, `test-console-shared`,
  `test-console-cli`, `test-console-server`, `test-console-ui`.
  Record pass/fail counts; any failure must be investigated.
  Files: (verification only).

### Phase 5: KV max-TPS flow analysis

- [ ] **Write KV max-TPS bench suite + flow analysis**: design a test
  suite that finds the maximum write TPS the KV cluster can sustain,
  then write a flow analysis doc (similar to
  `doc/design/kv/kv-write-flow-analysis.md` and
  `kv-read-flow-analysis.md`) documenting the flow, bench numbers, and
  bottlenecks.

  The max-TPS suite uses **more load threads, fewer connections** to
  achieve max bandwidth — the opposite of the read bench (which uses
  T:C ≈ 1:1). Write throughput is limited by the consensus pipeline
  (Paxos rounds), not by HTTP/2 connection concurrency, so piling
  many threads onto few connections maximizes offered load while
  keeping the gRPC channel pool small. Coalescing (R45/R45b) amplifies
  this: more concurrent proposals = fuller batches = fewer Paxos
  rounds = higher TPS.

  Config sweep (write-only, mem mode, 3-node cluster, 512B values,
  1M keys, coalesce=32, drain=1, max-inflight=32):
  - 1T:1C (baseline) → 4T:1C → 8T:1C → 16T:1C → 32T:1C → 64T:1C
  - Then 64T:2C, 64T:4C (diminishing returns — connection count
    shouldn't matter for writes, confirm this)
  - Then 128T:1C, 256T:1C (find the saturation knee)

  The flow analysis doc (`doc/design/kv/kv-max-tps-flow-analysis.md`)
  follows the structure of `kv-write-flow-analysis.md`:
  - Flow trace (reuse the existing write flow trace, focus on the
    saturation path: where does the pipeline bottleneck — inflight
    permits? coalescer drain? WAL append? quorum RTT?)
  - Benchmark results table (ops/s, avg/p50/p99/p999 latency, WAL
    append count, errors) for each config
  - Analysis: where is the knee, what saturates, T:C ratio effect
    (expected: none for writes, unlike reads)
  - Comparison with the existing write-flow-analysis numbers (which
    used T:C ≈ 1:1) — does high-T-low-C actually beat 1:1?
  - Bottleneck identification + optimization ideas

  Add the doc to `doc/doc_index.md` under the KV flow-analysis rows.
  Add a regression sentinel script `tools/bench-max-tps-regression.sh`
  with the key configs.

  Files: `doc/design/kv/kv-max-tps-flow-analysis.md` (new),
  `doc/doc_index.md` (add row),
  `tools/bench-max-tps-regression.sh` (new).

## File list

- `lib/crow-rpc/include/crow-rpc/c_api.h` — add
  `crow_rpc_server_register_echo_handler` declaration.
- `lib/crow-rpc/src/c_api.cpp` — implement built-in echo handler +
  registration function (calls `server->register_handler`).
- `lib/crow-rpc/ffi/src/server.rs` — add `register_echo_handler` safe
  wrapper (one line of unsafe FFI).
- `lib/crow-rpc/ffi/src/sys.rs` — add FFI binding for
  `crow_rpc_server_register_echo_handler`.
- `lib/crow-rpc/ffi/tests/ffi_loopback.rs` — add echo handler test.
- `app/crow-cli/Cargo.toml` — add `crow-rpc-ffi`, `async-trait` deps;
  add `[[test]]` for rpc_bench_test.
- `app/crow-cli/src/bench.rs` — add `target` module, remove
  `provision` module (moved into `target/kv.rs`).
- `app/crow-cli/src/bench/target.rs` — new: `BenchTarget` +
  `BenchClient` traits + `pub mod kv; pub mod rpc;` index.
- `app/crow-cli/src/bench/target/kv.rs` — moved from `provision.rs`;
  add `KvTarget` + `KvBenchClient` (wraps existing `BenchFixture` +
  `CrowkvClient`).
- `app/crow-cli/src/bench/target/rpc.rs` — new: `RpcTarget` +
  `RpcBenchClient`.
- `app/crow-cli/src/bench/runner.rs` — generic `run_bench<T>`, add
  `pipeline_depth` to `BenchConfig`.
- `app/crow-cli/src/bench/worker.rs` — generic `run_worker<C>`,
  pipeline depth support.
- `app/crow-cli/src/bench/report.rs` — add `target` field.
- `app/crow-cli/src/bench/report_format.rs` — show target in output.
- `app/crow-cli/src/commands/bench.rs` — add `--target`, `--pipeline-
  depth` flags; dispatch to target.
- `app/crow-cli/tests/rpc_bench_test.rs` — new: RPC bench integration
  test.
- `tools/bench-rpc-regression.sh` — new: RPC echo bench regression
  sentinel (thread/connection sweep with pipelining).
- `tools/bench-max-tps-regression.sh` — new: KV max-TPS regression
  sentinel (high-T-low-C sweep).
- `doc/design/kv/kv-max-tps-flow-analysis.md` — new: KV max-TPS flow
  analysis + bench numbers (mirrors `kv-write-flow-analysis.md`
  structure).
- `doc/doc_index.md` — add row for `kv-max-tps-flow-analysis.md`.
- `AGENTS.md` — note `--target` flag.

## Test checklist

### Unit / FFI
- [ ] FFI echo handler test: register built-in echo handler via
  `register_echo_handler`, send data, verify response data matches
  (extends `ffi_loopback.rs`).
- [ ] FFI existing tests still pass: `ping_loopback`,
  `ping_loopback_with_data`, `server_create_listen_start_stop`,
  `server_connect_to_peer`, `buffer_pool_alloc_write_release`.

### Integration
- [ ] RPC bench integration test: `crow-cli bench run --target rpc
  --duration-secs 2 --threads 4 --connections 2 --value-size 512`
  produces a report with `total_ops > 0`, `error_rate == 0.0`,
  `target == "rpc"`.
- [ ] KV bench regression: `test-console-cli` (includes
  `bench_benchmark_test`) passes unchanged.
- [ ] RPC bench with pipeline depth: `--target rpc --pipeline-depth 64`
  produces higher throughput than `--pipeline-depth 1`.

### Full suite (regression rerun — after all code changes)
- [ ] `pixi run test-rpc-ct` — 27/27 passed.
- [ ] `pixi run test-rpc-ffi` — 6/6 passed (5 existing + 1 new echo).
- [ ] `pixi run test-console-cli` — passes (KV bench regression check).
- [ ] `pixi run test-tree-ct` — 387/387 passed.
- [ ] `pixi run test-tree-ffi` — 30/30 passed.
- [ ] `pixi run test-common` — 8/8 passed.
- [ ] `pixi run test-protocol` — 92/92 passed.
- [ ] `pixi run test-kv-core` — 107/107 passed.
- [ ] `pixi run test-kv-client` — 6/6 passed.
- [ ] `pixi run test-diskdb-client` — 6/6 passed.
- [ ] `pixi run test-chunkdb-client` — 15/15 passed.
- [ ] `pixi run test-kv-server` — 5/5 passed.
- [ ] `pixi run test-diskdb` — 13/13 passed.
- [ ] `pixi run test-chunkdb` — 66/66 passed.
- [ ] `pixi run test-console-shared` — passes.
- [ ] `pixi run test-console-server` — 61/61 passed.
- [ ] `pixi run test-console-ui` — 75/75 passed.

### KV max-TPS bench (Phase 5)
- [ ] `tools/bench-max-tps-regression.sh` runs clean (zero errors
  across all configs).
- [ ] `doc/design/kv/kv-max-tps-flow-analysis.md` has bench results
  table + analysis (knee identified, T:C ratio effect documented).
- [ ] `tools/bench-rpc-regression.sh` runs clean (zero errors across
  all configs).
