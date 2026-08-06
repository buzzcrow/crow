<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# R51 Plan: S3-style scan pagination + server byte budget, drop ScanStream

## Task breakdown

### Phase 1: C++ engine — `byte_budget` in scan path

- [ ] Add `size_t byte_budget` param to `Crowtree::scan` signature
      (`crow-tree.h:578`) and implementation (`crow-tree.cpp:1720`).
      Add `accumulated_bytes` counter + budget stop + oversized warning
      in the `consider` lambda.
- [ ] Add `size_t byte_budget` param to `try_scan_no_load`
      (`crow-tree.h:991`, `crow-tree.cpp:1937`). Same `consider` lambda
      changes.
- [ ] Add `size_t byte_budget` param to `scan_async`
      (`crow-tree.h:595`, `crow-tree.cpp:2143`) and
      `scan_async_attempt` (`crow-tree.h:1004`, `crow-tree.cpp:2156`).
      Thread through to `try_scan_no_load` with `remaining_byte_budget`
      adjustment across cold-leaf retries.
- [ ] Update C API: `ct_scan` (`c_api.h:400`, `c_api.cpp:897`) and
      `ct_scan_async` (`c_api.h:347`, `c_api.cpp:786`) — add
      `size_t byte_budget` param.
- [ ] Update all C++ callers: `scan_step_bench.cpp:113`,
      `read_path_bench.cpp:86`, `c_api_test.cpp:72`,
      `async_scan_test.cpp` (all `ct_scan` / `ct_scan_async` calls),
      `read_path_test.cpp`, `parity_test.cpp`, `overflow_test.cpp`,
      `double_buffer_test.cpp`, `stress_test.cpp` — pass `0` (unlimited).
- [ ] Add C++ test: `ReadPath.ScanByteBudget` in `read_path_test.cpp` —
      entries with known sizes, budget stops mid-scan, `truncated` set,
      always-return-1 guard, oversized-entry path.

### Phase 2: FFI — thread `byte_budget`

- [ ] Add `byte_budget: usize` to `Crowtree::scan` (`ffi/src/lib.rs:1044`),
      `AsyncCrowtree::scan` (`ffi/src/lib.rs:1677`),
      `AsyncCrowtree::try_scan` (`ffi/src/lib.rs:1704`). Pass to C API.
- [ ] Update FFI callers: `crow_tree_engine.rs` (`iter_all:115`,
      `live_key_count:231` pass 0), `ffi_test.rs` (all `scan` calls
      pass 0).

### Phase 3: Rust KV layer — trait + engine + store

- [ ] Add `byte_budget: usize` to `KVEngine::scan` (`kv_engine.rs:65`).
- [ ] Update `CrowTreeEngine::scan` (`crow_tree_engine.rs:203`): pass
      `byte_budget` to `try_scan`.
- [ ] Update `InMemKV::scan` (`mem_kv_impl.rs:95`): accumulate
      `key.len() + value.len()`, stop with `truncated` when budget
      exceeded (always-return-1 guard). Pass 0 from existing test
      callers.
- [ ] Add `byte_budget: usize` to `PxLearner::engine_scan`
      (`learner.rs:220`).
- [ ] Add `SCAN_BYTE_BUDGET` constant (3.5 MiB) in `px_kv_store.rs`,
      pass to `engine_scan` in `kv_scan` (`px_kv_store.rs:191`).
- [ ] Update Rust test callers: `conformance.rs` (pass 0),
      `crow_tree_engine_test.rs` (pass 0), `mem_kv_test.rs` (pass 0),
      `kv_forward_test.rs` (no change — uses proto directly).

### Phase 4: Client — pagination loop + delete ScanStream

- [ ] Add internal pagination loop to `CrowkvClient::scan`
      (`client.rs:758`): loop pages until `!truncated` or total >=
      caller's `limit`.
- [ ] Delete `CrowkvClient::scan_stream` (`client.rs:839-944`).
- [ ] Delete `KvScanChunk` import from `client.rs`.
- [ ] Delete proto: `rpc ScanStream` (`kv.proto:183`), `message
      KvScanChunk` (`kv.proto:162`).
- [ ] Delete server: `scan_stream` handler (`kv_service.rs:555-683`),
      `chunk_scan_response` (`kv_service.rs:755-835`),
      `ScanStreamStream` type, `KvScanChunk` import,
      `tokio_stream::Stream` import (if now unused).
- [ ] Update bench: `bench/runner.rs:658` — `scan_stream` → `scan`.

### Phase 5: Tests + verification

- [ ] Add Rust conformance test: `scan_byte_budget_stops_and_truncates`
      in `conformance.rs`, wired into `crow_tree_engine_test.rs` and
      `mem_kv_test.rs`.
- [ ] Run `pixi run test-ct` (C++ tests).
- [ ] Run `pixi run test-ffi` (FFI tests).
- [ ] Run `pixi run test-core` (Rust lib tests).
- [ ] Run `pixi run cargo fmt --all -- --check`.
- [ ] Run `pixi run cargo clippy --all-targets -- -D warnings`.
- [ ] Run `clang-format --dry-run --Werror` on changed `.cpp`/`.h`.
- [ ] Run `tree-lint` on changed C++ files.

## File list

- `lib/crow-tree/include/crow-tree/crow-tree.h` — scan signatures
- `lib/crow-tree/include/crow-tree/c_api.h` — C API signatures
- `lib/crow-tree/src/crow-tree.cpp` — scan, try_scan_no_load,
  scan_async, scan_async_attempt
- `lib/crow-tree/src/c_api.cpp` — ct_scan, ct_scan_async
- `lib/crow-tree/bench/scan_step_bench.cpp` — pass 0
- `lib/crow-tree/bench/read_path_bench.cpp` — pass 0
- `lib/crow-tree/tests/integration/*.cpp` — pass 0, add byte_budget test
- `lib/crow-tree/ffi/src/lib.rs` — FFI scan methods
- `lib/crow-tree/ffi/tests/ffi_test.rs` — pass 0
- `lib/crow-kv/src/kv/kv_engine.rs` — trait
- `lib/crow-kv/src/kv/crow_tree_engine.rs` — engine impl
- `lib/crow-kv/src/paxos/learner.rs` — engine_scan
- `lib/crow-kv/src/cluster/px_kv_store.rs` — constant + kv_scan
- `lib/crow-kv/src/rpc/proto/kv.proto` — delete ScanStream + KvScanChunk
- `lib/crow-kv/src/rpc/kv_service.rs` — delete scan_stream +
  chunk_scan_response
- `lib/crow-kv-client/src/client.rs` — pagination loop, delete
  scan_stream
- `lib/crow-kv/tests/kv/conformance.rs` — byte_budget test + pass 0
- `lib/crow-kv/tests/kv/crow_tree_engine_test.rs` — pass 0 + new test
- `lib/crow-kv/tests/kv/mem_kv_impl.rs` — InMemKV scan + pass 0
- `lib/crow-kv/tests/kv/mem_kv_test.rs` — pass 0 + new test
- `app/crow-cli/src/bench/runner.rs` — scan_stream → scan

## Test checklist

- [ ] C++ `ReadPath.ScanByteBudget`: budget stops mid-scan, truncated,
      always-return-1, oversized entry
- [ ] Rust conformance `scan_byte_budget_stops_and_truncates`: both
      engines
- [ ] Existing `ReadPath.*` scan tests pass (pass 0 = unlimited)
- [ ] Existing `AsyncScan.*` tests pass (pass 0 = unlimited)
- [ ] Existing Rust scan tests pass (conformance, engine, mem_kv,
      forward)
- [ ] FFI tests pass
- [ ] Lint passes (fmt, clippy, clang-format, tree-lint)
