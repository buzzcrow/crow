<!-- Copyright 2026-present buzzcrow <126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# RPC Perf Gap2+Gap3 Plan

Goal: eliminate the tokio scheduler round-trip and per-request heap
allocation from the RPC client response path, closing the remaining
perf gap.

Context: gap analysis identified four perf gaps. Gap4 (ONESHOT re-arm
mutexes) and Gap1 (pending map mutex → folly::ConcurrentHashMap) are
done. This plan covers the remaining two.

Current path: C++ on_response → oneshot::Sender::send → tokio
reactor wake → thread switch → future poll → task resumes. That's 2
heap allocs + 1 free + channel + scheduler + thread switch per
request.

## Gap2: Callback-based client model (eliminate tokio scheduler)

Replace the tokio-oneshot response path with an inline callback model
where the C++ I/O worker directly resumes the Rust bench worker
without going through tokio's scheduler.

### Tasks

- [x] **Design completion callback ABI**: define a C ABI callback
  that the C++ `RpcClient::on_response` invokes directly on the I/O
  worker thread. The callback receives the response frame + a
  `user_data` pointer. No oneshot channel, no tokio wake.
  Files: `lib/crow-rpc/include/crow-rpc/c_api.h`,
  `lib/crow-rpc/include/crow-rpc/c_api_internal.h`,
  `lib/crow-rpc/src/c_api.cpp`.

- [x] **Add slab-based completion pool**: pre-allocate N completion
  slots (indexed by request_id mod N). Each slot holds a completion
  callback + user_data. The `call()` path reserves a slot by index;
  `on_response` looks up by index (O(1), no hash). This replaces both
  the folly map (Gap1) and the per-call heap allocation (Gap3).
  Files: `lib/crow-rpc/include/crow-rpc/client/client.h`,
  `lib/crow-rpc/src/client/client.cpp`.

- [x] **Rust bench worker callback model**: change the RPC bench
  worker from an `async fn` loop (tokio task) to a callback-driven
  loop. The callback (invoked inline on the I/O worker thread) records
  latency, builds the next request, and submits it — all without a
  tokio scheduler round-trip. A counting semaphore or atomic counter
  tracks completion for the bench deadline.
  Files: `app/crow-cli/src/bench/targets/rpc.rs`,
  `app/crow-cli/src/bench/target.rs`,
  `app/crow-cli/src/bench/runner.rs`,
  `lib/crow-rpc/ffi/src/client.rs`.

- [x] **Fallback for non-bench callers**: the existing oneshot-based
  `call()` API stays for non-bench callers (KV client, consensus).
  The callback model is opt-in via a new `call_callback()` method.
  This avoids forcing all callers to migrate.
  Files: `lib/crow-rpc/ffi/src/client.rs`.

### Key design decisions

- The slab pool size = max pipeline depth (bench `--pipeline-depth`).
  request_id is monotonic; slot = request_id % pool_size. This is
  safe because a slot is only reused after its response arrives
  (closed-loop) or after timeout cleanup.

- The callback runs on the C++ I/O worker thread. It must be
  non-blocking — no tokio async, no I/O. It builds the next request
  buffer and calls `submit()` (which does writev on the caller
  thread). This is an inline resume model — no scheduler round-trip.

- Thread safety: the slab slot is written by the submitter thread
  (tokio worker) and read by the I/O worker thread. Use
  `std::atomic<uint8_t>` state per slot: FREE → PENDING → DONE.
  The submitter sets PENDING before submit; the I/O worker sets DONE
  before invoking the callback. The callback does NOT reset to FREE
  after building the next request — the next `call_callback` sets
  PENDING, so resetting would overwrite that.

- Slot reuse: each callback advances request_id by pool_size (not +1)
  to stay in the SAME slab slot. This prevents slot reuse collisions
  when responses arrive out of order across workers sharing the pool.

## Gap3: Slab-based completion pool (eliminate per-request heap alloc)

This is subsumed by Gap2's slab pool — the completion slot lives in a
pre-allocated array, not a per-call `Box`. Zero per-request heap
allocation.

### Tasks (merged into Gap2)

- [x] **Remove oneshot channel from bench path**: the bench worker
  no longer creates `oneshot::channel()` per call. The completion is
  signaled via the slab slot state. This removes 2 allocs + 1 free
  per request.
  Files: `lib/crow-rpc/ffi/src/client.rs`,
  `app/crow-cli/src/bench/targets/rpc.rs`.

- [x] **Remove Box::into_raw/from_raw for user_data**: the user_data
  passed to C++ is a slab slot pointer (pre-allocated array), not a
  heap pointer. No Box allocation.
  Files: `app/crow-cli/src/bench/targets/rpc.rs`.

## File list

- `lib/crow-rpc/include/crow-rpc/c_api.h` — new callback-based call
  API
- `lib/crow-rpc/include/crow-rpc/c_api_internal.h` — shared
  Frame→handle helpers (extracted from OnCompleteAdapter)
- `lib/crow-rpc/src/c_api.cpp` — implement callback dispatch
- `lib/crow-rpc/include/crow-rpc/client/client.h` — slab pool type,
  `call_callback()` method
- `lib/crow-rpc/src/client/client.cpp` — slab pool implementation,
  O(1) lookup by index
- `lib/crow-rpc/ffi/src/client.rs` — Rust wrapper for callback model
- `lib/crow-rpc/ffi/src/sys.rs` — FFI declarations for new C funcs
- `lib/crow-rpc/ffi/src/lib.rs` — make `sys` module public
- `app/crow-cli/src/bench/target.rs` — `run_workers` override hook
- `app/crow-cli/src/bench/runner.rs` — delegate to `run_workers`
- `app/crow-cli/src/bench/targets/rpc.rs` — callback-driven worker

## Test checklist

- [x] **Unit**: slab pool insert/find/erase under concurrent access
  (crow-rpc tests — 30 C++ tests pass)
- [x] **Integration**: callback-based echo loopback
  (`ffi/tests/ffi_loopback.rs` — 6 tests pass)
- [ ] **Integration**: callback model with pipeline_depth > 1 —
  verify multiple in-flight requests complete correctly
  (not yet tested; pipeline_depth=4 works in bench but no dedicated
  unit test)
- [x] **Bench**: run `tools/bench-rpc-regression.sh` and compare
  TPS vs Gap4+Gap1 baseline. Peak 585K (1e4w 1000t32c) vs 276K
  baseline = 2.1x. Sentinel 358K (1e1w 256t8c) vs 202K = 1.8x.
  Zero errors across all 9 configs (vs 1452+1762 timeout errors in
  baseline).
- [x] **Bench**: verify no errors at 2e1w/1e2w 512t8c (was 1452/1762
  timeout errors in baseline — now 0 errors, callback model has no
  timeout path to break)

## Results

See `doc/working/rpc-echo-flow-analysis.md` § "Benchmark Results —
2026-08-20 (Gap2+Gap3, Linux)" and `tools/bench-rpc-regression.sh`
reference results B2.
