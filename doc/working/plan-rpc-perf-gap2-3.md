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

- [ ] **Design completion callback ABI**: define a C ABI callback
  that the C++ `RpcClient::on_response` invokes directly on the I/O
  worker thread. The callback receives the response frame + a
  `user_data` pointer. No oneshot channel, no tokio wake.
  Files: `lib/crow-rpc/include/crow-rpc/c_api.h`,
  `lib/crow-rpc/src/c_api.cpp`.

- [ ] **Add slab-based completion pool**: pre-allocate N completion
  slots (indexed by request_id mod N). Each slot holds a completion
  callback + user_data. The `call()` path reserves a slot by index;
  `on_response` looks up by index (O(1), no hash). This replaces both
  the folly map (Gap1) and the per-call heap allocation (Gap3).
  Files: `lib/crow-rpc/include/crow-rpc/client/client.h`,
  `lib/crow-rpc/src/client/client.cpp`.

- [ ] **Rust bench worker callback model**: change the RPC bench
  worker from an `async fn` loop (tokio task) to a callback-driven
  loop. The callback (invoked inline on the I/O worker thread) records
  latency, builds the next request, and submits it — all without a
  tokio scheduler round-trip. A counting semaphore or atomic counter
  tracks completion for the bench deadline.
  Files: `app/crow-cli/src/bench/target/rpc.rs`,
  `app/crow-cli/src/bench/worker.rs`,
  `lib/crow-rpc/ffi/src/client.rs`.

- [ ] **Fallback for non-bench callers**: the existing oneshot-based
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
  before invoking the callback. The callback reads DONE and clears
  to FREE after building the next request.

## Gap3: Slab-based completion pool (eliminate per-request heap alloc)

This is subsumed by Gap2's slab pool — the completion slot lives in a
pre-allocated array, not a per-call `Box`. Zero per-request heap
allocation.

### Tasks (merged into Gap2)

- [ ] **Remove oneshot channel from bench path**: the bench worker
  no longer creates `oneshot::channel()` per call. The completion is
  signaled via the slab slot state. This removes 2 allocs + 1 free
  per request.
  Files: `lib/crow-rpc/ffi/src/client.rs`,
  `app/crow-cli/src/bench/worker.rs`.

- [ ] **Remove Box::into_raw/from_raw for user_data**: the user_data
  passed to C++ is a slab index (uint32), not a heap pointer. No
  Box allocation.
  Files: `lib/crow-rpc/ffi/src/client.rs`.

## File list

- `lib/crow-rpc/include/crow-rpc/c_api.h` — new callback-based call
  API
- `lib/crow-rpc/src/c_api.cpp` — implement callback dispatch
- `lib/crow-rpc/include/crow-rpc/client/client.h` — slab pool type,
  `call_callback()` method
- `lib/crow-rpc/src/client/client.cpp` — slab pool implementation,
  O(1) lookup by index
- `lib/crow-rpc/ffi/src/client.rs` — Rust wrapper for callback model,
  slab index as user_data
- `app/crow-cli/src/bench/target/rpc.rs` — RPC bench target using
  callback model
- `app/crow-cli/src/bench/worker.rs` — callback-driven worker loop
  for RPC target

## Test checklist

- [ ] **Unit**: slab pool insert/find/erase under concurrent access
  (crow-rpc tests)
- [ ] **Integration**: callback-based echo loopback
  (`ffi/tests/ffi_loopback.rs`) — verify response data matches
- [ ] **Integration**: callback model with pipeline_depth > 1 —
  verify multiple in-flight requests complete correctly
- [ ] **Bench**: run `tools/bench-rpc-regression.sh` and compare
  TPS vs Gap4+Gap1 baseline. Target: 1e2w 512t8c should improve
  beyond 226K (current folly baseline).
- [ ] **Bench**: verify no errors at 1e2w 256t4c (currently 192
  errors from pre-existing connection.cpp retry_send bug — fix
  separately)
