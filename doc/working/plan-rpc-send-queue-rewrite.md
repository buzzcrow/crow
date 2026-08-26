# Plan: Rewrite crow-rpc send queue — remove IovecRing, two send paths

## Problem

Commit `b29cc0f7` introduced `IovecRing` + `in_send_` lock for send aggregation.
This causes a 5+ second stall in consensus heartbeat delivery, breaking 4
`crow-kv` group_test tests (g4_learner, g5_recovery, g6_reconfig,
membership_epoch_fence). The old path (ae2034a8, stack iovecs) was stable but
had an ordering bug on partial writes (re-enqueue to MPSC queue).

## Target design — two send paths

### Path A: Direct writev (with lock, multi-thread safe)

For low-latency sends from any thread (worker-thread responses, tokio handler
responses when latency matters). Uses a mutex to serialize concurrent writev.

```
submit_direct(conn, frame):
  lock(send_mu_)
  build iovec[] from frame (prepend partial if any)
  writev(fd, iovecs, count)
  on full send: release frame, clear partial
  on partial: keep frame + adjusted offset as pending_partial_
  on EAGAIN: arm EPOLLOUT, keep pending_partial_
  unlock(send_mu_)
```

- No MPSC queue, no eventfd
- One pending partial slot per connection (single frame, not array)
- `send_mu_` mutex serializes writev across threads
- Used by: `submit_inline` (worker thread), optionally by `submit` (cross-thread)

### Path B: Enqueue + worker flush

For aggregation-friendly sends from any thread (cross-thread, high throughput).
Deferred writev on the I/O worker thread.

```
submit(conn, frame):
  enqueue_send(frame)              // MPSC queue
  if worker thread: return         // post-event flush will drain
  else: eventfd notify             // wake worker

worker flush (in run_loop, after event processing):
  drain queue → flat iovec[]       // partials already at front from last time
                                   // append new frames after partials
  writev(fd, iovecs, count)
  consume written bytes:
    fully-sent iovecs → release frames, remove from front
    partial iovec → adjust iov_base/iov_len, move to front
  keep remaining partials in iovec[] for next flush
```

- No `in_send_` lock (only worker does writev)
- No IovecRing (flat `iovec[]` + `OutFrame*[]` arrays in Connection)
- Partials stay at front, new frames appended after — correct order
- Used by: `submit` (cross-thread), `submit` (worker thread via post-event flush)

### Which path is used where — controlled by `direct_write_` flag

**Critical: cannot mix Path A and Path B on the same connection.** If both
paths are active, two threads writev to the same fd → kernel interleaves →
corrupted frame stream. The `direct_write_` flag selects the path for the
**entire transport**, not per-call. Both `submit()` and `submit_inline()`
check the flag — same as the current code.

| Flag | `submit()` (cross-thread) | `submit_inline()` (worker) |
|---|---|---|
| `direct_write_=true` | Path A: `send_direct()` with mutex | Path A: `send_direct()` with mutex |
| `direct_write_=false` (default) | Path B: enqueue + eventfd notify | Path B: enqueue only (post-event flush) |

The `direct_write_` flag and `set_direct_write()` are **kept** — they select
which path the transport uses. The only change from current code is replacing
`try_send` (IovecRing-based) with `send_direct()` (Path A) and `flush_send()`
(Path B).

## Files to change (7 files)

### Remove (delete)
- `lib/crow-rpc/include/crow-rpc/iovec_ring.h` — IovecRing class
- `lib/crow-rpc/src/iovec_ring.cpp` — IovecRing implementation

### Keep unchanged (flag stays)
- `c_api.cpp`, `c_api.h` — `set_direct_write` kept
- `server.rs`, `sys.rs` — `set_direct_write` kept
- `rpc.rs`, `runner.rs` — `direct_write` config kept
- `echo_server.cpp` — `direct_write` CLI option kept

### Modify — C++

#### `lib/crow-rpc/include/crow-rpc/connection.h`
- Remove `#include "crow-rpc/iovec_ring.h"`
- Remove `IovecRing ring_` member
- Remove `std::atomic<bool> in_send_` member
- Remove `try_send(int fd, TransportStats *stats)` method
- Remove `has_pending_send()` (or rewrite to check queue + partials)
- Add: `send_direct(int fd, OutFrame *frame, TransportStats *stats)` — Path A
- Add: `flush_send(int fd, TransportStats *stats)` — Path B worker flush
- Add: `has_pending_send()` — check `send_queue_.has_pending() || has_partial_`
- Add flat iovec state for Path B:
  ```cpp
  iovec     pending_iovs_[3 * BATCH_MAX];   // partials at front + new frames
  OutFrame *pending_frames_[BATCH_MAX];      // frames backing pending_iovs_
  int       pending_iov_count_;              // total iovecs in pending_iovs_
  int       pending_frame_count_;            // frames in pending_frames_
  ```
- Add Path A partial state:
  ```cpp
  std::mutex send_mu_;                       // serializes direct writev
  OutFrame  *direct_partial_{nullptr};       // single partial frame (Path A)
  ```
- Add: `clear_send_state()` — release all pending frames (on close)

#### `lib/crow-rpc/src/connection.cpp`
- Remove `try_send()` implementation (the IovecRing-based one)
- Remove debug `fprintf` statements
- Implement `send_direct()`:
  - Lock `send_mu_`
  - If `direct_partial_` exists, prepend its remaining iovecs
  - Build iovecs from new frame, append after partial
  - `writev(fd, iovs, count)`
  - Consume bytes: if partial frame fully sent, release it, clear; if new
    frame fully sent, release it; if partial, keep in `direct_partial_` with
    updated offset
  - On EAGAIN: arm EPOLLOUT (caller does this), keep partial
  - On hard error: close, release frames
  - Unlock `send_mu_`
- Implement `flush_send()`:
  - No lock (only I/O worker calls this)
  - If `pending_iov_count_ > 0`, partials are already at front
  - Drain MPSC queue into `pending_iovs_[]` after partials, build iovecs
  - `writev(fd, pending_iovs_, pending_iov_count_)`
  - Consume bytes: release fully-sent frames, compact partials to front
  - On EAGAIN: keep partials, return false (caller arms EPOLLOUT)
  - On hard error: close, release all pending frames
- Implement `clear_send_state()`:
  - Release all `pending_frames_[]`, release `direct_partial_`
  - Clear counts
- Update `close()` to call `clear_send_state()` instead of `ring_.clear()`

#### `lib/crow-rpc/include/crow-rpc/transport/socket_transport.h`
- **Keep** `set_direct_write(bool)` method and `bool direct_write_` member
- **Keep** `submit_inline(Connection *, OutFrame *)` method (now calls `send_direct` when `direct_write_=true`, enqueues when false)
- Update comments to reflect new send paths

#### `lib/crow-rpc/src/transport/socket_transport.cpp`
- Remove debug `fprintf` statements (3 places: submit, on_readable_impl, try_send area)
- Rewrite `submit()`:
  - `direct_write_=true`: call `conn->send_direct(fd, frame, &stats_)` (Path A)
  - `direct_write_=false`: enqueue + eventfd notify (Path B, keep current logic)
- Rewrite `submit_inline()`:
  - `direct_write_=true`: call `conn->send_direct(fd, frame, &stats_)` (Path A)
  - `direct_write_=false`: enqueue only (Path B, post-event flush drains it)
- Rewrite `on_writable_impl()`:
  - Call `conn->flush_send(fd, stats)` instead of `try_send()`
- Update Worker::run_loop Notify handler:
  - Call `conn->flush_send(wfd, stats_)` instead of `conn->try_send(wfd, stats_)`
- Update Worker::run_loop post-event flush:
  - Call `conn->flush_send(wfd, stats_)` instead of `on_writable_impl()`

#### `lib/crow-rpc/src/server/server.cpp`
- Keep `transport_->submit_inline(conn, response)` calls (2 places, lines 206 and 224)
- `submit_inline` now dispatches to Path A or B based on `direct_write_` flag

#### `lib/crow-rpc/src/c_api.cpp`
- **Keep** `crow_rpc_server_set_direct_write()` function (flag still needed)

#### `lib/crow-rpc/include/crow-rpc/c_api.h`
- **Keep** `crow_rpc_server_set_direct_write` declaration

### Modify — Rust FFI

#### `lib/crow-rpc/ffi/src/server.rs`
- **Keep** `set_direct_write()` method (flag still needed)

#### `lib/crow-rpc/ffi/src/sys.rs`
- **Keep** `crow_rpc_server_set_direct_write` extern declaration

### Modify — Rust callers

#### `app/crow-cli/src/bench/targets/rpc.rs`
- **Keep** `server.set_direct_write(cfg.direct_write)` (flag still works)

#### `app/crow-cli/src/bench/runner.rs`
- **Keep** `direct_write: bool` field and init

### Modify — C++ examples/tests

#### `lib/crow-rpc/examples/echo_server.cpp`
- **Keep** `direct_write` CLI option and `crow_rpc_server_set_direct_write` call (flag still works)

#### `lib/crow-rpc/tests/transport_test.cpp`
- Check for `set_direct_write` / `direct_write` references, remove if any

## Implementation order

1. Remove debug `fprintf` from `socket_transport.cpp` and `connection.cpp`
2. Rewrite `connection.h` — new members (flat iovecs, mutex, partials), remove IovecRing/in_send_
3. Rewrite `connection.cpp` — `send_direct()`, `flush_send()`, `clear_send_state()`
4. Update `socket_transport.h` — keep direct_write_ flag, update comments
5. Rewrite `socket_transport.cpp` — submit() dispatches by flag, submit_inline() dispatches by flag, on_writable_impl uses flush_send, run_loop uses flush_send
6. `server.cpp` — no change needed (submit_inline signature unchanged)
7. Delete `iovec_ring.h` and `iovec_ring.cpp`
8. `c_api.cpp` / `c_api.h` — no change (keep set_direct_write)
9. Rust FFI — no change (keep set_direct_write)
10. Rust callers — no change (keep direct_write config)
11. `echo_server.cpp` — no change (keep direct_write option)
12. Build C++ (`pixi run build-cpp`)
13. Build Rust (`pixi run cargo build`)
14. Test: `pixi run cargo test -p crow-kv --test group_test -- --test-threads=1 g4_learner_stream::learner_stream_rapid_fire_writes g6_reconfig::reconfig_add_replica`
15. Run 5x to confirm stability
16. Run full `pixi run test-suite`

## Verification

- g4_learner_stream::learner_stream_rapid_fire_writes — passes 5/5
- g6_reconfig::reconfig_add_replica — passes 5/5
- g5_recovery, membership_epoch_fence — pass 3/3
- crow-rpc transport_test — passes
- crow-rpc echo_server — builds and runs
- Full test-suite — passes (or only pre-existing unrelated failures remain)
- `cargo fmt --check`, `cargo clippy -- -D warnings`, `clang-format --dry-run --Werror`
