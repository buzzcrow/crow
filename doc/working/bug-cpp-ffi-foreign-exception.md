# Bug: C++ foreign exception crossing FFI boundary during shutdown

## Status

**Partially resolved (Linux).** The foreign exception crash did NOT
reproduce on Linux. Instead, `cluster_restart_incremental_test` failed
deterministically with a Paxos election convergence failure ("group
X/Y failed to converge to one leader within 3s"). Root cause traced to
a **stale binary** — the `crow-kv-server` binary was built before
commit `4704d356` (Wire RPC transport into remote replicas during
restore), so the transport wiring in `start()` was missing. After
rebuilding, all 25 tests pass consistently across 5 full-suite runs.

The original macOS foreign-exception crash may be a separate issue
that still needs investigation on macOS. See "Linux investigation"
below.

## Failing test

`cluster_restart_incremental_test` — specifically `restart_5node_1group`
and `restart_5node_2group` in the full suite (not in isolation — single
test runs pass consistently).

```
pixi run cargo test -p crow-web --test cluster_restart_incremental_test -- --test-threads=1
```

## Error

```
fatal runtime error: Rust cannot catch foreign exceptions, aborting
```

SIGABRT (signal 6) from `std::process::abort()` inside Rust's
`catch_unwind` cleanup — Rust's personality routine encounters a
foreign (C++) exception and aborts the process.

## Crash trace (from macOS DiagnosticReports)

Faulting thread: `tokio-rt-worker` (varies — sometimes thread 4, 5, 7, 17).

Key frames (demangled):
```
 0  __pthread_kill
 1  pthread_kill
 2  abort
 3  std::sys::pal::unix::abort_internal
 4  std::process::abort
 5  ___rust_foreign_exception          ← Rust detected C++ exception
 6  ___rust_panic_cleanup
 7  std::panicking::catch_unwind::cleanup
 8  catch_unwind<..., crow_kv::cluster::group_election_candidate::PxGroup::run_prevote_round>
 9  __rust_try
10  catch_unwind<..., run_prevote_round>
11  tokio::runtime::task::harness::poll_future<run_prevote_round>
12-14  tokio task harness / raw::poll
15-19  tokio worker scheduler / run_task / run
```

The crash also appears in `run_heartbeat_round` (leader state) — same
pattern, different election function. Both call into `PxRpcTransport::send_*`
→ `RpcClient::call` → `crow_rpc_client_send` (C ABI) → C++ `RpcClient::send`.

## Root cause analysis

### What's happening

During the restart phase of `cluster_restart_incremental_test`, nodes
are killed (SIGTERM) and restarted. When a node is killed, its peer
nodes have outstanding RPC requests (PreVote, Heartbeat) to it. The
peer's TCP connection breaks, and the C++ I/O worker thread detects
the EOF/error and closes the connection.

The exception is thrown somewhere in the C++ I/O worker thread or the
caller thread (tokio worker) during the `send` path, and propagates
through the FFI boundary into Rust's `catch_unwind`, which cannot
handle foreign exceptions and aborts.

### Where the exception originates (unknown)

The C++ code does NOT explicitly throw. The likely sources of implicit
C++ exceptions are:
- `std::bad_alloc` from `new` (e.g. `new OutFrame` in `build_frame`)
- `std::system_error` from `std::mutex` lock failures
- `std::out_of_range` from `.at()` calls (e.g. `connections_.at(fd)` in
  `Worker::add_connection`)
- folly `ConcurrentHashMap` internal exceptions

### What was tried

1. **try/catch in all `c_api.cpp` functions** — wrapped every C ABI
   function in `try { ... } catch (...) { return error; }`. This is
   standard practice for C++ exposing a C ABI. Did NOT fix the crash —
   the catch blocks were never hit (no diagnostic output appeared).

2. **try/catch in `RpcClient::send`** (client.cpp) — wrapped the
   innermost C++ function before the exception could escape. Did NOT
   fix — catch blocks never hit.

3. **try/catch in `Worker::run_loop`** (socket_transport.cpp) — wrapped
   the I/O worker event loop. Did NOT fix — catch blocks never hit.

4. **try/catch in `Connection::try_send`** (connection.cpp) — wrapped
   the writev path. Did NOT fix — catch blocks never hit.

5. **`-fexceptions` flag** in build.rs — explicitly enabled exceptions
   in the C++ build. No effect (exceptions were already enabled by
   default with cc-rs).

### Why the catch blocks don't fire

The catch blocks in the FFI layer and C++ internals never fire, yet
the crash still occurs. This means the exception is NOT being thrown
through any of the wrapped functions. Possible explanations:

1. **The exception is thrown on a different thread** that doesn't go
   through the FFI boundary — e.g., the C++ I/O worker thread throws,
   and `std::terminate` is called (no catch on that thread), which
   calls `abort()`. But the crash trace shows the exception on a
   `tokio-rt-worker` thread, not a C++ I/O worker thread.

2. **The exception is thrown in a code path not wrapped** — e.g., in
   the `on_complete_cb` callback (Rust function called from C++ I/O
   thread), or in the `Buffer::from_raw` / `Buffer::release` path
   during cleanup.

3. **It's not actually a C++ exception** — the `___rust_foreign_exception`
   symbol fires for any non-Rust unwind, including signals or
   `longjmp`. But the crash trace shows `abort()` from Rust's
   `catch_unwind`, which is the foreign-exception path.

4. **The exception is thrown during stack unwinding of another
   exception** — if a C++ destructor throws during exception handling,
   `std::terminate` is called. This would bypass all catch blocks.

### Timing / race condition evidence

- Single test runs (`restart_5node_1group` alone) pass 6/6 consistently.
- Full suite runs fail ~1 in 3 (2 of 5 runs in one batch).
- The crash happens during the restart phase, not initial startup.
- The crash is on a peer node (not the restarted node) — the peer has
  an outstanding RPC to the killed node when the connection breaks.
- Failure is more frequent with more nodes/groups (5n-2g fails more
  than 5n-1g), suggesting connection teardown pressure matters.

## Reproduction

```bash
# Fails ~1 in 3 full-suite runs:
for i in 1 2 3 4 5; do
  pixi run cargo test -p crow-web --test cluster_restart_incremental_test -- --test-threads=1
done

# Single test passes consistently (6/6):
for i in 1 2 3 4 5 6; do
  pixi run cargo test -p crow-web --test cluster_restart_incremental_test restart_5node_1group -- --test-threads=1
done
```

Crash logs: `~/Library/Logs/DiagnosticReports/crow-kv-server-*.ips`
Test logs: `/Users/cj/cpp/crow/test-logs/restart-*`

## Next steps to investigate

1. **Enable core dumps** (`ulimit -c unlimited`) and get a full
   backtrace with locals — the macOS `.ips` crash reports only have
   frame addresses, not local variables or heap state.

2. **Add `std::set_terminate` handler** — install a custom
   `std::terminate_handler` in C++ that prints the current exception
   type (`std::current_exception()`) before aborting. This will
   distinguish between "C++ exception on wrong thread" vs "exception
   during stack unwinding".

3. **Build with `-fno-omit-frame-pointer` and full debug info** (`-g`
   instead of `-g1`) to get complete backtraces from the C++ side.

4. **Check if folly ConcurrentHashMap throws** — the `pending_` map in
   `RpcClient` is a `folly::ConcurrentHashMap`. If it throws during
   `insert_or_assign` or `erase` (e.g. rehash under memory pressure),
   the exception would escape through `send` → `crow_rpc_client_send`
   → Rust. But the try/catch in `send` should catch this...

5. **Check the `on_complete_cb` Rust callback path** — when a
   connection closes, `fail_all` invokes `invoke_c_complete` which
   calls the Rust `on_complete_cb`. If the Rust callback panics (not
   throws), that's a different path. But the crash trace shows
   `___rust_foreign_exception`, not a Rust panic.

6. **Check if the exception is from a destructor** — add
   `noexcept` to all C++ destructors in the hot path (`~Connection`,
   `~OutFrame`, `~Buffer`) to prevent destructor exceptions from
   causing `std::terminate`.

## Partial fix applied (try/catch guards)

The try/catch guards in `c_api.cpp`, `client.cpp`, `connection.cpp`,
and `socket_transport.cpp` are kept — they are correct standard
practice for C++ exposing a C ABI, even though they don't fix this
specific crash. They prevent future crashes from explicit C++ throws.

## Linux investigation (2026-08-25)

### Symptom on Linux

The foreign exception crash did **not** reproduce on Linux. Instead,
`cluster_restart_incremental_test` failed deterministically with:

```
group 0/1 failed to converge to one leader within 3s
```

This is a Paxos election failure, not a crash. All nodes restart but
never elect a leader because PreVote RPCs to peers fail silently.

### Root cause: stale binary

The `crow-kv-server` binary at
`target/debug/crow-kv-server` was built at **12:51**, but commit
`4704d356` ("Wire RPC transport into remote replicas during restore")
was committed at **16:39**. That commit added the transport wiring
loop in `KvServer::start()` (lines 124-133 of `kv_server.rs`) that
calls `real.set_rpc_transport(transport.clone())` on each remote
replica after the RPC server starts.

Without this wiring, `PxRemoteReplica::transport_or_err()` returns
`Err(Internal("crow-rpc transport unavailable: not set for peer N"))`
for every PreVote/RequestVote/Heartbeat call. The election driver
logs this at `debug!` level (filtered out at the default `INFO`
level), so the failure was invisible without raising the log level.

### Debug logging added

Added a `debug!` log in `KvServer::start()` that reports
`remote_count` and `wired` count per group, plus `group_count` in the
"kv server started" info log. This confirms the transport is now wired
correctly after rebuilding:

```
DEBUG start: wired rpc transport into remote replicas store_id=0 group_id=0 remote_count=4 wired=2
INFO  kv server started store_id=0 listen_addr=0.0.0.0:42769 group_count=1
```

### Verification

After rebuilding `crow-kv-server` (`pixi run cargo build -p
crow-kv-server`), ran the full test suite 5 times:

```
for i in 1 2 3 4 5; do
  pixi run cargo test -p crow-web --test cluster_restart_incremental_test -- --test-threads=1
done
```

Result: **25/25 tests passed** (5 runs × 5 tests each). Zero PreVote
transport errors in the logs; leaders elected successfully on all
groups after restart.

### Remaining concerns

- The macOS foreign-exception crash may still occur — it was not
  tested on macOS after the binary rebuild. The Linux election
  failure and the macOS crash may share the same root cause (RPC
  connection failures after restart) but manifest differently, or
  they may be independent issues.
- The C++ RPC layer (`SocketTransport::connect`, `RpcClient::send`)
  still has no logging on connection failures and no retry logic.
  Silent failures make debugging difficult. Consider adding `warn!`
  or `error!` level logs in the C++ transport layer for connection
  errors.
