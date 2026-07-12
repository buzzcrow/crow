# Crowtree Async FFI Bridge — Design

> **Status:** design (not yet implemented).
> **Parent:** [`design-crowtree.md`](design-crowtree.md) §2 (D-Q13).
> **Related:** [`design-crowtree-memory.md`](design-crowtree-memory.md) §4 (borrowed read views),
> [`design-crowtree-core.md`](design-crowtree-core.md) §11 (read path),
> [`design-crowtree-persistence.md`](design-crowtree-persistence.md) §4 (buffer pool, demand-load).

## 1. Problem

The current FFI bridge (`crowtree/ffi/src/lib.rs`) wraps all synchronous C++
engine calls in `tokio::task::spawn_blocking`. This has two problems:

1. **Thread-pool overhead.** Every `get`/`apply`/`scan` call hops through
   Tokio's blocking thread pool (default up to 512 threads). For μs-level
   in-memory operations, the ~5–10 μs scheduling overhead is significant
   relative to the operation itself.

2. **Design rule violation.** The engine must be high-performance: no blocking
   calls on async worker threads, no large thread pools. `spawn_blocking` is a
   workaround for synchronous I/O, not a long-term architecture.

## 2. Design principle

**Completion-based async I/O via io_uring. No blocking, no thread pools, no C++ coroutine dependency.**

The production design is **io_uring + eventfd + Rust `Future` polling**, not C++20
coroutines and not the Folly futures stack.

- **C++ coroutine (`co_await`) is not required.** It would add compiler/runtime
  surface and lifetime complexity at the C ABI without improving the kernel I/O
  path. The C boundary still needs an opaque handle (`ct_future`) that Rust can
  poll, so exposing coroutine state machines across FFI is the wrong abstraction.
- **Folly futures are not used.** They bring a large dependency stack and an
  executor model that duplicates Tokio on the Rust side. crowtree only needs a
  small completion object plus notification fd.
- **epoll is only the fallback readiness mechanism.** Linux production I/O uses
  io_uring for disk/block operations and `eventfd` for C++→Rust wakeups. Tokio
  already integrates the fd via `AsyncFd` (epoll on Linux internally), so crowtree
  does not run a separate epoll reactor for storage I/O.

- **Fast path** (in-memory hit): the C++ call completes synchronously and
  returns the result immediately. Rust calls it directly on the Tokio worker
  thread — zero scheduling overhead, μs-level latency.

- **Slow path** (I/O required): the C++ engine submits an io_uring SQE and
  returns `pending`. A single-thread C++ reactor processes completions and
  notifies the Rust `Future` via an `eventfd` that Tokio monitors through
  `AsyncFd`. No thread-per-operation.

## 3. Architecture

```
  Tokio async runtime                         C++ engine
  ┌──────────────────┐                ┌──────────────────────────┐
  │  async fn get()  │                │  ct_get_async()          │
  │  CtGetFuture     │──poll()───────►│  fast path? ──yes──► done│
  │  .poll()         │                │     │ no                 │
  │     │ pending    │◄──done=0───────│  submit io_uring SQE     │
  │     │ register   │                │  return pending          │
  │     │ waker on   │                │                          │
  │     │ AsyncFd    │                │  Reactor thread (1)      │
  │     │ (eventfd)  │◄──eventfd──────│  io_uring_enter loop     │
  │     │ wake       │                │  CQE → callback          │
  │  .poll() again   │──poll()───────►│  ct_future_poll → done=1 │
  │  → Ready(result) │                │  return ct_buf           │
  └──────────────────┘                └──────────────────────────┘
```

### 3.1 C++ Reactor

One reactor per `Crowtree` instance, running on a dedicated C++ thread.

```cpp
class Reactor {
 public:
  Reactor(int ring_entries = 256);
  ~Reactor();  // stops thread, closes io_uring

  // Submit a read request. Returns a future-like handle.
  uint64_t submit_read(int fd, void* buf, size_t len, off_t offset,
                       std::function<void(int)> on_complete);

  // Submit a write request.
  uint64_t submit_write(int fd, const void* buf, size_t len, off_t offset,
                        std::function<void(int)> on_complete);

  // Wake up Rust: write 1 byte to eventfd.
  void notify_rust();

  int eventfd() const { return eventfd_; }

 private:
  void run();  // loop: io_uring_enter + peek_cqe + dispatch callbacks
  int ring_fd_;
  int eventfd_;
  std::thread thread_;
  std::atomic<bool> stopped_{false};
  // SQE → callback mapping
  std::unordered_map<uint64_t, std::function<void(int)>> callbacks_;
};
```

The reactor thread:
1. Calls `io_uring_enter(timeout=...)` — blocks until at least one CQE arrives
   or a timeout fires.
2. Peeks CQEs, dispatches each completion callback.
3. After dispatching, writes to `eventfd_` to wake the Rust side.

### 3.2 `ct_future` handle

```cpp
struct ct_future {
  enum State { kPending, kDone, kError };
  State state = kPending;
  ct_status status = 0;
  ct_buf value = {};       // result bytes (owned)
  uint64_t slot = 0;
  EpochManager::Guard guard;  // keeps frame alive for borrowed fast-path return
  Reactor* reactor = nullptr;
  uint64_t sqe_id = 0;        // io_uring SQE user_data (for cancellation)
};
```

### 3.3 C API (async variants)

```c
// Returns a ct_future. Fast path: future->state == kDone immediately.
// Slow path: future->state == kPending; reactor will complete it.
ct_future* ct_get_async(ct_tree* t, const uint8_t* key, size_t klen);

// Always async (writes to disk).
ct_future* ct_flush_async(ct_tree* t);
ct_future* ct_snapshot_async(ct_tree* t);

// Non-blocking poll. Sets *done=1 if completed (read result + free future).
// Sets *done=0 if still pending (register waker, return Poll::Pending).
ct_status ct_future_poll(ct_future* f, int* done, ct_buf* out_value,
                         uint64_t* out_slot);

// Cancel + free an incomplete future.
void ct_future_free(ct_future* f);

// Get the reactor's eventfd for AsyncFd registration.
int ct_reactor_eventfd(ct_tree* t);
```

### 3.4 Rust FFI — `Future` implementation

```rust
pub struct CtGetFuture {
    fut: *mut sys::ct_future,
    tree: Arc<Crowtree>,
    eventfd: AsyncFd<std::fs::File>,  // or raw fd wrapper
}

impl Future for CtGetFuture {
    type Output = Result<Option<(u64, Vec<u8>)>, CtError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut done: c_int = 0;
        let mut val = sys::ct_buf { data: ptr::null_mut(), len: 0 };
        let mut slot: u64 = 0;
        let rc = unsafe {
            sys::ct_future_poll(self.fut, &mut done, &mut val, &mut slot)
        };
        if rc != 0 {
            return Poll::Ready(Err(check(rc).unwrap_err()));
        }
        if done == 0 {
            // Register waker: when eventfd becomes readable, cx.waker() is called.
            let mut guard = ready!(self.eventfd.poll_read_ready(cx));
            guard.clear_ready();
            return Poll::Pending;
        }
        let value = take_buf(val);
        Poll::Ready(Ok(if value.is_empty() { None } else { Some((slot, value)) }))
    }
}
```

The `AsyncFd` wraps the reactor's `eventfd`. When the reactor writes to it,
Tokio wakes the `Future` and `poll()` is called again — this time
`ct_future_poll` returns `done=1`.

### 3.5 `AsyncCrowtree` replacement

```rust
pub struct AsyncCrowtree {
    inner: Arc<Crowtree>,
}

impl AsyncCrowtree {
    pub async fn get(&self, key: Vec<u8>) -> Result<Option<(u64, Vec<u8>)>, CtError> {
        let fut = unsafe { sys::ct_get_async(self.inner.as_ptr(), key.as_ptr(), key.len()) };
        CtGetFuture { fut, tree: self.inner.clone(), eventfd: self.inner.async_fd() }.await
    }

    pub async fn flush(&self) -> Result<(), CtError> {
        let fut = unsafe { sys::ct_flush_async(self.inner.as_ptr()) };
        CtVoidFuture { fut, /* ... */ }.await
    }
    // ... snapshot, apply_put, apply_delete, scan similarly
}
```

**No `spawn_blocking` anywhere.** Fast-path `get` completes in the first
`poll()` with zero I/O and zero thread scheduling. Slow-path `get` parks on
`AsyncFd` until the reactor signals completion.

## 4. Fast-path vs slow-path classification

| Operation | Fast path (sync) | Slow path (io_uring) |
|-----------|-----------------|---------------------|
| `get` | L0 MemTable hit, L1 resident page hit | L1 cache miss → demand-load from disk |
| `scan` | All pages resident | Any page needs demand-load |
| `apply_put` / `apply_delete` | MemTable insert (no flush triggered) | Triggers flush → write to disk |
| `flush` / `snapshot` | — | Always: write dirty pages to disk |
| `snapshot_view` | All pages resident | Any page needs demand-load |

The C++ engine internally determines which path to take:
- If the operation can complete without I/O → fill `ct_future` synchronously,
  set `state = kDone`, return.
- If I/O is needed → submit SQE(s) to the reactor, set `state = kPending`,
  return. The reactor will complete the future when I/O finishes.

## 5. Zero-copy value across FFI

### Fast path (in-memory hit)

The `ct_future` holds an `EpochManager::Guard` that keeps the frame resident.
`ct_future_poll` returns a `ct_buf` that points directly into the frame bytes
(borrowed, not owned). The Rust side copies the bytes into a `Vec<u8>` via
`take_buf` (which does `slice::from_raw_parts` + `to_vec`), then the guard is
released when `ct_future_free` is called.

This is **one copy** (frame → Rust `Vec`), same as today. The difference is
*where* the copy happens: in the fast path it's a pure memcpy with no I/O
wait, no thread scheduling.

### Slow path (I/O)

The I/O read fills a C++-owned buffer. On completion, `ct_future_poll` returns
this buffer as an owned `ct_buf` (C++ allocates, Rust frees via
`ct_free_buf`). Rust copies it into a `Vec<u8>`. This is also **one copy**
(I/O buffer → Rust `Vec`), identical to today.

### Future optimization (not in initial implementation)

If Rust and C++ share an allocator (e.g., `jemalloc` with `tikv-jemalloc-ffi`),
the `ct_buf` could be moved into a `Vec<u8>` without copying — Rust takes
ownership of the same allocation. This is the "shared allocator" option from
`design-crowtree-memory.md §5` and can be layered on later.

## 6. io_uring integration points

### 6.1 Demand-load (`resident()`)

Currently `resident()` does a synchronous `store_->read_at()` when a page is
unloaded. In the async design:

1. `ct_get_async` detects the leaf page is unloaded.
2. Allocates a buffer, submits an `IORING_OP_READ` SQE to the reactor with the
   file descriptor, offset, and buffer.
3. Returns `pending`.
4. Reactor receives CQE → decodes the page → installs it in the mapping table
   → completes the `ct_future` with the `get` result.

### 6.2 Flush / snapshot

Currently `flush()` / `snapshot()` do synchronous writes. In the async
design:

1. `ct_flush_async` / `ct_snapshot_async` collects dirty pages.
2. Submits `IORING_OP_WRITE` SQEs for each dirty page.
3. Returns `pending`.
4. Reactor receives all CQEs → fsync (if needed, via `IORING_OP_FSYNC`) →
   completes the `ct_future`.

### 6.3 PageStore abstraction

The `PageStore` interface needs an async variant:

```cpp
class AsyncPageStore {
 public:
  virtual ~AsyncPageStore() = default;
  // Submit async read; callback called on completion.
  virtual uint64_t submit_read(PageAddr addr, void* buf, size_t len,
                               std::function<void(Status)> on_complete) = 0;
  virtual uint64_t submit_write(PageAddr addr, const void* buf, size_t len,
                                std::function<void(Status)> on_complete) = 0;
  virtual Status submit_fsync(std::function<void(Status)> on_complete) = 0;
  virtual void cancel(uint64_t op_id) = 0;
};
```

`FilePageStore` implements this using `io_uring` on the file fd.
`BlockPageStore` (block device) uses `io_uring` on the block fd.
The in-memory test store completes synchronously in the callback (no I/O).

## 7. Eventfd / notification mechanism

The reactor writes `1` (8 bytes) to `eventfd_` after processing CQEs. The
Rust side registers this fd with `tokio::io::AsyncFd`:

```rust
let eventfd_file = unsafe { File::from_raw_fd(ct_reactor_eventfd(ptr)) };
let async_fd = AsyncFd::new(eventfd_file)?;
```

When `eventfd` becomes readable, Tokio calls the registered waker, which
re-polls all pending `ct_future`s. The `eventfd` is level-triggered (readable
until consumed), so no missed notifications.

**Alternative (if eventfd is unavailable on macOS):** use a pipe pair.
`kqueue` (macOS) can monitor the read end. The protocol is identical.

## 8. Cancellation

`ct_future_free` on an incomplete future:
1. Cancel the io_uring SQE (if submitted) via `IORING_OP_ASYNC_CANCEL` or
   simply mark the future as cancelled and ignore the CQE when it arrives.
2. Free the buffer (if allocated).
3. Release the epoch guard (if held).
4. Deallocate the `ct_future`.

Cancellation is best-effort: if the I/O is already in flight, the CQE will
still arrive; the reactor checks if the future is still alive before
dispatching.

## 9. Thread model summary

| Thread | Role | Count |
|--------|------|-------|
| Tokio worker | Runs async tasks, calls `ct_future_poll` | N (default = CPU cores) |
| C++ reactor | io_uring event loop, processes CQEs, notifies Rust | 1 per `Crowtree` |
| C++ flush thread | Background flush (from #3/#8) | 1 per `Crowtree` |

**No blocking thread pool.** The reactor is the only additional C++ thread,
and it does no application logic — it only processes I/O completions and
notifies Rust.

## 10. macOS / portability

io_uring is Linux-only. For development on macOS:
- Use a `PipePageStore` (in-memory) that completes synchronously — no io_uring
  needed. Fast path works identically; slow path never triggers (everything is
  resident).
- For testing the slow path on macOS, use a `ThreadedPageStore` that simulates
  async I/O with a single background thread + pipe notification (temporary,
  dev-only).
- Production runs on Linux with real io_uring.

## 11. Implementation phasing

0. **Phase 0 — contract cleanup:** Keep the C ABI completion-based (`ct_future`),
   explicitly avoid C++ coroutines/Folly in v1, and define `ct_reactor_eventfd()`
   ownership rules so Rust wraps the fd without double-close.
1. **Phase 1 — C++ reactor + async PageStore:** Implement `Reactor`,
   `AsyncPageStore`, `FileAsyncPageStore` (io_uring). Unit test with
   synthetic I/O.
2. **Phase 2 — C API async variants:** `ct_get_async`, `ct_flush_async`,
   `ct_snapshot_async`, `ct_future_poll`, `ct_future_free`. Fast path
   returns synchronously; slow path submits to reactor.
3. **Phase 3 — Rust FFI futures:** `CtGetFuture`, `CtVoidFuture` with
   `AsyncFd` on eventfd. Replace `AsyncCrowtree` methods. Remove all
   `spawn_blocking`.
4. **Phase 4 — Zero-copy fast-path value:** Borrowed `ct_buf` pointing into
   frame bytes with epoch guard lifetime.
5. **Phase 5 — Tests + benchmarks:** Verify correctness; benchmark fast-path
   latency vs. old `spawn_blocking` path.

## 12. Decision log

| ID | Decision | Rationale |
|----|----------|-----------|
| D-Q13 | **Async FFI via io_uring + completion-based futures.** No `spawn_blocking`, no large thread pools, no C++ coroutine/Folly dependency. | Design rule: high-performance engine must not block async workers or rely on thread pools for I/O. io_uring gives true async I/O with zero-copy potential. Fast path (in-memory) completes synchronously with zero scheduling overhead. A `ct_future` handle is the correct FFI abstraction for Rust polling; C++ coroutine and Folly executor state must not cross the C ABI. |
| — | **One reactor thread per `Crowtree`.** | Each tree has its own buffer pool and I/O; a per-tree reactor keeps I/O completions local and avoids cross-tree coordination. The reactor does no application logic — only CQE dispatch. |
| — | **eventfd for Rust↔C++ notification.** | Simplest kernel-level notification: one `write(8 bytes)` wakes the Tokio `AsyncFd`. Level-triggered, no missed events. Falls back to pipe on macOS. |
| — | **macOS dev path: in-memory store, no io_uring.** | io_uring is Linux-only. For dev/testing on macOS, the in-memory store completes synchronously (fast path only). Production runs on Linux. |
