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

---

## 13. Detailed Implementation Plan with Test Examples

> Scoped 2026-07-08. Feasibility confirmed: `liburing` 2.14 available via
> conda-forge (not yet a `crowkv`/`crowtree` pixi dependency — add it), this
> environment's kernel (6.8) has full io_uring support. See
> [`design-crowkv-async-kvengine.md`](design-crowkv-async-kvengine.md) for
> the consumer-side (`KVEngine`/`PxLearner`/gRPC) plan this work needs to
> actually matter in production — that doc can and should land **before**
> this one (see its §7 sequencing), since it's independently valuable
> plumbing-only work and de-risks this larger effort by proving the
> `KVFuture::Pending` boundary shape *before* a real reactor exists behind it.

Each phase below is independently buildable/testable and should be its own
PR/session: `crowtree` is ASan+TSan-gated and this is the riskiest
concurrency surface added since #12/#13, so no phase should skip sanitizer
runs before moving to the next.

### Phase 0 — Contract cleanup (no code)

Confirm and record (this doc already does the following; treat as done):
- C ABI stays completion-based (`ct_future`), no coroutines/Folly.
- `ct_reactor_eventfd()` ownership: the `Reactor` owns and closes the
  `eventfd`; Rust wraps the raw fd via `AsyncFd` **without** taking close
  ownership (document this explicitly in the FFI wrapper's safety comment
  when Phase 3 lands, mirroring how `Crowtree`'s `Drop` already owns
  `ct_close`).
- Add `liburing` to `crowtree/CMakeLists.txt` (`find_package` or
  `pkg-config`) and to the workspace `pixi.toml` `[dependencies]` (matching
  the sibling `sirius` project's conda-forge `liburing` pin as a reference
  version).

### Phase 1 — C++ reactor + async `PageStore` (this session's recommended stopping point if only one phase lands)

**New files:** `crowtree/include/crowtree/reactor.h`, `crowtree/src/reactor.cpp`,
`crowtree/include/crowtree/async_page_store.h`,
`crowtree/src/file_async_page_store.cpp`.
**Fully additive:** no existing file changes required for this phase (not
even `crowtree.h`/`.cpp`) — `Reactor` and `AsyncPageStore` are new,
free-standing types exercised only by their own unit tests until Phase 2
wires them into `resident()`/`flush()`/`snapshot()`.

```cpp
// reactor.h
namespace crowtree {
class Reactor {
  public:
    explicit Reactor(unsigned ring_entries = 256);
    ~Reactor(); // stops thread, closes io_uring + eventfd

    Reactor(const Reactor &) = delete;
    Reactor &operator=(const Reactor &) = delete;

    // Submit a read/write; on_complete(res) receives the raw io_uring CQE
    // res (>=0 bytes transferred, <0 -errno). Returns an opaque op id
    // (io_uring SQE user_data) usable with cancel().
    uint64_t submit_read(int fd, void *buf, size_t len, off_t offset,
                         std::function<void(int)> on_complete);
    uint64_t submit_write(int fd, const void *buf, size_t len, off_t offset,
                          std::function<void(int)> on_complete);
    uint64_t submit_fsync(int fd, std::function<void(int)> on_complete);

    // Best-effort; see design §8 (already-in-flight CQEs still arrive, the
    // reactor checks a "cancelled" flag before dispatching).
    void cancel(uint64_t op_id);

    [[nodiscard]] int eventfd() const { return eventfd_; }

  private:
    void run(); // io_uring_enter loop; see design §3.1
    // ...
};
}
```

```cpp
// async_page_store.h
namespace crowtree {
class AsyncPageStore {
  public:
    virtual ~AsyncPageStore() = default;
    virtual uint64_t submit_read(PageAddr addr, void *buf, size_t len,
                                 std::function<void(Status)> on_complete) = 0;
    virtual uint64_t submit_write(PageAddr addr, const void *buf, size_t len,
                                  std::function<void(Status)> on_complete) = 0;
    virtual Status submit_fsync(std::function<void(Status)> on_complete) = 0;
    virtual void cancel(uint64_t op_id) = 0;
};

// FilePageStore's async twin: same fd, Reactor-backed. MemPageStore's async
// twin is NOT a separate class -- a synchronous "complete in the caller's
// stack frame, no reactor" implementation is enough for tests (see UT below).
class FileAsyncPageStore : public AsyncPageStore {
  public:
    static Status open(const std::string &path, uint32_t iu_size, Reactor *reactor,
                       std::unique_ptr<FileAsyncPageStore> *out);
    // ... submit_read/write/fsync/cancel using reactor_->submit_*
};
}
```

**Unit tests** (`crowtree/tests/unit/reactor_test.cpp`, new):

```cpp
TEST(Reactor, SubmitReadCompletesViaCallback) {
    // Write known bytes with a plain pwrite, then submit_read via the
    // reactor and block (test-only spin/condvar) on the callback firing;
    // assert bytes match and the callback's `res` == len.
}

TEST(Reactor, SubmitWriteThenReadRoundTrips) {
    // submit_write via the reactor, wait for completion, then a plain
    // pread confirms the bytes landed (proves the reactor thread actually
    // performed the I/O, not just invoked the callback with fake success).
}

TEST(Reactor, MultipleConcurrentSubmitsAllComplete) {
    // Submit N (e.g. 64) reads/writes at distinct offsets before waiting on
    // any completion; assert all N callbacks eventually fire exactly once,
    // with the correct per-op bytes (proves the SQE->callback map in
    // Reactor::run() dispatches to the right callback, not the first one).
}

TEST(Reactor, CancelBeforeCompletionSuppressesCallback) {
    // Submit a request, immediately cancel() it, and assert the callback
    // never fires (best-effort: allow a bounded wait then check a flag,
    // matching design §8's "check cancelled before dispatch" contract).
}

TEST(Reactor, DestructorStopsThreadCleanly) {
    // Construct + immediately destroy a Reactor with no submissions; must
    // not hang or leak the thread (join in ~Reactor). Run under TSan.
}
```

**Sanitizer note:** this phase is the first genuinely new concurrency
primitive since #12/#13 (a dedicated OS thread doing kernel-level
completion dispatch into arbitrary callbacks) — run
`pixi run ct-asan`/`ct-tsan` after every test added here, not just at the
end of the phase.

### Phase 2 — C API async variants

**Files:** `crowtree/include/crowtree/c_api.h`, `crowtree/src/c_api.cpp`
(extend, additive — no existing `ct_get`/`ct_flush`/`ct_snapshot` signatures
change), new `ct_future` opaque struct + `Crowtree::get_view_async`-style
internal helper in `crowtree.h`/`.cpp` that:
1. Runs the *exact* fast-path lookup `get_view()` already does (§B3 of
   `plan-tree.md` — no duplicated logic, this phase should factor the
   L0-then-L1-resident-hit check into a shared private helper both
   `get_view()` and this new path call).
2. On a hit (including L1-resident), fills `ct_future` synchronously,
   `state = kDone`.
3. On a genuine miss (page tagged unloaded in `MappingTable`), submits a
   read via the `Reactor`/`AsyncPageStore` from Phase 1, `state = kPending`.

```c
// c_api.h additions
typedef struct ct_future ct_future;
ct_future *ct_get_async(ct_tree *t, const uint8_t *key, size_t klen);
ct_future *ct_flush_async(ct_tree *t);
ct_future *ct_snapshot_async(ct_tree *t);
ct_status ct_future_poll(ct_future *f, int *done, ct_buf *out_value, uint64_t *out_slot);
void ct_future_free(ct_future *f);
int ct_reactor_eventfd(ct_tree *t);
```

**Unit tests** (`crowtree/tests/integration/async_get_test.cpp`, new;
`MemPageStore`-backed cases don't need a real reactor since nothing is ever
`kPending` against pure in-memory storage — use `FilePageStore` "opened as
async" only for the miss-path tests):

```cpp
TEST(AsyncGet, FastPathHitCompletesSynchronously) {
    // apply + flush a key (L1-resident, clean or not -- no eviction here),
    // ct_get_async returns a future whose FIRST ct_future_poll call has
    // done=1 -- assert this without ever touching the reactor/eventfd.
}

TEST(AsyncGet, MissAfterEvictionCompletesViaReactor) {
    // apply + flush + snapshot + evict_clean_leaves(0) to force the target
    // key's leaf unloaded, then ct_get_async -> first poll has done=0,
    // then poll again after a bounded wait (or after reading the eventfd)
    // -> done=1 with the correct value.
}

TEST(AsyncGet, FutureFreeBeforeCompletionDoesNotCrashOrLeak) {
    // Force the miss path, ct_future_free before the reactor completes;
    // run under ASan to confirm no use-after-free when the CQE later
    // arrives and the reactor checks liveness before dispatch.
}

TEST(AsyncFlushSnapshot, AlwaysPendingThenCompletes) {
    // ct_flush_async / ct_snapshot_async on a dirty tree: first poll is
    // always done=0 (design §4 table: flush/snapshot are *always* async),
    // eventual poll is done=1 and matches the synchronous flush()/
    // snapshot()'s observable effect (last_applied_slot advances, etc.).
}
```

### Phase 3 — Rust FFI futures

**Files:** `crowtree/ffi/src/lib.rs` (extend), new `crowtree/ffi/src/sys.rs`
bindings for the Phase 2 C API additions (bindgen or hand-written, matching
the existing `sys` module's style).

```rust
pub struct CtGetFuture {
    fut: *mut sys::ct_future,
    tree: Arc<Crowtree>,          // keeps the tree (and its Reactor) alive
    eventfd: AsyncFd<OwnedFd>,
}
// impl Future for CtGetFuture -- see design §3.4 (already fully specified).

pub struct CtVoidFuture { /* flush/snapshot: same shape, Output = Result<u64, CtError> for snapshot */ }
```

`AsyncCrowtree` (currently 100% `spawn_blocking`, per §1's Problem
statement) is rewritten method-by-method to construct + `.await` these
futures instead. **No `spawn_blocking` left anywhere in this file** is the
phase's exit criterion.

**Unit tests** (`crowtree/ffi/tests/ffi_test.rs`, extend the existing
`async_bridge_apply_get_snapshot` test + add):

```rust
#[tokio::test]
async fn async_get_fast_path_does_not_spawn_blocking() {
    // Regression guard for the whole point of this phase: wrap the call in
    // a way that would panic/detect if spawn_blocking's thread pool were
    // touched (e.g. assert tokio::runtime::Handle::current().metrics()
    // .num_blocking_threads() is unchanged before/after -- Tokio exposes
    // this metric; if unavailable in the pinned tokio version, fall back to
    // timing: a spawn_blocking hop has a measurable floor latency this test
    // asserts the fast path beats by a wide margin).
}

#[tokio::test]
async fn async_get_slow_path_completes_after_eviction() {
    // File-backed tree; apply+flush+snapshot+evict via the sync Crowtree
    // handle directly (crowtree_ffi already exposes evict_clean_leaves),
    // then AsyncCrowtree::get on the now-unloaded key resolves correctly
    // via the reactor/eventfd path, not spawn_blocking.
}

#[tokio::test]
async fn concurrent_async_gets_all_resolve_correctly() {
    // Spawn N tokio tasks each awaiting AsyncCrowtree::get on a distinct
    // (evicted) key concurrently; assert all N resolve to the correct
    // value -- proves the eventfd/AsyncFd wakeup fans out to every pending
    // future, not just one.
}
```

### Phase 4 — Zero-copy fast-path value

Borrowed `ct_buf` pointing into frame bytes, `ct_future` owning the
`EpochManager::Guard` for the borrow's lifetime (design §5). Reuses the
`GetView` machinery from `#5 B3` (`plan-tree.md`) directly — `ct_get_async`'s
fast path should literally call `get_view()` internally rather than
re-implement the L0/L1 resolution a third time.

**Unit test:** extend Phase 2's `AsyncGet.FastPathHitCompletesSynchronously`
to additionally assert the returned `ct_buf`'s pointer falls within the
resident frame's known address range (test-only introspection hook, or
simply assert no `memcpy` occurred by comparing pointer identity across two
consecutive `ct_get_async` calls for the same still-resident key).

### Phase 5 — Tests + benchmarks

- Full ASan + TSan pass across every new test file above.
- `crowtree/bench/` addition: fast-path `get` latency, `AsyncCrowtree::get`
  (new) vs. today's `spawn_blocking`-based `AsyncCrowtree::get` (keep the
  old implementation temporarily behind a bench-only flag, or benchmark
  against a git-stashed baseline) — the whole point of this effort is a
  measurable latency win here; if there isn't one, that's a finding worth
  recording before declaring the phase done.

### Phase 6 (new — not in the original 5-phase list) — Wire `CrowtreeEngine`

Once Phase 3 lands, `crowtree/../crowkv/src/kv/crowtree_engine.rs`'s
`get`/`scan`/`apply` (converted to return `KVFuture<T>` by
[`design-crowkv-async-kvengine.md`](design-crowkv-async-kvengine.md), which
should land **before** this phase) construct `KVFuture::Pending(Box::pin(...))`
wrapping the Phase 3 `CtGetFuture`/equivalent for a genuine miss, instead of
always returning `KVFuture::Ready(...)`. This is the phase where #11
actually starts mattering to `crowkv` in production.
