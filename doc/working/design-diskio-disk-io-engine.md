<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# diskio — Disk IO Engine (R105)

Implementation design draft for the diskio component. Covers the
detailed design, change scope, complexity, and test case design.

- **Backlog doc**: [`doc/backlog/R105-diskio-disk-io-engine.md`](../backlog/R105-diskio-disk-io-engine.md)
- **Root design doc**: [`doc/design/diskio/design-crow-diskio.md`](../design/diskio/design-crow-diskio.md)
- **Already landed**: R104 (crow-rpc — C++ `RpcServer` + handler
  registry, `crow-rpc-ffi` Rust facade), R72 (diskdb — `Segment` type,
  zone records), R85 (chunkdb — `DiskId` type, group-0
  `HardwareClient`), the existing `crow::tree::Reactor` in
  `lib/crow-tree/` (io_uring event loop, Linux-only,
  `CROW_TREE_HAVE_LIBURING` guard).

Architecture decisions and rationale are in the root design; this doc
does not repeat them.

## 1. Reactor Lift to crow-common (work item 1a)

### 1.1 Why

The `crow::tree::Reactor` (`lib/crow-tree/include/crow-tree/reactor.h`,
`lib/crow-tree/src/reactor.cpp`) is a generic io_uring event loop —
it submits read/write/fsync SQEs and dispatches CQE completions to
per-op callbacks. Nothing about it is tree-specific. diskio needs the
same reactor, and the WAL's io_uring integration (R66) will too.
Keeping it in `crow-tree` forces every consumer to depend on
`crow-tree` just for the reactor.

### 1.2 How

Relocate `reactor.h` and `reactor.cpp` from `lib/crow-tree/` to
`lib/crow-common/cpp/`:

- New files:
  - `lib/crow-common/cpp/include/crow-common/reactor.h`
  - `lib/crow-common/cpp/src/reactor.cpp`
- Delete:
  - `lib/crow-tree/include/crow-tree/reactor.h`
  - `lib/crow-tree/src/reactor.cpp`
- Namespace: `crow::tree::Reactor` → `crow::common::Reactor`
- Guard: `CROW_TREE_HAVE_LIBURING` → `CROW_HAVE_LIBURING`
- CMake: move `find_path(LIBURING...)` + conditional link from
  `lib/crow-tree/CMakeLists.txt` into
  `lib/crow-common/cpp/CMakeLists.txt`. Define `CROW_HAVE_LIBURING`
  in `crow-common`'s CMake (not `crow-tree`'s).
- `crow-tree` CMake: add `target_link_libraries(crow-tree PUBLIC
  crowcommon)` (already linked for `crow-common/log.h`); remove the
  liburing find_path + conditional link.

Update `crow-tree` consumers (include path + namespace):
- `lib/crow-tree/include/crow-tree/async_page_store.h`
- `lib/crow-tree/include/crow-tree/block_page_store.h`
- `lib/crow-tree/include/crow-tree/options.h`
- `lib/crow-tree/include/crow-tree/crow-tree.h`
- `lib/crow-tree/include/crow-tree/c_api.h`
- `lib/crow-tree/src/block_async_page_store.cpp`
- `lib/crow-tree/src/persist.cpp`
- `lib/crow-tree/src/c_api.cpp`
- `lib/crow-tree/src/crow-tree.cpp`

Update `crow-tree-ffi`:
- `lib/crow-tree/ffi/src/sys.rs` — `ct_reactor_eventfd` binding
  resolves the relocated C ABI symbol. The C ABI function name stays
  the same (it's declared in `c_api.h`); only the header path changes.
- `lib/crow-tree/ffi/src/reactor.rs` — update `extern "C"` block if
  it references the header directly (it shouldn't — FFI uses C ABI
  symbols, not headers).

a. `#include "crow-tree/reactor.h"` → `#include "crow-common/reactor.h"`
   in all listed files.
b. `crow::tree::Reactor` → `crow::common::Reactor` in all type
   references.
c. `CROW_TREE_HAVE_LIBURING` → `CROW_HAVE_LIBURING` in the guard.
d. `ct_reactor_eventfd` FFI binding: no change to the C ABI symbol
   name, only the header that declares it moves. The FFI `extern "C"`
   block links by symbol name, not header path.

**Edge cases:**
- `crow-tree` builds on macOS (no liburing) — the reactor header is
  absent in both old and new locations; `#include` is guarded by
  `CROW_HAVE_LIBURING` in the consuming headers. No change to the
  macOS build path.
- `test-tree-ct` and `test-tree-ffi` must pass unchanged — this is a
  pure relocation + rename, no behavior change.

## 2. Polling Modes (work item 1b)

### 2.1 Why

The current `run()` loop calls `io_uring_wait_cqe_timeout` every
iteration with a 50ms timeout. Under sustained I/O, this adds up to
50ms of unnecessary sleep between CQE batches. For diskio's
high-IOPS workloads (NVMe SSD, recovery scan), sub-µs CQE dispatch
matters. crow-tree's `BlockAsyncPageStore` is read-heavy and also
benefits — btree demand-load reads are latency-sensitive.

### 2.2 How

Add a `PollingMode` config to `crow::common::Reactor`:

```cpp
enum class PollingMode {
    Wait,            // current behavior (backward compat default)
    Hybrid,          // busy-poll + event-wait
    Sqpoll,          // kernel SQ-poll thread
};

struct HybridConfig {
    uint32_t busy_poll_budget;  // consecutive empty peeks before wait
};

struct SqpollConfig {
    uint32_t sq_thread_idle;    // ms before kernel SQ thread sleeps
};
```

Constructor change:
```cpp
explicit Reactor(unsigned ring_entries = 256,
                 PollingMode mode = PollingMode::Wait,
                 HybridConfig hybrid = {},
                 SqpollConfig sqpoll = {});
```

For `Sqpoll`, `io_uring_queue_init` is replaced with
`io_uring_queue_init_flags(entries, &ring, IORING_SETUP_SQPOLL)`, and
the SQ thread idle is set via `ring.sq_thread_idle = sqpoll.sq_thread_idle`.

`run()` loop changes by mode:

**Wait** (unchanged): `io_uring_wait_cqe_timeout(&ring, &cqe, &ts)`
every iteration, 50ms timeout. Drain all ready CQEs via
`io_uring_peek_cqe`. Submit pending SQEs once per iteration.

**Hybrid**:
a. Busy-poll phase: call `io_uring_peek_cqe(&ring, &cqe)` in a tight
   loop (no syscall — reads shared memory). If a CQE is ready,
   dispatch it and reset `empty_peek_count = 0`. If no CQE is ready,
   increment `empty_peek_count`.
b. When `empty_peek_count >= busy_poll_budget`, transition to
   event-wait: call `io_uring_submit_and_wait(&ring, wait_nr)` where
   `wait_nr` is the number of pending submissions (or 1 if none
   pending). This combines submit + wait in one syscall.
c. Any CQE resets `empty_peek_count = 0`, returning to busy-poll.
d. When idle (no pending submissions and no CQEs), the
   `io_uring_submit_and_wait` call blocks until a CQE arrives or the
   50ms timeout expires, then re-checks `stopped_`.

**Sqpoll**:
a. The kernel SQ-poll thread pulls SQEs and submits them — no
   `io_uring_submit()` call needed from the reactor.
b. The reactor busy-polls the CQ via `io_uring_peek_cqe` (same as
   Hybrid busy-poll phase).
c. When the SQ thread goes idle (no SQEs for `sq_thread_idle` ms), it
   sleeps and sets `IORING_SQ_NEED_WAKEUP`. The reactor detects this
   via `io_uring_sq_ring_needs_enter(&ring)` and calls
   `io_uring_enter(ring_fd, 0, 0, IORING_ENTER_SQ_WAKEUP)`.
d. Requires root or `CAP_SYS_NOPRIV`.

**Edge cases:**
- `Sqpoll` without root/CAP_SYS_NOPRIV → `io_uring_queue_init_flags`
  fails; `valid_` stays false; `submit_*()` returns `-EIO`
  synchronously. Documented as a config error.
- `Hybrid` with `busy_poll_budget = 0` → degenerates to `Wait` mode
  (immediately transitions to event-wait). Valid but pointless.
- Mode is set at construction time; no runtime switching (avoids
  ring teardown/recreate complexity).

## 3. Batched SQE Submission (work item 1c)

### 3.1 Why

The current `submit_locked` calls `io_uring_submit()` inside the lock
for every single SQE — N SQEs = N submit syscalls. Under high IOPS,
this is a measurable overhead. Batching reduces submit syscalls to
one per loop iteration.

### 3.2 How

Refactor `submit_locked`:

```cpp
// Before (current):
//   get_sqe → prep → io_uring_submit (per SQE)

// After:
//   get_sqe → prep → mark pending_submit_ = true
//   (no io_uring_submit inside submit_locked)
//
// run() loop:
//   if pending_submit_:
//     if mode == Hybrid && in wait phase:
//       io_uring_submit_and_wait(&ring, wait_nr)
//     else:
//       io_uring_submit(&ring)
//     pending_submit_ = false
```

a. Add `std::atomic<bool> pending_submit_{false}` to `Reactor`.
b. In `submit_locked`, after `prep(sqe)` + `callbacks_.emplace()`,
   set `pending_submit_ = true` instead of calling
   `io_uring_submit()`.
c. In `run()`, at the top of each iteration, if `pending_submit_` is
   true, call `io_uring_submit()` (or `io_uring_submit_and_wait` in
   Hybrid wait phase) and clear the flag.
d. The existing SQ-full retry path (4 attempts of
   `io_uring_submit` + `io_uring_get_sqe`) stays — it's the
   backpressure escape valve when the SQ is full.
e. For `Sqpoll` mode, no `io_uring_submit` is needed (the kernel
   thread pulls SQEs); `pending_submit_` is still set but the run
   loop skips the submit call.

**Edge cases:**
- SQ full + batched submit: `submit_locked` still enters the 4-attempt
  retry. If all 4 fail, `on_complete(-ENOMEM)` is called
  synchronously (unchanged behavior).
- `cancel()` does not need to submit — it only erases the callback.
  If the CQE later arrives, it's discarded (unchanged behavior).
- Cross-thread visibility: `pending_submit_` is atomic; the run loop
  reads it at the top of each iteration. A submit from another
  thread sets it; the run loop sees it on the next iteration. Worst
  case: one iteration of delay (50ms in Wait mode, one busy-poll
  cycle in Hybrid). Acceptable — the SQE is already in the ring; only
  the `io_uring_submit` syscall is deferred.

## 4. IoEngine Abstraction + UringEngine (work item 2)

### 4.1 Why

The RPC layer needs a backend-agnostic I/O interface so the same
handler code works with io_uring (Linux), thread-pool pwrite/pread
(macOS), and test engines (dummy, simulated). A virtual base with a
completion callback is the natural C++ abstraction for async I/O.

### 4.2 How

New files:
- `app/crow-diskio/src/engine/io_engine.h` — virtual base
- `app/crow-diskio/src/engine/uring/uring_engine.h`
- `app/crow-diskio/src/engine/uring/uring_engine.cpp`

```cpp
namespace crow::diskio {

class IoEngine {
  public:
    virtual ~IoEngine() = default;
    virtual void submit_write(Disk *disk, off_t phys_offset,
                              const uint8_t *data, size_t size,
                              std::function<void(int)> on_complete) = 0;
    virtual void submit_read(Disk *disk, off_t phys_offset,
                             uint8_t *buf, size_t size,
                             std::function<void(int)> on_complete) = 0;
    virtual void submit_fsync(Disk *disk,
                              std::function<void(int)> on_complete) = 0;
    // Cancel all in-flight I/O for a disk (bad-disk isolation).
    virtual void cancel_disk(DiskId disk_id) {}
};

} // namespace crow::diskio
```

`UringEngine` (Linux, `CROW_HAVE_LIBURING`):
- Wraps `crow::common::Reactor`.
- `submit_write`: calls `reactor_.submit_write(disk->fd(), data, size,
  phys_offset, on_complete)`. The `user_data` (op_id) returned by the
  reactor is tracked in `in_flight_[disk_id].insert(op_id)`.
- `submit_read`: calls `reactor_.submit_read(...)`. Same tracking.
- `submit_fsync`: calls `reactor_.submit_fsync(disk->fd(), on_complete)`.
- `cancel_disk(disk_id)`: for each `op_id` in
  `in_flight_[disk_id]`, submit `IORING_OP_ASYNC_CANCEL` via a new
  reactor method `submit_cancel(op_id, on_cancel_complete)`. Remove
  the op_ids from `in_flight_` after submitting cancels.
- Linked timeouts: each `submit_write`/`submit_read` prepends a
  `io_uring_prep_link_timeout` SQE after the I/O SQE. This requires
  getting 2 SQEs from the ring in `submit_locked` (or a new
  `submit_linked` path in the reactor). The timeout SQE's `user_data`
  is a sentinel (not tracked in `callbacks_`); its CQE is discarded.
- `O_DIRECT` alignment: if `disk->is_o_direct()` and `size` is not
  aligned to `disk->block_size()`, call `on_complete(-EINVAL)`
  synchronously (no reactor submit).
- `PollingMode`: `Hybrid` by default, `Sqpoll` if configured.

Per-disk in-flight tracking:
```cpp
std::mutex mu_;
std::unordered_map<DiskId, std::unordered_set<uint64_t>> in_flight_;
```
The `on_complete` callback passed to the reactor is wrapped: before
invoking the user's callback, remove the op_id from
`in_flight_[disk_id]`.

Reactor method additions for cancel + linked timeout:
```cpp
// In crow::common::Reactor:
uint64_t submit_cancel(uint64_t target_op_id,
                       std::function<void(int)> on_complete);
// Submits IORING_OP_ASYNC_CANCEL targeting target_op_id's user_data.
// The cancel CQE's res is -ECANCELED (if canceled) or 0 (if already
// done). on_complete is invoked with the cancel CQE's res.

void submit_linked_write(int fd, const void *buf, size_t len,
                         off_t offset, uint32_t timeout_ms,
                         std::function<void(int)> on_complete);
// Gets 2 SQEs: I/O SQE + link_timeout SQE. The I/O SQE's user_data
// is the op_id; the timeout SQE's user_data is 0 (sentinel, not
// tracked). If the I/O completes within timeout_ms, the timeout is
// canceled by the kernel. If not, the kernel cancels the I/O and
// posts -ECANCELED + -ETIME.
```

**Edge cases:**
- `IORING_OP_ASYNC_CANCEL` on an already-completed I/O → CQE returns
  0 (not -ECANCELED). The `on_complete` is still invoked; the
  original I/O's callback already ran. Best-effort.
- Linked timeout fires → I/O CQE is `-ECANCELED`, timeout CQE is
  `-ETIME`. The I/O's `on_complete` receives `-ECANCELED`; the
  timeout's CQE is discarded (sentinel user_data).
- SQ too full for 2 SQEs (linked timeout) → `submit_locked` retry
  path tries to free slots; if still full after 4 attempts,
  `on_complete(-ENOMEM)` synchronously.
- `O_DIRECT` with unaligned buffer/size/offset → `-EINVAL`
  synchronously, no reactor submit.

## 5. BlockingEngine (work item 3)

### 5.1 Why

macOS has no io_uring. Non-liburing Linux builds (liburing not
available at CMake time) need a production path. A thread pool with
blocking `pwrite`/`pread` gives correct semantics with a thread hop
per I/O — lower performance but cross-platform.

### 5.2 How

New files:
- `app/crow-diskio/src/engine/blocking/blocking_engine.h`
- `app/crow-diskio/src/engine/blocking/blocking_engine.cpp`

```cpp
namespace crow::diskio {

class BlockingEngine : public IoEngine {
  public:
    explicit BlockingEngine(uint32_t thread_count = 4);
    ~BlockingEngine() override;

    void submit_write(Disk *disk, off_t phys_offset,
                      const uint8_t *data, size_t size,
                      std::function<void(int)> on_complete) override;
    void submit_read(Disk *disk, off_t phys_offset,
                     uint8_t *buf, size_t size,
                     std::function<void(int)> on_complete) override;
    void submit_fsync(Disk *disk,
                      std::function<void(int)> on_complete) override;
    void stop();

  private:
    void worker_loop();
    std::vector<std::thread> threads_;
    std::queue<Job> queue_;
    std::mutex mu_;
    std::condition_variable cv_;
    bool stopped_ = false;
};

} // namespace crow::diskio
```

`Job` is a struct carrying `{Disk*, off_t, const uint8_t*/uint8_t*,
size_t, IoOp (Write/Read/Fsync), on_complete}`.

a. `submit_write`: enqueue a `Job{disk, phys_offset, data, size,
   Write, on_complete}`. Notify `cv_`.
b. Worker thread: pop a `Job`, call `::pwrite(disk->fd(), data, size,
   phys_offset)`. If `ret < 0`, invoke `on_complete(-errno)`. If
   `ret < size` (partial write), invoke `on_complete(ret)` — the
   caller checks `ret != size` and treats it as `IoError::PartialWrite`.
   If `ret == size`, invoke `on_complete(ret)`.
c. `submit_read`: same, with `::pread`.
d. `submit_fsync`: worker calls `::fdatasync(disk->fd())` (or
   `::fsync` if configured). Invoke `on_complete(0)` on success,
   `on_complete(-errno)` on failure.
e. `on_complete` is invoked from the worker thread (not the reactor
   thread). The RPC handler's callback must be thread-safe — it calls
   `crow_rpc_server_submit_response`, which is thread-safe (enqueues
   to the connection's send queue).

**Edge cases:**
- Thread pool exhausted: jobs queue up in `queue_` (backpressure, not
  error). No job is dropped. The queue grows unbounded in v1 (no max
  queue size); a future config can add a max with backpressure error.
- `pwrite` partial write: `on_complete(ret)` with `ret < size`. The
  caller (RPC handler) checks and returns `IoError::PartialWrite`.
- `stop()`: sets `stopped_ = true`, notifies all workers, joins
  threads. Pending jobs are abandoned (their `on_complete` is never
  invoked) — acceptable at shutdown.

## 6. DummyEngine + MemDisk (work item 4)

### 6.1 Why

Throughput benches need to measure the full RPC + engine path without
real disk capacity limits. `MemDisk` receives writes and drops them
(no storage); reads return deterministic content from a cached
pattern buffer. This lets benches measure RPC framing + engine
overhead at memory speed, with read-back integrity checks.

### 6.2 How

New files:
- `app/crow-diskio/src/engine/dummy/dummy_io_engine.h`
- `app/crow-diskio/src/engine/dummy/dummy_io_engine.cpp`
- `app/crow-diskio/src/disk/mem_disk.h`
- `app/crow-diskio/src/disk/mem_disk.cpp`

`MemDisk`:
```cpp
class MemDisk : public Disk {
  public:
    MemDisk(DiskId id, std::vector<Zone> zones, size_t max_read_size);

    // Read: memcpy from pattern_buf_ with wrap-around.
    // Write: drop (no-op), return success.
    int read(off_t phys_offset, uint8_t *buf, size_t size,
             std::optional<uint64_t> logical_object_offset);
    int write(off_t phys_offset, const uint8_t *data, size_t size);

  private:
    void generate_pattern(uint64_t seed);
    std::vector<uint8_t> pattern_buf_;  // size = 2 * max_read_size
    size_t pattern_len_;
};
```

a. `generate_pattern(seed)`: fills `pattern_buf_` with a deterministic
   PRNG (e.g., xorshift64) seeded by `seed`. The seed is
   `hash(disk_id, zone_index)` mixed with `logical_object_offset` when
   present, otherwise `hash(disk_id, zone_index)` alone.
b. `read(phys_offset, buf, size, logical_object_offset)`:
   - Compute the effective seed: if `logical_object_offset` is
     present, `seed = hash(disk_id, zone_index) ^
     hash(logical_object_offset)`; else `seed = hash(disk_id,
     zone_index)`.
   - If the pattern was generated with a different seed, regenerate.
     (In practice, the pattern is generated once per disk and the
     `logical_object_offset` is mixed into the read offset, not the
     seed — see below.)
   - Simpler approach: generate the pattern once with
     `seed = hash(disk_id, zone_index)`. For reads with
     `logical_object_offset`, XOR the offset into the pattern index:
     `idx = (phys_offset + logical_object_offset * stride) %
     pattern_len_`. This gives different content for different logical
     objects at the same physical offset without regenerating.
   - `memcpy(buf, pattern_buf_.data() + idx, size)` with wrap-around
     if `idx + size > pattern_len_`.
   - Return `size` (success).
c. `write(phys_offset, data, size)`: no-op, return `size` (success).

`DummyEngine`:
```cpp
class DummyEngine : public IoEngine {
  public:
    void submit_write(Disk *disk, off_t, const uint8_t *, size_t,
                      std::function<void(int)> on_complete) override {
        on_complete(size);  // immediate success
    }
    void submit_read(Disk *disk, off_t phys_offset, uint8_t *buf,
                     size_t size,
                     std::function<void(int)> on_complete) override {
        auto *mem = static_cast<MemDisk *>(disk);
        int ret = mem->read(phys_offset, buf, size, logical_offset_);
        on_complete(ret);
    }
    // ...
};
```

**Edge cases:**
- Read with `logical_object_offset` but `MemDisk` was created without
  it: the offset is mixed into the pattern index, producing different
  content. No error.
- Read size > `pattern_len_`: wrap-around handles it (multiple copies
  of the pattern). `pattern_len_ = 2 * max_read_size` ensures at most
  one wrap for any valid read.
- Two reads of the same range + same `logical_object_offset`:
  identical bytes (deterministic).

## 7. SimulatedEngine + SimulatedDisk (work item 5)

### 7.1 Why

Fault-injection tests need to exercise retry/error paths in chunk
writers and recovery without real hardware faults. `SimulatedDisk`
wraps a real or mem disk and injects per-I/O latency and errors per
the disk's properties.

### 7.2 How

New files:
- `app/crow-diskio/src/engine/simulated/simulated_io_engine.h`
- `app/crow-diskio/src/engine/simulated/simulated_io_engine.cpp`
- `app/crow-diskio/src/disk/simulated_disk.h`
- `app/crow-diskio/src/disk/simulated_disk.cpp`

```cpp
struct DiskProperties {
    uint32_t latency_min_ms = 0;
    uint32_t latency_max_ms = 0;
    double error_rate = 0.0;  // 0.0 = no errors, 1.0 = all errors
};

class SimulatedDisk : public Disk {
  public:
    SimulatedDisk(std::shared_ptr<Disk> inner, DiskProperties props);
    // Delegates I/O to inner_, injects latency + errors per props.
  private:
    std::shared_ptr<Disk> inner_;
    DiskProperties props_;
    std::mt19937 rng_;
};
```

`SimulatedEngine` wraps another `IoEngine`:
```cpp
class SimulatedEngine : public IoEngine {
  public:
    SimulatedEngine(std::unique_ptr<IoEngine> inner);
    void submit_write(Disk *disk, off_t phys_offset,
                      const uint8_t *data, size_t size,
                      std::function<void(int)> on_complete) override;
    // ...
  private:
    std::unique_ptr<IoEngine> inner_;
};
```

a. `submit_write(disk, ...)`: if `disk` is a `SimulatedDisk`, draw a
   random latency from `[latency_min_ms, latency_max_ms]` (uniform).
   Draw a random double; if `< error_rate`, schedule
   `on_complete(-EIO)` after the latency delay. Otherwise, delegate
   to `inner_->submit_write(disk->inner(), ...)` with a wrapped
   callback that delays by the latency before invoking the original
   `on_complete`.
b. Latency injection: use a timer thread or `std::this_thread::
   sleep_for` in a separate thread (not the caller's thread). A simple
   approach: spawn a detached thread that sleeps for the latency then
   invokes `on_complete`. For high-IOPS tests, a timer wheel is
   better (avoid thread-per-I/O), but v1 uses the simple approach.
c. Error injection: `error_rate = 1.0` → every I/O returns `-EIO`.
   `error_rate = 0.5` → ~50% of I/Os return `-EIO`.

**Edge cases:**
- `latency_min_ms == latency_max_ms`: fixed latency (degenerate case).
- `error_rate = 0.0`: no errors, pure latency injection.
- `error_rate = 1.0`: all errors, no successful I/O.
- RNG is per-disk (seeded by `disk_id`) for reproducibility.

## 8. Disk Abstraction + DiskSet + Zone (work item 6)

### 8.1 Why

The RPC handler needs to resolve `disk_id` to a disk handle and
compute physical offsets from zone records. `DiskSet` holds the
node's disk map; `Disk` is the per-disk handle owning its `IoEngine`;
`Zone` provides the base offset for physical offset computation.

### 8.2 How

New files:
- `app/crow-diskio/src/disk/disk.h` — virtual base
- `app/crow-diskio/src/disk/disk.cpp`
- `app/crow-diskio/src/disk/block_disk.h` / `.cpp`
- `app/crow-diskio/src/disk/file_disk.h` / `.cpp`
- `app/crow-diskio/src/disk/mem_disk.h` / `.cpp` (from §6)
- `app/crow-diskio/src/disk/simulated_disk.h` / `.cpp` (from §7)
- `app/crow-diskio/src/disk/disk_set.h` / `.cpp`
- `app/crow-diskio/src/disk/zone.h` / `.cpp`

```cpp
namespace crow::diskio {

struct Zone {
    uint32_t zone_index;
    off_t base_offset;    // physical offset of zone start on disk
    int64_t capacity;
    // state is not tracked by diskio (see design doc §3.4)
};

class Disk {
  public:
    virtual ~Disk() = default;
    virtual DiskType type() const = 0;
    virtual int fd() const = 0;
    virtual bool is_o_direct() const = 0;
    virtual size_t block_size() const = 0;
    virtual IoEngine *engine() = 0;
    Zone *find_zone(uint32_t zone_index);
    DiskId id() const { return id_; }
  protected:
    DiskId id_;
    std::vector<Zone> zones_;
    std::unique_ptr<IoEngine> engine_;
};

class DiskSet {
  public:
    bool init(/* disk list from config or group-0 */);
    std::shared_ptr<Disk> find_disk(DiskId disk_id);
    void shutdown();
  private:
    std::unordered_map<DiskId, std::shared_ptr<Disk>> disk_map_;
};

} // namespace crow::diskio
```

`BlockDisk` (Linux):
- Opens block device with `O_DIRECT | O_RDWR`.
- `block_size()` = logical block size (from `BLKSSZGET` ioctl, or
  default 512).
- `engine()` = `UringEngine` (Linux with liburing) or
  `BlockingEngine` (non-liburing Linux).

`FileDisk` (all platforms):
- Opens regular file with `O_RDWR` (no `O_DIRECT` by default).
- `block_size()` = 1 (no alignment requirement for regular files).
- `engine()` = `BlockingEngine`.

`MemDisk`:
- No fd; `fd()` returns -1.
- `engine()` = `DummyEngine`.

`SimulatedDisk`:
- Wraps another `Disk`; delegates `fd()`, `block_size()`, etc.
- `engine()` = `SimulatedEngine` wrapping the inner disk's engine.

a. `DiskSet::init` reads the disk list from config or auto-discovers
   from group-0 via `HardwareClient`. For each disk, creates the
   appropriate `Disk` subclass, opens the device, creates zones from
   the zone records, and assigns an `IoEngine`.
b. `find_disk(disk_id)` returns the `Disk` or nullptr (→
   `IoError::DiskNotExist`).
c. `Disk::find_zone(zone_index)` returns the `Zone` or nullptr (→
   `IoError::ZoneNotExist`).

**Edge cases:**
- Disk open fails at startup → log error, skip disk, continue with
  others. The disk is not in `disk_map_`; I/O to it returns
  `IoError::DiskNotExist`.
- Zone not found → `IoError::ZoneNotExist` (the caller sent an
  invalid `zone_index`).
- diskio does not track disk status — if a disk's I/O starts failing,
  the engine returns errors; the top layer handles it.

## 9. RPC Service + msg-handler dispatch (work item 7)

### 9.1 Why

The diskio server receives RPC frames from the Rust client and
dispatches them to the `IoEngine`. The handler resolves the disk,
computes the physical offset, and submits the I/O. The completion
callback builds the response and submits it.

### 9.2 How

New files:
- `app/crow-diskio/src/rpc/dio_server_msg_handler.h` / `.cpp`
- `app/crow-diskio/src/rpc/msg_disk_write_request.h` / `.cpp`
- `app/crow-diskio/src/rpc/msg_disk_read_request.h` / `.cpp`
- `app/crow-diskio/src/rpc/msg_disk_fsync_request.h` / `.cpp`

The handler uses crow-rpc's `HandlerFn` signature:
```cpp
using HandlerFn = std::function<OutFrame *(Frame *request, Connection *conn)>;
```

For async handlers (I/O doesn't complete inline), the handler returns
`nullptr` and submits the response later via
`crow_rpc_server_submit_response`.

`msg_disk_write_request` handler:
a. Parse the flatbuffer control message: `DiskWriteRequest { disk_id,
   zone_index, zone_offset, size }`.
b. The data payload is in `frame->data_buf` (raw bytes, `size` bytes).
c. `disk = disk_set_->find_disk(disk_id)`. If null, build
   `DiskWriteResponse{ret_code=DiskNotExist}` and return it inline.
d. `zone = disk->find_zone(zone_index)`. If null, return
   `DiskWriteResponse{ret_code=ZoneNotExist}` inline.
e. `phys_offset = zone->base_offset + zone_offset`.
f. `engine = disk->engine()`.
g. `engine->submit_write(disk, phys_offset, frame->data_buf, size,
   [this, conn, request_id](int res) {
       if (res < 0) {
           build_and_submit_response(conn, request_id, -res);
       } else if ((size_t)res != size) {
           build_and_submit_response(conn, request_id, PartialWrite);
       } else {
           build_and_submit_response(conn, request_id, Success);
       }
   })`.
h. Return `nullptr` (async — response is submitted from the callback).
i. The `Frame*` ownership: the handler takes ownership of the frame
   (deletes it or transfers to the async context). The async context
   keeps the `data_buf` alive until the I/O completes; it's deleted
   in the completion callback.

`msg_disk_read_request` handler:
a. Parse `DiskReadRequest { disk_id, zone_index, zone_offset, size,
   logical_object_offset }`.
b. Resolve disk + zone + phys_offset (same as write).
c. Allocate a read buffer from the `BufferPool` (size bytes).
d. `engine->submit_read(disk, phys_offset, read_buf, size,
   [this, conn, request_id, read_buf, size](int res) {
       if (res < 0) {
           submit_error_response(conn, request_id, -res);
       } else {
           submit_read_response(conn, request_id, Success,
                                read_buf, res);
       }
   })`.
e. Return `nullptr` (async).
f. The read response's data payload is `read_buf` (passed as the
   `data` parameter to `crow_rpc_server_submit_response`).

`msg_disk_fsync_request` handler:
a. Parse `DiskFsyncRequest { disk_id }`.
b. Resolve disk.
c. `engine->submit_fsync(disk, [this, conn, request_id](int res) {
       build_and_submit_response(conn, request_id,
                                 res < 0 ? -res : Success);
   })`.
d. Return `nullptr` (async).

Message type registration:
```cpp
server.register_handler(DISKIO_MSG_WRITE_REQUEST,
                        handle_disk_write_request);
server.register_handler(DISKIO_MSG_READ_REQUEST,
                        handle_disk_read_request);
server.register_handler(DISKIO_MSG_FSYNC_REQUEST,
                        handle_disk_fsync_request);
```

Message type IDs (diskio range 3600s, defined in `diskio.fbs`):
- `DiskWriteRequest` = 3600
- `DiskWriteResponse` = 3601
- `DiskReadRequest` = 3602
- `DiskReadResponse` = 3603
- `DiskFsyncRequest` = 3604
- `DiskFsyncResponse` = 3605

**Edge cases:**
- Unknown `disk_id` → `DiskNotExist` response (inline, no async).
- Unknown `zone_index` → `ZoneNotExist` response (inline).
- `O_DIRECT` alignment violation → `InvalidAlignment` response
  (inline, the engine checks before submit).
- Connection drop during async I/O → the I/O still completes on the
  server; the response submit fails silently (connection is gone).
  The client treats the drop as a failure and retries.
- Frame ownership: the handler must delete the `Frame*` if returning
  inline. For async, the handler transfers ownership to the async
  context (stored in a `unique_ptr` captured by the completion
  callback).

## 10. Flatbuffer Schemas (work item 8)

### 10.1 Why

The control messages need a schema that's compact, zero-copy on read,
and schema-evolvable. Flatbuffers are crow-rpc's control message
format (see `design-crow-rpc.md` §2).

### 10.2 How

New file:
- `lib/crow-protocol/src/fbs/diskio.fbs`

```
include "common_type.fbs";

namespace crow.diskio.proto;

enum FBDiskIoRetCode : int16 {
    Success = 0,
    DiskNotExist = 1,
    ZoneNotExist = 2,
    IoError = 3,
    PartialWrite = 4,
    InvalidAlignment = 5,
    ConnectionError = 6,
}

table FBDiskWriteRequest {
    disk_id:[ubyte; 16];     // 128-bit DiskId
    zone_index:uint32;
    zone_offset:uint64;
    size:uint32;
}

table FBDiskWriteResponse {
    ret_code:FBDiskIoRetCode;
}

table FBDiskReadRequest {
    disk_id:[ubyte; 16];
    zone_index:uint32;
    zone_offset:uint64;
    size:uint32;
    logical_object_offset:uint64;  // optional, 0 = absent
}

table FBDiskReadResponse {
    ret_code:FBDiskIoRetCode;
    // data payload follows as raw bytes (data_size in RPC header)
}

table FBDiskFsyncRequest {
    disk_id:[ubyte; 16];
}

table FBDiskFsyncResponse {
    ret_code:FBDiskIoRetCode;
}
```

Message type IDs added to the diskio range in `msg_type.fbs` (or
defined in `diskio.fbs` as a separate enum, registered at startup).

CMake: add `diskio.fbs` to the flatbuffer schema build in
`lib/crow-protocol/CMakeLists.txt` (or the appropriate schema build
target). Generated headers: `diskio_generated.h`.

**Edge cases:**
- `logical_object_offset = 0` means absent (the field is always
  present in the flatbuffer but 0 is the sentinel for "not set"). The
  server treats 0 as "physical-offset-only pattern" for `MemDisk`.
- Schema evolution: new fields can be added with new field IDs
  (flatbuffers are forward/backward compatible by design).

## 11. Rust Client Library (work item 9)

### 11.1 Why

Rust callers (chunkdb writers, recovery, rebalance) need a typed
client that hides the crow-rpc-ffi details and provides
`Segment`-based addressing.

### 11.2 How

New crate:
- `lib/crow-diskio-client/Cargo.toml`
- `lib/crow-diskio-client/src/lib.rs`
- `lib/crow-diskio-client/src/client.rs`
- `lib/crow-diskio-client/src/error.rs`
- `lib/crow-diskio-client/src/proto.rs` (flatbuffer-generated Rust)

```rust
pub struct DiskIoClient {
    rpc: crow_rpc_ffi::RpcClient,
    // node_id → connection routing
    topology: TopologyCache,
}

impl DiskIoClient {
    pub async fn write(&self, segment: &Segment, data: Bytes)
        -> Result<(), IoError>
    {
        let node_addr = self.topology.resolve(segment.node_id)?;
        let ctrl = build_disk_write_request(segment);
        self.rpc.send(node_addr, DISKIO_MSG_WRITE_REQUEST,
                      &ctrl, &data).await?;
        // parse response, check ret_code
    }

    pub async fn read(&self, segment: &Segment,
                      logical_object_offset: Option<u64>)
        -> Result<Bytes, IoError>
    {
        let node_addr = self.topology.resolve(segment.node_id)?;
        let ctrl = build_disk_read_request(segment, logical_object_offset);
        let resp = self.rpc.send(node_addr, DISKIO_MSG_READ_REQUEST,
                                 &ctrl, &[]).await?;
        // resp.data is the read payload (zero-copy from frame decoder)
        Ok(resp.data)
    }

    pub async fn fsync(&self, disk_id: &DiskId)
        -> Result<(), IoError>
    {
        // ...
    }
}
```

`IoError` enum:
```rust
pub enum IoError {
    DiskNotExist,
    ZoneNotExist,
    Io,
    PartialWrite,
    InvalidAlignment,
    Connection,
    Timeout,
}
```

a. `topology.resolve(node_id)` looks up the node's diskio server
   address from the group-0 service registry (via
   `ServiceRegistryClient` in `crow-kv-client`). Cached locally;
   refreshed on connection error.
b. Connection pooling: `crow-rpc-ffi`'s `ConnectionPool` handles
   reuse. The client does not manage connections directly.
c. `build_disk_write_request(segment)` constructs the flatbuffer
   control message from `segment.disk_id`, `segment.zone_index`,
   `segment.zone_offset`, `segment.size`.
d. Response parsing: the control message is a `FBDiskWriteResponse`
   (or `FBDiskReadResponse`); check `ret_code`. For read, the data
   payload is `resp.data` (zero-copy from the frame decoder).

**Edge cases:**
- Connection error → `IoError::Connection`. The caller retries.
  Idempotent write to the same offset is safe for the same data.
- Timeout → `IoError::Timeout`. Same handling as connection error.
- `NotLeaderHint`-style routing: not applicable (diskio has no
  leader; routing is by `node_id`, which is static).

## 12. Configuration + Startup (work item 10)

### 12.1 Why

The server needs CLI args / config file for node ID, bind address,
disk list, engine selection, and per-disk properties. It registers
with group-0 on startup so other services can discover it.

### 12.2 How

New files:
- `app/crow-diskio/src/dio_main.cpp`
- `app/crow-diskio/src/dio_server.h` / `.cpp`
- `app/crow-diskio/src/dio_config.h` / `.cpp`
- `app/crow-diskio/CMakeLists.txt`

`DioConfig`:
```cpp
struct DioConfig {
    uint64_t node_id;
    std::string bind_address;
    int bind_port;
    // Disk list: explicit or auto-discover
    std::vector<DiskSpec> disks;  // empty = auto-discover from group-0
    // Engine
    std::string engine;  // "auto", "uring", "blocking", "dummy", "simulated"
    uint32_t thread_pool_size;  // blocking engine
    bool o_direct;
    std::string polling_mode;  // "wait", "hybrid", "sqpoll"
    uint32_t busy_poll_budget;
    uint32_t sq_thread_idle;
    uint32_t linked_timeout_ms;
    uint32_t sq_entries;
    // Group-0 connection
    std::string group0_address;
};
```

Startup sequence:
a. Parse CLI args / config file → `DioConfig`.
b. Connect to group-0 via `HardwareClient` (from `crow-kv-client`).
   If `disks` is empty, auto-discover the node's disk list from
   group-0.
c. Create `DiskSet`, init disks (open devices, create zones, assign
   engines).
d. Create `RpcServer`, register handlers (write/read/fsync).
e. `server.listen(bind_address, bind_port)`.
f. `server.start()`.
g. Register with group-0 service registry: write
   `/srv/diskio/<instance_id>` → `InstanceValue { alive: true }`.
   Other services use this for health detection.
h. Wait for shutdown signal.
i. On shutdown: `server.stop()`, `disk_set.shutdown()`, deregister
   from group-0.

CMake:
- `app/crow-diskio/CMakeLists.txt` — links `crowcommon` + `crow-rpc`
  + `crow-protocol` flatbuffer generated headers.
- Mirrors `app/crow-diskdb`'s `conf/` + `src/` + `tests/` layout
  (but CMake-built, not Cargo).

**Edge cases:**
- Group-0 unreachable at startup → retry with backoff (same pattern
  as `crow-kv-server` and `crow-diskdb`). If still unreachable after
  N attempts, exit with error.
- Disk open fails → log error, skip disk, continue with others.
- Port already in use → exit with error (no retry on bind).

## Scope

New files:
- `lib/crow-common/cpp/include/crow-common/reactor.h` — relocated reactor
- `lib/crow-common/cpp/src/reactor.cpp` — relocated reactor + polling
  modes + batched submit
- `app/crow-diskio/` — entire new C++ binary
  - `CMakeLists.txt` — build config
  - `src/dio_main.cpp` — entry point
  - `src/dio_server.{h,cpp}` — server wiring
  - `src/dio_config.{h,cpp}` — config
  - `src/engine/io_engine.h` — IoEngine virtual base
  - `src/engine/uring/uring_engine.{h,cpp}` — UringEngine
  - `src/engine/blocking/blocking_engine.{h,cpp}` — BlockingEngine
  - `src/engine/dummy/dummy_io_engine.{h,cpp}` — DummyEngine
  - `src/engine/simulated/simulated_io_engine.{h,cpp}` — SimulatedEngine
  - `src/disk/disk.{h,cpp}` — Disk virtual base
  - `src/disk/block_disk.{h,cpp}` — BlockDisk
  - `src/disk/file_disk.{h,cpp}` — FileDisk
  - `src/disk/mem_disk.{h,cpp}` — MemDisk
  - `src/disk/simulated_disk.{h,cpp}` — SimulatedDisk
  - `src/disk/disk_set.{h,cpp}` — DiskSet
  - `src/disk/zone.{h,cpp}` — Zone
  - `src/rpc/dio_server_msg_handler.{h,cpp}` — handler dispatch
  - `src/rpc/msg_disk_write_request.{h,cpp}` — write handler
  - `src/rpc/msg_disk_read_request.{h,cpp}` — read handler
  - `src/rpc/msg_disk_fsync_request.{h,cpp}` — fsync handler
  - `tests/` — ctest suite
- `lib/crow-diskio-client/` — Rust client crate
  - `Cargo.toml`
  - `src/lib.rs`
  - `src/client.rs`
  - `src/error.rs`
  - `src/proto.rs`
  - `tests/` — cargo test suite
- `lib/crow-protocol/src/fbs/diskio.fbs` — flatbuffer schemas

Modified files:
- `lib/crow-tree/CMakeLists.txt` — remove liburing find_path, link
  crow-common
- `lib/crow-common/cpp/CMakeLists.txt` — add liburing find_path,
  reactor source
- `lib/crow-tree/include/crow-tree/async_page_store.h` — include path
- `lib/crow-tree/include/crow-tree/block_page_store.h` — include path
- `lib/crow-tree/include/crow-tree/options.h` — include path
- `lib/crow-tree/include/crow-tree/crow-tree.h` — include path
- `lib/crow-tree/include/crow-tree/c_api.h` — include path
- `lib/crow-tree/src/block_async_page_store.cpp` — namespace
- `lib/crow-tree/src/persist.cpp` — namespace
- `lib/crow-tree/src/c_api.cpp` — namespace
- `lib/crow-tree/src/crow-tree.cpp` — namespace
- `lib/crow-tree/ffi/src/sys.rs` — FFI symbol resolution (if needed)
- `lib/crow-protocol/src/fbs/msg_type.fbs` — diskio message type IDs

Deleted files:
- `lib/crow-tree/include/crow-tree/reactor.h` — relocated
- `lib/crow-tree/src/reactor.cpp` — relocated

## Complexity

**High.** The reactor relocation (1a) touches `crow-tree` across ~10
files and must not regress `test-tree-ct` / `test-tree-ffi`. The
polling modes (1b) and batched submission (1c) modify the reactor's
core loop — correctness-critical, subtle interaction between
busy-poll and event-wait. The `UringEngine` (2) with linked timeouts
and per-disk cancellation is new io_uring code with no existing
reference in CROW. The RPC handler async-completion pattern (9) is
new — crow-rpc's existing handlers are synchronous (ping, echo); the
diskio handlers return `nullptr` and submit responses from callbacks,
which is a new usage pattern. The Rust client (11) follows existing
patterns (`crow-diskdb-client`) and is Low complexity. The
`DummyEngine`/`SimulatedEngine` (6, 7) are Medium — straightforward
but need careful latency injection without blocking the caller's
thread.

## Test Design

### Unit tests (UT)

**Reactor lift (1a)**:
- `crow::common::Reactor` builds under `CROW_HAVE_LIBURING` on Linux;
  absent on macOS. UT.
- `submit_read` + `submit_write` round-trip via the relocated
  reactor: write 4 KB to a tmpfile, read it back, verify bytes. UT
  (Linux only).

**Polling modes (1b)**:
- `Hybrid` with `busy_poll_budget=N`: under sustained I/O (100
  concurrent writes), the reactor stays in busy-poll phase (CQEs
  dispatched with no `wait_cqe` syscall); after I/O stops, transitions
  to event-wait within `N` empty peeks. Verified by a counter tracking
  busy-poll vs wait-mode iterations. UT (Linux only).
- `Sqpoll` with `sq_thread_idle=N`: submit syscalls eliminated
  (verified by `strace` count = 0 during sustained I/O); after `N` ms
  idle, kernel SQ thread sleeps, reactor wakes it with one
  `io_uring_enter(IORING_ENTER_SQ_WAKEUP)`. UT (Linux only, requires
  root).

**Batched submission (1c)**:
- 100 SQEs submitted in one `io_uring_submit()` call (not 100 calls)
  — verified by syscall counting. UT (Linux only).

**UringEngine (2)**:
- `UringEngine::write` with 1 MB aligned data to `O_DIRECT`
  `BlockDisk` → data written at correct offset, verified by `pread`.
  UT (Linux only).
- `UringEngine::read` of 1 MB from known offset → correct bytes. UT
  (Linux only).
- `UringEngine::fsync` after write → re-read after process restart
  returns written data. UT (Linux only).
- Per-disk in-flight tracking: after submitting I/O to 3 disks,
  `in_flight(disk_id)` returns correct count per disk; after
  completion, count decrements. UT (Linux only).
- `IORING_OP_ASYNC_CANCEL` on in-flight I/O → CQE returns
  `-ECANCELED` (or completes normally if already done). UT (Linux
  only).
- Linked timeout: I/O to a slow disk with 100ms linked timeout → I/O
  cancelled at ~100ms, returns `-ECANCELED`; other I/O on same ring
  unaffected. UT (Linux only).

**BlockingEngine (3)**:
- `BlockingEngine::write` + `read` round-trip with 1 MB on `FileDisk`
  → data integrity verified. UT (all platforms).
- `BlockingEngine::fsync` after write → durability verified by
  re-read. UT (all platforms).
- Thread pool size 4, 100 concurrent writes → all complete without
  deadlock; work queue backs up, not errors. UT (all platforms).

**DummyEngine + MemDisk (4)**:
- `MemDisk` write of 1 MB → dropped (no storage); immediate success.
  UT.
- `MemDisk` read of 1 MB with `logical_object_offset` → returns
  deterministic content from cached pattern buffer (seed mixed with
  logical offset); read-back integrity verified by regenerating same
  pattern. UT.
- `MemDisk` read without `logical_object_offset` → returns
  physical-offset-only pattern. UT.
- Two reads of same range → identical bytes. UT.
- `MemDisk` read of 2 MB (max read size) with 4 MB cached buffer →
  served via wrap-around `memcpy`; no per-read generation cost
  (verified by timing: memcpy-bound, not generation-bound). UT.
- `MemDisk` read at offset beyond `pattern_len` → wrap-around
  (`offset % pattern_len`) produces correct content. UT.

**SimulatedEngine + SimulatedDisk (5)**:
- `SimulatedDisk` with `error_rate=1.0` → every I/O returns
  `IoError::Io`. UT.
- `SimulatedDisk` with `error_rate=0.0` + latency 5-15 ms → each I/O
  succeeds after random delay within [5, 15] ms. UT.
- `SimulatedDisk` with `error_rate=0.5` over 1000 I/Os → ~500 errors
  (within tolerance); distribution is random. UT.
- `SimulatedDisk` with `latency_min_ms = latency_max_ms = 10` →
  fixed 10 ms latency (degenerate case). UT.

**Disk types (6)**:
- `BlockDisk` opens block device with `O_DIRECT | O_RDWR` → aligned
  write succeeds. UT (Linux only).
- `FileDisk` opens regular file → `pwrite` at offset succeeds on all
  platforms. UT.
- `DiskSet::find_disk(disk_id)` → correct `Disk`; unknown `disk_id`
  → `IoError::DiskNotExist`. UT.

**Alignment**:
- Write with unaligned size (100 bytes) with `O_DIRECT` →
  `IoError::InvalidAlignment`. UT.
- Write with aligned size (4096 bytes) with `O_DIRECT` → success. UT.

**Partial write**:
- `UringEngine::write` that returns fewer bytes than requested
  (simulated via `SimulatedDisk` short-write injection) → engine
  returns `IoError::PartialWrite` immediately, no internal retry. UT.

**Flatbuffer schemas (8)**:
- `FBDiskWriteRequest`/`FBDiskReadRequest`/`FBDiskFsyncRequest`
  round-trip encode/decode preserves all fields. UT.
- Message type IDs registered in crow-rpc's `msg_type` enum (diskio
  range 3600s). UT.

### End-to-end tests (E2E)

**RPC service (7)**:
- `DiskIoClient::write(segment, data)` → server receives correct
  control message + data payload, writes to disk, returns success.
  E2E (local diskio server + Rust client).
- `DiskIoClient::read(segment, None)` → returns correct bytes as
  `Bytes` (zero-copy from frame decoder). E2E.
- `DiskIoClient::read(segment, Some(logical_offset))` → mem-disk
  returns rule-based content incorporating logical offset. E2E.
- `DiskIoClient::fsync(disk_id)` → flushes disk, returns success.
  E2E.
- Write to a disk that returns I/O errors (simulated via
  `SimulatedDisk` with `error_rate=1.0`) → `IoError::Io` returned to
  caller. E2E.

**Zone offset computation**:
- `DiskWriteRequest` with `{zone_index=2, zone_offset=4096}` →
  physical offset = `zone_base[2] + 4096`. Verified by reading the
  zone record and checking the written offset. E2E.

**Client (9)**:
- `DiskIoClient` routes to correct node's diskio server based on
  `segment.node_id`. E2E.
- Connection error → client treats as failure (result unknown);
  retry succeeds. E2E.

**Configuration + startup (10)**:
- diskio server registers with group-0 service registry on startup,
  reporting service is alive; other services use this for health
  detection. E2E.
- diskio server auto-discovers disk list from group-0 when no
  explicit disk list is configured. E2E.

**Node restart**:
- Node restart → all disk handles re-opened at startup; in-flight I/O
  from before restart is lost; client retry handles it. E2E.

**SQ full backpressure (Linux)**:
- Tiny-SQ reactor (`ring_entries=4`) + `SimulatedDisk` with
  `latency_max_ms=5000`: submit 4 writes (fills SQ), then 5th write
  → `submit_locked` enters bounded retry; 5th write does not return
  `-ENOMEM` immediately, blocks until one of first 4 completes (~5s),
  then succeeds. E2E (Linux only).
- SQ full + good-disk isolation on shared ring (`ring_entries=8`):
  disk A `latency_max_ms=10000`, disk B `latency_max_ms=1`. Submit 8
  writes to disk A (fills SQ), then 1 write to disk B → disk B's
  write waits for SQ slot, succeeds when disk A's I/O completes (not
  rejected with `-ENOMEM`). E2E (Linux only).
- SQ full + explicit cancellation frees slots: same setup, mark disk
  A bad → `UringEngine` submits `IORING_OP_ASYNC_CANCEL` for all 8
  in-flight I/Os → SQ slots freed → disk B's write completes within
  ~100ms (not ~10s). E2E (Linux only).

**Reactor topology (Linux)**:
- Two `BlockDisk`s on one shared `Reactor` (HDD topology): disk A's
  I/O is slow (simulated), disk B's I/O completes normally → disk B's
  CQEs dispatched without waiting for disk A. E2E (Linux only).
- Per-disk reactor (NVMe topology): one disk's full SQ does not block
  another disk's submits (separate rings). E2E (Linux only).
- `IORING_SETUP_ATTACH_WQ`: two rings with `ATTACH_WQ` share one
  io-wq pool (verified by `/proc/<pid>/task` showing fewer kernel
  io-wq threads than `2 × 8`). E2E (Linux 5.18+ only).

**Reactor relocation regression**:
- All existing crow-tree tests pass unchanged in `Wait` mode
  (`pixi run test-tree-ct`, `pixi run test-tree-ffi`). E2E.
- `crow-tree-ffi`'s `ct_reactor_eventfd` binding resolves relocated
  symbol; `test-tree-ffi` async get/flush/snapshot tests pass. E2E.

## Module Structure

```
app/crow-diskio/
├── CMakeLists.txt
├── conf/
│   └── dio.conf.example
├── src/
│   ├── dio_main.cpp              # entry point: parse config, start server
│   ├── dio_server.{h,cpp}        # server wiring: RpcServer + DiskSet + handlers
│   ├── dio_config.{h,cpp}        # DioConfig struct + CLI/config parsing
│   ├── engine/
│   │   ├── io_engine.h           # IoEngine virtual base
│   │   ├── uring/
│   │   │   └── uring_engine.{h,cpp}  # UringEngine (Linux, CROW_HAVE_LIBURING)
│   │   ├── blocking/
│   │   │   └── blocking_engine.{h,cpp}  # BlockingEngine (macOS + non-liburing)
│   │   ├── dummy/
│   │   │   └── dummy_io_engine.{h,cpp}  # DummyEngine (bench path)
│   │   └── simulated/
│   │       └── simulated_io_engine.{h,cpp}  # SimulatedEngine (fault injection)
│   ├── disk/
│   │   ├── disk.{h,cpp}          # Disk virtual base
│   │   ├── block_disk.{h,cpp}    # BlockDisk (O_DIRECT block device)
│   │   ├── file_disk.{h,cpp}     # FileDisk (regular file)
│   │   ├── mem_disk.{h,cpp}      # MemDisk (drop-write + rule-based read)
│   │   ├── simulated_disk.{h,cpp}  # SimulatedDisk (wrap + fault properties)
│   │   ├── disk_set.{h,cpp}      # DiskSet (HashMap<DiskId, shared_ptr<Disk>>)
│   │   └── zone.{h,cpp}          # Zone (zone_index, base_offset, capacity)
│   └── rpc/
│       ├── dio_server_msg_handler.{h,cpp}  # handler dispatch
│       ├── msg_disk_write_request.{h,cpp}  # write handler + async completion
│       ├── msg_disk_read_request.{h,cpp}   # read handler + async completion
│       └── msg_disk_fsync_request.{h,cpp}  # fsync handler + async completion
└── tests/
    └── ...                       # ctest suite

lib/crow-diskio-client/
├── Cargo.toml
├── src/
│   ├── lib.rs                    # crate root, re-exports
│   ├── client.rs                 # DiskIoClient (write/read/fsync)
│   ├── error.rs                  # IoError enum
│   └── proto.rs                  # flatbuffer-generated Rust
└── tests/
    └── ...                       # cargo test suite

lib/crow-common/cpp/
├── include/crow-common/
│   └── reactor.h                 # relocated from crow-tree (1a)
└── src/
    └── reactor.cpp               # relocated + polling modes (1b) + batched (1c)

lib/crow-protocol/src/fbs/
└── diskio.fbs                    # flatbuffer schemas for diskio messages
```

## Config Extensions

New config fields (in `DioConfig`, `app/crow-diskio/src/dio_config.h`):

- `node_id` (u64, required) — the node's ID from group-0.
- `bind_address` (string, default "0.0.0.0") — crow-rpc listen
  address.
- `bind_port` (int, default 0 = ephemeral) — crow-rpc listen port.
- `disks` (list, default empty = auto-discover) — explicit disk list.
- `engine` (string, default "auto") — engine selection.
- `thread_pool_size` (uint32, default 4) — blocking engine threads
  per disk.
- `o_direct` (bool, default true) — `O_DIRECT` for `BlockDisk`.
- `polling_mode` (string, default "hybrid") — reactor polling mode.
- `busy_poll_budget` (uint32, default 16) — Hybrid mode empty-peek
  threshold.
- `sq_thread_idle` (uint32, default 1000) — Sqpoll mode idle timeout
  (ms).
- `linked_timeout_ms` (uint32, default 30000) — per-I/O linked
  timeout.
- `sq_entries` (uint32, default 256) — SQ ring size.
- `group0_address` (string, required) — group-0 connection address.

`validate()`:
- `node_id > 0`.
- `engine` ∈ {auto, uring, blocking, dummy, simulated}.
- `polling_mode` ∈ {wait, hybrid, sqpoll}.
- `thread_pool_size > 0`.
- `linked_timeout_ms > 0`.
- `sq_entries >= 64`.
- If `engine == uring` and not `CROW_HAVE_LIBURING` → config error.
- If `polling_mode == sqpoll` → warn that root/CAP_SYS_NOPRIV is
  required.

## Server Wiring

Startup sequence (matching `dio_main.cpp`):

1. Parse CLI args / config file → `DioConfig`. Validate.
2. Connect to group-0 via `HardwareClient` (from `crow-kv-client`).
   Retry with backoff if unreachable.
3. If `disks` is empty, auto-discover the node's disk list from
   group-0.
4. Create `DiskSet`, init disks (open devices, create zones from
   diskdb zone records, assign `IoEngine` per disk type + platform).
5. Create `RpcServer` (C++), register handlers:
   - `DISKIO_MSG_WRITE_REQUEST` → `handle_disk_write_request`
   - `DISKIO_MSG_READ_REQUEST` → `handle_disk_read_request`
   - `DISKIO_MSG_FSYNC_REQUEST` → `handle_disk_fsync_request`
6. `server.listen(config.bind_address, config.bind_port)`.
7. `server.start()` — spawns worker threads + acceptor thread.
8. Register with group-0 service registry: write
   `/srv/diskio/<instance_id>` → `InstanceValue { alive: true }`.
9. Wait for SIGTERM/SIGINT.
10. On shutdown: deregister from group-0, `server.stop()`,
    `disk_set.shutdown()` (closes all disk handles, stops engines).

## Open Questions

None remaining — all questions resolved (see backlog doc R105
Resolved decisions).
