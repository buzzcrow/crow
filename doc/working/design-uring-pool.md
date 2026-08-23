<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# DiskIOUring: Multi-Pipeline io_uring Engine (R109)

This draft covers the redesign of the shared io_uring layer in
`crow-common`: replacing `Reactor` (a single-ring wrapper with a
misleading name) with `DiskIOUring` — a multi-pipeline io_uring engine
that maps fds to pipelines, shares polling threads across CQs, batches
SQE submission, and is fully lock-free on the hot path. Both diskio
and btree use `DiskIOUring` as their I/O interface.

Companion backlog doc: `doc/backlog/R109-common-diskio-uring.md`. Root
design doc:
- `doc/design/diskio/design-crow-diskio.md` §7 (Reactor), §7.3 (Reactor
  Topology), §7.4 (Bad-Disk SQ Isolation) — architecture and rationale.

Already landed:
- `crow::common::Reactor` — single io_uring instance, lock-free SQE
  claim (atomic shadow tail + per-slot ready flags), callback-in-
  user_data, deferred-delete free list, three polling modes
  (Classic/Hybrid/Sqpoll). Shared by diskio's `UringEngine` and btree's
  `BlockAsyncPageStore`.
- `UringEngine` — diskio adapter: composes one `Reactor`, adds 16-shard
  per-`DiskId` in-flight tracking (mutex-guarded) for `cancel_disk`.
- `BlockAsyncPageStore` — btree adapter: borrows one `Reactor*`, maps
  global byte offsets to per-extent fds via `fd_for_offset`.
- FFI: `ct_reactor_eventfd` + `EventfdPump` in
  `lib/crow-tree/ffi/src/reactor.rs` — one pump for one reactor's
  eventfd, fanning out to `Arc<Notify>` for all waiting
  `drive_ct_future` calls.

Architecture decisions and rationale are in the root design
(`design-crow-diskio.md` §7); this doc does not repeat them.

---

## 1. Naming: `Reactor` → `DiskIOUring`

### 1.1 Why

The name `Reactor` implies a generic event-reactor pattern (register
fds, wait for readiness, dispatch). The class is none of that — it
submits read/write/fsync SQEs and drains CQEs. It is an io_uring I/O
engine, not a reactor. The new design is not just a rename: it is a
multi-pipeline engine with fd routing, shared polling threads, and
batch submit. `DiskIOUring` says exactly what it is: a disk I/O engine
backed by io_uring. btree page I/O is also disk I/O (reading/writing
pages from/to block devices), so the name fits both consumers.

### 1.2 What `DiskIOUring` Replaces

`DiskIOUring` replaces both `Reactor` (the single-ring wrapper) and the
proposed `UringPool<Key>` (the key-routed pool from the prior draft).
The key insight from the prior draft's review: the natural routing key
is the **fd** — the caller already has it, and io_uring's cancel-by-fd
(`IORING_ASYNC_CANCEL_FD`, available in the system's liburing headers)
eliminates the need for per-key in-flight tracking on modern kernels.
No key template, no per-key map, no mutex-guarded shards.

---

## 2. Architecture Overview

```
                    DiskIOUring (the interface)
                    ┌─────────────────────────────────────────────┐
                    │  fd_table[fd] → pipeline_index (read-only)   │
                    │                                             │
submit_read(fd) ──► │  Pipeline 0        Pipeline 1        ...     │
submit_write(fd) ─► │  ┌──────────┐     ┌──────────┐              │
submit_fsync(fd) ─► │  │ io_uring │     │ io_uring │              │
                    │  │ SQ + CQ  │     │ SQ + CQ  │              │
cancel_fd(fd) ────► │  └────┬─────┘     └────┬─────┘              │
                    │       │                │                     │
                    │  ┌────┴────────────────┘                     │
                    │  │  PollThread 0 (busy-poll or epoll-wait)   │
                    │  │  drains CQ-0 + CQ-1, submits pending SQEs │
                    │  └───────────────────────────────────────────┘
                    └─────────────────────────────────────────────┘
```

Components:
- **Pipeline** — one `io_uring` instance: one SQ + one CQ in shared
  memory. The low-level uring wrapper (lock-free SQE claim, CQE drain).
  Internal to `DiskIOUring`; not exposed to callers.
- **PollThread** — one OS thread that polls a group of pipelines' CQs
  and submits their pending SQEs. Multiple CQs share one polling
  thread (no one-thread-per-CQ requirement). Configurable grouping.
- **fd_table** — direct-indexed array mapping fd → pipeline_index.
  Populated at `register_fd` time, read-only during I/O. No
  synchronization needed (concurrent reads of an immutable array).
- **CallbackEntry** — allocated per submit, freed after CQE dispatch
  via deferred-delete (polling-thread-only). Holds the callback; no
  atomics, no cancel flags, no hazard pointers. The polling thread is
  the sole toucher after submit — no race, no synchronization.
  Callback suppression is client-side (§7.1): the caller wraps the
  callback with a shared cancel flag, not a `DiskIOUring` API.

---

## 3. `DiskIOUring` Interface

### 3.1 API

```cpp
class DiskIOUring
{
  public:
    struct PipelineConfig
    {
        unsigned      entries = 256;
        PollingMode   mode    = PollingMode::Classic;
        HybridConfig  hybrid{};
        SqpollConfig  sqpoll{};
    };
    struct PollThreadGroupConfig
    {
        std::vector<size_t> pipelines;  // which pipelines this thread polls
        int                 cpu = -1;   // -1 = no pinning; >=0 = pin to core
    };
    struct Topology
    {
        std::vector<PipelineConfig>         pipelines;
        // Which pipelines each poll thread handles + optional CPU pinning.
        // Default: one poll thread for all pipelines, no pinning.
        std::vector<PollThreadGroupConfig>  poll_thread_groups;
        bool                                attach_wq = false; // IORING_SETUP_ATTACH_WQ
    };

    explicit DiskIOUring(Topology topo);
    ~DiskIOUring();
    DiskIOUring(const DiskIOUring&)            = delete;
    DiskIOUring& operator=(const DiskIOUring&) = delete;

    // --- fd → pipeline registration ---
    // Called at disk/store open time. Two modes:
    //
    // register_fd(fd) — auto-assign: DiskIOUring picks the pipeline
    //   with the lowest in-flight count and sticks the fd to it.
    //   The caller does not need to know pipeline indices. Best for
    //   simple topologies (single pipeline, or uniform disks).
    //
    // register_fd(fd, pipeline_index) — explicit: caller pins the fd
    //   to a specific pipeline. Used when the caller built the Topology
    //   with disk-class-specific pipelines (e.g. NVMe disk → pipeline 0,
    //   HDD → pipeline 2) and wants deterministic routing.
    //
    // Both are read-only after registration — no synchronization on the
    // hot path. Returns the assigned pipeline index (for diagnostics).
    size_t register_fd(int fd);
    size_t register_fd(int fd, size_t pipeline_index);
    void   unregister_fd(int fd); // cancels in-flight, removes mapping

    // --- Submit (fd-based routing) ---
    // Looks up fd → pipeline, submits SQE on that pipeline (lock-free).
    // on_complete is invoked exactly once: asynchronously on CQE
    // completion, or synchronously with -errno on submit failure (SQ
    // exhausted after bounded retry, or invalid uring). No op_id is
    // returned — callback suppression is client-side (§7.1).
    void submit_read (int fd, void*       buf, size_t len, off_t off,
                      std::function<void(int)> on_complete);
    void submit_write(int fd, const void* buf, size_t len, off_t off,
                      std::function<void(int)> on_complete);
    void submit_fsync(int fd, std::function<void(int)> on_complete);

    // --- Cancel ---
    // Cancel all in-flight ops for an fd: IORING_ASYNC_CANCEL_FD (kernel
    // 6.0+). One SQE, kernel cancels all matching. Fallback for older
    // kernels: see §7.2.
    void cancel_fd(int fd);

    // --- Diagnostics ---
    size_t in_flight_count(int fd); // atomic counter per fd

    // --- FFI: eventfds for tokio AsyncFd integration ---
    // Returns one eventfd per pipeline (for multi-pump FFI wiring).
    // Copies up to max eventfd numbers into out_fds; returns the count.
    // Caller allocates out_fds; fds are DiskIOUring-owned (no transfer).
    size_t eventfds(int* out_fds, size_t max);
};
```

### 3.2 fd → Pipeline Routing

`fd_table` is a `std::vector<size_t>` direct-indexed by fd number. fds
are small integers (typically < 1024). The vector is sized once at
construction to `ulimit -n` (queried via `sysconf(_SC_OPEN_MAX)`, capped
at a sane maximum like 4096) and **never grows** — no reallocation, no
data race on concurrent reads. Each slot defaults to `SIZE_MAX`
(sentinel: "not registered"). Looking up `fd_table[fd]` is a single
memory load — no hash, no collision, no atomic, no lock.

The array is written at `register_fd` / `unregister_fd` time and read on
every submit (hot path, multi-threaded). `register_fd` is called at
setup time, before any I/O is submitted on the fd — the write
happens-before any submit on that fd (publication via happens-before
from the registration call to the first submit call). `unregister_fd`
only clears the slot back to `SIZE_MAX` (no reallocation, no resize);
the caller contract is that no concurrent submit is in flight on the fd
being unregistered (`unregister_fd` cancels in-flight ops first via
`cancel_fd`, then waits for CQEs to drain before clearing the slot).
Concurrent submits on *other* fds are safe — they read different slots,
and the vector's memory is stable (never reallocates).

If an unregistered fd is submitted, `DiskIOUring` routes to pipeline 0
and logs a warning. This is a programming error, not a runtime
condition — the caller should always `register_fd` before submitting.

### 3.3 How Does the Caller Know `pipeline_index`?

Two cases:

**Auto-assignment (`register_fd(fd)` — no pipeline index):**

The caller does not need to know pipeline indices at all.
`DiskIOUring` picks the pipeline with the lowest in-flight count (via
the per-pipeline `pending_count_` atomic, §4) and sticks the fd to it.
This is the simple case — used by btree (single pipeline, or uniform
disks where any pipeline is fine). The caller just calls
`register_fd(fd)` for each fd it opens; `DiskIOUring` handles the rest.
The returned `pipeline_index` is for diagnostics/logging only.

Auto-assignment is sticky: once an fd is assigned to a pipeline, all
future submits for that fd go to the same pipeline. This is important
for io_uring semantics — an fd's in-flight SQEs are on one ring, and
`cancel_fd` (§7.2) cancels on the ring where the fd's SQEs were
submitted. If an fd could migrate between pipelines, cancel would need
to broadcast to all rings. Sticky assignment keeps cancel on one ring.

**Explicit assignment (`register_fd(fd, pipeline_index)`):**

Used when the caller built a disk-class-aware `Topology` and wants
deterministic routing — e.g. NVMe disks on separate pipelines (one per
disk for SQ/CQ isolation), HDDs sharing one pipeline. The caller knows
`pipeline_index` because **the caller built the Topology**:

a. The caller constructs `Topology{ .pipelines = {config0, config1,
   config2, ...} }`. The order of `pipelines` defines the index:
   `pipelines[0]` is pipeline 0, `pipelines[1]` is pipeline 1, etc.
b. The caller knows which disk goes to which pipeline because it
   classified the disks (NVMe/SATA/HDD) before building the Topology.
   For example: "NVMe disk A → pipeline 0, NVMe disk B → pipeline 1,
   HDD C → pipeline 2."
c. After opening each disk (getting its fd), the caller calls
   `register_fd(fd, pipeline_index)` with the index determined in (b).

The mapping from disk to pipeline_index is **caller-side knowledge** —
it is the caller's topology plan, not something `DiskIOUring` infers.
`DiskIOUring` just stores the fd→pipeline mapping and routes submits.
The caller's topology plan lives in the server startup code (diskio) or
the `ct_open` wiring (btree), not in `DiskIOUring`.

**When to use which:**
- **Single pipeline** (btree today, diskio with one disk class):
  `register_fd(fd)` — auto-assign, all fds go to pipeline 0.
- **Uniform multi-pipeline** (e.g. 4 NVMe disks, 4 pipelines, one per
  disk): `register_fd(fd)` — auto-assign distributes fds across
  pipelines by load. Or `register_fd(fd, i)` for explicit 1:1 mapping.
- **Mixed disk classes** (NVMe + HDD, different pipeline configs per
  class): `register_fd(fd, pipeline_index)` — explicit, because an
  NVMe fd must go to an Sqpoll pipeline and an HDD fd must go to a
  Hybrid pipeline. Auto-assign would put an HDD fd on an Sqpoll
  pipeline, which is wrong.

---

## 4. Pipeline (Internal)

A pipeline is one `io_uring` instance. It is an internal implementation
detail of `DiskIOUring`, not exposed to callers. Each pipeline has:

- `struct io_uring ring_` — the kernel ring (SQ + CQ in shared memory).
- `std::atomic<unsigned> sq_tail_` — shadow tail for lock-free SQE
  claiming (existing mechanism from `Reactor`).
- `std::unique_ptr<std::atomic<bool>[]> sqe_ready_` — per-slot ready
  flags (existing).
- `std::atomic<bool> pending_submit_` — set by client threads when an
  SQE is filled, checked and cleared by the polling thread.
- `std::atomic<size_t> pending_count_` — number of filled-but-unsubmitted
  SQEs (for batch threshold, §6).
- `int eventfd_` — registered with the ring via
  `IORING_REGISTER_EVENTFD` so CQE completions write to it. Used by the
  polling thread's epoll-wait and by FFI tokio pumps.
- `CallbackEntry* free_list_` — deferred-delete list (existing, polling-
  thread-only).

The lock-free SQE claim (`submit_lockfree` in current `reactor.cpp`
lines 87-135) is preserved unchanged: atomic CAS on `sq_tail_`, per-slot
ready flags, bounded retry on SQ full. No mutex.

### 4.1 PollThread (Internal)

A `PollThread` polls a group of pipelines' CQs and submits their pending
SQEs. Internal to `DiskIOUring`; not exposed to callers. Each poll thread
has:

- `std::vector<Pipeline*> pipelines_` — pointers to the assigned
  pipelines (set at construction, read-only during run).
- `int epoll_fd_` — epoll instance monitoring all assigned pipelines'
  eventfds (for event-wait mode).
- `std::atomic<bool> thread_sleeping_` — set `true` by the poll thread
  before entering `epoll_wait`, CAS'd to `false` by the first client
  that writes the eventfd wakeup (§6.4). Prevents redundant eventfd
  writes under burst load.
- `unsigned busy_poll_count_` — consecutive empty busy-poll iterations
  (poll-thread-only, no atomic). Reset to 0 when any CQE is dispatched
  on any assigned pipeline. Compared against `busy_poll_budget` (the
  minimum of all assigned Hybrid-mode pipelines'
  `HybridConfig::busy_poll_budget`) to decide when to transition from
  busy-poll to event-wait.
- `std::atomic<bool> stopped_` — set by `~DiskIOUring` to stop the loop.
- `std::thread thread_` — the OS thread running `run()`.

`busy_poll_count_` is per-poll-thread (one counter for all assigned
pipelines), not per-pipeline: the poll thread drains all assigned CQs in
one iteration, and the busy-poll ↔ event-wait transition is a thread-
level decision. When multiple Hybrid-mode pipelines share a thread, the
thread uses the **minimum** `busy_poll_budget` across its assigned
Hybrid pipelines — the most conservative budget wins, ensuring no
pipeline's budget is exceeded.

---

## 5. Polling Thread Groups

### 5.1 Multiple CQs Share One Polling Thread — Yes, This Is Correct

io_uring's CQ is a ring buffer in shared memory. `io_uring_peek_cqe`
reads the CQ tail pointer (a memory load, no syscall). One thread can
peek multiple CQs in a loop — this is correct and efficient. The
constraint is only on *waiting*: `io_uring_wait_cqe` blocks on one
ring's wait queue, so you can't wait on multiple rings simultaneously
with that API. The solution is eventfd + epoll:

- Each pipeline registers its eventfd via
  `IORING_REGISTER_EVENTFD`. When the kernel posts a CQE, it writes to
  the eventfd.
- The polling thread `epoll_wait`s on all assigned pipelines' eventfds.
  When any eventfd fires, the thread wakes, then peeks all assigned
  pipelines' CQs (no syscall — shared memory reads).

This gives one-thread-for-N-CQs with efficient wakeup. No busy-spinning
at idle (unlike naive multi-CQ polling).

### 5.2 Polling Loop

Each `PollThread` runs:

```
while (!stopped):
    // 0. Drain deferred-delete list from the previous iteration
    //    (existing mechanism — any concurrent client-side cancel flag
    //    store has completed by now).
    drain_free_list()

    // 1. Submit phase: flush pending SQEs on all assigned pipelines.
    for pipeline in assigned_pipelines:
        if pipeline.pending_submit.exchange(false):
            pipeline.publish_ready_sqes()  // updates ktail; calls
                                           // io_uring_enter in Classic/
                                           // Hybrid, or IORING_ENTER_
                                           // SQ_WAKEUP in Sqpoll if the
                                           // kernel SQ thread parked.

    // 2. CQ drain phase: peek all assigned CQs.
    any_cqe = false
    for pipeline in assigned_pipelines:
        while (cqe = io_uring_peek_cqe(pipeline.ring)):
            dispatch_callback(cqe)
            any_cqe = true

    // 3. Wait phase (mode-dependent).
    if any_cqe:
        busy_poll_count = 0
        continue                    // loop immediately (busy-poll)
    else:
        busy_poll_count++
        if busy_poll_count < budget:
            yield(); continue       // busy-poll: spin (no syscall)
        else:
            // Event-wait: sleep until an eventfd fires.
            thread_sleeping.store(true)       // §6.4: allow clients to
                                              // skip redundant eventfd writes
            epoll_wait(eventfds, timeout=50ms)
            thread_sleeping.store(false)      // clear after wakeup
            busy_poll_count = 0
```

The `thread_sleeping` set/clear protocol (§6.4) is integral to the loop:
the poll thread sets it `true` immediately before `epoll_wait` and clears
it `false` immediately after. A client that fills an SQE while the thread
is sleeping CASes `thread_sleeping` from `true` to `false` and writes the
eventfd (waking the thread); subsequent clients in the same burst see
`false` and skip the write. This ensures at most one eventfd write per
sleep cycle.

Key properties:
- **Busy-poll phase** (I/O active): zero syscalls. The thread loops,
  peeking all CQs (memory loads) and submitting pending SQEs (one
  `io_uring_enter` per pipeline with pending SQEs). Sub-µs CQE dispatch.
- **Event-wait phase** (idle): one `epoll_wait` syscall (monitors all
  assigned eventfds). Wakes on any CQE completion or submit wakeup.
- **Sqpoll mode**: the kernel's SQ poll thread submits SQEs without
  userspace `io_uring_enter` syscalls. The userspace polling thread
  still runs `publish_ready_sqes()` (updating `ktail` to publish filled
  SQE slots to the SQ ring), but the `io_uring_enter` call is only
  needed to wake a parked kernel SQ thread
  (`IORING_ENTER_SQ_WAKEUP` when `IORING_SQ_NEED_WAKEUP` is set). If the
  kernel SQ thread is active, no syscall at all — the kernel thread
  picks up the published SQEs directly. The userspace poll thread's
  primary job in Sqpoll mode is CQ drain.

### 5.3 Poll Thread Group Configuration

`Topology::poll_thread_groups` specifies which pipelines each thread
handles. Examples:

- **1 thread, all pipelines** (default, simple): `{ {0, 1, 2, ...} }`.
  One thread busy-polls all CQs. Best for low pipeline count (HDD farm,
  single-disk btree).
- **1 thread per pipeline** (max isolation): `{ {0}, {1}, {2}, ...} `.
  Each CQ has its own thread. Best for high-IOPS NVMe where CQ drain
  latency matters. This is the current `Reactor` model (one thread per
  ring).
- **Grouped by disk class** (balanced): `{ {0,1,2}, {3,4,5} }`. 2
  threads for 6 pipelines. Best for SATA SSD farms — medium IOPS,
  grouping reduces thread count while keeping CQ drain latency
  acceptable.
- **Grouped by NUMA node**: pipelines whose disks are on NUMA node 0
  → thread 0 (pinned to node 0 core); node 1 → thread 1. Avoids
  cross-node memory access on the CQ rings.

The polling thread can be CPU-pinned (`pthread_setaffinity_np`) per
group for latency-sensitive workloads.

### 5.4 `IORING_SETUP_ATTACH_WQ`

When `Topology::attach_wq` is true and there are ≥2 pipelines, the
first pipeline is constructed normally, and subsequent pipelines are
constructed with `IORING_SETUP_ATTACH_WQ` referencing the first
pipeline's `wq_id`. This shares the kernel io-wq pool across pipelines
(8 kernel threads total instead of N×8). For `O_DIRECT` on block
devices, I/O almost always completes inline and io-wq is rarely
involved; `attach_wq` matters most for non-`O_DIRECT` file I/O (WAL,
btree without `O_DIRECT`). Default: `false` (opt-in).

---

## 6. Batch Submit — When to Trigger

### 6.1 The Problem

`io_uring_submit` (or `io_uring_enter`) is a syscall that pushes filled
SQEs from the SQ ring to the kernel. Calling it per-SQE wastes syscalls
under high load. Batching — submitting N SQEs in one syscall — reduces
syscall overhead by N×.

### 6.2 Design: Client Fills, Polling Thread Submits

**Client threads** (calling `submit_read/write/fsync`):
a. Claim SQE slot — lock-free CAS on `sq_tail_` (existing, no mutex).
b. Fill SQE, set `sqe_ready_[idx] = true` (atomic store).
c. Set `pending_submit_ = true` (atomic store).
d. `pending_count_.fetch_add(1)` (relaxed atomic).
e. **No `io_uring_submit` call.** The client never calls
   `io_uring_enter`. It just fills the SQE and sets the flag.

**Polling thread** (each iteration, submit phase):
a. For each assigned pipeline: if `pending_submit_.exchange(false)` is
   true, call `publish_ready_sqes()` → one `io_uring_enter` that pushes
   all filled SQEs to the kernel.
b. This batches all SQEs filled since the last iteration into one
   syscall per pipeline.

### 6.3 When Does the Polling Thread Run the Submit Phase?

- **Busy-poll mode** (I/O active): every iteration (sub-µs interval).
  The thread is looping, checking `pending_submit_` each time. SQEs are
  submitted within <1µs of being filled. Zero extra syscalls from the
  client. This is the primary high-performance path.
- **Event-wait mode** (idle): the thread is sleeping in `epoll_wait`.
  It wakes on:
  - **CQE completion** — kernel writes to eventfd, `epoll_wait` returns.
    The thread runs the submit phase (submits any SQEs filled while it
    was asleep), then drains CQEs.
  - **Client submit wakeup** — after filling an SQE, if the polling
    thread may be sleeping, the client writes to the pipeline's
    eventfd. This wakes `epoll_wait`, and the thread runs the submit
    phase. One eventfd write (1 syscall) wakes the thread; the thread
    then submits all pending SQEs in one `io_uring_enter`. Total: 2
    syscalls for a batch of N SQEs.
  - **Timeout** — `epoll_wait` has a 50ms timeout as a safety net. If
    no eventfd fires, the thread wakes, submits any pending SQEs, and
    goes back to sleep.

### 6.4 Avoiding Redundant Eventfd Writes

If 1000 SQEs are submitted in 1µs (burst), we don't want 1000 eventfd
writes. The design uses an atomic `thread_sleeping_` flag per poll
thread (see §4.1 for the field, §5.2 for its placement in the loop):

a. Polling thread sets `thread_sleeping_ = true` before `epoll_wait`
   (step 3 of the loop in §5.2).
b. Client checks `thread_sleeping_` after filling SQE. If true, CAS it
   to false and write eventfd (wake the thread). If false (thread is
   already awake or waking), skip the eventfd write.
c. Polling thread clears `thread_sleeping_ = false` after `epoll_wait`
   returns (step 3 of the loop in §5.2).

This ensures at most one eventfd write per sleep cycle. Subsequent
clients in the same burst see `thread_sleeping_ == false` and skip the
write. The thread wakes once, submits all pending SQEs in one batch.

### 6.5 Summary: Syscall Cost

- **Busy-poll (active I/O):** 0 client syscalls. 1 `io_uring_enter` per
  pipeline per iteration (batches all pending SQEs). Sub-µs submit
  latency.
- **Event-wait (idle → first submit):** 1 eventfd write (client) + 1
  `io_uring_enter` (polling thread). 2 syscalls for the first batch.
  Subsequent submits in the same burst: 0 client syscalls (thread is
  awake, sees `pending_submit_`).
- **Per-SQE without batching (for comparison):** 1 `io_uring_enter` per
  SQE. N syscalls for N SQEs.

For N=32 SQEs in a burst: batching = 2 syscalls, unbatched = 32. **16×
reduction.**

---

## 7. Lock-Free Design

### 7.1 Hot Path — Zero Mutexes

The hot path (submit + CQE completion) uses no mutexes, no locks, no
blocking:

- **SQE claim**: atomic CAS on `sq_tail_` + per-slot `sqe_ready_` flags
  (existing, proven in `reactor.cpp` lines 87-135). Multi-threaded
  clients claim slots concurrently with no contention beyond CAS
  retries.
- **fd → pipeline lookup**: direct array index, read-only. No
  synchronization.
- **Batch submit coordination**: `pending_submit_` atomic flag,
  `pending_count_` atomic counter, `thread_sleeping_` atomic flag. All
  single-word atomics.
- **CQ drain**: polling thread only (single-threaded per CQ). No
  concurrency, no synchronization.
- **Callback dispatch**: polling thread only. `CallbackEntry` holds
  just the callback — no `cancelled`/`dispatched` atomics, no cancel
  flags. The dispatch path calls `entry->cb(res)` unconditionally; if
  the client wants to suppress the callback, it checks a shared cancel
  flag inside the callback itself (client-side cancel, below).
- **CallbackEntry lifetime**: deferred-delete on the polling thread
  (existing, `reactor.cpp` lines 254-258). The polling thread is the
  sole toucher of `CallbackEntry` after `submit_*` returns — no
  concurrent access, no hazard pointers, no generation counters. The
  free-list drain is a plain `delete` loop.
- **Callback suppression (client-side cancel)**: `DiskIOUring` does not
  provide `cancel(op_id)`. The caller wraps the callback with a shared
  cancel flag:
  ```cpp
  auto state = std::make_shared<OpState>();  // atomic<bool> cancelled
  uring.submit_read(fd, buf, len, off, [state](int res) {
      if (state->cancelled.load(std::memory_order_acquire)) return;
      // ... normal callback logic
  });
  // On timeout / teardown:
  state->cancelled.store(true, std::memory_order_release);
  ```
  The CQE always arrives, the callback always fires, the SQ slot is
  always freed — only the callback body is skipped via the flag. The
  `shared_ptr` keeps `OpState` alive until the CQE arrives, so there is
  no UAF. No `DiskIOUring`-side synchronization needed. Use cases:
  RPC timeout (callback chain torn down), btree cross-extent write
  teardown (`WriteState->cancelled`), double-completion prevention.

### 7.2 Cancel by fd — `IORING_ASYNC_CANCEL_FD`

`cancel_fd(fd)` cancels all in-flight I/O for an fd. The system's
liburing headers define `IORING_ASYNC_CANCEL_FD` (cancel by fd instead
of by user_data) and `IORING_ASYNC_CANCEL_ALL` (cancel all matching).
On kernel 6.0+:

a. `cancel_fd(fd)` submits one `IORING_OP_ASYNC_CANCEL` SQE with
   `flags = IORING_ASYNC_CANCEL_FD | IORING_ASYNC_CANCEL_ALL` and
   `addr = fd`.
b. The kernel cancels all in-flight ops on that fd and posts CQEs
   (`-ECANCELED` for each cancelled op, or the original result if
   already completed).
c. **No per-fd in-flight tracking needed.** The kernel does the lookup.
   Zero overhead on the hot path — no list, no counter, no per-op
   memory.

This is the primary cancel mechanism. It eliminates the 16-shard
mutex-guarded `HashMap<DiskId, HashSet<op_id>>` from `UringEngine`
entirely.

**SQ-full scenario (bad disk):** the motivation for `cancel_fd` is a
bad disk whose I/O is stuck. One concern: can the SQ ring be full of
stuck I/O, leaving no slot for the cancel SQE itself? No — the SQ ring's
capacity check is `tail - head >= entries`, where `head` is the kernel's
*consumed* SQ head (`io_uring_load_sq_head`). When the kernel picks up
an SQE from the SQ ring (to submit it to the block layer), it advances
`sq_head`, freeing that SQ ring slot. In-flight I/O on a bad disk is in
the block layer / device queue — the SQ ring slots it occupied are
already recycled. So the SQ ring has space for the cancel SQE even when
all of the fd's I/O is in-flight. The only failure mode is a transient
burst filling all slots before the polling thread publishes them
(handled by the existing 1000-retry-with-yield in `submit_lockfree`), or
a broken ring (`valid_ = false`, a fatal node-level error).

**Kernels < 6.0** (no `IORING_ASYNC_CANCEL_FD`): `cancel_fd(fd)` returns
`-ENOSYS` (runtime check via kernel-version probe or compile-time
`#ifdef` on the liburing header constant). The caller (diskio's
`cancel_disk`) handles the error: log a warning, fall back to waiting
for in-flight I/O to complete naturally (the bad-disk path already has a
timeout). No Treiber stack fallback, no per-fd mutex — see OQ1 for the
decision rationale. macOS never compiles `DiskIOUring` at all (io_uring
is Linux-only); `BlockingEngine` is the non-uring path.

### 7.3 Per-fd In-Flight Counter (Diagnostics)

`in_flight_count(fd)` maintains a per-fd `std::atomic<size_t>` counter:
- Increment on submit (relaxed atomic, client thread).
- Decrement on CQE dispatch (relaxed atomic, polling thread).
- Read for diagnostics (relaxed atomic, any thread).

Two relaxed atomics per I/O — negligible overhead. Used for
diagnostics and for load-aware fd→pipeline auto-assignment
(`register_fd(fd)` without explicit pipeline index — §3.3).

### 7.4 What About `io_uring_submit` Thread Safety?

`io_uring_submit` / `io_uring_enter` modifies the SQ ring's tail pointer
(shared memory with the kernel). In this design, **only the polling
thread calls `io_uring_enter`**. Client threads only fill SQE slots
(via `io_uring_get_sqe` equivalent — the lock-free CAS on `sq_tail_`)
and set the `pending_submit_` flag. Since `io_uring_enter` is
single-threaded (polling thread), there is no concurrent access to the
SQ tail pointer from `io_uring_enter`. The SQE slot filling (client
threads) and the SQ tail publishing (polling thread) are separated by
the `sqe_ready_` flags and the `sq_tail_` / `sqe_head_` shadow
mechanism (existing, `reactor.cpp` lines 137-166).

### 7.5 Kernel-Internal Locks (Not Controllable From Userspace)

The lock-free design above covers all **userspace** synchronization.
The kernel has its own internal locks on the I/O path; these are not
controllable from userspace, but the design's per-fd pipeline isolation
minimizes their impact. The full per-path analysis (lock scope, hold
duration, io_uring interaction) belongs in the root design doc
(`design-crow-diskio.md` §7); this section summarizes the key points
relevant to the implementation.

**Summary:**

| I/O path | Lock | Hold duration | Contention scope |
| --- | --- | --- | --- |
| SQE fill / CQE drain | None | — | — |
| `O_DIRECT` block device (diskio `BlockDisk`) | `blk-mq` per-HW-queue spinlock | µs (queue insertion, not I/O) | per-HW-queue |
| `O_DIRECT` regular file write (btree block files) | `i_rwsem` exclusive | **full I/O latency** (10µs-10ms) | per-inode |
| `O_DIRECT` regular file read | `i_rwsem` shared | full I/O latency | per-inode (concurrent reads OK) |
| `memfd`/tmpfs write (test path) | `i_rwsem` exclusive | sub-µs per op, O(N) under contention | per-inode |

**Key points:**
- io_uring never blocks the userspace submitter on `i_rwsem`. When an
  inline execution would block, io_uring punts to `io_wq` (kernel
  worker pool). The worker blocks on the semaphore; the CQE is delayed
  by the wait. io_uring also hashes regular file writes by inode in
  `io_wq`, serializing same-inode writes within the worker pool.
- The design's per-fd pipeline isolation aligns with the kernel's
  per-device / per-inode lock scope: if one fd's I/O contends on a
  kernel lock, it is on its own pipeline with its own polling thread —
  the contention does not block I/O on other fds/pipelines.
- All three consumers avoid same-inode write contention by design:
  btree uses one file per extent (different inodes), diskio writes to
  block devices (no `i_rwsem`), dummy disks use one memfd per disk
  (different inodes). The only unavoidable serialization is concurrent
  writes to the *same* file/inode, which affects any I/O system (only
  SPDK bypasses it — a non-goal for v1).
- **memfd benchmark caveat:** `i_rwsem` contention on a shared memfd
  can reach millisecond range under sustained concurrent writes — the
  same order of magnitude as real SSD I/O. Memfd benchmarks measure VFS
  lock contention, not uring performance. Use one memfd per disk
  (already the case) and document the bottleneck for single-disk
  concurrent-writer benchmarks.

---

## 8. diskio Integration

### 8.1 `UringEngine` Thins Out

`UringEngine` today owns a `Reactor` + 16-shard in-flight map + callback
wrapping + `cancel_disk` loop. All of that is eliminated — `DiskIOUring`
handles routing, tracking, and cancellation internally. What remains
diskio-specific: `Disk*` validation, `O_DIRECT` alignment checks.

```cpp
class UringEngine : public IoEngine
{
  public:
    // Engine owns the DiskIOUring (one engine per diskio server).
    explicit UringEngine(DiskIOUring::Topology topo);

    void submit_write(Disk* disk, off_t phys_offset, const uint8_t* data, size_t size,
                      std::function<void(int)> on_complete) override;
    // a. Validate disk != nullptr, disk->fd() >= 0 (→ -EBADF).
    // b. Validate O_DIRECT alignment if applicable (→ -EINVAL).
    // c. uring_.submit_write(disk->fd(), data, size, phys_offset, on_complete).

    void submit_read(Disk* disk, off_t phys_offset, uint8_t* buf, size_t size,
                     uint64_t test_pattern_offset, std::function<void(int)> on_complete) override;
    // Same validation → uring_.submit_read(disk->fd(), ...).

    void submit_fsync(Disk* disk, std::function<void(int)> on_complete) override;
    // Validate → uring_.submit_fsync(disk->fd(), ...).

    void cancel_disk(DiskId disk_id) override;
    // Looks up disk's fd via DiskSet, calls uring_.cancel_fd(fd).

    DiskIOUring& uring() { return uring_; }

  private:
    DiskIOUring uring_;
    DiskSet*    disks_; // for fd lookup in cancel_disk
};
```

Deleted from `UringEngine`: the 16-shard `InFlightShard` map, the
`shard()` helper, the callback wrapping lambda, the `cancel_disk` loop
body, `in_flight_count(DiskId)`. All absorbed by `DiskIOUring` (or
eliminated by `IORING_ASYNC_CANCEL_FD`).

**Breaking API change:** `UringEngine::in_flight_count(DiskId)` is
removed. It is a public method (`uring_engine.h` line 44) used by
`uring_engine_test.cpp`. Callers that need in-flight counts now use
`uring_.in_flight_count(fd)` (per-fd, not per-DiskId). The test is
updated in Scope. The `IoEngine` base class does not declare
`in_flight_count` (it was `UringEngine`-specific), so no virtual
interface breakage.

### 8.2 Server Wiring

At diskio server startup:
a. Load node disk list from group-0 sysdata (`HardwareClient`).
b. Populate `DiskSet` (open each disk, classify by type: NVMe / SATA /
   HDD).
c. Build `DiskIOUring::Topology` per §7.3 of the design doc:
   - NVMe: one pipeline per disk, `Sqpoll` mode, one poll thread per
     pipeline (max CQ drain latency isolation).
   - SATA SSD: one pipeline per 4-8 disks, `Hybrid` mode, one poll
     thread per group.
   - HDD: one shared pipeline for all HDDs, `Hybrid` mode, one poll
     thread. `entries = 2048`.
   - Mixed: union. `attach_wq = true` if ≥2 NVMe or ≥2 SATA pipelines.
d. Construct `UringEngine(topo)`.
e. For each disk: `engine.uring().register_fd(disk->fd(),
   pipeline_index)` — **explicit** assignment, because the pipeline
   config (Sqpoll for NVMe, Hybrid for HDD) must match the disk class.
   The `pipeline_index` is determined by the Topology construction in
   (c): the caller knows which pipeline index corresponds to which disk
   class because it built the `pipelines` vector in that order. See
   §3.3 for how the caller knows `pipeline_index`.
f. Wire `IoEngine*` into the RPC handler (unchanged — handler is
   engine-agnostic).

Edge cases:
- `DummyDiskEngine` wraps `UringEngine` for read-content hack + fault
  injection. It delegates `submit_*` to the inner engine, so it is
  unaffected — the inner engine now delegates to `DiskIOUring`, and the
  wrapper's post-read buffer overwrite happens in its own callback layer
  above.
- A node with zero disks: empty topology, zero pipelines. The engine's
  `submit_*` invokes `on_complete(-ENOENT)` synchronously. The server
  refuses to start (existing behavior).

---

## 9. btree Integration

### 9.1 `BlockAsyncPageStore` Takes `DiskIOUring*`

Today `BlockAsyncPageStore` borrows one `Reactor*`
(`async_page_store.h` line 67). After:

```cpp
class BlockAsyncPageStore : public AsyncPageStore
{
  public:
    // uring is non-owning; caller must keep it alive.
    BlockAsyncPageStore(BlockPageStore* store, DiskIOUring* uring);

    uint64_t submit_read(PageAddr addr, void* buf, size_t len,
                         std::function<void(Status)> on_complete) override;
    // a. fd = store_->fd_for_offset(addr, &local); validate fd >= 0.
    // b. uring_->submit_read(fd, buf, len, local, [cb, len](int res) {
    //        cb(result_to_status(res, len, "read"));
    //    });
    // Returns 0 (op_id is no longer meaningful — cancel is client-side).

    uint64_t submit_write(PageAddr addr, const void* buf, size_t len,
                          std::function<void(Status)> on_complete) override;
    // a. ensure_extents(addr, len); validate.
    // b. O_DIRECT alignment: maybe_align() into AlignedIoBuf (unchanged).
    // c. Cross-extent split: same WriteState fan-out, but each chunk's
    //    reactor_->submit_write → uring_->submit_write(fd, ...).
    // d. Single-extent: uring_->submit_write(fd, ...).

    Status submit_fsync(std::function<void(Status)> on_complete) override;
    // Chain fsync across all dirty extent fds → uring_->submit_fsync(fd, ...).

    void cancel(uint64_t /*op_id*/) override;
    // No-op. The existing AsyncPageStore base declares this virtual, but
    // cancel is now client-side (§7.1): callers wrap callbacks with a
    // shared_ptr<OpState> cancel flag. Kept as a no-op override rather
    // than removed from the base class to avoid breaking other
    // AsyncPageStore implementations (e.g. a future SPDK store). The
    // base-class declaration stays; this override makes it a no-op so
    // callers that still invoke it (if any) don't crash.

  private:
    BlockPageStore* store_;
    DiskIOUring*    uring_;
};
```

The fd is obtained from `store_->fd_for_offset(addr, &local)` — the
same call as today. No key, no mapping — the fd IS the routing key.
`DiskIOUring` looks up `fd_table[fd]` and routes to the pipeline.

The cross-extent write split (`block_async_page_store.cpp` lines
139-204) is preserved — it splits a write spanning multiple extents into
per-extent submissions sharing a `WriteState`. Each per-extent
`reactor_->submit_write` becomes `uring_->submit_write(fd, ...)`. The
`WriteState` fan-out logic (last completion invokes `on_complete`) is
unchanged. Callback suppression for cross-extent teardown is
client-side: `WriteState` gets a `std::atomic<bool> cancelled` flag;
each per-extent callback checks it before proceeding. If extent A
fails, `WriteState->cancelled` is set; extent B's callback fires,
checks the flag, returns — no `cancel(op_id)` call needed. The
`shared_ptr<WriteState>` keeps the state alive until all CQEs arrive.

### 9.2 FFI Wiring (`c_api.cpp`)

Today (`c_api.cpp` lines 330-338):
```cpp
h->reactor = std::make_unique<crow::common::Reactor>();
h->async_store = std::make_unique<BlockAsyncPageStore>(
    static_cast<BlockPageStore*>(h->store.get()), h->reactor.get());
o.async_reactor    = h->reactor.get();
o.async_page_store = h->async_store.get();
```

After:
```cpp
DiskIOUring::Topology topo{
    .pipelines = {{256, PollingMode::Hybrid}},
    .poll_thread_groups = {{0}},  // one thread for one pipeline
    .attach_wq = false,
};
h->uring = std::make_unique<DiskIOUring>(std::move(topo));
// Register all extent fds — auto-assign (single pipeline, so all go
// to pipeline 0; no need for the caller to know pipeline_index).
for (int fd : h->store->all_extent_fds()) {
    h->uring->register_fd(fd);  // auto-assign
}
h->async_store = std::make_unique<BlockAsyncPageStore>(
    static_cast<BlockPageStore*>(h->store.get()), h->uring.get());
o.async_uring      = h->uring.get();
o.async_page_store = h->async_store.get();
```

One pipeline today (preserves current behavior). Multi-disk btree would
add pipelines and register extent fds to different pipelines — future,
no code change to `DiskIOUring` or `BlockAsyncPageStore`.

---

## 10. FFI: Multi-Eventfd Pump

### 10.1 Why

Today: one reactor → one eventfd → one `EventfdPump` in `reactor.rs`
(lines 62-70). With N pipelines, there are N eventfds (one per
pipeline). All must fan out to the same `Arc<Notify>` so any pipeline's
CQE completion wakes all waiting `drive_ct_future` calls.

### 10.2 How

`Crowtree` holds `Vec<EventfdPump>` instead of one. At construction
(after `DiskIOUring` is wired):
a. Query all pipeline eventfds via `DiskIOUring::eventfds(out_fds,
   out_count)`.
b. For each eventfd, spawn one `EventfdPump` — same pump logic as today
   (`reactor.rs` lines 62-70), each monitoring its own `AsyncFd`, all
   sharing the same `Arc<Notify>`.
c. `Crowtree::Drop` aborts all pump tasks before `ct_close` runs.

`ct_future_poll` is already synchronous and pipeline-agnostic — it
polls the C++ future, which completes when whichever pipeline's CQE
fires. No change to the poll path. The `drive_ct_future` loop
(`reactor.rs` lines 293-326) is unchanged: it waits on the shared
`Notify`, which any pump can fire.

The existing `ct_reactor_eventfd` C ABI function is replaced by
`ct_uring_eventfds`:

```c
// Returns the number of pipeline eventfds. If out_fds is non-null and
// max > 0, copies up to max eventfd numbers into out_fds. Returns 0 if
// no DiskIOUring is wired or no valid pipelines exist. The caller
// allocates out_fds; no ownership transfer — the fds are owned by the
// DiskIOUring and remain valid until ct_close.
size_t ct_uring_eventfds(const ct_tree* t, int* out_fds, size_t max);
```

The Rust FFI wrapper calls this with a stack-allocated `[c_int; N]` (N
bounded by the topology's pipeline count, known at construction) and
collects the returned fds into a `Vec<RawFd>`. No heap allocation on the
C side, no ownership transfer — the fds are `DiskIOUring`-owned and
remain valid until `ct_close`. Rust wraps each via `RawFdView` (no-close
guard, existing) and spawns one `EventfdPump` per fd.

Edge cases:
- If `DiskIOUring` has zero valid pipelines (all invalid), the eventfd
  list is empty and no pumps are spawned. `drive_ct_future` falls back
  to `tokio::task::yield_now().await` (`reactor.rs` lines 317-324) —
  same as today's "no reactor wired" path.
- Pump task spawn failure (rare): log a warning, continue with fewer
  pumps. A missing pump means completions on that pipeline's eventfd
  won't wake waiters promptly — they'll wake on the next unrelated
  notification. Not a correctness issue (poll is synchronous), just a
  latency regression on that pipeline.

---

## 11. Topology Configuration

`DiskIOUring::Topology` is the single configuration knob, constructed
programmatically by each service at startup:

- **diskio:** reads the node's disk list, classifies by disk type,
  builds pipelines per `design-crow-diskio.md` §7.3, assigns each
  disk's fd to its pipeline via `register_fd`.
- **btree:** 1 pipeline today (single-disk store).
  `Topology{ .pipelines = {{256, Hybrid}}, .poll_thread_groups = {{0}},
  .attach_wq = false }`. All extent fds registered to pipeline 0.
  Future multi-disk btree would add pipelines and register per-extent
  fds to different pipelines.

### 11.1 Config Fields

- `pipelines: Vec<PipelineConfig>` — per-pipeline config (entries, mode,
  hybrid, sqpoll). Default: single pipeline `{256, Classic}`.
- `poll_thread_groups: Vec<Vec<size_t>>` — which pipelines each poll
  thread handles. Default: `{{0}}` (one thread, all pipelines).
- `attach_wq: bool` — share kernel io-wq across pipelines. Default:
  `false`.

### 11.2 diskio Server Config (existing config file, new fields)

- `uring_topology: "auto" | "single" | explicit` — default `"auto"`
  (classify disks, build per §7.3). `"single"` preserves current
  behavior (one pipeline for all disks).
- `uring_entries_nvme`, `uring_entries_sata`, `uring_entries_hdd` —
  per-class SQ depth. Defaults: 256, 256, 2048.
- `uring_poll_threads: "auto" | N` — default `"auto"` (one per NVMe
  pipeline, one per SATA group, one for all HDDs).
- `attach_wq: bool` — default `true` when ≥2 NVMe or ≥2 SATA pipelines.

---

## Scope

**crow-common (C++):**
- `lib/crow-common/cpp/include/crow-common/reactor.h` → replaced by
  `diskio_uring.h`; `class Reactor` → internal `Pipeline` struct within
  `DiskIOUring`. Lock-free SQE claim mechanism preserved.
- `lib/crow-common/cpp/src/reactor.cpp` → replaced by
  `diskio_uring.cpp`; `Reactor::run` → `PollThread::run` (multi-CQ
  loop); `Reactor::submit_lockfree` → `Pipeline::submit_lockfree`
  (unchanged logic).
- `lib/crow-common/cpp/CMakeLists.txt` — rename, add new files.

**crow-diskio (C++):**
- `app/crow-diskio/src/engine/uring/uring_engine.h` — remove 16-shard
  map, `shard()`, `in_flight_count(DiskId)` (breaking API change, see
  §8.1); replace `Reactor reactor_` with `DiskIOUring uring_`; add
  `uring()` accessor.
- `app/crow-diskio/src/engine/uring/uring_engine.cpp` — remove callback
  wrapping, `cancel_disk` loop body; delegate to `uring_.submit_*` and
  `uring_.cancel_fd`.
- `app/crow-diskio/src/server/` — build `Topology` from disk list,
  construct engine with topology, register fds.

**crow-tree (C++):**
- `lib/crow-tree/include/crow-tree/block_page_store.h` — add
  `all_extent_fds()` method (returns fds of all live, non-deleted
  extents; unlike `dirty_fds()`, does not clear the dirty flag — used
  for fd registration at `ct_open` time).
- `lib/crow-tree/include/crow-tree/async_page_store.h` —
  `BlockAsyncPageStore` constructor: `Reactor*` → `DiskIOUring*`;
  `cancel(uint64_t)` becomes a no-op override (see §9.1).
- `lib/crow-tree/src/block_async_page_store.cpp` — route through
  `DiskIOUring`; cross-extent split uses `uring_->submit_write(fd, ...)`.
- `lib/crow-tree/src/c_api.cpp` — `h->reactor` → `h->uring`; construct
  `DiskIOUring` with 1-pipeline topology; register extent fds via
  `store->all_extent_fds()`.
- `lib/crow-tree/include/crow-tree/c_api.h` — C ABI: replace
  `ct_reactor_eventfd` with `ct_uring_eventfds` (signature in §10.2);
  update `ct_open_opts` (`async_reactor` → `async_uring`).
- `lib/crow-tree/include/crow-tree/options.h` — `async_reactor` field →
  `async_uring` (type change from `Reactor*` to `DiskIOUring*`).

**crow-tree FFI (Rust):**
- `lib/crow-tree/ffi/src/reactor.rs` — `EventfdPump` spawned per
  pipeline; `Crowtree` holds `Vec<EventfdPump>`; update C ABI bindings
  for `ct_uring_eventfds`.
- `lib/crow-tree/ffi/src/tree.rs` — update `sys` bindings for renamed C
  ABI functions; `Crowtree` construction spawns N pumps.
- `lib/crow-tree/ffi/src/sys.rs` — regenerate/update for renamed
  functions.

**Tests:**
- `lib/crow-tree/tests/unit/reactor_test.cpp` → `uring_test.cpp`;
  adapt to `DiskIOUring` API.
- `lib/crow-tree/tests/integration/async_get_test.cpp`,
  `async_scan_test.cpp` — update reactor → uring wiring.
- `app/crow-diskio/tests/uring_engine_test.cpp` — update
  `in_flight_count(DiskId)` calls to `uring_.in_flight_count(fd)`;
  update `cancel_disk` assertions (now delegates to `cancel_fd`).

**Docs:**
- `doc/design/diskio/design-crow-diskio.md` §7 — update to reflect
  `DiskIOUring`, pipeline topology, shared polling threads, batch
  submit, `IORING_ASYNC_CANCEL_FD`; fold in the kernel-internal lock
  analysis (trimmed summary in §7.5 of this draft).
- `doc/design/tree/` — update btree async page store references.
- `doc/doc_index.md` — update if any doc titles change.

---

## Complexity

**High.** The rename + restructure touches every consumer (diskio,
btree, FFI, tests, docs). The multi-CQ polling thread is new logic —
busy-poll loop over N CQs + epoll-wait on N eventfds — but each
individual operation (peek CQ, publish SQE) is existing proven code
from `Reactor`. The batch submit design is an extension of the existing
`pending_submit_` + `publish_ready_sqes()` mechanism, generalized to
multi-pipeline. The hardest parts:
1. **Multi-CQ polling loop** — correct ordering of submit → drain → wait
   across N pipelines, with mode transitions (busy-poll ↔ event-wait).
2. **Eventfd write coalescing** — the `thread_sleeping_` CAS pattern to
   avoid redundant eventfd writes under burst load.
3. **FFI multi-eventfd pump** — spawning N pumps sharing one
   `Arc<Notify>`, handling pump spawn failure.
4. **`IORING_ASYNC_CANCEL_FD`** — correct usage of the cancel-by-fd
   flags, handling the CQE results (`-ECANCELED` vs original result).
5. **Keeping the refactor in one coherent change** without breaking the
   FFI boundary mid-refactor.

Main challenge: the multi-CQ polling loop is the core new logic. The
rest is restructuring existing proven code around it.

---

## Test Design

### Unit Tests (UT)

**`DiskIOUring` — pipeline + routing:**
- UT: single-pipeline, basic submit + complete. Setup:
  `DiskIOUring(Topology{ .pipelines = {{256, Hybrid}} })`, memfd,
  `register_fd(fd)` (auto-assign). Action: `submit_write(fd, buf,
  4096, 0, cb)`. Assert: callback `res == 4096`; read-back matches;
  returned pipeline index == 0.
- UT: multi-pipeline explicit routing. Setup: 2 pipelines,
  `register_fd(fa, 0)`, `register_fd(fb, 1)`. Action: submit writes
  for both fds. Assert: fa's I/O completes on pipeline 0's eventfd,
  fb's on pipeline 1's (verify via `eventfds()` + which eventfd
  fires).
- UT: auto-assign distributes by load. Setup: 2 pipelines,
  `register_fd(fd_a)` (auto → pipeline 0, in-flight count 0 vs 0,
  picks 0), submit 10 in-flight ops for fd_a. Then `register_fd(fd_b)`
  (auto → pipeline 1, lower in-flight count). Action: submit for
  fd_b. Assert: fd_b's I/O completes on pipeline 1's eventfd.
- UT: auto-assign is sticky. Setup: 2 pipelines, `register_fd(fd)`
  (auto → pipeline 0). Action: submit 100 ops for fd over time.
  Assert: all 100 route to pipeline 0 (never pipeline 1), even if
  pipeline 1 becomes less loaded.
- UT: unregistered fd routes to pipeline 0 with warning. Setup:
  2-pipeline `DiskIOUring`, no `register_fd` called. Action:
  `submit_write(fd, ...)`. Assert: completes on pipeline 0; log
  warning emitted.

**`DiskIOUring` — batch submit:**
- UT: batch coalescing under burst. Setup: 1 pipeline, Classic mode
  (deterministic timing). Action: 32 concurrent `submit_write` calls
  from 32 threads. Assert: ≤2 `io_uring_enter` syscalls total (1
  batch submit from polling thread, possibly 1 eventfd wakeup). Verify
  via syscall counting (strace or instrumented `io_uring_enter`).
- UT: eventfd write coalescing. Setup: 1 pipeline, simulate event-wait
  mode (force `thread_sleeping_ = true`). Action: 100 concurrent
  submits. Assert: exactly 1 eventfd write (only the first client CASes
  `thread_sleeping_` to false and writes; the rest skip).
- UT: busy-poll mode — zero eventfd writes on submit. Setup: 1
  pipeline, Hybrid mode, keep I/O active (continuous submits). Action:
  submit 100 writes. Assert: 0 eventfd writes from client threads
  (thread is busy-polling, sees `pending_submit_` each iteration).

**`DiskIOUring` — cancel:**
- UT: client-side callback suppression. Setup: submit a read on a
  memfd with a `shared_ptr<OpState>` cancel flag. Action: set
  `cancelled = true` before completion. Assert: callback fires but
  returns early (no side effects); no use-after-free (ASan clean);
  SQ slot freed normally.
- UT: `cancel_fd` via `IORING_ASYNC_CANCEL_FD`. Setup: submit 5 writes
  for fd on a slow device (or fault-injected delay). Action:
  `cancel_fd(fd)`. Assert: all 5 callbacks fire with `-ECANCELED`;
  `in_flight_count(fd) == 0`.
- UT: `cancel_fd` does not affect other fds. Setup: 2 fds on same
  pipeline, submit 3 writes for each. Action: `cancel_fd(fd_a)`.
  Assert: fd_a's 3 callbacks fire with `-ECANCELED`; fd_b's 3 callbacks
  fire with success.

**`DiskIOUring` — multi-CQ polling:**
- UT: 2 pipelines, 1 poll thread, both CQs drained. Setup: 2 pipelines,
  `poll_thread_groups = {{0, 1}}`. Action: submit I/O on both
  pipelines. Assert: both CQEs dispatched by the same thread (verify
  via thread name or TID in callback).
- UT: 2 pipelines, 2 poll threads, CQ isolation. Setup: 2 pipelines,
  `poll_thread_groups = {{0}, {1}}`. Action: submit I/O on both.
  Assert: pipeline 0's CQE dispatched by thread 0, pipeline 1's by
  thread 1.
- UT: busy-poll → event-wait transition. Setup: 1 pipeline, Hybrid
  mode, `busy_poll_budget = 4`. Action: submit one I/O, wait for
  completion, then no I/O for 5+ iterations. Assert: thread transitions
  to `epoll_wait` (verify via syscall trace — `epoll_wait` appears
  after budget exhausted). Then submit new I/O: thread wakes via
  eventfd, transitions back to busy-poll.

**`UringEngine` (diskio) — thinned:**
- UT: O_DIRECT alignment validation. Setup: `UringEngine` with a
  `BlockDisk` (memfd simulating O_DIRECT, block_size = 4096). Action:
  submit write with `size = 100` (not aligned). Assert: callback
  `-EINVAL`; `DiskIOUring` never touched.
- UT: `cancel_disk` delegates to `cancel_fd`. Setup: engine with
  1-pipeline `DiskIOUring`, submit 3 writes for disk D. Action:
  `engine.cancel_disk(D)`. Assert: `uring_.in_flight_count(D->fd())
  == 0`; all 3 callbacks fire with `-ECANCELED`.

**`BlockAsyncPageStore` (btree) — uring-routed:**
- UT: single-extent write via uring. Setup: `BlockPageStore` with one
  extent, `DiskIOUring` (1 pipeline), `BlockAsyncPageStore(store,
  uring)`. Register extent fd. Action: `submit_write(addr, buf, 4096,
  cb)`. Assert: callback `Status::Ok()`; read-back matches.
- UT: cross-extent write split. Setup: store with 2 extents,
  block_size = 4096. Register both extent fds. Action: `submit_write`
  spanning both extents (8192 bytes at offset 3072). Assert: two
  `uring_->submit_write` calls (one per extent fd); callback fires
  once with `Status::Ok()` after both CQEs; `AlignedIoBuf` kept alive
  until both complete (ASan clean).
- UT: cross-extent write teardown via WriteState cancel flag. Setup:
  store with 2 extents, submit a cross-extent write. Action: simulate
  extent A failure → set `WriteState->cancelled = true`. Assert:
  extent B's callback fires, checks flag, returns early; no UAF
  (ASan clean); `on_complete` invoked once with error status.
- UT: `cancel(op_id)` is a no-op. Setup: `BlockAsyncPageStore` with
  uring. Action: `submit_write` then `cancel(op_id)`. Assert: callback
  still fires normally (cancel is client-side; the no-op override does
  not suppress it).

### End-to-End Tests (E2E)

**diskio multi-pipeline topology:**
- E2E: NVMe topology (one pipeline per disk). Setup: 2-disk node,
  topology with 2 pipelines (NVMe class), each disk's fd registered to
  its pipeline. Action: concurrent writes to both disks. Assert: both
  complete; I/O for disk 1 routes to pipeline 0, disk 2 to pipeline 1
  (verify via per-pipeline eventfd or in-flight count). Bad-disk
  cancellation: fail disk 2, `cancel_disk(disk2)`, assert disk 1's I/O
  unaffected.
- E2E: HDD topology (one shared pipeline). Setup: 3-HDD node, 1
  pipeline, all 3 fds registered to pipeline 0. Action: concurrent
  writes to all 3 disks. Assert: all complete on pipeline 0; SQ does
  not exhaust (entries ≥ 2048). Bad-disk: fail disk 2,
  `cancel_disk(disk2)` via `cancel_fd(fd2)`, assert disks 1 and 3
  continue serving.
- E2E: mixed topology (NVMe + HDD). Setup: 1 NVMe + 2 HDD node, 2
  pipelines (NVMe → pipeline 0, HDDs → pipeline 1), 2 poll threads.
  Action: concurrent writes to all 3 disks. Assert: NVMe I/O on
  pipeline 0 (thread 0), HDD I/O on pipeline 1 (thread 1); no
  cross-pipeline routing.
- E2E: shared poll thread for 2 pipelines. Setup: 2 pipelines,
  `poll_thread_groups = {{0, 1}}`, 1 poll thread. Action: concurrent
  I/O on both pipelines. Assert: both CQs drained by the same thread;
  no CQE starvation (both pipelines' I/O completes in bounded time).

**btree async via uring:**
- E2E: `AsyncCrowtree::get` demand-load miss. Setup: `Crowtree` with
  `DiskIOUring` (1 pipeline) + `BlockAsyncPageStore`. Action: `get(key)`
  triggering a page fault (cold cache). Assert: page loaded via
  `uring_->submit_read`; `drive_ct_future` wakes on the pump's
  `Notify`; value returned correctly.
- E2E: multi-pipeline btree (future-proofing). Setup: `DiskIOUring`
  with 2 pipelines, store's extent fds registered to pipeline 0.
  Action: concurrent `get` + `flush`. Assert: both route to pipeline 0;
  both pumps' eventfds monitored; completions wake the shared `Notify`.

**FFI multi-eventfd pump:**
- E2E: 2-pipeline uring, 2 pumps. Setup: `Crowtree` with 2-pipeline
  `DiskIOUring`. Assert: 2 `EventfdPump` tasks spawned; both share one
  `Arc<Notify>`. Action: submit I/O that completes on pipeline 1.
  Assert: `drive_ct_future` wakes (pump 1 fires `Notify`); future
  completes.
- E2E: pump spawn failure resilience. Setup: simulate pump spawn
  failure for pipeline 1 (inject error). Action: submit I/O on
  pipeline 1. Assert: future still completes (via `yield_now` fallback
  or pipeline 0's pump firing `Notify` for an unrelated completion);
  latency regression but no hang.

**Batch submit:**
- E2E: batch submit under load. Setup: 1-pipeline `DiskIOUring`, 32
  client threads. Action: each thread submits 100 writes. Assert: total
  `io_uring_enter` syscalls ≈ 3200/32 ≈ 100 (one per polling-thread
  iteration, batching ~32 SQEs each), not 3200 (one per SQE). Verify
  via syscall count.

---

## Module Structure

```
lib/crow-common/cpp/
├── include/crow-common/
│   └── diskio_uring.h        # DiskIOUring class + internal Pipeline/PollThread
├── src/
│   └── diskio_uring.cpp      # DiskIOUring impl, PollThread::run (multi-CQ loop)
└── CMakeLists.txt            # rename reactor→diskio_uring

app/crow-diskio/src/
├── engine/uring/
│   ├── uring_engine.h        # thin: DiskIOUring member, delegate to uring_
│   └── uring_engine.cpp      # thin: validation + uring_.submit_*
└── server/                   # build Topology from disk list, register fds

lib/crow-tree/
├── include/crow-tree/
│   ├── async_page_store.h    # BlockAsyncPageStore takes DiskIOUring*; cancel=no-op
│   ├── block_page_store.h    # add all_extent_fds() for fd registration
│   ├── c_api.h               # ct_uring_eventfds; ct_open_opts change
│   └── options.h             # async_reactor → async_uring
├── src/
│   ├── block_async_page_store.cpp  # route through uring_
│   └── c_api.cpp             # h->uring wiring, 1-pipeline topology, register fds
├── ffi/src/
│   ├── reactor.rs            # Vec<EventfdPump>, multi-eventfd spawn
│   ├── tree.rs               # updated sys bindings, N-pump construction
│   └── sys.rs                # regenerated bindgen for renamed C ABI
└── tests/
    ├── unit/uring_test.cpp   # renamed from reactor_test.cpp
    └── integration/          # async_get_test, async_scan_test updated
```

---

## Server Wiring

**diskio server startup:**
1. Load node disk list from group-0 sysdata (`HardwareClient`).
2. Populate `DiskSet` (open each disk, classify by type).
3. Build `DiskIOUring::Topology` from disk classification per §7.3 of
   the design doc (NVMe: 1 pipeline/disk Sqpoll; SATA: 1/4-8 disks
   Hybrid; HDD: 1 shared Hybrid entries=2048).
4. Construct `UringEngine(topo)` (engine owns `DiskIOUring`).
5. For each disk: `engine.uring().register_fd(disk->fd(),
   pipeline_index)` — explicit assignment (pipeline config must match
   disk class; see §3.3).
6. Wire `IoEngine*` into the RPC handler (unchanged).

**btree `ct_open` (c_api.cpp):**
1. Construct `BlockPageStore` from opts (unchanged).
2. Construct `DiskIOUring` with 1-pipeline topology
   (`{256, Hybrid}`, `poll_thread_groups = {{0}}`).
3. Register all extent fds — auto-assign (single pipeline, no need to
   specify index): `uring->register_fd(fd)` for each
   `store->all_extent_fds()`.
4. Construct `BlockAsyncPageStore(store, uring.get())`.
5. Set `o.async_uring = uring.get()`, `o.async_page_store =
   async_store.get()`.
6. `ct_open` proceeds to construct `Crowtree` with the async store
   (unchanged).
7. FFI `Crowtree` construction queries `ct_uring_eventfds`, spawns one
   `EventfdPump` per eventfd, all sharing one `Arc<Notify>`.

---

## Open Questions

### OQ1: [Resolved] `cancel_fd` on kernels < 6.0

**Decided:** require kernel 6.0+ for `cancel_fd`. On older kernels,
`cancel_fd(fd)` returns `-ENOSYS` (runtime check via kernel-version
probe or compile-time `#ifdef`). The caller (diskio's `cancel_disk`)
handles the error: log a warning, fall back to waiting for in-flight
I/O to complete naturally (the bad-disk path already has a timeout). No
Treiber stack fallback, no per-fd mutex. macOS never compiles
`DiskIOUring`; `BlockingEngine` is the non-uring path. Incorporated
into §7.2.

### OQ2: [Resolved] Poll thread CPU pinning

**Decided:** implement in R109. Per-group CPU binding via
`pthread_setaffinity_np`. `poll_thread_groups` entries are structs:

```cpp
struct PollThreadGroupConfig {
    std::vector<size_t> pipelines;  // which pipelines this thread polls
    int                 cpu = -1;   // -1 = no pinning; >=0 = pin to core
};
std::vector<PollThreadGroupConfig> poll_thread_groups;
```

Default: `cpu = -1` (no pinning). The `cpu` field is `#ifdef`'d to a
no-op on non-Linux builds. Incorporated into §3.1 Topology.

### OQ3: [Resolved] `DiskIOUring` vs `IoUring` naming

**Decided:** `DiskIOUring`. The class does disk I/O (read/write/fsync
on block devices and files); the name is accurate for both diskio and
btree. `IoUring` is too generic (could be confused with network I/O).

### OQ4: [Resolved] Linked timeouts vs monitor + `cancel_fd`

**Decided:** monitor + `cancel_fd` replaces linked timeouts for
bad-disk resilience. Linked timeouts are not implemented in R109 (or
ever, unless a per-op timeout use case appears that the monitor can't
cover). `PipelineConfig::linked_timeout_ms` is dropped from the API.
`cancel(op_id)` is removed from the API — callback suppression is
client-side via a shared cancel flag (§7.1). `CallbackEntry` has no
atomics. The monitor task is a diskio-server-level component (not in
`DiskIOUring` itself — it calls `cancel_fd` and `in_flight_count` via
the public API). btree has no monitor (no bad-disk scenario).

Monitor task design (diskio server-level, not in `DiskIOUring`):

```
MonitorThread (one per diskio server):
  timerfd (1s tick)
  per-fd tracking:
    last_cqe_time[fd]     — atomic timestamp of last CQE
    in_flight[fd]         — atomic counter (already exists, §7.3)

  loop:
    read timerfd (1s tick)
    for each registered fd:
      if in_flight[fd] > 0 and (now - last_cqe_time[fd]) > timeout[fd]:
        log: "fd %d appears stuck (%d in-flight, no CQE for %ds)"
        cancel_fd(fd)           # kernel cancels all ops on this fd
        mark fd as bad in DiskSet  # diskio's existing bad-disk path
```

Cost: one timerfd read per second + one array scan per second.
Negligible. The monitor can be pinned to a core (OQ2's CPU pinning).

### OQ5: [Resolved] Hazard pointers for cancel — eliminated

**Decided:** resolved by removing `cancel(op_id)` (OQ4). With callback
suppression moved to the client side (shared cancel flag in
`shared_ptr<OpState>`), `DiskIOUring` no longer has any cross-thread
access to `CallbackEntry` after `submit_*` returns. The polling thread
is the sole toucher — no race, no hazard pointers, no EBR, no
generation counters. `CallbackEntry` has zero atomics.

### OQ6: [Resolved] Backlog doc not yet created

**Decided:** create the backlog doc first (option a). The backlog doc
`doc/backlog/R109-common-diskio-uring.md` has been created with problem
statement, use scenarios, solution work items, dependencies, and
acceptance criteria. The Test Design section of this working design
draft is now grounded in the backlog doc's acceptance criteria.
