<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R109: common — Multi-Pipeline io_uring Engine (`DiskIOUring`)

**Problem**

The shared io_uring layer in `crow-common` is `Reactor` — a
single-ring wrapper with a misleading name. Both diskio
(`UringEngine`) and btree (`BlockAsyncPageStore`) use it, but it
cannot route I/O to multiple io_uring instances, cannot share polling
threads across CQs, and cannot batch SQE submission across pipelines.
The diskio design doc specifies a multi-ring topology (one ring per
NVMe disk, one shared ring for HDDs) but `Reactor` is a single ring —
diskio works around this by having `UringEngine` own one `Reactor` and
adding a 16-shard mutex-guarded `HashMap<DiskId, HashSet<op_id>>` for
bad-disk cancellation tracking. This is the wrong layer for routing
and cancellation: the engine should be thin, and the io_uring layer
should handle fd→ring routing and kernel-level cancel.

**Current behavior + impact**: `Reactor`
(`lib/crow-common/cpp/include/crow-common/reactor.h`,
`lib/crow-common/cpp/src/reactor.cpp`) is a single `io_uring` instance
with one polling thread. `UringEngine`
(`app/crow-diskio/src/engine/uring/uring_engine.cpp`) wraps one
`Reactor` and adds 16-shard per-`DiskId` in-flight tracking
(`InFlightShard`, mutex-guarded) for `cancel_disk` — all of which is
eliminated by kernel-level cancel-by-fd (`IORING_ASYNC_CANCEL_FD`,
kernel 6.0+). `BlockAsyncPageStore`
(`lib/crow-tree/src/block_async_page_store.cpp`) borrows one
`Reactor*` and maps global byte offsets to per-extent fds via
`fd_for_offset`. The FFI layer
(`lib/crow-tree/ffi/src/reactor.rs`) spawns one `EventfdPump` per
reactor eventfd to fan out CQE completions to waiting
`drive_ct_future` calls. None of these can use multiple io_uring
rings, shared polling threads, or batched cross-pipeline submission —
all specified by the diskio design doc §7.3 (Reactor Topology).

**Design pointers**: diskio root design
`doc/design/diskio/design-crow-diskio.md` §7 (Reactor), §7.3 (Reactor
Topology — NVMe: one ring/disk, SATA: one ring/4-8 disks, HDD: one
shared ring), §7.4 (Bad-Disk SQ Isolation — explicit cancellation +
linked timeouts). The working design draft
(`doc/working/design-uring-pool.md`) has the full implementation
detail: pipeline struct, poll thread groups, batch submit,
lock-free design, fd→pipeline routing, cancel-by-fd, FFI multi-eventfd
pump, and all OQ decisions.

**Use scenarios**:

- **NVMe topology (one pipeline per disk)**: A diskio server starts
  with 4 NVMe disks. It builds a topology with 4 pipelines (Sqpoll
  mode, one poll thread per pipeline), opens each disk, and registers
  each disk's fd to its pipeline. Concurrent writes to all 4 disks
  route to their respective pipelines — one busy disk's full SQ does
  not block another. Expected: 4-way SQ/CQ isolation, no cross-disk
  backpressure.

- **HDD topology (one shared pipeline)**: A diskio server starts with
  30 HDDs. It builds a topology with 1 pipeline (Hybrid mode,
  `entries = 2048`), registers all 30 fds to that pipeline. Concurrent
  writes to all 30 disks share one SQ ring and one polling thread.
  Expected: low IOPS per disk means one ring's SQ handles 30 HDDs
  trivially; no SQ exhaustion.

- **Mixed topology (NVMe + HDD)**: A diskio server starts with 2 NVMe
  + 3 HDD. It builds 3 pipelines (2 NVMe Sqpoll + 1 HDD Hybrid), 3
  poll threads. Each NVMe disk's fd is explicitly registered to its
  pipeline; HDD fds are registered to the shared HDD pipeline.
  Expected: NVMe I/O on Sqpoll pipelines, HDD I/O on Hybrid pipeline;
  no cross-class routing.

- **Bad-disk cancellation**: A disk goes bad (hardware hang, no error
  reporting). The diskio monitor detects no CQEs for the fd for N
  seconds and calls `cancel_fd(fd)`. The kernel cancels all in-flight
  ops on that fd via `IORING_ASYNC_CANCEL_FD` and posts `-ECANCELED`
  CQEs. Expected: all callbacks fire with `-ECANCELED`, SQ slots freed,
  other fds' I/O unaffected.

- **Shared poll thread for 2 pipelines**: A topology with 2 pipelines
  and `poll_thread_groups = {{0, 1}}` (one thread for both). I/O is
  submitted on both pipelines. Expected: both CQs drained by the same
  thread; no CQE starvation; sub-µs dispatch when busy-polling.

- **btree async page load via uring**: A `Crowtree` with
  `DiskIOUring` (1 pipeline, Hybrid mode) + `BlockAsyncPageStore`.
  `get(key)` triggers a page fault (cold cache). The page is loaded
  via `uring_->submit_read(fd, ...)`. Expected: `drive_ct_future`
  wakes on the pump's `Notify`; value returned correctly.

- **Client-side callback suppression (RPC timeout)**: An RPC timeout
  tears down a callback chain while a uring I/O is in-flight. The
  caller sets a `shared_ptr<OpState>` cancel flag. The CQE arrives,
  the callback fires, checks the flag, returns early. Expected: no
  use-after-free (ASan clean), SQ slot freed normally, no
  `DiskIOUring`-side synchronization.

- **Batch submit under burst**: 32 client threads each submit 100
  writes to a 1-pipeline `DiskIOUring`. The polling thread batches
  all pending SQEs into one `io_uring_enter` per iteration. Expected:
  ~100 `io_uring_enter` syscalls total (one per iteration, batching
  ~32 SQEs each), not 3200 (one per SQE).

**Solution**

Replace `Reactor` with `DiskIOUring` — a multi-pipeline io_uring
engine that maps fds to pipelines, shares polling threads across CQs,
batches SQE submission, and is fully lock-free on the hot path. The
natural routing key is the fd (the caller already has it), and
io_uring's `IORING_ASYNC_CANCEL_FD` eliminates per-key in-flight
tracking. No key template, no per-key map, no mutex-guarded shards.

**One-line summary**: `DiskIOUring` replaces `Reactor` with a
multi-pipeline io_uring engine: fd→pipeline routing, shared polling
threads, batch submit, kernel-level cancel-by-fd, client-side callback
suppression — eliminating `UringEngine`'s 16-shard map and all
`Reactor`-side cancel atomics.

**Numbered work items**:

1. **`DiskIOUring` class**
   (`lib/crow-common/cpp/include/crow-common/diskio_uring.h`,
   `lib/crow-common/cpp/src/diskio_uring.cpp`) — replaces `Reactor`.
   Internal `Pipeline` struct (one `io_uring` instance, lock-free SQE
   claim preserved from `reactor.cpp` lines 87-135) and `PollThread`
   struct (multi-CQ polling loop, eventfd+epoll wakeup, busy-poll ↔
   event-wait transition). Public API: `register_fd`, `submit_read`/
   `write`/`fsync`, `cancel_fd`, `in_flight_count`, `eventfds`. fd→
   pipeline routing via direct-indexed `fd_table` (sized once to
   `ulimit -n`, never grows). `Topology` struct with
   `PipelineConfig` + `PollThreadGroupConfig` (pipelines + optional
   CPU pinning).

2. **`UringEngine` thin-out**
   (`app/crow-diskio/src/engine/uring/uring_engine.h`,
   `app/crow-diskio/src/engine/uring/uring_engine.cpp`) — remove
   16-shard `InFlightShard` map, `shard()` helper, callback wrapping
   lambda, `cancel_disk` loop body, `in_flight_count(DiskId)` (breaking
   API change). Replace `Reactor reactor_` with `DiskIOUring uring_`.
   Delegate `submit_*` to `uring_.submit_*` and `cancel_disk` to
   `uring_.cancel_fd`. What remains: `Disk*` validation, `O_DIRECT`
   alignment checks.

3. **diskio server topology wiring**
   (`app/crow-diskio/src/server/`) — build `DiskIOUring::Topology`
   from disk classification per design doc §7.3: NVMe → one
   pipeline/disk Sqpoll, SATA → one pipeline/4-8 disks Hybrid, HDD →
   one shared Hybrid pipeline `entries=2048`. Register each disk's fd
   to its pipeline via `register_fd(fd, pipeline_index)` (explicit,
   because pipeline config must match disk class). `attach_wq = true`
   when ≥2 NVMe or ≥2 SATA pipelines.

4. **`BlockAsyncPageStore` uring-routed**
   (`lib/crow-tree/include/crow-tree/async_page_store.h`,
   `lib/crow-tree/src/block_async_page_store.cpp`) — constructor takes
   `DiskIOUring*` instead of `Reactor*`. `submit_read`/`write` route
   through `uring_->submit_*(fd, ...)` where fd comes from
   `store_->fd_for_offset`. Cross-extent write split preserved
   (`WriteState` fan-out unchanged); callback suppression for
   cross-extent teardown is client-side (`WriteState->cancelled` flag).
   `cancel(uint64_t)` becomes a no-op override (base class keeps the
   virtual).

5. **`BlockPageStore::all_extent_fds()`**
   (`lib/crow-tree/include/crow-tree/block_page_store.h`) — new method
   returning fds of all live, non-deleted extents. Unlike
   `dirty_fds()`, does not clear the dirty flag. Used for fd
   registration at `ct_open` time.

6. **btree `ct_open` wiring**
   (`lib/crow-tree/src/c_api.cpp`) — `h->reactor` → `h->uring`;
   construct `DiskIOUring` with 1-pipeline topology
   (`{256, Hybrid}`, `poll_thread_groups = {{0}}`); register all
   extent fds via `store->all_extent_fds()` + `uring->register_fd(fd)`
   (auto-assign, single pipeline).

7. **C ABI: `ct_uring_eventfds`**
   (`lib/crow-tree/include/crow-tree/c_api.h`,
   `lib/crow-tree/include/crow-tree/options.h`) — replace
   `ct_reactor_eventfd` with `ct_uring_eventfds(const ct_tree*, int*,
   size_t)` (returns count + array, caller allocates, no ownership
   transfer). `ct_open_opts::async_reactor` → `async_uring` (type
   `Reactor*` → `DiskIOUring*`).

8. **FFI multi-eventfd pump**
   (`lib/crow-tree/ffi/src/reactor.rs`,
   `lib/crow-tree/ffi/src/tree.rs`,
   `lib/crow-tree/ffi/src/sys.rs`) — `Crowtree` holds
   `Vec<EventfdPump>` instead of one. Query all pipeline eventfds via
   `ct_uring_eventfds`, spawn one `EventfdPump` per eventfd, all
   sharing one `Arc<Notify>`. `drive_ct_future` loop unchanged (waits
   on shared `Notify`). Pump spawn failure: log warning, continue with
   fewer pumps (latency regression, not correctness).

9. **diskio bad-disk monitor** (diskio server-level, not in
   `DiskIOUring`) — a `MonitorThread` with a 1s timerfd tick that
   tracks `last_cqe_time[fd]` + `in_flight[fd]` per registered fd.
   When an fd has in-flight I/O with no CQE for N seconds, calls
   `cancel_fd(fd)` and marks the disk bad in `DiskSet`. Replaces
   linked timeouts (design doc §7.4) with richer per-fd heuristics at
   zero normal-path SQE cost. btree has no monitor (no bad-disk
   scenario).

**Flow diagram**:

```
                     DiskIOUring
 caller ─submit_read(fd)──►  fd_table[fd] → pipeline_index (read-only)
                            ┌──────────────────────────────────┐
                            │  Pipeline 0       Pipeline 1      │
                            │  io_uring SQ+CQ   io_uring SQ+CQ  │
                            └────┬──────────────────┬───────────┘
                                 │                  │
                            ┌────┴──────────────────┘
                            │  PollThread 0 (busy-poll or epoll-wait)
                            │  drains CQ-0 + CQ-1, submits pending SQEs
                            └──────────────────────────────────────

  cancel_fd(fd) ──► IORING_OP_ASYNC_CANCEL SQE (kernel 6.0+)
                    kernel cancels all in-flight ops on fd
                    posts -ECANCELED CQEs → callbacks fire

  FFI:  eventfds() → [fd0, fd1, ...]
        each eventfd → EventfdPump → shared Arc<Notify>
        drive_ct_future waits on Notify (any pump can fire it)
```

**Edge cases at a glance**:

- Unregistered fd submitted → routes to pipeline 0 with warning
  (programming error, not runtime condition).
- SQ exhausted after bounded retry → `on_complete(-ENOMEM)`
  synchronously (same as existing `Reactor` behavior).
- `cancel_fd` on kernel < 6.0 → returns `-ENOSYS`; caller falls back
  to waiting for in-flight I/O to complete naturally (bad-disk
  timeout).
- `cancel_fd` SQ-full scenario → not a problem: the kernel consumes
  SQEs from the ring (advancing `sq_head`), so in-flight I/O does not
  hold SQ ring slots; space is available for the cancel SQE.
- Zero disks (diskio) → empty topology, zero pipelines; engine's
  `submit_*` invokes `on_complete(-ENOENT)` synchronously; server
  refuses to start.
- Zero valid pipelines (FFI) → empty eventfd list, no pumps spawned;
  `drive_ct_future` falls back to `yield_now` (same as today's "no
  reactor wired" path).
- Pump spawn failure → log warning, continue with fewer pumps;
  completions on that pipeline wake via next unrelated notification
  (latency regression, not correctness).
- Cross-extent write teardown → `WriteState->cancelled` flag set;
  each per-extent callback checks flag, returns early; `shared_ptr`
  keeps state alive until all CQEs arrive (no UAF).
- `unregister_fd` during operation → cancels in-flight via
  `cancel_fd`, waits for CQEs to drain, then clears `fd_table` slot
  (no reallocation; vector sized once at construction).

**Dependencies**

- **Depends on**: nothing new. `Reactor` (landed) provides the
  lock-free SQE claim mechanism, deferred-delete free list, and three
  polling modes — all preserved unchanged inside `Pipeline`. liburing
  (in pixi environment) provides `IORING_ASYNC_CANCEL_FD` headers
  (kernel 6.0+ required at runtime for cancel-by-fd).
- **Depended on by**: **R66** (WAL io_uring backend) — R66's
  `IoBackend::Uring` variant will use `DiskIOUring` instead of
  `Reactor` once R109 lands. R66 currently references
  `lib/crow-tree/src/reactor.cpp` (the old location); after R109,
  R66's design must be updated to reference `DiskIOUring`. R66 is not
  blocked by R109 (R66 can proceed with `Reactor` and migrate later),
  but the migration is cleaner if R109 lands first.

**Acceptance**

**`DiskIOUring` — pipeline + routing**:
- Single-pipeline basic submit + complete: `DiskIOUring(Topology{
  .pipelines = {{256, Hybrid}} })`, memfd, `register_fd(fd)` (auto).
  `submit_write(fd, buf, 4096, 0, cb)` → callback `res == 4096`;
  read-back matches; returned pipeline index == 0. Unit test.
- Multi-pipeline explicit routing: 2 pipelines, `register_fd(fa, 0)`,
  `register_fd(fb, 1)`. Submit writes for both → fa's I/O completes on
  pipeline 0's eventfd, fb's on pipeline 1's. Unit test.
- Auto-assign distributes by load: 2 pipelines, `register_fd(fd_a)`
  (auto → 0), submit 10 in-flight for fd_a, `register_fd(fd_b)` (auto
  → 1, lower in-flight). Submit for fd_b → completes on pipeline 1's
  eventfd. Unit test.
- Auto-assign is sticky: 2 pipelines, `register_fd(fd)` (auto → 0),
  submit 100 ops → all 100 route to pipeline 0, never pipeline 1.
  Unit test.
- Unregistered fd routes to pipeline 0 with warning: 2-pipeline
  `DiskIOUring`, no `register_fd`. `submit_write(fd, ...)` → completes
  on pipeline 0; log warning emitted. Unit test.

**`DiskIOUring` — batch submit**:
- Batch coalescing under burst: 1 pipeline, Classic mode, 32
  concurrent `submit_write` from 32 threads → ≤2 `io_uring_enter`
  syscalls total. Verify via syscall counting. Unit test.
- Eventfd write coalescing: 1 pipeline, force `thread_sleeping_ =
  true`, 100 concurrent submits → exactly 1 eventfd write. Unit test.
- Busy-poll mode — zero eventfd writes on submit: 1 pipeline, Hybrid,
  keep I/O active, submit 100 writes → 0 eventfd writes from client
  threads. Unit test.

**`DiskIOUring` — cancel**:
- Client-side callback suppression: submit a read with
  `shared_ptr<OpState>` cancel flag, set `cancelled = true` before
  completion → callback fires but returns early; no UAF (ASan clean);
  SQ slot freed. Unit test.
- `cancel_fd` via `IORING_ASYNC_CANCEL_FD`: submit 5 writes for fd on
  a slow device, `cancel_fd(fd)` → all 5 callbacks fire with
  `-ECANCELED`; `in_flight_count(fd) == 0`. Unit test.
- `cancel_fd` does not affect other fds: 2 fds on same pipeline, 3
  writes each, `cancel_fd(fd_a)` → fd_a's 3 callbacks `-ECANCELED`,
  fd_b's 3 callbacks success. Unit test.

**`DiskIOUring` — multi-CQ polling**:
- 2 pipelines, 1 poll thread, both CQs drained: `poll_thread_groups =
  {{0, 1}}`, submit I/O on both → both CQEs dispatched by the same
  thread (verify via TID in callback). Unit test.
- 2 pipelines, 2 poll threads, CQ isolation: `poll_thread_groups =
  {{0}, {1}}`, submit I/O on both → pipeline 0's CQE by thread 0,
  pipeline 1's by thread 1. Unit test.
- Busy-poll → event-wait transition: 1 pipeline, Hybrid,
  `busy_poll_budget = 4`, submit one I/O, wait for completion, then
  no I/O for 5+ iterations → `epoll_wait` appears (syscall trace).
  Then submit new I/O → thread wakes via eventfd, transitions back.
  Unit test.

**`UringEngine` (diskio) — thinned**:
- O_DIRECT alignment validation: `UringEngine` with `BlockDisk`,
  block_size = 4096, submit write with `size = 100` → callback
  `-EINVAL`; `DiskIOUring` never touched. Unit test.
- `cancel_disk` delegates to `cancel_fd`: engine with 1-pipeline
  `DiskIOUring`, submit 3 writes for disk D, `cancel_disk(D)` →
  `uring_.in_flight_count(D->fd()) == 0`; all 3 callbacks
  `-ECANCELED`. Unit test.

**`BlockAsyncPageStore` (btree) — uring-routed**:
- Single-extent write via uring: `BlockPageStore` with one extent,
  `DiskIOUring` (1 pipeline), register extent fd, `submit_write(addr,
  buf, 4096, cb)` → callback `Status::Ok()`; read-back matches. Unit
  test.
- Cross-extent write split: store with 2 extents, block_size = 4096,
  register both fds, `submit_write` spanning both (8192 bytes at
  offset 3072) → two `uring_->submit_write` calls; callback fires
  once with `Status::Ok()` after both CQEs; `AlignedIoBuf` kept alive
  (ASan clean). Unit test.
- Cross-extent write teardown via WriteState cancel flag: 2 extents,
  cross-extent write, simulate extent A failure → set
  `WriteState->cancelled = true` → extent B's callback fires, checks
  flag, returns early; no UAF (ASan clean); `on_complete` invoked once
  with error. Unit test.
- `cancel(op_id)` is a no-op: `submit_write` then `cancel(op_id)` →
  callback still fires normally. Unit test.

**diskio multi-pipeline topology (E2E)**:
- NVMe topology (one pipeline per disk): 2-disk node, 2 pipelines,
  each disk's fd registered to its pipeline. Concurrent writes to
  both → both complete; I/O for disk 1 routes to pipeline 0, disk 2
  to pipeline 1. Bad-disk: fail disk 2, `cancel_disk(disk2)`, disk 1's
  I/O unaffected. E2E test.
- HDD topology (one shared pipeline): 3-HDD node, 1 pipeline, all 3
  fds registered to pipeline 0. Concurrent writes to all 3 → all
  complete on pipeline 0; SQ does not exhaust (entries ≥ 2048).
  Bad-disk: fail disk 2, `cancel_disk(disk2)`, disks 1 and 3 continue.
  E2E test.
- Mixed topology (NVMe + HDD): 1 NVMe + 2 HDD, 2 pipelines (NVMe → 0,
  HDDs → 1), 2 poll threads. Concurrent writes to all 3 → NVMe I/O on
  pipeline 0 (thread 0), HDD I/O on pipeline 1 (thread 1); no
  cross-pipeline routing. E2E test.
- Shared poll thread for 2 pipelines: 2 pipelines,
  `poll_thread_groups = {{0, 1}}`, 1 poll thread. Concurrent I/O on
  both → both CQs drained by same thread; no CQE starvation. E2E
  test.

**btree async via uring (E2E)**:
- `AsyncCrowtree::get` demand-load miss: `Crowtree` with
  `DiskIOUring` (1 pipeline) + `BlockAsyncPageStore`. `get(key)`
  triggering page fault → page loaded via `uring_->submit_read`;
  `drive_ct_future` wakes on pump's `Notify`; value returned
  correctly. E2E test.
- Multi-pipeline btree (future-proofing): `DiskIOUring` with 2
  pipelines, extent fds registered to pipeline 0. Concurrent `get` +
  `flush` → both route to pipeline 0; both pumps' eventfds monitored;
  completions wake shared `Notify`. E2E test.

**FFI multi-eventfd pump (E2E)**:
- 2-pipeline uring, 2 pumps: `Crowtree` with 2-pipeline
  `DiskIOUring` → 2 `EventfdPump` tasks spawned, both share one
  `Arc<Notify>`. Submit I/O on pipeline 1 → `drive_ct_future` wakes
  (pump 1 fires `Notify`); future completes. E2E test.
- Pump spawn failure resilience: simulate pump spawn failure for
  pipeline 1, submit I/O on pipeline 1 → future still completes (via
  `yield_now` fallback or pipeline 0's pump); latency regression but
  no hang. E2E test.

**Batch submit (E2E)**:
- Batch submit under load: 1-pipeline `DiskIOUring`, 32 client
  threads, each submits 100 writes → total `io_uring_enter` syscalls
  ≈ 100 (one per iteration, batching ~32 each), not 3200. Verify via
  syscall count. E2E test.

**Test commands**: `pixi run test-tree-ct` (btree uring tests),
`pixi run cargo test -p crow-tree-ffi` (FFI pump tests),
`pixi run cargo fmt --all -- --check`,
`pixi run cargo clippy --all-targets -- -D warnings`,
`clang-format --dry-run --Werror` (changed `.cpp`/`.h`),
`tree-lint` (clang-tidy, changed C++).

**Open Questions**

- None. All open questions (OQ1-OQ5) are resolved in the working
  design draft (`doc/working/design-uring-pool.md`). OQ6 (this backlog
  doc) is resolved by creating this file.
