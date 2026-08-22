<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R105: diskio — Disk IO Engine (io_uring + pwrite/pread)

**Problem**

diskdb allocates disk blocks but explicitly does not perform data I/O
(`doc/design/diskdb/design-crow-diskdb.md` §2 Non-Goals: "No data I/O.
A future diskio-like component does data I/O."). chunkdb manages chunk
metadata but also does not perform data I/O
(`doc/design/chunkdb/design-crow-chunkdb.md` §2 Non-Goals: "No data
I/O. chunkdb allocates blocks and manages chunk metadata; it does not
read/write block contents. A future diskio-like component does data
I/O."). Both designs point to a "future diskio service" that does not
exist.

Without a disk IO engine:

- The chunk object writers (R94, R106) have no way to write data to
  the disk blocks that chunkdb allocated. The writers can call chunkdb
  to get chunk metadata (strip layout, segment locations), but there
  is no service that actually performs the `pwrite`/`io_uring_submit`
  to the physical disk.
- R83 (chunkdb complete recovery flow) cannot rebuild lost data — it
  needs to read surviving replicas/parity and write rebuilt data, which
  requires data I/O.
- R80 (diskdb rebalance) defers real data relocation to "a future
  diskio service."

**Current behavior + impact**: There is no diskio component. The
chunkdb design (R85) ships with chunk metadata management but no data
path. R83 and R80 both block on the diskio service. The chunk object
writers (R94, R106) cannot be built without it. This is the
foundational missing piece for CROW's data path.

A mature io_uring reactor already exists in C++
(`lib/crow-tree/src/reactor.cpp`,
`lib/crow-tree/include/crow-tree/reactor.h`) — a dedicated io_uring
event-loop thread that submits read/write/fsync SQEs and dispatches
CQE completions to per-op callbacks. It is Linux-only (guarded by
`CROW_TREE_HAVE_LIBURING`), uses `liburing` (in the pixi environment as
`liburing`), handles `O_DIRECT` aligned I/O, and is wired into
crow-tree's `BlockAsyncPageStore`, `persist.cpp`, and `c_api.cpp`. It
is currently nested inside `crow-tree` but is a generic io_uring event
loop, not tree-specific. R105 lifts it into `crow-common` so both the
btree page store and the diskio engine share one reactor
implementation.

**Design pointers**: diskdb root design §2 (Non-Goals: "No data I/O"),
chunkdb root design §2 (Non-Goals: "No data I/O"), chunkdb root design
§5.1 (Disk block — `Segment { node_id, disk_id, zone_index,
zone_offset, size, tag }` is the addressing unit for diskio). The
diskio engine reads/writes at the `Segment` granularity defined by
diskdb's allocation. RPC root design
(`doc/design/rpc/design-crow-rpc.md`) — crow-rpc is a C++ engine with
a C ABI + Rust FFI facade; the diskio server uses the C++ server side
directly, the Rust client uses `crow-rpc/ffi`.

**Use scenarios**:

- **Strip block write**: A chunk writer (R94) has a 1 MB data block to
  write to a specific `Segment` on a specific node's disk. It calls
  `DiskIoClient::write(segment, data)` (Rust). The client builds a
  `DiskWriteRequest` control flatbuffer `{disk_id, zone_index,
  zone_offset, size}` + raw data payload and sends it via `crow-rpc-ffi`.
  The diskio server on that node receives the frame, resolves `disk_id`
  to a `DiskHandle`, computes the physical offset from `zone_index` +
  `zone_offset`, submits the write to the `IoEngine`, and returns
  success when the I/O completes. Expected: data is durably written to
  the disk at the specified offset.

- **Strip block read**: A chunk reader (R107) needs to read a 1 MB
  block from a `Segment`. It calls `DiskIoClient::read(segment)` (Rust,
  optionally with a `logical_object_offset` for mem-disk verification).
  The server reads from disk and returns the raw bytes as the data
  payload of the response. Expected: the correct bytes are returned
  with no extra copy.

- **Fsync after batch write**: A chunk writer has written 3 mirror
  replicas of a strip and needs to ensure durability before returning
  success to the caller. It calls `DiskIoClient::fsync(disk_id)`. The
  server submits an fsync to the `IoEngine` and returns success.
  Expected: all prior writes to that disk are flushed to stable
  storage.

- **Recovery read**: R83's recovery flow needs to read a surviving EC
  parity block from a disk on node X. It calls `DiskIoClient::read` on
  node X's diskio server. The server reads the block and returns it.
  Expected: the block data is returned for EC decode.

- **macOS dev/test**: A developer runs the diskio test suite on macOS.
  io_uring is not available. The `IoEngine` falls back to
  `BlockingEngine` — a dedicated C++ thread pool with `pwrite`/`pread`
  and configurable `fdatasync`. Expected: tests pass with the same
  semantics, lower performance. (macOS has POSIX `aio` but it is
  weak — internally thread-pool-based, limited queue depth, inconsistent
  across filesystems; `libaio` is Linux-only. A thread-pool
  `pwrite`/`pread` engine is the pragmatic cross-platform fallback and
  also serves as the Linux non-liburing production path.)

- **Mem-disk throughput bench**: An operator benchmarks the diskio
  RPC + engine path without real disk capacity limits. A `MemDisk`
  type receives writes and drops them; reads return deterministic
  content rebuilt from `(disk_id, zone, zone_offset, size,
  logical_object_offset)` by a content rule — so read verification
  works without storing data. Expected: bench measures the full
  RPC + engine path at memory speed, with read-back integrity checks.

- **Simulated-failure test**: A test marks a disk as `SimulatedDisk`
  with a configured error rate and latency. The `SimulatedEngine`
  wraps a real/mem engine and injects delays and I/O errors per the
  disk's properties. Expected: the caller observes `IoError` at the
  configured rate and the configured latency, exercising retry/error
  paths without real hardware faults.

**Solution**

A C++ disk IO engine binary (`app/crow-diskio`, CMake-built, mirroring
`app/crow-diskdb`'s structure) that runs on each storage node and
provides read/write/fsync RPCs over the crow-rpc C++ server. On Linux,
the engine uses io_uring directly via a shared reactor in `crow-common`
(no `spawn_blocking`, no thread hop, no Rust→C++ round-trip on the data
path). On macOS / non-liburing Linux, it falls back to a C++
`BlockingEngine` (thread pool + `pwrite`/`pread` + configurable
`fdatasync`). The RPC framing separates the control message
(disk/zone/offset/size) from the raw data payload — the data payload is
written directly to / read directly from the I/O buffer with no
intermediate serialization. A Rust client crate
(`lib/crow-diskio-client`) wraps `crow-rpc-ffi` with typed
`DiskIoClient` methods.

The architecture mirrors the reference `buzz-disk-io` project
(`/cjdata/cpp/buzz-cpp/src/app/buzz-disk-io`): a `DiskSet` + `Disk` +
`Zone` model, an `IoEngine` virtual base with multiple implementations
(uring / blocking / dummy / simulated), and a server msg-handler that
dispatches `DiskWriteRequest`/`DiskReadRequest`/`DiskFsyncRequest` to
the engine and submits the response on completion.

**One-line summary**: A per-node C++ disk IO engine using io_uring on
Linux (thread-pool pwrite/pread fallback elsewhere) with crow-rpc C++
server + Rust client via `crow-rpc-ffi`, sharing a lifted io_uring
reactor in `crow-common` with the btree page store — the missing
data-I/O component that chunkdb, the writers, and recovery all depend
on.

**Numbered work items**:

1. **Lift io_uring Reactor to crow-common**
   (`lib/crow-common/cpp/{include,src}/crow-common/reactor.{h,cpp}`)
   — relocate the existing `crow::tree::Reactor` from `lib/crow-tree`
   into `crow-common/cpp`, rename the namespace to `crow::common`, and
   move the `CROW_TREE_HAVE_LIBURING` guard + `find_path(LIBURING...)`
   conditional link from `lib/crow-tree/CMakeLists.txt` into
   `lib/crow-common/cpp/CMakeLists.txt` as `CROW_HAVE_LIBURING`. The
   reactor is a generic io_uring event loop (read/write/fsync SQE +
   CQE callback + eventfd) — not tree-specific. Update `crow-tree`
   includes (`async_page_store.h`, `block_page_store.h`, `options.h`,
   `crow-tree.h`, `c_api.h`, `reactor.cpp`, `block_async_page_store.cpp`,
   `persist.cpp`, `c_api.cpp`, `crow-tree.cpp`) to the new header path
   + namespace. Update `crow-tree-ffi`'s `ct_reactor_eventfd` binding
   (`lib/crow-tree/ffi/src/sys.rs`) to the relocated C ABI symbol.
   This is the foundation both the btree page store and the diskio
   engine build on; R66 (WAL io_uring) later adds Rust async submit
   wrappers over the same `crow-common` reactor C ABI. Pure
   relocation + rename — no behavior change, no new features. This
   is a standalone commit; `crow-tree` consumers are updated in the
   same commit.

1b. **Polling modes** (`lib/crow-common/cpp/src/crow-common/reactor.cpp`)
   — add a `PollingMode` config per reactor instance, building on the
   relocated reactor from work item 1a:
   `Wait` (current behavior: `io_uring_wait_cqe_timeout` every
   iteration, 50ms timeout — default for backward compat),
   `Hybrid { busy_poll_budget }` (busy-poll the CQ ring's shared
   memory via `io_uring_peek_cqe` with no syscall while I/O is
   active; after `busy_poll_budget` consecutive empty peeks,
   transition to `io_uring_wait_cqe_timeout` event-wait mode; any
   CQE resets the counter, returning to busy-poll — gives sub-µs
   CQE dispatch during I/O bursts, sleeps when idle, no core burned
   at idle), `Sqpoll { sq_thread_idle }` (opt-in for sustained
   high-IOPS: `IORING_SETUP_SQPOLL` eliminates submit syscalls via
   a kernel SQ-poll thread; requires root/CAP_SYS_NOPRIV; the
   reactor sets `IORING_SQ_NEED_WAKEUP` + calls `io_uring_enter()`
   once when transitioning from idle to active). Default: `Wait`
   for existing crow-tree callers (no regression on `test-tree-ct`/
   `test-tree-ffi`); crow-tree's `BlockAsyncPageStore` opts into
   `Hybrid` for read-heavy workloads; diskio's `UringEngine` selects
   `Hybrid` (or `Sqpoll` if configured). Standalone commit on top of
   1a — `Wait` mode is unchanged, new modes are opt-in.

1c. **Batched SQE submission**
   (`lib/crow-common/cpp/src/crow-common/reactor.cpp`) — the current
   `submit_locked` calls `io_uring_submit()` inside the lock for
   every single SQE (N SQEs = N submit syscalls). Add a
   `submit_batch()` path: fill SQEs under the lock without
   immediately calling `io_uring_submit`, and the reactor loop
   submits once per iteration (or when SQ is N entries full). In
   `Hybrid` wait phase, use `io_uring_submit_and_wait(&ring,
   wait_nr)` to combine submit + wait-for-completions in one
   syscall. The existing `submit_read`/`submit_write`/`submit_fsync`
   API stays stable for crow-tree (which uses `Wait` mode where
   per-SQE submit is fine for its lower IOPS); batching is an
   internal optimization that `Hybrid` mode exercises more.
   Standalone commit on top of 1b — internal optimization, no API
   change.

2. **IoEngine abstraction + UringEngine**
   (`app/crow-diskio/src/engine/io_engine.h`,
   `app/crow-diskio/src/engine/uring/uring_engine.{h,cpp}`) — a C++
   virtual base `IoEngine` mirroring the reference
   `buzz::dio::engine::io_engine`
   (`/cjdata/cpp/buzz-cpp/src/app/buzz-disk-io/engine/io_engine.h`):
   `submit_write`/`submit_read`/`submit_fsync` taking a disk handle,
   zone, zone_offset, buffer, size, and a completion callback
   (`std::function<void(int res)>`, `res` is bytes transferred or
   negative `-errno`). `UringEngine` (Linux, `CROW_HAVE_LIBURING`)
   wraps `crow::common::Reactor`. `O_DIRECT` aligned writes
   (configurable, default on for data blocks). No `spawn_blocking` —
   the entire I/O path is async, from RPC receive to reactor submit to
   CQE completion to RPC response. The completion callback resolves
   the request and calls `crow_rpc_server_submit_response`.

   **Reactor topology by disk type** (io_uring does not support
   sharing a CQ ring across separate `io_uring` instances — each
   `io_uring_queue_init` creates its own SQ + CQ pair; but one ring
   can submit I/O for any number of fds, so the answer is fewer rings,
   not shared CQs):
   - **NVMe SSD** (100k+ IOPS/disk): one `Reactor` per disk — high
     IOPS needs SQ headroom; per-disk isolates SQ/CQ backpressure so
     one busy disk's full SQ doesn't block another. Matches the
     reference's one-AIO-context-per-disk pattern.
   - **SATA SSD** (10k-100k IOPS/disk): one `Reactor` per 4-8 disks
     (grouping) — medium IOPS; grouping reduces reactor thread count
     while keeping SQ headroom. For 24-30 SSDs this gives 3-8 rings
     (3-8 CQs), not 24-30.
   - **HDD** (100-200 IOPS/disk): one shared `Reactor` for all HDDs —
     low IOPS; one ring's SQ handles 30 HDDs trivially. Bad-disk
     isolation requires explicit cancellation (see below) — a bad
     disk's in-flight I/O holds SQ slots, and if the SQ fills, good
     disks' I/O is rejected with `-ENOMEM`. The design below prevents
     this.

   **`IORING_SETUP_ATTACH_WQ`** (Linux 5.18+): when using multiple
   rings, this flag makes them share the kernel's io-wq (async worker
   pool) instead of each ring creating its own (~8 kernel threads per
   pool). io-wq is the kernel's fallback for I/O that cannot complete
   inline during SQ processing — e.g., buffered reads needing page
   faults, inode lock contention, VFS blocking. For `O_DIRECT` on
   block devices (CROW's `BlockDisk`), I/O almost always completes
   inline and io-wq is rarely involved; for `BlockDisk` without
   `O_DIRECT` io-wq may be used. Note: io-wq does NOT pull the SQ (that's done by
   `io_uring_submit()` in regular mode or the kernel `io_sq_thread` in
   SQPOLL mode) and does NOT poll the CQ (that's the reactor thread's
   job) — it only executes blocking I/O on behalf of the submission
   path. `ATTACH_WQ` reduces kernel thread count from `N_rings × 8` to
   `8` shared.

   **Bad-disk SQ isolation** (critical for shared-ring HDD topology):
   the SQ ring has N slots; once all N SQEs are in-flight (submitted,
   not yet completed), `io_uring_get_sqe()` returns NULL and new I/O
   is rejected with `-ENOMEM` — even for good disks on the same ring.
   A bad disk that hangs with N in-flight I/Os blocks the entire ring.
   Two-layer fix:
   - **Explicit cancellation on disk-Bad** (primary): when diskdb
     marks a disk `Bad`, the engine submits
     `IORING_OP_ASYNC_CANCEL` SQEs for all in-flight I/O on that disk.
     Requires **per-disk in-flight tracking**
     (`HashMap<DiskId, HashSet<user_data>>` in `UringEngine`) so the
     engine knows which I/Os to cancel. Each cancel references the
     original I/O's `user_data`; the kernel posts a CQE
     (`-ECANCELED` if canceled, or the original result if already
     done). Frees SQ slots immediately. Cancellation is best-effort
     for I/O already in the device queue — the linked timeout is the
     reliable bound.
   - **Linked timeouts on every I/O** (safety net):
     `io_uring_prep_link_timeout` links a timeout SQE to each I/O SQE.
     If the I/O doesn't complete within N ms (configurable per disk,
     default 30s), the kernel cancels it and posts CQEs
     (`-ECANCELED` for the I/O, `-ETIME` for the timeout). Bounds the
     time an SQ slot is held by a slow disk even if diskdb hasn't
     noticed yet (hardware hang with no error reporting). Cost: 2 SQE
     slots per I/O (halves effective SQ capacity) — size the SQ
     accordingly (2048+ for shared HDD ring with 30 disks × queue
     depth 32 = 960 in-flight × 2 = 1920 slots).

   Each `UringEngine` selects `PollingMode::Hybrid` by default (or
   `Sqpoll` if configured for sustained-IOPS workloads like recovery
   scan).

3. **BlockingEngine** (`app/crow-diskio/src/engine/blocking/`)
   — the macOS + non-liburing Linux production path. A dedicated C++
   thread pool (configurable size, default 4 threads per disk) with
   `pwrite`/`pread` (`::pwrite`/`::pread` POSIX) and configurable
   `fdatasync`/`fsync`. Each I/O operation is submitted to the pool;
   the worker thread performs the blocking syscall and invokes the
   completion callback. Correct semantics, lower performance (thread
   hop per I/O). The `IoEngine` trait abstracts over `UringEngine` and
   `BlockingEngine` so the RPC layer is backend-agnostic. This is also
   the `FileDisk` engine (pwrite to a regular file at an offset).

4. **DummyEngine + MemDisk**
   (`app/crow-diskio/src/engine/dummy/dummy_io_engine.{h,cpp}`,
   `app/crow-diskio/src/disk/mem_disk.{h,cpp}`) — the throughput-bench
   path. `MemDisk` receives writes and drops them (no storage); reads
   return deterministic content from a **repeating pattern with a
   cached buffer**. The pattern is generated once (seeded by `disk_id`
   + zone, mixed with `logical_object_offset` when present) into a
   buffer of size `2 × max_read_size` (e.g. 4 MB for a 2 MB max read).
   Any read range is served by `memcpy` from the cached buffer with
   wrap-around (`offset % pattern_len`) — no per-read generation cost,
   no storage per disk, memory-speed reads. The `logical_object_offset`
   (optional field in `DiskReadRequest`) is mixed into the seed so
   different logical objects produce different patterns at the same
   physical offset; when absent, the pattern is purely
   physical-offset-based. Read verification: the caller regenerates
   the same pattern with the same seed + offset and compares.
   `DummyEngine` serves the content from the cached buffer on read and
   immediately succeeds on write. This mirrors the reference
   `dummy_io_engine`
   (`/cjdata/cpp/buzz-cpp/src/app/buzz-disk-io/engine/dummy/dummy_io_engine.cpp`)
   extended with rule-based read content + cached buffer.

5. **SimulatedEngine + SimulatedDisk**
   (`app/crow-diskio/src/engine/simulated/simulated_io_engine.{h,cpp}`,
   `app/crow-diskio/src/disk/simulated_disk.{h,cpp}`) — the
   fault-injection test path. `SimulatedDisk` carries `DiskProperties`
   (`latency_min_ms`, `latency_max_ms`, `error_rate`, throughput cap).
   `SimulatedEngine` wraps another `IoEngine` (real or mem) and
   injects per-I/O **random latency** (uniform draw from
   `latency_min_ms`..`latency_max_ms`, sleep before completing) and
   **errors** (return `-EIO` at the configured `error_rate`) per the
   disk's properties. Per-disk configurable; no per-I/O-type
   differentiation in v1. Useful for exercising retry/error paths in
   chunk writers (R94/R106) and recovery (R83) without real hardware
   faults.

6. **Disk abstraction + DiskSet + Zone**
   (`app/crow-diskio/src/disk/disk.{h,cpp}`,
   `app/crow-diskio/src/disk/disk_set.{h,cpp}`,
   `app/crow-diskio/src/disk/zone.{h,cpp}`) — mirrors the reference
   `disk_set` + `disk` + `zone`
   (`/cjdata/cpp/buzz-cpp/src/app/buzz-disk-io/disk/`). `Disk` is a
   virtual base with subclasses: `BlockDisk` (real block device,
   `O_DIRECT | O_RDWR`), `FileDisk` (regular file, pwrite at offset),
   `MemDisk` (drop-write + rule-based read), `SimulatedDisk` (wraps a
   disk + properties). Each `Disk` owns its `IoEngine` instance
   (selected by disk type + platform: uring on Linux block/file,
   blocking on macOS, dummy for mem, simulated wraps another). `Zone`
   holds `{zone_index, base_offset, capacity, state}`. `DiskSet` holds
   `HashMap<DiskId, shared_ptr<Disk>>`, opened at startup from the
   node's disk list (via `HardwareClient` / diskdb). diskio does not
   track disk status — if an I/O fails, the engine returns the error
   to the caller; the top layer (chunkdb/diskdb) handles the failure
   and stops allocating new blocks on that disk. After all in-flight
   I/O drains, no further I/O is sent to the bad disk. `DiskId` is
   the 128-bit id from diskdb.

7. **RPC service + msg-handler dispatch**
   (`app/crow-diskio/src/rpc/dio_server_msg_handler.{h,cpp}`,
   `app/crow-diskio/src/rpc/msg_disk_write_request.{h,cpp}`,
   `app/crow-diskio/src/rpc/msg_disk_read_request.{h,cpp}`,
   `app/crow-diskio/src/rpc/msg_disk_fsync_request.{h,cpp}`) — uses
   crow-rpc's `RpcServer::register_handler(msg_type, fn)` to handle
   three message types. Each request's control message is a flatbuffer
   with `{disk_id, zone_index, zone_offset, size}` (read also has
   optional `logical_object_offset`); the write request also carries a
   raw data payload of `size` bytes. The handler resolves `disk_id` to
   a `Disk`, computes the physical offset from `zone_index` +
   `zone_offset` (zone base offset from the `Zone` record), and calls
   `IoEngine::write`/`read`/`fsync`. The completion callback builds the
   response and calls `crow_rpc_server_submit_response` — mirroring
   the reference flow (`msg_disk_write_request::run()` →
   `io_engine::submit_write` → `on_aio_complete` → `conn->post_send`,
   `/cjdata/cpp/buzz-cpp/src/app/buzz-disk-io/rpc/proto/msg_disk_write_request.cpp`).
   The read response includes the raw data payload. The data payload is
   passed from the crow-rpc frame decoder directly to
   `IoEngine::write` — no copy between RPC receive and I/O submit.

8. **Flatbuffer schemas**
   (`lib/crow-protocol/src/proto/diskio.fbs`) —
   `DiskWriteRequest { disk_id, zone_index, zone_offset, size }`,
   `DiskWriteResponse { ret_code }`,
   `DiskReadRequest { disk_id, zone_index, zone_offset, size,
   logical_object_offset }` (logical_object_offset optional, default
   absent), `DiskReadResponse { ret_code }` (data payload follows the
   control message), `DiskFsyncRequest { disk_id }`,
   `DiskFsyncResponse { ret_code }`. Message type IDs registered in
   crow-rpc's `msg_type` enum (diskio range). Mirrors the reference
   `FBDiskWriteRequest { id, rpc_create_nano, disk, zone, offset, size }`
   adapted to CROW's `Segment`-based addressing.

9. **Rust client library** (`lib/crow-diskio-client/`) — a
   `DiskIoClient` that wraps `crow-rpc-ffi` with typed methods:
   `async fn write(&self, segment: &Segment, data: Bytes) ->
   Result<(), IoError>`, `async fn read(&self, segment: &Segment,
   logical_object_offset: Option<u64>) -> Result<Bytes, IoError>`,
   `async fn fsync(&self, disk_id: &DiskId) -> Result<(), IoError>`.
   The client routes to the correct node's diskio server based on
   `segment.node_id` (from the node's service registry entry in
   group-0). Connection pooling is handled by `crow-rpc-ffi`'s
   `ConnectionPool`. Follows the existing `crow-diskdb-client` /
   `crow-chunkdb-client` crate pattern.

10. **Configuration + startup** (`app/crow-diskio/src/dio_main.cpp`,
    `app/crow-diskio/src/dio_server.{h,cpp}`,
    `app/crow-diskio/src/dio_config.{h,cpp}`) — CLI args / config file
    for node ID, bind address, disk list (or auto-discover from
    group-0), engine selection (auto: uring on Linux with liburing,
    blocking on macOS), thread pool size (blocking engine),
    `O_DIRECT` toggle, per-disk type + properties (for mem/simulated
    disks). Registers with group-0 service registry on startup (same
    pattern as `crow-kv-server` and `crow-diskdb`). CMake-built
    (`app/crow-diskio/CMakeLists.txt`), links `crowcommon` + `crow-rpc`
    + `crow-protocol` flatbuffer generated headers. Mirrors
    `app/crow-diskdb`'s `conf/` + `src/` + `tests/` layout.

**Flow diagram**:

```
Chunk Writer (R94/R106)          crow-diskio Server (Node X, C++)
     │                                    │
     │ DiskIoClient::write(seg, data)     │
     │ ──► crow-rpc-ffi frame:            │
     │     [hdr][DiskWriteReq][data]      │
     │ ──────────────────────────────────►│
     │                                    │
     │                          crow-rpc reader decodes frame
     │                          handler: resolve disk_id → Disk
     │                          compute phys_offset = zone_base + zone_offset
     │                                    │
     │                                    ▼
     │                          IoEngine::write(disk, phys_offset, data, on_complete)
     │                              │
     │                    ┌─────────┴────────────────────┐
     │                    │ UringEngine (Linux)          │ BlockingEngine (macOS/non-liburing)
     │                    │ crow::common::Reactor        │ thread pool
     │                    │ SQE submit → CQE completion  │ pwrite → callback
     │                    └─────────┬────────────────────┘
     │                              │ (also: DummyEngine = drop+rule-read,
     │                               │  SimulatedEngine = wrap + inject)
     │                                    │
     │                          on_complete → crow_rpc_server_submit_response
     │                          crow-rpc response: [hdr][DiskWriteResp]
     │ ◄──────────────────────────────────│
     │ write() returns Ok(())             │
```

**Edge cases at a glance**:

- Disk I/O fails mid-write → the engine returns the I/O error to
  the caller; the caller (chunk writer) handles by allocating a new
  strip via chunkdb and retrying. diskio does not track disk status
  — the top layer stops allocating new blocks on a failing disk.
- **Bad disk on a shared ring** → a bad disk's in-flight I/O holds SQ
  slots; if the SQ fills, good disks' I/O is rejected with `-ENOMEM`.
  Two-layer fix: (1) explicit cancellation — when diskdb marks a disk
  `Bad`, `UringEngine` submits `IORING_OP_ASYNC_CANCEL` for all
  in-flight I/O on that disk (tracked via per-disk
  `HashMap<DiskId, HashSet<user_data>>`), freeing SQ slots
  immediately; (2) linked timeouts — every I/O has a
  `io_uring_prep_link_timeout` (configurable, default 30s), so even a
  hardware hang that diskdb hasn't noticed is bounded. CQEs are
  independent (the reactor drains all ready CQEs each iteration — a
  slow disk's CQE arriving late doesn't block CQ drain for others),
  but the SQ slot hold is the real risk and the two-layer fix
  addresses it. Cancellation is best-effort for I/O already in the
  device queue; the linked timeout is the reliable bound.
- io_uring SQ full (submission queue backlog) → the reactor awaits
  until a slot is available (backpressure, bounded retry in
  `submit_locked`); does not drop the request. Configurable SQ size
  (default 256 entries; 1024+ for high-IOPS SSD rings).
- `O_DIRECT` alignment violation (write size not aligned to 512/4096)
  → `IoError::InvalidAlignment`; the caller must align writes to the
  disk block size.
- Partial write (less than requested bytes) → engine returns
  `IoError::PartialWrite` immediately; no internal retry. The
  caller decides whether to retry the whole write or fail.
- Blocking backend correctness → `pwrite`/`pread` have the same
  semantics as io_uring for aligned I/O; tests verify identical data
  integrity across `UringEngine` and `BlockingEngine`.
- MemDisk read with `logical_object_offset` → content rule incorporates
  it; without it, rule is physical-offset-only. Read-back integrity
  verified against the same rule.
- SimulatedDisk error injection → `SimulatedEngine` returns `-EIO` at
  the configured rate; caller observes `IoError::Io` and retries.
- Connection drop during a write → similar to a timeout: the client
  does not know the result (the I/O may still complete on the
  server — reactor submission is already in flight). The client
  treats it as a failure and retries; idempotent write to the same
  offset is safe for the same data.
- Node restart → all disk handles are re-opened at startup; in-flight
  I/O from before the restart is lost (callers retry).
- Reactor relocation (work item 1a) → no behavior change for `Wait`
  mode (existing crow-tree default); `crow-tree` page I/O continues
  to work via the relocated `crow::common::Reactor`. `Hybrid`/`Sqpoll`
  modes (1b) are opt-in — crow-tree's `BlockAsyncPageStore` can opt
  into `Hybrid` for read-heavy workloads (btree demand-load reads are
  latency-sensitive), but this is a config choice, not a forced
  change.

**Dependencies**

- **Depends on**: **R104** (crow-rpc, landed) — uses the C++ `RpcServer`
  + handler registry on the server side, `crow-rpc-ffi` on the client
  side. **diskdb** (landed, R72) — uses `Segment` type, zone records
  for physical offset computation. **chunkdb** (landed, R85) — uses
  `DiskId` type, group-0 `HardwareClient` for disk discovery. The
  existing `crow::tree::Reactor` (landed) — relocated to `crow-common`
  as work item 1.
- **Depended on by**:
  - **R94** (large object writer) — uses `DiskIoClient` to write strip
    blocks.
  - **R106** (small object writer) — uses `DiskIoClient` to write
    mirror strip blocks.
  - **R107** (chunk read flow) — uses `DiskIoClient` to read strip
    blocks.
  - **R83** (chunkdb recovery) — uses `DiskIoClient` to read
    surviving replicas and write rebuilt data.
  - **R80** (diskdb rebalance) — uses `DiskIoClient` for real data
    relocation (future, replaces `LogOnly` placeholder).
  - **R66** (WAL io_uring) — builds its Rust async submit wrappers over
    the `crow-common` reactor C ABI introduced by work item 1 (the
    reactor lift is the shared foundation; R66 adds the FFI bridge for
    the WAL, R105 uses the reactor directly from C++).

**Acceptance**

**Reactor lift (work item 1a)**:
- After relocation, `crow::common::Reactor` builds under
  `CROW_HAVE_LIBURING` on Linux and is absent on macOS (guard
  unchanged). Unit test.
- All existing crow-tree tests pass unchanged in `Wait` mode
  (`pixi run test-tree-ct`, `pixi run test-tree-ffi`) — no behavior
  change from the relocation. Integration test.
- `crow-tree-ffi`'s `ct_reactor_eventfd` binding resolves the relocated
  symbol; `test-tree-ffi` async get/flush/snapshot tests pass.
  Integration test.

**Polling modes (work item 1b)**:
- `PollingMode::Hybrid` with `busy_poll_budget=N`: under sustained I/O
  (100 concurrent writes), the reactor stays in busy-poll phase (CQEs
  dispatched with no `wait_cqe` syscall); after I/O stops, it
  transitions to event-wait within `N` empty peeks. Verified by a
  counter that tracks busy-poll vs wait-mode iterations. Unit test
  (Linux only).
- `PollingMode::Sqpoll` with `sq_thread_idle=N`: submit syscalls are
  eliminated (verified by `strace` count = 0 during sustained I/O);
  after `N` ms idle, the kernel SQ-poll thread sleeps and the reactor
  wakes it with one `io_uring_enter(IORING_ENTER_SQ_WAKEUP)`.
  Integration test (Linux only, requires root).

**Batched submission (work item 1c)**:
- Batched submission: 100 SQEs submitted in one `io_uring_submit()` call
  (not 100 calls) — verified by syscall counting. Unit test (Linux
  only).

**UringEngine (Linux, CROW_HAVE_LIBURING)**:
- `UringEngine::write` with 1 MB aligned data to an `O_DIRECT`
  `BlockDisk` → data is written at the correct offset, verified by
  `pread` of the same range. Integration test (`pixi run test-diskio-ct`
  — Linux only, skip on macOS).
- `UringEngine::read` of 1 MB from a known offset → returns the correct
  bytes. Integration test (Linux only).
- `UringEngine::fsync` after a write → a subsequent read after process
  restart returns the written data (durability). Integration test
  (Linux only).
- 100 concurrent `write` calls on the same disk → all complete without
  SQ overflow; reactor SQ size is respected (backpressure, not error).
  Integration test (Linux only).

**Reactor topology + bad-disk isolation (Linux)**:
- Two `BlockDisk`s on one shared `Reactor` (HDD topology): disk A's I/O
  is slow (simulated via `SimulatedDisk` wrapping), disk B's I/O
  completes normally → disk B's CQEs are dispatched without waiting for
  disk A. Integration test (Linux only).
- **SQ fill + explicit cancellation**: fill the SQ with in-flight I/O
  to disk A (simulated hung), then mark disk A `Bad` →
  `UringEngine` submits `IORING_OP_ASYNC_CANCEL` for all of disk A's
  in-flight I/O (tracked via per-disk `HashMap<DiskId,
  HashSet<user_data>>`) → SQ slots are freed → new I/O to disk B
  succeeds (no `-ENOMEM`). Integration test (Linux only).
- **Per-disk in-flight tracking**: after submitting I/O to 3 disks,
  `UringEngine::in_flight(disk_id)` returns the correct count per
  disk; after completion, the count decrements. Unit test (Linux only).
- Linked timeout: I/O to a bad disk (simulated slow) with a 100ms
  linked timeout → the I/O is cancelled by the kernel at ~100ms and
  returns `-ECANCELED`; other I/O on the same ring is unaffected.
  Integration test (Linux only).
- `IORING_OP_ASYNC_CANCEL` on an in-flight I/O → the CQE returns
  `-ECANCELED` (or completes normally if already done — best-effort).
  Unit test (Linux only).
- Per-disk reactor (NVMe topology): one disk's full SQ does not block
  another disk's submits (separate rings). Integration test (Linux
  only).
- `IORING_SETUP_ATTACH_WQ`: two rings with `ATTACH_WQ` share one io-wq
  pool (verified by `/proc/<pid>/task` showing fewer kernel io-wq
  threads than `2 × 8`). Integration test (Linux 5.18+ only).

**SQ full backpressure (Linux)**:
- Tiny-SQ reactor (`ring_entries=4`) + `SimulatedDisk` with
  `latency_max_ms=5000`: submit 4 writes (fills SQ), then a 5th
  write → `submit_locked` enters bounded retry; the 5th write does
  not return `-ENOMEM` immediately, blocks until one of the first 4
  completes (~5s), then succeeds. Integration test (Linux only).
- SQ full + good-disk isolation on shared ring (`ring_entries=8`):
  disk A `latency_max_ms=10000`, disk B `latency_max_ms=1`. Submit
  8 writes to disk A (fills SQ), then 1 write to disk B → disk B's
  write waits for an SQ slot, succeeds when disk A's I/O completes
  (not rejected with `-ENOMEM`). Integration test (Linux only).
- SQ full + explicit cancellation frees slots: same setup, mark
  disk A `Bad` → `UringEngine` submits `IORING_OP_ASYNC_CANCEL` for
  all 8 in-flight I/Os → SQ slots freed → disk B's write completes
  within ~100ms (not ~10s). Integration test (Linux only).

**Partial write**:
- `UringEngine::write` that returns fewer bytes than requested
  (simulated via `SimulatedDisk` short-write injection) → engine
  returns `IoError::PartialWrite` immediately, no internal retry.
  Unit test.

**BlockingEngine backpressure (all platforms)**:
- Thread pool size 2, 100 concurrent writes to `SimulatedDisk` with
  `latency_max_ms=100` → all 100 complete without error; work queue
  backs up (some writes wait) but none are dropped. Integration
  test.

**BlockingEngine (macOS + non-liburing Linux)**:
- `BlockingEngine::write` + `read` round-trip with 1 MB data on a
  `FileDisk` → data integrity verified. Integration test
  (`pixi run test-diskio-ct` — runs on all platforms).
- `BlockingEngine::fsync` after a write → durability verified by
  re-read. Integration test.
- Thread pool size 4, 100 concurrent writes → all complete without
  deadlock; thread pool is not exhausted (work queue backs up, not
  errors). Integration test.

**DummyEngine + MemDisk**:
- `MemDisk` write of 1 MB → dropped (no storage); immediate success.
  Unit test.
- `MemDisk` read of 1 MB with `logical_object_offset` set → returns
  deterministic content from the cached pattern buffer (seed mixed
  with logical offset); read-back integrity verified by regenerating
  the same pattern with the same seed + offset. Unit test.
- `MemDisk` read without `logical_object_offset` → returns
  physical-offset-only pattern content. Unit test.
- Two reads of the same range → identical bytes (pattern is
  deterministic). Unit test.
- `MemDisk` read of 2 MB (max read size) with 4 MB cached buffer →
  served via wrap-around `memcpy` from the cached buffer; no
  per-read generation cost (verified by timing: read latency is
  memcpy-bound, not generation-bound). Unit test.
- `MemDisk` read at offset beyond `pattern_len` → wrap-around
  (`offset % pattern_len`) produces correct content (verified by
  comparing to the equivalent offset within the pattern). Unit test.

**SimulatedEngine + SimulatedDisk**:
- `SimulatedDisk` with error rate 1.0 → every I/O returns
  `IoError::Io`. Unit test.
- `SimulatedDisk` with error rate 0.0 + latency range 5-15 ms → each
  I/O succeeds after a random delay within [5, 15] ms (latency
  observed within range). Unit test.
- `SimulatedDisk` with error rate 0.5 over 1000 I/Os → ~500 errors
  (within tolerance); success/failure distribution is random.
  Unit test.
- `SimulatedDisk` with `latency_min_ms = latency_max_ms = 10` →
  equivalent to fixed 10 ms latency (degenerate case of uniform
  range). Unit test.

**RPC service (C++ server)**:
- `DiskIoClient::write(segment, data)` → server receives the correct
  control message + data payload, writes to disk, returns success.
  Integration test (local diskio server + Rust client).
- `DiskIoClient::read(segment, None)` → returns the correct bytes as
  `Bytes` (zero-copy from crow-rpc frame decoder). Integration test.
- `DiskIoClient::read(segment, Some(logical_offset))` → mem-disk
  returns rule-based content incorporating the logical offset.
  Integration test.
- `DiskIoClient::fsync(disk_id)` → flushes the disk, returns success.
  Integration test.
- Write to a disk that returns I/O errors (simulated via
  `SimulatedDisk` with `error_rate=1.0`) → `IoError::Io` returned to
  the caller. Integration test.

**Zone offset computation**:
- `DiskWriteRequest` with `{zone_index=2, zone_offset=4096}` →
  physical offset = `zone_base[2] + 4096`. Verified by reading the
  zone record from the `Disk` and checking the written offset.
  Integration test.

**Disk types**:
- `BlockDisk` opens a block device with `O_DIRECT | O_RDWR` (Linux) →
  aligned write succeeds. Integration test (Linux only).
- `FileDisk` opens a regular file → `pwrite` at offset succeeds on all
  platforms. Integration test.
- `DiskSet::find_disk(disk_id)` → returns the correct `Disk`;
  unknown `disk_id` → `IoError::DiskNotExist`. Unit test.

**Alignment**:
- Write with unaligned size (e.g. 100 bytes, not 512-aligned) with
  `O_DIRECT` → `IoError::InvalidAlignment`. Unit test.
- Write with aligned size (4096 bytes) with `O_DIRECT` → success.
  Unit test.

**Client (Rust)**:
- `DiskIoClient` routes to the correct node's diskio server based on
  `segment.node_id`. Integration test.
- Connection error → client treats as failure (result unknown, similar
  to timeout); retry succeeds. Integration test.

**Flatbuffer schemas + msg_type registration**:
- `DiskWriteRequest`/`DiskReadRequest`/`DiskFsyncRequest` flatbuffer
  round-trip encode/decode preserves all fields (`disk_id`,
  `zone_index`, `zone_offset`, `size`, `logical_object_offset`).
  Unit test.
- diskio message type IDs registered in crow-rpc's `msg_type` enum
  (diskio range) — server dispatches write/read/fsync to the correct
  handler. Integration test.

**Configuration + startup**:
- diskio server registers with group-0 service registry on startup,
  reporting the service is alive; other services can use this for
  health detection. Integration test.
- diskio server auto-discovers its disk list from group-0 (via
  `HardwareClient`) when no explicit disk list is configured.
  Integration test.

**Node restart**:
- Node restart → all disk handles re-opened at startup; in-flight
  I/O from before restart is lost; client retry handles it (restart
  is fast). Integration test.

**Test commands**: `pixi run test-diskio-ct` (C++ ctest, engine +
server + disk types), `pixi run test-diskio-client` (Rust cargo test,
client crate), `pixi run test-tree-ct` + `pixi run test-tree-ffi`
(reactor relocation regression), `pixi run cargo fmt --all -- --check`,
`pixi run cargo clippy --all-targets -- -D warnings`,
`clang-format --dry-run --Werror` (changed `.cpp`/`.h`),
`tree-lint` (clang-tidy, changed C++).

**Open Questions**

None remaining — all questions resolved (see below).

**Resolved decisions** (moved here from open questions after review):

- **Work item 1 split into 1a/1b/1c**: the reactor work is split into
  three standalone commits — 1a (relocation to `crow-common`), 1b
  (polling modes), 1c (batched submission) — each building on the
  previous, done one by one. See work items 1a/1b/1c.

- **Partial write handling — return error, do not retry internally**:
  when `pwrite`/io_uring returns fewer bytes than requested, the
  engine returns `IoError::PartialWrite` immediately. No internal
  retry. Rationale: for `O_DIRECT` block devices (primary path),
  partial writes indicate hardware errors — retrying is futile; the
  caller (chunk writer R94/R106) has strip-level retry logic and
  decides whether to retry the whole strip or fail it; keeps the
  engine simple — one I/O per call, report the result, caller
  decides.

- **SQ full backpressure test scenarios**: simulated-disk latency
  approach is sufficient — tiny-SQ reactor + `SimulatedDisk` with
  high latency to fill the SQ deterministically, then verify the
  backpressure path (bounded retry, not drop/error). See acceptance
  section "SQ full backpressure" for the three test scenarios. No
  real block-device fault-injection (e.g. `device-mapper`) needed
  for v1.

- **Polling mode**: `Hybrid` busy-poll + event-wait is the default for
  `UringEngine` and opt-in for crow-tree's `BlockAsyncPageStore`
  (read-heavy). `Sqpoll` is opt-in for sustained high-IOPS workloads
  (recovery scan, bulk rebalance) — requires root/CAP_SYS_NOPRIV.
  `Wait` is the backward-compat default for existing crow-tree callers.
  See work item 1b for the `PollingMode` config + hybrid loop design.

- **Reactor topology**: per-disk for NVMe SSD (isolate backpressure),
  per-4-8-disk group for SATA SSD (balance threads vs headroom), one
  shared ring for all HDDs (low IOPS, bad disk doesn't block others —
  CQEs are independent, linked timeouts bound bad-disk latency). See
  work item 2 for the topology table. `IORING_SETUP_ATTACH_WQ` (Linux
  5.18+) shares the kernel async worker pool across rings to reduce
  kernel thread overhead when using multiple rings.

- **Batched submission**: the reactor batches SQEs and submits once
  per loop iteration (or `io_uring_submit_and_wait` in wait phase)
  instead of per-SQE. See work item 1c.

- **MemDisk content rule**: repeating pattern with a cached buffer.
  The pattern is generated once (seeded by `disk_id` + zone) into a
  buffer of size `2 × max_read_size` (e.g. 4 MB for a 2 MB max read).
  Any read range is served by indexing into this buffer with wrap-around
  (`offset % pattern_len`) — no per-read generation cost, no storage
  per disk. The `logical_object_offset` (when present) is mixed into
  the seed so different logical objects produce different patterns at
  the same physical offset. Read verification: the caller regenerates
  the same pattern with the same seed + offset and compares. See work
  item 4.

- **SimulatedDisk latency model**: random latency within a min-max
  range (`latency_min_ms`..`latency_max_ms`), uniform distribution.
  Each I/O draws a random delay from the range before completing (or
  before returning the injected error). Per-disk configurable; no
  per-I/O-type differentiation in v1. See work item 5.
