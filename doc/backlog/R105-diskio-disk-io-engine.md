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

**Design pointers**: diskdb root design §2 (Non-Goals: "No data I/O"),
chunkdb root design §2 (Non-Goals: "No data I/O"), chunkdb root design
§5.1 (Disk block — `Segment { node_id, disk_id, zone_index,
zone_offset, size, tag }` is the addressing unit for diskio). The
diskio engine reads/writes at the `Segment` granularity defined by
diskdb's allocation.

**Use scenarios**:

- **Strip block write**: A chunk writer (R94) has a 1 MB data block to
  write to a specific `Segment` on a specific node's disk. It sends a
  write RPC to the diskio server on that node: control message =
  `{disk_id, zone_index, zone_offset, size}`, data payload = 1 MB of
  block data. The diskio server submits the write to io_uring and
  returns success when the I/O completes. Expected: data is durably
  written to the disk at the specified offset.

- **Strip block read**: A chunk reader (R107) needs to read a 1 MB
  block from a `Segment`. It sends a read RPC: control message =
  `{disk_id, zone_index, zone_offset, size}`. The diskio server reads
  from disk and returns the raw bytes as the data payload of the
  response. Expected: the correct bytes are returned with no extra
  copy.

- **Fsync after batch write**: A chunk writer has written 3 mirror
  replicas of a strip and needs to ensure durability before returning
  success to the caller. It sends an fsync RPC: control message =
  `{disk_id}`. The diskio server submits an `io_uring_fsync` and
  returns success. Expected: all prior writes to that disk are
  flushed to stable storage.

- **Recovery read**: R83's recovery flow needs to read a surviving EC
  parity block from a disk on node X. It sends a read RPC to the
  diskio server on node X. The server reads the block and returns it.
  Expected: the block data is returned for EC decode.

- **macOS dev/test**: A developer runs the chunk writer test suite on
  macOS. io_uring is not available. The diskio engine falls back to
  `pwrite`/`pread` (blocking I/O via `spawn_blocking` or a dedicated
  thread pool). Expected: tests pass with the same semantics, lower
  performance.

**Solution**

A disk IO engine binary (`crow-diskio`) that runs on each storage node
and provides read/write/fsync RPCs over the R104 flatbuffer RPC
library. On Linux, the engine uses io_uring directly (SQE/CQE, no
`spawn_blocking`, no thread hop) for maximum throughput. On macOS
(dev/testing only), it falls back to `pwrite`/`pread` via a blocking
thread pool. The RPC framing separates the control message (disk/zone/
offset/size) from the raw data payload — the data payload is written
directly to / read directly from the I/O buffer with no intermediate
serialization.

**One-line summary**: A per-node disk IO engine using io_uring on
Linux (pwrite/pread fallback on macOS) with R104 flatbuffer RPC for
control+data framing — the missing data-I/O component that chunkdb,
the writers, and recovery all depend on.

**Numbered work items**:

1. **io_uring backend** (`crow-diskio/src/io_uring.rs`) — Linux-only.
   Uses the `io-uring` crate (or `liburing` via FFI) to submit
   `IORING_OP_WRITE`, `IORING_OP_READ`, `IORING_OP_FSYNC` SQEs. A
   single io_uring instance (SQ + CQ) per disk, with a completion
   polling thread (or `IORING_SETUP_SQPOLL` for kernel-side polling).
   Each I/O operation is tracked by a `user_data` field in the SQE
   that maps to a `oneshot::Sender<IoResult>` in a `Slab<Sender>`. On
   CQE, the sender is resolved. No `spawn_blocking` — the entire I/O
   path is async, from RPC receive to io_uring submit to CQE
   completion to RPC response. `O_DIRECT` aligned writes for bypassing
   the page cache (configurable, default on for data blocks).

2. **pwrite/pread fallback** (`crow-diskio/src/blocking.rs`) —
   macOS and non-Linux platforms. Uses a dedicated blocking thread
   pool (configurable size, default 4 threads per disk) with
   `std::fs::File::write_at` / `read_at` (`pwrite`/`pread` POSIX
   equivalents). Each I/O operation is submitted to the pool via
   `tokio::task::spawn_blocking`. This is the dev/test path — correct
   semantics, lower performance (thread hop per I/O). The trait
   `IoBackend` abstracts over `Uring` and `Blocking` so the RPC layer
   is backend-agnostic.

3. **IoBackend trait** (`crow-diskio/src/backend.rs`) —
   `async fn write(&self, disk: DiskHandle, offset: u64, data: Bytes)
   -> Result<(), IoError>`, `async fn read(&self, disk: DiskHandle,
   offset: u64, size: usize) -> Result<Bytes, IoError>`,
   `async fn fsync(&self, disk: DiskHandle) -> Result<(), IoError>`.
   `DiskHandle` wraps a file descriptor (Linux) or `File` (macOS) and
   is opened at startup for each disk managed by this node's diskdb.
   The trait is `Send + Sync`; the io_uring backend uses `&self` with
   internal synchronization via the SQ lock.

4. **Disk management** (`crow-diskio/src/disk.rs`) — opens disk
   devices at startup based on the node's disk list from group-0
   (via `HardwareClient`). Each disk is opened with `O_DIRECT |
   O_RDWR` (Linux) or `O_RDWR` (macOS). Disk handles are keyed by
   `DiskId` (128-bit, from diskdb). A `DiskSet` struct holds
   `HashMap<DiskId, DiskHandle>`. Disk health is monitored via
   diskdb's watch/notify (R78) — a `Bad` disk is closed and its
   handle removed; I/O to a `Bad` disk returns `IoError::DiskOffline`.

5. **RPC service** (`crow-diskio/src/rpc.rs`) — uses R104's `RpcServer`
   to handle three message types: `DiskWriteRequest`, `DiskReadRequest`,
   `DiskFsyncRequest`. Each request's control message is a flatbuffer
   with `{disk_id, zone_index, zone_offset, size}`; the write request
   also carries a raw data payload of `size` bytes. The handler
   resolves `disk_id` to a `DiskHandle`, computes the physical offset
   from `zone_index` + `zone_offset` (zone base offset from diskdb's
   zone record), and calls `IoBackend::write`/`read`/`fsync`. The
   response is a flatbuffer status message; read responses include the
   raw data payload. The data payload is passed as `Bytes` from the
   R104 frame decoder directly to `IoBackend::write` — no copy between
   RPC receive and I/O submit.

6. **Flatbuffer schemas** (`lib/crow-protocol/src/proto/diskio.fbs`)
   — `DiskWriteRequest { disk_id, zone_index, zone_offset, size }`,
   `DiskWriteResponse { ret_code }`, `DiskReadRequest { disk_id,
   zone_index, zone_offset, size }`, `DiskReadResponse { ret_code }`
   (data payload follows the control message), `DiskFsyncRequest {
   disk_id }`, `DiskFsyncResponse { ret_code }`. Message type IDs
   registered in R104's `msg_type` enum (diskio range).

7. **Client library** (`lib/crow-diskio-client/`) — a
   `DiskIoClient` that wraps R104's `RemoteCaller` with typed methods:
   `async fn write(&self, segment: &Segment, data: Bytes) ->
   Result<(), IoError>`, `async fn read(&self, segment: &Segment) ->
   Result<Bytes, IoError>`, `async fn fsync(&self, disk_id: &DiskId)
   -> Result<(), IoError>`. The client routes to the correct node's
   diskio server based on `segment.node_id` (from the node's service
   registry entry in group-0). Connection pooling is handled by R104's
   `ConnectionPool`.

8. **Configuration + startup** (`crow-diskio/src/main.rs`) — CLI args
   for node ID, bind address, disk list (or auto-discover from
   group-0), io_uring vs blocking backend selection (auto: io_uring on
   Linux, blocking on macOS), thread pool size (blocking backend),
   `O_DIRECT` toggle. Registers with group-0 service registry on
   startup (same pattern as `crow-kv-server` and `crow-diskdb`).

**Flow diagram**:

```
Chunk Writer (R94/R106)          crow-diskio Server (Node X)
     │                                    │
     │ DiskIoClient::write(seg, data)     │
     │ ──► R104 RPC frame:                │
     │     [hdr][DiskWriteReq][data]      │
     │ ──────────────────────────────────►│
     │                                    │
     │                          R104 reader decodes frame
     │                          handler: resolve disk_id → DiskHandle
     │                          compute phys_offset = zone_base + zone_offset
     │                                    │
     │                                    ▼
     │                          IoBackend::write(disk, phys_offset, data)
     │                              │
     │                    ┌─────────┴──────────┐
     │                    │ io_uring (Linux)   │ blocking (macOS)
     │                    │ SQE submit         │ spawn_blocking
     │                    │ CQE completion     │ pwrite
     │                    └─────────┬──────────┘
     │                                    │
     │                          R104 response: [hdr][DiskWriteResp]
     │ ◄──────────────────────────────────│
     │ write() returns Ok(())             │
```

**Edge cases at a glance**:

- Disk goes `Bad` mid-write → I/O fails with `IoError::DiskOffline`;
  the caller (chunk writer) handles by allocating a new strip via
  chunkdb and retrying.
- io_uring SQ full (submission queue backlog) → the backend awaits
  until a slot is available (backpressure); does not drop the request.
  Configurable SQ size (default 256 entries).
- `O_DIRECT` alignment violation (write size not aligned to 512/4096)
  → `IoError::InvalidAlignment`; the caller must align writes to the
  disk block size.
- Partial write (less than requested bytes) → retried internally for
  the remaining bytes; if still failing after 3 attempts,
  `IoError::PartialWrite`.
- macOS fallback correctness → `pwrite`/`pread` have the same
  semantics as io_uring for aligned I/O; tests verify identical data
  integrity.
- Connection drop during a write → the I/O may still complete on the
  server (io_uring submission is already in flight); the caller gets
  `ConnectionError` and must retry (idempotent write to the same
  offset is safe for the same data).
- Node restart → all disk handles are re-opened at startup; in-flight
  I/O from before the restart is lost (callers retry).

**Dependencies**

- **Depends on**: **R104** (flatbuffer RPC engine) — uses `crow-rpc`
  for framing, connection, server, client. **diskdb** (landed, R72) —
  uses `Segment` type, zone records for physical offset computation.
  **chunkdb** (landed, R85) — uses `DiskId` type, group-0
  `HardwareClient` for disk discovery.
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

**Acceptance**

**io_uring backend (Linux)**:
- `IoBackend::write` with 1 MB aligned data to an `O_DIRECT` disk →
  data is written at the correct offset, verified by `pread` of the
  same range. Integration test (`pixi run cargo test -p crow-diskio
  --test io_uring` — Linux only, skip on macOS).
- `IoBackend::read` of 1 MB from a known offset → returns the correct
  bytes. Integration test (Linux only).
- `IoBackend::fsync` after a write → a subsequent read after process
  restart returns the written data (durability). Integration test
  (Linux only).
- 100 concurrent `write` calls on the same disk → all complete without
  SQ overflow; io_uring SQ size is respected (backpressure, not
  error). Integration test (Linux only).

**Blocking backend (macOS + fallback)**:
- `IoBackend::write` + `IoBackend::read` round-trip with 1 MB data →
  data integrity verified. Integration test (`pixi run cargo test -p
  crow-diskio --test blocking` — runs on all platforms).
- `IoBackend::fsync` after a write → durability verified by re-read.
  Integration test.
- Thread pool size 4, 100 concurrent writes → all complete without
  deadlock; thread pool is not exhausted (spawn_blocking queues).
  Integration test.

**RPC service**:
- `DiskIoClient::write(segment, data)` → server receives the correct
  control message + data payload, writes to disk, returns success.
  Integration test (local diskio server + client).
- `DiskIoClient::read(segment)` → returns the correct bytes as `Bytes`
  (zero-copy from R104 frame decoder). Integration test.
- `DiskIoClient::fsync(disk_id)` → flushes the disk, returns success.
  Integration test.
- Write to a `Bad`/offline disk → `IoError::DiskOffline`. Integration
  test (mark disk bad via diskdb, attempt write).

**Zone offset computation**:
- `DiskWriteRequest` with `{zone_index=2, zone_offset=4096}` →
  physical offset = `zone_base[2] + 4096`. Verified by reading the
  zone record from diskdb and checking the written offset. Integration
  test.

**Alignment**:
- Write with unaligned size (e.g. 100 bytes, not 512-aligned) with
  `O_DIRECT` → `IoError::InvalidAlignment`. Unit test.
- Write with aligned size (4096 bytes) with `O_DIRECT` → success.
  Unit test.

**Test commands**: `pixi run cargo test -p crow-diskio`,
`pixi run cargo test -p crow-diskio-client`,
`pixi run cargo fmt --all -- --check`,
`pixi run cargo clippy --all-targets -- -D warnings`.

**Open Questions**

- **io_uring crate choice**: `io-uring` (pure Rust, unmaintained?),
  `tokio-uring` (tokio-native but requires owned buffers, conflicts
  with `Bytes` zero-copy), or `liburing` via FFI (C dependency, needs
  pixi system dep). The `tokio-uring` owned-buffer model is the
  cleanest async integration but may require copying `Bytes` into
  owned `BufMut` before submit, losing the zero-copy property. Direct
  `liburing` FFI allows passing `Bytes` pointers directly to the SQE
  but adds a C dependency. Trade-off: async ergonomics vs zero-copy vs
  dependency footprint. Needs profiling to decide.

- **SQPOLL vs completion polling**: `IORING_SETUP_SQPOLL` uses a
  kernel thread to poll the SQ, eliminating `io_uring_submit` syscalls
  — but requires the SQ to be busy or the kernel thread to wake. For
  bursty I/O (chunk writes), the syscall overhead of regular submit
  may be negligible. For sustained high-IOPS (recovery scan), SQPOLL
  helps. Default: regular submit; SQPOLL as a config option?

- **Per-disk vs per-node io_uring instance**: One io_uring instance
  per disk isolates SQ/CQ backpressure but uses more kernel resources
  (each instance has its own SQ/CQ rings). One instance per node with
  all disks sharing it is simpler but a full SQ blocks all disks.
  The reference uses one AIO context per disk. Follow that
  pattern?

---

<!-- Reference implementation details: see ~/.codeium/windsurf/memories/global_rules.md -->
