<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROW - Design: diskio (Overview)

diskio is the per-node data I/O engine. diskdb allocates disk blocks;
chunkdb manages chunk metadata; diskio reads and writes the block
contents. It is a C++ binary (`crow-diskio`) that runs on each storage
node, using io_uring on Linux for asynchronous I/O and a thread-pool
`pwrite`/`pread` fallback on macOS and non-liburing Linux builds. The
server uses the crow-rpc C++ engine directly; Rust callers
(chunkdb writers, recovery, rebalance) use a typed client crate that
wraps `crow-rpc-ffi`.

The io_uring reactor is a generic event loop that also serves the
crow-tree btree page store. It lives in `crow-common` and is shared by
both consumers.

Architecture decisions and rationale are here; implementation detail
(file paths, struct definitions, test design) lives in the working
design draft.

## Table of Contents

- [1. Overview](#1-overview)
- [2. Non-Goals (Design Envelope)](#2-non-goals-design-envelope)
- [3. Key Design Decisions](#3-key-design-decisions)
  - [3.1 C++ server, Rust client](#31-c-server-rust-client)
  - [3.2 io_uring on Linux; thread-pool pwrite/pread fallback](#32-io_uring-on-linux-thread-pool-pwritepread-fallback)
  - [3.3 Control + data separation in the RPC frame](#33-control--data-separation-in-the-rpc-frame)
  - [3.4 No disk status tracking](#34-no-disk-status-tracking)
  - [3.5 Partial writes are errors, not retried internally](#35-partial-writes-are-errors-not-retried-internally)
  - [3.6 Shared reactor in crow-common](#36-shared-reactor-in-crow-common)
- [4. Architecture Overview](#4-architecture-overview)
- [5. IoEngine Abstraction](#5-ioengine-abstraction)
  - [5.1 UringEngine](#51-uringengine)
  - [5.2 BlockingEngine](#52-blockingengine)
  - [5.3 DummyEngine](#53-dummyengine)
  - [5.4 SimulatedEngine](#54-simulatedengine)
- [6. Disk Model](#6-disk-model)
- [7. Reactor](#7-reactor)
  - [7.1 Polling Modes](#71-polling-modes)
  - [7.2 Batched SQE Submission](#72-batched-sqe-submission)
  - [7.3 Reactor Topology](#73-reactor-topology)
  - [7.4 Bad-Disk SQ Isolation](#74-bad-disk-sq-isolation)
- [8. RPC Service](#8-rpc-service)
- [9. Client Library](#9-client-library)
- [10. Invariants](#10-invariants)
- [11. Configuration](#11-configuration)
- [12. References](#12-references)

---

## 1. Overview

diskio is a **per-node data I/O server**. Each storage node runs one
`crow-diskio` process that owns a set of local disks and serves
read/write/fsync RPCs over crow-rpc. Callers (chunkdb's large-object
writer, small-object writer, chunk read flow, recovery, rebalance)
send a `Segment`-based address — `{disk_id, zone_index, zone_offset,
size}` — and diskio translates it to a physical offset and performs
the I/O.

**Language:** C++ server, Rust client. **I/O:** io_uring (Linux),
thread-pool `pwrite`/`pread` (macOS, non-liburing Linux).

**Core goals:**
- **Low-latency data path** — io_uring on Linux with no
  `spawn_blocking`, no thread hop, no Rust→C++ round-trip on the data
  path. The entire I/O path is async: RPC receive → reactor submit →
  CQE completion → RPC response.
- **Cross-platform dev/test** — thread-pool `pwrite`/`pread` fallback
  on macOS gives the same semantics at lower performance, so the test
  suite runs everywhere.
- **Shared reactor** — the io_uring event loop lives in `crow-common`
  and is shared by the btree page store and the diskio engine. One
  implementation, two consumers.
- **Testability** — `MemDisk` (drop-write + rule-based read) for
  throughput benches, `SimulatedDisk` (latency + error injection) for
  fault-path tests. Both run without real hardware.

## 2. Non-Goals (Design Envelope)

- **No disk allocation.** diskdb allocates blocks; diskio reads and
  writes their contents. diskio does not track which blocks are busy
  or free.
- **No disk status tracking.** diskio does not maintain disk health
  state. If an I/O fails, the engine returns the error to the caller;
  the top layer (chunkdb/diskdb) handles the failure and stops
  allocating new blocks on that disk.
- **No chunk metadata.** chunkdb manages chunk/strip metadata; diskio
  is unaware of chunks, strips, or redundancy. It reads and writes
  raw block contents at the `Segment` granularity.
- **No internal retry on partial writes.** A short write (fewer bytes
  than requested) is an error, not retried internally. The caller
  decides whether to retry the whole write or fail the operation.
- **No RDMA or SPDK (v1).** v1 uses Linux io_uring on block devices
  and regular files. RDMA and SPDK are future additions.
- **No streaming RPC.** v1 supports request-response only. The data
  payload (up to the max block size, default 2 MB) fits in one frame.

## 3. Key Design Decisions

### 3.1 C++ server, Rust client

The diskio server is C++ because the io_uring reactor and the
crow-rpc server engine are C++. Running the server in C++ means the
data path has no FFI boundary: RPC receive, frame decode, I/O submit,
CQE completion, and response submit all happen in C++ without a
Rust→C++ round-trip. The Rust client (`crow-diskio-client`) wraps
`crow-rpc-ffi` with typed `DiskIoClient` methods, following the
existing `crow-diskdb-client` / `crow-chunkdb-client` pattern.

### 3.2 io_uring on Linux; thread-pool pwrite/pread fallback

io_uring is the Linux asynchronous I/O interface. It provides
submission queues (SQ) and completion queues (CQ) in shared memory,
allowing batched I/O submission and completion polling with minimal
syscalls. For `O_DIRECT` on block devices, I/O completes inline
during SQ processing in most cases — no kernel thread pool needed.

macOS has no io_uring. POSIX `aio` exists but is weak: internally
thread-pool-based, limited queue depth, inconsistent across
filesystems. `libaio` is Linux-only. A dedicated C++ thread pool with
`pwrite`/`pread` and configurable `fdatasync` is the pragmatic
cross-platform fallback. It also serves as the Linux non-liburing
production path (if liburing is not available at build time).

The `IoEngine` virtual base abstracts over both backends so the RPC
layer is backend-agnostic.

### 3.3 Control + data separation in the RPC frame

A diskio RPC frame carries a small flatbuffer control message
(`{disk_id, zone_index, zone_offset, size}`) and a raw data payload
in separate buffers. crow-rpc's 12-byte header carries both
`msg_size` (control) and `data_size` (data), so the parser knows the
full frame shape after 12 bytes. The data payload is written directly
to / read directly from the I/O buffer with no intermediate
serialization. This mirrors crow-rpc's control+data separation design
(see `design-crow-rpc.md` §2).

### 3.4 No disk status tracking

diskio does not track whether a disk is Good, Bad, or Suspicious. It
opens disk handles at startup and serves I/O until a disk fails. When
an I/O fails, the engine returns the error to the caller. The top
layer (chunkdb/diskdb) is responsible for marking a disk as bad and
stopping new allocations on it. After all in-flight I/O to a failing
disk drains, no further I/O is sent to it.

This keeps diskio simple — it does one thing (data I/O) and reports
results. Disk health policy lives in diskdb, where it belongs.

### 3.5 Partial writes are errors, not retried internally

When `pwrite` or io_uring returns fewer bytes than requested, the
engine returns `IoError::PartialWrite` immediately. No internal retry.

For `O_DIRECT` block devices (the primary path), partial writes
essentially do not occur for correctly aligned I/O. If they do, it
usually indicates a hardware error — retrying the same range is
futile. The caller (chunkdb writer) has strip-level retry logic and
can decide whether to retry the whole strip or fail it. Internal
retry would complicate the completion callback path (resubmit for
remaining bytes, track partial progress, handle retry-failure) for no
benefit.

### 3.6 Shared reactor in crow-common

The io_uring reactor is a generic event loop: it submits
read/write/fsync SQEs and dispatches CQE completions to per-op
callbacks. It is not specific to the btree page store. It lives in
`crow-common` so both the btree page store (`crow-tree`) and the
diskio engine share one implementation. The WAL's io_uring integration
later builds Rust async submit wrappers over the same reactor C ABI.

## 4. Architecture Overview

```
Caller (Rust)                     crow-diskio Server (Node X, C++)
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
     │                    │ UringEngine (Linux)          │ BlockingEngine (macOS)
     │                    │ crow::common::Reactor        │ thread pool
     │                    │ SQE submit → CQE completion  │ pwrite → callback
     │                    └─────────┬────────────────────┘
     │                              │ (also: DummyEngine, SimulatedEngine)
     │                                    │
     │                          on_complete → crow_rpc_server_submit_response
     │                          crow-rpc response: [hdr][DiskWriteResp]
     │ ◄──────────────────────────────────│
     │ write() returns Ok(())             │
```

Components:
- **DiskSet** — holds `HashMap<DiskId, shared_ptr<Disk>>`, opened at
  startup from the node's disk list. Resolves `disk_id` to a `Disk`.
- **Disk** — virtual base with subclasses: `BlockDisk` (real block
  device, `O_DIRECT`), `FileDisk` (regular file), `MemDisk`
  (drop-write + rule-based read), `SimulatedDisk` (wraps a disk +
  fault properties). Each `Disk` owns its `IoEngine` instance.
- **Zone** — `{zone_index, base_offset, capacity, state}`. The
  physical offset for an I/O is `zone.base_offset + zone_offset`.
- **IoEngine** — virtual base: `submit_write`/`submit_read`/
  `submit_fsync` taking a disk handle, physical offset, buffer, size,
  and a completion callback. Implementations: `UringEngine`,
  `BlockingEngine`, `DummyEngine`, `SimulatedEngine`.
- **Reactor** — the shared io_uring event loop in `crow-common`. One
  dedicated thread per reactor instance; submits SQEs and dispatches
  CQEs to per-op callbacks.
- **RPC handler** — dispatches `DiskWriteRequest`/
  `DiskReadRequest`/`DiskFsyncRequest` to the `IoEngine`; the
  completion callback builds the response and submits it via
  `crow_rpc_server_submit_response`.
- **DiskIoClient** — Rust client wrapping `crow-rpc-ffi` with typed
  methods. Routes to the correct node's diskio server based on
  `segment.node_id`.

## 5. IoEngine Abstraction

`IoEngine` is a C++ virtual base:

```
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
};
```

`on_complete` is invoked exactly once with the raw result: `>=0` bytes
transferred, `<0` negative `-errno`. The RPC handler's callback
resolves the request and calls `crow_rpc_server_submit_response`.

### 5.1 UringEngine

Linux only (`CROW_HAVE_LIBURING`). Wraps `crow::common::Reactor`.
`O_DIRECT` aligned writes (configurable, default on for data blocks).
The entire I/O path is async — no `spawn_blocking`, no thread hop.

Each `UringEngine` selects `PollingMode::Hybrid` by default (or
`Sqpoll` if configured for sustained-IOPS workloads like recovery
scan). See §7.1.

Per-disk in-flight tracking (`HashMap<DiskId, HashSet<user_data>>`)
supports explicit cancellation when a disk fails — see §7.4.

### 5.2 BlockingEngine

macOS + non-liburing Linux. A dedicated C++ thread pool (configurable
size, default 4 threads per disk) with `pwrite`/`pread` and
configurable `fdatasync`/`fsync`. Each I/O operation is submitted to
the pool; the worker thread performs the blocking syscall and invokes
the completion callback. Correct semantics, lower performance (thread
hop per I/O). Also the `FileDisk` engine (pwrite to a regular file at
an offset).

### 5.3 DummyEngine

The throughput-bench path. `MemDisk` receives writes and drops them
(no storage); reads return deterministic content from a repeating
pattern in a cached buffer. The pattern is generated once (seeded by
`disk_id` + zone, mixed with `logical_object_offset` when present)
into a buffer of size `2 × max_read_size`. Any read range is served by
`memcpy` from the cached buffer with wrap-around
(`offset % pattern_len`) — no per-read generation cost, no storage per
disk, memory-speed reads. Read verification: the caller regenerates
the same pattern with the same seed + offset and compares.

### 5.4 SimulatedEngine

The fault-injection test path. `SimulatedDisk` carries
`DiskProperties` (`latency_min_ms`, `latency_max_ms`, `error_rate`,
throughput cap). `SimulatedEngine` wraps another `IoEngine` (real or
mem) and injects per-I/O random latency (uniform draw from
`latency_min_ms`..`latency_max_ms`, sleep before completing) and
errors (return `-EIO` at the configured `error_rate`). Per-disk
configurable; no per-I/O-type differentiation in v1.

## 6. Disk Model

`Disk` is a C++ virtual base with subclasses:

- **BlockDisk** — real block device, opened with `O_DIRECT | O_RDWR`
  (Linux). Aligned I/O only. The primary production disk type for
  NVMe/SATA SSDs and HDDs.
- **FileDisk** — regular file, `pwrite`/`pread` at offset. Works on
  all platforms. Used for testing and for disks backed by filesystem
  files.
- **MemDisk** — drop-write + rule-based read. No storage; reads
  return deterministic content from a cached pattern buffer. Used for
  throughput benches that measure the full RPC + engine path without
  real disk capacity limits.
- **SimulatedDisk** — wraps a real or mem disk + `DiskProperties`
  (latency, error rate). Used for fault-injection tests.

Each `Disk` owns its `IoEngine` instance, selected by disk type +
platform: uring on Linux block/file, blocking on macOS, dummy for mem,
simulated wraps another.

`Zone` holds `{zone_index, base_offset, capacity, state}`. The
physical offset for an I/O is `zone.base_offset + zone_offset`. Zone
records come from diskdb's allocation.

`DiskSet` holds `HashMap<DiskId, shared_ptr<Disk>>`, opened at startup
from the node's disk list (via `HardwareClient` / diskdb). `DiskId`
is the 128-bit id from diskdb.

## 7. Reactor

The reactor is a dedicated io_uring event-loop thread. It submits
read/write/fsync SQEs and dispatches each CQE's raw result to the
callback registered at submission time. One thread per reactor
instance; `submit_*()` may be called concurrently from any thread
(the SQ-side production and the callback map are mutex-guarded).

The reactor lives in `crow-common` (`crow::common::Reactor`), guarded
by `CROW_HAVE_LIBURING`. It is Linux-only. On macOS and non-liburing
Linux builds, the reactor is absent and `BlockingEngine` is used
instead.

### 7.1 Polling Modes

A `PollingMode` config per reactor instance:

- **Wait** — current behavior: `io_uring_wait_cqe_timeout` every
  iteration, 50ms timeout. Default for backward compatibility.
  Existing crow-tree callers use this mode with no regression.
- **Hybrid** `{ busy_poll_budget }` — busy-poll the CQ ring's shared
  memory via `io_uring_peek_cqe` with no syscall while I/O is active.
  After `busy_poll_budget` consecutive empty peeks, transition to
  `io_uring_wait_cqe_timeout` event-wait mode. Any CQE resets the
  counter, returning to busy-poll. Gives sub-µs CQE dispatch during
  I/O bursts, sleeps when idle, no core burned at idle. Default for
  `UringEngine` and opt-in for crow-tree's `BlockAsyncPageStore`
  (read-heavy workloads).
- **Sqpoll** `{ sq_thread_idle }` — opt-in for sustained high-IOPS:
  `IORING_SETUP_SQPOLL` eliminates submit syscalls via a kernel
  SQ-poll thread. Requires root/CAP_SYS_NOPRIV. The reactor sets
  `IORING_SQ_NEED_WAKEUP` + calls `io_uring_enter()` once when
  transitioning from idle to active. Useful for recovery scan and
  bulk rebalance.

### 7.2 Batched SQE Submission

The reactor batches SQEs and submits once per loop iteration (or
`io_uring_submit_and_wait` in wait phase) instead of per-SQE. SQEs
are filled under the lock without immediately calling
`io_uring_submit`; the reactor loop submits once per iteration or
when the SQ is N entries full. In `Hybrid` wait phase,
`io_uring_submit_and_wait(&ring, wait_nr)` combines submit +
wait-for-completions in one syscall.

The `submit_read`/`submit_write`/`submit_fsync` API stays stable for
crow-tree (which uses `Wait` mode where per-SQE submit is fine for
its lower IOPS); batching is an internal optimization that `Hybrid`
mode exercises more.

### 7.3 Reactor Topology

io_uring does not support sharing a CQ ring across separate
`io_uring` instances — each `io_uring_queue_init` creates its own SQ +
CQ pair. But one ring can submit I/O for any number of fds, so the
answer is fewer rings, not shared CQs. Topology by disk type:

- **NVMe SSD** (100k+ IOPS/disk): one `Reactor` per disk. High IOPS
  needs SQ headroom; per-disk isolates SQ/CQ backpressure so one busy
  disk's full SQ doesn't block another.
- **SATA SSD** (10k-100k IOPS/disk): one `Reactor` per 4-8 disks
  (grouping). Medium IOPS; grouping reduces reactor thread count
  while keeping SQ headroom. For 24-30 SSDs this gives 3-8 rings, not
  24-30.
- **HDD** (100-200 IOPS/disk): one shared `Reactor` for all HDDs. Low
  IOPS; one ring's SQ handles 30 HDDs trivially. Bad-disk isolation
  requires explicit cancellation and linked timeouts (§7.4).

`IORING_SETUP_ATTACH_WQ` (Linux 5.18+): when using multiple rings,
this flag makes them share the kernel's io-wq (async worker pool)
instead of each ring creating its own (~8 kernel threads per pool).
For `O_DIRECT` on block devices, I/O almost always completes inline
and io-wq is rarely involved; for `FileDisk` (regular files) io-wq
may be used. `ATTACH_WQ` reduces kernel thread count from
`N_rings × 8` to `8` shared.

### 7.4 Bad-Disk SQ Isolation

On a shared ring, a bad disk's in-flight I/O holds SQ slots. If the SQ
fills, good disks' I/O is rejected with `-ENOMEM`. Two-layer fix:

1. **Explicit cancellation** (primary): when a disk fails, the engine
   submits `IORING_OP_ASYNC_CANCEL` SQEs for all in-flight I/O on that
   disk. Per-disk in-flight tracking
   (`HashMap<DiskId, HashSet<user_data>>` in `UringEngine`) records
   which I/Os to cancel. Each cancel references the original I/O's
   `user_data`; the kernel posts a CQE (`-ECANCELED` if canceled, or
   the original result if already done). Frees SQ slots immediately.
   Cancellation is best-effort for I/O already in the device queue —
   the linked timeout is the reliable bound.

2. **Linked timeouts** (safety net): `io_uring_prep_link_timeout`
   links a timeout SQE to each I/O SQE. If the I/O doesn't complete
   within N ms (configurable per disk, default 30s), the kernel
   cancels it and posts CQEs (`-ECANCELED` for the I/O, `-ETIME` for
   the timeout). Bounds the time an SQ slot is held by a slow disk
   even if the failure hasn't been noticed yet (hardware hang with no
   error reporting). Cost: 2 SQE slots per I/O (halves effective SQ
   capacity) — size the SQ accordingly (2048+ for shared HDD ring
   with 30 disks × queue depth 32 = 960 in-flight × 2 = 1920 slots).

CQEs are independent — the reactor drains all ready CQEs each
iteration, so a slow disk's CQE arriving late doesn't block CQ drain
for others. The SQ slot hold is the real risk, and the two-layer fix
addresses it.

## 8. RPC Service

The diskio server uses crow-rpc's `RpcServer::register_handler` to
handle three message types. Message type IDs are in the diskio range
(3600s) of crow-rpc's `msg_type` enum.

Each request's control message is a flatbuffer with
`{disk_id, zone_index, zone_offset, size}` (read also has optional
`logical_object_offset`); the write request also carries a raw data
payload of `size` bytes. The handler:
1. Resolves `disk_id` to a `Disk` via `DiskSet`.
2. Computes the physical offset: `zone.base_offset + zone_offset`.
3. Calls `IoEngine::write`/`read`/`fsync` with the disk handle,
   physical offset, buffer, and a completion callback.
4. The completion callback builds the response and calls
   `crow_rpc_server_submit_response`.

The data payload is passed from the crow-rpc frame decoder directly to
`IoEngine::write` — no copy between RPC receive and I/O submit. The
read response includes the raw data payload.

Flatbuffer schemas (`diskio.fbs`):
- `DiskWriteRequest { disk_id, zone_index, zone_offset, size }`
- `DiskWriteResponse { ret_code }`
- `DiskReadRequest { disk_id, zone_index, zone_offset, size, logical_object_offset }`
  (`logical_object_offset` optional, default absent)
- `DiskReadResponse { ret_code }` (data payload follows the control
  message)
- `DiskFsyncRequest { disk_id }`
- `DiskFsyncResponse { ret_code }`

## 9. Client Library

`DiskIoClient` (Rust, in `crow-diskio-client`) wraps `crow-rpc-ffi`
with typed methods:

```
async fn write(&self, segment: &Segment, data: Bytes) -> Result<(), IoError>
async fn read(&self, segment: &Segment, logical_object_offset: Option<u64>) -> Result<Bytes, IoError>
async fn fsync(&self, disk_id: &DiskId) -> Result<(), IoError>
```

The client routes to the correct node's diskio server based on
`segment.node_id` (from the node's service registry entry in
group-0). Connection pooling is handled by `crow-rpc-ffi`'s
`ConnectionPool`. Follows the existing `crow-diskdb-client` /
`crow-chunkdb-client` crate pattern.

A connection drop during a write is similar to a timeout: the client
does not know the result (the I/O may still complete on the server —
reactor submission is already in flight). The client treats it as a
failure and retries; idempotent write to the same offset is safe for
the same data.

## 10. Invariants

- **I1 (data integrity)**: A successful `write(segment, data)`
  followed by a `read(segment, None)` on the same `Segment` returns
  the same bytes. Holds for `UringEngine`, `BlockingEngine`, and
  `DummyEngine` (with rule-based read content).
- **I2 (durability)**: A `write` followed by `fsync` followed by
  process restart returns the written data on re-read.
- **I3 (offset correctness)**: The physical offset for an I/O is
  `zone.base_offset + zone_offset`, where `zone` is looked up by
  `zone_index` from the `Disk`'s zone records.
- **I4 (no silent drop)**: The reactor does not drop I/O requests. If
  the SQ is full, `submit_locked` enters bounded retry waiting for a
  slot. If the ring is invalid, the completion callback is invoked
  synchronously with `-EIO`.
- **I5 (completion guarantee)**: `on_complete` is invoked exactly
  once per `submit_*` call — either from the reactor thread (CQE
  dispatched) or synchronously (submission failure). Cancellation
  (`IORING_OP_ASYNC_CANCEL`) replaces the original callback with a
  `-ECANCELED` CQE.
- **I6 (bad-disk isolation)**: A bad disk's in-flight I/O does not
  permanently block good disks' I/O on a shared ring. Explicit
  cancellation frees SQ slots; linked timeouts bound the hold time.
- **I7 (partial write is error)**: A short write returns
  `IoError::PartialWrite` immediately. No internal retry. The caller
  decides the next action.

## 11. Configuration

- **node_id** — the node's ID (from group-0 sysdata).
- **bind_address** — crow-rpc listen address + port.
- **disk_list** — explicit disk list, or auto-discover from group-0
  via `HardwareClient`.
- **engine** — `auto` (uring on Linux with liburing, blocking
  otherwise), `uring`, `blocking`, `dummy`, `simulated`.
- **thread_pool_size** — blocking engine thread count per disk
  (default 4).
- **o_direct** — toggle `O_DIRECT` for `BlockDisk` (default on).
- **polling_mode** — reactor polling mode: `wait`, `hybrid`,
  `sqpoll` (default `hybrid` for `UringEngine`).
- **busy_poll_budget** — consecutive empty peeks before transitioning
  to event-wait in `Hybrid` mode.
- **sq_thread_idle** — idle timeout for `Sqpoll` mode.
- **linked_timeout_ms** — per-I/O linked timeout (default 30000).
- **sq_entries** — SQ ring size (default 256; 1024+ for high-IOPS SSD
  rings; 2048+ for shared HDD rings with linked timeouts).
- **per_disk_properties** — `SimulatedDisk` properties (latency,
  error rate) for testing.

The server registers with the group-0 service registry on startup,
reporting the diskio service is alive. Other services use this for
health detection.

## 12. References

- [`design-crow-diskdb.md`](../diskdb/design-crow-diskdb.md) §2 (Non-Goals: "No
  data I/O") — diskdb allocates blocks; diskio reads/writes contents.
- [`design-crow-chunkdb.md`](../chunkdb/design-crow-chunkdb.md) §5.1
  (Disk block — `Segment { node_id, disk_id, zone_index,
  zone_offset, size, tag }`) — the addressing unit for diskio.
- [`../rpc/design-crow-rpc.md`](../rpc/design-crow-rpc.md) §2 (Key
  Design Decisions: control + data separation, native buffer, C ABI +
  oneshot FFI) — the RPC engine diskio builds on.
- [`design-crow-tree.md`](../tree/design-crow-tree.md) — the btree
  page store that shares the reactor.
