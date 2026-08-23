<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROW - Design: RPC Engine (Overview)

Depends on: [`design-crow-protocol.md`](../protocol/design-crow-protocol.md) §1 (Non-Goals: "No transport encoding" — this doc fills that gap)

`crow-rpc` is a reusable RPC library: a C++ engine (framing, I/O,
connection pool, schedule) with a thin Rust FFI wrapper exposing an
async facade to the rest of CROW. The engine is transport-agnostic
behind a `Transport` interface with three implementations — epoll
(Linux), kqueue (macOS), RDMA (Linux, ibverbs) — sharing all framing,
correlation, and pooling code. The buffer model is a ref-counted native
`Buffer` from a C++ `BufferPool`, allocator-agnostic (glibc /
RDMA-registered / future GDS / object handle), designed so the diskio
data path and future RDMA/GDS/S3 flows share one buffer abstraction.
Architecture decisions and rationale are here; transport-specific I/O
detail is in the sub-designs.

## Table of Contents

- [1. Non-Goals](#1-non-goals)
- [2. Key Design Decisions](#2-key-design-decisions)
- [3. Wire Format](#3-wire-format)
- [4. Control Plane](#4-control-plane)
- [5. Schema + Build](#5-schema--build)
- [6. Sub-Design Document Map](#6-sub-design-document-map)

---

## 1. Non-Goals

- **No service-specific schemas.** `crow-rpc` ships only the common
  flatbuffer schemas (`msg_type`, `ret_code`, `common_msg`,
  `common_type`). Diskio, consensus, and other services define their
  own schemas in their own crates.
- **No value serialization policy.** The data payload is raw bytes;
  the caller chooses how to serialize values (protobuf, bincode, raw).
- **No TLS.** CROW runs on a trusted internal cluster network.
- **No client-side retry or topology.** Retry, topology cache, and
  `NotLeaderHint` handling live in `crow-kv-client`, not here.
- **No streaming RPC.** v1 supports request-response and one-way
  messages only. Bidirectional streaming is a future addition.
- **No HTTP/2.** The custom framing layer is simpler and faster for
  the consensus hot path; h2's connection-level lock costs ~17%
  throughput under concurrent writers.

## 2. Key Design Decisions

- **Native ref-counted buffer, not `bytes::Bytes`.** RDMA needs
  `ibv_reg_mr`'d buffers, GDS needs GPU memory. A copy at the
  transport boundary costs ~100µs for a 1 MB strip. The buffer is
  native from day one — `BufferPool`-allocated, ref-counted on the
  wrapper, recycled to the pool when the last reference drops.
- **12-byte self-contained header.** Both `msg_size` and `data_size`
  are in the header — no `DataSizeResolver` indirection. The parser
  knows the full frame shape after 12 bytes.
- **Control + data separation.** A small flatbuffer control message
  and a potentially large raw data payload travel in one frame but
  separate buffers. The hot path sends both in one `writev` / RDMA
  send WR.
- **Transport interface isolates I/O divergence.** TCP and RDMA share
  everything except the I/O loop and buffer registration. One
  `Transport` interface (`submit`, `register_buffer`, `run_loop`), three
  implementations (`EpollEngine`, `KqueueEngine`, `RdmaTransport`);
  framing, correlation, and pooling are shared. `Connection` is a
  single transport-agnostic peer link (send queue, pending-request map,
  parser state) with one type-erased I/O handle (socket fd or
  `ibv_qp*`) — not a class hierarchy. The send queue holds `OutFrame*`
  (request_id, header, control buffer, data buffer); the worker drains
  up to `BATCH_MAX` (default 64) per cycle via scatter-gather.
- **Pull-based zero-copy parser.** `FrameParser` tells the read loop
  *where to read next* — directly into pool-allocated `Buffer`s. No
  scratch buffer, no copy. The same pull API unifies TCP `read()` and
  RDMA recv completions.
- **Per-connection writer, no connection-level lock.** Each connection
  is owned by one worker thread; the send queue is drained without
  locking. Cross-thread submit wakes the worker via eventfd /
  `EVFILT_USER` / CQ event.
- **`folly::ConcurrentHashMap` for request correlation.** Per-connection
  pending-request map; the worker's lookup (read side) is
  contention-free, cross-thread submit hits the lock-free insert/erase
  paths.
- **Worker-thread timer, no thread-per-timer.** Keepalive pings,
  reconnect backoff, and per-request timeouts are all callbacks fired
  by the worker's event loop (timerfd / `EVFILT_TIMER`). 1000
  concurrent scheduled tasks share one timer.
- **C ABI + oneshot channel for FFI.** The C++ engine exposes a stable
  C ABI; the Rust facade submits requests and awaits completions via
  oneshot channels. The C++→Rust callback is O(1) and non-blocking —
  it sends the response handle and returns; tokio does the real work.
- **Flatbuffers for control messages.** Compact, zero-copy on read,
  schema-evolvable. The receiver gets a `&[u8]` view with no
  deserialization step.

## 3. Wire Format

A frame is a 12-byte header followed by an optional flatbuffer control
message and an optional raw data payload. The header is self-contained
— both `msg_size` and `data_size` are read directly from it, no
out-of-band length resolution. Little-endian, field-by-field
serialization (not `memcpy` of the struct, to avoid compiler-layout
dependence).

```
offset: 0       2       4       6           10    11    12           12+msg_size
        ┌───────┬───────┬───────┬───────────┬─────┬─────┬────────────┬───────────┐
        │ magic │msgtype│msgsize│ data_size │m_off│flags│  control   │   data    │
        │ 2B    │ 2B    │ 2B    │   4B      │ 1B  │ 1B  │ (flatbuf)  │  (raw)    │
        └───────┴───────┴───────┴───────────┴─────┴─────┴────────────┴───────────┘
        \____________________ Header (12 B) _______________/
```

Header fields:

- `magic` — `0xCA70` (`u16`). Validates protocol alignment on every
  frame. `msg_size`/`data_size` sanity checks are the second defense.
- `msg_type` — `u16`. Indexes `FBMsgType` (`msg_type.fbs`). Opaque to
  the framing layer; dispatch is the handler's job.
- `msg_size` — `u16`. Control message length. Max 65535, covers every
  flatbuffer control message.
- `data_size` — `u32`. Data payload length. Max 4 GB.
- `msg_offset` — `u8`. Offset from header start to control message.
  Always 12 today; grows if a future header extension adds bytes before
  the control message. Existing field offsets stay frozen.
- `flags` — `u8`. Bit 0: one-way (no response). Bits 1–7: reserved
  (compression, priority) without header format changes.

The control message is a flatbuffer (`common_msg.fbs`); the data payload
is raw bytes the caller fills. Both travel in pool-allocated `Buffer`s
(§2) — the sender releases after the transport confirms the write, the
receiver releases after the handler finishes. The hot path sends all
three parts in one `writev` / RDMA send WR.

`FrameParser` drives receive-side zero-copy: it tells the read loop
where to read next — directly into pool-allocated `Buffer`s, no scratch
buffer. The same pull API unifies TCP `read()` and RDMA recv
completions. Partial reads across TCP segments are resumable.

## 4. Control Plane

### 4.1 Connection Pool + Reconnect

`ConnectionPool` round-robins among healthy connections (`get()`), or
picks a connection for a specific endpoint (`get_for(endpoint)`, used
when a caller targets a known node). On `Connection::close()`, a
reconnect task is scheduled on the worker timer with
`reconnect_initial_delay`; on failure it reschedules with doubled delay
(capped at `reconnect_max_delay`). After `reconnect_max_retries`
(default 0 = infinite), the endpoint is marked unhealthy. The reconnect
task runs on the C++ worker thread, not on a tokio thread.

### 4.2 Request/Response Correlation

`RpcClient` tracks pending requests in a per-connection
`folly::ConcurrentHashMap<request_id, CompletionCallback>`. `call`
allocates a monotonic `request_id`, inserts the callback, submits the
frame; `on_response` looks up the id, invokes the callback, removes the
entry. `call_one_way` skips the pending map. `fail_all` (on connection
close) invokes every pending callback with `ConnectionClosed`.

Per-request timeout is enforced by the worker timer: `call` schedules a
timer task for `request_timeout`; on expiry, if still pending, the
callback fires with `Timeout` and the entry is removed. A late response
then finds no entry and is discarded.

### 4.3 Schedule Subsystem

`ScheduledExecutor` runs on the worker's event loop — no thread-per-
timer. `schedule_task(delay, fn)` and `schedule_recurring(interval, fn)`
push into a priority queue ordered by deadline; `tick()` (called on
timer expiry) pops due entries, runs them, reschedules recurring ones,
resets the timer to the next deadline. 1000 concurrent scheduled tasks
share one timer (timerfd / `EVFILT_TIMER` / CQ-event timer).

### 4.4 Server Side

`RpcServer` listens (TCP socket or RDMA CM id), registers handlers per
`msg_type`, and spawns acceptor + worker threads. The worker looks up
`handlers_[msg_type]`, invokes the handler with the `Frame*` and
`Connection*`, and submits the response frame if the handler returns
one. Ping is registered automatically (`EConnectionPingRequest` →
`ConnectionPingResponse`).

Handlers run on the worker thread. Fast handlers (ping, diskio write
submit) return inline; slow handlers (diskio read waiting on io_uring)
offload to `RpcServer`'s `offload_pool` and return `nullptr` — the
response is submitted asynchronously when the I/O completes. Unknown
`msg_type` → `UnknownMessage` with `ret_code = HaveNotSupport`; handler
throw → error response with `ret_code = Error`; both keep the connection
open.

### 4.5 Backpressure

The per-connection send queue capacity is `send_queue_capacity` (default
256 frames). `Transport::submit` pushes to the queue in one of two
modes per `ConnectionConfig`:

- `Reject` — `try_enqueue`; on full, returns `RpcError::Backpressure`.
  Caller sheds load or retries. Default for the consensus hot path (fail
  fast, let the caller batch).
- `Await` — the caller blocks until the queue has room (via FFI, the
  oneshot future is not resolved until room). Default for the diskio
  data path (large payloads, prefer in-order delivery).

The data buffer is not a separate queue allocation — it's attached to
the `OutFrame` and sent in the same `writev` / RDMA send WR as the
header + control.

## 5. Schema + Build

All flatbuffer schemas live in `crow-protocol` (the single-home rule,
`design-crow-protocol.md` §2). `crow-rpc` ships only the common set
(`msg_type`, `ret_code`, `common_msg`, `common_type`); service-specific
schemas live in their own crates. `flatc` codegen runs via
`crow-protocol`'s `build.rs` (Rust) and `crow-rpc`'s CMake (C++).
Schema evolution is forward/backward compatible by default; `msg_type`
is append-only within each service's range.

### 5.1 Platform Build Matrix

| Platform | Socket Engine | RDMA | Status |
| --- | --- | --- | --- |
| Linux x86_64 | `EpollEngine` | `RdmaTransport` (if libibverbs found) | Full |
| macOS arm64 | `KqueueEngine` | N/A (no RNICs) | TCP only |

CMake probes for libibverbs/librdmacm; if found, `CROW_RPC_HAVE_RDMA=1`
is defined and `RdmaTransport` is compiled. If not found, RDMA sources
are excluded (same pattern as crow-tree's liburing gate). pixi provides
`flatbuffers` (for `flatc`); on Linux, `libibverbs` and `librdmacm` are
optional RDMA deps.

## 6. Flatbuffer Wrapper Convention

**Rule for all services migrating to crow-rpc (R32, R115, R116, R117).**
The flatbuffer control message is a buffer; field access is a direct
memory-offset read through the flatbuffers runtime — no deserialization
into an owned struct, no per-field copy. This rule governs both the C++
server side and the Rust client side.

- **`FB` prefix for flatbuffer types.** All flatbuffer table/enum/struct
  names use the `FB` prefix (`FBDiskWriteRequest`, `FBMsgType`,
  `FBDiskIoRetCode`). This is already established by R104/R105 and is
  mandatory for new schemas.
- **Zero-copy read — no owned intermediate.** The receiver gets a
  `&[u8]` (Rust) or `const uint8_t*` (C++) view of the control buffer.
  Field access is via the flatbuffers generated accessor
  (`flatbuffers::root::<FBT>(buf)` → `fb.field()`, or
  `flatbuffers::GetRoot<FBT>(ptr)` → `fb->field()`). Do **not**
  deserialize the flatbuffer into a separate owned Rust struct or C++
  class that copies each field out of the buffer. The buffer is the
  object; accessors read from it in place.
- **Wrapper classes in `crow-protocol`, defined when required.** When
  a service needs a typed API over a flatbuffer buffer (to encapsulate
  parse + null-check + field access, to add domain logic, or to hide
  the raw generated type from downstream code), define a **wrapper
  class** in `crow-protocol` that holds a reference to the buffer and
  exposes typed accessor methods. The wrapper does **not** copy fields
  — it reads through the flatbuffer root pointer on every accessor
  call. Wrappers are defined when a service needs them (not
  preemptively for every flatbuffer type), and they live in
  `crow-protocol` so every project (crow-kv, crow-diskdb, crow-chunkdb,
  crow-diskio) shares one definition.
- **Wrapper naming.** Wrapper types use the `FB` prefix + the domain
  type name, without a `View`/`Ref` suffix — the `FB` prefix already
  signals "flatbuffer-backed". Example: `FBDiskWriteRequest` is the
  generated flatbuffer table; a wrapper that adds null-safe access +
  domain logic would be `FBDiskWriteRequest` itself if the generated
  type suffices, or a thin extension trait/impl block on the generated
  type. If a separate wrapper struct is needed (e.g. to hold the
  buffer reference + parsed root), name it `FB<Type>Accessor` and put
  it in `crow-protocol`.
- **No extra allocation on the read path.** The control buffer is
  pool-allocated (C++) or `Bytes`-backed (Rust FFI); the wrapper holds
  a reference to it. Accessor calls are pure pointer-offset reads — no
  heap allocation, no `Vec`, no `String` construction unless the
  caller explicitly converts a field to an owned type. The data
  payload (raw bytes after the control message) is the one exception:
  it may be copied into an owned `Vec<u8>` when the caller needs owned
  bytes, because it is not a flatbuffer.
- **Write path: build, finish, attach.** The sender builds the
  flatbuffer with `FlatBufferBuilder` (Rust) or
  `flatbuffers::FlatBufferBuilder` (C++), calls `finish`, and attaches
  the finished bytes to the frame's control buffer. The builder is
  dropped after the buffer is attached — no retained builder state.

### 6.1 Pattern (Rust, client side)

```rust
// In crow-protocol: a wrapper that encapsulates parse + access.
// The generated FBDiskWriteResponse already has accessors; the
// wrapper adds null-safe construction + domain-typed return.
pub struct FBDiskWriteResponseRef<'a> {
    root: flatbuffers::root::Result<'a, FBDiskWriteResponse<'a>>,
}
impl<'a> FBDiskWriteResponseRef<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self { root: flatbuffers::root::<FBDiskWriteResponse>(buf) }
    }
    pub fn ret_code(&self) -> Option<DiskIoRetCode> {
        self.root.as_ref().ok()?.ret_code().try_into().ok()
    }
    pub fn request_id(&self) -> Option<u64> {
        Some(self.root.as_ref().ok()?.id())
    }
}
// Caller: no copy, no owned struct.
let view = FBDiskWriteResponseRef::new(resp.control.bytes());
let code = view.ret_code().unwrap_or(DiskIoRetCode::IoError);
```

### 6.2 Pattern (C++, server side)

```cpp
// In crow-protocol (header): a wrapper that encapsulates GetRoot +
// null check + typed access. The handler calls the wrapper, not
// GetRoot directly.
class FBDiskWriteRequestRef {
  public:
    explicit FBDiskWriteRequestRef(const uint8_t *data, size_t size)
        : root_(size >= 4 ? ::flatbuffers::GetRoot<FBDiskWriteRequest>(data) : nullptr) {}
    bool valid() const { return root_ != nullptr; }
    DiskId disk_id() const { return parse_disk_id(root_->disk_id()); }
    uint32_t zone_index() const { return root_->zone_index(); }
    // ... one accessor per field, reading through root_ in place.
  private:
    const FBDiskWriteRequest *root_;
};
// Handler: no copy, no owned struct.
FBDiskWriteRequestRef req(request->control.data(), request->control.size());
if (!req.valid()) { send_error_response(...); return nullptr; }
DiskId did = req.disk_id();
```

## 7. Sub-Design Document Map

- [`design-crow-rpc-tcp.md`](design-crow-rpc-tcp.md) — socket transport:
  `SocketTransport` shared base, `EpollEngine` (Linux, level-triggered),
  `KqueueEngine` (macOS, `EV_CLEAR` write), worker loop, scatter-gather
  `writev` send, zero-copy receive.
- [`design-crow-rpc-rdma.md`](design-crow-rpc-rdma.md) — RDMA transport:
  `RdmaTransport`, CQ poll loop, `librdmacm` connection setup,
  pre-registered buffer pools.
