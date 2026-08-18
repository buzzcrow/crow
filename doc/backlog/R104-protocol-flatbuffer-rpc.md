<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R104: protocol — Flatbuffer RPC Engine Library

**Problem**

CROW has two RPC needs that gRPC (tonic + h2) serves poorly:

1. **Consensus hot path** — h2 serializes concurrent writers on a
   connection-level userspace lock (HPACK table, frame buffer,
   flow-control windows). Measured cost: ~17% throughput drop at 2T:1C
   (`doc/design/kv/kv-read-flow-analysis.md` ≈L520-545). The loss grows
   with thread:connection ratio. This is a design mismatch, not a
   tuning problem — h2 cannot accept concurrent writers without a lock.

2. **Data-path I/O** — the diskio service (R105) and chunk object
   writers (R94, R106) need to move raw data payloads (MB-scale) over
   RPC with minimal framing overhead. gRPC's per-message HTTP/2 frame
   layer adds unnecessary overhead for large contiguous data transfers,
   and h2's flow-control windows throttle bulk transfers.

Neither path benefits from h2's features (streaming, header compression,
per-stream flow control). Both need a lightweight framing that
separates a small control message from a potentially large raw data
payload, with no connection-level userspace lock.

**Current behavior + impact**: All internal RPC uses gRPC/tonic. The
consensus path pays the h2-lock tax. The diskio service does not exist
yet (R83 and R80 both call out the missing diskio component as an
unlanded dependency). There is no RPC library that supports
control-message + raw-data-payload framing, which the diskio engine
needs for its write/read RPCs (control message = disk/zone/offset/size,
followed by raw bytes).

**Design pointers**: Root protocol design
`doc/design/protocol/design-crow-protocol.md` §1 (Non-Goals: "No
transport encoding") — R104 fills the transport-encoding gap that the
protocol design explicitly left open. The KV RPC sub-design
`doc/design/kv/design-crow-kv-rpc.md` covers the current gRPC wire
protocol; R104 introduces an alternative transport, not a new protocol
schema.

**Use scenarios**:

- **Consensus replica-to-replica**: A follower submits a Paxos accept
  to the leader. The leader's connection carries concurrent accepts
  from N followers. With gRPC, these funnel through the h2 connection
  lock; with R104, each accept is a framed message queued to a
  per-connection writer task — no userspace lock, just a cheap MPSC
  queue push. Expected: linear thread scaling, no 17% loss at 2T:1C.

- **Diskio write**: A chunk writer sends a write request to a diskio
  server. The RPC carries a small flatbuffer control message (disk_id,
  zone, offset, size) followed by the raw data payload (up to 1 MB per
  strip block). The server parses the control message, reads the data
  payload directly into the I/O buffer, and submits to io_uring. No
  extra copy, no h2 frame layer. Expected: data payload lands in the
  I/O buffer without an intermediate serialization step.

- **Diskio read**: A chunk reader sends a read request (disk_id, zone,
  offset, size). The server reads from disk and returns the raw bytes
  as the data payload of the response, with a small flatbuffer status
  message as the control header. Expected: zero-copy data return — the
  read buffer is sent directly as the data payload.

- **Connection lifecycle**: A connection drops mid-operation. The RPC
  library detects the failure, fails in-flight requests with a
  connection-error, and reconnects. Callers retry against the new
  connection. Expected: in-flight requests fail fast (not hang until
  timeout); reconnection is automatic.

- **Scheduled task**: A periodic ping keeps a connection alive. The
  schedule subsystem fires the ping at a fixed interval using a timer
  wheel, without spawning a dedicated thread per timer. Expected: pings
  fire on time under load; no thread-per-timer overhead.

**Solution**

A reusable Rust RPC library crate (`crow-rpc`) that provides
flatbuffer-based framing with control-message + raw-data-payload
separation, a tokio TCP transport with a per-connection lock-free
writer queue, a schedule subsystem for delayed/recurring tasks, and
connection pooling with automatic reconnect. The library is transport-
agnostic at the framing layer and tokio-based at the I/O layer. No h2,
no HTTP, no per-stream state.

**One-line summary**: Custom flatbuffer-over-TCP RPC library with
control+data framing, lock-free per-connection writer, schedule
subsystem, and connection pooling — reusable by consensus (R32) and
diskio (R105).

**Numbered work items**:

1. **Framing layer** (`crow-rpc/src/framing.rs`) — the 20-byte header
   format `[magic:4][msg_type:2][msg_offset:2][msg_size:2][padding:2]
   [create_ms:8]` followed by a flatbuffer control message of
   `msg_size` bytes, followed by an optional raw data payload of
   `data_size` bytes (the `data_size` field lives inside the control
   message, not the header — the header only carries the control
   message size). A `Frame` struct encapsulates header + control +
   data. `FrameCodec` implements `tokio_util::codec::Encoder`/
   `Decoder` for length-delimited framing over `BytesMut`. The magic
   number (`0xCA70F0F`) validates protocol alignment on reconnect.
   This is the wire format; the control message schema is defined per
   service (consensus, diskio) in flatbuffer `.fbs` files, not here.

2. **Connection layer** (`crow-rpc/src/connection.rs`) — a
   `Connection` struct wrapping `tokio::net::TcpStream` with a
   per-connection writer task. The writer task drains a lock-free MPSC
   queue (`tokio::sync::mpsc` or a custom ring buffer) and batches
   sends with `writev` via `tokio::io::AsyncWriteExt::write_vectored`.
   Multiple callers push frames to the queue without holding a mutex —
   the queue push is the only serialization point, vastly cheaper than
   h2's HPACK + stream state + flow-control under a lock. The
   connection also runs a reader task that feeds incoming bytes into
   the `FrameCodec` decoder and dispatches decoded messages to
   per-request-id channels.

3. **Schedule subsystem** (`crow-rpc/src/schedule.rs`) — a
   `ScheduledExecutor` backed by tokio's timer wheel
   (`tokio::time::Interval` / `tokio::time::sleep`). Supports
   `schedule_task(delay, task)` for one-shot delayed tasks and
   `schedule_recurring(interval, task)` for periodic tasks (pings,
   cleanup, retry). No `timerfd` — tokio's timer is already a priority
   queue backed by `mio` timers. This mirrors the reference's
   `scheduled_executor` + `delayed_task` but uses tokio's native timer
   instead of Linux `timerfd` for portability.

4. **Connection pool + reconnect** (`crow-rpc/src/pool.rs`) — a
   `ConnectionPool` managing multiple `Connection`s to the same
   endpoint (or multiple endpoints). Round-robin or least-outstanding
   selection. On connection error, in-flight requests fail with
   `ConnectionError`, and a background reconnect task retries with
   exponential backoff (configurable initial delay, max delay, max
   retries). Reconnected connections are handed back to the pool.
   Timeout: per-request deadline via `tokio::time::timeout`; on
   expiry, the request fails with `TimeoutError` and the connection is
   flagged for health check.

5. **Request/response correlation** (`crow-rpc/src/caller.rs`) — a
   `RemoteCaller` that matches responses to requests by `request_id`
   (u64, monotonic per connection). `call(request_id, control_msg,
   data) -> Future<Response>` stores a `oneshot::Sender` in a
   `DashMap<request_id, Sender>`. The reader task looks up the
   `request_id` in the response frame and resolves the oneshot. Data
   payload in the response is returned as `Bytes` (zero-copy via
   `Bytes::split_to`). Supports both one-way (no response expected)
   and request-response patterns.

6. **Flatbuffer schema + codegen** (`crow-rpc/src/proto/`) — common
   flatbuffer schemas: `msg_type.fbs` (message type enum), `ret_code.fbs`
   (return codes), `common_msg.fbs` (ping request/response). Service-
   specific schemas (consensus, diskio) live in their own crates and
   reference the common types. Codegen via `flatc` or `rust-flatbuffers`
   build script. The `msg_type` enum is extensible — each service
   registers its message type range.

7. **Server side** (`crow-rpc/src/server.rs`) — a `RpcServer` that
   accepts TCP connections, spawns reader/writer tasks per connection,
   and dispatches decoded request frames to a handler function
   registered by message type. The handler receives the control
   message + data payload and returns a response frame (control +
   optional data). Handler dispatch is via a `HashMap<msg_type,
   HandlerFn>` — no per-stream state, no h2 stream multiplexing.

8. **Backpressure** (`crow-rpc/src/connection.rs`) — the MPSC queue has
   a configurable capacity (default 256 frames). When full, `call()`
   returns `BackpressureError` (or awaits, configurable). This
   prevents unbounded queue growth under burst load. The data payload
   is not queued as a separate allocation — it's attached to the frame
   struct and sent via `writev` in the same syscall as the header +
   control message (scatter-gather I/O, zero-copy).

**Flow diagram**:

```
Caller                    crow-rpc                     Server
  │                          │                           │
  │ call(req_id, ctrl, data) │                           │
  │─────────────────────────►│                           │
  │                          │ [Frame: hdr+ctrl+data]    │
  │                          │ push to MPSC queue        │
  │                          │                           │
  │                          │ Writer task:              │
  │                          │ drain queue → writev()    │
  │                          │──────────────────────────►│
  │                          │                           │
  │                          │                     Reader task:
  │                          │                     decode frame
  │                          │                     dispatch by msg_type
  │                          │                     handler(ctrl, data)
  │                          │                           │
  │                          │                     Response frame:
  │                          │                     hdr+ctrl+data
  │                          │◄──────────────────────────│
  │                          │ Reader task:              │
  │                          │ decode, match req_id      │
  │                          │ resolve oneshot           │
  │◄─────────────────────────│                           │
  │ Response(ctrl, data)     │                           │
```

**Edge cases at a glance**:

- Partial `writev` send → writer task continues with remaining iovecs
  on next `writev` call; no frame corruption (frames are length-
  delimited, the writer tracks per-frame send progress).
- Connection drops mid-send → in-flight requests fail with
  `ConnectionError`; reconnect task starts; caller retries.
- Reader gets malformed frame (bad magic) → connection is closed and
  reconnected; in-flight requests fail.
- Request times out → `oneshot::Sender` is dropped, response is
  discarded on arrival (logged as late response).
- MPSC queue full → `BackpressureError` (or await, configurable);
  caller can retry or shed load.
- Server handler panics → connection task catches via
  `tokio::spawn` + `JoinError` handling, closes connection, reconnects.
- Large data payload (1 MB+) → sent as one frame; `writev` handles it
  in one syscall (header + control + data as separate iovecs). No
  chunking needed at the framing layer — TCP handles segmentation.
- Reconnect to a dead endpoint → exponential backoff; after max
  retries, endpoint is marked unhealthy and removed from pool.

**Dependencies**

- **Depends on**: nothing (foundation library). Uses `tokio`,
  `bytes`, `flatbuffers`, `dashmap`, `tokio-util`.
- **Depended on by**:
  - **R32** — KV consensus hot path migrates from gRPC to this library.
  - **R105** — diskio engine uses this library for control+data
    framing over TCP.

**Acceptance**

**Framing**:
- `FrameCodec` encodes a frame with header + 128-byte control message
  + 1 MB data payload → decode yields identical header, control, data.
  Unit test.
- `FrameCodec` rejects a frame with wrong magic number → returns
  `FramingError::BadMagic`. Unit test.
- `FrameCodec` handles partial reads (header split across two TCP
  segments) → reassembles correctly. Unit test (feed bytes in two
  chunks).
- `FrameCodec` handles a frame with `data_size = 0` (control-only
  message) → data payload is `None`. Unit test.

**Connection + writer**:
- Two concurrent `call()` invocations on the same connection → both
  responses received, no interleaving corruption. Integration test
  (local server echoing frames).
- Writer task batches multiple queued frames into one `writev` call
  when frames arrive faster than the socket drains → verify via a
  test that pushes 10 frames and checks the server receives all 10 in
  order. Integration test.
- Connection drop mid-operation → in-flight `call()` returns
  `ConnectionError` within 1 second (not hung until timeout).
  Integration test (kill server mid-call).

**Schedule**:
- `schedule_recurring(10ms, task)` fires the task ~100 times in ~1
  second → count is 100 ± 5. Unit test.
- `schedule_task(50ms, task)` fires exactly once after ~50ms. Unit
  test.
- Schedule subsystem does not spawn a thread per timer → verify
  thread count is stable (1 timer thread) under 1000 concurrent
  scheduled tasks. Unit test.

**Pool + reconnect**:
- Pool with 3 connections, round-robin selection → 6 sequential calls
  hit connections 1,2,3,1,2,3. Integration test.
- Connection drops → reconnect task retries with exponential backoff
  → connection is restored → subsequent calls succeed. Integration
  test (restart server).
- Per-request timeout (100ms) on a server that takes 500ms →
  `TimeoutError` returned at ~100ms. Integration test.

**Request/response**:
- `call(req_id, ctrl, data)` with 1 MB data payload → server receives
  control message + 1 MB data intact. Integration test.
- Server returns response with 1 MB data payload → caller receives
  `Bytes` of correct length, zero-copy (no memcpy in the library).
  Integration test (verify `Bytes::ptr_eq` to the read buffer).
- One-way message (no response) → `call()` returns immediately, server
  receives the message. Integration test.

**Backpressure**:
- MPSC queue capacity 2, push 3 frames rapidly → 3rd `call()` returns
  `BackpressureError` (in non-await mode). Integration test.

**Test commands**: `pixi run cargo test -p crow-rpc`,
`pixi run cargo fmt --all -- --check`,
`pixi run cargo clippy --all-targets -- -D warnings`.

**Open Questions**

- **Flatbuffer vs protobuf for control messages**: The existing CROW
  codebase uses protobuf (prost) for all gRPC schemas. R104 introduces
  flatbuffers for the new RPC library. Alternatives: (a) keep protobuf
  for control messages, only change the transport framing — reuses
  existing `.proto` schemas, no new serialization dependency, but
  protobuf is not zero-copy (prost copies into the message struct); (b)
  flatbuffers for control messages — zero-copy read access, matches
  the reference, but introduces a second serialization format in
  the codebase and requires rewriting schemas as `.fbs`. The user's
  requirement explicitly says "start use flatbuffer to transfer the
  data," so (b) is the intended direction. Decision needed: do we
  migrate existing consensus `.proto` schemas to `.fbs` as part of
  R32, or only use flatbuffers for new (diskio) services and keep
  protobuf for consensus with a prost-to-flatbuffer bridge? This
  affects R32's scope.

- **RDMA transport**: the reference supports both epoll/TCP and RDMA
  transports. R104 v1 is TCP-only (tokio). RDMA is a significant
  addition (rdma-core bindings, pre-registered memory pools, QP
  management). Defer to a future requirement? Or design the
  `Connection` trait to be transport-agnostic from the start (at the
  cost of an extra abstraction layer)?

---

<!-- Reference implementation details: see ~/.codeium/windsurf/memories/global_rules.md -->
