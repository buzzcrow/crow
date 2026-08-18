<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Flatbuffer RPC Engine Library (R104)

Backlog: [`doc/backlog/R104-protocol-flatbuffer-rpc.md`](../backlog/R104-protocol-flatbuffer-rpc.md)
Root design: [`doc/design/protocol/design-crow-protocol.md`](../design/protocol/design-crow-protocol.md) §1 (Non-Goals: "No transport encoding" — R104 fills that gap).

This draft covers the implementation design for `crow-rpc`, a reusable
RPC library: a C++ engine (framing, I/O, connection pool, schedule) with
a thin Rust FFI wrapper exposing an async facade to the rest of CROW.
The engine is transport-agnostic behind a `Transport` interface with
three implementations — epoll (Linux), kqueue (macOS), RDMA (Linux,
ibverbs) — sharing all framing, correlation, and pooling code. The
buffer model is a ref-counted native `Buffer` from a C++ `BufferPool`,
allocator-agnostic (glibc / RDMA-registered / future GDS / object
handle), designed so the diskio data path and future RDMA/GDS/S3 flows
share one buffer abstraction.

Nothing is landed yet — R104 is a foundation library with no
dependencies on prior R-items. The design draws on the author's prior
C++ RPC library as a reference;
the core ideas (12-byte header, control+data separation, per-connection
writer, request/response correlation, flatbuffer schemas, ping-in-base)
are ported, but the code is rewritten — the data structures and I/O loop
are new, designed for the unified TCP+RDMA transport and the
ref-counted buffer model. Architecture decisions and rationale are in
the root design; this doc does not repeat them.

---

## 1. Buffer Model

`lib/crow-rpc/cpp/include/crow-rpc/buffer.h`

### 1.1 Why

R104's two use cases both need raw-byte payloads with no copy:
- Consensus hot path — small control messages, high rate.
- Diskio data path — MB-scale raw payloads, written once and sent.

The future targets (RDMA, GDS) all require the data to live in a
transport-specific memory region: RDMA needs `ibv_reg_mr`'d buffers,
GDS needs GPU memory. `bytes::Bytes` (Rust refcounted, glibc-allocated)
cannot back either without a copy at the transport boundary — and a copy
of a 1 MB strip across the FFI boundary costs ~100µs, dwarfing any
scheduler overhead. The buffer must be native from day one.

If S3-over-RDMA is added in the future, the data is fetched from the
object into a real buffer (System) *before* the RPC call — the
RPC library sends bytes, not object references. Object handling belongs
in the storage layer above the RPC library, not in the buffer model.

A second requirement: the same buffer is consumed by multiple flows —
EC computation, SHA256/MD5 checksumming, then RPC send. Each consumer
holds a reference; the buffer recycles to the pool only when the last
reference drops. This is whole-buffer ref-counting on the wrapper object
instance, not slice-based refcounting like `Bytes::split_to`. EC and
checksum read the full buffer; they don't slice it.

### 1.2 Buffer

```cpp
enum class BufferType : uint8_t {
    System,           // aligned system malloc (posix_memalign on Linux/macOS), recycled to pool
    Registered,       // ibv_reg_mr'd, recycled to RDMA pool
};

struct Buffer {
    uint8_t*  data;
    uint32_t  len;         // bytes written (payload length)
    uint32_t  capacity;    // allocated capacity
    BufferType type;
    BufferPool* pool;      // owning pool (for recycle on refcount → 0)
    std::atomic<int32_t>* ref;  // shared refcount slot (pool-allocated)
    ibv_mr*   mr;          // registered memory region (Registered only; nullptr for System)
};
```

Lifecycle: **allocate → write once → ref-count down across consumers →
recycle to pool when ref == 0.**

a. `BufferPool::alloc(capacity)` — returns a `Buffer*` with `ref == 1`,
   `len == 0`. The pool may reuse a recycled buffer or allocate fresh.
b. `Buffer::write(src, len)` — copies `len` bytes into `data`, sets
   `len = len`. Called once. The buffer is not sliced or partially
   written; the caller fills the whole payload.
c. `Buffer::ref()` — increments the refcount, returns a new `Buffer*`
   pointing at the same allocation. Each consumer that needs to hold the
   buffer calls `ref()`; when done, calls `release()`.
d. `Buffer::release()` — decrements refcount. On `ref == 0`, the buffer
   returns to `pool` (recycle, not free — the pool reuses the
   allocation for the next `alloc`). For `Registered`, the MR stays
   registered across recycles (registration is expensive; the pool
   keeps MRs alive for the pool's lifetime).

The refcount is a separate pool-allocated `std::atomic<int32_t>`, not
embedded in the `Buffer` struct — so multiple `Buffer*` handles to the
same allocation share one refcount slot, and the `Buffer` struct itself
is cheap to copy by value if needed (though the API passes `Buffer*`).

### 1.3 BufferPool

```cpp
class BufferPool {
public:
    Buffer* alloc(uint32_t capacity);
    void    recycle(Buffer* buf);   // called when ref → 0
    // stats: pool size, alloc count, recycle count, miss count
};
```

a. `SystemBufferPool` — `posix_memalign` for cache-line alignment (works
   on both Linux glibc and macOS libmalloc), free list of recycled
   buffers keyed by capacity bucket. Default pool for TCP transport.
   Allocation is fast (system malloc); no registration overhead.
b. `RdmaBufferPool` — RDMA memory registration (`ibv_reg_mr`) is
   expensive (~10–100µs per registration), so the pool pre-registers a
   large memory region at construction time and carves buffers out of it
   as offsets. Recycled buffers return to the free list without
   unregistering — the MR stays registered for the pool's lifetime.
   This amortizes registration cost across thousands of alloc/recycle
   cycles. The pool grows by registering additional large regions if the
   initial capacity is exhausted (growth is rare and happens in
   large chunks, not per-buffer).

The RPC library's `call(control, data)` accepts any `Buffer*` regardless
of pool — the transport inspects `Buffer::type` to decide how to send
it. Callers on the RDMA path allocate from `RdmaBufferPool` directly to
avoid a copy; callers on the TCP path use `SystemBufferPool`.

### 1.4 FFI Surface

Rust sees `Buffer` as an opaque handle:

```rust
// ffi/src/buffer.rs
pub struct Buffer { handle: u64 }  // Buffer* cast to u64

impl Buffer {
    pub fn alloc(pool: &BufferPool, capacity: u32) -> Result<Buffer, RpcError>;
    pub fn write(&mut self, data: &[u8]);           // write once
    pub fn ref_clone(&self) -> Buffer;              // increment refcount
    pub fn release(self);                           // decrement, recycle on 0
}

impl Drop for Buffer {
    fn drop(&mut self) { /* release if not already released */ }
}
```

`Buffer` is `Send` but not `Clone` (use `ref_clone` for an explicit
refcount bump). The RAII `Drop` calls `release` if the handle is still
valid — so a Rust `Buffer` that goes out of scope recycles automatically,
matching the C++ lifecycle. `ref_clone` is the explicit multi-consumer
path: EC, checksum, and RPC send each hold their own `Buffer` via
`ref_clone`; when all drop, the pool recycles.

### 1.5 Edge Cases

- `alloc(capacity)` when the pool is exhausted → returns `nullptr` /
  `RpcError::PoolExhausted`. The pool does not grow unbounded; capacity
  is configured. Caller retries or sheds load.
- `write` called twice → assertion failure in debug, no-op in release
  (the second write is ignored). Documented precondition: write once.
- `release` called on a buffer with `ref > 0` → decrements only; the
  buffer recycles when the last reference releases.
- `ref_clone` on a released buffer → undefined behavior (use-after-free).
  The Rust wrapper prevents this via `Drop` semantics; C++ callers must
  manage lifetimes explicitly.
- Pool shutdown while buffers are outstanding → outstanding buffers are
  leaked (not recycled). Acceptable for shutdown; the pool's destructor
  frees the underlying memory regardless of recycle state.

---

## 2. Framing Layer

`lib/crow-rpc/cpp/include/crow-rpc/framing.h`

### 2.1 Why

The consensus hot path pays a ~17% throughput tax under h2 because h2
serializes concurrent writers on a connection-level userspace lock
(HPACK table, frame buffer, flow-control windows). The diskio data path
needs to move MB-scale raw payloads with minimal framing overhead and no
intermediate copy. Both need a framing that separates a small control
message from a potentially large raw data payload, with no
connection-level lock. A custom framing layer is the foundation.

### 2.2 Header Format

The 12-byte header — redesigned from the reference's 20-byte header to
be self-contained (no `DataSizeResolver` needed) and minimal:

```
[magic:2][msg_type:2][msg_size:2][data_size:4][msg_offset:1][flags:1]
```

- `magic` — `0xCA70` (`u16` little-endian). Validates protocol
  alignment on every frame; a mismatch means the stream is corrupted or
  a stale buffer from a previous connection. 2 bytes (65536 values) is
  sufficient — the second line of defense is `msg_size`/`data_size`
  sanity checks, which catch corruption even if magic accidentally
  matches.
- `msg_type` — `u16`. Indexes into the `FBMsgType` enum
  (`lib/crow-protocol/src/proto/msg_type.fbs`). The framing layer treats it as an
  opaque integer; dispatch is the handler's job.
- `msg_size` — `u16`. Control message length in bytes. Max 65535, which
  covers every flatbuffer control message (the largest is
  `FBFetchDiskResponse` with a disk array, still well under 64 KB for
  realistic device sizes). The writer rejects larger control messages.
- `data_size` — `u32`. Data payload length in bytes. Max 4 GB. The
  parser reads this directly from the header — no `DataSizeResolver`
  needed. The framing layer is fully self-contained: it knows both
  `msg_size` and `data_size` after parsing the 12-byte header.
- `msg_offset` — `u8`. Byte offset from the start of the header to the
  start of the control message. Always `HEADER_SIZE` (12) today.
  Forward compatibility: if a future version extends the header,
  `msg_offset` grows and an old receiver skips the extra bytes to find
  the control message. Existing field offsets stay frozen. 1 byte (max
  255) is plenty for any reasonable header extension.
- `flags` — `u8`. Bit flags. Bit 0: one-way message (no response
  expected). Bits 1–7: reserved for future use (compression, priority,
  etc.) without header format changes.

What was removed from the reference's 20-byte header and why:
- `create_ms` (`u64`, 8 bytes) — redundant. Every control message
  carries `rpc_create_nano: uint64` by schema convention
  (`common_msg.fbs`). Latency measurement uses that; the header does not
  need its own timestamp.
- `padding` (`u16`, 2 bytes) — existed only for 8-byte alignment of
  `create_ms`. With `create_ms` gone, no padding needed.
- `magic` reduced 4→2 bytes — sufficient with size-sanity checks as
  second defense.
- `msg_offset` reduced 2→1 byte — max 255 is plenty for header
  extension offset.

What was added:
- `data_size` (`u32`, 4 bytes) — replaces the `DataSizeResolver`
  indirection. The parser is self-contained.
- `flags` (`u8`, 1 byte) — room for one-way/compression/priority flags
  without header format changes.

```cpp
constexpr uint16_t MAGIC = 0xCA70;
constexpr uint8_t  HEADER_SIZE = 12;

struct Header {
    uint16_t magic;
    uint16_t msg_type;
    uint16_t msg_size;
    uint32_t data_size;
    uint8_t  msg_offset;
    uint8_t  flags;
};

// flags bit definitions
constexpr uint8_t FLAG_ONE_WAY = 0x01;
```

Fields are serialized field-by-field (not `memcpy` of the struct) to
avoid compiler-layout dependence — same approach as the reference's
`parse_header`. Little-endian. `data_size` is at offset 6 (2-byte
aligned, not 4-byte); this is fine since fields are extracted
individually, and unaligned 4-byte reads are cheap on x86/arm.

### 2.3 Frame

```cpp
struct Frame {
    Header   header;
    Buffer*  control;   // flatbuffer control message (from pool)
    Buffer*  data;      // raw data payload, nullptr for control-only
};
```

`control` and `data` are pool-allocated `Buffer*`. The framing layer
does not own them — ownership transfers with the frame: the sender
releases the buffers after the transport confirms the write; the
receiver releases them after the handler finishes. This keeps the
buffer lifecycle explicit and pool-driven, not hidden in smart-pointer
magic.

### 2.4 Parser — Pull-Based Zero-Copy

`FrameParser` is a state machine that drives receive-side zero-copy.
Instead of reading into a scratch buffer and copying, the parser tells
the read loop *where to read next* — directly into pool-allocated
`Buffer`s. This unifies the TCP and RDMA receive paths: TCP reads via
`read()` into the parser-provided target; RDMA pre-posts recv WRs into
the same pool buffers. No scratch buffer, no copy on the receive side.

```cpp
enum class ParseState {
    ReadingHeader,
    ReadingControl,
    ReadingData,
};

class FrameParser {
public:
    // Pull API: the read loop calls next_read_target() to get where to
    // read, reads into it, then calls advance(n) to mark bytes consumed.
    // Returns a (ptr, len) pair; len == 0 means the parser needs no more
    // bytes right now (shouldn't happen in practice — the read loop
    // always has bytes to offer).
    struct ReadTarget { uint8_t* ptr; uint32_t len; };
    ReadTarget next_read_target();

    // Mark n bytes as consumed. Transitions state, allocates the next
    // buffer from the pool if needed. Returns a complete Frame* when the
    // frame is done, or nullptr if more bytes are needed.
    Frame* advance(uint32_t bytes_read);

    // Reset to ReadingHeader (after a frame is yielded or on error).
    void reset();

private:
    ParseState state_;
    Header header_;
    uint8_t header_buf_[HEADER_SIZE];  // 12-byte scratch for header
    uint32_t header_offset_;
    Buffer* control_;                  // pool-allocated, filled by read loop
    uint32_t control_offset_;
    Buffer* data_;                     // pool-allocated, filled by read loop
    uint32_t data_offset_;
    BufferPool* pool_;
};
```

State transitions:

a. `ReadingHeader` — `next_read_target()` returns
   `(header_buf_ + header_offset_, HEADER_SIZE - header_offset_)`. The
   read loop reads into the 12-byte header scratch space. `advance(n)`
   appends to `header_offset_`; when `header_offset_ == HEADER_SIZE`,
   parse the header, validate `magic == MAGIC` (else
   `FramingError::BadMagic`), validate `msg_offset >= HEADER_SIZE` (else
   `FramingError::BadOffset`), validate `data_size <= max_data_size`
   (else `FramingError::DataTooLarge`), transition to `ReadingControl`
   (or directly to `ReadingData` if `msg_size == 0`, or yield the frame
   if both `msg_size == 0` and `data_size == 0`).
b. `ReadingControl` — on entering this state, allocate `control_` from
   `pool_` with capacity `header_.msg_size`. `next_read_target()`
   returns `(control_->data + control_offset_, header_.msg_size -
   control_offset_)`. The read loop reads directly into the pool buffer.
   `advance(n)` appends to `control_offset_`; when `control_offset_ ==
   header_.msg_size`, if `header_.data_size == 0`, yield the frame
   (control-only); else allocate `data_` from `pool_` with capacity
   `header_.data_size`, transition to `ReadingData`.
c. `ReadingData` — `next_read_target()` returns `(data_->data +
   data_offset_, header_.data_size - data_offset_)`. The read loop reads
   directly into the pool buffer. `advance(n)` appends to
   `data_offset_`; when `data_offset_ == header_.data_size`, yield the
   complete `Frame`, `reset()` to `ReadingHeader`.

The read loop (§4.5) is a tight cycle: `target = parser.next_read_target()`
→ `n = read(fd, target.ptr, target.len)` → `frame = parser.advance(n)`
→ if `frame`, dispatch. No scratch buffer, no copy. The pool buffer
*is* the receive buffer — the `Frame` handed to the handler points
directly at the bytes the kernel wrote.

For RDMA, the recv path is the same model: pre-posted recv WRs write
into pool-allocated `Buffer`s. When a recv completion fires, the buffer
is handed to the parser's `advance()` as if the bytes were already read
(they are — RDMA wrote them directly). The pull API unifies both
transports.

**Partial reads across TCP segments:** if `read()` returns fewer bytes
than `target.len`, `advance(n)` records the partial progress and
`next_read_target()` on the next call returns the remaining slice of the
same buffer. The parser is resumable — it tracks per-state offsets and
continues where it left off. No frame corruption, no re-read.

### 2.5 Edge Cases

- Wrong magic → `FramingError::BadMagic`, connection closed and
  reconnected (§6).
- `msg_offset < HEADER_SIZE` → `FramingError::BadOffset`, same handling.
- `msg_size == 0` and `data_size == 0` → control-only, data-less frame
  (e.g. one-way ping). Valid.
- `msg_size == 0` and `data_size > 0` → data-only frame (no control
  message). Valid for raw data transfer if a service defines it.
- Partial header across TCP segments → the parser waits in
  `ReadingHeader` until 12 bytes arrive; `next_read_target()` returns
  the remaining slice of the 12-byte scratch space.
- `data_size` exceeds a configurable max (default 4 MB) →
  `FramingError::DataTooLarge`, connection closed. Prevents a malformed
  header from triggering a multi-GB allocation. Validated in
  `ReadingHeader` transition, before any data buffer is allocated.

---

## 3. Transport Interface

`lib/crow-rpc/cpp/include/crow-rpc/transport.h`

### 3.1 Why

TCP and RDMA share everything except the I/O loop and buffer
registration. The `Transport` interface isolates that divergence so
framing, correlation, pooling, and handler dispatch are shared. Three
implementations: `TcpTransport` (epoll, Linux), `KqueueTransport`
(macOS), `RdmaTransport` (ibverbs, Linux). epoll and kqueue share a
common `SocketTransport` base (§4); RDMA is a separate implementation
(§5).

### 3.2 Interface

```cpp
class Transport {
public:
    virtual ~Transport() = default;

    // Submit a frame on a connection (non-blocking).
    // Returns the request_id, or 0 on error.
    virtual uint64_t submit(Connection* conn, Buffer* control, Buffer* data) = 0;

    // Register a buffer for this transport.
    // TCP/kqueue: noop (returns the same pointer).
    // RDMA: ibv_reg_mr, returns the MR-backed Buffer.
    virtual Buffer* register_buffer(Buffer* buf) = 0;

    // Run the I/O loop on a worker thread (blocks).
    virtual void run_loop(Worker* worker) = 0;

    // Shutdown the transport.
    virtual void shutdown() = 0;
};
```

a. `submit` — non-blocking. Pushes the frame to the connection's send
   queue and wakes the worker (eventfd on Linux epoll, EVFILT_USER on
   macOS kqueue, CQ event on RDMA). Returns the assigned `request_id`.
b. `register_buffer` — TCP/kqueue: returns the buffer unchanged. RDMA:
   if the buffer is not already `Registered`, copies it into the
   RDMA pool and returns the registered `Buffer*`; if already
   registered, returns it. This is the one place a copy may happen on
   the send path — only when the caller hands a System buffer to an RDMA
   transport. Callers on the RDMA path allocate from `RdmaBufferPool`
   directly to avoid this.
c. `run_loop` — blocks on the worker thread, driving the I/O. See §4
   (epoll/kqueue) and §5 (RDMA).

### 3.3 Connection

`Connection` is a single peer link — one instance per TCP connection or
RDMA QP pair. It is transport-agnostic: it holds the send queue, the
pending-request map, and the parser state. The transport-specific I/O
handle (socket fd for TCP, QP pointer for RDMA) is stored as a type-
erased `transport_handle` that only the transport interprets.

```cpp
class Connection {
public:
    int64_t id() const;
    const std::string& name() const;
    bool is_open() const;

    // Push a frame to the send queue (called by Transport::submit).
    void enqueue_send(OutFrame* frame);

    // Close the connection, fail pending requests, signal reconnect.
    void close();

    // Called by the parser when a complete frame arrives.
    void on_frame(Frame* frame);

    // User data slot (for caller-side bookkeeping).
    void* user_data;

    // Transport-specific I/O handle. TcpTransport casts to int (socket fd);
    // RdmaTransport casts to ibv_qp* (queue pair). The connection itself
    // never uses this — only the transport's worker loop does.
    uint64_t transport_handle;
};
```

**Why `Connection` is not a class hierarchy** (no `TcpConnection` /
`RdmaConnection` subclasses): the only transport-specific field is the
I/O handle. Everything else — send queue, parser, pending-request map,
close/reconnect logic — is shared. A class hierarchy with one virtual
method and one extra field per subclass is over-engineering. The type-
erased handle keeps `Connection` as one class; the transport casts it
back to the right type inside its worker loop, where the type is known.

The transport's worker thread drains the send queue and does I/O via
`transport_handle`; received bytes go to the parser, which calls
`Connection::on_frame` on each complete frame, dispatching to the
handler (server side) or resolving the pending request (client side).

### 3.4 OutFrame

```cpp
struct OutFrame {
    uint64_t request_id;
    Header   header;
    Buffer*  control;
    Buffer*  data;
};
```

The send queue holds `OutFrame*`. The worker drains up to `BATCH_MAX`
(default 64) per drain cycle and sends them via scatter-gather (§4.3).

---

## 4. Socket Transport — epoll + kqueue

`lib/crow-rpc/cpp/include/crow-rpc/socket_transport.h`
`lib/crow-rpc/cpp/src/socket_transport.cpp`

### 4.1 Why

TCP is the v1 transport. epoll (Linux) and kqueue (macOS) are the two
kernel event interfaces; they differ in API but share the same
event-driven loop structure. A common `SocketTransport` base holds the
shared logic (send queue drain, parser feed, connection management); the
event-dispatch primitives are in `EpollEngine` and `KqueueEngine`
subclasses.

### 4.2 Shared Base — SocketTransport

```cpp
class SocketTransport : public Transport {
public:
    // Transport interface — shared logic
    uint64_t submit(Connection* conn, Buffer* control, Buffer* data) override;
    Buffer*  register_buffer(Buffer* buf) override;  // noop
    void     shutdown() override;

protected:
    // Engine-specific primitives (implemented by EpollEngine / KqueueEngine)
    virtual void arm_read(int fd) = 0;
    virtual void arm_write(int fd) = 0;
    virtual void disarm_write(int fd) = 0;
    virtual void add_connection(int fd, Connection* conn) = 0;
    virtual void remove_connection(int fd) = 0;
    virtual void notify_worker(Worker* worker) = 0;  // wake for cross-thread submit

    // Shared I/O logic
    void on_readable(Connection* conn);   // read → parse → on_frame
    void on_writable(Connection* conn);   // drain send queue → writev
};
```

`on_readable` and `on_writable` are the shared hot path. The engine
subclass tells the base *when* to read/write (via the event loop); the
base does the actual I/O and parsing.

### 4.3 Worker Loop (shared structure)

```
Per worker thread:
  init engine (epoll_fd / kqueue_fd)
  register: eventfd/notify-fd, timerfd/kqueue-timer

  loop:
    events = engine.wait(timeout)      // epoll_wait / kevent
    for event in events:
      if event.fd == listen_socket:    // acceptor worker only
        accept → create Connection → assign to worker (round-robin)
                → engine.add_connection(fd, conn) → arm_read(fd)
      elif event.fd == notify_fd:
        drain cross-thread submit queue
        for each pending send: conn->enqueue_send(frame) → arm_write(fd)
      elif event.fd == timer_fd:
        run due scheduled tasks → reset timer to next deadline
      elif event is READABLE:
        on_readable(conn)              // read → parse → on_frame
      elif event is WRITABLE:
        on_writable(conn)              // drain send queue → writev
        if send queue empty: disarm_write(fd)
      elif event is ERROR/HUP:
        conn->close() → remove_connection(fd) → trigger reconnect
```

Key points:
- **WRITABLE is armed only when there's data to send.** Idle
  connections don't wake the worker. When the send queue has data, arm
  write; when it drains, disarm. This avoids busy-looping. (epoll:
  `EPOLLOUT` add/clear via `epoll_ctl MOD`; kqueue: `EVFILT_WRITE`
  add/delete via `kevent`, with `EV_CLEAR` for edge-triggered.)
- **notify_fd for cross-thread submit.** Rust calls
  `crow_rpc_submit` from a tokio thread → C++ pushes to the connection's
  send queue → writes to the worker's notify_fd → engine wakes → worker
  drains and sends. No locking on the hot path; the notify_fd write is
  the wakeup. (Linux: `eventfd`; macOS: `EVFILT_USER` on kqueue, or a
  pipe fallback.)
- **timer for scheduled tasks.** One timer per worker, set to the next
  deadline. On expiry, run due tasks, reset timer. (Linux: `timerfd`;
  macOS: kqueue `EVFILT_TIMER`.)
- **Connection-to-worker assignment.** Round-robin at accept time. Each
  connection is owned by one worker — no cross-worker locking for that
  connection's I/O.

### 4.4 on_writable — Scatter-Gather Send

```cpp
void SocketTransport::on_writable(Connection* conn) {
    auto& queue = conn->send_queue();
    OutFrame* batch[BATCH_MAX];
    int n = queue.drain(batch, BATCH_MAX);   // lock-free drain

    // Build iovec array: 3 per frame (header, control, data)
    iovec iov[3 * BATCH_MAX];
    int iov_count = 0;
    uint8_t header_bufs[BATCH_MAX][HEADER_SIZE];  // 12-byte header on stack

    for (int i = 0; i < n; i++) {
        serialize_header(header_bufs[i], batch[i]->header);
        iov[iov_count++] = {header_bufs[i], HEADER_SIZE};
        iov[iov_count++] = {batch[i]->control->data, batch[i]->control->len};
        if (batch[i]->data) {
            iov[iov_count++] = {batch[i]->data->data, batch[i]->data->len};
        }
    }

    ssize_t written = writev(conn->fd(), iov, iov_count);
    if (written < 0) { /* EPIPE / error → close */ return; }

    // Advance past written bytes; if partial, keep remaining frames in queue
    advance_iov(iov, iov_count, written, ...);
    // Release buffers for fully-sent frames; keep partially-sent for next on_writable
    release_sent_frames(batch, n, written);
}
```

`writev` sends header + control + data for multiple frames in one
syscall — scatter-gather, zero-copy (the kernel reads directly from the
pool buffers). On partial write, `advance_iov` skips the written bytes
and the remaining iovecs stay queued; the next `on_writable` continues.
Fully-sent frames' buffers are released (refcount decrement → pool
recycle).

### 4.5 on_readable — Receive + Parse (Zero-Copy)

```cpp
void SocketTransport::on_readable(Connection* conn) {
    auto& parser = conn->parser();
    while (true) {
        auto target = parser.next_read_target();  // where to read next
        ssize_t n = read(conn->fd(), target.ptr, target.len);
        if (n <= 0) {
            if (n == 0) { /* EOF → close */ }
            else if (errno == EAGAIN || errno == EWOULDBLOCK) { break; }  // no more data
            else { /* error → close */ }
            return;
        }
        Frame* frame = parser.advance(static_cast<uint32_t>(n));
        if (frame) {
            conn->on_frame(frame);   // dispatch to handler or resolve request
            // parser is now reset to ReadingHeader; loop continues for
            // any remaining bytes in the socket buffer
        }
        // if no frame yielded, loop continues — parser needs more bytes,
        // next_read_target() returns the remaining slice of the current buffer
    }
}
```

No scratch buffer. The `read()` writes directly into the pool-allocated
`Buffer` that the parser provides via `next_read_target()`. The `Frame`
handed to `on_frame` points at the same bytes the kernel wrote —
zero-copy from socket to handler. On `EAGAIN`, the loop breaks;
level-triggered epoll will wake again when more data arrives.

### 4.6 EpollEngine (Linux)

```cpp
class EpollEngine {
    int epoll_fd_ = epoll_create1(0);
    // arm_read:  epoll_ctl(EPOLL_CTL_MOD, EPOLLIN)
    // arm_write: epoll_ctl(EPOLL_CTL_MOD, EPOLLIN | EPOLLOUT)
    // disarm_write: epoll_ctl(EPOLL_CTL_MOD, EPOLLIN)
    // wait: epoll_wait(epoll_fd_, events, max, timeout_ms)
};
```

Level-triggered (not edge-triggered) — simpler correctness model, the
worker re-arms write only when there's data. `EPOLLET` would require
draining the socket to EAGAIN every wake; level-triggered lets us arm
on-demand and disarm when idle.

### 4.7 KqueueEngine (macOS)

```cpp
class KqueueEngine {
    int kq_ = kqueue();
    // arm_read:  kevent(EVFILT_READ, EV_ADD)
    // arm_write: kevent(EVFILT_WRITE, EV_ADD | EV_CLEAR)  // edge-triggered on macOS
    // disarm_write: kevent(EVFILT_WRITE, EV_DELETE)
    // wait: kevent(kq_, changelist, nchanges, events, maxevents, timeout)
    // notify: EVFILT_USER (or pipe fallback on older macOS)
    // timer:  EVFILT_TIMER
};
```

kqueue uses `EV_CLEAR` (edge-triggered) for write — the API is cleaner
this way on macOS, and the on_writable logic already drains the queue
fully per wake. Read uses level-triggered (`EV_ADD` without `EV_CLEAR`)
to match epoll semantics. The shared `SocketTransport` base handles
both; the engine subclass only implements the arm/disarm/wait
primitives.

### 4.8 Edge Cases

- Partial `writev` → `advance_iov` skips written bytes, remaining
  iovecs stay queued; next `on_writable` continues. No frame corruption
  — frames are length-delimited via `msg_size` + resolved `data_size`.
- Worker thread panics (segfault) → the engine's main loop detects the
  thread exit, closes all its connections, fails pending requests,
  triggers reconnect. (Production: a watchdog thread monitors worker
  liveness.)
- `read` returns EAGAIN/EWOULDBLOCK → level-triggered means the worker
  will be woken again when data arrives; no busy-loop.
- Connection drops mid-send → `writev` returns `EPIPE` /
  `ENOTCONN`, `on_writable` calls `conn->close()`, reconnect triggers.
- Large data payload (1 MB) → one frame, three iovecs (20 + control + 1
  MB). `writev` handles it in one syscall. No chunking at the framing
  layer — TCP segments it.

---

## 5. RDMA Transport

`lib/crow-rpc/cpp/include/crow-rpc/rdma_transport.h`
`lib/crow-rpc/cpp/src/rdma_transport.cpp`

### 5.1 Why

RDMA is the target transport for the diskio data path — kernel bypass,
no syscall per send, pre-registered memory pools. The reference's RDMA
implementation (`rdma/` subtree) is the basis; rewritten for the unified
`Transport` interface and the ref-counted buffer model. RDMA is
Linux-only (no RNICs on macOS); the build gates it behind
`#ifdef __linux__` + a CMake `CROW_RPC_HAVE_RDMA` flag (probed via
`find_package` for libibverbs/librdmacm).

### 5.2 RdmaTransport

```cpp
class RdmaTransport : public Transport {
public:
    uint64_t submit(Connection* conn, Buffer* control, Buffer* data) override;
    Buffer*  register_buffer(Buffer* buf) override;  // ibv_reg_mr
    void     run_loop(Worker* worker) override;
    void     shutdown() override;

private:
    ibv_context*  context_;
    ibv_pd*       pd_;            // protection domain
    RdmaBufferPool* send_pool_;   // pre-registered send buffers
    RdmaBufferPool* recv_pool_;   // pre-registered recv buffers
};
```

a. `register_buffer` — if `buf->type == Registered`, returns it
   unchanged. If `buf->type == System`, copies into `send_pool_` and
   returns the registered `Buffer*`. Callers on the RDMA path allocate
   from `RdmaBufferPool` directly to avoid this copy.
b. `submit` — builds a send work request (WR) with the control and data
   buffers' `ibv_mr` handles, posts it via `ibv_post_send`. The
   connection's QP sends the data directly from the registered memory —
   no kernel involvement, no copy.
c. `run_loop` — polls the completion queue (CQ):

```
Per worker thread:
  cq = ibv_create_cq(context_, depth, ...)
  post initial recv WRs (refill recv queue)

  loop:
    // poll CQ (blocking via ibv_get_cq_event or busy-poll)
    ibv_poll_cq(cq, wcs)
    for each wc:
      if wc is send completion:
        recycle send buffer (refcount decrement)
        if more in send queue: post next send WR
      if wc is recv completion:
        get recv buffer → feed to parser → on_frame
        post new recv WR (refill)
    
    // check for cross-thread submits (via eventfd or ibv async event)
    // post send WRs for newly submitted requests
```

Same submit/completion/dispatch logic as the socket transport. The I/O
primitive changes (CQ poll vs epoll), the buffer registration changes
(`ibv_reg_mr` vs noop), everything else is shared via the
`Transport`/`Connection`/`FrameParser` interfaces.

### 5.3 RDMA Connection Setup

Connection establishment uses `librdmacm` (RDMA CM):
a. Server: `rdma_create_id`, `rdma_bind_addr`, `rdma_listen`,
   `rdma_get_request`, `rdma_create_qp`, `rdma_accept`.
b. Client: `rdma_create_id`, `rdma_resolve_addr`, `rdma_resolve_route`,
   `rdma_connect`, `rdma_create_qp`.
c. After QP creation: post initial receive WRs (refill the recv queue so
   incoming sends have a destination buffer).

This is standard RDMA CM code; the reference's `rdma_epoll_cm_context`
is the basis, rewritten for the new connection model.

### 5.4 Edge Cases

- RNIC disconnect → CQ error completion, connection closes, reconnect
  triggers.
- Send queue full (QP capacity) → `submit` returns
  `RpcError::SendQueueFull`; caller retries or backpressures.
- Recv queue empty (no posted recv WRs) → incoming send is dropped
  (RDMA behavior); the worker refills recv WRs as fast as possible. The
  recv pool depth is sized to avoid this under normal load.
- `ibv_reg_mr` fails (memory exhausted) → `register_buffer` returns
  `nullptr` / `RpcError::RegistrationFailed`.
- RDMA CM event (connection rejected, addr unreachable) → reconnect with
  backoff, same as TCP.

---

## 6. Connection Pool + Reconnect

`lib/crow-rpc/cpp/include/crow-rpc/pool.h`

### 6.1 Why

Callers (consensus replicas, diskio clients) talk to a fixed set of
endpoints. They want connection reuse (no handshake per call), load
spreading, and automatic recovery. The reference's `connection_pool` is
a simple vector; R104 adds round-robin selection and a background
reconnect task.

### 6.2 ConnectionPool

```cpp
class ConnectionPool {
public:
    Connection* get();        // round-robin among healthy connections
    Connection* get_for(const std::string& endpoint);  // pick conn for endpoint

    void add(std::shared_ptr<Connection> conn);
    void remove(Connection* conn);
    void close_all();

private:
    std::vector<std::shared_ptr<Connection>> connections_;
    std::atomic<size_t> next_;
    std::mutex pool_mutex_;   // only for add/remove, not get hot path
    PoolConfig config_;
};
```

a. `get()` — `next_.fetch_add(1) % connections_.size()`, skip
   unhealthy (`is_open == false`) connections. If all down, returns
   `nullptr` / `PoolError::AllDown`.
b. `get_for(endpoint)` — finds connections to that endpoint, round-robin
   among them. Used when the caller targets a specific node (e.g.
   consensus follower → specific leader).

### 6.3 Reconnect

Each connection has a reconnect task, scheduled on the engine's timer:

a. On `Connection::close()`, schedule a reconnect task with
   `config_.reconnect_initial_delay`.
b. The task: `transport->connect(endpoint)` → on success, build a new
   `Connection`, swap it into the pool slot, mark healthy. On failure,
   reschedule with doubled delay (capped at `config_.reconnect_max_delay`).
c. After `config_.reconnect_max_retries` (default 0 = infinite), mark
   the endpoint unhealthy; a periodic health probe can re-trigger
   reconnect later.

The reconnect task runs on the engine's worker thread (via the timer),
not on a tokio thread — it's a C++ internal concern.

### 6.4 Edge Cases

- Reconnect to a dead endpoint → exponential backoff, then mark
  unhealthy. `get()` skips unhealthy endpoints.
- All connections down → `PoolError::AllDown`. Caller decides whether to
  wait or fail.
- Reconnect succeeds but old connection's worker is still draining
  stale bytes → old connection's tasks exit on close; new connection
  starts fresh. No cross-talk.

---

## 7. Request/Response Correlation

`lib/crow-rpc/cpp/include/crow-rpc/caller.h`

### 7.1 Why

The caller needs a completion signal when the response arrives. The
reader (worker thread) needs to find the right callback to invoke. The
reference uses a hazard-pointer hashmap; R104 uses
`folly::ConcurrentHashMap<request_id, CompletionCallback>` — a
lock-free concurrent hashmap from folly (already a pixi dependency for
crow-tree). The consensus hot path has high TPS on request-id
insert/lookup/remove, so a high-performance concurrent hashmap matters.
The map is per-connection; each connection is owned by one worker
thread, so the worker's lookup (read side) is contention-free. Cross-
thread submit (from the caller's thread) and remove (on timeout) hit
the concurrent map's lock-free insert/erase paths.

### 7.2 RemoteCaller

```cpp
using CompletionCallback = std::function<void(Frame* response, RpcError err)>;

class RemoteCaller {
public:
    // Submit a request-response call. Returns request_id.
    uint64_t call(Connection* conn, Buffer* control, Buffer* data,
                  CompletionCallback on_complete);

    // Submit a one-way message (no response expected).
    void call_one_way(Connection* conn, Buffer* control, Buffer* data);

    // Called by Connection::on_frame when a response arrives.
    void on_response(uint64_t request_id, Frame* response);

    // Called by Connection::close to fail all pending requests.
    void fail_all(RpcError err);
};
```

a. `call` — allocates `request_id` (atomic monotonic), inserts
   `(request_id, on_complete)` into the pending map, submits the frame
   via `transport->submit`. On submit error, removes the entry and
   invokes `on_complete` with the error.
b. `call_one_way` — submits without inserting into the pending map.
c. `on_response` — looks up `request_id`, invokes the callback, removes
   the entry. If not found (late response after timeout), logs and
   discards.
d. `fail_all` — invoked on connection close; iterates the pending map,
   invokes each callback with `RpcError::ConnectionClosed`, clears the
   map.

### 7.3 Timeout

Per-request timeout is enforced by the engine's timer: when `call`
submits, it also schedules a timer task for `config_.request_timeout`.
On expiry, if the request is still pending, invoke the callback with
`RpcError::Timeout` and remove the entry. When the (late) response
arrives, `on_response` finds no entry, logs "late response", discards.

### 7.4 Edge Cases

- Response arrives after timeout → callback already invoked with
  `Timeout`; `on_response` finds no entry, logs, discards the frame
  (releases its buffers).
- Duplicate `request_id` → impossible; atomic monotonic per connection.
  On reconnect, new connection starts fresh; old pending already failed.
- `call` on a closed connection → `submit` returns error, callback
  invoked with `ConnectionError`, caller retries via pool.
- Callback panics (throws) → caught at the worker loop boundary, logged,
  the request is removed. The connection stays open.

---

## 8. Schedule Subsystem

`lib/crow-rpc/cpp/include/crow-rpc/schedule.h`

### 8.1 Why

Connections need keepalive pings at a fixed interval. Reconnect needs
delayed retries with backoff. Per-request deadlines need timeout. The
reference uses Linux `timerfd` + a dedicated timer thread; R104 uses the
worker's timer (timerfd on Linux epoll, `EVFILT_TIMER` on macOS kqueue,
CQ-event-based timer on RDMA). No thread-per-timer — every scheduled
task is a callback fired by the worker's event loop.

### 8.2 ScheduledExecutor

```cpp
class ScheduledExecutor {
public:
    // One-shot: run task after delay.
    void schedule_task(Duration delay, std::function<void()> task);

    // Recurring: run task every interval until cancelled.
    TimerHandle schedule_recurring(Duration interval, std::function<void()> task);

    // Cancel a recurring timer.
    void cancel(TimerHandle handle);

    // Run due tasks (called by the worker on timer expiry).
    void tick();

private:
    // Priority queue ordered by deadline.
    std::priority_queue<TimerEntry> timers_;
    std::mutex timers_mutex_;
};
```

a. `schedule_task` — pushes a `TimerEntry{deadline, task, recurring=false}`
   into the priority queue. The worker's timer is set to the earliest
   deadline.
b. `schedule_recurring` — same, with `recurring=true`; after `tick`
   fires it, reschedules at `now + interval`.
c. `tick` — called by the worker when the timer fires. Pops all entries
   with `deadline <= now`, runs their tasks, reschedules recurring ones,
   resets the timer to the next deadline.
d. `cancel` — marks a `TimerHandle` as cancelled; `tick` skips cancelled
   entries.

The timer is per-worker (one timerfd / `EVFILT_TIMER` per worker
thread). 1000 concurrent scheduled tasks share one timer — the priority
queue is the only state, no thread-per-timer.

### 8.3 Edge Cases

- `schedule_recurring` with `interval == 0` → assertion failure
  (precondition: interval > 0).
- Task throws → caught at `tick` boundary, logged, the recurring timer
  is cancelled (no auto-restart).
- Shutdown while tasks are pending → `shutdown()` clears the queue;
  pending tasks are not run.

---

## 9. Server Side

`lib/crow-rpc/cpp/include/crow-rpc/server.h`

### 9.1 Why

The library is not useful without a server. The consensus leader and
the diskio server both need to accept connections, parse frames, and
dispatch to service handlers.

### 9.2 RpcServer

```cpp
using HandlerFn = std::function<Frame*(Frame* request, Connection* conn)>;

class RpcServer {
public:
    RpcServer(Transport* transport, BufferPool* pool);

    void listen(const std::string& addr, int port);
    void register_handler(uint16_t msg_type, HandlerFn handler);
    void start();   // spawns acceptor + worker threads
    void stop();

private:
    Transport* transport_;
    std::unordered_map<uint16_t, HandlerFn> handlers_;
    // ...
};
```

a. `listen` — binds the listening socket (TCP) or RDMA CM id.
b. `register_handler` — inserts into `handlers_`. The handler receives
   the full `Frame*` (control + data) and the `Connection*` (to send the
   response). It returns a `Frame*` (response) or `nullptr` (one-way).
c. `start` — spawns the acceptor thread (accepts connections, assigns to
   workers) and the worker threads (run `transport->run_loop`).
d. The worker, on receiving a request frame, looks up
   `handlers_[msg_type]`, invokes the handler, and (if a response is
   returned) submits it via `transport->submit`.
e. `stop` — closes the listener, signals workers to exit, joins threads.

Common handlers (ping) are registered by `RpcServer` automatically:
`EConnectionPingRequest` → echo back `ConnectionPingResponse` with the
same `id`. This matches the reference's `fb_msg_handler` handling ping
in the base class.

### 9.3 Handler Dispatch Threading

The handler runs on the worker thread that received the frame. For
fast handlers (ping, diskio write submit), this is fine — the handler
is O(1) and returns quickly. For slow handlers (diskio read that waits
for io_uring), the handler should offload to a separate thread pool and
return the response asynchronously via `transport->submit` when the I/O
completes. The `RpcServer` provides an `offload_pool` (a
`thread_pool`) for this purpose; the handler can `offload_pool->post(...)`
and return `nullptr` (response sent later).

### 9.4 Edge Cases

- Unknown `msg_type` → server sends back an `UnknownMessage` response
  with `ret_code = HaveNotSupport`. Connection stays open.
- Handler throws → caught at the worker boundary, logged, an error
  response (`ret_code = Error`) is sent if the request had a
  `request_id`, the connection stays open.
- Handler takes too long → the handler is responsible for internal
  timeouts. The server does not impose a handler deadline (unlike the
  client-side per-request timeout); a slow handler occupies a worker
  thread, so offload long handlers to `offload_pool`.
- Connection drops while handler is running → the handler's
  `transport->submit` for the response fails; the handler logs and
  exits. No crash.

---

## 10. Backpressure

`lib/crow-rpc/cpp/include/crow-rpc/connection.h` (shared with §3)

### 10.1 Why

Under burst load, if callers produce frames faster than the socket
drains, the send queue grows unbounded without a cap. The send queue
capacity is the bound.

### 10.2 Mechanism

a. The per-connection send queue capacity is
   `config_.send_queue_capacity` (default 256 frames).
b. `Transport::submit` pushes to the queue. Two modes, per
   `ConnectionConfig`:
   - `BackpressureMode::Reject` — `try_enqueue`; on full, returns
     `RpcError::Backpressure`. Caller sheds load or retries.
   - `BackpressureMode::Await` — the caller's thread blocks until the
     queue has room. (For the FFI async facade, this is hidden behind
     the oneshot future — see §11.)
c. The data buffer is not a separate queue allocation — it's attached to
   the `OutFrame` and sent via `writev` / RDMA send WR in the same
   operation as the header + control. No extra copy, no extra queue
   entry.
d. Default mode is `Reject` for the consensus hot path (fail fast, let
   the caller batch) and `Await` for the diskio data path (large
   payloads, prefer in-order delivery).

### 10.3 Edge Cases

- Queue full in `Reject` mode → `Backpressure` error returned to
  caller. The pending-request entry is not inserted, so no leak.
- Queue full in `Await` mode → caller blocks (or, via FFI, the oneshot
  future is not resolved until the queue has room). If the connection
  dies while awaiting, `submit` returns `ConnectionError`.
- Queue capacity 0 → invalid config, rejected at validation (min 1).

---

## 11. FFI — Rust Async Facade

`lib/crow-rpc/ffi/`

### 11.1 Why

The rest of CROW is Rust + tokio. `crow-rpc` must expose an async API
that tokio code can `await`, `select!`, and cancel. The C++ engine runs
its own I/O threads; the Rust side is a thin async facade that submits
requests and awaits completions via oneshot channels.

### 11.2 C ABI

`lib/crow-rpc/cpp/include/crow-rpc/c_api.h` — a stable C ABI, same
pattern as `crow-tree/c_api.h`. Opaque handles, exception-free,
`crow_rpc_status` return codes.

```c
// Opaque handles
typedef struct crow_rpc_pool*    crow_rpc_pool_t;
typedef struct crow_rpc_buffer*  crow_rpc_buffer_t;
typedef struct crow_rpc_conn*    crow_rpc_conn_t;
typedef struct crow_rpc_caller*  crow_rpc_caller_t;
typedef struct crow_rpc_server*  crow_rpc_server_t;
typedef struct crow_rpc_sched*   crow_rpc_sched_t;

// Status: 0 = ok, negative = error code
typedef int32_t crow_rpc_status;

// Buffer
crow_rpc_buffer_t* crow_rpc_buffer_alloc(crow_rpc_pool_t* pool, uint32_t capacity);
void               crow_rpc_buffer_write(crow_rpc_buffer_t* buf, const uint8_t* data, uint32_t len);
crow_rpc_buffer_t* crow_rpc_buffer_ref(crow_rpc_buffer_t* buf);  // increment refcount
void               crow_rpc_buffer_release(crow_rpc_buffer_t* buf);

// Pool
crow_rpc_pool_t*   crow_rpc_pool_create(uint32_t default_capacity, uint32_t max_buffers);
void               crow_rpc_pool_destroy(crow_rpc_pool_t* pool);

// Caller (async)
// Submit a request-response call. on_complete is invoked on the C++ I/O thread
// when the response arrives or on error. The callback must be O(1) and non-blocking.
typedef void (*crow_rpc_on_complete)(uint64_t request_id, crow_rpc_buffer_t* control,
                                     crow_rpc_buffer_t* data, crow_rpc_status status,
                                     void* user_data);
crow_rpc_status    crow_rpc_caller_call(crow_rpc_caller_t* caller, crow_rpc_conn_t* conn,
                                        crow_rpc_buffer_t* control, crow_rpc_buffer_t* data,
                                        crow_rpc_on_complete on_complete, void* user_data,
                                        uint64_t* out_request_id);

// One-way
crow_rpc_status    crow_rpc_caller_call_one_way(crow_rpc_caller_t* caller, crow_rpc_conn_t* conn,
                                                crow_rpc_buffer_t* control, crow_rpc_buffer_t* data);

// ... (pool, server, schedule, transport create/destroy)
```

### 11.3 Two-Direction FFI — Performance

**No JNI-like penalty.** Rust FFI is a plain C function call — no
runtime, no GC, no safepoint, no thread attach. Cost: ~5-10 ns per call.

The real constraint: **the C++→Rust callback (`on_complete`) runs on
the C++ I/O thread.** If it does heavy work, it stalls the epoll/CQ
loop. The rule: the callback is O(1) and non-blocking — it looks up the
oneshot by `request_id`, sends the response handle, returns. The tokio
task that receives the oneshot does the real work (parsing, dispatch)
on a tokio worker thread.

### 11.4 Rust Async Facade

```rust
// ffi/src/caller.rs
pub struct RemoteCaller { handle: u64 }

pub struct Response {
    pub control: Buffer,
    pub data: Option<Buffer>,
}

impl RemoteCaller {
    pub async fn call(&self, conn: &Connection,
                      control: Buffer, data: Option<Buffer>)
        -> Result<Response, RpcError>
    {
        let (tx, rx) = oneshot::channel();
        let req_id = unsafe {
            // submit to C++; on complete, the C++ I/O thread calls
            // on_complete_cb, which sends via tx
            sys::crow_rpc_caller_call(
                self.handle, conn.handle,
                control.handle, data.map(|d| d.handle).unwrap_or(0),
                on_complete_cb, Box::into_raw(Box::new(tx)) as *mut _,
            )
        }?;
        rx.await.map_err(|_| RpcError::Cancelled)
    }
}

// The C++→Rust callback — O(1), non-blocking, runs on C++ I/O thread
extern "C" fn on_complete_cb(
    _req_id: u64, control: u64, data: u64, status: i32,
    user_data: *mut c_void,
) {
    let tx = unsafe { Box::from_raw(user_data as *mut oneshot::Sender<Result<Response, RpcError>>) };
    let result = if status == 0 {
        Ok(Response {
            control: Buffer { handle: control },
            data: if data != 0 { Some(Buffer { handle: data }) } else { None },
        })
    } else {
        Err(RpcError::from_status(status))
    };
    let _ = tx.send(result);  // if rx is dropped (cancelled), this is a no-op
}
```

The `call()` returns a real `impl Future` backed by `oneshot::Receiver`.
tokio awaits it normally — `tokio::select!`, `tokio::time::timeout`,
cancellation all work. Dropping the future orphans the result on the C++
side (C++ keeps processing, the late `on_complete_cb` finds the
`oneshot::Sender` send fails, logs, discards).

### 11.5 Buffer on the Rust Side

```rust
// ffi/src/buffer.rs
pub struct Buffer { handle: u64 }

impl Buffer {
    pub fn alloc(pool: &BufferPool, capacity: u32) -> Result<Self, RpcError>;
    pub fn write(&mut self, data: &[u8]);
    pub fn ref_clone(&self) -> Buffer;   // bump refcount
    // Drop::drop calls crow_rpc_buffer_release
}

impl Drop for Buffer {
    fn drop(&mut self) {
        if self.handle != 0 {
            unsafe { sys::crow_rpc_buffer_release(self.handle) };
            self.handle = 0;
        }
    }
}
// Buffer is Send (C++ buffers are thread-safe via atomic refcount).
// Not Sync (the write path is single-threaded per buffer).
unsafe impl Send for Buffer {}
```

### 11.6 Edge Cases

- `call()` dropped (cancelled) before completion → `oneshot::Sender`
  send fails, C++ callback logs "late response", releases buffers. No
  leak.
- C++ I/O thread calls `on_complete_cb` after the tokio runtime is shut
  down → the `oneshot::Sender` is dropped (runtime gone), send fails,
  logged. The C++ engine should be shut down before the tokio runtime;
  documented shutdown order.
- `Buffer` dropped on the Rust side without explicit `release` → `Drop`
  calls `release`, refcount decrements, pool recycles. Same lifecycle
  as C++.

---

## 12. Flatbuffer Schema + Codegen

`lib/crow-protocol/` — all flatbuffer schemas and generated code live in
the `crow-protocol` crate, alongside the existing protobuf schemas. This
keeps all cross-component protocol types in one place (the single-home
rule from `design-crow-protocol.md` §2). `crow-rpc` depends on
`crow-protocol` for the generated flatbuffer types; service crates
(diskio, consensus) also depend on `crow-protocol` for their
service-specific schemas.

### 12.1 Why

Control messages need a serialization format that is compact, zero-copy
on read, and schema-evolvable. Flatbuffers gives all three: the receiver
gets a `&[u8]` view into the buffer with no deserialization step. The
reference uses flatbuffers throughout; R104 follows suit. This
introduces a second serialization format alongside protobuf (prost) in
the codebase — the backlog's Open Question (a) is resolved by the
user's explicit direction to use flatbuffers for the new RPC library.

### 12.2 Common Schemas

Ported from the reference implementation's proto schemas:

- `msg_type.fbs` — `enum FBMsgType : int16` with extensible ranges.
  Common messages (0–99), diskdb (1000s), chunkdb (2000s), diskio
  (3000s), disk management (3100s). Each service registers its range;
  the enum is the single dispatch key.
- `ret_code.fbs` — `enum FBRetCode : int16`. Success, Error,
  HaveNotSupport, plus service-specific codes.
- `common_msg.fbs` — `ConnectionPingRequest`, `ConnectionPingResponse`,
  `UnknownMessage`. Every request/response carries `id: uint64`
  (request_id) and `rpc_create_nano: uint64` (creation timestamp for
  latency measurement).
- `common_type.fbs` — `struct FBInt128 { high: uint64; low: uint64 }`,
  `struct FBInt192 { high; mid; low }`. Inline structs for DiskId /
  ChunkId.

Service-specific schemas (diskio, consensus) live in their own crates
and `include` the common schemas. R104 ships only the common set; the
diskio schema is R105's concern.

### 12.3 Codegen

`flatc` via `crow-protocol`'s `build.rs` (Rust side) and CMake (C++ side,
when the C++ engine needs the generated headers). The `flatc` compiler
is provided by pixi (add `flatbuffers` to `pixi.toml`).

a. Rust: `crow-protocol`'s `build.rs` runs
   `flatc --rust -o $OUT_DIR src/proto/*.fbs`, generating
   `*_generated.rs` modules. `crow-protocol` re-exports the generated
   types. `crow-rpc` and service crates depend on `crow-protocol` for
   these types.
b. C++: `crow-rpc`'s CMake runs
   `flatc --cpp -o include/crow-rpc/proto/ <crow-protocol>/src/proto/*.fbs`
   at build time, generating `*_generated.h` headers included by the
   C++ engine. The `.fbs` source files live in `crow-protocol`; the C++
   generated headers are build artifacts in `crow-rpc`'s build tree.
c. `cargo:rerun-if-changed=src/proto/*.fbs` in `crow-protocol`'s
   `build.rs` so schema changes trigger rebuild.

The `flatbuffers` crate (runtime) is a dependency of `crow-protocol`.
The C++ side uses `flatbuffers/cpp` headers (vendored or via pixi).

### 12.4 Edge Cases

- `flatc` not found → CMake/build.rs panics with a clear message. Pixi
  guarantees it in CI and local dev.
- Schema evolution — flatbuffers tables are forward/backward compatible
  by default (new fields are optional, old readers ignore them). The
  `msg_type` enum is append-only within each service's range.

---

## 13. Platform Build Matrix

| Platform | Socket Engine | RDMA | Status |
| --- | --- | --- | --- |
| Linux x86_64 | `EpollEngine` | `RdmaTransport` (if libibverbs found) | Full |
| macOS arm64 | `KqueueEngine` | N/A (no RNICs) | TCP only |

CMake probes for libibverbs/librdmacm; if found, `CROW_RPC_HAVE_RDMA=1`
is defined and `RdmaTransport` is compiled. If not found, RDMA sources
are excluded (same pattern as crow-tree's liburing gate). macOS builds
always exclude RDMA.

pixi: add `flatbuffers` (for `flatc`) to dependencies. On Linux, add
`libibverbs` and `librdmacm` as optional RDMA deps (or rely on
system packages).

---

## Scope

New crate `lib/crow-rpc/` (mirrors `lib/crow-tree/` structure):

C++ engine (`lib/crow-rpc/cpp/`):
- `CMakeLists.txt` — build config, libibverbs/librdmacm probe, flatc
  codegen, `CROW_RPC_HAVE_RDMA` gate.
- `include/crow-rpc/buffer.h` — `Buffer`, `BufferPool`,
  `SystemBufferPool`, `BufferType`.
- `include/crow-rpc/framing.h` — `Header`, `Frame`, `FrameParser`,
  `FramingError`.
- `include/crow-rpc/transport.h` — `Transport` interface, `Connection`,
  `OutFrame`.
- `include/crow-rpc/socket_transport.h` — `SocketTransport` base,
  `EpollEngine`, `KqueueEngine`.
- `include/crow-rpc/rdma_transport.h` — `RdmaTransport`,
  `RdmaBufferPool` (Linux + `CROW_RPC_HAVE_RDMA` only).
- `include/crow-rpc/caller.h` — `RemoteCaller`, `CompletionCallback`.
- `include/crow-rpc/pool.h` — `ConnectionPool`, `PoolConfig`,
  reconnect.
- `include/crow-rpc/schedule.h` — `ScheduledExecutor`, `TimerHandle`.
- `include/crow-rpc/server.h` — `RpcServer`, `HandlerFn`, `offload_pool`.
- `include/crow-rpc/c_api.h` — stable C ABI for FFI.
- `include/crow-rpc/proto/*_generated.h` — flatc-generated headers (from
  `crow-protocol`'s `.fbs` schemas).
- `src/` — mirrors `include/`.
- `tests/` — C++ unit tests (gtest), like crow-tree.

Rust FFI (`lib/crow-rpc/ffi/`):
- `Cargo.toml` — deps: `tokio` (rt, sync), `crow-protocol` (for
  flatbuffer types), `thiserror`, `tracing`. Build-dep: `cc`.
- `build.rs` — links C++ via CMake (like `crow-tree/ffi/build.rs`).
- `src/lib.rs` — re-exports.
- `src/buffer.rs` — `Buffer` (RAII handle, `ref_clone`, `Drop` =
  release).
- `src/pool.rs` — `BufferPool` handle.
- `src/connection.rs` — `Connection` handle.
- `src/caller.rs` — `RemoteCaller` (async facade, oneshot-backed
  future), `Response`, `on_complete_cb`.
- `src/server.rs` — `RpcServer` (async facade, handler registration).
- `src/schedule.rs` — `ScheduledExecutor` (async facade).
- `src/error.rs` — `RpcError`, status code mapping.
- `src/sys.rs` — `extern "C"` declarations (bindgen or hand-written).

Schemas (`lib/crow-protocol/src/proto/`):
- `msg_type.fbs`, `ret_code.fbs`, `common_msg.fbs`, `common_type.fbs`.
- `crow-protocol`'s `build.rs` runs `flatc --rust` and re-exports the
  generated types.

Rust integration tests (`lib/crow-rpc/ffi/tests/`):
- `framing_test.rs`, `buffer_test.rs`, `connection_test.rs`,
  `pool_test.rs`, `caller_test.rs`, `server_test.rs`,
  `schedule_test.rs`.

Modified files:
- `Cargo.toml` (workspace root) — add `lib/crow-rpc` and
  `lib/crow-rpc/ffi` to `members`; add `flatbuffers` to
  `[workspace.dependencies]`.
- `lib/crow-protocol/Cargo.toml` — add `flatbuffers` dependency; add
  `.fbs` schemas to `src/proto/`; update `build.rs` to run `flatc --rust`
  alongside the existing `tonic-build` proto codegen.
- `pixi.toml` — add `flatbuffers` conda-forge dep (provides `flatc`);
  on Linux, `libibverbs`/`librdmacm` for RDMA; add
  `test-rpc = { cmd = "cargo test -p crow-rpc-ffi --all-targets", depends-on = ["build"] }`
  task.
- `doc/doc_index.md` — no change (working doc, not indexed).

---

## Complexity

**High.** The C++ engine is a full transport stack — epoll, kqueue,
RDMA, buffer pool, framing, correlation, pooling, schedule, server.
The hard parts:

- **Unified `Transport` interface** — getting the abstraction right so
  epoll, kqueue, and RDMA share framing/correlation/pooling without
  leaking transport details into the shared code. The `SocketTransport`
  base + `EpollEngine`/`KqueueEngine` split is the main design effort.
- **RDMA implementation** — QP/CQ management, buffer registration, CM
  event handling. Standard but intricate; the reference is the basis,
  rewritten for the new interfaces.
- **Ref-counted buffer lifecycle across FFI** — the `Buffer` refcount
  lives in C++; Rust `Drop` calls `release`. Getting the
  `Send`/`Drop`/refcount semantics right so no use-after-free and no
  leak across the FFI boundary.
- **Receive-side zero-copy (pull-based parser)** — the parser drives
  allocation and tells the read loop where to read, directly into pool
  buffers. The state machine must correctly handle partial reads across
  TCP segments (resumable per-state offsets) and allocate the control
  and data buffers at the right transition points. This is new code not
  in the reference (which uses a push-based parser with scratch buffer).
- **Two-direction FFI** — the `on_complete` callback runs on the C++
  I/O thread; keeping it O(1) and ensuring the tokio runtime outlives
  the C++ engine (shutdown order).
- **Cross-platform (epoll + kqueue)** — two event-loop implementations
  sharing one `SocketTransport` base. The kqueue path is standard but
  untested in the reference (which is Linux-only).

What is reused vs new: the header layout, the control+data separation,
the msg_type/ret_code/common_msg schemas, and the ping-in-base-handler
pattern are direct ports from the reference. The unified
`Transport` interface, the `SocketTransport`/`EpollEngine`/
`KqueueEngine` split, the ref-counted `Buffer`/`BufferPool` model, the
Rust async FFI facade, and the rewritten RDMA transport are new.

---

## Test Design

### C++ Unit Tests (UT, gtest)

**Buffer** (`tests/buffer_test.cpp`):
- `alloc(1024)` → returns `Buffer*` with `capacity >= 1024`, `size == 0`,
  `ref == 1`. Guards the alloc invariant.
- `write(data, 512)` → `size == 512`, bytes match. Guards the
  write-once path.
- `ref_clone()` → two handles, `ref == 2`; `release()` one → `ref == 1`,
  buffer not recycled; `release()` other → `ref == 0`, buffer recycled
  to pool. Guards the refcount + recycle invariant.
- `alloc` from a pool with one recycled buffer → reuses the recycled
  allocation (same `data` pointer). Guards pool recycling.
- `alloc` when pool exhausted → returns `nullptr`. Guards the
  capacity bound.

**Framing** (`tests/framing_test.cpp`):
- Encode a frame with header + 128-byte control + 1 MB data → parse
  yields identical header fields, control bytes, data bytes. Guards the
  round-trip invariant.
- Parse a header with wrong magic → `FramingError::BadMagic`. Guards
  protocol alignment.
- Feed the parser 10 bytes, then 10 bytes (header split) → reassembles
  into a valid header. Guards partial-read handling.
- Parse a control-only frame (`data_size == 0`) → `Frame` with
  `data == nullptr`. Guards the control-only path.
- Parse a header with `data_size` exceeding `max_data_size` (4 MB) →
  `FramingError::DataTooLarge`. Guards the allocation-bomb defense.
  `max_data_size` → `FramingError::DataTooLarge`. Guards the
  allocation-bomb defense.

**Schedule** (`tests/schedule_test.cpp`):
- `schedule_recurring(10ms, counter)` run for ~1s → counter is 100 ± 5.
  Guards the periodic timing invariant.
- `schedule_task(50ms, flag)` → flag set exactly once after ≥ 50ms, not
  before. Guards the one-shot invariant.
- 1000 concurrent `schedule_task(100ms, ...)` → thread count of the
  process does not increase (all on worker thread). Guards the
  no-thread-per-timer invariant.

### Rust Integration Tests (E2E, via FFI)

All use an in-process echo `RpcServer` on `127.0.0.1:0` (ephemeral
port).

**Connection + writer** (`tests/connection_test.rs`):
- Two concurrent `call()` on the same connection → both responses
  received, no interleaving. Guards the multi-producer queue + reader
  correlation.
- Push 10 frames rapidly → server receives all 10 in order. Guards the
  writer batching + partial-write resume.
- Kill the server mid-call → `call()` returns `ConnectionError` within
  1 second. Guards the fail-fast-on-drop invariant.
- Send a 1 MB data payload via `call()` → server receives control + 1
  MB data intact. Guards the large-payload scatter-gather path.
- Server returns a 1 MB data payload → caller receives `Buffer` of
  correct size; verify the `Buffer`'s data pointer is the same address
  the kernel wrote into (zero-copy receive: the pool buffer *is* the
  receive buffer, no scratch copy). Guards the pull-based parser's
  zero-copy receive invariant.
- `call_one_way()` → returns immediately, server receives the message.
  Guards the one-way path.

**Buffer across consumers** (`tests/buffer_test.rs`):
- Allocate a `Buffer`, `ref_clone` for 3 consumers (EC, checksum, RPC
  send), drop all 3 → buffer recycled to pool (next `alloc` reuses it).
  Guards the multi-consumer refcount lifecycle.

**Pool + reconnect** (`tests/pool_test.rs`):
- Pool with 3 connections, 6 sequential `call()`s → connections hit in
  round-robin order 1,2,3,1,2,3. Guards round-robin selection.
- Drop the server, restart it → reconnect task restores the connection
  → subsequent `call()`s succeed. Guards the reconnect path.
- Per-request timeout 100ms on a handler that sleeps 500ms →
  `TimeoutError` at ~100ms. Guards the timeout invariant.
- `BackpressureMode::Reject` with queue capacity 2, push 3 frames
  rapidly → 3rd `call()` returns `BackpressureError`. Guards the
  backpressure bound.

**Server** (`tests/server_test.rs`):
- Send a frame with unregistered `msg_type` → server responds with
  `UnknownMessage`, `ret_code = HaveNotSupport`. Connection stays open;
  next valid call succeeds. Guards the unknown-type fallback.
- Handler throws → server sends error response (`ret_code = Error`),
  connection stays open. Guards the handler-exception isolation.
- Ping request → ping response with matching `id`. Guards the common
  handler.

**Test commands**: `pixi run test-rpc` (Rust FFI integration tests),
`ctest --test-dir lib/crow-rpc/cpp/build` (C++ unit tests),
`pixi run cargo fmt --all -- --check`,
`pixi run cargo clippy --all-targets -- -D warnings`,
`clang-format --dry-run --Werror` (changed `.cpp`/`.h`),
`tree-lint` (clang-tidy, changed C++).

---

## Module Structure

```
lib/crow-rpc/
├── CMakeLists.txt                 # C++ engine build, flatc codegen (from crow-protocol .fbs), RDMA gate
├── cpp/
│   ├── CMakeLists.txt
│   ├── include/crow-rpc/
│   │   ├── buffer.h               # Buffer, BufferPool, BufferType, refcount, ibv_mr
│   │   ├── framing.h              # Header, Frame, FrameParser, FramingError
│   │   ├── transport.h            # Transport interface, Connection, OutFrame
│   │   ├── socket_transport.h     # SocketTransport base, EpollEngine, KqueueEngine
│   │   ├── rdma_transport.h       # RdmaTransport, RdmaBufferPool (Linux+RDMA)
│   │   ├── caller.h               # RemoteCaller, CompletionCallback, folly::ConcurrentHashMap
│   │   ├── pool.h                 # ConnectionPool, reconnect
│   │   ├── schedule.h             # ScheduledExecutor, TimerHandle
│   │   ├── server.h               # RpcServer, HandlerFn, offload_pool
│   │   ├── c_api.h                # stable C ABI for FFI
│   │   └── proto/                 # flatc-generated headers (build artifacts)
│   ├── src/                       # mirrors include/
│   └── tests/                     # C++ unit tests (gtest)
├── ffi/                           # Rust FFI wrapper (like crow-tree/ffi)
│   ├── Cargo.toml                 # deps: tokio, crow-protocol, thiserror, tracing
│   ├── build.rs                   # links C++ via CMake
│   ├── src/
│   │   ├── lib.rs                 # re-exports
│   │   ├── buffer.rs              # Buffer (RAII, ref_clone, Drop=release)
│   │   ├── pool.rs                # BufferPool handle
│   │   ├── connection.rs          # Connection handle
│   │   ├── caller.rs              # RemoteCaller (async, oneshot-backed)
│   │   ├── server.rs              # RpcServer (async facade)
│   │   ├── schedule.rs            # ScheduledExecutor (async facade)
│   │   ├── error.rs               # RpcError, status mapping
│   │   └── sys.rs                 # extern "C" declarations
│   └── tests/                     # Rust integration tests (via FFI)

lib/crow-protocol/src/proto/       # flatbuffer schemas (single home for all proto types)
├── msg_type.fbs                   # FBMsgType enum
├── ret_code.fbs                   # FBRetCode enum
├── common_msg.fbs                 # ping request/response, unknown message
└── common_type.fbs                # FBInt128, FBInt192
```

---

## Config Extensions

`ConnectionConfig` (C++):
- `send_queue_capacity: uint32_t` — send queue bound (default 256).
- `backpressure_mode: BackpressureMode` — `Reject` or `Await` (default
  `Reject`).
- `max_data_size: uint32_t` — max data payload per frame (default 4 MB).
- `recv_buf_size: uint32_t` — receive scratch buffer (default 64 KB).

`PoolConfig` (C++):
- `request_timeout: Duration` — per-request deadline (default 5 s).
- `retry_count: uint32_t` — retries on `ConnectionError` (default 2).
- `reconnect_initial_delay: Duration` — first backoff (default 100 ms).
- `reconnect_max_delay: Duration` — backoff cap (default 10 s).
- `reconnect_max_retries: uint32_t` — retries before unhealthy (default
  0 = infinite).

`ServerConfig` (C++):
- `max_connections: uint32_t` — accept limit (default 1024).
- `max_data_size: uint32_t` — same as `ConnectionConfig`.
- `offload_pool_threads: uint32_t` — offload thread pool size (default
  4).

`BufferPoolConfig` (C++):
- `default_capacity: uint32_t` — default buffer capacity (default 1 MB).
- `max_buffers: uint32_t` — pool capacity (default 1024).

All configs have a `validate()` method; invalid configs are rejected at
construction. No changes to existing CROW config files — `crow-rpc` is a
library; server/client crates that use it (R32, R105) add their own
config sections.

---

## Server Wiring

`crow-rpc` is a library, not a binary. It plugs into a server (e.g. the
future diskio server, R105) as:

a. Construct `BufferPool` (glibc for TCP, RDMA-registered for RDMA).
b. Construct `Transport` (`TcpTransport` / `RdmaTransport`).
c. Construct `RpcServer(transport, resolver, pool)` — registers the
   common ping handler.
d. `server.register_handler(msg_type, handler)` for each service RPC.
e. `server.listen(addr)` → `server.start()` — acceptor + worker threads
   run. The caller's `main` spawns this and joins on shutdown.
f. On shutdown: `server.stop()` → `transport.shutdown()` →
   `pool.destroy()`.

On the client side (e.g. the future diskio client):
a. Construct `BufferPool` + `Transport` + `ConnectionPool`.
b. `pool.get_for(endpoint)` → `Connection`.
c. `caller.call(conn, control_buf, data_buf).await` → `Response`.
d. The pool manages connections, reconnect, timeout internally.

No changes to `crow-kv-server` or `crow-diskdb` startup in R104 — those
integrations are R32 and R105 respectively. R104 delivers the library +
its own test suite.

---

## Impact on Other Requirements

The `Buffer`-from-pool model (not `bytes::Bytes`) affects downstream
requirements that will use `crow-rpc`:

- **R105 (diskio engine)** — the diskio write/read RPCs must accept
  `Buffer*` for the data payload, not `Bytes`. The chunk writer gets a
  `Buffer` from the RPC pool, writes the strip data into it, calls
  `diskio_write(control, data_buf)`. The data lands in the I/O buffer
  without a copy because the RPC pool buffer *is* the I/O buffer (or is
  RDMA-registered for direct disk I/O). R105's design doc must reflect
  this.
- **R94 / R106 (chunk writers)** — the chunk writer's strip data flows
  through `Buffer`: EC computation reads the buffer, checksum reads the
  buffer, RPC send consumes the buffer. Each holds a `ref_clone`; the
  buffer recycles when all drop. R94/R106 must design around this
  multi-consumer buffer lifecycle.
- **R32 (KV consensus hot path)** — the consensus control messages are
  small (no data payload); `call(control_buf, nullptr)` is the pattern.
  The migration from gRPC to `crow-rpc` changes the transport, not the
  consensus logic. R32's design doc must reflect the `crow-rpc` API.

These requirements are not modified by R104. After R104 implementation
is complete and the design is confirmed, a follow-up task will be
created to update each impacted requirement's backlog doc / design
draft to reference the `Buffer`-from-pool model and the `crow-rpc` API.
R104 establishes the buffer abstraction; the follow-up task propagates
it to the consumers.

---

## Open Questions

None open. All design decisions resolved (see Decisions below).

## Decisions

1. **12-byte header with `data_size` in header.** The header carries
   `data_size: u32` directly, making the parser fully self-contained —
   no `DataSizeResolver` indirection, no schema dependency in the
   framing layer. The header is 12 bytes
   `[magic:2][msg_type:2][msg_size:2][data_size:4][msg_offset:1][flags:1]`,
   down from the reference's 20 bytes. Removed from the reference:
   `create_ms` (redundant with `rpc_create_nano` in the control
   message), `padding` (no longer needed), magic reduced 4→2 bytes,
   `msg_offset` reduced 2→1 byte. Added: `data_size`, `flags` (one-way,
   compression, priority bits).

2. **RDMA implemented in R104, testing deferred.** RDMA code lands in
   R104 alongside TCP, behind the unified `Transport` interface. This
   avoids design issues that arise when bolting RDMA onto a TCP-only
   implementation later — the `Transport`/`Connection`/`Buffer`/parser
   interfaces are designed for both from the start, and RDMA code
   validates that the abstractions hold. Full RDMA testing is deferred
   until RNIC hardware is available; the `RdmaTransport` code is gated
   behind `CROW_RPC_HAVE_RDMA` and unit-tested via mocks where possible.
   TCP is tested on both Linux (epoll) and macOS (kqueue).

3. **Receive-side zero-copy in v1.** The parser uses a pull-based API
   (§2.4) — `next_read_target()` tells the read loop where to read,
   directly into pool-allocated `Buffer`s. No scratch buffer, no copy on
   the receive side. This unifies the TCP and RDMA receive paths (RDMA
   pre-posts recv WRs into the same pool buffers). No design blocking
   issue — the parser API change is contained in `framing.h`/
   `framing.cpp`, and the read loop (§4.5) is a straightforward
   `target = next_read_target() → read → advance` cycle.

4. **kqueue testing coverage.** The kqueue engine is new (the reference
   is Linux-only). macOS dev machines provide the test platform. The
   integration tests run on both Linux (epoll) and macOS (kqueue) via
   pixi. If kqueue-specific bugs surface, they're caught by the same
   E2E tests that run on epoll. No separate kqueue test suite — the
   shared `SocketTransport` base means the test surface is shared.

5. **Impact on other requirements — follow-up task after R104 impl.**
   After R104 implementation is complete and the design is confirmed, a
   follow-up task will be created to update R105, R94/R106, and R32 to
   reference the `Buffer`-from-pool model and the `crow-rpc` API. R104
   does not modify those requirements.
