<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R114: rpc — crow-rpc Streaming RPC Support

**Problem**

R104 shipped crow-rpc with request-response and one-way messages
only — `design-crow-rpc.md` §1 Non-Goals explicitly defers streaming
RPC: "v1 supports request-response and one-way messages only.
Bidirectional streaming is a future addition." The implementation
confirms: `HandlerFn` returns a single `OutFrame*`, `RpcClient::send`
pairs one request with one completion callback, and the Rust FFI
`CallFuture` resolves to one `Response`. There is no stream
abstraction on either the C++ engine or the Rust FFI side.

Three KV RPCs need streaming and are blocked by this gap:
- `LearnerStream` (bi-directional) — follower catch-up, pxos.proto
  L323. The follower sends a sequence of "give me slots N..M"
  requests; the leader streams back log entries.
- `StreamSnapshot` (server-streaming) — snapshot transfer, pxos.proto
  L369. The leader streams snapshot chunks to a follower.
- `WatchNotify` (bi-directional) — kv.proto L194. The client sends
  watch registrations; the server streams change notifications.

**Current behavior + impact**: R32 (KV consensus migration) and R117
(KvService client-facing migration) are blocked. Without streaming,
these three RPCs cannot migrate to crow-rpc, leaving the h2-lock
throughput loss un-recovered on the LearnerStream path and the
WatchNotify path on gRPC indefinitely.

**Design pointers**: `design-crow-rpc.md` §1 (Non-Goals — streaming
deferred), §3 (Wire Format — the 12-byte header + control + data
frame), §4.2 (Request/Response Correlation — the pending-request
map), §4.4 (Server Side — handler dispatch). The streaming design
extends these sections; it does not replace them. The three streaming
RPCs are defined in `design-crow-kv-rpc.md` (LearnerStream,
StreamSnapshot) and `design-crow-kv-watch-notify.md` (WatchNotify).

**Use scenarios**:

- **Follower catch-up via LearnerStream**: A follower joins or falls
  behind. It opens a persistent connection to the leader and sends
  "give me slots from N" requests. The leader streams back log
  entries as a sequence of response frames on the same connection.
  The follower sends new requests as it consumes entries. Expected:
  the follower reaches `CaughtUp` state; the stream stays open for
  incremental catch-up.

- **Snapshot transfer via StreamSnapshot**: A follower needs a
  snapshot (too far behind for log catch-up). It sends one
  `SnapshotRequest`; the leader streams back `SnapshotStreamItem`
  frames until the snapshot is fully transferred. Expected: the
  follower applies the snapshot and resumes from the snapshot's
  `last_applied` slot.

- **Watch registration + notification via WatchNotify**: A client
  opens a persistent connection, sends watch registrations (prefix
  patterns), and receives change notifications as the KV store
  applies writes to matching keys. The stream is long-lived (minutes
  to hours). Expected: the client receives notifications for all
  matching writes; missed notifications are caught by the safety-net
  poller (existing design, unchanged).

**Solution**

Extend crow-rpc with streaming support — two patterns:
**server-streaming** (one request → N response frames) and
**bi-directional streaming** (N request frames ↔ N response frames on
one persistent connection). Both reuse the existing 12-byte header +
flatbuffer control + raw data frame format. The stream is a
conversation on a single `Connection`; each frame carries the same
`request_id` (or a stream-scoped `stream_id` — see Open Questions).

**One-line summary**: Add server-streaming and bi-directional
streaming to crow-rpc by extending the connection to carry multi-
frame conversations, reusing the existing frame format and
correlation mechanism.

**Numbered work items**:

1. **Stream correlation model** (`lib/crow-rpc/include/crow-rpc/`)
   — decide whether streams are correlated by `request_id` (one id
   per stream, reused across all frames) or by a new `stream_id`
   field in the header (or a stream-open control message that
   allocates a `stream_id`). The existing `request_id`-keyed pending
   map must be extended to track open streams vs. single
   request-response calls. A stream-open/stream-close handshake
   (control messages with `FBMsgType::EStreamOpen` /
   `EStreamClose`) cleanly separates stream lifecycle from
   per-frame request_id. Files: `framing.h`, `connection.h`,
   `client/client.h`.

2. **C++ server-side streaming handler** (`lib/crow-rpc/include/
   crow-rpc/server/handler.h`, `lib/crow-rpc/src/server/`) —
   extend `HandlerFn` or add a `StreamHandlerFn` that receives the
   request frame + connection + a `StreamWriter` interface. The
   handler calls `StreamWriter::send_frame(control, data)` to push
   response frames; for bi-directional streams, the handler also
   receives a `StreamReader` to pull subsequent request frames. The
   handler signals stream end by returning (server-streaming) or by
   a stream-close frame (bi-directional). Files: `server/handler.h`,
   `server/server.cpp`.

3. **C++ client-side streaming** (`lib/crow-rpc/include/crow-rpc/
   client/`) — add `RpcClient::open_stream(...)` that sends a
   stream-open control message, allocates a stream-scoped callback
   queue, and returns a `StreamWriter` + `StreamReader` pair (or a
   `Stream` object combining both). The pending map tracks the
   stream; incoming frames with the stream's id are routed to the
   stream's reader queue, not the single-call callback. Files:
   `client/client.h`, `client/client.cpp`.

4. **Rust FFI streaming facade** (`lib/crow-rpc/ffi/src/`) — expose
   `Stream` as an async Rust type with `send()` (pushes a frame) and
   a `StreamReceiver` (yields response frames as an `async
   Stream`). The C++→Rust callback routes stream frames to the
   receiver's channel. `CallFuture` is not reused — streams have
   their own completion model (stream-close or error). Files:
   `lib/crow-rpc/ffi/src/lib.rs`, `lib/crow-rpc/ffi/src/stream.rs`
   (new).

5. **Stream lifecycle + error handling** — stream-open fails if the
   server rejects the `msg_type` (unknown stream type →
   `UnknownMessage` response). Mid-stream connection drop → all
   pending stream frames are failed with `ConnectionClosed`; the
   Rust `StreamReceiver` yields an error item. Stream-close is
   idempotent (double-close is a no-op). Per-frame timeout: each
   frame in a stream may have its own timeout, or the stream has a
   global idle timeout (see Open Questions). Files: `connection.cpp`,
   `client/client.cpp`.

6. **Common flatbuffer stream-control schemas** (`lib/crow-protocol/
   src/fbs/common_msg.fbs`) — add `FBStreamOpenRequest`,
   `FBStreamOpenResponse`, `FBStreamClose` control messages. Register
   `EStreamOpenRequest`, `EStreamOpenResponse`, `EStreamClose` in
   `msg_type.fbs` (common range, below 100). These are
   service-agnostic; the service-specific stream payload types
   (LearnerStream entries, SnapshotStreamItem, WatchNotify) are
   defined in each service's own `.fbs` schema (R32, R117). Files:
   `lib/crow-protocol/src/fbs/common_msg.fbs`,
   `lib/crow-protocol/src/fbs/msg_type.fbs`,
   `lib/crow-protocol/build.rs`.

7. **Echo stream test** (`lib/crow-rpc/ffi/tests/`) — an end-to-end
   test: client opens a bi-directional stream, sends N request
   frames, server echoes each as a response frame, client verifies
   all N responses. Also a server-streaming test: client sends one
   request, server sends N responses, client verifies all N. This
   validates the streaming primitive before any service uses it.
   Files: `lib/crow-rpc/ffi/tests/ffi_stream_test.rs` (new).

**Flow diagram**:

```
Bi-directional stream (LearnerStream, WatchNotify)

Client                              Server
  │                                   │
  │── StreamOpen(msg_type) ──────────►│  handler registered for msg_type
  │◄────────── StreamOpenResp ────────│  stream accepted
  │                                   │
  │── Frame(stream_id, req1) ────────►│  StreamHandler reads req1
  │◄────────── Frame(stream_id, resp1)│  StreamHandler sends resp1
  │── Frame(stream_id, req2) ────────►│  ...
  │◄────────── Frame(stream_id, resp2)│
  │   ...                             │
  │── StreamClose(stream_id) ────────►│  handler returns
  │◄────────── StreamClose ───────────│
  │                                   │

Server-streaming (StreamSnapshot)

Client                              Server
  │                                   │
  │── StreamOpen(msg_type, req) ─────►│  handler reads req
  │◄────────── StreamOpenResp ────────│  stream accepted
  │◄────────── Frame(stream_id, item1)│  handler pushes N items
  │◄────────── Frame(stream_id, item2)│
  │   ...                             │
  │◄────────── StreamClose ───────────│  handler done
  │                                   │
```

**Edge cases at a glance**:

- Stream-open rejected (unknown `msg_type`) → client receives
  `UnknownMessage` response, no stream created.
- Mid-stream connection drop → all in-flight stream frames failed
  with `ConnectionClosed`; Rust `StreamReceiver` yields error;
  caller retries (open a new stream on a new connection).
- Server handler crashes mid-stream → connection is closed (handler
  exception → error response + connection close, same as §4.4).
- Stream-close from client while server is still sending → server
  stops sending, drains its write queue, acknowledges close.
- Per-frame timeout vs. stream idle timeout → see Open Questions.
- Concurrent streams on one connection → multiple `stream_id`s
  active; the connection's send queue interleaves frames from
  different streams (the per-connection writer is already
  lock-free).

**Dependencies**

- **Depends on**: R104 (crow-rpc engine — framing, connection,
  correlation, FFI). R104 is finished.
- **Depended on by**: **R32** (LearnerStream, StreamSnapshot),
  **R117** (WatchNotify). Both are blocked until R114 lands.

**Acceptance**

**Streaming primitives**:
- A bi-directional echo stream: client opens stream, sends 10
  request frames, server echoes each → client receives all 10
  response frames in order. Integration test
  (`ffi_stream_test.rs`).
- A server-streaming stream: client sends 1 request, server sends 10
  response frames → client receives all 10 in order. Integration
  test.
- Stream-open rejected for an unregistered `msg_type` → client
  receives `UnknownMessage` error, no stream created. Integration
  test.
- Mid-stream connection drop → client `StreamReceiver` yields
  `ConnectionClosed` error. Integration test (kill server
  mid-stream).
- Stream-close from client mid-stream → server stops sending,
  client receives no more frames. Integration test.

**Performance**:
- A bi-directional stream carrying 1000 frames has < 5% overhead
  vs. 1000 independent request-response calls on the same
  connection (streaming avoids per-call stream-open/close). Benchmark
  test (Linux).

**FFI**:
- Rust `Stream` `send()` is async (awaits send-queue capacity in
  `Await` mode, fails fast in `Reject` mode). Integration test.
- Rust `StreamReceiver` implements `futures::Stream` (yields
  `Result<Frame, RpcError>`). Integration test.

**Test commands**: `pixi run cargo test -p crow-rpc-ffi --test
ffi_stream_test`, `pixi run cargo fmt --all -- --check`,
`pixi run cargo clippy --all-targets -- -D warnings`.

**Open Questions**

- **Stream correlation: `request_id` reuse vs. `stream_id`**. Reusing
  `request_id` for all frames in a stream is simpler (no header
  change) but conflates single-call and stream correlation in the
  pending map. A separate `stream_id` (allocated by a stream-open
  control message) cleanly separates the two but adds a round-trip
  for stream setup. The stream-open handshake is cleaner and
  supports concurrent streams on one connection; the `request_id`
  reuse approach is simpler for single-stream-per-connection use
  cases. Recommendation: `stream_id` via stream-open handshake —
  concurrent streams matter for WatchNotify (one client may watch
  multiple prefixes on one connection).

- **Per-frame timeout vs. stream idle timeout**. Single-call RPCs
  have a per-call timeout (§4.2). Streams are long-lived (WatchNotify
  runs for hours); a per-frame timeout would fire constantly. An
  idle timeout (no frames for N seconds → close stream) fits
  long-lived streams better. LearnerStream and StreamSnapshot are
  short-lived but bursty; they need a per-frame timeout on the
  client side (waiting for the next entry). Recommendation: both —
  per-frame timeout for short-lived streams (LearnerStream,
  StreamSnapshot), idle timeout for long-lived streams (WatchNotify),
  selected by the stream-open request's flags.
