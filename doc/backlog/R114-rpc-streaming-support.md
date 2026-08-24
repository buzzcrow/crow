<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R114: rpc — crow-rpc Request-Response Completion + RequestId Generator

**Problem**

R104 shipped crow-rpc with request-response and one-way messages.
The wire protocol is a raw socket conversation — every frame is
`[12-byte header][flatbuffer control][data]`, correlated by
`request_id` extracted from the control message during parse. There
is no "stream" concept (unlike HTTP/2); the connection is a
persistent bidirectional byte stream where either side can send a
request and expect a response.

Three KV RPCs need crow-rpc support and are blocked by gaps in the
current wiring:
- `LearnerStream` — follower catch-up, pxos.proto L323. The
  follower sends a sequence of "give me slots N..M" requests; the
  leader responds with log entries. Each request gets one
  response — normal request-response, no protocol change needed.
- `StreamSnapshot` — snapshot transfer, pxos.proto L369. The
  follower sends "give me chunk N" requests; the leader responds
  with chunk N. Per-chunk request-response, no protocol change
  needed.
- `WatchNotify` — kv.proto L194. The client sends watch
  registrations; the server sends change notifications **as
  requests** (server→client), and the client sends ack responses.
  This is server-initiated request-response — the connection is
  bidirectional, so the server can send a request and await an ack.

**Current behavior + impact**: R32 (KV consensus migration) and R117
(KvService client-facing migration) are blocked. The current code
only wires one direction of request-response (client→server
requests, server→client responses). The server side has
`HandlerRegistry` (dispatch incoming requests) + `transport->submit`
(send responses) but no request-response correlation (send a
request, await ack, retry). The client side has `RpcClient` (send
requests, route responses) but no request handler dispatch (handle
incoming requests, send responses). WatchNotify needs both halves.
Additionally, the `request_id` generator is duplicated across
clients (C++ `RpcClient::next_request_id_`, Rust
`DiskioClient::next_req_id`) and should be consolidated into
`crow-common`.

**Design pointers**: `design-crow-rpc.md` §1 (Non-Goals — streaming
deferred; this requirement resolves it as "not needed"), §3 (Wire
Format — the 12-byte header + control + data frame), §4.2
(Request/Response Correlation — the pending-request map), §4.4
(Server Side — handler dispatch), §6 (Flatbuffer Wrapper Convention —
including the data-payload zero-copy rule for streaming handlers). The
three KV RPCs are defined in `design-crow-kv-rpc.md` (LearnerStream,
StreamSnapshot) and `design-crow-kv-watch-notify.md` (WatchNotify).

**Use scenarios**:

- **Follower catch-up via LearnerStream**: A follower joins or falls
  behind. It sends "give me slots from N" requests to the leader on
  a persistent connection; the leader responds with log entries for
  each request. The follower sends new requests as it consumes
  entries. Expected: the follower reaches `CaughtUp` state; the
  connection stays open for incremental catch-up.

- **Snapshot transfer via StreamSnapshot**: A follower needs a
  snapshot (too far behind for log catch-up). It sends "give me
  chunk N" requests; the leader responds with `SnapshotChunk`
  frames until the snapshot is fully transferred. Expected: the
  follower applies the snapshot and resumes from the snapshot's
  `last_applied` slot.

- **Watch registration + notification via WatchNotify**: A client
  sends watch registrations (prefix patterns) via normal
  request-response. When a matching key is written, the server
  sends a **notify request** to the client; the client handles it
  and sends an **ack response**. If the server doesn't receive a
  success response after retry, it logs `WSCritical` and retries
  the notify on the next change. Expected: the client receives
  notifications for all matching writes; missed notifications are
  caught by the safety-net poller (existing design, unchanged).

**Solution**

No new protocol concept, no streaming, no `stream_id`, no
`FLAG_LAST_FRAME`, no `call_multi`, no new handler type. Every RPC
is one request frame → one response frame, correlated by
`request_id`. The three KV RPCs map onto normal request-response:
LearnerStream and StreamSnapshot are per-batch/per-chunk `call()`
with zero protocol change; WatchNotify is server-initiated
request-response (the server sends a request, the client sends a
response). The work is: (1) consolidate the `request_id` generator
into `crow-common`, (2) add server-side request-response
correlation (reuse `RpcClient`), (3) add client-side request
handler dispatch, (4) retry + `WSCritical` logging for WatchNotify.

**One-line summary**: Complete the request-response model on both
sides of crow-rpc (server can send requests, client can handle
requests) and consolidate the `request_id` generator into
`crow-common` — no streaming, no protocol change.

**Numbered work items**:

1. **RequestId generator in crow-common** (`lib/crow-common/`) —
   add a `RequestIdGen` struct with an internal `AtomicU64` counter
   and a `next() -> u64` method (thread-safe `fetch_add(1,
   Relaxed)`). Per-client instance (not global static) —
   `request_id` only needs uniqueness within one client's pending
   map, and per-client counters yield smaller numbers → smaller
   slab pool + pending hashmap. Rust side: `RequestId(u64)` newtype
   in `lib/crow-common/rust/src/`, `as_u64() -> u64` at the FFI
   boundary. C++ side: `RequestIdGen` in
   `lib/crow-common/cpp/include/crow-common/`. Remove
   `RpcClient::next_request_id_` (`client/client.h` L162) —
   verified only 4 test call sites use it, zero production code.
   Update those sites to use `crow-common`'s generator. Update
   `DiskioClient::next_req_id` (`crow-diskio-client/src/client.rs`
   L86) to use the shared generator.

2. **Server-side request-response correlation** (`lib/crow-rpc/`)
   — the server needs to send requests (not just responses) and
   await acks. Reuse `RpcClient` (keep the name) for
   request-response correlation on the server side: `send()` +
   `on_response()` + the pending map + reaper. The server already
   has `HandlerRegistry` (dispatch incoming requests) +
   `transport->submit` (send responses); `RpcClient` adds the
   "send a request, await ack, retry" half. Wire `RpcClient` into
   the server's connection so server-sent requests get their acks
   routed back. Files: `lib/crow-rpc/include/crow-rpc/server/
   server.h`, `lib/crow-rpc/src/server/`, `lib/crow-rpc/include/
   crow-rpc/client/client.h` (shared).

3. **Client-side request handler dispatch** (`lib/crow-rpc/`) —
   the client needs to handle incoming requests (not just
   responses) and send responses. Add a `HandlerRegistry`-like
   dispatch on the client side: `on_frame` tries `request_id`
   routing first (existing — for responses to client-sent
   requests); if no match, dispatch by `msg_type` to a registered
   handler. The handler receives `Frame* + Connection*`, processes
   the request, and submits a response via `transport->submit` with
   the same `request_id`. This mirrors the server-side dispatch
   model. Files: `lib/crow-rpc/include/crow-rpc/client/client.h`,
   `lib/crow-rpc/src/client/`, `lib/crow-rpc/ffi/src/client.rs`.

4. **WatchNotify retry + WSCritical logging** (`lib/crow-kv/`) —
   when the server sends a notify request, it tracks the
   `request_id` in its `RpcClient` pending map with a timeout. If
   the ack doesn't arrive after retry, log `WSCritical` and retry
   the notify on the next change. The retry policy and timeout are
   service-level (defined here, not in crow-rpc). Files:
   `lib/crow-kv/src/cluster/watch_registry.rs`,
   `lib/crow-kv/src/cluster/remote_replica.rs`.

5. **Echo request-response test** (`lib/crow-rpc/ffi/tests/`) —
   an end-to-end test validating both directions: (a) client sends
   a request, server responds (existing path, regression check);
   (b) server sends a request, client responds (new path). Also a
   WatchNotify-style test: server sends a notify request, client
   acks, server verifies ack received; server sends notify, client
   drops, server retries then logs `WSCritical`. Files:
   `lib/crow-rpc/ffi/tests/ffi_request_test.rs` (new).

**Zero-copy data payload for streaming handlers**: The per-batch
`call()` design is zero-copy on the data payload, not just the
control buffer. The frame's data buffer is a ref-counted pool buffer;
streaming-data handlers (LearnerStream log entries, StreamSnapshot
chunks) consume it with `pwrite(fd, &buf, len)` and
`engine.apply(slot, &batch)`, both of which take `&[u8]` for the
duration of the call and need no owned bytes. These handlers hold the
data buffer by reference and drop it after the async write completes
— no copy to owned `Vec<u8>`. The "may copy to owned `Vec`" exception
in `design-crow-rpc.md` §6 does not apply here: the receiver does not
need owned bytes. This is the receive-side companion to §6's "no
owned intermediate struct" rule for the control buffer; the canonical
rule lives in `design-crow-rpc.md` §6 ("Data payload: zero-copy when
the receiver consumes by reference"). Handler implementation is
R32/R117's scope; R114's protocol design (per-batch `call()`) enables
it.

**Pull, not push (deliberate)**: R114 uses pull — per-batch/per-chunk
`call()`, one response per call. Push (one request → many responses)
is lower-latency for steady-state catch-up but needs multi-response
machinery (`FLAG_LAST_FRAME`, `call_multi`, new handler type) that
R114 drops. Pipelining (multiple in-flight `call()`s on the
persistent connection) amortizes the per-batch round-trip. Push is
an additive future extension if catch-up latency under pipelining
proves insufficient; R114 does not block on it. See
`doc/working/todo_fb.md` § "Pull vs Push — Deliberate Decision" for
the full rationale.

**Flow diagram**:

```
WatchNotify — server-initiated request-response

Client                               Server
  │                                    │
  │── RegisterWatch(prefix) ──────────►│  normal call()
  │◄────────── RegisterAck ────────────│  watch_id=42
  │                                    │
  │  (key "foo/bar" written)           │
  │◄────────── NotifyReq(id=R) ────────│  server sends request
  │── NotifyAck(R) ───────────────────►│  server awaits ack
  │                                    │  ack received → done
  │                                    │
  │  (client drops, no ack)            │
  │◄────────── NotifyReq(id=R2) ───────│  retry
  │  (still no ack)                    │
  │                                    │  log WSCritical
  │                                    │  retry on next change
  │                                    │

LearnerStream / StreamSnapshot — normal request-response

Client                               Server
  │                                    │
  │── GiveMeSlots(N..M, id=R1) ───────►│
  │◄────────── LogEntries(R1) ─────────│
  │── GiveMeSlots(M+1..P, id=R2) ─────►│
  │◄────────── LogEntries(R2) ─────────│
  │   ...                              │
  │                                    │
```

**Edge cases at a glance**:

- Server sends notify request, client doesn't ack → server retries
  per retry policy; after retries exhausted, logs `WSCritical`,
  retries on next change.
- Server sends notify request, connection drops → `RpcClient`'s
  `fail_all` fires the pending entry with `ConnectionClosed`; the
  server treats it as a failed notify (log + retry on next change).
- Client receives a request with an unregistered `msg_type` →
  client sends `UnknownMessage` response (same as server-side
  behavior today).
- Client receives a request with a `request_id` that matches a
  pending client-sent request → `request_id` routing takes
  precedence (it's a response, not a request); the frame is
  routed to the pending callback, not the handler dispatch.
- Concurrent server-initiated requests on one connection →
  multiple `request_id`s in the server's `RpcClient` pending map;
  the connection's send queue interleaves frames (the
  per-connection writer is already lock-free).

**Dependencies**

- **Depends on**: R104 (crow-rpc engine — framing, connection,
  correlation, FFI). R104 is finished.
- **Depended on by**: **R32** (LearnerStream, StreamSnapshot),
  **R117** (WatchNotify). Both are blocked until R114 lands.

**Acceptance**

**RequestId generator**:
- `RequestIdGen::next()` returns monotonically increasing `u64`
  values, thread-safe (concurrent `next()` calls never return the
  same value). Unit test.
- `RequestId(u64)` newtype: `as_u64()` returns the inner value;
  the type is not accidentally interchangeable with raw `u64` at
  call sites (compile-time check). Unit test.
- C++ `RpcClient::next_request_id_` removed; the 4 test call sites
  (`client_pool_test.cpp` L254, `load_test.cpp` L142/L276/L402)
  use `crow-common`'s `RequestIdGen`. Tests pass.
- `DiskioClient` uses the shared `RequestIdGen` instead of its own
  `AtomicU64`. Integration test (`cargo test -p crow-diskio-client`).

**Server-side request-response**:
- Server sends a request via `RpcClient::send`, client responds,
  server's `on_response` fires the pending callback. Integration
  test.
- Server sends a request, client drops, server's `fail_all` fires
  the pending entry with `ConnectionClosed`. Integration test (kill
  client mid-request).

**Client-side request handler dispatch**:
- Client receives a request frame with a registered `msg_type` →
  handler dispatched, response sent with the same `request_id`.
  Integration test.
- Client receives a request frame with an unregistered `msg_type`
  → `UnknownMessage` response sent. Integration test.
- Client receives a response frame (matching a pending
  client-sent request) → routed to the pending callback, not the
  handler dispatch. Integration test.

**WatchNotify retry + WSCritical**:
- Server sends notify request, client acks → server pending entry
  removed, no retry. Integration test.
- Server sends notify request, client doesn't ack → server
  retries per policy; after retries exhausted, `WSCritical` logged,
  pending entry removed. Integration test.
- Server sends notify request, connection drops → `fail_all` fires
  with `ConnectionClosed`; server logs + retries on next change.
  Integration test.

**Test commands**: `pixi run cargo test -p crow-rpc-ffi --test
ffi_request_test`, `pixi run cargo test -p crow-diskio-client`,
`pixi run cargo fmt --all -- --check`,
`pixi run cargo clippy --all-targets -- -D warnings`.

**Open Questions**

None — all design decisions resolved in `doc/working/todo_fb.md`
§ "R114 — Revised Design" (including "Copy Streaming —
Application-Level, Not Transport-Level" and "Pull vs Push —
Deliberate Decision"). The zero-copy data-payload rule for streaming
handlers is canonical in `design-crow-rpc.md` §6.
