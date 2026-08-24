<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Flatbuffer RPC Migration — Todo + Notes

Tracking doc for the gRPC → crow-rpc (flatbuffer) migration across
all CROW services. Backlog items: R114 (streaming), R115 (diskdb),
R116 (chunkdb), R117 (KvService client-facing), R32 (KV consensus).

## Rules to Follow During Migration

These rules apply to **every** migration item (R32, R115, R116,
R117). The formal spec lives in `design-crow-rpc.md` §6 (Flatbuffer
Wrapper Convention); this section is the quick-reference checklist.

- **`FB` prefix for all flatbuffer types.** Table, enum, and struct
  names in `.fbs` schemas use the `FB` prefix (`FBDiskWriteRequest`,
  `FBMsgType`, `FBDiskIoRetCode`). Already established by R104/R105;
  mandatory for new schemas.
- **The flatbuffer object IS the buffer.** A flatbuffer table is not
  a Rust/C++ object with owned fields — it is a typed view over a
  byte buffer. Field access is a direct memory-offset read through
  the flatbuffers runtime accessor (`fb.field()` in Rust,
  `fb->field()` in C++). There is no deserialization step.
- **No owned intermediate struct, no per-field copy.** Do NOT
  deserialize the flatbuffer into a separate owned Rust struct or
  C++ class that copies each field out of the buffer. The buffer is
  the object; accessors read from it in place. This is the core
  rule — violating it defeats the purpose of flatbuffers.
- **Wrapper classes in `crow-protocol`, defined when required.**
  When a service needs a typed API over a flatbuffer buffer
  (encapsulate parse + null-check + field access, add domain logic,
  hide the raw generated type), define a **wrapper class** in
  `crow-protocol` that holds a reference to the buffer and exposes
  typed accessor methods. The wrapper does NOT copy fields — it
  reads through the flatbuffer root pointer on every accessor call.
  Define wrappers **when a service needs them**, not preemptively
  for every flatbuffer type. Because they live in `crow-protocol`,
  every project (crow-kv, crow-diskdb, crow-chunkdb, crow-diskio)
  shares one definition.
- **No extra allocation on the read path.** The control buffer is
  pool-allocated (C++) or `Bytes`-backed (Rust FFI); the wrapper
  holds a reference to it. Accessor calls are pure pointer-offset
  reads — no heap allocation, no `Vec`, no `String` construction
  unless the caller explicitly converts a field to an owned type.
  The data payload (raw bytes after the control message) is not a
  flatbuffer; whether it is copied depends on what the receiver does
  with it. Streaming-data handlers (LearnerStream, StreamSnapshot)
  consume the data with `pwrite`/`apply` — both take `&[u8]`, no
  owned bytes needed — so they hold the data buffer by reference and
  drop it after the async write completes (zero-copy). The "copy to
  owned `Vec`" exception applies only when a handler genuinely needs
  to retain the data beyond the frame's lifetime. Full rule in
  `design-crow-rpc.md` §6 ("Data payload: zero-copy when the receiver
  consumes by reference").
- **Write path: build, finish, attach.** The sender builds the
  flatbuffer with `FlatBufferBuilder`, calls `finish`, and attaches
  the finished bytes to the frame's control buffer. The builder is
  dropped after the buffer is attached — no retained builder state.

### Anti-patterns to avoid

- Deserializing `FBDiskWriteRequest` into a `DiskWriteRequest` Rust
  struct with `String` + `Vec` fields, then passing that struct to
  the handler. This copies every field — defeats zero-copy.
- Calling `fb.disk_id().to_string()` or `fb.strips().to_vec()` on
  the hot path. These allocate. Use the flatbuffer reference
  directly; convert to owned only at the boundary where the caller
  truly needs owned data.
- Defining wrapper classes per-service (e.g. one in `crow-diskdb`,
  one in `crow-chunkdb`) for the same flatbuffer type. Define once
  in `crow-protocol`, share everywhere.
- Adding a `prost`-to-flatbuffer bridge that decodes protobuf into
  a Rust struct then re-encodes to flatbuffer. This is a full
  deserialize + reserialize on every call. The decision is: full
  `.fbs` conversion, no bridge (R32 Open Question — resolved).

## Migration Order

```
R115 (diskdb, unary, proof-of-pattern)
  ↓ validates: schema conversion, wrappers, error mapping, mixed rollout
R114 (streaming support)
  ↓ enables: LearnerStream, StreamSnapshot, WatchNotify
R32 (KV consensus, internal — needs R114 + R115 pattern)
  ↓ validates: KV NotLeaderHint flatbuffer model, kv_rpc.fbs schema
R117 (KvService client-facing — needs R114 + R32)
R116 (ChunkdbService — after chunk-layer refactor stabilizes)
```

Rationale:
- R115 first — 11 unary RPCs, no streaming, independent of everything.
  Cheapest way to validate the full `.fbs` conversion approach +
  zero-copy wrapper convention + error mapping + mixed rollout before
  tackling streaming.
- R114 next — unblocks R32 and R117 (the three streaming RPCs).
- R32 after R114 — highest perf value (recovers the ~17% h2-lock
  loss); R115 has proven the unary pattern by then.
- R117 after R32 — reuses the `kv_rpc.fbs` schema sub-range +
  `NotLeaderHint` flatbuffer model validated by R32.
- R116 last — blocked on the chunk-layer refactor anyway (R113),
  and chunkdb is the newest service with the least production
  exposure.

## R114 — Revised Design (request_id, no stream)

**Decision**: crow-rpc is a raw socket protocol, not HTTP/2. A
connection is a persistent bidirectional byte stream; every frame is
`[12-byte header][flatbuffer control][data]`. Frames are correlated by
`request_id` (extracted from the control message during parse). There
is no "stream" concept — just requests and responses, each carrying an
id. R114's original design (`FBStreamOpen/Close` control messages,
`stream_id` field, `StreamHandlerFn`/`StreamWriter`/`StreamReader`,
`open_stream` handshake) imports HTTP/2's stream model onto a protocol
that doesn't need it. **R114 is rewritten to drop all of that.**

What the existing engine already gives us:
- `HandlerFn` receives `Frame* + Connection*`; slow handlers return
  `nullptr` and submit response(s) later via `transport->submit`
  (`server/handler.h` L25-26). Nothing limits a handler to one
  response frame.
- `on_response(request_id, Frame*)` routes by `request_id`
  (`client/client.h` L130). The pending map is the only correlation
  mechanism.
- `request_id` is extracted from the flatbuffer control during parse
  (`framing.h` L50). It is the per-frame correlation key.

What's actually missing (minimal):
- **One request, one response.** Every RPC is one request frame → one
  response frame, correlated by `request_id`. No multi-response, no
  `FLAG_LAST_FRAME`, no `call_multi`, no new handler type. The
  existing `send()` + `on_response()` + `HandlerFn` already handle
  this. The three KV RPCs map onto this model:
  - *LearnerStream*: each "give me slots N..M" is a normal `call()`
    with its own `request_id`, gets one response. The follower sends
    the next call when ready. Zero protocol change.
  - *StreamSnapshot*: the client sends "give me chunk N" → server
    responds with chunk N. Repeat per chunk. Each is a normal
    `call()`. Zero protocol change.
  - *WatchNotify*: server-initiated request-response — the server
    sends a notify **request**, the client sends an ack
    **response**. See Resolved (continued) below.

**RequestId generator consolidation**: the `request_id` generator
moves to `crow-common` so every service client shares one definition
instead of each re-implementing `AtomicU64::new(1)` + `fetch_add`.
Currently duplicated:
- C++ `RpcClient::next_request_id_` (`crow-rpc/include/crow-rpc/
  client/client.h` L162) — used by 4 test call sites.
- Rust `DiskioClient::next_req_id` (`crow-diskio-client/src/client.rs`
  L86) — own `AtomicU64`.
- Rust `CrowkvClient` uses `(client_id, next_seq)` — a higher-level
  idempotency key, NOT the RPC `request_id`. When R117 migrates
  kv-client to crow-rpc, it'll need the shared RPC `request_id`
  generator alongside the existing `(client_id, seq)`.

### Resolved

- **RequestId generator**: per-client `RequestIdGen` struct in
  `crow-common`. Per-client counter → smaller slab pool + pending
  hashmap.
- **RequestId type**: newtype `RequestId(u64)`, internal `AtomicU64`
  (thread-safe `fetch_add(1, Relaxed)`), `next()` returns `u64`.
- **C++ `RpcClient::next_request_id()` removal**: remove it, move to
  `crow-common/cpp`. Verified: only 4 test call sites use it, zero
  production code.
- **One request, one response**: every RPC is one request frame → one
  response frame. No multi-response, no `FLAG_LAST_FRAME`, no
  `call_multi`, no new handler type. StreamSnapshot = per-chunk
  `call()`. LearnerStream = per-batch `call()`.

### Resolved (continued)

- **WatchNotify: server-initiated request-response.** The notify is
  a **request** (server→client), the client sends an **ack
  response**, and the server retries on timeout. If retries are
  exhausted, the server logs `WSCritical` and retries the notify on
  the next change. This is normal request-response in the reverse
  direction — no protocol change. The implementation gap is the
  "other half" of request-response on each side:
  - **Server side**: reuse `RpcClient` (keep the name) for
    request-response correlation — send a notify request, await
    ack, retry. The existing `HandlerRegistry` +
    `transport->submit` already handle incoming requests and
    outgoing responses.
  - **Client side**: add a `HandlerRegistry`-like dispatch (handle
    incoming requests by `msg_type`) + `transport->submit` to send
    responses. The existing `RpcClient` already handles sending
    requests and routing responses.

### Copy Streaming — Application-Level, Not Transport-Level

The "streaming is copy streaming" concern is about where the copy
happens, not whether the protocol needs a stream concept. Two layers:

- **Transport is zero-copy.** Pool-allocated ref-counted buffers,
  pull-based `FrameParser` (reads directly into pool buffers, no
  scratch), scatter-gather `writev` send. The frame's data payload
  rides in a pool buffer from receive to handler.
- **Application copies when it wants owned bytes.** The §6 wrapper
  convention's data-payload exception ("may be copied into an owned
  `Vec<u8>` when the caller needs owned bytes") is a caller choice,
  not a transport limitation.

For the two data-heavy streaming RPCs, the receiver does
`pwrite(fd, &buf, len)` and `engine.apply(slot, &batch)` — both take
`&[u8]` for the duration of the call, neither needs owned bytes. So
the handlers stay zero-copy on the data payload: hold the frame's
data buffer by reference, pass `&[u8]` to `pwrite`/`apply`, drop the
reference after the async write completes. The pool buffer recycles
when the last reference drops. This is the receive-side companion to
§6's "no owned intermediate struct" rule for the control buffer; the
rule itself now lives in `design-crow-rpc.md` §6 ("Data payload:
zero-copy when the receiver consumes by reference").

The "copy to owned `Vec`" exception applies only when a handler
genuinely needs to retain the data beyond the frame's lifetime (e.g.
queuing it for later processing without holding the pool buffer). The
streaming-data handlers do not.

### Pull vs Push — Deliberate Decision

R114 uses pull: the follower sends per-batch/per-chunk `call()` and
gets one response per call. The alternative — push (one "start from
slot N" request → server streams response frames as entries arrive)
— is lower-latency for steady-state catch-up but needs the
multi-response machinery R114 dropped (`FLAG_LAST_FRAME`, `call_multi`,
new handler type).

Pull is the right call for R114:
- StreamSnapshot is inherently pull (follower requests chunks at its
  own pace → natural backpressure).
- WatchNotify is server-initiated request-response (the revised
  design above).
- LearnerStream catch-up is the only case where push helps, and
  pipelining (multiple in-flight `call()`s on the persistent
  connection) closes most of the gap without protocol change.
- LearnerStream steady-state traffic (Accept→Accepted, Chosen→one-way)
  maps to `call()` + one-way send on a persistent connection — no
  stream needed.

Push (`call_multi` + `FLAG_LAST_FRAME`) is an additive future
extension if catch-up latency under pipelining proves insufficient.
R114 does not block on it.

## Suggestions (apply across all migration items)

### 1. Shared error-mapping helper

R115 work item 5 maps `RpcError` → service errors. Each of the four
services (R32, R115, R116, R117) would reimplement the same
retryable-error classification. Extract a shared helper in
`crow-rpc-ffi` instead:

- `RpcError::is_retryable(&self) -> bool` — returns true for
  `ConnectionClosed`, `Timeout`, `SendQueueFull` (transport-level
  retryable); false for `RegistrationFailed`, `AllDown`.
- Or a `RetryPolicy` enum on `RpcError` (`RetryImmediately`,
  `RetryWithBackoff`, `Fail`).

Not blocking, but avoids 4 copies of the same logic. Define in R115
(the first migration) and reuse in R32/R116/R117.

### 2. `grpc_endpoint` → `rpc_endpoint` — DONE (R115)

DONE as part of R115. Renamed the proto field to `rpc_endpoint` in all
3 proto messages (`InstanceValue`, `ChunkdbRangeBindingValue` in
`sysdata_type.proto`; `NotMyRangeHint` in `chunkdb_type.proto`).
Protobuf binary wire format uses tag numbers (not field names), so
this is binary-wire-compatible — no `#[prost(rename)]` needed. Updated
all 29 Rust files, 3 TS files, 4 C++ files that referenced
`grpc_endpoint`. The keepalive struct `with_grpc_endpoint` →
`with_rpc_endpoint`. FFI parameter `grpc_endpoint` → `rpc_endpoint`
in `crow-kv-client/src/ffi.rs` + `c_api.h` (no ABI change — it's a
`*const c_char` either way).

### 3. `msg_type.fbs` sub-range coordination

R32 and R117 both use the 1000s range (KV). Decide the sub-range
split once (in R32, since it lands first) and reference it in R117:

- Consensus (R32): 1000–1099
- Client-facing (R117): 1100–1199

Other ranges (already reserved in `msg_type.fbs` comments):
- sys meta: 2000s
- diskdb (R115): 3000s
- chunkdb (R116): 3300s
- diskio (R105, done): 3600s

### 4. Wrapper class location

The zero-copy wrapper convention (`design-crow-rpc.md` §6) says
wrappers live in `crow-protocol`. Open question: module layout.

Option A (current R115 proposal): `lib/crow-protocol/src/fb_wrappers/
{diskdb,chunkdb,kv_client}.rs` — separate module, one file per
service.

Option B: extension traits on the generated flatbuffer types, in
`lib/crow-protocol/src/lib.rs` alongside the `pub mod fb` re-exports.

Recommendation: Option A. Separate module keeps the generated code
(read-only, `unsafe_code = "deny"` opt-out) cleanly separated from
the safe wrapper layer. The `fb_wrappers` module is the public,
safe, domain-typed surface; the generated `*_generated` modules are
the raw flatbuffer runtime.

R115 (first migration) establishes the convention — diskdb wrappers
in `lib/crow-protocol/src/fb_wrappers/diskdb.rs`. R116/R117/R32
follow the same layout.

### 5. Mixed-rollout cutover mechanism

R115/R116/R117 all say "clients switch via a config flag." Decide
the mechanism once:

- **Option A**: separate ports — gRPC server on the old port,
  crow-rpc server on a new port. Clients pick the port from config.
  Simple, no protocol negotiation. Downside: two ports to manage.
- **Option B**: same port, protocol detection — the server peeks
  the first bytes; gRPC starts with `PRI * HTTP/2`, crow-rpc starts
  with `0xCA70` magic. Downside: complex, and the two servers can't
  share a socket cleanly.
- **Option C**: config-driven server mode — the server runs either
  gRPC or crow-rpc, not both. Rolling upgrade: switch one node at a
  time. Downside: no mixed-rollout window; clients must be updated
  in lockstep.

Recommendation: Option A (separate ports). It's the simplest and
matches how R105/diskio already works (diskio runs crow-rpc on its
own port; the legacy gRPC port is unused). The service registry
stores both ports during the rollout window; clients pick based on
their config.

### 6. Benchmark baseline capture

Before starting R115, capture gRPC baselines for all four services:
- diskdb: concurrent `AllocateBlocks` throughput at 1T:1C + 2T:1C
- chunkdb: concurrent `AppendChunk` throughput
- KV consensus: 2T:1C read bench (already measured — ~17% loss)
- KV client: concurrent Put throughput

These baselines are the regression targets for the crow-rpc paths.
Without them, "no regression" is unmeasurable. Capture in
`doc/design/rpc/rpc-migration-baselines.md` (new) or extend
`rpc-echo-flow-analysis.md`.

## Per-Item Checklist (apply to each migration)

- [ ] `.fbs` schema created in `lib/crow-protocol/src/fbs/`
- [ ] `msg_type.fbs` extended with the service's range
- [ ] `build.rs` + `lib.rs` re-exports updated
- [ ] Zero-copy wrappers in `lib/crow-protocol/src/fb_wrappers/`
- [ ] Server handler rewritten (dispatch by `msg_type`)
- [ ] Client rewritten (`RpcClient` + `ConnectionPool`)
- [ ] Error mapping (`RpcError` → service error variants)
- [ ] `grpc_endpoint` → `rpc_endpoint` (if not already done)
- [ ] Mixed-rollout: both servers run, clients switch via config
- [ ] Benchmark: gRPC baseline vs crow-rpc, no regression at 1T:1C
- [ ] Cutover: gRPC server removed, `.proto` stays as legacy/reserved
- [ ] Tests pass: `cargo test -p <service>`, `cargo fmt --check`,
      `cargo clippy -- -D warnings`

## R115 — diskdb Migration Status (2026-08-24)

### Completed

- **Schema**: `diskdb.fbs` with all 11 request/response tables, `FBDiskdbRetCode` enum, `FBHwStatus`/`FBDiskType`/`FBZoneAllocationState` enums. `msg_type.fbs` extended with 3000s range. `build.rs` + `lib.rs` re-exports.
- **Server handlers**: All 11 `DiskdbRpcService` handlers implemented in `app/crow-diskdb/src/service/diskdb_rpc_service.rs` — allocate, free, commit, query_capacity, get_disk_group_info, get_disk_info, rebuild_zone_bitmap, recalc_disk_usage, compact_zone, trigger_scan, get_scan_status.
- **Server wiring**: `crow-rpc` server started alongside gRPC in `main.rs`; `rpc_listen_addr` added to `DdbConfig` + config file + validation.
- **Client transport**: `DiskdbRpcTransport` in `lib/crow-diskdb-client/src/rpc_transport.rs` — builds flatbuffer requests, sends via `RpcClient::call`, parses flatbuffer responses into existing proto types. `DiskdbClient.with_rpc_transport()` builder selects crow-rpc when set; falls back to tonic gRPC otherwise.
- **Error mapping**: `From<RpcError> for DiskdbClientError` — retryable → `Unreachable`, non-retryable → `Rpc`. `FBDiskdbRetCode` mapped to `DiskdbClientError` variants.
- **E2E tests**: `diskdb_rpc_transport_test.rs` — full flow via crow-rpc (allocate, free, query drill-down, recalc, compact+reclaim, trigger_scan, get_scan_status, rebuild_zone_bitmap). All pass alongside existing gRPC E2E test.
- **Bug fix**: `build_query_capacity_response` now passes `include_zones = true` for disk-level queries (was `false`, causing empty `zone_usages`).
- **`grpc_endpoint` → `rpc_endpoint` rename**: Renamed the proto field in all 3 proto messages (`InstanceValue`, `ChunkdbRangeBindingValue`, `NotMyRangeHint`). Protobuf binary wire format uses tag numbers, so this is binary-wire-compatible. Updated all 29 Rust files, 3 TS files, 4 C++ files.
- **`conn_handle` lifetime safety**: Added a live-connection registry to `SocketTransport` that maps `Connection*` → `weak_ptr<Connection>`. `submit()` looks up the connection before accessing it; stale handles return false instead of crashing. The `on_close` callback unregisters connections when they close.

### Open Issues (deferred to follow-up items)

- **Zero-copy wrappers** (R115 follow-up): The current client transport parses flatbuffer responses into owned proto types (allocates per response). The design doc's "no owned intermediate struct" rule is violated for the client side — this is acceptable during the mixed-rollout window but should be addressed in a follow-up that switches the client to use flatbuffer views directly.
- **Mixed-rollout cutover**: Both gRPC and crow-rpc servers run simultaneously. The client selects transport via `with_rpc_transport()`. No config-based toggle yet — callers must explicitly enable crow-rpc.
- **R114/R32/R116/R117**: Other migration items not yet started. R114 (streaming) has a revised design but no implementation. R32 (KV consensus) is the highest-risk item. R116 (chunkdb) and R117 (KvService client-facing) follow the same pattern as R115.
