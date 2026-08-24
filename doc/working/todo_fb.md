<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Flatbuffer RPC Migration — Todo + Notes

Tracking doc for the gRPC → crow-rpc (flatbuffer) migration across
all CROW services. Backlog items: R114 (done), R115 (done), R116
(chunkdb), R117 (KvService client-facing), R32 (KV consensus).

## Rules to Follow During Migration

These rules apply to **every** migration item (R32, R116, R117). The
formal spec lives in `design-crow-rpc.md` §6 (Flatbuffer Wrapper
Convention); this section is the quick-reference checklist.

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
R115 (diskdb, unary)         — DONE
R114 (bidirectional req-resp) — DONE
R32 (KV consensus, internal)  — NEXT
  ↓ validates: KV NotLeaderHint flatbuffer model, kv_rpc.fbs schema
R117 (KvService client-facing — needs R114 + R32)
R116 (ChunkdbService — after chunk-layer refactor stabilizes)
```

Rationale:
- R115 done — validated the full `.fbs` conversion approach +
  zero-copy wrapper convention + error mapping + mixed rollout.
- R114 done — bidirectional request-response (server can send
  requests, client can handle requests) + `RequestIdGen` in
  `crow-common`. Unblocks R32 and R117.
- R32 next — highest perf value (recovers the ~17% h2-lock loss);
  R115 has proven the unary pattern.
- R117 after R32 — reuses the `kv_rpc.fbs` schema sub-range +
  `NotLeaderHint` flatbuffer model validated by R32. Also needs
  R114's server→client request path for WatchNotify.
- R116 last — blocked on the chunk-layer refactor anyway (R113),
  and chunkdb is the newest service with the least production
  exposure.

## Suggestions (apply across all migration items)

### 1. Shared error-mapping helper — DONE (R115)

`RpcError::is_retryable(&self) -> bool` is implemented in
`crow-rpc-ffi/src/server.rs`. Returns true for `ConnectionClosed`,
`Timeout`, `SendQueueFull`, `ConnectionError`; false for
`RegistrationFailed`, `AllDown`, `InvalidArg`. Reused by R32/R116/R117.

### 2. `grpc_endpoint` → `rpc_endpoint` — DONE (R115)

Renamed the proto field to `rpc_endpoint` in all 3 proto messages
(`InstanceValue`, `ChunkdbRangeBindingValue`, `NotMyRangeHint`).
Protobuf binary wire format uses tag numbers, so this is
binary-wire-compatible. Updated all 29 Rust files, 3 TS files, 4 C++
files. The keepalive struct `with_grpc_endpoint` →
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
- diskdb (R115, done): 3000s
- chunkdb (R116): 3300s
- diskio (R105, done): 3600s

### 4. Wrapper class location

The zero-copy wrapper convention (`design-crow-rpc.md` §6) says
wrappers live in `crow-protocol`. R115 established the convention:
diskdb wrappers in `lib/crow-protocol/src/fb_wrappers/diskdb.rs`.
R116/R117/R32 follow the same layout — separate module, one file per
service, keeping the generated code (read-only, `unsafe_code = "deny"`
opt-out) cleanly separated from the safe wrapper layer.

### 5. Mixed-rollout cutover mechanism

R115/R116/R117 all say "clients switch via a config flag." The
mechanism decided in R115: **separate ports** (Option A). gRPC
server on the old port, crow-rpc server on a new port. Clients pick
the port from config. Matches how R105/diskio already works (diskio
runs crow-rpc on its own port; the legacy gRPC port is unused). The
service registry stores both ports during the rollout window;
clients pick based on their config.

### 6. Benchmark baseline capture

Before starting R32, capture gRPC baselines:
- KV consensus: 2T:1C read bench (already measured — ~17% loss)
- KV client: concurrent Put throughput
- chunkdb: concurrent `AppendChunk` throughput (for R116)

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
- [ ] `grpc_endpoint` → `rpc_endpoint` (already done — skip)
- [ ] Mixed-rollout: both servers run, clients switch via config
- [ ] Benchmark: gRPC baseline vs crow-rpc, no regression at 1T:1C
- [ ] Cutover: gRPC server removed, `.proto` stays as legacy/reserved
- [ ] Tests pass: `cargo test -p <service>`, `cargo fmt --check`,
      `cargo clippy -- -D warnings`

## Open Issues (deferred to follow-up items)

- **R115 zero-copy wrappers**: The current diskdb client transport
  parses flatbuffer responses into owned proto types (allocates per
  response). The design doc's "no owned intermediate struct" rule is
  violated for the client side — acceptable during the mixed-rollout
  window but should be addressed in a follow-up that switches the
  client to use flatbuffer views directly.
- **R115 mixed-rollout cutover**: Both gRPC and crow-rpc servers run
  simultaneously. The client selects transport via
  `with_rpc_transport()`. No config-based toggle yet — callers must
  explicitly enable crow-rpc.
- **R114 client handler dispatch E2E gap**: The
  `client_handler_dispatch_via_server_chain` test registers a
  client-side `NOTIFY` handler but never exercises it — the server's
  PING handler builds a NOTIFY request buffer but drops it (`let _ =
  (nreq_id, ctrl);`) because `request_client.send()` needs a
  `&Connection` wrapper but the handler only has the raw
  `conn_handle` pointer. The test only verifies the PING→ack path
  (already covered by `server_dispatch_handler_first_order`). The
  client's `dispatch_request` path (server sends request → client
  handler fires → client acks) needs a proper E2E test. Blocked on
  the FFI gap below.
- **R114 server→client send FFI gap**: `RpcClient::send()` takes
  `&Connection` (a Rust wrapper), but a server-side handler only has
  the raw `conn_handle` (`*mut c_void`) from `ServerRequest`. There
  is no FFI helper to send a request from a raw `conn_handle`. R117
  will need either a `send_raw(server, conn_handle, ...)` FFI method
  or a way to reconstruct a `Connection` wrapper from the raw
  pointer. Without this, the server cannot initiate requests to the
  client from within a handler.
- **R114 server→client timeout test missing**: The
  `server_to_client_timeout_no_handler` test (client doesn't ack,
  reaper times out) was dropped during the dispatch-order fix. The
  timeout/error path for server-initiated requests is untested.
  Should be re-added once the send-FFI gap is resolved (the test
  needs the server to actually send a request to the client).
- **R114 `fail_all` is all-or-nothing**: `fail_all(ConnectionClosed)`
  fires for ALL pending entries on the `request_client_`, not
  per-connection. Fine for R114's single-connection test scope, but
  R117 (WatchNotify with multiple watcher connections) will need
  per-connection scoping — either a per-connection `RpcClient` or a
  connection-scoped `fail_all`. Flagged as R117's scope.
