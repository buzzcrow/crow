<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Flatbuffer RPC Migration — Todo + Notes

Tracking doc for the gRPC → crow-rpc (flatbuffer) migration across
all CROW services. Backlog items: R114 (done), R115 (done), R116
(chunkdb), R117 (KvService client-facing), R32 (KV consensus — done).

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
R32 (KV consensus, internal)  — DONE
  ↓ validated: KV NotLeaderHint flatbuffer model, kv_consensus.fbs schema
R117 (KvService client-facing — needs R114 + R32) — DONE
  ↓ validated: zero-copy Ref wrappers, with_rpc_transport, forwarded loop-guard
R116 (ChunkdbService — after chunk-layer refactor stabilizes) — NEXT
```

Rationale:
- R115 done — validated the full `.fbs` conversion approach +
  error mapping + mixed rollout. (Zero-copy wrapper convention
  deferred — R115 parses into owned proto types; R32 implements
  zero-copy properly, R115 retrofit is a follow-up — see Open
  Issues.)
- R114 done — bidirectional request-response (server can send
  requests, client can handle requests) + `RequestIdGen` in
  `crow-common`. Unblocks R32 and R117. (Server→client send FFI gap
  + E2E test gap + timeout test — carried as open issues, resolved
  by R32 work item 7 — see Open Issues.)
- R32 done — highest perf value (recovers the ~17% h2-lock loss);
  R115 has proven the unary pattern. Open questions resolved (see
  R32 doc § Resolved Questions): `kv_consensus.fbs` schema,
  zero-copy wrappers, `LearnerStream` as persistent-connection
  request-response (not R114 bidi), separate consensus + client
  ports, R32 resolves the R114 server→client send FFI gap to
  unblock R117.
- R117 done — reuses the `kv_consensus.fbs` schema sub-range
  split (R32: 1000-1099, R117: 1100-1199) + `NotLeaderHint`
  flatbuffer model + zero-copy wrapper pattern validated by R32.
  Also uses R32's `Connection::from_handle` FFI helper for
  WatchNotify server-push. Established the zero-copy `Ref` wrapper
  pattern (R115 deferred this, R117 implemented it properly) +
  `with_rpc_transport` programmatic selection + `forwarded` loop-guard
  for transparent leader-forwarding.
- R116 next — reuses R115's unary-only migration pattern (diskdb) +
  R117's zero-copy `Ref` wrapper pattern + `with_rpc_transport`
  programmatic selection. All 8 ChunkdbService RPCs are unary (no
  streaming, R114 not needed). Port: `CHUNKDB_RPC_BASE = 9961`
  (fills gap between diskdb RPC 9931-9940 and chunkdb gRPC
  9971-9990). `NotMyRangeHint` is diagnostic-only (no leader
  endpoint — client refreshes from group-0 + re-routes). The
  chunk-layer refactor (R113) is NOT done but strip prefetch is
  already inside `ChunkWriter`, RPC call sites are consolidated
  and stable. The allocator pool (`pool.rs`) calls diskdb (not
  chunkdb) — that path is R115's scope, already done, NOT changed
  by R116.

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

### 3. `msg_type.fbs` sub-range coordination — DECIDED (R32)

R32 and R117 both use the 1000s range (KV). Sub-range split decided
in R32 (lands first), referenced in R117:

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

R115 (diskdb), R32 (KV consensus), R117 (KV client-facing) — DONE.
R116 (chunkdb) — NEXT. Checklist status per item:

- [x] `.fbs` schema created in `lib/crow-protocol/src/fbs/`
      (R115: diskdb.fbs, R32: kv_consensus.fbs, R117: kv_client.fbs)
- [x] `msg_type.fbs` extended with the service's range
      (R115: 3000s, R32: 1000s, R117: 1100s)
- [x] `build.rs` + `lib.rs` re-exports updated
- [x] Zero-copy wrappers in `lib/crow-protocol/src/fb_wrappers/`
      (R32: kv_consensus.rs, R117: kv_client.rs, R115: diskdb.rs —
      R115 retrofitted, see Open Issues)
- [x] Server handler rewritten (dispatch by `msg_type`)
      (R115: diskdb, R32: px_rpc_service, R117: kv_rpc_service)
- [x] Client rewritten (`RpcClient` + `ConnectionPool`)
      (R115: DiskdbRpcTransport, R117: KvRpcTransport — R32 uses
      PxRpcTransport)
- [x] Error mapping (`RpcError` → service error variants)
- [x] `grpc_endpoint` → `rpc_endpoint` (already done — skip)
- [x] Mixed-rollout: both servers run, clients switch via
      `with_rpc_transport()` (R115, R117 — gRPC server NOT yet
      removed, see Open Issues)
- [ ] Benchmark: gRPC baseline vs crow-rpc, no regression at 1T:1C
      (NOT done for any service — baselines not captured, see
      Suggestions §6)
- [ ] Cutover: gRPC server removed, `.proto` stays as legacy/reserved
      (NOT done for any service — all three still run both servers)
- [x] Tests pass: `cargo test -p <service>`, `cargo fmt --check`,
      `cargo clippy -- -D warnings`

## Open Issues (deferred to follow-up items)

- **R115 zero-copy wrappers** → **RESOLVED**: Retrofitted zero-copy
  `FB<Type>Ref` wrappers onto `DiskdbRpcTransport`. Created
  `lib/crow-protocol/src/fb_wrappers/diskdb.rs` (11 wrapper structs
  for all diskdb response types, mirroring R117's `kv_client.rs`
  pattern). Updated `rpc_transport.rs` to use
  `FBAllocateResponseRef::new(buf)` etc. instead of
  `flatbuffers::root::<FB<Type>>`. R116 followed R117's pattern from
  the start (no retrofit needed).

- **R115 mixed-rollout cutover** → **still open (applies to R115,
  R117, and future R116)**: Both gRPC and crow-rpc servers run
  simultaneously. The client selects transport via
  `with_rpc_transport()`. No config-based toggle yet — callers must
  explicitly enable crow-rpc. The gRPC server is not yet removed
  from any service.

- **R114 client handler dispatch E2E gap** → **RESOLVED**: Fixed the
  `client_handler_dispatch_via_server_chain` test to actually send
  the NOTIFY request via `request_client.call_to_handle()` using the
  raw `req.conn_handle`. The test now verifies the full
  server→client→server roundtrip: PING → server handler → NOTIFY to
  client → client NOTIFY handler fires → ack → server receives ack.
  Also added `notify_handler_fired` AtomicBool to verify the client
  handler actually executed.

- **R114 server→client send FFI gap** → **RESOLVED by R32 + FFI
  fix**: R32 added `Connection::from_handle(raw)`, but
  `Connection::from_handle` + `RpcClient::send`/`call` was
  type-confused: the handler's `conn_handle` is a raw `Connection*`
  (C++), but `crow_rpc_client_send` expects a `crow_rpc_conn_s*`
  (which wraps `shared_ptr<Connection>`). Passing a `Connection*`
  where `crow_rpc_conn_s*` is expected dereferences invalid memory
  (SIGSEGV). **Fix**: added `crow_rpc_client_send_conn` C ABI
  function that takes `void *conn_handle` (raw `Connection*`) and
  calls `RpcClient::send` directly. Added Rust
  `RpcClient::send_to_handle`/`call_to_handle` methods. Fixed R117's
  production code (`send_watch_notify_error` + `CrowRpcPushTarget`)
  to use `send_to_handle`. This was a latent segfault bug in R117's
  WatchNotify push path — it would have crashed on the first
  server→client send.

- **R114 server→client timeout test missing** → **RESOLVED**: Added
  `server_to_client_timeout_no_handler` test: server sends a request
  with a msg_type the client has no handler for (and no transport
  set, so the frame is dropped). The `request_client`'s reaper
  (300ms timeout, 50ms scan) times out the pending entry. The test
  verifies the `CallFuture` resolves with `Err(RpcError::Timeout)`.

- **R114 `fail_all` is all-or-nothing** → **still open; flagged as
  R117's scope but R117 did not address it**: `fail_all(
  ConnectionClosed)` fires for ALL pending entries on the
  `request_client_`, not per-connection. R117's WatchNotify uses
  fire-and-forget `send()` (no pending entries on the client side),
  so the all-or-nothing `fail_all` does not affect WatchNotify.
  However, if a future service uses server→client `call()`
  (request-response) with multiple client connections, this will
  cause incorrect failure propagation. **Action**: either add
  per-connection scoping to `fail_all`, or document that
  server→client `call()` is limited to single-connection use until
  fixed.
