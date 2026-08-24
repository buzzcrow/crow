<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Flatbuffer RPC Migration — Todo + Notes

Tracking doc for the gRPC → crow-rpc (flatbuffer) migration across
all CROW services. Backlog items: R114 (done), R115 (done), R116
(chunkdb — done), R117 (KvService client-facing — done), R32 (KV
consensus — done). The migration is **complete**: no gRPC/tonic/prost
dependency remains, no `.proto` files exist, and `protoc` has been
removed from the pixi env.

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
R116 (ChunkdbService — after chunk-layer refactor stabilizes) — DONE
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
- R116 done — reuses R115's unary-only migration pattern (diskdb) +
  R117's zero-copy `Ref` wrapper pattern + `with_rpc_transport`
  programmatic selection. All 8 ChunkdbService RPCs are unary (no
  streaming, R114 not needed). Port: `CHUNKDB_RPC_BASE = 9961`
  (fills gap between diskdb RPC 9931-9940 and the legacy chunkdb
  gRPC port 9971-9990, now removed). `NotMyRangeHint` is
  diagnostic-only (no leader endpoint — client refreshes from
  group-0 + re-routes). The chunk-layer refactor (R113) is NOT
  done but strip prefetch is already inside `ChunkWriter`, RPC
  call sites are consolidated and stable. The allocator pool
  (`pool.rs`) calls diskdb (not chunkdb) — that path is R115's
  scope, already done, NOT changed by R116.

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

R115 (diskdb), R32 (KV consensus), R117 (KV client-facing), R116
(chunkdb) — ALL DONE. Checklist status per item:

- [x] `.fbs` schema created in `lib/crow-protocol/src/fbs/`
      (R115: diskdb.fbs, R32: kv_consensus.fbs, R117: kv_client.fbs,
      R116: chunkdb.fbs)
- [x] `msg_type.fbs` extended with the service's range
      (R115: 3000s, R32: 1000s, R117: 1100s, R116: 3300s)
- [x] `build.rs` + `lib.rs` re-exports updated
- [x] Zero-copy wrappers in `lib/crow-protocol/src/fb_wrappers/`
      (R32: kv_consensus.rs, R117: kv_client.rs, R115: diskdb.rs,
      R116: chunkdb.rs — R115 retrofitted, see Open Issues)
- [x] Server handler rewritten (dispatch by `msg_type`)
      (R115: diskdb, R32: px_rpc_service, R117: kv_rpc_service,
      R116: chunkdb_rpc_service)
- [x] Client rewritten (`RpcClient` + `ConnectionPool`)
      (R115: DiskdbRpcTransport, R117: KvRpcTransport — R32 uses
      PxRpcTransport, R116: ChunkdbRpcTransport)
- [x] Error mapping (`RpcError` → service error variants)
- [x] `grpc_endpoint` → `rpc_endpoint` (all call sites — proto field,
      keepalive struct, FFI param, transport/service param names)
- [x] Mixed-rollout cutover: crow-rpc is the only transport; the
      `with_rpc_transport()` switch has been replaced with a required
      `rpc_transport` parameter at construction. No service runs a
      gRPC server anymore.
- [x] Cutover: gRPC server + tonic traits + prost types removed from
      all crates; `.proto` files deleted; `protobuf`/`protoc` removed
      from pixi env. `Cargo.lock` has no tonic/prost/protobuf/grpcio.
- [x] Tests pass: `cargo test -p <service>`, `cargo fmt --check`,
      `cargo clippy -- -D warnings`

## Open Issues (deferred to follow-up items)

- **R115 mixed-rollout cutover** → **done for all services**: gRPC
  and prost types have been removed from every crate
  (`crow-protocol`, `crow-diskdb`, `crow-diskdb-client`,
  `crow-chunkdb`, `crow-chunkdb-client`, `crow-web`, `crow-kv`,
  `crow-kv-client`). The tonic server traits and gRPC client paths
  are gone; crow-rpc is the only transport. The
  `with_rpc_transport()` switch has been replaced with a required
  `rpc_transport` parameter at construction. `Cargo.lock` carries no
  tonic/prost/protobuf/grpcio packages; no `.proto` files remain in
  the repo; `protobuf`/`protoc` has been removed from the pixi env
  (only `flatc`, from the `flatbuffers` package, is needed for
  codegen). The last cosmetic legacy — the `grpc_endpoint`
  parameter name in the transport/service call sites — has been
  renamed to `rpc_endpoint`.

- **R114 `fail_all` is all-or-nothing** → **resolved**: Added
  per-connection scoping to `fail_all` in the C++ `RpcClient`. Each
  pending entry now carries a `Connection*` pointer; `fail_all` only
  fails entries matching the specified connection.
