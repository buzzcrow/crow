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
  The data payload (raw bytes after the control message) is the one
  exception: it may be copied into an owned `Vec<u8>` when the
  caller needs owned bytes, because it is not a flatbuffer.
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

### 2. `grpc_endpoint` → `rpc_endpoint` as a standalone commit

R115 work item 6 does this for diskdb, but the rename touches
`crow-kv-client/src/service_registry.rs` (shared by all services).
Doing it inside R115 means R116/R117 conflict on the same files.

Better: a **standalone commit before R115** that renames all Rust
struct fields + method parameters from `grpc_endpoint` to
`rpc_endpoint`. The group-0 sysdata **wire field name stays
`grpc_endpoint`** (backward compat — old nodes reading new registry
entries). Only Rust source identifiers change. The keepalive struct
`with_grpc_endpoint` → `with_rpc_endpoint`. FFI parameter
`grpc_endpoint` → `rpc_endpoint` in `crow-kv-client/src/ffi.rs` +
`c_api.h` (no ABI change — it's a `*const c_char` either way).

Files touched:
- `lib/crow-kv-client/src/service_registry.rs`
- `lib/crow-kv-client/src/ffi.rs`
- `lib/crow-kv-client/include/crow-kv-client/c_api.h`
- `app/crow-diskdb/src/liveness/keepalive.rs`
- `app/crow-diskdb/src/main.rs`
- `app/crow-chunkdb/src/main.rs`
- `app/crow-kv-server/src/keepalive.rs`
- `app/crow-diskio/src/group0/group0_sync.cpp` (C++ — `grpc_endpoint`
  field in `g0_cfg` struct + the `crow_svc_heartbeat_diskio` call)
- `app/crow-diskio/src/dio_main.cpp`
- Test harnesses (`lib/crow-test-harness/src/*.rs`)

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
