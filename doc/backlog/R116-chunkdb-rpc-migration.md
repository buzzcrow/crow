<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R116: chunkdb — ChunkdbService gRPC → crow-rpc Migration

**Problem**

ChunkdbService runs on tonic/gRPC (`app/crow-chunkdb/src/service.rs`,
`app/crow-chunkdb/src/main.rs`). The client surface is split:
`crow-chunkdb-client` calls ChunkdbService RPCs (`AllocateChunk`,
`AppendChunk`, `QueryChunk`, `SealChunk`, `DeleteChunk`,
`DeleteChunkRange`, `UpdateChunkStrip`, `ListChunks`) during the chunk
write/read path, and `crow-chunkdb`'s own allocator pool
(`app/crow-chunkdb/src/allocator/pool.rs`) uses tonic `Channel` to call
peer diskdb instances (not chunkdb — that path is R115's scope, already
done). gRPC's h2 connection-level lock serializes concurrent writers —
the same design mismatch as R32, R115, and R117. The chunk write path
is high-throughput: a 1 TB object generates ~250K `AppendChunk` calls
(R113), and concurrent writers from multiple chunk-client instances
share connections.

**Current behavior + impact**: All 8 ChunkdbService RPCs go through
tonic/gRPC. The `chunkdb_service.proto`, `chunkdb_op.proto`, and
`chunkdb_type.proto` schemas define the wire format. The
`NotMyRangeHint` error (prost-encoded in a `Status` with
`FailedPrecondition` code, `app/crow-chunkdb/src/service.rs` L38-57)
must be preserved in the flatbuffer response — but note that
`NotMyRangeHint` is purely diagnostic: `instance_id`, `rpc_endpoint`,
and `sub_range_index` are unused (the server does not know the owner).
The client refreshes its binding cache from group-0 and re-routes via
`RangeBindingClient::refresh_and_route`. This is different from KV's
`NotLeaderHint` (which carries the leader endpoint) — R116 carries
only the diagnostic bucket range + a `ret_code` indicating
`NotMyRange`; the client's re-route logic is unchanged.

**Design pointers**: `design-crow-rpc.md` §6 (Flatbuffer Wrapper
Convention — zero-copy rule), §5 (Schema + Build), §4.4 (Server Side).
`design-crow-kv-rpc-client.md` (R117 — client-facing KV RPC, the
closest reference: same `with_rpc_transport` programmatic selection,
same zero-copy `Ref` wrapper pattern, same `forwarded` loop-guard
concept). `design-crow-chunkdb.md` (chunkdb architecture, chunk
lifecycle, strip types, RPC surface). R115 (diskdb migration —
reference for the unary-only migration pattern + port offset
convention). R113 (batch strip allocation — NOT done, but the
chunk-layer refactor is partially stable: strip prefetch is already
inside `ChunkWriter`, RPC call sites are consolidated).

**Use scenarios**:

- **Concurrent chunk allocation**: Multiple chunk-client writers
  allocate chunks simultaneously. `AllocateChunk` calls from
  different writers on a shared connection funnel through the h2
  lock under gRPC. Under crow-rpc, each call is an independent
  framed message. Expected: throughput scales with
  thread:connection ratio.

- **AppendChunk during large object write**: A chunk-client writer
  appends strips to a chunk via `AppendChunk`. Under crow-rpc, each
  append is a framed message — no h2 stream management overhead.
  Expected: identical semantics, lower per-message overhead.

- **UpdateChunkStrip during mirror→EC conversion (R93)**: The
  background conversion task updates strip layouts via
  `UpdateChunkStrip`. Under crow-rpc, the update is a unary
  request-response. Expected: no contract change.

- **NotMyRangeHint redirect**: A chunkdb instance receives a request
  for a chunk outside its owned hash range, returns
  `NotMyRangeHint`. Under crow-rpc, the hint is a flatbuffer
  response with `ret_code = NotMyRange` + diagnostic
  `range_start`/`range_end` fields (no leader endpoint — the server
  does not know the owner). The client's `RangeBindingClient` retry
  logic is unchanged: refresh binding cache from group-0, re-route.
  Expected: no contract change.

- **Mixed rollout**: A chunkdb instance runs both gRPC and crow-rpc
  servers during migration. Clients switch via
  `with_rpc_transport()`. After all clients migrated, gRPC server
  removed. Expected: no downtime.

**Solution**

Migrate ChunkdbService from tonic/gRPC to the R104 `crow-rpc`
library. All 8 RPCs are unary (no streaming) — R114 is not needed.
The `.proto` schemas are converted to `.fbs` (full conversion,
consistent with R105/diskio, R115/diskdb, R32/KV-consensus, and
R117/KV-client). Zero-copy wrapper classes per `design-crow-rpc.md`
§6 — R117 established the proper zero-copy `Ref` wrapper pattern
(R115 deferred this, R117 implemented it correctly; R116 follows
R117's pattern from the start). The `NotMyRangeHint` error model is
preserved as a flatbuffer response with a `NotMyRange` `ret_code` +
diagnostic `range_start`/`range_end` fields (not a tonic `Status`
with prost-encoded details). The client selects transport via
`with_rpc_transport()` (programmatic, same as R115/R117 — no config
flag).

**One-line summary**: Replace gRPC on the ChunkdbService path with
crow-rpc, converting all 8 unary RPCs to flatbuffer-over-TCP,
preserving protocol semantics including `NotMyRangeHint`.

**Numbered work items**:

1. **Port allocation** (`lib/crow-protocol/src/ports.rs`) — add
   `CHUNKDB_RPC_BASE = 9961` (fills the gap between diskdb RPC
   9931-9940 and chunkdb gRPC 9971-9990). Stride 1 (one port per
   instance, matching diskdb RPC). Add `ChunkdbRpc` variant to
   `ServicePort` enum. The RPC port offset is
   `CHUNKDB_RPC_BASE - CHUNKDB_GRPC_BASE = 9961 - 9971 = -10`
   (subtraction, matching R115's pattern). Files:
   `lib/crow-protocol/src/ports.rs`.

2. **Flatbuffer schemas for ChunkdbService** (`lib/crow-protocol/
   src/fbs/chunkdb.fbs`) — convert `chunkdb_service.proto`,
   `chunkdb_op.proto`, `chunkdb_type.proto` to `.fbs` schemas.
   Message types: AllocateChunk, AppendChunk, QueryChunk, SealChunk,
   DeleteChunk, DeleteChunkRange, UpdateChunkStrip, ListChunks —
   each a request + response table. Enums: `FBChunkState`,
   `FBStripType`, `FBChunkType`, `FBEcState`, `FBChunkdbRetCode`
   (Success, NotMyRange, ChunkSealed, ChunkNotFound, Internal,
   InvalidArgument). The `NotMyRangeHint` is not a separate message
   — it is `ret_code = NotMyRange` + `range_start`/`range_end`
   diagnostic fields on the response table. Nested types:
   `FBMirrorStrip`, `FBEcStrip`, `FBChunkStrip` (with
   `mirror_strip`/`ec_strip` union), `FBChunk`, `FBChunkId`,
   `FBSegment` (from diskdb schema — reuse or re-define). The
   `usage_bitmap` field (`bytes` in proto) becomes `[ubyte]` in fbs.
   Register message type IDs in the 3300s range in `msg_type.fbs`
   (3300-3315: 8 request + 8 response). Follow the `FB` prefix
   convention. Files: `lib/crow-protocol/src/fbs/chunkdb.fbs`
   (new), `lib/crow-protocol/src/fbs/msg_type.fbs`,
   `lib/crow-protocol/build.rs`, `lib/crow-protocol/src/lib.rs`.

3. **Zero-copy wrapper classes** (`lib/crow-protocol/src/
   fb_wrappers/chunkdb.rs`) — define `FB<Type>Ref` wrappers for
   chunkdb response types per §6, following R117's pattern
   (`FBKvResponseRef` etc.): `new(buf)`, `valid()`, `ret_code()`,
   `error_msg()`, `ok()`, plus per-response accessors. The
   `NotMyRange` diagnostic is accessed via `ret_code()` +
   `range_start()`/`range_end()` on the response wrapper. Files:
   `lib/crow-protocol/src/fb_wrappers/chunkdb.rs` (new),
   `lib/crow-protocol/src/fb_wrappers.rs` (module registration).

4. **Server-side migration** (`app/crow-chunkdb/src/rpc/`)
   — create `ChunkdbRpcService` mirroring R117's `KvRpcService`:
   holds `Arc<LifecycleHandler>` + `Handle` (tokio runtime).
   `register_handlers` wires one handler per request `msg_type`
   into the crow-rpc `RpcServer`. Each handler: parse request via
   `flatbuffers::root::<FB<Type>Request>`, dispatch to the existing
   `LifecycleHandler` methods (same logic as the tonic handler),
   build flatbuffer response, `submit_response`. The
   `NotMyRange` response is a flatbuffer with `ret_code = NotMyRange`
   + diagnostic fields (not a tonic `Status`). The crow-rpc server
   runs alongside the tonic server during mixed rollout. No
   transparent forwarding (chunkdb has no leader — range routing is
   client-side via `RangeBindingClient`). Files:
   `app/crow-chunkdb/src/rpc/mod.rs` (new),
   `app/crow-chunkdb/src/rpc/chunkdb_rpc_service.rs` (new),
   `app/crow-chunkdb/src/main.rs` (add `start_chunkdb_rpc_server`).

5. **Client-side transport** (`lib/crow-chunkdb-client/src/
   rpc_transport.rs`) — create `ChunkdbRpcTransport` mirroring
   R115's `DiskdbRpcTransport` + R117's `KvRpcTransport`: holds
   `Arc<RpcServer>` + `Arc<RpcClient>` + `DashMap<String,
   Connection>` + `AtomicU64`. `conn_for` derives the chunkdb RPC
   port from the gRPC port via the offset (`grpc_port - 10`).
   Unary methods (`send_allocate_chunk`, `send_append_chunk`, etc.):
   build request flatbuffer → `rpc.call` → await → parse via `Ref`
   wrapper → map to existing proto response types. `NotMyRange` is
   detected via `ret_code` and surfaced as
   `ChunkdbClientError::NotMyRange` (the client's
   `RangeBindingClient` retry logic is unchanged). Files:
   `lib/crow-chunkdb-client/src/rpc_transport.rs` (new),
   `lib/crow-chunkdb-client/src/lib.rs` (module + re-export).

6. **ChunkdbClient transport selection**
   (`lib/crow-chunkdb-client/src/client.rs`) — add
   `rpc_transport: Option<Arc<ChunkdbRpcTransport>>` field +
   `with_rpc_transport()` builder (same pattern as R115's
   `DiskdbClient::with_rpc_transport` and R117's
   `CrowkvClient::with_rpc_transport`). Each public method
   (`allocate_chunk`, `append_chunk`, `query_chunk`, `seal_chunk`,
   `delete_chunk`, `delete_chunk_range`, `update_chunk_strip`,
   `list_chunks`) checks `self.rpc_transport` first: when set,
   delegate to the transport's `send_*` (with the existing retry +
   `RangeBindingClient` re-route logic — only the wire send
   changes); when not set, the existing tonic path. Files:
   `lib/crow-chunkdb-client/src/client.rs`.

7. **Error model parity** (`lib/crow-chunkdb-client/src/lib.rs`)
   — map crow-rpc `RpcError` variants to chunk-client error
   variants. `ConnectionClosed` → retry on next connection.
   `Timeout` → `ChunkdbClientError::Timeout`. `SendQueueFull` →
   retry with backoff. `NotMyRange` is a protocol-level response
   (carried in `ret_code`), not a transport error — the client's
   `RangeBindingClient::refresh_and_route` logic is unchanged.
   `ChunkSealed` → `ChunkdbClientError::ChunkSealed` (client
   allocates a new chunk). Files:
   `lib/crow-chunkdb-client/src/lib.rs`.

8. **Mixed rollout + cutover** — same pattern as R115/R117: both
   servers run simultaneously, clients switch via
   `with_rpc_transport()`, gRPC server removed in a follow-up
   commit. `chunkdb_service.proto` stays as legacy/reserved. The
   allocator pool (`app/crow-chunkdb/src/allocator/pool.rs`) calls
   diskdb (not chunkdb) — that path is R115's scope (already done)
   and is NOT changed by R116. Files:
   `app/crow-chunkdb/src/main.rs`.

**Flow diagram**:

```
                    Before (gRPC)                          After (crow-rpc)
                    ─────────────                          ────────────────

chunk-client ─┐                            chunk-client ─┐
  writer A    ─┼─► tonic Client ──►         writer A    ─┼─► RpcClient ──► MPSC queue
  writer B    ─┤    (h2 lock)               writer B    ─┤    (no lock)       │
              ┘                            writer C    ─┘                     │
                                                                   Writer task
                                                                   writev() ──► TCP
                                                                         │
                                                                         ▼
                                                                  Server reader
                                                                  dispatch by msg_type
                                                                  ChunkdbRpcService
                                                                  handler → LifecycleHandler
```

**Edge cases at a glance**:

- `NotMyRange` with a stale binding cache → client's
  `RangeBindingClient` refreshes from group-0, re-routes. Same
  semantics as gRPC.
- Connection to a removed chunkdb instance → crow-rpc reconnect
  fails; endpoint removed from client cache via membership change
  callback.
- Mixed gRPC + crow-rpc during rollout → both servers run; clients
  switch via `with_rpc_transport()`.
- `AppendChunk` on a sealed chunk → `ret_code = ChunkSealed`;
  client allocates a new chunk. Same semantics.
- `UpdateChunkStrip` during mirror→EC conversion (R93) → the
  conversion task's retry logic is unchanged; only the transport
  differs.
- Allocator pool (`pool.rs`) calls diskdb, not chunkdb → NOT
  changed by R116 (R115 already migrated diskdb).

**Dependencies**

- **Depends on**: R104 (crow-rpc — finished). R114 (streaming) not
  needed — all 8 RPCs are unary. R115 (diskdb migration — finished,
  validates the unary migration pattern + port offset convention).
  R117 (KV client-facing migration — finished, validates the
  zero-copy `Ref` wrapper pattern + `with_rpc_transport` programmatic
  selection). The chunk-layer refactor (R113 — NOT done, but strip
  prefetch is already inside `ChunkWriter`, RPC call sites are
  consolidated and stable enough for R116 to proceed).
- **Depended on by**: nothing (terminal migration item).

**Acceptance**

**Transport parity**:
- `AllocateChunk` over crow-rpc produces the same chunk allocation
  as over gRPC. Integration test.
- `AppendChunk` over crow-rpc produces the same strip append as
  over gRPC. Integration test.
- `QueryChunk` / `SealChunk` / `DeleteChunk` / `DeleteChunkRange`
  / `ListChunks` over crow-rpc produce the same result as over
  gRPC. Integration test.
- `UpdateChunkStrip` over crow-rpc produces the same strip update
  as over gRPC. Integration test.
- `NotMyRange` response over crow-rpc is parsed correctly by the
  client → client refreshes binding cache + re-routes. Integration
  test.
- `ChunkSealed` response over crow-rpc → client allocates a new
  chunk. Integration test.

**Error model**:
- crow-rpc `ConnectionClosed` → client retries on next connection.
  Integration test (kill chunkdb mid-call).
- crow-rpc `Timeout` → client returns `ChunkdbClientError::Timeout`.
  Integration test.
- `NotMyRange` in flatbuffer `ret_code` → client's
  `RangeBindingClient` refreshes + retries. Integration test.

**Mixed rollout**:
- A chunkdb instance running both gRPC and crow-rpc: gRPC client
  connects to gRPC port, crow-rpc client to crow-rpc port, both
  succeed. Integration test.

**Zero-copy wrapper**:
- The chunkdb server handler parses requests via `FB<Type>Ref`
  wrappers (no owned intermediate, no field copy). Verified by
  code review.

**Port allocation**:
- `CHUNKDB_RPC_BASE = 9961` registered in `ports.rs`, `ChunkdbRpc`
  variant in `ServicePort` enum, stride 1. Port ranges do not
  overlap with existing services. Unit test (`ports_test.rs`).

**Test commands**: `pixi run cargo test -p crow-chunk-client`,
`pixi run cargo test -p crow-chunkdb`,
`pixi run cargo test -p crow-protocol`,
`pixi run cargo fmt --all -- --check`,
`pixi run cargo clippy --all-targets -- -D warnings`.
