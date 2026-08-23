<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R116: chunkdb — ChunkdbService gRPC → crow-rpc Migration

**Problem**

ChunkdbService runs on tonic/gRPC (`crow-chunkdb/src/service.rs`,
`crow-chunkdb/src/main.rs` L164). The client surface is split:
chunkdb's own allocator pool (`crow-chunkdb/src/allocator/pool.rs`)
uses tonic `Channel` to call peer diskdb instances, and
`crow-chunk-client` calls ChunkdbService RPCs (`AllocateChunk`,
`AppendChunk`, `QueryChunk`, `UpdateChunkStrip`) during the chunk
write path. gRPC's h2 connection-level lock serializes concurrent
writers — the same design mismatch as R32 and R115. The chunk write
path is high-throughput: a 1 TB object generates ~250K `AppendChunk`
calls (R113), and concurrent writers from multiple chunk-client
instances share connections.

**Current behavior + impact**: All 8 ChunkdbService RPCs go through
tonic/gRPC. The `chunkdb_service.proto`, `chunkdb_op.proto`, and
`chunkdb_type.proto` schemas define the wire format. The
`NotLeaderHint` error (prost-encoded in a `Status` with
`FailedPrecondition` code, `crow-chunkdb/src/service.rs` L48-53) must
be preserved in the flatbuffer response. The chunkdb server is the
newest service and has the least production exposure — migrating it
last gives R115 (diskdb) and R32 (KV consensus) time to validate the
pattern first.

**Design pointers**: `design-crow-rpc.md` §6 (Flatbuffer Wrapper
Convention — zero-copy rule), §5 (Schema + Build), §4.4 (Server Side).
`design-crow-chunkdb.md` (chunkdb architecture, chunk lifecycle,
strip types, RPC surface). R115 (diskdb migration) is the reference
for the unary-only migration pattern. R113 (batch strip allocation)
is optimizing the `AppendChunk` call count — R116 does not change
the call count, only the transport.

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

- **NotLeaderHint redirect**: A chunkdb instance receives a request
  destined for the range owner, returns `NotLeaderHint`. Under
  crow-rpc, the hint is a flatbuffer response with a `ret_code`
  indicating redirect + the leader endpoint. The client's
  `RangeBindingClient` retry logic is unchanged. Expected: no
  contract change.

- **Mixed rollout**: A chunkdb instance runs both gRPC and crow-rpc
  servers during migration. Clients switch via config flag. After
  all clients migrated, gRPC server removed. Expected: no downtime.

**Solution**

Migrate ChunkdbService from tonic/gRPC to the R104 `crow-rpc`
library. All 8 RPCs are unary (no streaming) — R114 is not needed.
The `.proto` schemas are converted to `.fbs` (full conversion,
consistent with R105/diskio and R115/diskdb). Zero-copy wrapper
classes per `design-crow-rpc.md` §6. The `NotLeaderHint` error model
is preserved as a flatbuffer response with a redirect `ret_code` +
leader endpoint field.

**One-line summary**: Replace gRPC on the ChunkdbService path with
crow-rpc, converting all 8 unary RPCs to flatbuffer-over-TCP,
preserving protocol semantics including `NotLeaderHint`.

**Numbered work items**:

1. **Flatbuffer schemas for ChunkdbService** (`lib/crow-protocol/
   src/fbs/chunkdb.fbs`) — convert `chunkdb_service.proto`,
   `chunkdb_op.proto`, `chunkdb_type.proto` to `.fbs` schemas.
   Message types: AllocateChunk, AppendChunk, QueryChunk, SealChunk,
   DeleteChunk, DeleteChunkRange, UpdateChunkStrip, ListChunks —
   each a request + response table. The `NotLeaderHint` payload
   (leader endpoint + membership epoch) is a field in the response
   table, not a separate error message. Register message type IDs
   in the 3300s range in `msg_type.fbs`. Follow the `FB` prefix
   convention. Files: `lib/crow-protocol/src/fbs/chunkdb.fbs`
   (new), `lib/crow-protocol/src/fbs/msg_type.fbs`,
   `lib/crow-protocol/build.rs`, `lib/crow-protocol/src/lib.rs`.

2. **Zero-copy wrapper classes** (`lib/crow-protocol/src/
   fb_wrappers/`) — define `FB<Type>Ref` wrappers for chunkdb
   request/response types per §6. Include a `NotLeaderHint` accessor
   on the response wrapper (returns `Option<(endpoint, epoch)>`).
   Files: `lib/crow-protocol/src/fb_wrappers/chunkdb.rs` (new).

3. **Server-side migration** (`app/crow-chunkdb/src/service.rs`)
   — replace the tonic `ChunkdbService` server with a crow-rpc
   `RpcServer` handler set. Each handler dispatches by `msg_type`
   to the existing chunkdb logic (chunk lifecycle, strip
   management). The `NotLeaderHint` response is a flatbuffer with
   a redirect `ret_code` + leader endpoint (not a tonic `Status`
   with prost-encoded details). The crow-rpc server runs alongside
   the tonic server during mixed rollout. Files:
   `app/crow-chunkdb/src/service.rs` (rewrite),
   `app/crow-chunkdb/src/main.rs`.

4. **Client-side migration** (`lib/crow-chunk-client/src/`,
   `app/crow-chunkdb/src/allocator/pool.rs`) — replace tonic
   `Channel` usage with crow-rpc `RpcClient` + `ConnectionPool`.
   The chunk-client's `ChunkIoWriter` trait callers
   (`chunk_writer.rs`, `chunk_prefetch.rs`) switch from tonic
   client calls to crow-rpc calls. The `NotLeaderHint` is parsed
   from the flatbuffer response via the wrapper; the
   `RangeBindingClient` retry logic is unchanged. Files:
   `lib/crow-chunk-client/src/chunk/chunk_writer.rs`,
   `lib/crow-chunk-client/src/chunk/chunk_prefetch.rs`,
   `app/crow-chunkdb/src/allocator/pool.rs`.

5. **Error model parity** (`lib/crow-chunk-client/src/`,
   `lib/crow-diskdb-client/src/client.rs` reference) — map crow-rpc
   `RpcError` variants to chunk-client error variants.
   `ConnectionClosed` → retry on next connection. `Timeout` →
   `ChunkClientError::Timeout`. `SendQueueFull` → retry with
   backoff. `NotLeaderHint` is a protocol-level response (carried
   in `ret_code`), not a transport error. Files:
   `lib/crow-chunk-client/src/lib.rs`,
   `lib/crow-chunk-client/src/chunk/chunk_writer.rs`.

6. **`grpc_endpoint` → `rpc_endpoint` rename** — same mechanical
   rename as R115, applied to chunkdb's keepalive + service
   registry registration. Files:
   `app/crow-chunkdb/src/main.rs`,
   `lib/crow-kv-client/src/service_registry.rs` (if not already
   done by R115).

7. **Mixed rollout + cutover** — same pattern as R115: both servers
   run simultaneously, clients switch via config, gRPC server
   removed in a follow-up commit. `chunkdb_service.proto` stays as
   legacy/reserved. Files: `app/crow-chunkdb/src/main.rs`.

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
                                                                  ChunkdbService
                                                                  handler → chunk lifecycle
```

**Edge cases at a glance**:

- `NotLeaderHint` with a stale leader hint → client's
  `RangeBindingClient` refreshes topology cache, retries. Same
  semantics as gRPC.
- Connection to a removed chunkdb instance → crow-rpc reconnect
  fails; endpoint removed from client cache via membership change
  callback.
- Mixed gRPC + crow-rpc during rollout → both servers run; clients
  switch via config flag.
- `AppendChunk` on a sealed chunk → `ret_code` indicates
  `ChunkSealed`; client allocates a new chunk. Same semantics.
- `UpdateChunkStrip` during mirror→EC conversion (R93) → the
  conversion task's retry logic is unchanged; only the transport
  differs.

**Dependencies**

- **Depends on**: R104 (crow-rpc — finished). R114 (streaming) not
  needed — all 8 RPCs are unary. **R115 (diskdb migration)** should
  land first to validate the migration pattern. The chunk-layer
  refactor (`doc/working/design-chunk-layer-refactor.md`) should be
  far enough along that `ChunkWriter`'s RPC call sites are stable
  (the refactor changes which module makes the `AppendChunk` call).
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
- `NotLeaderHint` response over crow-rpc is parsed correctly by
  the client → client redirects to the hinted owner. Integration
  test.

**Error model**:
- crow-rpc `ConnectionClosed` → client retries on next connection.
  Integration test (kill chunkdb mid-call).
- crow-rpc `Timeout` → client returns `ChunkClientError::Timeout`.
  Integration test.
- `NotLeaderHint` in flatbuffer `ret_code` → client's
  `RangeBindingClient` refreshes + retries. Integration test.

**Mixed rollout**:
- A chunkdb instance running both gRPC and crow-rpc: gRPC client
  connects to gRPC port, crow-rpc client to crow-rpc port, both
  succeed. Integration test.

**Zero-copy wrapper**:
- The chunkdb server handler parses requests via `FB<Type>Ref`
  wrappers (no owned intermediate, no field copy). Verified by
  code review.

**Test commands**: `pixi run cargo test -p crow-chunk-client`,
`pixi run cargo test -p crow-chunkdb`, `pixi run cargo fmt --all
-- --check`, `pixi run cargo clippy --all-targets -- -D warnings`.
