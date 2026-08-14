<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R90: chunkdb — Client Library

**Problem**:

- **Current behavior + impact** — Higher-level services (a future object
  store, chunkio, the crow-tree storage engine, console/CLI tools) need
  a client library to interact with chunkdb without dealing with gRPC
  details, endpoint discovery, or retry logic directly. R85 lands only
  a `ChunkdbClient` skeleton with method stubs and no retry, no
  endpoint discovery, no connection pooling. Without a real client
  library, every caller would have to reimplement endpoint discovery
  (service registry scan), channel pooling, and transient-error retry
  — duplicating the `DiskdbClient` pattern (R74) across multiple
  consumers. This is the reusable API surface for chunkdb; R91 E2E
  tests and every downstream consumer depends on it.
- **Design pointers** —
  [`doc/design/chunkdb/design-crow-chunkdb.md`](../design/chunkdb/design-crow-chunkdb.md)
  §11 (crate layout — `lib/crow-chunkdb-client/` with `client.rs` +
  `types.rs`), §3.8 (proto types used directly; no Rust type
  duplication),
  [`doc/design/kv/design-crow-kv-group0.md`](../design/kv/design-crow-kv-group0.md)
  §2.1 (`crow-kv-client` is the single sysdata API surface —
  `ServiceRegistryClient` for endpoint discovery), §4.1 (service
  registry registration + keep-alive — chunkdb instances register like
  diskdb instances),
  `lib/crow-diskdb-client/src/client.rs` (R74 — the pattern to follow:
  `DashMap` endpoint cache + channel pool, `RetryConfig`, refresh on
  cache miss, retry on transient errors). aioss analog: aioss
  `chunkdb-client` (`libs/chunkdb-client/src/client.rs`) — wraps the
  gRPC stubs with typed helpers; CROW follows the same structure with
  CROW's `ServiceRegistryClient` + `NotLeaderHint` retry (design §11 —
  direct port of the client structure, adapted to CROW's service
  registry).
- **Use scenarios** —
  - **Client discovers chunkdb endpoints** — a consumer constructs
    `ChunkdbClient::new(svc_registry_client)`; the client lazily
    discovers chunkdb server endpoints from the service registry on
    first use; subsequent calls reuse the cached endpoint + channel.
  - **Client calls allocate and gets a chunk** — a consumer calls
    `client.allocate_chunk(AllocateChunkRequest { ... })`; the client
    routes to a chunkdb instance, the server (R89) allocates + persists
    + returns the `Chunk`; the client returns it to the caller.
  - **Client retries on transient error** — a chunkdb instance is
    briefly unreachable (network blip); the client retries the call
    with exponential backoff (up to `max_retries`); the second attempt
    succeeds; the caller sees no error.
  - **Client refreshes endpoints on cache miss** — a new chunkdb
    instance is added to the service registry; the client's endpoint
    cache does not have it; on cache miss the client refreshes from
    the service registry and retries; the call reaches the new
    instance.
  - **Client handles `NotLeaderHint`** — a chunkdb RPC returns a
    `NotLeaderHint` (the chunk's KV group leader moved); the client
    refreshes its endpoint cache and retries against the hinted leader;
    the call succeeds. (Follows the `crow-kv-client` pattern.)
  - **Client seals + deletes a chunk** — a consumer calls
    `client.seal_chunk(id, seal_length)` then
    `client.delete_chunk(id)`; both succeed; the chunk is sealed then
    deleted; disk blocks are freed.
  - **Client lists chunks with pagination** — a consumer calls
    `client.list_chunks(start_token, max_keys=100)` in a loop until
    `next_token` is empty; all chunks in the KV group are enumerated.

**Solution**:

**One-line summary**: implement `ChunkdbClient` with endpoint discovery
(service registry scan + `DashMap` cache), channel pooling, `RetryConfig`
(exponential backoff on transient errors + `NotLeaderHint`), and typed
methods for all 8 `ChunkdbService` RPCs — following the `DiskdbClient`
(R74) pattern.

1. **ChunkdbClient core** —
   `lib/crow-chunkdb-client/src/client.rs` (replace R85 skeleton):
   - `ChunkdbClient` struct: `ServiceRegistryClient` for endpoint
     discovery, `DashMap<ChunkGroupId, String>` endpoint cache
     (chunkdb instances own KV-group routing — or a single endpoint
     if chunkdb is fronted by a load balancer; see Open Questions),
     `DashMap<String, Channel>` channel pool, `RetryConfig`.
   - `refresh_endpoints()` — scan the service registry for chunkdb
     instances; populate the endpoint cache. Follows
     `DiskdbClient::refresh_endpoints` (R74).
   - `endpoint_for()` — cache miss → refresh → retry; `channel_for()`
     — lazy `Channel::connect_lazy` with timeout; `client_for()` —
     `ChunkdbServiceClient::new(channel)`.
   - Construction: `ChunkdbClient::new(svc)`,
     `with_retry_config(retry)`.

2. **Typed RPC methods** —
   `lib/crow-chunkdb-client/src/client.rs`:
   - `allocate_chunk(req) -> Chunk` — wraps `AllocateChunk` RPC.
   - `append_chunk(req) -> Chunk` — wraps `AppendChunk` RPC.
   - `seal_chunk(chunk_id, seal_length) -> Chunk` — wraps `SealChunk`.
   - `delete_chunk(chunk_id) -> Chunk` — wraps `DeleteChunk`.
   - `delete_chunk_range(chunk_id, offset, size) -> ()` — wraps
     `DeleteChunkRange`.
   - `update_chunk_strip(chunk_id, strip_index, strip) -> Chunk` —
     wraps `UpdateChunkStrip`.
   - `query_chunk(chunk_id) -> Chunk` — wraps `QueryChunk`.
   - `list_chunks(start_token, partition, max_keys) -> (Vec<Chunk>,
     next_token)` — wraps `ListChunks`; returns the page + cursor.
   - All methods use proto types directly (design §3.8 — no Rust type
     duplication); `Chunk`, `ChunkStrip`, `ChunkId` from
     `crow-protocol`.

3. **Retry + error handling** —
   `lib/crow-chunkdb-client/src/error.rs` (new):
   - `ChunkdbClientError` enum: `Unreachable`, `Unavailable`
     (transient — retry), `NotFound`, `AlreadyExists`,
     `FailedPrecondition` (invalid state), `Aborted` (state conflict),
     `Internal`, `DeadlineExceeded` (transient — retry).
   - Retry policy: retry on `Unavailable` + `DeadlineExceeded` +
     `NotLeaderHint` (refresh endpoints, retry against hint);
     exponential backoff (`initial_backoff` × 2^attempt, capped);
     up to `max_retries` (default 3, matching `DiskdbClient`).
   - Map gRPC status codes to `ChunkdbClientError`; surface non-
     transient errors immediately (no retry).

**Flow diagram**:

```
  caller ──► ChunkdbClient::allocate_chunk(req)
       │
       ▼
  endpoint_for(group) ──► DashMap cache hit? ──► channel_for(endpoint)
       │  cache miss
       ▼
  refresh_endpoints() ──► ServiceRegistryClient scan ──► populate cache
       │
       ▼
  ChunkdbServiceClient::allocate_chunk(req)
       │
       ├── success ──► return Chunk
       │
       ├── Unavailable / DeadlineExceeded ──► retry (backoff)
       │
       └── NotLeaderHint ──► refresh + retry against hint
```

- **Edge cases at a glance**:
  - No chunkdb instances registered → `refresh_endpoints` returns
    empty; `endpoint_for` returns `Unreachable`; the caller gets an
    error (no silent failure).
  - All chunkdb instances unreachable after `max_retries` → return
    `Unavailable`; the caller sees the last error.
  - `NotLeaderHint` with an invalid hint (hint points to a non-
    registered instance) → refresh endpoints; if the hint is still
    not in the registry, retry against any registered instance; if
    that also fails, return `Unavailable`.
  - Channel pool entry stale (endpoint changed after a chunkdb
    restart) → the gRPC call fails with `Unavailable`; retry refreshes
    the endpoint cache; the new channel is created.
  - `list_chunks` with `max_keys=0` → server returns empty page (R89);
    the client returns `(vec![], start_token)`; no error.
  - `query_chunk` on a non-existent ID → server returns `NotFound`;
    the client returns `ChunkdbClientError::NotFound` (no retry — not
    transient).
  - `delete_chunk` on an already-deleted chunk → server returns the
    `Deleted` chunk (idempotent, per R89 Open Question); the client
    returns it as success.
  - Concurrent `refresh_endpoints` calls (multiple threads miss the
    cache simultaneously) → `DashMap` serializes the insert; the
    refresh may run more than once but the cache ends up consistent
    (idempotent populate).

**Dependencies**:

- **R85** (foundation) — `lib/crow-chunkdb-client` crate skeleton +
  `crow-protocol` chunkdb types must exist.
- **R89** (lifecycle) — the server-side RPC handlers must be
  implemented for the client to call (the client is tested against a
  real server in R91).
- **`ServiceRegistryClient`** in `crow-kv-client` — endpoint discovery
  API; must be landed (it is — used by `DiskdbClient`).
- **`DiskdbClient`** (R74) — the pattern to follow; R90 mirrors its
  structure (endpoint cache, channel pool, retry config).
- **R91** (E2E) depends on R90 — E2E tests use `ChunkdbClient` to
  drive the server.
- **R99** (dynamic range binding) — R90 v1 routes to any registered
  instance; R99 upgrades the routing to hash-range-based instance
  selection with `NotMyRange` reject-and-retry. R90 is not blocked by
  R99 (works without sharding); R99 reuses R90's `ChunkdbClient`
  structure.

**Acceptance**:

**Construction + endpoint discovery**:
- `ChunkdbClient::new(svc)` constructs with default `RetryConfig`;
  `with_retry_config(custom)` overrides. Unit test.
- `refresh_endpoints()` with 2 chunkdb instances registered → endpoint
  cache has 2 entries; `endpoint_for(group)` returns the correct
  endpoint. Integration test (with service registry).
- `endpoint_for(group)` on a cache miss → triggers
  `refresh_endpoints`; retry hits the cache; no error to caller.
  Integration test.

**Typed RPC methods**:
- `allocate_chunk(req)` against a running chunkdb server (R89) →
  returns a `Chunk` with `state=Active`. Integration test.
- `seal_chunk(id, 1024)` → returns `Chunk` with `state=Sealed`,
  `sealed_length=1024`. Integration test.
- `delete_chunk(id)` → returns `Chunk` with `state=Deleted`.
  Integration test.
- `query_chunk(id)` → returns the `Chunk` (any state). Integration
  test.
- `list_chunks(start, 0, 100)` in a loop until `next_token` empty →
  enumerates all chunks in the KV group; total count matches the
  number of chunks created. Integration test.
- `update_chunk_strip(id, 0, strip)` → returns the updated `Chunk`.
  Integration test.
- `delete_chunk_range(id, offset, size)` → returns `()` (success).
  Integration test.
- `append_chunk(req)` on an `Active` chunk → returns `Chunk` with the
  new strips. Integration test.

**Retry + error handling**:
- `allocate_chunk` where the first attempt returns `Unavailable`
  (chunkdb briefly down) → client retries with backoff; second
  attempt succeeds; caller sees the `Chunk` (no error). Integration
  test (inject a transient failure).
- `allocate_chunk` where all `max_retries` attempts return
  `Unavailable` → client returns `ChunkdbClientError::Unavailable`.
  Unit test (mock the channel).
- `query_chunk` on a non-existent ID → client returns
  `ChunkdbClientError::NotFound` immediately (no retry — not
  transient). Unit test.
- `NotLeaderHint` returned → client refreshes endpoints + retries
  against the hint; succeeds. Integration test.

**Edge cases**:
- No chunkdb instances registered → `refresh_endpoints` returns
  empty; `allocate_chunk` returns `Unreachable`. Unit test.
- `list_chunks(max_keys=0)` → returns `(vec![], start_token)`. Unit
  test.
- Concurrent `refresh_endpoints` (10 threads, cache miss) → cache
  ends up consistent (no panic, no torn entries). Unit test.

**Lint + test commands**:
- `pixi run cargo fmt --all -- --check` passes.
- `pixi run cargo clippy --all-targets -- -D warnings` passes.
- `pixi run test-chunkdb` (client unit tests + integration tests
  against a running chunkdb server pass).

**Open Questions**:

- **Endpoint routing — resolved by R99.** chunkdb is stateless
  (design §3.6) but scales out via hash-range sharding (R99 —
  Dynamic Range Binding Framework). R90 v1 (without R99) routes to
  any registered chunkdb instance (round-robin or random) —
  simplest, leverages statelessness, works when only one instance
  exists. When R99 lands, the client switches to
  `RangeBindingClient::route(chunk_id)` → specific instance that
  owns the chunk's hash range, with `NotMyRange` reject-and-retry
  (following the `NotLeaderHint` pattern). R90's `ChunkdbClient`
  structure (endpoint cache, channel pool, retry config) is reused
  by R99 — only the routing decision changes from "any instance"
  to "the instance that owns the range". See R99 for the sharding
  design + open questions on the common framework.
