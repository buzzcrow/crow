<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R115: diskdb — DiskdbService gRPC → crow-rpc Migration

**Problem**

DiskdbService runs on tonic/gRPC (`crow-diskdb/src/service/
diskdb_service.rs`, `crow-diskdb/src/main.rs` L279). The client
library (`crow-diskdb-client/src/client.rs`) uses tonic `Channel`
pools and `tonic::Status` error mapping. gRPC's h2 connection-level
lock serializes concurrent writers on the same connection — the same
design mismatch that R32 addresses for the KV consensus path. While
diskdb is not as latency-sensitive as the Paxos hot path, it is on
the chunk write data path (block allocation via `AllocateBlocks`),
where concurrent writers from multiple chunkdb instances share
connections.

Migrating DiskdbService first (before R32 and R117) serves as the
**proof-of-pattern** for the full `.fbs` conversion approach: it has
11 unary RPCs (no streaming), is independent of R114 (streaming
support), and exercises every migration step (schema, server, client,
error mapping, mixed rollout, cutover) that R32 and R117 will repeat.

**Current behavior + impact**: All 11 DiskdbService RPCs go through
tonic/gRPC. The `crow-diskdb-client` library manages a
`DashMap<String, tonic::transport::Channel>` pool keyed by
`grpc_endpoint`, with retry on `Unavailable`/`DeadlineExceeded`/
`Aborted`. The `grpc_endpoint` field name is stored in group-0
sysdata (service registry) and is a misnomer after migration — the
endpoint becomes a raw `host:port` (no `http://` prefix). The
`diskdb_service.proto` and `diskdb_op.proto` / `diskdb_type.proto`
schemas are the wire definition; they must be converted to `.fbs`.

**Design pointers**: `design-crow-rpc.md` §6 (Flatbuffer Wrapper
Convention — zero-copy rule), §5 (Schema + Build — single-home in
`crow-protocol`), §4.4 (Server Side — handler dispatch),
`design-crow-diskdb.md` (diskdb architecture, RPC surface). The
diskio migration (R105) is the reference implementation —
`diskio.fbs`, `dio_server.cpp`, `diskio-client/src/client.rs` show
the completed pattern for a unary-only service.

**Use scenarios**:

- **Concurrent block allocation**: Multiple chunkdb instances
  allocate blocks from the same diskdb instance over shared
  connections. Under gRPC, concurrent `AllocateBlocks` calls on one
  connection funnel through the h2 lock. Under crow-rpc, each call
  is a framed message pushed to the per-connection MPSC queue — no
  userspace lock. Expected: throughput scales with thread:connection
  ratio.

- **Block free + commit during chunk seal**: A chunkdb instance
  seals a chunk, calls `FreeBlocks` + `CommitBlocks` in quick
  succession. Under crow-rpc, both are independent framed messages
  on the same connection — no h2 stream management overhead.
  Expected: identical semantics, lower per-message overhead.

- **Capacity query during rebalance**: The console or a rebalance
  planner calls `QueryCapacityStats` / `GetDiskGroupInfo`. Under
  crow-rpc, the query is a unary request-response — same semantics,
  different transport. Expected: no contract change.

- **Mixed rollout**: A diskdb instance runs both gRPC and crow-rpc
  servers during migration. A gRPC client connects to the gRPC
  port; a crow-rpc client connects to the crow-rpc port. After all
  clients are migrated, the gRPC server is removed. Expected: no
  downtime, no consensus disruption.

**Solution**

Migrate DiskdbService from tonic/gRPC to the R104 `crow-rpc`
library. All 11 RPCs are unary (no streaming) — R114 is not needed.
The protocol semantics (request/response shapes, error codes) are
preserved; only the transport changes. The `.proto` schemas are
converted to `.fbs` flatbuffer schemas (full conversion, consistent
with R105/diskio — the prost bridge is not used). Zero-copy wrapper
classes for the new flatbuffer types are defined in `crow-protocol`
per `design-crow-rpc.md` §6.

**One-line summary**: Replace gRPC on the DiskdbService path with
crow-rpc, converting all 11 unary RPCs to flatbuffer-over-TCP,
preserving protocol semantics and establishing the migration pattern
for R32 and R117.

**Numbered work items**:

1. **Flatbuffer schemas for DiskdbService** (`lib/crow-protocol/
   src/fbs/diskdb.fbs`) — convert `diskdb_service.proto`,
   `diskdb_op.proto`, and `diskdb_type.proto` to a single
   `diskdb.fbs` (or split as `diskdb_type.fbs` + `diskdb_op.fbs` +
   `diskdb_service.fbs` to mirror the proto split). Message types:
   AllocateBlocks, FreeBlocks, CommitBlocks, QueryCapacityStats,
   GetDiskGroupInfo, GetDiskInfo, RebuildZoneBitmap, RecalcDiskUsage,
   CompactZone, TriggerScan, GetScanStatus — each a request +
   response table. Register message type IDs in the 3000s range in
   `msg_type.fbs`. Follow the `FB` prefix convention. Files:
   `lib/crow-protocol/src/fbs/diskdb.fbs` (new),
   `lib/crow-protocol/src/fbs/msg_type.fbs`,
   `lib/crow-protocol/build.rs`,
   `lib/crow-protocol/src/lib.rs` (re-exports).

2. **Zero-copy wrapper classes** (`lib/crow-protocol/src/fb_wrappers/`)
   — define `FB<Type>Ref` wrappers for the diskdb request/response
   types per `design-crow-rpc.md` §6. Each wrapper holds a reference
   to the flatbuffer buffer and exposes typed accessor methods
   (null-safe, domain-typed return). No owned intermediate structs.
   Define wrappers only for types that need them (request parsing on
   the server side, response parsing on the client side). Files:
   `lib/crow-protocol/src/fb_wrappers/diskdb.rs` (new),
   `lib/crow-protocol/src/fb_wrappers/mod.rs` (new).

3. **Server-side migration** (`app/crow-diskdb/src/service/`) —
   replace the tonic `DiskdbService` server with a crow-rpc
   `RpcServer` handler set. Each handler dispatches by `msg_type`
   to the existing diskdb logic (allocator, scanner, zone
   management). The response is a flatbuffer frame built per §6
   (build → finish → attach). The crow-rpc server runs alongside
   the existing tonic server during the mixed-rollout window. Files:
   `app/crow-diskdb/src/service/diskdb_service.rs` (rewrite),
   `app/crow-diskdb/src/main.rs` (add crow-rpc server startup).

4. **Client-side migration** (`lib/crow-diskdb-client/src/`) —
   replace the tonic `Channel` pool with a crow-rpc `RpcClient` +
   `ConnectionPool`. The `disk_group_id → endpoint` cache stays;
   the endpoint string changes from `http://host:port` to
   `host:port`. The retry logic (on `Unavailable`/
   `DeadlineExceeded`/`Aborted`) maps to crow-rpc `RpcError`
   variants (`ConnectionClosed`, `Timeout`, `SendQueueFull`). The
   `map_status` function is replaced by a `From<RpcError>` impl.
   Files: `lib/crow-diskdb-client/src/client.rs` (rewrite),
   `lib/crow-diskdb-client/src/lib.rs`.

5. **Error model parity** (`lib/crow-diskdb-client/src/client.rs`)
   — map crow-rpc transport errors to the existing
   `DiskdbClientError` variants. `ConnectionClosed` → retry on next
   connection (same as gRPC `Unavailable`). `Timeout` →
   `DiskdbClientError::Timeout`. `SendQueueFull` → retry with
   backoff (same as gRPC `Unavailable` under load). The
   `ResourceExhausted` / `NotFound` / `InvalidArgument` /
   `PermissionDenied` gRPC status codes are protocol-level errors
   carried in the flatbuffer response body (a `ret_code` field),
   not transport errors. Files:
   `lib/crow-diskdb-client/src/client.rs`.

6. **`grpc_endpoint` → `rpc_endpoint` rename** (`lib/crow-kv-client/
   src/service_registry.rs`, `app/crow-diskdb/src/liveness/`) —
   rename the Rust struct fields and method parameters from
   `grpc_endpoint` to `rpc_endpoint`. The group-0 sysdata wire
   field name stays `grpc_endpoint` (backward compat — old nodes
   reading new registry entries). The keepalive struct field
   `with_grpc_endpoint` becomes `with_rpc_endpoint`. This is a
   mechanical rename confined to Rust source; no sysdata schema
   change. Files: `lib/crow-kv-client/src/service_registry.rs`,
   `app/crow-diskdb/src/liveness/keepalive.rs`,
   `app/crow-diskdb/src/main.rs`.

7. **Mixed rollout + cutover** — the diskdb server runs both tonic
   (gRPC) and crow-rpc servers simultaneously. Clients switch via a
   config flag (or by detecting which port responds). After all
   clients are migrated, the tonic server is removed in a follow-up
   commit. The `diskdb_service.proto` stays in `crow-protocol` as a
   legacy/reserved schema (same as `diskio_service.proto` after
   R105). Files: `app/crow-diskdb/src/main.rs`.

8. **Benchmark + regression** (`tools/bench-diskdb-rpc.sh` or
   extend `crow-cli bench`) — a benchmark that runs concurrent
   `AllocateBlocks` calls against both the gRPC path (baseline) and
   the crow-rpc path. Verifies no regression at 1T:1C and improved
   scaling at 2T:1C+. Added to the regression sentinel suite.
   Files: `tools/bench-diskdb-rpc.sh` (new) or
   `app/crow-cli/src/bench/targets/`.

**Flow diagram**:

```
                    Before (gRPC)                          After (crow-rpc)
                    ─────────────                          ────────────────

chunkdb A ─┐                       chunkdb A ─┐
chunkdb B ─┼─► tonic Client ──►    chunkdb B ─┼─► RpcClient ──► MPSC queue
chunkdb C ─┤    (h2 lock)          chunkdb C ─┤    (no lock)       │
           ┘                       chunkdb D ─┘                    │
                                                             Writer task
                                                             writev() ──► TCP
                                                                   │
                                                                   ▼
                                                            Server reader
                                                            dispatch by msg_type
                                                            DiskdbService
                                                            handler → allocator
```

**Edge cases at a glance**:

- Connection to a removed diskdb instance → crow-rpc reconnect
  fails; the endpoint is removed from the client's cache via the
  membership change callback. No retry to a dead endpoint.
- `ResourceExhausted` (no space) → carried in the flatbuffer
  response `ret_code`, not a transport error. Client maps it to
  `DiskdbClientError::Rpc` (same as gRPC `ResourceExhausted`).
- Mixed gRPC + crow-rpc during rollout → both servers run
  simultaneously; clients switch via config flag. After all clients
  migrated, gRPC server removed.
- Backpressure under burst → crow-rpc `SendQueueFull` maps to retry
  with backoff (same as gRPC `Unavailable` under load).
- `grpc_endpoint` field in sysdata → wire name stays (backward
  compat); Rust struct fields rename to `rpc_endpoint`.

**Dependencies**

- **Depends on**: R104 (crow-rpc engine — finished). R114
  (streaming) is **not** needed — all 11 RPCs are unary.
- **Depended on by**: nothing (terminal migration item). R32 and
  R117 reuse the migration pattern established here (schema
  conversion, wrapper classes, error mapping, mixed rollout).

**Acceptance**

**Transport parity**:
- `AllocateBlocks` over crow-rpc produces the same block allocation
  as over gRPC (same blocks reserved, same `AllocateResponse`
  content). Integration test (run a diskdb instance, allocate via
  crow-rpc, verify blocks).
- `FreeBlocks` + `CommitBlocks` over crow-rpc produce the same
  state change as over gRPC. Integration test.
- `QueryCapacityStats` / `GetDiskGroupInfo` / `GetDiskInfo` over
  crow-rpc return the same data as over gRPC. Integration test.
- `CompactZone` / `TriggerScan` / `GetScanStatus` / `RebuildZoneBitmap`
  / `RecalcDiskUsage` over crow-rpc produce the same state change
  + response as over gRPC. Integration test.

**Error model**:
- crow-rpc `ConnectionClosed` → client retries on next connection
  (same as gRPC `Unavailable`). Integration test (kill diskdb
  mid-call).
- crow-rpc `Timeout` → client returns `DiskdbClientError::Timeout`
  (same as gRPC deadline exceeded). Integration test.
- crow-rpc `SendQueueFull` → client retries with backoff (same as
  gRPC `Unavailable` under load). Integration test.
- `ResourceExhausted` in flatbuffer `ret_code` → client maps to
  `DiskdbClientError::Rpc("no space")` (same as gRPC
  `ResourceExhausted`). Integration test.

**Mixed rollout**:
- A diskdb instance running both gRPC and crow-rpc servers: a gRPC
  client connects to the gRPC port, a crow-rpc client connects to
  the crow-rpc port, both succeed. Integration test.

**Zero-copy wrapper**:
- The diskdb server handler parses a request flatbuffer via the
  `FB<Type>Ref` wrapper (no owned intermediate struct, no field
  copy). Verified by code review — no `to_vec()` / `clone()` on
  control buffer fields in the handler path.

**Test commands**: `pixi run cargo test -p crow-diskdb-client`,
`pixi run cargo test -p crow-diskdb`, `pixi run cargo fmt --all
-- --check`, `pixi run cargo clippy --all-targets -- -D warnings`.
