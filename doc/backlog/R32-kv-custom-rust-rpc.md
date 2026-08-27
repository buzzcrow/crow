<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R32: kv — KV Consensus Hot Path → R104 RPC

**Problem**

gRPC (tonic + h2) serializes concurrent writers on a connection-level
userspace lock. HTTP/2's stream/HPACK/flow-control architecture
inherently requires a connection-level lock for correctness — the
shared HPACK table, frame output buffer, and flow-control windows are
mutable state that demands serialization. When N threads submit to one
gRPC connection, they funnel through a single-threaded userspace
critical section before any of them reach `write()`. The kernel's TCP
coalescing never sees concurrent writers; h2 hands it one merged buffer
from one thread.

The measured cost is a **~17% throughput drop at 2T:1C** (read bench,
`doc/design/kv/kv-read-flow-analysis.md` ≈L520-545). At 1T:1C the lock
is uncontended and the cost is zero; the loss grows with
thread:connection ratio.

This is a **design mismatch**, not a tuning problem — h2 cannot accept
concurrent writers without a lock. The R104 RPC library
(`doc/backlog/R104-protocol-flatbuffer-rpc.md`) provides the
replacement transport: flatbuffer-over-TCP with a per-connection
lock-free writer queue. R32 is the migration of the KV consensus hot
path from gRPC to R104.

**Current behavior + impact**: All internal KV RPC (replica-to-replica
Paxos, LearnerStream, PxService) uses tonic/gRPC. The h2 connection
lock costs ~17% at 2T:1C and forces the production deployment to run
1T:1C to avoid the loss. The management API (Axum HTTP) and
client-facing surface are separate and unaffected.

**Design pointers**: KV RPC sub-design
`doc/design/kv/design-crow-kv-rpc.md` covers the current gRPC wire
protocol (PxService, LearnerStream, error model). R32 swaps the
transport layer; the protocol semantics (request/response shapes,
error codes, `NotLeaderHint`) are preserved. The h2-lock analysis
lives in `doc/design/kv/kv-read-flow-analysis.md` ≈L546-622.

**Use scenarios**:

- **Paxos accept under concurrency**: 4 followers each submit accepts
  to a leader over 2 connections (2T:1C). With gRPC, the two threads on
  each connection funnel through the h2 lock. With R104, each accept is
  a framed message pushed to the per-connection MPSC queue — no
  userspace lock. Expected: throughput recovers the ~17% loss; higher
  thread:connection ratios scale linearly.

- **LearnerStream catch-up**: A follower catching up streams data from
  the leader. The LearnerStream is a long-lived request-response
  pattern. Under R104, it becomes a sequence of framed messages on a
  persistent connection — no h2 stream management overhead. Expected:
  identical catch-up semantics, lower per-message overhead.

- **NotLeaderHint redirect**: A follower receives a request destined
  for the leader, returns `NotLeaderHint`. Under R104, the error is a
  flatbuffer response with the same `NotLeaderHint` payload. The
  client's retry logic is unchanged. Expected: no contract change.

- **Mixed workload**: Heartbeats and accepts share a connection. Under
  gRPC, heartbeats can be starved behind write backpressure (E5
  reserves queue admission, not wire priority — tracked separately as
  R53). Under R104, the lock-free queue does not starve heartbeats
  behind accepts in the same way (both are cheap queue pushes).
  Expected: reduced heartbeat starvation, though R53's separate-channel
  mitigation is still the correctness fix.

**Solution**

Migrate the KV consensus internal RPC from tonic/gRPC to the R104
`crow-rpc` library. The protocol semantics (request/response shapes,
error codes, `NotLeaderHint`, LearnerStream) are preserved — only the
transport changes. The existing `pxos.proto` is converted to a `.fbs`
flatbuffer schema (full conversion — no prost bridge, consistent with
R105/diskio and R115/diskdb; see Resolved Questions).

**One-line summary**: Replace gRPC on the KV internal replica-to-
replica path with the R104 flatbuffer RPC library, preserving protocol
semantics and recovering the h2-lock throughput loss.

**Numbered work items**:

1. **Flatbuffer schemas for consensus messages**
   (`lib/crow-protocol/src/fbs/`) — convert the existing
   `lib/crow-kv/src/rpc/proto/pxos.proto` (PxService + SnapshotService:
   Prepare/Promise, Accept/Accepted, PreVote/RequestVote/Heartbeat/
   StepDown, LearnerStream, StreamSnapshot, ChosenNotification,
   FetchGap) to a `.fbs` schema. `NotLeaderHint` is carried as fields
   on the response tables (leader endpoint + term + membership epoch),
   not a separate wrapper. Register message type IDs in the 1000–1099
   sub-range of `msg_type.fbs` (consensus range; R117 takes 1100–1199
   for the client-facing path). Follow R115's codegen layout: a new
   `kv_consensus.fbs`, a `kv_consensus_generated` module in
   `lib/crow-protocol/src/lib.rs` re-exported as
   `pub mod kv_consensus_fb`, and `build.rs` updated to invoke
   `flatc --gen-all`.

2. **Server-side migration** (`app/crow-kv-server/src/`) — replace the
   tonic `PxService` + `SnapshotService` servers with an R104
   `RpcServer` handler. Follow R115's pattern
   (`app/crow-diskdb/src/service/diskdb_rpc_service.rs`): a new
   `px_rpc_service.rs` module that registers handlers keyed by
   `FBMsgType` and dispatches to the existing consensus logic
   (`ConsensusHandler`, `LearnerStreamHandler`,
   `SnapshotStreamHandler`). The response is built with
   `FlatBufferBuilder` + `submit_response`. The crow-rpc server runs
   alongside the existing Axum HTTP management server (which stays on
   HTTP) on a new consensus port (`KV_RPC_BASE` — inter-KV-server
   only; R117 later adds a separate client-facing port for outside
   services). The gRPC server remains temporarily for the
   client-facing surface until R117 migrates it. Wiring lands in
   `startup.rs` (where the existing tonic services are bound today).

3. **Client-side migration** (`lib/crow-kv/src/rpc/` +
   `lib/crow-kv/src/cluster/learner_stream.rs`) — replace the tonic
   `PxServiceClient` with an R104 `RpcClient` + connection pool. The
   existing `lib/crow-kv/src/rpc/px_service.rs`,
   `snapshot_service.rs`, `kv_response.rs`, and
   `lib/crow-kv/src/cluster/learner_stream.rs` are the rewrite sites.
   `NotLeaderHint` is parsed from the flatbuffer response (via the
   zero-copy wrapper from work item 6) and fed into the existing
   retry logic (`crow-kv-client` retry + topology cache). The
   `LearnerStream` client becomes a persistent R104 connection with
   pipelined unary `call()`s: `Accept`/`Heartbeat`/`FetchGap` are
   request→response `call()`s with per-request correlation IDs;
   `ChosenNotification`/`BatchChosenNotification` are fire-and-forget
   sends (no response awaited, matching the current `reply_tx: None`
   path). The `StreamSnapshot` client becomes an R114 server-streaming
   `Stream` (one `SnapshotRequest`, many `SnapshotStreamItem`
   responses). Follow R115's `DiskdbRpcTransport` structure
   (`lib/crow-diskdb-client/src/rpc_transport.rs`): shared `RpcServer`
   (connection owner) + `RpcClient` (completion pool) +
   `DashMap<endpoint, Connection>` per-peer pool.

4. **Error model parity** (`lib/crow-kv/src/rpc/`) — map R104
   transport errors to the existing KV error variants
   (`NotLeaderHint`, `Unavailable`, `Timeout`). Reuse R115's
   `RpcError::is_retryable()` helper
   (`lib/crow-rpc/ffi/src/server.rs`): `ConnectionClosed`/`Timeout`/
   `SendQueueFull`/`ConnectionError` are retryable;
   `RegistrationFailed`/`AllDown`/`InvalidArg` are not. The client
   retry logic must treat R104 errors the same as the equivalent
   gRPC status codes. `NotLeaderHint` is a protocol-level error
   (carried in the response body as response fields, not a transport
   error) — unchanged.

5. **Benchmark + regression** (`tools/bench-kv-rpc.sh`, new) — a
   benchmark script that runs the 2T:1C read bench (`crow-cli bench
   kv`) against both the gRPC path (baseline) and the R104 path.
   Verifies the ~17% loss is recovered. Also runs 1T:1C to verify no
   regression at the uncontended point. Added to the regression
   sentinel suite alongside `bench-kv-read-regression.sh` and
   `bench-kv-write-regression.sh`. Prerequisite: capture the gRPC
   baseline before starting the migration (todo_fb.md §6 — the
   baseline goes in `doc/design/rpc/rpc-migration-baselines.md`, new,
   or extends `rpc-flow-analysis.md`). Note:
   `tools/bench-rpc-regression.sh` is the RPC-echo sentinel (R104
   transport only, no KV layer) — distinct from this KV-path bench.

6. **Zero-copy wrapper classes** (`lib/crow-protocol/src/
   fb_wrappers/kv_consensus.rs`, new) — define `FB<Type>Ref` wrappers
   for the consensus response types (`FBPromiseResponseRef`,
   `FBAcceptedResponseRef`, `FBHeartbeatResponseRef`,
   `FBFetchGapResponseRef`, `FBLearnerStreamResponseRef`). Each
   wrapper holds a `&[u8]` reference to the control buffer, parses
   the root on construction, and exposes typed accessor methods that
   read through the root pointer — no per-field copy, no owned
   intermediate struct. Includes `NotLeaderHint` accessors (leader
   endpoint + term + membership epoch) on response wrappers. Follows
   `design-crow-rpc.md` §6.1 pattern. This is the design-doc-correct
   approach that R115 deferred (R115 parses into owned proto types —
   follow-up tasks tracked in todo_fb.md to fix R115's gaps). R32
   implements it properly because the Paxos hot path is the
   perf-critical path where per-response allocation would partially
   offset the h2-lock recovery.

7. **Server→client send FFI helper** (`lib/crow-rpc/ffi/src/`) —
   resolve the R114 open issue: `RpcClient::send()` takes
   `&Connection`, but a server-side handler only has the raw
   `conn_handle` from `ServerRequest`. Add a
   `Connection::from_handle(raw_conn_handle)` constructor in
   `lib/crow-rpc/ffi/src/server.rs` — `Connection` is already a
   trivial wrapper around `sys::crow_rpc_conn_t` with a no-op `Drop`
   (the transport owns the connection), so constructing one from the
   raw pointer is safe and lets a server-side handler call
   `RpcClient::call()`/`send()`. R32 itself does not
   need this (LearnerStream's server side only sends responses via
   `submit_response`), but R117's WatchNotify (server pushes
   notifications to the client) does. R32 resolves it to unblock R117
   rather than leaving it as an R114 carry-over. Also unblocks the
   R114 E2E test gap (the `client_handler_dispatch_via_server_chain`
   test that was dropped).

**Flow diagram**:

```
                          Before (gRPC)                          After (R104)
                          ─────────────                          ────────────

Follower A ─┐                       Follower A ─┐
Follower B ─┼─► tonic Client ──►    Follower B ─┼─► RpcClient ──► MPSC queue
Follower C ─┤    (h2 lock)          Follower C ─┤    (no lock)       │
Follower D ─┘                       Follower D ─┘                    │
                                                                     ▼
                                                              Writer task
                                                              writev() ──► TCP
                                                                     │
                                                                     ▼
                                                              Server reader
                                                              dispatch by type
                                                              ConsensusHandler
```

**Edge cases at a glance**:

- Connection to a removed replica → R104 reconnect fails; the replica
  is removed from the connection pool via the membership change
  callback. No retry to a dead endpoint.
- `NotLeaderHint` with a stale leader hint → client's existing
  topology-cache refresh handles this; no change.
- LearnerStream mid-stream connection drop → R104 fails the in-flight
  request; the learner reconnects and resumes from the last applied
  slot. Same semantics as gRPC stream reconnect.
- Mixed gRPC + R104 during rollout → both servers run simultaneously
  during migration; clients switch via a config flag. After all
  clients are migrated, the gRPC server is removed.
- Backpressure under burst → R104 `BackpressureError` maps to
  `Unavailable`; the client retries on a different connection or
  backs off. Same behavior as gRPC `UNAVAILABLE`.

**Dependencies**

- **Depends on**: **R104** (flatbuffer RPC engine — finished) — uses
  `crow-rpc` crate for framing, connection, pool, schedule.
  **R114** (bidirectional request-response — finished) —
  `StreamSnapshot` (server-streaming) uses R114's streaming
  primitives. `LearnerStream` does NOT need R114 bidi — it is modeled
  as pipelined unary `call()`s on a persistent connection (see
  Resolved Questions). **R115** (diskdb migration — finished) is the
  proof-of-pattern for the unary migration steps: `.fbs` schema +
  `diskdb_rpc_service.rs` server handler + `DiskdbRpcTransport`
  client + `is_retryable()` error mapping + port-offset mixed rollout.
  R32 follows the same layout for the KV consensus path.
- **Depended on by**: **R117** (KvService client-facing migration)
  reuses the 1000s `msg_type` sub-range split, the `NotLeaderHint`
  flatbuffer response-field model, and the zero-copy wrapper pattern
  established here. R32's work item 7 (server→client send FFI helper)
  unblocks R117's WatchNotify server-push path. R53 (heartbeat
  channel) is separate and independent.

**Acceptance**

**Transport parity**:
- A Paxos accept request over R104 produces the same consensus state
  change as over gRPC (same proposal accepted, same log entry
  persisted). Integration test (run a 3-node cluster, submit accepts
  via R104, verify log consistency).
- `NotLeaderHint` response over R104 is parsed correctly by the
  client → client redirects to the hinted leader. Integration test.
- LearnerStream over R104: a follower joins, catches up via
  LearnerStream, reaches `CaughtUp` state. Integration test.

**Performance**:
- 2T:1C read throughput over R104 is within 5% of the 1T:1C baseline
  (i.e., the h2-lock loss is recovered). Benchmark test
  (`tools/bench-kv-rpc.sh`, Linux only).
- 1T:1C read throughput over R104 is within 5% of the gRPC 1T:1C
  baseline (no regression at the uncontended point). Benchmark test.

**Error model**:
- R104 `ConnectionError` → client retries on next connection (same as
  gRPC `UNAVAILABLE`). Integration test (kill a replica mid-call).
- R104 `TimeoutError` → client returns `Timeout` to caller (same as
  gRPC deadline exceeded). Integration test.
- R104 `BackpressureError` → client retries with backoff (same as
  gRPC `UNAVAILABLE` under load). Integration test.

**Mixed rollout**:
- A cluster running both gRPC and R104 servers: a gRPC client can
  still connect to the gRPC server, an R104 client can connect to the
  R104 server. Integration test (3-node cluster, 1 node on gRPC, 2 on
  R104, verify consensus works).

**Test commands**: `pixi run cargo test -p crow-kv --test rpc_migration`
(new test file), `pixi run tools/bench-kv-rpc.sh` (Linux),
`pixi run cargo fmt --all -- --check`,
`pixi run cargo clippy --all-targets -- -D warnings`.

**Resolved Questions**

- **Schema conversion scope** — full `.fbs` conversion. R105 (diskio)
  chose full `.fbs` conversion (`diskio_service.proto` is now
  legacy/reserved); R115 (diskdb) and R116 (chunkdb) follow the same
  approach. R32 converts `pxos.proto` to a `.fbs` schema in
  `lib/crow-protocol/src/fbs/`. The prost-to-flatbuffer bridge is not
  used — it would add a conversion step and a dual-schema maintenance
  burden. The zero-copy wrapper convention (`design-crow-rpc.md` §6)
  applies: `FB`-prefixed types, no owned intermediate structs, no
  per-field copy.
- **Streaming support** — R114 (bidirectional request-response) is
  finished. `StreamSnapshot` (server-streaming) uses R114's streaming
  primitive. `LearnerStream` does NOT need R114 bidi — see the
  `LearnerStream` modeling decision below. R32 is unblocked.
- **`.fbs` schema filename + module name** — `kv_consensus.fbs`,
  re-exported as `pub mod kv_consensus_fb` (parallel to R117's
  planned `kv_client.fbs` / `kv_client_fb`). R117's doc previously
  referenced `kv_rpc.fbs` — updated to `kv_consensus.fbs` in R117's
  Dependencies section.
- **Zero-copy wrapper vs. R115's parse-into-owned pattern** —
  zero-copy (option a). R32 defines true `FB<Type>Ref` wrappers in
  `lib/crow-protocol/src/fb_wrappers/kv_consensus.rs` (work item 6).
  R115's actual pattern (parse into owned proto types per call) is a
  deferred gap — follow-up tasks tracked in todo_fb.md to retrofit
  zero-copy wrappers onto R115's diskdb client transport. R32
  implements it properly because the Paxos hot path is the
  perf-critical path where per-response allocation would partially
  offset the h2-lock recovery (the whole point of R32).
- **R114 server→client send FFI gap** — resolved by R32 (work item
  7). The fix is a `Connection::from_handle(raw_conn_handle)`
  constructor in `lib/crow-rpc/ffi/src/server.rs`: `Connection` is
  already a trivial wrapper around `sys::crow_rpc_conn_t` with a
  no-op `Drop` (the transport owns the connection), so constructing
  one from the raw `conn_handle` in `ServerRequest` is safe and lets
  a server-side handler call `RpcClient::call()`/`send()`. R32
  itself does not need it (confirmed: `LearnerStream`'s server side
  only sends responses via `submit_response` — see code at
  `lib/crow-kv/src/rpc/px_service.rs` L395-459, the server's
  `learner_stream` handler only calls `tx.send(Ok(...))` in response
  to inbound frames; `ChosenNotification`/`BatchChosenNotification`
  get no reply at all). R32 resolves it to unblock R117's WatchNotify
  (server pushes notifications to the client) and the R114 E2E test
  gap (the dropped `client_handler_dispatch_via_server_chain` test).
  Do not leave it as an R114 carry-over.
- **`LearnerStream` modeling — R114 bidi vs. persistent-connection
  request-response** — persistent-connection request-response. The
  proto declares `LearnerStream` as bidi (`stream` request, `stream`
  response), and semantically it is true bidi in the gRPC sense (both
  halves carry independently-framed messages). But the actual usage
  is request→response over a long-lived connection: the leader is the
  client (`PxLearnerStream` in `lib/crow-kv/src/cluster/
  learner_stream.rs` opens the stream to the follower), the follower
  is the server (only responds via `submit_response`).
  `Accept`/`Heartbeat`/`FetchGap` are request→response `call()`s with
  per-request correlation IDs; `ChosenNotification`/
  `BatchChosenNotification` are fire-and-forget sends
  (`reply_tx: None` in `OutboundCmd`). The server never initiates a
  request to the client. The practical difference between "R114 bidi
  `Stream`" and "pipelined unary `call()`s on a persistent
  connection": the bidi `Stream` primitive maintains an
  independently-initiated outbound channel (server can push unsolicited
  frames); the persistent-connection unary model only has
  client-initiated requests + server responses. Since the server never
  initiates, the bidi primitive's extra machinery (independent
  server→client send path, separate stream state machine) is unused
  complexity. The persistent-connection unary model is simpler,
  reuses R115's `RpcClient::call()` path directly, and still pays the
  connection-setup cost once per leadership tenure (the original
  rationale for the bidi stream). `StreamSnapshot` is genuinely
  server-streaming (one `SnapshotRequest`, many `SnapshotStreamItem`
  responses) — that one uses R114's server-streaming primitive.
- **Mixed-rollout port scheme** — separate ports. R32 adds a
  `KV_RPC_BASE` constant to `crow-protocol/src/ports.rs` for the
  inter-KV-server consensus port (replica-to-replica Paxos). R117
  later adds a separate client-facing port for outside services
  (crow-kv-client, crow-diskio). Two crow-rpc servers in the same
  `crow-kv-server` process, each on its own port, each with its own
  `RpcServer` instance + handler map. Rationale: the consensus path
  is internal-only (trusted peers, no client auth), the client-facing
  path is exposed to outside services (different trust boundary,
  different authz, different connection-pool sizing). Mixing them on
  one port would couple their backpressure and security profiles.
