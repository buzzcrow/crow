<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROWDB - Design: RPC Wire Protocol

Depends on: [`design-crowdb-kv.md`](design-crowdb-kv.md) §2, §3, §9.2, §10
Satisfies: [`design-crowdb-kv.md`](design-crowdb-kv.md) §3, §9.2, §10.1

This document defines the wire-serialization contract for all
node-to-node and client-to-node RPC communication. The consensus hot
path (Prepare/Promise, Accept/Accepted, PreVote/RequestVote,
Heartbeat/StepDown, ChosenNotification, FetchGap) uses the
**crowdb-rpc flatbuffer transport** — a custom epoll/kqueue-based RPC
library with flatbuffer framing and request/response correlation via
oneshot channels. Client-to-node KV RPCs (Get/Set/Delete/Batch/Scan)
use the same crowdb-rpc engine on a separate port with a dedicated
schema — see [`design-crowdb-kv-rpc-client.md`](design-crowdb-kv-rpc-client.md)
for the client-facing transport design. Every message carries
a `version: u32` field for forward/backward compatibility; no
`required` fields; field numbers are append-only.

## Table of Contents

- [1. Design Principles](#1-design-principles)
- [2. Message Surface](#2-message-surface)
- [3. Transport Architecture — crowdb-rpc for Consensus](#3-transport-architecture--crowdb-rpc-for-consensus)
- [4. Server-Side Handler (PxRpcService)](#4-server-side-handler-pxrpcservice)
- [5. Client-Side Transport (PxRpcTransport)](#5-client-side-transport-pxrpctransport)
- [6. RPC Routing in PxRemoteReplica](#6-rpc-routing-in-pxremotereplica)
- [7. Flow Control and Parallelism](#7-flow-control-and-parallelism)
  - [7.1 Quorum Short-Circuit](#71-quorum-short-circuit)
  - [7.2 RPC Deadline](#72-rpc-deadline)
- [8. Paxos Error Model](#8-paxos-error-model)
- [9. Version Compatibility Rules](#9-version-compatibility-rules)
- [10. Flatbuffer Schema (kv_consensus.fbs)](#10-flatbuffer-schema-kv_consensusfbs)
- [11. Zero-Copy Wrapper Classes](#11-zero-copy-wrapper-classes)
- [12. Port Allocation](#12-port-allocation)
- [13. Flatbuffer `bytes` Field Mapping — `bytes::Bytes`](#13-flatbuffer-bytes-field-mapping--bytesbytes)

---

## 1. Design Principles

1. **All RPCs over crowdb-rpc.** The Paxos hot
   path (Prepare/Accept/Heartbeat/Chosen/FetchGap) runs on the
   crowdb-rpc flatbuffer transport for zero-copy framing and
   epoll/kqueue I/O efficiency. Client-facing KV RPCs (Get/Set/Delete/
   Batch/Scan/Watch) run on the same crowdb-rpc engine on a separate
   port with a dedicated schema for client library ergonomics
   and `Bytes` mapping.
2. **Pipelined unary calls on persistent connections.** Steady-state
   data traffic (`Accept`, `ChosenNotification`) multiplexes over
   pipelined unary `call()`s on a persistent crowdb-rpc connection per
   `(group_id, peer_id)` pair. Connection setup cost is paid once per
   server lifetime. Fire-and-forget frames (`ChosenNotification`,
   `BatchChosenNotification`) use `send()` with a no-op completion
   callback — no reply is awaited.
3. **Heartbeats on the same connection.** Steady-state `Heartbeat`
   traffic flows over the same crowdb-rpc connection as `Accept` and
   `Chosen`. The crowdb-rpc transport multiplexes frames at the I/O
   layer (epoll/kqueue), so heartbeats are not blocked behind data
   frames the way they would in a bidi stream's FIFO send-half.
   The term fence handles cross-term reordering; same-term
   heartbeat/accept mutate independent state.
4. **No `required` fields.** All flatbuffer fields are optional or
   have sensible defaults. A missing `version` field defaults to `0`.
5. **Field numbers are append-only.** Once assigned, a field number is
   never reused for a different semantic meaning. This is a hard rule
   for rolling upgrades.
6. **Plaintext in P1/P4; TLS hooks reserved.** The transport layer is
   plaintext TCP loopback in tests and P1/P4 integration. TLS config
   slots exist in the service builder but are unimplemented.

---

## 2. Message Surface

The wire protocol defines four classic Paxos messages (P1 M2):
`Prepare`, `Promise`, `Accept`, `Accepted`, sufficient to run a full
classic Paxos round across a real network boundary. Election messages
(`Heartbeat`, `RequestVote`, `PreVote`, `StepDown`) and the
`ChosenNotification` / `BatchChosenNotification` / `FetchGap` frames
are added in P1 M3; snapshot and client RPCs land in P4.

Key reusable sub-message: `AcceptedValue` carries `(slot, round,
leader_id, term, payload)`. Used in `Accept` requests and `Promise`
responses for classic Paxos value-recovery. The `payload` is opaque
bytes in P1 M2; `kind` discrimination (empty = `NoOp`, non-empty =
`Write`) is not a wire field. `ConfigChange` and `DedupCheckpoint`
kinds are designed but not yet implemented.

The full flatbuffer schema is in `lib/crowdb-protocol/src/fbs/kv_consensus.fbs`;
the crowdb-rpc schema definitions for client KV RPCs are in
`lib/crowdb-kv/src/rpc/fbs/`. This doc covers design decisions only.

---

## 3. Transport Architecture — crowdb-rpc for Consensus

The consensus RPC path uses the R104 `crowdb-rpc` engine: a C++
epoll/kqueue transport with flatbuffer framing and request/response
correlation. The Rust facade (`crowdb-rpc-ffi`) exposes `RpcServer`
(listen + register handlers), `RpcClient` (connect + `call()` for
request-response, `send()` for fire-and-forget), and `Connection`
(per-peer persistent connection).

**Why crowdb-rpc for consensus:**

- **No per-stream lock.** The crowdb-rpc transport handles
  concurrent frames without a per-stream lock, avoiding the ~17%
  throughput loss that per-stream serialization would cause at 2T:1C
  write workload. The epoll/kqueue I/O loop processes frames
  concurrently.
- **Zero-copy framing.** Flatbuffer responses are read in-place via
  `FB<Type>Ref` wrappers — no deserialization into owned types on the
  hot path.
- **Fire-and-forget support.** `ChosenNotification` and
  `BatchChosenNotification` are sent via `send()` with a no-op
  completion callback — no oneshot channel allocation, no reply
  correlation overhead.

**Client-facing KV RPCs:** Client-facing KV RPCs (`Get`, `Set`, `Delete`,
`Batch`, `Scan`, `Watch`) run on the same crowdb-rpc engine on a separate
port with a dedicated schema. The client library
(`crowdb-kv-client`) uses crowdb-rpc for retry, topology cache, and
`NotLeaderHint` handling. The crowdb-rpc server also serves
`SnapshotService` (snapshot install stream).

**Connection model:** Each `PxKvStore` runs one `RpcServer` on the
crowdb-rpc port (derived from the base port via a fixed offset, see
§12). The shared `PxRpcTransport` holds one `RpcClient` + a
`DashMap<endpoint, Connection>` connection cache. All `PxRemoteReplica`
instances in the store share the same transport.

---

## 4. Server-Side Handler (PxRpcService)

`PxRpcService` is the crowdb-rpc handler set for consensus RPCs. It
holds `Arc<PxKvStore>` + a tokio `Handle` (for spawning async
handlers). `register_handlers` wires one handler per `msg_type` into
the `RpcServer`:

- `EPrepareRequest` → `handle_prepare` — dispatches to
  `PxLocalReplica::on_prepare`, builds `FBPromiseResponse`.
- `EAcceptRequest` → `handle_accept` — dispatches to
  `PxLocalReplica::on_accept`, builds `FBAcceptedResponse`.
- `EPreVoteRequest` → `handle_pre_vote` — dispatches to
  `PxLocalReplica::on_pre_vote`, builds `FBPreVoteResponse`.
- `ERequestVoteRequest` → `handle_request_vote` — dispatches to
  `PxLocalReplica::on_request_vote`, builds `FBRequestVoteResponse`.
- `EHeartbeatRequest` → `handle_heartbeat` — dispatches to
  `PxLocalReplica::on_heartbeat`, builds `FBHeartbeatResponse`.
- `EStepDownRequest` → `handle_step_down` — dispatches to
  `PxLocalReplica::on_step_down`, builds `FBStepDownResponse`.
- `EChosenNotification` → `handle_chosen_notice` — fire-and-forget,
  updates the learner's chosen frontier, no response sent.
- `EBatchChosenNotification` → `handle_batch_chosen` — fire-and-forget,
  batch frontier update, no response sent.
- `EFetchGapRequest` → `handle_fetch_gap` — looks up the accepted
  value at the requested slot, builds `FBFetchGapResponse` (or
  `NotFound` error if the slot is not yet chosen).

Each handler reads the request flatbuffer directly via
`flatbuffers::root::<FB<Type>Request>(req.control)`, dispatches to the
existing consensus logic on `PxLocalReplica`, and builds the response
flatbuffer via `FlatBufferBuilder`. The response is submitted via
`server.submit_response(conn_handle, &ctrl, None, msg_type, req_id)`.

---

## 5. Client-Side Transport (PxRpcTransport)

`PxRpcTransport` is the shared client-side transport for outbound
consensus RPCs. It holds:

- `server: Arc<RpcServer>` — the local server handle (for connection
  attachment).
- `rpc: RpcClient` — the client facade for `call()` / `send()`.
- `connections: DashMap<String, Connection>` — per-endpoint
  connection cache. `conn_for(endpoint)` lazily connects and caches.

**Request-response path (`send_prepare`, `send_accept`, etc.):**
Builds the flatbuffer request via `FlatBufferBuilder`, calls
`rpc.call(server, conn, req_id, control, None, msg_type)`, which
returns a `CallFuture`. The future resolves to a `Response` containing
the control buffer. The response is parsed via the zero-copy
`FB<Type>ResponseRef` wrapper (§11).

**Fire-and-forget path (`send_chosen`, `send_batch_chosen`):** Builds
the flatbuffer, calls `rpc.send(server, conn, req_id, control, None,
msg_type, noop_completion(), null_mut())`. The no-op completion
callback satisfies the C++ side's non-null `on_complete` requirement.
No reply is awaited; failures are returned for caller-side
observability but treated as best-effort.

**Port derivation:** `conn_for(endpoint)` parses the server endpoint
(host:port) and connects to `port + RPC_PORT_OFFSET` (see §12).

---

## 6. RPC Routing in PxRemoteReplica

`PxRemoteReplica` routes all RPCs through crowdb-rpc. Each
`ReplicaClient` trait method (`send_prepare`, `send_accept`,
`send_pre_vote`, `send_request_vote`, `send_heartbeat`,
`send_step_down`) delegates to the transport with a
`tokio::time::timeout` wrapper and records metrics.

`send_chosen_notice` and `send_batch_chosen_notice` use
the transport — fire-and-forget via `transport.send_chosen()` /
`transport.send_batch_chosen()`.

`send_fetch_gap` delegates to
`transport.send_fetch_gap()` (request-response). The `group_fetchgap` loop
also uses `remote.rpc_transport()` to route FetchGap through the
transport.

---

## 7. Flow Control and Parallelism

The crowdb-rpc transport enables the parallel-slot pipelining described
in `design-crowdb-kv-slot.md` §5:

- **Multiple in-flight `Accept` frames per peer.** Each `send_accept`
  call creates a new `CallFuture` and returns immediately; the caller
  does not block waiting for the peer's reply. This allows slot N+1's
  `Accept` to be sent before slot N's `Accepted` response arrives.
- **Connection-level flow control.** The crowdb-rpc engine manages a
  send queue per connection; when the queue is full, `send()` /
  `call()` returns `SendQueueFull`, which the caller treats as
  retryable.
- **Reconnect safety.** On transport failure, the connection is
  dropped from the cache; the next `conn_for()` lazily reconnects.
  The proposer treats this as retryable and re-sends after reconnect.

### 7.1 Quorum Short-Circuit

`run_prepare_phase` and `run_accept_phase` fan out to all peers but
do not wait for all replies. A `FuturesUnordered` (local + each remote,
tagged with `(voting, kind)`) is drained via `StreamExt::next`; the
phase returns as soon as `accepted >= quorum` AND the local reply has
been folded (the W6 invariant: `Chosen`/`Proceed` is never returned
before the local WAL persist / CAS reply is counted). The
still-pending futures are moved into a detached drain task that
continues folding for side effects only: a late `TermStale` triggers
`become_follower`; a late `EpochMismatch` adopts the responder epoch.
The drain captures `self_weak` (upgrades to `Arc<PxGroup>`), so a
dropped group lets the upgrade fail and the task exit cleanly. It
honors `tenure_cancel` so a step-down aborts the drain.

In a 3-node group (quorum = local + 1), the proposal latency is the
quorum-th-fastest reply, not `max(all peers)`. A slow but connected
follower no longer drags every write. Failure detection is preserved.
It just moves off the latency path.

### 7.2 RPC Deadline

Each `send_*` method wraps its `CallFuture` await with
`tokio::time::timeout(rpc_timeout)`. On expiry the caller surfaces a
typed retryable error. A connected-yet-unresponsive peer (GC pause,
half-open socket, overloaded server) is surfaced as a retryable
failure within `learner_stream_rpc_timeout_ms` (default 2000 ms,
aligned with the 2 s election max) rather than blocking the fan-out
indefinitely.

---

## 8. Paxos Error Model

The error categories below determine whether the proposer retries the
same slot, runs classic repair, moves the client operation to a new
slot, redirects the client, or returns a retryable error.

### 8.1 Error Categories

- **`NotLeader`** — KV request reaches follower. Client retries with
  leader hint.
- **`PrepareRejected`** — Phase 1 blocked by higher promised ballot.
  Retry same slot with a higher ballot.
- **`AcceptRejected`** — Phase 2 blocked by higher promised ballot.
  Run classic Phase 1 repair on the same slot.
- **`ForeignValueChosen`** — Phase 1 adopts another value or chosen
  value differs from client value. Learn the chosen value, then retry
  the client operation on a new slot.
- **`QuorumUnavailable`** — Not enough reachable voting peers. Retry
  same slot until budget exhausted.
- **`TransportFailure`** — RPC timeout, connect failure, or
  unavailable peer. Retry same slot; repeated failures become
  `QuorumUnavailable`.
- **`Busy`** — Admission/retry budget exhausted. Client retries with
  backoff.
- **`InternalInvariantViolation`** — Missing required value, invalid
  state transition. Fail test fast; production maps to internal error.

### 8.2 Retry Rules

- **Prepare rejection:** retry Phase 1 for the same slot using a
  ballot above the highest observed rejected ballot. Do not move to a
  new slot.
- **Accept rejection:** run classic Phase 1 for the same slot. If
  Phase 1 discovers a foreign accepted value, repair and learn it
  before the client operation moves to a new slot.
- **Transport or quorum failure:** retry the same slot with the same
  ballot until the retry budget is exhausted. Increasing the ballot is
  only required after a higher-ballot rejection.
- **Foreign value:** adopt it for the current slot. If not the client
  value, retry the client operation on a later slot only after the
  foreign value is learned.

Retry backoff (`retry_backoff`) is `base * 2^attempt` with ±50% jitter
(`base * multiplier / 1000` where `multiplier ∈ [500, 1500]`), using a
thread-local `XorShift64` PRNG seeded from monotonic nanos. This
decorrelates retry storms across replicas that collide on the same
slot. The admission permit is held during backoff (releasing it would
let a duplicate `(client_id, seq)` admit concurrently).

### 8.3 RPC Mapping

Consensus responses carry rejected metadata directly in the
flatbuffer response fields (`rejected`, `term_stale`, `epoch_mismatch`
booleans + the term/epoch fields). RPC transport failures are mapped
by the caller into `TransportFailure`. Malformed consensus RPCs map
to `InternalInvariantViolation` at the Paxos model level and to
`FBKvRetCode::InvalidArgument` at the crowdb-rpc boundary.

---

## 9. Version Compatibility Rules

1. **Sender rule:** every message sets `version = 1` (the initial wire version).
2. **Receiver rule:** decode must accept any `version <= max_supported`. Unknown fields are ignored (flatbuffer default behavior).
3. **Upgrade rule:** new fields are added with new field numbers; old fields are never removed or renumbered.
4. **Field-number freeze per message:** field numbers are frozen once
   assigned. The frozen ranges are documented in the schema file
   comments. Future versions may add new fields starting at the
   next free field number.

---

## 10. Flatbuffer Schema (kv_consensus.fbs)

`lib/crowdb-protocol/src/fbs/kv_consensus.fbs` mirrors the consensus
messages from `pxos.fbs`, following the `diskdb.fbs` conventions
proven by R115:

- `include "common_type.fbs";` for `FBInt128`.
- `namespace crowdb.kv_consensus.proto;`
- Every request/response table carries `id` (request_id) +
  `rpc_create_nano` as its first two fields.
- `FBAcceptedValue` is a table (has a `payload: [ubyte]` vector,
  which requires a vtable).
- `FBDedupTag` is an inline struct (fixed-layout, two `uint64` fields).
- `NotLeaderHint` is NOT a separate message — it is fields on the
  response tables (`not_leader_hint:string` + `term:uint64` +
  `membership_epoch:uint64`).

Message type IDs registered in `msg_type.fbs` (1000–1099 range):

```
EPrepareRequest = 1000,
EPromiseResponse = 1001,
EAcceptRequest = 1002,
EAcceptedResponse = 1003,
EPreVoteRequest = 1004,
EPreVoteResponse = 1005,
ERequestVoteRequest = 1006,
ERequestVoteResponse = 1007,
EHeartbeatRequest = 1008,
EHeartbeatResponse = 1009,
EStepDownRequest = 1010,
EStepDownResponse = 1011,
EChosenNotification = 1012,       // fire-and-forget (no response)
EBatchChosenNotification = 1013,  // fire-and-forget (no response)
EFetchGapRequest = 1014,
EFetchGapResponse = 1015,
ESnapshotRequest = 1016,
ESnapshotResponse = 1017,
```

No separate LearnerStream request/response msg_types — each frame type
has its own msg_type. The persistent connection carries a mix of these
msg_types; the server dispatches each frame independently by its
msg_type.

**Build integration:** `lib/crowdb-protocol/build.rs` compiles
`kv_consensus.fbs` via `flatc --rust --gen-all` (inlines
`common_type.fbs` so `FBInt128` resolves). The generated module is
re-exported as `crowdb_protocol::kv_consensus_fb`.

---

## 11. Zero-Copy Wrapper Classes

`lib/crowdb-protocol/src/fb_wrappers/kv_consensus.rs` defines one
`FB<Type>Ref` struct per response type:

```rust
pub struct FBPromiseResponseRef<'a> {
    root: flatbuffers::Result<'a, FBPromiseResponse<'a>>,
}
impl<'a> FBPromiseResponseRef<'a> {
    pub fn new(buf: &'a [u8]) -> Self { Self { root: flatbuffers::root::<FBPromiseResponse>(buf) } }
    pub fn valid(&self) -> bool { self.root.is_ok() }
    pub fn request_id(&self) -> Option<u64> { self.root.as_ref().ok()?.id() }
    pub fn slot(&self) -> Option<u64> { self.root.as_ref().ok()?.slot() }
    // ... remaining fields
}
```

Same pattern for: `FBAcceptedResponseRef`, `FBHeartbeatResponseRef`,
`FBPreVoteResponseRef`, `FBRequestVoteResponseRef`,
`FBStepDownResponseRef`, `FBFetchGapResponseRef`.

Request wrappers are NOT needed — the server handler reads the request
flatbuffer directly via `flatbuffers::root::<FB<Type>Request>(req.control)`.
The client builds requests with `FlatBufferBuilder` and doesn't need
to read them back.

Edge cases:
- Malformed flatbuffer → `valid()` returns false; the caller treats it
  as a transport error (maps to `PxReplicaError::Internal`).
- Missing optional field → accessor returns `None` / default; the
  caller handles the missing-field case.

---

## 12. Port Allocation

The crowdb-rpc port is derived from the base port via a fixed offset:

```
KV_SERVER_RPC_BASE = 28001  (rpc port base for KV server)
KV_RPC_BASE         = 28101  (crowdb-rpc port base for KV server)
RPC_PORT_OFFSET     = KV_RPC_BASE - KV_SERVER_RPC_BASE = 100
```

When a `PxKvStore` binds on port P, the crowdb-rpc server listens
on port `P + 100`. The client transport's `conn_for(endpoint)` parses
the server endpoint and connects to `rpc_port + 100`.

For ephemeral ports (port 0 in tests), the server binds first,
then `start_rpc_server` reads the actual bound port from
`server_state.listen_addr` and derives the crowdb-rpc port.

Port constants are in `lib/crowdb-protocol/src/ports.rs` and the
`KvServerPort` enum.

---

## 13. Flatbuffer `bytes` Field Mapping — `bytes::Bytes`

By default, `flatc` maps flatbuffer `[ubyte]` fields to `Vec<u8>`,
making every clone an O(n) heap allocate + memcpy. For hot-path KV
fields that are cloned across retry loops or fanout, this is
avoidable.

`lib/crowdb-kv/build.rs` configures selected
flatbuffer `[ubyte]` fields to map to `bytes::Bytes` instead, turning
clones into O(1) atomic ref-count bumps:

- `AcceptedValue.payload` — Paxos Accept-fanout payload cloned across
  N peers.
- `KvSetRequest.key`, `KvSetRequest.value` — single-key write path.
- `KvGetRequest.key` — single-key read path.
- `KvDeleteRequest.key` — single-key delete path.
- `KvBatchItem.key`, `KvBatchItem.value` — batch write path; the
  client retry loop clones the entire `items` vec per attempt.
- `KvResponse.value` — read response.
- `KvScanRequest.prefix`, `KvScanRequest.start_after` — scan request.
- `KvScanItem.key`, `KvScanItem.value` — scan response items.

The client `BatchOp` type (`lib/crowdb-kv-client/src/client.rs`) also uses
`Bytes` for `key` and `value`, so `BatchOp` → `KvBatchItem` conversion
and the per-retry `items.clone()` are both O(1) per item.

The server-side `encode_kv_batch_items` (`px_kv_store.rs`) still
flattens `KvBatchItem` fields into a `Vec<u8>` consensus payload.
That copy remains (eliminating it requires changing the consensus
payload contract, out of scope).

---

## References

- `design-crowdb-kv-leader-election.md` §6 — heartbeat/lease interaction with
  stream ordering
- `design-crowdb-kv-slot.md` §5 — pipelined fanout and per-peer flow control
- `design-crowdb-kv-observability.md` — write-path phase metrics
  (`paxos.propose.e2e.l`, `paxos.classic.prepare.l`,
  `paxos.accept.quorum_rpc.l`, `paxos.learn.apply.l`)
- `design-crowdb-rpc.md` §6 — Flatbuffer Wrapper Convention
- `design-crowdb-rpc-diskdb-migration.md` — R115's proven migration pattern
