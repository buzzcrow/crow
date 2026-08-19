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
transport changes. Existing `.proto` schemas are either converted to
`.fbs` flatbuffer schemas or bridged via prost-to-flatbuffer (see Open
Questions in R104).

**One-line summary**: Replace gRPC on the KV internal replica-to-
replica path with the R104 flatbuffer RPC library, preserving protocol
semantics and recovering the h2-lock throughput loss.

**Numbered work items**:

1. **Flatbuffer schemas for consensus messages**
   (`lib/crow-protocol/src/proto/kv_rpc.fbs`) — convert the existing
   `kv_rpc.proto` service definitions (PxService, LearnerStream,
   NotLeaderHint) to flatbuffer `.ffs` schemas. Message types: accept
   request/response, prepare request/response, LearnerStream
   request/response, NotLeaderHint. Register message type IDs in
   R104's `msg_type` enum (consensus range). If the prost-to-flatbuffer
   bridge is chosen (R104 Open Question), this item is replaced by a
   bridge layer instead.

2. **Server-side migration** (`crow-kv-server/src/rpc/`) — replace the
   tonic `PxService` server with an R104 `RpcServer` handler. The
   handler dispatches by message type to the existing consensus logic
   (`ConsensusHandler`, `LearnerStreamHandler`). The response is a
   flatbuffer frame. The server runs alongside the existing Axum HTTP
   management server (which stays on HTTP). The gRPC server is removed
   from the internal path but may remain temporarily for the
   client-facing surface until separately migrated.

3. **Client-side migration** (`crow-kv/src/rpc/`) — replace the tonic
   `PxServiceClient` with an R104 `RpcClient`. The connection pool
   manages connections to peer replicas. `NotLeaderHint` is parsed
   from the flatbuffer response and fed into the existing retry logic
   (`crow-kv-client` retry + topology cache). The `LearnerStream`
   client becomes a long-lived R104 connection with sequential
   request-response frames.

4. **Error model parity** (`crow-kv/src/rpc/error.rs`) — map R104
   transport errors (`ConnectionError`, `TimeoutError`,
   `BackpressureError`) to the existing KV error variants
   (`NotLeaderHint`, `Unavailable`, `Timeout`). The client retry logic
   must treat R104 errors the same as the equivalent gRPC status codes.
   `NotLeaderHint` is a protocol-level error (carried in the response
   body), not a transport error — unchanged.

5. **Benchmark + regression** (`tools/bench-rpc.sh`) — a benchmark
   script that runs the 2T:1C read bench against both the gRPC path
   (baseline) and the R104 path. Verifies the ~17% loss is recovered.
   Also runs 1T:1C to verify no regression at the uncontended point.
   Added to the regression sentinel suite alongside
   `bench-write-regression.sh`.

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

- **Depends on**: **R104** (flatbuffer RPC engine library) — uses
  `crow-rpc` crate for framing, connection, pool, schedule.
- **Depended on by**: nothing (terminal migration item). R53
  (heartbeat channel) is separate and independent.

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
  (`tools/bench-rpc.sh`, Linux only).
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

**Test commands**: `pixi run cargo test -p crow-kv --test rpc_migration`,
`pixi run bench-rpc.sh` (Linux), `pixi run cargo fmt --all -- --check`,
`pixi run cargo clippy --all-targets -- -D warnings`.

**Open Questions**

- **Schema conversion scope**: Do we convert all existing KV `.proto`
  schemas to `.fbs` (full flatbuffer migration), or bridge prost
  messages to flatbuffer at the R104 boundary (keep `.proto` as the
  schema source, serialize to flatbuffer at the wire layer)? Full
  conversion is cleaner but is a large diff touching every RPC-using
  crate. The bridge approach is smaller but adds a conversion step.
  This is shared with R104's Open Question on flatbuffer vs protobuf.
