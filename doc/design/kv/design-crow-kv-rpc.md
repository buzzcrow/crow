<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROW - Design: RPC Wire Protocol

Depends on: [`design-crow-kv.md`](design-crow-kv.md) §2, §3, §9.2, §10
Satisfies: [`design-crow-kv.md`](design-crow-kv.md) §3, §9.2, §10.1

This document defines the wire-serialization contract for all node-to-node and client-to-node RPC communication. The implementation uses **gRPC with protobuf** (tonic + prost). Every message carries a `version: u32` field at fixed protobuf tag 1 for forward/backward compatibility; no `required` fields; field numbers are append-only.

## Table of Contents

- [1. Design Principles](#1-design-principles)
- [2. Message Surface](#2-message-surface)
- [3. LearnerStream — Why a Dedicated Bidi Stream](#3-learnerstream--why-a-dedicated-bidi-stream)
- [4. Service Definitions](#4-service-definitions)
- [5. Version Compatibility Rules](#5-version-compatibility-rules)
- [6. Flow Control and Parallelism](#6-flow-control-and-parallelism)
  - [6.1 Quorum Short-Circuit](#61-quorum-short-circuit)
  - [6.2 RPC Deadline](#62-rpc-deadline)
- [7. Paxos Error Model](#7-paxos-error-model)
- [8. Open Questions](#8-open-questions)
- [9. Proto `bytes` Field Mapping — `bytes::Bytes`](#9-proto-bytes-field-mapping--bytesbytes)

---

## 1. Design Principles

1. **One bidirectional stream per peer pair.** Steady-state data traffic
   (`Accept`, `Chosen`) multiplexes over a single gRPC bidi stream
   (`LearnerStream`) keyed by `(group_id, peer_node_id)`. Steady-state
   `Heartbeat` traffic moves over a **dedicated unary gRPC channel**
   (separate TCP connection) so liveness messages are never blocked
   behind data on the `LearnerStream` (see §3). One-shot messages
   (`Prepare`, `PreVote`, `RequestVote`, `StepDown`) remain unary RPCs on
   the control channel.
2. **Frame multiplexing.** Each `LearnerStreamRequest` / `LearnerStreamResponse` is a protobuf `oneof` frame. New steady-state message types add new oneof arms without changing existing field numbers.
3. **No `required` fields.** All protobuf fields are `optional` or have sensible defaults. A missing `version` field defaults to `0` (meaning "pre-versioning, treat as earliest").
4. **Field numbers are append-only.** Once assigned, a field number is never reused for a different semantic meaning. This is a hard rule for rolling upgrades.
5. **Plaintext in P1/P4; TLS hooks reserved.** The transport layer is plaintext TCP loopback in tests and P1/P4 integration. TLS config slots exist in the service builder but are unimplemented.

---

## 2. Message Surface

The wire protocol defines four classic Paxos messages (P1 M2):
`Prepare`, `Promise`, `Accept`, `Accepted`, sufficient to run a full
classic Paxos round across a real network boundary. Election messages
(`Heartbeat`, `RequestVote`, `PreVote`, `StepDown`) and the
`LearnerStream` / `ChosenNotification` frames are added in P1 M3;
snapshot and client RPCs land in P4.

Key reusable sub-message: `AcceptedValue` carries `(slot, round,
leader_id, term, payload)`. Used in `Accept` requests and `Promise`
responses for classic Paxos value-recovery. The `payload` is opaque
bytes in P1 M2; `kind` discrimination (empty = `NoOp`, non-empty =
`Write`) is not a protobuf field. `ConfigChange` and
`DedupCheckpoint` kinds are designed but not yet implemented.

The full protobuf definitions are in `lib/crow-kv/src/rpc/proto/`; this
doc covers design decisions only.

---

## 3. LearnerStream — Why a Dedicated Bidi Stream

The steady-state **data** traffic (`Accept`, `ChosenNotification`,
`BatchChosenNotification`)
moves onto a single gRPC bidi stream per `(group_id, peer_id)` pair.
Two problems the unary-per-RPC pattern cannot solve for data:

1. **Connection churn.** Paying TCP + HTTP/2 setup cost once per
   leadership tenure, rather than per-RPC, amortizes overhead under
   high write throughput.
2. **Per-peer backpressure.** A bounded `mpsc` on the stream gives the
   proposer an explicit signal (`Busy`) when a peer cannot keep up.

**Heartbeats move to a separate unary channel.** Steady-state
`Heartbeat` traffic no longer flows through the `LearnerStream`. It
routes over a **dedicated gRPC `Channel`** (separate TCP connection)
via the existing unary `heartbeat` RPC, established lazily on first
heartbeat and reused for the peer's lifetime. The reason is an
**availability** issue: when the `LearnerStream`'s send-half loop
flushes frames FIFO via `out_tx.send(frame).await`, a heartbeat
admitted to the queue still sits behind every accept already queued.
With 16 KiB values, the cumulative wire-flush time of N accepts can
exceed the election timeout — the follower's election deadline fires,
a spurious election challenges the leader, and the leader loses
quorum. A separate connection gives heartbeats their own wire with no
data traffic, so they are never delayed by accept backpressure.

**Why the FIFO invariant can be relaxed.** The original design
justified multiplexing heartbeats with accepts on a single stream
with an ordering hazard: a heartbeat reordering ahead of an Accept
could cause the follower to reject the Accept while already having
promised not to vote. Code analysis shows this hazard does not hold:

- `handle_heartbeat` mutates `election_state` (`current_term`,
  `voted_for`, `leader_id`, `vote_lockout_until`).
  `on_accept_inner` checks the term fence then calls the acceptor's
  per-slot ballot CAS. The two operate on **independent state**.
- `vote_lockout_until` only gates `handle_request_vote` /
  `handle_pre_vote`, not `on_accept`. A heartbeat extending the
  lockout cannot cause an accept to be rejected.
- The only coupling is `current_term`, and the term fence
  (`req.term < local_term → TermStale`) handles all cross-term
  reordering correctly. A stale-term accept being rejected is
  correct behavior (the old leader lost leadership).
- Same-term reordering is harmless: heartbeat and accept mutate
  independent state.

The `term` **is** the epoch mechanism; no new timestamp or epoch
field is needed to make separate connections safe.

**What stays unary:** `Prepare` (one-shot Phase-1, no ordering need
with steady-state traffic), `PreVote` / `RequestVote` (election probe
/ real vote, must not be queued behind `Accept`s), `StepDown` (admin
primitive, must cut through immediately), `Heartbeat` (steady-state
liveness, must not be blocked behind data).

---

## 4. Service Definitions

`PxService` is the node-to-node gRPC service. It exposes:

- **Unary RPCs:** `Prepare`, `Accept` (classic Paxos), `PreVote`,
  `RequestVote`, `Heartbeat`, `StepDown` (leader election).
- **Bidi stream:** `LearnerStream` (steady-state `Accept` + `Chosen`
  multiplexed).

The unary `Accept` RPC is kept alongside `LearnerStream` for callers
that need a one-shot path. In practice, steady-state `Accept` traffic
flows through `LearnerStream`. The unary `Heartbeat` RPC is used for
steady-state heartbeats over a dedicated channel (§3). The
`LearnerStream` no longer carries heartbeat frames in steady state,
though the server-side bidi handler still accepts them for backward
compatibility during rolling upgrades.

**Cluster discovery — HTTP, not gRPC.** A gRPC `AdminService.DescribeCluster`
RPC was sketched but **rejected, not deferred**. Cluster/topology
discovery is served by `crow-kv-server`'s existing HTTP management API
(`GET /topology`), which every client polls for
`(store_id, group_id) -> leader_endpoint` discovery. No
`AdminService` gRPC service exists or is planned.

---

## 5. Version Compatibility Rules

1. **Sender rule:** every message sets `version = 1` (the initial wire version).
2. **Receiver rule:** decode must accept any `version <= max_supported`. Unknown fields are ignored (protobuf default behavior).
3. **Upgrade rule:** new fields are added with new field numbers; old fields are never removed or renumbered.
4. **Field-number freeze per message:** field numbers are frozen once
   assigned. The frozen ranges are documented in the proto file
   comments. Future versions may add new oneof arms starting at the
   next free field number.

---

## 6. Flow Control and Parallelism

The `LearnerStream` design directly enables the parallel-slot
pipelining described in `design-crow-kv-slot.md` §5:

- **Multiple in-flight `Accept` frames per peer.** The background task
  maintains a `PendingMap` keyed by `request_id`. Each `send_accept`
  call inserts a new oneshot and returns immediately; the caller does
  not block waiting for the peer's reply. This allows slot N+1's
  `Accept` to be sent before slot N's `Accepted` response arrives.
- **Bounded mpsc backpressure.** The user-facing `cmd_tx` is a bounded
  `tokio::sync::mpsc` (default 64 frames, tunable via
  `PxElectionConfig`). When full, `dispatch` fails and the proposer
  surfaces `PxPaxosError::Busy`.
- **Reconnect safety.** On transport failure the background task fails
  all pending oneshots, then reconnects with capped exponential
  backoff (50 ms → 2 s). The proposer treats this as retryable and
  re-sends the `Accept` after reconnect.

### 6.1 Quorum Short-Circuit

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

### 6.2 RPC Deadline

`send_accept` wraps its oneshot await with
`tokio::time::timeout(rpc_timeout)`. On expiry the caller removes its
pending-map entry (the recv half no-ops on a missing entry, so a late
reply is logged at `debug!` and dropped) and surfaces a typed
retryable error. `send_prepare` and `send_heartbeat` wrap the unary
gRPC call with the same timeout. A connected-yet-unresponsive peer (GC
pause, half-open socket, overloaded server) is surfaced as a
retryable failure within `learner_stream_rpc_timeout_ms` (default 2000
ms, aligned with the 2 s election max) rather than blocking the
fan-out indefinitely.

Belt-and-braces: h2 keepalive (`http2_keep_alive_interval` +
`keep_alive_while_idle`) is enabled on the `get_client`,
`get_heartbeat_client`, and `learner_stream` connect `Endpoint`s, so a
silent half-open connection is detected at the transport layer
independent of the application timeout.

---

## 7. Paxos Error Model

The error categories below determine whether the proposer retries the
same slot, runs classic repair, moves the client operation to a new
slot, redirects the client, or returns a retryable error.

### 7.1 Error Categories

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

### 7.2 Retry Rules

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

### 7.3 RPC Mapping

Unary Paxos responses carry rejected metadata directly. RPC transport
failures are mapped by the caller into `TransportFailure`. Malformed
Paxos RPCs map to `InternalInvariantViolation` at the Paxos model
level and to `invalid_argument` at the gRPC boundary.

---

## 8. Open Questions

- **Q1: `AcceptedValue.payload` kind discrimination** — Resolved:
  opaque bytes in M2; empty payload = `NoOp`, non-empty = `Write`.
  `ConfigChange` and `DedupCheckpoint` kinds are designed but not yet
  implemented.
- **Q2: Should `Promise` and `Accepted` carry `term` for leader
  fencing?** — Resolved: `term` added in P1 M3 to all messages for the
  two-fence rule (see `design-crow-kv-leader-election.md` §2.3).
- **Q3: Should `Prepare`/`Accept` carry `membership_epoch`?** —
  Resolved: Added in P5 M2. Responses echo `membership_epoch` and set
  `epoch_mismatch` when the proposer's epoch doesn't match the
  responder's. The proposer adopts the responder's epoch and retries
  without bumping its ballot.

---

## 9. Proto `bytes` Field Mapping — `bytes::Bytes`

By default, `prost-build` maps protobuf `bytes` fields to `Vec<u8>`,
making every clone an O(n) heap allocate + memcpy. For hot-path KV
fields that are cloned across retry loops or fanout, this is
avoidable.

`lib/crow-kv/build.rs` uses `prost_build::Configure::bytes([...])` to map
selected `bytes` proto fields to `bytes::Bytes` instead, turning
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

The client `BatchOp` type (`lib/crow-kv-client/src/client.rs`) also uses
`Bytes` for `key` and `value`, so `BatchOp` → `KvBatchItem` conversion
and the per-retry `items.clone()` are both O(1) per item.

The server-side `encode_kv_batch_items` (`px_kv_store.rs`) still
flattens `KvBatchItem` fields into a `Vec<u8>` consensus payload.
That copy remains (eliminating it requires changing the consensus
payload contract, out of scope).

**Alternatives considered**:

- **Borrowed slices in client API** (`&[(&[u8], Option<&[u8]>)]`):
  eliminates copies but changes the public API ergonomics and
  complicates retry (borrowed data must outlive the retry loop).
  `Bytes` achieves the same zero-copy-with-ergonomics via ref-counting.
- **Server-side payload encoding elimination**: skip
  `encode_kv_batch_items` and pass `KvBatchItem` directly as the Paxos
  payload. Requires changing `Batch::decode` to match the protobuf
  wire format. High complexity, ripples through consensus + WAL +
  replay. Separate requirement.

---

## References

- `design-crow-kv-leader-election.md` §6 — heartbeat/lease interaction with
  stream ordering
- `design-crow-kv-slot.md` §5 — pipelined fanout and per-peer flow control
- `design-crow-kv-observability.md` — write-path phase metrics
  (`write.propose_e2e.l`, `write.prepare_phase.l`, `write.accept_phase.l`,
  `write.accept_quorum_rpc.l`, `write.engine_apply.l`)
