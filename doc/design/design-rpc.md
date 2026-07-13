# CrowKV - Design: RPC Wire Protocol

Depends on: [`requirement.md`](../requirement.md) §3, §9.2, §10, [`design.md`](../design.md) §2, §3
Satisfies: [requirement.md §3](../requirement.md#3-dependencies-and-assumptions), [requirement.md §9.2](../requirement.md#92-rolling-upgrade), [requirement.md §10.1](../requirement.md#101-client-discovery)

This document defines the wire-serialization contract for all node-to-node and client-to-node RPC communication. The implementation uses **gRPC with protobuf** (tonic + prost). Every message carries a `version: u32` field at fixed protobuf tag 1 for forward/backward compatibility; no `required` fields; field numbers are append-only.

## Table of Contents

- [1. Design Principles](#1-design-principles)
- [2. Classic Paxos Messages (P1 M2 subset)](#2-classic-paxos-messages-p1-m2-subset)
  - [2.1 Prepare](#21-prepare)
  - [2.2 Promise](#22-promise)
  - [2.3 Accept](#23-accept)
  - [2.4 Accepted](#24-accepted)
- [3. LearnerStream Bidirectional Stream (P1 M3)](#3-learnerstream-bidirectional-stream-p1-m3)
  - [3.1 Why a dedicated stream](#31-why-a-dedicated-stream)
  - [3.2 Frame types](#32-frame-types)
  - [3.3 What stays unary](#33-what-stays-unary)
- [4. Service Definitions](#4-service-definitions)
  - [4.1 PxService (node-to-node)](#41-pxservice-node-to-node)
  - [4.2 Cluster Discovery — HTTP, not gRPC](#42-cluster-discovery--http-not-grpc)
- [5. Rust Mapping](#5-rust-mapping)
- [6. Version Compatibility Rules](#6-version-compatibility-rules)
- [7. Flow Control and Parallelism](#7-flow-control-and-parallelism)
- [8. Open Questions](#8-open-questions)

---

## 1. Design Principles

1. **One bidirectional stream per peer pair.** All steady-state node-to-node traffic (`Accept`, `Heartbeat`, `Chosen`) multiplexes over a single gRPC bidi stream (`LearnerStream`) keyed by `(group_id, peer_node_id)`. One-shot messages (`Prepare`, `PreVote`, `RequestVote`, `StepDown`) remain unary RPCs. This reduces connection count for the hot path while keeping election messages unblocked.
2. **Frame multiplexing.** Each `LearnerStreamRequest` / `LearnerStreamResponse` is a protobuf `oneof` frame; the concrete message type (`AcceptRequest`, `HeartbeatRequest`, etc.) carries its own `group_id`, `version`, and correlation fields. New steady-state message types add new oneof arms without changing existing field numbers.
3. **No `required` fields.** All protobuf fields are `optional` or have sensible defaults. A missing `version` field defaults to `0` (meaning "pre-versioning, treat as earliest").
4. **Field numbers are append-only.** Once assigned, a field number is never reused for a different semantic meaning. This is a hard rule for rolling upgrades ([requirement.md §9.2](../requirement.md#92-rolling-upgrade)).
5. **Plaintext in P1/P4; TLS hooks reserved.** The transport layer is plaintext TCP loopback in tests and P1/P4 integration. TLS config slots exist in the service builder but are unimplemented ([requirement.md §11](../requirement.md#11-security)).

---

## 2. Classic Paxos Messages (P1 M2 subset)

These four messages are the **minimum viable wire surface** introduced in P1 M2. They are sufficient to run a full classic Paxos round across a real network boundary. Election messages (`Heartbeat`, `RequestVote`, `PreVote`, `StepDown`) and the `LearnerStream` / `ChosenNotification` frames are added in P1 M3; snapshot and client RPCs land in P4. No existing field numbers are mutated.

### 2.1 Prepare

Phase-1 request sent by the leader (proposer) to all acceptors.

```protobuf
message PrepareRequest {
  uint32 version = 1;   // wire-format version, always present
  uint64 slot    = 2;   // PxSlot index being prepared
  uint64 round   = 3;   // ballot.round
  uint64 leader_id = 4; // ballot.leader_id (PxNodeId)
}
```

### 2.2 Promise

Phase-1 response returned by each acceptor.

```protobuf
message PromiseResponse {
  uint32 version = 1;
  uint64 slot    = 2;
  uint64 round   = 3;
  uint64 leader_id = 4;   // ballot that was promised

  // If the acceptor had previously accepted a value at this slot,
  // it must return it so the leader can adopt it (classic Paxos value-recovery).
  // If absent, the slot was empty.
  optional AcceptedValue previously_accepted = 5;

  bool rejected = 6;      // true if the acceptor already promised a higher ballot
  uint64 rejected_round   = 7;   // valid only if rejected == true
  uint64 rejected_leader_id = 8; // valid only if rejected == true
}
```

`AcceptedValue` is a reusable sub-message (also used in `Accept` / `Accepted`):

```protobuf
message AcceptedValue {
  uint64 slot  = 1;
  uint64 round = 2;       // ballot.round at time of acceptance
  uint64 leader_id = 3; // ballot.leader_id at time of acceptance
  uint64 term  = 4;       // PxTerm of the accepting leader (for fencing)

  // The opaque payload. For P1 M2 this is an arbitrary byte blob;
  // P1 M4 introduces LogEntryKind discrimination.
  bytes payload = 5;
}
```

### 2.3 Accept

Phase-2 request sent by the leader after receiving a quorum of promises.

```protobuf
message AcceptRequest {
  uint32 version = 1;
  uint64 slot    = 2;
  uint64 round   = 3;
  uint64 leader_id = 4;
  uint64 term    = 5;     // leader's current term (for fencing)
  AcceptedValue value = 6;
}
```

### 2.4 Accepted

Phase-2 response returned by each acceptor.

```protobuf
message AcceptedResponse {
  uint32 version = 1;
  uint64 slot    = 2;
  uint64 round   = 3;     // ballot.round that was accepted
  uint64 leader_id = 4; // ballot.leader_id that was accepted

  bool rejected = 5;      // true if the acceptor promised a higher ballot
  uint64 rejected_round   = 6;   // valid only if rejected == true
  uint64 rejected_leader_id = 7; // valid only if rejected == true
}
```

---

## 3. LearnerStream Bidirectional Stream (P1 M3)

The steady-state consensus traffic (`Accept`, `Heartbeat`, and `Chosen` notification) moves onto a **single gRPC bidi stream per `(group_id, peer_id)` pair** called `LearnerStream`. One-shot messages (`Prepare`, `PreVote`, `RequestVote`, `StepDown`) remain unary RPCs.

### 3.1 Why a dedicated stream

Three problems the unary-per-RPC pattern cannot solve:

1. **Ordering hazard.** A heartbeat carries a lease grant ("I won't vote before T"). If that heartbeat reorders ahead of an earlier-sent `Accept` on the same peer, the follower could reject the `Accept` while already having promised not to vote. A single stream guarantees FIFO delivery.
2. **Connection churn.** Paying TCP + HTTP/2 setup cost once per leadership tenure, rather than per-RPC, amortizes overhead under high write throughput.
3. **Per-peer backpressure.** A bounded `mpsc` on the stream gives the proposer an explicit signal (`Busy`) when a peer cannot keep up.

### 3.2 Frame types

```protobuf
message LearnerStreamRequest {
  oneof frame {
    AcceptRequest      accept    = 1;
    HeartbeatRequest   heartbeat = 2;
    ChosenNotification chosen    = 3;
  }
}

message LearnerStreamResponse {
  oneof frame {
    AcceptedResponse  accepted  = 1;
    HeartbeatResponse heartbeat = 2;
  }
}
```

`AcceptRequest` / `AcceptedResponse` and `HeartbeatRequest` / `HeartbeatResponse` reuse the same message shapes as the unary RPCs (§2.3 / §2.4 and the election messages in `design-leader-election.md`). `ChosenNotification` is a new fire-and-forget frame (no response arm) used by the learner to tell peers that a slot has reached quorum.

### 3.3 What stays unary

| Message | RPC style | Reason |
|---|---|---|
| `Prepare` | unary | One-shot Phase-1; no ordering need with steady-state traffic |
| `PreVote` | unary | Election probe; must not be queued behind a stream of `Accept`s |
| `RequestVote` | unary | Real vote; same one-shot property |
| `StepDown` | unary | Admin primitive; must cut through immediately |

---

## 4. Service Definitions

### 4.1 PxService (node-to-node)

```protobuf
service PxService {
  // Classic Paxos (unary)
  rpc Prepare(PrepareRequest) returns (PromiseResponse);
  rpc Accept(AcceptRequest) returns (AcceptedResponse);

  // Leader election (unary)
  rpc PreVote(PreVoteRequest) returns (PreVoteResponse);
  rpc RequestVote(RequestVoteRequest) returns (RequestVoteResponse);
  rpc Heartbeat(HeartbeatRequest) returns (HeartbeatResponse);
  rpc StepDown(StepDownRequest) returns (StepDownResponse);

  // Steady-state bidi stream (P1 M3)
  rpc LearnerStream(stream LearnerStreamRequest) returns (stream LearnerStreamResponse);

  // Snapshot install (P5 M2)
  // (defined in SnapshotService, not PxService)
}
```

The unary `Accept` RPC is kept alongside `LearnerStream` for callers that need a one-shot path. In practice, steady-state `Accept` traffic flows through `LearnerStream`.

### 4.2 Cluster Discovery — HTTP, not gRPC

> **Decision record (2026-07):** a gRPC `AdminService.DescribeCluster` RPC was sketched here but never implemented. **Rejected, not deferred** — cluster/topology discovery is served by `crowkv-server`'s existing HTTP management API (`GET /topology`, `crowkv-server/src/mgmt_api.rs::export_topology`), which every client (gRPC-only or not) is expected to poll for `(store_id, group_id) -> leader_endpoint` discovery. See [requirement.md §10.1](../requirement.md#101-client-discovery) and [requirement.md §7.1](../requirement.md#71-groups-and-cluster-topology). No `AdminService` gRPC service exists or is planned.

---

## 5. Rust Mapping

| Protobuf message | Generated Rust type (tonic-build) | Notes |
|---|---|---|
| `PrepareRequest` | `rpc::PrepareRequest` | Hand-coded struct with `#[derive(prost::Message)]` |
| `PromiseResponse` | `rpc::PromiseResponse` | Hand-coded struct |
| `AcceptRequest` | `rpc::AcceptRequest` | Hand-coded struct |
| `AcceptedResponse` | `rpc::AcceptedResponse` | Hand-coded struct |
| `LearnerStreamRequest` | `rpc::LearnerStreamRequest` | tonic-build from `build.rs` |
| `LearnerStreamResponse` | `rpc::LearnerStreamResponse` | tonic-build from `build.rs` |
| `ChosenNotification` | `rpc::ChosenNotification` | tonic-build from `build.rs` |

**Client-side transport:** `crowkv/src/cluster/learner_stream.rs` defines `PxLearnerStream` — a thin handle that enqueues frames on a per-peer background task. The background task owns the actual gRPC bidi stream, reconnects on transport failure with capped exponential backoff, and correlates inbound responses to outbound oneshots via `request_id`.

**Server-side handler:** `crowkv/src/rpc/px_service.rs` implements `PxService::learner_stream`. Each inbound stream spawns one task. Frames are routed through the existing `ReplicaHandler` methods (`on_accept`, `on_heartbeat`, `on_chosen`) and matching replies are shipped back on the outbound half of the same stream.

---

## 6. Version Compatibility Rules

1. **Sender rule:** every message sets `version = 1` (the initial wire version).
2. **Receiver rule:** decode must accept any `version <= max_supported`. Unknown fields are ignored (protobuf default behavior).
3. **Upgrade rule:** new fields are added with new field numbers; old fields are never removed or renumbered.
4. **Field-number freeze per message:**
   - `PrepareRequest`: 1–9 frozen (fields 5–9: `request_id`, `request_create_ms`, `group_id`, `term`, `membership_epoch`)
   - `PromiseResponse`: 1–14 frozen (fields 9–14: `request_id`, `request_create_ms`, `term`, `term_stale`, `membership_epoch`, `epoch_mismatch`)
   - `AcceptRequest`: 1–12 frozen (fields 7–12: `request_id`, `request_create_ms`, `client_id`, `seq`, `group_id`, `membership_epoch`)
   - `AcceptedResponse`: 1–13 frozen (fields 8–13: `request_id`, `request_create_ms`, `term`, `term_stale`, `membership_epoch`, `epoch_mismatch`)
   - `LearnerStreamRequest` / `LearnerStreamResponse` oneof arms: 1–3 frozen
   - Future versions may add new oneof arms starting at the next free field number.

---

## 7. Flow Control and Parallelism

The `LearnerStream` design directly enables the parallel-slot pipelining described in [`design-slot.md`](design-slot.md) §5:

- **Multiple in-flight `Accept` frames per peer.** The background task maintains a `PendingMap` (`HashMap<request_id, oneshot::Sender>`). Each `send_accept` call inserts a new oneshot and returns immediately; the caller does not block waiting for the peer's reply. This allows slot N+1's `Accept` to be sent before slot N's `Accepted` response arrives.
- **Bounded mpsc backpressure.** The user-facing `cmd_tx` is a bounded `tokio::sync::mpsc` whose capacity is `learner_stream_window_frames` (default 64, tunable via `PxElectionConfig`). When the queue is full, `dispatch` fails and the proposer surfaces `PxPaxosError::Busy` (already classified `FailRetryable` in §8 below).
- **Reconnect safety.** On transport failure the background task fails all pending oneshots with `PxReplicaError::Internal("stream reset")`, then reconnects with capped exponential backoff (50 ms → 2 s). The proposer treats this as a retryable failure and re-sends the `Accept` after the reconnect.

---

## 8. Paxos Error Model

> **Merged from `design-paxos-error.md` (2026-07).** The error categories
> below determine whether the proposer retries the same slot, runs classic
> repair, moves the client operation to a new slot, redirects the client,
> or returns a retryable error.

### 8.1 Error Categories

| Error | Source | Retry behavior | Client meaning |
|---|---|---|---|
| `NotLeader` | KV request reaches follower | Client retries leader hint. | Definite failure on this node; this node did not propose the request. |
| `PrepareRejected` | Phase 1 quorum is blocked by higher promised ballot | Retry same slot with a higher ballot. | Internal recoverable contention. |
| `AcceptRejected` | Phase 2 quorum is blocked by higher promised ballot | Run classic Phase 1 repair on the same slot with a higher ballot. | Internal recoverable contention. |
| `ForeignValueChosen` | Phase 1 adopts another value or chosen value differs from client value | Learn the chosen value, then retry the client operation on a new slot. | Internal safe slot conflict recovery. |
| `QuorumUnavailable` | Not enough reachable voting peers | Retry same slot until retry budget is exhausted. | Retryable failure with unknown outcome if any accept might have reached a peer. |
| `TransportFailure` | RPC timeout, connect failure, or unavailable peer | Retry same slot; repeated transport failures become `QuorumUnavailable`. | Unknown outcome unless the request definitely was not sent. |
| `Busy` | Admission/retry budget exhausted | Client retries with backoff. | Request was not completed in the current attempt. |
| `InternalInvariantViolation` | Missing required value, invalid state transition, impossible branch | Fail test fast; production maps to internal error. | Bug, not a normal retry path. |

### 8.2 Retry Rules

**Prepare Rejection.** A prepare rejection means at least one acceptor has
promised a higher ballot. The proposer must not move to a new slot. It
retries Phase 1 for the same slot using a ballot above the highest observed
rejected ballot.

**Accept Rejection.** An accept rejection means the current fast-path or
Phase 2 ballot is stale. The proposer must run classic Phase 1 for the same
slot. If Phase 1 later discovers a foreign accepted value, that value is
repaired and learned before the client operation moves to a new slot.

**Transport or Quorum Failure.** A transport failure without a higher
promised ballot does not prove Paxos contention. The proposer retries the
same slot with the same ballot until the retry budget is exhausted.
Increasing the ballot is only required after a higher-ballot rejection.

**Foreign Value.** A foreign value discovered through Phase 1b may already
be chosen or may be required for safety. The proposer must adopt it for the
current slot. If it is not the client value, the client operation is retried
on a later slot only after the foreign value is learned.

### 8.3 RPC Mapping

Unary Paxos responses continue to carry rejected metadata directly. RPC
transport failures are mapped by the caller into `TransportFailure`.
Malformed Paxos RPCs, such as an `AcceptRequest` without a value, map to
`InternalInvariantViolation` at the Paxos model level and to
`invalid_argument` at the gRPC boundary.

### 8.4 Test Requirements

A dedicated `crowkv/tests/paxos_error.rs` file owns error behavior tests. It
should cover:

1. prepare rejection records the higher ballot and requires same-slot retry;
2. accept rejection triggers classic repair for the same slot;
3. foreign value adoption retries the client operation on a later slot;
4. quorum unavailable produces retryable failure without abandoning the slot as chosen;
5. follower KV requests return `NotLeader` with a leader hint;
6. malformed accept RPC is rejected by the gRPC service.

---

## 9. Open Questions

- **Q1:** Should `AcceptedValue.payload` carry a `LogEntryKind` enum in P1 M2, or is it purely opaque bytes until P1 M4 introduces the learner?  
  **Resolved:** opaque bytes in M2; `kind` discrimination is not a protobuf field — empty payload = `NoOp`, non-empty = `Write`. `ConfigChange` and `DedupCheckpoint` kinds are designed but not yet implemented.
- **Q2:** Should `Promise` and `Accepted` carry `term` for leader-fencing, or is `ballot` sufficient in classic Paxos?  
  **Resolved:** `term` added in P1 M3 to all messages for the two-fence rule (see [`design-leader-election.md`](design-leader-election.md) §2.3).
- **Q3:** Should `Prepare`/`Accept` carry `membership_epoch` for membership-fencing?  
  **Resolved:** Added in P5 M2. `PrepareRequest.membership_epoch` (field 9), `AcceptRequest.membership_epoch` (field 12). Responses echo `membership_epoch` and set `epoch_mismatch` when the proposer's epoch doesn't exactly match the responder's. The proposer adopts the responder's epoch and retries without bumping its ballot.

---

## References

- [Protocol Buffers Language Guide](https://developers.google.com/protocol-buffers/docs/proto3)
- [Tonic gRPC framework](https://github.com/hyperium/tonic)
- [prost — Protocol Buffers implementation for the Rust Language](https://github.com/tokio-rs/prost)
- [`design-leader-election.md`](design-leader-election.md) §6 — heartbeat/lease interaction with stream ordering
- [`design-slot.md`](design-slot.md) §5 — pipelined fanout and per-peer flow control
- §8 — Paxos error model (merged from former `design-paxos-error.md`)
