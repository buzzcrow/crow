# CrowKV - Design: RPC Wire Protocol

Depends on: [`requirement.md`](../requirement.md) §3, §9.2, §10, [`design.md`](../design.md) §2, §3, [`plan.md`](../plan.md) §1 M2
Satisfies: [requirement.md §3](../requirement.md#3-dependencies-and-assumptions), [requirement.md §9.2](../requirement.md#92-rolling-upgrade), [requirement.md §10.1](../requirement.md#101-client-discovery)

This document defines the wire-serialization contract for all node-to-node and client-to-node RPC communication. The implementation uses **gRPC with protobuf** (tonic + prost). Every message carries a `version: u32` field at fixed protobuf tag 1 for forward/backward compatibility; no `required` fields; field numbers are append-only.

## Table of Contents

- [1. Design Principles](#1-design-principles)
- [2. Classic Paxos Messages (P1 M2 subset)](#2-classic-paxos-messages-p1-m2-subset)
  - [2.1 Prepare](#21-prepare)
  - [2.2 Promise](#22-promise)
  - [2.3 Accept](#23-accept)
  - [2.4 Accepted](#24-accepted)
- [3. PeerStream Bidirectional Stream (P1 M3)](#3-peerstream-bidirectional-stream-p1-m3)
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

1. **One bidirectional stream per peer pair.** All steady-state node-to-node traffic (`Accept`, `Heartbeat`, `Chosen`) multiplexes over a single gRPC bidi stream keyed by `(group_id, peer_node_id)`. One-shot messages (`Prepare`, `PreVote`, `RequestVote`, `StepDown`) remain unary RPCs. This reduces connection count for the hot path while keeping election messages unblocked.
2. **Frame multiplexing.** Each `PeerStreamRequest` / `PeerStreamResponse` is a protobuf `oneof` frame; the concrete message type (`AcceptRequest`, `HeartbeatRequest`, etc.) carries its own `group_id`, `version`, and correlation fields. New steady-state message types add new oneof arms without changing existing field numbers.
3. **No `required` fields.** All protobuf fields are `optional` or have sensible defaults. A missing `version` field defaults to `0` (meaning "pre-versioning, treat as earliest").
4. **Field numbers are append-only.** Once assigned, a field number is never reused for a different semantic meaning. This is a hard rule for rolling upgrades ([requirement.md §9.2](../requirement.md#92-rolling-upgrade)).
5. **Plaintext in P1/P4; TLS hooks reserved.** The transport layer is plaintext TCP loopback in tests and P1/P4 integration. TLS config slots exist in the service builder but are unimplemented ([requirement.md §11](../requirement.md#11-security)).

---

## 2. Classic Paxos Messages (P1 M2 subset)

These four messages are the **minimum viable wire surface** introduced in P1 M2. They are sufficient to run a full classic Paxos round across a real network boundary. Election messages (`Heartbeat`, `RequestVote`, `PreVote`, `StepDown`) and the `PeerStream` / `ChosenNotification` frames are added in P1 M3; snapshot and client RPCs land in P4. No existing field numbers are mutated.

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

## 3. PeerStream Bidirectional Stream (P1 M3)

The steady-state consensus traffic (`Accept`, `Heartbeat`, and `Chosen` notification) moves onto a **single gRPC bidi stream per `(group_id, peer_id)` pair**. One-shot messages (`Prepare`, `PreVote`, `RequestVote`, `StepDown`) remain unary RPCs.

### 3.1 Why a dedicated stream

Three problems the unary-per-RPC pattern cannot solve:

1. **Ordering hazard.** A heartbeat carries a lease grant ("I won't vote before T"). If that heartbeat reorders ahead of an earlier-sent `Accept` on the same peer, the follower could reject the `Accept` while already having promised not to vote. A single stream guarantees FIFO delivery.
2. **Connection churn.** Paying TCP + HTTP/2 setup cost once per leadership tenure, rather than per-RPC, amortizes overhead under high write throughput.
3. **Per-peer backpressure.** A bounded `mpsc` on the stream gives the proposer an explicit signal (`Busy`) when a peer cannot keep up.

### 3.2 Frame types

```protobuf
message PeerStreamRequest {
  oneof frame {
    AcceptRequest      accept    = 1;
    HeartbeatRequest   heartbeat = 2;
    ChosenNotification chosen    = 3;
  }
}

message PeerStreamResponse {
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
  rpc PeerStream(stream PeerStreamRequest) returns (stream PeerStreamResponse);
}
```

The unary `Accept` RPC is kept during the P1 M3 → M4 migration window. Once all `Accept` traffic flows through `PeerStream` (P1 M4), the unary handler may be deprecated (kept for one release for binary compatibility) and eventually removed.

### 4.2 Cluster Discovery — HTTP, not gRPC

> **Decision record (2026-07, see [`plan-client.md`](../plan-client.md) §6 Issue 3):** a gRPC `AdminService.DescribeCluster` RPC was sketched here but never implemented. **Rejected, not deferred** — cluster/topology discovery is served by `crowkv-server`'s existing HTTP management API (`GET /topology`, `crowkv-server/src/mgmt_api.rs::export_topology`), which every client (gRPC-only or not) is expected to poll for `(store_id, group_id) -> leader_endpoint` discovery. See [requirement.md §10.1](../requirement.md#101-client-discovery) and [requirement.md §7.1](../requirement.md#71-groups-and-cluster-topology). No `AdminService` gRPC service exists or is planned.

---

## 5. Rust Mapping

| Protobuf message | Generated Rust type (tonic-build) | Notes |
|---|---|---|
| `PrepareRequest` | `rpc::PrepareRequest` | Hand-coded struct with `#[derive(prost::Message)]` |
| `PromiseResponse` | `rpc::PromiseResponse` | Hand-coded struct |
| `AcceptRequest` | `rpc::AcceptRequest` | Hand-coded struct |
| `AcceptedResponse` | `rpc::AcceptedResponse` | Hand-coded struct |
| `PeerStreamRequest` | `rpc::PeerStreamRequest` | tonic-build from `build.rs` |
| `PeerStreamResponse` | `rpc::PeerStreamResponse` | tonic-build from `build.rs` |
| `ChosenNotification` | `rpc::ChosenNotification` | tonic-build from `build.rs` |

**Client-side transport:** `crowkv/src/cluster/peer_stream.rs` defines `PxPeerStream` — a thin handle that enqueues frames on a per-peer background task. The background task owns the actual gRPC bidi stream, reconnects on transport failure with capped exponential backoff, and correlates inbound responses to outbound oneshots via `request_id`.

**Server-side handler:** `crowkv/src/rpc/px_service.rs` implements `PxService::peer_stream`. Each inbound stream spawns one task. Frames are routed through the existing `ReplicaHandler` methods (`on_accept`, `on_heartbeat`, `on_chosen`) and matching replies are shipped back on the outbound half of the same stream.

---

## 6. Version Compatibility Rules

1. **Sender rule:** every message sets `version = 1` (the initial wire version).
2. **Receiver rule:** decode must accept any `version <= max_supported`. Unknown fields are ignored (protobuf default behavior).
3. **Upgrade rule:** new fields are added with new field numbers; old fields are never removed or renumbered.
4. **Field-number freeze per message:**
   - `PrepareRequest`: 1–8 frozen
   - `PromiseResponse`: 1–12 frozen
   - `AcceptRequest`: 1–11 frozen
   - `AcceptedResponse`: 1–11 frozen
   - `PeerStreamRequest` / `PeerStreamResponse` oneof arms: 1–3 frozen
   - P4 may add new oneof arms starting at the next free field number.

---

## 7. Flow Control and Parallelism

The `PeerStream` design directly enables the parallel-slot pipelining described in [`design-parallel-slots.md`](design-parallel-slots.md) §5:

- **Multiple in-flight `Accept` frames per peer.** The background task maintains a `PendingMap` (`HashMap<request_id, oneshot::Sender>`). Each `send_accept` call inserts a new oneshot and returns immediately; the caller does not block waiting for the peer's reply. This allows slot N+1's `Accept` to be sent before slot N's `Accepted` response arrives.
- **Bounded mpsc backpressure.** The user-facing `cmd_tx` is a bounded `tokio::sync::mpsc` whose capacity is `peer_stream_window_frames` (default 64, tunable via `PxElectionConfig`). When the queue is full, `dispatch` fails and the proposer surfaces `PxPaxosError::Busy` (already classified `FailRetryable` in [`design-paxos-error.md`](design-paxos-error.md)).
- **Reconnect safety.** On transport failure the background task fails all pending oneshots with `PxReplicaError::Internal("stream reset")`, then reconnects with capped exponential backoff (50 ms → 2 s). The proposer treats this as a retryable failure and re-sends the `Accept` after the reconnect.

---

## 8. Open Questions

- **Q1:** Should `AcceptedValue.payload` carry a `LogEntryKind` enum in P1 M2, or is it purely opaque bytes until P1 M4 introduces the learner?  
  **Resolved:** opaque bytes in M2; `kind` discrimination added in M3 (`NoOp` for bulk Phase-1) and fully wired in M4.
- **Q2:** Should `Promise` and `Accepted` carry `term` for leader-fencing, or is `ballot` sufficient in classic Paxos?  
  **Resolved:** `term` added in P1 M3 to all messages for the two-fence rule (see [`design-leader-election.md`](design-leader-election.md) §2.3).

---

## References

- [Protocol Buffers Language Guide](https://developers.google.com/protocol-buffers/docs/proto3)
- [Tonic gRPC framework](https://github.com/hyperium/tonic)
- [prost — Protocol Buffers implementation for the Rust Language](https://github.com/tokio-rs/prost)
- [`design-leader-election.md`](design-leader-election.md) §6 — heartbeat/lease interaction with stream ordering
- [`design-parallel-slots.md`](design-parallel-slots.md) §5 — pipelined fanout and per-peer flow control
- [`plan.md`](../plan.md) §1 P1 M3 — leader election + bidi stream milestone
- [`plan.md`](../plan.md) §1 P1 M4 — parallel proposer milestone
