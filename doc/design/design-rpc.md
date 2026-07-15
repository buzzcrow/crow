<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CrowKV - Design: RPC Wire Protocol

Depends on: [`design.md`](design.md) §3, §9.2, §10, [`design.md`](design.md) §2, §3
Satisfies: design.md §3](design.md), design.md §9.2](design.md), design.md §10.1](design.md)

This document defines the wire-serialization contract for all node-to-node and client-to-node RPC communication. The implementation uses **gRPC with protobuf** (tonic + prost). Every message carries a `version: u32` field at fixed protobuf tag 1 for forward/backward compatibility; no `required` fields; field numbers are append-only.

## Table of Contents

- [1. Design Principles](#1-design-principles)
- [2. Message Surface](#2-message-surface)
- [3. LearnerStream — Why a Dedicated Bidi Stream](#3-learnerstream--why-a-dedicated-bidi-stream)
- [4. Service Definitions](#4-service-definitions)
- [5. Version Compatibility Rules](#5-version-compatibility-rules)
- [6. Flow Control and Parallelism](#6-flow-control-and-parallelism)
- [7. Paxos Error Model](#7-paxos-error-model)
- [8. Open Questions](#8-open-questions)

---

## 1. Design Principles

1. **One bidirectional stream per peer pair.** All steady-state node-to-node traffic (`Accept`, `Heartbeat`, `Chosen`) multiplexes over a single gRPC bidi stream (`LearnerStream`) keyed by `(group_id, peer_node_id)`. One-shot messages (`Prepare`, `PreVote`, `RequestVote`, `StepDown`) remain unary RPCs. This reduces connection count for the hot path while keeping election messages unblocked.
2. **Frame multiplexing.** Each `LearnerStreamRequest` / `LearnerStreamResponse` is a protobuf `oneof` frame. New steady-state message types add new oneof arms without changing existing field numbers.
3. **No `required` fields.** All protobuf fields are `optional` or have sensible defaults. A missing `version` field defaults to `0` (meaning "pre-versioning, treat as earliest").
4. **Field numbers are append-only.** Once assigned, a field number is never reused for a different semantic meaning. This is a hard rule for rolling upgrades.
5. **Plaintext in P1/P4; TLS hooks reserved.** The transport layer is plaintext TCP loopback in tests and P1/P4 integration. TLS config slots exist in the service builder but are unimplemented.

---

## 2. Message Surface

The wire protocol defines four classic Paxos messages (P1 M2):
`Prepare`, `Promise`, `Accept`, `Accepted` — sufficient to run a full
classic Paxos round across a real network boundary. Election messages
(`Heartbeat`, `RequestVote`, `PreVote`, `StepDown`) and the
`LearnerStream` / `ChosenNotification` frames are added in P1 M3;
snapshot and client RPCs land in P4.

Key reusable sub-message: `AcceptedValue` — carries `(slot, round,
leader_id, term, payload)`. Used in `Accept` requests and `Promise`
responses for classic Paxos value-recovery. The `payload` is opaque
bytes in P1 M2; `kind` discrimination (empty = `NoOp`, non-empty =
`Write`) is not a protobuf field — `ConfigChange` and
`DedupCheckpoint` kinds are designed but not yet implemented.

The full protobuf definitions are in `crowkv/src/rpc/proto/`; this
doc covers design decisions only.

---

## 3. LearnerStream — Why a Dedicated Bidi Stream

The steady-state consensus traffic moves onto a single gRPC bidi
stream per `(group_id, peer_id)` pair. Three problems the
unary-per-RPC pattern cannot solve:

1. **Ordering hazard.** A heartbeat carries a lease grant ("I won't
   vote before T"). If that heartbeat reorders ahead of an
   earlier-sent `Accept` on the same peer, the follower could reject
   the `Accept` while already having promised not to vote. A single
   stream guarantees FIFO delivery.
2. **Connection churn.** Paying TCP + HTTP/2 setup cost once per
   leadership tenure, rather than per-RPC, amortizes overhead under
   high write throughput.
3. **Per-peer backpressure.** A bounded `mpsc` on the stream gives the
   proposer an explicit signal (`Busy`) when a peer cannot keep up.

**What stays unary:** `Prepare` (one-shot Phase-1, no ordering need
with steady-state traffic), `PreVote` / `RequestVote` (election probe
/ real vote — must not be queued behind `Accept`s), `StepDown` (admin
primitive — must cut through immediately).

---

## 4. Service Definitions

`PxService` is the node-to-node gRPC service. It exposes:

- **Unary RPCs:** `Prepare`, `Accept` (classic Paxos), `PreVote`,
  `RequestVote`, `Heartbeat`, `StepDown` (leader election).
- **Bidi stream:** `LearnerStream` (steady-state `Accept` + `Heartbeat`
  + `Chosen` multiplexed).

The unary `Accept` RPC is kept alongside `LearnerStream` for callers
that need a one-shot path. In practice, steady-state `Accept` traffic
flows through `LearnerStream`.

**Cluster discovery — HTTP, not gRPC.** A gRPC `AdminService.DescribeCluster`
RPC was sketched but **rejected, not deferred**. Cluster/topology
discovery is served by `crowkv-server`'s existing HTTP management API
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
pipelining described in `design-slot.md` §5:

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
  two-fence rule (see `design-leader-election.md` §2.3).
- **Q3: Should `Prepare`/`Accept` carry `membership_epoch`?** —
  Resolved: Added in P5 M2. Responses echo `membership_epoch` and set
  `epoch_mismatch` when the proposer's epoch doesn't match the
  responder's. The proposer adopts the responder's epoch and retries
  without bumping its ballot.

---

## References

- `design-leader-election.md` §6 — heartbeat/lease interaction with
  stream ordering
- `design-slot.md` §5 — pipelined fanout and per-peer flow control
