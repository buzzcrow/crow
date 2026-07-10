# CrowKV - Design: RPC Wire Protocol

Depends on: [`requirement.md`](requirement.md) §3, §9.2, §10, [`design.md`](design.md) §2, §3, [`plan-consensus.md`](plan/plan-consensus.md) §1 M2
Satisfies: [requirement.md §3](requirement.md#3-dependencies-and-assumptions), [requirement.md §9.2](requirement.md#92-rolling-upgrade), [requirement.md §10.1](requirement.md#101-client-discovery)

This document defines the wire-serialization contract for all node-to-node and client-to-node RPC communication. The implementation uses **gRPC with protobuf** (tonic + prost). Every message carries a `version: u32` field at fixed protobuf tag 1 for forward/backward compatibility; no `required` fields; field numbers are append-only.

## Table of Contents

- [1. Design Principles](#1-design-principles)
- [2. Classic Paxos Messages (P1 M2 subset)](#2-classic-paxos-messages-p1-m2-subset)
  - [2.1 Prepare](#21-prepare)
  - [2.2 Promise](#22-promise)
  - [2.3 Accept](#23-accept)
  - [2.4 Accepted](#24-accepted)
- [3. Message Envelope (P4 extension)](#3-message-envelope-p4-extension)
- [4. Service Definitions](#4-service-definitions)
  - [4.1 PeerService (node-to-node)](#41-peerservice-node-to-node)
  - [4.2 AdminService (client / operator)](#42-adminservice-client--operator)
- [5. Rust Mapping](#5-rust-mapping)
- [6. Version Compatibility Rules](#6-version-compatibility-rules)
- [7. Open Questions](#7-open-questions)

---

## 1. Design Principles

1. **One bidirectional stream per peer pair.** All node-to-node messages multiplex over a single gRPC bidi stream keyed by `(group_id, peer_node_id)`. This reduces connection count and simplifies backpressure.
2. **Envelope + payload pattern.** Every stream message is a `PeerMessage` oneof envelope; the payload carries the actual Paxos / heartbeat / snapshot chunk. New message types add new oneof arms without changing existing field numbers.
3. **No `required` fields.** All protobuf fields are `optional` or have sensible defaults. A missing `version` field defaults to `0` (meaning "pre-versioning, treat as earliest").
4. **Field numbers are append-only.** Once assigned, a field number is never reused for a different semantic meaning. This is a hard rule for rolling upgrades ([requirement.md §9.2](requirement.md#92-rolling-upgrade)).
5. **Plaintext in P1/P4; TLS hooks reserved.** The transport layer is plaintext TCP loopback in tests and P1/P4 integration. TLS config slots exist in the service builder but are unimplemented ([requirement.md §11](requirement.md#11-security)).

---

## 2. Classic Paxos Messages (P1 M2 subset)

These four messages are the **minimum viable wire surface** introduced in P1 M2. They are sufficient to run a full classic Paxos round across a real network boundary. All other messages (`Heartbeat`, `RequestVote`, `Vote`, `Chosen`, `SnapshotChunk`, client RPCs) are added in P4 without mutating the field numbers below.

### 2.1 Prepare

Phase-1 request sent by the leader (proposer) to all acceptors.

```protobuf
message Prepare {
  uint32 version = 1;   // wire-format version, always present
  uint64 slot    = 2;   // PxSlot index being prepared
  uint64 round   = 3;   // ballot.round
  uint64 leader_id = 4; // ballot.leader_id (PxNodeId)
}
```

### 2.2 Promise

Phase-1 response returned by each acceptor.

```protobuf
message Promise {
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
message Accept {
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
message Accepted {
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

## 3. Message Envelope (P4 extension)

In P1 M2 the service uses unary RPCs (one request → one response) for simplicity. P4 generalizes to a bidirectional `PeerMessage` stream envelope:

```protobuf
message PeerMessage {
  uint32 version = 1;
  uint64 group_id = 2;
  uint64 sender_node_id = 3;

  oneof payload {
    Prepare    prepare    = 10;
    Promise    promise    = 11;
    Accept     accept     = 12;
    Accepted   accepted   = 13;
    // P4 additions (append-only field numbers):
    // Heartbeat  heartbeat  = 14;
    // Chosen     chosen     = 15;
    // SnapshotChunk snapshot_chunk = 16;
  }
}
```

**P1 M2 simplification:** because there is no `Heartbeat`, `RequestVote`, `Vote`, `Chosen`, or `SnapshotChunk` yet, the service is defined as two unary methods (`Prepare`, `Accept`) rather than a bidi stream. The envelope is reserved for P4 and must use the **same field numbers** listed above.

> **Reading back a slot in M2:** there is no dedicated query RPC. Tests verify follower state by issuing a `Prepare` with a higher ballot than any previously used ballot for that slot. The returned `Promise.previously_accepted` field carries the accepted value if one exists, or is absent if the slot is empty. This reuses the classic-Paxos value-recovery mechanism as an implicit read.

---

## 4. Service Definitions

### 4.1 PeerService (node-to-node)

P1 M2 minimal surface:

```protobuf
service PeerService {
  rpc Prepare(Prepare) returns (Promise);
  rpc Accept(Accept) returns (Accepted);
}
```

P4 extension: replaces the two unary methods with a single bidi stream:

```protobuf
service PeerService {
  rpc Stream(stream PeerMessage) returns (stream PeerMessage);
}
```

The P4 `Stream` method must still be able to carry the four classic-Paxos message types using the `PeerMessage` oneof arms defined in §3.

### 4.2 AdminService (client / operator)

Not needed in P1 M2. Defined in P4 per [`plan-rpc.md`](plan/plan-rpc.md) §1 M3/M4:

```protobuf
service AdminService {
  rpc DescribeCluster(google.protobuf.Empty) returns (DescribeClusterResponse);
}
```

---

## 5. Rust Mapping

| Protobuf message | Generated Rust type (tonic-build) | Runtime Rust equivalent (pre-P4) |
|---|---|---|
| `Prepare` | `rpc::proto::Prepare` | `paxos::protocol::PrepareRequest` (hand-coded struct, same fields) |
| `Promise` | `rpc::proto::Promise` | `paxos::protocol::PxPrepareReply` (existing enum, extended with `rejected` info) |
| `Accept` | `rpc::proto::Accept` | `paxos::protocol::AcceptRequest` (hand-coded struct, same fields) |
| `Accepted` | `rpc::proto::Accepted` | `paxos::protocol::PxAcceptReply` (existing enum, extended with `rejected` info) |

**P1 M2 strategy:** because `.proto` generation via `tonic-build` in a `build.rs` is a P4 milestone (`plan-rpc.md` M1), P1 M2 uses **hand-coded Rust structs** that mirror the protobuf shape above, **including the `version: u32 = 1` field on every message**. This ensures P4 can decode M2 wire bytes without ambiguity.

The structs are annotated with `#[derive(prost::Message)]` (or an equivalent lightweight encode/decode impl) so that P4's `.proto` generation produces byte-identical output. This avoids:
- Adding a `build.rs` dependency to the `crowkv` crate in P1.
- Committing to exact `.proto` file paths before P4 reviews them.

If `prost` derive is unavailable, a manual `Encoder`/`Decoder` trait impl targeting the protobuf wire format is acceptable for M2 only; the `version` field must still occupy tag 1 in the manual impl.

---

## 6. Version Compatibility Rules

1. **Sender rule:** every message sets `version = 1` (the initial wire version).
2. **Receiver rule:** decode must accept any `version <= max_supported`. Unknown fields are ignored (protobuf default behavior).
3. **Upgrade rule:** when P4 introduces `version = 2`, new fields are added with new field numbers; old fields are never removed or renumbered.
4. **P1 M2 freeze:** field numbers 1–13 are frozen. P4 may add new oneof arms starting at field number 14. No changes to the semantic meaning of fields 1–13.

---

## 7. Open Questions

- **Q1:** Should `AcceptedValue.payload` carry a `LogEntryKind` enum in P1 M2, or is it purely opaque bytes until P1 M4 introduces the learner?  
  **Tentative:** opaque bytes in M2; the leader and acceptor treat it as a blob. M4 introduces `kind` discrimination when the learner needs to distinguish `Write` from `NoOp`.
- **Q2:** Should `Promise` and `Accepted` carry `term` for leader-fencing, or is `ballot` sufficient in classic Paxos?  
  **Tentative:** `term` is omitted from the P1 M2 wire format; the leader's `term` is implicit because there is no election. M3 (leader election) adds `term` to all messages for fencing.

---

## References

- [Protocol Buffers Language Guide](https://developers.google.com/protocol-buffers/docs/proto3)
- [Tonic gRPC framework](https://github.com/hyperium/tonic)
- [prost — Protocol Buffers implementation for the Rust Language](https://github.com/tokio-rs/prost)
- [`plan-rpc.md`](plan/plan-rpc.md) §1 — full RPC phase plan (P4)
- [`plan-consensus.md`](plan/plan-consensus.md) §1 M2 — minimal RPC milestone that introduces this subset
