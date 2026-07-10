# CrowKV - Plan: RPC and Client Implementation

Depends on: [`plan.md`](plan/plan.md), [`design.md`](design.md) §2, §7, [`design-rpc.md`](design/design-rpc.md), [`plan-consensus.md`](plan/plan-consensus.md), [`plan-wal.md`](plan/plan-wal.md), [`plan-storage.md`](plan/plan-storage.md)
Satisfies: [requirement.md §10](requirement.md#10-client-interaction), [requirement.md §3 RPC framework assumption](requirement.md#3-dependencies-and-assumptions)

Phase 4: gRPC wire protocol, node-to-node transport, client library. P4 replaces the in-process channels from P1's `test_harness` with a real network transport. P2 WAL and P3 storage are integrated here for the first real-restart tests.

**Note on early RPC subset:** P1 M2 (see [`plan-consensus.md`](plan/plan-consensus.md) §1) introduces a minimal RPC service for classic Paxos (`Prepare`/`Promise`/`Accept`/`Accepted`) using hand-coded Rust structs with `prost`-style encode/decode, so that classic Paxos can be exercised over a real loopback wire before leader election lands. Tests use `TestNodeHarness` (in-process threads with real `TcpListener`) rather than the `crowkv-server` binary. P4 builds on that foundation: it formalizes the `.proto` schema, adds the remaining message types (`Chosen`, `Heartbeat`, `RequestVote`, `Vote`, `SnapshotChunk`, client RPCs), and introduces the `version` field. Field numbers used in P1 M2 are append-only — P4 must not mutate them.

## 1. Milestones

### M1 — Protobuf schema (extends P1 M2 wire shapes)

- Formalize `.proto` files in `rpc/proto/` for the full message set: `Prepare`, `Promise`, `Accept`, `Accepted` (inherited from P1 M2 [`design-rpc.md`](design/design-rpc.md) §2, field numbers must match); plus `Chosen`, `Heartbeat`, `RequestVote`, `Vote`, `SnapshotChunk`, `ClientRequest`, `ClientResponse`, `DescribeCluster`, `NotLeaderHint`.
- Add `version: u32` at fixed tag 1 to every message (P1 M2 used hand-coded structs without explicit versioning; this milestone retrofits it).
- Append-only field numbers; document forbidden-mutation rules in a header comment.
- `build.rs` invokes `tonic-build` (or equivalent) to generate Rust types.
- **Freeze:** `.proto` schema after this milestone — no field-number changes without version bump.

**Acceptance:** all message types encode/decode round-trip; `protoc --decode_raw` of every generated message succeeds.

### M2 — Node-to-node gRPC service

- `PeerService`: bidirectional gRPC stream per `(group_id, peer_id)` pair carrying `Accept` / `Accepted` / `Chosen` / `Heartbeat`. One stream per peer (not per slot); messages are independently routed.
- `VoteService`: unary `RequestVote` → `Vote`.
- `SnapshotService`: server-streaming `SnapshotChunk` for snapshot install (used by P5; protocol shape frozen here).
- TLS deferred ([requirement.md §3](requirement.md#3-dependencies-and-assumptions)); plaintext-only in P4 (no TLS hooks).

**Acceptance:** 3-node cluster on `127.0.0.1` loopback passes the same integration scenarios that `test_harness` ran (S1 leader change, S2 parallel slots with gap, S3 partition).

### M3 — Client library

- Seed list (static config), `DescribeCluster` RPC, topology cache (`group_id → leader_endpoint`).
- Key hash → `group_id` → cached leader; fallback to any group member which responds with `NotLeaderHint`.
- Retry policy ([requirement.md §10.2](requirement.md#102-retry-and-idempotency)): exponential backoff on timeout, immediate retry on `NotLeaderHint`, configurable max retries.
- `(client_id, sequence_number)` attached to every write; client allocates sequence monotonically per session.
- Cache `safe_slot` from every server response for use in subsequent stale reads.

**Acceptance:** client survives leader change mid-request with automatic retry, returns same result; client survives Group-0 leader change for `DescribeCluster` refresh.

### M4 — Read mode routing

- `Linearizable` → leader only, lease check enforced. The lease state machine itself is unchanged from P1 M4 (`lease.rs`); P4 only swaps `TestTimer` for the real monotonic clock and wires in real heartbeat round-trip times.
- `SafeSlot` / `AtSlot(N)` → any replica with sufficient resolved-slot.
- `BestEffortStale` → any replica.
- Lease fallback: ReadIndex implemented as quorum heartbeat round-trip ([`design-leader-election.md`](design/design-leader-election.md) §7).

**Acceptance:** mixed crowbench workload (50% leader reads, 50% follower reads) returns zero divergence; ReadIndex fallback exercised by deliberately disabling lease in test config.

## 2. Module Breakdown

Module: **`rpc`** inside `crowkv`. Hosts both the gRPC server side (used by `crowkv-server` binary) and the gRPC client library (used by external clients and by `crowkv::reconfig`).

| Path (in `crowkv/src/rpc`) | Responsibility |
|---|---|
| `proto/*.proto` | Protobuf schema source files |
| `build.rs` | `tonic-build` invocation that generates Rust types from `.proto` |
| `peer.rs` | `PeerService` server + client (bidirectional stream per peer) |
| `vote.rs` | `VoteService` server + client (unary) |
| `snapshot.rs` | `SnapshotService` server + client (server-streaming) |
| `server.rs` | gRPC server bootstrap, wires services into the Group Manager |
| `client.rs` | Client library: seed list, topology cache, routing, retry |
| (no new lease module) | `election::lease` from `crowkv::consensus` (P1 M4) reused as-is; only the clock source changes from `TestTimer` to system monotonic clock |
| `topology.rs` | Topology refresh, `NotLeaderHint` handling, `config_version` tracking |

`crowkv::rpc` depends on `crowkv::consensus`, `crowkv::engine`, and `crowkv::wal`. `crowkv::reconfig` (P5) depends on `crowkv::rpc`.

## 3. Group-0

[`design.md`](design.md) §7 requires Group-0 (system group) to hold cluster topology. **Decision:** static read-only Group-0 config in P4 (sufficient to bootstrap the loopback cluster); dynamic Group-0 (writable config, runtime topology change) added in P5 alongside reconfiguration.

## 4. Freeze Checklist

Before P5 (Reconfiguration) starts:
- [ ] `.proto` schema frozen (append-only field numbers, version field at tag 1)
- [ ] Snapshot streaming protocol frozen (P5 uses it for new-member install)
- [ ] Lease (from P1) wired to real monotonic clock; P1 lease tests still pass under `TestTimer`
- [ ] G4 milestone passes: 3-node loopback cluster, crowbench 10k ops, zero divergence

## 5. Out-of-Scope for P4

- Joint consensus / membership change (P5)
- Rolling upgrade testing (P5)
- TLS (deferred per [requirement.md §3](requirement.md#3-dependencies-and-assumptions))
- Authentication / authorization (future)
