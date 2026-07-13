# CrowKV - Design: Paxos Error Model
Depends on: [`../requirement.md`](../requirement.md), [`../design.md`](../design.md), [`design-rpc.md`](design-rpc.md)
Satisfies: [../requirement.md §6.1](../requirement.md#61-write-guarantee), [../requirement.md §10.2](../requirement.md#102-retry-and-idempotency), [../requirement.md §13.2](../requirement.md#132-mandatory-observability-signals), [../requirement.md §14](../requirement.md#14-testing-requirements)

## 1. Goal

CrowKV must handle Paxos failures through named categories instead of ad-hoc strings. The category determines whether the proposer retries the same slot, runs classic repair, moves the client operation to a new slot, redirects the client, or returns a retryable error.

## 2. Error Categories

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

## 3. Retry Rules

### 3.1 Prepare Rejection

A prepare rejection means at least one acceptor has promised a higher ballot. The proposer must not move to a new slot. It retries Phase 1 for the same slot using a ballot above the highest observed rejected ballot.

### 3.2 Accept Rejection

An accept rejection means the current fast-path or Phase 2 ballot is stale. The proposer must run classic Phase 1 for the same slot. If Phase 1 later discovers a foreign accepted value, that value is repaired and learned before the client operation moves to a new slot.

### 3.3 Transport or Quorum Failure

A transport failure without a higher promised ballot does not prove Paxos contention. The proposer retries the same slot with the same ballot until the retry budget is exhausted. Increasing the ballot is only required after a higher-ballot rejection.

### 3.4 Foreign Value

A foreign value discovered through Phase 1b may already be chosen or may be required for safety. The proposer must adopt it for the current slot. If it is not the client value, the client operation is retried on a later slot only after the foreign value is learned.

## 4. RPC Mapping

Unary Paxos responses continue to carry rejected metadata directly. RPC transport failures are mapped by the caller into `TransportFailure`. Malformed Paxos RPCs, such as an `AcceptRequest` without a value, map to `InternalInvariantViolation` at the Paxos model level and to `invalid_argument` at the gRPC boundary.

## 5. Test Requirements

A dedicated `crowkv/tests/paxos_error.rs` file owns error behavior tests. It should cover:

1. prepare rejection records the higher ballot and requires same-slot retry;
2. accept rejection triggers classic repair for the same slot;
3. foreign value adoption retries the client operation on a later slot;
4. quorum unavailable produces retryable failure without abandoning the slot as chosen;
5. follower KV requests return `NotLeader` with a leader hint;
6. malformed accept RPC is rejected by the gRPC service.
