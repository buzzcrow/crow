# CrowKV - Test Design: RPC and Client

Depends on: [`test-design.md`](test-design.md), [`design.md`](design.md) §2, §7, §10
Satisfies: [requirement.md §10.1](requirement.md#101-client-discovery), [requirement.md §10.2](requirement.md#102-retry-and-idempotency)

Invariants for wire protocol and client behavior.

## 1. Invariants

| ID | Claim | Trigger | Ref |
|---|---|---|---|
| R1 | Protobuf forward compat | Message decode with unknown fields | [`requirement.md`](requirement.md) §9.2 |
| R2 | `NotLeader` hint routing | Client receives `NotLeaderHint` | [`design.md`](design.md) §7 |
| R3 | Retry idempotency | Duplicate `(client_id, seq)` | [`design.md`](design.md) §8.6 |
| R4 | Lease check before linearizable read | Leader read path | [`design-leader-election.md`](design-leader-election.md) §6 |
| R5 | ReadIndex fallback when lease invalid | Lease expired | [`design-leader-election.md`](design-leader-election.md) §7 |
| R6 | Bidirectional stream survives reconnect | Transient network drop | [`design.md`](design.md) §3 |

## 2. Unit Tests

| Module | Test | Assertion |
|---|---|---|
| `rpc` | `protobuf_roundtrip` | All message types encode/decode |
| `client` | `retry_not_leader` | Follows hint, succeeds on retry |
| `client` | `retry_timeout` | Exponential backoff, eventually succeeds |
| `client` | `idempotent_seq` | Same seq returns same slot |

## 3. Failure Injection

| Failure | Sim | Invariant | Assertion |
|---|---|---|---|
| Connection drop | tonic in-process channel close | R6 | Stream reconnects, in-flight requests resumed via retry |
| Stale `config_version` | mock topology mismatch | R2 | Client refreshes topology and retries |
| Lease expired (no quorum heartbeat) | `TestTimer::advance(>lease_duration)` | R5 | Linearizable read falls back to ReadIndex |
| Slow follower | `TestRouter::delay(...)` | R3 | Client retry hits same slot via dedup |

## 4. Integration Scenarios

**S-R1 — Network partition mid-request:** client retries on heal, returns same result via dedup.

**S-R2 — Leader change mid-stream:** client rediscovers via `DescribeCluster`, retries against new leader.

**S-R3 — Version mismatch:** `config_version` mismatch detected, client refreshes topology and retries.

## 5. Resolved Decisions

- **Test transport:** in-process direct call for unit tests; real loopback (`127.0.0.1`) for integration tests.
- **Connection pooling:** one client connection per node (simpler than per-group; sharing across groups on the same node is fine).
