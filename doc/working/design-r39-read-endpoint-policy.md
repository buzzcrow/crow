<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R39 Design: Least-conn / latency read-endpoint policy

## Problem

R26 shipped `ReadEndpointPolicy::AnyReplica` — a round-robin selector
that distributes `MinSlot` reads across all replica endpoints. Round-robin
is blind rotation: a slow replica (demand-loading cold pages, on a busy
node) gets the same share as a fast one, so p99 is bounded by the slowest
replica for `1/N` of reads.

## Current behavior

`resolve_read_endpoint` (`client.rs:208`) handles two policies:
- `Leader` → `resolve_leader` (always the leader).
- `AnyReplica` → round-robin via a per-group `AtomicU64` cursor
  (`read_rr` `DashMap`), `fetch_add` then `% replicas.len()`.

Linearizable reads bypass the policy entirely (line 214). The
`read_endpoint_distributed` / `read_endpoint_fallback` counters fire
only when `policy == AnyReplica` (lines 391, 635, 718).

## Proposed approach

Two new `ReadEndpointPolicy` variants, each with client-local
per-endpoint state:

- `LeastConnections` — route to the replica with the fewest in-flight
  reads. The client tracks per-endpoint in-flight count via an
  `AtomicI64` (increment on send, decrement on response via an RAII
  guard).
- `Latency` — route to the replica with the lowest recent RTT. The
  client maintains a per-endpoint EWMA of get RTT (`AtomicU64` in
  micros), updated on each response. `alpha = 0.25` (quarter-weight to
  new sample); first sample initializes the EWMA. A CAS loop updates
  the atomic concurrently.

Both policies:
- Fall back to round-robin on ties (including the first request when
  no history exists) via the existing `read_rr` cursor.
- Retain R26's `NotLeader`-hint fallback to the leader when a
  `MinSlot` read hits a lagging follower — unchanged.
- Increment `read_endpoint_distributed` on every selection and
  `read_endpoint_fallback` on every NotLeader redirect — the counters
  cover all distributed policies, not just `AnyReplica`.

### Per-endpoint state

`DashMap<String, EndpointStats>` keyed by endpoint string (same keys as
the replica list from the topology cache). `EndpointStats`:
- `in_flight: AtomicI64` — incremented before the gRPC send, decremented
  on drop of an `InFlightGuard` (RAII; covers all exit paths: success,
  error, redirect, `?`).
- `rtt_ewma_us: AtomicU64` — updated on every `Ok` response (success,
  not-found, NotLeader redirect); not updated on transport errors (a
  timeout doesn't reflect the endpoint's serving latency).

Entries are created lazily on first selection and are not evicted — the
map grows with endpoint churn, but endpoints are bounded by the cluster
size × groups, and stale entries (replica removed from topology) simply
accumulate zero in-flight and zero RTT, never selected again. An
eviction pass is not worth the complexity at this scale.

### Selection logic

Refactor `resolve_read_endpoint` to delegate to a per-policy selector
after the common replica-list resolution:

- `AnyReplica` → round-robin (existing).
- `LeastConnections` → min `in_flight`; ties broken by round-robin
  cursor.
- `Latency` → min `rtt_ewma_us`; zero (no history) treated as a tie;
  ties broken by round-robin cursor.

### In-flight tracking scope

The in-flight guard wraps each gRPC attempt in the retry loop, not just
the initial selection. Rationale: the counter reflects actual load on
the endpoint. When a NotLeader redirect changes the endpoint, the guard
for the old endpoint drops (end of iteration) and a new guard is created
for the new endpoint (next iteration). This is correct — the old
endpoint's load decreased (it responded) and the new endpoint's load
increased (new request in flight).

### RTT recording scope

RTT is recorded once per gRPC attempt, measured from `t0` (before the
send) to the response. On the `Ok` arm, `t0.elapsed()` is recorded for
the current `endpoint`. On the `Err` arm (transport error), no RTT is
recorded — a connection failure or timeout doesn't reflect the
endpoint's serving capacity. The RTT is recorded before the
success/error branching within the `Ok` arm, so NotLeader redirects
also update the EWMA (the endpoint responded, just couldn't serve).

### Enum shape: separate variants, not Adaptive

The backlog raised "separate enum variants vs. a single `Adaptive`
policy that picks based on history depth." Separate variants are
chosen:
- Explicit — the operator sees exactly which policy is active.
- Simpler — no meta-decision about when to switch.
- Matches the spec's "New `ReadEndpointPolicy` variants" wording.
- An `Adaptive` policy can be added later as a wrapper if needed.

## Alternatives considered

- **Single `Adaptive` policy** — rejected: adds a meta-decision (when
  to switch between least-conn and latency) without clear benefit;
  harder to reason about and debug.
- **Server-side load reporting** — rejected: requires a new RPC or
  metadata field; R39 is explicitly client-local state, no server
  change (the delay seam is test-only, not production).
- **Per-group per-endpoint state** — rejected: endpoints are shared
  across groups on the same node; per-endpoint (not per-group-per-
  endpoint) state is simpler and sufficient — a slow node is slow for
  all groups.
- **Eviction of stale endpoint stats** — rejected: map size is bounded
  by cluster size; stale entries are harmless (never selected); eviction
  adds complexity for no benefit.

## Test seam

A `#[cfg(feature = "test-util")]` delay on `PxKvStore::kv_get` — a
`Mutex<Option<Duration>>` field set via `set_get_delay_for_tests`.
When set, `kv_get` sleeps before `resolve_read_point`. This lets a
test make one replica artificially slow, verifying the new policies
route fewer reads to it. The `crow-kv-client` crate gains a
`test-util` feature that forwards to `crow-kv/test-util`, auto-enabled
for the crate's own tests via a self dev-dependency (same pattern as
`crow-kv`'s own `test-util`).

## Acceptance test plan

1. **Distribution** — `LeastConnections` and `Latency` distribute
   `MinSlot` reads across `[A, B, C]` (all healthy, `min_slot = 0`):
   `read_endpoint_distributed >= N`; both `Found` (A/B) and `NotFound`
   (C) branches fire. Mirrors the existing
   `any_replica_distributes_minslot_reads_with_lagging_follower`.

2. **Slow-replica bias** — inject a 50 ms get delay on C; fire
   concurrent `MinSlot` reads with `min_slot = 0`. `LeastConnections`
   routes fewer reads to C than round-robin (C's in-flight stays high
   while it sleeps); `Latency` routes fewer reads to C (C's RTT EWMA
   is high after the first response). Verified by per-endpoint request
   count (count `Found` vs `NotFound` outcomes — C returns `NotFound`
   for `min_slot = 0`).

3. **Fallback** — `min_slot = write.revision`; both policies fall back
   to the leader when C returns `NotLeader`. Every read returns
   `Found`; `read_endpoint_fallback >= 1`.

4. **No effect on linearizable** — linearizable reads always target
   the leader; `read_endpoint_distributed == 0`.

5. **Config selectable** — new policies parse from the bench CLI
   (`--read-endpoint-policy least-connections|latency`).
