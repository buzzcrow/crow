<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R39: Least-conn / latency read-endpoint policy

**Problem**: R26 shipped `ReadEndpointPolicy::AnyReplica` — a
round-robin selector that distributes MinSlot reads across all replica
endpoints. Round-robin is blind rotation: it sends equal load to every
replica regardless of actual capacity or current latency. A slow replica
(e.g. one demand-loading cold crowtree pages, or running on a busier
node) gets the same share as a fast one, so p99 read latency is bounded
by the slowest replica in the rotation. The leader carries no
stale-read load under `AnyReplica`, but a single slow follower drags
tail latency for 1/N of MinSlot reads.

**Target**:
- New `ReadEndpointPolicy` variants beyond `Leader` and `AnyReplica`:
  - `LeastConnections` — route to the replica with the fewest in-flight
    reads. Requires the client to track per-endpoint in-flight counts
    (already observable client-side: increment on send, decrement on
    response).
  - `Latency` — route to the replica with the lowest recent RTT.
    Requires a per-endpoint EWMA of get RTT (updated on each response),
    with a decay so a transient spike doesn't pin reads away from a
    recovered replica.
- Both policies fall back to round-robin on ties and on the first
  request (no RTT history). Both retain R26's `NotLeader`-hint fallback
  to the leader when a MinSlot read hits a lagging follower.
- The existing `read_endpoint_distributed` / `read_endpoint_fallback`
  counters cover the new policies unchanged.

**Acceptance**:
- Under a mixed workload where one replica is artificially slowed (e.g.
  delay injected into its engine get), `LeastConnections` and `Latency`
  route fewer MinSlot reads to the slow replica than round-robin,
  measured by per-endpoint request count.
- p99 MinSlot read latency under the new policies is no worse than
  round-robin when all replicas are healthy (no regression in the
  uniform case).
- The policy is selectable via the same config as R26
  (`read_endpoint_policy`), no new CLI flag required (just a new enum
  value).

**Dependencies**: None new — builds on R26's `ReadEndpointPolicy`,
`resolve_read_endpoint`, and the topology cache's replica list. The
in-flight counter (for `LeastConnections`) is client-local state; the
RTT EWMA (for `Latency`) is client-local state updated on each response.

**Priority**: Low-medium — round-robin (R26) already distributes load;
the new policies help tail latency when replicas are heterogeneous, a
secondary concern until read scaling is the primary bottleneck.

**Complexity**: Medium — the selection logic is small (per-endpoint
in-flight counter or RTT EWMA + min select). The design decisions are
the EWMA decay rate, the tie-break policy, and whether to expose the
choice as separate enum variants or a single `Adaptive` policy that
picks based on history depth.

**Files**: `lib/crow-kv-client/src/config.rs` (`ReadEndpointPolicy` enum),
`lib/crow-kv-client/src/client.rs` (`resolve_read_endpoint` — new selection
logic, per-endpoint state), `lib/crow-kv-client/src/metrics.rs` (optional:
per-endpoint RTT gauge).
