<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# R26 Design — Follower Read Distribution for MinSlot

Upstream: `doc/backlog/R26-follower-read.md`,
`doc/working/read-flow-analysis.md` G8, `design/design.md` §6 / §10,
`crowkv-client/src/{client,topology,config,metrics}.rs`.

## Problem

`CrowkvClient::get` / `scan` always call `resolve_leader(store_id,
group_id)` to pick the first endpoint, regardless of `read_mode`. The
server already serves `MinSlot` reads from the local replica without
forwarding (`crowkv/src/rpc/kv_service.rs:247-251`,
`crowkv/src/cluster/px_kv_store.rs:514-528`), so any follower could
serve them. With the current client routing, all stale-read load is
concentrated on the leader, leaving follower capacity unused and making
the leader a read bottleneck under read-heavy workloads.

The topology cache already receives the full replica list in
`/topology` (`GroupStatus.local_replica` + `GroupStatus.remotes` in
`crowkv/src/cluster/status.rs:60-69`), but `TopologyCache::merge`
(`crowkv-client/src/topology.rs:116-134`) only stores the leader
endpoint — the per-group replica list is discarded.

## Design

### Approach: client-side read-endpoint selector

No server-side change. The server already serves `MinSlot` reads
locally and returns `NotLeader { hint }` (the leader endpoint) when the
local frontier has not caught up to `min_slot`
(`px_kv_store.rs:521-526`). The client already follows `not_leader_hint`
on `get` (`client.rs:608-615`). R26 adds the missing piece: a
client-side selector that picks *which* replica to send a `MinSlot` read
to in the first place.

### Topology cache: expose replica endpoints

`TopologyCache` gains a second `DashMap<(u64, u64), Vec<String>>`
(`replicas`) populated in `merge` from `local_replica` (using
`store.listen_addr`) plus every `remote` in `group.remotes`. The leader
cache is unchanged. New accessor `replicas(store_id, group_id) ->
Option<Vec<String>>` returns a clone of the list (never does I/O).

`set_leader` (the `NotLeaderHint` fast path) does **not** update
`replicas` — a hint only carries the leader endpoint, not the full
replica set. The replica list is refreshed only by `refresh()`.

### Config: `ReadEndpointPolicy`

New enum in `config.rs`:

```rust
pub enum ReadEndpointPolicy {
    Leader,       // default — backward compatible, linearizable-safe
    AnyReplica,   // MinSlot reads round-robin across replicas
}
```

Added to `ClientConfig` as `read_endpoint_policy`, defaulting to
`Leader` in `ClientConfig::new`. `Leader` keeps linearizable reads
correct (they must target the leader) and preserves the existing
behavior for callers that never set the flag.

### Selector: round-robin per group

`CrowkvClient` gains a `read_rr: DashMap<(u64, u64), AtomicU64>` — one
round-robin cursor per `(store_id, group_id)`. New private async
`resolve_read_endpoint(store_id, group_id, read_mode) -> Result<String>`:

- `read_mode == Linearizable` **or** `policy == Leader` → delegate to
  the existing `resolve_leader` (linearizable reads must always target
  the leader; `Leader` policy preserves today's behavior for both
  modes).
- `read_mode == MinSlot` **and** `policy == AnyReplica`:
  1. Read `topology.replicas(store_id, group_id)`. If `None` or empty,
     call `topology.refresh().await` once and re-read.
  2. If still empty, fall back to `resolve_leader` (a single-replica
     group, or `/topology` is stale) — never fail just because the
     replica list is unknown.
  3. Otherwise pick `replicas[cursor % len]`, incrementing `cursor` with
     `fetch_add(1, Relaxed)`.

`get` and `scan` call `resolve_read_endpoint` instead of
`resolve_leader` for their *initial* endpoint. The retry bodies are
unchanged — they already handle `NotLeaderHint` (get) and counted
errors (scan) against whatever endpoint was chosen.

### Fallback path

- **`get`** — when a chosen follower has not applied `min_slot`, the
  server returns `NotLeader { hint = leader_endpoint }`, which surfaces
  as `resp.not_leader_hint`. The existing `follow_not_leader` branch
  (`client.rs:324-331`) follows the hint to the leader and retries —
  exactly the "fall back to the leader for that request" semantics R26
  asks for. No new code needed on the get path.
- **`scan`** — `KvScanResponse` has no `not_leader_hint` field. The
  server encodes the redirect as `scan_err("not leader; retry scan at
  {hint}", …)` (`px_kv_store.rs:162-167`). R26 adds a small
  `follow_scan_not_leader` helper that parses the `not leader; retry
  scan at ` prefix out of `resp.error`, returns the leader endpoint,
  and the scan retry loop follows it (uncounted, like the get path).
  Non-matching errors keep the existing counted-error behavior.

### Metrics

Two new client-side counters in `metrics.rs` (lock-free `AtomicU64`,
same pattern as the existing client counters):

- `read_endpoint_distributed` — incremented each time the selector
  picks a non-leader replica for a `MinSlot` read (i.e. the
  `AnyReplica` branch fired and produced a replica list). Lets an
  operator confirm distribution is actually happening.
- `read_endpoint_fallback` — incremented each time a distributed
MinSlot read fell back to the leader via `NotLeaderHint` (get) or
  the scan `not leader; retry scan at ` parse. Pairs with the
  server-side `read.minslot_fallback.c` to confirm the fallback rate
  stays low.

Both surface in `ClientMetricsSnapshot` and the `serde` shape stays
backward compatible (`#[serde(default)]`).

### Alternatives considered

- **Server-side read load balancer** — a dedicated proxy that fans
  reads out across replicas. Rejected: adds a network hop and a new
  component; the client already has the topology and the routing seam.
- **Least-connection / latency-based selector** — mentioned in R26 as a
  future policy. Rejected for v1: needs per-replica inflight tracking
  or RTT samples the client doesn't have today. Round-robin is the
  minimal seam; the selector is the single place future policies plug
  into.
- **Always include the leader in the round-robin pool** — rejected.
  The leader is already one of the replicas in the list returned by
  `/topology`, so it is naturally included in round-robin rotation.
  Excluding it would over-correct; including it via the natural list
  keeps the leader's read share at `1/N` rather than `0` or `1`.
- **Parse the scan error string for the hint** — fragile in general,
  but the format `"not leader; retry scan at {endpoint}"` is produced
  by one site (`px_kv_store.rs:164`) and the prefix is stable. A
  dedicated `not_leader_hint` field on `KvScanResponse` would be
  cleaner but is a protocol change — out of scope for R26 ("no
  protocol change"). Documented as a future cleanup.

## Acceptance test plan

- `read_endpoint_policy = Leader` (default): all reads route to the
  leader; existing `e2e_single_node_test` / `e2e_retry_test` still
  pass — no behavior change.
- `read_endpoint_policy = AnyReplica`, single-replica group: selector
  falls back to the leader (only one replica in the list); read
  succeeds; `read_endpoint_distributed` increments.
- `read_endpoint_policy = AnyReplica`, two-replica group, MinSlot
  reads against a caught-up follower: reads succeed without fallback;
  `read_endpoint_distributed` increments; `read_endpoint_fallback`
  stays 0.
- `read_endpoint_policy = AnyReplica`, MinSlot read against a
  follower whose frontier has not reached `min_slot`: server returns
  `NotLeader` hint → client follows to leader → read succeeds;
  `read_endpoint_fallback` increments.
- `read_endpoint_policy = AnyReplica`, Linearizable read: still
  targets the leader regardless of policy; `read_endpoint_distributed`
  does not increment.
- Scan under `AnyReplica`: same distribution; fallback via the
  `not leader; retry scan at ` parse follows to the leader.
