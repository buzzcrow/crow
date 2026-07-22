<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# R19 Design — Read Performance Profiling and Metrics

## Problem

The write path has a well-instrumented latency-bandwidth-counter
hierarchy (WAL append/fsync latency, write bandwidth, election
counters, inflight gauge). The read path has only `kv.get.lh`
(end-to-end get) and `kv.scan.l` (end-to-end scan). Operators
cannot decompose read latency into its contributors (barrier vs
engine), cannot tell lease-path from ReadIndex-path usage, cannot
separate read bandwidth from write bandwidth, and cannot detect
forwarding or MinSlot-fallback rates. See
[`read-flow-analysis.md`](read-flow-analysis.md) gaps G1–G7, G10.

## Current Behavior

- `KvMetrics` (`crowkv/src/rpc/kv_service.rs`): `put_lh`, `get_lh`,
  `delete_c`, `scan_l`, `bytes_in_bw`, `bytes_out_bw`, `errors_c`,
  `no_leader_c`. All read + write bandwidth flows through the shared
  `bytes_in_bw` / `bytes_out_bw`; no read-specific bandwidth.
  Forwarding is invisible (forwarded gets record `get_lh` at the
  forwarder, but no counter distinguishes forwarded from local).
- `ElectionRegistryHandles` (`crowkv/src/cluster/local_replica.rs`):
  `elections`, `step_downs_*`, `inflight_slots` gauge. No read
  handles.
- `linearizable_read_barrier`
  (`crowkv/src/cluster/group_election.rs`): returns
  `ReadBarrierOutcome` with no timing or path counter.
- `resolve_read_point` (`crowkv/src/cluster/px_kv_store.rs`):
  returns `ReadDecision` with no MinSlot-fallback counter.
- `PxLearner::engine_get_bytes` (`crowkv/src/paxos/learner.rs`):
  no timing; engine cost is indistinguishable from barrier cost in
  `get.lh`.

## Proposed Approach

Add a read metrics hierarchy mirroring the write path's structure,
following `design-observability.md` design principles
(counter/summary non-redundancy, latency hierarchy, bandwidth
hierarchy).

### New metric handles

**`KvMetrics`** (per store, group; lazily registered in
`kv_service.rs`):
- `kv.read_bytes_in.bw` — Bandwidth, read request bytes
  (key/prefix length).
- `kv.read_bytes_out.bw` — Bandwidth, read response bytes
  (value / scan items length).
- `kv.get_forwarded.c` — Counter, linearizable reads forwarded to
  leader (server-side, incremented at the forwarder on successful
  forward).
- `kv.get_forward_failed.c` — Counter, forward attempts that failed
  (incremented at the forwarder on forward error, before fallthrough).

**`ReadRegistryHandles`** (new struct, per store, group; stored on
`PxGroup` via `OnceLock`, mirroring `ElectionRegistryHandles` on
`PxLocalReplica`):
- `read.lease_path.c` — Counter, linearizable reads via lease fast
  path.
- `read.readindex_path.c` — Counter, linearizable reads via ReadIndex
  fallback.
- `read.barrier.l` — LatencySummary, `linearizable_read_barrier`
  wall-clock duration (near-zero for lease, one heartbeat RTT for
  ReadIndex).
- `read.engine_get.l` — LatencySummary, `KVEngine::get_bytes`
  wall-clock duration (isolates engine cost from barrier cost).
- `read.minslot_fallback.c` — Counter, MinSlot reads redirected to
  leader because `contiguous_applied < min_slot`.
- `read.lease_valid.g` — Gauge, 1 if leader's read lease is valid at
  the most recent barrier, 0 otherwise.
- `read.contiguous_applied.g` — Gauge, current `contiguous_applied`
  at the most recent read point.
- `read.safe_slot.g` — Gauge, current `group_safe_slot` at the most
  recent read point.

### Instrumentation points

- `linearizable_read_barrier` (`group_election.rs`): wrap the body in
  `Instant::now()` → `elapsed()`, observe `read.barrier.l`. On
  `Ready` from lease path → `read.lease_path.c.inc()`. On `Ready`
  from ReadIndex path → `read.readindex_path.c.inc()`. Set
  `read.lease_valid.g` to 1/0 based on `lease_read_valid` at barrier
  entry.
- `resolve_read_point` (`px_kv_store.rs`): on MinSlot
  `NotLeader` branch → `read.minslot_fallback.c.inc()`. Bridge
  `read.contiguous_applied.g` and `read.safe_slot.g` from
  `replica.contiguous_applied()` and `group.group_safe_slot()` on
  every call (cheap atomic stores, reflects state at the most recent
  read — same on-demand bridge pattern as `inflight_slots.g`).
- `kv_get` (`px_kv_store.rs`): wrap
  `learner.engine_get_bytes(key).await` in `Instant::now()` →
  `elapsed()`, observe `read.engine_get.l`.
- `KvStoreService::get` (`kv_service.rs`): on successful forward →
  `kv.get_forwarded.c.inc()`; on forward failure →
  `kv.get_forward_failed.c.inc()`. Observe `kv.read_bytes_in.bw` /
  `kv.read_bytes_out.bw` alongside the existing `bytes_in_bw` /
  `bytes_out_bw` on every get path (forwarded, forward-failed,
  local).
- `KvStoreService::scan` (`kv_service.rs`): observe
  `kv.read_bytes_in.bw` / `kv.read_bytes_out.bw` alongside the
  existing combined bandwidth on every scan path.

### Non-redundancy notes

- `read.lease_path.c + read.readindex_path.c` are outcome counters
  (which path served the read), not call counters — the
  `LatencySummary` `read.barrier.l` already carries the total call
  count. Justified under design principle (b) "different
  population/outcome".
- `read.engine_get.l` is a distinct layer from `read.barrier.l` —
  together they roughly sum to the consensus + engine portion of
  `kv.get.lh`. Justified under the latency-hierarchy principle.
- `kv.read_bytes_in.bw` / `kv.read_bytes_out.bw` are not redundant
  with `bytes_in.bw` / `bytes_out.bw` — they measure a
  domain-separated subset (read vs. combined). The combined metrics
  are kept for backward compatibility; read bandwidth lets operators
  derive write bandwidth by subtraction.

### Alternatives considered

- **Per-mode get histograms (`kv.get.linearizable.lh` /
  `kv.get.min_slot.lh`)** — rejected: doubles histogram
  registrations per group for a split that the path counters +
  barrier summary already expose. The end-to-end `kv.get.lh` stays
  combined; operators correlate mode mix via `read.lease_path.c` +
  `read.readindex_path.c` + `read.minslot_fallback.c`.
- **`ReadRegistryHandles` on `PxLocalReplica`** — rejected: the
  barrier lives on `PxGroup`, and `group_safe_slot` is a group-level
  field. Placing handles on `PxGroup` keeps the barrier path
  self-contained and avoids threading handles through method
  arguments.
- **Gauges bridged in `election_metrics_snapshot`** — rejected: that
  path is on-demand (management API only), so gauges would go stale
  between API calls. Bridging at `resolve_read_point` reflects state
  at the most recent actual read, which is what an operator
  diagnosing read performance needs.

## Acceptance Test Plan

- Unit test: register all new handles in a `MetricsRegistry`, flush,
  verify names and types appear in the correct sections.
- Integration test: drive a linearizable get through a leader with a
  valid lease → `read.lease_path.c` increments, `read.barrier.l`
  count increments with near-zero avg, `read.engine_get.l` count
  increments, `kv.read_bytes_in.bw` / `kv.read_bytes_out.bw`
  observe the key/value sizes.
- Integration test: expire the lease (or force ReadIndex path) →
  `read.readindex_path.c` increments, `read.barrier.l` avg is
  non-trivially higher than the lease-path window.
- Integration test: MinSlot read with `min_slot` ahead of
  `contiguous_applied` → `read.minslot_fallback.c` increments.
- Integration test: linearizable get on a non-leader with a known
  leader endpoint → `kv.get_forwarded.c` increments on successful
  forward; kill the leader endpoint → `kv.get_forward_failed.c`
  increments on forward failure.
- Gauge check: after a read, `read.lease_valid.g` matches
  `lease_read_valid(now)`, `read.contiguous_applied.g` matches
  `contiguous_applied()`, `read.safe_slot.g` matches
  `group_safe_slot()`.
