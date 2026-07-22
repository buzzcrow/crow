<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R19: Read performance profiling and metrics

**Problem**: The write path has a well-instrumented
latency-bandwidth-counter hierarchy (WAL append/fsync latency, write
bandwidth, election counters, inflight gauge). The read path has
only a single `get.lh` histogram and a `scan.l` summary — no
per-mode breakdown, no consensus-layer metrics (lease vs ReadIndex),
no engine-layer latency, no read-specific bandwidth, and no
forward/fallback counters. Operators cannot diagnose read
performance issues at the same granularity as write issues.

Full analysis in
[`read-flow-analysis.md`](../working/read-flow-analysis.md).

**Approach**: Add a read metrics hierarchy mirroring the write
path's structure, following the design principles in
`../design/design-observability.md`:

- **Latency hierarchy** (feature layer → thinnest layer):
  - `kv.get.lh` — existing, get RPC end-to-end
  - `read.barrier.l` — new LatencySummary for
    `linearizable_read_barrier` (near-zero for lease path, one
    heartbeat RTT for ReadIndex)
  - `read.engine_get.l` — new LatencySummary for `KVEngine::get`
    (isolates engine cost from consensus barrier cost)
  - `kv.scan.l` — existing, scan RPC end-to-end
- **Bandwidth hierarchy** (read vs write separation):
  - `kv.read_bytes_in.bw` / `kv.read_bytes_out.bw` — new, read
    traffic separated from the combined `bytes_in/out.bw`
- **Counters** (outcome / population separation):
  - `read.lease_path.c` — linearizable reads via lease fast path
  - `read.readindex_path.c` — linearizable reads via ReadIndex
    fallback
  - `kv.get_forwarded.c` — reads forwarded to leader (server-side)
  - `kv.get_forward_failed.c` — forward attempts that failed
  - `read.minslot_fallback.c` — MinSlot reads redirected to leader
    because the local replica hasn't caught up
- **Gauges** (state, bridged from existing atomics):
  - `read.lease_valid.g` — 1 if leader's read lease is valid
  - `read.contiguous_applied.g` — current `contiguous_applied`
  - `read.safe_slot.g` — current `group_safe_slot`

**Priority**: Medium — read performance is undiagnosable today;
the metrics infrastructure (MetricsRegistry, KvMetrics,
ElectionRegistryHandles) already exists and just needs new handles
wired in.

**Complexity**: Medium — new metric handles in `KvMetrics` and
`ElectionRegistryHandles` (or a new `ReadRegistryHandles`), timing
instrumentation in `linearizable_read_barrier`, `resolve_read_point`,
`KvStoreService::get/scan`, and `PxLearner::engine_get`. No
algorithm or protocol change.

**Files**: `crowkv/src/rpc/kv_service.rs` (KvMetrics — new handles,
forward/fallback counters, read bandwidth), `crowkv/src/cluster/
local_replica.rs` (ReadRegistryHandles — lease/ReadIndex counters,
barrier latency, gauges), `crowkv/src/cluster/group_election.rs`
(`linearizable_read_barrier` — timing + path counter),
`crowkv/src/cluster/px_kv_store.rs` (`resolve_read_point` — MinSlot
fallback counter), `crowkv/src/paxos/learner.rs`
(`engine_get` — timing).

**Acceptance**:
- Metrics log shows read-specific counters, latency summaries,
  bandwidth, and gauges per (store, group).
- `read.lease_path.c + read.readindex_path.c` equals the total
  linearizable get count in the same window.
- `read.barrier.l` avg is near-zero when lease is valid; matches
  heartbeat RTT when ReadIndex path is taken.
- `read.engine_get.l` isolates engine cost (trivial for InMemKV,
  measurable for CrowtreeEngine demand-load misses).
- Read bandwidth (`read_bytes_in/out.bw`) + write bandwidth
  (derived: `bytes_in/out.bw` minus read) accounts for total KV
  bandwidth.
