<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Read Flow Analysis

End-to-end trace of the CrowKV read path, from client request to
response. Covers the per-mode routing model, the linearizable read
barrier, follower read semantics, and tracked optimization
opportunities. Mirrors the structure of
[`write-flow-analysis.md`](write-flow-analysis.md).

---

## Read Flow — Point Read (Get)

```
Client GET(key, read_mode, min_slot?)
  → CrowkvClient::get
    1. resolve_min_slot — for MinSlot, auto-attach
       write watermark; for Linearizable, 0
    2. resolve_leader(store_id, group_id) — ALWAYS resolves to the
       cached leader endpoint, regardless of read_mode
    3. send KvGetRequest { key, read_mode, min_slot } to endpoint
    4. retry loop (NotLeaderHint → follow, unknown leader → wait+refresh,
       transport error → backoff+refresh)
  → KvStoreService::get (server-side gRPC handler)
    5. [linearizable only] if local is not leader and not already
       forwarded → forward_kv_get to leader endpoint (at-most-once
       via x-crowkv-forwarded header)
       - on forward success → return leader's response
       - on forward failure → fall through to local store (degraded)
    6. [MinSlot] no forwarding — serve from local store directly
  → PxKvStore::kv_get
    7. resolve_read_point(group, read_mode, min_slot)
       → ReadDecision (see per-mode routing below)
    8. [Serve] learner.engine_get(key) → KVEngine::get(key)
       → Some((resolved_slot, value)) or None
    9. build KvResponse with read_slot + safe_slot
```

### Per-Mode Routing (`resolve_read_point`)

- **Linearizable** — if local replica is the leader:
  - `linearizable_read_barrier()` → lease fast path or ReadIndex
    heartbeat fallback
  - `Ready { read_slot }` → serve from local engine at
    `contiguous_chosen`
  - `NotLeader` → redirect (should not happen after server-side
    forwarding, but handles race / loop-guard)
  - `NoQuorum` → `Unavailable` error
  - If local replica is NOT the leader → `NotLeader` redirect (the
    server-side forwarding in step 5 should have handled this, but
    if forwarding failed or the loop-guard header is set, the store
    returns `NotLeader` rather than serving stale)

- **MinSlot** — if `contiguous_applied >= min_slot`:
  serve locally at `contiguous_applied`. Otherwise: `NotLeader`
  redirect (the local replica has not yet applied the client's last
  write; the leader is guaranteed to have applied it). `min_slot = 0`
  accepts any staleness.

### Linearizable Read Barrier (`linearizable_read_barrier`)

Two paths, mirroring the Raft leader-read fencing model:

1. **Lease fast path** — if `lease_read_valid(now)` is true, the
   leader is guaranteed to be the only leader that could have
   committed anything. Serve immediately at `contiguous_chosen` with
   no round-trip. The lease is renewed by each successful heartbeat
   round and is valid for `lease_duration - max_clock_skew` after
   the last quorum ack.

2. **ReadIndex fallback** — if the lease has expired, run one
   `run_heartbeat_round`. A quorum ack confirms the leader is still
   leader at this term; a higher term steps down; no quorum means
   `NoQuorum`. The read is served at the `contiguous_chosen`
   captured *before* the heartbeat (every slot ≤ this is already
   chosen and applied on the leader).

### Key Data Structures

- **`ReadDecision`** — `Serve { read_slot, safe_slot }` |
  `NotLeader { hint }` | `Unavailable { msg }`. The outcome of
  `resolve_read_point`.
- **`ReadBarrierOutcome`** — `Ready { read_slot }` | `NotLeader` |
  `NoQuorum`. The outcome of `linearizable_read_barrier`.
- **`ReadMode`** (proto enum) — `Linearizable` (0), `MinSlot` (1).
- **`group_safe_slot`** — `min(local contiguous_applied, all voting
  peers' last-reported contiguous_applied)`. Monotone within a
  tenure. The freshness floor for bounded-stale reads.
- **`contiguous_applied`** — highest contiguous slot applied to the
  local engine. Under V1 (apply == learn), tracks
  `contiguous_chosen` directly.

---

## Read Flow — Range Read (Scan)

```
Client SCAN(prefix, start_after, limit, read_mode, min_slot?)
  → CrowkvClient::scan
    1. resolve_min_slot (same as get)
    2. send KvScanRequest to endpoint
    3. retry loop (no NotLeaderHint field in KvScanResponse;
       !ok is a plain error)
  → KvStoreService::scan
    4. [linearizable only] forward_kv_scan to leader (same
       at-most-once pattern as get)
    5. [MinSlot] serve locally
  → PxKvStore::kv_scan
    6. resolve_read_point (min_slot passed through)
    7. [Serve] learner.engine_scan(prefix, start_after, limit)
       → Vec<(key, slot, value)>, truncated
    8. build KvScanResponse with read_slot
```

Scans differ from point reads in two ways:

- **No `not_leader_hint` in `KvScanResponse`** — the client cannot
  follow a hint; any `!ok` is a counted error. Server-side forwarding
  handles linearizable scan redirection transparently.
- **`LatencySummary` instead of `LatencyHistogram`** — scan latency
  uses the lighter-weight summary (count + avg + max) rather than
  fixed-bucket percentiles. Scan latency is dominated by result-set
  size, not protocol overhead, so p50/p99 buckets are less
  informative.

---

## Read Concurrency Model

### No Admission Window

Unlike writes, reads do **not** pass through the `InflightAdmission`
gate (semaphore-based window). There is no `max_inflight_reads`
limit. A burst of read requests is bounded only by:

- Tokio task scheduling (cooperative async)
- Engine `get` / `scan` throughput (lock-free for `InMemKV`; FFI +
  potential demand-load for `CrowtreeEngine`)
- gRPC HTTP/2 stream concurrency

This is intentional: reads are cheap, non-blocking, and do not
consume consensus resources (no WAL, no Paxos rounds in the lease
fast path). Adding an admission window would only be needed if
read volume causes resource starvation for writes.

### Parallel Reads Across Replicas

Reads **can** run in parallel across replicas, but only for
`MinSlot` mode. The current implementation does not exploit this
fully:

- **MinSlot** — the server serves these from the local replica
  without forwarding. If the client sends these requests to different
  replicas (e.g. via a round-robin endpoint selector), they execute in
  parallel on each replica. However, the current client
  (`CrowkvClient`) always resolves to the **leader** endpoint via
  `resolve_leader`, even for MinSlot reads. So in practice, all reads
  go to the leader today.

- **Linearizable** — must be served from the leader (with lease or
  ReadIndex). No parallelism across replicas by definition.

### Gap: Client Always Targets Leader

The client's `resolve_leader` call is used for **all** read modes,
including stale modes. This means:

- Stale reads that could be served from any follower are
  concentrated on the leader, wasting the parallelism available
  from follower replicas.
- The leader carries unnecessary read load that could be
  distributed.

This is a known design choice (simplicity over read scaling). A
future optimization could have the client resolve to any replica
for stale reads, or introduce a server-side read-load-balancer.

---

## Correctness Analysis — User's Thoughts

### "Read does not use multiple window, but it can run parallel at each replica"

**Correct.** Reads do not use the inflight admission window. MinSlot
reads can be served from any replica in parallel — each replica's
engine has its own `get` / `scan` path with no cross-replica
coordination. Only linearizable reads are serialized through the
leader.

### "If the slot is accepted and persistent to crowtree in each replica then read on any replica will return same result"

**Correct.** Once a Paxos slot is chosen (accepted by quorum) and
the value is applied to the engine on each replica, all replicas
have the same `(resolved_slot(k), value)` for that key. The engine
is single-version per key (highest-slot-wins), so reads return the
same result. This is guaranteed by:

- **Engine idempotency**: `KVEngine::apply` is idempotent —
  applying slot `s` for key `k` when `resolved_slot(k) >= s` is a
  no-op.
- **Single versioning**: the engine stores exactly one
  `(slot, value)` per key. No MVCC, no stale versions.
- **Per-key resolved-slot**: `resolved_slot(k)` is monotone per
  key. Two replicas that have applied the same set of slots have
  identical `(resolved_slot(k), value)` for every key.

### "For values not yet delivered to other replicas' learners, we still need to forward request to leader node to get correct result"

**Correct for linearizable reads.** The leader applies the value
locally via `learn_chosen` before returning `ProposeResult::Chosen`
to the client. The leader's `contiguous_applied` is therefore always
up-to-date. Followers receive `ChosenNotification` via the
fire-and-forget `fan_out_chosen_notice` (non-blocking `try_send` on
mpsc), which may lag behind. A follower that has not yet processed
the `ChosenNotification` has a lower `contiguous_applied` and would
return a stale value for a linearizable read.

This is why linearizable reads are forwarded to the leader: the
leader is the only replica guaranteed to have the latest applied
state.

**For MinSlot**: the client carries `min_slot` (e.g. the slot
of its last write). If the follower's `contiguous_applied >=
min_slot`, the follower has applied that write and can serve the
read. If not, the read is redirected to the leader. This is
correct — the follower self-checks before serving. `min_slot = 0`
accepts any staleness.

### Comparison with Raft Cluster Read Implementation

CrowKV's read model closely mirrors Raft's, with Paxos-specific
adaptations:

- **LeaseRead** — identical to Raft. The leader renews its lease
  via heartbeats; while the lease is valid, it serves linearizable
  reads without a quorum round-trip. Both systems use
  `lease_duration - max_clock_skew` as the effective lease.

- **ReadIndex** — identical to Raft. When the lease is expired, the
  leader runs one heartbeat round to confirm it is still leader,
  then serves the read. The read slot is captured *before* the
  heartbeat (equivalent to Raft's "read index" = commit index at
  request time).

- **Follower reads** — Raft supports follower reads for stale
  semantics (e.g. etcd's `serializable` reads). CrowKV's `MinSlot`
  mode with `min_slot = 0` is analogous. The key difference is that
  Raft followers receive log entries via `AppendEntries` (blocking,
  in-order), while CrowKV followers receive `ChosenNotification` via
  fire-and-forget mpsc (best-effort, out-of-order). This means CrowKV
  followers may lag more than Raft followers under load, but the
  correctness argument is the same: stale reads are explicitly
  labeled as stale, and the `safe_slot` provides a freshness bound.

- **MinSlot** — not a standard Raft mode. CrowKV adds this as a
  middle ground: the client carries `min_slot` (e.g. its last write's
  slot), and any replica (including followers) that has applied up to
  that slot can serve the read. This is similar to etcd's
  `linearizable` vs `serializable` distinction but with a
  per-client freshness guarantee.

- **ReadIndex batching** — documented in
  `design-leader-election.md` §7.2 as "not yet implemented". Raft
  implementations (e.g. etcd) batch ReadIndex requests into a
  single heartbeat round. CrowKV's `linearizable_read_barrier`
  serves one read at a time. This is a latency optimization
  opportunity under high linearizable read load.

---

## Read Path Components — Detailed

### Client-Side Read Routing

`CrowkvClient::get` always calls `resolve_leader(store_id,
group_id)` to find the leader endpoint, regardless of `read_mode`.
The topology cache is populated from the HTTP management API
(`/topology`) and updated via `NotLeaderHint` on response errors.

For `MinSlot`, `resolve_min_slot` auto-attaches the
client's write watermark (`write_watermark`) as `min_slot` if the
caller did not supply one. This watermark is the highest `revision`
(slot) the client has observed from its own writes to this group.

**Retry logic**: `NotLeaderHint` with a non-empty hint → follow
immediately (uncounted). "not leader" with empty hint →
`wait_and_refresh_leader` (100ms sleep + topology refresh). Transport
error → exponential backoff + topology refresh. All counted against
`max_retries` except `NotLeaderHint` follows.

### Server-Side Forwarding (gRPC Layer)

`KvStoreService::get` performs transparent leader-forwarding for
linearizable reads only:

1. Check `x-crowkv-forwarded` header (loop-guard, at-most-once hop).
2. If not forwarded and local is not leader → `forward_kv_get` to
   leader endpoint.
3. On forward success → return leader's response (with metrics
   recorded at the forwarding node).
4. On forward failure → fall through to local store (degraded
   read; the store will return `NotLeader` for linearizable mode).

`MinSlot` reads are **never forwarded** — they are served from the
local replica directly.

### Engine Read (`KVEngine::get`)

- **`InMemKV`** — `DashMap::get(key)` → `Option<(slot, value)>`.
  Lock-free, O(1). Trivial latency.
- **`CrowtreeEngine`** — FFI → `ct_get(tree, key)`. Fast path is a
  lock-free memtable lookup (epoch guard + root pointer). Slow path
  is a demand-load miss: the page is fetched from the `BlockPageStore`
  via an io_uring async FFI bridge (`ct_future`), which suspends the
  `KVEngine::get` future until the I/O completes. This is the
  primary latency contributor for cold reads on `CrowtreeEngine`.

### Engine Scan (`KVEngine::scan`)

Ordered prefix scan returning up to `limit` live (non-tombstoned)
entries. For `InMemKV`, this is a sorted iteration over `DashMap`
entries. For `CrowtreeEngine`, it is an in-order traversal of the
memtable's B+tree (or LSM-structured equivalent). No async path
exists yet for scan (no `ct_scan_async` C API).

---

## Current Read Metrics

### Server-Side (KvMetrics in `kv_service.rs`)

- `s.{sid}.g.{gid}.kv.get.lh` — LatencyHistogram for get RPC
  latency (all read modes combined, no per-mode breakdown)
- `s.{sid}.g.{gid}.kv.scan.l` — LatencySummary for scan RPC latency
- `s.{sid}.g.{gid}.kv.bytes_in.bw` — Bandwidth for request bytes
  (shared with writes)
- `s.{sid}.g.{gid}.kv.bytes_out.bw` — Bandwidth for response bytes
  (shared with writes)
- `s.{sid}.g.{gid}.kv.errors.c` — Counter for all KV errors
  (shared with writes)
- `s.{sid}.g.{gid}.kv.no-leader.c` — Counter for "not leader"
  errors (shared with writes)

### Client-Side (ClientMetrics in `metrics.rs`)

- `get_errors` — AtomicU64 counter for get failures
- `scan_errors` — AtomicU64 counter for scan failures
- Window latency histograms (hdrhistogram) for get and scan
- Leader-change episode tracking (shared with writes)

### Consensus-Level (ElectionRegistryHandles in `local_replica.rs`)

- `s.{sid}.g.{gid}.paxos.elections.c` — election count
- `s.{sid}.g.{gid}.paxos.step_downs.*.c` — step-down counters
- `s.{sid}.g.{gid}.paxos.inflight_slots.g` — in-flight proposal gauge

**No read-specific consensus metrics exist today.** There are no
counters for lease-path vs ReadIndex-path reads, no read barrier
latency, no forward count, no read-mode breakdown.

---

## Gaps and Optimization Opportunities

### G1 — No read-mode-specific latency breakdown

**Problem**: All `get` RPCs share a single `get.lh` histogram
regardless of read mode. A linearizable read (which may incur a
heartbeat round-trip) and a MinSlot read with `min_slot = 0` (which
is a local engine lookup) are indistinguishable in the metrics.
Operators cannot diagnose "linearizable reads are slow because the
lease keeps expiring" vs "MinSlot reads are slow because the engine
is slow."

**Fix**: Add per-mode latency histograms or a mode-tagged summary.
Options:
- (a) Separate histograms: `kv.get.linearizable.lh`,
  `kv.get.min_slot.lh`
- (b) Keep one histogram but add per-mode counters so the operator
  can compute per-mode average latency from the summary.

### G2 — No lease vs ReadIndex path counter

**Problem**: The `linearizable_read_barrier` has two paths (lease
fast path, ReadIndex fallback) with very different latency
profiles. No counter distinguishes them. An operator seeing high
linearizable read latency cannot tell whether the lease is
ineffective (causing ReadIndex round-trips) or the engine is slow.

**Fix**: Add two counters in `ElectionRegistryHandles` or a new
`ReadRegistryHandles`:
- `s.{sid}.g.{gid}.read.lease_path.c` — lease fast-path reads
- `s.{sid}.g.{gid}.read.readindex_path.c` — ReadIndex fallback reads

### G3 — No read barrier latency metric

**Problem**: The ReadIndex heartbeat round adds one network
round-trip to linearizable read latency when the lease is expired.
There is no latency metric for the barrier itself, so the operator
cannot distinguish "barrier is slow" from "engine get is slow."

**Fix**: Add a latency summary for the read barrier:
- `s.{sid}.g.{gid}.read.barrier.l` — LatencySummary for
  `linearizable_read_barrier` (includes heartbeat round-trip time
  when ReadIndex path is taken; near-zero for lease path).

### G4 — No forward counter for server-side read forwarding

**Problem**: The server-side gRPC handler forwards linearizable
reads to the leader. No counter tracks how many reads are
forwarded vs served locally. High forward counts indicate the
client is misrouting reads (not using the topology cache
effectively) or the leader has changed and the cache is stale.

**Fix**: Add counters in `KvMetrics`:
- `s.{sid}.g.{gid}.kv.get_forwarded.c` — reads forwarded to leader
- `s.{sid}.g.{gid}.kv.get_forward_failed.c` — forward attempts that
  failed (degraded to local or NotLeader)

### G5 — No engine read latency metric

**Problem**: The `KVEngine::get` call has no dedicated latency
tracking. For `InMemKV` this is trivial, but for `CrowtreeEngine`
the FFI get call can be a significant latency contributor,
especially for demand-load misses (io_uring page fetch). The
`get.lh` histogram covers the entire RPC (barrier + engine), so
the operator cannot isolate engine read latency.

**Fix**: Add a latency summary for engine get:
- `s.{sid}.g.{gid}.read.engine_get.l` — LatencySummary for
  `KVEngine::get` (the thinnest layer; isolates engine cost from
  consensus barrier cost).

This mirrors the write path's `wal.{bl}.append.l` (thinnest-layer
disk IO latency) and `wal.{bl}.fsync.l` (fsync latency).

### G6 — No read bandwidth hierarchy

**Problem**: The write path has `bytes_in.bw` and `bytes_out.bw`
shared across all KV ops (put, get, delete, scan, batch_write).
Reads and writes are indistinguishable in bandwidth metrics. Under
mixed workloads, the operator cannot tell how much bandwidth is
read traffic vs write traffic.

**Fix**: Add read-specific bandwidth metrics:
- `s.{sid}.g.{gid}.kv.read_bytes_in.bw` — request bytes for get +
  scan
- `s.{sid}.g.{gid}.kv.read_bytes_out.bw` — response bytes for get +
  scan

Alternatively, separate per-op-kind bandwidth (get vs scan), but
this may be excessive. A read-vs-write split is the minimum useful
hierarchy.

### G7 — No MinSlot fallback counter

**Problem**: MinSlot reads that cannot be served locally
(`contiguous_applied < min_slot`) are redirected to the leader.
No counter tracks how often this happens. High fallback counts
indicate the follower is lagging behind the client's write rate,
which may signal a replication lag problem.

**Fix**: Add a counter:
- `s.{sid}.g.{gid}.read.minslot_fallback.c` — MinSlot reads
  redirected to leader because local replica hasn't caught up

### G8 — No read parallelism (client always targets leader)

**Problem**: The client always calls `resolve_leader` for all read
modes, including stale modes that could be served from any
follower. This concentrates all read load on the leader, wasting
follower capacity. Under read-heavy workloads, the leader becomes a
bottleneck even for stale reads that don't require leadership.

**Fix**: For MinSlot reads, the client could resolve to any replica
endpoint, not just the leader. This requires:
- Topology cache to expose all replica endpoints, not just the
  leader.
- A read-endpoint selector (round-robin, least-connection, or
  latency-based).
- The server already serves stale reads locally without forwarding,
  so no server-side change is needed.

This is a client-side optimization. Gate behind a config flag
(`read_endpoint_policy = leader | any_replica`).

### G9 — ReadIndex batching not implemented

**Problem**: Documented in `design-leader-election.md` §7.2 as
"not yet implemented." Each linearizable read that falls back to
ReadIndex triggers a separate heartbeat round. Under high
linearizable read load with an expired lease, this multiplies
heartbeat traffic.

**Fix**: Batch multiple pending ReadIndex reads into a single
heartbeat round. The leader maintains a queue of pending read
barriers; when a heartbeat round completes with quorum, all
pending reads are resolved at once. This is a consensus-layer
optimization.

### G10 — No read-specific gauge metrics

**Problem**: The write path exposes `paxos.inflight_slots.g`
(in-flight proposals). There is no equivalent gauge for read-side
state. Useful gauges would include:
- `s.{sid}.g.{gid}.read.lease_valid.g` — 1 if lease is currently
  valid, 0 if expired (useful for correlating with ReadIndex path
  counter)
- `s.{sid}.g.{gid}.read.contiguous_applied.g` — current
  `contiguous_applied` (useful for tracking follower lag; already
  available in `GroupStatus` but not in the metrics log)
- `s.{sid}.g.{gid}.read.safe_slot.g` — current `group_safe_slot`
  (same; available in status but not in metrics log)

---

## Proposed Read Metrics Hierarchy

Mirroring the write path's hierarchy (feature-layer latency +
thinnest-layer latency + bandwidth + counters + gauges):

### Latency Hierarchy

- `s.{sid}.g.{gid}.kv.get.lh` — **existing** — get RPC end-to-end
  (feature layer)
- `s.{sid}.g.{gid}.read.barrier.l` — **new** — linearizable read
  barrier latency (consensus layer; near-zero for lease path,
  one heartbeat RTT for ReadIndex)
- `s.{sid}.g.{gid}.read.engine_get.l` — **new** — engine `get`
  latency (thinnest layer; isolates engine cost)
- `s.{sid}.g.{gid}.kv.scan.l` — **existing** — scan RPC end-to-end
  (feature layer)

### Bandwidth Hierarchy

- `s.{sid}.g.{gid}.kv.read_bytes_in.bw` — **new** — read request
  bytes (key sizes for get + prefix sizes for scan)
- `s.{sid}.g.{gid}.kv.read_bytes_out.bw` — **new** — read response
  bytes (value sizes for get + item sizes for scan)
- `s.{sid}.g.{gid}.kv.bytes_in.bw` — **existing** — all KV request
  bytes (read + write combined; keep for backward compat)
- `s.{sid}.g.{gid}.kv.bytes_out.bw` — **existing** — all KV
  response bytes (read + write combined; keep for backward compat)

### Counters

- `s.{sid}.g.{gid}.read.lease_path.c` — **new** — linearizable
  reads served via lease fast path
- `s.{sid}.g.{gid}.read.readindex_path.c` — **new** — linearizable
  reads served via ReadIndex fallback
- `s.{sid}.g.{gid}.kv.get_forwarded.c` — **new** — reads forwarded
  to leader (server-side)
- `s.{sid}.g.{gid}.kv.get_forward_failed.c` — **new** — forward
  attempts that failed
- `s.{sid}.g.{gid}.read.minslot_fallback.c` — **new** — MinSlot
  reads redirected to leader
- `s.{sid}.g.{gid}.kv.errors.c` — **existing** — all KV errors
  (shared)
- `s.{sid}.g.{gid}.kv.no-leader.c` — **existing** — "not leader"
  errors (shared)

### Gauges

- `s.{sid}.g.{gid}.read.lease_valid.g` — **new** — 1 if leader's
  read lease is valid, 0 if expired
- `s.{sid}.g.{gid}.read.contiguous_applied.g` — **new** — current
  `contiguous_applied` (also in `GroupStatus` but not in metrics
  log)
- `s.{sid}.g.{gid}.read.safe_slot.g` — **new** — current
  `group_safe_slot` (also in `GroupStatus` but not in metrics log)

### Design Principles Applied

Following `design-observability.md`:

- **Latency Hierarchy** — get RPC (feature layer) ≈ barrier
  (consensus layer) + engine_get (thinnest layer). A persistent gap
  between get.lh and (barrier.l + engine_get.l) indicates
  unaccounted overhead (gRPC serialization, forwarding hop).
- **Bandwidth Hierarchy** — read_bytes_in/out separate from the
  combined bytes_in/out, domain-separated (read vs write traffic).
- **Counter/Summary Non-Redundancy** — `lease_path.c` and
  `readindex_path.c` are outcome counters (different populations),
  not redundant with `get.lh` (which carries count). `minslot_fallback.c`
  is a different outcome (redirected vs served). The forward
  counters are a different population (forwarded vs local).
- **Gauge** — `lease_valid.g` is a state gauge (can change without
  an event). `contiguous_applied.g` and `safe_slot.g` are bridged
  from existing atomics (no new state tracking needed).

---

## Summary

The read flow is simpler than the write flow: no Paxos rounds, no
WAL, no admission window. The primary latency contributors are:

1. **Linearizable read barrier** — zero (lease path) or one heartbeat
   RTT (ReadIndex path)
2. **Engine get** — trivial (InMemKV) or FFI + possible demand-load
   (CrowtreeEngine)
3. **Server-side forwarding** — one extra hop for linearizable reads
   on non-leader replicas

The user's analysis is correct:

- Reads do not use the inflight window and can run in parallel across
  replicas for stale modes.
- Once a value is chosen and applied on each replica, all replicas
  return the same result (single-version per key, idempotent apply).
- For values not yet delivered to follower learners, linearizable
  reads must go to the leader (the leader always has the latest
  applied state; followers may lag due to fire-and-forget
  `ChosenNotification`).

The main gaps are in **metrics instrumentation** (G1–G7, G10) and
**read parallelism** (G8). The metrics gaps mean operators cannot
diagnose read performance issues at the same granularity as write
issues. The read parallelism gap means stale read capacity is
concentrated on the leader rather than distributed across replicas.
ReadIndex batching (G9) is a documented future optimization.

---

## Memory Copy Analysis

Audit of every point in the read path where data is allocated or
copied. The read path is much simpler than the write path — no Paxos
rounds, no WAL, no payload encoding. The primary data movement is the
key (request) and value (response).

### Notation

- **O(n) copy** = heap allocate + memcpy, proportional to key or value
  size. These matter for large values (e.g. 1 MB).
- **O(1) ref-count** = atomic ref-count increment. Negligible.
- **move** = ownership transfer, zero cost.

### Read Path: Client → Engine → Response

**Step 1 — Client sends key** (`crowkv-client/src/client.rs`)
The client sends `KvGetRequest { key: Vec<u8>, ... }` via gRPC.
- **O(n) copy** — gRPC serializes the key into the HTTP/2 frame
  buffer. Unavoidable for network transport.

**Step 2 — Server gRPC deserialization**
The gRPC framework deserializes the request into `KvGetRequest`.
- **O(n) copy** — allocates `Vec<u8>` for the key from the network
  frame. Unavoidable for gRPC.

**Step 3 — `kv_get` → `engine_get`** (`px_kv_store.rs:52`,
`learner.rs:120`)
`group.local_replica().learner.engine_get(key)` where `key: &[u8]`.
- **No copy** — passes a slice reference to the engine.

**Step 4a — InMemKV get** (test-only engine)
`DashMap::get(key)` → `Option<(slot, value)>` where `value: Vec<u8>`.
- **No copy** — returns a reference to the stored value. The caller
  receives an owned `Vec<u8>` via `DashMap`'s `get` returning a ref,
  but the `KVEngine::get` trait returns `Vec<u8>` — so the value is
  cloned out of the `DashMap` on return.
- **O(n) copy** — value cloned from the map's internal storage.

**Step 4b — CrowtreeEngine get (fast path)** (`crowtree_engine.rs:168`,
`ffi/src/lib.rs:1337`)
`try_get(key.to_vec())` → `ct_get_async` → `ct_future_poll` →
`copy_buf(value)`.
- **O(n) copy (key)** — `key.to_vec()` allocates a copy of the key
  for the FFI call. The C API takes `*const u8, len`, so the key
  could theoretically be passed as a borrow without copying, but the
  async API wraps it in a `Vec<u8>` for `Send` safety across the
  reactor boundary.
- **O(n) copy (value)** — `copy_buf` does
  `slice::from_raw_parts(..).to_vec()`. The C++ engine returns a
  `ct_buf` that may be a borrowed pointer into a still-live frame
  (zero-copy fast path within C++), but the Rust side must copy it
  out because the epoch guard is released immediately after
  `ct_future_free`. So the value is always copied from the engine's
  internal buffer into an owned `Vec<u8>`.

**Step 4c — CrowtreeEngine get (slow path / demand-load miss)**
Same as fast path, but `ct_future_poll` returns `done == 0`, so the
  future suspends until the io_uring page fetch completes. On
  completion, the same `copy_buf` path runs.
- **O(n) copy (value)** — same as fast path. Additionally, the C++
  engine copies the page from disk into the buffer pool, but that is
  internal to the engine and not counted here.

**Step 5 — Response construction** (`kv_response.rs:69`)
`KvResponse::ok_value_with_revision(value, slot, ...)` takes
`value: Vec<u8>` by value.
- **move** — the `Vec<u8>` from the engine is moved into the response
  struct. No copy.

**Step 6 — gRPC response serialization**
The gRPC framework serializes `KvResponse` into the HTTP/2 frame
buffer.
- **O(n) copy** — the value is copied into the socket buffer.
  Unavoidable for network transport.

### Scan Path: Client → Engine → Response

**Step 1-3** — same as point read (key/prefix sent, deserialized,
passed to engine as `&[u8]`).

**Step 4 — CrowtreeEngine scan** (`crowtree_engine.rs:174`,
`ffi/src/lib.rs:1357`)
`scan(prefix.to_vec(), limit)` → `ct_scan_async` →
`decode_scan(&out.value, ..)`.
- **O(n) copy (prefix)** — `prefix.to_vec()` for FFI.
- **O(n) copy (results)** — `take_buf(value)` copies the entire
  packed scan result buffer from the C++ engine. Then `decode_scan`
  unpacks it into `Vec<ScanEntry>`, where each `ScanEntry` owns
  `key: Vec<u8>` and `value: Vec<u8>` — additional per-entry
  allocations.

**Step 5 — Response construction**
`Vec<(Vec<u8>, u64, Vec<u8>)>` from the engine is moved into the
gRPC response. Each entry's key and value are moved, not copied.
- **move** — no copy at this step.

**Step 6 — gRPC serialization**
- **O(n) copy** — all entries serialized into the socket buffer.
  Unavoidable.

### Summary Table

- **O(n) copies (unavoidable):**
  - gRPC request deserialization — key from network frame.
  - gRPC response serialization — value(s) into socket buffer.
  - Engine get (InMemKV) — value cloned from internal map.
  - Engine get (Crowtree) — key `to_vec()` for FFI; value `copy_buf`
    from C++ buffer (epoch guard lifetime constraint).
  - Engine scan (Crowtree) — prefix `to_vec()`; packed result
    `take_buf`; per-entry `Vec<u8>` allocations in `decode_scan`.

- **O(n) copies (potentially avoidable):**
  - Engine get key copy (`key.to_vec()`) — the C API takes
    `*const u8, len`, so a borrow could work if the async FFI
    wrapper did not require `Send` ownership. Would need an API
    change to pass a raw pointer + lifetime guarantee instead of
    `Vec<u8>`.
  - Engine get value copy (`copy_buf`) — the C++ engine's zero-copy
    fast path returns a borrowed pointer into a frame, but the Rust
    side must copy because the epoch guard is released before the
    value is returned. Could be eliminated with a "pinned value"
    API that extends the epoch guard lifetime to the Rust caller,
    but this would require a new C API and careful lifetime
    management.

- **Zero-copy (move or borrow):**
  - `kv_get` → `engine_get` passes `&[u8]` (no key copy at the
    Rust call boundary).
  - Response `Vec<u8>` moved into `KvResponse` (no copy).

### Comparison with Write Path

The read path has fewer O(n) copies than the write path:
- **No WAL encode/replay** — reads do not touch the WAL.
- **No payload encoding** — reads pass the key directly, no
  `encode_kv_payload` step.
- **No Batch decode** — reads do not decode a `Batch`; the engine
  returns the value directly.
- **No FFI batch encode** — reads use `ct_get` / `ct_scan`, not
  `ct_apply_batch_slices`.

The main O(n) copy unique to the read path is the engine value copy
(`copy_buf` for Crowtree, clone for InMemKV). This is structurally
unavoidable: the engine owns its internal storage, and the caller
needs an owned `Vec<u8>` to return via gRPC. The only way to
eliminate it would be a zero-copy engine read API that returns a
borrowed reference with a guarded lifetime — a significant engine
API change.
