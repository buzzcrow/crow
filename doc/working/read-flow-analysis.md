<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Read Flow Analysis

End-to-end trace of the CrowKV read path. Mirrors the structure of
[`write-flow-analysis.md`](write-flow-analysis.md). Focuses on flow,
conclusions, and data — not rationale prose.

---

## Read Flow — Point Read (Get)

```
Client GET(key, read_mode, min_slot?)
  → CrowkvClient::get
    1. resolve_min_slot — MinSlot: auto-attach write watermark;
       Linearizable: 0
    2. resolve_leader(store_id, group_id) — ALWAYS cached leader
       endpoint, regardless of read_mode
    3. send KvGetRequest { key, read_mode, min_slot }
       [copy: key → HTTP/2 frame, unavoidable]
    4. retry: NotLeaderHint → follow; empty hint → wait+refresh;
       transport error → backoff+refresh
  → KvStoreService::get (gRPC)
    5. [Linearizable] if local not leader and not already forwarded →
       forward_kv_get to leader (at-most-once via x-crowkv-forwarded)
       - success → return leader's response
       - failure → fall through to local store (degraded)
    6. [MinSlot] no forwarding — serve local
       [copy: key allocated from network frame, unavoidable]
  → PxKvStore::kv_get
    7. resolve_read_point(group, read_mode, min_slot) → ReadDecision
    8. [Serve] learner.engine_get_bytes(key: &[u8]) → KVEngine::get_bytes
       → Some((resolved_slot, Bytes)) or None
       [no key copy at Rust boundary]
       [value copy: InMemKV clones out of DashMap; Crowtree fast path
        one copy frame→Bytes (R21 eliminated the intermediate Vec);
        Crowtree slow path one copy I/O buf→Bytes on reactor thread]
    9. build KvResponse { read_slot, safe_slot, value: Bytes }
       [move: Bytes moved into response, no copy]
       [copy: value → socket buffer on gRPC serialize, unavoidable]
```

### Per-Mode Routing (`resolve_read_point`)

- **Linearizable** — local is leader:
  - `linearizable_read_barrier()` → lease fast path or ReadIndex
    fallback → `Ready { read_slot }` serve at `contiguous_chosen`
  - `NotLeader` → redirect (race / loop-guard after forwarding failed)
  - `NoQuorum` → `Unavailable`
  - local not leader → `NotLeader` redirect (forwarding should have
    handled it; falls back to `NotLeader` rather than serving stale)
- **MinSlot** — `contiguous_applied >= min_slot` → serve locally at
  `contiguous_applied`; else `NotLeader` redirect (follower hasn't
  applied the client's last write; leader has). `min_slot = 0` accepts
  any staleness.

### Linearizable Read Barrier

1. **Lease fast path** — `lease_read_valid(now)` true → serve at
   `contiguous_chosen` with no round-trip. Lease renewed by each
   successful heartbeat; valid for `lease_duration - max_clock_skew`
   after the last quorum ack.
2. **ReadIndex fallback** — lease expired → one `run_heartbeat_round`.
   Quorum ack confirms leadership at this term; higher term steps
   down; no quorum → `NoQuorum`. Read served at `contiguous_chosen`
   captured *before* the heartbeat.

### Key Data Structures

- **`ReadDecision`** — `Serve { read_slot, safe_slot }` |
  `NotLeader { hint }` | `Unavailable { msg }`. Output of
  `resolve_read_point`.
- **`ReadBarrierOutcome`** — `Ready { read_slot }` | `NotLeader` |
  `NoQuorum`. Output of `linearizable_read_barrier`.
- **`ReadMode`** (proto) — `Linearizable` (0), `MinSlot` (1).
- **`group_safe_slot`** — `min(local contiguous_applied, all voting
  peers' last-reported contiguous_applied)`. Monotone within a tenure;
  freshness floor for bounded-stale reads.
- **`contiguous_applied`** — highest contiguous slot applied to the
  local engine. Under V1 (apply == learn) tracks
  `contiguous_chosen` directly.

---

## Read Flow — Range Read (Scan)

```
Client SCAN(prefix, start_after, limit, read_mode, min_slot?)
  → CrowkvClient::scan
    1. resolve_min_slot (same as get)
    2. send KvScanRequest
       [copy: prefix → HTTP/2 frame, unavoidable]
    3. retry: no NotLeaderHint in KvScanResponse; !ok is a plain error
  → KvStoreService::scan
    4. [Linearizable] forward_kv_scan to leader (same at-most-once)
    5. [MinSlot] serve local
       [copy: prefix allocated from network frame, unavoidable]
  → PxKvStore::kv_scan
    6. resolve_read_point (min_slot passed through)
    7. [Serve] learner.engine_scan(prefix, start_after, limit)
       → Vec<(key, slot, value)>, truncated
       [Crowtree: prefix to_vec() for FFI; packed result take_buf;
        per-entry Vec<u8> for key+value in decode_scan]
    8. build KvScanResponse { read_slot, entries }
       [move: entries moved into response, no copy]
       [copy: entries → socket buffer on gRPC serialize, unavoidable]
```

- **No `not_leader_hint` in `KvScanResponse`** — client can't follow a
  hint; any `!ok` is a counted error. Server-side forwarding handles
  linearizable redirection transparently.
- **`LatencySummary` not `LatencyHistogram`** — scan latency is
  dominated by result-set size, so fixed-bucket percentiles are less
  informative than count + avg + max.

---

## Read Concurrency

- **No admission window** — reads bypass `InflightAdmission`; no
  `max_inflight_reads`. Burst bounded only by Tokio scheduling, engine
  throughput (lock-free `InMemKV`; FFI + demand-load for
  `CrowtreeEngine`), and HTTP/2 stream concurrency. Intentional: reads
  are cheap, non-blocking, and lease-path reads consume no consensus
  resources.
- **Parallel across replicas — MinSlot only.** Each replica's engine
  has its own `get`/`scan` path with no cross-replica coordination.
  Linearizable reads are serialized through the leader by definition.
- **Gap: client always targets leader.** `resolve_leader` is used for
  all read modes including stale. Stale-read capacity on followers is
  wasted; leader carries unnecessary read load. Design choice
  (simplicity over read scaling); future optimization could resolve to
  any replica for MinSlot, or add a server-side read-load-balancer.

---

## Correctness Conclusions

- **No inflight window; parallel per replica for stale modes** —
  correct. Only linearizable reads serialize through the leader.
- **Chosen + applied on every replica ⇒ identical read result** —
  correct. Engine is single-version per key (highest-slot-wins);
  `KVEngine::apply` is idempotent (applying slot `s` when
  `resolved_slot(k) >= s` is a no-op); `resolved_slot(k)` is monotone
  per key. Two replicas that applied the same slots have identical
  `(resolved_slot(k), value)` for every key.
- **Values not yet delivered to follower learners ⇒ forward
  linearizable read to leader** — correct. Leader applies via
  `learn_chosen` before returning `ProposeResult::Chosen`, so its
  `contiguous_applied` is always up-to-date. Followers receive
  `ChosenNotification` via fire-and-forget mpsc (`try_send`), which may
  lag. For MinSlot, the follower self-checks
  `contiguous_applied >= min_slot` before serving; `min_slot = 0`
  accepts any staleness.

---

## Read Path Components

- **Client routing** — `CrowkvClient::get` always calls
  `resolve_leader`. Topology cache populated from `/topology`, updated
  via `NotLeaderHint`. For MinSlot, `resolve_min_slot` auto-attaches
  the client's `write_watermark` (highest `revision` observed from its
  own writes to this group) if caller omitted `min_slot`. Retry:
  `NotLeaderHint` non-empty → follow (uncounted); empty hint →
  `wait_and_refresh_leader` (100ms + refresh); transport error →
  backoff + refresh. All counted against `max_retries` except hint
  follows.
- **Server forwarding** — `KvStoreService::get`: check
  `x-crowkv-forwarded` (loop-guard); if not forwarded and local not
  leader → `forward_kv_get`; success → return leader response (metrics
  recorded at forwarder); failure → fall through (degraded; store
  returns `NotLeader` for linearizable). MinSlot never forwarded.
- **Engine get** — `InMemKV`: `DashMap::get` → ref, cloned out via
  trait return → `Bytes::from(vec)`. `CrowtreeEngine`: FFI
  `ct_get_async` → fast path lock-free memtable lookup with epoch
  guard; slow path io_uring demand-load via `ct_future` (primary
  latency contributor for cold reads).
- **Engine scan** — ordered prefix scan, up to `limit` live
  (non-tombstoned) entries. `InMemKV`: sorted `DashMap` iteration.
  `CrowtreeEngine`: in-order memtable traversal. No async scan C API
  yet.

---

## Gaps and Optimization Opportunities

- **G1 — No per-mode latency breakdown.** `get.lh` merges linearizable
  (may incur heartbeat RTT) with MinSlot (local lookup). Fix: separate
  `kv.get.linearizable.lh` / `kv.get.min_slot.lh`, or per-mode counters
  + shared histogram.
- **G2 — No lease vs ReadIndex counter.** Can't tell whether
  linearizable latency is lease ineffectiveness or engine slowness.
  Fix: `read.lease_path.c`, `read.readindex_path.c`.
- **G3 — No read barrier latency.** Can't separate barrier RTT from
  engine get. Fix: `read.barrier.l` (LatencySummary; near-zero lease,
  one RTT ReadIndex).
- **G4 — No forward counter.** Can't tell client misrouting from
  cache-stale leader changes. Fix: `kv.get_forwarded.c`,
  `kv.get_forward_failed.c`.
- **G5 — No engine read latency.** `get.lh` covers barrier + engine;
  can't isolate engine cost (matters for `CrowtreeEngine` demand-load).
  Fix: `read.engine_get.l` (mirrors write path's
  `wal.{bl}.append.l` / `fsync.l` thinnest-layer pattern).
- **G6 — No read bandwidth hierarchy.** `bytes_in/out.bw` shared with
  writes; mixed workloads indistinguishable. Fix:
  `kv.read_bytes_in.bw`, `kv.read_bytes_out.bw` (keep combined for
  backward compat).
- **G7 — No MinSlot fallback counter.** Can't detect follower lag
  driving redirects. Fix: `read.minslot_fallback.c`.
- **G8 — No read parallelism (resolved by R26).** Client previously
  targeted leader for all modes; stale-read capacity on followers
  wasted. Fix (shipped): client resolves to any replica for MinSlot
  via `read_endpoint_policy = any_replica`; topology cache exposes all
  endpoints; round-robin read-endpoint selector with `NotLeader`-hint
  fallback. Least-conn / latency policies remain future work.
- **G9 — ReadIndex batching not implemented.** Documented in
  `design-leader-election.md` §7.2. Each expired-lease linearizable
  read triggers a separate heartbeat. Fix: queue pending barriers,
  resolve all on one quorum ack.
- **G10 — No read-specific gauges.** Fix: `read.lease_valid.g` (1/0),
  `read.contiguous_applied.g`, `read.safe_slot.g` (latter two bridged
  from existing atomics already in `GroupStatus`).

---

## Summary

The read flow is simpler than the write flow: no Paxos rounds, no WAL,
no admission window. Primary latency contributors:

1. **Linearizable read barrier** — zero (lease) or one heartbeat RTT
   (ReadIndex).
2. **Engine get** — trivial (`InMemKV`) or FFI + possible demand-load
   (`CrowtreeEngine`).
3. **Server-side forwarding** — one extra hop for linearizable reads
   on non-leader replicas.

User analysis confirmed: no inflight window; parallel across replicas
for stale modes; chosen+applied ⇒ identical results across replicas
(single-version, idempotent apply); undelivered follower values require
forwarding linearizable reads to the leader (MinSlot self-checks
instead).

Main gaps: **metrics instrumentation** (G1–G7, G10) — operators can't
diagnose reads at write-path granularity; **read parallelism** (G8,
resolved by R26) — stale-read capacity now distributed via
`read_endpoint_policy = any_replica`; **ReadIndex batching** (G9) —
documented future optimization.

---

## Memory Copy Summary

Copy points are annotated inline in the flow diagrams above. Summary
of what remains:

- **O(n) unavoidable** — gRPC request deserialize (key/prefix from
  network frame); gRPC response serialize (value(s) into socket);
  `InMemKV` get (value cloned from `DashMap`); `CrowtreeEngine` get
  (one copy: frame → `Bytes` fast path, or I/O buffer → `Bytes` slow
  path); `CrowtreeEngine` scan (prefix `to_vec()` for FFI, packed
  result `take_buf`, per-entry `Vec<u8>` in `decode_scan`).
- **Eliminated by R21** — engine get key copy (Rust-side `to_vec()`
  gone; C API copies internally); engine get intermediate `Vec<u8>`
  (fast path returns `PinnedValue` borrowing the C++ frame; final
  `Bytes` produced in one copy instead of frame → `Vec` → `Bytes`).
- **Remaining, blocked by R6** — engine get value copy (frame →
  `Bytes`): true zero-copy via `Bytes::from_raw_parts` with a drop
  calling `ct_future_free` would eliminate it, but `Bytes` is `Send`
  and could be dropped on another thread while the epoch guard must be
  released on the entering thread (thread-local). Blocked by R6
  (cross-thread epoch guard).
- **Zero-copy (move/borrow)** — `kv_get` → `engine_get_bytes` passes
  `&[u8]` (no key copy at the Rust boundary); response `Bytes` moved
  into `KvResponse` (no copy).

The read path has fewer O(n) copies than the write path: no WAL
encode/replay, no payload encoding, no Batch decode, no FFI batch
encode. After R21, the read path's remaining engine value copy
(frame → `Bytes`) is structurally similar to the write path's: the
engine owns its internal storage and the caller needs an owned `Bytes`
for gRPC. True zero-copy is blocked by R6 (cross-thread epoch guard),
since `Bytes` is `Send` but the guard is thread-local.
