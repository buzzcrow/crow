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
- **G9 — ReadIndex batching (resolved by R27).** Documented in
  `design-leader-election.md` §7.2. Each expired-lease linearizable
  read previously triggered a separate heartbeat; concurrent barriers
  now coalesce onto one in-flight round (queue pending barriers,
  resolve all on one quorum ack).
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
`read_endpoint_policy = any_replica`; **ReadIndex batching** (G9,
resolved by R27) — concurrent barriers now coalesce onto one
heartbeat round.

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

---

## Benchmark Results — 2026-07-23

3-node cluster, in-memory WAL + in-memory KV (mem-block), read-only,
512-byte values, 200K key space pre-populated with deterministic
per-byte hash values (`byte_at(key_id, offset) = splitmix64(key_id ^
splitmix64(offset)) mod 256`), 12-second measurement window,
`election_profile = e2e`. Reads draw uniformly from `[0, 200K)`.
Correctness spot-check disabled (`--verify-bytes 0`) for clean
throughput; verified separately with `--verify-bytes 8`
(`correctness_errors = 0` across all configs). Pre-population takes
~22s for 200K keys (excluded from measurement; reported as
`pre_pop_ms`).

Benchmark scripts: full sweep `tools/bench-read-sweep.sh`, regression
subset `tools/bench-read-regression.sh`.

Two read modes benchmarked, both at 1 thread : 1 connection (no
HTTP/2 stream multiplexing overhead — see note below):

- **Linearizable** — lease fast path (barrier ~0 when the leader's
  lease is valid), ReadIndex fallback when the lease has expired.
  Client `read_endpoint_policy = Leader` (always targets leader).
- **MinSlot `min_slot=0` + AnyReplica** — pure local serve (no
  barrier, any staleness accepted), reads distributed round-robin
  across all 3 replicas. Each replica handles 1/3 of reads, removing
  the leader as the single bottleneck. `read_endpoint_distributed`
  confirms reads reach followers; `read_endpoint_fallback = 0`
  (no redirects — `min_slot=0` always serves locally).

### Scaling: 1T:1C — Linearizable

| Threads | Conn | Throughput (ops/s) | avg (µs) | p50 (µs) | p99 (µs) | p999 (µs) | Errors |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 3 | 3 | 32,318 | 91 | 72 | 201 | 287 | 0 |
| 6 | 6 | 35,003 | 170 | 162 | 368 | 453 | 0 |
| 12 | 12 | 75,278 | 157 | 154 | 250 | 289 | 0 |
| 24 | 24 | 86,556 | 275 | 273 | 407 | 467 | 0 |
| 48 | 48 | 90,381 | 528 | 531 | 743 | 837 | 0 |

### Scaling: 1T:1C — MinSlot `min_slot=0` + AnyReplica

| Threads | Conn | Throughput (ops/s) | avg (µs) | p50 (µs) | p99 (µs) | p999 (µs) | Errors |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 3 | 3 | 31,852 | 93 | 83 | 150 | 206 | 0 |
| 6 | 6 | 30,807 | 193 | 171 | 337 | 432 | 0 |
| 12 | 12 | 46,800 | 254 | 230 | 429 | 577 | 0 |
| 24 | 24 | 60,399 | 395 | 369 | 541 | 658 | 0 |
| 48 | 48 | 85,136 | 561 | 550 | 761 | 867 | 0 |

### Note: 1T:2C boosts MinSlot (extra connection per replica)

MinSlot benefits from more connections per replica — 2 connections
per replica per thread (1T:2C) keeps each replica's pipeline fuller.
Linearizable does not benefit (the single leader is already
pipeline-saturated at 1T:1C).

| Threads | MinSlot 1T:1C | MinSlot 1T:2C | Gain |
| --- | --- | --- | --- |
| 3 | 31,852 | 36,532 | +15% |
| 6 | 30,807 | 59,042 | +92% |
| 12 | 46,800 | 77,352 | +65% |
| 24 | 60,399 | 80,679 | +34% |

At 6T, 1T:2C nearly doubles throughput (31K → 59K) — 1 connection per
replica was starving the replica's pipeline; 2 connections keeps it
fed. At 24T the gain narrows to +34% as replicas approach saturation.

With 1T:2C, MinSlot beats Linearizable at low-mid concurrency:
6T 59K vs 35K (1.7×), 12T 77K vs 75K (1.03×). At 24T+ the leader's
deeper pipeline catches up (Linearizable 87K vs MinSlot 81K).

### Conclusions

- **Reads are ~1.8× faster than writes at peak** — write peak ~50K
  ops/s (write-flow § Benchmark Results); Linearizable read peak
  ~90K ops/s (48T/48C). Reads skip the consensus critical path (no
  WAL append, no quorum RPC) — the lease barrier is ~0 when the
  leader's lease is valid, so a linearizable read is just engine get
  + gRPC RTT.
- **Linearizable scales cleanly to 90K at 48T** — zero errors across
  all 5 configs, p99 stays under 743µs. The single leader's gRPC
  pipeline absorbs 48 concurrent connections without degradation.
  No oversaturation cliff within the tested range.
- **MinSlot 1T:1C underperforms Linearizable at 1T:1C** — the
  per-request round-robin across 3 replicas adds selection overhead
  (topology cache lookup + atomic cursor) without enough pipeline
  depth per replica. At 6T/6C, MinSlot (30.8K) is slower than
  Linearizable (35.0K). The advantage appears only at 48T (85K vs
  90K — within noise).
- **MinSlot 1T:2C is the optimal MinSlot config** — 2 connections
  per replica per thread keeps each replica's pipeline full. At 6T,
  MinSlot 1T:2C (59K) is 1.7× Linearizable 1T:1C (35K). The win is
  distributing reads across 3 replicas × 2 connections = 6 parallel
  pipelines vs the leader's 6.
- **HTTP/2 stream sharing hurts throughput** — 2 threads sharing 1
  connection (2T:1C) drops MinSlot throughput 17% at 6T (31K → 26K).
  HTTP/2 multiplexing overhead: concurrent streams on one connection
  contend on the frame layer. 1T:1C (dedicated connection per
  thread) is the sweet spot for both modes.
- **Correctness verified separately** — `correctness_errors = 0`
  across all configs when `--verify-bytes 8` is enabled. The
  deterministic `byte_at(key_id, offset)` formula + 8-random-byte
  spot-check confirms the read path returns the exact bytes written
  by pre-population. Verification costs ~7% throughput (10.3K vs
  9.6K at 1T/1C Linearizable) — disabled in the scaling tables for
  clean throughput numbers.
- **`not_found` near 0** — a handful of reads returned `NotFound`
  (0–927 per run) from pre-population gaps (retry-exhausted writes
  during the 22s pre-pop phase). These are counted separately and
  are not correctness errors.

### gRPC transport: HTTP/2 connection lock design problem

The ideal pattern for high-TPS small-message RPC is **multiple
threads sharing one TCP connection**: each thread writes its request
to the socket, the kernel coalesces concurrent `write()` calls into
one TCP segment, and the server demuxes by request ID. This is
efficient because the kernel's TCP lock is a fast spinlock held for
one syscall, and coalescing happens for free in the kernel write
buffer.

HTTP/2 breaks this pattern. Its design requires a **connection-level
lock in userspace** that serializes all concurrent writers before
they reach the socket:

- **HTTP/2 multiplexes via streams, not request IDs.** Each request
  is a stream with its own state (open, half-closed, closed) and its
  own HEADERS/DATA frames. The frame encoder must interleave frames
  from multiple streams into one byte stream on the connection.
  This interleaving requires exclusive access to the connection's
  frame output buffer — a userspace lock.
- **HPACK header encoding is stateful.** The HPACK encoder maintains
  a per-connection dynamic table (shared across all streams). Two
  threads encoding headers concurrently would corrupt the table, so
  HPACK encoding is serialized under the connection lock.
- **Flow control is per-connection and per-stream.** Each `DATA`
  frame write must check and decrement both the connection-level and
  stream-level flow-control windows — shared mutable state that
  requires the connection lock.

The result: when N threads submit to one gRPC connection
concurrently, they serialize on the h2 connection lock during frame
encoding + HPACK + flow-control bookkeeping. The kernel's TCP
coalescing still works, but it's fed by a **single-threaded
userspace funnel** — the lock serializes all N threads before any of
them reach `write()`. The kernel never sees concurrent writers; h2
hands it one merged buffer from one thread.

A custom protocol (`[len][req_id][protobuf]` over raw TCP) has **no
connection-level userspace lock**. Each thread calls `write()`
independently — the length-prefixed framing is stateless, there's no
shared encoder table, no per-stream state, no flow-control windows.
The kernel's TCP lock (fast spinlock, held for one syscall) is the
only serialization point. N threads produce N independent `write()`
calls that the kernel coalesces into one segment. The userspace
funnel is gone.

This is a **design mismatch**, not a tuning problem. HTTP/2's
stream/HPACK/flow-control architecture inherently requires a
connection-level lock for correctness. You cannot make h2 accept
concurrent writers without a lock — the shared mutable state
(HPACK table, frame output buffer, flow-control windows) demands
it. The 2T:1C 17% throughput drop measured in the bench is this
lock's cost: two threads that should run in parallel are funneled
through one userspace critical section.

**Decision: not replacing gRPC now; will write a custom Rust RPC
library in the future.** A custom transport would eliminate the
connection lock and recover the lost concurrency, but requires
reimplementing connection pooling, reconnect, timeout, cancellation,
error propagation, backpressure, and TLS — 2–4K lines of
infrastructure that gRPC provides. The lock's cost is bounded (~17%
at 2T:1C, avoided entirely at 1T:1C) and the current bottleneck for
production workloads is consensus (writes) or disk I/O, not
read-path framing. The long-term plan is to build a purpose-built
Rust RPC library for CrowKV (length-prefixed framing over raw TCP,
keeping prost/protobuf for serialization) to replace gRPC on the
internal replica-to-replica hot path. This is deferred until read
throughput becomes the primary constraint and the h2 connection lock
is profiled as the hot spot. Reference implementations to study
before building: protosocket (Momento, 2.75x over gRPC, tokio +
prost, no HTTP/2), Volo (CloudWeGo, custom binary transport, 350k+
QPS), and Cap'n Proto RPC (zero-copy serialization, promise
pipelining).
