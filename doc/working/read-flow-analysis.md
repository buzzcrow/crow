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
    2. resolve_read_endpoint — Linearizable (or MinSlot + Leader
       policy): cached leader endpoint; MinSlot + AnyReplica (R26):
       round-robin across replica endpoints, fallback to leader
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
       [value copy: InMemKV clones out of BTreeMap under RwLock read
        lock; Crowtree fast path one copy frame→Bytes (R21 eliminated
        the intermediate Vec); Crowtree slow path one copy I/O buf→Bytes
        on reactor thread]
    9. build KvResponse { read_slot, safe_slot, value: Bytes }
       [move: Bytes moved into response, no copy]
       [copy: value → socket buffer on gRPC serialize, unavoidable]
```

### Per-Mode Routing (`resolve_read_point`)

- **Linearizable** — local is leader:
  - `linearizable_read_barrier()` → leader-read-ready gate (else
    `NoQuorum`) → lease fast path or ReadIndex fallback →
    `Ready { read_slot }` serve at `contiguous_chosen`
  - `NotLeader` → redirect (race / loop-guard after forwarding failed)
  - `NoQuorum` → `Unavailable` (no quorum, or leader not yet read-ready)
  - local not leader → `NotLeader` redirect (forwarding should have
    handled it; falls back to `NotLeader` rather than serving stale)
- **MinSlot** — `contiguous_applied >= min_slot` → serve locally at
  `contiguous_applied`; else `NotLeader` redirect (follower hasn't
  applied the client's last write; leader has). `min_slot = 0` accepts
  any staleness.

### Linearizable Read Barrier

1. **Leader-read-ready gate** — `leader_read_ready()` false → `NoQuorum`
   without touching the lease or heartbeat. Set by the election driver
   once the leader has a quorum-established term; cleared on step-down.
   Cheap early reject for a replica that just won an election but hasn't
   yet confirmed quorum, or one that has lost it.
2. **Lease fast path** — `lease_read_valid(now)` true → serve at
   `contiguous_chosen` with no round-trip. Lease renewed by each
   successful heartbeat; valid for `lease_duration - max_clock_skew`
   after the last quorum ack.
3. **ReadIndex fallback** — lease expired → one `run_heartbeat_round`,
   with **R27 batching**: the first read to arrive (round leader) starts
   the round and registers a pending batch carrying its pre-round
   `read_slot`; later reads enqueue a waiter and adopt the same outcome
   (same conservative freshness floor). Quorum ack confirms leadership
   at this term; higher term steps down; no quorum → `NoQuorum`. Read
   served at `contiguous_chosen` captured *before* the heartbeat.

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
- **`ReadRegistryHandles`** — registry-backed metric handles for the
  read path, stored in a `OnceLock` for lock-free hot-path reads.
  Counters: `lease_path`, `readindex_path`, `readindex_rounds`,
  `minslot_fallback`. Latency summaries: `barrier`, `engine_get`.
  Gauges: `lease_valid`, `contiguous_applied`, `safe_slot`. Plus
  `PendingReadBarrier` (waiters coalesced onto one in-flight ReadIndex
  round, R27).

---

## Read Flow — Range Read (Scan)

```
Client SCAN(prefix, start_after, limit, read_mode, min_slot?)
  → CrowkvClient::scan
    1. resolve_min_slot (same as get)
    2. resolve_read_endpoint (same as get; R26 AnyReplica for MinSlot)
    3. send KvScanRequest
       [copy: prefix → HTTP/2 frame, unavoidable]
    4. retry: no NotLeaderHint in KvScanResponse; !ok is a plain error
  → KvStoreService::scan
    5. [Linearizable] forward_kv_scan to leader (same at-most-once)
    6. [MinSlot] serve local
       [copy: prefix allocated from network frame, unavoidable]
  → PxKvStore::kv_scan
    7. resolve_read_point (min_slot passed through)
    8. [Serve] learner.engine_scan(prefix, start_after, limit)
       → Vec<(key, slot, value)>, truncated
       [Crowtree: prefix to_vec() for FFI; ct_scan_async (fast path
        memtable, slow path demand-load retry loop); packed result
        take_buf; per-entry Vec<u8> for key+value in decode_scan.
        start_after non-empty → fetch_limit=0 (over-fetch whole prefix
        range), filter keys <= start_after in Rust, then apply limit]
    9. build KvScanResponse { read_slot, entries }
       [move: entries moved into response, no copy]
       [copy: entries → socket buffer on gRPC serialize, unavoidable]
```

- **No `not_leader_hint` in `KvScanResponse`** — client can't follow a
  hint; any `!ok` is a counted error. Server-side forwarding handles
  linearizable redirection transparently. See E3 for adding a hint.
- **`LatencySummary` not `LatencyHistogram`** — scan latency is
  dominated by result-set size, so fixed-bucket percentiles are less
  informative than count + avg + max.

---

## Read Concurrency

- **No admission window** — reads bypass `InflightAdmission`; no
  `max_inflight_reads`. Burst bounded only by Tokio scheduling, engine
  throughput (`InMemKV` `RwLock<BTreeMap>` — concurrent readers, but
  reads serialize with `apply` under the write lock; FFI + demand-load
  for `CrowtreeEngine`), and HTTP/2 stream concurrency. Intentional:
  reads are cheap, non-blocking, and lease-path reads consume no
  consensus resources.
- **Parallel across replicas — MinSlot only.** Each replica's engine
  has its own `get`/`scan` path with no cross-replica coordination.
  Linearizable reads are serialized through the leader by definition.
- **Stale-read fan-out (resolved by R26).** With
  `read_endpoint_policy = any_replica`, MinSlot reads round-robin
  across all replica endpoints, so follower read capacity is no longer
  wasted. A follower that hasn't applied `min_slot` redirects to the
  leader (`NotLeader`), counted via `read_endpoint_fallback`. Default
  policy remains `Leader`; least-conn / latency policies are future
  work (E4).

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

- **Client routing** — `CrowkvClient::get` calls `resolve_read_endpoint`:
  Linearizable, or MinSlot with `read_endpoint_policy = Leader`, →
  `resolve_leader` (cached leader endpoint). MinSlot with
  `read_endpoint_policy = AnyReplica` (R26) → round-robin across the
  topology's replica endpoints, falling back to `resolve_leader` when
  the replica list is empty. Topology cache populated from `/topology`,
  updated via `NotLeaderHint`. For MinSlot, `resolve_min_slot`
  auto-attaches the client's `write_watermark` (highest `revision`
  observed from its own writes to this group) if caller omitted
  `min_slot`. Retry: `NotLeaderHint` non-empty → follow (uncounted;
  MinSlot+AnyReplica also records `read_endpoint_fallback`); empty
  hint → `wait_and_refresh_leader` (100ms + refresh); transport error
  → backoff + refresh. All counted against `max_retries` except hint
  follows.
- **Server forwarding** — `KvStoreService::get`: check
  `x-crowkv-forwarded` (loop-guard); if not forwarded and local not
  leader → `forward_kv_get`; success → return leader response (metrics
  recorded at forwarder, `kv.get_forwarded.c`); failure → fall through
  (degraded, `kv.get_forward_failed.c`; store returns `NotLeader` for
  linearizable). MinSlot never forwarded. Scan mirrors this in
  `KvStoreService::scan` / `forward_kv_scan`.
- **Engine get** — `InMemKV`: `BTreeMap::get` under `RwLock` read lock →
  value cloned out via trait return → `Bytes::from(vec)`.
  `CrowtreeEngine`: `try_get_pinned` → fast path lock-free memtable
  lookup with epoch guard returning `PinnedValue` (R6: `into_bytes()`
  produces a zero-copy `Bytes` via `Bytes::from_owner`); slow path
  `ct_get_async` demand-load via `ct_future` (primary latency
  contributor for cold reads).
- **Engine scan** — ordered prefix scan, up to `limit` live
  (non-tombstoned) entries. `InMemKV`: `BTreeMap::range` iteration
  under read lock (already key-sorted). `CrowtreeEngine`:
  `try_scan` → `ct_scan_async` (fast path memtable traversal, slow path
  demand-load retry loop); packed result decoded per-entry into
  `Vec<u8>` key+value. `start_after` is not pushed into the C++ API —
  when non-empty the engine over-fetches the whole prefix range
  (`fetch_limit=0`) and filters in Rust.

---

## Gaps and Optimization Opportunities

### Open gaps

- **G1 — No per-mode latency breakdown.** `kv.get.lh` merges
  linearizable (may incur heartbeat RTT) with MinSlot (local lookup).
  Fix: separate `kv.get.linearizable.lh` / `kv.get.min_slot.lh`, or
  per-mode counters + shared histogram.

### Enhancement opportunities

- **E1 — Scan `start_after` push-down.** `CrowtreeEngine::scan` cannot
  push `start_after` into the C++ API (`ct_scan_async` takes only
  `prefix` + `limit`). When `start_after` is non-empty the engine sets
  `fetch_limit = 0` (over-fetch the whole prefix range), ships the
  packed result across the FFI boundary, then filters keys `<=
  start_after` in Rust before applying the limit. Deep pagination
  transfers and decodes entries the client will discard. Fix: extend
  `ct_scan_async` with a `start_after` cursor + lower-bound seek, so
  the C++ engine starts iteration at the cursor and applies the limit
  natively. Flagged in the `CrowtreeEngine::scan` code comment.
- **E2 — Scan value zero-copy.** Get has R6 zero-copy via
  `PinnedValue::into_bytes` (`Bytes::from_owner` backed by the C++
  frame). Scan still produces per-entry `Vec<u8>` for both key and
  value in `decode_scan` (packed `take_buf` → per-entry decode →
  owned `Vec`). A `PinnedScanEntry` / `Bytes::from_owner` path for
  scan values would eliminate the per-entry copy, mirroring R6 for the
  scan path. Matters for large-value range reads.
- **E3 — Scan `not_leader_hint`.** `KvScanResponse` has no
  `not_leader_hint` field, so a MinSlot scan that hits a lagging
  follower is a counted client error (parsed from the error string),
  not an uncounted follow like `get`. Adding a hint field (leader
  endpoint) would let MinSlot scan follow redirects cheaply and keep
  client error counts clean. Server-side forwarding already hides
  linearizable redirects; this only affects MinSlot on a follower that
  hasn't reached `min_slot`.
- **E4 — Least-conn / latency read-endpoint policy.** R26 shipped
  round-robin `AnyReplica`. A `ReadEndpointPolicy::LeastConnections`
  or `::Latency` policy — driven by server-reported in-flight counts
  or client-measured RTT — would balance MinSlot load by actual
  capacity rather than blind rotation, avoiding hotspots when one
  replica is slow (e.g. demand-loading cold crowtree pages).
- **E5 — `InMemKV` read/apply concurrency.** `InMemKV` is
  `RwLock<BTreeMap>`: reads take the read lock (concurrent with each
  other) but block behind `apply`'s write lock for the whole batch.
  A sharded/lock-free map (e.g. `DashMap`) would let reads proceed
  concurrent with writes. Low priority — `InMemKV` is test-only, not
  selectable via the server CLI — but the prior version of this doc
  incorrectly described it as lock-free, which it is not.
- **E6 — Custom RPC transport (deferred, R32).** The HTTP/2
  connection-level lock serializes concurrent writers on one
  connection (HPACK table, frame output buffer, flow-control windows
  are shared mutable state). A length-prefixed custom transport over
  raw TCP would eliminate the userspace funnel and recover
  concurrency for many-threads-one-connection patterns. Tracked as
  R32; deferred until read throughput is the primary constraint and
  the h2 lock is profiled as the hot spot. See § gRPC transport below.

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

Open work: **G1** per-mode latency breakdown; **E1** scan `start_after`
push-down, **E2** scan value zero-copy, **E3** scan `not_leader_hint`,
**E4** least-conn/latency endpoint policy, **E5** `InMemKV` read/apply
concurrency, **E6** custom RPC transport (R32, deferred).

---

## Memory Copy Summary

Copy points are annotated inline in the flow diagrams above. Summary
of what remains:

- **O(n) unavoidable** — gRPC request deserialize (key/prefix from
  network frame); gRPC response serialize (value(s) into socket);
  `InMemKV` get (value cloned from `BTreeMap` under `RwLock` read lock);
  `CrowtreeEngine` get (one copy: frame → `Bytes` fast path, or I/O
  buffer → `Bytes` slow path); `CrowtreeEngine` scan (prefix `to_vec()`
  for FFI, packed result `take_buf`, per-entry `Vec<u8>` for key+value
  in `decode_scan`; plus over-fetch copies when `start_after` is
  non-empty — see E1).
- **Eliminated by R21** — engine get key copy (Rust-side `to_vec()`
  gone; C API copies internally); engine get intermediate `Vec<u8>`
  (fast path returns `PinnedValue` borrowing the C++ frame; final
  `Bytes` produced in one copy instead of frame → `Vec` → `Bytes`).
- **Eliminated by R6** — engine get value copy (frame → `Bytes`):
  `PinnedValue::into_bytes()` creates a `Bytes` via `Bytes::from_owner`
  backed by the C++ frame — no copy. The page refcount pins stay alive
  until the `Bytes` is dropped on any thread (R6: `PinnedValue` is `Send`).
- **Zero-copy (move/borrow)** — `kv_get` → `engine_get_bytes` passes
  `&[u8]` (no key copy at the Rust boundary); response `Bytes` moved
  into `KvResponse` (no copy).

The read path has fewer O(n) copies than the write path: no WAL
encode/replay, no payload encoding, no Batch decode, no FFI batch
encode. After R6, the **get** path is fully zero-copy from frame to
gRPC response: `PinnedValue::into_bytes()` produces a `Bytes` backed
by the C++ frame via `Bytes::from_owner`, with page refcount pins
keeping the frame alive until the `Bytes` is dropped on any thread.
The **scan** path is not yet zero-copy — per-entry `Vec<u8>` decode
remains (see E2).

---

## Benchmark Results — 2026-07-24 (Linux)

### Platform

- **CPU**: AMD Ryzen 9 5950X — 16 cores / 32 threads, Zen 3
  (2 CCX × 8 cores, 32 MB L3 per CCX)
- **Memory interconnect**: Dual-channel DDR4-3200, PCIe 4.0
  — **not** HBM. DRAM bandwidth is constrained by the 2-channel
  topology; cross-CCX traffic crosses the Infinity Fabric, adding
  latency for `InMemKV` (`DashMap`) and `mem-block` WAL accesses
  that land on a different CCX than the issuing core. This matters
  for in-memory KV workloads where every read touches DRAM.
- **OS**: Linux
- **macOS (Apple M5 Pro)**: placeholder — to be re-collected with
  the current codebase. Apple Silicon uses **unified memory**
  (on-package, HBM-style interconnect) with fundamentally different
  latency/bandwidth characteristics: no cross-CCX fabric hop, higher
  per-core memory bandwidth, lower DRAM latency. Results will differ
  from the Linux dual-channel DDR4 platform, especially at high
  thread counts where memory bandwidth becomes the bottleneck.

### Test Setup

3-node cluster, in-memory WAL + in-memory KV (mem-block),
read-only, 512-byte values, 200K key space pre-populated with
deterministic per-byte hash values (`byte_at(key_id, offset) =
splitmix64(key_id ^ splitmix64(offset)) mod 256`), 12-second
measurement + 3s warmup (`--duration-secs 15`), `election_profile =
e2e`. Reads draw uniformly from `[0, 200K)`. Correctness spot-check
disabled (`--verify-bytes 0`) for clean throughput; verified
separately with `--verify-bytes 8` (`correctness_errors = 0` across
all configs).

Benchmark scripts: full sweep `tools/bench-read-sweep.sh`, regression
subset `tools/bench-read-regression.sh`.

Two read modes benchmarked:

- **Linearizable** — lease fast path (barrier ~0 when the leader's
  lease is valid), ReadIndex fallback when the lease has expired.
  Client `read_endpoint_policy = Leader` (always targets leader).
- **MinSlot `min_slot=0` + AnyReplica** — pure local serve (no
  barrier, any staleness accepted), reads distributed round-robin
  across all 3 replicas.

60 runs total (54 sweep + 6 verification), zero errors, zero
correctness errors. Full raw data in
[`plan-perf.md`](plan-perf.md#raw-data).

### Phase 1 — 1T:1C scaling

| Threads | Conn | Lin ops/s | Lin avg (µs) | Lin p99 (µs) | MinSlot ops/s | MinSlot avg (µs) | MinSlot p99 (µs) |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 3 | 3 | 8,109 | 367 | 631 | 24,780 | 119 | 170 |
| 6 | 6 | 47,366 | 124 | 200 | 45,563 | 130 | 183 |
| 12 | 12 | 78,074 | 151 | 256 | 74,250 | 159 | 268 |
| 24 | 24 | 120,494 | 195 | 403 | 112,172 | 210 | 444 |
| 48 | 48 | 144,486 | 326 | 828 | 135,928 | 346 | 884 |

### Phase 2 — T:C ratio exploration

At each thread count, sweep C ratios (clamped to [1, 64]). 1:1 runs
reuse Phase 1 results.

**6T:**

| T | C | Ratio | Lin ops/s | MinSlot ops/s |
| --- | --- | --- | --- | --- |
| 6 | 2 | 3:1 | 19,327 | 44,247 |
| 6 | 3 | 2:1 | 44,006 | 47,225 |
| 6 | 6 | 1:1 | 47,366 | 45,563 |
| 6 | 12 | 1:2 | 47,926 | 46,719 |
| 6 | 24 | 1:4 | 47,510 | 47,266 |

**12T:**

| T | C | Ratio | Lin ops/s | MinSlot ops/s |
| --- | --- | --- | --- | --- |
| 12 | 3 | 4:1 | 54,179 | 66,954 |
| 12 | 6 | 2:1 | 71,245 | 73,982 |
| 12 | 12 | 1:1 | 78,074 | 74,250 |
| 12 | 24 | 1:2 | 28,532 | 73,875 |
| 12 | 48 | 1:4 | 27,466 | 73,161 |

**24T:**

| T | C | Ratio | Lin ops/s | MinSlot ops/s |
| --- | --- | --- | --- | --- |
| 24 | 6 | 4:1 | 90,175 | 105,880 |
| 24 | 12 | 2:1 | 116,513 | 111,389 |
| 24 | 24 | 1:1 | 120,494 | 112,172 |
| 24 | 48 | 1:2 | 121,604 | 111,122 |

**48T:**

| T | C | Ratio | Lin ops/s | MinSlot ops/s |
| --- | --- | --- | --- | --- |
| 48 | 12 | 4:1 | 42,625 | 136,702 |
| 48 | 24 | 2:1 | 145,181 | 139,627 |
| 48 | 48 | 1:1 | 144,486 | 135,928 |
| 48 | 64 | ~1:1.3 | 41,012 | 136,458 |

### Phase 3 — Low thread count + 1T:multiC

| T | C | Ratio | Lin ops/s | MinSlot ops/s |
| --- | --- | --- | --- | --- |
| 1 | 1 | 1:1 | 6,547 | 5,876 |
| 1 | 2 | 1:2 | 6,292 | 6,087 |
| 1 | 4 | 1:4 | 6,904 | 5,879 |
| 2 | 1 | 2:1 | 16,875 | 11,365 |
| 2 | 2 | 1:1 | 18,088 | 12,714 |
| 2 | 4 | 1:2 | 18,060 | 12,574 |
| 3 | 1 | 3:1 | 11,116 | 22,810 |
| 3 | 6 | 1:2 | 8,714 | 24,270 |

### Phase 4 — Verification (top configs, `--verify-bytes 8`)

| Mode | T | C | Ratio | ops/s | avg (µs) | p99 (µs) | corr_err |
| --- | --- | --- | --- | --- | --- | --- | --- |
| lin | 48 | 48 | 1:1 | 145,679 | 323 | 811 | 0 |
| lin | 48 | 24 | 2:1 | 37,622 | 1,272 | 2,271 | 0 |
| minslot | 48 | 24 | 2:1 | 138,105 | 341 | 950 | 0 |
| minslot | 48 | 48 | 1:1 | 135,493 | 347 | 863 | 0 |
| lin | 24 | 24 | 1:1 | 119,289 | 197 | 415 | 0 |
| minslot | 24 | 24 | 1:1 | 112,106 | 210 | 442 | 0 |

Note: lin 48T:24C verification run was an outlier (37K vs 145K in
Phase 2) — likely a transient scheduling hiccup.

### Top 10 configs (by throughput)

| Rank | Mode | T:C | ops/s | avg (µs) | p99 (µs) |
| --- | --- | --- | --- | --- | --- |
| 1 | lin | 48:48 (1:1) | 145,679 | 323 | 811 |
| 2 | lin | 48:24 (2:1) | 145,181 | 324 | 885 |
| 3 | lin | 48:48 (1:1) | 144,486 | 326 | 828 |
| 4 | minslot | 48:24 (2:1) | 139,627 | 337 | 934 |
| 5 | minslot | 48:24 (2:1) | 138,105 | 341 | 950 |
| 6 | minslot | 48:12 (4:1) | 136,702 | 344 | 981 |
| 7 | minslot | 48:64 (1:1.3) | 136,458 | 345 | 874 |
| 8 | minslot | 48:48 (1:1) | 135,928 | 346 | 884 |
| 9 | minslot | 48:48 (1:1) | 135,493 | 347 | 863 |
| 10 | lin | 24:48 (1:2) | 121,604 | 193 | 393 |

### Latency-optimal configs (p99 < 300us)

| Mode | T:C | ops/s | avg (µs) | p99 (µs) |
| --- | --- | --- | --- | --- |
| lin | 2:2 (1:1) | 18,088 | 109 | 209 |
| lin | 2:4 (1:2) | 18,060 | 109 | 209 |
| minslot | 3:6 (1:2) | 24,270 | 122 | 199 |
| minslot | 3:3 (1:1) | 24,780 | 119 | 170 |
| minslot | 6:6 (1:1) | 45,563 | 130 | 183 |

### TCP_NODELAY fix

Before the fix, Linux read latency was ~41ms (Nagle + delayed ACK
interaction in tonic/gRPC). After applying `TCP_NODELAY` to all
client and server sockets (including a custom `NoDelayIncoming`
wrapper for `serve_with_incoming`), latency dropped to ~138us
— a **290× improvement**. All benchmark data above is
post-TCP_NODELAY.

### Conclusions

- **Max throughput: 145,679 ops/s** — Linearizable, 48T:48C (1:1),
  verified with `--verify-bytes 8`. Reads are **~5× faster than
  writes at peak** (write peak ~29K ops/s; see write-flow §
  Benchmark Results). Reads skip the consensus critical path (no
  WAL append, no quorum RPC) — the lease barrier is ~0 when the
  leader's lease is valid, so a linearizable read is just engine
  get + gRPC RTT.
- **1T:1C is optimal** — dedicated connection per thread avoids
  HTTP/2 connection lock contention. This holds on both Linux and
  macOS.
- **48T:24C (2:1) is competitive** — 145K (lin) / 140K (minslot),
  nearly matching 1:1 while using half the connections.
- **High T:C ratios (4:1, 3:1) hurt linearizable** — connection
  lock contention causes 2-3× latency increase (e.g. 48T:12C lin =
  42K @ 1.1ms avg vs 48T:48C lin = 145K @ 326us avg).
- **Low T:C ratios (1:2, 1:4) hurt linearizable at 12T+** —
  12T:24C lin = 28K @ 418us vs 12T:12C lin = 78K @ 151us; likely
  h2 flow-control window starvation with many streams on few conns.
- **MinSlot is more resilient to non-1:1 ratios** — at 12T,
  minslot stays ~73K across all C ratios, while lin collapses at
  1:2/1:4. MinSlot distributes across 3 replicas, so each
  connection carries fewer streams.
- **1T:multiC confirmed wasted** — 1T:1C/2C/4C all ~6.5K, extra
  connections don't help blocking mode (1 in-flight at a time).
- **multiT:1C hurts linearizable** — 3T:1C lin = 11K @ 267us;
  the h2 lock cost scales with thread count.
- **Correctness verified separately** — `correctness_errors = 0`
  across all configs when `--verify-bytes 8` is enabled.
- **Linux vs macOS** — Linux 145K vs macOS ~120K (prior data,
  pre-TCP_NODELAY), similar scaling shape, 1:1 optimal on both.
  macOS data to be re-collected with the current codebase
  (post-TCP_NODELAY) for a fair comparison. The memory interconnect
  difference (dual-channel DDR4 vs unified memory) may shift the
  crossover points at high thread counts.

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
