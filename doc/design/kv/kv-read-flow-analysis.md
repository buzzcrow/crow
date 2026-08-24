<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Read Flow Analysis

End-to-end trace of the CROW point-read (get) path. Complements
[`kv-write-flow-analysis.md`](kv-write-flow-analysis.md) and
[`kv-scan-flow-analysis.md`](kv-scan-flow-analysis.md) (range read).
Regression sentinel: `tools/bench-read-regression.sh`.

---

## Read Flow

```
Client GET(key, read_mode, min_slot?)
  → CrowkvClient::get                            [client.rs]
    1. resolve_min_slot — MinSlot: auto-attach write watermark;
       Linearizable: 0
    2. resolve_read_endpoint — Linearizable (or MinSlot + Leader
       policy): cached leader endpoint; MinSlot + AnyReplica (R26):
       round-robin across replica endpoints, fallback to leader
    3. send KvGetRequest { key, read_mode, min_slot }
       [copy: key → HTTP/2 frame, unavoidable]
    4. retry: NotLeaderHint → follow; empty hint → wait+refresh;
       transport error → backoff+refresh
  → KvStoreService::get (crow-rpc)                    [kv_service.rs]
    5. [Linearizable] if local not leader and not already forwarded →
       forward_kv_get to leader (at-most-once via x-crow-kv-forwarded)
       - success → return leader's response
       - failure → fall through to local store (degraded)
    6. [MinSlot] no forwarding — serve local
       [copy: key allocated from network frame, unavoidable]
  → PxKvStore::kv_get                             [px_kv_store.rs]
    7. resolve_read_point(group, read_mode, min_slot) → ReadDecision
    8. [Serve] learner.engine_get_bytes(key: &[u8]) → KVEngine::get_bytes
       → Some((resolved_slot, Bytes)) or None
       [no key copy at Rust boundary]
       [value copy: InMemKV clones out of DashMap shard; Crowtree fast
        path one copy frame→Bytes (R21 eliminated the intermediate Vec);
        Crowtree slow path one copy I/O buf→Bytes on reactor thread]
    9. build KvResponse { read_slot, safe_slot, value: Bytes }
       [move: Bytes moved into response, no copy]
       [copy: value → socket buffer on crow-rpc serialize, unavoidable]
```

**Copy points**: O(1) for the get path, one value copy (frame → `Bytes`
fast path, or I/O buffer → `Bytes` slow path on demand-load); O(n)
unavoidable for crow-rpc serialize/deserialize. After R6, the get path is
fully zero-copy from C++ frame to crow-rpc response: `PinnedValue::into_bytes()`
produces a `Bytes` backed by the C++ frame via `Bytes::from_owner`, with
page refcount pins keeping the frame alive until the `Bytes` is dropped.

---

## Read Modes

- **Linearizable** — served by the leader only. The leader first passes
  a read barrier (lease fast path ~0 RTT, or ReadIndex heartbeat
  fallback) to confirm it still holds quorum, then serves at
  `contiguous_chosen`. Non-leader replicas forward the request to the
  leader. Guarantees the read reflects all committed writes.
- **MinSlot** — served locally by any replica whose
  `contiguous_applied >= min_slot`; no barrier, no forwarding. With
  `read_endpoint_policy = any_replica` (R26), reads round-robin across
  all replicas. `min_slot = 0` accepts any staleness; a non-zero
  `min_slot` gives read-your-writes (the client's last write is
  guaranteed applied). A lagging follower redirects to the leader.

---

## Change History

- **R6** — Zero-copy value returns: `PinnedValue::into_bytes()` produces
  a `Bytes` via `Bytes::from_owner` backed by the C++ frame — no copy
  from frame to crow-rpc response. Page refcount pins keep the frame alive
  until the `Bytes` is dropped on any thread.
- **R21** — Eliminated the intermediate `Vec<u8>` in engine get: the
  fast path returns `PinnedValue` borrowing the C++ frame; final `Bytes`
  produced in one copy instead of frame → `Vec` → `Bytes`.
- **R26** — `ReadEndpointPolicy::AnyReplica` for MinSlot: round-robin
  across all replica endpoints so follower read capacity is not wasted.
  A follower that hasn't applied `min_slot` redirects to the leader.
- **R27** — ReadIndex batching: the first read to arrive after lease
  expiry starts one heartbeat round and registers a pending batch;
  later reads enqueue a waiter and adopt the same outcome. Eliminates
  per-read heartbeat rounds under lease-expiry bursts.
- **TCP_NODELAY** — Before the fix, read latency was ~41ms (Nagle +
  delayed ACK interaction in crow-rpc). After applying `TCP_NODELAY`
  to all client and server sockets, latency dropped to ~138us — a 290×
  improvement.

---

## Latest Benchmark Results — 2026-08-19 (macOS)

**Platform**: Apple M5 Pro, 18c, arm64, macOS 26.5.
**Setup**: 10s mem mode, 3-node cluster, 100k pre-populated keys, 64B
values. Raw TSV: `doc/working/bench-read-regression.tsv` (gitignored).

### Single-thread (1T:1C) — per-read engine cost

| Label | Mode | ops/s | avg us | p50 us | p99 us | p999 us | err |
|-------|------|------:|-------:|-------:|-------:|--------:|----:|
| lin_1t | lin | 21112 | 46 | 46 | 67 | 97 | 0 |
| minslot_1t | minslot | 21691 | 45 | 44 | 66 | 96 | 0 |

At 1T:1C both modes are identical (~21K ops/s, 46us) — no concurrency
advantage for MinSlot, and the Linearizable lease barrier is ~0 (lease
fast path). Per-read cost is engine get + crow-rpc RTT only.

### Multi-thread — max throughput + read-mode split

| Label | Mode | T:C | ops/s | avg us | p50 us | p99 us | p999 us | err |
|-------|------|-----|------:|-------:|-------:|-------:|--------:|----:|
| lin_6t | lin | 6:6 | 70668 | 84 | 79 | 163 | 206 | 0 |
| minslot_6t | minslot | 6:6 | 77622 | 76 | 73 | 142 | 181 | 0 |
| lin_16t | lin | 16:16 | 106399 | 148 | 142 | 251 | 298 | 0 |
| minslot_16t | minslot | 16:16 | 107455 | 147 | 145 | 235 | 281 | 0 |
| lin_32t | lin | 32:32 | 119473 | 265 | 260 | 418 | 512 | 0 |
| minslot_32t | minslot | 32:32 | 113270 | 280 | 278 | 432 | 521 | 0 |

Both modes scale well from 1T to 16T (21K → 107K, 5.1x). MinSlot shows
a clear advantage at 6T (+9.8%, 77622 vs 70668): distributed read
serving across 3 replicas scales better than single-leader at low
concurrency. At 16T the modes converge (~107K, +1.0% MinSlot); the 3
replicas are approaching per-replica saturation. At 32T Linearizable
pulls ahead (+5.2%, 119K vs 113K). MinSlot saturates earlier because
each replica is already at capacity; the leader mode still has headroom
from the lease fast path (no round-trip).

### HTTP/2 connection lock sentinel

| Label | Mode | T:C | ops/s | avg us | p99 us | err |
|-------|------|-----|------:|-------:|-------:|----:|
| minslot_6t_2to1 | minslot | 6:3 | 74752 | 79 | 151 | 0 |

6T:3C (2:1 ratio) drops only -3.7% vs 6T:6C (74752 vs 77622). MinSlot
distributes across 3 replicas (2 connections per replica), so the h2
connection lock contention is lower than with a single leader. The
1T:1C pattern (dedicated connection per thread) avoids the lock
entirely and remains optimal for max throughput.

### Correctness verification (`--verify-bytes 8`)

| Label | Mode | T:C | ops/s | avg us | p99 us | err | corr |
|-------|------|-----|------:|-------:|-------:|----:|-----:|
| lin_16t_verify | lin | 16:16 | 105613 | 150 | 252 | 0 | 0 |
| minslot_16t_verify | minslot | 16:16 | 106662 | 148 | 237 | 0 | 0 |

Zero correctness errors across both modes. Verify overhead is negligible
(<1% throughput impact vs non-verify 16T runs).

### Linux results — 2026-08-06

**Platform**: AMD Ryzen 9 5950X, 16c/32t, x86_64, Ubuntu 24.04.
**Setup**: 10s mem mode, 3-node cluster, 100k pre-populated keys, 64B
values. Raw TSV: `doc/working/bench-read-regression.tsv` (gitignored).

#### Single-thread (1T:1C)

| Label | Mode | Linux ops/s | macOS ops/s | Δ% | Linux p99 us | macOS p99 us | err |
|-------|------|------:|------:|---:|-------:|-------:|----:|
| lin_1t | lin | 6608 | 20441 | -68% | 219 | 75 | 0 |
| minslot_1t | minslot | 6160 | 20478 | -70% | 222 | 73 | 0 |

Linux single-thread is ~3x slower (6608 vs 20441) — same gap as scan,
dominated by crow-rpc RTT and engine get cost on x86_64.

#### Multi-thread

| Label | Mode | T:C | Linux ops/s | macOS ops/s | Δ% | Linux p99 us | macOS p99 us | err |
|-------|------|-----|------:|------:|---:|-------:|-------:|----:|
| lin_6t | lin | 6:6 | 50851 | 68560 | -26% | 194 | 173 | 0 |
| minslot_6t | minslot | 6:6 | 52252 | 75158 | -30% | 168 | 150 | 0 |
| lin_16t | lin | 16:16 | 105313 | 105613 | -0% | 255 | 254 | 0 |
| minslot_16t | minslot | 16:16 | 101871 | 106727 | -5% | 268 | 240 | 0 |
| lin_32t | lin | 32:32 | 144262 | 118390 | +22% | 498 | 428 | 0 |
| minslot_32t | minslot | 32:32 | 138610 | 112621 | +23% | 532 | 440 | 0 |
| lin_64t | lin | 64:32 | 168849 | — | — | 1009 | — | 0 |
| minslot_64t | minslot | 64:32 | 165317 | — | — | 1132 | — | 0 |
| lin_128t | lin | 128:32 | 205406 | — | — | 1760 | — | 0 |
| minslot_128t | minslot | 128:32 | 189030 | — | — | 2290 | — | 0 |
| lin_256t | lin | 256:32 | 231983 | — | — | 2658 | — | 0 |
| minslot_256t | minslot | 256:32 | 215334 | — | — | 4284 | — | 0 |

Linux catches up at 16T (-0% to -5%) and **overtakes macOS at 32T**
(+22% to +23%): the 32-thread Ryzen scales better than the 18-core M5
Pro at saturation. On macOS, MinSlot beats Linearizable at 6T (+9.6%)
and 16T (+1.1%); distributed read serving helps when the single leader
is the bottleneck. On Linux this advantage **disappears**: MinSlot is
marginally better only at 6T (+2.8%) and **slower** at 16T (-3.3%) and
32T (-3.9%). The leader lease fast path is cheap enough on x86_64 that
distributing reads across replicas doesn't compensate for the added
MinSlot routing overhead (min_slot resolution, round-robin endpoint
selection). MinSlot's benefit is platform-dependent. It helps when the
leader read barrier is the bottleneck (macOS arm64), not when the engine
itself is the limiter (Linux x86_64).

Beyond 32T, Linearizable continues to scale and widen its lead:
- **64T**: lin 168849 vs minslot 165317 (-2.1%)
- **128T**: lin 205406 vs minslot 189030 (-8.0%)
- **256T**: lin 231983 vs minslot 215334 (-7.2%)

Linearizable scales 32T → 256T (144K → 232K, 1.6x). The lease fast
path has no per-read round-trip, so the leader absorbs more concurrent
reads. MinSlot scales 32T → 256T (139K → 215K, 1.5x) but falls further
behind because the 3 replicas saturate earlier (each handles ~72K at
256T vs the leader's 232K). MinSlot's p99 also degrades faster at high
thread counts (4284us vs 2658us at 256T): round-robin distribution
adds tail latency under heavy contention.

#### HTTP/2 connection lock sentinel

| Label | Mode | T:C | Linux ops/s | macOS ops/s | Δ% | Linux p99 us | macOS p99 us | err |
|-------|------|-----|------:|------:|---:|-------:|-------:|----:|
| minslot_6t_2to1 | minslot | 6:3 | 52228 | 73484 | -29% | 172 | 157 | 0 |

6T:3C drops -0.04% vs 6T:6C on Linux (52228 vs 52252), even less
contention than macOS's -2.2%, consistent with the smaller MinSlot
throughput advantage on Linux.

#### Correctness verification (`--verify-bytes 8`)

| Label | Mode | T:C | Linux ops/s | macOS ops/s | Δ% | Linux p99 us | macOS p99 us | err | corr |
|-------|------|-----|------:|------:|---:|-------:|-------:|----:|-----:|
| lin_16t_verify | lin | 16:16 | 105781 | 104917 | +1% | 252 | 256 | 0 | 0 |
| minslot_16t_verify | minslot | 16:16 | 101552 | 104988 | -3% | 268 | 241 | 0 | 0 |

Zero correctness errors on Linux. Verify overhead negligible (<1%).

---

## Existing Problems

- **HTTP/2 connection lock (deferred, R32).** HTTP/2 requires a
  connection-level userspace lock (HPACK dynamic table, frame output
  buffer, flow-control windows are shared mutable state). When N threads
  submit to one crow-rpc connection concurrently, they serialize on this
  lock during frame encoding, a single-threaded userspace funnel before
  any `write()` reaches the kernel. The 2T:1C throughput drop measured by
  the `minslot_6t_2to1` sentinel is this lock's cost. 1T:1C avoids it
  entirely (dedicated connection per thread). A custom length-prefixed
  transport over raw TCP (no HPACK, no stream state, no flow-control)
  would eliminate the lock, but requires reimplementing connection
  pooling, reconnect, timeout, cancellation, backpressure, and TLS.
  Deferred until read throughput is the primary constraint and the h2
  lock is profiled as the hot spot.
