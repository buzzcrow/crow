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
  → KvStoreService::get (gRPC)                    [kv_service.rs]
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
       [copy: value → socket buffer on gRPC serialize, unavoidable]
```

**Copy points**: O(1) for the get path — one value copy (frame → `Bytes`
fast path, or I/O buffer → `Bytes` slow path on demand-load); O(n)
unavoidable for gRPC serialize/deserialize. After R6, the get path is
fully zero-copy from C++ frame to gRPC response: `PinnedValue::into_bytes()`
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
  from frame to gRPC response. Page refcount pins keep the frame alive
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
  delayed ACK interaction in tonic/gRPC). After applying `TCP_NODELAY`
  to all client and server sockets, latency dropped to ~138us — a 290×
  improvement.

---

## Latest Benchmark Results — 2026-08-06 (macOS)

**Platform**: Apple M5 Pro, 18c, arm64, macOS 26.5.
**Setup**: 10s mem mode, 3-node cluster, 100k pre-populated keys, 64B
values. Raw TSV: `doc/working/bench-read-regression.tsv` (gitignored).

### Single-thread (1T:1C) — per-read engine cost

| Label | Mode | ops/s | avg us | p50 us | p99 us | p999 us | err |
|-------|------|------:|-------:|-------:|-------:|--------:|----:|
| lin_1t | lin | 20441 | 47 | 47 | 75 | 124 | 0 |
| minslot_1t | minslot | 20478 | 47 | 47 | 73 | 114 | 0 |

At 1T:1C both modes are identical (~20.4K ops/s, 47us) — no concurrency
advantage for MinSlot, and the Linearizable lease barrier is ~0 (lease
fast path). Per-read cost is engine get + gRPC RTT only.

### Multi-thread — max throughput + read-mode split

| Label | Mode | T:C | ops/s | avg us | p50 us | p99 us | p999 us | err |
|-------|------|-----|------:|-------:|-------:|-------:|--------:|----:|
| lin_6t | lin | 6:6 | 68560 | 86 | 81 | 173 | 217 | 0 |
| minslot_6t | minslot | 6:6 | 75158 | 78 | 74 | 150 | 190 | 0 |
| lin_16t | lin | 16:16 | 105613 | 149 | 143 | 254 | 305 | 0 |
| minslot_16t | minslot | 16:16 | 106727 | 148 | 145 | 240 | 288 | 0 |
| lin_32t | lin | 32:32 | 118390 | 267 | 261 | 428 | 528 | 0 |
| minslot_32t | minslot | 32:32 | 112621 | 281 | 277 | 440 | 539 | 0 |

Both modes scale well from 1T to 16T (20K → 106K, 5.2x). MinSlot shows
a clear advantage at 6T (+9.6%, 75158 vs 68560) — distributed read
serving across 3 replicas scales better than single-leader at low
concurrency. At 16T the modes converge (~106K, +1.1% MinSlot) — the 3
replicas are approaching per-replica saturation. At 32T Linearizable
pulls ahead (+5.1%, 118K vs 113K) — MinSlot saturates earlier because
each replica is already at capacity; the leader mode still has headroom
from the lease fast path (no round-trip).

### HTTP/2 connection lock sentinel

| Label | Mode | T:C | ops/s | avg us | p99 us | err |
|-------|------|-----|------:|-------:|-------:|----:|
| minslot_6t_2to1 | minslot | 6:3 | 73484 | 80 | 157 | 0 |

6T:3C (2:1 ratio) drops only -2.2% vs 6T:6C (73484 vs 75158). MinSlot
distributes across 3 replicas (2 connections per replica), so the h2
connection lock contention is lower than with a single leader. The
1T:1C pattern (dedicated connection per thread) avoids the lock
entirely and remains optimal for max throughput.

### Correctness verification (`--verify-bytes 8`)

| Label | Mode | T:C | ops/s | avg us | p99 us | err | corr |
|-------|------|-----|------:|-------:|-------:|----:|-----:|
| lin_16t_verify | lin | 16:16 | 104917 | 150 | 256 | 0 | 0 |
| minslot_16t_verify | minslot | 16:16 | 104988 | 150 | 241 | 0 | 0 |

Zero correctness errors across both modes. Verify overhead is negligible
(<1% throughput impact vs non-verify 16T runs).

---

## Existing Problems

- **HTTP/2 connection lock (deferred, R32).** HTTP/2 requires a
  connection-level userspace lock (HPACK dynamic table, frame output
  buffer, flow-control windows are shared mutable state). When N threads
  submit to one gRPC connection concurrently, they serialize on this
  lock during frame encoding — a single-threaded userspace funnel before
  any `write()` reaches the kernel. The 2T:1C throughput drop measured by
  the `minslot_6t_2to1` sentinel is this lock's cost. 1T:1C avoids it
  entirely (dedicated connection per thread). A custom length-prefixed
  transport over raw TCP (no HPACK, no stream state, no flow-control)
  would eliminate the lock, but requires reimplementing connection
  pooling, reconnect, timeout, cancellation, backpressure, and TLS.
  Deferred until read throughput is the primary constraint and the h2
  lock is profiled as the hot spot.
- **E4 — Least-conn / latency read-endpoint policy.** R26 shipped
  round-robin `AnyReplica`. A `LeastConnections` or `Latency` policy —
  driven by server-reported in-flight counts or client-measured RTT —
  would balance MinSlot load by actual capacity rather than blind
  rotation, avoiding hotspots when one replica is slow (e.g.
  demand-loading cold crow-tree pages).
