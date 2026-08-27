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

## Latest Benchmark Results — 2026-08-28 (Linux)

**Platform**: AMD Ryzen 9 5950X, 16c/32t, x86_64, Ubuntu 24.04.
**Setup**: 20s mem mode, 3-node cluster, 100k pre-populated keys, 64B
values. Script wall time: 9m29.941s. Raw TSV:
`doc/working/bench-read-regression.tsv` (gitignored).

### Single-thread (1T:1C) — per-read engine cost

| Label | Mode | ops/s | avg us | p50 us | p99 us | p999 us | err | corr |
|-------|------|------:|-------:|-------:|-------:|--------:|----:|-----:|
| lin_1t | lin | 13495 | 73 | 76 | 107 | 151 | 0 | 0 |
| minslot_1t | minslot | 11746 | 84 | 85 | 125 | 161 | 0 | 0 |

### Multi-thread — max throughput + read-mode split

| Label | Mode | T:C | ops/s | avg us | p50 us | p99 us | p999 us | err |
|-------|------|-----|------:|-------:|-------:|-------:|--------:|----:|
| lin_6t | lin | 6:6 | 77338 | 76 | 74 | 127 | 152 | 0 |
| minslot_6t | minslot | 6:6 | 95949 | 61 | 59 | 105 | 131 | 0 |
| lin_16t | lin | 16:16 | 232893 | 67 | 65 | 116 | 147 | 0 |
| minslot_16t | minslot | 16:16 | 236501 | 66 | 64 | 109 | 150 | 0 |
| lin_32t | lin | 32:32 | 271184 | 116 | 112 | 221 | 273 | 0 |
| minslot_32t | minslot | 32:32 | 265130 | 119 | 116 | 206 | 276 | 0 |

The 6T MinSlot run leads Linearizable by 24.0% (95949 vs 77338 ops/s).
At 16T the modes are nearly equal, with MinSlot 1.6% higher. At 32T
Linearizable is 2.3% higher, indicating saturation in the distributed
MinSlot path on this host.

### Connection fan-in sentinel

| Label | Mode | T:C | ops/s | avg us | p99 us | err |
|-------|------|-----|------:|-------:|-------:|----:|
| minslot_6t_2to1 | minslot | 6:3 | 89795 | 66 | 113 | 0 |

The 6T:3C run remained error-free and delivered 93.6% of the 6T:6C
throughput, with a lower measured average latency in this run.

### Correctness verification (`--verify-bytes 8`)

| Label | Mode | T:C | ops/s | avg us | p99 us | err | corr |
|-------|------|-----|------:|-------:|-------:|----:|-----:|
| lin_16t_verify | lin | 16:16 | 233716 | 67 | 65 | 114 | 0 |
| minslot_16t_verify | minslot | 16:16 | 233090 | 67 | 65 | 111 | 0 |

Zero correctness errors across both modes.

## macOS Baseline Results — 2026-08-19

**Platform**: Apple M5 Pro, 18c, arm64, macOS 26.5.
**Setup**: 10s mem mode, 3-node cluster, 100k pre-populated keys, 64B
values. Raw TSV: `doc/working/bench-read-regression.tsv` (gitignored).

### Single-thread (1T:1C) — per-read engine cost

| Label | Mode | ops/s | avg us | p50 us | p99 us | p999 us | err |
|-------|------|------:|-------:|-------:|-------:|--------:|----:|
| lin_1t | lin | 21112 | 46 | 46 | 67 | 97 | 0 |
| minslot_1t | minslot | 21691 | 45 | 44 | 66 | 96 | 0 |

### Multi-thread — max throughput + read-mode split

| Label | Mode | T:C | ops/s | avg us | p50 us | p99 us | p999 us | err |
|-------|------|-----|------:|-------:|-------:|-------:|--------:|----:|
| lin_6t | lin | 6:6 | 70668 | 84 | 79 | 163 | 206 | 0 |
| minslot_6t | minslot | 6:6 | 77622 | 76 | 73 | 142 | 181 | 0 |
| lin_16t | lin | 16:16 | 106399 | 148 | 142 | 251 | 298 | 0 |
| minslot_16t | minslot | 16:16 | 107455 | 147 | 145 | 235 | 281 | 0 |
| lin_32t | lin | 32:32 | 119473 | 265 | 260 | 418 | 512 | 0 |
| minslot_32t | minslot | 32:32 | 113270 | 280 | 278 | 432 | 521 | 0 |

### Connection fan-in sentinel

| Label | Mode | T:C | ops/s | avg us | p99 us | err |
|-------|------|-----|------:|-------:|-------:|----:|
| minslot_6t_2to1 | minslot | 6:3 | 74752 | 79 | 151 | 0 |

### Correctness verification (`--verify-bytes 8`)

| Label | Mode | T:C | ops/s | avg us | p99 us | err | corr |
|-------|------|-----|------:|-------:|-------:|----:|----:|
| lin_16t_verify | lin | 16:16 | 105613 | 150 | 252 | 0 | 0 |
| minslot_16t_verify | minslot | 16:16 | 106662 | 148 | 237 | 0 | 0 |

## Linux/macOS Comparison — 2026-08-28 Linux

**Platform**: AMD Ryzen 9 5950X, 16c/32t, x86_64, Ubuntu 24.04.
**Setup**: 20s mem mode, 3-node cluster, 100k pre-populated keys, 64B
values. macOS columns retain the 2026-08-19 baseline. Raw TSV:
`doc/working/bench-read-regression.tsv` (gitignored).

#### Single-thread (1T:1C)

| Label | Mode | Linux ops/s | macOS ops/s | Δ% | Linux p99 us | macOS p99 us | err |
|-------|------|------:|------:|---:|-------:|-------:|----:|
| lin_1t | lin | 13495 | 20441 | -34.0% | 107 | 75 | 0 |
| minslot_1t | minslot | 11746 | 20478 | -42.6% | 125 | 73 | 0 |

Linux is still slower on single-thread reads, but the gap is smaller:
Linearizable is 34.0% below macOS (13495 vs 20441 ops/s), while MinSlot
is 42.6% below (11746 vs 20478).

#### Multi-thread

| Label | Mode | T:C | Linux ops/s | macOS ops/s | Δ% | Linux p99 us | macOS p99 us | err |
|-------|------|-----|------:|------:|---:|-------:|-------:|----:|
| lin_6t | lin | 6:6 | 77338 | 68560 | +12.8% | 127 | 173 | 0 |
| minslot_6t | minslot | 6:6 | 95949 | 75158 | +27.7% | 105 | 150 | 0 |
| lin_16t | lin | 16:16 | 232893 | 105613 | +120.5% | 116 | 254 | 0 |
| minslot_16t | minslot | 16:16 | 236501 | 106727 | +121.6% | 109 | 240 | 0 |
| lin_32t | lin | 32:32 | 271184 | 118390 | +129.1% | 221 | 428 | 0 |
| minslot_32t | minslot | 32:32 | 265130 | 112621 | +135.4% | 206 | 440 | 0 |
Linux is faster once concurrency reaches 6T: it leads macOS by 12.8%
(77338 vs 68560 ops/s) for Linearizable and 27.7% (95949 vs 75158) for
MinSlot. At 32T, Linux is **2.3× faster** for Linearizable (271184 vs
118390 ops/s) and **2.4× faster** for MinSlot (265130 vs 112621).
Single-thread remains slower on Linux, so the advantage comes from the
Ryzen's higher concurrent throughput rather than lower per-read latency.
On macOS, MinSlot beats Linearizable at 6T (+13.2%) and 16T (+1.0%);
on Linux it is also ahead at 6T (+24.0%) and 16T (+1.6%), then falls
slightly behind Linearizable at 32T (-2.2%).

#### Connection fan-in sentinel

| Label | Mode | T:C | Linux ops/s | macOS ops/s | Δ% | Linux p99 us | macOS p99 us | err |
|-------|------|-----|------:|------:|---:|-------:|-------:|----:|
| minslot_6t_2to1 | minslot | 6:3 | 89795 | 73484 | +22.2% | 113 | 157 | 0 |

6T:3C reaches 89795 ops/s on Linux, 22.2% above the retained macOS
baseline (73484 ops/s), and remains error-free.

#### Correctness verification (`--verify-bytes 8`)

| Label | Mode | T:C | Linux ops/s | macOS ops/s | Δ% | Linux p99 us | macOS p99 us | err | corr |
|-------|------|-----|------:|------:|---:|-------:|-------:|----:|-----:|
| lin_16t_verify | lin | 16:16 | 233716 | 104917 | +122.8% | 114 | 256 | 0 | 0 |
| minslot_16t_verify | minslot | 16:16 | 233090 | 104988 | +122.0% | 111 | 241 | 0 | 0 |

Zero correctness errors on Linux. Verify overhead negligible (<1%).

---

## Existing Problems

No open problems are recorded for the read transport. The former HTTP/2
connection-lock bottleneck was resolved by moving the internal Paxos path
to crow-rpc's flatbuffer-over-TCP transport. The connection fan-in
benchmark remains as a regression sentinel for concurrent request handling.

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
- **Benchmark update (2026-08-28)** — Replaced the previous Linux/gRPC
  comparison with the latest Linux/crow-rpc run while retaining the macOS
  baseline. Compared with the previous Linux values, throughput changed by
  +104.2% / +90.7% at 1T, +52.1% / +83.6% at 6T, +121.1% / +132.2% at
  16T, and +88.0% / +91.3% at 32T for Linearizable / MinSlot. The p99
  latency changed by −51.1% / −43.7%, −34.5% / −37.5%, −54.5% / −59.3%,
  and −55.6% / −61.3% at those same thread counts. The 32T results are
  271184 / 265130 ops/s, up from 144262 / 138610 ops/s.
- **Consensus RPC transport** — The internal Paxos path now uses
  crow-rpc's flatbuffer-over-TCP transport with concurrent frame handling,
  replacing the HTTP/2/gRPC connection-lock bottleneck.
- **TCP_NODELAY** — Before the fix, read latency was ~41ms (Nagle +
  delayed ACK interaction in crow-rpc). After applying `TCP_NODELAY`
  to all client and server sockets, latency dropped to ~138us — a 290×
  improvement.

- **TCP_NODELAY Linux comparison (2026-08-28)** — The current Linux run
  uses crow-rpc and the previous Linux baseline used the legacy gRPC path.
  Positive throughput changes and negative p99 changes are improvements.

| Config | Old ops/s | New ops/s | Δ ops/s | Old p99 us | New p99 us | Δ p99 |
|--------|----------:|----------:|--------:|-----------:|-----------:|------:|
| `lin_1t` | 6608 | 13495 | +104.2% | 219 | 107 | −51.1% |
| `minslot_1t` | 6160 | 11746 | +90.7% | 222 | 125 | −43.7% |
| `lin_6t` | 50851 | 77338 | +52.1% | 194 | 127 | −34.5% |
| `minslot_6t` | 52252 | 95949 | +83.6% | 168 | 105 | −37.5% |
| `lin_16t` | 105313 | 232893 | +121.1% | 255 | 116 | −54.5% |
| `minslot_16t` | 101871 | 236501 | +132.2% | 268 | 109 | −59.3% |
| `lin_32t` | 144262 | 271184 | +88.0% | 498 | 221 | −55.6% |
| `minslot_32t` | 138610 | 265130 | +91.3% | 532 | 206 | −61.3% |
| `minslot_6t_2to1` | 52228 | 89795 | +71.9% | 172 | 113 | −34.3% |
| `lin_16t_verify` | 105781 | 233716 | +120.9% | 252 | 114 | −54.8% |
| `minslot_16t_verify` | 101552 | 233090 | +129.5% | 268 | 111 | −58.6% |

The largest throughput gain is MinSlot at 16T: **+132.2%**, while the
largest p99 improvement is MinSlot at 32T: **−61.3%**. This is the measured
Linux improvement from replacing gRPC with crow-rpc.
