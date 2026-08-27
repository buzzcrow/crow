<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# KV Read Flow Analysis

Point reads (`get`) from the client through crow-rpc, the Paxos read
policy, and the storage engine. The benchmark sentinel is
`tools/bench-kv-read-regression.sh`.

## 1. Flow

```text
CrowkvClient::get(key, read_mode, min_slot?)
  -> resolve_min_slot and resolve_read_endpoint
     Linearizable: cached leader
     MinSlot + AnyReplica: round-robin replicas, then leader fallback
  -> KvGetRequest
  -> retry NotLeaderHint by following the hint; refresh after transport errors
  -> KvStoreService::get
     Linearizable: forward non-leader requests once
     MinSlot: serve locally
  -> PxKvStore::kv_get
  -> resolve_read_point -> ReadDecision
  -> learner.engine_get_bytes -> KVEngine::get_bytes
  -> KvResponse { read_slot, safe_slot, value: Bytes }
  -> crow-rpc serializes the response
```

Read policies:

- **Linearizable** uses the leader. A lease fast path avoids an extra round
  trip; after lease expiry, a batched ReadIndex heartbeat confirms quorum.
- **MinSlot** serves from any replica whose `contiguous_applied` reaches
  `min_slot`. The client attaches its last write slot for read-your-writes.
  `min_slot = 0` permits any replica state.

The hot path has no intermediate key copy after request decoding. Values use
`Bytes`; the in-memory engine clones the value out of its shard, while the
crow-tree fast path returns bytes backed by the resident frame. Serialization
and the kernel socket copy remain unavoidable.

## 2. Latest Benchmark Results

Both runs use a 3-node cluster, 100k pre-populated keys, 64B values, and mem
mode. Linux ran for 20s; macOS is the retained 10s baseline. All runs had zero
errors and zero correctness errors where verification was enabled.

### Linux — 2026-08-28

AMD Ryzen 9 5950X, 16c/32t, x86_64, Ubuntu 24.04.

| Config | Mode | T:C | ops/s | avg us | p50 us | p99 us | p999 us |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| 1T | Linearizable | 1:1 | 13,495 | 73 | 76 | 107 | 151 |
| 1T | MinSlot | 1:1 | 11,746 | 84 | 85 | 125 | 161 |
| 6T | Linearizable | 6:6 | 77,338 | 76 | 74 | 127 | 152 |
| 6T | MinSlot | 6:6 | 95,949 | 61 | 59 | 105 | 131 |
| 16T | Linearizable | 16:16 | 232,893 | 67 | 65 | 116 | 147 |
| 16T | MinSlot | 16:16 | 236,501 | 66 | 64 | 109 | 150 |
| 32T | Linearizable | 32:32 | 271,184 | 116 | 112 | 221 | 273 |
| 32T | MinSlot | 32:32 | 265,130 | 119 | 116 | 206 | 276 |
| 6T fan-in | MinSlot | 6:3 | 89,795 | 66 | — | 113 | — |

At 6T, MinSlot is 24.0% faster. The modes are within 1.6% at 16T, and
Linearizable is 2.3% faster at 32T. The 6T:3C fan-in run is error-free and
reaches 93.6% of the 6T:6C MinSlot result.

### macOS — 2026-08-19

Apple M5 Pro, 18c, arm64, macOS 26.5.

| Config | Mode | T:C | ops/s | avg us | p50 us | p99 us | p999 us |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| 1T | Linearizable | 1:1 | 21,112 | 46 | 46 | 67 | 97 |
| 1T | MinSlot | 1:1 | 21,691 | 45 | 44 | 66 | 96 |
| 6T | Linearizable | 6:6 | 70,668 | 84 | 79 | 163 | 206 |
| 6T | MinSlot | 6:6 | 77,622 | 76 | 73 | 142 | 181 |
| 16T | Linearizable | 16:16 | 106,399 | 148 | 142 | 251 | 298 |
| 16T | MinSlot | 16:16 | 107,455 | 147 | 145 | 235 | 281 |
| 32T | Linearizable | 32:32 | 119,473 | 265 | 260 | 418 | 512 |
| 32T | MinSlot | 32:32 | 113,270 | 280 | 278 | 432 | 521 |
| 6T fan-in | MinSlot | 6:3 | 74,752 | 79 | — | 151 | — |

Linux is slower at 1T but faster from 6T onward. At 32T it reaches 2.3x the
macOS Linearizable throughput and 2.4x the MinSlot throughput. The platforms
are not identical benchmark environments, so compare trends rather than
absolute engine cost.

## 3. Change History

### Read endpoint distribution

MinSlot reads can use all replica endpoints with a `min_slot` fence.
Previously every MinSlot read went to the leader, wasting follower capacity.

Perf: 6T Linux MinSlot throughput rose from 77,338 to 95,949 ops/s, a 24.0%
gain over Linearizable at the same concurrency.

### Batched read barrier

Reads arriving after lease expiry now share one ReadIndex heartbeat instead
of each starting its own round. A pending batch collects later readers and
adopts the same outcome.

Perf: eliminates per-read heartbeat rounds under lease-expiry bursts; no
throughput regression at steady state.

### Zero-copy value path

`PinnedValue::into_bytes()` produces a `Bytes` backed by the C++ frame via
`Bytes::from_owner`; page refcount pins keep the frame alive until the
`Bytes` is dropped. The intermediate `Vec<u8>` in the engine get path was
removed — the fast path returns `PinnedValue` borrowing the frame, and the
final `Bytes` is produced in one copy instead of frame → `Vec` → `Bytes`.

Perf: one fewer heap allocation and one fewer memcpy per read on the crow-tree
fast path.

### TCP transport migration

The internal Paxos path moved from the HTTP/2/gRPC connection-lock transport
to flatbuffer-over-TCP crow-rpc with concurrent frame handling.

Perf: compared with the previous Linux gRPC baseline, throughput improved
104–132% from 1T to 16T and 88–91% at 32T; p99 latency fell 34–61%. The 32T
Linearizable result went from 144,262 to 271,184 ops/s.

### Socket latency fix

Before the fix, read latency was ~41ms due to Nagle + delayed ACK
interaction in crow-rpc. Applying `TCP_NODELAY` to all client and server
sockets dropped latency to ~138us.

Perf: 290x latency reduction (41ms → 138us).

### Benchmark update (2026-08-28)

Replaced the Linux reference with the current crow-rpc run and retained the
macOS baseline. The current peak is 271,184 Linearizable ops/s at 32T:32C,
with zero errors. The previous Linux baseline used the legacy gRPC path;
positive throughput deltas and negative p99 deltas are improvements.

| Config | Old ops/s | New ops/s | Δ ops/s | Old p99 us | New p99 us | Δ p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| lin_1t | 6,608 | 13,495 | +104.2% | 219 | 107 | −51.1% |
| minslot_1t | 6,160 | 11,746 | +90.7% | 222 | 125 | −43.7% |
| lin_6t | 50,851 | 77,338 | +52.1% | 194 | 127 | −34.5% |
| minslot_6t | 52,252 | 95,949 | +83.6% | 168 | 105 | −37.5% |
| lin_16t | 105,313 | 232,893 | +121.1% | 255 | 116 | −54.5% |
| minslot_16t | 101,871 | 236,501 | +132.2% | 268 | 109 | −59.3% |
| lin_32t | 144,262 | 271,184 | +88.0% | 498 | 221 | −55.6% |
| minslot_32t | 138,610 | 265,130 | +91.3% | 532 | 206 | −61.3% |
| minslot_6t_2to1 | 52,228 | 89,795 | +71.9% | 172 | 113 | −34.3% |
| lin_16t_verify | 105,781 | 233,716 | +120.9% | 252 | 114 | −54.8% |
| minslot_16t_verify | 101,552 | 233,090 | +129.5% | 268 | 111 | −58.6% |

Largest throughput gain: MinSlot 16T at +132.2%. Largest p99 improvement:
MinSlot 32T at −61.3%.
