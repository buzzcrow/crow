<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Scan Flow Analysis

End-to-end trace of the CROW scan (range read) path. Complements
[`kv-read-flow-analysis.md`](kv-read-flow-analysis.md) (point-read get
path) and [`kv-write-flow-analysis.md`](kv-write-flow-analysis.md).
Regression sentinel: `tools/bench-scan-regression.sh`.

---

## Scan Flow — Range Read

```
Client SCAN(prefix, start_after, limit, read_mode, min_slot?)
  → CrowkvClient::scan_stream                       [client.rs]
    1. resolve_min_slot — MinSlot: auto-attach write watermark;
       Linearizable: 0                                 [same as get]
    2. resolve_read_endpoint — Linearizable (or MinSlot + Leader
       policy): cached leader endpoint; MinSlot + AnyReplica:
       round-robin across replica endpoints, fallback to leader
    3. send KvScanRequest { prefix, start_after, limit, read_mode,
       min_slot } via ScanStream server-streaming RPC
       [copy: prefix + start_after → Bytes::copy_from_slice → HTTP/2
        frame, unavoidable]
    4. retry: first chunk carries not_leader_hint → follow (uncounted);
       error → counted + backoff; transport error → backoff + refresh
  → KvStoreService::scan_stream (gRPC)               [kv_service.rs]
    5. [Linearizable] if local not leader and not already forwarded →
       forward_kv_scan (unary) to leader, re-chunk response locally
    6. [MinSlot] no forwarding — serve local
       [copy: prefix + start_after from network frame, unavoidable]
  → PxKvStore::kv_scan                              [px_kv_store.rs]
    7. resolve_read_point(group, read_mode, min_slot) → ReadDecision
       [same resolver as get; min_slot passed through]
    8. [Serve] learner.engine_scan(prefix, start_after, limit)
       → KVEngine::scan → KVFuture<(Vec<(Bytes, slot, Bytes)>, truncated)>
       [CrowTreeEngine: prefix + start_after to_vec() for FFI;
        try_scan → ScanOutcome::Ready (fast path, all pages resident)
        or ScanOutcome::Pending (cold-leaf miss, reactor demand-load
        retry loop, cursor resumes from last resolved key); packed
        result take_buf; decode_scan slices a single Bytes per entry —
        zero-copy, no per-entry Vec<u8> allocation]
       [start_after pushed down into C++ engine: descent targets
        the leaf containing start_after, merge loop skips keys <=
        start_after natively, limit applied without over-fetching —
        O(limit) FFI + decode, not O(prefix range)]
  → chunk_scan_response                             [kv_service.rs]
    9. split KvScanResponse.items into KvScanChunk messages (256
       entries or 1 MiB per chunk); first chunk carries ok/error/
       not_leader_hint/read_slot; final chunk carries truncated
  → Client receives KvScanChunk stream
    10. reassemble: items.push((chunk.key, chunk.value)) per chunk;
        first chunk checked for redirect/error; return ScanOutcome
        { items: Vec<(Bytes, Bytes)>, truncated }
        [zero-copy: prost Bytes passed through directly, no to_vec]
```

---

## Memory Copy Summary

Copy points annotated inline in the flow diagram above. What remains:

- **O(N_l0) per scan — `MemTable::snapshot()`** — copies all memtable
  entries into a vector on every scan call, regardless of limit. Not
  the 1KiB anomaly's root cause (flush-after-prepopulate experiment
  refuted the L0 hypothesis — draining L0 did not close the gap; the
  maintenance loop's 3s tick already keeps L0 small during
  measurement). A lazy btree_map cursor would still make this O(limit)
  but would not fix the anomaly.
- **O(n) unavoidable** — gRPC request serialize (prefix + start_after
  into HTTP/2 frame); gRPC response serialize (items into socket); FFI
  prefix + start_after `to_vec()` (owned copies for the C API
  boundary).

The scan path is now zero-copy from packed buffer to client `Bytes`
(matching the get path after R6), except for the unavoidable FFI and
gRPC serialization copies.

---

## Benchmark Results — 2026-08-05 (macOS)

**Platform**: Apple M5 Pro, 18c, arm64, macOS 26.5. Not comparable to
the Linux Ryzen write baseline — re-capture on the same machine for
cross-comparable numbers.

**Setup**: 1T:1C, 10s mem mode, 3-node cluster, 100k pre-populated
keys. Raw TSV: `doc/working/bench-scan-regression.tsv` (gitignored).
Reference numbers in `tools/bench-scan-regression.sh`.

### Per-config results

All runs: `--workload list --mode mem --threads 1 --connections 1
--duration-secs 10 --pre-populate 100000`, 3-node cluster.

| Label | Limit | Prefix | Start_after | Val B | Mode | scans/s | avg us | p99 us | err |
|-------|------:|--------|-------------|------:|------|--------:|-------:|-------:|----:|
| full_1k | 1000 | | | 64 | lin | 243 | 4109 | 4672 | 0 |
| full_10k | 10000 | | | 64 | lin | 165 | 6063 | 6732 | 0 |
| full_100k | 100000 | | | 64 | lin | 20 | 49485 | 52416 | 0 |
| bounded_10 | 10 | | | 64 | lin | 223 | 4490 | 5036 | 0 |
| bounded_100 | 100 | | | 64 | lin | 230 | 4350 | 4804 | 0 |
| bounded_1k | 1000 | | | 64 | lin | 224 | 4463 | 5088 | 0 |
| bounded_10k | 10000 | | | 64 | lin | 164 | 6109 | 6764 | 0 |
| from_start_10 | 10 | | | 64 | lin | 231 | 4321 | 4836 | 0 |
| deep_pag_10 | 10 | | k...99989 | 64 | lin | 147 | 6786 | 9800 | 0 |
| deep_pag_100 | 100 | | k...99899 | 64 | lin | 143 | 6994 | 10136 | 0 |
| valuesize_64B | 1000 | | | 64 | lin | 202 | 4938 | 5380 | 0 |
| valuesize_1KiB | 1000 | | | 1024 | lin | 766 | 1304 | 2512 | 0 |
| valuesize_16KiB | 1000 | | | 16384 | lin | 27 | 17368 | 65184 | 309 |
| valuesize_64B_flushed | 1000 | | | 64 | lin | 213 | 4704 | 4888 | 0 |
| valuesize_1KiB_flushed | 1000 | | | 1024 | lin | 821 | 1217 | 1549 | 0 |
| prefix_1k | 1000 | k00 | | 64 | lin | 214 | 4679 | 5036 | 0 |
| whole_1k | 1000 | | | 64 | lin | 209 | 4788 | 5184 | 0 |
| lin_1k | 1000 | | | 64 | lin | 217 | 4599 | 4984 | 0 |
| minslot_1k | 1000 | | | 64 | minslot | 206 | 4845 | 5396 | 0 |

**Full-keyspace**: scales linearly (1k→10k: 2.2x keys, 1.5x time).
`full_100k` completes via the streaming `ScanStream` RPC — previously
0 scans/s with 6 transport errors from the 4 MiB unary cap.

**Bounded limit**: per-entry cost is small relative to the per-scan
setup — limit 10/100/1k are all ~4.4ms. At limit=10k the per-entry
cost becomes visible (6.1ms).

**Deep pagination**: O(limit) verdict CONFIRMED. Deep pagination is
1.6x slower than from-start (6786us vs 4321us at limit=10) — the
deeper B+tree descent cost (O(log N) inner-page levels), not O(prefix)
over-fetch. If the engine over-fetched, deep_pag_10 would cost
~49485us (the full_100k number), not 6786us. Deep pagination limit 10
vs 100 (6786 vs 6994us) are within 3% — cost is dominated by the
fixed descent, not entry count.

**Value-size sweep**: 16 KiB streaming mostly works (27 scans/s vs
previously 0) but 309 residual errors remain — retry edge cases under
high payload (follow-on item). 1 KiB anomaly: 1 KiB values are 3.8x
faster than 64 B (766 vs 202 scans/s) despite returning 16x more data
per scan. Persists after zero-copy scan values and L0 drain — root
cause is in the L1 B+tree scan path, not per-byte copy or L0 snapshot
cost.

**Flush-after-prepopulate**: the 3.8x gap is unchanged with L0
deterministically drained. The flag is a no-op because the
maintenance loop (3s tick) already keeps L0 small during measurement.
Useful for deterministic L1-only baselines.

**Prefix range**: no measurable difference — the per-scan fixed
overhead dominates at this scan width.

**Read-mode split**: no measurable difference at 1T:1C — the single
client has no concurrency to exploit MinSlot's follower local-serve.
The split would show a difference at higher concurrency.

### Cost split

- **Per-scan fixed** (~4.3ms): ReadIndex consensus round + gRPC
  roundtrip + B+tree descent. Dominates at small limit (10/100).
- **Per-entry** (~0.18us at 64B): leaf-chain traversal + packed-buffer
  decode + zero-copy value slice. Visible at limit >= 10k.
- **Deep-pagination descent** (~2.5ms): O(log N) B+tree descent to a
  leaf near the end. Fixed cost per scan.

### Comparison vs pre-zero-copy/streaming baseline

| Config | Before scans/s | After scans/s | Before err | After err | Notes |
|--------|---------------|---------------|-----------|----------|-------|
| full_1k | 227 | 243 | 0 | 0 | noise |
| full_10k | 139 | 165 | 0 | 0 | +19% |
| full_100k | 0 | 20 | 6 | 0 | streaming fix |
| bounded_10k | 137 | 164 | 0 | 0 | +20% |
| valuesize_1KiB | 666 | 766 | 0 | 0 | +15% |
| valuesize_16KiB | 0 | 27 | 1701 | 309 | streaming, residual errors |

Small scans (limit 10/100/1k) unchanged — the per-scan fixed cost
dominates. Larger scans (10k+) improved 15-20% from zero-copy values.
Streaming unblocked `full_100k` and `valuesize_16KiB`.

---

## Unsolved Issues

- **1 KiB anomaly root cause (unknown)**: 1 KiB values are 3.8x faster
  than 64 B despite returning 16x more data. The L0-snapshot
  hypothesis was refuted (flush-after-prepopulate did not close the
  gap). Zero-copy scan values did not close it. The real cause is in
  the L1 B+tree scan path or the decode path and needs an engine-level
  C++ microbench (flushed L1-only tree, vary value size, isolate
  per-leaf merge / delta-chain / decode cost) to identify. This is a
  prerequisite for scoping a fix.
- **L0 snapshot O(N_l0) cost**: `MemTable::snapshot()` copies all
  entries on every scan. The maintenance loop keeps L0 small during
  measurement, so this is not the anomaly's cause, but a
  lazy/range-bounded L0 cursor would still be an O(limit) improvement.
- **Streaming scan residual errors**: `valuesize_16KiB` shows 309
  errors despite the streaming scan RPC. Investigating retry edge
  cases under high payload is a follow-on item.
- **High-concurrency read-mode split**: this baseline is 1T:1C; the
  MinSlot vs Linearizable split at higher concurrency (where MinSlot's
  any-replica round-robin distributes load) is an end-to-end
  follow-on item.
- **Reverse scan**: `scan` is forward-only today; reverse scan is a
  distinct cost shape and would need its own baseline if added.
- **Engine-level cost split**: an engine-level C++ microbench would
  isolate per-entry copy from leaf-chain traversal and L0 merge,
  giving a tighter before/after measurement than the end-to-end
  baseline provides.
