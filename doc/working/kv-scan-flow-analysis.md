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

**Full-keyspace scan (limit >= keyspace, 64 B values)**

- `full_1k` (limit=1000): 243 scans/s, avg=4109us, p99=4672us
- `full_10k` (limit=10000): 165 scans/s, avg=6063us, p99=6732us
- `full_100k` (limit=100000): 20 scans/s, avg=49485us, p99=52416us, 0 errors

Scales linearly with key count (1k→10k: 2.2x keys, 1.5x time). The
100k scan completes via the streaming `ScanStream` RPC — previously
0 scans/s with 6 transport errors from the 4 MiB unary cap.

**Bounded limit over 100k keyspace (64 B values)**

- `bounded_10` (limit=10): 223 scans/s, avg=4490us
- `bounded_100` (limit=100): 230 scans/s, avg=4350us
- `bounded_1k` (limit=1000): 224 scans/s, avg=4463us
- `bounded_10k` (limit=10000): 164 scans/s, avg=6109us

Per-entry cost is small relative to the per-scan setup: limit
10/100/1k are all ~4.4ms. At limit=10k the per-entry cost becomes
visible (6.1ms).

**Deep pagination (start_after near end vs from-start companion)**

- `from_start_10` (limit=10, start_after=""): 231 scans/s, avg=4321us
- `deep_pag_10` (limit=10, start_after=k...99989): 147 scans/s, avg=6786us
- `deep_pag_100` (limit=100, start_after=k...99899): 143 scans/s, avg=6994us

O(limit) verdict: CONFIRMED. Deep pagination is 1.6x slower than
from-start (6786us vs 4321us at limit=10) — the deeper B+tree descent
cost (O(log N) inner-page levels), not O(prefix) over-fetch. If the
engine over-fetched, deep_pag_10 would cost ~49485us (the full_100k
number), not 6786us. Deep pagination limit 10 vs 100 (6786 vs 6994us)
are within 3% — cost is dominated by the fixed descent, not entry
count.

**Value-size sweep (fixed limit=1000, 100k keyspace)**

- `valuesize_64B`: 202 scans/s, avg=4938us, p99=5380us
- `valuesize_1KiB`: 766 scans/s, avg=1304us, p99=2512us
- `valuesize_16KiB`: 27 scans/s, avg=17368us, p99=65184us, 309 errors

16 KiB: streaming scan mostly works (27 scans/s vs previously 0) but
309 residual errors remain — retry edge cases under high payload
(follow-on item).

1 KiB anomaly: 1 KiB values are 3.8x faster than 64 B values (766 vs
202 scans/s) despite returning 16x more data per scan. Persists after
zero-copy scan values and L0 drain — root cause is in the L1 B+tree
scan path, not per-byte copy or L0 snapshot cost.

**Value-size sweep with --flush-after-prepopulate (L0 hypothesis)**

- `valuesize_64B_flushed`: 213 scans/s, avg=4704us (vs 202 unflushed)
- `valuesize_1KiB_flushed`: 821 scans/s, avg=1217us (vs 766 unflushed)

The 3.8x gap is unchanged with L0 deterministically drained. The flag
is a no-op because the maintenance loop (3s tick) already keeps L0
small during measurement. The flag remains useful for deterministic
L1-only baselines.

**Prefix range (bounded prefix vs whole-keyspace, same entry count)**

- `prefix_1k` (prefix="k00", limit=1000): 214 scans/s, avg=4679us
- `whole_1k` (prefix="", limit=1000): 209 scans/s, avg=4788us

No measurable difference — the per-scan fixed overhead dominates at
this scan width.

**Read-mode split (linearizable vs minslot, limit=1000)**

- `lin_1k` (linearizable): 217 scans/s, avg=4599us
- `minslot_1k` (minslot, any-replica): 206 scans/s, avg=4845us

No measurable difference at 1T:1C — the single client has no
concurrency to exploit MinSlot's follower local-serve. The split
would show a difference at higher concurrency.

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
