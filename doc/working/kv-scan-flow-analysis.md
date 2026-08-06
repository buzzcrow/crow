setup metrics can measure each major step of scan, then we can compare the difference.
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

- **O(limit) per scan — L0 cursor walk** (R50): the MemTable
  skip-list cursor materializes only entries that reach the output,
  not the whole L0. This replaced the O(N_l0) `snapshot()` copy.
- **O(n) unavoidable** — gRPC request serialize (prefix + start_after
  into HTTP/2 frame); gRPC response serialize (items into socket); FFI
  prefix + start_after `to_vec()` (owned copies for the C API
  boundary).

The scan path is now zero-copy from packed buffer to client `Bytes`
(matching the get path after R6), except for the unavoidable FFI and
gRPC serialization copies.

---

## Benchmark Results — 2026-08-06 (macOS, post-R48+R50)

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
| full_1k | 1000 | | | 64 | lin | 4264 | 233 | 312 | 0 |
| full_10k | 10000 | | | 64 | lin | 524 | 1906 | 2104 | 0 |
| full_100k | 100000 | | | 64 | lin | 50 | 19975 | 20880 | 0 |
| bounded_10 | 10 | | | 64 | lin | 20932 | 46 | 67 | 0 |
| bounded_100 | 100 | | | 64 | lin | 15429 | 63 | 84 | 0 |
| bounded_1k | 1000 | | | 64 | lin | 4355 | 228 | 255 | 0 |
| bounded_10k | 10000 | | | 64 | lin | 513 | 1947 | 2074 | 0 |
| from_start_10 | 10 | | | 64 | lin | 20708 | 47 | 71 | 0 |
| deep_pag_10 | 10 | | k...99989 | 64 | lin | 20664 | 47 | 67 | 0 |
| deep_pag_100 | 100 | | k...99899 | 64 | lin | 15423 | 63 | 85 | 0 |
| valuesize_64B | 1000 | | | 64 | lin | 4339 | 229 | 254 | 0 |
| valuesize_1KiB | 1000 | | | 1024 | lin | 1492 | 669 | 782 | 0 |
| valuesize_16KiB | 1000 | | | 16384 | lin | 74 | 8383 | 21536 | 452 |
| valuesize_64B_flushed | 1000 | | | 64 | lin | 4344 | 229 | 259 | 0 |
| valuesize_1KiB_flushed | 1000 | | | 1024 | lin | 1428 | 699 | 795 | 0 |
| prefix_1k | 1000 | k00 | | 64 | lin | 4263 | 233 | 258 | 0 |
| whole_1k | 1000 | | | 64 | lin | 4290 | 232 | 259 | 0 |
| lin_1k | 1000 | | | 64 | lin | 4380 | 227 | 254 | 0 |
| minslot_1k | 1000 | | | 64 | minslot | 4230 | 235 | 262 | 0 |

**Full-keyspace**: scales linearly (1k→10k: 2.2x keys, 8.2x time —
per-entry cost is now visible at the lower fixed-cost baseline).
`full_100k` completes via S3-style pagination (byte-budgeted unary
RPC) — previously 0 scans/s with 6 transport errors from the 4 MiB
unary cap.

**Bounded limit**: the per-scan fixed cost collapsed from ~4.4ms to
~46us (95x improvement) — R48 (lazy leaf cursor) + R50 (epoch-protected
MemTable) eliminated the O(entries-per-leaf) and O(N_l0) costs that
dominated every scan regardless of limit. At limit=10k the per-entry
cost is visible again (1.9ms).

**Deep pagination**: O(limit) verdict CONFIRMED and now flat. Deep
pagination is equal to from-start (20664 vs 20708 scans/s at limit=10)
— the B+tree descent cost is negligible at this scale. Previously 1.6x
slower (6786us vs 4321us).

**Value-size sweep**: the 1 KiB anomaly is FIXED and INVERTED — 64B
is now 2.9x faster than 1 KiB (4339 vs 1492 scans/s), the expected
ordering (cost tracks bytes returned, not entries per leaf). 16 KiB
errors persist (452) — root cause identified as replication
backpressure, not a scan path issue (see § Unsolved Issues).

**Flush-after-prepopulate**: no measurable difference vs non-flushed
— the maintenance loop (3s tick) already keeps L0 small during
measurement. Useful for deterministic L1-only baselines.

**Prefix range**: no measurable difference — the per-scan fixed
overhead dominates at this scan width.

**Read-mode split**: no measurable difference at 1T:1C — the single
client has no concurrency to exploit MinSlot's follower local-serve.
The split would show a difference at higher concurrency.

### Before/after comparison (2026-08-05 → 2026-08-06, R48+R50)

| Config | Before scans/s | After scans/s | Improvement | Before err | After err |
|--------|---------------:|--------------:|------------:|-----------:|----------:|
| full_1k | 243 | 4264 | 17.5x | 0 | 0 |
| full_10k | 165 | 524 | 3.2x | 0 | 0 |
| full_100k | 20 | 50 | 2.5x | 0 | 0 |
| bounded_10 | 223 | 20932 | 93.9x | 0 | 0 |
| bounded_100 | 230 | 15429 | 67.1x | 0 | 0 |
| bounded_1k | 224 | 4355 | 19.4x | 0 | 0 |
| bounded_10k | 164 | 513 | 3.1x | 0 | 0 |
| deep_pag_10 | 147 | 20664 | 140.6x | 0 | 0 |
| valuesize_64B | 202 | 4339 | 21.5x | 0 | 0 |
| valuesize_1KiB | 766 | 1492 | 1.9x | 0 | 0 |

The dominant improvements are on bounded scans (20-140x) where the
O(entries-per-leaf) and O(N_l0) costs were eliminated. Full-keyspace
scans improve less because they genuinely touch more data. The 1 KiB
anomaly is inverted: 64B is now faster than 1 KiB, as expected.

### Cost split

- **Per-scan fixed** (~0.4-0.6ms): ReadIndex consensus round + gRPC
  roundtrip + FFI decode. The portion of end-to-end latency NOT inside
  the C++ `scan` call (end-to-end avg minus `scan.l` avg).
- **L1 leaf resolve** (~3.98ms at 64B / ~0.49ms at 1KiB): was the
  dominant cost — each touched leaf's full live entry set was rebuilt
  into a `std::map` per scan. Fixed by the lazy `LeafChainCursor`; see
  § Scan Per-Step Profile.
- **Merge + decode** (~19us at 64B / ~37us at 1KiB): min-key selection +
  winner resolution + value materialization. Small.
- **Deep-pagination descent** (~2.5ms): O(log N) B+tree descent to a
  leaf near the end. Fixed cost per scan.

### Comparison vs pre-zero-copy/streaming baseline (historical)

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

**Note**: this table predates R48 (lazy leaf cursor) and R50
(epoch-protected MemTable). See the "Before/after comparison" table
above for the post-R48+R50 numbers — bounded scans improved 20-140x
from those two changes alone.

---

## Scan Per-Step Profile (R48)

Per-step timing added to the C++ scan path (`Crowtree::scan` and
`try_scan_no_load`): `l0_snapshot` (MemTable::snapshot copy), `l0_skip`
(upper_bound pass), `l1_descent` (find_leaf_page_id), `l1_resolve`
(resolve_chain_sorted, summed across touched leaves), `merge` (min-key
select + winner + consider/decode, excluding l1_resolve), `total`.
Exposed via `Crowtree::scan_profile()` and the `scan.*` C++ metrics; the
`scan_step_profile` microbench (`lib/crow-tree/bench/scan_step_bench.cpp`,
build with `-DCROW_TREE_BENCH=ON`) isolates the L1 path in a controlled
flushed tree.

### Production (3-node, mem, 100k keys, limit=1000, leader C++ scan time)

| Config | scan.l avg | l1_resolve | l0_snapshot | merge | l1_resolve % |
|--------|-----------:|-----------:|------------:|------:|-------------:|
| 64B (flushed) | 4004us | 3985us | 0us | 19us | 99.5% |
| 64B (non-flushed) | 3858us | 3837us | 0us | 20us | 99.5% |
| 1KiB (flushed) | 529us | 492us | 0us | 37us | 93.0% |

`l0_snapshot` is 0us in production both with and without
`--flush-after-prepopulate` — the maintenance loop's 3s tick already
drains L0 during pre-populate, so L0 is empty by measurement time. The
flag is a confirmed no-op for this benchmark. End-to-end 64B flushed =
4408us, 1KiB flushed = 1154us (3.8x); the C++ `scan.l` ratio is 7.6x
(4004/529), diluted to 3.8x end-to-end by the ~625us fixed
consensus+gRPC overhead.

### Microbench (scan_step_profile, L1-only, single flush, 100k keys)

| Config | total | l1_resolve | merge | l0_snapshot |
|--------|------:|-----------:|------:|------------:|
| 64B limit=1000 | 1183us | 1170us (98.7%) | 15us | 0.1us |
| 1KiB limit=1000 | 476us | 443us (93%) | 33us | 0.1us |
| 64B limit=10 | 1163us | 1162us | 0.2us | 0.1us |
| 64B limit=10000 | 2522us | 2367us | 154us | 0.1us |

`l1_resolve` is 2.6x slower for 64B (1170 vs 443us) even with L0 empty —
the L1 path alone is a 2.6x cause. limit=10 costs the same as limit=1000
(both resolve the whole first leaf): the cost is O(entries-per-leaf),
not O(limit). limit=10000 (~16 leaves) roughly doubles vs limit=1000
(~2 leaves), confirming per-leaf scaling.

### L0 snapshot cost (microbench, 100k entries in L0, unflushed)

| Config | l0_snapshot | total | l0_snapshot % |
|--------|------------:|------:|--------------:|
| 64B | 120us | 136us | 88% |
| 1KiB | 387us | 429us | 90% |

With an EQUAL L0 entry count, 1KiB snapshot is SLOWER (bigger cells to
copy in `materialize()`). So the production L0 advantage for 1KiB (if
any) would come purely from fewer entries (byte-threshold freezing:
4MiB/1KiB ≈ 4k vs 4MiB/64B ≈ 46k), not per-entry cost. In practice L0
is empty by measurement time, so this is moot for the anomaly.

### L0 snapshot cost under concurrent write+scan (R50 Gate 2)

The single-threaded microbench above forces L0 full by not flushing —
artificial. The production bench shows 0us because it is
pre-populate-then-scan with no concurrent writes (the 3s maintenance
tick drains L0 during pre-populate). The Gate 2 bench
(`scan_step_bench.cpp` `run_concurrent`) adds the missing ingredient:
a sustained writer thread + scanner, with a 3s flush thread mimicking
the maintenance tick. Pre-populates 100k keys into L1, then concurrent
write+scan for 10s, `max_memtable_count=2` (production default).

| Scenario | Write rate | Flush | l0_snapshot avg | max | % of scan | total avg |
|----------|-----------:|------|----------------:|----:|----------:|----------:|
| 64B flush 3s | 1261k/s | 3s | 126.2us | 1554us | 81.8% | 154.3us |
| 1KiB flush 3s | 888k/s | 3s | 1122us | 2538us | 93.5% | 1199.8us |
| 64B no flush | 1294k/s | none | 128.4us | 380us | 82.8% | 155.0us |
| 1KiB no flush | 1224k/s | none | 5816us | 7449us | 97.8% | 5944us |

Under sustained concurrent write+scan, L0 fills between flush ticks (1
frozen + 1 overgrown active_, bounded by the keyspace with overwrites),
and `l0_snapshot` dominates scan time (82–94%). The 64B flush vs
no-flush numbers are nearly identical because the writer fills both
memtable slots in <100ms, so the 3s tick barely matters — L0 size is
bounded by the keyspace, not the flush rate. The 1KiB no-flush upper
bound (5816us) shows the cost when flush completely can't keep up
(`max_memtable_count=8`, frozen_ accumulates). Post-R48, with
`l1_resolve` at 0us, `l0_snapshot` is the dominant remaining scan cost
when L0 is non-empty. This clears R50 Gate 2 — the premise is verified.

### Microbench after the lazy cursor (same scenarios, same machine)

| Config | total before | total after | l1_resolve after |
|--------|-------------:|------------:|-----------------:|
| 64B limit=1000 | 1183us | 19.9us | 0.0us |
| 1KiB limit=1000 | 476us | 32.2us | 0.0us |
| 64B limit=10 | 1163us | 0.4us | 0.0us |
| 64B limit=10000 | 2522us | 190.4us | 0.1us |

O(limit) is confirmed end to end: limit=10 is now ~50x cheaper than
limit=1000 (it was identical before), and 64B is cheaper than 1KiB at
equal limit — cost tracks bytes returned, not entries per leaf, so the
anomaly is inverted back to the expected ordering. `l1_resolve` now
measures per-leaf cursor setup + seek only; the per-entry cursor step
falls under `merge` (timing each step would cost more than the step).

### Root cause (fixed) — eager whole-leaf resolution

`resolve_chain_sorted` rebuilds each touched leaf's ENTIRE live entry
set into a `std::map<Slice, Slice>` (node-per-entry, heap-allocated
red-black tree) plus a sorted `std::vector<leaf_entry>` with per-entry
`.to_string()` key/cell copies, on EVERY scan — even when only `limit`
entries are needed. The merge loop's `refill_l1` calls it on the whole
leaf before producing any entries, so the cost is
O(entries-per-leaf × log entries-per-leaf), not O(limit). 64B values
pack ~640 entries per 64KiB leaf (leaf_split_bytes default) vs ~58 for
1KiB, so each leaf resolve is far more expensive for 64B. This is why
1KiB (16x more data, 11x fewer entries per leaf) is faster: the scan
touches more leaves for 1KiB but each resolve is cheap, while 64B
resolves a few dense leaves expensively.

Production amplifies the 64B per-leaf cost beyond the single-flush
microbench (3985us vs 1170us) — the production flush pattern (freeze at
~46k entries via the 4MiB byte threshold, drain via the 3s maintenance
tick) shapes the tree with denser leaves than a single end-of-load
flush. An incremental-flush microbench variant (flush every 30k keys)
produces sparser split leaves and drops 64B l1_resolve to 292us,
confirming leaf fullness is the driver. The root mechanism (per-leaf
std::map rebuild scaling with entries-per-leaf) is identical in all
cases.

### Fix — lazy `LeafChainCursor`

The chain's inputs are already key-sorted (each BatchDelta's entries,
the base frame's main slots) except the small in-frame delta overlay.
The cursor merges them k-way on demand, resolving highest-slot-wins as
it goes, and binary-searches each stream on `seek` — so a scan pays for
the entries it emits, not for the leaf. `resolve_chain_sorted` is now
that cursor drained into an owned vector, for the callers that really
do need the whole page (`iter_all` / `compare`, snapshot materialize, GC).
The L0 copy cost is a separate issue (R50, epoch-protected MemTable);
it saves ~0us in this benchmark because L0 is already empty by
measurement time.

---

## Unsolved Issues

- **1 KiB anomaly (FIXED)**: 1 KiB values scanned 3.8x faster than 64 B
  despite returning 16x more data. Root cause was eager whole-leaf
  resolution — each touched leaf's full entry set rebuilt into a
  `std::map` per scan, O(entries-per-leaf × log) rather than O(limit).
  64B packs ~640 entries/64KiB leaf vs ~58 for 1KiB, so each leaf
  resolve was far more expensive for 64B. Confirmed by per-step
  metrics: l1_resolve was 99.5% of the 64B C++ scan (3985us) and 0us of
  it was L0 (L0 is empty by measurement time). Fixed by the lazy
  `LeafChainCursor` (R48) — see § Scan Per-Step Profile for before/after.
  Post-fix: 64B is now 2.9x faster than 1 KiB (4339 vs 1492 scans/s),
  the expected ordering (cost tracks bytes returned, not entries per
  leaf).
- **L0 snapshot O(N_l0) cost (FIXED — R50)**:
  `MemTable::snapshot()` copied all entries on every scan. Replaced by
  a `ConcurrentSkipList` with epoch-deferred reclamation (R50) —
  readers traverse L0 lock-free under their existing epoch guard with
  zero copy; the cursor seeks directly and materializes only O(limit)
  entries. Post-fix: bounded_10 improved 93.9x (223 → 20932 scans/s),
  bounded_1k improved 19.4x (224 → 4355 scans/s). See § L0 snapshot
  cost under concurrent write+scan for the Gate 2 measurements that
  motivated the fix.
- **16 KiB scan errors (ROOT CAUSE FOUND — replication backpressure)**:
  `valuesize_16KiB` shows intermittent errors (452 in the latest run,
  0 in some re-runs). Root cause is NOT the scan path — it is the
  learner_stream outbound queue filling up during pre-populate with
  16 KiB values (1.6 GiB of data), blocking heartbeats to followers.
  Server logs show: `learner_stream: outbound queue full (reserved for
  heartbeats)` followed by `kv scan failed: not leader` (the leader
  loses leadership when heartbeats can't be sent). This is a
  replication-layer backpressure issue amplified by large value sizes,
  not a scan correctness bug. The scan path itself is correct — the
  linearizable read barrier fails because the leader can't maintain
  quorum. Fix: increase the learner_stream outbound queue capacity or
  add backpressure signaling so the writer slows down when the queue is
  near full. Follow-on item.
- **High-concurrency read-mode split**: this baseline is 1T:1C; the
  MinSlot vs Linearizable split at higher concurrency (where MinSlot's
  any-replica round-robin distributes load) is an end-to-end
  follow-on item. No measurable difference at 1T:1C (4380 vs 4230
  scans/s — noise).
- **Reverse scan**: `scan` is forward-only today; reverse scan is a
  distinct cost shape and would need its own baseline. Tracked as
  a backlog item (R52).
