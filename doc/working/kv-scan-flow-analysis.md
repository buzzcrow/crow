<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Scan Flow Analysis

End-to-end trace of the CROW scan (range read) path. Complements
[`kv-read-flow-analysis.md`](kv-read-flow-analysis.md) (point-read get
path) and [`kv-write-flow-analysis.md`](kv-write-flow-analysis.md).
Regression sentinel: `tools/bench-scan-regression.sh`.

---

## Scan Flow

```
Client SCAN(prefix, start_after, limit, read_mode, min_slot?)
  → CrowkvClient::scan                            [client.rs]
    1. resolve_min_slot — MinSlot: auto-attach write watermark;
       Linearizable: 0
    2. resolve_read_endpoint — Linearizable: cached leader;
       MinSlot + AnyReplica: round-robin across replicas
    3. send KvScanRequest via unary RPC, S3-style pagination
       (server applies a 3.5 MiB byte budget per page; client
       transparently pages until !truncated or limit reached)
    4. retry: not_leader_hint → follow (uncounted);
       error → counted + backoff; transport error → refresh + backoff
  → KvStoreService::scan (gRPC)                    [kv_service.rs]
    5. [Linearizable] forward to leader if not already
    6. [MinSlot] serve local — no forwarding
  → PxKvStore::kv_scan                             [px_kv_store.rs]
    7. resolve_read_point → ReadDecision (same as get)
    8. learner.engine_scan(prefix, start_after, limit, byte_budget)
  → CrowTreeEngine::scan → try_scan                [crow_tree_engine.rs]
    9. try_scan → ScanOutcome::Ready (fast path, all pages resident)
       or ScanOutcome::Pending (cold-leaf miss, reactor demand-load
       retry loop, cursor resumes from last resolved key)
       [start_after pushed down: descent targets the leaf containing
        start_after, merge loop skips keys <= start_after natively,
        limit applied without over-fetching — O(limit), not O(prefix)]
  → Crowtree::scan (C++ engine)                    [crow-tree.cpp]
    10. L0: skip-list cursor (R50) — lock-free, zero-copy,
        materializes only entries that reach the output
    11. L1: LeafChainCursor (R48) — lazy k-way merge of delta chain
        + base frame, binary-search seek, O(limit) not O(entries-per-leaf)
    12. merge loop: min-key select, highest-slot-wins on collision,
        early stop past prefix; byte budget caps the page
    13. packed result → take_buf → decode_scan slices one Bytes per
        entry — zero-copy, no per-entry Vec<u8>
  → Client receives KvScanResponse
    14. prost Bytes passed through directly (no to_vec);
        pagination continues if truncated and limit not reached
```

**Copy points**: O(limit) for L0/L1 cursor materialization (only
entries that reach the output); O(n) unavoidable for gRPC + FFI
serialization. The scan path is zero-copy from packed buffer to client
`Bytes`, matching the get path after R6.

---

## Change History

- **R6** — Zero-copy value returns: L1 hits borrow directly from the
  resident leaf frame (no `std::string` staging). Scan values are
  zero-copy from packed buffer to client `Bytes`.
- **R38/R44/R49** — Zero-copy scan values + streaming scan RPC.
  Unblocked `full_100k` (previously 0 scans/s, 4 MiB unary cap) and
  `valuesize_16KiB` (previously 0 scans/s). 15-20% improvement on
  large scans.
- **R48** — Lazy `LeafChainCursor`: replaced eager whole-leaf
  resolution with an on-demand k-way cursor. Scan cost went from
  O(entries-per-leaf) to O(limit). Fixed the 1 KiB anomaly.
- **R50** — Epoch-protected lock-free MemTable: replaced
  `absl::btree_map` under `mu_` with a `ConcurrentSkipList`.
  Eliminated the O(N_l0) `snapshot()` copy; readers traverse L0
  lock-free with zero copy.

---

## Latest Benchmark Results — 2026-08-06 (post-R48+R50)

**Platform**: Apple M5 Pro, 18c, arm64, macOS 26.5.
**Setup**: 10s mem mode, 3-node cluster, 100k pre-populated keys.
Raw TSV: `doc/working/bench-scan-regression.tsv` (gitignored).

### Single-thread (1T:1C) — per-scan engine cost

| Label | Limit | Start_after | Val B | Mode | scans/s | avg us | p99 us | err |
|-------|------:|-------------|------:|------|--------:|-------:|-------:|----:|
| bounded_10 | 10 | | 64 | lin | 19558 | 50 | 79 | 0 |
| bounded_1k | 1000 | | 64 | lin | 4339 | 229 | 258 | 0 |
| bounded_10k | 10000 | | 64 | lin | 518 | 1929 | 2060 | 0 |
| full_100k | 100000 | | 64 | lin | 50 | 20216 | 20848 | 0 |
| deep_pag_10 | 10 | k...99989 | 64 | lin | 20681 | 47 | 66 | 0 |
| mixed_1k | 1000 | | mixed | lin | 991 | 1007 | 1222 | 0 |
| minslot_1k | 1000 | | 64 | minslot | 4293 | 232 | 262 | 0 |

`mixed_1k` uses `--value-size-mix 64:70,1024:20,16384:10` — 70% 64B,
20% 1KiB, 10% 16KiB values, deterministically assigned by key id. At
991 scans/s it sits between the old `valuesize_1KiB` (1492) and
`valuesize_16KiB` (74), reflecting the weighted average of the three
sizes with 0 errors (the 16KiB fraction is small enough to avoid the
replication backpressure issue seen at 100% 16KiB).

### Multi-thread — max throughput + read-mode split

| Label | Limit | Val B | Mode | T:C | scans/s | avg us | p99 us | err |
|-------|------:|------:|------|-----|--------:|-------:|-------:|----:|
| lin_4t | 1000 | 64 | lin | 4:4 | 14264 | 279 | 473 | 0 |
| minslot_4t | 1000 | 64 | minslot | 4:4 | 14810 | 269 | 385 | 0 |
| lin_16t | 1000 | 64 | lin | 16:16 | 30799 | 517 | 822 | 0 |
| minslot_16t | 1000 | 64 | minslot | 16:16 | 33015 | 482 | 791 | 0 |
| lin_32t | 1000 | 64 | lin | 32:32 | 37840 | 842 | 3600 | 0 |
| minslot_32t | 1000 | 64 | minslot | 32:32 | 38256 | 830 | 2028 | 0 |

Linearizable scales well up to 16T (4339 → 30799, 7.1x) then saturates
at 32T (37840) — the leader read barrier becomes the bottleneck.
MinSlot shows a clear advantage:
- **16T:16C**: +7.2% throughput (33015 vs 30799) — distributed read
  serving across 3 replicas scales better than single-leader.
- **32T:32C**: throughput saturates for both (+1.1%), but MinSlot's
  p99 is 44% better (2028us vs 3600us) — load distribution keeps tail
  latency low even when throughput is capped by the engine.

### Linux results — 2026-08-06

**Platform**: AMD Ryzen 9 5950X, 16c/32t, x86_64, Ubuntu 24.04.
**Setup**: 10s mem mode, 3-node cluster, 100k pre-populated keys.
Raw TSV: `doc/working/bench-scan-regression.tsv` (gitignored).

#### Single-thread (1T:1C)

| Label | Limit | Start_after | Val B | Mode | Linux scans/s | macOS scans/s | Δ% | L/M | Linux p99 us | macOS p99 us | err |
|-------|------:|-------------|------:|------|--------:|--------:|---:|----:|-------:|-------:|----:|
| bounded_10 | 10 | | 64 | lin | 5367 | 19558 | -73% | 0.27 | 261 | 79 | 0 |
| bounded_1k | 1000 | | 64 | lin | 1475 | 4339 | -66% | 0.34 | 1054 | 258 | 0 |
| bounded_10k | 10000 | | 64 | lin | 110 | 518 | -79% | 0.21 | 10464 | 2060 | 0 |
| full_100k | 100000 | | 64 | lin | 13 | 50 | -74% | 0.26 | 115968 | 20848 | 0 |
| deep_pag_10 | 10 | k...99989 | 64 | lin | 5704 | 20681 | -72% | 0.28 | 249 | 66 | 0 |
| mixed_1k | 1000 | | mixed | lin | 265 | 991 | -73% | 0.27 | 5788 | 1222 | 0 |
| minslot_1k | 1000 | | 64 | minslot | 993 | 4293 | -77% | 0.23 | 1376 | 262 | 0 |

#### Multi-thread

| Label | Limit | Val B | Mode | T:C | Linux scans/s | macOS scans/s | Δ% | L/M | Linux p99 us | macOS p99 us | err |
|-------|------:|------:|------|-----|--------:|--------:|---:|----:|-------:|-------:|----:|
| lin_4t | 1000 | 64 | lin | 4:4 | 5967 | 14264 | -58% | 0.42 | 1247 | 473 | 0 |
| minslot_4t | 1000 | 64 | minslot | 4:4 | 5573 | 14810 | -62% | 0.38 | 1393 | 385 | 0 |
| lin_16t | 1000 | 64 | lin | 16:16 | 18283 | 30799 | -41% | 0.59 | 1515 | 822 | 0 |
| minslot_16t | 1000 | 64 | minslot | 16:16 | 16417 | 33015 | -50% | 0.50 | 1748 | 791 | 0 |
| lin_32t | 1000 | 64 | lin | 32:32 | 23133 | 37840 | -39% | 0.61 | 3732 | 3600 | 0 |
| minslot_32t | 1000 | 64 | minslot | 32:32 | 23164 | 38256 | -39% | 0.61 | 2746 | 2028 | 0 |

Linux is ~3.6x slower than macOS on single-thread bounded scans
(5367 vs 19558 for `bounded_10`), consistent with the x86_64 build
running under a slower single-core memory subsystem. Multi-thread
scaling is also lower: 16T reaches 18283 scans/s (vs 30799 on macOS),
and MinSlot does **not** show the throughput advantage seen on macOS —
linearizable is actually faster at 4T and 16T. At 32T both modes
saturate at ~23k scans/s with near-identical throughput, though
MinSlot's p99 is still 26% better (2746 vs 3732 us). The MinSlot
advantage appears platform-dependent and may relate to the different
cache hierarchy and inter-core latency of x86_64 vs arm64.

### Improvement summary (pre-R48 → post-R48+R50, macOS)

| Config | Before scans/s | After scans/s | Improvement |
|--------|---------------:|--------------:|------------:|
| bounded_10 | 223 | 19558 | 87.7x |
| bounded_1k | 224 | 4339 | 19.4x |
| deep_pag_10 | 147 | 20681 | 140.7x |
| full_100k | 20 | 50 | 2.5x |

Two changes drove the 20-140x improvement on bounded scans:

- **R48 (lazy `LeafChainCursor`)**: the old `resolve_chain_sorted`
  rebuilt each touched leaf's entire live entry set into a `std::map`
  per scan — O(entries-per-leaf × log), not O(limit). 64B packs
  ~640 entries per 64KiB leaf vs ~58 for 1 KiB, so each leaf resolve
  was far more expensive for 64B (this caused the 1 KiB anomaly where
  1 KiB was 3.8x faster than 64B despite returning 16x more data). The
  lazy cursor merges delta chain + base frame on demand, binary-searches
  on seek, and emits only the entries the scan returns — cost tracks
  `limit`, not leaf fullness. Post-fix: 64B is 2.9x faster than 1 KiB
  (cost tracks bytes returned, not entries per leaf).
- **R50 (epoch-protected MemTable)**: `MemTable::snapshot()` deep-copied
  every live L0 entry on every scan — O(N_l0) regardless of limit.
  Under concurrent write+scan this dominated scan time (82-94% per the
  Gate 2 microbench). Replaced with a `ConcurrentSkipList` (inline keys,
  versioned cell pointers, epoch-deferred reclamation) — readers
  traverse L0 lock-free under their existing epoch guard with zero copy;
  the cursor seeks directly and materializes only O(limit) entries.

Deep pagination is flat (equal to from-start) — O(limit) confirmed.

---

## Existing Problems

- **16 KiB scan errors (replication backpressure)**:
  `valuesize_16KiB` shows intermittent errors (452 in the latest run,
  0 in some re-runs). Root cause is NOT the scan path — it is the
  learner_stream outbound queue filling up during pre-populate with
  16 KiB values (1.6 GiB of data), blocking heartbeats to followers.
  Server logs show `learner_stream: outbound queue full` → leader
  loses leadership → `kv scan failed: not leader`. The scan path
  itself is correct; the linearizable read barrier fails because the
  leader can't maintain quorum. Fix: increase outbound queue capacity
  or add backpressure signaling. Follow-on item.
- **High-concurrency read-mode split (MEASURED)**: MinSlot shows a
  +7.2% throughput advantage at 16T:16C (33015 vs 30799 scans/s) and
  44% better p99 at 32T:32C (2028us vs 3600us). The throughput
  advantage peaks around 16T then both modes saturate near ~38k
  scans/s at 32T — the engine itself becomes the bottleneck, not the
  read barrier. No code change needed.
- **Reverse scan**: `scan` is forward-only today. Tracked as backlog
  item [R52](../backlog/R52-reverse-scan.md).
