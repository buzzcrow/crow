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
  resolution (`resolve_chain_sorted` rebuilt each touched leaf's
  entire entry set into a `std::map` per scan, O(entries-per-leaf))
  with an on-demand k-way cursor that merges delta chain + base frame,
  binary-searches on seek, and emits only the entries the scan
  returns. Cost went from O(entries-per-leaf) to O(limit). This fixed
  the 1 KiB anomaly (1 KiB was 3.8x faster than 64B because 64B packs
  ~640 entries/leaf vs ~58 for 1 KiB, so each leaf resolve was far
  more expensive for 64B).
- **R50** — Epoch-protected lock-free MemTable: replaced
  `absl::btree_map` under `mu_` with a `ConcurrentSkipList` (inline
  keys, versioned cell pointers, epoch-deferred reclamation). Readers
  traverse L0 lock-free under their existing epoch guard with zero
  copy; the cursor seeks directly and materializes only O(limit)
  entries. Eliminated the O(N_l0) `snapshot()` copy that dominated
  scan time under concurrent write+scan (82-94% of scan time per the
  Gate 2 microbench).

---

## Latest Benchmark Results — 2026-08-06 (post-R48+R50)

**Platform**: Apple M5 Pro, 18c, arm64, macOS 26.5.
**Setup**: 1T:1C, 10s mem mode, 3-node cluster, 100k pre-populated keys.
Raw TSV: `doc/working/bench-scan-regression.tsv` (gitignored).

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

### Improvement summary (pre-R48 → post-R48+R50)

| Config | Before scans/s | After scans/s | Improvement |
|--------|---------------:|--------------:|------------:|
| bounded_10 | 223 | 20932 | 93.9x |
| bounded_100 | 230 | 15429 | 67.1x |
| bounded_1k | 224 | 4355 | 19.4x |
| deep_pag_10 | 147 | 20664 | 140.6x |
| valuesize_64B | 202 | 4339 | 21.5x |
| full_1k | 243 | 4264 | 17.5x |

Bounded scans improved 20-140x — R48 eliminated O(entries-per-leaf)
and R50 eliminated O(N_l0), the two costs that dominated every scan
regardless of limit. The 1 KiB anomaly is inverted: 64B is now 2.9x
faster than 1 KiB (cost tracks bytes returned, not entries per leaf).
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
- **High-concurrency read-mode split**: this baseline is 1T:1C; the
  MinSlot vs Linearizable split (where MinSlot's any-replica
  round-robin distributes load) only appears at higher concurrency.
  No measurable difference at 1T:1C (4380 vs 4230 scans/s — noise).
  The code is correct; only the benchmark coverage is missing.
  Follow-on item.
- **Reverse scan**: `scan` is forward-only today. Tracked as backlog
  item [R52](../backlog/R52-reverse-scan.md).
