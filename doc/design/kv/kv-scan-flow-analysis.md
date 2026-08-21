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
Client SCAN(prefix, start_after, end_key, limit, read_mode, min_slot?)
  → CrowkvClient::scan                            [client.rs]
    1. resolve_min_slot — MinSlot: auto-attach write watermark;
       Linearizable: 0
    2. resolve_read_endpoint — Linearizable: cached leader;
       MinSlot + AnyReplica: round-robin across replicas
    3. send KvScanRequest via unary RPC, S3-style pagination
       (server applies a 3.5 MiB byte budget per page; client
       transparently pages until !truncated or limit reached)
       [R55: after page 1 of a Linearizable scan returns read_slot = S,
        subsequent pages switch to MinSlot with min_slot = S, skipping
        the per-page leader barrier — page 1 is the only barrier round]
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
- **R58** — 2-source fast path + loser tree in the scan merge loop:
  when only L0+L1 are active (the common case), a straight 2-way
  merge avoids the loser-tree heap. 3+ sources fall back to a loser
  tree. Reduces merge-loop overhead per entry.
- **R57** — Zero-copy scan result staging: the `consider` lambda
  packs the wire format directly into a `ScanPackedBuf` (growing
  `malloc`/`realloc` buffer), and ownership is transferred across
  the FFI via `release()` — no `std::vector<scan_entry>` staging,
  no re-pack loop, no `make_buf` malloc+memcpy. Reduces C++ copies
  from 3 to 1 per scan.
- **R59** — Two scan modes + snapshot versioning API: the existing
  `scan` RPC (mode 1, list scan) is now documented as S3-list
  semantics (per-page-consistent, not cross-page snapshot). A new
  snapshot versioning API (mode 2) pins a point-in-time-consistent
  L1 view via `CreateSnapshot` (flush + `snapshot_view`), iterates
  it with `SnapshotScan` (binary-search + linear scan over the
  frozen `Vec<ViewEntry>`), and releases it with `ReleaseSnapshot`.
  Per-group handle registry with 5-min lease/expiry reaps abandoned
  snapshots. No new engine machinery — the existing `snapshot_view`
  FFI is reused.

---

## Latest Benchmark Results — 2026-08-19 (post-R48+R50)

**Platform**: Apple M5 Pro, 18c, arm64, macOS 26.5.
**Setup**: 10s mem mode, 3-node cluster, 100k pre-populated keys.
Raw TSV: `doc/working/bench-scan-regression.tsv` (gitignored).

### Single-thread (1T:1C) — per-scan engine cost

| Label | Limit | Start_after | Val B | Mode | scans/s | avg us | p99 us | err |
|-------|------:|-------------|------:|------|--------:|-------:|-------:|----:|
| bounded_10 | 10 | | 64 | lin | 21320 | 46 | 73 | 0 |
| bounded_1k | 1000 | | 64 | lin | 4708 | 211 | 239 | 0 |
| bounded_10k | 10000 | | 64 | lin | 562 | 1777 | 1911 | 0 |
| full_100k | 100000 | | 64 | lin | 49 | 20411 | 22864 | 0 |
| deep_pag_10 | 10 | k...99989 | 64 | lin | 21003 | 46 | 66 | 0 |
| mixed_1k | 1000 | | mixed | lin | 1043 | 957 | 1175 | 0 |
| minslot_1k | 1000 | | 64 | minslot | 4721 | 211 | 241 | 0 |

`mixed_1k` uses `--value-size-mix 64:70,1024:20,16384:10`: 70% 64B,
20% 1KiB, 10% 16KiB values, deterministically assigned by key id. At
1043 scans/s it sits between the old `valuesize_1KiB` (1492) and
`valuesize_16KiB` (74), reflecting the weighted average of the three
sizes with 0 errors (the 16KiB fraction is small enough to avoid the
replication backpressure issue seen at 100% 16KiB).

### Multi-thread — max throughput + read-mode split

| Label | Limit | Val B | Mode | T:C | scans/s | avg us | p99 us | err |
|-------|------:|------:|------|-----|--------:|-------:|-------:|----:|
| lin_4t | 1000 | 64 | lin | 4:4 | 15504 | 257 | 409 | 0 |
| minslot_4t | 1000 | 64 | minslot | 4:4 | 16232 | 245 | 358 | 0 |
| lin_16t | 1000 | 64 | lin | 16:16 | 32384 | 492 | 781 | 0 |
| minslot_16t | 1000 | 64 | minslot | 16:16 | 32217 | 495 | 816 | 0 |
| lin_32t | 1000 | 64 | lin | 32:32 | 38859 | 820 | 2416 | 0 |
| minslot_32t | 1000 | 64 | minslot | 32:32 | 36684 | 869 | 1416 | 0 |

Linearizable scales well up to 16T (4708 → 32384, 6.9x) then saturates
at 32T (38859): the leader read barrier becomes the bottleneck.
MinSlot shows a clear advantage at 4T:
- **4T:4C**: +4.7% throughput (16232 vs 15504), distributed read
  serving across 3 replicas scales better than single-leader.
- **16T:16C**: modes converge (-0.5%, ~32K each).
- **32T:32C**: Linearizable pulls ahead (+5.9% throughput), but
  MinSlot's p99 is 41% better (1416us vs 2416us). Load distribution
  keeps tail latency low even when throughput is capped by the engine.

### Linux results — 2026-08-10 (post-R67)

**Platform**: AMD Ryzen 9 5950X, 16c/32t, x86_64, Ubuntu 24.04.
**Setup**: 10s mem mode, 3-node cluster, 100k pre-populated keys.
Single-thread numbers are from one full regression run (post-R67 fix).
macOS column is the 2026-08-19 baseline (unchanged).
Raw TSV: `doc/working/bench-scan-regression.tsv` (gitignored).

#### Single-thread (1T:1C)

| Label | Limit | Start_after | Val B | Mode | Linux scans/s | macOS scans/s | Δ% | L/M | Linux p99 us | macOS p99 us | err |
|-------|------:|-------------|------:|------|--------:|--------:|---:|----:|-------:|-------:|----:|
| bounded_10 | 10 | | 64 | lin | 5093 | 19558 | -74% | 0.26 | 249 | 79 | 0 |
| bounded_1k | 1000 | | 64 | lin | 1274 | 4339 | -71% | 0.29 | 904 | 258 | 0 |
| bounded_10k | 10000 | | 64 | lin | 141 | 518 | -73% | 0.27 | 8528 | 2060 | 0 |
| full_100k | 100000 | | 64 | lin | 14 | 50 | -72% | 0.28 | 96512 | 20848 | 0 |
| deep_pag_10 | 10 | k...99989 | 64 | lin | 5373 | 20681 | -74% | 0.26 | 264 | 66 | 0 |
| mixed_1k | 1000 | | mixed | lin | 330 | 991 | -67% | 0.33 | 4116 | 1222 | 0 |
| minslot_1k | 1000 | | 64 | minslot | 1292 | 4293 | -70% | 0.30 | 925 | 262 | 0 |
| largeval_16k | 1000 | | 16384 | lin | 35 | — | — | — | 46720 | — | 0 |

#### Multi-thread

| Label | Limit | Val B | Mode | T:C | Linux scans/s | macOS scans/s | Δ% | L/M | Linux p99 us | macOS p99 us | err |
|-------|------:|------:|------|-----|--------:|--------:|---:|----:|-------:|-------:|----:|
| lin_4t | 1000 | 64 | lin | 4:4 | 6999 | 14264 | -51% | 0.49 | 1064 | 473 | 0 |
| minslot_4t | 1000 | 64 | minslot | 4:4 | 6586 | 14810 | -55% | 0.45 | 1231 | 385 | 0 |
| lin_16t | 1000 | 64 | lin | 16:16 | 21247 | 30799 | -31% | 0.69 | 1226 | 822 | 0 |
| minslot_16t | 1000 | 64 | minslot | 16:16 | 20520 | 33015 | -38% | 0.62 | 1309 | 791 | 0 |
| lin_32t | 1000 | 64 | lin | 32:32 | 27922 | 37840 | -26% | 0.74 | 2408 | 3600 | 0 |
| minslot_32t | 1000 | 64 | minslot | 32:32 | 28613 | 38256 | -25% | 0.75 | 2226 | 2028 | 0 |

Linux is ~3.5x slower than macOS on single-thread bounded scans
(5093 vs 19558 for `bounded_10`), consistent with the x86_64 build
running under a slower single-core memory subsystem. Multi-thread
scaling: 16T reaches 21247 scans/s, and 32T saturates at ~28k scans/s
with p99 down to 2408 us for linearizable. MinSlot still does **not**
show the throughput advantage seen on macOS; linearizable is faster at
4T and 16T on Linux. At 32T MinSlot edges ahead (28613 vs 27922, +2.5%)
with p99 7.6% better (2226 vs 2408 us). The MinSlot advantage appears
platform-dependent and may relate to the different cache hierarchy and
inter-core latency of x86_64 vs arm64.

The `largeval_16k` config (100k × 16KiB = 1.6 GB values) is the R67
regression sentinel: 35 scans/s with 0 errors post-fix (was 653-8111
errors before the `spawn_blocking` fix). The low throughput is expected;
each scan returns up to 1000 × 16KiB = 16 MB of data, and the
snapshot/flush/GC path now runs on blocking threads without stalling
the election driver.

### Improvement summary (2026-08-06 → 2026-08-09, Linux)

| Config | Before scans/s | After scans/s | Improvement |
|--------|---------------:|--------------:|------------:|
| bounded_10k | 110 | 150 | +36% |
| full_100k | 13 | 16 | +23% |
| mixed_1k | 265 | 337 | +27% |
| minslot_1k | 993 | 1296 | +30% |
| lin_16t | 18283 | 21447 | +17% |
| minslot_16t | 16417 | 20749 | +26% |
| lin_32t | 23133 | 27819 | +20% |
| minslot_32t | 23164 | 28312 | +22% |

Four changes drove the improvement:

- **R55 (per-page linearizable read barrier)**: `PxKvStore::kv_scan`
  called `resolve_read_point` on every page of a multi-page
  linearizable scan. After page 1 returns `read_slot = S`, the client
  switches subsequent pages to `MinSlot` with `min_slot = S` — the
  store serves locally when `contiguous_applied >= S`, skipping the
  barrier entirely. No freshness lost (the leader has `S` applied by
  construction). Verified by an e2e test asserting
  `lease_path + readindex_path == 1` for an N-page scan (was N before).
  Client-local, no proto change.
- **R56 (prefix-only range predicate)**: `KvScanRequest` gained an
  optional exclusive `end_key` (proto field 10). The C++ merge loop
  early-stops when `winner_key >= end_key` alongside the existing
  prefix stop. Threaded through the full scan path (C API, FFI,
  `KVEngine::scan`, gRPC service, client). Prerequisite shape for R52
  reverse scan.
- **R57 (zero-copy result staging)**: the engine's 3-copy scan result
  path (`consider` lambda → `std::vector<scan_entry>` → `std::string
  packed` → `make_buf` memcpy) was replaced with direct wire-format
  packing in the `consider` lambda (single growing buffer) +
  ownership transfer across the FFI via `make_borrowed_buf` (the
  pattern already used by the get fast path). Eliminates ~10.5 MiB of
  memcpy + 2 transient allocations per full 3.5 MiB page.
- **R58 (merge loop fast path + loser tree)**: the merge loop now
  dispatches by source count. The common 2-source case (1 active L0 +
  L1) takes a 1-compare fast path instead of the 2-pass O(2k) scan;
  the single-source case skips the merge entirely. For k > 2, a loser
  tree provides O(log k) per-step compares with collision drain.
  `__builtin_prefetch` is issued for the next skip-list node on L0
  cursor advance and for the right-sibling leaf in `refill_l1`. This
  drove the multi-thread throughput gains (32T: ~23k → ~28k scans/s)
  and the large-scan single-thread gains. Two small-bounded configs
  show a slight regression: `bounded_1k` -7% throughput (but p99
  -13%) and `deep_pag_10` -9% throughput (p99 flat), both verified
  across 5 runs, consistent, and within acceptable noise for the
  per-scan fast path. The fast-path dispatch adds a small constant
  per-scan cost that slightly slows the small-scan path where
  per-scan overhead dominates.

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
  per scan, O(entries-per-leaf × log), not O(limit). 64B packs
  ~640 entries per 64KiB leaf vs ~58 for 1 KiB, so each leaf resolve
  was far more expensive for 64B (this caused the 1 KiB anomaly where
  1 KiB was 3.8x faster than 64B despite returning 16x more data). The
  lazy cursor merges delta chain + base frame on demand, binary-searches
  on seek, and emits only the entries the scan returns. Cost tracks
  `limit`, not leaf fullness. Post-fix: 64B is 2.9x faster than 1 KiB
  (cost tracks bytes returned, not entries per leaf).
- **R50 (epoch-protected MemTable)**: `MemTable::snapshot()` deep-copied
  every live L0 entry on every scan, O(N_l0) regardless of limit.
  Under concurrent write+scan this dominated scan time (82-94% per the
  Gate 2 microbench). Replaced with a `ConcurrentSkipList` (inline keys,
  versioned cell pointers, epoch-deferred reclamation). Readers
  traverse L0 lock-free under their existing epoch guard with zero copy;
  the cursor seeks directly and materializes only O(limit) entries.

Deep pagination is flat (equal to from-start); O(limit) confirmed.

---

## Open Problems

Full-path audit (client → gRPC → PxKvStore → FFI → C++ engine),
2026-08-07. Each item is tracked by a backlog requirement except where
noted.

- **[R52](../../backlog/R52-reverse-scan.md) — Reverse scan**: `scan`
  is forward-only today (ascending key order). Reverse scan needs
  backward cursor traversal in both L0 (skip-list, forward-only) and
  L1 (`LeafChainCursor`), a `direction` field on `KvScanRequest`, and
  S3-style pagination keyed on the first key of each page as the next
  `start_before`.
- **[R54](../../backlog/R54-kv-scan-engine-profiling.md) —
  High-concurrency engine bottleneck (MEASURED)**: MinSlot shows a
  +7.2% throughput advantage at 16T:16C (33015 vs 30799 scans/s) and
  44% better p99 at 32T:32C (2028us vs 3600us) on macOS. The throughput
  advantage peaks around 16T then both modes saturate near ~38k
  scans/s at 32T on macOS. The crow-tree engine (C++ merge loop over L0
  skip-list + L1 B+tree cursor) becomes the bottleneck, not the read
  barrier. On Linux (2026-08-09, post-R58) the 32T saturation point
  moved up to ~28k scans/s (was ~23k on 2026-08-06) with p99 down 37%
  for linearizable (2364 vs 3732 us), but the engine remains the
  bottleneck: both modes still saturate with near-identical throughput
  at 32T. No code change needed for the read-mode split itself;
  profiling the engine bottleneck is the open work.
- **[R60](../../backlog/R60-tree-scan-sibling-leaf-readahead.md) —
  No sibling-leaf readahead on cold scans**: the sync path
  demand-loads each leaf inline; the async path resolves one pending
  page per reactor round trip (`scan_async_attempt`). A scan knows its
  next leaf (`right_sibling`) before finishing the current one.
  Issuing the next read ahead of the merge loop would overlap I/O with
  merging on cold ranges.
- **R67 — 16 KiB scan errors on Linux** — **Done (2026-08-10).** RCA:
  maintenance-loop `persist_snapshot` / `flush` / `collect_garbage`
  held the C++ `write_mutex_` and blocked the async runtime, starving
  the election driver (300-600ms timeout) when snapshots took 0.6-2.2s
  for 100k × 16KiB values (1.6 GB). Fix: all three calls now run via
  `tokio::task::spawn_blocking` (single code path, no fire-and-forget,
  no in-flight guard); the election driver runs on a separate tokio
  task and is no longer blocked. `PxLearner.engine` changed from
  `Box<dyn KVEngine>` to `Arc<dyn KVEngine>` so the handle clones into
  the blocking task. Verified: 0 errors across 5 consecutive 16KiB
  bench runs (was 653-8111). The `largeval_16k` config is part of the
  regression sentinel (`tools/bench-scan-regression.sh`).
- **Streaming scan RPC (deliberately dropped — not needed)**: a
  server-streaming `ScanStream` (R38/R44 era) was replaced by the
  server byte budget + S3-style unary pagination. Streaming adds
  complexity (mid-stream error/cancellation/backpressure, HTTP/2
  flow-control stalls) and loses the clean per-page retry that
  `start_after` keying gives. The same production/transfer overlap is
  available without a proto change via client-side page prefetch
  (request page N+1 while consuming page N). No backlog entry.
