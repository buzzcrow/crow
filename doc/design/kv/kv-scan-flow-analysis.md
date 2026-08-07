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
  leader can't maintain quorum. E5 (heartbeat reserved capacity)
  guarantees heartbeat admission to the queue but not wire priority —
  a heartbeat behind N 16 KiB accepts is delayed by their cumulative
  flush time. Tracked as
  [R53](../../backlog/R53-kv-replica-heartbeat-channel.md) — separate gRPC
  Channel for heartbeats via the existing unary RPC (pure gRPC change,
  independent of R32).
- **High-concurrency read-mode split (MEASURED)**: MinSlot shows a
  +7.2% throughput advantage at 16T:16C (33015 vs 30799 scans/s) and
  44% better p99 at 32T:32C (2028us vs 3600us). The throughput
  advantage peaks around 16T then both modes saturate near ~38k
  scans/s at 32T — the crow-tree engine (C++ merge loop over L0
  skip-list + L1 B+tree cursor) becomes the bottleneck, not the read
  barrier. No code change needed for the read-mode split itself;
  profiling the engine bottleneck is tracked as
  [R54](../../backlog/R54-kv-scan-engine-profiling.md).
- **Reverse scan**: `scan` is forward-only today. Tracked as backlog
  item [R52](../../backlog/R52-reverse-scan.md).

---

## Gap Analysis — Enhancement Opportunities

Full-path audit (client → gRPC → PxKvStore → FFI → C++ engine),
2026-08-07. Items marked **→ R55** / **→ R56** / **→ R57** / **→ R58**
are now backlog entries; items under "Tiny Tasks" are small fix-later
to-dos tracked here; the rest remain candidates (medium/large features
not yet prioritized).

### Performance

- **Engine-side result staging is 3 copies, not zero-copy**. The
  "zero-copy" claim above holds only from the FFI packed buffer to the
  client. Inside the engine, each page's result set is copied three
  times before crossing the FFI boundary:
  1. `Crowtree::scan`'s `consider` lambda stages every entry via
     `key.to_string()` + `value.to_string()` into
     `std::vector<scan_entry>` (`crow-tree.cpp` ~1853/1868);
  2. `ct_scan` re-packs those strings into a `std::string packed`
     (`c_api.cpp` ~913-921);
  3. `make_buf` mallocs and memcpys `packed` again (`c_api.cpp:43`).
  For a full 3.5 MiB page that is ~10.5 MiB of memcpy + 2 transient
  allocations. Fix: pack the wire format directly in the `consider`
  lambda (single growing buffer) and transfer ownership across the FFI
  instead of `make_buf` (an ownership-transfer path already exists —
  see `make_borrowed_buf` and the get fast path). Likely the cheapest
  large win. **→ R57**
- **Per-page linearizable read barrier**. `PxKvStore::kv_scan` calls
  `resolve_read_point` on every page (`px_kv_store.rs:183`), so a
  multi-page linearizable scan pays the barrier (lease check, or a
  quorum heartbeat round on the ReadIndex fallback) once per page.
  Enhancement: the client already receives `read_slot` from page 1 —
  carry it forward as `min_slot` and serve subsequent pages via the
  MinSlot path. Semantics are unchanged (cross-page results are
  already not a single snapshot; each page would still be at least as
  fresh as page 1) and later pages skip the barrier entirely. **→ R55**
- **Retry restarts pagination from scratch**. On a `not_leader_hint`
  redirect or transport error the client clears `all_items` and resets
  `page_start_after` to the caller's original `start_after`
  (`client.rs:858, 875`), re-fetching every previous page. Because
  pagination is keyed on `start_after` (S3-style), resuming from the
  last received key on the new endpoint is safe — no duplicates, no
  gaps in key order. Cheap client-only fix.
- **Merge loop is O(N_sources) comparisons per entry**. Min-key
  selection re-compares every L0 cursor plus L1 for each output entry
  (`crow-tree.cpp` ~1893-1934), each a byte-wise `Slice::compare`.
  With several frozen memtables live this multiplies. A loser-tree /
  two-element fast path (common case: 1 active L0 + L1) would cut
  compares; no prefetch (`__builtin_prefetch`) is issued for the next
  skip-list node or the right-sibling leaf. **→ R58**
- **No sibling-leaf readahead on cold scans**. The sync path
  demand-loads each leaf inline; the async path resolves one pending
  page per reactor round trip (`scan_async_attempt`). A scan knows its
  next leaf (`right_sibling`) before finishing the current one —
  issuing the next read ahead of the merge loop would overlap I/O with
  merging on cold ranges.
- **`SCAN_BYTE_BUDGET` is a hardcoded constant** (3.5 MiB,
  `px_kv_store.rs:33`) — not in server config, not visible to the
  client. Making it a config knob allows tuning page size vs. RPC
  overhead per deployment.

### Functionality

- **No cross-page snapshot isolation (by design, undocumented)**. Each
  page resolves its own read point and scans live data; a paginated
  scan is a sequence of per-page-consistent slices, not one snapshot —
  a value can change or a key vanish between pages. This matches
  S3-list semantics and is fine for the KV Operator UI, but is wrong
  for backup/analytics-style consumers. The engine already has a
  snapshot mechanism (`ct_snapshot_view` / `snapshot_view()`); a
  `snapshot scan` variant could pin a view for the duration of one
  paginated scan (needs a server-side handle + lease/expiry). At
  minimum the current semantics should be stated in the user guide.
- **Prefix-only range predicate**. `KvScanRequest` has `prefix` +
  `start_after` but no exclusive `end_key`, so an arbitrary
  `[start, end)` range cannot be expressed. The engine's early-stop
  already compares against `prefix`; adding an optional `end_key`
  bound to the merge loop, FFI, proto, and client is a small, low-risk
  extension (and is a prerequisite shape for R52 reverse scan). **→ R56**
- **No keys-only / count-only projection**. Scans always materialize
  and ship values. A `keys_only` flag would skip value staging in the
  engine (including overflow-chain assembly — the most expensive
  materialization) and shrink pages by the value fraction; a
  count-style scan falls out of the same pushdown. Useful for key
  listing, prefix cardinality, and the console UI.
- **No scan deadline / cancellation**. No per-scan timeout at any
  layer; an unbounded `limit=0` scan over a large keyspace runs until
  the transport gives up, and the engine loop has no cancellation
  check between pages. A request deadline (proto field + engine-side
  budget check per page) bounds worst-case server work.
- **Single-group scope**. Scan targets one Paxos group; a
  keyspace-wide scan across groups requires manual client fan-out and
  merge. Acceptable while key→group placement is caller-owned, but a
  client-side fan-out+merge helper is the natural follow-up once
  multi-group stores are common.
- **Streaming scan RPC (deliberately dropped — revisit for huge
  scans)**. A server-streaming `ScanStream` existed (R38/R44 era) and
  was replaced by the server byte budget + S3-style unary pagination.
  That trade keeps the RPC surface simple, but every 3.5 MiB page now
  costs one client round trip, serialized with the next page's engine
  work — a full-keyspace scan cannot overlap page production with
  network transfer. If bulk-export workloads matter, revisit a
  streaming variant (or page prefetch: client requests page N+1 while
  consuming page N, which gets the same overlap without a proto
  change).

### Tiny Tasks

Small fix-later items tracked here (not large enough for a backlog
requirement). Check off when done.

- [ ] **Scan retry resumes from last key, not from scratch**. On a
  `not_leader_hint` redirect or transport error the client clears
  `all_items` and resets `page_start_after` to the caller's original
  `start_after` (`client.rs:858, 875`), re-fetching every previous page.
  Because pagination is keyed on `start_after` (S3-style), resuming from
  the last received key on the new endpoint is safe — no duplicates, no
  gaps in key order. Fix: keep `all_items` and set
  `page_start_after = all_items.last().0` instead of resetting to
  `start_after`. Client-only, ~4 lines.
- [ ] **`SCAN_BYTE_BUDGET` as a server config knob**. The 3.5 MiB page
  budget is a hardcoded `const` (`px_kv_store.rs:33`), not in server
  config and not visible to the client. Plumb it through the server
  config struct so deployments can tune page size vs. RPC overhead
  (e.g. smaller pages for low-latency interactive scans, larger for
  bulk export). Note the 4 MiB gRPC `max_decoding_message_size` ceiling
  that the 3.5 MiB value leaves headroom for (post-R32 custom RPC that
  ceiling may change).
- [ ] **Document cross-page scan semantics in the user guide**. A
  paginated scan is a sequence of per-page-consistent slices, not one
  snapshot — a value can change or a key vanish between pages (S3-list
  semantics). This is by design and fine for the KV Operator UI, but is
  not stated in `doc/user-manual/user-guide.md`. Add a short note to the
  scan section so backup/analytics-style consumers know the limitation
  (and that a future snapshot-scan variant is the answer for them).
  Doc-only; rebuild `user-guide.html` via
  `python3 doc/user-manual/build_html.py` in the same commit.
