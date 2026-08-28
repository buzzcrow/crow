<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# KV Scan Flow Analysis

Range reads from the client through crow-rpc, the read policy, and the
crow-tree cursors. The benchmark sentinel is
`tools/bench-kv-scan-regression.sh`.

## 1. Flow

```text
CrowkvClient::scan(prefix, start_after, end_key, limit, read_mode, min_slot?)
  -> resolve_min_slot and resolve_read_endpoint
  -> paginated KvScanRequest
     server byte budget: 3.5 MiB per page
     Linearizable page 1 returns read_slot S; later pages use MinSlot(S)
  -> retry NotLeaderHint by following the hint; refresh after transport errors
  -> KvStoreService::scan
     Linearizable: forward to leader once
     MinSlot: serve locally
  -> PxKvStore::kv_scan -> resolve_read_point
  -> KVEngine::scan -> CrowTreeEngine::scan -> try_scan
  -> crow-tree scan cursors
     L0: lock-free skip-list cursor
     L1: lazy LeafChainCursor over delta chain and base frame
     merge sources, discard collisions by highest slot, stop at end_key
  -> pack only returned entries into the wire buffer
  -> client decodes Bytes and requests the next page when needed
```

`start_after` is pushed into cursor seek, so work is proportional to the
returned range rather than the whole prefix. The common L0+L1 case uses a
two-source merge; larger source sets use a loser tree. Cold leaves return
`Pending` and resume from the last resolved key after demand-load.

A normal scan is S3-style pagination: each page is consistent, but pages do
not form one cross-page snapshot. Snapshot scans use the separate snapshot
versioning API when a point-in-time view is required.

Copy cost is O(limit) for materialized entries. The packed result crosses the
FFI as owned bytes without per-entry `Vec<u8>` allocations. RPC serialization
and the kernel socket copy remain unavoidable.

## 2. Latest Benchmark Results

Both runs use a 3-node cluster, 100k pre-populated keys, and mem mode. Linux
ran for 20s; macOS is the retained 10s baseline. Values are 64B except the
Linux `largeval_16k` sentinel. Every Linux configuration completed with zero
errors.

### Linux — 2026-08-28

AMD Ryzen 9 5950X, 16c/32t, x86_64, Ubuntu 24.04.

| Config | Limit | Value | Mode | T:C | scans/s | avg us | p99 us |
| --- | ---: | ---: | --- | --- | ---: | ---: | ---: |
| bounded_10 | 10 | 64B | Linearizable | 1:1 | 12,999 | 76 | 113 |
| bounded_1k | 1,000 | 64B | Linearizable | 1:1 | 1,436 | 695 | 873 |
| bounded_10k | 10,000 | 64B | Linearizable | 1:1 | 118 | 8,495 | 11,592 |
| full_100k | 100,000 | 64B | Linearizable | 1:1 | 20 | 49,126 | 92,032 |
| deep_pag_10 | 10 | 64B | Linearizable | 1:1 | 12,553 | 79 | 113 |
| mixed_1k | 1,000 | mixed | Linearizable | 1:1 | 194 | 5,162 | 7,880 |
| minslot_1k | 1,000 | 64B | MinSlot | 1:1 | 1,426 | 700 | 920 |
| largeval_16k | 1,000 | 16KiB | Linearizable | 1:1 | 20 | 50,462 | 72,832 |
| lin_4t | 1,000 | 64B | Linearizable | 4:4 | 4,732 | 844 | 1,232 |
| minslot_4t | 1,000 | 64B | MinSlot | 4:4 | 4,935 | 809 | 1,181 |
| lin_16t | 1,000 | 64B | Linearizable | 16:16 | 23,960 | 666 | 1,016 |
| minslot_16t | 1,000 | 64B | MinSlot | 16:16 | 23,328 | 684 | 1,045 |
| lin_32t | 1,000 | 64B | Linearizable | 32:32 | 26,232 | 1,218 | 2,450 |
| minslot_32t | 1,000 | 64B | MinSlot | 32:32 | 26,545 | 1,203 | 2,064 |

MinSlot is 4.3% faster at 4T and 1.2% faster at 32T. At 32T it also has a
15.8% lower p99. The 16KiB sentinel remains error-free; its low throughput
is expected because each scan can return up to 16MiB.

### macOS — 2026-08-19

Apple M5 Pro, 18c, arm64, macOS 26.5.

| Config | Limit | Value | Mode | T:C | scans/s | avg us | p99 us |
| --- | ---: | ---: | --- | --- | ---: | ---: | ---: |
| bounded_10 | 10 | 64B | Linearizable | 1:1 | 21,320 | 46 | 73 |
| bounded_1k | 1,000 | 64B | Linearizable | 1:1 | 4,708 | 211 | 239 |
| bounded_10k | 10,000 | 64B | Linearizable | 1:1 | 562 | 1,777 | 1,911 |
| full_100k | 100,000 | 64B | Linearizable | 1:1 | 49 | 20,411 | 22,864 |
| deep_pag_10 | 10 | 64B | Linearizable | 1:1 | 21,003 | 46 | 66 |
| mixed_1k | 1,000 | mixed | Linearizable | 1:1 | 1,043 | 957 | 1,175 |
| minslot_1k | 1,000 | 64B | MinSlot | 1:1 | 4,721 | 211 | 241 |
| lin_4t | 1,000 | 64B | Linearizable | 4:4 | 15,504 | 257 | 409 |
| minslot_4t | 1,000 | 64B | MinSlot | 4:4 | 16,232 | 245 | 358 |
| lin_16t | 1,000 | 64B | Linearizable | 16:16 | 32,384 | 492 | 781 |
| minslot_16t | 1,000 | 64B | MinSlot | 16:16 | 32,217 | 495 | 816 |
| lin_32t | 1,000 | 64B | Linearizable | 32:32 | 38,859 | 820 | 2,416 |
| minslot_32t | 1,000 | 64B | MinSlot | 32:32 | 36,684 | 869 | 1,416 |

MinSlot leads at 4T by 4.7%. At 16T the modes converge; at 32T Linearizable
has 5.9% more throughput, while MinSlot has 41% lower p99.

The Linux run is slower on most single-thread and 4T scans, but the gap
narrows at 16T and 32T. These platform results are not direct hardware
comparisons.

## 3. Change History

### Zero-copy value returns

L1 hits now borrow directly from the resident leaf frame (no `std::string`
staging). Scan values are zero-copy from packed buffer to client `Bytes`.

Perf: removes one `std::string` copy per returned entry on the L1 path.

### Streaming scan RPC + zero-copy values

Server byte budget + S3-style unary pagination replaced the 4 MiB unary cap.
Unblocked `full_100k` (previously 0 scans/s) and `valuesize_16KiB`
(previously 0 scans/s).

Perf: 15–20% improvement on large scans; `full_100k` went from 0 to 20
scans/s on Linux.

### Lazy L1 cursor

Replaced eager whole-leaf materialization (`resolve_chain_sorted` rebuilding
each touched leaf's entire live entry set into a `std::map` per scan) with an
on-demand k-way cursor. The lazy cursor merges delta chain + base frame on
demand, binary-searches on seek, and emits only the entries the scan returns.
Cost tracks `limit`, not leaf fullness.

Perf: `bounded_10` rose from 223 to 19,558 scans/s on macOS (87.7x);
`bounded_1k` from 224 to 4,339 (19.4x); `deep_pag_10` from 147 to 20,681
(140.7x). Fixed the 1 KiB anomaly where 1 KiB was 3.8x faster than 64B
despite returning 16x more data.

### Lock-free L0 cursor

Replaced `MemTable::snapshot()` (deep-copied every live L0 entry on every
scan, O(N_l0) regardless of limit) with a `ConcurrentSkipList` using inline
keys, versioned cell pointers, and epoch-deferred reclamation. Readers
traverse L0 lock-free under their existing epoch guard with zero copy; the
cursor seeks directly and materializes only O(limit) entries.

Perf: eliminated the O(N_l0) snapshot copy that dominated scan time (82–94%
per the Gate 2 microbench) under concurrent write+scan.

### Merge loop fast path + loser tree

The merge loop now dispatches by source count. The common 2-source case
(1 active L0 + L1) takes a 1-compare fast path instead of the 2-pass O(2k)
scan; the single-source case skips the merge entirely. For k > 2, a loser
tree provides O(log k) per-step compares with collision drain.
`__builtin_prefetch` is issued for the next skip-list node on L0 cursor
advance and for the right-sibling leaf in `refill_l1`.

Perf: in the 2026-08-06 → 2026-08-09 Linux comparison, `lin_32t` improved
20% (23,133 → 27,819 scans/s) and `minslot_32t` improved 22% (23,164 →
28,312). `bounded_10k` +36% (110 → 150), `full_100k` +23% (13 → 16).

### Zero-copy scan result staging

The engine's 3-copy scan result path (`consider` lambda →
`std::vector<scan_entry>` → `std::string packed` → `make_buf` memcpy) was
replaced with direct wire-format packing in the `consider` lambda (single
growing buffer) + ownership transfer across the FFI via `make_borrowed_buf`.

Perf: eliminates ~10.5 MiB of memcpy + 2 transient allocations per full 3.5
MiB page. Reduces C++ copies from 3 to 1 per scan.

### Per-page read barrier

`PxKvStore::kv_scan` previously called `resolve_read_point` on every page of
a multi-page Linearizable scan. After page 1 returns `read_slot = S`, the
client switches subsequent pages to MinSlot with `min_slot = S` — the store
serves locally when `contiguous_applied >= S`, skipping the barrier entirely.

Perf: an N-page Linearizable scan now performs 1 barrier round instead of N.
No freshness lost (the leader has `S` applied by construction).

### Bounded range predicate

`KvScanRequest` gained an optional exclusive `end_key`. The C++ merge loop
early-stops when `winner_key >= end_key` alongside the existing prefix stop.

Perf: avoids unnecessary leaf work for bounded ranges; prerequisite shape
for reverse scan.

### Large-value durability fix

Maintenance-loop `persist_snapshot` / `flush` / `collect_garbage` held the
C++ `write_mutex_` and blocked the async runtime, starving the election
driver (300–600ms timeout) when snapshots took 0.6–2.2s for 100k × 16KiB
values (1.6 GB). All three calls now run via `tokio::task::spawn_blocking`.

Perf: the 16KiB Linux sentinel changed from 653–8111 errors per run to five
consecutive zero-error runs.

### Benchmark update (2026-08-28)

Replaced the Linux reference with the current crow-rpc run and retained the
macOS baseline. The latest run has zero errors in all 14 configurations;
large and mixed-value scans remain engine-limited. The previous Linux
baseline used the legacy legacy path; positive throughput deltas and negative
p99 deltas are improvements.

| Config | Old scans/s | New scans/s | Δ scans/s | Old p99 us | New p99 us | Δ p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| bounded_10 | 5,093 | 12,999 | +155.2% | 249 | 113 | −54.6% |
| bounded_1k | 1,274 | 1,436 | +12.7% | 904 | 873 | −3.4% |
| bounded_10k | 141 | 118 | −16.3% | 8,528 | 11,592 | +35.9% |
| full_100k | 14 | 20 | +42.9% | 96,512 | 92,032 | −4.6% |
| deep_pag_10 | 5,373 | 12,553 | +133.6% | 264 | 113 | −57.2% |
| mixed_1k | 330 | 194 | −41.2% | 4,116 | 7,880 | +91.4% |
| minslot_1k | 1,292 | 1,426 | +10.4% | 925 | 920 | −0.5% |
| largeval_16k | 35 | 20 | −42.9% | 46,720 | 72,832 | +55.9% |
| lin_4t | 6,999 | 4,732 | −32.4% | 1,064 | 1,232 | +15.8% |
| minslot_4t | 6,586 | 4,935 | −25.1% | 1,231 | 1,181 | −4.1% |
| lin_16t | 21,247 | 23,960 | +12.8% | 1,226 | 1,016 | −17.1% |
| minslot_16t | 20,520 | 23,328 | +13.7% | 1,309 | 1,045 | −20.2% |
| lin_32t | 27,922 | 26,232 | −6.1% | 2,408 | 2,450 | +1.7% |
| minslot_32t | 28,613 | 26,545 | −7.2% | 2,226 | 2,064 | −7.3% |

Strongest improvement: `bounded_10` at +155.2% throughput. Largest
regression: `mixed_1k` at −41.2% throughput and +91.4% p99. The update
improves the common bounded and 16T paths, but large and mixed-value scans
remain engine-limited.
