<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Scan Path Perf Baseline (R46)

End-to-end scan regression baseline captured via
`tools/bench-scan-regression.sh` driving `crow-cli bench run --workload
list` against a 3-node mem-mode cluster. Raw results in
`doc/working/bench-scan-regression.tsv` (gitignored, regenerated each
run). Reference numbers are embedded in the script's comment block so
they persist in git. Summary of key findings is in
`doc/design/tree/design-crow-tree-engine.md` §1.9.

## How to update this baseline

1. Run `bash tools/bench-scan-regression.sh` — regenerates
   `doc/working/bench-scan-regression.tsv` with fresh numbers (~2 min
   on a quiet machine).
2. Compare the new TSV against the reference numbers in the script's
   comment block (lines 84–103). A regression is a scans/s drop > ~5%
   or a new non-zero error count on configs that were clean before.
3. If updating the reference: replace the comment block in
   `tools/bench-scan-regression.sh` with the new numbers, update the
   analysis sections below, and commit both the script and this doc.
4. When re-capturing on a different platform, record the CPU/OS in the
   Platform section below and note that absolute numbers are not
   comparable across platforms.

## Platform

- **CPU**: Apple M5 Pro — 18 cores / 18 threads, arm64
- **OS**: macOS 26.5.2 (Darwin 25.5.0)
- **Page size**: 16384 bytes

**Note**: The write baseline (`bench-write-regression.tsv`) and
read-flow analysis were captured on an AMD Ryzen 9 5950X Linux
machine. This baseline was captured on Apple silicon macOS — the
absolute numbers are not directly comparable to the Linux write
baseline. The official cross-comparable baseline should be
re-captured on the same Linux Ryzen machine. The bench code and
script are platform-independent and run identically on both.

## Results

All runs: `--workload list --mode mem --threads 1 --connections 1
--duration-secs 10 --pre-populate 100000`, 3-node cluster. Scans/s =
`total_ops * 1000 / duration_ms` (rounded; runs with scans/s=0 had
fewer than 5 completed scans in 10s due to very high per-scan latency).

### Full-keyspace scan (limit >= keyspace, 64 B values)

- `full_1k` (limit=1000): 227 scans/s, avg=4411us, p99=5020us
- `full_10k` (limit=10000): 139 scans/s, avg=7181us, p99=7900us
- `full_100k` (limit=100000): 0 scans/s, avg=1752411us, p99=1766400us, **6 errors**

Scales linearly with key count (1k→10k: 2.2x keys, 1.6x time). The
100k scan hits the tonic default 4 MiB max gRPC message size (100k ×
~70 B = ~7 MB payload) — the 6 errors are transport-level
`max_recv_message_length` rejections, not engine failures. This is
the gRPC-message-size analog of etcd's range-read OOM risk (issue
#12342): a single scan response that exceeds the transport limit
fails entirely. A streaming scan response (mirroring etcd's PR
#19766) or a raised max-message-size config would fix it.

### Bounded limit over 100k keyspace (64 B values)

- `bounded_10` (limit=10): 225 scans/s, avg=4438us
- `bounded_100` (limit=100): 223 scans/s, avg=4479us
- `bounded_1k` (limit=1000): 216 scans/s, avg=4640us
- `bounded_10k` (limit=10000): 137 scans/s, avg=7297us

Per-entry cost is small relative to the per-scan setup (descent +
ReadIndex round + gRPC roundtrip): limit 10/100/1k are all ~4.4 ms.
At limit=10k the per-entry cost becomes visible (7.3 ms). The
per-scan fixed cost is ~4.2 ms (the from_start_10 baseline below);
per-entry cost is ~0.3 us/entry at 64 B values.

### Deep pagination (start_after near end vs from-start companion)

- `from_start_10` (limit=10, start_after=""): 236 scans/s, avg=4236us, p99=4800us
- `deep_pag_10` (limit=10, start_after=k...99989): 141 scans/s, avg=7084us, p99=9936us
- `deep_pag_100` (limit=100, start_after=k...99899): 133 scans/s, avg=7538us, p99=10344us

**§1.7 O(limit) verdict: CONFIRMED.** Deep pagination is 1.7x slower
than from-start (7084us vs 4236us at limit=10), but this is the
deeper B+tree descent cost (O(log N) inner-page levels to reach a
leaf near the end of the keyspace), not O(prefix) over-fetch. If the
engine over-fetched the prefix, deep_pag_10 would scan all 100k keys
then truncate — costing ~1752411us (the full_100k number), not 7084us.
The 1.7x overhead is consistent with O(limit) + O(log N), not
O(prefix). The over-fetch proxy (deep_pag / from_start ratio) is
1.7x; the etcd-style "fetch all then truncate" regression would show
a ratio of ~400x (1752411 / 4236). Deep pagination limit 10 vs 100
(7084 vs 7538us) are within 6% — confirming the cost is dominated by
the fixed descent + setup, not by the number of entries returned.

### Value-size sweep (fixed limit=1000, 100k keyspace)

- `valuesize_64B`: 206 scans/s, avg=4852us, p99=5496us
- `valuesize_1KiB`: 666 scans/s, avg=1500us, p99=3004us
- `valuesize_16KiB`: 0 scans/s, avg=6195us, p99=815us, p999=1612800us, **1701 errors**

**16 KiB**: hits the tonic 4 MiB limit (1000 × 16 KiB = 16 MiB
payload). The bimodal latency (p50=520us, p999=1612800us) reflects
fast failures (transport rejection) plus slow retries that eventually
time out. Same root cause as full_100k above.

**1 KiB anomaly**: 1 KiB values are 3.2x faster than 64 B values
(666 vs 206 scans/s) despite returning 16x more data per scan. This
is counterintuitive — larger values should be slower (more per-byte
copy, the cost R38 targets), not faster.

**L0-snapshot hypothesis REFUTED (R47 flush-after-prepopulate).** The
original code-reading hypothesis blamed `MemTable::snapshot()` for
copying all N_l0 entries per scan (O(N_l0), not O(limit)), with 64 B
leaving ~60k unflushed vs 1 KiB leaving ~4k. R47 added a
`--flush-after-prepopulate` bench flag that drains L0 via a new
`POST /stores/{sid}/groups/{gid}/flush` management API endpoint before
the measurement window. Re-running the value-size sweep with the flag:

- `valuesize_64B_flushed`: 219 scans/s, avg=4564us (vs 224 unflushed)
- `valuesize_1KiB_flushed`: 721 scans/s, avg=1386us (vs 709 unflushed)

The 3.2x gap is unchanged with L0 deterministically drained. The flag
is a no-op because the "e2e" election profile sets
`maintenance_tick_ms = 3000`, so the per-group maintenance loop already
calls `KVEngine::flush()` every 3 s; during the 10 s measurement
window (~3 ticks) and the multi-second pre-pop, L0 is already largely
drained, and the scan bench issues no writes to refill it. So L0 is
small at scan time with or without the flag.

Conclusion: the anomaly is in the L1 B+tree scan path (or the
decode/copy path), not L0. The real root cause is unknown and needs a
separate investigation (likely an engine-level C++ microbench
isolating per-leaf merge / delta-chain cost vs value size, with a
flushed L1-only tree). The `--flush-after-prepopulate` flag remains
useful as a deterministic drain for clean L1-only baselines.

### Prefix range (bounded prefix vs whole-keyspace, same entry count)

- `prefix_1k` (prefix="k00", limit=1000): 209 scans/s, avg=4787us
- `whole_1k` (prefix="", limit=1000): 210 scans/s, avg=4753us

No measurable difference. The prefix descent targets the first "k00"
leaf directly, but the cost is dominated by the per-scan fixed
overhead (ReadIndex + gRPC), not the descent. At this scan width
(1000 entries) the prefix optimization is invisible end-to-end.

### Read-mode split (linearizable vs minslot, limit=1000)

- `lin_1k` (linearizable): 208 scans/s, avg=4797us
- `minslot_1k` (minslot, any-replica): 206 scans/s, avg=4855us

No measurable difference at 1T:1C — expected, since the single client
has no concurrency to exploit MinSlot's follower local-serve. The
read-mode split would show a difference at higher concurrency
(MinSlot's any-replica round-robin distributes load); that is an
end-to-end follow-on item, not captured in this 1T:1C baseline.

## Cost-split conclusion

- **Per-scan fixed cost** (~4.2 ms): ReadIndex consensus round + gRPC
  roundtrip + B+tree descent. This dominates at small limit (10/100).
- **Per-entry cost** (~0.3 us/entry at 64 B): visible at limit >= 10k.
  At 64 B values, leaf-chain traversal + packed-buffer decode +
  per-entry value copy together are ~0.3 us/entry.
- **Per-byte copy cost** (R38's target): not cleanly separable from
  the per-entry cost at the end-to-end level. The 1 KiB anomaly
  (3.2x faster than 64 B) is NOT caused by per-byte copy (larger
  values would be slower) and NOT caused by L0 snapshot cost (R47
  refuted that — see the value-size sweep section above). The real
  root cause is unknown; an engine-level C++ microbench with a
  flushed L1-only tree is needed to isolate it.
- **L0 snapshot cost**: `MemTable::snapshot()` is O(N_l0) per scan
  regardless of limit, but R47's flush-after-prepopulate experiment
  showed this is NOT the 1 KiB anomaly's cause — draining L0 did not
  close the gap. The maintenance loop (3 s tick in the e2e profile)
  already keeps L0 small during measurement. A lazy/range-bounded L0
  cursor would still be an O(limit) improvement but would not fix the
  anomaly.
- **Deep-pagination descent** (~2.8 ms overhead): the O(log N) B+tree
  descent to a leaf near the end of the keyspace. This is a fixed
  cost per scan, not per-entry.

**Prioritization**: R38 (zero-copy scan values) targets per-byte copy
cost (~0.3 us/entry at 64 B, grows with value size). The 1 KiB
anomaly's root cause is unknown (L0 refuted by R47) and needs a
separate engine-level investigation before a fix can be scoped. For
large-value workloads (>= 1 KiB), R38's zero-copy win is significant,
but the gRPC 4 MiB message size limit caps practical scan width before
per-byte copy becomes dominant — so streaming scan response (R49) is a
prerequisite for R38's win to matter at scale.

## Future gaps (not measured by R46)

- **1 KiB anomaly root cause (unknown)**: R47's flush-after-prepopulate
  experiment refuted the L0-snapshot hypothesis — draining L0 did not
  close the 3.2x gap. The real cause is in the L1 B+tree scan path or
  the decode/copy path and needs an engine-level C++ microbench
  (flushed L1-only tree, vary value size, isolate per-leaf merge /
  delta-chain / decode cost) to identify. This is a prerequisite for
  scoping a fix; the lazy L0 cursor (R48) does not address it.
- **L0 snapshot O(N_l0) cost**: `MemTable::snapshot()` copies all
  entries on every scan. R47's `--flush-after-prepopulate` flag (now
  implemented) drains L0 for a clean L1-only baseline; the maintenance
  loop already keeps L0 small during measurement. A lazy/range-bounded
  L0 cursor (R48) would make the L0 cost O(limit) but is not the
  anomaly's fix.
- **Streaming scan response**: the gRPC 4 MiB message size limit caps
  scan response size. A streaming response (mirroring etcd PR #19766)
  would remove this cap and is a prerequisite for large-value /
  wide-limit scan perf to matter.
- **High-concurrency read-mode split**: this baseline is 1T:1C; the
  MinSlot vs Linearizable split at higher concurrency (where MinSlot's
  any-replica round-robin distributes load) is an end-to-end
  follow-on item.
- **Reverse scan**: `scan` is forward-only today; reverse scan is a
  distinct cost shape (per CockroachDB's `MVCCReverseScan`) and would
  need its own baseline if added.
- **Engine-level cost split**: an engine-level C++ microbench (the
  deferred Layer 2) would isolate per-entry copy from leaf-chain
  traversal and L0 merge, giving R38 a tighter before/after
  measurement than the end-to-end baseline provides.
