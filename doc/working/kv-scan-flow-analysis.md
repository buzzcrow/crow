<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Scan Flow Analysis

End-to-end trace of the CROW scan (range read) path. Complements
[`kv-read-flow-analysis.md`](kv-read-flow-analysis.md) (which covers the
point-read get path and has a shorter scan section) and
[`kv-write-flow-analysis.md`](kv-write-flow-analysis.md). Focuses on
flow, conclusions, and data — not rationale prose. Baseline numbers
are in [`scan-perf-baseline.md`](scan-perf-baseline.md); the regression
sentinel is `tools/bench-scan-regression.sh`.

---

## Scan Flow — Range Read

```
Client SCAN(prefix, start_after, limit, read_mode, min_slot?)
  → CrowkvClient::scan                                  [client.rs:585]
    1. resolve_min_slot — MinSlot: auto-attach write watermark;
       Linearizable: 0                                 [same as get]
    2. resolve_read_endpoint — Linearizable (or MinSlot + Leader
       policy): cached leader endpoint; MinSlot + AnyReplica (R26):
       round-robin across replica endpoints, fallback to leader
    3. send KvScanRequest { prefix, start_after, limit, read_mode,
       min_slot }
       [copy: prefix + start_after → Bytes::copy_from_slice → HTTP/2
        frame, unavoidable]
    4. retry: NotLeaderHint non-empty → follow (uncounted; MinSlot+
       AnyReplica records read_endpoint_fallback); empty hint →
       counted error + backoff; transport error → backoff + refresh
       [client.rs:628–648 — scan now has NotLeaderHint (E3 done),
        mirroring get]
  → KvStoreService::scan (gRPC)                        [kv_service.rs]
    5. [Linearizable] if local not leader and not already forwarded →
       forward_kv_scan to leader (at-most-once via
       x-crow-kv-forwarded)
       - success → return leader's response
       - failure → fall through to local store (degraded)
    6. [MinSlot] no forwarding — serve local
       [copy: prefix + start_after allocated from network frame,
        unavoidable]
  → PxKvStore::kv_scan                              [px_kv_store.rs:138]
    7. resolve_read_point(group, read_mode, min_slot) → ReadDecision
       [same resolver as get; min_slot passed through]
    8. [Serve] learner.engine_scan(prefix, start_after, limit)
       → KVEngine::scan → KVFuture<(Vec<(key, slot, value)>, truncated)>
       [CrowTreeEngine: prefix + start_after to_vec() for FFI;
        try_scan → ScanOutcome::Ready (fast path, all pages resident)
        or ScanOutcome::Pending (cold-leaf miss, reactor demand-load
        retry loop); packed result take_buf; decode_scan per-entry
        Vec<u8> for key + value]
       [start_after is pushed down into the C++ engine (R37 done):
        the descent targets the leaf containing start_after, the
        merge loop skips keys <= start_after natively, and the limit
        is applied without over-fetching the prefix range — O(limit)
        FFI + decode cost, not O(prefix range)]
    9. build Vec<KvScanItem> { key: Bytes::from(key), value:
       Bytes::from(value) }
       [copy: per-entry key + value moved into Bytes (from Vec<u8>);
        this is the second copy of each value — decode_scan already
        copied from the packed buffer into Vec<u8>, and here the Vec
        is converted to Bytes (which takes ownership of the Vec's
        allocation, so no re-copy — but the decode_scan copy remains)]
    10. build KvScanResponse { read_slot, items, truncated,
        not_leader_hint }
        [move: items moved into response, no copy]
        [copy: items → socket buffer on gRPC serialize, unavoidable]
  → Client receives KvScanResponse
    11. resp.items.into_iter().map(|i| (i.key.to_vec(),
        i.value.to_vec())).collect()
        [copy: per-entry key + value to_vec() — prost Bytes → Vec<u8>]
        [this is the third touch of each value: C++ packed buffer →
         decode_scan Vec<u8> → Bytes (response) → Vec<u8> (client
         ScanOutcome)]
```

### Per-Mode Routing

Same resolver as get (`resolve_read_point`), see
[`kv-read-flow-analysis.md`](kv-read-flow-analysis.md) §"Per-Mode
Routing" for full detail. Scan-specific notes:

- **Linearizable** — `linearizable_read_barrier()` → lease fast path
  or ReadIndex fallback → serve at `contiguous_chosen`. The barrier
  cost is the same fixed ~4 ms per scan regardless of `limit` (the
  baseline confirms: from_start_10 and bounded_10 are both ~4.2 ms).
- **MinSlot** — `contiguous_applied >= min_slot` → serve locally at
  `contiguous_applied`; else `NotLeader` redirect (follower hasn't
  applied the client's last write). `min_slot = 0` accepts any
  staleness. At 1T:1C the baseline shows no difference between
  Linearizable and MinSlot (208 vs 206 scans/s) — the split would
  show at higher concurrency where MinSlot's any-replica round-robin
  distributes load.

### Deep Pagination (`start_after` pushdown — R37 done)

The C++ `scan` API takes `start_after` as an exclusive lower bound
(`crow-tree.h:545`). When non-empty, the B+tree descent targets the
leaf that would contain `start_after` (instead of the prefix start),
and the L0/L1 merge loop skips keys `<= start_after` natively. The
limit is applied after the skip, so the engine returns exactly the
entries after the cursor — O(limit) FFI + decode cost, not O(prefix
range).

**Baseline confirms §1.7 O(limit) claim**: deep pagination
(`start_after` near end, limit=10) is 1.7x slower than from-start
(7084us vs 4236us) — the O(log N) B+tree descent to a deeper leaf, not
O(prefix) over-fetch. If the engine over-fetched the prefix, deep
pagination would cost ~1.75s (the full_100k number), not 7ms. The
over-fetch proxy ratio is 1.7x; the etcd-style "fetch all then
truncate" regression would show ~400x.

Before R37, `start_after` was not pushed down — the engine set
`fetch_limit=0` (over-fetch the whole prefix range), shipped the
packed result across FFI, then filtered keys `<= start_after` in Rust
before applying the limit. R37 eliminated that over-fetch.

---

## Scan Concurrency

- **No admission window** — scans bypass `InflightAdmission`; no
  `max_inflight_scans`. Same as get: reads are cheap, non-blocking,
  and lease-path reads consume no consensus resources. Burst bounded
  only by Tokio scheduling and HTTP/2 stream concurrency.
- **Parallel across replicas — MinSlot only.** Each replica's engine
  has its own `scan` path with no cross-replica coordination.
  Linearizable scans serialize through the leader by definition.
- **gRPC response size cap** — tonic's default 4 MiB
  `max_decoding_message_size` limits the scan response payload. A scan
  returning > 4 MiB (e.g. 100k keys × ~70 B = 7 MB, or 1000 × 16 KiB =
  16 MiB) fails with a transport error. The baseline confirms:
  `full_100k` and `valuesize_16KiB` both hit this limit. This is the
  gRPC-message-size analog of etcd's range-read OOM risk (issue
  #12342). A streaming scan response (mirroring etcd PR #19766) or a
  raised max-message-size config would fix it.

---

## Correctness Conclusions

- **Single-version per key, highest-slot-wins** — the engine returns
  the latest live (non-tombstoned) value per key. Two replicas that
  applied the same slots have identical scan results. Same invariant
  as get (see `kv-read-flow-analysis.md` §"Correctness Conclusions").
- **`start_after` is an exclusive lower bound** — only keys strictly
  greater than `start_after` are returned. Verified by
  `ReadPath.ScanStartAfterCursorSkipsEarlierEntries` (sync) and
  `AsyncScan.StartAfterCursorSkipsEarlierEntries` (async).
- **`truncated` flag is trustworthy** — the C++ engine applies both
  `start_after` and `limit` natively (R37), so the `truncated` flag
  reflects whether more entries matched beyond the limit, not whether
  the Rust-side filter discarded entries. Before R37, `truncated` was
  set by the C++ limit (which was 0 = unlimited when `start_after` was
  non-empty), so it was always false for deep-pagination scans — the
  Rust-side truncation was not reflected.
- **Tombstones excluded by default** — `scan` with
  `include_tombstones = false` (the default) skips tombstoned keys.
  `iter_all` (for `compare`) includes tombstones and always runs on a
  pinned `RootVersion`.

---

## Scan Path Components

- **Client routing** — `CrowkvClient::scan` (`client.rs:585`) calls
  `resolve_read_endpoint` (same as get). Retry: `NotLeaderHint`
  non-empty → follow (uncounted; MinSlot+AnyReplica records
  `read_endpoint_fallback`); empty hint or transport error → counted
  error + backoff. The scan response's `not_leader_hint` field (E3
  done) enables the same redirect-follow retry as get.
- **Server forwarding** — `KvStoreService::scan`: check
  `x-crow-kv-forwarded` (loop-guard); if not forwarded and local not
  leader → `forward_kv_scan`; success → return leader response;
  failure → fall through (degraded). MinSlot never forwarded. Mirrors
  `KvStoreService::get` / `forward_kv_get`.
- **Engine scan (CrowTreeEngine)** — `try_scan`
  (`crow_tree_engine.rs:216`) → `ScanOutcome::Ready` (fast path, all
  pages resident) or `ScanOutcome::Pending` (cold-leaf miss, reactor
  demand-load). `start_after` is pushed into the C++ API (R37 done).
  Packed result decoded per-entry into `Vec<u8>` key + value via
  `decode_scan` (`ffi/src/lib.rs:1243`). The packed format is
  `[klen:u32][key][slot:u64][tombstone:u8][vlen:u32][value]` per entry.
- **Engine scan (InMemKV, test-only)** — `DashMap` is not key-ordered;
  collects matching live entries then sorts. Acceptable for test-only
  use.
- **Response build** — `px_kv_store.rs:180`: `Vec<(Vec<u8>, u64,
  Vec<u8>)>` → `Vec<KvScanItem>` with `Bytes::from(key)` /
  `Bytes::from(value)`. The slot is dropped (not sent to the client).
- **Client decode** — `client.rs:617`: `resp.items.into_iter().map(|i|
  (i.key.to_vec(), i.value.to_vec())).collect()` — prost `Bytes` →
  `Vec<u8>` per entry.

---

## Memory Copy Summary

Copy points are annotated inline in the flow diagram above. The scan
path has more O(n) copies than the get path (which is zero-copy after
R6):

- **Copy 1 (FFI decode)** — `decode_scan` (`ffi/src/lib.rs:1261,1272`):
  packed buffer → per-entry `Vec<u8>` for key and value. This is the
  copy R38 (scan value zero-copy) targets — a `PinnedScanEntry` /
  `Bytes::from_owner` path mirroring R6 would eliminate it.
- **Copy 2 (response build)** — `px_kv_store.rs:183`:
  `Bytes::from(key)` / `Bytes::from(value)` — `Bytes::from(Vec)` takes
  ownership of the Vec's allocation, so this is a move, not a copy.
  The decode_scan copy (Copy 1) is the real cost.
- **Copy 3 (client decode)** — `client.rs:620`: `i.key.to_vec()` /
  `i.value.to_vec()` — prost `Bytes` → `Vec<u8>`. This is a copy from
  the gRPC response buffer into the client's owned `Vec`. R44 flags
  this as "client copies response values (`to_vec` per scan entry)
  despite prost `Bytes`".
- **O(n) unavoidable** — gRPC request serialize (prefix + start_after
  into HTTP/2 frame); gRPC response serialize (items into socket
  buffer); FFI prefix + start_after `to_vec()` (owned copies for the
  C API boundary).

**Total value copies per entry: 2** (decode_scan packed → Vec, client
Bytes → Vec). R38 eliminates copy 1; R44's "client to_vec" subitem
eliminates copy 3 (by returning `Bytes` directly instead of `Vec`).
After both, the scan path would be zero-copy from frame to client
`Bytes`, matching get after R6.

---

## Benchmark Results — 2026-08-05 (macOS)

### Platform

- **CPU**: Apple M5 Pro — 18 cores / 18 threads, arm64
- **OS**: macOS 26.5.2 (Darwin 25.5.0)
- **Page size**: 16384 bytes

**Note**: The write baseline (`bench-write-regression.tsv`) and
read-flow analysis were captured on an AMD Ryzen 9 5950X Linux
machine. This scan baseline was captured on Apple silicon macOS —
absolute numbers are not directly comparable to the Linux baselines.
Re-capture on the same Linux Ryzen machine for cross-comparable
numbers. The bench code and script are platform-independent.

### Test Setup

- `crow-cli bench run --workload list --mode mem --threads 1
  --connections 1 --duration-secs 10 --pre-populate 100000`, 3-node
  cluster.
- 16 configs covering: full-keyspace (1k/10k/100k), bounded limit
  (10/100/1k/10k over 100k), deep pagination (start_after near end +
  from-start companion), value-size sweep (64B/1KiB/16KiB), prefix
  range, read-mode split.
- Raw TSV: `doc/working/bench-scan-regression.tsv` (gitignored,
  regenerated by `tools/bench-scan-regression.sh`). Reference numbers
  embedded in the script's comment block.

### Full-keyspace scan (limit >= keyspace, 64 B values)

- `full_1k` (limit=1000): 227 scans/s, avg=4411us, p99=5020us
- `full_10k` (limit=10000): 139 scans/s, avg=7181us, p99=7900us
- `full_100k` (limit=100000): 0 scans/s, avg=1752411us, **6 errors**

Scales linearly with key count (1k→10k: 2.2x keys, 1.6x time). The
100k scan hits the tonic 4 MiB max gRPC message size (100k × ~70 B =
~7 MB payload) — the 6 errors are transport-level rejections, not
engine failures.

### Bounded limit over 100k keyspace (64 B values)

- `bounded_10` (limit=10): 225 scans/s, avg=4438us
- `bounded_100` (limit=100): 223 scans/s, avg=4479us
- `bounded_1k` (limit=1000): 216 scans/s, avg=4640us
- `bounded_10k` (limit=10000): 137 scans/s, avg=7297us

Per-entry cost is small relative to the per-scan fixed cost (~4.2 ms):
limit 10/100/1k are all ~4.4 ms. At limit=10k the per-entry cost
becomes visible (7.3 ms). Per-entry cost is ~0.3 us/entry at 64 B
values.

### Deep pagination (start_after near end vs from-start companion)

- `from_start_10` (limit=10, start_after=""): 236 scans/s, avg=4236us
- `deep_pag_10` (limit=10, start_after=k...99989): 141 scans/s, avg=7084us
- `deep_pag_100` (limit=100, start_after=k...99899): 133 scans/s, avg=7538us

**§1.7 O(limit) verdict: CONFIRMED.** Deep pagination is 1.7x slower
than from-start — the O(log N) B+tree descent to a deeper leaf, not
O(prefix) over-fetch. If the engine over-fetched, deep_pag_10 would
cost ~1.75s (the full_100k number), not 7ms. The over-fetch proxy
ratio is 1.7x; the etcd-style regression would show ~400x. Deep
pagination limit 10 vs 100 (7084 vs 7538us) are within 6% — the cost
is dominated by the fixed descent + setup, not by entries returned.

### Value-size sweep (fixed limit=1000, 100k keyspace)

- `valuesize_64B`: 206 scans/s, avg=4852us
- `valuesize_1KiB`: 666 scans/s, avg=1500us
- `valuesize_16KiB`: 0 scans/s, **1701 errors** (gRPC 4 MiB limit)

**1 KiB anomaly**: 1 KiB values are 3.2x faster than 64 B values
despite returning 16x more data per scan. Root cause:
`MemTable::snapshot()` (`memtable.cpp:195`) copies **all** N_l0
entries into a vector on every scan call, regardless of the scan
limit — O(N_l0) per scan, not O(limit). With
`memtable_flush_bytes = 4 MiB`:
- 64 B values (~104 B/entry): byte threshold hit at ~40k entries →
  after 100k pre-pop (no flush-after-prepopulate), ~60k entries still
  in L0. Every scan pays `snapshot(60k entries)` even though
  limit=1000.
- 1 KiB values (~1064 B/entry): byte threshold hit at ~4k entries →
  ~25 flushes during pre-pop → after 100k, only ~4k entries in L0.
  Every scan pays `snapshot(4k entries)`.

The 60k vs 4k snapshot cost difference (15x) explains the 3.2x
throughput difference. The memtable per-entry is not slower than the
B+tree (sorted vector vs `resolve_chain_sorted`'s `std::map` build per
leaf) — the problem is `snapshot()` eagerly copies the entire memtable
instead of doing a lazy or range-bounded iteration. Fix: a
lazy/range-bounded L0 cursor (lower_bound to start_after, then iterate
up to limit) would make the L0 cost O(limit). Tracked as R47
(flush-after-prepopulate bench flag to verify) + a follow-on for the
lazy L0 cursor.

### Prefix range (bounded prefix vs whole-keyspace, same entry count)

- `prefix_1k` (prefix="k00", limit=1000): 209 scans/s, avg=4787us
- `whole_1k` (prefix="", limit=1000): 210 scans/s, avg=4753us

No measurable difference — the per-scan fixed cost (~4.2 ms) dominates
the descent difference at this scan width.

### Read-mode split (linearizable vs minslot, limit=1000)

- `lin_1k` (linearizable): 208 scans/s, avg=4797us
- `minslot_1k` (minslot, any-replica): 206 scans/s, avg=4855us

No measurable difference at 1T:1C — expected, since the single client
has no concurrency to exploit MinSlot's follower local-serve. The
read-mode split would show at higher concurrency.

---

## Cost Split

- **Per-scan fixed cost** (~4.2 ms): ReadIndex consensus round + gRPC
  roundtrip + B+tree descent. Dominates at small limit (10/100).
- **Per-entry cost** (~0.3 us/entry at 64 B): visible at limit >= 10k.
  At 64 B values, leaf-chain traversal + packed-buffer decode +
  per-entry value copy together are ~0.3 us/entry.
- **Per-byte copy cost** (R38's target): not cleanly separable from
  per-entry cost at the end-to-end level. The 1 KiB anomaly is caused
  by `MemTable::snapshot()` O(N_l0) cost, not per-byte copy — so the
  value-size sweep does not isolate R38's target. An engine-level C++
  microbench with a flushed tree (L0 empty) would be needed.
- **L0 snapshot cost** (the 1 KiB anomaly's root cause):
  `MemTable::snapshot()` is O(N_l0) per scan regardless of limit. At
  64 B values with ~60k unflushed entries, this adds ~60k entry copies
  per scan; at 1 KiB with ~4k unflushed, only ~4k. A lazy/range-bounded
  L0 cursor would make this O(limit).
- **Deep-pagination descent** (~2.8 ms overhead): the O(log N) B+tree
  descent to a leaf near the end of the keyspace. Fixed cost per scan,
  not per-entry.

**Prioritization**: R38 (zero-copy scan values) targets per-byte copy
cost (~0.3 us/entry at 64 B, grows with value size). The 1 KiB
anomaly is caused by `MemTable::snapshot()` O(N_l0) cost, not per-byte
copy — so the lazy L0 cursor fix (R47 follow-on) is higher priority
than R38 for small-value workloads with unflushed memtables. For
large-value workloads (>= 1 KiB), R38's zero-copy win is more
significant, but the gRPC 4 MiB message size limit caps practical scan
width before per-byte copy becomes dominant — so streaming scan
response is a prerequisite for R38's win to matter at scale.

---

## Gaps and Optimization Opportunities

### Open gaps

- **G1 — gRPC 4 MiB message size limit.** Scans returning > 4 MiB fail
  with transport errors (`full_100k`, `valuesize_16KiB`). A streaming
  scan response (mirroring etcd PR #19766) or a raised
  max-message-size config would fix it. Prerequisite for R38's
  zero-copy win to matter at scale (large-value / wide-limit scans).
- **G2 — L0 snapshot O(N_l0) cost.** `MemTable::snapshot()` copies all
  entries on every scan, regardless of limit. The bench has no
  flush-after-prepopulate flag, so the L0 size at scan time depends on
  value size (inadvertently). R47 adds a `--flush-after-prepopulate`
  flag to verify; a follow-on would make the L0 cursor
  lazy/range-bounded (lower_bound to start_after, iterate up to limit).
- **G3 — Value-size anomaly explained (G2 root cause).** 1 KiB values
  scan 3.2x faster than 64 B because `snapshot()` is O(N_l0): 64 B
  pre-pop leaves ~60k entries in L0, 1 KiB leaves ~4k. Not a per-byte
  or merge-cursor effect.

### Enhancement opportunities

- **E1 — Scan value zero-copy (R38).** `decode_scan` produces per-entry
  `Vec<u8>` for key and value from the packed buffer. A
  `PinnedScanEntry` / `Bytes::from_owner` path mirroring R6 would
  eliminate copy 1. Matters for large-value range reads; medium-high
  complexity (C++ packed format must support borrowing individual
  entry values; `KVEngine::scan` trait signature changes from
  `Vec<u8>` to `Bytes`).
- **E2 — Client `to_vec` copies (R44 subitem).** `client.rs:620` copies
  per-entry `Bytes` → `Vec<u8>` via `to_vec()`. Returning `Bytes`
  directly (changing `ScanOutcome.items` from `Vec<(Vec<u8>,
  Vec<u8>)>` to `Vec<(Bytes, Bytes)>`) would eliminate copy 3.
- **E3 — L0-overlay scan hardening (R44 subitem).** C++ `scan_async`
  restarts the whole scan on any cold leaf (no cursor resume). A
  cursor-resume path would avoid re-traversing already-visited leaves
  after a demand-load miss. Composes with R37's `start_after` cursor.
- **E4 — Lazy/range-bounded L0 cursor.** `MemTable::snapshot()` is
  O(N_l0) per scan. A lazy cursor (lower_bound to start_after, iterate
  up to limit) would make the L0 cost O(limit), eliminating the 1 KiB
  anomaly's root cause. Medium complexity (MemTable's `absl::btree_map`
  supports lower_bound; the scan merge loop needs to iterate the map
  directly instead of a snapshot vector).
- **E5 — Streaming scan response.** A streaming gRPC response (server
  streams scan entries in chunks) would remove the 4 MiB message size
  cap (G1) and let the client process entries incrementally, mirroring
  etcd PR #19766 and FDB's `getRange` with `WANT_ALL` streaming.

---

## Summary

The scan flow shares the read resolver and retry logic with get, but
diverges at the engine: scan walks a range of leaves via a merge
cursor (L0 overlay on L1), packs the result into a flat buffer, and
decodes per-entry into owned `Vec<u8>` — two more value copies than
get's zero-copy path (R6). The `start_after` pushdown (R37) is
confirmed correct and O(limit) by the baseline. The dominant costs
are the per-scan fixed overhead (~4.2 ms: ReadIndex + gRPC + descent),
at large limit the per-entry decode + copy (~0.3 us/entry at 64 B),
and — if the memtable is unflushed — the `MemTable::snapshot()` O(N_l0)
cost (the 1 KiB anomaly's root cause).

Open work: **G1** gRPC 4 MiB limit (streaming response), **G2** L0
snapshot O(N_l0) cost (R47 flush-after-prepopulate flag to verify),
**G3** value-size anomaly (explained, G2 root cause). Enhancements:
**E1** scan value zero-copy (R38), **E2** client `to_vec` copies (R44
subitem), **E3** scan_async cursor resume (R44 subitem), **E4**
lazy/range-bounded L0 cursor, **E5** streaming scan response.
