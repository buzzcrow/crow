<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Scan Flow Analysis

End-to-end trace of the CROW scan (range read) path. Complements
[`kv-read-flow-analysis.md`](kv-read-flow-analysis.md) (point-read get
path) and [`kv-write-flow-analysis.md`](kv-write-flow-analysis.md).
Focuses on flow, conclusions, and data — not rationale prose. Baseline
numbers in [`scan-perf-baseline.md`](scan-perf-baseline.md); regression
sentinel: `tools/bench-scan-regression.sh`.

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
    4. retry: NotLeaderHint non-empty → follow (uncounted); empty hint
       → counted error + backoff; transport error → backoff + refresh
  → KvStoreService::scan (gRPC)                        [kv_service.rs]
    5. [Linearizable] if local not leader and not already forwarded →
       forward_kv_scan to leader (at-most-once via
       x-crow-kv-forwarded)
    6. [MinSlot] no forwarding — serve local
       [copy: prefix + start_after from network frame, unavoidable]
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
       [start_after pushed down into C++ engine (R37): descent targets
        the leaf containing start_after, merge loop skips keys <=
        start_after natively, limit applied without over-fetching —
        O(limit) FFI + decode, not O(prefix range)]
       [MemTable::snapshot() copies all N_l0 entries per scan —
        O(N_l0), not O(limit); root cause of the 1KiB anomaly (R48)]
    9. build Vec<KvScanItem> { key: Bytes::from(key), value:
       Bytes::from(value) }
       [move: Vec → Bytes takes ownership of the allocation, no re-copy]
    10. build KvScanResponse { read_slot, items, truncated,
        not_leader_hint }
        [move: items moved into response, no copy]
        [copy: items → socket buffer on gRPC serialize, unavoidable]
  → Client receives KvScanResponse
    11. resp.items.into_iter().map(|i| (i.key.to_vec(),
        i.value.to_vec())).collect()
        [copy: per-entry key + value to_vec() — prost Bytes → Vec<u8>]
```

---

## Design Changes and Effects

- **R37 — `start_after` pushdown (done).** Before R37, `start_after`
  was not pushed into the C++ engine — the engine set `fetch_limit=0`
  (over-fetch the whole prefix range), shipped the packed result across
  FFI, then filtered keys `<= start_after` in Rust. R37 pushed
  `start_after` into the C++ API: the descent targets the cursor's
  leaf, the merge loop skips earlier keys natively, and the limit is
  applied without over-fetching. Effect: deep pagination (limit=10,
  start_after near end) dropped from ~1.75s (O(prefix)) to ~7ms
  (O(limit) + O(log N) descent) — confirmed by the R46 baseline.

- **R46 — Scan perf baseline (done).** Added `--scan-limit`,
  `--scan-prefix`, `--scan-start-after` CLI flags to `crow-cli bench
  run`, wired the `OpKind::List` arm to use `cfg.read_mode` /
  `cfg.min_slot_policy`, and created
  `tools/bench-scan-regression.sh` (16 configs). Captured the baseline
  on Apple M5 Pro macOS. Key findings: per-scan fixed cost ~4.2ms,
  per-entry ~0.3us at 64B, gRPC 4 MiB limit caps scan payload, 1KiB
  anomaly (see R48).

- **R47 — Flush-after-prepopulate bench flag (done).** Added a
  `--flush-after-prepopulate` flag to `crow-cli bench run` and a
  `POST /stores/{sid}/groups/{gid}/flush` management API endpoint.
  Empirical result: the flag drains L0 deterministically but the 3.2x
  gap is unchanged (64B flushed 219/s vs 1KiB flushed 721/s), REFUTING
  the `MemTable::snapshot()` O(N_l0) hypothesis. The maintenance loop's
  3s tick already keeps L0 small during measurement. The flag remains
  useful for deterministic L1-only baselines.

---

## Memory Copy Summary

Copy points annotated inline in the flow diagram above. Summary of
what remains:

- **O(N_l0) per scan — `MemTable::snapshot()`** — copies all memtable
  entries into a vector on every scan call, regardless of limit. R47's
  `--flush-after-prepopulate` experiment REFUTED this as the 1KiB
  anomaly's root cause: draining L0 did not close the 3.2x gap (the
  maintenance loop's 3s tick already keeps L0 small during
  measurement). R48 (lazy btree_map cursor) would still make this
  O(limit) but would not fix the anomaly. The real anomaly root cause
  is unknown (L1 scan path / decode) and needs a separate
  investigation.
- **O(limit) per scan — `decode_scan`** — packed buffer → per-entry
  `Vec<u8>` for key and value. R38 eliminates this via
  `PinnedScanEntry` / `Bytes::from_owner` mirroring R6.
- **O(limit) per scan — client `to_vec`** — prost `Bytes` → `Vec<u8>`
  per entry. R44 subitem: return `Bytes` directly instead of `Vec`.
- **O(n) unavoidable** — gRPC request serialize (prefix + start_after
  into HTTP/2 frame); gRPC response serialize (items into socket); FFI
  prefix + start_after `to_vec()` (owned copies for the C API
  boundary).

The scan path has more copies than the get path (zero-copy after R6).
After R38 + R48, the scan path would be zero-copy from frame to client
`Bytes`, matching get.

---

## Benchmark Results — 2026-08-05 (macOS, post-R38/R44/R49)

**Platform**: Apple M5 Pro, 18c, arm64, macOS 26.5. Not comparable to
the Linux Ryzen write baseline — re-capture on the same machine for
cross-comparable numbers.

**Setup**: 1T:1C, 10s mem mode, 3-node cluster, 100k pre-populated
keys. Raw TSV: `doc/working/bench-scan-regression.tsv` (gitignored).
Reference numbers in `tools/bench-scan-regression.sh`.

### Key findings

- **Per-scan fixed cost ~4.3ms** (ReadIndex + gRPC + descent); per-entry
  ~0.18us at 64B (visible at limit >= 10k).
- **Streaming scan (R49) fixes the 4 MiB cap**: `full_100k` now
  completes (20 scans/s, 0 errors) — previously 0 scans/s with 6
  transport errors. `valuesize_16KiB` mostly works (27 scans/s) but
  has 309 residual errors (retry edge cases under high payload).
- **1KiB values 3.8x faster than 64B** — anomaly persists after R38
  (zero-copy) and R47 (L0 drain). Root cause is in the L1 B+tree scan
  path, not L0 or per-byte copy. Needs an engine-level C++ microbench
  to isolate.

### Comparison vs pre-R38/R44/R49 baseline

| Config | Before scans/s | After scans/s | Before err | After err | Notes |
|--------|---------------|---------------|-----------|----------|-------|
| full_1k | 227 | 243 | 0 | 0 | +7% (noise) |
| full_10k | 139 | 165 | 0 | 0 | +19% |
| full_100k | 0 | 20 | 6 | 0 | R49 streaming fix |
| bounded_10 | 225 | 223 | 0 | 0 | noise |
| bounded_100 | 223 | 230 | 0 | 0 | noise |
| bounded_1k | 216 | 224 | 0 | 0 | noise |
| bounded_10k | 137 | 164 | 0 | 0 | +20% |
| from_start_10 | 236 | 231 | 0 | 0 | noise |
| deep_pag_10 | 141 | 147 | 0 | 0 | noise |
| deep_pag_100 | 133 | 143 | 0 | 0 | noise |
| valuesize_64B | 206 | 202 | 0 | 0 | noise |
| valuesize_1KiB | 666 | 766 | 0 | 0 | +15% |
| valuesize_16KiB | 0 | 27 | 1701 | 309 | R49 streaming, residual errors |
| prefix_1k | 209 | 214 | 0 | 0 | noise |
| whole_1k | 210 | 209 | 0 | 0 | noise |
| lin_1k | 208 | 217 | 0 | 0 | noise |
| minslot_1k | 206 | 206 | 0 | 0 | noise |

**Takeaways**: R49 (streaming scan) unblocked `full_100k` and
`valuesize_16KiB`. R38 (zero-copy) and R44 (read-path hardening) gave
a modest ~15-20% throughput improvement on larger scans (full_10k,
bounded_10k, valuesize_1KiB) but no measurable change on small scans
(where the per-scan fixed cost dominates). The 1KiB anomaly persists
(3.8x, up from 3.2x) — R38's zero-copy did not close it, confirming
the root cause is not per-byte copy.

### Cost split

- **Per-scan fixed** (~4.3ms): ReadIndex + gRPC + B+tree descent.
  Dominates at small limit (10/100).
- **Per-entry** (~0.18us at 64B): leaf-chain traversal + packed-buffer
  decode + zero-copy value slice. Visible at limit >= 10k. Down from
  ~0.3us pre-R38 (zero-copy reduced per-entry allocation).
- **L0 snapshot** (O(N_l0)): `MemTable::snapshot()` copies all unflushed
  entries per scan. R47 refuted this as the 1KiB anomaly's cause —
  draining L0 did not close the gap. The maintenance loop (3s tick)
  keeps L0 small during measurement.
- **Deep-pagination descent** (~2.5ms): O(log N) B+tree descent to a
  leaf near the end. Fixed cost per scan.

Full per-config analysis in
[`scan-perf-baseline.md`](scan-perf-baseline.md).

---

## Unsolved Issues

Tracked as backlog requirements:

- **[R38](../backlog/R38-scan-value-zero-copy.md)** — Scan value
  zero-copy (done). `decode_scan` now converts the packed buffer to a
  single `Bytes` and slices per entry (0 copies, down from 2N). The
  `KVEngine::scan` trait returns `Vec<(Bytes, u64, Bytes)>`. Simpler
  than the backlog's `PinnedScanEntry` plan — `take_buf` already owns
  the packed buffer in Rust, so no C++ page refcount pinning needed.
- **[R44](../backlog/R44-kv-read-path-hardening.md)** — Read-path
  hardening batch (done). All 8 items: scan forward-fail hint parity,
  scan error propagation (Corruption → scan_err not empty ok),
  structured `KvErrorCode` proto enum, topology refresh failure
  logging, bounded catch-up (64 slots/round off ReadIndex path), C++
  scan cursor resume (O(N) not O(N²) cold-leaf loads), zero-copy
  `Bytes` client API, per-mode scan latency + forward counters.
- **[R47](../backlog/R47-bench-flush-after-prepopulate.md)** —
  Flush-after-prepopulate bench flag (done). Result: REFUTED the L0
  snapshot hypothesis — draining L0 did not close the 1KiB anomaly.
- **[R48](../backlog/R48-scan-lazy-l0-cursor.md)** — Lazy/range-bounded
  L0 cursor. Replaces `MemTable::snapshot()` with a lazy btree_map
  cursor (lower_bound to start_after, advance up to limit). Fixes the
  1KiB anomaly's root cause.
- **[R49](../backlog/R49-scan-streaming-response.md)** — Streaming
  gRPC scan response (done). New `ScanStream` server-streaming RPC
  emits `KvScanChunk` messages (256 entries or 1 MiB per chunk). Client
  `scan_stream` reassembles into `ScanOutcome` with same retry/redirect
  semantics. Bench uses `scan_stream`. Unary `Scan` retained for
  compatibility. Composes with R38 (zero-copy values) for full
  large-scan solution.
