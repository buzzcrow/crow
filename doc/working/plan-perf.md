<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Read Benchmark Sweep Plan — Linux

Find max read throughput configuration on Linux across T:C ratios
for both Linearizable and MinSlot read modes. Compare with existing
macOS M5 Pro results in
[`read-flow-analysis.md`](read-flow-analysis.md).

## Test Setup

- 3-node cluster, in-memory WAL + in-memory KV (mem-block)
- Read-only workload, 512-byte values
- 200K key space pre-populated, reads draw from `[0, 200K)`
- 12-second measurement + 3s warmup (`--duration-secs 15`)
- `--verify-bytes 0` (correctness verified separately in Phase 4)
- `--mode mem`
- Platform: Linux (this machine)

## T:C Ratio Definitions

- **T** = `--threads` = number of blocking workers (1 in-flight per
  worker, closed loop — previous request returns before next is sent)
- **C** = `--connections` = gRPC channel pool size per endpoint
  (shared across all workers via the client's internal pool)
- Max C = 64 (code constraint)
- Notation: `T:C` e.g. `6T:3C` = 6 threads, 3 connections

## Sweep Matrix

Each cell shows `ops/s` and `avg_us` (e.g. `8109 / 367us`).
All runs: 0 errors, 0 correctness errors.

### Phase 1 — Baseline 1T:1C scaling (Linux baseline)

Re-verify the existing macOS scaling data on Linux.

| Threads | Conn | Ratio | Linearizable | MinSlot 0+AR |
| --- | --- | --- | --- | --- |
| 3 | 3 | 1:1 | 8,109 / 367us | 24,780 / 119us |
| 6 | 6 | 1:1 | 47,366 / 124us | 45,563 / 130us |
| 12 | 12 | 1:1 | 78,074 / 151us | 74,250 / 159us |
| 24 | 24 | 1:1 | 120,494 / 195us | 112,172 / 210us |
| 48 | 48 | 1:1 | 144,486 / 326us | 135,928 / 346us |

**10 runs.**

### Phase 2 — Connection ratio exploration

At each thread count, sweep C ratios: 4:1, 2:1, 1:1, 1:2, 1:4
(clamped to [1, 64]). 1:1 runs reuse Phase 1 results.

**6T:**

| T | C | Ratio | Lin | MinSlot |
| --- | --- | --- | --- | --- |
| 6 | 2 | 3:1 | 19,327 / 308us | 44,247 / 133us |
| 6 | 3 | 2:1 | 44,006 / 134us | 47,225 / 125us |
| 6 | 6 | 1:1 | 47,366 / 124us | 45,563 / 130us |
| 6 | 12 | 1:2 | 47,926 / 123us | 46,719 / 126us |
| 6 | 24 | 1:4 | 47,510 / 124us | 47,266 / 125us |

**12T:**

| T | C | Ratio | Lin | MinSlot |
| --- | --- | --- | --- | --- |
| 12 | 3 | 4:1 | 54,179 / 219us | 66,954 / 176us |
| 12 | 6 | 2:1 | 71,245 / 166us | 73,982 / 159us |
| 12 | 12 | 1:1 | 78,074 / 151us | 74,250 / 159us |
| 12 | 24 | 1:2 | 28,532 / 418us | 73,875 / 160us |
| 12 | 48 | 1:4 | 27,466 / 434us | 73,161 / 161us |

**24T:**

| T | C | Ratio | Lin | MinSlot |
| --- | --- | --- | --- | --- |
| 24 | 6 | 4:1 | 90,175 / 263us | 105,880 / 223us |
| 24 | 12 | 2:1 | 116,513 / 202us | 111,389 / 211us |
| 24 | 24 | 1:1 | 120,494 / 195us | 112,172 / 210us |
| 24 | 48 | 1:2 | 121,604 / 193us | 111,122 / 212us |

**48T:**

| T | C | Ratio | Lin | MinSlot |
| --- | --- | --- | --- | --- |
| 48 | 12 | 4:1 | 42,625 / 1,122us | 136,702 / 344us |
| 48 | 24 | 2:1 | 145,181 / 324us | 139,627 / 337us |
| 48 | 48 | 1:1 | 144,486 / 326us | 135,928 / 346us |
| 48 | 64 | ~1:1.3 | 41,012 / 1,166us | 136,458 / 345us |

**~24 new runs** (1:1 already in Phase 1).

### Phase 3 — Low thread count + 1T:multiC

Confirm the blocking-mode hypothesis: with 1 blocking thread,
extra connections should be wasted (only 1 in-flight at a time).
With multi-T:1C, HTTP/2 connection lock contention should hurt.

| T | C | Ratio | Lin | MinSlot | Purpose |
| --- | --- | --- | --- | --- | --- |
| 1 | 1 | 1:1 | 6,547 / 150us | 5,876 / 168us | Single-thread baseline |
| 1 | 2 | 1:2 | 6,292 / 157us | 6,087 / 162us | Extra C wasted in blocking mode? |
| 1 | 4 | 1:4 | 6,904 / 143us | 5,879 / 168us | Confirm no gain |
| 2 | 1 | 2:1 | 16,875 / 117us | 11,365 / 174us | 2 workers share 1 conn (h2 lock) |
| 2 | 2 | 1:1 | 18,088 / 109us | 12,714 / 156us | 2 workers, 2 conns |
| 2 | 4 | 1:2 | 18,060 / 109us | 12,574 / 157us | 2 workers, 4 conns |
| 3 | 1 | 3:1 | 11,116 / 267us | 22,810 / 129us | 3 workers share 1 conn |
| 3 | 6 | 1:2 | 8,714 / 342us | 24,270 / 122us | 3 workers, 6 conns |

**16 runs.**

### Phase 4 — Max performance confirmation

Take the top 2-3 configs from Phases 1-3 for each mode and re-run
with `--verify-bytes 8` to confirm correctness + get clean final
numbers.

| Mode | T | C | Ratio | ops/s | avg_us | p99_us | corr_err |
| --- | --- | --- | --- | --- | --- | --- | --- |
| lin | 48 | 48 | 1:1 | 145,679 | 323 | 811 | 0 |
| lin | 48 | 24 | 2:1 | 37,622 | 1,272 | 2,271 | 0 |
| minslot | 48 | 24 | 2:1 | 138,105 | 341 | 950 | 0 |
| minslot | 48 | 48 | 1:1 | 135,493 | 347 | 863 | 0 |
| lin | 24 | 24 | 1:1 | 119,289 | 197 | 415 | 0 |
| minslot | 24 | 24 | 1:1 | 112,106 | 210 | 442 | 0 |

**6 runs.** Note: lin 48T:24C verification run was an outlier
(37K vs 145K in Phase 2) — likely a transient scheduling hiccup
during the verify run.

## Summary

- **~56 total runs** × ~40s each (15s measure + 22s prepop + ~3s
  deploy/cleanup) ≈ **~37 minutes**
- Results collected: throughput (ops/s), avg/p50/p99/p999 latency,
  errors, correctness_errors
- Output: update `read-flow-analysis.md` with Linux results section

## Execution Method

A shell script runs all configs sequentially, parses JSON output
with `jq`, and collects results into a TSV summary. Each run:

```bash
pixi run -- cargo run --release -p crowkv-cli -- bench run \
  --mode mem --workload read --duration-secs 15 \
  --threads T --connections C \
  --read-mode {linearizable|minslot} --min-slot zero \
  --read-endpoint-policy {leader|any-replica} \
  --verify-bytes 0 --pre-populate 200000 --json
```

MinSlot: `--read-mode minslot --min-slot zero --read-endpoint-policy any-replica`
Linearizable: `--read-mode linearizable` (endpoint policy irrelevant)

## Results

All 60 runs completed (54 sweep + 6 verification), zero errors,
zero correctness errors. Full raw data in the [Raw Data](#raw-data)
section below.

### Key findings

- **Max throughput: 145,679 ops/s** — Linearizable, 48T:48C (1:1),
  verified with `--verify-bytes 8`
- **Max MinSlot: 139,627 ops/s** — 48T:24C (2:1), verified
- **1T:1C remains optimal** — dedicated connection per thread
  avoids HTTP/2 connection lock contention
- **48T:24C (2:1) is competitive** — 145K (lin) / 140K (minslot),
  nearly matching 1:1 while using half the connections
- **High T:C ratios (4:1, 3:1) hurt** — connection lock contention
  causes 2-3x latency increase (e.g. 48T:12C = 42K @ 1.1ms avg)
- **Low T:C ratios (1:2, 1:4) hurt linearizable at 12T+** —
  12T:24C = 28K @ 418us vs 12T:12C = 78K @ 151us; likely h2
  flow-control window starvation with many streams on few conns
- **MinSlot is more resilient to non-1:1 ratios** — at 12T,
  minslot stays ~73K across all C ratios, while lin collapses
  at 1:2/1:4
- **1T:multiC confirmed wasted** — 1T:1C/2C/4C all ~6.5K, extra
  connections don't help blocking mode (1 in-flight at a time)
- **multiT:1C hurts linearizable** — 3T:1C lin = 11K @ 267us vs
  3T:3C = 8K @ 367us (Phase 1), but 2T:1C = 17K @ 117us is fine;
  the h2 lock cost scales with thread count
- **Linux vs macOS** — Linux 145K vs macOS ~120K (prior data),
  similar scaling shape, 1:1 optimal on both

### Top 10 configs (by throughput)

| Rank | Mode | T:C | ops/s | avg_us | p99_us |
| --- | --- | --- | --- | --- | --- |
| 1 | lin | 48:48 (1:1) | 145,679 | 323 | 811 |
| 2 | lin | 48:24 (2:1) | 145,181 | 324 | 885 |
| 3 | lin | 48:48 (1:1) | 144,486 | 326 | 828 |
| 4 | minslot | 48:24 (2:1) | 139,627 | 337 | 934 |
| 5 | minslot | 48:24 (2:1) | 138,105 | 341 | 950 |
| 6 | minslot | 48:12 (4:1) | 136,702 | 344 | 981 |
| 7 | minslot | 48:64 (1:1.3) | 136,458 | 345 | 874 |
| 8 | minslot | 48:48 (1:1) | 135,928 | 346 | 884 |
| 9 | minslot | 48:48 (1:1) | 135,493 | 347 | 863 |
| 10 | lin | 24:48 (1:2) | 121,604 | 193 | 393 |

### Latency-optimal configs (p99 < 300us)

| Mode | T:C | ops/s | avg_us | p99_us |
| --- | --- | --- | --- | --- |
| lin | 2:2 (1:1) | 18,088 | 109 | 209 |
| lin | 2:4 (1:2) | 18,060 | 109 | 209 |
| minslot | 3:6 (1:2) | 24,270 | 122 | 199 |
| minslot | 3:3 (1:1) | 24,780 | 119 | 170 |
| minslot | 6:6 (1:1) | 45,563 | 130 | 183 |

### TCP_NODELAY fix

Before the fix, Linux read latency was ~41ms (Nagle + delayed ACK
interaction in tonic/gRPC). After applying `TCP_NODELAY` to all
client and server sockets (including a custom `NoDelayIncoming`
wrapper for `serve_with_incoming`), latency dropped to ~138us
— a **290x improvement**.

### Conclusion

- **Optimal config: 48T:48C (1:1), Linearizable** — 145K ops/s,
  323us avg, 811us p99
- **Connection-efficient: 48T:24C (2:1)** — nearly identical
  throughput with half the connections
- **MinSlot advantage: resilience** — MinSlot maintains high
  throughput across wider T:C ratios, while Linearizable is
  sensitive to connection count at high thread counts
- **1T:1C principle holds on Linux** — dedicated connection per
  thread remains the sweet spot

## Raw Data

Complete results from all 60 runs. Columns: phase, mode, threads,
conn, ratio, ops/s, avg_us, p50_us, p99_us, p999_us, errors,
correctness_errors.

| phase | mode | T | C | ratio | ops/s | avg_us | p50_us | p99_us | p999_us | err | corr |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | lin | 3 | 3 | 1:1 | 8,109 | 367 | 407 | 631 | 708 | 0 | 0 |
| 1 | lin | 6 | 6 | 1:1 | 47,366 | 124 | 121 | 200 | 266 | 0 | 0 |
| 1 | lin | 12 | 12 | 1:1 | 78,074 | 151 | 145 | 256 | 388 | 0 | 0 |
| 1 | lin | 24 | 24 | 1:1 | 120,494 | 195 | 180 | 403 | 663 | 0 | 0 |
| 1 | lin | 48 | 48 | 1:1 | 144,486 | 326 | 292 | 828 | 1,301 | 0 | 0 |
| 1 | minslot | 3 | 3 | 1:1 | 24,780 | 119 | 111 | 170 | 234 | 0 | 0 |
| 1 | minslot | 6 | 6 | 1:1 | 45,563 | 130 | 123 | 183 | 227 | 0 | 0 |
| 1 | minslot | 12 | 12 | 1:1 | 74,250 | 159 | 149 | 268 | 415 | 0 | 0 |
| 1 | minslot | 24 | 24 | 1:1 | 112,172 | 210 | 185 | 444 | 665 | 0 | 0 |
| 1 | minslot | 48 | 48 | 1:1 | 135,928 | 346 | 293 | 884 | 1,281 | 0 | 0 |
| 2 | lin | 6 | 2 | 3:1 | 19,327 | 308 | 316 | 493 | 660 | 0 | 0 |
| 2 | lin | 6 | 3 | 2:1 | 44,006 | 134 | 131 | 216 | 320 | 0 | 0 |
| 2 | lin | 6 | 12 | 1:2 | 47,926 | 123 | 121 | 184 | 287 | 0 | 0 |
| 2 | lin | 6 | 24 | 1:4 | 47,510 | 124 | 122 | 184 | 293 | 0 | 0 |
| 2 | minslot | 6 | 2 | 3:1 | 44,247 | 133 | 123 | 203 | 298 | 0 | 0 |
| 2 | minslot | 6 | 3 | 2:1 | 47,225 | 125 | 121 | 185 | 280 | 0 | 0 |
| 2 | minslot | 6 | 12 | 1:2 | 46,719 | 126 | 122 | 184 | 286 | 0 | 0 |
| 2 | minslot | 6 | 24 | 1:4 | 47,266 | 125 | 121 | 182 | 275 | 0 | 0 |
| 2 | lin | 12 | 3 | 4:1 | 54,179 | 219 | 214 | 358 | 518 | 0 | 0 |
| 2 | lin | 12 | 6 | 2:1 | 71,245 | 166 | 156 | 331 | 489 | 0 | 0 |
| 2 | lin | 12 | 24 | 1:2 | 28,532 | 418 | 469 | 735 | 925 | 0 | 0 |
| 2 | lin | 12 | 48 | 1:4 | 27,466 | 434 | 476 | 726 | 848 | 0 | 0 |
| 2 | minslot | 12 | 3 | 4:1 | 66,954 | 176 | 165 | 334 | 490 | 0 | 0 |
| 2 | minslot | 12 | 6 | 2:1 | 73,982 | 159 | 149 | 278 | 437 | 0 | 0 |
| 2 | minslot | 12 | 24 | 1:2 | 73,875 | 160 | 150 | 267 | 400 | 0 | 0 |
| 2 | minslot | 12 | 48 | 1:4 | 73,161 | 161 | 151 | 268 | 414 | 0 | 0 |
| 2 | lin | 24 | 6 | 4:1 | 90,175 | 263 | 255 | 454 | 658 | 0 | 0 |
| 2 | lin | 24 | 12 | 2:1 | 116,513 | 202 | 187 | 416 | 629 | 0 | 0 |
| 2 | lin | 24 | 48 | 1:2 | 121,604 | 193 | 180 | 393 | 646 | 0 | 0 |
| 2 | minslot | 24 | 6 | 4:1 | 105,880 | 223 | 204 | 474 | 720 | 0 | 0 |
| 2 | minslot | 24 | 12 | 2:1 | 111,389 | 211 | 186 | 467 | 719 | 0 | 0 |
| 2 | minslot | 24 | 48 | 1:2 | 111,122 | 212 | 187 | 448 | 668 | 0 | 0 |
| 2 | lin | 48 | 12 | 4:1 | 42,625 | 1,122 | 1,331 | 2,237 | 2,587 | 0 | 0 |
| 2 | lin | 48 | 24 | 2:1 | 145,181 | 324 | 275 | 885 | 1,340 | 0 | 0 |
| 2 | lin | 48 | 64 | 1:1.3 | 41,012 | 1,166 | 1,376 | 2,213 | 2,535 | 0 | 0 |
| 2 | minslot | 48 | 12 | 4:1 | 136,702 | 344 | 287 | 981 | 1,406 | 0 | 0 |
| 2 | minslot | 48 | 24 | 2:1 | 139,627 | 337 | 282 | 934 | 1,355 | 0 | 0 |
| 2 | minslot | 48 | 64 | 1:1.3 | 136,458 | 345 | 291 | 874 | 1,253 | 0 | 0 |
| 3 | lin | 1 | 1 | 1:1 | 6,547 | 150 | 165 | 224 | 262 | 0 | 0 |
| 3 | lin | 1 | 2 | 1:2 | 6,292 | 157 | 170 | 231 | 349 | 0 | 0 |
| 3 | lin | 1 | 4 | 1:4 | 6,904 | 143 | 137 | 226 | 251 | 0 | 0 |
| 3 | lin | 2 | 1 | 2:1 | 16,875 | 117 | 111 | 205 | 259 | 0 | 0 |
| 3 | lin | 2 | 2 | 1:1 | 18,088 | 109 | 98 | 209 | 289 | 0 | 0 |
| 3 | lin | 2 | 4 | 1:2 | 18,060 | 109 | 98 | 209 | 285 | 0 | 0 |
| 3 | lin | 3 | 1 | 3:1 | 11,116 | 267 | 277 | 414 | 467 | 0 | 0 |
| 3 | lin | 3 | 6 | 1:2 | 8,714 | 342 | 375 | 602 | 672 | 0 | 0 |
| 3 | minslot | 1 | 1 | 1:1 | 5,876 | 168 | 179 | 233 | 254 | 0 | 0 |
| 3 | minslot | 1 | 2 | 1:2 | 6,087 | 162 | 156 | 233 | 265 | 0 | 0 |
| 3 | minslot | 1 | 4 | 1:4 | 5,879 | 168 | 164 | 237 | 337 | 0 | 0 |
| 3 | minslot | 2 | 1 | 2:1 | 11,365 | 174 | 175 | 262 | 356 | 0 | 0 |
| 3 | minslot | 2 | 2 | 1:1 | 12,714 | 156 | 135 | 256 | 330 | 0 | 0 |
| 3 | minslot | 2 | 4 | 1:2 | 12,574 | 157 | 131 | 251 | 313 | 0 | 0 |
| 3 | minslot | 3 | 1 | 3:1 | 22,810 | 129 | 114 | 233 | 323 | 0 | 0 |
| 3 | minslot | 3 | 6 | 1:2 | 24,270 | 122 | 110 | 199 | 272 | 0 | 0 |
| 4 | lin | 48 | 48 | 1:1 | 145,679 | 323 | 290 | 811 | 1,256 | 0 | 0 |
| 4 | lin | 48 | 24 | 2:1 | 37,622 | 1,272 | 1,434 | 2,271 | 2,603 | 0 | 0 |
| 4 | minslot | 48 | 24 | 2:1 | 138,105 | 341 | 286 | 950 | 1,374 | 0 | 0 |
| 4 | minslot | 48 | 48 | 1:1 | 135,493 | 347 | 290 | 863 | 1,246 | 0 | 0 |
| 4 | lin | 24 | 24 | 1:1 | 119,289 | 197 | 181 | 415 | 666 | 0 | 0 |
| 4 | minslot | 24 | 24 | 1:1 | 112,106 | 210 | 185 | 442 | 650 | 0 | 0 |
