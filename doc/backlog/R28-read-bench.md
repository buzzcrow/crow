<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R28: Read path benchmark

**Problem**: The write path has a benchmark harness
(`crowkv-console/cli/src/bench/`) with configurable threads,
connections, window, and duration, plus recorded results in
[`write-flow-analysis.md`](../working/write-flow-analysis.md) §
Benchmark Results. The read path has no equivalent benchmark.

The existing bench runner supports `WorkloadKind::Read`, but it
hardcodes `ReadMode::Linearizable` with `min_slot = None`
(`runner.rs:413`). There is no way to bench MinSlot reads, no way to
vary read mode, no min_slot policy, and no pre-population step — so
read-only workloads on an empty key space mostly return `NotFound`
(a different code path than `Found`). The read path has two distinct
serving models (Linearizable with lease/ReadIndex barrier; MinSlot
with local self-check) whose throughput and latency characteristics
are unmeasured.

Full read flow in
[`read-flow-analysis.md`](../working/read-flow-analysis.md).

**Approach**: Extend the bench harness to support both read modes with
deterministic data and cheap correctness verification, then run a
two-phase benchmark.

### Data Preparation

**Key space**: 200,000 keys (`k00000000000000000000` ..
`k000000000000000019999`), pre-populated before measurement. At ~50K
write TPS this is ~4s setup — fast enough not to dominate the bench
run, large enough to avoid hot-key caching effects (each key is read
~0.5 times/sec at 100K read TPS / 200K keys).

**Value generation — deterministic per-key formula**: each byte of a
value is independently computable from `(key_id, offset)`:

```
byte_at(key_id, offset) = splitmix64(key_id ^ splitmix64(offset)) mod 256
```

- `key_id` is the integer key (the `id` in `k{id:020}`).
- `offset` is the byte position within the value.
- Per-byte independent hash (not a seeded PRNG over the full value):
  O(1) to compute any single byte — to verify 8 random bytes, compute
  8 hashes, not 512. A seeded PRNG would require O(N) to reach offset
  N.
- The value still looks like random noise (no compression-friendly
  pattern), so wire transfer and engine storage costs are realistic.
- Pre-population writes use the same formula:
  `value = (0..value_size).map(|i| byte_at(key_id, i)).collect()`.
  Written data is verifiable by the same formula reads use.

**Pre-population phase**:

- New `--pre-populate <count>` flag (default 200,000). Writes `count`
  keys with deterministic values before the measurement window begins.
- Pre-population is **not measured** — excluded from latency histograms
  and TPS. Report its duration separately (`pre_pop_ms`).
- Pre-population uses the existing write path (`client.put`), so it
  also establishes the client's `write_watermark` — MinSlot reads with
  `min_slot = auto` carry this watermark.
- Pre-population writes are sequential over `[0, count)`; retries on
  `NotLeader` to minimize gaps. A few remaining gaps produce `NotFound`
  on read (counted separately, not a correctness error).

**Why not concurrent background writes during measurement**: writes
consume consensus resources (WAL, Paxos rounds) that perturb read
latency. Pre-population completes before measurement starts, so reads
run against a stable dataset with no write interference.

### Correctness Verification

**Random spot-check per read**: on each read returning `Found`:

1. Pick `R` random offsets in `[0, value_size)` (R default 8,
   configurable via `--verify-bytes`).
2. For each offset, compute `expected = byte_at(key_id, offset)`.
3. Compare with `actual[offset]` from the read result.
4. If any mismatch → record a **correctness error** (new counter,
   distinct from transport/NotLeader errors).

**Why R=8 not 512**: at 100K read TPS, full 512-byte comparison =
51.2 GB/s of comparison work — would become the bottleneck. 8 random
bytes = 800 MB/s at 100K TPS — negligible. Offset selection is random
per-read (not fixed), so different reads check different parts of the
value; over many reads the entire value is covered with high
probability. For deterministic verification (every read, every time),
increase R to 16 or 32 via `--verify-bytes`.

### Read Order / Key Selection

Reads draw uniformly from `[0, pre_populate_count)` — the populated
range is the implicit candidate set. No explicit candidate key list
needed; every key in the range was written during pre-population.
Uniform random is the default (measures random-read latency,
cache-unfriendly, realistic for KV workloads). Keys use the same
`k{id:020}` format as pre-population, so populated keys and read keys
match.

For MinSlot reads with `min_slot = auto`: the client's
`write_watermark` (set by pre-population) is carried → the follower
must have applied up to this slot to serve locally; otherwise
redirects to leader. This tests the real MinSlot path including
follower-lag fallback. For `min_slot = 0`: no watermark — any follower
can serve at any staleness (pure local-serve path, no fallback).

### Benchmark Phases

Phase 1 — single test, verify results are reasonable:

- Pre-populate 200K keys, then run a single read-only bench at
  moderate concurrency (e.g. 16T/4C) for each read mode.
- Sanity-check the latency:
  - Linearizable with valid lease: barrier ~0, latency ≈ engine get
    + gRPC RTT.
  - Linearizable with expired lease: barrier ≈ one heartbeat RTT.
  - MinSlot with `min_slot = 0`: latency ≈ engine get + gRPC RTT
    (no barrier, local serve).
  - MinSlot with `min_slot = <write watermark>`: same as above if the
    follower has caught up; redirects to leader if not.
- Correctness errors must be 0.

Phase 2 — scale test, find max TPS per read mode:

- Sweep threads (1T → 64T) and connections (1C → 8C) for each read
  mode, mirroring the write benchmark methodology.
- Record throughput, avg/p50/p99 latency, error count, correctness
  errors.
- Compare Linearizable vs MinSlot max TPS — expect MinSlot to scale
  higher (no barrier, no leader serialization) once R26 (follower read
  distribution) lands; today both target the leader, so the gap may be
  small until R26.

### Bench Harness Changes

- Add `read_mode: ReadMode` to `BenchConfig` (default
  `Linearizable`).
- Add `min_slot_policy` to `BenchConfig`: `Auto` (carry client write
  watermark, the production default), `Zero` (always 0, max
  staleness), or `Fixed(u64)`.
- Add `pre_populate: Option<u64>` to `BenchConfig` (default 200,000):
  number of keys to write before measurement.
- Add `verify_bytes: usize` to `BenchConfig` (default 8): random
  spot-check bytes per `Found` read.
- Add deterministic value generation: `byte_at(key_id, offset)` via
  `splitmix64` (or `DefaultHasher`) in `workload.rs`, replacing the
  current `vec![b'v'; value_size]` fill for read benches. Pre-population
  writes use the same formula.
- Add pre-population phase to `run_bench`: before warmup, sequentially
  write `[0, pre_populate)` keys; retry on `NotLeader`; record
  `pre_pop_ms` and `pre_pop_errors` in the report.
- Wire `read_mode` and `min_slot` through the runner's `OpKind::Read`
  dispatch (currently hardcoded at `runner.rs:413`).
- Read workloads draw keys from `[0, pre_populate)` instead of
  `[0, key_space)`.
- Add spot-check verification on `GetOutcome::Found`: pick R random
  offsets, compare `byte_at(key_id, offset)` with the result; record
  `correctness_errors`.
- Add CLI flags: `--read-mode {linearizable|minslot}`,
  `--min-slot {auto|zero|<n>}`, `--pre-populate <count>` (default
  200000), `--verify-bytes <n>` (default 8).

### New Report Fields

- `correctness_errors` — count of reads where spot-check bytes didn't
  match the expected formula. Should be 0 in a correct system.
- `not_found` — count of reads returning `NotFound` (pre-population
  gaps or key selection outside populated range). Should be near-0 if
  pre-population succeeded.
- `pre_pop_ms` — duration of the pre-population phase.
- `pre_pop_errors` — write failures during pre-population.

**Concept change**: none — purely a benchmark/observability extension.
No server-side changes.

**Priority**: Medium — read throughput is unmeasured; needed to
validate R19 (read metrics), R26 (follower read distribution), and
R27 (ReadIndex batching). The write benchmark already proved the
harness pattern; this extends it to reads.

**Complexity**: Medium — bench harness config + runner dispatch +
pre-population phase + deterministic value generation + spot-check
verification + CLI flags. No consensus or engine changes.

**Dependencies**: R19 (read metrics) — ideally land first so the
benchmark can report per-mode latency, lease vs ReadIndex path
counts, and forward/fallback counters alongside raw TPS. Can proceed
without R19 but with less diagnostic depth.

**Files**: `crowkv-console/cli/src/bench/runner.rs` (BenchConfig —
read_mode, min_slot_policy, pre_populate, verify_bytes; runner
dispatch — wire read_mode/min_slot into `OpKind::Read`; pre-population
phase; spot-check verification), `crowkv-console/cli/src/bench/
workload.rs` (deterministic `byte_at(key_id, offset)` value
generation; read key range `[0, pre_populate)`), `crowkv-console/cli/
src/commands/bench.rs` (CLI flags — `--read-mode`, `--min-slot`,
`--pre-populate`, `--verify-bytes`), `crowkv-console/cli/src/bench/
report.rs` (new fields: correctness_errors, not_found, pre_pop_ms,
pre_pop_errors).

**Acceptance**:
- `bench --workload read --read-mode linearizable` runs against a
  pre-populated 200K key space and reports throughput + latency.
- `bench --workload read --read-mode minslot --min-slot auto` runs
  with the client's write watermark carried as min_slot.
- `--pre-populate` and `--verify-bytes` flags configurable; defaults
  200000 and 8.
- Phase 1 sanity check: linearizable lease-path latency ≈ engine get
  + gRPC RTT; MinSlot `min_slot=0` latency ≈ same (no barrier);
  correctness_errors = 0.
- Phase 2 results recorded in
  [`read-flow-analysis.md`](../working/read-flow-analysis.md) as a
  Benchmark Results section, mirroring the write-flow format (tables
  for scaling + config impact, conclusions).
- Linearizable vs MinSlot max TPS comparison documented.
