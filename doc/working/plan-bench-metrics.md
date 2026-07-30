<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# R13 — Implementation plan

Design: `doc/working/design-bench-metrics.md`.

## Task breakdown

- [ ] T1: `PreciseHistogram` skeleton + HDR indexing math
  - `crow-common/rust/src/metrics/precise.rs` new file.
  - Constants: `SUB_BUCKET_COUNT = 1024` (3 sig digits),
    `MAX_TRACKABLE = 2^32`, `NUM_BUCKETS = 22`.
  - `counts: Vec<u64>` of size `NUM_BUCKETS * SUB_BUCKET_COUNT`.
  - `record(value)`: compute `(bucket, sub)` index, increment.
  - Helpers: `bucket_index(value)`, `sub_index(value, bucket)`,
    `value_for_index(bucket, sub)` (lower bound of the sub-bucket).
- [ ] T2: `PreciseHistogram` query API
  - `is_empty`, `len`, `min`, `max`, `mean`, `value_at_quantile(q)`.
  - `value_at_quantile`: walk counts accumulating, find target rank,
    return `value_for_index` of the containing sub-bucket.
  - `add(&mut self, other: &Self)`, `Clone`, `reset`.
  - `new(sig_digits)`, `auto(_)` no-op.
- [ ] T3: `PreciseHistogram` unit tests
  - Known-distribution p50/p99 (mirror `LatencyHistogram` tests).
  - 3-sig-digit precision: uniform `[1, 1_000_000]`, p50/p90/p99 within
    0.1% of true.
  - `add`/`clone` merge correctness.
  - Clamp-above-max, `is_empty`/`len`/`min`/`max`/`mean`.
- [ ] T4: Re-export from `metrics/mod.rs`
  - `pub mod precise;` + `pub use precise::PreciseHistogram;`.
- [ ] T5: `crowkv-client` port
  - `crowkv-client/Cargo.toml`: add `crow-common`, remove `hdrhistogram`.
  - `crowkv-client/src/metrics.rs`: `WindowLatency` /
    `WindowLatencySnapshot` fields → `PreciseHistogram`; `drain_window`,
    `flush_latencies`, `Default` updated.
- [ ] T6: Bench `report.rs` port
  - `OpStats.histogram: PreciseHistogram`; `new()` uses
    `PreciseHistogram::new(3)`; `record`/`merge` updated.
  - `percentiles_from_histogram(h: &PreciseHistogram)`.
- [ ] T7: Bench `runner.rs` port — `CumulativeLatency`
  - Fields → `PreciseHistogram`; `merge` uses `add(other.clone())`;
    percentile extraction via `percentiles_from_histogram`.
- [ ] T8: Bench `runner.rs` port — `WorkerCounters`
  - Fields `AtomicU64` → `Counter`; add `new()` constructor;
    `record(kind, ok)` uses `inc()`; `total_ops`/`total_errors` read
    `.snapshot().total`.
  - Progress snapshotter: read `.snapshot().total`.
  - Metrics flusher: use `Counter::flush().count` for window deltas,
    drop `prev_*` bookkeeping.
- [ ] T9: `crowkv-console/cli/Cargo.toml` — add `crow-common`, remove
  `hdrhistogram`.
- [ ] T10: Verify — `cargo tree -p crowkv-cli` and `-p crowkv-client`
  show no `hdrhistogram`; `pixi run test-cli` passes; clippy + fmt clean.

## File list

- `crow-common/rust/src/metrics/precise.rs` (new)
- `crow-common/rust/src/metrics/mod.rs`
- `crowkv-client/Cargo.toml`
- `crowkv-client/src/metrics.rs`
- `crowkv-console/cli/Cargo.toml`
- `crowkv-console/cli/src/bench/report.rs`
- `crowkv-console/cli/src/bench/runner.rs`

## Dependency ordering

T1→T2→T3 (histogram core, self-contained) → T4 (export) → T5 (client,
unblocks T6/T7 which reference `PreciseHistogram`) → T6, T7, T8 (bench,
can be done together) → T9 (dep swap) → T10 (verify).

## Test checklist

- [ ] `pixi run cargo test -p crow-common` (PreciseHistogram unit tests)
- [ ] `pixi run test-cli` (bench report + runner tests)
- [ ] `pixi run cargo clippy --all-targets -- -D warnings`
- [ ] `pixi run cargo fmt --all -- --check`
- [ ] `cargo tree -p crowkv-cli` / `-p crowkv-client` — no `hdrhistogram`
- [ ] `pixi run test-suite` (full, Step 6)
