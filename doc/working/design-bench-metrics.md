<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# R13 — Unify bench client stats with the metrics library

## Problem

The benchmark client and the client library maintain their own
statistical infrastructure, separate from `crow-common`'s metrics
primitives. They depend on the external `hdrhistogram` crate and on
hand-rolled `AtomicU64` counters, while `crow-common` already ships
`Counter`, `LatencyHistogram`, `LatencySummary`, `Bandwidth`, and a
`MetricsRegistry`. R12 extracted those primitives into `crow-common`;
R13 finishes the unification by switching the bench/client onto them
and removing `hdrhistogram`.

`hdrhistogram::Histogram<u64>` is used in **three** places, not just
the bench runner the backlog entry names:

- `crowkv-console/cli/src/bench/report.rs` — `OpStats.histogram`
  (per-worker final-report histograms; `auto(true)`; merged via
  `add(&other)`).
- `crowkv-console/cli/src/bench/runner.rs` — `CumulativeLatency`
  (metrics-flusher cumulative histograms; merged via
  `add(snap.put.clone())`).
- `crowkv-client/src/metrics.rs` — `WindowLatency` /
  `WindowLatencySnapshot` (client-library per-op window histograms;
  drained via `mem::replace`; `WindowLatencySnapshot` is a **public**
  type whose fields are `hdrhistogram::Histogram<u64>`).

`crowkv-cli` depends on `crowkv-client`, and `WindowLatencySnapshot`
exposes `hdrhistogram` types in its public API. So removing
`hdrhistogram` from `crowkv-cli/Cargo.toml` (an explicit R13 acceptance
criterion) requires `crowkv-client` to drop it too — otherwise the
dependency is pulled in transitively through the public type.

`WorkerCounters` (`runner.rs:50`) is eight hand-rolled `AtomicU64`
fields bumped per op. R13 acceptance also calls for replacing these
"manual atomics" with `crow-common`'s `Counter`.

### Why the existing `LatencyHistogram` is not enough

`crow-common::metrics::LatencyHistogram` (`histogram.rs`) uses 12 fixed
buckets (1µs, 10µs, …, 1s, ∞). `percentile()` returns the bucket upper
bound, so any latency in `[500µs, 1ms)` reports as `1ms`. The bench
needs p50/p90/p99/p999/max at ≥3 significant digits (the precision
`hdrhistogram` at `sig_digits=3` provides). The fixed-bucket design is
intentional for the server hot path (zero allocation, no locks) and
must stay; R13 adds a **second** histogram for high-precision
reporting.

## Proposed approach

### 1. `PreciseHistogram` in `crow-common`

New module `crow-common/rust/src/metrics/precise.rs`, re-exported from
`metrics/mod.rs`. An HDR-style logarithmic histogram offering ≥3
significant digits, matching `hdrhistogram::Histogram<u64>`'s precision
at `sig_digits=3`.

**Algorithm** — standard HDR layout:

- `sub_bucket_count = 2^ceil(log2(10^sig_digits))`. For `sig_digits=3`
  → `1024`. Within each power-of-2 magnitude the first `sub_bucket_count`
  values are linear; relative error ≤ `1/sub_bucket_count ≈ 0.098%`
  (3 sig digits).
- Counts array indexed by `bucket_index * sub_bucket_count +
  sub_index`, where `bucket_index = floor(log2(value)) -
  floor(log2(sub_bucket_count))` (clamped ≥0) and `sub_index` is the
  high bits of `value` within that magnitude.
- Pre-allocated range covers `0..=2^32` µs (~71 min) — far beyond any
  bench latency — so **no auto-resize logic** is needed. Values above
  the range clamp to the top bucket. `auto(true)` is accepted and
  ignored (a no-op) since the range already covers everything.
- Memory: `22 buckets * 1024 * 8B ≈ 180 KB` per histogram; five
  histograms ≈ 900 KB. Negligible for a bench client.

**Concurrency** — `&mut self` methods (`record`, `add`, `reset`). All
three call sites already have exclusive access: `WindowLatency` is
behind a `Mutex`, `OpStats` is per-worker owned, `CumulativeLatency` is
owned by the flusher task. A lock-free impl would add significant
complexity (atomic bucket arrays, CAS resize) for no benefit here. The
server hot-path `LatencyHistogram` remains the lock-free option;
`PreciseHistogram` is the simpler, precise, single-owner option.

**API** — mirrors the subset of `hdrhistogram::Histogram<u64>` the three
call sites use, so the port is mechanical:

- `PreciseHistogram::new(sig_digits: u8) -> Self` (panics only on
  `sig_digits == 0` or `> 5`, like hdrhistogram).
- `record(value: u64)` — clamps to `[1, max_trackable]`.
- `auto(_enabled: bool)` — no-op (range pre-covers all bench values).
- `add(&mut self, other: &Self)` — merge counts (used by `OpStats::merge`
  and `CumulativeLatency::merge`).
- `Clone` — for `CumulativeLatency::merge` (`add(snap.put.clone())`).
- `is_empty() -> bool`, `len() -> u64`, `min() -> u64`, `max() -> u64`,
  `mean() -> f64`, `value_at_quantile(q: f64) -> u64` (q in `[0,1]`).
- `reset(&mut self)` — clear all counts (not strictly required;
  `drain_window` uses `mem::replace`, but provided for symmetry).

### 2. `crowkv-client` port

- Add `crow-common` dependency to `crowkv-client/Cargo.toml`.
- Replace `hdrhistogram::Histogram<u64>` with
  `crow_common::metrics::PreciseHistogram` in `WindowLatency` and
  `WindowLatencySnapshot`.
- `drain_window` keeps `mem::replace` (works with any owned type).
- `flush_latencies` uses `.is_empty()`, `.len()`, `.mean()`,
  `.value_at_quantile()`, `.max()` — all provided by `PreciseHistogram`.
- Remove `hdrhistogram` from `crowkv-client/Cargo.toml`.

### 3. Bench client port

- `report.rs`: `OpStats.histogram: PreciseHistogram`;
  `percentiles_from_histogram(h: &PreciseHistogram)`. The `.auto(true)`
  call becomes a no-op but is kept to minimize diff churn (or dropped).
- `runner.rs`: `CumulativeLatency` uses `PreciseHistogram`; `merge` uses
  `add(other.clone())`.
- `runner.rs`: `WorkerCounters` — replace each `AtomicU64` field with a
  `crow_common::metrics::Counter`. Add a `new()` constructor (no
  `Default` derive, since `Counter` needs a `MetricName`). Read
  cumulative totals via `.snapshot().total` (progress snapshotter) and
  window deltas via `.flush().count` (metrics flusher), which lets the
  flusher drop its manual `prev_*` delta bookkeeping.
- Remove `hdrhistogram` from `crowkv-console/cli/Cargo.toml`.

### 4. Out of scope

- The C++ `LatencyHistogram` (`crow-common/cpp/.../latency_histogram.h`)
  is the server hot-path fixed-bucket type and is unchanged. R13 is
  Rust-only; the C++ side has no bench client.
- The `MetricsRegistry` is not used by the bench (the bench does not
  flush via the registry's periodic runner; it has its own
  `spawn_metrics_flusher`). R13 only reuses the primitive types
  (`Counter`, `PreciseHistogram`).

## Alternatives considered

- **Lock-free `PreciseHistogram`** — rejected. All call sites have
  exclusive access (`Mutex`-guarded or single-owner). A lock-free HDR
  impl needs atomic bucket arrays and CAS-on-resize, adding ~150 lines
  and subtle correctness risk for zero benefit at the call sites.
- **Reuse `hdrhistogram` inside `crow-common` as a wrapper** — rejected.
  R13's goal is to *eliminate* the external dependency and unify on
  project-owned primitives. Wrapping keeps the dep and the duplication.
- **Pre-allocate a small range + implement true auto-resize** —
  rejected. Auto-resize (redistribute counts across new magnitudes on
  overflow) is the trickiest part of HDR. Pre-allocating to `2^32` µs
  (~71 min) covers every realistic bench latency at 180 KB/histogram,
  making `auto(true)` a no-op and removing the resize code entirely.
- **Port `WorkerCounters` to raw `crow_common` atomics instead of
  `Counter`** — rejected. R13 acceptance explicitly names `Counter`.
  `Counter` adds an unused `window` field per counter, but the metrics
  flusher can use `Counter::flush()` for window deltas, deleting its
  manual `prev_*` bookkeeping — a net simplification that justifies the
  type choice.

## Acceptance test plan

- `PreciseHistogram` unit tests in `precise.rs`:
  - Known distribution (e.g. 80×1µs + 20×10ms) → p50 in 1µs bucket, p99
    in 10ms bucket, matching the existing `LatencyHistogram` test shape
    but at full precision.
  - `value_at_quantile` within `0.1%` of true quantile for a uniform
    distribution over `[1, 1_000_000]` (3-sig-digit guarantee).
  - `add` / `clone` round-trip: merge two histograms, percentiles equal
    a single histogram recorded with the union of values.
  - `record` clamps values above `max_trackable` to the top bucket.
  - `is_empty` / `len` / `min` / `max` / `mean` correctness.
- `crowkv-client` tests: `drain_window` + `flush_latencies` produce the
  same percentile columns as before (existing client metrics tests, if
  any, pass unchanged).
- Bench tests (`pixi run test-cli`): existing bench report tests pass
  with `PreciseHistogram`-backed `OpStats` / `CumulativeLatency`.
- `cargo tree -p crowkv-cli` shows no `hdrhistogram` anywhere.
- `cargo tree -p crowkv-client` shows no `hdrhistogram`.
- `pixi run cargo clippy --all-targets -- -D warnings` clean.
- `pixi run cargo fmt --all -- --check` clean.

## Files

- `crow-common/rust/src/metrics/precise.rs` — new.
- `crow-common/rust/src/metrics/mod.rs` — re-export `PreciseHistogram`.
- `crowkv-client/Cargo.toml` — add `crow-common`, drop `hdrhistogram`.
- `crowkv-client/src/metrics.rs` — `WindowLatency` /
  `WindowLatencySnapshot` use `PreciseHistogram`.
- `crowkv-console/cli/Cargo.toml` — add `crow-common`, drop
  `hdrhistogram`.
- `crowkv-console/cli/src/bench/report.rs` — `OpStats`,
  `percentiles_from_histogram`.
- `crowkv-console/cli/src/bench/runner.rs` — `CumulativeLatency`,
  `WorkerCounters`, metrics-flusher delta bookkeeping.
