<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R13: Unify bench client stats with metrics library

**Problem**: The benchmark client (`bench/runner.rs`) maintains its own
statistical infrastructure separate from crowkv's metrics module:

- `OpStats` uses the external `hdrhistogram` crate (`Histogram<u64>`) for
  latency distributions, plus manual `u64` counters for `ops`, `errors`,
  `not_found`.
- `WorkerCounters` uses hand-rolled `AtomicU64` for live progress
  snapshotting.

Meanwhile, crowkv's `MetricsRegistry` provides `LatencyHistogram`,
`Counter`, `LatencySummary`, and `Bandwidth` — the same primitives the
server uses for its metrics log. The bench client should reuse these
classes so there is one statistical infrastructure across the project.

**Two problems blocking immediate adoption**:

1. **Dependency boundary**: `crowkv-cli` does not depend on `crowkv`
   today (only `crowkv-client` and `crowkv-console-shared`/`crowkv-web`).
   Adding `crowkv` as a dependency just for metrics would pull in the
   entire consensus/cluster/WAL stack. This is resolved by R12 (Crow
   Common shared project), which extracts the metrics core into a
   standalone `crow-common` crate with no crowkv-specific coupling.

2. **Histogram precision**: crowkv's `LatencyHistogram` uses 12 fixed
   buckets (1µs, 10µs, 100µs, 500µs, 1ms, 5ms, 10ms, 50ms, 100ms, 500ms,
   1s, ∞). Percentile queries return the bucket upper bound, not the
   actual value — e.g. any latency between 500µs and 1ms reports as 1ms.
   The bench client needs high-precision percentiles (p90, p99, p999, max)
   for meaningful latency analysis; the current `hdrhistogram` crate
   provides 3 significant digits with auto-resizing. The fixed-bucket
   design is intentional for the server hot path (zero allocation, no
   locks, cache-friendly), but it is too coarse for benchmark reporting.

**Plan**: After R12 extracts metrics into `crow-common`, implement a new
`PreciseHistogram` (or similar) in `crow-common` that offers higher
percentile precision at a slightly higher per-observe cost — e.g. an
HDR-style logarithmic bucket scheme or a lock-free variant of
`hdrhistogram`. The existing `LatencyHistogram` stays as the low-overhead
server hot-path option; the new histogram is used by the bench client and
any other consumer that needs precise tail latency. The bench client then
replaces `OpStats` / `WorkerCounters` with `crow-common` metrics
primitives, eliminating the `hdrhistogram` external dependency and
unifying the statistical infrastructure.

**Dependencies**: R12 (Crow Common shared project) — must extract metrics
core first.

**Priority**: Medium — consistency and maintainability improvement; not
blocking current benchmark work.

**Complexity**: Medium — new histogram implementation in `crow-common`,
bench client refactor to use metrics primitives, update report generation.

**Files**: `crow-common/rust/src/metrics/` (new precise histogram),
`crowkv-console/cli/src/bench/runner.rs`,
`crowkv-console/cli/src/bench/report.rs`,
`crowkv-console/cli/Cargo.toml` (replace `hdrhistogram` with
`crow-common`).

**Acceptance**:
- Bench client uses `crow-common` metrics primitives (`Counter`,
  precise histogram) instead of `hdrhistogram` and manual atomics.
- Benchmark report percentiles (p50, p90, p99, p999, max) are at least as
  precise as the previous `hdrhistogram`-based values.
- `hdrhistogram` dependency removed from `crowkv-cli/Cargo.toml`.
- Existing benchmark tests pass with the new infrastructure.
