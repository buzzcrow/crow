<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R11: GUI internal state display

**Problem**: The GUI already queries health and internal state from the
backend, but does not surface metrics (operation counts, latency, WAL stats)
in the UI. Operators cannot see what a Store or Group is doing in real time.

**Approach**:
- Extend the existing health/internal-state query infrastructure to carry
  metrics from R8 (op counts, latency p50/p99, WAL flush lag, election count)
  in the response.
- Display metrics in the Inspector panel — per Store and per Group — with
  real-time refresh (5–10 s polling window).
- Show recent operation counts (puts, gets, deletes, scans) and key latency
  indicators.
- Keep it lightweight: no historical charts in v1, just current snapshot
  values that update on refresh.

**Dependencies**: R8 (metrics module) for the underlying counters and
histograms.

**Priority**: Medium — improves operational visibility once metrics exist.

**Complexity**: Medium — extend API response, add UI components to Inspector,
wire up polling.

**Files**: `crowkv-console/web/src/` (API handlers), `crowkv-console/web/ui/src/`
(Inspector component), `crowkv-console/shared/src/` (shared types).

**Acceptance**: Select a Store or Group in the UI, see real-time metrics
(op count, latency, WAL stats) in the Inspector, values update every 5–10 s.
