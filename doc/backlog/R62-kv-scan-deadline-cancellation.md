<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R62: Scan — Per-Scan Deadline / Cancellation

**Problem**: there is no per-scan timeout at any layer. An unbounded
`limit=0` scan over a large keyspace runs until the transport gives up
(gRPC default deadline, if any), and the engine merge loop has no
cancellation check between pages or between leaves. A scan that hits a
cold range, a slow disk, or a pathological merge (many frozen
memtables) can hold server CPU and memory for an unbounded duration
with no way for the client or server to abort it mid-flight.

Concretely:

- **Client** (`CrowkvClient::scan`, `client.rs:785-843`): the
  pagination loop has no deadline — it pages until `!truncated` or
  `limit` reached. A transport error eventually breaks the loop, but
  only after the per-RPC timeout (if set) fires per page, so a
  multi-page scan can run for `N_pages × per_page_timeout` with no
  overall bound.
- **gRPC service** (`kv_service.rs::scan`): no per-request deadline
  propagated to the store; the handler runs to completion or until the
  gRPC channel cancels.
- **Engine** (`Crowtree::scan`, `crow-tree.cpp:1730+`): the merge loop
  (`while (true)` at `:1890`) has no cancellation check — once entered,
  it runs until the prefix/limit/byte-budget stop fires. A cold-leaf
  demand-load stall inside the loop blocks indefinitely. The async path
  (`scan_async_attempt`) retries across reactor round trips with no
  deadline.

This is a robustness/safety gap: server work for a single scan should
be bounded, and a client should be able to cancel a scan that is taking
too long without waiting for the next page boundary.

**Solution**: add a per-scan deadline (absolute timestamp) and a
cancellation check.

- **Proto** (`kv.proto`): add `uint64 deadline_ms = 11;` to
  `KvScanRequest` (absolute deadline in unix-ms; 0 = no deadline,
  preserves today's behavior). Coordinate the field number with R56
  (`end_key`) and R61 (`keys_only`) — take the next free slot after
  whichever land first.
- **Client** (`CrowkvClient::scan`): add an optional `deadline` (or
  `timeout`) parameter. Set `deadline_ms = now + timeout` on every page
  request. The pagination loop checks `now > deadline` before fetching
  the next page and returns a partial result with a `timed_out` flag
  (or an error, TBD — partial-with-flag is more useful for bulk
  consumers). The per-RPC gRPC timeout is set to `deadline - now` so a
  single page can't overrun the overall deadline.
- **gRPC service** (`kv_service.rs::scan`): forward `deadline_ms` to
  the store. The handler can check `now > deadline` before dispatching
  and return a deadline-exceeded error if the scan hasn't started yet
  (cheap guard).
- **Engine** (`crow-tree.cpp`): the merge loop checks `now_ms() >
  deadline_ms` periodically — not every entry (per-entry clock reads
  are too expensive), but once per leaf (in `refill_l1`, when walking
  to the right sibling) and once per page (at the byte-budget stop). If
  exceeded, the loop breaks early with `truncated = true` and the
  partial result is returned. The async path checks the deadline in the
  retry loop between reactor round trips.
- **FFI** (`ct_scan` / `ct_scan_async`): add `deadline_ms` parameter;
  thread through `CrowTreeEngine` / `PxKvStore`.

**Scope**: one new field per layer (proto, engine, FFI, Rust wrapper,
store, service, client) plus the periodic deadline checks in the merge
loop and pagination loop.

**Complexity**: Medium. The proto/FFI threading is mechanical
(parallel to `start_after`). The subtle part is the engine check
frequency: too frequent (per-entry) wastes clock-read cycles; too
infrequent (per-page) lets a single huge leaf overrun. Per-leaf is the
right granularity (leaves are bounded by `frame_bytes`, so per-leaf
work is bounded). The client partial-result semantics (return what we
have + `timed_out` flag vs error) is a small API decision.

**Dependencies**:
- Coordinate the `KvScanRequest` field number with R56 (`end_key`) and
  R61 (`keys_only`).
- Independent of R57/R58/R60 — they speed up the scan; R62 bounds its
  worst case regardless of speed. Complementary: a faster scan (R57/
  R58/R60) hits the deadline less often, but the deadline is the
  safety net.
- The client partial-result-with-flag shape may interact with R59
  (snapshot scan) — a `snapshot_scan` with a deadline returns a partial
  snapshot. No conflict, just a shared API pattern.

**Acceptance**:
- A scan with `deadline_ms` set returns a partial result (the entries
  fetched before the deadline) with a `timed_out` indicator, not an
  error, when the deadline fires mid-scan.
- A scan with `deadline_ms = 0` behaves identically to today (no
  deadline).
- The engine merge loop checks the deadline at least once per leaf and
  breaks early when exceeded — verifiable via a test that sets a tight
  deadline mid-scan and asserts a partial, correctly-ordered, no-gap
  result.
- The client pagination loop checks the deadline before fetching the
  next page and stops when exceeded.
- Existing scan tests and `tools/bench-scan-regression.sh` pass
  unchanged (default `deadline_ms = 0`).

**Note**: the gap lives in
`doc/design/kv/kv-scan-flow-analysis.md` Gap Analysis → Functionality →
"No scan deadline / cancellation".
