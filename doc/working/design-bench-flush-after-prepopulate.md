<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# R47 Design — Bench flush-after-prepopulate flag

## Problem

The R46 scan perf baseline uncovered a 1KiB anomaly: 1KiB values scan
3.2x faster than 64B values (666 vs 206 scans/s) despite returning 16x
more data per scan. Root cause identified by code reading:
`MemTable::snapshot()` (`lib/crow-tree/src/memtable.cpp`) copies all
N_l0 entries into a vector on every scan call — O(N_l0) per scan, not
O(limit). With `memtable_flush_bytes = 4 MiB`, 64B values (~104B/entry)
leave ~60k unflushed entries after 100k pre-pop; 1KiB values
(~1064B/entry) leave only ~4k. The 60k vs 4k snapshot cost difference
explains the 3.2x throughput gap.

This is a code-reading hypothesis, not yet verified empirically. The
bench has no way to force a flush after pre-population, so the L0 size
at scan time depends on value size (inadvertently). R47 adds a flag
that drains L0 before the measurement window, verifying the hypothesis
and enabling a clean L1-only scan baseline.

## Current behavior

- `run_bench` (`app/crow-cli/src/bench/runner.rs:286`) pre-populates
  `[0, count)` keys via sequential `client.put` calls, then immediately
  opens the measurement window. No drain is forced between pre-pop and
  measurement.
- The per-group maintenance loop
  (`lib/crow-kv/src/cluster/group_maintenance.rs:108`) calls
  `KVEngine::flush()` every `maintenance_tick_ms`, which forces a
  freeze + drain of all memtables up to `contiguous_slot`. But the
  measurement window may open before the first post-pre-pop tick, and
  the timing is not deterministic — so L0 size at scan time varies with
  pre-pop write rate and value size.
- `KVEngine::flush()` (`lib/crow-kv/src/kv/kv_engine.rs:108`, default
  no-op; `CrowTreeEngine` overrides via
  `crow_tree_ffi::Crowtree::flush` → `ct_flush` → `Crowtree::flush()`
  at `lib/crow-tree/src/crow-tree.cpp:998`) forces `maybe_freeze_active
  (force=true)` and drains every frozen table into L1 up to
  `contiguous_slot`. Cheap when L0 is empty.
- No flush trigger exists on any RPC/HTTP surface today. The mgmt API
  (`app/crow-kv-server/src/mgmt_api.rs`) has admin endpoints
  (`step-down`, `join`, `remotes`) but no flush. The kv.proto gRPC
  surface has only `Put`/`Get`/`Delete`/`BatchWrite`/`Scan`.

## Proposed approach

Add a `--flush-after-prepopulate` flag to `crow-cli bench run` that
drains L0 on every node after the pre-population phase, before the
measurement window opens. The drain is triggered via a new management
API endpoint (admin operation, alongside `step-down`/`join`); the bench
fixture already holds every node's mgmt URL.

### Management API endpoint

- `POST /stores/{sid}/groups/{gid}/flush` — calls
  `group.local_replica().learner.engine().flush()` on the local
  replica's engine. Synchronous (flush is in-memory, no disk I/O —
  `Crowtree::flush()` never touches the page store). Returns
  `FlushResult { store_id, group_id, accepted: true }`. 404 if the
  store/group is not found on this node. No request body required.

Rationale for mgmt API over a gRPC Flush RPC: flush is an admin/debug
operation, not a hot-path KV op. The mgmt API is the natural admin
surface and the bench fixture already speaks HTTP to it. Adding a Flush
RPC to `kv.proto` would touch the hot-path wire contract for a
bench-only need.

### Bench wiring

- `BenchConfig` gains `flush_after_prepopulate: bool` and
  `flush_mgmt_urls: Vec<String>` (the per-node mgmt URLs).
- `run_bench`, right after the pre-population block and before
  `started_at`/`measure_start`: if `flush_after_prepopulate`, sleep
  briefly (let lagging followers apply pending learns up to the
  leader's `contiguous_slot`), then POST flush to each mgmt URL via
  `ServerClient::flush`. Log the total flush elapsed time. Failures
  are logged as warnings but do not abort the bench (a failed flush
  degrades the measurement's cleanliness, not its correctness).
- `RunArgs` gains `--flush-after-prepopulate` (bool, default false).
- `bench_benchmark` sets `cfg.flush_after_prepopulate` from the flag
  and `cfg.flush_mgmt_urls = fixture.node_mgmt_urls().to_vec()`.

### Catch-up wait

After the last pre-pop `put` returns, the leader has applied all writes
(`contiguous_slot = count`). Followers may still be applying the tail
of the learn stream. `engine.flush()` only drains entries with
`slot <= contiguous_slot`, so flushing a lagging follower before it
catches up leaves the tail in L0. A conservative fixed wait
(500ms) before the flush lets followers converge for the 100k-entry
pre-pop scale; the flush is then deterministic across nodes. The wait
is only taken when the flag is set, so the no-flag baseline is
unchanged.

## Alternatives considered

- **Sleep only, no explicit flush.** Add a fixed sleep after pre-pop
  and rely on the maintenance loop's periodic `flush()` to drain L0.
  Rejected: non-deterministic (depends on `maintenance_tick_ms` phase
  relative to pre-pop end), does not guarantee a drain, and cannot
  verify the hypothesis cleanly.
- **gRPC Flush RPC in kv.proto.** Rejected: touches the hot-path wire
  contract for a bench/admin need; the mgmt API is the right surface.
- **Bench-internal engine handle.** Rejected: the bench is a separate
  process driving a deployed cluster over the network; it has no
  in-process engine handle.

## Acceptance test plan

- With `--flush-after-prepopulate`, the `valuesize_64B` and
  `valuesize_1KiB` scan configs (limit=1000, 100k pre-pop) produce
  comparable throughput — the 3.2x gap closes, confirming the
  `MemTable::snapshot()` O(N_l0) hypothesis. Verified by running the
  flushed variant added to `tools/bench-scan-regression.sh`.
- Without the flag, the existing baseline numbers are reproducible
  (no behavior change when the flag is absent) — verified by re-running
  the unchanged sentinel configs.
- `POST /stores/{sid}/groups/{gid}/flush` returns 200 with
  `accepted: true` on a node hosting the group; 404 on a node that
  does not. Covered by a management-API integration test.
- `cargo fmt --check`, `cargo clippy -- -D warnings`, and the
  relevant test suites (`test-core`, `test-server`, `test-cli`) pass.
