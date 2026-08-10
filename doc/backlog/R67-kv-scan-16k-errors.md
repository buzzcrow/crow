<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R67: 16 KiB Scan Errors — Maintenance-Loop Snapshot Stall Causes Election Churn on Linux

**Problem**: R53 fixed the 16 KiB scan error spike on macOS by routing
steady-state heartbeats over a dedicated gRPC `Channel` (separate TCP
connection) so a heartbeat behind N 16 KiB accepts was no longer delayed
by their cumulative flush time. Re-running the 16 KiB scan bench on
Linux (AMD Ryzen 9 5950X, 16c/32t, Ubuntu 24.04) shows the errors are
back — and worse: every run fails, not just intermittently.

The original R53 observation was "452 in one run, 0 in others"
(intermittent, macOS). On Linux now: **653-8111 errors in every run**,
with `scan_errors` reaching 2614-32444. The R53 fix (dedicated heartbeat
channel) addressed the heartbeat-blocking symptom, but a remaining —
possibly separate — scan error path is still failing at 16 KiB values.

**Reproduction** (Linux, 2026-08-09, AMD Ryzen 9 5950X, 16c/32t,
Ubuntu 24.04, 10s mem mode, 3-node cluster, 100k pre-populated keys):

Command:
```
pixi run -- cargo run --release -p crow-cli -- bench run \
    --mode mem --workload list --duration-secs 10 \
    --threads 1 --connections 1 \
    --read-mode linearizable --min-slot auto \
    --read-endpoint-policy leader \
    --scan-limit 1000 --scan-prefix "" --scan-start-after "" \
    --pre-populate 100000 --value-size 16384 \
    --key-space 100000 --verify-bytes 0 --json
```

5 consecutive runs:

| Run | scans/s | p99 us | errors | scan_errors | retries_exhausted | WAL GB | RSS GB |
|-----|--------:|-------:|-------:|------------:|------------------:|-------:|-------:|
| 1 | 6 | 10904 | 7142 | 28568 | 7142 | 12.8 | 10.5 |
| 2 | 10 | 28720 | 5318 | 21272 | 5319 | 12.6 | 9.6 |
| 3 | 3 | 2104 | 8111 | 32444 | 8112 | 13.2 | 10.2 |
| 4 | 30 | 41664 | 653 | 2614 | 653 | 10.3 | 8.9 |
| 5 | 12 | 29104 | 4207 | 16831 | 4210 | 11.7 | 8.8 |

**Observations**:
- `scan_errors` dominate (`put_errors` / `batch_write_errors` are 0) —
  the scans themselves are failing, not the pre-populate writes.
- `retries_exhausted` tracks `total_errors` closely — the client retries
  but exhausts its budget.
- WAL is 10-13 GB and RSS is 9-10 GB for 100k keys × 16 KiB = 1.6 GB of
  values — ~8x amplification (3-node replication + WAL overhead).
- A single scan returns up to 1000 keys × 16 KiB = 16 MB per response —
  likely hitting a gRPC message-size limit or causing memory pressure
  during response serialization.
- Run 4 was the "best" (653 errors, 30 scans/s) but still far from clean.
- The error rate is consistent (every run fails), not intermittent like
  the original R53 macOS observation.

**Candidate root causes (all RULED OUT by RCA — see below)**:
- gRPC/tonic max message size — RULED OUT. The scan byte budget is
  3.5 MiB per page (`ServerConfig::DEFAULT.scan_byte_budget`), well under
  tonic's 4 MiB default. Server logs show the errors are application-level
  `"not leader"`, not transport-level. `transport_error_retry` is 0.
- Memory pressure — RULED OUT. RSS is high (9-10 GB) but the errors are
  `"not leader"` from the read barrier, not OOM/allocation failures.
- Replication backpressure (residual from R53) — RULED OUT. The dedicated
  heartbeat channel is working: `transport_error_retry` is 0 and
  `leader_changes` is empty on the client. The `LearnerStream` queue is
  not the cause.
- Platform difference — PARTIALLY CONFIRMED (the symptom is
  Linux-specific) but not for the originally hypothesized TCP/gRPC
  reasons; see RCA.

## Root Cause Analysis (2026-08-09)

Reproduced on Linux (AMD Ryzen 9 5950X, 16c/32t, Ubuntu 24.04) with the
exact command above. A 6th run captured: 164 scans/s, 329 errors, 1322
`scan_errors`, `transport_error_retry: 0`, `leader_changes: []`, p999 =
4.4 s. Server logs (`bench-runs/bench-79-359839-*/workspace/N-bn0/log/`)
show every `scan_error` is a `WARN ... kv scan failed ... error="not
leader"` from `kv_service.rs::scan` → `px_kv_store.rs::kv_scan` →
`resolve_read_point` returning `ReadDecision::NotLeader` (the
linearizable read barrier lost leadership).

**The errors are leader-election churn, not scan-path failure.** During
the 10 s measurement window, group_id=1 had 6+ leader changes across the
3 nodes (terms 1→2→3→4→5→6→7→8→9). Each leadership loss returns
`NotLeader` for every in-flight scan; the client retries (up to
`max_retries = 3`) but the churn is fast enough to exhaust the budget
(`retries_exhausted` ≈ `total_errors`).

**Why the churn: the maintenance loop's `persist_snapshot()` stalls the
election driver.** The per-group maintenance loop
(`group_maintenance.rs::run_pass`) runs `engine.persist_snapshot()`
synchronously on the tokio runtime when the slot/time threshold is met.
The `s.1.g.1.ct.mem.snapshot.apply.l` metric on all 3 nodes shows
snapshot applies taking **617 ms – 2215 ms** during the 16 KiB run:

| Node | snap 1 (ms) | snap 2 (ms) | snap 3 (ms) | snap 4 (ms) | snap 5 (ms) |
|------|------------:|------------:|------------:|------------:|------------:|
| N-bn0 | 617 | 984 | 1555 | 2081 | — |
| N-bn1 | 328 | 1243 | 1769 | 1846 | 1944 |
| N-bn2 | 429 | 814 | 1801 | 1714 | 2215 |

The bench fixture uses the **test election profile**
(`election_min_ms = 300`, `election_max_ms = 600`, `lease_duration_ms =
800`, `heartbeat_interval_ms = 100`). A snapshot apply of 0.6–2.2 s
**exceeds the election timeout (300–600 ms)**, so the leader's election
driver misses its deadline, a follower times out and starts a new
election, and the leader steps down on `HigherTerm`. This is visible in
the logs: `become_precandidate` → `become_candidate` → `won quorum` →
`become_leader` cycles every 2–7 s, with `stepping down from leader ...
reason=HigherTerm(...)` at each transition.

**Why 16 KiB values trigger it but 64B does not:** the snapshot apply
cost scales with the live data size. 100k keys × 16 KiB = 1.6 GB of
values (vs 6.4 MB for 64B) makes each `persist_snapshot` serialization
~250× larger, pushing it from sub-millisecond (64B, well under the
election timeout) to 0.6–2.2 s (16 KiB, well over). The 64B scan benches
run clean because the snapshot apply finishes within one heartbeat
interval and never stalls the election driver.

**Why Linux and not macOS:** the macOS runs (M5 Pro, arm64) used the
same test election profile but the snapshot apply was fast enough on
that platform/CPU to stay under the 300 ms election floor. The Linux
x86_64 build's snapshot apply is slower per-byte (different memcpy /
page-write characteristics), pushing it over the threshold. The root
cause is the same on both platforms; macOS was just below the cliff and
Linux is just above it. This is why R53's fix (dedicated heartbeat
channel) appeared to work on macOS — it addressed a separate, smaller
heartbeat-delay symptom, but the dominant snapshot-stall cause was still
latent.

**The title is misleading.** This is not "replication backpressure
resurfaces" — it is a maintenance-loop snapshot stall causing election
churn. R53's fix remains correct for what it targeted.

**Solution (implemented 2026-08-10)**: Move all three maintenance-loop
engine calls that hold the C++ `write_mutex_` — `flush()`,
`persist_snapshot()`, and `collect_garbage()` — off the async runtime
via `tokio::task::spawn_blocking`. The maintenance loop `await`s each
blocking task; the election driver runs on a separate tokio task, so
this does not block it. Single code path — no `test-util` branching,
no fire-and-forget, no in-flight guard. The `persist_snapshot` logic is
extracted into `persist_snapshot_blocking()` to keep `run_pass` under
clippy's line limit.

**Scope** (changed files):
- `lib/crow-kv/src/cluster/group_maintenance.rs` — `flush()`,
  `persist_snapshot()`, and `collect_garbage()` wrapped in
  `spawn_blocking`; `persist_snapshot_blocking()` helper extracted.
- `lib/crow-kv/src/cluster/group.rs` — no structural changes (the
  `snapshot_in_flight` field from an earlier iteration was removed).
- `lib/crow-kv/src/paxos/learner.rs` — `PxLearner.engine` changed from
  `Box<dyn KVEngine>` to `Arc<dyn KVEngine>` (needed to clone the engine
  handle into `spawn_blocking`); added `PxLearner::engine_arc()` accessor.
- `lib/crow-kv/tests/group/maintenance_test.rs` —
  `maintenance_loop_uses_configured_tick_interval` switched from
  `current_thread` + `start_paused` to `multi_thread` with a real timer,
  since `spawn_blocking` uses real OS threads that don't respect tokio's
  virtual clock.

**Complexity**: Low. The `spawn_blocking` wraps are small; the work was
changing `PxLearner.engine` to `Arc<dyn KVEngine>` so the engine handle
can be cloned into the blocking task, and verifying correctness across
5 bench runs.

**Dependencies**: None. The 16 KiB scan path is stable; this is a
correctness/robustness issue, not a performance optimization.

**Acceptance** (all met 2026-08-10):
- The 16 KiB scan bench (`--value-size 16384`, 100k keys, 1T:1C, 10s)
  runs with **0 errors** on Linux, consistently across **5 consecutive
  runs** (28–38 scans/s, 0 scan_errors, 0 retries_exhausted).
- The root cause is identified and documented above.
- The fix is targeted at the root cause (snapshot/flush/GC blocking the
  election driver), not a workaround.
- No regression on the 64B scan bench: 3 consecutive runs at 1263–1349
  scans/s with 0 errors (matches the verified 2026-08-09 baseline).
- All 98 group tests pass; clippy + fmt clean.
- The `largeval_16k` config is part of the regression sentinel
  (`tools/bench-scan-regression.sh`) to prevent future regressions.

**Note**: R53 is marked Done — its fix (dedicated heartbeat channel) is
correct for the heartbeat-blocking symptom it targeted. R67's RCA shows
the remaining 16 KiB scan errors are a **different root cause**:
maintenance-loop `persist_snapshot()` stalls the election driver, not
replication backpressure. The original R53 observation was on macOS;
this is a Linux-specific resurfacing because Linux's snapshot apply is
slower per-byte, pushing it over the test election timeout.
