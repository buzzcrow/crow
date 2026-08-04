<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# I/O-Path Cleanup & Tuning Tasks

**Override:** This file is **persistent** — it is not deleted after a
task is complete. Only completed tasks are removed; the file itself
remains as the ongoing I/O-path (read + write) cleanup/tuning backlog.
This overrides the `/implement-requirement` workflow's cleanup step
which would normally delete `plan-<topic>.md`.

Small-scope read- and write-path tasks traced here for later
implementation. Each has a checkbox. Larger changes live in the
backlog (R35 apply fence, R36 proposal coalescing, R37 scan
`start_after` push-down, R38 scan value zero-copy, R39 read-endpoint
policy). See [`write-flow-analysis.md`](write-flow-analysis.md) §
Write-Path Enhancement Ideas and
[`read-flow-analysis.md`](read-flow-analysis.md) § Gaps and
Optimization Opportunities for the full lists and rationale.

---

## T4 — Early-ack p99 tail-mass shift investigation

The early-ack A/B (T1.5, results in
[`write-flow-analysis.md`](write-flow-analysis.md) § Early-ack A/B)
showed the expected avg/p999 wins at 48T:48C (+7.7% throughput,
−7.2% avg, −11.7% p999) but **p99 went up slightly** (+6.7%,
2,206 → 2,354 µs). Working hypothesis: the deferred
`spawn_accept_persist` shifts some tail mass from p999 into p99 by
adding a small amount of background-persist contention on the leader.
Needs confirmation before treating the early-ack flip as a clean win
on all tail percentiles.

- [ ] Reproduce the p99 uptick on a second run (rule out 1-run noise);
      if it holds, capture a per-percentile profile (p90/p95/p99/p99.9)
      at 48T:48C with early-ack on vs off.
- [ ] If the uptick is real, instrument `spawn_accept_persist`'s
      scheduling latency vs the quorum-RPC completion (does the
      background spawn contend with the next proposal's
      `on_accept_inner` CAS / `tokio::join!`?). Check whether the
      deferred persist runs on the same worker that the next accept
      lands on.
- [ ] Decide: accept the p99 shift as a net win (avg + p999 improve,
      p99 slightly worse), or bound the background persist's scheduling
      priority / pin it off the accept worker.
- [ ] After T4 is resolved, delete `tools/bench-early-ack.sh` (one-off
      A/B script, not a regression sentinel) and update the
      `write-flow-analysis.md` § Early-ack A/B reference to drop the
      script citation.

**Scope**: Small — measurement + a possible scheduling tweak. No
consensus-path change; the deferred persist is already off the
`Chosen` critical path.

**Files**: `tools/bench-early-ack.sh` (add per-percentile columns if
not already present), `lib/crow-kv/src/cluster/local_replica.rs`
(`spawn_accept_persist` scheduling, if a tweak is needed),
`doc/working/write-flow-analysis.md` (results + decision).

---
