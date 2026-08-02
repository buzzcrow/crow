<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R36: Server-side proposal coalescing

**Problem**: Each client `PUT`/`DELETE` is its own Paxos proposal — one
slot, one WAL record, one quorum RPC round, one fsync batch entry. The
per-proposal fixed cost (quorum RPC + fsync) is paid per key, capping
throughput at the saturation ceiling (~29K Intel / ~48K M5 Pro at
MI=64). The `Batch` payload format already supports multiple ops per
slot and `kv_batch_write` exposes it to clients, but there is no
server-side coalescer: concurrent single-key proposes each take a
distinct slot. Under many concurrent writers the leader issues N
parallel quorum rounds where one round carrying N keys would suffice.

WAL batch aggregation (already done) amortizes fsync across concurrent
proposals — K proposals → K WAL records → 1 fsync. But it does **not**
amortize the quorum RPC round, the per-accept CPU on followers (CAS +
WAL encode + queue), the leader's quorum check, or the learner apply
overhead. Those are paid per proposal. Since the MI=16+ convergence
ceiling is not caused by fsync throughput (fsync is already batched),
the ceiling is set by one of: quorum RPC rate, per-accept follower CPU,
or leader-side per-proposal CPU. Coalescing K keys into one proposal
turns K quorum rounds into 1, attacking the first two directly.

**Target**:
- A server-side coalescer that merges concurrent single-key proposes
  into one multi-key Paxos proposal, amortizing the per-proposal fixed
  cost (quorum RPC + per-accept CPU + learner apply) across many keys.
- Measurable throughput gain over the current saturation ceiling under
  high-concurrency write workloads, with the actual gain characterized
  by benchmark (the theoretical case is strong; the real number depends
  on which of quorum-RPC-rate / per-accept-CPU / per-key-CPU is the
  bottleneck, which must be profiled and measured).
- Correctness preserved: `(client_id, seq)` dedup ordering, per-key
  highest-slot-wins, and the `ProposeResult::Chosen { slot }` contract
  (one slot per batch; each coalesced client gets that slot back).
- Tunable, with a disable path (`coalesce_window_us = 0` = current
  behavior) so the latency tradeoff is opt-in.

**Acceptance**:
- Throughput: under a write-heavy single-key workload with many
  concurrent clients, throughput increases vs coalesce-off at the same
  window. The max-perf point (best `coalesce_window_us` ×
  `coalesce_max_keys` × `max_inflight`) is found by sweep and recorded
  in `write-flow-analysis.md`.
- Latency: the coalesce-window latency floor is characterized; the
  throughput/latency tradeoff curve is recorded. At low concurrency
  (1T) coalescing must not regress throughput (single writer can't fill
  the window).
- Correctness: dedup holds (retried `(client_id, seq)` returns the
  cached slot); per-key ordering holds; no regression at
  `coalesce_window_us = 0`.
- Profiling: before claiming a gain, profile the coalesce-off path to
  confirm the bottleneck is quorum RPC rate / per-accept CPU (which
  coalescing attacks) vs. per-key CPU (which it doesn't). Record the
  finding.

**Dependencies**: None new — builds on the existing `Batch` payload
format, `InflightAdmission` (R18), and the learner's per-key apply.

**Priority**: Medium — directly attacks the throughput saturation
ceiling; the largest throughput (vs latency) win available.

**Complexity**: Medium-high — touches the admission gate and the
`propose` entry; the coalescer is a new component with timing, ordering,
and backpressure semantics. Must not break dedup ordering or the
`ProposeResult::Chosen { slot }` contract.

**Files**: `crowkv/src/cluster/group.rs` (propose entry, new coalescer),
`crowkv/src/cluster/px_kv_store.rs` (payload encode path), config flags
in `PaxosConfig` / CLI, `tools/bench-write-*.sh` (coalesce sweep).
