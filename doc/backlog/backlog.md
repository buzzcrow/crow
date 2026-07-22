<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# New Requirements — Backlog & Analysis

Forward-looking implementation items. Each item is classified by priority,
complexity, and dependency. Before implementation, follow the
[Implementation Process](#implementation-process) below.

---

## Item Index

### Medium Priority

**Complexity — Medium:**
- **[R2](R2-persistent-config.md)** — Persistent node config — Area: crowkv-server — Per-node server config
  is not persisted; a restart relies on the console to re-push topology, making
  standalone startup non-deterministic.
- **[R12](R12-crow-common.md)** — Crow Common shared project — Area: workspace — Extract a
  standalone `crow-common` project with a Rust crate and a C++ static
  library. Move reusable utilities (metrics core, logging wrapper, CRC32C,
  time helpers, operation report) out of `crowkv`/`crowtree` so future
  storage-system components can share them without re-implementing.
- **[R11](R11-gui-state.md)** — GUI internal state display — Area: web UI — Surface internal
  metrics (from R8) in the GUI via existing health/internal-state query
  infrastructure. Show recent operation counts and metrics per Store/Group
  with real-time refresh (5–10 s window).
- **[R13](R13-bench-metrics.md)** — Unify bench client stats with metrics library — Area: console CLI
  / metrics — Benchmark client-side statistics (`OpStats`, `WorkerCounters`
  in `bench/runner.rs`) currently use a hand-rolled `hdrhistogram` + manual
  `AtomicU64` counters instead of crowkv's own `MetricsRegistry` /
  `LatencyHistogram` / `Counter` classes. After R12 extracts metrics into
  `crow-common`, the bench client should reuse the same metrics primitives
  for consistency and to eliminate duplicate statistical infrastructure.
- **[R16](R16-overlap-fsync.md)** — Overlap local WAL fsync with remote RPC fan-out — Area:
  consensus / WAL — The leader's local `on_accept` awaits `fdatasync`
  before returning `PxAcceptReply::Accepted`, putting the leader's disk
  fsync on the critical path *before* remote RPCs begin. Overlapping the
  local WAL persist with the remote accept RPCs would hide fsync latency
  behind network round-trips. **Concept change**: weakens the W6 ack
  contract (persist-before-reply) for the local replica — the proposer
  would need to track local persist completion separately from quorum.
  Gate behind a feature flag; test under crash-recovery scenarios.
- **[R17](R17-async-apply.md)** — Async engine apply after quorum — Area: consensus / engine —
  `learn_chosen` (decode payload + `KVEngine::apply`) runs on the
  proposer critical path before `ProposeResult::Chosen` is returned to
  the client. Returning `Chosen` immediately after quorum confirmation
  and applying asynchronously would remove engine apply latency from
  the write path. **Concept change**: the client receives "chosen"
  before the local engine has applied the value — read-your-writes
  semantics break unless a read barrier or apply-fence is added. Gate
  behind a feature flag; test read-after-write consistency.
- **[R27](R27-readindex-batch.md)** — ReadIndex batching for linearizable reads — Area: consensus
  — Each linearizable read that falls back to ReadIndex triggers a
  separate heartbeat round. Batch pending ReadIndex barriers into a
  single heartbeat round so one quorum ack resolves all pending reads.
  Depends on R19 for validation (path counter + barrier latency).
  Documented as "not yet implemented" in `../design/design-leader-election.md`
  §7.2. See G9 in
  [`read-flow-analysis.md`](../working/read-flow-analysis.md).
- **[R28](R28-read-bench.md)** — Read path benchmark — Area: bench / metrics — The write path has
  a benchmark harness with recorded results; the read path does not.
  The existing bench runner hardcodes `ReadMode::Linearizable` with
  `min_slot = None` and has no pre-population step. Extend the harness
  to support both read modes (Linearizable + MinSlot), min_slot
  policy, deterministic value generation (per-byte hash from
  `(key_id, offset)`), key-space pre-population (200K keys), and
  random spot-check correctness verification (8 random bytes per
  read). Two-phase run: (1) single test to verify latency is
  reasonable (lease path ~0 barrier, MinSlot local serve,
  correctness_errors = 0), (2) scale sweep to find max TPS per read
  mode. Depends on R19 for diagnostic depth.
- **[R29](R29-lagging-follower-e2e.md)** — Lagging-follower e2e for MinSlot fallback — Area: client
  test — R26's `AnyReplica` follower-read distribution includes a
  `NotLeader`-hint fallback when a chosen follower hasn't applied
  `min_slot`, but the existing 2-node e2e cluster can't trigger it
  (the follower applies on accept before the leader reaches quorum).
  Add a 3-node cluster with a non-voting learner whose
  `contiguous_applied` stays 0, giving deterministic end-to-end
  coverage of: distributed read → lagging follower → NotLeader redirect
  → leader retry → read succeeds → `read_endpoint_fallback` increments.
  Test-only; depends on R26.

### Low Priority

**Complexity — Low (placeholder):**
- **[R5](R5-rdma-alloc.md)** — RDMA-pinned allocation — Blocked by: RDMA backend — Area: crowtree
  engine — `buffer::allocate` seam is designed for RDMA-pinned memory but no
  RDMA backend exists yet; placeholder only.

**Complexity — Medium:**
- **[R3](R3-zero-copy-ffi.md)** — Zero-copy FFI write path — Area: crowtree FFI — `ct_apply_put` copies
  key+value into an internal buffer; for large values this memcpy is avoidable
  via a direct-write alloc handle.
- **[R4](R4-bounded-mempool.md)** — Bounded memory pool — Area: crowtree engine — `buffer::allocate` uses
  unbounded `std::malloc`; a burst of large writes can spike RSS without
  backpressure.

**Complexity — High:**
- **[R6](R6-cross-thread-guard.md)** — Cross-thread EpochManager::Guard — Area: crowtree engine —
  `EpochManager::Guard` is thread-bound, forcing copies in async read handoff,
  snapshot consistency, and stale-root GC scenarios.

---

## Implementation Process

Each item follows the lifecycle defined in the
[`/implement-requirement` workflow](../../.devin/workflows/implement-requirement.md):
understand → design → plan → implement → merge design → cleanup.

After the PR is merged, all obsolete working docs (design draft, plan doc)
must be deleted — see the workflow's Post-merge cleanup section.
