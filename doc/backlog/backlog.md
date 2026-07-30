<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# New Requirements — Backlog & Analysis

Forward-looking implementation items. Each item is classified by priority,
complexity, and dependency. Before implementation, follow the
[Implementation Process](#implementation-process) below.

---

## Item Index

**Next R number: R34** — Bump this line in the same commit when adding a new item.

### Medium Priority

**Complexity — Medium:**
- **[R2](R2-persistent-config.md)** — Persistent node config — Area: crowkv-server — Per-node server config
  is not persisted; a restart relies on the console to re-push topology, making
  standalone startup non-deterministic.
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
- **[R16a](R16a-concurrent-fanout.md)** — Concurrent local + remote fan-out — Area:
  consensus / WAL — `run_accept_phase` and `run_prepare_phase` `await`
  the local `on_accept`/`on_prepare` (acceptor CAS + WAL append +
  `fdatasync`) *before* issuing any remote RPC, putting the leader's
  local fsync on the critical path ahead of the network RTT. Folding
  the local call into the same `join_all` as the remote RPCs overlaps
  the local fsync with the network round-trip. **No contract change** —
  W6 only forbids the local replica replying `Accepted` before persist,
  and `on_accept` still does not return until `wal.append` resolves; only
  the *issue order* of remote RPCs changes. Pure win, no feature flag.
- **[R16b](R16b-early-ack.md)** — Early ack before local WAL persist — Area: consensus / WAL —
  Return `Chosen` as soon as *remote* quorum is met, without waiting
  for the local WAL flush; track local persist separately. Builds on
  R16a's concurrent join. **Concept change**: weakens the W6 ack
  contract (persist-before-reply) for the local replica — the proposer
  would need to track local persist completion separately from quorum.
  Gate behind a feature flag; test under crash-recovery scenarios.
  Depends on R16a.
- **[R17](R17-async-apply.md)** — Async engine apply after quorum — Area: consensus / engine —
  `learn_chosen` (decode payload + `KVEngine::apply`) runs on the
  proposer critical path before `ProposeResult::Chosen` is returned to
  the client. Returning `Chosen` immediately after quorum confirmation
  and applying asynchronously would remove engine apply latency from
  the write path. **Concept change**: the client receives "chosen"
  before the local engine has applied the value — read-your-writes
  semantics break unless a read barrier or apply-fence is added. Gate
  behind a feature flag; test read-after-write consistency.
- **[R30](R30-zero-copy-engine-apply.md)** — Zero-copy engine apply — Area: consensus / engine / FFI —
  R3 delivered handle-based FFI (`ct_alloc` / `ct_apply_put_owned`), but the
  consensus layer still copies: Paxos deserialization materializes `Vec<u8>`
  keys/values, then `ct_apply_batch_slices` copies again at the C++ boundary.
  This item wires the full path so data flows from Paxos payload to crowtree
  frame with zero intermediate copies: deserialize directly into handles,
  extend the C API for batch handles, and add a `KVEngine` apply-handles
  variant. Depends on R3 (completed).
- **[R31](R31-write-regression-investigation.md)** — Investigate 50K→29K write throughput regression — Area:
  consensus / bench — The 2026-07-21 sweep reported ~50K ops/s peak at
  64T:8C (Intel Ryzen 9 5950X). The 2026-07-24 sweep measured ~29K at
  the same config on the same Intel platform (direct re-run: 29,319),
  a ~42% drop. **Step 1 done (2026-07-29)**: a macOS M5 Pro retest hits
  ~48K at the same config, within 4% of the original Intel 50K claim —
  the 50K→29K difference is largely a platform effect, not a code
  regression. The Intel same-platform bisect remains open but is lower
  priority; R16a/R17/R30 can proceed on M5 Pro without waiting.
- **[R32](R32-custom-rust-rpc.md)** — Custom Rust RPC library to replace gRPC on the hot path — Area:
  RPC / consensus — gRPC (tonic + h2) serializes concurrent writers on a
  connection-level userspace lock (HPACK table, frame buffer,
  flow-control windows); measured cost is ~17% at 2T:1C, zero at
  1T:1C. A custom `[len][req_id][protobuf]`-over-raw-TCP transport
  removes the userspace funnel — the kernel TCP lock is the only
  serialization point. **Deferred until** read throughput is the
  primary constraint AND the h2 lock is profiled as the hot spot; until
  then write-path (R16a/R17/R30) and disk-I/O work take precedence.
  High complexity (2–4K lines: framing, pool, reconnect, timeout,
  cancellation, backpressure, TLS). Scope is the internal
  replica-to-replica path only; management API stays on Axum/HTTP.
  Reference implementations: protosocket (Momento), Volo (CloudWeGo),
  Cap'n Proto RPC.

### Low Priority

**Complexity — Low (placeholder):**
- **[R5](R5-rdma-alloc.md)** — RDMA-pinned allocation — Blocked by: RDMA backend — Area: crowtree
  engine — `buffer::allocate` seam is designed for RDMA-pinned memory but no
  RDMA backend exists yet; placeholder only.

**Complexity — Medium:**
- **[R33](R33-crow-tree-rename.md)** — Rename crowtree → crow-tree — Area: workspace — Rename the
  `crowtree` directory/crate to `crow-tree` and renamespace C++ from
  `crowtree::` to `crow::tree`, plus `CROWTREE_*` macro prefixes to
  `CROW_TREE_*`. Cosmetic / naming consistency with the `crow-*` convention
  from R12; no functional change. Independent of R12 (can run before or
  after, most naturally after).
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
