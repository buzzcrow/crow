<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# New Requirements — Backlog & Analysis

Forward-looking implementation items. Each item is classified by priority,
complexity, and dependency. Before implementation, follow the
[Implementation Process](#implementation-process) below.

---

## Item Index

**Next R number: R43** — Bump this line in the same commit when adding a new item.

### High Priority

**Complexity — Low-medium:**
- **[R41](R41-dedup-window.md)** — Bounded per-client dedup window (fix
  single-entry false-positive) — Area: consensus / idempotency —
  `PxLearner` keeps only the single latest `(seq, slot)` per client
  ("latest wins"), but `design.md` promises a ≥64-request retention
  window. Since `propose` checks dedup unconditionally (not only on
  retries) and slots can be chosen out of order, a concurrent
  lower-`seq` request from the same `client_id` can false-positive
  against a higher-`seq` request that committed first, silently
  dropping its payload while reporting success. Reachable by the
  documented concurrent-pipelining client pattern (shared
  `Arc<CrowkvClient>` on the default `ids=None` write path, one
  `client_id`, many in-flight requests). Correctness bug, not a
  tuning item.

### Medium Priority

**Complexity — Medium:**
- **[R11](R11-gui-state.md)** — GUI internal state display — Area: web UI — Surface internal
  metrics (from R8) in the GUI via existing health/internal-state query
  infrastructure. Show recent operation counts and metrics per Store/Group
  with real-time refresh (5–10 s window).
- **[R32](R32-custom-rust-rpc.md)** — Custom Rust RPC library to replace gRPC on the hot path — Area:
  RPC / consensus — gRPC (tonic + h2) serializes concurrent writers on a
  connection-level userspace lock (HPACK table, frame buffer,
  flow-control windows); measured cost is ~17% at 2T:1C, zero at
  1T:1C. A custom `[len][req_id][protobuf]`-over-raw-TCP transport
  removes the userspace funnel — the kernel TCP lock is the only
  serialization point. **Deferred until** read throughput is the
  primary constraint AND the h2 lock is profiled as the hot spot; until
  then write-path (R16a/R17) and disk-I/O work take precedence.
  High complexity (2–4K lines: framing, pool, reconnect, timeout,
  cancellation, backpressure, TLS). Scope is the internal
  replica-to-replica path only; management API stays on Axum/HTTP.
  Reference implementations: protosocket (Momento), Volo (CloudWeGo),
  Cap'n Proto RPC.
- **[R33](R33-crow-tree-rename.md)** — Extract crow-tree to separate repo and rename — Area:
  workspace — Move `crowtree/` into its own git repository (preserving
  history), wire `crowkv` to depend on `crow-tree-ffi` as an external
  dependency, and rename the crate/namespace/macros from `crowtree` to
  `crow-tree` / `crow::tree` / `CROW_TREE_*`. Establishes the `crowkv` →
  `crow-tree` dependency boundary analogous to `crowkv` → `crow-common`.
  Most naturally done after R12.
- **[R35](R35-apply-fence.md)** — Apply fence for async engine apply (enable R17 by default) — Area:
  read path / learner — R17 moves `learn_chosen` (engine apply) off the
  write critical path via `spawn_learn_chosen`, but ships default-off
  because it breaks the **Linearizable** read mode's read-your-writes
  (MinSlot already gates on `contiguous_applied` and is unaffected).
  Gate the Linearizable barrier on the learner's `contiguous_applied`
  frontier (already an `AtomicU64`) so a read awaits the write's slot
  before serving. Then flip the `async_engine_apply` internal default to
  true (no CLI flag / public API — internal config only) and carry it
  across group rebuild. Enable by default if no regression. Unblocks
  the biggest remaining write-path latency win. Medium complexity,
  confined to the Linearizable read path + learner + `mgmt_api`;
  composes with R27 ReadIndex batching.
- **[R36](R36-proposal-coalescing.md)** — Server-side proposal coalescing — Area: consensus / write path —
  A bounded micro-batcher at the `PxGroup::propose` entry merges
  concurrent single-key proposes into one multi-key Paxos proposal
  (one slot, one quorum RPC, one fsync), amortizing the per-proposal
  fixed cost. Must preserve `(client_id, seq)` dedup ordering; tunable
  coalesce window (`coalesce_window_us = 0` disables). Directly attacks
  the throughput saturation ceiling. Medium-high complexity; touches
  the admission gate and propose entry.
- **[R37](R37-scan-start-after-pushdown.md)** — Scan `start_after` push-down into the C++ engine — Area:
  read path / scan — `ct_scan_async` takes only `prefix` + `limit`; when
  `start_after` is non-empty the Rust wrapper over-fetches the whole
  prefix range (`fetch_limit = 0`) and filters in Rust. Deep pagination
  transfers and decodes entries the client discards. Extend the C++
  scan API with a `start_after` cursor + lower-bound seek so the engine
  starts at the cursor and applies the limit natively. Medium
  complexity; touches the C++ scan API, FFI binding, and Rust wrapper.
- **[R38](R38-scan-value-zero-copy.md)** — Scan value zero-copy (mirror R6 for scan) — Area:
  read path / scan — The get path is zero-copy after R6
  (`PinnedValue::into_bytes` via `Bytes::from_owner`); scan still copies
  per-entry values out of the packed result buffer into owned `Vec<u8>`.
  A `PinnedScanEntry` / `Bytes::from_owner` path for scan values would
  eliminate the per-entry copy, mirroring R6. Medium-high complexity;
  the `KVEngine::scan` trait signature changes from `Vec<u8>` to `Bytes`.
- **[R39](R39-read-endpoint-policy.md)** — Least-conn / latency read-endpoint policy — Area:
  read path / client — R26's `AnyReplica` is round-robin (blind
  rotation); a slow replica drags p99 for 1/N of MinSlot reads. New
  `LeastConnections` (per-endpoint in-flight) and `Latency` (per-endpoint
  RTT EWMA) policies route by actual capacity. Medium complexity;
  client-local state, no server change.
- **[R40](R40-crowkv-config.md)** — Unified `CrowKVConfig` (merge all sub-configs, JSON file loading) — Area:
  config / workspace — Today configuration is scattered across
  `WalConfig`, `PxElectionConfig`, `PaxosConfig`, `ServerConfig`, and
  three `PxGroup` internal bool flags (`wal_early_ack`,
  `async_engine_apply`, `force_classic`), wired through 4
  `create_group_with_wal` call sites each passing ~14 individual params
  pulled from `KvStoreRegistry` fields. Merge all into one
  `CrowKVConfig` with `serde` derives, loaded from a JSON config file
  (CLI args override individual fields). Eliminates the scattered
  params, the per-flag `mgmt_api` rebuild-carry blocks, and the
  per-flag setter/getter pattern on `PxGroup`. Prerequisite for T1's
  `wal_early_ack` default flip and R35's `async_engine_apply` carry.
  Medium complexity; touches `config.rs`, `PxGroup`, `KvStoreRegistry`,
  `create_group_with_wal`, `mgmt_api` rebuild, `main.rs` CLI wiring.

### Low Priority

**Complexity — Low (placeholder):**
- **[R5](R5-rdma-alloc.md)** — RDMA-pinned allocation — Blocked by: RDMA backend — Area: crowtree
  engine — `buffer::allocate` seam is designed for RDMA-pinned memory but no
  RDMA backend exists yet; placeholder only.

**Complexity — Medium:**
- **[R4](R4-bounded-mempool.md)** — Bounded memory pool — Area: crowtree engine — `buffer::allocate` uses
  unbounded `std::malloc`; a burst of large writes can spike RSS without
  backpressure.

**Complexity — Low:**
- **[R42](R42-forward-target-redundant-lookup.md)** — Drop redundant
  group lookup in read-path `NotLeader` redirect — Area: read path —
  `PxKvStore::resolve_read_point`'s three `NotLeader` sites call
  `self.forward_target_for(group.group_id())`, which re-derives the same
  `Arc<PxGroup>` via a `DashMap` lookup + clone even though the function
  already holds `group: &Arc<PxGroup>`. Fires on every linearizable
  non-leader redirect and every `MinSlot` staleness fallback. Replace
  with `group.leader_endpoint()` directly; no behavior change.

---

## Implementation Process

Each item follows the lifecycle defined in the
[`/implement-requirement` workflow](../../.devin/workflows/implement-requirement.md):
understand → design → plan → implement → merge design → cleanup.

After the PR is merged, all obsolete working docs (design draft, plan doc)
must be deleted — see the workflow's Post-merge cleanup section.
