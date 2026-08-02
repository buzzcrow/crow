<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# New Requirements — Backlog & Analysis

Forward-looking implementation items. Each item is classified by priority,
complexity, and dependency. Before implementation, follow the
[Implementation Process](#implementation-process) below.

---

## Item Index

**Next R number: R45** — Bump this line in the same commit when adding a new item.

### High Priority

*(none currently)*

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
- **[R43](R43-write-path-fanout-hardening.md)** — Write-path fan-out hardening — Area:
  consensus / write path — Six enhancements from the write-flow review,
  all in the prepare/accept fan-out and `PxLearnerStream`: quorum
  short-circuit (fold replies via `FuturesUnordered`, return on quorum
  + local reply instead of `join_all` over all peers — per-proposal
  latency becomes k-th-fastest, not slowest peer), RPC deadline on
  accept/heartbeat oneshots (a hung-but-connected peer currently
  stalls all writes indefinitely), write-path phase latency metrics
  (propose-e2e / prepare / accept / first-quorum-RPC / apply),
  backoff jitter, a heartbeat priority/reserved lane on the shared
  LearnerStream queue, and a reply-fold helper extraction that
  de-risks the short-circuit rewrite. Medium complexity; the
  short-circuit must preserve W6 and late TermStale/Epoch side
  effects.
- **[R44](R44-read-path-hardening.md)** — Read-path hardening — Area:
  read path — Eight enhancements from the read-flow review, all small
  and outside the items already tracked (R37/R38/R39/R32/R42): scan
  forward-fail path drops the leader hint (get sets it, scan doesn't);
  `decode_scan_with_start_after` swallows FFI errors (corruption reads
  as empty `ok` result); client retry matches `"not leader"` by string
  instead of a structured error code; client ignores topology refresh
  failures and retries against stale endpoints; ReadIndex heartbeat
  round runs peer catch-up replay inline (lagging follower inflates
  linearizable read p99 during recovery); C++ `scan_async` restarts
  the whole scan on any cold leaf (no cursor resume); client copies
  response values (`to_vec` per get / per scan entry) despite prost
  `Bytes`; no per-mode scan latency split or over-fetch counters.
  Low-medium complexity; kv_service / crowtree_engine / client are
  mechanical, bounded catch-up needs care, scan cursor composes with
  R37.

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
