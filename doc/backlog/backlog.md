<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# New Requirements — Backlog & Analysis

Forward-looking implementation items. Each item is classified by priority,
complexity, and dependency. Before implementation, follow the
[Implementation Process](#implementation-process) below.

---

## Item Index

**Next R number: R46** — Bump this line in the same commit when adding a new item.

### High Priority

*(none currently)*

### Medium Priority

**Complexity — Medium:**
- **[R32](R32-kv-custom-rust-rpc.md)** — Custom Rust RPC library to replace gRPC on the hot path — Area:
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
  history), wire `crow-kv` to depend on `crow-tree-ffi` as an external
  dependency, and rename the crate/namespace/macros from `crowtree` to
  `crow-tree` / `crow::tree` / `CROW_TREE_*`. Establishes the `crow-kv` →
  `crow-tree` dependency boundary analogous to `crow-kv` → `crow-common`.
  Most naturally done after R12.
- **[R38](R38-scan-value-zero-copy.md)** — Scan value zero-copy (mirror R6 for scan) — Area:
  read path / scan — The get path is zero-copy after R6
  (`PinnedValue::into_bytes` via `Bytes::from_owner`); scan still copies
  per-entry values out of the packed result buffer into owned `Vec<u8>`.
  A `PinnedScanEntry` / `Bytes::from_owner` path for scan values would
  eliminate the per-entry copy, mirroring R6. Medium-high complexity;
  the `KVEngine::scan` trait signature changes from `Vec<u8>` to `Bytes`.
- **[R39](R39-kv-read-endpoint-policy.md)** — Least-conn / latency read-endpoint policy — Area:
  read path / client — R26's `AnyReplica` is round-robin (blind
  rotation); a slow replica drags p99 for 1/N of MinSlot reads. New
  `LeastConnections` (per-endpoint in-flight) and `Latency` (per-endpoint
  RTT EWMA) policies route by actual capacity. Medium complexity;
  client-local state, no server change.
- **[R44](R44-kv-read-path-hardening.md)** — Read-path hardening — Area:
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
- **[R42](R42-kv-forward-target-redundant-lookup.md)** — Drop redundant
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
