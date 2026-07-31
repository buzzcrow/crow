<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# New Requirements — Backlog & Analysis

Forward-looking implementation items. Each item is classified by priority,
complexity, and dependency. Before implementation, follow the
[Implementation Process](#implementation-process) below.

---

## Item Index

**Next R number: R35** — Bump this line in the same commit when adding a new item.

### Medium Priority

**Complexity — Medium:**
- **[R11](R11-gui-state.md)** — GUI internal state display — Area: web UI — Surface internal
  metrics (from R8) in the GUI via existing health/internal-state query
  infrastructure. Show recent operation counts and metrics per Store/Group
  with real-time refresh (5–10 s window).
- **[R30](R30-zero-copy-engine-apply.md)** — Zero-copy engine apply — Area: consensus / engine / FFI —
  R3 delivered handle-based FFI (`ct_alloc` / `ct_apply_put_owned`), but the
  consensus layer still copies: Paxos deserialization materializes `Vec<u8>`
  keys/values, then `ct_apply_batch_slices` copies again at the C++ boundary.
  This item wires the full path so data flows from Paxos payload to crowtree
  frame with zero intermediate copies: deserialize directly into handles,
  extend the C API for batch handles, and add a `KVEngine` apply-handles
  variant. Depends on R3 (completed).
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
- **[R33](R33-crow-tree-rename.md)** — Extract crow-tree to separate repo and rename — Area:
  workspace — Move `crowtree/` into its own git repository (preserving
  history), wire `crowkv` to depend on `crow-tree-ffi` as an external
  dependency, and rename the crate/namespace/macros from `crowtree` to
  `crow-tree` / `crow::tree` / `CROW_TREE_*`. Establishes the `crowkv` →
  `crow-tree` dependency boundary analogous to `crowkv` → `crow-common`.
  Most naturally done after R12.

### Low Priority

**Complexity — Low (placeholder):**
- **[R5](R5-rdma-alloc.md)** — RDMA-pinned allocation — Blocked by: RDMA backend — Area: crowtree
  engine — `buffer::allocate` seam is designed for RDMA-pinned memory but no
  RDMA backend exists yet; placeholder only.

**Complexity — Medium:**
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
