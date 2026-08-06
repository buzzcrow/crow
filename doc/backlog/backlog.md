<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# New Requirements — Backlog & Analysis

Forward-looking implementation items. Each item is classified by priority,
complexity, and dependency. Before implementation, follow the
[Implementation Process](#implementation-process) below.

---

## Item Index

**Next R number: R52** — Bump this line in the same commit when adding a new item.

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
- **[R39](R39-kv-read-endpoint-policy.md)** — Least-conn / latency read-endpoint policy — Area:
  read path / client — R26's `AnyReplica` is round-robin (blind
  rotation); a slow replica drags p99 for 1/N of MinSlot reads. New
  `LeastConnections` (per-endpoint in-flight) and `Latency` (per-endpoint
  RTT EWMA) policies route by actual capacity. Medium complexity;
  client-local state, no server change.
- **[R48](R48-scan-lazy-l0-cursor.md)** — Lazy limit-bounded L1 leaf
  resolver — Area: scan / crow-tree engine —
  `resolve_chain_sorted` (`crow-tree.cpp:65`) rebuilds each touched
  Bw-tree page's full entry set into a `std::map` per scan
  (O(entries-per-leaf), not O(limit)); 64B packs ~640 entries/leaf vs
  ~58 for 1KiB, producing the 3.8x 1KiB anomaly. Per-step metrics
  confirmed `l1_resolve` is 99.5% of the 64B scan cost; the original
  L0-snapshot premise is refuted (L0 is empty by measurement time).
  Fix: lazy per-page resolver (O(limit)) + flat merge of sorted base +
  deltas (no `std::map`). Medium complexity. The L0 copy issue is
  covered separately by R50.
- **[R50](R50-epoch-protected-memtable.md)** — Epoch-protected lock-free
  MemTable — Area: scan / get / crow-tree engine —
  `MemTable::snapshot()` deep-copies every live L0 entry (key + full
  cell payload) on every scan, and `get()` takes `mu_` + copies on
  every L0 hit. Root cause: L0 is the only reader-visible structure
  not integrated into the engine's epoch-based reclamation (EBR)
  scheme — L1 pages are epoch-protected (lock-free, zero-copy reads),
  L0 cells live in a `btree_map` under a mutex and are freed
  immediately on erase, forcing readers to snapshot-copy for safety.
  R50 replaces the `btree_map` with a concurrent skip list whose
  nodes are epoch-protected, so readers (get/scan) iterate L0
  lock-free under their existing epoch guard with zero copy, and the
  Flusher erases entries via epoch-deferred reclamation (same
  mechanism as `retire_page()`). Scan L0 cost drops from O(N_l0) copy
  to O(log N + limit) traversal; `get()` L0 hit drops from mutex +
  copy to lock-free + zero-copy borrow. Closes the known gap flagged
  at `crow-tree.h:81`. High complexity (~800–1300 lines: concurrent
  skip list + MemTable rewrite + scan/get path changes); reference:
  RocksDB's `InlineSkipList`.
- **[R51](R51-scan-s3-pagination-drop-stream.md)** — S3-style scan
  pagination + server byte budget, drop ScanStream — Area: scan / RPC —
  The `ScanStream` server-streaming RPC is "fake streaming" (server
  materializes the full result, then slices into chunks; client
  reassembles into one `Vec`) existing solely to bypass gRPC's 4 MiB
  unary cap. The wire protocol already supports S3-style pagination
  (`start_after` + `truncated` + `limit`), and the unary `scan` path
  already implements the pagination loop. What's missing is a
  server-side byte budget so every unary response is provably bounded
  regardless of value sizes — each KV has a different length, so
  `limit × avg_size` cannot be estimated; the budget is measured
  incrementally in the engine merge loop. Oversized-single-entry
  policy: always return at least one entry even if it alone exceeds
  the budget (so the client makes progress), with a warning log
  identifying the oversized key. The 4 MiB cap is tonic's *default*,
  not hard — configurable via `max_decoding_message_size` (unused
  today; `max_message_size` in the sample config is never read).
  After R32 (custom Rust RPC) lands, the byte budget stays in both
  roles (hard-stop safety cap + large-value warning) — the cap is a
  safety boundary against injection / runaway values, not just a gRPC
  workaround; R32 only decouples its value from gRPC's 4 MiB default,
  making it an independent operator knob. Pagination is
  transport-independent. Medium complexity. Orthogonal to R48/R50
  (scan cost/copy, not response size).

### Low Priority

**Complexity — Low (placeholder):**
- **[R5](R5-rdma-alloc.md)** — RDMA-pinned allocation — Blocked by: RDMA backend — Area: crowtree
  engine — `buffer::allocate` seam is designed for RDMA-pinned memory but no
  RDMA backend exists yet; placeholder only.

**Complexity — Medium:**
- **[R4](R4-bounded-mempool.md)** — Bounded memory pool — Area: crowtree engine — `buffer::allocate` uses
  unbounded `std::malloc`; a burst of large writes can spike RSS without
  backpressure.

---

## Implementation Process

Each item follows the lifecycle defined in the
[`/implement-requirement` workflow](../../.devin/workflows/implement-requirement.md):
understand → design → plan → implement → merge design → cleanup.

After the PR is merged, all obsolete working docs (design draft, plan doc)
must be deleted — see the workflow's Post-merge cleanup section.
