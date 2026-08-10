<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROW Documentation Index

One-line pointer to every doc and section. Read this first; open the listed
doc only when a task touches a topic in its row.

## Top-Level Docs

| Doc | When to read |
| --- | --- |
| `doc/design/kv/design-crow-kv.md` | Root KV design document: what CROW is, why key choices were made, architecture overview, data model, read modes, consensus/storage/lifecycle/client interaction, module decomposition, crate layout, concurrency model. Read first for any design or architecture question. |
| `doc/user-manual/user-guide.md` | User guide: three interfaces (Web UI, CLI, REST API), quick start (bootstrap a 3-node cluster), KV operations, cluster management (health, add/remove replicas, replace nodes), rolling upgrade, emergency procedures, backup, and full CLI + REST API reference. Run `python3 doc/user-manual/build_html.py` to generate `user-guide.html` with tabbed CLI/curl examples. |

## Backlog (`doc/backlog/`)

| Doc | When to read |
| --- | --- |
| `doc/backlog/backlog.md` | Forward-looking implementation backlog index with priority/complexity classification and brief intros. Read before picking up lib/crow-tree/crow-kv follow-up work. Each entry links to its detail doc. |
| `doc/backlog/R**-<component>-<topic>.md` | Per-requirement detailed analysis (problem, approach, files, acceptance). `<component>` is the owning crate/area (`kv`, `tree`, `console`, `client`, `server`). Open only the matched `R**` file; delete it after the requirement is implemented and merged. |

## Working & Flow-Analysis Docs

Long-lived task backlogs and per-path flow analyses. Plan files live under
`doc/working/`; flow analyses live under `doc/design/kv/`.

| Doc | When to read |
| --- | --- |
| `doc/working/plan-test.md` | Unfinished test task backlog with checkboxes. Read when picking the next test to implement. |
| `doc/design/kv/kv-read-flow-analysis.md` | KV point-read (get) flow trace, benchmark results, and open issues. Read when working on read-path performance. |
| `doc/design/kv/kv-scan-flow-analysis.md` | KV scan (range read) flow trace, benchmark results, and open issues. Read when working on scan-path performance. |
| `doc/design/kv/kv-write-flow-analysis.md` | KV write path trace and optimization opportunities. Read when working on write-path performance. |

## Project Files (repo root)

| File | When to read |
| --- | --- |
| `AGENTS.md` | Always — project overview + dispatch table for AI agents |
| `CONTRIBUTING.md` | Before opening a PR — setup, conventions, process |
| `CHANGELOG.md` | When releasing or checking what changed between versions |
| `SECURITY.md` | When reporting or handling a security vulnerability |
| `CODE_OF_CONDUCT.md` | Community behavior guidelines |

## `doc/design/kv/design-crow-kv.md` Sections

| § | Topic |
| --- | --- |
| 1 | Overview |
| 2 | Non-Goals (Design Envelope) |
| 3 | Key Design Decisions |
| 4 | Architecture Overview |
| 5 | Data Model |
| 6 | Read Modes |
| 7 | Consensus |
| 8 | Storage and Durability |
| 9 | Cluster Lifecycle |
| 10 | Client Interaction |
| 11 | Module Decomposition |
| 12 | Crate Layout |
| 13 | Concurrency Model |
| 14 | Components |
| 15 | Performance Targets |
| 16 | Observability |
| 17 | Testing |
| 18 | References |

## Sub-Designs (`doc/design/{kv,tree,console,protocol,diskdb}/`)

### KV — Consensus

| Doc | Read when working on |
| --- | --- |
| `doc/design/kv/design-crow-kv-leader-election.md` | Term/ballot bridge, election protocol, new-leader bulk Phase 1, heartbeats, leader lease, ReadIndex, step-down. |
| `doc/design/kv/design-crow-kv-slot.md` | Parallel slot pipelining (§1–§14): sliding window, gap detection/repair, safe-slot, per-key resolved-slot, follower-side apply and catch-up via ChosenNotice + FetchGap (§9A), correctness analysis, linearizability proof. Concurrent sparse slot list (§15–§22): `SlotList<T>`, chunk layout, trim/GC, reclamation. Server-side proposal coalescing (§23): timer-driven micro-batcher, dedup tag threading, drain threshold, config, correctness, benchmark results. |
| `doc/design/kv/design-crow-kv-rpc.md` | Wire protocol design: classic Paxos message surface, LearnerStream bidi stream (why dedicated stream, flow control, parallelism), PxService, version compatibility, Paxos error model (§7). Cluster discovery is HTTP, not gRPC. |
| `doc/design/kv/design-crow-kv-reconfiguration.md` | Direct per-node mutation model, member add/remove, leader transfer, `membership_epoch` fence, safety argument, design history. Applies to all groups including system group (group 0). |
| `doc/design/kv/design-crow-kv-state-machine.md` | Storage plug-in: per-key slot tracking, apply semantics, snapshot, compaction, compare, engine impls. |

### KV — Storage

| Doc | Read when working on |
| --- | --- |
| `doc/design/kv/design-crow-kv-wal.md` | Write-ahead log: multi-disk segments, backend-neutral durable flush, ack contract, replay/restore/recovery, GC, disk loss. |
| `doc/design/kv/design-crow-kv-server.md` | `crow-kv-server` binary: KV engine selection, startup ordering (§2.2: `node-config.json` auto-restore + group 0 reconciliation), concurrency model, HTTP management API (axum, §2.4: system group endpoints `/system/init`, `/topology/finalize`, `/topology/ready`), group lifecycle, shutdown, port pool. |

### Tree — Storage Engine

| Doc | Read when working on |
| --- | --- |
| `doc/design/tree/design-crow-tree.md` | crow-tree overview: goals/non-goals, architecture, `KVEngine`/`EngineView` abstraction, out-of-order apply + two-GC model, FFI boundary, sub-doc map, decision log (D1-D19). Read first for storage-engine work. |
| `doc/design/tree/design-crow-tree-engine.md` | crow-tree in-memory engine: slot cell, pages/delta records, write path (apply→delta→consolidate→split/merge), versioned root, lock-free epoch reclamation, read path, concurrency invariants; `buffer` memory-ownership model (owned/borrowed, SBO, zero-copy pipelines); io_uring async FFI bridge (reactor, `ct_future`, fast/slow path); Rust-side `KVEngine` async trait (`KVFuture<T>`, §4). |
| `doc/design/tree/design-crow-tree-storage.md` | crow-tree durable storage: `PageStore` backends (`TextPageStore` debug, `BlockPageStore` O_DIRECT), zero-copy slotted frame format, buffer pool (frame arena, CLOCK eviction, epoch-safe reuse), internal-WAL decision, snapshot pipeline + recovery + export/import, mapping table (PID indirection, segment recycling), GC watermarks + consensus-WAL GC coupling, new-member install. |

### Console — Operations / UI

| Doc | Read when working on |
| --- | --- |
| `doc/design/console/design-crow-console.md` | `crow-console` design: shared core crate, web (Axum + React) and CLI (`clap`) frontends, two-hierarchy API (physical `/api/racks`,`/api/nodes` vs. logical `/api/stores`), monitor task, SSH lifecycle, Swagger UI hosting, CLI design rules, persistent cluster config / system group (§4.3: two-phase bootstrap, topology KV schema, three-way fallback, divergence reconciliation). |
| `doc/design/console/design-crow-console-ui.md` | Web UI design (v1 lean rewrite): 3-pane shell, two hierarchy views, slim React Flow canvas, inspector (Details/Activity), embedded Swagger, KV Operator center panel (§6.1: store/group selector, paginated scan, inline CRUD, demo inject/delete), minimal embedding contract. |

### KV — Cross-Cutting

| Doc | Read when working on |
| --- | --- |
| `doc/design/kv/design-crow-kv-test.md` | Test strategy and layer-by-layer test guide: architecture stack, test binary map, cross-cutting coverage rules (placement, KV-op correctness, cluster verification, leader change & reconfig), per-layer scope, crow-tree C++ test layers. Per-layer coverage checklists in `doc/working/plan-test.md`; benchmark design in `doc/design/kv/kv-write-flow-analysis.md`. Read when designing tests or deciding where a test belongs. |
| `doc/design/kv/design-crow-kv-observability.md` | Metrics module design: five metric types (Counter, Gauge, Bandwidth, LatencyHistogram, LatencySummary), registry lifecycle, naming convention, instrumentation points, system metrics collector, log file format, in-memory snapshot access, FFI boundary. Read when working on metrics or observability. |

### protocol — Shared Key Encoding & Wire Types

| Doc | Read when working on |
| --- | --- |
| `doc/design/protocol/design-crow-key.md` | Cross-component binary key encoding for crow-kv. Read when changing a key layout or a key/proto message. |

### diskdb — Disk-Block Allocator

| Doc | Read when working on |
| --- | --- |
| `doc/design/diskdb/design-crow-diskdb.md` | diskdb root design: what diskdb is (lightweight, stateless disk-block allocator on CROW KV), key design decisions (group 0 sysdata, disk-group→paxos-group bind table, journal-is-source-of-truth, no CAS), architecture, group-0 sysdata schema, zone journal + crash recovery, allocation algorithm, state machines, crate layout, implementation split (R70–R77). Read first for any diskdb design or architecture question. |

## How AI Should Use This Index

1. Match the task description to a row above.
2. Open only the matched doc. For docs with sections, jump to the listed §
   via grep instead of reading top-to-bottom.
3. If unsure between two docs, prefer the most specific one
   (`doc/design/{kv,tree,console}/design-crow-*.md` over `doc/design/kv/design-crow-kv.md`).
4. If the task spans multiple sub-designs, open `doc/design/kv/design-crow-kv.md` first to learn
   how they interact, then drill into specifics.
5. After the task: if the index row is now wrong (renamed file, materially
   changed scope), update this file in the same commit.
