<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CrowKV Documentation Index

One-line pointer to every doc and section. Read this first; open the listed
doc only when a task touches a topic in its row.

## Top-Level Docs

| Doc | When to read |
| --- | --- |
| `design/design.md` | Root design document: what CrowKV is, why key choices were made, architecture overview, data model, read modes, consensus/storage/lifecycle/client interaction, module decomposition, crate layout, concurrency model. Read first for any design or architecture question. |
| `user-manual/user-guide.md` | User guide: three interfaces (Web UI, CLI, REST API), quick start (bootstrap a 3-node cluster), KV operations, cluster management (health, add/remove replicas, replace nodes), rolling upgrade, emergency procedures, backup, and full CLI + REST API reference. Run `python3 doc/user-manual/build_html.py` to generate `user-guide.html` with tabbed CLI/curl examples. |

## Backlog (`doc/backlog/`)

| Doc | When to read |
| --- | --- |
| `doc/backlog/backlog.md` | Forward-looking implementation backlog index with priority/complexity classification and brief intros. Read before picking up crowtree/crowkv follow-up work. Each entry links to its detail doc. |
| `doc/backlog/R**-<topic>.md` | Per-requirement detailed analysis (problem, approach, files, acceptance). Open only the matched `R**` file; delete it after the requirement is implemented and merged. |

## Working Files (`doc/working/`)

| Doc | When to read |
| --- | --- |
| `doc/working/plan-test.md` | Unfinished test task backlog with checkboxes. Read when picking the next test to implement. |
| `doc/working/read-flow-analysis.md` | Read path gap analysis and proposed metrics hierarchy. Read when working on read performance (R19/R26/R27). |
| `doc/working/write-flow-analysis.md` | Write path trace and optimization opportunities. Read when working on write-path performance. |

## Project Files (repo root)

| File | When to read |
| --- | --- |
| `AGENTS.md` | Always — project overview + dispatch table for AI agents |
| `CONTRIBUTING.md` | Before opening a PR — setup, conventions, process |
| `CHANGELOG.md` | When releasing or checking what changed between versions |
| `SECURITY.md` | When reporting or handling a security vulnerability |
| `CODE_OF_CONDUCT.md` | Community behavior guidelines |

## `design/design.md` Sections

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

## Sub-Designs (`design/design-xxx.md`)

### Consensus

| Doc | Read when working on |
| --- | --- |
| `design/design-leader-election.md` | Term/ballot bridge, election protocol, new-leader bulk Phase 1, heartbeats, leader lease, ReadIndex, step-down. |
| `design/design-slot.md` | Parallel slot pipelining (§1–§14): sliding window, gap detection/repair, safe-slot, per-key resolved-slot, correctness analysis, linearizability proof. Concurrent sparse slot list (§15–§22): `SlotList<T>`, chunk layout, trim/GC, reclamation. |
| `design/design-rpc.md` | Wire protocol design: classic Paxos message surface, LearnerStream bidi stream (why dedicated stream, flow control, parallelism), PxService, version compatibility, Paxos error model (§7). Cluster discovery is HTTP, not gRPC. |
| `design/design-reconfiguration.md` | Direct per-node mutation model, member add/remove, leader transfer, `membership_epoch` fence, safety argument, design history. Applies to all groups including system group (group 0). |
| `design/design-state-machine.md` | Storage plug-in: per-key slot tracking, apply semantics, snapshot, compaction, compare, engine impls. |

### Storage

| Doc | Read when working on |
| --- | --- |
| `design/design-wal.md` | Write-ahead log: multi-disk segments, backend-neutral durable flush, ack contract, replay/restore/recovery, GC, disk loss. |
| `design/design-crowtree.md` | crowtree overview: goals/non-goals, architecture, `KVEngine`/`EngineView` abstraction, out-of-order apply + two-GC model, FFI boundary, sub-doc map, full decision log (D1-D19). Read first for storage-engine work. |
| `design/design-crowtree-engine.md` | crowtree in-memory engine: slot cell, pages/delta records, write path (apply→delta→consolidate→split/merge), versioned root (`RootVersion`), tree-owned lock-free epoch reclamation, read path, concurrency invariants; the `buffer` memory-ownership model (owned/borrowed, SBO, zero-copy write/read pipelines); the io_uring async FFI bridge (reactor, `ct_future`, fast/slow path); Rust-side `KVEngine` async trait shape (`KVFuture<T>`, §4). |
| `design/design-crowtree-storage.md` | crowtree durable storage: `PageStore` backends (`TextPageStore` debug text files, `BlockPageStore` array-of-blocks / O_DIRECT), zero-copy slotted frame format, buffer pool (frame arena, CLOCK eviction, epoch-safe reuse), internal-WAL decision, snapshot pipeline + recovery + export/import, mapping table (PID indirection, segment persistence/recycling), GC watermarks + consensus-WAL GC coupling, new-member install. |

### Operations / UI

| Doc | Read when working on |
| --- | --- |
| `design/design-console.md` | `crowkv-console` design: shared core crate, web (Axum + React) and CLI (`clap`) frontends, two-hierarchy API (physical `/api/racks`,`/api/nodes` vs. logical `/api/stores`), monitor task, SSH lifecycle, Swagger UI hosting, CLI design rules, persistent cluster config / system group (§4.3: two-phase bootstrap, topology KV schema, three-way fallback, divergence reconciliation). |
| `design/design-ui.md` | Web UI design (v1 lean rewrite): 3-pane shell, two hierarchy views, slim React Flow canvas, inspector (Details/Activity), embedded Swagger, KV Operator center panel (§6.1: store/group selector, paginated scan, inline CRUD, demo inject/delete), minimal embedding contract. |
| `design/design-kv-server.md` | `crowkv-server` binary: KV engine selection, startup ordering (§2.2: `node-config.json` auto-restore + group 0 reconciliation), concurrency model, HTTP management API design (axum, §2.4: system group endpoints `/system/init`, `/topology/finalize`, `/topology/ready`), group lifecycle, shutdown, port pool. |

### Cross-Cutting

| Doc | Read when working on |
| --- | --- |
| `design/design-test.md` | Test strategy, layer scope definitions, coverage rules per layer, tiered strategy for Group/Store/Deployment/UI E2E layers, console mgmt API layer, crowtree C++ test layers, benchmark layer (lifecycle, storage modes, write-only design, baseline results), feature-dependent test gaps. Read when designing tests or deciding where a test belongs. |
| `design/design-observability.md` | Metrics module design: five metric types (Counter, Gauge, Bandwidth, LatencyHistogram, LatencySummary), registry lifecycle, naming convention, instrumentation points, system metrics collector, log file format, in-memory snapshot access, FFI boundary. Read when working on metrics or observability. |

## How AI Should Use This Index

1. Match the task description to a row above.
2. Open only the matched doc. For docs with sections, jump to the listed §
   via grep instead of reading top-to-bottom.
3. If unsure between two docs, prefer the most specific one
   (`design/design-xxx.md` over `design/design.md`).
4. If the task spans multiple sub-designs, open `design/design.md` first to learn
   how they interact, then drill into specifics.
5. After the task: if the index row is now wrong (renamed file, materially
   changed scope), update this file in the same commit.
