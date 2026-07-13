# CrowKV Documentation Index

One-line pointer to every doc and section. Read this first; open the listed
doc only when a task touches a topic in its row. Line counts are approximate
(grow over time); use them to gauge "do I really need to load this?".

## Top-Level Docs

| Doc | Lines | When to read |
| --- | ---: | --- |
| `requirement.md` | ~600 | Source of truth for what must be built. Any feature gap → fix here first. Design-level detail (linearizability proof, CLI tree, Web UI spec, server API) has been moved to sub-design docs; pointers remain in-place. |
| `design.md` | ~560 | Master design: cross-cutting architecture, write/read flows, module decomposition, crate layout, concurrency model (§12) with async disk I/O substrate (§12.1, merged from design-async-io.md). Read for scope-spanning questions. |
| `plan-test.md` | ~30 | Unfinished test task backlog with checkboxes. Read when picking the next test to implement. |
| `procedures.md` | ~450 | Operator-facing procedures for a standard CrowKV cluster: bootstrap, rolling upgrade, node replacement, replica add/remove, quorum-loss handling, health checks, backup, full API reference. Uses `crowkv-server` and the console HTTP API as examples. |
| `todo_code.md` | ~50 | Forward-looking code-level TODO backlog (open implementation items, blocked/deferred work with rationale). Read before picking up crowtree/crowkv follow-up work. |

## `requirement.md` Sections

| § | Topic |
| --- | --- |
| 1 | Overview |
| 2 | Non-Goals |
| 3 | Dependencies and Assumptions |
| 4 | Concepts and Terminology |
| 5 | Data Model and Client API |
| 6 | Consistency and Read Model |
| 7 | Consensus Architecture |
| 8 | Storage and Durability |
| 9 | Cluster Lifecycle |
| 10 | Client Interaction |
| 11 | Security |
| 12 | Performance and Batching |
| 13 | Operational Tooling and Observability |
| 14 | Testing Requirements |
| 15 | Components |

## `design.md` Sections

| § | Topic |
| --- | --- |
| 1 | Design Philosophy |
| 2 | Architecture Overview |
| 3 | Module Decomposition |
| 4 | Core Data Shapes |
| 5 | Write Flow |
| 6 | Read Flows |
| 7 | Cluster Bootstrap and Topology Management |
| 8 | Cross-Cutting Topics |
| 9 | Failure Mode Catalogue |
| 10 | Observability Hooks |
| 11 | Crate Layout |
| 12 | Concurrency Model |
| 13 | Open Design Questions |
| 14 | References |

## Sub-Designs (`design/design-xxx.md`)

Grouped by topic area. Open the most specific doc for the task at hand.

### Consensus

| Doc | Lines | Read when working on |
| --- | ---: | --- |
| `design/design-leader-election.md` | ~330 | Term/ballot bridge, election protocol, new-leader bulk Phase 1, heartbeats, leader lease, ReadIndex, step-down. |
| `design/design-slot.md` | ~470 | Parallel slot pipelining (§1–§14): sliding window, gap detection/repair, safe-slot, per-key resolved-slot, correctness analysis, linearizability proof. Concurrent sparse slot list (§15–§22): `SlotList<T>`, chunk layout, trim/GC, reclamation. |
| `design/design-rpc.md` | ~330 | Wire protocol: classic Paxos messages, LearnerStream bidi stream (frames, flow control, parallelism), PxService, Rust mapping, Paxos error model (§8). Cluster discovery is HTTP, not gRPC. |
| `design/design-reconfiguration.md` | ~310 | Direct per-node mutation model, member add/remove, leader transfer, `membership_epoch` fence, safety argument, design history. |
| `design/design-state-machine.md` | ~310 | Storage plug-in: per-key slot tracking, apply semantics, snapshot, compaction, compare, engine impls. |

### Storage

| Doc | Lines | Read when working on |
| --- | ---: | --- |
| `design/design-wal.md` | ~470 | Write-ahead log: multi-disk segments, backend-neutral durable flush, ack contract, replay/restore/recovery, GC, disk loss. |
| `design/design-crowtree.md` | ~240 | crowtree overview: goals/non-goals, architecture, `KVEngine`/`EngineView` abstraction, out-of-order apply + two-GC model, FFI boundary, sub-doc map, full decision log (D1-D19). Read first for storage-engine work. |
| `design/design-crowtree-engine.md` | ~635 | crowtree in-memory engine: slot cell, pages/delta records, write path (apply→delta→consolidate→split/merge), versioned root (`RootVersion`), tree-owned lock-free epoch reclamation, read path, concurrency invariants; the `buffer` memory-ownership model (owned/borrowed, SBO, zero-copy write/read pipelines); the io_uring async FFI bridge (reactor, `ct_future`, fast/slow path); Rust-side `KVEngine` async trait shape (`KVFuture<T>`, §4). |
| `design/design-crowtree-storage.md` | ~350 | crowtree durable storage: `PageStore` (file/block/RDMA), zero-copy slotted frame format, buffer pool (frame arena, CLOCK eviction, epoch-safe reuse), internal-WAL decision, snapshot pipeline + recovery + export/import, mapping table (PID indirection, segment persistence/recycling), GC watermarks + consensus-WAL GC coupling, new-member install. |

### Operations / UI

| Doc | Lines | Read when working on |
| --- | ---: | --- |
| `design/design-console.md` | ~830 | `crowkv-console` design: shared core crate, web (Axum + React) and CLI (`clap`) frontends, two-hierarchy API (physical `/api/racks`,`/api/nodes` vs. logical `/api/stores`), monitor task, SSH lifecycle, Swagger UI hosting; CLI command hierarchy (§12, moved from requirement.md). |
| `design/design-ui.md` | ~390 | Web UI design (v1 lean rewrite): 3-pane shell, two hierarchy views, slim React Flow canvas, inspector (Details/KV/Activity), embedded Swagger, minimal embedding contract; Web UI requirements spec (§13, moved from requirement.md). |
| `design/design-kv-server.md` | ~350 | `crowkv-server` binary: CLI, HTTP management API, store/group/replica wiring, topology export, lifecycle. |

### Cross-Cutting

| Doc | Lines | Read when working on |
| --- | ---: | --- |
| `design/design-test.md` | ~320 | Test strategy, layer scope definitions, high-level coverage per layer, crowtree C++ test layers, feature-dependent test gaps. Read when designing tests or deciding where a test belongs. |

## How AI Should Use This Index

1. Match the task description to a row above.
2. Open only the matched doc. For docs with sections, jump to the listed §
   via grep instead of reading top-to-bottom.
3. If unsure between two docs, prefer the most specific one
   (`design/design-xxx.md` over `design.md`).
4. If the task spans multiple sub-designs, open `design.md` first to learn
   how they interact, then drill into specifics.
5. After the task: if the index row is now wrong (renamed file, materially
   changed scope), update this file in the same commit.
