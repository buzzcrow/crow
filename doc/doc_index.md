# CrowKV Documentation Index

One-line pointer to every doc and section. Read this first; open the listed
doc only when a task touches a topic in its row. Line counts are approximate
(grow over time); use them to gauge "do I really need to load this?".

## Top-Level Docs

| Doc | Lines | When to read |
| --- | ---: | --- |
| `requirement.md` | ~900 | Source of truth for what must be built. Any feature gap → fix here first. UI requirements live in §15.4.6. |
| `design.md` | ~490 | Master design: cross-cutting architecture, write/read flows, module decomposition. Read for scope-spanning questions. |
| `plan.md` | ~190 | Phases (P1–P5), milestones, dependency order, decision log. Read before picking a task. |
| `test.md` | ~200 | Test strategy, layer scope definitions, high-level coverage per layer, and feature-dependent test gaps. Read when designing tests or deciding where a test belongs. |
| `plan-test.md` | ~30 | Unfinished test task backlog with checkboxes. Read when picking the next test to implement. |
| `todo_code.md` | ~50 | Forward-looking code-level TODO backlog (open implementation items, blocked/deferred work with rationale). Read before picking up crowtree/crowkv follow-up work. |

Web UI requirements now live in `requirement.md` §15.4.6 (single-page embeddable
console, two hierarchy views, functional surface mapped to the `crowkv-web` API,
embedded Swagger, V2 deferral list).

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
| 7 | Cluster Bootstrap and Group-0 |
| 8 | Cross-Cutting Topics |
| 9 | Failure Mode Catalogue |
| 10 | Observability Hooks |
| 11 | Open Design Questions |
| 12 | References |

## `plan.md` Sections

| § | Topic |
| --- | --- |
| 1 | Phase Overview |
| 2 | Cross-Stream Dependencies |
| 3 | Global Milestones |
| 4 | Test Pairing Rule |
| 5 | Concurrency Model |
| 6 | Decision Log |

## Sub-Designs (`design/design-xxx.md`)

| Doc | Lines | Read when working on |
| --- | ---: | --- |
| `design/design-async-io.md` | ~200 | Async disk I/O backend (`tokio-uring`, `spawn_blocking` fallback), buffer mgmt, runtime topology. |
| `design/design-console.md` | ~715 | `crowkv-console` design: shared core crate, web (Axum + React) and CLI (`clap`) frontends, two-hierarchy API (physical `/api/racks`,`/api/nodes` vs. logical `/api/stores`), monitor task, SSH lifecycle, Swagger UI hosting. |
| `design/design-crowkv-async-kvengine.md` | ~85 | Rust-side `KVEngine` trait async shape: `KVFuture<T>` (zero-alloc `Ready` / boxed `Pending`), the `dyn KVEngine` vs. native-`async-fn` tension and why `async-trait` was rejected, caller-side wiring (`PxLearner`, `PxKvStore`). Implemented; kept as rationale record. |
| `design/design-crowtree.md` | ~240 | crowtree overview: goals/non-goals, architecture, `KVEngine`/`EngineView` abstraction, out-of-order apply + two-GC model, FFI boundary, sub-doc map, full decision log (D1-D19). Read first for storage-engine work. |
| `design/design-crowtree-engine.md` | ~330 | crowtree in-memory engine: slot cell, pages/delta records, write path (apply→delta→consolidate→split/merge), versioned root (`RootVersion`), tree-owned lock-free epoch reclamation, read path, concurrency invariants; the `buffer` memory-ownership model (owned/borrowed, SBO, zero-copy write/read pipelines); the io_uring async FFI bridge (reactor, `ct_future`, fast/slow path). |
| `design/design-crowtree-storage.md` | ~350 | crowtree durable storage: `PageStore` (file/block/RDMA), zero-copy slotted frame format, buffer pool (frame arena, CLOCK eviction, epoch-safe reuse), internal-WAL decision, snapshot pipeline + recovery + export/import, mapping table (PID indirection, segment persistence/recycling), GC watermarks + consensus-WAL GC coupling, new-member install. |
| `design/design-crowtree-test.md` | ~150 | crowtree test strategy: layers (C++ unit/integration, crash/recovery, Rust FFI, cross-engine parity, sanitizer), cases, benchmarks, tooling. |
| `design/design-kv-server.md` | ~350 | `crowkv-server` binary: CLI, HTTP management API, store/group/replica wiring, topology export, lifecycle. |
| `design/design-leader-election.md` | ~330 | Term/ballot bridge, election protocol, new-leader bulk Phase 1, heartbeats, leader lease, ReadIndex, step-down. |
| `design/design-parallel-slots.md` | ~325 | Parallel slot pipelining, sliding window, gap detection / repair, safe-slot, per-key resolved-slot. |
| `design/design-paxos-error.md` | ~55 | Paxos error categories, retry rules, RPC mapping. |
| `design/design-reconfiguration.md` | ~280 | Joint consensus, member add/remove, leader transfer, group-0 special cases, quorum-overlap safety. |
| `design/design-rpc.md` | ~265 | Wire protocol: classic Paxos messages, PeerStream bidi stream (frames, flow control, parallelism), PxService / AdminService, Rust mapping. |
| `design/design-slot.md` | ~650 | `PxSlotList` / `PxSlotNode`: chunk layout, insert/get/trim, reclamation, performance model, future evolution. |
| `design/design-state-machine.md` | ~310 | Storage plug-in: per-key slot tracking, apply semantics, snapshot, compaction, compare, engine impls. |
| `design/design-ui.md` | ~270 | Web UI design (v1 lean rewrite): 3-pane shell, two hierarchy views, slim React Flow canvas, inspector (Details/KV/Activity), embedded Swagger, minimal embedding contract. |
| `design/design-wal.md` | ~470 | Write-ahead log: multi-disk segments, backend-neutral durable flush, ack contract, replay/restore/recovery, GC, disk loss. |

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
