# CrowKV Documentation Index

One-line pointer to every doc and section. Read this first; open the listed
doc only when a task touches a topic in its row. Line counts are approximate
(grow over time); use them to gauge "do I really need to load this?".

## Top-Level Docs

| Doc | Lines | When to read |
| --- | ---: | --- |
| `requirement.md` | ~770 | Source of truth for what must be built. Any feature gap → fix here first. |
| `design.md` | ~490 | Master design: cross-cutting architecture, write/read flows, module decomposition. Read for scope-spanning questions. |
| `plan.md` | ~190 | Phases (P1–P5), milestones, dependency order, decision log. Read before picking a task. |
| `test.md` | ~250 | Test pyramid, invariants, failure-injection taxonomy, CI gates, suites. Read when adding/restructuring tests. |
| `requirement-ui.md` | ~220 | Web UI requirements: single-page embeddable console, functional surface mapped to current `crowkv-web` API, embedded Swagger. |
| `todo_leader.md` | ~180 | TEMP: P1 M3 leader-election implementation plan, current-status survey, and gaps list. Delete when M3 lands. |

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

## `test.md` Sections

| § | Topic |
| --- | --- |
| 1 | Test Pyramid |
| 2 | Invariant Framework (consensus, WAL, storage, reconfig) |
| 3 | Failure Injection Taxonomy |
| 4 | Milestone Test Gates |
| 5 | Regression Suites |
| 6 | CI Pipeline |
| 7 | crowbench Architecture |
| 8 | Per-Area Test Outlines (consensus, WAL, storage, RPC, reconfig) |
| 9 | Test Commands (Suites A/B, WAL benches, crowbench) |

## Sub-Designs (`design/design-xxx.md`)

| Doc | Lines | Read when working on |
| --- | ---: | --- |
| `design/design-async-io.md` | ~200 | Async disk I/O backend (`tokio-uring`, `spawn_blocking` fallback), buffer mgmt, runtime topology. |
| `design/design-console.md` | ~715 | `crowkv-console` design: shared core crate, web (Axum + React) and CLI (`clap`) frontends, two-hierarchy API (physical `/api/racks`,`/api/nodes` vs. logical `/api/stores`), monitor task, SSH lifecycle, Swagger UI hosting. |
| `design/design-kv-server.md` | ~350 | `crowkv-server` binary: CLI, HTTP management API, store/group/replica wiring, topology export, lifecycle. |
| `design/design-leader-election.md` | ~330 | Term/ballot bridge, election protocol, new-leader bulk Phase 1, heartbeats, leader lease, ReadIndex, step-down. |
| `design/design-parallel-slots.md` | ~325 | Parallel slot pipelining, sliding window, gap detection / repair, safe-slot, per-key resolved-slot. |
| `design/design-paxos-error.md` | ~55 | Paxos error categories, retry rules, RPC mapping. |
| `design/design-reconfiguration.md` | ~280 | Joint consensus, member add/remove, leader transfer, group-0 special cases, quorum-overlap safety. |
| `design/design-rpc.md` | ~265 | Wire protocol: classic Paxos messages, PeerStream bidi stream (frames, flow control, parallelism), PxService / AdminService, Rust mapping. |
| `design/design-slot.md` | ~650 | `PxSlotList` / `PxSlotNode`: chunk layout, insert/get/trim, reclamation, performance model, future evolution. |
| `design/design-storage-engine.md` | ~310 | Storage plug-in: per-key slot tracking, apply semantics, snapshot, compaction, compare, engine impls. |
| `design/design-ui.md` | ~300 | Web UI design: SPA stack, single-page shell, theme tokens, topology canvas, embedding contract, embedded Swagger panel. |
| `design/design-wal.md` | ~300 | Write-ahead log: multi-disk segments, batched fsync, ack contract, replay, GC, disk loss. |

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
