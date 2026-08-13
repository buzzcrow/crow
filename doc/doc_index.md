<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROW Documentation Index

One-line pointer to every doc. Read this first; open the listed doc only
when a task touches a topic in its row.

## Top-Level Docs

| Doc | When to read |
| --- | --- |
| `doc/design/kv/design-crow-kv.md` | Root KV design — read first for any KV design or architecture question. |
| `doc/design/protocol/design-crow-protocol.md` | Root protocol design — read first for protocol or key encoding questions. |
| `doc/design/diskdb/design-crow-diskdb.md` | Root diskdb design — read first for any diskdb design or architecture question. |
| `doc/design/tree/design-crow-tree.md` | Root tree design — read first for storage-engine work. |
| `doc/design/console/design-crow-console.md` | Root console design — read first for console work. |
| `doc/user-manual/user-guide.md` | User guide: Web UI, CLI, REST API, quick start, cluster ops, upgrade. |

## Backlog (`doc/backlog/`)

| Doc | When to read |
| --- | --- |
| `doc/backlog/backlog.md` | Backlog index with priority/complexity and brief intros. Read before picking up follow-up work. |
| `doc/backlog/R**-<component>-<topic>.md` | Per-requirement detail (problem, approach, files, acceptance). Open only the matched `R**` file; delete after merge. |

## Working & Flow-Analysis Docs

Plan files live under `doc/working/`; flow analyses live under `doc/design/kv/`.

| Doc | When to read |
| --- | --- |
| `doc/working/plan-test.md` | Unfinished test task backlog. Read when picking the next test to implement. |
| `doc/working/plan-diskdb-zone-management.md` | diskdb zone-management implementation plan — persist-only free, `compact_ts` watermark compaction, atomic batch_write, preparatory thread. Read when implementing the zone-management design delta. |
| `doc/design/kv/kv-read-flow-analysis.md` | KV point-read flow trace, benchmarks, open issues. |
| `doc/design/kv/kv-scan-flow-analysis.md` | KV scan flow trace, benchmarks, open issues. |
| `doc/design/kv/kv-write-flow-analysis.md` | KV write path trace and optimization opportunities. |

## Project Files (repo root)

| File | When to read |
| --- | --- |
| `AGENTS.md` | Always — project overview + dispatch table for AI agents. |
| `CONTRIBUTING.md` | Before opening a PR — setup, conventions, process. |
| `CHANGELOG.md` | When releasing or checking what changed between versions. |
| `SECURITY.md` | When reporting or handling a security vulnerability. |
| `CODE_OF_CONDUCT.md` | Community behavior guidelines. |

## Sub-Designs (`doc/design/{kv,tree,console,protocol,diskdb}/`)

| Doc | Read when working on |
| --- | --- |
| `doc/design/kv/design-crow-kv-leader-election.md` | Election protocol, leader lease, ReadIndex, step-down. |
| `doc/design/kv/design-crow-kv-slot.md` | Parallel slot pipelining, gap repair, follower catch-up, `SlotList`, proposal coalescing. |
| `doc/design/kv/design-crow-kv-rpc.md` | Wire protocol, LearnerStream, PxService, Paxos error model. |
| `doc/design/kv/design-crow-kv-reconfiguration.md` | Member add/remove, leader transfer, `membership_epoch` fence. |
| `doc/design/kv/design-crow-kv-group0.md` | Group-0 sysdata schema, service registry, cluster topology records. |
| `doc/design/kv/design-crow-kv-state-machine.md` | Per-key slot tracking, apply semantics, snapshot, compaction. |
| `doc/design/kv/design-crow-kv-wal.md` | WAL segments, durable flush, replay/restore/recovery, GC. |
| `doc/design/kv/design-crow-kv-server.md` | `crow-kv-server` binary: startup, concurrency, HTTP management API, group lifecycle. |
| `doc/design/kv/design-crow-kv-test.md` | Test strategy, layer-by-layer test guide, coverage rules. |
| `doc/design/kv/design-crow-kv-observability.md` | Metrics module: five metric types, registry, instrumentation points, log format. |
| `doc/design/tree/design-crow-tree.md` | crow-tree overview, `KVEngine`/`EngineView`, out-of-order apply + two-GC model, FFI boundary. |
| `doc/design/tree/design-crow-tree-engine.md` | In-memory engine: slot cell, pages/delta, write path, versioned root, lock-free epoch reclamation, io_uring FFI. |
| `doc/design/tree/design-crow-tree-storage.md` | Durable storage: `PageStore` backends, slotted frame format, buffer pool, snapshot/recovery, mapping table, GC. |
| `doc/design/console/design-crow-console.md` | Console core crate, web + CLI frontends, two-hierarchy API, monitor task, SSH lifecycle, bootstrap. |
| `doc/design/console/design-crow-console-ui.md` | Web UI v1: 3-pane shell, React Flow canvas, inspector, KV Operator center panel. |
| `doc/design/protocol/design-crow-protocol-key.md` | Key encoding: flat structs, 3-byte header, `BinaryKey` + `TextKey`, frozen layouts, append-only evolution. |
| `doc/design/protocol/design-crow-protocol-types.md` | Wire types, `u64` ID aliases, re-export pattern, `utoipa` schema derives. |
| `doc/design/diskdb/design-crow-diskdb.md` | diskdb root: architecture, group-0 sysdata, disk status management, space metrics, background scanner, crate layout, concurrency. |
| `doc/design/diskdb/design-crow-diskdb-zone-management.md` | Zone management: record model, allocation algorithm, persist-only free, compaction-on-rotation, preparatory thread, crash recovery, zone-level concurrency, invariants. |
| `doc/design/diskdb/design-crow-diskdb-space-metrics.md` | Space metrics component: usage accessors, `QueryCapacityStats` handler, per-disk counters, recalc verifier, reporting loop, keepalive piggyback, kv-client aggregation, `crow-diskdb-client` library. |

## How AI Should Use This Index

1. Match the task description to a row above.
2. Open only the matched doc; grep for the relevant `##` section instead of reading top-to-bottom.
3. If unsure between two docs, prefer the most specific sub-design over the root design.
4. If the task spans multiple sub-designs, open the root design first to learn how they interact.
5. After the task: if the index row is now wrong (renamed file, materially changed scope), update this file in the same commit.
