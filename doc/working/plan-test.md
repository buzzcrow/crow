<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CrowKV Test Task Backlog

**Override:** This file is **persistent** — it is not deleted after the
requirement (R9) is complete. Only completed tasks are removed; the file
itself remains as the ongoing test task backlog. This overrides the
`/implement-requirement` workflow's cleanup step which would normally delete
`plan-<topic>.md`.

Unfinished test tasks, grouped by layer. Each task has a checkbox for tracking.
For test strategy, layer scope, and coverage details, see [`design/design-test.md`](../design/design-test.md).

## Suite Timing

Measured on 2026-07-20 (clean build, macOS). Build time: 72.8 s.
Run `pixi run clean` before measuring for reproducible results.

| Suite | Tests | macOS | Linux |
| --- | --- | --- | --- |
| `test-ct` | 328 | 9.1 s | — |
| `test-ffi` | 15 | 29.9 s | — |
| `test-core` | 508 | 43.5 s | — |
| `test-server` | 55 | 37.0 s | — |
| `test-cli` | 13 | 35.1 s | — |
| `test-mgmt-api` | 49 | 39.1 s | — |
| `test-ui` | 51 | 81.4 s | — |

---

## Unit Layer — paxos/acceptor

Source: `lib/crow-kv/src/paxos/acceptor.rs`. Tests: `acceptor_test.rs` (6 tests).

## Unit Layer — paxos/learner

Source: `lib/crow-kv/src/paxos/learner.rs`. Tests: `learner_test.rs` (4), `learner_dedup_test.rs` (10), `learner_async_test.rs` (1). Coverage is thorough.

## Unit Layer — paxos/error

Source: `lib/crow-kv/src/paxos/error.rs`. Tests: `error_test.rs` (11 variants).

## Unit Layer — kv/mem_kv + kv/op

Source: `lib/crow-kv/src/kv/`. Tests: `mem_kv_test.rs` (9 + conformance), `op_codec_test.rs` (11), `kv_future_test.rs` (5), `conformance.rs` (shared). Coverage is thorough.

## Unit Layer — wal/record

Source: `lib/crow-kv/src/wal/record.rs`. Tests: `record_tests.rs` (9). No gaps identified.

## Election Unit

Source: `lib/crow-kv/src/election/`. Tests: 8 files, 72 tests. No gaps identified.

## WAL Subsystem

Source: `lib/crow-kv/src/wal/`. Tests: 12 files, ~92 tests. Coverage is thorough.

- [ ] **WAL disk-loss recovery**: simulate fsync failure or file loss after write — verify engine surfaces error and reads/replays are consistent with last durable state. Feature-dependent per design-test.md.

## Slot Subsystem

Source: `lib/crow-kv/src/paxos/slot_list.rs`, `slot_node.rs`. Tests: `slot_list_test.rs` (18 tests).

## Replica

Source: `lib/crow-kv/src/cluster/local_replica.rs`. Tests: 10 files, ~56 tests.

- [ ] **WAL GC safe slot integration**: `lib/crow-kv/src/wal/gc.rs` uses `safe_slot = u64::MAX`. Needs snapshot persistence and a slot marker so GC can safely truncate below the applied frontier. Add a dedicated GC test once the slot marker is implemented.

## Group

Source: `lib/crow-kv/src/cluster/group.rs`. Tests: 23 files, ~65 tests.

- [ ] **Reconfig — remove leader**: needs separate plan — requires leader transfer before removal to avoid cluster stall.

## Store

Source: `lib/crow-kv/src/store/`. Tests: 8 files, 26 tests (node, multi_group,
multi_node_multi_group, status, health, shutdown, shutdown_under_load,
persistence, kv_correctness).

- [ ] **Per-group WAL-root isolation**: needs separate plan — WAL-root per-group not yet configurable in test harness.

## Deployment

Source: `app/crow-kv-server/`. Tests: 7 files, 55 tests (server_api, async_ops,
cli_parse, cluster_e2e, startup, snapshot_join_e2e, deployment_reconfig).

- [ ] Re-enable the four ignored process-level tests once their root causes are fixed.
- [ ] Multi-store-per-node process test that mirrors the Web UI multi-store topology end-to-end.
- [ ] **Reconfig via API — remove leader**: needs separate plan — requires leader transfer before removal.
- [ ] **Network partition between processes**: needs separate plan — requires network partition simulation infrastructure.

## Console Mgmt API Layer

Source: `lib/crow-console-shared/`, `app/crow-web/`, `app/crow-cli/`. Tests: web 13 files (~37 tests), shared/cli 7
files (~9 tests). Covers REST routes, CLI commands, API forwarding, health
aggregation, config persistence, OpenAPI proxy. No gaps identified.

## crow-tree C++ Tests

Source: `lib/crow-tree/tests/`. Tests: 334 tests (unit: 26 files, integration:
24 files). Covers cell encoding, leaf/frame/inner pages, delta replay,
consolidation, mapping table, epoch manager, split/merge, snapshot
roundtrip, crash recovery, C API, async get/scan, eviction, compression,
persist, write/read paths, stress. No gaps identified.

## Rust FFI / Cross-Engine Parity

Source: `lib/crow-kv/tests/kv/crow_tree_engine_test.rs`. Tests: conformance
suite (shared with `InMemKV`), async pending path, durable reopen,
cross-engine parity, clear. No gaps identified.

## E2E / Playwright UI Tests

Source: `app/crow-web/ui/e2e/`. Tests: 47 spec files (Phases 0–5).
All phases complete. No gaps identified.

