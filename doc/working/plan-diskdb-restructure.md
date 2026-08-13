<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# diskdb Module Restructure Plan

Design: `doc/working/design-diskdb-restructure.md`. Goal: restructure
`app/crow-diskdb` so the module tree surfaces the domain concepts, separates
domain from infrastructure, and replaces the ad-hoc runtime wiring with a
status machine + keep-alive driver + bg-task framework + startup lifecycle.

## Phase 1 — Domain rename + gather (C1–C4, C10)

- [ ] **1.1 Create `domain/` module + index**: create `src/domain.rs` (pure
  index: `pub mod` + `pub use`) and `src/domain/` dir. Files: `src/domain.rs`.
- [ ] **1.2 Move + rename `node.rs` → `domain/disk_group.rs`**: `Node` →
  `DdbDiskGroup`, `AllocError` stays here temporarily (moves in 1.6). Files:
  `src/domain/disk_group.rs` (new), `src/node.rs` (delete).
- [ ] **1.3 Move + rename `node/container.rs` → `domain/disk_group_container.rs`**:
  `NodeContainer` → `DdbDiskGroupContainer`, `nodes` → `disk_groups`,
  `add_node`→`add_disk_group`, `remove_node`→`remove_disk_group`,
  `get_node`→`get_disk_group`, `node_ids`→`disk_group_ids`. Files:
  `src/domain/disk_group_container.rs` (new), `src/node/container.rs` (delete).
- [ ] **1.4 Move + rename `node/disk.rs` → `domain/disk.rs`**: `ZoneDisk` →
  `DdbDisk`. Files: `src/domain/disk.rs` (new), `src/node/disk.rs` (delete).
- [ ] **1.5 Move + rename `zone.rs` → `domain/zone.rs`**: `Zone` → `DdbZone`,
  `ZoneHealth` → `DdbZoneHealth`. Files: `src/domain/zone.rs` (new),
  `src/zone.rs` (delete).
- [ ] **1.6 Create `domain/alloc.rs`**: move `AllocError` from `disk_group.rs`,
  add `AllocClaim`/`AllocatableDiskContext` type aliases here. Files:
  `src/domain/alloc.rs` (new).
- [ ] **1.7 Update `lib.rs`**: drop `pub mod node` + `pub mod zone`, add
  `pub mod domain`. Files: `src/lib.rs`.
- [ ] **1.8 Update all imports**: update `src/{main,grpc,sync,recovery,
  persistence,status}.rs`, `src/recovery/compaction.rs`, and all tests to
  `crate::domain::{...}`. Files: all src + test files.
- [ ] **1.9 Delete `src/node/`** dir. Files: `src/node/` (delete).
- [ ] **1.10 Verify**: `pixi run cargo fmt --all -- --check`, `pixi run cargo
  clippy --all-targets -- -D warnings`, `pixi run test-diskdb`.

## Phase 2 — Split persistence.rs (C11)

- [ ] **2.1 Create `domain/records.rs`**: move `BusyRecord`/`FreeRecord`/
  `ZoneRecords` from `persistence.rs`. Files: `src/domain/records.rs` (new),
  `src/domain.rs` (add `pub mod records`).
- [ ] **2.2 Move domain parts to `domain/alloc.rs`**: move `FreeError` +
  `allocate_block`/`allocate_blocks`/`free_block`/`free_blocks` from
  `persistence.rs` to `domain/alloc.rs`. Files: `src/domain/alloc.rs`,
  `src/persistence.rs`.
- [ ] **2.3 Create `data_group_client.rs`**: rename `persistence.rs` →
  `src/data_group_client.rs`, keep `DataGroupClient` + `Bind` + KV I/O methods
  only. Files: `src/data_group_client.rs` (new from `persistence.rs`),
  `src/persistence.rs` (delete).
- [ ] **2.4 Update `lib.rs` + imports**: drop `pub mod persistence`, add
  `pub mod data_group_client`. Update all importers. Files: `src/lib.rs`,
  all importers.
- [ ] **2.5 Verify**: fmt + clippy + test-diskdb.

## Phase 3 — gRPC service module (C8)

- [ ] **3.1 Create `service/` module**: move `grpc.rs` →
  `service/diskdb_service.rs`, create `src/service.rs` index. Files:
  `src/service.rs` (new), `src/service/diskdb_service.rs` (new from
  `grpc.rs`), `src/grpc.rs` (delete).
- [ ] **3.2 Update `lib.rs` + `main.rs`**: `pub mod grpc` → `pub mod service`;
  update import. Files: `src/lib.rs`, `src/main.rs`.
- [ ] **3.3 Verify**: fmt + clippy + test-diskdb.

## Phase 4 — Config system (C6a + C7)

- [ ] **4.1 Rename `config.rs` → `ddb_config.rs`**: `DiskdbConfig` →
  `DdbConfig`, add `load_from_file`. Files: `src/ddb_config.rs` (new from
  `config.rs`), `src/config.rs` (delete).
- [ ] **4.2 Move inline tests**: `#[cfg(test)] mod tests` →
  `tests/config_test.rs`. Files: `tests/config_test.rs` (new),
  `src/ddb_config.rs`.
- [ ] **4.3 Update `lib.rs` + `main.rs` + imports**: `pub mod config` →
  `pub mod ddb_config`. Files: `src/lib.rs`, `src/main.rs`.
- [ ] **4.4 Create sample config + docs**: `conf/ddb-config.sample.json` +
  `conf/ddb-config.sample.md`. Files: `app/crow-diskdb/conf/` (new).
- [ ] **4.5 Fix `.gitignore`**: remove `**/conf/`, add explicit
  `app/crow-diskdb/conf/runtime/`. Files: `.gitignore`.
- [ ] **4.6 Verify**: fmt + clippy + test-diskdb + `git check-ignore -v`.

## Phase 5 — Status machine (C13)

- [ ] **5.1 Rename `status.rs` → `status_machine.rs`**: `StatusManager` →
  `HwStateMachine`, add `Op` enum, add `impl HwStatus { permits, on_enter_disk,
  on_enter_disk_group }`, add `transition_disk`/`transition_disk_group`.
  Files: `src/status_machine.rs` (new from `status.rs`), `src/status.rs`
  (delete).
- [ ] **5.2 Move inline tests**: → `tests/status_machine_test.rs`. Files:
  `tests/status_machine_test.rs` (new), `src/status_machine.rs`.
- [ ] **5.3 Update `domain/disk.rs`**: remove `set_effective_status` body
  side-effects (move to `HwStatus::on_enter_disk`). Files: `src/domain/disk.rs`.
- [ ] **5.4 Update `lib.rs` + imports**: `pub mod status` → `pub mod
  status_machine`. Files: `src/lib.rs`, importers.
- [ ] **5.5 Verify**: fmt + clippy + test-diskdb.

## Phase 6 — Keep-alive loop (C14)

- [ ] **6.1 Rename `sync.rs` → `keepalive.rs`**: `SyncLoop` → `KeepAlive`,
  `SyncOutcome` → `KeepAliveOutcome`, `SyncConfig` → `KeepAliveConfig`,
  `sync_once` → `tick`. Split into `heartbeat`/`observe_ownership`/
  `observe_disks`/`disk_add_init`. Files: `src/keepalive.rs` (new from
  `sync.rs`), `src/sync.rs` (delete).
- [ ] **6.2 Drive state machine**: in `observe_disks`, replace
  `set_effective_status` + `rebuild_allocating_disks` with
  `machine.transition_disk(...)`. Files: `src/keepalive.rs`.
- [ ] **6.3 Update `lib.rs` + `main.rs` + imports**: `pub mod sync` →
  `pub mod keepalive`. Files: `src/lib.rs`, `src/main.rs`, tests.
- [ ] **6.4 Verify**: fmt + clippy + test-diskdb.

## Phase 7 — Recovery restructure (C12)

- [ ] **7.1 Create `recovery/full_scan.rs`**: move strategy 1
  (`rebuild_zone_bitmap_full_scan`) from `recovery.rs`. Files:
  `src/recovery/full_scan.rs` (new).
- [ ] **7.2 Create `recovery/journal_replay.rs`**: move strategy 2
  (`recover_zone_inner`, `merge_ops_by_slot`, `find_free_unit_count_at_slot`,
  `zone_snapshots_exist`) from `recovery.rs`. Files:
  `src/recovery/journal_replay.rs` (new).
- [ ] **7.3 Slim `recovery.rs` to index + orchestrator**: `recovery.rs`
  keeps `RecoveryEngine` + `RecoveryError` + `ZoneStats` + `pub mod`
  declarations. `recover_node` → `recover_disk_group`. Files:
  `src/recovery.rs`.
- [ ] **7.4 Create `recovery/disk_recovery.rs` placeholder**: module doc +
  `// TODO`. Files: `src/recovery/disk_recovery.rs` (new).
- [ ] **7.5 Verify**: fmt + clippy + test-diskdb.

## Phase 8 — Background-task framework (C5)

- [ ] **8.1 Create `bg_task.rs`**: `BackgroundTask` trait + `BgRunner` +
  `Trigger` enum + `BgCtx` + `BgError`. Files: `src/bg_task.rs` (new).
- [ ] **8.2 Refactor `CompactionEngine` to implement `BackgroundTask`**:
  `compaction_loop` → `run_cycle` + `trigger`. Files:
  `src/recovery/compaction.rs`.
- [ ] **8.3 Update `lib.rs`**: add `pub mod bg_task`. Files: `src/lib.rs`.
- [ ] **8.4 Verify**: fmt + clippy + test-diskdb.

## Phase 9 — Startup lifecycle (C9)

- [ ] **9.1 Create `lifecycle.rs`**: `StartupPhase` enum + `LifecycleState`
  (AtomicU8). Files: `src/lifecycle.rs` (new).
- [ ] **9.2 Add phase to service gating**: `DiskdbService` holds
  `Arc<LifecycleState>`, rejects `AllocateBlocks`/`FreeBlocks`/
  `RebuildZoneBitmap` when not `Up`. Files: `src/service/diskdb_service.rs`.
- [ ] **9.3 Rewrite `main.rs` startup flow**: service-up-first, background
  recovery, phase transitions, `BgRunner` wiring. Files: `src/main.rs`.
- [ ] **9.4 Update `lib.rs`**: add `pub mod lifecycle`. Files: `src/lib.rs`.
- [ ] **9.5 Verify**: fmt + clippy + test-diskdb.

## Phase 10 — Tests + final verification

- [ ] **10.1 New UT**: `tests/status_machine_test.rs` (expanded),
  `tests/lifecycle_test.rs`, `tests/bg_task_test.rs`, `tests/config_test.rs`.
- [ ] **10.2 New E2E**: `tests/lifecycle_e2e_test.rs`,
  `tests/keepalive_e2e_test.rs`.
- [ ] **10.3 Update existing tests**: all import/type-name updates in
  `tests/{zone_alloc,disk_alloc,diskdb_e2e,recovery}_test.rs`,
  `tests/common/cluster.rs`.
- [ ] **10.4 Full CI check**: `pixi run cargo fmt --all -- --check`,
  `pixi run cargo clippy --all-targets -- -D warnings`,
  `pixi run test-diskdb`.

## File list

- `src/lib.rs` — module index updates (every phase).
- `src/main.rs` — startup flow rewrite (Phase 9) + import updates.
- `src/domain.rs` — new, pure index.
- `src/domain/disk_group.rs` — new, from `node.rs`.
- `src/domain/disk_group_container.rs` — new, from `node/container.rs`.
- `src/domain/disk.rs` — new, from `node/disk.rs`.
- `src/domain/zone.rs` — new, from `zone.rs`.
- `src/domain/alloc.rs` — new, from `persistence.rs` domain parts.
- `src/domain/records.rs` — new, from `persistence.rs` record types.
- `src/status_machine.rs` — new, from `status.rs`.
- `src/keepalive.rs` — new, from `sync.rs`.
- `src/recovery.rs` — slim to index + orchestrator.
- `src/recovery/full_scan.rs` — new, strategy 1.
- `src/recovery/journal_replay.rs` — new, strategy 2.
- `src/recovery/compaction.rs` — refactor to `BackgroundTask`.
- `src/recovery/disk_recovery.rs` — new, placeholder.
- `src/data_group_client.rs` — new, from `persistence.rs` infra parts.
- `src/bg_task.rs` — new, bg-task framework.
- `src/lifecycle.rs` — new, startup phases.
- `src/service.rs` — new, pure index.
- `src/service/diskdb_service.rs` — new, from `grpc.rs`.
- `src/ddb_config.rs` — new, from `config.rs`.
- `src/metrics.rs` — unchanged.
- `conf/ddb-config.sample.json` — new, tracked sample.
- `conf/ddb-config.sample.md` — new, tracked field docs.
- Delete: `src/config.rs`, `src/grpc.rs`, `src/node.rs`, `src/node/`,
  `src/persistence.rs`, `src/status.rs`, `src/sync.rs`, `src/zone.rs`.
- `tests/common/cluster.rs` — import + type updates.
- `tests/zone_alloc_test.rs` — import + type updates.
- `tests/disk_alloc_test.rs` — import + type updates.
- `tests/diskdb_e2e_test.rs` — import + type + startup-flow updates.
- `tests/recovery_test.rs` — import + type updates.
- `tests/config_test.rs` — new, from `config.rs` inline tests.
- `tests/status_machine_test.rs` — new, from `status.rs` inline tests.
- `tests/lifecycle_test.rs` — new.
- `tests/bg_task_test.rs` — new.
- `tests/lifecycle_e2e_test.rs` — new.
- `tests/keepalive_e2e_test.rs` — new.
- `Cargo.toml` — add `tokio-util`, `async-trait` if needed.
- `.gitignore` — remove `**/conf/`, add explicit `conf/runtime/` paths.

## Test checklist

### Unit tests

- [ ] `tests/config_test.rs` — validation + `load_from_file` (moved from
  inline).
- [ ] `tests/status_machine_test.rs` — legal/illegal transitions,
  `on_enter_disk(Bad)` marks zones, `permits(Op::*)`, effective status
  (moved + expanded from inline).
- [ ] `tests/lifecycle_test.rs` — `LifecycleState` get/set, concurrent
  access.
- [ ] `tests/bg_task_test.rs` — `BgRunner` timer/predicate cycles, fatal
  error handling, shutdown.

### Integration / E2E

- [ ] `tests/zone_alloc_test.rs` — passes unchanged (import updates only).
- [ ] `tests/disk_alloc_test.rs` — passes unchanged (import updates only).
- [ ] `tests/diskdb_e2e_test.rs` — passes with startup-flow updates.
- [ ] `tests/recovery_test.rs` — passes unchanged (import updates only).
- [ ] `tests/lifecycle_e2e_test.rs` — service reachable during recovery,
  phase-gated RPCs, per-disk-group readiness.
- [ ] `tests/keepalive_e2e_test.rs` — status change via state machine,
  `permits` gates allocation.
