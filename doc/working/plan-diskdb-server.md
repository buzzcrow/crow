<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Plan: diskdb Server — R72 (Allocate / Free) + R71 Sync Completion

Task plan for implementing R72 (zone allocator + record persistence)
and the R71 sync-completion work it depends on. Design details are in
`doc/working/design-diskdb-server.md`; root design is
`doc/design/diskdb/design-crow-diskdb.md`. Delete this file after the
work is merged.

R71 shipped a partial `SyncLoop` (heartbeat + owner-map filter + group
add/remove + degraded mode) and a standalone `StatusManager`. R72
needs disks and binds populated before allocate/free can run, so the
sync-completion steps are folded in here as Phase 0.

## Ordering

Phases are ordered by dependency. Within a phase, steps are mostly
independent and can be done in parallel unless noted. Each step ends
with its own verification (fmt + clippy + relevant tests); commit per
the one-commit-per-task rule (small related steps may merge).

## Phase 0 — Protocol + sync completion (unblocks everything)

- [ ] **P0.1 Proto fixes** — `lib/crow-protocol/src/proto/diskdb_type.proto`
  - `BusyBlockValue`: replace `allocate_count` (field 3) with
    `unit_size: u32`; add `state: BlockState` (field 4).
  - `FreeBlockValue`: drop `free_count` (field 3).
  - `ZoneValue`: drop `disk_offset_units` (1), `zone_size_units` (2),
    `alloc_state` (3), `used_units` (4); keep `usage_bitmap` (5); add
    `snapshot_slot: u64` (field 6), `crc32: u32` (field 7).
  - Add `BlockState` enum (`BLOCK_STATE_OK=0`, `BLOCK_STATE_SUSPECT=1`,
    `BLOCK_STATE_CORRUPT=2`).
  - `DiskValue` is unchanged (already has `capacity_units`,
    `zone_size_units`, `unit_size_bytes`, `zone_count`, `status`).
  - Regenerate proto bindings (build step).
  - Files: `diskdb_type.proto`, generated Rust.
- [ ] **P0.2 Keepalive usage piggyback** —
  `lib/crow-protocol/src/proto/sysdata_type.proto` +
  `lib/crow-protocol/src/key/diskdb.rs` +
  `lib/crow-kv-client/src/service_registry.rs`
  - Add `DiskGroupUsageSummary` message to `sysdata_type.proto`
    (`{disk_group_id, capacity_bytes, used_bytes, free_bytes,
    disk_count, allocatable_disk_count}`); extend `DiskdbExtra` with
    `repeated DiskGroupUsageSummary group_usages`.
  - Extend `heartbeat_diskdb` to take
    `group_usages: &[DiskGroupUsageSummary]`.
  - Add `DiskGroupUsageKey { disk_group_id }` (text-path
    `/hw/dg_usage/<dg_id>`, binary tag next free `0x000B`) to
    `key/diskdb.rs` with `BinaryKey` + `TextKey`; group 0 stores the
    summary at this key.
  - Files: `sysdata_type.proto`, `key/diskdb.rs`,
    `service_registry.rs`, generated Rust.
- [ ] **P0.3 SyncLoop: bind map + disk reads** —
  `app/crow-diskdb/src/sync.rs`
  - Call `hw.list_binds()`; populate `Node.bind` `(store_id, group_id)`.
  - Call `hw.list_disks_in_group(rack, node, dg)` per owned
    disk-group; populate `Node.disks`.
  - Detect disk add/remove; populate `SyncOutcome` fields
    (`disks_added`, `disks_removed`, `status_changes`).
  - Depends on: proto regen from P0.1 (no DiskValue change needed).
- [ ] **P0.4 Disk-add init flow** — `app/crow-diskdb/src/sync.rs` +
  `node/disk.rs`
  - On new disk: create `ZoneDisk` with one `Zone` per zone
    (word-aligned capacity), write baseline `ZoneValue` records (empty
    bitmap, `snapshot_slot = 0`, `crc32 = crc32fast::hash` of the empty
    bitmap) to the bound data group via `DataGroupClient` (needs P2.1),
    call `rebuild_active_zones()`, add to `Node.disks`.
  - Depends on: P0.3, P2.1 (DataGroupClient), P1.1 (Zone struct).
  - **Note:** this couples Phase 0 and Phase 2 — do P1.1 + P2.1 first,
    then P0.4.
- [ ] **P0.5 StatusManager wiring** — `sync.rs` + `status.rs`
  - `apply_disk_status(disk_id, new_status)` — validate transition,
    write back via `hw.set_disk_status(...)`, update in-memory
    `ZoneDisk.disk_value`.
  - Wire `check_suspect_timeout` into each sync tick.
  - Call `StatusManager` on detected status changes from P0.3.
- [ ] **P0.6 Bad-disk handling** — `sync.rs` + `zone.rs` + `node/disk.rs`
  - On `Bad` transition: mark `ZoneDisk` + all `Zone`s `Bad`
    (`allocatable()` → `false`).
  - Scan impacted busy blocks via `read_zone_records` per zone
    (§4.4); collect live `BusyBlockValue`s with `owner_chunk`.
  - Emit metric `disk.bad.impacted_blocks` (see P1.4 for metrics
    plumbing); log the hand-off.
  - Future relocation/rebuild is out of R72 scope (stub the hand-off).
  - Depends on: P0.5, P1.1 (Zone), P2.1 (DataGroupClient for
    `read_zone_records`).
- [ ] **P0.7 Blocking initial sync** — `main.rs`
  - Run one `sync_once()` (await) before starting the gRPC server;
    server must not serve until disks + binds are populated.
  - Pass `DataGroupClient` + `StatusManager` into `SyncLoop`.

## Phase 1 — Zone allocator (core, no KV)

- [ ] **P1.1 `Zone` struct** — `app/crow-diskdb/src/zone.rs` (new) +
  `lib.rs` module decl
  - Define `ZoneHealth` enum (`Healthy`, `Missing`, `Bad`) — zones
    inherit the disk's `HwStatus`; no separate zone-level CAS state
    machine (§9).
  - Fields: `disk_id`, `zone_index`, `disk_group_id`, `zone_state:
    RwLock<ZoneHealth>`, `unit_capacity`, `usage_bits: UsageBitmap`,
    `last_pos_64: AtomicU64`, `used_count: AtomicU32`,
    `snapshot_slot: AtomicU64`, `uncompacted_free_record_count:
    AtomicU32`.
  - `allocatable()`, `derived_alloc_state()`.
  - `AllocatedRange { unit_offset: u64, unit_count: u32 }` (offset is
    `u64` to match `BusyBlockKey.unit_offset`; bitmap ops cast to
    `u32` since zone capacity fits in 32 bits).
- [ ] **P1.2 `UsageBitmap` CAS helpers** —
  `lib/crow-protocol/src/bitmap.rs`
  - `load_word(index) -> u64`, `cas_word(index, expected, new) ->
    Result<u64, u64>`, `cas_bit(bit_index, set) -> bool`.
  - Unit tests: round-trip, CAS success/failure under contention.
  - Keep existing `range_set`/`range_clear` (R73 bulk replay).
- [ ] **P1.3 `Zone::allocate` / `free`** — `zone.rs`
  - `allocate(unit_count)`: scan from `last_pos_64` (rotating),
    `countr_one` per word, `cas_bit` set, retry bound
    `cas_retry_limit`, increment the per-zone
    `allocate.retry.cms.bit.count` counter on retry, multi-unit run
    support, update `last_pos_64` + `used_count`.
  - `free(unit_offset, unit_count)`: `cas_bit` clear, decrement
    `used_count`, double-free → `false`.
  - Unit tests: concurrent allocate (no double-alloc, unique bits),
    double-free, multi-unit contiguous, CAS retry bound.
  - Depends on: P1.1, P1.2, P3.1 (`cas_retry_limit` config),
    P1.4 (metrics plumbing).
- [ ] **P1.4 Metrics plumbing** — `app/crow-diskdb/src/metrics.rs`
  (new) + `lib.rs` module decl
  - Wire `crow-common` metrics registry (design §11): per-zone
    `allocate.retry.cms.bit.count` counter (used by P1.3), per-disk
    `disk.bad.impacted_blocks` gauge (used by P0.6). Hot-path counters
    are atomics flushed into the registry at reporting intervals.
  - v1 scope: only the two metrics referenced by R72 steps; the full
    §11 latency hierarchy + gauge set is R74.
  - Depends on: P1.1 (zone identity for per-zone labels).

## Phase 2 — Persistence layer

- [ ] **P2.1 `DataGroupClient`** —
  `app/crow-diskdb/src/persistence.rs` (new) + `lib.rs` module decl
  - Wraps `CrowkvClient`; uses `(store_id, group_id)` from `Node.bind`.
    All methods take `(dg_id, bind, ...)` so the caller reads `Node.bind`
    and passes it (design §4.4).
  - `persist_busy(dg_id, bind, disk_id, zone_idx, unit_offset, value:
    &BusyBlockValue)` — **`batch_write`**: put `BusyBlockKey` + delete
    any prior `FreeBlockKey` for the same offset (§4.8 re-allocate
    clears the free marker).
  - `persist_busy_batch(dg_id, bind, records: &[(disk_id, zone_idx,
    unit_offset, BusyBlockValue)])` — `batch_write` all records (each:
    put `BusyBlockKey` + delete prior `FreeBlockKey`) in one async
    round-trip (one `batch_write` per data group, atomic within the
    group).
  - `persist_free(dg_id, bind, disk_id, zone_idx, unit_offset, value:
    &FreeBlockValue)` — one `batch_write` that **deletes the
    `BusyBlockKey`** and **puts the `FreeBlockValue`** at
    `FreeBlockKey` (per §3.4/§7 record model).
  - `persist_free_batch(dg_id, bind, records: &[(disk_id, zone_idx,
    unit_offset, FreeBlockValue)])` — `batch_write` that, for each
    record, deletes `BusyBlockKey` + puts `FreeBlockKey` (one
    round-trip per data group). Reused by R79's size-threshold batch.
  - `read_zone_records(dg_id, bind, disk_id, zone_idx) ->
    Result<ZoneRecords>` — prefix scan via `CrowkvClient::scan` using
    `BusyBlockKey::prefix_for_zone` + `FreeBlockKey::prefix_for_zone`
    + `ZoneKey::prefix_for_disk` (§4.4). Used by R73 recovery + P0.6
    bad-disk scan.
  - `delete_free_records_batch(dg_id, bind, keys: &[Vec<u8>]) ->
    Result<()>` — `batch_write` with `Delete` ops for free records only.
    Used by R73 compaction.
  - Keys via `BinaryKey::encode` (`BusyBlockKey`/`FreeBlockKey`/
    `ZoneKey`).
  - Depends on: P0.1 (BusyBlockValue/FreeBlockValue/ZoneValue fields).
- [ ] **P2.2 Two-phase async allocate** — `persistence.rs`
  - `allocate_block(node, dg_id, unit_count, owner_chunk, unit_size,
    kv)`: Phase 1 `node.allocate_block` (sync CAS), Phase 2
    `persist_busy` (async); rollback on failure.
  - `allocate_blocks(...)`: Phase 1 `node.allocate_blocks`, Phase 2
    `persist_busy_batch` (one `batch_write` per data group); rollback
    all on failure.
  - Depends on: P1.3, P3.2 (Node allocate methods), P2.1.
- [ ] **P2.3 Immediate free** — `persistence.rs`
  - `free_block(node, segment, kv)`: clear bitmap locally,
    `persist_free` (delete `BusyBlockKey` + put `FreeBlockValue` in
    one `batch_write`).
  - `free_blocks(node, segments, kv)`: group by `dg_id`,
    `persist_free_batch` per data group.
  - `validate_owner_on_free` toggle (default false) → optional KV read
    first.
  - Depends on: P1.3, P3.2, P2.1, P3.1 (config toggle).

## Phase 3 — Disk / node allocation orchestration

- [ ] **P3.1 Config extensions** — `app/crow-diskdb/src/config.rs`
  - `StorageDefaults`: add `cas_retry_limit: u32` (100),
    `validate_owner_on_free: bool` (false).
  - `PersistenceConfig`: add `free_batch_enabled: bool` (false);
    remove `free_flush_interval_ms` (also remove from `Default` impl;
    no existing validation for it).
  - Validation: `cas_retry_limit > 0`.
  - Unit tests for new fields.
- [ ] **P3.2 `ZoneDisk` + `Node` allocation methods** —
  `node/disk.rs`, `node.rs`
  - `ZoneDisk`: add `active_zone_context: RwLock<Arc<ActiveZoneContext>>`,
    `pos_v_zone_ctx: AtomicU64`; `ZoneRef` → `Arc<Zone>`;
    `ActiveZoneContext = Vec<Arc<Zone>>`.
  - `disk_allocate`, `rotate_active_zones`, `rebuild_active_zones`,
    `free`.
  - `Node`: add `allocating_disks: RwLock<Arc<AllocateDiskContext>>`,
    `pos_v_disk_ctx: AtomicU64`; `AllocateDiskContext =
    Vec<Arc<ZoneDisk>>`; side `HashMap<DiskId, Arc<ZoneDisk>>` index
    for O(1) free lookup (or switch `disks` to `HashMap`).
  - `allocate_block`, `allocate_blocks`, `free_block`,
    `refresh_disk_context`.
  - Unit tests: `disk_allocate` round-robin + rotation;
    `Node::allocate_block` round-robin within named disk-group;
    `exclude_disks` anti-affinity.
  - Depends on: P1.1 (`ZoneRef → Arc<Zone>`), P1.3, P3.1.

## Phase 4 — gRPC + server wiring

- [ ] **P4.1 gRPC allocate/free handlers** —
  `app/crow-diskdb/src/grpc.rs`
  - `allocate_blocks`: validate `unit_count` (non-zero, multiple of
    `block_size_bytes`) and `count` (1–1024); check not degraded; look
    up the `Node` for `disk_group_id`; read `unit_size` from the
    target disk's `DiskValue.unit_size_bytes` (not from the request —
    `AllocateBlocksRequest` has no `unit_size` field); call
    `persistence::allocate_blocks`, return segments.
  - `free_blocks`: parse segments, call `persistence::free_blocks`.
  - `query_capacity_stats`: keep stub (returns empty) — R74 fills it
    in. `get_disk_group_info` / `get_disk_info` already implemented;
    update iteration if `disks` switches to `HashMap`.
  - Error mapping: `NoSpace`→`ResourceExhausted`, not-owner→
    `PermissionDenied`, invalid→`InvalidArgument`, degraded→
    `Unavailable`.
  - Add `DataGroupClient` to `DiskdbService` struct.
  - Depends on: P2.2, P2.3, P3.2.
- [ ] **P4.2 main.rs wiring** — `app/crow-diskdb/src/main.rs`
  - Create `DataGroupClient`; pass to `SyncLoop` + `DiskdbService`.
  - Create `StatusManager`; pass to `SyncLoop`.
  - Blocking initial sync before gRPC serve (P0.7).
  - Depends on: P4.1, P0.7.
- [ ] **P4.3 Cargo.toml** — `app/crow-diskdb/Cargo.toml`
  - Add `rand` (retry random start in `allocate_blocks` full scan).
  - Test dev-deps: add `crow-kv` (path) only if the in-process
    `PxKvStore` harness path is used in P5.1; the spawned-binary path
    needs no extra dep (it spawns the `crow-diskdb` + `crow-kv-server`
    binaries from the already-built workspace). Decide the P5.1 path
    first, then add only what it needs.

## Phase 5 — Integration tests (real group-0 cluster)

- [ ] **P5.1 Test harness (real group-0 cluster)** —
  `app/crow-diskdb/tests/common/` (new)
  - Pick one path (decide before P4.3): spawned `crow-kv-server` binary
    (`app/crow-kv-server/tests/common/process.rs`) for end-to-end
    fidelity, or in-process `PxKvStore`
    (`lib/crow-kv/tests/common/cluster.rs`) for hermetic tests.
  - Start a single-node group-0 cluster (self-elect); call
    `system_init` (or equivalent) to create group 0; wait for leader.
  - Create the bound data group for zone records via `add_group` mgmt
    endpoint / `KVClusterAdmin`.
  - Seed group 0 with test topology via `HardwareClient` (rack, node,
    disk-group, disks, `set_owner`, `set_bind`) — same writes as
    `http_cluster_init` Phase 5.
  - **Start the diskdb server under test** — either spawn the
    `crow-diskdb` binary (mirrors `process.rs`) or run `DiskdbService`
    + `SyncLoop` in-process on an OS-assigned port; wait for gRPC
    ready + the blocking initial sync (P0.7) to complete.
  - Build `CrowkvClient` (`seed_leader(0, 0, addr)`) +
    `HardwareClient` + `ServiceRegistryClient` + `DataGroupClient`;
    return handle struct for teardown (drops/kills the diskdb + kv
    servers).
  - No mock server — see `design-crow-kv-group0.md` for the group-0
    design and `design-diskdb-server.md` §2.
- [ ] **P5.2 Sync integration tests** —
  `app/crow-diskdb/tests/sync.rs` (new)
  - Sync populates `NodeContainer` + `Node.bind`; disk-add init writes
    `ZoneValue` baselines; status transition updates in-memory state;
    degraded mode on group-0 leader stop / recovery on restart.
  - Bad disk: allocate blocks, set status `Bad` in group 0, sync,
    verify zones go `Bad`, allocates stop, impacted busy blocks
    enumerable via `read_zone_records`.
- [ ] **P5.3 Allocate/free integration tests** —
  `app/crow-diskdb/tests/alloc_free.rs` (new)
  - Single/multi allocate → `BusyBlockValue` at `BusyBlockKey`;
    rollback on persist failure.
  - Single/multi free → `FreeBlockValue` at `FreeBlockKey` +
    `BusyBlockKey` deleted; allocate-after-free.
  - Round-robin across disks within named disk-group; `exclude_disks`.
  - gRPC end-to-end + error mapping.
  - Crash-safety invariants (§4.8): allocate-then-crash-before-persist
    → no `BusyBlockKey` (block free by current-state rule);
    free-then-crash-before-persist → `BusyBlockKey` still exists (block
    busy); re-allocate deletes prior `FreeBlockKey` in same
    `batch_write`.
  - Depends on: P5.1, P4.2.

## Phase 6 — Verification + commit

- [ ] **P6.1 Full verification**
  - `pixi run cargo fmt --all -- --check`
  - `pixi run cargo clippy --all-targets -- -D warnings`
  - `pixi run cargo test -p crow-diskdb`
  - `pixi run cargo test -p crow-protocol` (bitmap + key additions)
  - `pixi run cargo test -p crow-kv-client` (service_registry
    keepalive usage piggyback, P0.2)
  - Fix lint/test failures up to 3 rounds; skip pre-existing failures
    with a stated reason.
- [ ] **P6.2 Commit**
  - One commit for the R72 task (squash unpushed commits from this
    task via soft reset to remote tip, re-commit).
  - Single-line subject only; no body, no trailers, no R-numbers.
  - Verify no temp/generated files staged.

## Dependency graph (summary)

```
P0.1 (protos) ─┬─> P0.3 (sync) ──> P0.5 (status wire) ─┬─> P0.7 (init sync)
               │                │                      └─> P0.6 (bad-disk)
               │                └─> P0.4 (disk-add init)
               │                     depends on P1.1 + P2.1
               └─> P2.1 (DataGroupClient) ─┐
P0.2 (keepalive)  independent of P0.3      │
   needs P0.1 (sysdata_type.proto regen)   │
                                          │
P1.1 (Zone) ─┬─> P1.2 (bitmap CAS) ────────┤
             └─> P1.4 (metrics)            │
P1.2 + P1.1 ─> P1.3 (alloc/free) ─────────┤
P3.1 (config) ─> P3.2 (disk/node orch) <───┤  (P3.2 also needs P1.1)
                  P2.2 (async alloc) <─────┤  (needs P1.3, P3.2, P2.1)
                  P2.3 (immediate free) <──┤  (needs P1.3, P3.2, P2.1, P3.1)
P2.2 + P2.3 + P3.2 ─> P4.1 (gRPC) ─> P4.2 (main) ─> P5.* (tests) ─> P6
P0.4 + P0.5 + P0.6 ─> P0.7 ─> P4.2
```

Critical path (longest dependency chain to P6):
P0.1 → P1.1 → P1.2 → P1.3 → P2.1 → P0.4 → P0.7 → P4.2 → P5.1 → P5.3 → P6
(with P3.1 → P3.2 and P2.2 running in parallel off P1.3/P2.1 before P4.1,
which feeds P4.2).
