<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R76: diskdb — Disk Discovery + Health Probing + Failure Recovery

**Problem**: R71–R75 assume diskdb knows its disks and their health.
But the design doc (§10) specifies that diskdb does its own minimal
disk discovery + health probing — CROW has no `diskio` service (unlike
aioss). R71's sync loop reads disk metadata from group 0, but the
**live health** (does the disk exist, is it readable, is it the right
size) must be probed locally. Without this, diskdb cannot detect a
disk that has gone missing, changed size, or started returning I/O
errors.

The design doc states: "Source of truth for disk identity/capacity is
group 0; live health is probed locally." This means:
- Group 0 stores the **expected** disk metadata (UUID, capacity, type,
  zone count) — written by the operator or a future coordinator.
- diskdb probes the **actual** disk locally and compares against group
  0's expected state.
- If a disk is missing, has wrong capacity, or fails a basic I/O test,
  diskdb transitions it to the appropriate state (Missing, Bad) and
  updates group 0.

The aioss reference does not have disk discovery/probing code (it
relies on the separate `diskio` service). This is new CROW-specific
work. The design doc also specifies a disk failure detection +
recovery flow that "does not exist yet and must be designed as part of
this scope."

**Solution**: Implement config-driven disk discovery, health probing,
and the disk failure detection + recovery flow.

1. **Config-driven disk discovery** — create
   `app/crow-diskdb/src/discovery/mod.rs`:
   - v1 uses **config-driven discovery** (not `/dev` scanning). The
     operator specifies disk paths in the diskdb config or in group 0
     `DiskMeta`. Each disk entry has: `disk_uuid`, `path` (e.g.
     `/dev/sda` or `/data/disk1`), `disk_type`, `capacity_bytes`
     (expected).
   - `DiskDiscovery` — reads the disk list from:
     a. Group 0 `DiskMeta` (the source of truth for disk identity).
     b. Local config override (optional: a config file mapping
        `disk_uuid → path` for the local node, since group 0 stores
        identity but not the OS-specific device path).
   - `discover_local_disks(node_uuid, config) -> Vec<LocalDiskInfo>`:
     - For each disk in the config/group-0 list, check if the path
       exists (`std::path::Path::exists()`).
     - Return `LocalDiskInfo { disk_uuid, path, exists: bool,
       actual_size_bytes: Option<u64>, disk_type }`.
   - v1 does **not** scan `/dev` for unknown disks — all disks must be
     declared in config/group 0. Future: `/dev` scan with udev/hwinfo
     to auto-discover disks (follow-up requirement).

2. **Health probing** — create
   `app/crow-diskdb/src/discovery/health.rs`:
   - `HealthProbe` — probes a single disk's health:
     - `probe_existence(path) -> bool` — `Path::exists()`.
     - `probe_size(path) -> Result<u64>` — get the disk's actual size.
       For a block device: `std::fs::metadata` or ioctl
       (`BLKGETSIZE64`). For a file-backed disk (testing): file size.
       Use `tokio::task::spawn_blocking` for the ioctl/fs call (it's
       a blocking syscall).
     - `probe_basic_io(path) -> Result<()>` — write a small test
       block (e.g. 4 KB) to a reserved offset (e.g. the last block of
       the disk, or a dedicated probe zone), read it back, verify
       content. This is a **basic** test — not a full surface scan.
       Use `spawn_blocking` for the I/O. Clean up the test block
       after.
       - **Safety**: the probe must not overwrite allocated data. Use
         a reserved offset (e.g. offset 0, or a dedicated probe zone
         index 0 that is never allocated). v1: skip the I/O test if
         the disk is in use (has active zones); only test on first
         discovery or after a failure.
   - `probe_disk(disk: &DiskMeta, local: &LocalDiskInfo) ->
     HealthResult`:
     a. If `!local.exists`: return `HealthResult::Missing`.
     b. If `local.actual_size != Some(disk.capacity_bytes)`: return
        `HealthResult::SizeMismatch { expected, actual }`.
     c. Run `probe_basic_io()`. If it fails: return
        `HealthResult::Bad { error }`.
     d. Return `HealthResult::Healthy`.
   - `HealthResult` enum: `Healthy`, `Missing`, `SizeMismatch { ... }`,
     `Bad { error }`, `Suspect { error }` (for intermittent errors).

3. **Health probe loop** — integrate into the sync loop (R71) or as a
   separate background task:
   - `health_probe_loop(node_container, discovery, config)`:
     a. `sleep(health_probe_interval_secs)` (default: same as sync
        interval, 13 s, or configurable separately).
     b. For each owned node, for each disk:
        - `probe_disk()` — check existence, size, basic I/O.
        - If health changed (Healthy → Missing/Bad): update
          `DiskState` in the in-memory `ZoneDisk`, update `DiskMeta`
          in group 0 (via `SysdataClient`), trigger failure handling.
        - If health recovered (Missing → Healthy): update state,
          trigger recovery.
     c. Log a health summary.
   - **Probe frequency**: probing every 13 s for all disks may be
     expensive (the basic I/O test writes + reads). v1: probe
     existence + size on every tick (cheap), run the I/O test only:
     - On first discovery (disk just added).
     - After a failure (disk was Missing/Bad, check if it recovered).
     - Periodically (e.g. every 10th tick = ~2 min) for healthy
       disks.
     - Configurable via `health_probe_io_test_interval` (default:
       every 10 ticks).

4. **Disk failure detection + recovery flow** — create
   `app/crow-diskdb/src/discovery/recovery.rs`:
   - `DiskFailureHandler` — handles disk state transitions and
     triggers recovery actions:
   - **On disk failure** (Healthy → Missing/Bad):
     a. Transition `DiskState` to `Missing` or `Bad` in-memory.
     b. Update `DiskMeta` in group 0 (via `SysdataClient`).
     c. Transition the disk's `Status` to `TempFailure` (via R71's
        `StatusManager`). This prevents new allocations on the disk
        (effective status = TempFailure → `allows_allocate() = false`).
     d. The disk's zones are marked `ZoneState::Missing` or
        `ZoneState::Bad` in-memory. Zones in the active deque are
        removed (they are no longer allocatable).
     e. Log the failure with details (disk_uuid, path, error).
     f. **Impacted-blocks scan** (§10, updated): scan the zone
        records for the bad disk (`DataGroupClient::
        read_zone_records` per zone, §7) and collect all live
        `BusyBlockValue`s — these are the impacted blocks. Each
        carries `owner_chunk` (the chunk that owns the allocation)
        so the caller / data-IO layer can be notified. Emit the
        `disk.bad.impacted_blocks` gauge (§11; the metric handle
        already exists in `DiskdbMetrics` from R72) and log the
        hand-off. The collected list is handed to a future
        recovery/relocation path: the data-IO layer rebuilds from
        EC/mirror, or the owner is notified to re-allocate
        elsewhere.
     g. **Recovery action**: v1 does **not** automatically migrate
        data off a failed disk (there is no data I/O service — diskdb
        only allocates). The disk's existing allocations remain in
        the journal; callers that hold `Segment`s for the failed disk
        will encounter I/O errors when they try to write (that's the
        caller's problem, not diskdb's). diskdb's job is to stop
        allocating on the failed disk and report the failure.
   - **On disk recovery** (Missing/Bad → Healthy):
     a. Transition `DiskState` to `Active` in-memory.
     b. Update `DiskMeta` in group 0.
     c. Transition `Status` to `Online` (via `StatusManager`).
     d. Mark zones as `ZoneState::Healthy`.
     e. Rebuild the active zone deque (`rebuild_active_zones()`).
     f. Run R73's `recover_zone()` for each zone on the recovered
        disk — the journal may have entries from before the failure
        that need to be replayed (if the disk was temporarily
        unavailable and allocations were rolled back).
     g. Log the recovery.
   - **On disk size mismatch**:
     a. Log a warning. Size mismatch usually means the wrong disk is
        at the path (operator error) or the disk was replaced with a
        different model.
     b. Transition to `Bad` (do not allocate on a disk with wrong
        capacity — the zone layout assumes the configured capacity).
     c. Update group 0. Operator must investigate and fix the config
        or replace the disk with the correct one.
   - **On disk added** (new disk in config/group 0 not previously
     known):
     a. Probe health. If healthy: create `ZoneDisk`, create zones
        (zone_count = capacity / zone_size), add to node, update
        group 0 with initial `DiskMeta`.
     b. If the disk is new (no journal entries): zones start fresh
        (`allocate_pos = 0`, empty bitmap). No recovery needed.
   - **On disk removed** (disk in group 0 no longer in config):
     a. Transition to `Offline`. Remove from `allocating_disks`.
     b. Do not delete journal entries — the disk may be re-added
        later. The operator can manually clean up via admin RPC.
   - `DiskFailureHandler` is called by the health probe loop when
     health changes are detected.

5. **Zone health integration** — update
   `app/crow-diskdb/src/zone/mod.rs` (from R72):
   - `Zone::set_zone_state(new_state: ZoneState)` — update
     `zone_state` (under `RwLock`). Called by the health probe loop.
   - `Zone::is_allocatable()` already checks `zone_state == Healthy`
     (from R72). When a disk goes Missing/Bad, all its zones become
     non-allocatable automatically.
   - When a disk recovers, zones are set back to `Healthy` and
     `rebuild_active_zones()` re-adds them to the deque.

6. **Admin RPCs** — add to `app/crow-diskdb/src/grpc/admin.rs`:
   - `ProbeDisk` — manually trigger a health probe for a specific
     disk. Returns `HealthResult`.
   - `GetDiskHealth` — return current health status for a disk (or
     all disks).
   - `AddDisk` / `RemoveDisk` (from R71) — now fully functional with
     health probing (add disk → probe → create zones → update group 0;
     remove disk → transition offline → update group 0).
   - Add corresponding proto messages.

**Scope** (expected changed files):
- `app/crow-diskdb/src/discovery/mod.rs` — `DiskDiscovery`,
  config-driven disk list.
- `app/crow-diskdb/src/discovery/health.rs` — `HealthProbe`,
  `probe_disk()`, `HealthResult`.
- `app/crow-diskdb/src/discovery/recovery.rs` — `DiskFailureHandler`,
  failure/recovery flow.
- `app/crow-diskdb/src/zone/mod.rs` — add `set_zone_state()`.
- `app/crow-diskdb/src/grpc/admin.rs` — `ProbeDisk`,
  `GetDiskHealth`, update `AddDisk`/`RemoveDisk`.
- `app/crow-diskdb/src/lib.rs` — add `discovery` module.
- `app/crow-diskdb/src/config/mod.rs` — add `DiscoveryConfig`
  (disk paths, probe intervals).
- `lib/crow-protocol/src/proto/diskdb.proto` — add `ProbeDisk`,
  `GetDiskHealth` RPCs and messages.
- `app/crow-diskdb/src/main.rs` — spawn health probe loop,
  wire `DiskFailureHandler`.
- `app/crow-diskdb/src/sync/mod.rs` (from R71) — integrate health
  probe with sync loop (or run as separate task).

**Complexity**: Medium-High. The health probing itself is
straightforward (existence check, size check, basic I/O test). The
complexity is in the failure/recovery flow design — the design doc
says "the disk-recovery flow does not exist yet and must be designed
as part of this scope." v1 takes a conservative approach: detect
failures, stop allocating, report, and let the operator/caller handle
data recovery. Automatic data migration is a future requirement (it
needs the data I/O service that does not exist yet). The integration
with R71's status management and R73's recovery engine requires
careful coordination.

**Dependencies**: R70 (types, config), R71 (SysdataClient,
StatusManager, NodeContainer), R72 (ZoneDisk, Zone), R73
(RecoveryEngine). No dependency on R77.

**Acceptance**:
- `DiskDiscovery` reads the disk list from config and/or group 0.
  Unit test: config with 3 disks, verify `discover_local_disks()`
  returns 3 `LocalDiskInfo` entries with correct `exists` flags.
- `HealthProbe.probe_existence()` correctly detects present/missing
  paths. Unit test with tempfile.
- `HealthProbe.probe_size()` returns correct size for a file-backed
  disk. Unit test with tempfile of known size.
- `HealthProbe.probe_basic_io()` writes, reads back, and verifies a
  test block. Unit test with tempfile. Returns error on I/O failure
  (e.g. read-only path).
- `probe_disk()` returns `Missing` for non-existent path,
  `SizeMismatch` for wrong size, `Bad` for I/O failure, `Healthy` for
  a good disk. Unit tests for each case.
- Health probe loop runs periodically and updates `DiskState` on
  health changes. Integration test: remove a disk file, wait for
  probe, verify disk transitions to `Missing` in-memory and in group
  0.
- `DiskFailureHandler` transitions disk to `TempFailure` on failure,
  stops allocations on that disk. Integration test: fail a disk,
  attempt allocate, verify it routes to other disks (or returns
  `NoSpace` if all disks failed).
- Disk recovery: re-create the disk file, wait for probe, verify disk
  transitions back to `Active`/`Online`, zones re-added to active
  deque.
- `AddDisk` admin RPC: add a new disk, verify health probe runs,
  zones created, group 0 updated. Integration test.
- `ProbeDisk` admin RPC: manually probe a disk, returns
  `HealthResult`. Integration test.
- `pixi run cargo fmt --all -- --check` and
  `pixi run cargo clippy --all-targets -- -D warnings` clean.
- Relevant tests pass.
