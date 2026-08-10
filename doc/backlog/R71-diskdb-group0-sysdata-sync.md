<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R71: diskdb — Group-0 Sysdata Schema + Sync Loop + Disk Status Management

**Problem**: R70 defines the core types and group-0 sysdata key layout,
but there is no code to read from or write to group 0, no sync loop,
and no disk status management. diskdb is a "thin, stateless client of
crow-kv" — on startup it must fetch its assigned disk-groups, their
disks, and the ownership/binding maps from group 0, and it must
periodically re-sync to detect ownership changes, new disks, and
status updates. Without this, diskdb cannot know which disk-groups it
owns or which paxos data group to write zone journals to.

The design doc (§5, §10) specifies the group-0 sysdata schema and the
sync loop at 13 s intervals. The aioss reference has a `refresh/mod.rs`
with a `ping_loop` that syncs with metadb and enters degraded mode
after 3 missed pings — this pattern maps to CROW but the target is
group 0 (store 0, group 0) via `crow-kv-client`, not a separate
metadb service.

**Solution**: Implement the first major component — disk status
management — which talks with group 0 to sync metadata and manage
disk/disk-group/node status transitions.

1. **Group-0 sysdata read/write** — create
   `lib/crow-diskdb/src/sysdata/` module:
   - `SysdataClient` — wraps `CrowkvClient` (from `crow-kv-client`)
     with diskdb-specific read/write helpers targeting group 0 (store
     0, group 0). All reads use `get` or `scan` (prefix scan); all
     writes use `put` or `batch_write`.
   - `read_node(node_uuid) -> Result<Option<NodeMeta>>` — get from
     `/diskdb/node/{node_uuid}/meta`.
   - `read_all_nodes() -> Result<Vec<NodeMeta>>` — prefix scan
     `/diskdb/node/`.
   - `read_disk_group(node_uuid, dg_index) -> Result<Option<DiskGroupMeta>>`.
   - `read_all_disk_groups() -> Result<Vec<DiskGroupMeta>>` — prefix
     scan `/diskdb/node/` and for each node scan its disk-groups, or
     scan a flat `/diskdb/dg/` prefix if the key layout is flattened.
     Decide during implementation: the design doc §5 uses nested keys
     (`/diskdb/node/{uuid}/dg/{index}/meta`), so a two-level scan is
     needed. Consider a secondary flat index key
     `/diskdb/dg/{dg_id}/meta` for O(1) lookup by disk-group ID —
     decide during design.
   - `read_disk(node_uuid, disk_uuid) -> Result<Option<DiskMeta>>`.
   - `read_disks_for_node(node_uuid) -> Result<Vec<DiskMeta>>` —
     prefix scan `/diskdb/node/{uuid}/disk/`.
   - `read_owner_map() -> Result<HashMap<DiskGroupId, OwnerEntry>>` —
     prefix scan `/diskdb/map/owner/`. `OwnerEntry` = `{instance_id,
     lease_expiry_ms}`.
   - `read_bind_map() -> Result<HashMap<DiskGroupId, BindEntry>>` —
     prefix scan `/diskdb/map/bind/`. `BindEntry` = `{store_id,
     group_id}`.
   - `read_instance(instance_id) -> Result<Option<InstanceMeta>>`.
   - `write_node_meta(node: &NodeMeta) -> Result<()>` — `put` to
     group 0.
   - `write_disk_group_meta(dg: &DiskGroupMeta) -> Result<()>`.
   - `write_disk_meta(disk: &DiskMeta) -> Result<()>`.
   - `write_instance_heartbeat(instance: &InstanceMeta) -> Result<()>`
     — updates `last_heartbeat_ms` on every sync tick.
   - `write_owner_entry(dg_id, entry: &OwnerEntry) -> Result<()>`.
   - `write_bind_entry(dg_id, entry: &BindEntry) -> Result<()>`.
   - All writes serialize via serde_json (matching the design doc's
     JSON value convention for sysdata). Values are small (< 1 KB).

2. **Sync loop** — create `lib/crow-diskdb/src/sync/` module:
   - `SyncLoop` — owns a `SysdataClient`, a `NodeContainer` (shared
     state), and a `SyncConfig`. Runs as a `tokio::spawn` background
     task.
   - `run()` — loop: `sleep(interval)` → `sync_once()` → repeat.
   - `sync_once() -> Result<SyncOutcome>`:
     a. Read the ownership map from group 0. Filter to entries where
        `instance_id == self.instance_id`. These are the disk-groups
        this instance owns.
     b. Read the binding map. For each owned disk-group, look up its
        `(store_id, group_id)` — the paxos data group for zone
        journals.
     c. For each owned disk-group, read its `DiskGroupMeta` and its
        member disks' `DiskMeta`. Build/update the in-memory
        `NodeContainer` state.
     d. Detect changes: new disk-groups assigned (add to container),
        disk-groups removed (remove from container), disks
        added/removed, status changes (apply transitions).
     e. Write instance heartbeat to group 0
        (`/diskdb/instance/{instance_id}`).
     f. Return `SyncOutcome` with counts: `groups_added`,
        `groups_removed`, `disks_added`, `disks_removed`,
        `status_changes`, `sync_duration_ms`.
   - **Degraded mode**: track `missed_count` of consecutive sync
     failures. After `miss_threshold` (default 3), enter degraded mode
     (`NodeContainer.enter_degraded_mode()`). In degraded mode,
     allocation/free RPCs return `Unavailable` (matching aioss
     pattern). On first successful sync, exit degraded mode
     (`exit_degraded_mode()`).
   - **Notify mechanism (deferred)**: the design doc §10 raises a
     zookeeper-like watch/notify as an open question. v1 uses polling
     at 13 s. If polling cost is acceptable (it is — group 0 reads are
     cheap prefix scans), defer notify to a follow-up. Record this
     decision in the design doc during implementation.

3. **Disk status management** — create
   `lib/crow-diskdb/src/status/` module:
   - `StatusManager` — applies status transitions and computes
     effective status. Integrated with the sync loop.
   - `apply_node_status(node_uuid, new_status)` — validates transition
     legality (design doc §9), updates `NodeMeta` in group 0, updates
     in-memory state.
   - `apply_disk_group_status(dg_id, new_status)` — same pattern.
   - `apply_disk_status(disk_uuid, new_status)` — same pattern.
   - `effective_status(node_status, group_status, disk_status) ->
     Status` — `max(node, group, disk)` (from R70's `Status` enum,
     which is `Ord`).
   - **Transition rules** (design doc §9):
     - Init → {Online, Offline, Maintenance} on startup (load from
       group 0).
     - Online → TempFailure (3 missed syncs).
     - Online → Offline / Maintenance (operator).
     - TempFailure → Online (sync recovers) or → Offline (15 min
       elapsed).
     - Offline ↔ Maintenance (operator).
     - Offline → Online (operator).
   - `check_temp_failure_timeouts()` — called on each sync tick;
     transitions any disk/disk-group/node that has been in
     TempFailure > `temp_failure_timeout_secs` (default 900 s / 15
     min) to Offline.
   - `allows_allocate(effective) -> bool` — Online only.
   - `allows_free(effective) -> bool` — Online, Maintenance,
     TempFailure.

4. **NodeContainer** — create `lib/crow-diskdb/src/node/` module:
   - `NodeContainer` — per-instance singleton managing all owned
     disk-groups. `nodes: RwLock<HashMap<DiskGroupId, Arc<Node>>>`,
     `instance_id: String`, `config: DiskdbConfig`, `degraded:
     AtomicBool`.
   - `add_node(node)`, `remove_node(dg_id)`, `get_node(dg_id) ->
     Option<Arc<Node>>`, `node_ids() -> Vec<DiskGroupId>`.
   - `enter_degraded_mode()` / `exit_degraded_mode()` /
     `is_degraded() -> bool` — atomic flag.
   - `Node` — disk-group manager. `id: DiskGroupId`, `disks:
     RwLock<HashMap<Uuid, Arc<ZoneDisk>>>`, `allocating_disks:
     RwLock<Vec<Arc<ZoneDisk>>>`, `disk_allocate_iterator:
     AtomicU32`, `status: RwLock<Status>`, `bind: (u64, u64)` (store_id,
     group_id for the bound paxos data group).
   - `ZoneDisk` — disk struct with `zones: Vec<ZoneRef>`,
     `active_zones: SegQueue<ZoneRef>`, `disk_state: RwLock<DiskState>`,
     `disk_type: DiskType`, `capacity_bytes`, `zone_capacity`,
     `granularity_shift`. Zone management methods (`add_zone`,
     `rebuild_active_zones`) are defined here but zone **allocation**
     logic (CAS claim) is R72.
   - v1: `ZoneDisk` is a single implementation for all disk types
     (BlockHdd, BlockSsd). SMR/SSD trait variants are stubbed
     (non-goal per design doc §2).

5. **Operator/admin gRPC handlers** — create
   `lib/crow-diskdb/src/grpc/admin.rs`:
   - `set_disk_status`, `set_disk_group_status`, `set_node_status` —
     delegate to `StatusManager`.
   - `add_disk`, `remove_disk` — update group 0 sysdata + in-memory
     state.
   - `get_node_info`, `get_disk_info` — read from in-memory state.
   - These are thin handlers; the gRPC service struct itself is
     created in this requirement (wiring the tonic-generated service
     trait). The allocate/free handlers are stubs (return
     `Unimplemented`) — R72 fills them in.

6. **Server binary skeleton** — create `app/crow-diskdb-server/`:
   - `Cargo.toml` — depends on `crow-diskdb`, `crow-kv-client`,
     `crow-common`, `crow-protocol`, `tonic`, `tokio`, `clap`,
     `tracing`, `serde`, `toml`.
   - `src/main.rs` — CLI (clap), config loading, wiring:
     `CrowkvClient` → `SysdataClient` → `NodeContainer` → `SyncLoop`
     → gRPC server. Spawns sync loop as background task, serves gRPC
     on `listen_addr`.
   - `src/lib.rs` — re-exports for integration tests.
   - The server starts, loads config, connects to crow-kv, runs an
     initial sync from group 0, starts the sync loop, and serves
     gRPC. Allocation/free RPCs return `Unimplemented` until R72.

**Scope** (expected changed files):
- `lib/crow-diskdb/src/sysdata/mod.rs` — `SysdataClient` with
  group-0 read/write helpers.
- `lib/crow-diskdb/src/sync/mod.rs` — `SyncLoop` with periodic sync
  and degraded mode.
- `lib/crow-diskdb/src/status/mod.rs` — `StatusManager` with
  transition rules and effective status.
- `lib/crow-diskdb/src/node/mod.rs` — `Node`, `NodeContainer`.
- `lib/crow-diskdb/src/node/container.rs` — `NodeContainer` impl.
- `lib/crow-diskdb/src/node/disk.rs` — `ZoneDisk` struct (zone
  allocation methods stubbed for R72).
- `lib/crow-diskdb/src/grpc/mod.rs` — gRPC service struct.
- `lib/crow-diskdb/src/grpc/admin.rs` — operator/admin handlers.
- `lib/crow-diskdb/src/lib.rs` — module declarations and re-exports.
- `lib/crow-diskdb/Cargo.toml` — add `crow-kv-client`, `crow-common`,
  `tokio` (full features for sync), `crossbeam-queue` (for SegQueue),
  `clap` (optional, or keep CLI in server binary only).
- `app/crow-diskdb-server/` — new crate: `Cargo.toml`, `src/main.rs`,
  `src/lib.rs`.
- `Cargo.toml` (workspace) — `app/crow-diskdb-server` already listed
  as a member; ensure it builds.

**Complexity**: Medium-High. The sync loop and status management are
well-specified in the design doc. The main integration work is wiring
`CrowkvClient` (which targets crow-kv's gRPC API) as the group-0
client — diskdb uses `put`/`get`/`scan`/`batch_write` on store 0,
group 0. The degraded-mode pattern and transition rules are directly
modeled on the aioss reference.

**Dependencies**: R70 (core types, config, key layout). R69 (design
doc, skeleton). No dependency on R72–R77 — this is the first
functional component.

**Acceptance**:
- `SysdataClient` reads and writes all group-0 sysdata types
  (`NodeMeta`, `DiskGroupMeta`, `DiskMeta`, `InstanceMeta`,
  `OwnerEntry`, `BindEntry`) via `CrowkvClient` targeting store 0,
  group 0. Unit tests with a mock/in-process crow-kv verify
  round-trip.
- `SyncLoop.sync_once()` fetches ownership map, binding map, and
  disk-group/disk metadata from group 0; updates `NodeContainer`;
  writes instance heartbeat. Returns `SyncOutcome` with change counts.
- Degraded mode activates after `miss_threshold` consecutive sync
  failures and deactivates on first success. Unit test verifies the
  counter and flag transitions.
- `StatusManager` enforces all transition rules (design doc §9):
  illegal transitions return an error; TempFailure timeout (15 min)
  transitions to Offline. Unit tests cover each legal and illegal
  transition.
- `effective_status()` correctly computes `max(node, group, disk)`.
- `NodeContainer` supports `add_node`/`remove_node`/`get_node` with
  `RwLock` concurrency; `enter_degraded_mode`/`exit_degraded_mode`
  via atomic flag.
- `app/crow-diskdb-server` compiles, starts, loads config, connects
  to crow-kv, runs initial sync, starts sync loop, and serves gRPC
  (admin RPCs functional; allocate/free return `Unimplemented`).
- `pixi run cargo fmt --all -- --check` and
  `pixi run cargo clippy --all-targets -- -D warnings` clean.
- Relevant tests pass (`pixi run clean-env && pixi run test-kv-core`
  unaffected; new tests in `lib/crow-diskdb` and
  `app/crow-diskdb-server` pass).
