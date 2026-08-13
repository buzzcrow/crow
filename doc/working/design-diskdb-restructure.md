<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# diskdb Module Restructure + Domain Rename + Lifecycle Refactor

Working design draft for the `app/crow-diskdb` restructure derived from the
first-round code review (`doc/working/review-diskdb.md`, comments C1–C14). The
review found that the module/file structure does not surface the diskdb domain
concepts, mixes domain and infrastructure layers, and bundles unrelated
concerns into vaguely named files. This doc specifies the target module tree,
the renames, the layer split, the status-machine + keep-alive redesign, the
background-task framework, the startup lifecycle, and the config system.

Root design: `doc/design/diskdb/design-crow-diskdb.md` (architecture, group-0
sysdata, zone records, three recovery strategies, allocation, state machines,
metrics). R71 (sync loop, `HardwareClient`, `ServiceRegistryClient`,
`NodeContainer`) and R72 (`DataGroupClient`, `Zone` allocator, record
persistence, two-phase allocate/free) and R73 (crash recovery + compaction) are
already landed in `app/crow-diskdb/src/` — this doc references the actual code
paths. Architecture decisions and rationale are in the root design; this doc
does not repeat them.

This is a restructure, not a new feature — no protocol changes, no new RPCs,
no on-disk format changes. The behavior of every existing path is preserved;
only the module layout, type names, and internal wiring change. The one
behavioral addition is the startup lifecycle (C9: service-up-first + background
recovery + phase-gated RPCs).

---

## 1. Target module tree

The diskdb server has four concept groups (see `review-diskdb.md` §"Target
module structure"). The target tree:

```
app/crow-diskdb/src/
├── lib.rs                      # pure index: pub mod + pub use
├── main.rs                     # entry point: wire phases, start bg tasks, serve
│
├── domain.rs                   # pure index (R6): pub mod + pub use re-exports
├── domain/
│   ├── disk_group.rs           # DdbDiskGroup — per-disk-group manager + allocation
│   ├── disk_group_container.rs # DdbDiskGroupContainer — owns all DdbDiskGroups
│   ├── disk.rs                 # DdbDisk — per-disk manager + zone rotation
│   ├── zone.rs                 # DdbZone + DdbZoneHealth + AllocatedRange
│   ├── alloc.rs                # allocate/free orchestration + AllocError/FreeError/AllocClaim
│   └── records.rs              # BusyRecord/FreeRecord/ZoneRecords (read-models of protocol keys)
│
├── status_machine.rs           # HwStateMachine — transitions + per-state dispatch (Op enum)
│
├── keepalive.rs                # KeepAlive — group-0 sync + state-machine driver
│
├── recovery.rs                 # pure index (R6): pub mod + orchestrator (RecoveryEngine)
├── recovery/
│   ├── full_scan.rs            # strategy 1 — rebuild from live BusyBlockKeys
│   ├── journal_replay.rs       # strategy 2 — snapshot + journal replay
│   ├── compaction.rs           # strategy 3 — bg free-record merge into snapshot
│   └── disk_recovery.rs        # (future) disk recovery triggered by state-machine change
│
├── data_group_client.rs        # DataGroupClient — KV I/O on the bound data group (infra)
│
├── bg_task.rs                  # BackgroundTask trait + BgRunner + Trigger
│
├── lifecycle.rs                # StartupPhase (Init→Syncing→Recovering→Up)
│
├── service.rs                  # pure index (R6): pub mod + pub use
├── service/
│   └── diskdb_service.rs       # DiskdbService gRPC impl
│                               # future: diskio_service.rs, chunkdb_service.rs
│
├── ddb_config.rs               # DdbConfig + load_from_file + validate
│
└── metrics.rs                  # DiskdbMetrics registration
```

### 1.1 Why

The current tree (`config.rs`, `grpc.rs`, `metrics.rs`, `node.rs`, `node/`,
`persistence.rs`, `recovery.rs`, `recovery/compaction.rs`, `status.rs`,
`sync.rs`, `zone.rs`) violates the module-design rules synthesized in
`review-diskdb.md` (R1–R10):

- **Kind/transport names** — `grpc.rs`, `persistence.rs`, `sync.rs`,
  `status.rs` name categories, not subjects (R1).
- **Borrowed/legacy terms** — `Node`/`NodeContainer`/`ZoneDisk`/`Zone` don't
  match the domain vocabulary (disk-group / disk / zone) and clash with the
  `crow-protocol` type family (R2, R9).
- **Scattered domain model** — the disk-group/disk/zone model is spread across
  `node.rs`, `node/`, `zone.rs`, and `persistence.rs` (R3).
- **Mixed layers** — `persistence.rs` holds both `DataGroupClient` (infra) and
  `FreeError`/`allocate_blocks`/`BusyRecord` (domain) (R4).
- **Hidden structure** — the three recovery strategies are invisible in the
  file layout (R5); multiple future gRPC services have no scalable home (R5).
- **Overloaded files** — `sync.rs` does four unrelated things (R8).

The target tree fixes all of the above: domain gathered in `domain/`,
infrastructure named by subject, recovery strategies as sibling files, one
file per gRPC service, keep-alive and status machine as distinct named modules.

### 1.2 Dependency direction

```
domain  ──────────────► data_group_client (infra interface)
  ▲                        │
  │                        │
status_machine             │
keepalive ──► status_machine
  │
  ▼
recovery ──► domain, data_group_client
service  ──► domain, data_group_client, recovery, lifecycle
lifecycle ─► domain (phase state)
main     ──► everything (wiring only)
```

Domain never imports a runtime/infra module directly (R4). `data_group_client`
is the one infra module the domain's orchestration (`domain/alloc.rs`) needs.
Two options, decided in §6:

- **(a)** accept the one domain→infra edge (`domain/alloc.rs` imports
  `crate::data_group_client::DataGroupClient`);
- **(b)** define a `DurableStore` trait the client satisfies, so
  `domain/alloc.rs` depends on the trait, not the concrete client.

Default is (a) for simplicity; escalate to (b) only if the coupling causes
test friction.

---

## 2. Domain rename + gather (C1–C4, C10, C11)

### 2.1 Type renames

| Current | Target | Notes |
| --- | --- | --- |
| `Node` | `DdbDiskGroup` | per-disk-group manager (`node.rs`) |
| `NodeContainer` | `DdbDiskGroupContainer` | owns all disk-groups (`node/container.rs`) |
| `ZoneDisk` | `DdbDisk` | per-disk manager (`node/disk.rs`) |
| `Zone` | `DdbZone` | per-zone allocation-state manager (`zone.rs`) |
| `ZoneHealth` | `DdbZoneHealth` | in-memory zone health enum |
| `DiskdbConfig` | `DdbConfig` | config root (`config.rs` → `ddb_config.rs`, §8) |

Identity fields stay unprefixed (they refer to the real physical thing and are
protocol types): `node_id: NodeId`, `rack_id: RackId`, `disk_id: DiskId`,
`disk_group_id: DiskGroupId`.

### 2.2 Container method renames (C2)

`DdbDiskGroupContainer`:
- `add_node` → `add_disk_group`
- `remove_node` → `remove_disk_group`
- `get_node` → `get_disk_group`
- `node_ids` → `disk_group_ids`
- field `nodes` → `disk_groups`

### 2.3 File moves (C10, C11)

| Current file | Target file | Content |
| --- | --- | --- |
| `src/node.rs` | `src/domain/disk_group.rs` | `DdbDiskGroup` + impl + `AllocError` (moves to `domain/alloc.rs`) |
| `src/node/container.rs` | `src/domain/disk_group_container.rs` | `DdbDiskGroupContainer` |
| `src/node/disk.rs` | `src/domain/disk.rs` | `DdbDisk` |
| `src/zone.rs` | `src/domain/zone.rs` | `DdbZone` + `DdbZoneHealth` + `AllocatedRange` |
| `src/persistence.rs` (domain parts) | `src/domain/alloc.rs` | `allocate_block`/`allocate_blocks`/`free_block`/`free_blocks` + `FreeError` + `AllocError` + `AllocClaim` |
| `src/persistence.rs` (record types) | `src/domain/records.rs` | `BusyRecord`/`FreeRecord`/`ZoneRecords` |
| `src/persistence.rs` (infra parts) | `src/data_group_client.rs` | `DataGroupClient` + `Bind` + KV I/O methods |

`src/domain.rs` is a pure index: `pub mod disk_group; pub mod
disk_group_container; pub mod disk; pub mod zone; pub mod alloc; pub mod
records;` + `pub use` re-exports of the public types.

Delete `src/node.rs`, `src/node/`, `src/zone.rs`, `src/persistence.rs`. Update
`src/lib.rs`: drop `pub mod node` + `pub mod zone` + `pub mod persistence`, add
`pub mod domain` + `pub mod data_group_client`.

### 2.4 How

a. Create `src/domain/` + `src/domain.rs` index.
b. Move + rename each file per the table above; apply the type/field/method
   renames inside each moved file.
c. Update all imports across `src/{main,grpc→service,sync→keepalive,recovery,
   status→status_machine}.rs`, `src/recovery/compaction.rs`, and tests
   (`tests/common/cluster.rs`, `tests/{zone_alloc,disk_alloc,diskdb_e2e,
   recovery}_test.rs`) to `crate::domain::{...}`.
d. `cargo fmt && cargo clippy -- -D warnings` + relevant tests.

- Edge cases:
  - `AllocError` currently lives in `node.rs`; it moves to `domain/alloc.rs`
    alongside `FreeError` so the allocation/free error model is in one place.
  - `AllocClaim` and `AllocatableDiskContext` type aliases move to
    `domain/alloc.rs` (or stay in `domain/disk_group.rs` if they read better
    there — decide during impl).
  - `ActiveZoneContext` stays a zone concept → `domain/zone.rs`.

---

## 3. Status machine (C13)

### 3.1 Why

`StatusManager` (`src/status.rs`) is a bag of static helpers: a
transition-legality table, a `max()` effective-status computation, two boolean
gates (`allows_allocate`/`allows_free`), and a suspect-timeout check. There is
no state-machine object that holds current state, validates + applies a
transition (legality + side-effects together), and dispatches per-state
behavior. The per-status side-effects are scattered: `disk.rs::
set_effective_status` marks zones `Bad` on entering `Bad`; `sync.rs` manually
calls `rebuild_allocating_disks` after a status change; the booleans are
checked at call sites instead of dispatched by state.

### 3.2 Target

`src/status_machine.rs` (rename from `src/status.rs`):

```rust
pub struct HwStateMachine {
    temp_failure_timeout: Duration,
}

/// Operations a status may permit or deny.
pub enum Op { Allocate, Free, Rebuild, Probe }

impl HwStateMachine {
    /// Validate + apply a transition, running entry side-effects.
    /// Returns the new status, or Err on an illegal transition.
    pub fn transition_disk(
        &self,
        disk: &DdbDisk,
        to: HwStatus,
    ) -> Result<HwStatus, IllegalTransition>;

    pub fn transition_disk_group(
        &self,
        dg: &DdbDiskGroup,
        to: HwStatus,
    ) -> Result<HwStatus, IllegalTransition>;

    /// Effective status = max(node, group, disk) — unchanged.
    pub fn effective_status(node: HwStatus, group: HwStatus, disk: HwStatus) -> HwStatus;
}

impl HwStatus {
    /// Per-state operation permission (replaces allows_allocate/allows_free).
    pub fn permits(self, op: Op) -> bool;
    /// Entry side-effects for a disk (replaces set_effective_status body).
    pub fn on_enter_disk(self, disk: &DdbDisk);
    /// Entry side-effects for a disk-group.
    pub fn on_enter_disk_group(self, dg: &DdbDiskGroup);
}
```

a. `transition_disk` validates legality (`is_legal_transition`, unchanged
   table), runs `to.on_enter_disk(&disk)` (the side-effects currently in
   `set_effective_status`), and returns the new status. The caller no longer
   touches `set_effective_status` or `rebuild_allocating_disks` directly.
b. `on_enter_disk(Bad)` marks all zones `DdbZoneHealth::Bad` (current
   `set_effective_status` body). `on_enter_disk(Up|Maintenance|Offline|...)`
   triggers `rebuild_allocating_disks` on the owning disk-group — the machine
   needs a back-reference or a callback to do this; simplest is for the caller
   (keep-alive loop, §4) to call `dg.rebuild_allocating_disks()` after
   `transition_disk` returns Ok, keeping the machine free of disk-group
   back-references. Decide during impl: machine-internal callback vs
   caller-responsible.
c. `permits(Op::Allocate)` replaces `allows_allocate`; `permits(Op::Free)`
   replaces `allows_free`; extensible to `Rebuild`/`Probe` for future gating.
d. `check_suspect_timeout` stays on the machine (it's a transition trigger,
   not a permission).

- Edge cases:
  - `Bad` is terminal — `is_legal_transition` already rejects `Bad` → anything
    and anything → `Bad`; the machine preserves this.
  - `Init` → `Up` is the normal first transition at startup; the keep-alive
    loop drives it after the first successful observation.
  - Illegal transition returns `Err(IllegalTransition)`; the keep-alive loop
    logs and keeps the current state (current behavior).

Move inline `#[cfg(test)] mod tests` → `tests/status_machine_test.rs`
(review rule 13).

---

## 4. Keep-alive loop (C14)

### 4.1 Why

`sync.rs` / `SyncLoop` is vaguely named and bundles four concerns: heartbeat,
ownership reconciliation, disk reconciliation, disk-add init. The name "sync"
doesn't say what's synced or why. The key insight: this loop is what *drives*
the `HwStateMachine` (§3) — each tick it observes the group-0 hardware view
and feeds transitions into the machine.

### 4.2 Target

`src/keepalive.rs` (rename from `src/sync.rs`):

```rust
pub struct KeepAlive {
    hw: HardwareClient,
    svc: ServiceRegistryClient,
    container: Arc<DdbDiskGroupContainer>,
    machine: HwStateMachine,
    config: KeepAliveConfig,
    missed_count: u32,
    kv: Option<DataGroupClient>,
    cas_retry_metric: Option<Arc<Counter>>,
}

pub struct KeepAliveConfig { /* interval, miss_threshold, zone_rotate_count, cas_retry_limit */ }
pub struct KeepAliveOutcome { /* groups_added, groups_removed, disks_added, disks_removed, status_changes, duration_ms */ }
```

a. `tick` (renamed from `sync_once`) is a thin orchestrator calling four
   named methods in order:
   1. `heartbeat(&mut self) -> Result<(), ...>` — keep-alive +
      missed-count/degraded logic.
   2. `observe_ownership(&self, ...) -> ...` — owner/bind map read +
      disk-group add/remove/bind-update.
   3. `observe_disks(&self, ...)` — per-disk-group disk add/status/remove
      (renamed from `reconcile_disks`).
   4. `disk_add_init(&self, ...)` — create `DdbDisk` + `DdbZone`s, write
      baseline `ZoneValue`s, rebuild active zones. Consider moving to
      `domain/disk.rs` or `domain/alloc.rs` (constructs the domain model, not
      group-0 I/O) — decide during impl.
b. **State-machine driving (the core change):** in `observe_disks`, when a
   disk's observed status differs from current, call
   `machine.transition_disk(&disk, observed)` (§3) — the machine validates
   legality + runs entry side-effects. The keep-alive loop no longer touches
   `set_effective_status` or `rebuild_allocating_disks` directly. Same for
   missing-disk detection → `machine.transition_disk(&disk, HwStatus::Missing)`.
c. `run` stays the loop driver (timer + stop signal), but becomes a
   `BackgroundTask` impl (§5) instead of a raw `tokio::spawn`.

- Edge cases:
  - Heartbeat failure → missed-count → degraded mode (unchanged); the
    `KeepAliveOutcome` records the failure.
  - New disk-group assigned → `observe_ownership` adds it with `Init` status;
    the next `observe_disks` tick transitions it.
  - Disk-group removed → `observe_ownership` removes it from the container
    (no state-machine transition — removal is terminal).

---

## 5. Background-task framework (C5)

### 5.1 Why

Compaction is one background task (timer + threshold triggered). Future bg
tasks: health probing (R76), background scanner (R75), disk recovery (§9
future), ownership-transfer prep. Today each would duplicate the same
boilerplate: own a `DataGroupClient`, take the container, loop forever, respect
a stop signal, log failures, wire individually in `main.rs`.

### 5.2 Target

`src/bg_task.rs`:

```rust
pub struct BgCtx {
    pub container: Arc<DdbDiskGroupContainer>,
    pub kv: Arc<DataGroupClient>,
    pub metrics: DiskdbMetrics,
}

pub enum Trigger {
    Timer(Duration),
    Event(Arc<tokio::sync::Notify>),
    /// Predicate checked each iteration (e.g. threshold-based).
    Predicate(Box<dyn Fn() -> bool + Send + Sync>),
}

#[async_trait]
pub trait BackgroundTask: Send + Sync + 'static {
    /// One cycle of work. Called repeatedly per the trigger.
    /// Err on fatal; Ok on cycle-complete (loop continues).
    async fn run_cycle(&self, ctx: &BgCtx) -> Result<(), BgError>;
    fn trigger(&self) -> Trigger;
    fn name(&self) -> &'static str;
}

pub struct BgRunner {
    tasks: Vec<Arc<dyn BackgroundTask>>,
    stop: tokio_util::sync::CancellationToken,
}
```

a. `BgRunner` spawns each task with the shared `CancellationToken`; each task
   loops: wait on its trigger → `run_cycle(&ctx)` → log on err, continue on
   Ok, surface fatal err to the runner. On shutdown, cancel + await all.
b. `CompactionEngine` (§7) implements `BackgroundTask` with
   `Trigger::Timer(cadence)` + a threshold predicate checked each cycle.
c. `KeepAlive` (§4) implements `BackgroundTask` with `Trigger::Timer(interval)`.
d. Recovery (§7) is a one-shot startup task, not a `BackgroundTask` — it runs
   once via the runner's `spawn_once` path or directly in `main.rs` before
   registering the bg tasks.

- Edge cases:
  - A task's `run_cycle` panics → the runner catches via `tokio::spawn`'s
    `JoinHandle` and logs; the task is not restarted automatically (operator
    must restart the instance). Decide during impl whether to auto-restart
    with backoff.
  - `Trigger::Event` + `Trigger::Predicate` compose (compaction uses Timer +
    Predicate) — the runner checks both each iteration.

Keep minimal — don't over-engineer for tasks that don't exist yet. The trait +
runner + trigger enum + shared `BgCtx` is the irreducible shape.

---

## 6. Recovery restructure (C12)

### 6.1 Why

The three recovery strategies are documented in `recovery.rs`'s module doc but
invisible in the file layout: strategy 1 (`rebuild_zone_bitmap_full_scan`) and
strategy 2 (`recover_zone_inner`) are loose functions in one flat
`recovery.rs`; strategy 3 (`CompactionEngine`) is in a submodule. The
`RecoveryEngine` doesn't model the strategy choice.

### 6.2 Target

```
src/recovery.rs                 # pure index + RecoveryEngine (orchestrator) + RecoveryError + ZoneStats
src/recovery/full_scan.rs       # strategy 1 — rebuild_zone_bitmap_full_scan
src/recovery/journal_replay.rs  # strategy 2 — recover_zone_inner + merge_ops_by_slot + find_free_unit_count_at_slot
src/recovery/compaction.rs      # strategy 3 — CompactionEngine (stays; becomes a BackgroundTask per §5)
src/recovery/disk_recovery.rs   # (future) disk recovery triggered by state-machine change
```

a. `RecoveryEngine` (in `recovery.rs`) is the orchestrator: picks strategy 2
   first, strategy 1 fallback. `recover_node` → `recover_disk_group` (C1
   rename). Keeps `RecoveryError` + `ZoneStats`.
b. Move strategy 1 impl to `recovery/full_scan.rs`; strategy 2 impl + helpers
   to `recovery/journal_replay.rs`. `recovery.rs` becomes a pure index +
   orchestrator (R6).
c. Optional `RecoveryStrategy` enum (`FullScan` / `JournalReplay` /
   `Compaction`) to make the choice explicit in logs/metrics: "zone recovered
   via JournalReplay, fallback FullScan". Add if it reads cleanly; skip if it's
   just a label with no dispatch.
d. `zone_snapshots_exist` (fresh-vs-recovered decision helper) →
   `recovery/journal_replay.rs` (it's a strategy-2 precondition).
e. `disk_recovery.rs` is a placeholder for future disk-level recovery
   triggered by state-machine changes (a disk going `Bad` → rebuild its
   zones). Not built in this restructure; file created with a module doc +
   `// TODO` per the project's `doc/todo_code.md` convention.

- Edge cases:
  - Strategy 2 → strategy 1 fallback on `JournalScanGcGap` / `SnapshotCrcFail`
    — unchanged; the orchestrator calls strategy 1 when strategy 2 returns
    either error.
  - Strategy 1 also fails → empty zone (unchanged; logged).

---

## 7. Startup lifecycle (C9)

### 7.1 Why

Today startup is fully sequential and blocking: initial sync → blocking R73
recovery (all disk-groups/disks/zones in a `for` loop on the main task) → only
then gRPC starts. The service is not listening during recovery (looks hung to
operators); recovery runs serially on the main task.

### 7.2 Target

`src/lifecycle.rs`:

```rust
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartupPhase { Init = 0, Syncing = 1, Recovering = 2, Up = 3 }

pub struct LifecycleState(AtomicU8);

impl LifecycleState {
    pub fn get(&self) -> StartupPhase;
    pub fn set(&self, phase: StartupPhase);
}
```

a. `LifecycleState` is held on the `DdbDiskGroupContainer` (or a shared
   `Arc<LifecycleState>` passed to the service) — lock-free read on the hot
   path (`AtomicU8`).
b. **Service up first:** `main.rs` starts gRPC + HTTP immediately after
   `Init`/`Syncing`, *before* recovery. The service is reachable so health
   checks work during recovery.
c. **Recovery in background:** spawn recovery as a bg task (one per owned
   disk-group, or one task with bounded concurrency — aligns with §5). The
   main task does not block on it. Each disk-group transitions itself to `Up`
   as its recovery finishes (per-disk-group granularity).
d. **Health/readiness API:** expose the current `StartupPhase` via the health
   endpoint / a new `/ready` on the HTTP mgmt API. Each service's periodic
   health check returns the phase.
e. **Command gating:** `DiskdbService` rejects `AllocateBlocks`/`FreeBlocks`/
   `RebuildZoneBitmap` with `Status::unavailable("diskdb not ready:
   phase={phase}")` while the phase is not `Up` (or per-disk-group: reject if
   that disk-group's recovery isn't done). Read-only RPCs
   (`GetDiskGroupInfo`, `GetDiskInfo`) allowed earlier. This is the primary
   gate; `is_degraded()` stays as a runtime (post-`Up`) condition.

### 7.3 Startup flow (main.rs)

a. Load config → validate → build container + clients + metrics. Phase =
   `Init`.
b. Start gRPC + HTTP servers (phase-gated). Phase = `Syncing`.
c. Run initial keep-alive `tick` (blocking) — populates the container with
   owned disk-groups + disks + baseline zones.
d. Phase = `Recovering`. Spawn recovery as a bg task (one per disk-group or
   bounded). Each disk-group → `Up` as its recovery finishes.
e. When all disk-groups recovered → phase = `Up`.
f. Register keep-alive + compaction as bg tasks (§5) with the `BgRunner`.
g. Serve gRPC until shutdown signal → cancel bg tasks → await → exit.

- Edge cases:
  - A disk-group added after `Up` (keep-alive observes a new ownership) →
    its recovery runs on the next keep-alive tick; the per-disk-group ready
    flag gates RPCs for that disk-group only.
  - Recovery failure for a disk-group → that disk-group stays not-ready;
    RPCs for it return `unavailable`; other disk-groups serve normally.
  - `StartupPhase` is orthogonal to `HwStatus` (lifecycle vs runtime health);
    readiness = `StartupPhase::Up` && `!degraded` (or per-disk-group
    `HwStatus::Up`).

---

## 8. Config system (C6a, C7)

### 8.1 Scope note

C6 has three sub-items (C6a diskdb-only, C6b crow-common base, C6c
dynamic/static classification). This restructure covers **C6a + C7 only** —
the diskdb-local rename + JSON loader + sample file + gitignore fix. C6b and
C6c are separate follow-up tasks (they touch `crow-common` and
`crow-kv-server` config, out of scope for this restructure).

### 8.2 Target (C6a)

`src/ddb_config.rs` (rename from `src/config.rs`):

```rust
pub struct DdbConfig {
    pub server: ServerConfig,
    pub storage: StorageDefaults,
    pub heartbeat: HeartbeatConfig,
    pub persistence: PersistenceConfig,
    pub scanner: ScannerConfig,
    pub sync: SyncConfig,  // → keepalive config; field name stays for JSON compat
}

impl DdbConfig {
    pub fn load_from_file(path: &Path) -> Result<Self, serde_json::Error>;
}
```

a. Rename `DiskdbConfig` → `DdbConfig`; update `lib.rs`/`main.rs`/tests.
b. Add `DdbConfig::load_from_file` (mirror `CrowKVConfig::load_from_file`).
c. Ship `app/crow-diskdb/conf/ddb-config.sample.json` with the full default
   config. JSON doesn't allow comments — ship a paired
   `app/crow-diskdb/conf/ddb-config.sample.md` documenting every field's
   meaning + operational impact.
d. Audit per-field doc comments on every `DdbConfig` field (some exist;
   complete the rest).
e. Move inline `#[cfg(test)] mod tests` → `tests/config_test.rs` (review
   rule 13).

### 8.3 Gitignore fix (C7)

a. Remove `.gitignore:45` (`**/conf/` — too broad).
b. Add explicit per-crate runtime-conf paths (mirroring how `log/` is handled
   on lines 48–54): `app/crow-diskdb/conf/runtime/`,
   `app/crow-kv-server/conf/runtime/` (ignored, operator-supplied real
   configs).
c. `app/crow-diskdb/conf/` (tracked) holds `ddb-config.sample.json` +
   `ddb-config.sample.md`; `app/crow-diskdb/conf/runtime/` (ignored) holds the
   operator's real `ddb-config.json`.
d. Bench workspace `conf/` dirs stay ignored via the existing
   `app/crow-cli/bench-runs/` line (line 40).
e. Verify: `git check-ignore -v app/crow-diskdb/conf/ddb-config.sample.json`
   (not ignored) + `git check-ignore -v
   app/crow-diskdb/conf/runtime/ddb-config.json` (ignored).

---

## 9. gRPC service module (C8)

### 9.1 Why

`grpc.rs` is named after the transport (a kind), not the resource. The protos
define three services (`DiskdbService` current, `DiskioService` +
`ChunkdbService` future); the file name doesn't scale.

### 9.2 Target

```
src/service.rs              # pure index: pub mod diskdb_service; pub use diskdb_service::DiskdbService;
src/service/diskdb_service.rs  # current grpc.rs content (DiskdbService impl)
```

a. Move `src/grpc.rs` → `src/service/diskdb_service.rs`; create
   `src/service.rs` index.
b. Update `lib.rs` (`pub mod grpc` → `pub mod service`) + `main.rs` import.
c. The struct `DiskdbService` stays (matches proto service name); only the
   file name changes.
d. Future `DiskioService`/`ChunkdbService` → `src/service/diskio_service.rs`
   / `chunkdb_service.rs`.

---

## 10. Data-group client (C11 infra part)

`src/data_group_client.rs` (rename from the infra part of `persistence.rs`):

a. `DataGroupClient` + `Bind` + all KV I/O methods (`put_zone`,
   `read_zone_records`, `journal_scan_*`, `delete_free_records_batch`,
   `get_applied_slot`, `get_zone_value`, etc.).
b. The domain record types (`BusyRecord`/`FreeRecord`/`ZoneRecords`) move to
   `domain/records.rs`; the infra client imports them from `domain/`.
c. The allocate/free orchestration (`allocate_block`/`allocate_blocks`/
   `free_block`/`free_blocks`) moves to `domain/alloc.rs`; it takes
   `&DataGroupClient` as a param (the one domain→infra edge, §1.2).

---

## Scope

Grouped by crate. New and modified files; the reviewer's map of the diff.

### `app/crow-diskdb/` (all changes)

- `src/lib.rs` — drop `pub mod {config,grpc,node,persistence,status,sync,
  zone}`, add `pub mod {domain,data_group_client,bg_task,lifecycle,service,
  ddb_config,status_machine,keepalive,metrics}`.
- `src/main.rs` — rewrite startup flow per §7.3; update imports; wire
  `BgRunner`; phase-gated service.
- `src/domain.rs` — new, pure index.
- `src/domain/disk_group.rs` — from `node.rs`, `Node`→`DdbDiskGroup`.
- `src/domain/disk_group_container.rs` — from `node/container.rs`,
  `NodeContainer`→`DdbDiskGroupContainer`, method renames.
- `src/domain/disk.rs` — from `node/disk.rs`, `ZoneDisk`→`DdbDisk`;
  `set_effective_status` side-effects move to `HwStateMachine` (§3).
- `src/domain/zone.rs` — from `zone.rs`, `Zone`→`DdbZone`,
  `ZoneHealth`→`DdbZoneHealth`.
- `src/domain/alloc.rs` — new, from `persistence.rs` domain parts:
  allocate/free orchestration + `AllocError`/`FreeError`/`AllocClaim`.
- `src/domain/records.rs` — new, from `persistence.rs`: `BusyRecord`/
  `FreeRecord`/`ZoneRecords`.
- `src/status_machine.rs` — from `status.rs`, `StatusManager`→`HwStateMachine`
  + `Op` enum + `impl HwStatus` per-state dispatch (§3).
- `src/keepalive.rs` — from `sync.rs`, `SyncLoop`→`KeepAlive`, split `sync_once`
  → `tick` + 4 named methods, drive `HwStateMachine` (§4).
- `src/recovery.rs` — pure index + `RecoveryEngine` orchestrator;
  `recover_node`→`recover_disk_group`; strategies moved out.
- `src/recovery/full_scan.rs` — new, strategy 1 from `recovery.rs`.
- `src/recovery/journal_replay.rs` — new, strategy 2 + helpers from
  `recovery.rs`; `zone_snapshots_exist` moves here.
- `src/recovery/compaction.rs` — `CompactionEngine` becomes a
  `BackgroundTask` (§5); otherwise unchanged.
- `src/recovery/disk_recovery.rs` — new, placeholder for future disk recovery.
- `src/data_group_client.rs` — from `persistence.rs` infra parts:
  `DataGroupClient` + `Bind` + KV I/O.
- `src/bg_task.rs` — new, `BackgroundTask` trait + `BgRunner` + `Trigger`
  + `BgCtx` (§5).
- `src/lifecycle.rs` — new, `StartupPhase` + `LifecycleState` (§7).
- `src/service.rs` — new, pure index.
- `src/service/diskdb_service.rs` — from `grpc.rs`, `DiskdbService` impl +
  phase-gated RPCs (§7, §9).
- `src/ddb_config.rs` — from `config.rs`, `DiskdbConfig`→`DdbConfig` +
  `load_from_file` (§8).
- `src/metrics.rs` — unchanged.
- `conf/ddb-config.sample.json` — new, tracked sample config (§8).
- `conf/ddb-config.sample.md` — new, tracked field documentation (§8).
- `conf/runtime/` — new, ignored (operator-supplied real config) (§8).
- Delete: `src/config.rs`, `src/grpc.rs`, `src/node.rs`, `src/node/`,
  `src/persistence.rs`, `src/status.rs`, `src/sync.rs`, `src/zone.rs`.
- `tests/common/cluster.rs` — update imports + type names.
- `tests/zone_alloc_test.rs` — update imports + type names.
- `tests/disk_alloc_test.rs` — update imports + type names.
- `tests/diskdb_e2e_test.rs` — update imports + type names + startup-flow
  expectations (phase-gated).
- `tests/recovery_test.rs` — update imports + type names.
- `tests/config_test.rs` — new, from `config.rs` inline tests (§8).
- `tests/status_machine_test.rs` — new, from `status.rs` inline tests (§3).
- `Cargo.toml` — add `tokio-util` (CancellationToken), `async-trait` (if not
  already a dep); no other dep changes.

### `.gitignore`

- Remove line 45 (`**/conf/`).
- Add `app/crow-diskdb/conf/runtime/`, `app/crow-kv-server/conf/runtime/`
  (§8.3).

### Out of scope (follow-up tasks)

- `lib/crow-common/rust/src/config.rs` — C6b (crow-common config base +
  file-watch). Separate task.
- `lib/crow-kv/src/common/config.rs` — C6c (dynamic/static field
  classification for both `DdbConfig` and `CrowKVConfig`). Separate task.
- `app/crow-kv-server/conf/` migration from `dist/` — optional, decide during
  C6b.

---

## Complexity

**Medium.** The restructure is mechanical (move + rename + import fix) for the
bulk of the change — no algorithmic difficulty, no protocol/format changes.
The genuinely hard parts are:

1. **Status-machine refactor (C13)** — moving scattered side-effects
   (`set_effective_status` + `rebuild_allocating_disks`) into a single
   `transition_disk` call without changing behavior. The transition table +
   effective-status math are reused; the risk is missing a side-effect or
   calling `rebuild_allocating_disks` at the wrong time.
2. **Startup lifecycle (C9)** — reordering `main.rs` so the service starts
   before recovery, recovery runs in the background, and RPCs are phase-gated.
   The risk is a race where an RPC arrives before the container is populated;
   the per-disk-group ready flag mitigates this.
3. **Background-task framework (C5)** — small but new abstraction; the risk is
   over-engineering. Kept minimal (trait + runner + trigger enum + shared
   ctx).

Everything else is rename/move/split with `cargo fmt && cargo clippy` +
existing tests as the safety net. No new tests are needed for the rename/move
parts; the status-machine + lifecycle + bg-task parts get new tests (§Test
Design).

---

## Test Design

### Unit tests (UT)

**Status machine** (`tests/status_machine_test.rs` — moved + expanded from
`status.rs` inline tests):
- Legal transitions (Init→Up, Up→Suspect, Suspect→Up, Suspect→Missing,
  Missing→Bad, Missing→Up, Offline→Maintenance, Maintenance→Offline,
  Offline→Up) — assert `transition_disk` returns Ok with the new status.
- Illegal transitions (Up→Init, Bad→Up, Up→Bad, Init→Suspect) — assert
  `transition_disk` returns `Err(IllegalTransition)` and the disk's status is
  unchanged.
- `on_enter_disk(Bad)` marks all zones `DdbZoneHealth::Bad` — setup a disk
  with 3 zones Up, transition to Bad, assert all zones Bad.
- `permits(Op::Allocate)` is true only for `Up`; false for Maintenance,
  Offline, Suspect, Missing, Bad, Init.
- `permits(Op::Free)` is true for Up, Maintenance, Suspect; false for
  Offline, Missing, Bad, Init.
- `effective_status` max computation — unchanged from current tests.

**Lifecycle** (`tests/lifecycle_test.rs` — new):
- `LifecycleState` get/set — set each phase, assert get returns it.
- Concurrent get/set — spawn 10 readers + 1 writer, assert no panic, readers
  see a valid phase (AtomicU8 correctness).

**Background task** (`tests/bg_task_test.rs` — new):
- `BgRunner` runs a mock task with `Trigger::Timer(10ms)` for 3 cycles, then
  cancels — assert `run_cycle` called 3 times, no panic on shutdown.
- `BgRunner` with `Trigger::Predicate(|| true)` — assert immediate cycle.
- Fatal `run_cycle` error → runner logs + task stops; other tasks continue.

**Config** (`tests/config_test.rs` — moved from `config.rs` inline tests):
- Existing validation tests (block size range, power-of-two, zone multiple of
  block, granularity, listen addr, zero intervals) — unchanged.
- `load_from_file` — write a temp JSON, load, assert fields match.

### End-to-end tests (E2E)

**Existing E2E** (`tests/diskdb_e2e_test.rs`, `tests/recovery_test.rs`,
`tests/zone_alloc_test.rs`, `tests/disk_alloc_test.rs`):
- All existing E2E tests must pass unchanged after the restructure (behavior
  preserved). Update imports + type names only.

**New E2E — startup lifecycle** (`tests/lifecycle_e2e_test.rs` — new):
- Start a diskdb instance with a populated data group (snapshots exist) →
  gRPC server is reachable *during* recovery (a `GetDiskGroupInfo` RPC
  succeeds while phase is `Recovering`) → `AllocateBlocks` returns
  `unavailable` until phase is `Up` → after recovery, `AllocateBlocks`
  succeeds. Proves the service-up-first + phase-gated RPC invariant.
- Per-disk-group readiness: two disk-groups, one recovers fast, one slow →
  `AllocateBlocks` for the fast one succeeds before the slow one is ready →
  `AllocateBlocks` for the slow one returns `unavailable` until its recovery
  finishes. Proves per-disk-group granularity.

**New E2E — status machine driving** (`tests/keepalive_e2e_test.rs` — new):
- Start a diskdb instance, mark a disk `Maintenance` in group 0 → next
  keep-alive tick transitions the disk via `HwStateMachine` →
  `AllocateBlocks` for that disk-group skips the disk (anti-affinity) →
  `permits(Op::Allocate)` is false for the disk. Proves the keep-alive loop
  drives the state machine and the machine's `permits` gates allocation.

---

## Module Structure

See §1 (target tree) + §Scope (file-by-file changes).

## Config Extensions

- `DdbConfig::load_from_file` (new method, mirrors `CrowKVConfig`).
- `conf/ddb-config.sample.json` (new tracked sample).
- `conf/ddb-config.sample.md` (new tracked field documentation).
- No new config fields in this restructure — C6c (dynamic/static) is
  follow-up.
- `.gitignore`: remove `**/conf/`, add explicit `conf/runtime/` paths.

## Server Wiring

See §7.3 (startup flow). The `main.rs` rewrite is the single largest change:
load config → build container + clients + metrics → start gRPC/HTTP
(phase-gated) → initial keep-alive tick → spawn recovery (bg, per-disk-group)
→ phase `Up` → register keep-alive + compaction bg tasks → serve until
shutdown.

## Open Questions

1. **Domain→infra edge for `domain/alloc.rs`** — accept the direct import of
   `DataGroupClient` (option a, simple) or define a `DurableStore` trait
   (option b, decoupled). Default: (a). Escalate to (b) only if test friction
   appears. *Cannot be resolved automatically — it's a coupling-vs-simplicity
   trade-off; default to simple unless the reviewer prefers the trait.*

2. **`on_enter_disk` side-effects: machine-internal vs caller-responsible** —
   should `HwStateMachine::transition_disk` call
   `dg.rebuild_allocating_disks()` internally (needs a back-reference or
   callback) or should the keep-alive caller do it after `transition_disk`
   returns Ok? Default: caller-responsible (keeps the machine free of
   disk-group back-references). *Trade-off: machine encapsulation vs
   caller boilerplate; default to caller-responsible.*

3. **`disk_add_init` location** — stays in `keepalive.rs` (it's called from
   the keep-alive tick) or moves to `domain/disk.rs`/`domain/alloc.rs` (it
   constructs the domain model)? Default: move to `domain/` (it's domain
   construction, not group-0 I/O); the keep-alive loop calls it. *Cannot be
   resolved automatically — depends on whether the baseline `ZoneValue` write
   (which needs `DataGroupClient`) stays in the function or is split out.*

4. **`RecoveryStrategy` enum** — add it (explicit strategy in logs/metrics)
   or skip (just a label, no dispatch)? Default: skip unless it reads cleanly
   with real dispatch. *Minor; decide during impl.*

5. **`disk_recovery.rs` placeholder** — create the file now (with module doc +
   `// TODO`) or defer until the feature is designed? Default: create the
   placeholder so the module tree shows the future home. *Minor.*
