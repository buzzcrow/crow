<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# crow-diskdb — Code Review Notes

Review target: `app/crow-diskdb/` (distributed disk-block allocator; sync loop, status
management, gRPC service stubs; allocation logic is R72).

Workflow: comments are recorded one by one below with a reference (file:line or
symbol). Each comment is refined into an actionable item. All items will be
addressed in a single pass after the review is complete — do not act on them
mid-review.

Reference checklist: `.devin/workflows/review.md` (hot-path rules, 16-point
checklist, pitfalls).

## Module / file design rules (synthesized from C1–C14)

These rules are derived from the review comments above. They generalize the
specific fixes into principles to apply going forward — not just in diskdb,
but as the project-wide convention for module/file layout.

### R1 — Name by subject, not by kind or transport
- A file/module name says **what thing** it holds, not **what category** it is.
- Bad (kind/transport): `grpc.rs`, `persistence.rs`, `sync.rs`, `status.rs`.
- Good (subject): `service/diskdb_service.rs`, `data_group_client.rs`,
  `keepalive.rs`, `status_machine.rs`.
- Already in the review checklist (rule 14 bans `types.rs`/`impl.rs`/`core.rs`/
  `misc.rs`); this extends it to transport/layer names (`grpc`, `rpc`, `persistence`,
  `sync`) and generic verbs (`status`).

### R2 — Name by the domain concept, not a borrowed/legacy term
- Use the term the domain actually uses. If the unit is a **disk-group**, the
  struct is `DdbDiskGroup`, not `Node` (C1). If it's a **disk**, it's `DdbDisk`,
  not `ZoneDisk` (C3). If it's a **zone**, it's `DdbZone`, not `Zone` (C4).
- Never reuse a name that a lower layer (e.g. `crow-protocol`) already owns for
  a different thing — prefix local manager types (`Ddb` prefix) to avoid
  `Zone` (protocol value) vs `Zone` (in-memory manager) confusion.

### R3 — One concept = one module; gather a cohesive model into one place
- Concepts that belong together live together. The diskdb **domain model**
  (`DdbDiskGroup` / `DdbDisk` / `DdbZone` + their allocation logic + domain
  errors + record read-models) goes in one `domain/` module (C10, C11), not
  scattered across `node.rs` / `node/` / `zone.rs` / `persistence.rs`.
- A reader should find the whole model in one place, separate from
  infrastructure (I/O, recovery, gRPC, config).

### R4 — Separate domain from infrastructure by layer
- **Domain** = the in-memory model + its invariants + orchestration logic
  (allocate/free, status transitions, record read-models).
- **Infrastructure** = transport/I/O wrappers (`DataGroupClient`), gRPC service
  wiring, config loading, metrics.
- Don't mix both in one file. `persistence.rs` mixed `DataGroupClient` (infra)
  with `FreeError`/`allocate_blocks`/`BusyRecord` (domain) — split them (C11).
- Dependency direction: domain may depend on an infra interface, infra depends
  on domain types; never the reverse unconstrained.

### R5 — File layout must surface the conceptual structure
- If the design has **three recovery strategies**, the file layout should show
  three strategy files (`full_scan.rs`, `journal_replay.rs`, `compaction.rs`),
  not one flat `recovery.rs` with the strategies hidden in functions (C12).
- If there are **multiple gRPC services**, there's a `service/` module with one
  file per service (C8), not one `grpc.rs`.
- The file tree is the first thing a reader sees — it should read like a table
  of contents of the concepts.

### R6 — `foo.rs` is a pure index; logic lives in `foo/<subject>.rs`
- A `foo.rs` that introduces a module contains only: module doc, `pub mod`,
  `pub use` re-exports. No headline types, no impl logic (review rule 14).
- `domain.rs` → `pub mod disk_group; pub mod disk; pub mod zone; pub use ...`.
- `service.rs` → `pub mod diskdb_service; pub use diskdb_service::DiskdbService;`.
- `recovery.rs` → `pub mod full_scan; pub mod journal_replay; pub mod compaction;`
  + the orchestrator (or orchestrator gets its own `recovery/engine.rs`).

### R7 — One file per resource/service, not one file per verb
- Handlers group by **resource**, not by **action** (review rule 15).
- `service/diskdb_service.rs`, `service/diskio_service.rs`,
  `service/chunkdb_service.rs` — one per proto service (C8).
- Not `allocate.rs` / `free.rs` / `query.rs` (verbs) under service.

### R8 — A module's responsibility must be nameable in one phrase
- If you can't name what a file does in one short subject phrase, it's doing
  too much. `sync.rs` did four things (heartbeat + ownership reconcile + disk
  reconcile + disk-add init) — none captured by "sync" (C14).
- Split until each file's responsibility is one phrase: `keepalive.rs`
  (drive the state machine via periodic observation), `status_machine.rs`
  (own hardware status transitions + per-state dispatch), `data_group_client.rs`
  (KV I/O on the bound data group).

### R9 — Prefix local types to avoid clashes with shared/protocol crates
- When a lower/shared crate (`crow-protocol`) owns a type family (`DiskId`,
  `DiskGroupKey`, `ZoneValue`, `HwStatus`), the local in-memory manager types
  get a project prefix (`Ddb`) to avoid name shadowing and reader confusion
  (C2, C3, C4): `DdbDiskGroup`, `DdbDisk`, `DdbZone`, `DdbZoneHealth`.
- Identity fields that refer to the real physical thing stay unprefixed:
  `node_id: NodeId`, `rack_id: RackId`, `disk_id: DiskId` (those are protocol
  types, correct as-is).

### R10 — The file tree separates "what it is" (domain) from "how it runs" (runtime)
- **Domain/** — what the system *is*: disk-groups, disks, zones, allocation
  model, status states, errors, record read-models.
- **Runtime modules** (top-level) — how it *runs*: `keepalive` (state-machine
  driver), `recovery/` (startup + bg strategies), `bg_task` (bg-task framework),
  `service/` (gRPC), `data_group_client` (KV I/O), `ddb_config` (config),
  `lifecycle` (startup phases), `metrics`.
- A change to a domain invariant touches `domain/`; a change to a runtime
  flow touches the runtime module — they don't entangle.

## Target module structure (consolidates C1–C14 into one blueprint)

The diskdb server has four major concept groups. The module tree below is the
target layout derived from the comments — each group is a top-level module (or
set of modules), named by subject, with the domain model gathered in one place
and the runtime concerns separated by responsibility.

### Concept groups

1. **Domain model** — the in-memory model of what the system *is*:
   `DdbDiskGroup`, `DdbDisk`, `DdbZone`; the allocation/free orchestration and
   its domain errors (`AllocError`, `FreeError`, `AllocClaim`); the record
   read-models (`BusyRecord`, `FreeRecord`, `ZoneRecords`) that continue the
   `BusyBlockKey`/`FreeBlockKey`/`ZoneKey` records defined in `crow-protocol`.
   No I/O, no transport — pure domain logic + invariants.
2. **Status machine + keep-alive** — how the system *observes and reacts*:
   the `HwStateMachine` (owns `HwStatus` transitions + per-state operation
   dispatch), and the `keepalive` loop that syncs with the system group
   (group 0) and drives the state machine on each observed change.
3. **Recovery** — how the system *rebuilds* in-memory state: zone recovery
   (3 strategies: full-scan, journal-replay, compaction), and (future) disk
   recovery triggered by state-machine changes (e.g. a disk going `Bad` →
   recover/rebuild its zones).
4. **Config / service / health** — how the system *runs and is operated*:
   config loading (`DdbConfig`), gRPC services (one file per service),
   startup lifecycle phases, health/readiness checks, metrics.

### Target tree

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
├── keepalive.rs                # KeepAlive — group-0 sync + state-machine driver (C14)
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
├── bg_task.rs                  # BackgroundTask trait + BgRunner + Trigger (C5)
│
├── lifecycle.rs                # StartupPhase (Init→Syncing→Recovering→Up) (C9)
│
├── service.rs                  # pure index (R6): pub mod + pub use
├── service/
│   └── diskdb_service.rs       # DiskdbService gRPC impl (C8)
│                               # future: diskio_service.rs, chunkdb_service.rs
│
├── ddb_config.rs               # DdbConfig + load_from_file + validate (C6a)
│
└── metrics.rs                  # DiskdbMetrics registration
```

### Notes on the mapping

- **Domain (group 1)** → `domain/` (R3, R4, R10). The `BusyBlock`/`FreeBlock`
  record types in `domain/records.rs` *continue* the key types from
  `crow-protocol` (`BusyBlockKey`/`FreeBlockKey`/`ZoneKey`) — they are the
  in-memory read-model of those durable records, so they live with the domain,
  not in the infra client.
- **Status machine + keep-alive (group 2)** → `status_machine.rs` (the machine,
  C13) + `keepalive.rs` (the driver, C14). The keep-alive loop observes group 0
  and calls `HwStateMachine::transition(...)`; the machine owns the
  side-effects. C13 and C14 land together.
- **Recovery (group 3)** → `recovery/` with one file per strategy (R5, C12).
  `recovery.rs` is the orchestrator index. `disk_recovery.rs` is the future
  home for disk-level recovery triggered by state-machine changes (a disk going
  `Bad` → rebuild/recover its zones) — noted as future, not built now.
- **Config / service / health (group 4)** → `ddb_config.rs` (C6a), `service/`
  (C8), `lifecycle.rs` (C9, the startup phases + health/readiness), `metrics.rs`.
  The health/readiness API exposes `StartupPhase` (C9) and is served from
  `service/` or a small `health.rs` if it grows.
- **Infrastructure** → `data_group_client.rs` (KV I/O, renamed from
  `persistence.rs` per C11) + `bg_task.rs` (the bg-task framework, C5). These
  are how the domain and runtime modules talk to the outside world.
- **`main.rs`** wires it: load config → build container + clients → start
  service early (C9) → run initial keep-alive tick (`Syncing`) → spawn recovery
  as a bg task (`Recovering`) → transition to `Up` → register keep-alive +
  compaction as bg tasks (C5) → serve gRPC until shutdown.

### Dependency direction

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
is the one infra module the domain's orchestration (`domain/alloc.rs`) needs —
either accept that one domain→infra edge, or define a small trait the client
satisfies so `domain/alloc.rs` depends on the trait, not the concrete client
(decide during impl).

## Files in scope

- `src/lib.rs`
- `src/main.rs`
- `src/config.rs`
- `src/grpc.rs`
- `src/metrics.rs`
- `src/node.rs`
- `src/node/container.rs`
- `src/node/disk.rs`
- `src/persistence.rs`
- `src/recovery.rs`
- `src/recovery/compaction.rs`
- `src/status.rs`
- `src/sync.rs`
- `src/zone.rs`
- `tests/common.rs`, `tests/common/`
- `tests/zone_alloc_test.rs`
- `tests/disk_alloc_test.rs`
- `tests/diskdb_e2e_test.rs`
- `tests/recovery_test.rs`
- `Cargo.toml`

## Comments

<!-- Add one entry per comment. Format:

### C1 — <short title>
- **Ref:** `<file>:<line>` or `<symbol>`
- **Comment:** <original observation>
- **Refined:** <actionable restatement>
- **Status:** open / addressed

-->

### C1 — `Node`/`NodeContainer` misnamed; use the disk-group concept directly
- **Ref:** `src/node/container.rs:14` (`pub struct NodeContainer`), `src/node.rs:29` (`pub struct Node`)
- **Comment:** There is no `Node` concept in diskdb — the unit of management is the **disk-group**, which plays the role of a node. A physical node can host multiple disk-groups, but a disk-group never crosses a node. A single diskdb instance manages **multiple** disk-groups (that is exactly what `NodeContainer` holds — a map keyed by `DiskGroupId`). The current `Node` struct is really a per-disk-group manager (it holds `disk_group_id`, `disks`, the RCU allocatable-disk context, and the round-robin cursor), and `NodeContainer` is really a container of disk-groups. The `Node`/`node` naming conflates the physical node with the disk-group and is misleading.
- **Refined:** Rename to the disk-group vocabulary throughout `app/crow-diskdb`, using the `Ddb` prefix to avoid clashes with the `crow-protocol` family (`DiskId`, `DiskGroupId`, `DiskGroupKey`, `DiskGroupValue`, `DiskValue`):
  - `Node` → `DdbDiskGroup` (the per-disk-group manager struct in `node.rs`).
  - `NodeContainer` → `DdbDiskGroupContainer` (in `container.rs`); its `nodes` field → `disk_groups`; methods `add_node`/`remove_node`/`get_node`/`node_ids` → `add_disk_group`/`remove_disk_group`/`get_disk_group`/`disk_group_ids`.
  - File/module rename: `src/node.rs` → `src/disk_group.rs`, `src/node/` → `src/disk_group/` (`container.rs`, `disk.rs` stay as children); update `lib.rs`/`main.rs` `mod`/`use` paths.
  - Keep the physical-node identity fields on `DdbDiskGroup`/`DdbDisk` as-is (`node_id: NodeId`, `rack_id: RackId`) — those refer to the real physical node/rack and are correct.
  - Update all call sites in `src/{main,grpc,sync,recovery,persistence,status}.rs`, `src/recovery/compaction.rs`, and tests (`tests/common/cluster.rs`, `tests/{disk_alloc,diskdb_e2e,recovery}_test.rs`).
  - Scope: 15 files. Mechanical rename + module path fix; no behavior change.
- **Status:** open

### C2 — `NodeContainer` methods should be disk-group add/remove/get
- **Ref:** `src/node/container.rs:29-44` (`add_node`, `remove_node`, `get_node`, `node_ids`)
- **Comment:** These methods operate on disk-groups (the value is `Arc<Node>` keyed by `DiskGroupId`, and `add_node` even reads `node.disk_group_id` as the key). The `*_node` names are wrong — this is a disk-group container, so the API should be `add_disk_group`/`remove_disk_group`/`get_disk_group`/`disk_group_ids`.
- **Refined:** Same rename as C1, called out at the method level:
  - `add_node(&self, node: Arc<Node>)` → `add_disk_group(&self, dg: Arc<DdbDiskGroup>)` (key from `dg.disk_group_id`).
  - `remove_node(&self, dg_id: DiskGroupId)` → `remove_disk_group(&self, dg_id: DiskGroupId)`.
  - `get_node(&self, dg_id: DiskGroupId)` → `get_disk_group(&self, dg_id: DiskGroupId)`.
  - `node_ids(&self)` → `disk_group_ids(&self)`.
  - Field `nodes` → `disk_groups`.
  - Update all callers (`grpc.rs`, `sync.rs`, `recovery.rs`, `persistence.rs`, `main.rs`, tests).
- **Status:** open

### C3 — `ZoneDisk` misnamed; define `DdbDisk` (and `DdbDiskGroup`) with `Ddb` prefix
- **Ref:** `src/node/disk.rs:23` (`pub struct ZoneDisk`)
- **Comment:** There is no `ZoneDisk` concept — a disk belongs to a disk-group and manages zones; "ZoneDisk" conflates the container role with the zone feature. Define the two core concepts clearly: the disk-group manager and the disk manager. To avoid clashing with the `crow-protocol` type family (`DiskId`, `DiskGroupId`, `DiskGroupKey`, `DiskGroupValue`, `DiskValue`), prefix diskdb-local manager structs with `Ddb` (diskdb).
- **Refined:**
  - `ZoneDisk` → `DdbDisk` (the per-physical-disk manager in `node/disk.rs`: holds `disk_id`, `disk_value`, `zones`, the RCU active-zone context, round-robin cursors, `effective_status`).
  - `Node` → `DdbDiskGroup` (already in C1; restated here so the two renames land together as one coherent vocabulary change: `DdbDiskGroup` manages `DdbDisk`s).
  - Type aliases follow: `AllocateDiskContext` → keep or rename to `AllocatableDiskContext` (cosmetic, optional); `ActiveZoneContext` stays (it is a zone concept, not a disk concept).
  - Update all references in `src/node.rs`, `src/node/disk.rs`, `src/{sync,recovery,persistence,grpc,main,status}.rs`, `src/recovery/compaction.rs`, and tests (`tests/{disk_alloc,diskdb_e2e,recovery}_test.rs`, `tests/common/cluster.rs`).
  - Scope: ~10 files. Mechanical rename; no behavior change. Lands as part of the C1+C2+C3 rename pass.
- **Status:** open

### C4 — `Zone` misnamed; use `DdbZone` for the in-memory zone manager
- **Ref:** `src/zone.rs:43` (`pub struct Zone`), also `ZoneHealth` (`src/zone.rs:22`)
- **Comment:** Same rationale as C3 — `crow-protocol` already owns the `Zone*` family (`ZoneValue`, `ZoneKey`, `ZoneAllocationState`, plus proto comments referencing "Zone"). The in-memory per-zone allocation manager in `zone.rs` is a diskdb-local concept and should carry the `Ddb` prefix to avoid confusion with the protocol types.
- **Refined:**
  - `Zone` → `DdbZone` (the per-zone allocation-state manager in `zone.rs`: `usage_bits`, `last_pos_64`, `used_count`, snapshot/compaction cursors, CAS retry counter).
  - `ZoneHealth` → `DdbZoneHealth` (the in-memory zone health enum, distinct from the protocol `ZoneAllocationState`).
  - Keep `AllocatedRange` as-is (no `Zone`/protocol clash; it's a generic result type).
  - File/module: `src/zone.rs` moves into the domain module per C10 → `src/domain/zone.rs` (the struct inside is renamed `DdbZone`). This supersedes the earlier "stays `zone.rs` / optional `ddb_zone.rs`" note — C10's `domain/` restructure is the final home.
  - Update all references in `src/zone.rs`, `src/node.rs`, `src/node/disk.rs`, `src/{sync,recovery,main}.rs`, `src/recovery/compaction.rs`, and tests (`tests/{zone_alloc,disk_alloc,recovery}_test.rs`).
  - Scope: ~10 files. Mechanical rename; no behavior change. Lands as part of the unified C1–C4+C10 rename/restructure pass.
- **Status:** open

### C5 — Generalize background-task structure (compaction is one of many future bg tasks)
- **Ref:** `src/recovery/compaction.rs:50` (`CompactionEngine`), `src/recovery/compaction.rs:68` (`compaction_loop`), `src/main.rs:208-222` (ad-hoc `tokio::spawn` of the compaction loop)
- **Comment:** Compaction is one kind of background task — triggered by timer (cadence) or event (free-record threshold). There will be multiple background tasks in the future (e.g. health probing R76, the §12 scanner R75, snapshot/zone maintenance, ownership-transfer prep). Today each task would have to duplicate the same boilerplate: own a `DataGroupClient`, take the container, loop forever, respect a stop signal, log failures, and be wired individually in `main.rs`. There is no shared structure for this flow.
- **Refined:** Introduce a background-task framework in `app/crow-diskdb` so compaction (and future bg tasks) share one lifecycle/scheduling shape. Concretely:
  - New module `src/bg_task.rs` (or `src/bg/` if it grows) with:
    - A `BackgroundTask` trait (or enum dispatch, per review rule 6) — `async fn run(self: Arc<Self>, ctx: BgCtx, stop: CancellationToken) -> Result<(), BgError>` (or `!` for never-ending loops). `BgCtx` carries the shared deps every bg task needs: `Arc<DdbDiskGroupContainer>`, `Arc<DataGroupClient>`, config, metrics.
    - A `BgScheduler`/`BgRunner` that owns the join set, spawns each task with the shared `CancellationToken`, and awaits them on shutdown (replacing the per-task `tokio::spawn` + `handle.await` in `main.rs`).
    - Trigger model: support both **timer** (cadence) and **event** (threshold/notify) triggers in one place — e.g. a `Trigger` enum (`Timer(Duration)` / `Event(Arc<Notify>)` / `Threshold(...)`), or a small `should_run()` predicate called each loop iteration. Compaction's "cadence OR threshold, whichever fires first" becomes one instantiation of this.
    - Uniform error handling: a failed cycle logs + continues (current compaction behavior); a fatal error stops the task and surfaces to the scheduler.
  - Refactor `CompactionEngine` to implement `BackgroundTask` (its `compaction_loop` becomes `run`); keep `compact_zone`/`compact_zone_now` as-is.
  - `main.rs` wires a single `BgRunner` with the compaction task registered; future tasks just register another `BackgroundTask` impl.
  - Keep it minimal — don't over-engineer for tasks that don't exist yet, but the trait + scheduler + trigger enum + shared `BgCtx` is the irreducible shape for "timer/event-triggered bg task with clean shutdown."
  - Scope: new `src/bg_task.rs` (or `src/bg/`), refactor `compaction.rs`, update `main.rs` wiring. No behavior change to compaction itself.
- **Status:** open

### C6 — Unified config system: `DdbConfig` root + JSON loader + crow-common base + dynamic/static fields
- **Ref:** `src/config.rs:3-4` (module doc / `DiskdbConfig`), `src/config.rs:13` (`pub struct DiskdbConfig`); reference pattern `app/crow-kv-server/dist/crow-kv-config.sample.json:1-2` (`{ "server": { ... } }`), `lib/crow-kv/src/common/config.rs:406` (`CrowKVConfig`), `:476` (`load_from_file`)
- **Comment:** Three related asks, in increasing scope:
  1. **diskdb config rename + JSON file**: rename `config.rs` → `ddb_config.rs` and `DiskdbConfig` → `DdbConfig` (consistent with the C1–C4 `Ddb` prefix). Provide a root `DdbConfig` that loads from a JSON file the way `CrowKVConfig::load_from_file` does, with a shipped sample file (`dist/ddb-config.sample.json`) mirroring the kv-server sample. Each field needs a comment explaining what it does and its operational impact, so the file is readable and safe to edit by hand.
  2. **crow-common config base**: design a base config facility in `lib/crow-common/rust` (new `config` module) that both `DdbConfig` and `CrowKVConfig` can build on — shared concerns: load-from-file, validate, serialize-for-display, and **file-change monitoring** (watch the config file and reload dynamically).
  3. **dynamic vs static fields**: distinguish fields that can be applied at runtime (prefix `dynamic_` or tag via a marker) from fields that require a service restart (prefix `static_`). The file watcher reloads dynamic fields live; static field changes are detected and logged with "restart required." This requires reviewing and classifying every existing field in **both** diskdb config and kv-server config.
- **Refined:** This is a large, multi-crate task — split into sub-items so it can be staged:
  - **C6a (diskdb-only, do first):**
    - Rename `src/config.rs` → `src/ddb_config.rs`, `DiskdbConfig` → `DdbConfig`; update `lib.rs`/`main.rs`/tests.
    - Add `DdbConfig::load_from_file` (mirror `CrowKVConfig::load_from_file`).
    - Ship `app/crow-diskdb/dist/ddb-config.sample.json` with the full default config, every field commented (JSON doesn't allow comments — so ship a paired `ddb-config.sample.md` doc, or use a `*.jsonc`/`*.json5` if the loader supports it; decide during impl).
    - Add per-field doc comments on every `DdbConfig` field explaining meaning + operational impact (some exist; audit the rest).
    - Move the inline `#[cfg(test)] mod tests` out to `tests/config_test.rs` per review rule 13.
  - **C6b (crow-common base):**
    - New `lib/crow-common/rust/src/config.rs` (or `config/`) with a `BaseConfig` trait (or a small `ConfigLoader` struct): `load_from_file`, `validate`, `to_json`, and `watch(self, on_change)` using `notify`-style file watching (add `notify` dep to crow-common).
    - Refactor `DdbConfig` (C6a) and `CrowKVConfig` to use the base — keep their per-field `Default`/`validate` logic, share the load/watch/serialize plumbing.
  - **C6c (dynamic/static classification):**
    - Define the convention: `static_<field>` = requires restart; `dynamic_<field>` = applied live on file change. Document in the config doc.
    - Audit every field in `DdbConfig` and `CrowKVConfig`, classify each, and rename/prefix accordingly. For each dynamic field, wire the live-apply path (e.g. cadence/threshold fields → update the running bg task's config atomically; listen addresses → require restart, so `static_`).
    - The watcher logs a diff on reload: dynamic fields applied, static fields flagged "restart required."
  - **Scope:** C6a = diskdb only (~4 files + new sample). C6b = crow-common + both apps (~10 files). C6c = both apps' config structs + watchers (~15 files). Land in order C6a → C6b → C6c; do not attempt C6c before C6b.
- **Status:** open

### C7 — Put diskdb config under `conf/`; narrow the `**/conf/` gitignore to specific runtime paths
- **Ref:** `app/crow-diskdb/src/grpc.rs:41` (cursor — config consumption context), `.gitignore:45` (`**/conf/`), `.gitignore:42-46` (the "Per-crate runtime dirs" block)
- **Comment:** Want to place the diskdb config file under a `conf/` folder (e.g. `app/crow-diskdb/conf/ddb-config.json`). Today `.gitignore:45` has `**/conf/`, which ignores **any** `conf/` directory anywhere in the repo. The intent (per the comment on lines 42–43) is to ignore per-crate **runtime** config dirs — operator-supplied real configs that must not be committed. But the glob is too broad: it would also ignore a `conf/` dir holding the sample/default config we *want* to track (the equivalent of `app/crow-kv-server/dist/crow-kv-config.sample.json`, which is tracked because it lives under `dist/`, not `conf/`). The glob currently works only because every existing `conf/` dir is bench-runtime-generated (under `app/crow-cli/bench-runs/.../workspace/N-*/conf/`) and correctly ignored — but it would silently swallow a tracked `app/crow-diskdb/conf/` the moment we create it.
- **Refined:**
  - **Narrow the gitignore:** replace the broad `**/conf/` with explicit per-crate runtime-conf paths, mirroring how `log/` is handled (lines 48–54 list `app/<crate>/log/` explicitly instead of globbing). Concretely:
    - Remove `.gitignore:45` (`**/conf/`).
    - Add the specific runtime-conf dirs that should stay ignored, e.g. `app/crow-kv-server/conf/`, `app/crow-diskdb/conf/runtime/` (or whatever convention C6 adopts for operator-supplied vs tracked configs), and `app/crow-cli/bench-runs/` already covered by line 40.
    - Keep the bench workspace `conf/` dirs ignored via the existing `app/crow-cli/bench-runs/` line (line 40) — verify no bench `conf/` escapes that rule after the glob is removed.
  - **Adopt a tracked-vs-runtime split for config dirs** (aligns with C6):
    - `app/crow-diskdb/conf/` — **tracked**, holds `ddb-config.sample.json` (+ the comment doc from C6a). This is the template operators copy from.
    - `app/crow-diskdb/conf/runtime/` (or a `.gitignore`d `conf/` subdir) — **ignored**, holds the operator's real `ddb-config.json` at deploy time. Add this specific path to `.gitignore`.
    - Same convention for `app/crow-kv-server/conf/` (tracked sample) + `conf/runtime/` (ignored) — and migrate `dist/crow-kv-config.sample.json` → `conf/crow-kv-config.sample.json` for consistency (optional, decide during C6).
  - Verify with `git check-ignore -v app/crow-diskdb/conf/ddb-config.sample.json` that the tracked sample is **not** ignored, and `git check-ignore -v app/crow-diskdb/conf/runtime/ddb-config.json` that the runtime file **is** ignored.
  - **Scope:** `.gitignore` + the new `conf/` layout from C6a. Small change, but must land with (or right after) C6a so the tracked sample isn't accidentally ignored.
- **Status:** open

### C8 — `grpc.rs` is a bad file name; use a `service/` module with one file per gRPC service
- **Ref:** `src/grpc.rs:41` (`impl DiskdbService`), `src/lib.rs:14` (`pub mod grpc`), `src/main.rs:12` (`use crow_diskdb::grpc::DiskdbService`)
- **Comment:** `grpc.rs` is named after the transport (a kind), not the resource — violates review rule 14 (banned kind-named files) and rule 15 (handlers by resource not verb). It currently holds a single service (`DiskdbService` implementing the `DiskdbService` trait: `AllocateBlocks`, `FreeBlocks`, `QueryCapacityStats`, `GetDiskGroupInfo`, `GetDiskInfo`, `RebuildZoneBitmap`). The protos define three services total — `DiskdbService` (current), `DiskioService` and `ChunkdbService` (both future, per `diskio_service.proto:16` / `chunkdb_service.proto:17` comments). The file name doesn't scale to multiple services and doesn't tell the reader what's inside.
- **Refined:**
  - Replace `src/grpc.rs` with a `service/` module:
    - `src/service.rs` — pure index (review rule: `foo.rs` is a pure index): `pub mod diskdb_service; pub use diskdb_service::DiskdbService;` (+ docs).
    - `src/service/diskdb_service.rs` — the current `DiskdbService` impl (move all of `grpc.rs` content here).
  - Update `src/lib.rs`: `pub mod grpc` → `pub mod service`; update `src/main.rs`: `use crow_diskdb::grpc::DiskdbService` → `use crow_diskdb::service::DiskdbService`.
  - When `DiskioService` / `ChunkdbService` are implemented, add `src/service/diskio_service.rs` / `src/service/chunkdb_service.rs` and re-export from `service.rs`. One file per service, named after the service.
  - Note: the struct `DiskdbService` itself is fine (it matches the proto service name); only the *file* name changes.
  - **Scope:** 3 files (`grpc.rs` → `service.rs` + `service/diskdb_service.rs`, `lib.rs`, `main.rs`). Mechanical move + rename; no behavior change.
- **Status:** open

### C9 — Startup lifecycle: finish start first, run recovery in background, gate service on a startup-phase health status
- **Ref:** `src/main.rs:127-128` (`info!("running R73 recovery"); for dg_id in container.node_ids() {`), `src/main.rs:117-206` (the whole blocking recovery block), `src/main.rs:224-241` (gRPC serve starts only after recovery finishes)
- **Comment:** Today startup is fully sequential and blocking: initial sync → blocking R73 recovery (every owned disk-group, every disk, every zone, in a single `for` loop on the main task) → only then start gRPC. Two problems:
  1. The gRPC/HTTP service is not listening during recovery, so there is no way for an operator or orchestrator to query progress or know the instance is alive — it just looks hung.
  2. Recovery of all owned disk-groups runs serially on the main task; a large node can take a long time before serving anything.
- **Refined:** Redesign the startup lifecycle as an explicit phase machine with the service up early and recovery in the background:
  - **Startup phase enum** (new, in `src/status.rs` or a new `src/lifecycle.rs`): `StartupPhase { Init, Syncing, Recovering, Up }` (extend as needed — e.g. `Degraded`). Held in an `AtomicU8` (or `Arc<RwLock<StartupPhase>>`) on the container/server so it's lock-free to read on the hot path.
    - `Init` — process started, config loaded.
    - `Syncing` — initial group-0 sync running.
    - `Recovering` — initial sync done, R73 recovery running in background.
    - `Up` — recovery complete, serving normally.
  - **Service up first:** start the gRPC + HTTP servers immediately after `Init` (or after `Syncing`), *before* recovery. The service is reachable so health checks work during recovery.
  - **Recovery in background:** spawn recovery as a background task (one task per owned disk-group, or one task iterating disk-groups with bounded concurrency — aligns with C5's bg-task framework). The main task does not block on it. Each disk-group transitions itself to `Up` as its recovery finishes (per-disk-group granularity, not all-or-nothing).
  - **Health/readiness API:** expose the current `StartupPhase` via the existing health endpoint (and/or a new `/ready` on the HTTP mgmt API). Each service's periodic health check returns the current phase. Operators/orchestrators poll this to know when the instance is ready.
  - **Command gating:** the gRPC service rejects `AllocateBlocks`/`FreeBlocks`/`RebuildZoneBitmap` with `Status::unavailable("diskdb not ready: phase={phase}")` (or a per-disk-group check: reject if that disk-group's recovery isn't done) while the phase is not `Up`. Read-only RPCs (`GetDiskGroupInfo`, `GetDiskInfo`) can be allowed earlier. This replaces the current `is_degraded()` check as the primary gate, with degraded mode as a runtime (post-`Up`) condition.
  - **Integration with existing status:** `StatusManager` (`src/status.rs`) already models `HwStatus` transitions; the `StartupPhase` is orthogonal (lifecycle vs runtime health) — keep them separate, but the readiness check combines both: ready = `StartupPhase::Up` && `!degraded` (or per-disk-group `HwStatus::Up`).
  - **Scope:** new `src/lifecycle.rs` (or extend `status.rs`), refactor `main.rs` startup ordering (move gRPC serve before recovery, spawn recovery as a task), add phase to the service struct + gate RPCs, expose phase via health endpoint. Medium-sized; touches `main.rs`, `status.rs`/`lifecycle.rs`, `service/diskdb_service.rs` (C8), and the HTTP mgmt API. Coordinate with C5 (bg-task framework) — recovery becomes a bg task too.
- **Status:** open

### C10 — Gather the diskdb domain model into one `domain/` module
- **Ref:** `src/node.rs:45` (`impl Node`), `src/lib.rs:16` (`pub mod node`), `src/lib.rs:20` (`pub mod zone`), `src/node/` (`container.rs`, `disk.rs`)
- **Comment:** Reinforces C1 — there is no `Node` concept, only `DiskGroup` (and `Disk`, `Zone`). Beyond the rename, the domain model is currently scattered across three top-level modules: `node.rs` (the disk-group manager), `node/` (`container.rs` + `disk.rs`), and `zone.rs`. These are the diskdb **domain** — the in-memory model of disk-groups/disks/zones and their allocation logic. They should live together in one cohesive domain module so a reader finds the whole model in one place, separate from infrastructure (persistence, recovery, sync, grpc, config).
- **Refined:** Introduce a `domain/` module grouping the diskdb domain model, and apply the C1–C4 renames inside it:
  - New `src/domain.rs` — pure index (review rule: `foo.rs` is a pure index): module docs + `pub mod disk_group; pub mod disk; pub mod zone;` + `pub use` re-exports of the public types (`DdbDiskGroup`, `DdbDiskGroupContainer`, `DdbDisk`, `DdbZone`, `DdbZoneHealth`, `AllocatedRange`, `AllocClaim`, `AllocError`, `AllocatableDiskContext`, `ActiveZoneContext`).
  - `src/domain/disk_group.rs` — the `DdbDiskGroup` struct + impl (from `node.rs`, renamed per C1).
  - `src/domain/disk_group_container.rs` — the `DdbDiskGroupContainer` (from `node/container.rs`, renamed per C1/C2).
  - `src/domain/disk.rs` — the `DdbDisk` struct + impl (from `node/disk.rs`, renamed per C3).
  - `src/domain/zone.rs` — the `DdbZone` + `DdbZoneHealth` + `AllocatedRange` (from `zone.rs`, renamed per C4).
  - Remove `src/node.rs`, `src/node/`, `src/zone.rs`; update `src/lib.rs`: drop `pub mod node` + `pub mod zone`, add `pub mod domain`.
  - Update all imports across `src/{main,grpc,sync,recovery,persistence,status}.rs`, `src/recovery/compaction.rs`, and tests to `crate::domain::{...}` (or `crate::domain::disk_group::...`).
  - Keep infrastructure modules (`persistence`, `recovery`, `sync`, `service` (C8), `ddb_config` (C6a), `metrics`, `status`, `lifecycle` (C9), `bg_task` (C5)) at top level — only the domain model moves under `domain/`.
  - **Scope:** ~15 files (new `domain.rs` + 4 `domain/*.rs`, delete `node.rs`/`node/`/`zone.rs`, update `lib.rs` + all importers + tests). Lands as part of the unified C1–C4+C10 rename/restructure pass. No behavior change.
- **Status:** open

### C11 — `persistence.rs` mixes infrastructure and domain; split domain types/logic out into the domain model
- **Ref:** `src/persistence.rs:29-30` (`pub enum FreeError`), `src/persistence.rs:26` (`pub type Bind`), `src/persistence.rs:87-101` (`BusyRecord`, `FreeRecord`, `ZoneRecords`), `src/persistence.rs:557-763` (`allocate_block`, `allocate_blocks`, `free_block`, `free_blocks`), `src/persistence.rs:112` (`DataGroupClient`)
- **Comment:** `persistence.rs` is a bad file name (kind, not subject — review rule 14) and the file conflates two layers:
  1. **Infrastructure** — `DataGroupClient` (wraps `CrowkvClient` for put/delete/scan on the bound paxos data group) and its KV I/O methods (`put_zone`, `read_zone_records`, `journal_scan_*`, `delete_free_records_batch`, `get_applied_slot`, etc.).
  2. **Domain** — the allocation/free *orchestration* (`allocate_block`/`allocate_blocks`/`free_block`/`free_blocks`), the domain error types (`FreeError`, and `AllocError` currently lives in `node.rs`), and the domain record types (`BusyRecord`, `FreeRecord`, `ZoneRecords`).
  `FreeError` (double-free, ownership mismatch) is a domain concept — it describes invariants of the block allocation model, not KV transport failures (the `Kv` variant just wraps the transport error). It belongs with the domain model, next to `AllocError` and `AllocClaim`.
- **Refined:** Split `persistence.rs` along the layer boundary, and move domain types into the `domain/` module (C10):
  - **Domain (move to `domain/`):**
    - `FreeError` → `src/domain/alloc.rs` (or `domain/free.rs`) — alongside `AllocError` (which moves from `node.rs`→`domain/disk_group.rs` or a shared `domain/alloc.rs`). Group the allocation/free errors + result types together: `AllocError`, `FreeError`, `AllocClaim`, `AllocatedRange`.
    - `BusyRecord`, `FreeRecord`, `ZoneRecords` → `src/domain/records.rs` (or fold into `domain/zone.rs` if they're zone-scan results) — they are domain read-models of the durable records, not transport.
    - `allocate_block`/`allocate_blocks`/`free_block`/`free_blocks` → `src/domain/alloc.rs` (the two-phase allocate/free orchestration is domain logic — it coordinates the in-memory `DdbDiskGroup`/`DdbDisk`/`DdbZone` model with durable writes). These functions take a `&DataGroupClient`, so `domain/alloc.rs` depends on the infra client type — keep the dependency one-way (domain → infra interface), or define a small trait the client satisfies to avoid domain→infra coupling (decide during impl).
  - **Infrastructure (rename `persistence.rs`):**
    - Keep `DataGroupClient` + its KV I/O methods + `Bind` in an infra module renamed by subject, e.g. `src/data_group_client.rs` (or `src/kv_io.rs` / `src/data_group_io.rs`). It's the data-group KV transport, not "persistence."
    - This module depends on domain types only for the record read-models (`ZoneRecords` etc.) — if those moved to `domain/`, the infra module imports them from `domain/`.
  - Update `lib.rs`: drop `pub mod persistence`, add `pub mod data_group_client` (+ `pub mod domain::alloc` etc. via C10).
  - Update all importers (`service/diskdb_service.rs` (C8), `recovery.rs`, `sync.rs`, `main.rs`, `recovery/compaction.rs`, tests).
  - **Scope:** ~10 files. Split + rename + move domain types. Lands after C10 (needs the `domain/` module to exist). No behavior change.
- **Status:** open

### C12 — Recovery module doesn't surface the 3 recovery strategies; restructure so the file layout reflects them
- **Ref:** `src/recovery.rs:89` (`impl RecoveryEngine`), `src/recovery.rs:10-18` (module doc listing Strategy 1 / 2 / 3), `src/recovery.rs:197` (`rebuild_zone_bitmap_full_scan` = strategy 1), `src/recovery.rs:279` (`recover_zone_inner` = strategy 2), `src/recovery/compaction.rs:50` (`CompactionEngine` = strategy 3)
- **Comment:** There are three recovery strategies, documented in the `recovery.rs` module doc but not visible in the file structure:
  - **Strategy 1** — full scan (`rebuild_zone_bitmap_full_scan`): scan all live `BusyBlockKey`s, set bits. Always available; fallback.
  - **Strategy 2** — journal replay (`recover_zone_inner`): load latest `ZoneValue` snapshot, replay journal from `snapshot_slot+1` to frontier. Fast when compaction keeps the record set small.
  - **Strategy 3** — compaction (`CompactionEngine` in `recovery/compaction.rs`): background merge of free records into a new snapshot.
  From the file layout (`recovery.rs` + `recovery/compaction.rs`) you cannot recognize that there are three strategies or which file holds which. Strategy 1 and 2 are both loose functions/methods in one flat `recovery.rs`; strategy 3 is in a submodule. The `RecoveryEngine` struct is the only entry point but it doesn't model the strategy choice.
- **Refined:** Restructure `recovery/` so the three strategies are first-class in the file layout, and model the strategy choice explicitly:
  - `src/recovery.rs` — pure index + the `RecoveryEngine` (orchestrator) + `RecoveryError` + `ZoneStats`. The engine picks the strategy per zone (strategy 2 first, strategy 1 fallback) and exposes `recover_disk_group` (renamed from `recover_node` per C1). Keep the orchestration here; move the strategy implementations out.
  - `src/recovery/full_scan.rs` — strategy 1 (`rebuild_zone_bitmap_full_scan`, moved out of `recovery.rs`). Clearly named by strategy.
  - `src/recovery/journal_replay.rs` — strategy 2 (`recover_zone_inner`, `merge_ops_by_slot`, `find_free_unit_count_at_slot`, moved out of `recovery.rs`). Clearly named by strategy.
  - `src/recovery/compaction.rs` — strategy 3 (stays; already a submodule). Rename `CompactionEngine` → `Compactor` if it reads better (optional).
  - Optional: a `RecoveryStrategy` enum (`FullScan` / `JournalReplay` / `Compaction`) to make the choice explicit in the engine and in logs/metrics (e.g. "zone recovered via JournalReplay, fallback FullScan"). Today the strategy is only implicit in which function ran.
  - `zone_snapshots_exist` (the fresh-vs-recovered decision helper) stays in `recovery.rs` (orchestration helper) or moves to `recovery/journal_replay.rs` (it's a strategy-2 precondition) — decide during impl.
  - Align with C5: compaction (strategy 3) is a bg task; the orchestrator (strategies 1+2) is the startup recovery task from C9.
  - Update `lib.rs` (`pub mod recovery` stays; the submodules are `pub mod` under it) + importers.
  - **Scope:** ~5 files (split `recovery.rs` into 3 strategy files + orchestrator index, update imports). Lands after/independent of C10. No behavior change; optional `RecoveryStrategy` enum is a small additive change.
- **Status:** open

### C13 — `StatusManager` is a bag of helpers; make it a real status machine with per-state operation dispatch
- **Ref:** `src/status.rs:19` (`impl StatusManager`), `src/status.rs:15` (`pub(crate) struct StatusManager`), `src/status.rs:27-71` (the helper methods), `src/node/disk.rs:73-84` (`set_effective_status` — side-effects live here, not in the manager), `src/sync.rs:272-301` (caller does `set_effective_status` + `rebuild_allocating_disks` manually), `lib/crow-protocol/src/proto/common_type.proto:13-21` (`HwStatus` enum: Init/Up/Maintenance/Suspect/Missing/Bad/Offline)
- **Comment:** Two issues:
  1. **What status does this manage?** `HwStatus` is shared by Rack/Node/DiskGroup/Disk (per the proto comment), but in diskdb the relevant entities are **DiskGroup** and **Disk** (zones inherit the disk's status). The `StatusManager` doc just says "hardware status transitions" — it doesn't say it's the disk-group/disk status machine. With C1's rename, this should be explicit: it's the `DdbDiskGroup`/`DdbDisk` status machine.
  2. **It's not a state machine.** Today `StatusManager` is a bag of static-ish helpers: a transition-legality table (`is_legal_transition`), a `max()` effective-status computation, two boolean gates (`allows_allocate`/`allows_free`), and a suspect-timeout check. There is no state-machine object that:
     - holds the current state,
     - validates + applies a transition (legality + side-effects together),
     - dispatches per-state behavior (what operations are permitted, what happens on entry/exit).
     The per-status side-effects are scattered: `disk.rs::set_effective_status` marks all zones `Bad` on entering `Bad`; `sync.rs` manually calls `rebuild_allocating_disks` after a status change; the `allows_allocate`/`allows_free` booleans are checked at call sites instead of dispatched by the state. The "state machine" is implicit across three files.
- **Refined:** Turn this into a real status machine:
  - **Rename:** `StatusManager` → `HwStateMachine` (or `DdbStatusMachine` to match the `Ddb` prefix). Rename `src/status.rs` → `src/status_machine.rs` (or `src/status_machine/` if it grows). Update `lib.rs` + importers.
  - **Per-state operation dispatch:** model each `HwStatus` variant as knowing its own behavior, not as boolean checks at call sites. Two viable shapes (decide during impl):
    - **(a) Methods on the enum** (`impl HwStatus`): `fn allows_allocate(self) -> bool`, `fn allows_free(self) -> bool`, `fn on_enter_disk(self, disk: &DdbDisk)`, `fn on_enter_disk_group(self, dg: &DdbDiskGroup)`. Each state owns its entry side-effects (e.g. `Bad` → mark zones Bad; `Maintenance`/`Offline` → rebuild allocatable disks). Call sites ask the state what to do instead of branching on it.
    - **(b) A `State` trait + enum dispatch** (review rule 6: prefer enum dispatch): a `DiskStatusState` enum wrapping the `HwStatus`, with `impl DiskStatusState { fn transition(self, to) -> Result<Self>; fn permits(&self, op: Op) -> bool; fn on_enter(&self, ctx) }`. Heavier; only if behavior diverges enough to justify it.
    Prefer (a) for simplicity unless the entry/exit logic grows.
  - **Transition + side-effects together:** `HwStateMachine::transition(current, to, ctx)` validates legality *and* runs the entry side-effect in one call — callers no longer do `set_effective_status` + `rebuild_allocating_disks` manually in `sync.rs`. The state machine owns the full transition.
  - **Operation enum:** define `Op { Allocate, Free, Rebuild, Probe }` (or similar); `state.permits(&Op::Allocate)` replaces `allows_allocate`. Extensible when new operations appear.
  - **Scope:** `status.rs` → `status_machine.rs` (rewrite), update `sync.rs` (call the machine instead of manual side-effects), `domain/disk.rs` (move `set_effective_status` side-effects into the state machine or `impl HwStatus`), `domain/disk_group.rs`, `lib.rs`. ~6 files. Behavior-preserving refactor; the transition table + effective-status math stay, they just get owned by the machine. Move inline tests to `tests/status_machine_test.rs` (review rule 13).
- **Status:** open

### C14 — `sync.rs` / `SyncLoop` is vaguely named and bundles 4 concerns; clarify what it does and split by responsibility
- **Ref:** `src/sync.rs:33` (`pub struct SyncOutcome`), `src/sync.rs:4` (module doc "keep-alive + periodic hardware sync from group 0"), `src/sync.rs:112-246` (`sync_once` — the whole tick), `src/sync.rs:250-307` (`reconcile_disks`), `src/sync.rs:309-379` (`disk_add_init`), `src/sync.rs:382-396` (`run`)
- **Comment:** "sync" is a bad name — it doesn't say what's being synced or why. Reading the code, `SyncLoop.sync_once` actually does four distinct things each tick:
  1. **Keep-alive heartbeat** — `svc.heartbeat_diskdb(instance_id, ...)` to the service registry; missed-count → degraded mode.
  2. **Ownership reconciliation** — read the owner map from group 0, filter to disk-groups owned by this instance, add/remove `DdbDiskGroup`s in the container, update binds.
  3. **Disk reconciliation** — per owned disk-group, read member disks from group 0, run disk-add init for new disks, apply status changes, detect missing disks.
  4. **Disk-add init** — create `DdbDisk` + `DdbZone`s, write baseline `ZoneValue` records, rebuild active zones.
  These are different concerns crammed into one `sync_once` + helpers. The file name and the `SyncOutcome`/`SyncLoop` types don't surface that this is really "group-0 hardware-view reconciliation + keep-alive." A reader can't tell from the name what the loop is for.
- **Refined:**
  - **The loop is the state-machine driver.** The key insight (per reviewer): this loop is what *drives* the `HwStateMachine` (C13). Each tick it observes the group-0 hardware view (owner map, bind map, disk statuses) and feeds transitions into the state machine — `machine.transition(current, observed, ctx)` — instead of manually poking `set_effective_status` + `rebuild_allocating_disks`. So the naming should reflect "keep-alive + drive the state machine," not just "reconcile."
  - **Rename by subject:** `src/sync.rs` → `src/keepalive.rs` (the keep-alive loop that drives the state machine). `SyncLoop` → `KeepAlive` (or `KeepAliveLoop`); `SyncOutcome` → `KeepAliveOutcome`; `SyncConfig` → `KeepAliveConfig`. "Keep-alive" captures both the heartbeat (concern 1) and the periodic observation that drives state transitions (concerns 2–4). Update `lib.rs` + `main.rs` + tests.
    - Alternative if "keep-alive" reads too narrow: `src/state_driver.rs` / `StateDriver` — emphasizes the state-machine-driving role. Pick during impl; "keep-alive" is the default.
  - **Split the loop's responsibilities** so each is a named, testable unit, even if they still run in one tick:
    - `fn heartbeat(&mut self) -> Result<(), ...>` — keep-alive + missed-count/degraded logic (concern 1).
    - `fn observe_ownership(&self, ...) -> ...` — owner/bind map read + disk-group add/remove/bind-update (concern 2).
    - `fn observe_disks(&self, ...)` — per-disk-group disk add/status/remove (concern 3; renamed from `reconcile_disks`).
    - `disk_add_init` stays (concern 4; already a method) — consider moving it to `domain/disk.rs` or `domain/alloc.rs` since it constructs the domain model, not group-0 I/O. Decide during impl.
    - `sync_once` → `tick` (or `run_once`): a thin orchestrator calling the four in order; the `KeepAliveOutcome` records what each step did.
  - **State-machine driving (the core change):** in `observe_disks`, when a disk's observed status differs from current, call `HwStateMachine::transition(current, observed, &disk_ctx)` (C13) — the machine validates legality + runs entry side-effects. The keep-alive loop no longer touches `set_effective_status` or `rebuild_allocating_disks` directly. Same for disk-group add/remove and missing-disk detection — these become state-machine transitions driven by observation.
  - **Align with C5:** the keep-alive loop is a bg task (timer-triggered); the initial blocking `tick` from `main.rs` (C9) is the `Syncing` → `Recovering` transition. With C5's `BgRunner`, `KeepAlive` registers as a `BackgroundTask` with `Trigger::Timer(interval)`.
  - **Align with C13:** the status-change side-effects move into the `HwStateMachine` — the keep-alive loop is the driver, the machine is the dispatcher. This is the C13↔C14 link: C13 defines the machine, C14 makes this loop call it.
  - **Scope:** `sync.rs` → `keepalive.rs` (rename + split `sync_once` into 4 named methods + drive the state machine), `lib.rs`, `main.rs`, tests. ~5 files. Behavior-preserving; the tick still does the same four things in the same order, just named, separable, and routed through the state machine. Coordinate with C5 (bg task) and C13 (status machine) — C13 and C14 land together.
- **Status:** open
