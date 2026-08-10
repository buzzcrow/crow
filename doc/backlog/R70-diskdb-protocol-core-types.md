<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R70: diskdb — Protocol + Core Types + Config Validation

**Problem**: The project skeleton (R69, now completed) set up (`lib/protocol` with a stub
`diskdb.proto`, `app/crow-diskdb` client skeleton) and the high-level
design doc (`doc/design/diskdb/design-crow-diskdb.md`). The skeleton
compiles but has no functionality. Before any diskdb logic can be
implemented (R71–R76), the protocol surface, core data types, and
config validation must be in place — every follow-up requirement
depends on these types.

The current `lib/protocol/src/proto/diskdb.proto` defines the five gRPC
RPCs (`AllocateBlock`, `AllocateBlocks`, `FreeBlock`, `ActiveZone`,
`QueryDiskUsage`) with basic request/response messages, but it is
missing:
- The `ConditionAllocateBlocks` RPC (negative disk hints for
  replica-aware multi-block allocation).
- Operator/admin RPCs (`AddDisk`, `RemoveDisk`, `SetDiskStatus`,
  `SetDiskGroupStatus`, `SetNodeStatus`, `GetNodeInfo`, `GetDiskInfo`)
  needed by R71/R76 and the console (R77).
- Enum types (`DiskType`, `Status`, `ZoneState`, `ZoneAllocationState`,
  `DiskState`) that the core types and gRPC messages share.

The `app/crow-diskdb` client crate has only an error enum stub — no
core types, no config, no client struct. The server binary
(`app/crow-diskdb`) is listed as a workspace member but does not
exist yet.

**Solution**: Fill in the protocol and core types layer so R71–R76 can
build on it. This requirement produces **types and validation only** —
no runtime logic, no allocation, no KV I/O.

1. **Protobuf services** — extend `lib/protocol/src/proto/diskdb.proto`:
   - Add `ConditionAllocateBlocks` RPC with
     `ConditionAllocateBlocksRequest` (includes `repeated string
     exclude_disk_uuids`).
   - Add operator/admin RPCs: `AddDisk`, `RemoveDisk`,
     `SetDiskStatus`, `SetDiskGroupStatus`, `SetNodeStatus`,
     `GetNodeInfo`, `GetDiskInfo`. These are used by R71 (status
     management), R76 (disk discovery), and R77 (console).
   - Add enum messages: `DiskType` (BlockHdd/BlockSsd/ZoneSsd/SmrHdd),
     `Status` (Online/Init/Maintenance/TempFailure/Offline),
     `ZoneState` (Healthy/Missing/Bad), `ZoneAllocationState`
     (Active/Busy/Error/Full), `DiskState`
     (Init/Active/Suspect/Missing/Bad).
   - Extend `DiskUsage` with zone-level detail fields
     (`zone_count`, `active_zone_count`, `busy_zone_count`,
     `bad_zone_count`) for R74's query API.
   - Add `ZoneUsage` message for per-zone busy/free breakdown (used by
     R74 and R77's block-array visualization).

2. **Core types** — create `app/crow-diskdb/src/types/` module:
   - `Segment` — allocation handle: `(disk_group_id, disk_uuid,
     zone_index, zone_offset, size, tag)`. The `disk_group_id` is
     `"{node_uuid}-{index}"` (D2). `tag` is a nanosecond timestamp for
     debugging/tracking.
   - `ClaimSnapshot` — zone state snapshot before a claim, for
     rollback: `(prev_pos, count)`.
   - `ZoneRecord` — the **compacted snapshot** of a zone
     (`allocate_pos`, `max_allocate_pos`, `usage_bitmap`, `zone_state`,
     `snapshot_slot`, `checksum`). This is the full-zone state written
     periodically by R73's snapshot compaction — **not** on every
     allocate (D4).
   - `BusyRecord` — small record appended on each allocate:
     `(zone_offset, size, tag)`. ≤ 32 bytes serialized. This is the
     journal entry that the paxos data group stores (D4).
   - `FreeRecord` — small record appended on each free: same shape as
     `BusyRecord` `(zone_offset, size, tag)`.
   - `ZoneSnapshot` — wrapper for the compacted full-zone state at a
     point in time, stored at the snapshot key. Contains
     `ZoneRecord` + `snapshot_slot` (the max journal slot included in
     the compaction).
   - `DiskGroupId` — type alias for `String`, formatted as
     `"{node_uuid}-{index}"`.
   - `Status` — shared enum for node/disk-group/disk with
     `effective_status(node, group, disk)` = `max(node, group, disk)`.
     `allows_allocate()` (Online only), `allows_free()` (Online,
     Maintenance, TempFailure).
   - `ZoneState` — health enum (Healthy, Missing, Bad).
   - `ZoneAllocationState` — lifecycle enum (Active, Busy, Error, Full),
     `#[repr(u8)]` for `AtomicU8` CAS. `from_u8()` helper.
   - `DiskState` — health enum (Init, Active, Suspect, Missing, Bad).
   - `DiskType` — physical disk technology enum (BlockHdd, BlockSsd,
     ZoneSsd, SmrHdd).
   - `NodeMeta`, `DiskGroupMeta`, `DiskMeta`, `InstanceMeta` —
     group-0 sysdata types matching the schema in design doc §5. These
     are used by R71 but defined here as shared types.
   - All types derive `Clone, Debug` and use `serde` for
     serialization (matching the aioss pattern). Journal records
     (`BusyRecord`, `FreeRecord`) use compact binary serialization
     (bincode or prost) to minimize paxos journal write size.

3. **Journal key layout** — implement key-formatting helpers in
   `app/crow-diskdb/src/types/`:
   - `journal_key_busy(dg_id, disk_uuid, zone_idx, slot)` →
     `/diskdb/journal/{dg_id}/{disk_uuid}/z{zone_idx:04}/busy/{slot}`
   - `journal_key_free(dg_id, disk_uuid, zone_idx, slot)` →
     `/diskdb/journal/{dg_id}/{disk_uuid}/z{zone_idx:04}/free/{slot}`
   - `journal_key_snapshot(dg_id, disk_uuid, zone_idx)` →
     `/diskdb/journal/{dg_id}/{disk_uuid}/z{zone_idx:04}/snapshot`
   - `journal_prefix_zone(dg_id, disk_uuid, zone_idx)` →
     `/diskdb/journal/{dg_id}/{disk_uuid}/z{zone_idx:04}/`
   - `journal_prefix_disk(dg_id, disk_uuid)` →
     `/diskdb/journal/{dg_id}/{disk_uuid}/`
   - `journal_prefix_dg(dg_id)` → `/diskdb/journal/{dg_id}/`
   - Group-0 sysdata keys (matching design doc §5):
     `sysdata_key_node(node_uuid)`,
     `sysdata_key_disk_group(node_uuid, dg_index)`,
     `sysdata_key_disk(node_uuid, disk_uuid)`,
     `sysdata_key_owner(dg_id)`, `sysdata_key_bind(dg_id)`,
     `sysdata_key_instance(instance_id)`.
   - The `slot` in journal keys is a monotonically increasing per-zone
     counter that identifies the journal entry's position. It is
     **not** the crow-kv paxos slot — it is a diskdb-assigned sequence
     number embedded in the key, enabling prefix-scan replay without
     needing crow-kv slot feedback (resolving the D4 open question in
     favor of the prefix-scan approach).

4. **Config types + validation** — create
   `app/crow-diskdb/src/config/` module:
   - `DiskdbConfig` — top-level config (server, storage, heartbeat,
     persistence, scanner, sync). TOML-based, serde-serialized.
   - `ServerConfig` — `listen_addr` (default `0.0.0.0:9941`),
     `http_listen_addr` (default `0.0.0.0:9942`), `instance_id`
     (optional, auto-generated UUID if absent).
   - `StorageDefaults` — `zone_size_bytes` (default 16 GB),
     `block_size_bytes` (default 1 MB, configurable 512 KB–2 MB),
     `allocate_granularity` (default 1 MB, must be power of 2).
   - `HeartbeatConfig` — `interval_secs` (default 13),
     `miss_threshold` (default 3),
     `temp_failure_timeout_secs` (default 900).
   - `PersistenceConfig` — `free_flush_interval_ms` (default 500),
     `free_flush_max_batch` (default 256),
     `snapshot_interval_secs` (default 300),
     `snapshot_journal_threshold` (default 4096, compact when journal
     entries per zone exceed this).
   - `ScannerConfig` — `scan_interval_secs` (default 600),
     `detect_ghost_allocations` (default true),
     `verify_record_integrity` (default true).
   - `SyncConfig` — `group0_store_id` (default 0), `group0_group_id`
     (default 0), `sync_interval_secs` (default 13).
   - `validate(config) -> Result<(), String>` — checks:
     `block_size_bytes` is a power of 2 and in [512 KB, 2 MB];
     `zone_size_bytes` > 0 and a multiple of `block_size_bytes`;
     `allocate_granularity` == `block_size_bytes` (v1: granularity =
     block size); `free_flush_max_batch` > 0; `snapshot_interval_secs`
     > 0; `listen_addr` parses as `SocketAddr`.

5. **Bitmap utilities** — create `app/crow-diskdb/src/zone/bitmap.rs`:
   - `UsageBitmap` — wraps `Vec<AtomicU64>` for lock-free bit
     operations. `range_set(offset, count) -> bool` (atomic fetch_or,
     rollback on collision), `range_clear(offset, count) -> bool`
     (atomic fetch_and, rollback on collision), `snapshot() ->
     Vec<u8>`, `restore(bytes) -> Self`, `count_set() -> u64`.
   - `create_usage_bitmap(block_count: u32) -> UsageBitmap`.
   - Unit tests: double-set detection, double-clear detection,
     snapshot/restore round-trip, boundary alignment.

6. **CRC integrity** — add `checksum` helpers for `ZoneRecord` and
   `ZoneSnapshot`: `compute_checksum()` (CRC32 over serialized record
   with checksum field zeroed), `verify_checksum() -> bool`. Used by
   R73 (snapshot write) and R75 (integrity scanner).

**Scope** (expected changed files):
- `lib/protocol/src/proto/diskdb.proto` — extend with
  `ConditionAllocateBlocks`, operator/admin RPCs, enums, `ZoneUsage`.
- `lib/protocol/src/lib.rs` — re-export new types if needed.
- `app/crow-diskdb/src/lib.rs` — add `types`, `config`, `zone` modules.
- `app/crow-diskdb/src/types/` — new module: `mod.rs`, `ids.rs`,
  `status.rs`, `zone_state.rs`, `disk_state.rs`, `disk.rs`,
  `disk_group.rs`, `node.rs`, `journal.rs` (key layout + record
  types), `instance.rs`.
- `app/crow-diskdb/src/config/` — new module: `mod.rs`,
  `validation.rs`.
- `app/crow-diskdb/src/zone/bitmap.rs` — usage bitmap with atomic
  operations.
- `app/crow-diskdb/Cargo.toml` — add `serde`, `serde_json`, `uuid`,
  `crc32fast`, `bincode` dependencies.
- `app/crow-diskdb/src/lib.rs` — update module declarations and
  re-exports.
- No server binary changes (R71+ create the server).

**Complexity**: Medium. The types are well-specified in the design doc
and the aioss reference provides concrete patterns. The main work is
defining the journal record format (BusyRecord/FreeRecord/ZoneSnapshot)
which is new — aioss uses full ZoneRecord on every write, but CROW's
D4 design uses a journal. The slot-based key layout resolves the D4
open question (prefix-scan replay, no crow-kv slot feedback needed).

**Dependencies**: The design doc (`doc/design/diskdb/design-crow-diskdb.md`). No dependency on R71–R77
— this is the foundation.

**Acceptance**:
- `lib/protocol/src/proto/diskdb.proto` defines all RPCs
  (`AllocateBlock`, `AllocateBlocks`, `ConditionAllocateBlocks`,
  `FreeBlock`, `ActiveZone`, `QueryDiskUsage`, `GetNodeInfo`,
  `GetDiskInfo`, `AddDisk`, `RemoveDisk`, `SetDiskStatus`,
  `SetDiskGroupStatus`, `SetNodeStatus`) and all enums
  (`DiskType`, `Status`, `ZoneState`, `ZoneAllocationState`,
  `DiskState`).
- Core types (`Segment`, `ClaimSnapshot`, `ZoneRecord`, `BusyRecord`,
  `FreeRecord`, `ZoneSnapshot`, `Status`, `ZoneState`,
  `ZoneAllocationState`, `DiskState`, `DiskType`, `NodeMeta`,
  `DiskGroupMeta`, `DiskMeta`, `InstanceMeta`) are defined in
  `app/crow-diskdb/src/types/` with serde serialization and unit tests
  for serialization round-trips.
- Journal key layout helpers produce the exact key formats from design
  doc §7, with unit tests verifying key format strings.
- `DiskdbConfig` with all sub-configs is defined with production
  defaults; `validate()` catches all invalid configs (non-power-of-2
  block size, zone not multiple of block, etc.).
- `UsageBitmap` passes unit tests: range_set/range_clear, double-set
  detection, double-clear detection, snapshot/restore round-trip.
- `ZoneRecord` and `ZoneSnapshot` have CRC32 checksum
  compute/verify with unit tests.
- `pixi run cargo fmt --all -- --check` and
  `pixi run cargo clippy --all-targets -- -D warnings` clean.
- `pixi run clean-env && pixi run test-kv-core` not affected (no
  crow-kv changes); new tests in `app/crow-diskdb` pass.
