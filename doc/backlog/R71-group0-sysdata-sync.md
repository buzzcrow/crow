<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R71: Group-0 Sysdata — Schema, Sync Loop, Disk Status Management

**Design doc**: [`doc/design/kv/design-crow-kv-group0.md`](../design/kv/design-crow-kv-group0.md)

**Problem**: R70 defines the core types and group-0 sysdata key layout,
but there is no code to read from or write to group 0, no sync loop,
and no disk status management. diskdb is a "thin, stateless client of
crow-kv" — on startup it must fetch its assigned disk-groups, their
disks, and the ownership/binding maps from group 0, and it must
periodically re-sync to detect ownership changes, new disks, and
status updates. Without this, diskdb cannot know which disk-groups it
owns or which paxos data group to write zone records to.

A second, related problem: **group-0 sysdata is currently written by
the wrong component, using the wrong schema.** `crow-kv-server` owns a
`POST /topology/finalize` HTTP endpoint (`app/crow-kv-server/src/
mgmt_api.rs`) backed by `topology_kv.rs` (`lib/crow-kv/src/cluster/
topology_kv.rs`) that writes rack/node/store/group/replica metadata
into group 0 as JSON values under `/topology/...` string keys with
string `node_id`s. `crow-web`'s `http_cluster_init`
(`app/crow-web/src/mgmt.rs`) builds a `TopologyFinalizeRequest` from
console config and posts it to that endpoint. This violates two
principles:

- **`crow-kv-server` must not own domain concepts.** It is a generic
  KV store; rack/node/disk semantics do not belong there.
- **The group-0 sysdata key layout must have one owner.** R70 defined
  binary keys (`lib/crow-protocol/src/key/`), but `topology_kv.rs`
  defines a parallel JSON/string-key schema. Two schemas for the same
  keyspace is a bug.

A third problem: **R70's group-0 keys have only a binary encoding
and are missing `rack_id`.** R70 defined binary keys for all
diskdb-related keys — both group-0 sysdata (`NodeKey`, `RackKey`,
`DiskGroupKey`, `DiskKey`, `OwnerMapKey`, `BindMapKey`,
`InstanceKey`) and diskdb's own data (`ZoneKey`, `BusyBlockKey`,
`FreeBlockKey`). Group 0 holds a small amount of metadata (racks,
nodes, disk-groups, disks, maps, service instances); readability
matters more than encoding density there, so text path keys are the
right choice. diskdb's own data (zones, busy/free blocks) is high-
volume and binary keys are correct for it. The R70 key concept
structs are the right abstraction, but they need a second encoding
(`TextKey`) and `rack_id` in the hierarchy fields (R70 omitted it
from `DiskGroupKey`, `DiskKey`, etc., which prevents rack-level
prefix scans).

**Solution**: Implement the group-0 sysdata architecture defined in
`doc/design/kv/design-crow-kv-group0.md`. Make `crow-kv-client` the
single home of the group-0 sysdata API with multiple service classes
(hardware, service registry, KV-cluster metadata, KV-cluster admin).
Add a `TextKey` encoding trait to `crow-protocol` alongside the
existing `BinaryKey`; update the R70 group-0 key concepts with
`rack_id` and implement `TextKey` on them. Use text path keys with
JSON values for group 0 (readable, scan-friendly); keep binary keys
for diskdb's own data groups. Migrate the existing topology-write
path onto the new client; make `crow-kv-server`'s mgmt API internal
(absorb `ServerClient`'s mgmt methods into `KVClusterAdmin`); then
build the diskdb keep-alive + sync loop, status management, and node
container on top of the same client classes.

**Architectural decisions** (key concept, schema, monitoring models,
keep-alive, kv-server mgmt API becoming internal) are defined in the
design doc; the steps below are the implementation plan.

1. **Unified key concept with two encodings** — the key concept
   (struct + fields) is the single source of truth in
   `lib/crow-protocol/src/key/`. Each key type is a plain struct with
   hierarchy fields. Two encoding traits map the same struct to
   bytes:
   - `BinaryKey` (existing) — `magic_byte | type_tag:u16 BE | fields
     BE`, prost-encoded protobuf values. Used by diskdb data groups
     (high-volume, machine-only).
   - `TextKey` (new) — `/magic/type/<field1>/<field2>/...` slash-
     delimited path, JSON-encoded values (serde on the same proto
     types). Used by group 0 (small, human-inspected, scan-friendly).

   ```
   Same key concept:  DiskKey { rack_id, node_id, dg_id, disk_id }

   Binary encoding:   0xC0 | 0x0004 | rack_id:u64 | node_id:u64 | dg_id:u32 | disk_id:16B
                      value = prost_encode(DiskValue)

   Text encoding:     /hw/disk/<rack_id>/<node_id>/<dg_id>/<disk_id_hex>
                      value = serde_json::to_vec(DiskValue)
                      (/hw = magic, /disk = type, rest = fields in text)
   ```

   - `BinaryKey::TYPE_TAG` and `TextKey::PATH_TYPE` are two
     representations of the same kind discriminator.
   - **Magic is per-encoding, not shared.** The binary magic
     (`CROW_KEY_MAGIC` = 0xC0) is one const today and can stay one
     const for all binary keys, or be split into per-namespace magics
     later (e.g. one for hardware, one for data-group records) — a
     future decision, not needed now. The text encoding uses
     different path-prefix magics per namespace from the start:
     `/hw` for hardware, `/srv` for service registry, `/kv` for
     KV-cluster topology. The two
     encodings are independent — the text magic set does not need to
     mirror the binary magic set. What matters is that within one
     encoding, the magic + type pair uniquely identifies the key
     kind.
   - A key type implements `BinaryKey`, `TextKey`, or both. Group-0
     keys implement `TextKey` (and may implement `BinaryKey` if a
     future need arises). Data-group keys (`ZoneKey`, `BusyBlockKey`,
     `FreeBlockKey`) implement `BinaryKey` only.
   - `crow-protocol`'s `build.rs` already derives
     `serde::Serialize`/`Deserialize` on `RackValue`, `NodeValue`,
     `DiskValue`, `DiskGroupValue`, `HwStatus`, `DiskId`, etc., so
     JSON values use the proto types directly — no Rust type
     duplication (§3.8 preserved).

   The hardware hierarchy is rack → node → disk-group → disk. A node
   contains one or more disk-groups. To enable scanning all
   disk-groups in one prefix scan AND narrowing by rack or node,
   `rack_id` is embedded in the dg and disk key fields (a schema
   update from R70, which omitted `rack_id` from `DiskGroupKey` and
   `DiskKey`):

   ```
   # Service registry — scan all instances of a service
   /srv/diskdb/<instance_id>                              -> InstanceValue
   # Future: /srv/chunkdb/<instance_id>, /srv/<service>/<instance_id>

   # KV-cluster topology — persistent records for disaster recovery
   /kv/store/<store_id>                                   -> StoreValue
   /kv/group/<store_id>/<group_id>                        -> GroupValue
   /kv/replica/<store_id>/<group_id>/<replica_id>         -> ReplicaValue

   # Hardware hierarchy — full path in each key for scan narrowing
   /hw/rack/<rack_id>                                     -> RackValue
   /hw/node/<rack_id>/<node_id>                           -> NodeValue
   /hw/dg/<rack_id>/<node_id>/<dg_id>                     -> DiskGroupValue
   /hw/disk/<rack_id>/<node_id>/<dg_id>/<disk_id_hex>     -> DiskValue
   # disk_id_hex = 32-char hex (128-bit DiskId, high:low, lowercase)

   # Per-disk-group maps — same hierarchy path as the dg they refer to
   /hw/dg_owner/<rack_id>/<node_id>/<dg_id>               -> DiskdbOwnerEntry
   /hw/dg_bind/<rack_id>/<node_id>/<dg_id>                -> KVGroupBindEntry
   ```

   **Scan patterns supported by this layout**:
   - `/hw/rack/` → all racks.
   - `/hw/node/` → all nodes; `/hw/node/<rack_id>/` → nodes in one
     rack.
   - `/hw/dg/` → all disk-groups; `/hw/dg/<rack_id>/` → disk-groups
     in one rack; `/hw/dg/<rack_id>/<node_id>/` → disk-groups of one
     node.
   - `/hw/disk/` → all disks; `/hw/disk/<rack_id>/` → disks in one
     rack; `/hw/disk/<rack_id>/<node_id>/` → disks of one node;
     `/hw/disk/<rack_id>/<node_id>/<dg_id>/` → disks of one
     disk-group.
   - `/hw/dg_owner/` → entire ownership map; `/hw/dg_owner/<rack_id>/`
     → entries in one rack; `/hw/dg_owner/<rack_id>/<node_id>/` →
     entries for one node.
   - `/hw/dg_bind/` → entire binding map (same narrowing as
     dg_owner).
   - `/srv/diskdb/` → all live diskdb instances (for discovery and
     R78 notify).
   - `/kv/store/` → all stores; `/kv/group/` → all groups;
     `/kv/group/<store_id>/` → groups in one store;
     `/kv/replica/` → all replicas;
     `/kv/replica/<store_id>/<group_id>/` → replicas in one group.

   `DiskdbOwnerEntry` = `{ instance_id: u64, lease_expiry_ms: u64 }`,
   `KVGroupBindEntry` = `{ store_id: u64, group_id: u64 }`,
   `InstanceValue` = `{ instance_id, grpc_endpoint: String,
   last_heartbeat_ms: u64, owned_dg_ids: Vec<u32> }` — plain Rust
   structs with `serde::Serialize`/`Deserialize`, JSON-encoded in the
   KV value (internal sysdata, not exposed via gRPC, per §5).

2. **`HardwareClient` in `crow-kv-client`** — create
   `lib/crow-kv-client/src/hardware.rs`. Wraps a `CrowkvClient`
   pinned to group 0 (store 0, group 0). Owns the group-0 text path
   key layout and the hardware hierarchy + maps. All writes are blind
   puts (no CAS, §3.3); values < 1 KB.
   - **Hardware hierarchy operations**:
     - `add_rack(rack_id, value: &RackValue) -> Result<()>` /
       `remove_rack(rack_id) -> Result<()>`.
     - `add_node(rack_id, node_id, value: &NodeValue) -> Result<()>`
       / `remove_node(rack_id, node_id) -> Result<()>`.
     - `create_disk_group(rack_id, node_id, dg_id, value:
       &DiskGroupValue) -> Result<()>` / `remove_disk_group(rack_id,
       node_id, dg_id) -> Result<()>` — also updates the parent
       `NodeValue.disk_group_ids` list (read-modify-write via
       `get`+`put`; no CAS, exclusive ownership per §3.3).
     - `add_disk(rack_id, node_id, dg_id, disk_id, value:
       &DiskValue) -> Result<()>` / `remove_disk(rack_id, node_id,
       dg_id, disk_id) -> Result<()>` — also updates the parent
       `DiskGroupValue.disk_ids` list.
     - `set_rack_status(rack_id, status: HwStatus) -> Result<()>` /
       `set_node_status(rack_id, node_id, status)` /
       `set_disk_group_status(rack_id, node_id, dg_id, status)` /
       `set_disk_status(rack_id, node_id, dg_id, disk_id, status)` —
       read-modify-write the value, bumping `status_changed_at_ms`
       and clearing/setting `temp_failure_since_ms` as appropriate.
   - **Read helpers** (all via `get` or prefix `scan`):
     - `read_rack(rack_id) -> Result<Option<RackValue>>`.
     - `read_all_racks() -> Result<Vec<(u64, RackValue)>>`.
     - `read_node(rack_id, node_id) -> Result<Option<NodeValue>>`.
     - `read_nodes_for_rack(rack_id) -> Result<Vec<(u64,
       NodeValue)>>`.
     - `read_all_nodes() -> Result<Vec<(u64, NodeValue)>>`.
     - `read_disk_group(rack_id, node_id, dg_id) ->
       Result<Option<DiskGroupValue>>`.
     - `read_disk_groups_for_node(rack_id, node_id) ->
       Result<Vec<(u32, DiskGroupValue)>>`.
     - `read_all_disk_groups() -> Result<Vec<DiskGroupEntry>>`
       (`DiskGroupEntry` = `{ rack_id, node_id, dg_id, value }`).
     - `read_disk(rack_id, node_id, dg_id, disk_id) ->
       Result<Option<DiskValue>>`.
     - `read_disks_for_disk_group(rack_id, node_id, dg_id) ->
       Result<Vec<(DiskId, DiskValue)>>`.
   - **Ownership / binding maps**:
     - `read_owner_map() -> Result<Vec<DiskdbOwnerEntry>>`
       (`DiskdbOwnerEntry` = `{ rack_id, node_id, dg_id, instance_id,
       lease_expiry_ms }` — key fields parsed from the path, included
       in the returned struct).
     - `read_bind_map() -> Result<Vec<KVGroupBindEntry>>` (same
       pattern).
     - `set_owner_entry(rack_id, node_id, dg_id, instance_id,
       lease_expiry_ms) -> Result<()>`.
     - `set_bind_entry(rack_id, node_id, dg_id, store_id, group_id)
       -> Result<()>`.
   - Re-export `HardwareClient` and the sysdata structs
     (`DiskdbOwnerEntry`, `KVGroupBindEntry`, `DiskGroupEntry`) from
     `crow-kv-client`'s `lib.rs`. The proto `*Value` types are re-
     exported from `crow-protocol` (already the source of truth).

3. **`ServiceRegistryClient` in `crow-kv-client`** — create
   `lib/crow-kv-client/src/service_registry.rs`. Wraps a
   `CrowkvClient` pinned to group 0. Manages the service instance
   registry under `/srv/<service>/<instance_id>`. **Generic across
   services** (diskdb and kv-server now; chunkdb and future services
   later) — the `service` name selects the path namespace and value
   shape.
   - Generic API:
     - `register(service: &str, instance_id: u64, value:
       &InstanceValue) -> Result<()>` — writes
       `/srv/<service>/<instance_id>` (JSON-encoded).
     - `heartbeat(service: &str, instance_id: u64, value:
       &InstanceValue) -> Result<()>` — updates
       `last_heartbeat_ms = now()` and the service-specific extra
       fields on every keep-alive tick.
     - `unregister(service: &str, instance_id: u64) -> Result<()>` —
       deletes the instance key (clean shutdown).
     - `read_instance(service: &str, instance_id: u64) ->
       Result<Option<InstanceValue>>`.
     - `read_all_instances(service: &str) ->
       Result<Vec<(u64, InstanceValue)>>` — prefix scan
       `/srv/<service>/`. Used by the console for discovery and by
       R78 notify to find live instance endpoints.
   - `InstanceValue` (proto in `crow-protocol`, see §3.3 of the
     design doc): `{ instance_id, grpc_endpoint, last_heartbeat_ms,
     extra: ServiceExtra }`. `ServiceExtra` is a per-service oneof
     — diskdb carries `owned_dg_ids: Vec<u64>`; kv-server carries
     `hosted_stores: Vec<u64>`, `hosted_groups: Vec<(u64, u64)>`,
     `health` (aggregate). Convenience wrappers
     (`register_diskdb`/`heartbeat_diskdb`/`read_all_diskdb_instances`
     and the kv-server equivalents) delegate to the generic methods.
   - Re-export `ServiceRegistryClient` and `InstanceValue` from
     `crow-kv-client`'s `lib.rs`.
   - Add `crow-protocol` and `prost` as dependencies of
     `crow-kv-client` (verified no cycle: `crow-protocol` depends on
     neither `crow-kv` nor `crow-kv-client`). `serde_json` is already
     a dependency.

4. **`KVClusterMetaClient` in `crow-kv-client`** — create
   `lib/crow-kv-client/src/kv_cluster.rs`. Wraps a `CrowkvClient`
   pinned to group 0. Owns the KV-cluster topology records (store,
   group, replica) under `/kv/...` text-path keys with JSON values.
   This is the persistent record of the KV cluster's structure —
   which stores exist, which groups are in each store, which
   replicas are in each group and on which node. It enables
   rebuilding the KV cluster topology from group 0 after a total
   console-config loss (disaster recovery).
   - **Key layout** (text-path, same encoding as hardware keys):
     ```
     /kv/store/<store_id>                              -> StoreValue
     /kv/group/<store_id>/<group_id>                   -> GroupValue
     /kv/replica/<store_id>/<group_id>/<replica_id>    -> ReplicaValue
     ```
     **Scan patterns**: `/kv/store/` → all stores;
     `/kv/group/` → all groups; `/kv/group/<store_id>/` → groups in
     one store; `/kv/replica/` → all replicas;
     `/kv/replica/<store_id>/` → replicas in one store;
     `/kv/replica/<store_id>/<group_id>/` → replicas in one group.
   - **Value types** (proto messages in `crow-protocol`, JSON-encoded;
     see design doc §3.3 — all cross-component data structures live in
     `crow-protocol`, no per-crate redefinition):
     - `StoreValue { store_id: u64, node_ids: Vec<u64> }` — which
       nodes host this store.
     - `GroupValue { store_id: u64, group_id: u64 }` — a group
       within a store.
     - `ReplicaValue { store_id: u64, group_id: u64, replica_id:
       u64, node_id: u64, role: String, voting: bool, endpoint:
       String }` — a replica within a group.
   - **Operations**:
     - `add_store(store_id, node_ids: &[u64]) -> Result<()>` /
       `remove_store(store_id) -> Result<()>`.
     - `add_group(store_id, group_id) -> Result<()>` /
       `remove_group(store_id, group_id) -> Result<()>`.
     - `add_replica(store_id, group_id, replica_id, value:
       &ReplicaValue) -> Result<()>` / `remove_replica(store_id,
       group_id, replica_id) -> Result<()>`.
     - `read_store(store_id) -> Result<Option<StoreValue>>` /
       `read_all_stores() -> Result<Vec<(u64, StoreValue)>>`.
     - `read_groups_for_store(store_id) -> Result<Vec<(u64,
       GroupValue)>>` / `read_all_groups() -> Result<Vec<GroupEntry>>`.
     - `read_replicas_for_group(store_id, group_id) ->
       Result<Vec<(u64, ReplicaValue)>>` /
       `read_all_replicas() -> Result<Vec<ReplicaEntry>>`.
   - Re-export `KVClusterMetaClient`, `StoreValue`, `GroupValue`,
     `ReplicaValue` from `crow-kv-client`'s `lib.rs`.
   - **Relationship to `GET /topology`**: `KVClusterMetaClient` owns
     the **persistent** topology records in group 0. `GET /topology`
     on `crow-kv-server` exports **live runtime state** (which
     stores/groups/replicas are currently running on this node, who
     is the leader). These are distinct: the records are the
     intended structure; the export is the current observed state.
     `crow-kv-client`'s `TopologyCache` continues to consume
     `GET /topology` for leader discovery — that does not change.

5. **Update key concepts and add `TextKey` trait** — the R70 group-0
   key types in `lib/crow-protocol/src/key/` are NOT dead code — they
   are the key concepts. Keep them; update their fields; add a
   `TextKey` trait alongside the existing `BinaryKey`. Also add the
   missing ID type aliases (design doc §2.5) and widen `DiskGroupId`
   u32 → u64 (design doc §3.5). ID aliases are defined **once** in
   `crow-protocol` and imported everywhere — no per-crate redefinition:
   - **ID type aliases** in `lib/crow-protocol/src/common_type.rs`:
     `NodeId` already exists (`u64`); `DiskGroupId` already exists
     (`u32`) — widen to `u64`. Add `RackId`, `StoreId`, `GroupId`,
     `ReplicaId`, `InstanceId` (all `u64`). (`DiskId` and `ChunkId`
     are already proto structs.) Re-export all from
     `lib/crow-protocol/src/lib.rs`. Use these aliases in all key
     structs, client APIs, console config, and proto comments.
   - **Add `TextKey` trait** to `lib/crow-protocol/src/key/mod.rs`:
     ```rust
     pub trait TextKey: Sized {
         const PATH_MAGIC: &'static str;  // "/hw", "/srv", "/kv"
         const PATH_TYPE: &'static str;   // "rack", "disk", "diskdb", "store", ...
         fn encode_to_path(&self) -> String;
         fn decode_path(parts: &[&str]) -> Result<Self, KeyError>;
         fn to_path(&self) -> String { self.encode_to_path() }
         fn from_path(s: &str) -> Result<Self, KeyError> { ... }
         // Prefix helpers for scan narrowing
         fn prefix_all() -> String { format!("{}/{}/", Self::PATH_MAGIC, Self::PATH_TYPE) }
     }
     ```
   - **Update group-0 key types** to include `rack_id` in their
     fields (schema update from R70), widen `disk_group_id` from
     `u32` to `u64` (`DiskGroupId`), **drop `dc_id` from `RackKey`**
     (design doc §3.6), and implement `TextKey`:
     - `RackKey { rack_id: RackId }` → text `/hw/rack/<rack_id>`
       (R70 had `{ dc_id, rack_id }`; drop `dc_id`).
     - `NodeKey { rack_id: RackId, node_id: NodeId }` → text
       `/hw/node/<rack_id>/<node_id>` (R70 had only `node_id`; add
       `rack_id`).
     - `DiskGroupKey { rack_id, node_id, dg_id: DiskGroupId }` →
       text `/hw/dg/<rack_id>/<node_id>/<dg_id>` (R70 had only
       `node_id, dg_id: u32`; add `rack_id`, widen `dg_id` to u64).
     - `DiskKey { rack_id, node_id, dg_id: DiskGroupId, disk_id:
       DiskId }` → text
       `/hw/disk/<rack_id>/<node_id>/<dg_id>/<disk_id_hex>` (R70 had
       only `node_id, dg_id: u32, disk_id`; add `rack_id`, widen
       `dg_id`).
     - `OwnerMapKey { rack_id, node_id, dg_id: DiskGroupId }` →
       text `/hw/dg_owner/<rack_id>/<node_id>/<dg_id>` (add
       `rack_id`, widen `dg_id`).
     - `BindMapKey { rack_id, node_id, dg_id: DiskGroupId }` →
       text `/hw/dg_bind/<rack_id>/<node_id>/<dg_id>` (add
       `rack_id`, widen `dg_id`).
     - `InstanceKey { instance_id: InstanceId, service: String }` →
       text `/srv/<service>/<instance_id>` (`PATH_MAGIC = "/srv"`,
       `PATH_TYPE` = the service name). Generic across services
       (diskdb, kv-server).
   - **Widen proto `disk_group_id` fields** from `uint32` to
     `uint64` (design doc §3.5): `NodeValue.disk_group_ids`
     (`repeated uint32` → `repeated uint64`), `NodeValue.last_used_dg_id`
     (`uint32` → `uint64`); `diskdb_op.proto`
     (`AllocateBlocksRequest.disk_group_id`,
     `QueryCapacityStatsRequest.disk_group_id`,
     `GetDiskGroupInfoRequest.disk_group_id`,
     `GetDiskInfoRequest.disk_group_id`); and the Info response types
     (`NodeInfo.disk_group_ids`, `NodeInfo.last_used_dg_id`,
     `DiskInfo.disk_group_id`, `DiskGroupInfo.disk_group_id`). This
     is a breaking schema change, acceptable because diskdb is
     greenfield.
   - **Drop `dc_id` from proto** (design doc §3.6): `RackInfo.dc_id`,
     `NodeInfo.dc_id`, `NodeValue.dc_id`. v1 ships flat (rack → node
     → disk-group → disk, no DC layer).
   - **Add new KV-cluster key types** (not in R70) implementing
     `TextKey` only (no `BinaryKey` — these are group-0 only):
     - `KvStoreKey { store_id: u64 }` → text `/kv/store/<store_id>`
       (`PATH_MAGIC = "/kv"`, `PATH_TYPE = "store"`).
     - `KvGroupKey { store_id, group_id }` → text
       `/kv/group/<store_id>/<group_id>`.
     - `KvReplicaKey { store_id, group_id, replica_id }` → text
       `/kv/replica/<store_id>/<group_id>/<replica_id>`.
   - **Add new group-0 sysdata value types** as proto messages in
     `crow-protocol` (design doc §3.3): `StoreValue`, `GroupValue`,
     `ReplicaValue`, `InstanceValue` (with `ServiceExtra` oneof),
     `OwnerMapValue { instance_id, lease_expiry_ms }`,
     `BindMapValue { store_id, group_id }`. Add `serde::Serialize`/
     `Deserialize` derives in `build.rs`. Add plain serde Entry
     return structs: `DiskGroupEntry`, `DiskdbOwnerEntry`,
     `KVGroupBindEntry` (key fields + value).
   - **Delete `diskdb_sys_service.proto` and `diskdb_sys_op.proto`**
     (design doc §2.8) — the `DiskdbAdminService` gRPC surface is
     removed (admin ops move to `HardwareClient` via the console;
     `FetchHardware`/`Keepalive` are replaced by kv-client calls).
     Remove them from `build.rs`'s proto list.
   - **Keep `BinaryKey` impls** on group-0 key types where they
     already exist (R70 defined them). They are unused in v1 but
     available if a future need puts group-0 sysdata on a binary
     encoding. Update the binary field layouts to include `rack_id`
     (and drop `dc_id` from `RackKey`) so binary and text encodings
     round-trip the same struct.
   - **Data-group keys** (`ZoneKey`, `BusyBlockKey`, `FreeBlockKey`)
     keep `BinaryKey` only — no `TextKey` impl.
   - Update `tests.rs` to cover both encodings for group-0 keys and
     `TextKey` for the new KV-cluster keys.
   - Update `lib.rs` re-exports to include `TextKey` and the new
     `KvStoreKey`, `KvGroupKey`, `KvReplicaKey`.
   - Update `doc/design/protocol/design-crow-key.md` §5 to document
     the unified key concept: one struct, two encoding traits
     (`BinaryKey` + `TextKey`), per-namespace encoding choice
     (binary + prost for data groups, text + JSON for group 0).
   - Update proto comments in `common_type.proto` and
     `diskdb_type.proto` that reference key types to reflect the
     unified concept (e.g. "Rack key is a `RackKey` (TextKey encoding
     `/hw/rack/<rack_id>` in group 0)").

6. **Migrate the existing topology-write path onto `HardwareClient` + `KVClusterMetaClient`**:
   - **Delete `lib/crow-kv/src/cluster/topology_kv.rs`** — its schema
     (JSON values, `/topology/...` string keys, string `node_id`s) is
     superseded by the text-path + JSON schema owned by
     `HardwareClient` (hardware records) and `KVClusterMetaClient`
     (KV-cluster topology records). Remove its module declaration
     and tests.
   - **Remove `POST /topology/finalize` and `GET /topology/ready`
     from `crow-kv-server`** (`app/crow-kv-server/src/mgmt_api.rs`):
     delete the `topology_finalize` and `topology_ready` handlers,
     the `TopologyFinalize*` / `TopologyReady*` JSON types, the
     `write_topology_metadata` / `put_topology_entry` helpers, and
     their routes from `router()`. The server no longer writes
     persistent topology records — that responsibility moves to
     `HardwareClient` and `KVClusterMetaClient` in `crow-kv-client`.
   - **Remove the `topology_finalize` / `topology_ready` client
     methods** from `lib/crow-console-shared/src/mgmt.rs`
     (`TopologyFinalizeRequest` / `TopologyFinalizeResponse` /
     `TopologyReadyResponse` and the `ServerClient::topology_finalize`
     / `topology_ready` calls). (The actual type is `ServerClient` in
     `clients/http.rs`; `mgmt.rs` adds methods to it — there is no
     `MgmtClient` type.)
   - **Migrate `app/crow-kv-server/src/reconcile.rs`** — it reads
     `topology_kv` (`READY_KEY`, `STORES_PREFIX`, `GROUPS_PREFIX`,
     `TopologyStore`/`TopologyGroup`) to reconcile local stores/groups
     with group 0. With `topology_kv` deleted and `/topology/ready`
     gone, rewrite it to read the new `/kv/store/` and `/kv/group/`
     records via `KVClusterMetaClient` (or directly via the local
     group-0 `PxKvStore` text-path keys), and drop the readiness-flag
     check (treat an empty `/kv/store/` prefix as "not yet
     initialized"). Update `reconcile_test.rs` accordingly.
   - The `/topology` (export) and `/system/init` endpoints stay —
     they are KV-cluster mechanics (live runtime state discovery,
     group-0 bootstrap), not persistent domain records.

7. **Refactor `crow-web` cluster init onto `HardwareClient` +
   `KVClusterMetaClient`** — `app/crow-web/src/mgmt.rs::http_cluster_init`:
   - After Phase 4 (persist topology in console config) and once the
     group-0 leader is elected (Phase 1–3 unchanged: `/system/init` +
     remote wiring), build a `CrowkvClient` seeded with the
     group-0 gRPC endpoints and wrap it in `HardwareClient` and
     `KVClusterMetaClient`.
   - Replace Phase 5 (`topology_finalize` call) with:
     - `HardwareClient` calls: `add_rack` for each console rack,
       `add_node` for each console node (mapping console string
       `node_id` (`RackId` / `NodeId` from console config, see §10),
       `set_node_status(..., HwStatus::Up)` for each. This writes
       the hardware hierarchy into group 0.
     - `KVClusterMetaClient` calls: `add_store` for each store,
       `add_group` for each group, `add_replica` for each replica.
       This writes the KV-cluster topology records into group 0
       under `/kv/...`.
   - **Verify leader readiness before writing**: both clients write
     via gRPC to the group-0 leader, so Phase 5 can only proceed
     after a group-0 leader is elected and reachable. For single-
     node init this is immediate (self-elect). For multi-node, verify
     the existing init flow waits on leader election before Phase 5;
     if not, add a readiness poll (retry `get` on a test key until
     it succeeds or a timeout fires).
   - **No `/topology/ready` flag**: the old schema had a readiness
     flag key; the text-path schema has no equivalent. v1 does not
     need it — diskdb's sync loop treats an empty group 0 as
     "nothing assigned yet" and retries. If a readiness signal is
     needed later, add a derived condition.
   - **Schema migration**: diskdb is greenfield (R70 just merged, no
     production diskdb). The old `/topology/...` records are
     superseded by `/hw/...` and `/kv/...`. Treat as greenfield —
     require a fresh cluster init. Old `/topology/...` keys are
     orphaned harmlessly; clean them up if they cause confusion.

8. **`crow-kv-server` mgmt API becomes internal; `crow-kv-client`
   is the single management API surface** — today `crow-web` and
   `crow-cli` call `ServerClient` (in `crow-console-shared`'s
   `clients/http.rs`, with mgmt methods added in `mgmt.rs`) directly
   to reach kv-server's HTTP mgmt API (21+ call sites in `crow-web`
   alone). After this step, `crow-kv-client` becomes the sole
   management API surface, and kv-server's mgmt API is internal
   (only called by `crow-kv-client`).
   - **`crow-kv-server` keeps its HTTP mgmt API** — the lifecycle
     endpoints (`add_store`, `remove_store`, `add_group`,
     `remove_group`, `add_remote_replicas`,
     `remove_remote_replica`, `step_down`, `join_group_via_snapshot`,
     `flush_group`, `system_init`) operate on local in-process state
     (create/destroy `PxKvStore`/`PxGroup` with WALs and election
     drivers) — they cannot move to a client. The runtime state
     export (`GET /topology`, `GET /health`, `GET /metrics`) also
     stays. But these endpoints are now internal: no code outside
     `crow-kv-client` calls them.
   - **Removed from kv-server**: `POST /topology/finalize`,
     `GET /topology/ready` — persistent topology-record management
     moves to `KVClusterMetaClient` / `HardwareClient` in
     `crow-kv-client`.
   - **New `KVClusterAdmin` class in `crow-kv-client`**
     (`lib/crow-kv-client/src/kv_cluster.rs`, alongside
     `KVClusterMetaClient`): the control-plane surface for cluster
     management. **Contains a `KVClusterMetaClient`** internally.
     Wraps the kv-server HTTP mgmt API for lifecycle operations AND
     writes group-0 metadata records by delegating to its inner
     `KVClusterMetaClient`. Each lifecycle method does both:
     1. Call kv-server HTTP mgmt API to create/destroy the actual
        store/group/replica (WAL, election driver).
     2. Delegate to `self.meta.add_*` / `self.meta.remove_*` to
        write/delete the corresponding `/kv/...` metadata record in
        group 0.
     - `add_store(node_endpoint, store_id, node_ids) -> Result<()>`:
       HTTP `POST /stores` on the target node, then
       `KVClusterMetaClient.add_store(store_id, node_ids)`.
     - `remove_store(node_endpoint, store_id) -> Result<()>`: HTTP
       `DELETE /stores/{sid}`, then
       `KVClusterMetaClient.remove_store(store_id)`.
     - `add_group(node_endpoint, store_id, group_id, replica_id,
       initial_role) -> Result<()>`: HTTP `POST /stores/{sid}/groups`,
       then `KVClusterMetaClient.add_group(store_id, group_id)`.
     - `remove_group(node_endpoint, store_id, group_id) ->
       Result<()>`: HTTP `DELETE`, then
       `KVClusterMetaClient.remove_group(store_id, group_id)`.
     - `add_replica(node_endpoint, store_id, group_id,
       replica_info) -> Result<()>`: HTTP
       `POST /stores/{sid}/groups/{gid}/remotes`, then
       `KVClusterMetaClient.add_replica(store_id, group_id,
       replica_id, value)`.
     - `remove_replica(node_endpoint, store_id, group_id,
       replica_id) -> Result<()>`: HTTP `DELETE`, then
       `KVClusterMetaClient.remove_replica(...)`.
     - `step_down(node_endpoint, store_id, group_id, reason) ->
       Result<StepDownResult>`: HTTP `POST .../step-down` (no
       metadata change — operational, not structural).
     - `join_group(node_endpoint, store_id, group_id,
       peer_endpoint) -> Result<()>`: HTTP `POST .../join` (metadata
       update via `add_replica` after the join succeeds).
     - `flush_group(node_endpoint, store_id, group_id) ->
       Result<()>`: HTTP `POST .../flush` (no metadata change —
       operational).
     - `system_init(node_endpoint, req) -> Result<SystemInitResponse>`:
       HTTP `POST /system/init` (no metadata change — bootstrap
       mechanics).
     - Query methods (read-only, no metadata write):
       `list_stores(node_endpoint)`, `list_groups(node_endpoint,
       store_id)`, `list_remote_replicas(node_endpoint, store_id,
       group_id)`, `export_topology(node_endpoint)`,
       `health(node_endpoint)`, `metrics(node_endpoint)`.
   - **Absorb `ServerClient`'s mgmt methods into `KVClusterAdmin`**
     — the mgmt methods defined in
     `lib/crow-console-shared/src/mgmt.rs` (on `ServerClient`) move
     to `KVClusterAdmin`. `KVClusterAdmin` owns its own HTTP
     transport (reqwest) and a `KVClusterMetaClient`. `ServerClient`
     is removed from `crow-console-shared` once all call sites
     (`mgmt.rs`, `lifecycle.rs`, `monitor.rs`, `topology.rs`,
     `ssh.rs` tests, `clients/console.rs`) migrate to
     `KVClusterAdmin` (lifecycle/monitor/topology use the query +
     lifecycle methods; the `ServerClient::new` transport becomes
     `KVClusterAdmin`'s internal reqwest client). The HTTP
     request/response types (`AddStoreRequest`, `AddGroupRequest`,
     `RemoteReplicaInfo`, `StepDownRequest`, `StepDownResult`,
     `SystemInitRequest`, `SystemInitResponse`, `StoreSummary`,
     `GroupSummary`, `TopologyResponse`, etc.) move to
     `crow-protocol` under a new `mgmt` module
     (`lib/crow-protocol/src/mgmt.rs` or
     `lib/crow-protocol/src/proto/mgmt_api.proto`). All
     cross-component protocol types live in `crow-protocol`;
     `crow-kv-client`, `crow-console-shared`, `crow-web`, and
     `crow-cli` import from `crow-protocol::mgmt`.
   - **Refactor callers**: `crow-web` (`mgmt.rs`, `physical.rs`,
     `lifecycle.rs` — 21+ call sites) and `crow-cli` (via
     `ConsoleClient` → `ServerClient` indirection) switch from
     `ServerClient` to `KVClusterAdmin`. This is the bulk of the
     refactor work.
   - **`crow-kv-server` is domain-free**: `crow-kv` and
     `crow-kv-server` must not depend on `crow-protocol`'s diskdb
     keys or proto types. The server's mgmt API speaks only in
     KV-cluster terms (store/group/replica), not hardware terms.

9. **Keep-alive + sync loop** — create `app/crow-diskdb/src/sync/`
   module. diskdb communicates with group 0 via two kv-client service
   classes: `ServiceRegistryClient` (keep-alive) and
   `HardwareClient` (hardware state read). The sync loop combines
   both in one tick:
   - `SyncLoop` — owns a `HardwareClient`, a `ServiceRegistryClient`,
     a `NodeContainer` (shared state), and a `SyncConfig`. Runs as a
     `tokio::spawn` background task.
   - `run()` — loop: `sleep(interval)` → `sync_once()` → repeat.
   - `sync_once() -> Result<SyncOutcome>`:
     a. **Keep-alive**: `ServiceRegistryClient.heartbeat_diskdb(
        instance_id, endpoint, owned_dg_ids)` — registers/refreshes
        this diskdb instance in group 0 under
        `/srv/diskdb/<instance_id>`. This is the heartbeat that
        tells the cluster this instance is alive and which
        disk-groups it currently owns.
     b. **Read ownership**: `HardwareClient.read_owner_map()`; filter
        to entries where `instance_id == self.instance_id` — the
        disk-groups this instance owns.
     c. **Read bindings**: `HardwareClient.read_bind_map()`; for each
        owned disk-group, look up its `(store_id, group_id)` — the
        paxos data group for zone journals.
     d. **Read hardware**: for each owned disk-group,
        `HardwareClient.read_disk_group` +
        `HardwareClient.read_disks_for_disk_group`; build/update the
        in-memory `NodeContainer` state.
     e. Detect changes: new disk-groups assigned, disk-groups removed,
        disks added/removed, status changes (apply transitions).
     f. Return `SyncOutcome` with counts: `groups_added`,
        `groups_removed`, `disks_added`, `disks_removed`,
        `status_changes`, `sync_duration_ms`.
   - **Degraded mode**: track `missed_count` of consecutive sync
     failures. After `miss_threshold` (default 3), enter degraded mode
     (`NodeContainer.enter_degraded_mode()`). In degraded mode,
     allocation/free RPCs return `Unavailable` (aioss pattern). On
     first successful sync, exit degraded mode.
   - **Notify mechanism (deferred to R78, simplified)**: §10 raises a
     watch/notify as an open question. v1 uses polling at fixed 10 s
     (same on success and failure — no back-off in v1). Because all
     diskdb instances register their gRPC endpoints in group 0 via
     `ServiceRegistryClient` (under `/srv/diskdb/<instance_id>`),
     R78's notify path is straightforward: when hardware status
     changes, the notifier scans `/srv/diskdb/` to discover all live
     instance endpoints and pushes notifications to them. No
     separate discovery channel needed.

10. **Disk status management** — create
   `app/crow-diskdb/src/status/` module:
   - `StatusManager` — applies status transitions and computes
     effective status. Integrated with the sync loop. All persistent
     writes go through `HardwareClient.set_*_status`; in-memory state
     via `NodeContainer`.
   - `apply_node_status(rack_id, node_id, new_status: HwStatus)` —
     validates transition legality (design doc §9), calls
     `HardwareClient.set_node_status`, updates in-memory state.
   - `apply_disk_group_status(rack_id, node_id, dg_id, new_status)`
     — same.
   - `apply_disk_status(rack_id, node_id, dg_id, disk_id,
     new_status)` — same.
   - `effective_status(node_status, group_status, disk_status) ->
     HwStatus` — `max(node, group, disk)` (`HwStatus` is `Ord`,
     ordered by severity).
   - **Transition rules** (design doc §9):
     - Init → {Up, Offline, Maintenance} on startup (load from
       group 0).
     - Up → Suspect (3 missed syncs).
     - Up → Offline / Maintenance (operator).
     - Suspect → Up (sync recovers) or → Missing (cannot probe) or →
       Offline.
     - Missing → Bad (confirmed) or → Up (rediscovered). **Missing is
       detected by absence from a group-0 sync response.**
     - Offline ↔ Maintenance (operator).
     - Offline → Up (operator).
   - `check_suspect_timeouts()` — called on each sync tick;
     transitions any disk/disk-group/node in `Suspect` >
     `temp_failure_timeout_secs` (default 900 s / 15 min) to
     `Offline`.
   - `allows_allocate(effective: HwStatus) -> bool` — `Up` only.
   - `allows_free(effective: HwStatus) -> bool` — `Up`, `Maintenance`,
     or `Suspect`.

11. **NodeContainer** — create `app/crow-diskdb/src/node/` module:
    - `NodeContainer` — per-instance singleton managing all owned
      disk-groups. `nodes: RwLock<HashMap<DiskGroupId, Arc<Node>>>`,
      `instance_id: u64`, `config: DiskdbConfig`, `degraded:
      AtomicBool`.
    - `add_node(node)`, `remove_node(dg_id)`, `get_node(dg_id) ->
      Option<Arc<Node>>`, `node_ids() -> Vec<DiskGroupId>`.
    - `enter_degraded_mode()` / `exit_degraded_mode()` /
      `is_degraded() -> bool` — atomic flag.
    - `Node` — disk-group manager. `disk_group_id: DiskGroupId`,
      `node_id: u64`, `rack_id: u64`, `disks:
      RwLock<HashMap<DiskId, Arc<ZoneDisk>>>`,
      `allocating_disks: RwLock<Arc<AllocateDiskContext>>` (RCU
      context of allocatable disks, R72), `pos_v_disk_ctx: AtomicU64`
      (round-robin cursor, R72), `status: RwLock<HwStatus>`, `bind:
      (u64, u64)` (`(store_id, group_id)` for the bound paxos data
      group).
    - `ZoneDisk` — disk struct with `disk_id: DiskId`, `disk_group_id:
      DiskGroupId`, `node_id: u64`, `rack_id: u64`, `disk_value:
      RwLock<DiskValue>` (capacity, zone size, unit size, status),
      `zones: RwLock<Vec<ZoneRef>>`, `active_zone_context:
      RwLock<Arc<ActiveZoneContext>>` (RCU active zone set, R72),
      `pos_v_zone_ctx: AtomicU64` (round-robin cursor over active set,
      R72), `pos_v_zone: AtomicU64` (rotating cursor for zone rotation
      scan, R72). Zone management methods (`add_zone`,
      `rebuild_active_zones`) are defined here but zone **allocation**
      logic (CAS claim) is R72.
    - v1: `ZoneDisk` is a single implementation for all disk types
      (BlockHdd, BlockSsd). SMR/SSD trait variants are stubbed
      (non-goal per design doc §2).

12. **diskdb gRPC service (stubs)** — create
    `app/crow-diskdb/src/grpc/mod.rs` wiring the tonic-generated
    `DiskdbService` trait. **No admin gRPC service** (design doc §2.8):
    hardware admin ops go through the console via `HardwareClient`/
    `KVClusterAdmin` in `crow-kv-client`; the deleted
    `DiskdbAdminService` proto is not served by diskdb.
    - `AllocateBlocks` / `FreeBlocks` handlers return
      `Unimplemented` — R72 fills them in.
    - `GetDiskGroupInfo` / `GetDiskInfo` read from in-memory
      `NodeContainer` state (functional in v1 — the data is available
      from the sync loop).
    - `QueryCapacityStats` returns `Unimplemented` (R74).

13. **kv-server keep-alive loop** — add a background task in
    `crow-kv-server` (design doc §2.7) that registers the instance
    under `/srv/kv-server/<instance_id>` and heartbeats via
    `ServiceRegistryClient` with `hosted_stores`, `hosted_groups`,
    and aggregate health. `crow-kv-server` adds a `crow-kv-client`
    dependency (no cycle: `crow-kv-client` → `crow-kv`, and
    `crow-kv-server` already depends on `crow-kv`). The instance id
    comes from server config (new field); the loop runs on the same
    interval as diskdb's sync (default 10 s). On shutdown, the loop
    unregisters (clean shutdown).

14. **Server binary skeleton** — extend `app/crow-diskdb/` (skeleton
    exists from R70):
    - `Cargo.toml` — already depends on `crow-kv-client`,
      `crow-protocol`, `crow-common`, `tonic`, `tokio`, `clap`,
      `prost`. Add `tracing`, `serde`, `toml` if not present.
    - `src/main.rs` — CLI (clap), config loading, wiring:
      `CrowkvClient` → `HardwareClient` + `ServiceRegistryClient` →
      `NodeContainer` → `SyncLoop` → gRPC server. Spawns sync loop
      as background task, serves `DiskdbService` on `listen_addr`.
    - `src/lib.rs` — re-exports for integration tests.
    - The server starts, loads config, connects to crow-kv, runs an
      initial sync from group 0 (keep-alive + hardware read), starts
      the sync loop, and serves gRPC. Allocation/free RPCs return
      `Unimplemented` until R72.

**Scope** (expected changed files):
- `lib/crow-kv-client/src/hardware.rs` — `HardwareClient` with
  group-0 text-path keys, hardware hierarchy + maps read/write.
- `lib/crow-kv-client/src/service_registry.rs` —
  `ServiceRegistryClient` with service instance registry + keep-alive.
- `lib/crow-kv-client/src/kv_cluster.rs` — `KVClusterMetaClient` with
  KV-cluster topology records (store/group/replica) read/write.
- `lib/crow-kv-client/src/lib.rs` — module declarations + re-exports.
- `lib/crow-kv-client/Cargo.toml` — add `crow-protocol`, `prost`.
- `lib/crow-protocol/src/key/common.rs` — update `RackKey` (drop
  `dc_id`, keep `rack_id`), `NodeKey` (add `rack_id`); add `TextKey`
  impls; update `BinaryKey` layouts.
- `lib/crow-protocol/src/key/diskdb.rs` — update `DiskGroupKey`,
  `DiskKey`, `OwnerMapKey`, `BindMapKey` (add `rack_id`, widen
  `disk_group_id` u32→u64), `InstanceKey` (add `service` field);
  add `TextKey` impls; keep `BinaryKey` impls; keep
  `ZoneKey`, `BusyBlockKey`, `FreeBlockKey` unchanged (binary only).
- `lib/crow-protocol/src/key/kv_cluster.rs` — **new file**:
  `KvStoreKey`, `KvGroupKey`, `KvReplicaKey` with `TextKey` impls.
- `lib/crow-protocol/src/key/mod.rs` — add `TextKey` trait; add
  `kv_cluster` module; update re-exports to include `TextKey`,
  `KvStoreKey`, `KvGroupKey`, `KvReplicaKey`.
- `lib/crow-protocol/src/key/tests.rs` — add tests for `TextKey`
  encoding/decoding of group-0 keys and KV-cluster keys; update
  binary tests for new `rack_id` field, `dc_id` removal, and
  `DiskGroupId` u32→u64 widening.
- `lib/crow-protocol/src/common_type.rs` — widen `DiskGroupId` to
  `u64`; add `RackId`, `StoreId`, `GroupId`, `ReplicaId`,
  `InstanceId` aliases (`NodeId` already exists).
- `lib/crow-protocol/src/lib.rs` — re-export the new ID aliases,
  `TextKey`, `KvStoreKey`, `KvGroupKey`, `KvReplicaKey`, the new
  sysdata value/entry types, and the `mgmt` module.
- `lib/crow-protocol/src/proto/common_type.proto` — widen
  `NodeValue.disk_group_ids` (`repeated uint32`→`repeated uint64`)
  and `NodeValue.last_used_dg_id` (`uint32`→`uint64`); drop
  `NodeValue.dc_id`, `RackInfo.dc_id`, `NodeInfo.dc_id`; widen
  `NodeInfo.disk_group_ids`/`last_used_dg_id`,
  `DiskInfo.disk_group_id`, `DiskGroupInfo.disk_group_id` to
  `uint64`; update key reference comments to reflect the unified
  key concept.
- `lib/crow-protocol/src/proto/diskdb_type.proto` — widen
  `DiskInfo.disk_group_id`, `DiskGroupInfo.disk_group_id` to
  `uint64` (covered above); update key reference comments.
- `lib/crow-protocol/src/proto/diskdb_op.proto` — widen
  `disk_group_id` fields (`AllocateBlocksRequest`,
  `QueryCapacityStatsRequest`, `GetDiskGroupInfoRequest`,
  `GetDiskInfoRequest`) from `uint32` to `uint64`.
- `lib/crow-protocol/src/proto/diskdb_sys_service.proto` —
  **deleted** (no admin gRPC service).
- `lib/crow-protocol/src/proto/diskdb_sys_op.proto` — **deleted**.
- `lib/crow-protocol/build.rs` — remove the deleted sys protos from
  the build list; add serde derives on the new sysdata value types
  (`StoreValue`, `GroupValue`, `ReplicaValue`, `InstanceValue`,
  `OwnerMapValue`, `BindMapValue`).
- `lib/crow-protocol/src/sysdata.rs` (or similar) — **new**: plain
  serde Entry return structs (`DiskGroupEntry`, `DiskdbOwnerEntry`,
  `KVGroupBindEntry`) and the `ServiceExtra` oneof / `InstanceValue`
  helpers if not proto-generated.
- `lib/crow-console-shared/src/config.rs` — widen every rack-id and
  node-id reference to `RackId` / `NodeId` (from `crow-protocol`):
  `RackEntry.id`, `NodeEntry.id`, `NodeEntry.rack_id`,
  `ServerEntry.node_id`, `StoreEntry.nodes`, `ReplicaEntry.node_id`,
  and the persisted BTreeMap keys (`PersistedRackEntry`/`PersistedNodeEntry`/
  `PersistedServerEntry`/`PersistedStoreEntry`). `ServerEntry.id`
  stays `String` (console handle). `crow-console-shared` adds a
  `crow-protocol` dependency. Update all consumers and test fixtures.
- `doc/design/protocol/design-crow-key.md` — update §5: document
  unified key concept (one struct, two encoding traits); per-
  namespace encoding choice (binary + prost for data groups, text +
  JSON for group 0); field updates (rack_id added to group-0 keys,
  dc_id dropped from RackKey).
- `lib/crow-kv/src/cluster/topology_kv.rs` — **deleted**; module
  declaration removed from `lib/crow-kv/src/cluster/mod.rs`.
- `app/crow-kv-server/src/reconcile.rs` — migrated off `topology_kv`
  onto `/kv/...` records (via `KVClusterMetaClient` or local
  group-0 text-path reads); readiness-flag check dropped.
- `app/crow-kv-server/src/mgmt_api.rs` — remove `topology_finalize`,
  `topology_ready`, their JSON types, helpers, and routes. The
  remaining lifecycle + export endpoints stay (now internal — only
  called by `crow-kv-client`'s `KVClusterAdmin`).
- `lib/crow-console-shared/src/clients/http.rs` — `ServerClient`
  removed (mgmt methods absorbed into `KVClusterAdmin`); transport
  becomes `KVClusterAdmin`'s internal reqwest client.
- `lib/crow-console-shared/src/mgmt.rs` — mgmt methods on
  `ServerClient` removed; HTTP request/response types moved to
  `crow-protocol::mgmt`.
- `lib/crow-protocol/src/mgmt.rs` (or
  `lib/crow-protocol/src/proto/mgmt_api.proto`) — **new module**:
  HTTP mgmt API request/response types (`AddStoreRequest`,
  `AddGroupRequest`, `RemoteReplicaInfo`, `StepDownRequest`,
  `StepDownResult`, `SystemInitRequest`, `SystemInitResponse`,
  `StoreSummary`, `GroupSummary`, `TopologyResponse`, etc.).
- `lib/crow-protocol/src/lib.rs` — add `mgmt` module; update
  re-exports.
- `app/crow-web/src/mgmt.rs` — `http_cluster_init` Phase 5 rewritten
  to use `HardwareClient` + `KVClusterMetaClient`; all `ServerClient` call
  sites (21+) switched to `KVClusterAdmin`;
  `build_topology_finalize_body` removed.
- `app/crow-web/src/physical.rs` — `ServerClient` call sites (4)
  switched to `KVClusterAdmin`.
- `app/crow-web/src/lifecycle.rs` — `ServerClient` call sites (2)
  switched to `KVClusterAdmin`.
- `app/crow-diskdb/src/sync/mod.rs` — `SyncLoop` with keep-alive +
  periodic sync and degraded mode.
- `app/crow-diskdb/src/status/mod.rs` — `StatusManager` with
  transition rules and effective status.
- `app/crow-diskdb/src/node/mod.rs` — `Node`, `NodeContainer`.
- `app/crow-diskdb/src/node/container.rs` — `NodeContainer` impl.
- `app/crow-diskdb/src/node/disk.rs` — `ZoneDisk` struct (zone
  allocation methods stubbed for R72).
- `app/crow-diskdb/src/grpc/mod.rs` — `DiskdbService` wiring
  (allocate/free stubs; `GetDiskGroupInfo`/`GetDiskInfo` functional).
- `app/crow-diskdb/src/lib.rs` — module declarations and re-exports.
- `app/crow-diskdb/src/main.rs` — wiring with `HardwareClient` +
  `ServiceRegistryClient`.
- `app/crow-diskdb/Cargo.toml` — add `tracing`, `serde`, `toml` if
  missing.
- `app/crow-kv-server/src/keepalive.rs` — **new**: kv-server
  keep-alive loop (registers/heartbeats/unregisters via
  `ServiceRegistryClient` under `/srv/kv-server/<instance_id>`).
- `app/crow-kv-server/Cargo.toml` — add `crow-kv-client` dependency.
- `app/crow-kv-server/src/main.rs` (or startup) — spawn the keep-alive
  loop; add `instance_id` to server config.
- `app/crow-kv-server/tests/system_init_test.rs`,
  `app/crow-kv-server/tests/reconcile_test.rs` — update tests that
  reference `topology_finalize` / `topology_kv`.

**Complexity**: Medium-High. The `HardwareClient` +
`ServiceRegistryClient` extraction, the `topology_kv` migration, and
the R70 group-0 key-schema update (add `rack_id`, drop `dc_id`, widen
`DiskGroupId`, add `TextKey`) touch four crates (`crow-kv-client`,
`crow-protocol`, `crow-kv`/`crow-kv-server`, `crow-web`) and are the
riskiest part: they change the cluster-init write path from an
in-process `PxKvStore::kv_put` on the leader to a gRPC
`HardwareClient` call, they delete a schema (`topology_kv`), and they
delete the `DiskdbAdminService` proto. The sync loop, status
management, and node container are well-specified in the design doc
and model directly on
the aioss reference.

**Dependencies**: R70 (core types, config — partially superseded by
this requirement's key-format change). The design doc
(`doc/design/diskdb/design-crow-diskdb.md`) and skeleton. No
dependency on R72–R77 — this is the first functional component.

**Acceptance**:
- `HardwareClient` reads and writes all group-0 hardware types
  (`RackValue`, `NodeValue`, `DiskGroupValue`, `DiskValue`,
  `DiskdbOwnerEntry`, `KVGroupBindEntry`) via `CrowkvClient` targeting store 0,
  group 0, using text-path keys with JSON values. Unit tests with a
  mock/in-process crow-kv verify round-trip and scan correctness
  (all-rack, all-node, node-in-rack, dg-in-node, disk-in-dg).
- `ServiceRegistryClient` (generic) registers, heartbeats,
  unregisters, and discovers service instances via
  `/srv/<service>/<instance_id>` text-path keys with JSON values, for
  both `diskdb` and `kv-server`. Unit tests verify round-trip and
  `read_all_instances` scan for each service.
- `KVClusterMetaClient` reads and writes KV-cluster topology records
  (`StoreValue`, `GroupValue`, `ReplicaValue`) via `/kv/store/...`,
  `/kv/group/...`, `/kv/replica/...` text-path keys with JSON
  values. Unit tests verify round-trip and scan correctness
  (all-stores, groups-for-store, replicas-for-group).
- R70 group-0 key types (`RackKey`, `NodeKey`, `DiskGroupKey`,
  `DiskKey`, `OwnerMapKey`, `BindMapKey`, `InstanceKey`) are updated
  with `rack_id` fields, `dc_id` dropped from `RackKey`,
  `DiskGroupId` widened from `u32` to `u64`, and implement `TextKey`.
  New KV-cluster key types (`KvStoreKey`, `KvGroupKey`, `KvReplicaKey`)
  implement `TextKey`. ID type aliases (`RackId`, `NodeId`,
  `DiskGroupId`, `StoreId`, `GroupId`, `ReplicaId`, `InstanceId`) are
  defined once in `crow-protocol` and used in key structs, client
  APIs, and console config. `TextKey` encode/decode round-trip tests
  pass for each type. `BinaryKey` impls on data-group keys
  (`ZoneKey`, `BusyBlockKey`, `FreeBlockKey`) are unchanged. `cargo
  build` of the workspace succeeds.
- `diskdb_sys_service.proto` and `diskdb_sys_op.proto` are deleted;
  `build.rs` no longer compiles them. No `DiskdbAdminService` gRPC
  surface exists.
- Console config uses `RackId`/`NodeId` (from `crow-protocol`) for
  every rack-id and node-id reference (`RackEntry.id`, `NodeEntry.id`,
  `NodeEntry.rack_id`, `ServerEntry.node_id`, `StoreEntry.nodes`,
  `ReplicaEntry.node_id`, persisted BTreeMap keys) — not `String`.
  `ServerEntry.id` stays `String`. `http_cluster_init` passes u64
  values directly to `HardwareClient` — no `parse::<u64>()` at the
  boundary.
- `topology_kv.rs` is deleted; `crow-kv` and `crow-kv-server` contain
  no references to rack/node/disk schema. `reconcile.rs` reads
  `/kv/...` records (no `topology_kv`, no readiness flag). `cargo`
  build of `crow-kv-server` succeeds without `crow-protocol`.
- `POST /topology/finalize` and `GET /topology/ready` are gone from
  `crow-kv-server`'s router.
- `ServerClient` is removed from `crow-console-shared`. No code
  outside `crow-kv-client` calls kv-server's HTTP mgmt API directly.
  `KVClusterAdmin` in `crow-kv-client` is the sole management API
  surface. `crow-web` and `crow-cli` use `KVClusterAdmin` for all
  cluster management operations. HTTP mgmt API types live in
  `crow-protocol::mgmt`. `cargo build` of the workspace succeeds.
- `KVClusterAdmin.add_store` / `add_group` / `add_replica` both call
  the kv-server lifecycle HTTP endpoint AND write the corresponding
  `/kv/...` metadata record to group 0. Unit tests verify both
  effects (HTTP mock + group-0 record check).
- `http_cluster_init` writes rack/node hardware records into group 0
  via `HardwareClient` and store/group/replica topology records via
  `KVClusterMetaClient` (text-path keys, JSON values). An end-to-end init
  test verifies both sets of records appear in group 0 under the
  text-path schema.
- The kv-server keep-alive loop registers the instance under
  `/srv/kv-server/<instance_id>` and heartbeats with
  `hosted_stores`/`hosted_groups`/health; unregisters on shutdown.
  Unit/integration test verifies the record appears and refreshes.
- `SyncLoop.sync_once()` performs keep-alive heartbeat via
  `ServiceRegistryClient`, then fetches ownership map, binding map,
  and disk-group/disk metadata from group 0 via `HardwareClient`;
  updates `NodeContainer`. Returns `SyncOutcome` with change counts.
- Degraded mode activates after `miss_threshold` consecutive sync
  failures and deactivates on first success. Unit test verifies the
  counter and flag transitions.
- `StatusManager` enforces all transition rules (design doc §9):
  illegal transitions return an error; `Suspect` timeout (15 min)
  transitions to `Offline`. Unit tests cover each legal and illegal
  transition.
- `effective_status()` correctly computes `max(node, group, disk)`.
- `NodeContainer` supports `add_node`/`remove_node`/`get_node` with
  `RwLock` concurrency; `enter_degraded_mode`/`exit_degraded_mode`
  via atomic flag.
- `app/crow-diskdb` compiles, starts, loads config, connects
  to crow-kv, runs initial sync (keep-alive + hardware read), starts
  sync loop, and serves `DiskdbService` (`GetDiskGroupInfo`/
  `GetDiskInfo` functional; allocate/free return `Unimplemented`).
  No admin gRPC service is served.
- `pixi run cargo fmt --all -- --check` and
  `pixi run cargo clippy --all-targets -- -D warnings` clean.
- Relevant tests pass (`pixi run clean-env && pixi run test-kv-core`
  unaffected; `test-kv-server` updated and passes; new tests in
  `crow-kv-client` and `app/crow-diskdb` pass).

## 10. Gaps

None. All open questions have been resolved:

- **Console config schema**: every rack-id and node-id reference
  widens to `RackId`/`NodeId` (from `crow-protocol`, defined once);
  `ServerEntry.id` stays `String`. Mechanical but wide change;
  included in the implementation scope.
- **`KVClusterAdmin` / `KVClusterMetaClient` split**: two classes,
  Admin contains MetaClient (settled).
- **Shared HTTP types**: move to `crow-protocol::mgmt` (settled). The
  absorbed type is `ServerClient` (in `clients/http.rs`), not
  `MgmtClient` (no such type exists).
- **Schema migration**: greenfield (settled).
- **`/topology/ready` flag**: not needed in v1 (settled).
- **`BinaryKey` impls on group-0 keys**: keep, update with
  `rack_id`, drop `dc_id` from `RackKey` (settled).
- **`ServiceRegistryClient` API**: generic across services
  (`register(service, ...)`), with diskdb + kv-server convenience
  wrappers (settled).
- **kv-server keep-alive**: included in R71 (settled; design §2.7).
- **Hardware admin surface**: no `DiskdbAdminService` gRPC; admin ops
  via the console through `HardwareClient`/`KVClusterAdmin`;
  `diskdb_sys_service.proto`/`diskdb_sys_op.proto` deleted
  (settled; design §2.8).
- **New group-0 sysdata value types**: defined in `crow-protocol`
  (proto for stored values; plain serde structs for Entry return
  types) (settled; design §3.3).
