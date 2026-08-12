<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROW - Design: System Group (Group 0)

Depends on: [`design-crow-kv.md`](design-crow-kv.md) [§3.3](design-crow-kv.md),
[`design-crow-kv-server.md`](design-crow-kv-server.md),
[`design-crow-kv-reconfiguration.md`](design-crow-kv-reconfiguration.md)
Satisfies: [`design-crow-kv.md`](design-crow-kv.md) §3.3

---

## 1. Overview

The **system group** (store 0, group 0) is a designated Paxos group
that stores cluster-wide system data ("sysdata") as regular KV entries.
Because it is a Paxos group, sysdata is replicated, consistent, and
highly available by the same mechanism that protects user data. No
external coordinator is needed.

Group 0 holds several categories of sysdata:

- **Hardware hierarchy** — racks, nodes, disk-groups, disks.
- **Per-disk-group maps** — ownership map (which diskdb instance owns
  a disk-group), binding map (which paxos data group a disk-group's
  zone records live on).
- **KV-cluster topology** — stores, groups, replicas (the persistent
  record of the KV cluster's structure, for disaster recovery).
- **Service registry** — live service instances (diskdb, kv-server,
  future services) with their endpoints and heartbeat timestamps.

This document defines the architecture of group 0: who owns the
schema, who writes what, how services register and keep alive, and
how the circular-dependency between kv-server and group 0 is handled.

---

## 2. Design Decisions

### 2.1 `crow-kv-client` is the single sysdata API surface

Group-0 sysdata is owned by `crow-kv-client`, not `crow-kv-server`.
The server is a generic KV store; it must not know domain concepts
(rack/node/disk/disk-group). `crow-kv-client` provides multiple
service classes, each wrapping a `CrowkvClient` pinned to group 0:

- **`HardwareClient`** — hardware hierarchy + per-disk-group maps
  (rack/node/disk-group/disk CRUD, ownership map, binding map).
- **`ServiceRegistryClient`** — service instance registry + keep-alive
  (register, heartbeat, unregister, discover).
- **`KVClusterMetaClient`** — KV-cluster topology records
  (store/group/replica metadata read/write).
- **`KVClusterAdmin`** — control-plane surface for cluster management
  (lifecycle HTTP calls to kv-server + delegates metadata writes to
  `KVClusterMetaClient`). Contains a `KVClusterMetaClient` internally.

All writes are blind puts (no CAS); values are small (< 1 KB) and
JSON-encoded. See §3 for the key/value schema.

### 2.2 `crow-kv-server` mgmt API is internal

The kv-server HTTP management API (lifecycle endpoints: `add_store`,
`add_group`, `add_remote_replicas`, `step_down`, `system_init`, etc.)
stays in kv-server — these create/destroy actual `PxKvStore`/`PxGroup`
objects with WALs and election drivers, which cannot move to a client.
But the API is now **internal**: only `crow-kv-client`'s
`KVClusterAdmin` calls it. All other code (`crow-web`, `crow-cli`)
goes through `KVClusterAdmin`.

Removed from kv-server: `POST /topology/finalize`,
`GET /topology/ready` — persistent topology-record management moves
to `KVClusterMetaClient` / `HardwareClient` in `crow-kv-client`.

Retained in kv-server (internal): lifecycle endpoints, `GET /topology`
(live runtime state export), `GET /health`, `GET /metrics`.

### 2.3 Unified key concept with two encodings

CROW keys use a single key concept (struct + fields) with two encoding
traits — `BinaryKey` (binary, for data groups) and `TextKey`
(text-path, for group 0). The encoding protocol — rules, frozen
layouts, evolution policy — is defined in
[`design-crow-key.md`](../protocol/design-crow-key.md) §5. Group 0
uses text keys + JSON values; the full group-0 key/value schema is in
§3 below.

### 2.4 All cross-component protocol types live in `crow-protocol`

`crow-protocol` is the single home for all cross-component protocol
types — data structures, RPC messages, key types, and HTTP mgmt API
contracts. When `ServerClient`'s mgmt methods are absorbed into
`KVClusterAdmin`, the
HTTP request/response types (`AddStoreRequest`, `AddGroupRequest`,
`RemoteReplicaInfo`, `StepDownRequest`, `StoreSummary`,
`TopologyResponse`, etc.) move to `crow-protocol` under a new `mgmt`
module.

### 2.5 ID types defined in `crow-protocol`

All identifier types are defined in `crow-protocol` and used
consistently across all crates (console config, kv-client, kv-server,
diskdb, chunkdb). No crate uses `String` for an ID that is
fundamentally numeric. ID aliases are defined **once** in
`crow-protocol` (`RackId`, `NodeId`, `DiskGroupId`, `StoreId`,
`GroupId`, `ReplicaId`, `InstanceId`) and imported by every other
crate — no per-crate redefinition. Console config
(`crow-console-shared`) widens every rack-id and node-id reference to
`RackId`/`NodeId` (not just `RackEntry.id`/`NodeEntry.id`/
`NodeEntry.rack_id` — also `ServerEntry.node_id`, `StoreEntry.nodes`,
`ReplicaEntry.node_id`, and the persisted BTreeMap keys). The
console-side `ServerEntry.id` label stays `String` (it is a console
handle, not a numeric cluster ID).

- **`RackId`** — `u64`. Console config today uses `String` for
  `RackEntry.id`; the values are numeric strings (e.g. "1"). Console
  config schema changes to `u64`; `http_cluster_init` passes the value
  directly to `HardwareClient.add_rack(rack_id: u64)`.
- **`NodeId`** — `u64`. Console config today uses `String` for
  `NodeEntry.id`; the values are numeric strings. Console config
  schema changes to `u64`.
- **`DiskGroupId`** — `u64`. (R70 key structs used `u32`; this is a
  widening — see §3.5.)
- **`StoreId`** — `u64`. Already u64 in kv-server and console config.
- **`GroupId`** — `u64`. Already u64 in kv-server and console config.
- **`ReplicaId`** — `u64`. Already u64 in kv-server and console config.
- **`InstanceId`** — `u64`. Service instance identifier (diskdb
  instance, kv-server instance).
- **`DiskId`** — 128-bit: two `u64` (`high`, `low`). Already defined
  as a proto message in `common_type.proto`. Globally unique.
- **`ChunkId`** — 192-bit: three `u64` (`high`, `mid`, `low`).
  Already defined as a proto message in `common_type.proto`.

The simple integer IDs (`RackId`, `NodeId`, `DiskGroupId`, `StoreId`,
`GroupId`, `ReplicaId`, `InstanceId`) are type aliases (`pub type
RackId = u64;`) in `crow-protocol`, not newtypes. They exist for
documentation and API clarity (function signatures read
`rack_id: RackId` rather than `rack_id: u64`), not for type-safety
enforcement. The composite IDs (`DiskId`, `ChunkId`) are already
proto structs.

### 2.6 Two monitoring models: push (services) and pull (infrastructure)

There are two distinct monitoring models in the cluster, determined
by the relationship to group 0:

**Push model (service keep-alive):** Application services (diskdb,
chunkdb, future services) are **guests** on the platform. They
register themselves in group 0 under `/srv/<service>/<instance_id>`
and heartbeat periodically. Other components discover them by
scanning group 0. This works because the service is not the host of
group 0 — it's a client.

**Pull model (infrastructure health):** The console (crow-web) polls
kv-server nodes via `GET /health` and `GET /topology` to observe
runtime state. This is the existing on-demand pattern; it can be
extended to periodic polling.

### 2.7 kv-server keep-alive to group 0 (revised)

**Yes, kv-server instances do keep-alive to group 0**, same push
model as application services. Each kv-server instance registers
under `/srv/kv-server/<instance_id>` and heartbeats with its hosted
stores/groups and health status. This is implemented in R71 (a
background keep-alive loop in `crow-kv-server` writing via
`ServiceRegistryClient`); it is not deferred to a follow-up.

**Circular-dependency analysis:** The concern is that group 0 is
hosted by kv-server, so kv-server writing to group 0 is "writing to
itself." This is not actually harmful:

- **Group 0 is replicated.** If one node hosting a group-0 replica
  goes down, the remaining replicas maintain quorum. The down node's
  heartbeat expires — which is the desired behavior.
- **kv-server is already a client of group 0.** `KVClusterMetaClient`
  writes topology records to group 0; `KVClusterAdmin` delegates to
  it. A heartbeat is just another client write.
- **If ALL group-0 replicas are down, the cluster is down.** Heartbeats
  don't matter in that scenario — no component can read group 0
  anyway.
- **A node hosting group 0 also hosts other groups.** When it goes
  down, its heartbeat expires (visibility), and each of its groups
  elects a new leader independently (Paxos failover). The heartbeat
  doesn't trigger failover — it provides visibility into which nodes
  are alive and what they host.

The keep-alive pattern for kv-server:

```
/srv/kv-server/<instance_id> -> KvServerInstanceValue {
    instance_id: u64,
    mgmt_endpoint: String,
    hosted_stores: Vec<u64>,
    hosted_groups: Vec<(u64, u64)>,  // (store_id, group_id)
    health: HealthSummary,           // aggregate of per-store/per-group status
    last_heartbeat_ms: u64,
}
```

Other components (console, diskdb) can scan `/srv/kv-server/` to
discover live kv-server instances and their health. This complements
the pull model — push provides self-registration and discovery; pull
provides detailed runtime state.

**What about the group-0 leader writing its own heartbeat?** If the
group-0 leader is also a kv-server instance (which it usually is), it
writes its heartbeat to group 0 locally (as a blind put to its own
group). This is no more circular than a kv-server instance writing
user data to a group it hosts — it's a normal client write that goes
through the Paxos consensus path.

### 2.8 Hardware admin via kv-client (no admin gRPC service)

Hardware admin operations (add/remove rack/node/disk-group/disk,
set `*_status`) are writes to group-0 sysdata. They are performed
through `HardwareClient` in `crow-kv-client`, invoked by the console
(`crow-web` / `crow-cli`). There is **no `DiskdbAdminService` gRPC
surface**: the previous `diskdb_sys_service.proto` /
`diskdb_sys_op.proto` admin RPCs (`AddRack`, `SetDiskStatus`,
`FetchHardware`, `Keepalive`, …) are removed — `FetchHardware` is
replaced by `HardwareClient` prefix scans, `Keepalive` by
`ServiceRegistryClient.heartbeat`, and the add/remove/status ops by
`HardwareClient` methods. The diskdb server serves only
`DiskdbService` (allocate/free; stubbed `Unimplemented` until R72) and
reads hardware state from group 0 via `HardwareClient` in its sync
loop — it does not own or serve hardware admin.

---

## 3. Group-0 Sysdata Schema

### 3.1 Key layout (text-path encoding)

```
# Service registry — scan all instances of a service
/srv/diskdb/<instance_id>                              -> InstanceValue
/srv/kv-server/<instance_id>                           -> KvServerInstanceValue
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

### 3.2 Text magic namespaces

- `/hw` — hardware namespace (rack/node/disk-group/disk + maps).
- `/srv` — service registry namespace (all service instances).
- `/kv` — KV-cluster topology namespace (store/group/replica records).

The encoding rationale (text vs binary magic independence, per-
namespace choice) is in
[`design-crow-key.md`](../protocol/design-crow-key.md) §5.

### 3.3 Value encoding

All group-0 values are JSON-encoded (`serde_json::to_vec` /
`serde_json::from_slice`). Value types live in `crow-protocol` (the
single home for cross-component data structures) and are used by
`crow-kv-client` directly — no per-crate redefinition:

- **Existing proto `*Value` types** (`RackValue`, `NodeValue`,
  `DiskGroupValue`, `DiskValue`, `HwStatus`, `DiskId`) — used directly.
  `crow-protocol`'s `build.rs` already derives
  `serde::Serialize`/`Deserialize` on them.
- **New group-0 sysdata value types** added as proto messages in
  `crow-protocol` (parallel to the hardware values): `StoreValue`,
  `GroupValue`, `ReplicaValue`, `InstanceValue` (service-registry
  value, generic across services), `OwnerMapValue`
  (`{ instance_id, lease_expiry_ms }`), `BindMapValue`
  (`{ store_id, group_id }`). `build.rs` derives serde on each.
- **Entry return types** (key fields parsed from the path + value,
  produced by `HardwareClient`/`KVClusterMetaClient` reads) are plain
  `#[derive(serde)]` Rust structs in `crow-protocol`: `DiskGroupEntry`
  (`{ rack_id, node_id, dg_id, value: DiskGroupValue }`),
  `DiskdbOwnerEntry` (`{ rack_id, node_id, dg_id, instance_id,
  lease_expiry_ms }`), `KVGroupBindEntry` (`{ rack_id, node_id, dg_id,
  store_id, group_id }`). These are not stored as-is; they are the
  decoded form of a path + its JSON value.

### 3.4 Scan patterns

- `/hw/rack/` → all racks.
- `/hw/node/` → all nodes; `/hw/node/<rack_id>/` → nodes in one rack.
- `/hw/dg/` → all disk-groups; `/hw/dg/<rack_id>/` → disk-groups in
  one rack; `/hw/dg/<rack_id>/<node_id>/` → disk-groups of one node.
- `/hw/disk/` → all disks; narrowing by rack/node/disk-group.
- `/hw/dg_owner/` → entire ownership map; narrowing by rack/node.
- `/hw/dg_bind/` → entire binding map; narrowing by rack/node.
- `/srv/diskdb/` → all live diskdb instances.
- `/srv/kv-server/` → all live kv-server instances.
- `/kv/store/` → all stores; `/kv/group/<store_id>/` → groups in one
  store; `/kv/replica/<store_id>/<group_id>/` → replicas in one group.

### 3.5 `DiskGroupId` widening: u32 → u64

R70 key structs (`DiskGroupKey`, `DiskKey`, `OwnerMapKey`,
`BindMapKey`) use `u32` for `disk_group_id`. The proto type
`NodeValue` also uses `u32` for `disk_group_ids` (`repeated uint32`)
and `last_used_dg_id` (`uint32`). (`DiskGroupValue` has no
`disk_group_id` field — the id is in the key.) This requirement
widens `DiskGroupId` to `u64` for consistency with all other integer
IDs and to remove the artificial 4-billion limit. The `NodeValue`
proto fields change from `uint32` to `uint64`; the key struct fields
change from `u32` to `u64`. The `disk_group_id` fields in
`diskdb_op.proto` (`AllocateBlocksRequest`, `QueryCapacityStatsRequest`,
`GetDiskGroupInfoRequest`, `GetDiskInfoRequest`) and the Info response
types (`NodeInfo`, `DiskInfo`, `DiskGroupInfo`) widen from `uint32` to
`uint64` as well. This is a breaking schema change, acceptable because
diskdb is greenfield.

### 3.6 `dc_id` removal

R70 reserved `dc_id` in `RackKey { dc_id, rack_id }`, `RackInfo`,
`NodeInfo`, and `NodeValue.dc_id` (v1 ships flat — no DC layer). The
text-path schema has no `dc_id` (`/hw/rack/<rack_id>`). R71 drops
`dc_id` from `RackKey` (struct + binary layout), `RackInfo`,
`NodeInfo`, and `NodeValue.dc_id`. The binary `RackKey` layout changes
accordingly (greenfield).

---

## 4. Service Registry

### 4.1 Registration and keep-alive

Every long-running service instance registers itself in group 0
under `/srv/<service>/<instance_id>` and heartbeats periodically.
`ServiceRegistryClient` in `crow-kv-client` is **generic across
services** (diskdb and kv-server now; chunkdb and future services
later) — it takes a `service` name that selects the path namespace
(`/srv/diskdb/`, `/srv/kv-server/`, …) and the value shape. The API:

- `register(service, instance_id, value: &InstanceValue) -> Result<()>`
- `heartbeat(service, instance_id, value: &InstanceValue) -> Result<()>`
- `unregister(service, instance_id) -> Result<()>` (clean shutdown)
- `read_instance(service, instance_id) -> Result<Option<InstanceValue>>`
- `read_all_instances(service) -> Result<Vec<(u64, InstanceValue)>>` (prefix scan)

`InstanceValue` is the generic service-registry value
(`{ instance_id, grpc_endpoint, last_heartbeat_ms, extra: ServiceExtra }`).
`ServiceExtra` is a per-service enum (diskdb carries `owned_dg_ids:
Vec<u64>`; kv-server carries `hosted_stores`, `hosted_groups`,
`health`). Diskdb convenience wrappers
(`register_diskdb`/`heartbeat_diskdb`/`read_all_diskdb_instances`) and
kv-server wrappers delegate to the generic methods.

### 4.2 Services registered

- **diskdb** — `/srv/diskdb/<instance_id>`. Heartbeat includes
  `owned_dg_ids` (which disk-groups this instance owns). Used by the
  diskdb sync loop and by R78 notify for instance discovery.
- **kv-server** — `/srv/kv-server/<instance_id>`. Heartbeat includes
  `hosted_stores`, `hosted_groups`, and aggregate health. Used by
  the console and other components for kv-server discovery and
  health visibility. **Implemented in R71** (a background keep-alive
  loop in `crow-kv-server` writing via `ServiceRegistryClient`).
- **Future services** (chunkdb, etc.) — same pattern under
  `/srv/<service>/<instance_id>`.

### 4.3 Liveness and expiry

A service instance is considered live if its `last_heartbeat_ms` is
within a configurable TTL (default: 3× heartbeat interval). Readers
filter expired entries when scanning. There is no active eviction —
expired entries are ignored and eventually overwritten or deleted on
clean shutdown.

---

## 5. Bootstrap and Cutover

### 5.1 Two-phase bootstrap

Phase 1 uses console TOML as the topology source of truth
(operator-managed, existing behavior). Phase 2 cuts over to group 0
authoritative: `http_cluster_init` writes hardware records via
`HardwareClient` and KV-cluster topology records via
`KVClusterMetaClient` into group 0.

### 5.2 No `/topology/ready` flag

The old schema had a `/topology/ready` flag key indicating group 0
is authoritative. The text-path schema has no equivalent. v1 does
not need it — diskdb's sync loop treats an empty group 0 as "nothing
assigned yet" and retries. If a readiness signal is needed later,
add a derived condition (e.g. "group 0 has ≥1 `/hw/node/` key").

### 5.3 Greenfield migration

diskdb is greenfield (R70 just merged, no production diskdb). The
old `/topology/...` records are superseded by `/hw/...` and
`/kv/...`. Treat as greenfield — require a fresh cluster init. Old
`/topology/...` keys are orphaned harmlessly.

### 5.4 Leader readiness before writing

`HardwareClient` and `KVClusterMetaClient` write via gRPC to the
group-0 leader. `http_cluster_init` can only write sysdata after a
group-0 leader is elected and reachable. For single-node init this
is immediate (self-elect). For multi-node, the init flow must wait
on leader election before writing — add a readiness poll if the
existing flow doesn't already wait.

---

## 6. Relationship to Existing Design Docs

- **`design-crow-kv.md` §3.3** — defines the system group concept.
  This doc expands it with the sysdata schema, service registry, and
  keep-alive model.
- **`design-crow-kv-server.md` §2.4** — defines the HTTP management
  API. This doc clarifies that the API is internal (only
  `crow-kv-client` calls it) and that `topology_finalize` /
  `topology_ready` are removed.
- **`design-crow-kv-reconfiguration.md`** — group 0 membership
  evolves using the shipped Model B reconfiguration. No new consensus
  primitive required.
- **`design-crow-key.md` §5** — documents the unified key concept
  (one struct, two encoding traits: `BinaryKey` + `TextKey`).
