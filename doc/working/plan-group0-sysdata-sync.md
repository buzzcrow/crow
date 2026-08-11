<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Plan — R71: Group-0 Sysdata, Sync Loop, Disk Status Management

Requirement: [`doc/backlog/R71-group0-sysdata-sync.md`](../backlog/R71-group0-sysdata-sync.md)
Design: [`doc/design/kv/design-crow-kv-group0.md`](../design/kv/design-crow-kv-group0.md)

This is a large change crossing six crates (`crow-protocol`,
`crow-kv-client`, `crow-console-shared`, `crow-web`, `crow-cli`,
`crow-kv`/`crow-kv-server`, `crow-diskdb`). It is broken into seven
stages. **After every stage, run the full test suite + fmt + clippy
before starting the next stage.** Each stage leaves the workspace
building and tests passing.

## Decisions resolved (pre-plan)

- **A — `ServiceRegistryClient` is generic** across services
  (`register(service, instance_id, ...)`), with diskdb + kv-server
  convenience wrappers. Future services (chunkdb) reuse the same API.
- **B — kv-server keep-alive is in R71** (background loop in
  `crow-kv-server` writing `/srv/kv-server/<instance_id>` via
  `ServiceRegistryClient`).
- **C — no `DiskdbAdminService` gRPC.** Hardware admin ops go through
  the console via `HardwareClient`/`KVClusterAdmin` in `crow-kv-client`.
  `diskdb_sys_service.proto` / `diskdb_sys_op.proto` are deleted
  (`FetchHardware`/`Keepalive` replaced by kv-client calls). The
  diskdb server serves only `DiskdbService` (stubs for allocate/free).
- **D — new group-0 sysdata value types live in `crow-protocol`**:
  proto messages for stored values (`StoreValue`, `GroupValue`,
  `ReplicaValue`, `InstanceValue`+`ServiceExtra`, `OwnerMapValue`,
  `BindMapValue`); plain serde structs for Entry return types
  (`DiskGroupEntry`, `DiskdbOwnerEntry`, `KVGroupBindEntry`).
- **E — console config: every rack-id/node-id reference → `RackId`/
  `NodeId`** (defined once in `crow-protocol`, imported everywhere;
  no per-crate redefinition). `ServerEntry.id` stays `String`.

## Pre-plan completed (doc consistency fixes)

Already applied before this plan (no code changed):

- `design-crow-kv-group0.md`: §2.5 (ID consolidation), §2.7 (kv-server
  keep-alive in R71), new §2.8 (admin via kv-client, no admin gRPC),
  §3.3 (value-type location), §3.5 (DiskGroupId widening scope), new
  §3.6 (dc_id removal), §4.1 (generic API), §4.2 (kv-server in R71).
- `design-crow-diskdb.md`: §5 (text-path schema, dc_id drop, value
  types), §6 (no DiskdbAdminService; FetchHardware/Keepalive removed),
  hierarchy (rack in v1), disk-add flow (`HardwareClient.add_disk`).
- `R71-group0-sysdata-sync.md`: steps 3/4/5/6/8/12/13 rewritten;
  scope + acceptance + gaps updated; `MgmtClient`→`ServerClient`;
  reconcile.rs, proto deletions, kv-server keep-alive, console u64
  blast radius added.

## Test commands (run the full set after every stage)

```
pixi run cargo fmt --all -- --check
pixi run cargo clippy --all-targets -- -D warnings
pixi run clean-env && pixi run test-tree-ct
pixi run clean-env && pixi run test-tree-ffi
pixi run clean-env && pixi run test-kv-core
pixi run clean-env && pixi run test-kv-server
pixi run clean-env && pixi run test-console-cli
pixi run clean-env && pixi run test-console-server
pixi run clean-env && pixi run test-console-ui
```

---

## Stage 1 — `crow-protocol` foundation + mgmt types

Goal: the single source of truth for keys, IDs, sysdata value types,
and the mgmt HTTP contract. No behavior change elsewhere yet.

- [ ] ID aliases in `common_type.rs`: widen `DiskGroupId` `u32`→`u64`;
      add `RackId`, `StoreId`, `GroupId`, `ReplicaId`, `InstanceId`
      (`NodeId` already exists). Re-export from `lib.rs`.
- [ ] `TextKey` trait in `key/mod.rs` (`PATH_MAGIC`, `PATH_TYPE`,
      `encode_to_path`, `decode_path`, `to_path`, `from_path`,
      `prefix_all`).
- [ ] Update group-0 key structs in `key/common.rs` + `key/diskdb.rs`:
      `RackKey { rack_id }` (drop `dc_id`), `NodeKey { rack_id,
      node_id }`, `DiskGroupKey`/`DiskKey`/`OwnerMapKey`/`BindMapKey`
      (add `rack_id`, widen `disk_group_id` u32→u64), `InstanceKey
      { instance_id, service }`. Update `BinaryKey` layouts; add
      `TextKey` impls. Keep `ZoneKey`/`BusyBlockKey`/`FreeBlockKey`
      binary-only.
- [ ] New `key/kv_cluster.rs`: `KvStoreKey`, `KvGroupKey`,
      `KvReplicaKey` (`TextKey` only). Register module + re-exports.
- [ ] Proto widening: `common_type.proto` (`NodeValue.disk_group_ids`/
      `last_used_dg_id` uint64; drop `NodeValue.dc_id`,
      `RackInfo.dc_id`, `NodeInfo.dc_id`; widen Info `disk_group_id`/
      `disk_group_ids`/`last_used_dg_id`); `diskdb_type.proto`
      (`DiskInfo`/`DiskGroupInfo.disk_group_id` uint64);
      `diskdb_op.proto` (`disk_group_id` uint64 in
      AllocateBlocks/QueryCapacityStats/GetDiskGroupInfo/GetDiskInfo).
- [ ] Delete `diskdb_sys_service.proto` + `diskdb_sys_op.proto`;
      remove from `build.rs` proto list.
- [ ] New sysdata value types (proto, in `common_type.proto` or a new
      `sysdata_type.proto`): `StoreValue`, `GroupValue`,
      `ReplicaValue`, `InstanceValue` (with `ServiceExtra` oneof:
      diskdb `owned_dg_ids`, kv-server `hosted_stores`/
      `hosted_groups`/`health`), `OwnerMapValue`, `BindMapValue`.
      Add serde derives in `build.rs`.
- [ ] New `sysdata.rs`: plain serde Entry structs `DiskGroupEntry`,
      `DiskdbOwnerEntry`, `KVGroupBindEntry`. Re-export from `lib.rs`.
- [ ] New `mgmt` module (`mgmt.rs`): move HTTP mgmt request/response
      types from `crow-console-shared/src/mgmt.rs`
      (`AddStoreRequest`, `AddGroupRequest`, `RemoteReplicaInfo`,
      `StepDownRequest`, `StepDownResult`, `SystemInitRequest`,
      `SystemInitResponse`, `StoreSummary`, `GroupSummary`,
      `TopologyResponse`, etc.). `crow-console-shared` re-imports
      them (temporary, until Stage 4 removes its `mgmt.rs`).
- [ ] Update `key/tests.rs`: TextKey round-trips for group-0 + KV-
      cluster keys; binary tests for rack_id/dc_id/dg_id widening.
- [ ] Update proto comments + `design-crow-key.md` §5 (unified key
      concept, two encodings, dc_id drop).
- [ ] **Test gate:** full suite + fmt + clippy.

Files: `lib/crow-protocol/src/{common_type.rs,lib.rs,sysdata.rs,mgmt.rs,
key/{mod.rs,common.rs,diskdb.rs,kv_cluster.rs,tests.rs},build.rs,
proto/{common_type.proto,diskdb_type.proto,diskdb_op.proto}}`,
delete `proto/diskdb_sys_{service,op}.proto`,
`lib/crow-console-shared/src/mgmt.rs` (re-import only),
`doc/design/protocol/design-crow-key.md`.

---

## Stage 2 — `crow-kv-client` service classes

Goal: the single management/sysdata API surface, with unit tests
against an in-process/mock crow-kv.

- [ ] `crow-kv-client/Cargo.toml`: add `crow-protocol`, `prost` deps.
- [ ] `hardware.rs` — `HardwareClient` wrapping a `CrowkvClient`
      pinned to group 0. Hierarchy CRUD + status setters (read-
      modify-write, bump `status_changed_at_ms`), read helpers
      (single + prefix scans), ownership/bind map read+set. Return
      `DiskGroupEntry`/`DiskdbOwnerEntry`/`KVGroupBindEntry`.
- [ ] `service_registry.rs` — generic `ServiceRegistryClient`
      (`register`/`heartbeat`/`unregister`/`read_instance`/
      `read_all_instances(service)`). diskdb + kv-server convenience
      wrappers. `InstanceValue` + `ServiceExtra` from `crow-protocol`.
- [ ] `kv_cluster.rs` — `KVClusterMetaClient` (store/group/replica
      CRUD + reads via `/kv/...` scans) and `KVClusterAdmin` (owns a
      reqwest transport + a `KVClusterMetaClient`; each lifecycle
      method calls the kv-server HTTP endpoint AND writes the
      `/kv/...` record; query methods list_stores/list_groups/
      list_remote_replicas/export_topology/health/metrics). Uses
      `crow-protocol::mgmt` types.
- [ ] `lib.rs`: module declarations + re-exports
      (`HardwareClient`, `ServiceRegistryClient`,
      `KVClusterMetaClient`, `KVClusterAdmin`, sysdata types).
- [ ] Unit tests: round-trip + scan correctness for each client
      (mock HTTP for `KVClusterAdmin`; in-process crow-kv or mock for
      the group-0 clients).
- [ ] **Test gate:** full suite + fmt + clippy.

Files: `lib/crow-kv-client/src/{hardware.rs,service_registry.rs,
kv_cluster.rs,lib.rs,Cargo.toml}` + tests.

---

## Stage 3 — console config `String`→`u64` migration

Goal: console config uses `RackId`/`NodeId` everywhere; no
per-crate redefinition. Mechanical but wide. No behavior change.

- [ ] `crow-console-shared/Cargo.toml`: add `crow-protocol` dep.
- [ ] `config.rs`: `RackEntry.id: RackId`, `NodeEntry.id: NodeId`,
      `NodeEntry.rack_id: RackId`, `ServerEntry.node_id:
      Option<NodeId>`, `StoreEntry.nodes: Vec<NodeId>`,
      `ReplicaEntry.node_id: NodeId`. `ServerEntry.id` stays `String`.
      Persisted layer (`PersistedRackEntry`/`PersistedNodeEntry`/
      `PersistedServerEntry`/`PersistedStoreEntry`) + BTreeMap keys
      → `u64`. Update `add_rack`/`remove_rack`/`add_node`/`remove_node`/
      `purge_node_topology`/`record_store`/`ensure_store_node`/
      `server_for_node`/etc. signatures (`&str`→`u64`).
- [ ] Update all consumers: `lifecycle.rs`, `monitor.rs`,
      `topology.rs`, `ssh.rs` (tests), `clients/console.rs`,
      `cluster.rs`, `expand.rs`, `model.rs`, `ops_log.rs`.
- [ ] `build_topology_finalize_body` (crow-web) converts u64→String
      for the still-present `topology_finalize` (removed in Stage 4).
- [ ] Update test fixtures (TOML configs, test helpers) to numeric IDs.
- [ ] **Test gate:** full suite + fmt + clippy (esp.
      `test-console-cli`, `test-console-server`).

Files: `lib/crow-console-shared/src/{config.rs,lifecycle.rs,monitor.rs,
topology.rs,ssh.rs,clients/console.rs,cluster.rs,expand.rs,model.rs,
ops_log.rs,Cargo.toml}` + tests/fixtures; `app/crow-web/src/mgmt.rs`
(`build_topology_finalize_body` boundary).

---

## Stage 4 — console migration to `KVClusterAdmin` + `http_cluster_init` rewrite

Goal: `crow-web`/`crow-cli` use `KVClusterAdmin`/`HardwareClient`/
`KVClusterMetaClient`; `ServerClient` removed from
`crow-console-shared`.

- [ ] `crow-web/src/mgmt.rs::http_cluster_init`: Phase 5 rewritten —
      build `CrowkvClient` seeded with group-0 gRPC endpoints, wrap in
      `HardwareClient` + `KVClusterMetaClient`; write rack/node
      hardware records (`add_rack`/`add_node`/`set_node_status(Up)`)
      and store/group/replica topology records. Add leader-readiness
      poll before Phase 5. Remove `build_topology_finalize_body`.
- [ ] Switch all `ServerClient` call sites in `crow-web`
      (`mgmt.rs`, `physical.rs`, `lifecycle.rs` — 21+) to
      `KVClusterAdmin`.
- [ ] `crow-cli`: `ConsoleClient`→`ServerClient` indirection →
      `KVClusterAdmin`.
- [ ] `crow-console-shared`: remove `clients/http.rs` `ServerClient`
      and the mgmt methods in `mgmt.rs` (types already in
      `crow-protocol::mgmt`). `lifecycle.rs`/`monitor.rs`/
      `topology.rs` use `KVClusterAdmin` (query + lifecycle methods).
- [ ] Remove `topology_finalize`/`topology_ready` client methods.
- [ ] End-to-end init test: rack/node records appear in group 0 under
      `/hw/...`; store/group/replica records under `/kv/...`.
- [ ] **Test gate:** full suite + fmt + clippy (esp.
      `test-console-cli`, `test-console-server`, `test-console-ui`).

Files: `app/crow-web/src/{mgmt.rs,physical.rs,lifecycle.rs}`,
`app/crow-cli/src/**`, `lib/crow-console-shared/src/{clients/http.rs,
mgmt.rs,lifecycle.rs,monitor.rs,topology.rs,clients/console.rs,
lib.rs}` + tests.

---

## Stage 5 — `crow-kv-server` cleanup + kv-server keep-alive

Goal: server no longer writes persistent topology records; reconciler
reads `/kv/...`; kv-server self-registers in group 0.

- [ ] `mgmt_api.rs`: remove `topology_finalize`, `topology_ready`,
      their JSON types, `write_topology_metadata`/`put_topology_entry`
      helpers, and routes from `router()`. Remove the `topology_kv`
      import. Drop the `TopologyFinalize*`/`TopologyReady*` schemas
      from the OpenAPI doc.
- [ ] Delete `lib/crow-kv/src/cluster/topology_kv.rs`; remove module
      declaration from `cluster/mod.rs`.
- [ ] `reconcile.rs`: rewrite to read `/kv/store/` + `/kv/group/`
      records (via local group-0 `PxKvStore` text-path keys, or
      `KVClusterMetaClient`). Drop the `READY_KEY` readiness check
      (empty `/kv/store/` prefix = not yet initialized). Update
      `reconcile_test.rs`.
- [ ] `app/crow-kv-server/Cargo.toml`: add `crow-kv-client` dep.
- [ ] New `keepalive.rs`: kv-server keep-alive loop —
      `ServiceRegistryClient.register("kv-server", instance_id, ...)`
      on start, `heartbeat` every 10 s with `hosted_stores`/
      `hosted_groups`/health, `unregister` on shutdown. Add
      `instance_id` to `ServerConfig`.
- [ ] Wire the keep-alive loop into server startup/shutdown
      (`main.rs`/startup).
- [ ] Update `system_init_test.rs` (no `topology_finalize`).
- [ ] **Test gate:** full suite + fmt + clippy (esp.
      `test-kv-server`).

Files: `app/crow-kv-server/src/{mgmt_api.rs,reconcile.rs,keepalive.rs,
main.rs,startup.rs,Cargo.toml}`, `lib/crow-kv/src/cluster/{mod.rs}`,
delete `lib/crow-kv/src/cluster/topology_kv.rs`,
`app/crow-kv-server/tests/{system_init_test.rs,reconcile_test.rs}`.

---

## Stage 6 — diskdb core (sync, status, node, gRPC stubs)

Goal: `crow-diskdb` runs, syncs from group 0, serves `DiskdbService`
stubs. No admin gRPC.

- [ ] `node/` module: `NodeContainer` (RwLock<HashMap<DiskGroupId,
      Arc<Node>>>, degraded AtomicBool), `Node` (disk-group manager),
      `ZoneDisk` (zone management methods; allocation stubbed for
      R72).
- [ ] `status/` module: `StatusManager` — transition rules (design
      §9), `effective_status = max(node, group, disk)`,
      `check_suspect_timeouts`, `allows_allocate`/`allows_free`.
- [ ] `sync/` module: `SyncLoop` — keep-alive
      (`ServiceRegistryClient.heartbeat_diskdb`) + hardware read
      (owner map, bind map, dg/disk metadata via `HardwareClient`),
      update `NodeContainer`, `SyncOutcome` counts, degraded mode
      after `miss_threshold`.
- [ ] `grpc/` module: wire `DiskdbService` — `AllocateBlocks`/
      `FreeBlocks`/`QueryCapacityStats` return `Unimplemented`;
      `GetDiskGroupInfo`/`GetDiskInfo` read from `NodeContainer`.
- [ ] `main.rs`: CLI + config load + wiring
      (`CrowkvClient`→`HardwareClient`+`ServiceRegistryClient`→
      `NodeContainer`→`SyncLoop`→gRPC). `lib.rs` re-exports.
- [ ] `Cargo.toml`: add `tracing`/`serde`/`toml` if missing.
- [ ] Unit tests: StatusManager transitions, effective_status,
      NodeContainer concurrency + degraded flag, SyncLoop
      sync_once + degraded-mode counter.
- [ ] **Test gate:** full suite + fmt + clippy + new diskdb tests.

Files: `app/crow-diskdb/src/{node/{mod.rs,container.rs,disk.rs},
status/mod.rs,sync/mod.rs,grpc/mod.rs,lib.rs,main.rs,Cargo.toml}` +
tests.

---

## Stage 7 — doc merge + cleanup

Goal: fold working docs into formal docs; delete the requirement doc
and this plan.

- [ ] Verify formal design docs are up to date
      (`design-crow-kv-group0.md`, `design-crow-diskdb.md` §5/§6,
      `design-crow-key.md` §5) — already updated in pre-plan + Stage 1.
- [ ] Delete `doc/working/plan-group0-sysdata-sync.md` (this file).
- [ ] Delete `doc/backlog/R71-group0-sysdata-sync.md`; remove its
      entry from `doc/backlog/backlog.md` Item Index.
- [ ] Final full-suite CI check (fmt, clippy, all test commands).

## Notes

- `crow-kv` does not depend on `crow-protocol` (and must not).
  `crow-kv-server` builds without `crow-protocol` — verified.
- `crow-protocol` depends on neither `crow-kv` nor `crow-kv-client` —
  no cycle when `crow-kv-client` and `crow-kv-server` depend on it.
- `crow-kv-server` → `crow-kv-client` → `crow-kv` is acyclic
  (`crow-kv` depends on neither client nor server).
- Each stage is one or more commits (per the workflow's commit
  cadence); the final implementation commit includes this plan doc.
- The pre-plan doc fixes are committed as part of Stage 0 (the first
  commit) since they are part of this task.

## Blocked

None.
