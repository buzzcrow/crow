<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0 -->

# R2 Implementation Plan: HA Persistent Cluster Config

Reference design: `doc/working/design-persistent-config.md`

## Task breakdown

### T1: Per-node config cache (`node-config.json`) — replaces `GroupConfigStore`

**Goal**: Single per-node config file containing all stores/groups on
this node, including membership. Replaces per-group
`conf/store{sid}_group{gid}.json` files.

**Files**:
- New: `crowkv/src/cluster/node_config.rs` — `NodeConfig` struct,
  `NodeConfigStore` (load/save/atomic write), replaces
  `GroupConfigStore`.
- Modify: `crowkv/src/cluster/mod.rs` — export `node_config`.
- Modify: `crowkv-server/src/startup.rs` —
  `maybe_apply_persisted_config` reads from `node-config.json` instead
  of per-group files. `create_group_with_wal` takes `NodeConfigStore`
  reference.
- Modify: `crowkv/src/cluster/group.rs` — `set_config_store` accepts
  `NodeConfigStore` (or trait). `persist_config` writes to
  `node-config.json`.
- Delete: `crowkv/src/cluster/group_config.rs` (after migration).
- Tests: `crowkv/tests/node_config_test.rs` — load/save/round-trip,
  missing file, corrupt file, multi-store.

**Steps**:
1. Define `NodeConfig` struct (version, stores: Vec<NodeStoreEntry>).
2. Define `NodeConfigStore` (path-based, atomic write, load).
3. Wire into `startup.rs` — replace `GroupConfigStore::new` +
   `maybe_apply_persisted_config` with `NodeConfigStore` equivalents.
4. Wire into `group.rs` — `persist_config` writes membership into
   `node-config.json` (update the relevant group entry, not rewrite
   from scratch — read-modify-write the file).
5. Remove `GroupConfigStore` and its tests.
6. Run: `pixi run -- cargo test -p crowkv --test node_config_test`,
   `pixi run -- cargo test -p crowkv-server --test startup_test`.

### T2: Topology KV schema + API on group 0

**Goal**: Define the KV key schema for topology metadata in group 0
and provide typed read/write helpers.

**Files**:
- New: `crowkv/src/cluster/topology_kv.rs` — key builders, value
  codecs, `TopologyKV` helper struct wrapping a `CrowkvClient` or
  direct KV engine access to group 0.
- Modify: `crowkv/src/cluster/mod.rs` — export `topology_kv`.
- Tests: `crowkv/tests/topology_kv_test.rs` — key encoding, value
  serde, round-trip.

**Steps**:
1. Define key prefix constants (`/topology/ready`,
   `/topology/racks/`, `/topology/nodes/`, etc.).
2. Define value structs (`TopologyRack`, `TopologyNode`,
   `TopologyStore`, `TopologyGroup`, `TopologyReplica`) with serde.
3. Define `TopologyKV` with `get_*`, `put_*`, `scan_*` methods that
   operate on group 0's KV engine.
4. Unit tests for key encoding and value serde.

### T3: System group bootstrap (store 0 / group 0 creation)

**Goal**: Management API endpoint to create the system group. Console
init flow calls this.

**Files**:
- Modify: `crowkv-server/src/mgmt_api.rs` — new endpoint
  `POST /system/init` that creates store 0 + group 0 on this node.
- Modify: `crowkv-server/src/main.rs` — on startup, if
  `node-config.json` contains store 0 / group 0, create them
  automatically (same as any other store/group from cache).
- Tests: `crowkv-server/tests/system_init_test.rs`.

**Steps**:
1. Add `POST /system/init` handler — creates store 0 (if not exists),
   creates group 0 with local replica, starts election (single-node
   self-elect or multi-node deferred).
2. Returns group 0 info (replica_id, endpoint).
3. Integration test: single-node init → group 0 created → topology KV
   writable.

### T4: Console init flow (Initialize Cluster button + auto-finalize)

**Goal**: Console UI + backend endpoint for cluster initialization.

**Files**:
- Modify: `crowkv-console/web/src/mgmt.rs` — new endpoint
  `POST /api/cluster/init` that orchestrates: create store 0 on
  selected nodes → create group 0 with replicas → wire remotes →
  auto-finalize.
- Modify: `crowkv-console/shared/src/clients/console.rs` —
  `init_cluster` client method.
- Modify: `crowkv-console/web/ui/src/` — init button + node selector
  dialog component.
- Tests: `crowkv-console/web/tests/cluster_init_test.rs`.

**Steps**:
1. Backend: `POST /api/cluster/init` handler — takes list of node IDs,
   fans out `POST /system/init` to each, wires remotes for group 0,
   calls finalize.
2. Client: `ConsoleClient::init_cluster(nodes)`.
3. UI: "Initialize Cluster" button in toolbar (visible when group 0
   not yet initialized). Opens node selector dialog. Calls
   `POST /api/cluster/init`.
4. Integration test: init with 1 node → group 0 created → topology
   ready.

### T5: Topology finalize (TOML→group 0 cutover)

**Goal**: `POST /topology/finalize` endpoint that migrates TOML
topology into group 0 KV entries.

**Files**:
- New: `crowkv-console/web/src/topology.rs` — finalize logic.
- Modify: `crowkv-console/web/src/lib.rs` — route
  `POST /api/cluster/finalize`.
- Modify: `crowkv-console/web/src/mgmt.rs` — finalize handler.
- Tests: `crowkv-console/web/tests/topology_finalize_test.rs`.

**Steps**:
1. `finalize` reads `ConsoleConfig` (TOML) → writes all racks, nodes,
   stores, groups, replicas as KV puts to group 0 → sets
   `/topology/ready`.
2. Idempotent: re-running writes same keys with same values.
3. Integration test: finalize after init → group 0 contains full
   topology → `/topology/ready` set.

### T6: Console restart three-way fallback

**Goal**: Console startup checks group 0 state and picks the right
topology source.

**Files**:
- Modify: `crowkv-console/web/src/main.rs` — replace
  `restore_persisted_topology` with three-way check.
- Modify: `crowkv-console/web/src/mgmt.rs` — new
  `load_topology_from_group0` function.
- Tests: `crowkv-console/web/tests/restart_fallback_test.rs`.

**Steps**:
1. On startup, try to read `/topology/ready` from group 0 (find
   group 0 leader via monitor cache or node-config).
2. If group 0 not found → TOML mode (phase 1).
3. If group 0 found but not ready → TOML mode, log warning.
4. If group 0 found and ready → load topology from group 0 KV, update
   in-memory config.
5. Tests for all three paths.

### T7: Divergence reconciliation on node startup

**Goal**: Node startup reconciles local cache with group 0.

**Files**:
- Modify: `crowkv-server/src/startup.rs` — after
  `create_group_with_wal`, if group 0 is reachable, compare local
  cache against group 0 KV.
- New: `crowkv-server/src/reconcile.rs` — reconciliation logic.
- Tests: `crowkv-server/tests/reconcile_test.rs`.

**Steps**:
1. After loading `node-config.json` and creating stores/groups, check
   if group 0 is reachable.
2. If reachable, compare local stores/groups against group 0 topology
   entries.
3. Create missing stores/groups (node was offline when added).
4. Remove stale stores/groups (topology rolled back).
5. Update `node-config.json` after reconciliation.
6. If group 0 not reachable, skip (deferred).
7. Tests: node with stale cache → group 0 reachable → reconciliation
   creates missing group.

### T8: Block data store/group creation until cluster initialized

**Goal**: Console blocks data store/group creation when group 0 is
not initialized.

**Files**:
- Modify: `crowkv-console/web/src/mgmt.rs` — `http_add_store` and
  `http_add_group` check cluster init state, return 409 if not
  initialized.
- Modify: `crowkv-console/web/ui/src/` — disable create buttons in
  UI when not initialized.
- Tests: extend existing mgmt tests.

**Steps**:
1. Add `is_cluster_initialized` check (group 0 exists and
   `/topology/ready` set).
2. `http_add_store` / `http_add_group` return 409 with helpful
   message if not initialized.
3. UI: disable "Add Store" / "Add Group" buttons, show "Initialize
   Cluster First" tooltip.
4. Test: create store before init → 409.

## Execution order

```
T1 (node-config.json) ──┐
                        ├─ T2 (topology KV) ── T3 (system init) ── T4 (console init) ── T5 (finalize)
                        │                                              │
                        └─ T7 (reconcile) ─────────────────────────────┘
                                                                       │
                                              T6 (restart fallback) ──┘
                                                                       │
                                              T8 (block creation) ────┘
```

T1 and T2 are independent foundations. T3 depends on T1+T2. T4 depends
on T3. T5 depends on T4. T6 depends on T5. T7 depends on T1. T8 depends
on T5.

## Testing strategy

- **T1**: Unit tests for `NodeConfigStore` load/save/round-trip.
- **T2**: Unit tests for KV key encoding and value serde.
- **T3**: Integration test: single-node system init.
- **T4**: Integration test: console init flow with 1 and 3 nodes.
- **T5**: Integration test: finalize idempotency, topology in group 0.
- **T6**: Integration test: three restart paths (no group 0, not
  ready, ready).
- **T7**: Integration test: stale cache reconciliation.
- **T8**: Integration test: blocked creation before init.

Relevant existing tests to keep passing:
- `crowkv-server/tests/startup_test.rs`
- `crowkv/tests/wal/`
- `crowkv/tests/cluster/`
- `crowkv-console/web/tests/`

## Quality gate

- `pixi run -- cargo fmt --check`
- `pixi run -- cargo clippy -- -D warnings`
- `pixi run -- cargo test -p crowkv`
- `pixi run -- cargo test -p crowkv-server`
- `pixi run -- cargo test -p crowkv-console-web`
