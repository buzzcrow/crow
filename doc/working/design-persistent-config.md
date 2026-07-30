<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0 -->

# R2 Design: HA Persistent Cluster Config

Depends on: `design.md` §9, `design-reconfiguration.md` §2/§6,
`design-wal.md`, `design-kv-server.md`

## Problem

Cluster topology (racks, nodes, stores, groups, replicas) is managed
in-memory by the console and persisted via a single
`crowkv-console-db.toml` on the console host. Losing the console host
loses the full topology. Per-node server config is not persisted
independently; a node restart relies on the console to re-push topology.

## Current behavior

- Console TOML is the single source of truth (`ConsoleConfig` in
  `crowkv-console/shared/src/config.rs`).
- `restore_persisted_topology` (`crowkv-console/web/src/mgmt.rs:306`)
  re-pushes topology to nodes on console startup.
- Per-group membership persisted via `GroupConfigStore`
  (`crowkv/src/cluster/group_config.rs`) as
  `conf/store{sid}_group{gid}.json`.
- WAL replay + `restore_from_replay` restores consensus state on
  startup (`crowkv-server/src/startup.rs`).
- Model B reconfiguration (direct HTTP mutation + `membership_epoch`
  fence) is shipped — no joint-consensus primitive needed.

## Proposed approach: System Group (Group 0)

A designated Paxos group — **system group (store 0, group 0)** — stores
the full cluster topology as regular KV entries. Since it is a Paxos
group, topology is replicated, consistent, and HA by the same
mechanism that protects user data. No external coordinator needed.

Group 0 membership evolves using the shipped Model B reconfiguration
path (direct HTTP mutation + `membership_epoch` fence), the same
mechanism data groups use today.

### Decisions (confirmed with user)

- **D1: Explicit "Initialize Cluster" button** — UI has an init button
  that opens a node selector dialog. Operator picks which nodes host
  group 0 replicas. Console creates store 0 on each, then creates
  group 0 with replicas on those nodes.
- **D2: Auto-finalize after init** — init creates store 0/group 0,
  then immediately calls `POST /topology/finalize`. One-step for
  operator. Cutover is idempotent so retry-safe.
- **D3: Require group 0 first** — UI blocks data store/group creation
  until cluster is initialized (group 0 exists and `/topology/ready`
  is set).
- **D4: Merge per-group config files into `node-config.json`** —
  Eliminate separate `conf/store{sid}_group{gid}.json` files. The new
  `conf/node-config.json` is the single per-node cache, containing
  which stores/groups exist on this node AND their membership (members,
  endpoints, voting, membership_epoch). `GroupConfigStore` is replaced
  by `NodeConfigStore`.
- **D5: Local cache only contains this node's groups** —
  `node-config.json` stores only groups where this node has a replica.
  Data group availability is governed by each group's own quorum, not
  group 0 reachability. Group 0 unreachability blocks topology
  mutations only, not data ops.

### Bootstrap (TOML → group 0)

**Phase 1 — pre-group-0 (console TOML is source of truth):**

- Console keeps `crowkv-console-db.toml` as the topology store (existing
  behavior).
- Operator creates rack, node → console deploys `crowkv-server` →
  writes to TOML as usual.
- Operator clicks "Initialize Cluster" → selects nodes → console
  creates store 0 on each selected node, creates group 0 with replicas.
- During this phase the console TOML is the only copy. Acceptable
  because initial cluster setup is a single-operator action.

**Phase 2 — cutover (group 0 becomes authoritative):**

- Cutover happens **immediately after group 0 is created** (auto-finalize).
  Group 0 is authoritative from creation, even with a single replica.
  HA improves as replicas are added via Model B reconfiguration.
- `POST /topology/finalize` reads all topology from TOML, writes it to
  group 0 as KV puts, then sets `/topology/ready` flag.
- From this point, all topology reads/writes go through group 0. TOML
  becomes a stale backup.

**Cutover safety rules:**

- **Idempotent**: migration is a pure replay — read TOML, write same KV
  puts, set ready flag. Re-running is always safe.
- **Explicit trigger via init button**: the init flow calls finalize
  automatically. No auto-detection.
- **Multi-console safe**: if two consoles both call `finalize`
  concurrently, both write the same KV puts (idempotent) and both try
  to set `/topology/ready`. Paxos serializes the ready-flag write.
- **No concurrent topology edits during migration**: `finalize` briefly
  blocks new topology mutations until migration completes.

**Console restart behavior:**

- Console checks group 0 for `/topology/ready` flag:
  - **Group 0 does not exist** → fall back to TOML (still in bootstrap
    phase 1).
  - **Group 0 exists but `/topology/ready` not set** → fall back to
    TOML, retry cutover on next init/finalize.
  - **Group 0 exists and `/topology/ready` is set** → load topology
    from group 0 (HA, authoritative).

### Topology KV schema (stored in group 0)

- `/topology/ready` — flag key; presence means group 0 is authoritative
- `/topology/racks/<rack_id>` — rack metadata (name)
- `/topology/nodes/<node_id>` — node metadata (rack_id, mgmt_endpoint,
  grpc_endpoint, election_profile, auto_start)
- `/topology/stores/<store_id>` — store metadata (node_id, port)
- `/topology/groups/<group_id>` — group metadata (store_id)
- `/topology/replicas/<group_id>/<replica_id>` — replica metadata
  (node_id, role, voting, endpoint)
- `/topology/counters/<entity>` — server-side ID allocation counters
  (rack_id, node_id, store_id, group_id)

All mutations are KV puts through group 0's Paxos consensus.

### Per-node config cache (`node-config.json`)

Replaces both `conf/store{sid}_group{gid}.json` files and the implicit
"no per-node config" state. Single file per node:

```json
{
  "version": 1,
  "stores": [
    {
      "store_id": 0,
      "groups": [
        {
          "group_id": 0,
          "replica_id": 1,
          "members": [
            {"replica_id": 1, "endpoint": "127.0.0.1:28001", "voting": true}
          ],
          "membership_epoch": 3,
          "term": 5
        }
      ]
    }
  ]
}
```

- On startup: load `node-config.json` → create stores/groups → replay
  WAL → reconcile with group 0 (if reachable).
- If cache is lost (node disk gone): node queries group 0 to rebuild.
- Cache is refreshed after every topology change (membership mutation
  writes through `NodeConfigStore`).
- `GroupConfigStore` is replaced by `NodeConfigStore`. The
  `maybe_apply_persisted_config` path in `startup.rs` reads from
  `node-config.json` instead of per-group files.

### Divergence reconciliation

On node startup:

1. Load `node-config.json` → create stores/groups → replay WAL.
2. If group 0 is reachable, **compare** local cache against group 0 KV:
   - Group 0 says a store/group exists but local cache doesn't → create
     it (node was offline when it was added).
   - Local cache says a store/group exists but group 0 doesn't → remove
     it (topology was rolled back while node was offline).
   - Both agree → no action needed.
3. Per-group membership is reconciled by comparing local cache members
   against group 0's `/topology/replicas/<group_id>/` entries.
4. After reconciliation, update `node-config.json` to match group 0.

If group 0 is **not reachable** (network partition, quorum loss), node
boots from local cache only. Reconciliation deferred until group 0 is
reachable again. Data groups continue serving based on their own
quorum — group 0 unreachability does not affect data plane.

### Operations

**Initialize Cluster** (new):
1. Operator clicks "Initialize Cluster" in UI.
2. UI shows node selector (all deployed nodes).
3. Operator selects nodes → console creates store 0 on each, creates
   group 0 with replicas, auto-finalizes (TOML→group 0 cutover).
4. UI unblocks data store/group creation.

**Add rack**: KV put to group 0. Pure metadata, no node action.

**Add node**:
1. KV put `/topology/nodes/<node_id>` to group 0.
2. Console deploys `crowkv-server` on the new node.
3. New node boots → loads `node-config.json` (empty on first boot) →
   contacts group 0 → joins via `join_group_via_snapshot`.

**Add store/group** (data store/group, not system):
1. KV put to group 0 (new store/group entry).
2. Management API call to target node: `POST /stores` +
   `POST /stores/{sid}/groups`.
3. Target node creates group, replays WAL, starts serving.
4. `node-config.json` updated on target node.

**Add replica (to a data group)**:
1. KV put to group 0 (new replica entry).
2. Target node joins data group via `join_group_via_snapshot`.
3. `node-config.json` updated on target node.

### Failure scenarios

**Node down (temporary)**:
- Group 0 maintains quorum (if 3+ replicas).
- Node restarts → loads `node-config.json` → creates stores/groups →
  replays WAL → rejoins group 0.
- No data loss, no topology loss.

**Node lost (permanent disk failure)**:
- Group 0 maintains quorum (if 3+ replicas survive).
- Operator adds replacement node → joins group 0 → gets full topology.
- Lost replica removed from group 0 via Model B reconfiguration.

**Console host lost**:
- Operator starts new console anywhere → reads topology from group 0.
- Multiple consoles can run simultaneously (all go through group 0).
- TOML is a stale backup — not needed for recovery.

### Startup flow change

Before R2:
- Server boots empty → console pushes topology via
  `restore_persisted_topology`.

After R2:
- Server boots → loads `node-config.json` → creates stores/groups →
  replays WAL → reconciles with group 0 (if reachable).
- Console's `restore_persisted_topology` becomes secondary (fallback
  for pre-cutover or group-0-unreachable cases).

Time impact: negligible for single-node dev (group 0 self-elects
immediately). For multi-node, adds one group-0 reconciliation round-trip
(typically <100ms on localhost).

### UI impact

- New "Initialize Cluster" button (visible when group 0 not yet
  initialized).
- Data store/group creation controls disabled until cluster initialized.
- After init, existing create-store/create-group flows work unchanged
  (console writes to group 0 instead of TOML, but the API surface is
  the same).

### What is already in place

- Per-group config persistence (`GroupConfigStore`) — to be replaced
  by `NodeConfigStore`.
- WAL replay + `restore_from_replay` — consensus state restored on
  startup.
- `join_group_via_snapshot` — new nodes can join via snapshot pull.
- Model B reconfiguration — group 0 membership evolves same as data
  groups.
- `restore_persisted_topology` — becomes secondary path.

### Alternatives considered

- **Pure static config file (no group 0)**: does not meet HA +
  consistency requirements for runtime mutations. Every system (etcd,
  CockroachDB, ZK) uses self-hosted metadata.
- **External coordinator (ZooKeeper/etcd)**: violates the no-external-
  coordinator constraint.
- **Keep per-group config files + add group 0**: two persistence paths
  for the same data, more complexity. Merging into `node-config.json`
  is simpler.
- **Auto-create group 0 on first deploy**: less explicit, harder to
  test. Rejected in favor of explicit init button with node selector.

### Acceptance criteria

- Node starts with `node-config.json`, creates stores/groups/replicas
  without console intervention.
- Topology metadata survives node loss (group 0 maintains quorum).
- Adding a rack/node/store/group/replica is consistent (via Paxos in
  group 0).
- No external coordinator process required.
- Topology metadata survives console host loss automatically. Multiple
  consoles safe.
- Divergence reconciliation on node startup.
- Cutover is idempotent and retry-safe.
- Data store/group creation blocked until cluster initialized.
- `GroupConfigStore` replaced by `NodeConfigStore` (no per-group config
  files).
