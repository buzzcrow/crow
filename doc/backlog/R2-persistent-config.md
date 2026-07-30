<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R2: HA persistent cluster config

**Problem**: Cluster topology (racks, nodes, stores, groups, replicas) is
managed in-memory by the console and persisted via a single
`crowkv-console-db.toml` on the console host. This is a single point of
failure — losing the console host loses the full topology. Per-node server
config is also not persisted independently; a node restart relies on the
console to re-push topology, making standalone startup non-deterministic.

**Goal**: Topology metadata must be **consistent** (no split-brain) and
**highly available** (survives node loss) **without an external coordinator**
(no ZooKeeper/etcd). A node restart must self-restore its stores/groups from
local durable state without console intervention.

**Priority**: Medium — console-less deployments and production HA need it.

**Complexity**: High — requires a system consensus group, topology KV schema,
bootstrap protocol, and console refactoring.

**Reuses**: Model B reconfiguration (shipped — direct HTTP mutation +
`membership_epoch` fence, per `doc/design/design-reconfiguration.md` §11.2);
`join_group_via_snapshot` (already implemented). No joint-consensus
ConfigChange primitive required — group 0 grows/shrinks its membership the
same way data groups already do.

---

## Industry context

This problem exists in every Paxos/Raft cluster. The universal pattern is
**self-hosted metadata**: the cluster stores its own topology in its own
consensus log. No external coordinator is needed.

- **etcd**: `--initial-cluster` CLI flag for bootstrap, then membership in
  Raft log. `etcdctl member add` goes through Raft.
- **CockroachDB**: first node creates system ranges (Raft-replicated).
  `--join` for new nodes. Topology stored in system ranges.
- **ZooKeeper**: static `zoo.cfg` for bootstrap, then
  `/zookeeper/config` znode (Zab-replicated) for dynamic reconfig.
- **TiKV**: uses PD (Placement Driver) which embeds etcd/Raft — still
  self-hosted, just an extra layer.

The chicken-and-egg (need consensus to store metadata, need metadata to
form consensus) is resolved universally by: **static bootstrap for first
members, then self-hosted metadata through that same consensus.**

No system solves this without self-hosting. A pure static config file
(like `zoo.cfg` alone, without dynamic reconfig) does not meet HA +
consistency requirements for runtime mutations.

---

## Design: System Group (Group 0)

### Core idea

A designated Paxos group — **system group (store 0, group 0)** — stores the
full cluster topology as regular KV entries. Since it is a Paxos group, the
topology is replicated, consistent, and HA by the same mechanism that
protects user data. No external coordinator needed.

This is the standard industry pattern (closest analog: CockroachDB system
ranges).

Group 0 membership evolves using the **already-shipped Model B
reconfiguration path** (direct HTTP mutation + `membership_epoch` fence),
the same mechanism data groups use today. No new consensus primitive
required.

### Bootstrap (TOML → group 0)

**Phase 1 — pre-group-0 (console TOML is source of truth):**

- Console keeps `crowkv-console-db.toml` as the topology store (existing
  behavior, no console change needed initially).
- Operator creates rack, node → console deploys `crowkv-server` → writes
  to TOML as usual.
- Operator creates store 0, group 0 → first Paxos group bootstrapped on
  the target node.
- During this phase the console TOML is the only copy. This is
  inherently non-HA, but acceptable because initial cluster setup is a
  single-operator action. If the console dies during setup, restart from
  TOML.

**Phase 2 — cutover (group 0 becomes authoritative):**

- Cutover happens **immediately after group 0 is created** — no waiting
  for 3+ replicas. Group 0 is authoritative from creation, even with a
  single replica. HA is a property that improves as replicas are added,
  not a precondition for cutover.
- Operator triggers cutover via an explicit, idempotent API call:
  `POST /topology/finalize`. This reads all topology from TOML, writes
  it to group 0 as KV puts, then sets `/topology/ready` flag.
- From this point, all topology reads/writes go through group 0. TOML
  becomes a stale backup (useful for forensic inspection, not
  authoritative).

**Cutover safety rules:**

- **Idempotent**: migration is a pure replay — read TOML, write same KV
  puts, set ready flag. A crash before `/topology/ready` is set simply
  means retry-from-scratch on the next attempt. Last-write values are
  identical, so re-running the full migration is always safe.
- **Explicit trigger**: cutover is an operator action (`POST
  /topology/finalize`), not auto-detected. This avoids racing with
  concurrent topology edits mid-migration and makes the operation
  testable.
- **Multi-console safe**: if two consoles both call `finalize`
  concurrently, both write the same KV puts (idempotent) and both try to
  set `/topology/ready`. Paxos serializes the ready-flag write. No harm.
- **No concurrent topology edits during migration**: the `finalize` call
  should briefly block new topology mutations until migration completes.
  This is a short-lived lock (seconds), not a structural constraint.

**Console restart behavior:**

- Console checks group 0 for `/topology/ready` flag:
  - **Group 0 does not exist** (no store 0/group 0 created yet) → fall
    back to TOML (still in bootstrap phase 1).
  - **Group 0 exists but `/topology/ready` not set** (cutover not done
    or crashed mid-migration) → fall back to TOML, retry cutover on
    next `finalize`.
  - **Group 0 exists and `/topology/ready` is set** → load topology
    from group 0 (HA, authoritative).

### Topology KV schema (stored in group 0)

- `/topology/ready` — flag key; presence means group 0 is authoritative
- `/topology/racks/<rack_id>` — rack metadata (name, created timestamp)
- `/topology/nodes/<node_id>` — node metadata (rack_id, mgmt_endpoint,
  grpc_endpoint, election_profile, auto_start)
- `/topology/stores/<store_id>` — store metadata (node_id, port)
- `/topology/groups/<group_id>` — group metadata (store_id, replica list
  reference)
- `/topology/replicas/<group_id>/<replica_id>` — replica metadata (node_id,
  role, voting, endpoint)

All mutations are KV puts through group 0's Paxos consensus. Consistent by
construction.

### ID allocation

If two consoles concurrently create a store/group/node, they could pick
the same next-id client-side before either write lands — a lost-update,
not a KV conflict Paxos would catch. Solution: a **server-side ID counter**
in group 0.

- `/topology/counters/rack_id` — next rack ID
- `/topology/counters/node_id` — next node ID
- `/topology/counters/store_id` — next store ID
- `/topology/counters/group_id` — next group ID

To allocate an ID, the console does a read-modify-write through group 0's
KV API: read current counter, increment, put back. Paxos serializes this,
so concurrent consoles get distinct IDs. Alternatively, a dedicated
`next_id` RPC on the group 0 leader avoids the read round-trip.

### Divergence reconciliation

Three persistence layers exist: group-0 KV entries (authoritative),
`conf/node-config.json` (local cache), and `conf/store{sid}_group{gid}.json`
(per-group membership). On node startup, reconciliation rules:

1. Load `conf/node-config.json` → create stores/groups → replay WAL.
2. If group 0 is reachable, **compare** local cache against group 0 KV:
   - If group 0 says a store/group exists but local cache doesn't →
     create it (node was offline when it was added).
   - If local cache says a store/group exists but group 0 doesn't →
     **remove it** (topology was rolled back while node was offline).
   - If both agree → no action needed.
3. Per-group membership (`GroupConfigStore`) is always reconciled via
   `maybe_apply_persisted_config` (existing behavior) — group 0 KV is the
   cluster-wide view, per-group config is the per-group membership detail.
4. After reconciliation, update `conf/node-config.json` to match group 0.

If group 0 is **not reachable** (network partition, quorum loss), node
boots from local cache only. Reconciliation deferred until group 0 is
reachable again.

### Dev / single-node case

For 1-node dev deployments where HA is moot, group 0 still works — it's
created with a single replica and becomes authoritative immediately via
`finalize`. HA improves automatically as replicas are added via Model B
reconfiguration. This keeps a uniform code path: every deployment uses
group 0, regardless of size.

### Per-node config cache

- `conf/node-config.json` is a **local cache** derived from the system
  group, not the source of truth.
- On startup: load cache → create stores/groups → replay WAL → reconcile
  with group 0 (see Divergence reconciliation above).
- If cache is lost (node disk gone): node queries the system group to
  rebuild it.
- Cache is refreshed after every topology change.

### Operations

**Add rack**: KV put to group 0. Pure metadata, no node action needed.

**Add node**:
1. KV put `/topology/nodes/<node_id>` to group 0 (Paxos consensus).
2. Console deploys `crowkv-server` on the new node with
   `--seed <any-existing-node-endpoint>`.
3. New node boots → contacts seed → joins group 0 via
   `join_group_via_snapshot` (already implemented).
4. New node receives full topology snapshot from group 0. HA improved.

**Add store/group**:
1. KV put to group 0 (new store/group entry, including which node hosts it).
2. Management API call to the target node: `POST /stores` +
   `POST /stores/{sid}/groups`.
3. Target node creates the group, replays WAL (if any), starts serving.
4. Per-node config cache updated on the target node.

**Add replica (to a data group)**:
1. KV put to group 0 (new replica entry).
2. Target node joins the data group via `join_group_via_snapshot`.
3. Group config (`conf/store{sid}_group{gid}.json`) updated via consensus on
   the data group.
4. Both the system group (topology) and the data group (membership) are
   consistent.

### Failure scenarios

**Node down (temporary)**:
- Group 0 maintains quorum (if 3+ replicas).
- Node restarts → loads per-node cache → creates stores/groups → replays
  WAL → rejoins group 0.
- No data loss, no topology loss.

**Node lost (permanent disk failure)**:
- Group 0 maintains quorum (if 3+ replicas survive).
- Operator adds replacement node → joins group 0 → gets full topology.
- Replacement node joins data groups via snapshot.
- Lost replica removed from group 0 via Model B reconfiguration (direct
  HTTP mutation + `membership_epoch` fence — already shipped).

**Console host lost**:
- Operator starts a new console instance anywhere → reads topology from
  group 0. Topology data is automatically consistent and available.
- Multiple consoles can run simultaneously (all go through group 0 for
  consistency).
- TOML is a stale backup on the old console host — not needed for
  recovery.
- Note: console process recovery is an operator action; topology data
  recovery is automatic.

### Pros

- Full HA for topology metadata — survives any single failure.
- Console is stateless — can run anywhere, multiple instances safe.
- Industry-standard pattern (etcd, CockroachDB, ZK all do this).
- Single source of truth — no divergence risk.

### Cons

- More complex implementation — topology KV schema, console refactoring,
  node discovery protocol, ID allocation counters.
- Group 0 is a new failure domain — if it loses quorum, topology
  operations are blocked (but data groups continue serving).
- Bootstrapping group 0 itself has a non-HA window (before 3 replicas).
- Dual code paths (TOML + group 0) during bootstrap phase; resolved after
  cutover. Migration is idempotent and retry-safe, so the dual-path
  window is bounded and low-risk.

### What needs building (in order)

1. **System group bootstrap** — `--stores 0 --groups 0` creates the topology
   store on first node; console triggers this when operator creates store 0.
2. **Topology KV schema + API** — CRUD operations on
   racks/nodes/stores/groups/replicas through group 0's KV interface,
   including server-side ID allocation counters.
3. **Node discovery** — `--seed <endpoint>` CLI arg; new node contacts seed
   to join group 0.
4. **Per-node config cache** — `conf/node-config.json` derived from system
   group, persisted locally for fast standalone startup.
5. **Console refactoring** — read/write topology through group 0 after
   cutover; implement `POST /topology/finalize` (idempotent TOML→group 0
   migration + `/topology/ready` flag). Console restart checks ready flag
   with three-way fallback (group 0 missing / not ready / ready).
6. **Divergence reconciliation** — startup reconciliation between local
   cache and group 0 KV (see Divergence reconciliation above).
7. **Group 0 membership evolution** — reuse shipped Model B reconfiguration
   (direct HTTP mutation + `membership_epoch` fence) — no new primitive.

---

## What is already in place

- Per-group config persistence (`GroupConfigStore`) — group membership is
  already durable per-node.
- WAL replay + `restore_from_replay` — consensus state restored on startup.
- `join_group_via_snapshot` — new nodes can join via snapshot pull.
- `persist_config` after membership changes — group configs kept up-to-date.
- `restore_persisted_topology` — console re-push (becomes secondary path).

---

## Acceptance criteria

- Node starts with local config cache, creates stores/groups/replicas
  without console intervention.
- Topology metadata survives node loss (group 0 maintains quorum).
- Adding a rack/node/store/group/replica is consistent (via Paxos in
  group 0; ID allocation via server-side counters).
- No external coordinator process required.
- Topology metadata survives console host loss automatically. Multiple
  consoles safe. Divergence reconciliation on node startup. Cutover is
  idempotent and retry-safe.
