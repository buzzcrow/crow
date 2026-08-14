<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R81: sysdata — Epoch/Generation for Reusable Integer IDs

**Problem**: The cluster-topology integer IDs — `RackId`, `NodeId`,
`DiskGroupId` (u64), and the paxos `store_id`, `group_id`,
`replica_id` (u64) — are reusable. They are not globally unique like
`DiskId` (128-bit) or `ChunkId` (192-bit). When an entity is removed
and a new entity is later created with the same integer ID, stale
sysdata records, stale cross-references, and stale derived state can
be incorrectly associated with the new entity.

- **`DiskGroupId` reuse on a re-added node** — `NodeValue` carries
  `last_used_dg_id` (`common_type.proto`), so dg IDs are monotonically
  allocated *within a node's lifetime* (last_used + 1). But when a node
  is removed and re-added, `last_used_dg_id` resets to 0
  (`app/crow-web/src/mgmt/cluster_init.rs` initializes it to 0). A new
  disk-group on the re-added node can get `dg_id = 1`, the same integer
  a previously-removed disk-group on that node used. Stale
  `OwnerMapKey` / `BindMapKey` records (`/hw/dg_owner/...`,
  `/hw/dg_bind/...`) and stale `DiskGroupUsageKey` summaries from the
  old disk-group would then be read as belonging to the new one.
- **`RackId` / `NodeId` reuse** — rack and node IDs are
  operator/config-chosen (`cluster_init.rs`), not monotonic. A rack or
  node removed and re-added with the same integer ID inherits the old
  `RackValue` / `NodeValue` cross-reference lists (`node_ids`,
  `disk_group_ids`) and status history unless they were explicitly
  deleted.
- **`store_id` / `group_id` / `replica_id` reuse** — paxos group
  identity. `replica_id` is auto-assigned as "max existing + 1"
  (`app/crow-web/src/mgmt/replica_ops.rs`), monotonic within a group's
  current view but reset when a group is recreated. Groups already
  carry a `membership_epoch` fence
  (`design-crow-kv-reconfiguration.md` §6; `PxGroup.membership_epoch`
  in `lib/crow-kv/src/cluster/group.rs`) — but that fences
  *reconfiguration* (Prepare/Accept quorum matching), not
  *identity reuse* (a brand-new group at the same store/group id with
  epoch reset to 0 is not distinguished from the old group's
  pre-first-reconfiguration state).
- **diskdb derived state** — diskdb keys zone/busy/free/recovery-scan
  records by globally-unique `DiskId`, so those are safe (R76 gap
  review confirmed `RecoveryScanProgressKey` does not collide). The
  reuse risk is on the group-0 sysdata side: ownership/bind/usage
  records keyed by `(rack_id, node_id, dg_id)`.

**Current behavior + impact**: nothing distinguishes a re-added entity
from the original. Removal is by record deletion
(`HardwareClient::remove_rack` / `remove_node` / `remove_disk_group` /
`remove_disk`), which deletes the primary record but does **not**
cascade-delete derived/cross-referenced records (ownership map, bind
map, usage summaries, membership in parent `node_ids`/`disk_group_ids`
lists). If the operator re-creates an entity with the same integer ID
without manually cleaning up every derived record, the new entity
inherits stale state. Today this is mitigated only by operator
discipline (use a new integer ID on re-add) and by `DiskId`/`ChunkId`
being globally unique for the data-path records. Root cause: integer
IDs are bare scalars with no generation, and cleanup is not cascading.

**Design pointers**: `design-crow-kv-group0.md` §2.5 (ID types —
`DiskId` 128-bit globally unique; `RackId`/`NodeId`/`DiskGroupId` are
u64 scalars "for documentation and API clarity, not for type-safety
enforcement"), §2.4 (sysdata schema, key/value layout),
`design-crow-protocol-key.md` §3.4 (fixed-width 128-bit/192-bit
identifiers — `DiskId` 16 bytes; integer IDs are fixed-width u64),
`design-crow-kv-reconfiguration.md` §6 (`membership_epoch` fence —
consensus reconfiguration fencing, not identity reuse). No direct
aioss analog — aioss uses string node/rack identifiers.

**Use scenarios**:
- **Node removed then re-added with same node_id** — operator removes
  a node (rack 1, node 2) for decommission, later re-provisions a node
  at rack 1, node 2. The new node's `NodeValue` reuses node_id 2. Any
  stale `OwnerMapKey`/`BindMapKey`/usage records for disk-groups that
  existed on the old node 2 and were not cleaned up are now read as
  belonging to the new node 2. diskdb may try to adopt a disk-group
  bind that points at a stale paxos group.
- **Disk-group removed then re-added with same dg_id on a re-added
  node** — node re-added with `last_used_dg_id` reset to 0; first new
  disk-group gets dg_id 1, colliding with the old disk-group 1's
  ownership/bind/usage records.
- **Paxos group recreated at same store/group id** — a group is
  deleted and a new group created at the same (store_id, group_id).
  `membership_epoch` resets to 0. A client with a cached topology
  entry for the old group at epoch 0 cannot distinguish it from the
  new group at epoch 0.
- **Operator uses a fresh integer ID on re-add (current mitigation)** —
  operator re-adds the node as node_id 3 instead of 2. No collision.
  This is the only safe path today; it is manual and error-prone.

**Solution**: **No clear solution yet — deferred to design.** The
fix is to make entity identity reuse-safe. Candidate directions (to be
evaluated in the design draft):

- **(A) Per-entity epoch/generation field** — add `epoch` (or
  `generation`) to `RackValue`, `NodeValue`, `DiskGroupValue`, and the
  paxos group identity. Bumped on re-create. Consumers compare
  `(id, epoch)`, not `id` alone. Smaller proto change; touches all
  consumers that key by these IDs.
- **(B) Make all IDs globally unique** — widen `RackId`/`NodeId`/
  `DiskGroupId`/`store_id`/`group_id`/`replica_id` to 128-bit
  UUID-like values (like `DiskId`/`ChunkId`). Largest change: key
  encoding (`design-crow-protocol-key.md` — every key with these
  fields grows), all sysdata records, all consumers, console config.
  Eliminates reuse entirely.
- **(C) Enforce monotonic, never-reuse allocation everywhere** — make
  every ID allocator monotonic and persistent across re-adds (e.g.
  `last_used_dg_id` survives node re-add by storing it outside the
  deletable `NodeValue`, or by a cluster-wide monotonic counter in
  group 0). Lightest touch for dg_id/replica_id (already partly
  monotonic); does not help operator-chosen rack/node/store/group IDs
  unless those are also forced monotonic.
- **(D) Cascading cleanup on removal** — on `remove_*`, cascade-delete
  all derived records (ownership, bind, usage, parent list membership).
  Does not prevent collision if cleanup is incomplete or a re-add
  races with cleanup propagation, but shrinks the stale-state surface.

These are not mutually exclusive; the design draft should pick a
primary mechanism (likely A or C) and state whether B is a long-term
goal. Approach-dependent acceptance bullets below are marked
`pending design`.

**One-line summary**: deferred to design — pick a mechanism (per-entity
epoch, globally-unique IDs, monotonic-never-reuse, or cascading
cleanup) to make reusable integer IDs reuse-safe.

**Edge cases at a glance**:
- Re-add with same ID before stale records propagate → collision; the
  chosen mechanism must be safe under concurrent re-add + cleanup.
- Re-add with a fresh ID (current mitigation) → no collision; the
  mechanism must not break this existing safe path.
- paxos group at same (store, group) with `membership_epoch` reset →
  consensus-fence does not cover identity reuse; needs separate
  treatment.
- `DiskId`/`ChunkId` already globally unique → out of scope, no
  change needed for data-path records.

**Dependencies**: none on unlanded extensions. Touches
`crow-protocol` (proto + key encoding if B), `crow-kv-client`
(`HardwareClient` — all `set_*_status` / `add_*` / `remove_*` paths),
`crow-kv` (`PxGroup` identity if paxos groups are in scope),
`crow-diskdb` (keepalive reconcile, ownership/bind adoption),
`crow-web` / `crow-cli` (cluster_init, replica_ops, any ID-assignment
path). R76's `RecoveryScanProgressKey` is **not** a dependency — it is
already safe (globally-unique `DiskId`).

**Acceptance**:
- **Identity disambiguation** (`pending design`): a removed-then-
  re-added entity with the same integer ID is distinguishable from the
  original by every consumer that reads its sysdata. Setup → remove
  entity → re-add same ID → assert no consumer reads stale derived
  state from the old entity. Integration test. (Exact assertion
  depends on chosen mechanism.)
- **No regression on fresh-ID re-add** (`pending design`): re-adding
  with a fresh integer ID continues to work exactly as today. Setup →
  remove entity → re-add with new ID → assert clean state, no stale
  inheritance. Integration test.
- **Cascading cleanup (if approach D is chosen)** (`pending design`):
  `remove_rack` / `remove_node` / `remove_disk_group` deletes all
  derived records (ownership, bind, usage, parent list membership).
  Setup → add hierarchy → remove a node → assert no `OwnerMapKey` /
  `BindMapKey` / `DiskGroupUsageKey` records remain for that node's
  disk-groups. Integration test.
- **Proto/key round-trip (if approach A or B is chosen)** (`pending
  design`): new `epoch` field (A) or widened ID (B) round-trips
  through KV and key encoding. Unit test.
- `pixi run cargo fmt --all -- --check` and
  `pixi run cargo clippy --all-targets -- -D warnings` clean.
- `pixi run test-diskdb` (relevant integration tests pass) + any
  `test-kv-client` / `test-kv-server` tests touched by sysdata changes.

**Open Questions**:
- **Which mechanism?** A (per-entity epoch) vs B (globally-unique IDs)
  vs C (monotonic-never-reuse) vs D (cascading cleanup) vs a
  combination. Trade-offs: A is the smallest proto change but touches
  every consumer's comparison logic; B eliminates reuse but is the
  largest key-encoding change; C is lightest where allocators already
  exist but does not cover operator-chosen IDs; D shrinks the surface
  but is not airtight under concurrent re-add. Cannot be resolved
  automatically — needs a decision on how much key-encoding
  disruption is acceptable and whether paxos group identity reuse is
  in scope.
- **Is paxos group identity (store_id/group_id/replica_id) in scope,
  or only the hardware hierarchy (rack/node/dg)?** `membership_epoch`
  already fences reconfiguration; identity-reuse for a recreated
  group is a separate, harder question (affects client topology
  cache, WAL replay, snapshot ownership). Scoping this decides whether
  `crow-kv` / `PxGroup` is touched.
- **Should `last_used_dg_id` (and any future monotonic counters) be
  persisted outside the deletable `NodeValue`?** If approach C is
  chosen for dg_id, the counter must survive node removal/re-add —
  where does it live (a separate group-0 counter record? a per-rack
  counter?).
