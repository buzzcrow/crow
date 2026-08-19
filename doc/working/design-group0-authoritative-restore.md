<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Group-0 Authoritative Restore (R104)

Backlog: [`doc/backlog/R104-kv-server-group0-authoritative-restore.md`](../backlog/R104-kv-server-group0-authoritative-restore.md).
Root design: [`design-crow-kv-group0.md`](../design/kv/design-crow-kv-group0.md) §5.1 (cutover), §3.1 (topology schema);
[`design-crow-kv-server.md`](../design/kv/design-crow-kv-server.md) §2.2 (startup ordering).

Already landed: `create_group_with_wal` (`app/crow-kv-server/src/startup.rs:138`)
replays WAL + opens the crow-tree engine + applies persisted group config from
`node-config.json` (`maybe_apply_persisted_config`, `startup.rs:27`).
`reconcile_with_group0` (`app/crow-kv-server/src/reconcile.rs`) scans group 0
`/kv/store/` + `/kv/group/` but only warns. Keep-alive loop
(`app/crow-kv-server/src/keepalive.rs`) registers under
`/srv/kv-server/<instance_id>` via `ServiceRegistryClient`. The toml
(`CrowKVConfig`, `lib/crow-kv/src/common/config.rs:423`) is required at boot
(`main.rs:88` panics if missing); paths are `#[serde(skip)]`, set from CLI.

Architecture decisions and rationale are in the root design; this doc does not
repeat them.

## 1. Fixed node layout + `--root` CLI

### 1.1 Why

The toml mixes two concerns: cluster topology (which should come from group 0)
and node-local operational settings (paths, backends, tunables). Once group 0
exists, topology is authoritative there and the toml is redundant for it. But
paths still need a source. The deployed-on-disk layout is fixed per node
(`waldata/`, `conf/`, `ctdata/`, `log/` under a root), so a single `--root`
plus fixed subfolder names replaces the three path flags and the toml's path
fields. Tunables have sensible `CrowKVConfig::DEFAULT` values and only need
the toml for first-boot overrides.

### 1.2 CLI changes (`app/crow-kv-server/src/cli.rs`)

Add `--root`:

```rust
/// Node root directory. Derives wal_root=<root>/waldata,
/// config_root=<root>/conf, data_root=<root>/ctdata, log_dir=<root>/log.
/// Required on every start.
#[arg(long)]
pub root: std::path::PathBuf,
```

Make `--config` optional (remove the "Required" doc; default `None`):

```rust
/// Optional TOML config for first-boot tunable overrides. Ignored in
/// restore mode (group 0 present on disk).
#[arg(long)]
pub config: Option<std::path::PathBuf>,
```

a. `--root` is required on every start (clap enforces this; no default).
b. The four paths are always derived from `--root`. The legacy
   `--wal-root`/`--config-root`/`--data-root` flags are removed (the fixed
   layout is the only supported layout); `--log-dir` was never a flag.
c. If `--config` is omitted, tunables come from `CrowKVConfig::default()`.

### 1.3 Path derivation (`lib/crow-kv/src/common/config.rs`)

Add a helper on `CrowKVConfig`:

```rust
pub fn apply_root(&mut self, root: &Path) {
    self.wal_root   = root.join("waldata");
    self.config_root = root.join("conf");
    self.data_root  = root.join("ctdata");
    self.log_dir    = root.join("log");
}
```

a. Called from `main.rs` after loading the toml (or constructing
   `CrowKVConfig::default()` when no toml).
b. Fixed subfolder names are the only supported layout; no config field to
   rename them (the user confirmed the layout is fixed per node).

### 1.4 Config loading rewrite (`app/crow-kv-server/src/main.rs:86-123`)

Replace the panicking `load_from_file` with:

```rust
let mut config = match args.config.as_ref() {
    Some(path) => CrowKVConfig::load_from_file(path)
        .unwrap_or_else(|e| panic!("failed to load config from {}: {e}", path.display())),
    None => CrowKVConfig::default(),
};
config.apply_root(&args.root);
// tunable CLI overrides (election_profile, max_inflight, coalesce_*,
// no_fsync, backends) apply on top as today.
```

a. Tunable CLI overrides (`election_profile`, `max_inflight`,
   `coalesce_*`, `no_fsync`, backends) apply on top as today.
b. The config-file watcher (`main.rs:139`) only starts when `--config` is
   passed; otherwise skip (nothing to watch).

Edge cases:

- `--root` points at a non-existent dir → `apply_root` still sets the paths;
  first-boot store creation creates the dirs (`create_group_with_wal` already
  `create_dir_all`s). Restore mode treats a missing `waldata/store0` as
  "no group 0" → first-boot mode.
- `--config` points at a missing file → panic with the path (unchanged); the
  operator passed it explicitly so a missing file is a real error.
- `--root` omitted → clap rejects the invocation before `main` runs.

## 2. Local-disk scan + restore mode

### 2.1 Why

The node knows which stores/groups it hosts by what WAL data it has on disk —
no `node_id` round-trip to group 0 is needed to decide what to load. This
matches the operator mental model ("this node's disks hold these groups") and
avoids the chicken-and-egg of needing group 0 to find group 0. Crucially,
`create_group_with_wal` already calls `maybe_apply_persisted_config`
(`startup.rs:27`) which loads `node-config.json` and — via `apply_config`
(`group_membership.rs:294`) — restores the remote replicas (id + endpoint +
voting) from the per-node cache. So a normal restart rebuilds stores/groups
WITH remotes wired from local disk alone; group 0 is only consulted
afterward (§3) as verification and as the fallback when `node-config.json`
is missing or stale for a group.

### 2.2 New module `app/crow-kv-server/src/restore.rs`

```rust
/// A local (store_id, group_id) pair discovered by scanning waldata.
pub struct LocalGroup { pub store_id: u64, pub group_id: u64 }

/// Scan `<wal_root>` for `store{S}/group{G}` directories.
/// Returns the list sorted by (store_id, group_id). Empty if no waldata.
pub async fn scan_local_groups(wal_root: &Path) -> io::Result<Vec<LocalGroup>>

/// True if `waldata/store0/group0` exists (group 0 is on disk).
pub async fn group0_exists(wal_root: &Path) -> bool
```

a. `scan_local_groups` reads `wal_root`, for each entry matching
   `store(\d+)` parses the store_id, then scans that dir for `group(\d+)`
   entries and parses the group_id.
b. Non-matching entries are skipped (e.g. legacy segment files at the
   `waldata/store{S}/` level are not `group*` dirs).
c. `group0_exists` is `scan_local_groups` filtered to `(0,0)` — kept as a
   separate fn for clarity at the call site.

### 2.3 Startup branch (`app/crow-kv-server/src/main.rs:202-215`)

Replace the current `if let Some(b) = bootstrap` + `reconcile_with_group0`
block with:

```rust
let local_groups = restore::scan_local_groups(&config.wal_root).await
    .unwrap_or_default();
if restore::group0_exists(&config.wal_root).await {
    info!(local_count = local_groups.len(), "restore mode: group 0 on disk");
    restore::load_local_groups(&local_groups, args.replica, &registry).await;
    reconcile::reconcile_with_group0(&registry).await;
} else {
    info!("first-boot mode: no group 0 on disk");
    if let Some(b) = bootstrap.as_ref() {
        create_and_start_stores(&b.store_ids, &b.group_ids, b.replica_id,
            b.ports.clone(), registry.clone()).await;
    }
    // mgmt API is up; operator calls POST /system/init.
}
```

a. `load_local_groups` groups `LocalGroup`s by store_id, creates each
   `PxKvStore` (port from `persisted_port_for_store` or port pool, falling
   back to 0), then calls `create_group_with_wal` per group with
   `PxLocalReplicaRole::Follower` (the election driver promotes), `add_group`,
   `store.start()`, `registry.add_store`. This reuses `create_and_start_stores`'s
   skip-on-error policy.
b. In restore mode, `--stores`/`--groups` are ignored (with a warn log) —
   local disk is the source of truth. (Escape hatch: a future `--force-cli`
   flag; out of scope for R104.)
c. First-boot mode keeps the current `--stores`/`--groups` path for
   backward compat with existing scripts/tests.

Edge cases:

- `scan_local_groups` IO error → log warn, treat as empty (first-boot mode).
  The server still comes up; operator can init group 0.
- A `store{S}` dir exists but has no `group*` subdirs → store is not created
  (no groups to load); logged at debug.
- `create_group_with_wal` fails for one group → that group skipped, store
  still starts with its other groups (matches current behavior).

## 3. Group-0 topology verification + fallback wiring

### 3.1 Why

`node-config.json` is the per-node cache of group 0's topology. On a normal
restart it is present and `apply_config` wires remotes from it — group 0 is
not read for wiring. But the cache can be missing or stale (lost file, a
membership change that happened while this node was down, a fresh replica
dir whose `node-config.json` was never written). In those cases the group
comes up as a `quorum=1` singleton with no remotes and cannot reach quorum.
group 0 — the authoritative `/kv/replica/` records — is the fallback that
re-wires the correct peers. It is also the verification source: the reconcile
step logs any mismatch between local state and group 0 so the operator knows
the cache is stale.

### 3.2 Rewrite `app/crow-kv-server/src/reconcile.rs`

Replace the warn-only body with `reconcile_with_group0` (same name, new
behavior): scan group 0 `/kv/replica/`, and for each local group that has
**no** remote replicas wired (the singleton/fallback case), seed its remotes
from the group 0 records. Groups that already have remotes (from
`node-config.json`) are verified against group 0 and discrepancies are
logged but not forcibly overwritten (the live membership may be ahead of
group 0 during an in-flight reconfiguration).

```rust
pub async fn reconcile_with_group0(registry: &KvStoreRegistry) {
    let Some(store0) = registry.get_store(0) else { return; };
    if store0.get_group(0).is_none() { return; }
    let replicas = scan_replica_records(store0).await;   // /kv/replica/*
    // Group records by (store_id, group_id) -> Vec<(replica_id, endpoint, voting)>
    let mut by_group: HashMap<(u64,u64), Vec<(u64,String,bool)>> = HashMap::new();
    for r in replicas { by_group.entry((r.store_id,r.group_id)).or_default()
        .push((r.replica_id, r.endpoint, r.voting)); }
    for ((sid,gid), peers) in by_group {
        let Some(store) = registry.get_store(sid) else { continue; };
        let Some(group) = store.get_group(gid) else { continue; };
        let local_id = group.local_replica().id;
        let existing = group.remote_replica_info();
        if existing.is_empty() {
            // Fallback: node-config.json was missing/stale. Seed from group 0.
            let remotes: Vec<_> = peers.iter()
                .filter(|(rid,_,_)| *rid != local_id)
                .map(|(rid,ep,v)| PxRemoteReplica::new(*rid, ep.clone()).with_voting(*v))
                .collect();
            if !remotes.is_empty() {
                let new_group = rebuild_group_with_remotes(&group, &remotes);
                store.add_group(new_group);
                info!(store_id=sid, group_id=gid, "reconcile: seeded remotes from group 0");
            }
        } else {
            // Verify: log any peer in group 0 not present locally.
            for (rid, ep, _) in &peers {
                if *rid != local_id && !existing.iter().any(|(eid,_,_)| eid == rid) {
                    warn!(store_id=sid, group_id=gid, replica_id=rid,
                        endpoint=ep, "reconcile: group 0 has peer not wired locally");
                }
            }
        }
    }
}
```

a. `scan_replica_records` does a prefix scan of `/kv/replica/` on group 0,
   decodes each `ReplicaValue` (`crow_protocol::common::ReplicaValue`:
   `store_id`, `group_id`, `replica_id`, `node_id`, `role`, `voting`,
   `endpoint`), returns the list.
b. The fallback seed reuses `rebuild_group_with_new_remotes`'s pattern
   (`mgmt/replica_ops.rs:52`): when the group has no remotes, bulk-set them
   via `set_remote_replicas` (non-bumping). This requires rebuilding the
   group (`rebuild_group_with_same_config`) and `store.add_group` — the
   same path the mgmt API uses — because remotes cannot be mutated on a
   running group without a rebuild.
c. The verify path only logs; it does not overwrite a live group's
   membership (which may be legitimately ahead of group 0 during
   reconfiguration).
d. Group 0 unreachable / empty → silent skip (same policy as today);
   fallback retries on next restart.

Edge cases:

- A `/kv/replica/` record's `endpoint` is unreachable now → still wire it
  (Paxos will retry the channel); liveness is the election driver's job.
- `ReplicaValue` parse failure for one record → skip that record, continue.
- Group 0 leader not yet elected at restore time → scan returns `ok=false`;
  skip silently, retry next restart. (R104 does not add a readiness poll;
  single-node self-elect makes this immediate, multi-node relies on the
  election driver + next restart.)

## 4. Persist node root to group 0

### 4.1 Why

The cluster/console needs to know where each node keeps its data (for ops,
disk move, capacity views). The keep-alive loop already writes a
`KvServerInstanceValue` every tick — piggybacking the root there is the
smallest schema change and avoids a new key namespace.

### 4.2 Proto change (`lib/crow-protocol/src/proto/sysdata_type.proto`)

Add to `KvServerExtra`:

```proto
message KvServerExtra {
  repeated uint64         hosted_stores  = 1;
  repeated HostedGroup    hosted_groups  = 2;
  string                  health         = 3;
  string                  data_root      = 4;  // node root dir (R104)
}
```

a. `build.rs` already derives serde on proto types — no extra wiring.
b. Old readers ignore field 4 (proto3 unknown-field-safe); no migration.

### 4.3 Keep-alive change (`app/crow-kv-server/src/keepalive.rs`)

`KeepAliveLoop::spawn` gains a `data_root: String` param; `register_kv_server`/
`heartbeat_kv_server` in `crow-kv-client`'s `ServiceRegistryClient` gain a
`data_root` arg threaded into `KvServerExtra`. The value is
`config.wal_root.parent()` (the root) or a new `config.node_root` field set
from `--root`.

a. This is cluster-awareness only; local restore (§2) does not read it.

Edge cases:

- `--root` is always passed (required), so `data_root` is always populated.
  If the root is a relative path, it is stored as-given (resolved by the
  operator's process working dir).

## Scope

- `app/crow-kv-server/src/cli.rs` — add required `--root`, make `--config`
  optional, remove `--wal-root`/`--config-root`/`--data-root`.
- `app/crow-kv-server/src/main.rs` — config-load rewrite (§1.4), restore-mode
  branch (§2.3), thread `data_root` to keep-alive.
- `app/crow-kv-server/src/restore.rs` — new: `scan_local_groups`,
  `group0_exists`, `load_local_groups`.
- `app/crow-kv-server/src/reconcile.rs` — rewrite to verification + fallback
  wiring from group 0 (§3.2).
- `app/crow-kv-server/src/keepalive.rs` — pass `data_root` into registration.
- `lib/crow-kv/src/common/config.rs` — `apply_root` helper (§1.3); optional
  `node_root` field.
- `lib/crow-protocol/src/proto/sysdata_type.proto` — `KvServerExtra.data_root`.
- `lib/crow-kv-client` (`ServiceRegistryClient`) — thread `data_root` through
  `register_kv_server`/`heartbeat_kv_server`.
- `doc/design/kv/design-crow-kv-server.md` §2.2 — update to describe restore
  mode (fold on merge).
- `doc/design/kv/design-crow-kv-group0.md` §5.1 — mark Phase 2 cutover landed.
- `doc/backlog/R104-...md` — delete on merge.
- `doc/doc_index.md` — no new doc (R104 folds into existing design docs).

## Complexity

Medium. The hard part is the startup-flow rewrite in `main.rs` (two modes,
correct ordering with the mgmt API already up) and making the fallback
wiring correct without a group-0 readiness poll. `create_group_with_wal` is reused
as-is. The proto/keep-alive change is a small additive field. No new
consensus or storage primitive. Main risk: existing tests/scripts that pass
`--config` + `--stores` must keep working (first-boot mode preserves them).

## Test Design

**Unit tests (UT):**

- `scan_local_groups` on a temp `waldata` with `store0/group0`,
  `store1/group1`, `store1/group2`, a stray file, and a `store3` with no
  groups → returns `[(0,0),(1,1),(1,2)]`, stray ignored, store3 absent.
  UT.
- `scan_local_groups` on a missing dir → returns empty (no panic). UT.
- `group0_exists` true when `store0/group0` present; false otherwise. UT.
- `apply_root(root)` sets the four paths to `root/{waldata,conf,ctdata,log}`. UT.
- `reconcile_with_group0` fallback: fake registry with a quorum=1 group (no
  remotes) + fake group-0 scan returning two replicas (one self, one peer) →
  self skipped, peer seeded via rebuild. UT.
- `reconcile_with_group0` verify: group already has the peer wired → no
  rebuild, no duplicate, no warn. UT.
- `reconcile_with_group0` verify-mismatch: group 0 has a peer not wired
  locally → warn logged, no rebuild. UT.

**End-to-end tests (E2E):**

- Single-node: `--root` only on empty dir → `/health` 200, no stores;
  `POST /system/init` → group 0 created; stop, delete toml, restart
  `--root` only → group 0 restored from disk, `/topology` shows store 0 /
  group 0. E2E.
- Two-node: A hosts group 0 + group 1 (leader), B hosts a group 1 follower;
  write `/kv/replica/1/1/*`; restart B with `--root` only → B loads group 1
  from WAL, wires A's endpoint, rejoins quorum (a proposal on A replicates
  to B). E2E.
- Restart with `--config <toml>` + `--root` in restore mode → toml tunables
  ignored for topology (restore wins), paths from `--root`. E2E.
- `/kv/replica/` record points at this node but no local WAL → gap logged,
  no crash, other groups load. E2E.
- Corrupt WAL for one group → that group skipped, others serve. E2E.

## Module Structure

```
app/crow-kv-server/src/
  cli.rs          (mod)   --root, optional --config
  main.rs         (mod)   config-load rewrite, restore-mode branch
  restore.rs      (new)   scan_local_groups, group0_exists, load_local_groups
  reconcile.rs    (mod)   reconcile_with_group0 (verification + fallback wiring)
  keepalive.rs    (mod)   thread data_root
  startup.rs      (unch)  create_group_with_wal reused
lib/crow-kv/src/common/
  config.rs       (mod)   apply_root, optional node_root
lib/crow-protocol/src/proto/
  sysdata_type.proto (mod) KvServerExtra.data_root
lib/crow-kv-client/.../service_registry.rs (mod) data_root threading
```

## Config Extensions

- `--root <dir>` (new, required) — derives wal/config/data/log paths.
- `--config <toml>` (was required, now optional) — first-boot tunables only.
- `--wal-root`/`--config-root`/`--data-root` (removed) — replaced by `--root`.
- `CrowKVConfig::node_root: Option<PathBuf>` (new, `#[serde(skip)]`) — set
  from `--root`; used to populate `KvServerExtra.data_root`. Defaults None.
- `validate()` unchanged.

## Server Wiring

1. `main.rs`: parse CLI; build `config` from `--config` or `default()`; if
   `--root`, `apply_root`; apply CLI tunable overrides.
2. Start mgmt API (unchanged — must be up before stores).
3. `scan_local_groups(&config.wal_root)`; `group0_exists`?
4. Yes → `load_local_groups` (create stores + `create_group_with_wal` per
   local group) → `reconcile_with_group0` (fallback wiring + verify).
5. No → `create_and_start_stores` from `--stores`/`--groups` if given (else
   empty); operator calls `POST /system/init`.
6. Keep-alive loop spawned with `data_root = config.node_root` (§4).
7. Binding monitor, metrics runner, serve (unchanged).

## Open Questions

- **`add_remote_replica` idempotency.** Need to confirm whether
  `PxGroup::add_remote_replica` is idempotent or needs a
  `get_remote_replica` guard before calling. To verify in code during impl.
