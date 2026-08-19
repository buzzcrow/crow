<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R104: server — Group-0 authoritative restore (toml bootstrap-only)

## Problem

**Current behavior + impact.** `crow-kv-server` requires `--config
<toml>` at every boot and panics if the file is missing
(`app/crow-kv-server/src/main.rs:88`). Stores/groups are created only
from `--stores`/`--groups` CLI args; there is no auto-restore of
stores/groups from local disk or group 0 at startup. The design doc
`doc/design/kv/design-crow-kv-server.md` §2.2 *describes* auto-restore
from `conf/node-config.json`, but `main.rs` does not implement it —
`maybe_apply_persisted_config` (`startup.rs:27`) only patches group
membership onto already-created groups; it does not create them.
`reconcile_with_group0` (`reconcile.rs`) only **warns** about missing
stores/groups and defers creation to the management API.

The impact: after `POST /system/init` creates group 0, the operator
must keep the toml + pass the same `--stores`/`--groups` on every
restart, or the node comes back empty. The toml is the de-facto source
of truth for topology even though group 0 already holds the
authoritative `/kv/store/`, `/kv/group/`, `/kv/replica/` records
(`design-crow-kv-group0.md` §3.1, §5.1 "Phase 2 cuts over to group 0
authoritative"). The cutover described in §5.1 was never implemented.

**Design pointers.** `design-crow-kv-group0.md` §5.1 (two-phase
bootstrap, toml → group 0 cutover), §3.1 (topology key layout),
`design-crow-kv-server.md` §2.2 (startup ordering / node-config cache
restore). No direct aioss analog — new work.

**Use scenarios.**

- **First boot (greenfield).** Operator starts a node with `--root
  <dir>` (and optionally `--config <toml>` for tunables). No
  `waldata/store0/group0` exists on disk. Server boots the management
  API empty. Operator calls `POST /system/init` → store 0 / group 0
  created, leader self-elects. Operator then deletes the toml.
- **Restart after group 0 exists.** Operator starts the node with
  `--root <dir>` (no `--config`, no `--stores`/`--groups`). Server
  scans `<root>/waldata/store*/group*`, loads every local store/group
  from disk (replay WAL + open crow-tree), reads group 0 topology to
  wire remote replicas, and rejoins the cluster. The toml is not
  required.
- **Restart with group 0 present but a new data group's WAL missing
  locally.** Group 0 says store 1 / group 1 should exist on this node
  (a `/kv/replica/1/1/<rid>` record points here), but no local WAL dir
  exists yet. Server logs the gap and defers creation to the
  management API (cannot fabricate a replica with no WAL).
- **First boot with no toml and no group 0.** Server starts with
  code-default tunables + `--root`-derived paths, empty management
  API. `POST /system/init` still works (group 0 created from defaults).

## Solution

**One-line summary.** Make the toml optional: on restart the server
scans `<root>/waldata` for local stores/groups, loads them from disk,
then reads group 0 to wire remote replicas; the toml is only needed
for first-boot tunables before group 0 exists.

**Numbered work items.**

- **Fixed node layout + `--root` CLI** (`app/crow-kv-server/src/cli.rs`,
  `lib/crow-kv/src/common/config.rs`). Add `--root <dir>`. Derive
  `wal_root = root/waldata`, `config_root = root/conf`,
  `data_root = root/ctdata`, `log_dir = root/log` (fixed subfolder
  names, code defaults). Make `--config` optional. Tunables
  (paxos/election/wal/server) fall back to `CrowKVConfig::default()`
  when no toml is supplied.
- **Local-disk scan + restore mode** (new
  `app/crow-kv-server/src/restore.rs`, `main.rs`). On boot, scan
  `<root>/waldata` for `store{S}/group{G}` dirs. If
  `store0/group0` is present → restore mode: ignore the toml for
  topology, load every local (store, group) via the existing
  `create_group_with_wal` (`startup.rs:138`), start each store. If no
  `store0/group0` → first-boot mode: behave as today (toml/CLI-driven,
  empty mgmt API ready for `/system/init`).
- **Group-0 topology read + remote replica auto-wiring** (rewrite
  `app/crow-kv-server/src/reconcile.rs`). After local load, scan group
  0 `/kv/replica/<store>/<group>/<replica>` records. For each local
  group, wire the peer replicas' endpoints as remote replicas
  (`PxGroup::add_remote_replica`). Replace the current warn-only
  behavior with actual wiring. Local replica_id comes from the
  replayed WAL state (already known after `create_group_with_wal`).
- **Persist node root to group 0** (`lib/crow-protocol/src/proto/sysdata_type.proto`,
  `crow-kv-client`). Add `data_root` to `KvServerExtra` (or a new
  `/kv/node/<node_id>` record). The keep-alive loop
  (`app/crow-kv-server/src/keepalive.rs`) writes this node's root so
  the cluster/console knows each node's data location. (Local restore
  does not depend on this — it is for cluster awareness.)

**Flow diagram.**

```
                 boot: --root <dir>  [--config <toml>]
                          |
        scan <root>/waldata/store*/group*
                          |
              store0/group0 present?
                    /            \
                 yes              no
                 /                  \
        RESTORE MODE             FIRST-BOOT MODE
   load local stores/groups    toml (if given) or defaults
   via create_group_with_wal   empty mgmt API
          |                          |
   read group0 /kv/replica/*    POST /system/init
   wire remote replicas         creates store0/group0
          |                          |
   persist root to group0       operator deletes toml
   (keep-alive)                 (next boot -> RESTORE MODE)
          |                          |
        rejoin cluster           cluster initialized
```

**Edge cases at a glance.**

- No `--root` and no toml → error, cannot determine paths.
- `--root` given but dir missing → create it (first boot) or error if
  restore mode expected group 0 (treat as first boot).
- Local WAL dir exists but WAL replay fails → log error, skip that
  group, continue with the rest (matches current `create_and_start_stores`
  skip-on-error behavior).
- Group 0 reachable but has no `/kv/store/` records yet → silent skip
  (not yet initialized), keep local state.
- Group 0 unreachable on restore → keep local state, retry wiring on
  next restart (best-effort, same policy as today).
- `/kv/replica/` record points here but no local WAL → log gap, defer
  to management API (cannot fabricate a replica with no WAL).

## Dependencies

- Depends on existing `create_group_with_wal` (`startup.rs:138`),
  `reconcile_with_group0` (`reconcile.rs`), keep-alive loop
  (`keepalive.rs`), group-0 sysdata schema
  (`design-crow-kv-group0.md` §3).
- Depends on `crow-kv-client` `KVClusterMetaClient` for topology reads
  + `ServiceRegistryClient` for root persistence.
- No unlanded dependencies. The `node_id` field in `StoreValue` /
  `ReplicaValue` is not required for local-disk-scan restore (restore
  is driven by local WAL presence, not by node_id filtering).

## Acceptance

**First boot / toml optionality:**

- Start server with `--root <dir>` only (no `--config`) on an empty
  dir → server boots, `GET /health` returns 200, no stores present.
  `POST /system/init` creates store 0 / group 0, leader self-elects.
  Integration test.
- Start server with neither `--root` nor `--config` → process exits
  with a clear error. Integration test.
- Start server with `--config <toml>` + `--root <dir>` on empty dir →
  toml tunables applied (verify via a tunable-dependent log line or
  `/metrics`), paths derived from `--root`. Integration test.

**Restore mode (restart after group 0):**

- Init group 0 + add store 1 / group 1 with a remote replica, write
  `/kv/replica/` records, shut down, delete the toml, restart with
  `--root <dir>` only → store 0 / group 0 and store 1 / group 1
  restored from local WAL, remote replica wired (verify via
  `GET /topology` showing the peer endpoint). E2E test.
- After restart in restore mode, no `--stores`/`--groups` passed →
  all local stores/groups still present. E2E test.

**Auto-wiring from group 0:**

- Two-node cluster: node A hosts group 0 + group 1 leader, node B
  hosts a group 1 follower replica. Restart node B with `--root` only
  → B loads group 1 from local WAL, reads group 0 `/kv/replica/1/1/*`,
  wires A's endpoint as a remote replica, rejoins quorum. E2E test.
- Group 0 has a `/kv/replica/` record pointing at this node but no
  local WAL dir → server logs the gap, does not crash, other groups
  still load. Integration test.

**Edge cases:**

- Corrupt/unreadable WAL for one group on restart → that group is
  skipped with an error log, other groups still load and serve.
  Integration test.
- Group 0 unreachable on restart → local stores/groups still load and
  serve; remote wiring retried on next restart. Integration test.

**Test commands.** `pixi run test-tree-ct` (no C++ change here),
`pixi run cargo test -p crow-kv-server`,
`pixi run cargo test -p crow-kv-client`,
`pixi run cargo fmt --all -- --check`,
`pixi run cargo clippy --all-targets -- -D warnings`.

## Open Questions

- **`--root` vs keeping `--wal-root`/`--data-root`/`--config-root`.**
  `--root` is the new single knob; the three legacy flags stay as
  optional overrides (escape hatch for non-standard layouts). Default
  when none passed: cwd. To confirm during design.
- **Where to persist the node root in group 0.** Candidate A: add
  `data_root` (string) to `KvServerExtra` in `sysdata_type.proto`
  (rides the existing keep-alive). Candidate B: a new
  `/kv/node/<node_id>` record. Lean A (smaller schema change, no new
  key namespace). To confirm during design.
