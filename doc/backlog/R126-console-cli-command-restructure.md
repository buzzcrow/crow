<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R126: console — CLI command restructure by function

### Proposed Command Hierarchy

Four top-level groups, split by service domain: `cluster` (hardware
topology + cluster-level ops, alias `cls`), `kv` (KV layer — server +
logical concepts + data-plane), `chunk` (chunk storage service cluster
— diskdb + chunkdb + diskio servers + future chunk data-plane), `bench`
(load injection per layer). Every current verb mapped to its new home.
Old command names in `(was: ...)` annotations.

```
crowdb-cli
│
├── cluster  (alias: cls)           ← hardware topology + cluster-level ops
│   ├── init                        (was: cluster init)
│   ├── reset                       (NEW — full teardown: POST /internal/reset, §13)
│   ├── clean                       (NEW — wipe user data, keep cluster metadata + group-0)
│   ├── status                      (was: cluster status)
│   ├── topology                    (was: cluster topology)
│   ├── rack
│   │   ├── add                     (was: rack add)
│   │   ├── remove                  (was: rack remove)
│   │   └── list                    (was: rack list)
│   ├── node
│   │   ├── add                     (was: node add)
│   │   ├── remove                  (was: node remove — requires empty node: no servers/services)
│   │   ├── list                    (was: node list)
│   │   └── ping                    (was: node ping)
│   ├── disk-group                  (was: disk-group — hardware topology)
│   │   ├── add
│   │   ├── remove
│   │   ├── list
│   │   └── set-status              (was: diskdb set-dg-status — moved: hardware state)
│   └── disk                        (was: disk — hardware topology)
│       ├── add
│       ├── remove
│       ├── list
│       └── set-status              (was: diskdb set-status — moved: hardware state)
│
├── kv                              ← KV layer: server + logical concepts + data-plane
│   ├── server                      (was: server — crowdb-kv-server lifecycle)
│   │   ├── deploy
│   │   ├── restart                 (alias: start)
│   │   ├── stop
│   │   ├── delete                  (NEW — graceful removal; requires empty server:
│   │   │                                no replicas/groups/stores hosted; see Concept Gaps §2)
│   │   └── list
│   ├── store                       (was: store — KV logical store)
│   │   ├── add
│   │   ├── remove
│   │   ├── list
│   │   └── inspect
│   ├── group                       (was: group, alias paxos — KV paxos group)
│   │   ├── add
│   │   ├── remove
│   │   ├── list
│   │   └── inspect
│   ├── replica                     (was: replica — KV group replica)
│   │   ├── add
│   │   └── remove
│   ├── put                         (was: kv put — data-plane)
│   ├── get                         (was: kv get — data-plane)
│   ├── delete                      (was: kv delete — data-plane)
│   ├── scan                        (was: kv scan / kv list — data-plane; API uses scan)
│   └── snapshot                    (was: kv snapshot — data-plane)
│       ├── create
│       ├── list
│       ├── scan
│       └── release
│
├── chunk                           ← chunk storage service cluster (diskdb + chunkdb + diskio + chunk lib)
│   ├── diskdb                      (was: diskdb — disk block allocator server)
│   │   ├── deploy
│   │   ├── restart
│   │   ├── stop
│   │   ├── delete
│   │   ├── list                    (was: diskdb instances — renamed for consistency)
│   │   ├── usage
│   │   ├── scan-status
│   │   ├── scan
│   │   ├── recalc
│   │   ├── compact
│   │   └── rebuild
│   ├── chunkdb                     (future — chunk metadata server, one per node)
│   │   ├── deploy
│   │   ├── restart
│   │   ├── stop
│   │   ├── delete
│   │   └── list
│   ├── diskio                      (future — disk I/O server, one per node)
│   │   ├── deploy
│   │   ├── restart
│   │   ├── stop
│   │   ├── delete
│   │   └── list
│   ├── allocate                    (future — chunk data-plane)
│   ├── free                        (future — chunk data-plane)
│   ├── write                       (future — chunk data-plane)
│   ├── read                        (future — chunk data-plane)
│   └── gc                          (future — chunk garbage collection)
│
└── bench                           ← load injection only (against a deployed cluster)
    ├── kv
    │   ├── read                    (was: bench run --workload read)
    │   ├── write                   (was: bench run --workload write)
    │   ├── scan                    (was: bench run --workload list — renamed: API uses scan)
    │   └── mix                     (was: bench run --workload mix)
    ├── rpc                         (was: bench rpc — transport echo at lib level, no KV layer)
    ├── diskdb                      (future)
    │   ├── allocate
    │   └── mix
    ├── chunkdb                     (future)
    │   ├── allocate
    │   └── mix
    └── chunk                       (future)
        ├── write
        ├── read
        └── mix
```

**Design decisions reflected above:**

- **Split by service domain** — four top-level groups (`cluster`, `kv`,
  `chunk`, `bench`), each cohesive and focused:
  - `cluster` — all hardware topology (rack, node, disk-group, disk,
    including runtime hardware state via `set-status`) + cluster-level
    ops (init, reset, clean, status, topology).
  - `kv` — KV layer: `kv server` (crowdb-kv-server lifecycle, including
    cascading `delete`), `kv store`/`kv group`/`kv replica` (logical
    concepts), `kv put`/`get`/`delete`/`scan`/`snapshot` (data-plane).
    The verb distinguishes management from data-plane; no `kv` prefix
    needed on resource names.
  - `chunk` — chunk storage service cluster: `chunk diskdb`/`chunk
    chunkdb`/`chunk diskio` (server lifecycle + maintenance) + future
    chunk data-plane (`allocate`/`free`/`write`/`read`/`gc`). diskdb
    (block allocator), chunkdb (chunk metadata), diskio (disk I/O), and
    the chunk client lib compose the chunk storage service cluster; the
    group name reflects the unified service, not individual servers.
  - `bench` — load injection per layer.
- **`disk-group` and `disk` (including `set-status`) are in `cluster`**
  — they are hardware topology concepts (physical disks grouped into
  disk-groups on nodes in racks). Both the topology CRUD
  (`add`/`remove`/`list`) and the runtime hardware state change
  (`set-status`) are cluster-level concerns. `set-status`/`set-dg-status`
  are executed through the diskdb service API, but the CLI verb belongs
  under `cluster` because it changes hardware topology state, not chunk
  service state. `chunk diskdb` owns only the diskdb service lifecycle
  and maintenance (scan/recalc/compact/rebuild).
- **`cluster` alias `cls`** — short alias for convenience via clap
  `alias`. `cls status` is equivalent to `cluster status`.
- **`cluster reset` vs `cluster clean`** — two distinct teardown levels:
  `cluster reset` is full teardown (user data + group-0 + processes +
  config, via `POST /internal/reset` §13); `cluster clean` wipes user
  data only, keeping cluster metadata (group-0 sysdata, topology,
  running servers) for a fresh data-plane without re-bootstrapping.
- **`cluster inspect` removed** — per-resource `inspect` verbs (`kv
  store inspect`, `kv group inspect`) cover detail views; `cluster
  inspect` was a redundant multi-entity path-grammar verb. An operator
  inspects a specific resource under its own domain.
- **`kv server delete` is graceful + requires empty** — all operations
  use graceful Paxos reconfiguration, no force-kill. `delete` requires
  the server to be empty (no replicas/groups/stores hosted); the
  operator must delete in bottom-up order (replicas → groups → stores →
  server → node). Same policy applies to `cluster node remove`. See
  Concept Gaps §2 for full semantics.
- **Server lifecycle verbs are consistent across all server types** —
  every server sub-group (`kv server`, `chunk diskdb`, `chunk chunkdb`,
  `chunk diskio`) has `deploy`/`restart`/`stop`/`delete`/`list`. Servers
  are deployed one-per-node by default; `list` enumerates instances
  across all nodes. `diskdb instances` renamed to `diskdb list` for
  consistency.
- **Data-plane uses `scan`, not `list`** — the API uses `scan` for
  prefix-scan operations. `kv scan` consolidates the old `kv scan`/`kv
  list` pair. `bench kv scan` replaces `bench run --workload list`.
  Management verbs (`rack list`, `node list`, `server list`, `store
  list`, etc.) keep `list` — they enumerate resources, not data.
- **Bench workload types are sub-commands, not `--workload` flags** —
  `bench kv read` is more discoverable than `bench kv --workload read`.
  Future bench types follow the same `bench <layer> <workload>` pattern.
  `bench rpc` is flat (no sub-commands) — it benchmarks the RPC lib at
  the transport layer, not a KV workload, so there are no workload
  sub-types.
- **`bench report` / `bench compare` removed** — not load injection.
  Bench runs save JSON reports to disk; comparison is done with
  external tools (`diff`, `jq`).

### Production Standard — Shared Across CLI and UI

The four-domain hierarchy (`cluster` / `kv` / `chunk` / `bench`) is the
**standard concept** across the production system. It is not
CLI-specific — the console UI (`crowdb-web`) will be refined later to
use the same domain grouping for its navigation and operation surfaces.

**Common operations in `crowdb-console-shared`.** The operation logic
behind each verb — the calls to group-0 sysdata, KV data-plane,
diskdb/chunkdb/diskio services — lives in the `crowdb-console-shared`
crate, not in the CLI or UI frontend. Both frontends call the same
shared operations:
- `crowdb-cli` — thin clap routing → shared operation → group-0 direct.
- `crowdb-web` — Axum handler → shared operation → group-0 direct (UI
  only; no longer a CLI dependency).

This ensures CLI and UI behave identically: the same `cluster clean`,
`kv server delete`, `chunk diskdb scan` operation invoked from either
frontend produces the same result. The shared layer owns the
group-0-direct connection model (Concept Gaps §1), the graceful +
require-empty deletion policy (§2), and the clean boundary semantics
(§3).

**What changes in each crate:**
- `crowdb-console-shared` — gains the operation functions currently
  inlined in CLI command handlers (`app/crowdb-cli/src/commands/*.rs`)
  and web handlers (`app/crowdb-web/src/mgmt.rs`). The existing
  `ConsoleClient` HTTP-based calls are replaced with group-0-direct
  calls via `KVClusterMetaClient` + service clients.
- `crowdb-cli` — command handlers become thin wrappers: parse args →
  call shared operation → print result.
- `crowdb-web` — Axum handlers become thin wrappers: parse request →
  call shared operation → return JSON. The web UI navigation is
  regrouped to match the four domains in a later task.

### Concept Gaps (resolved)

Three semantic ambiguities were identified in the hierarchy. All three
are now resolved with decisions below; the design draft implements
these decisions directly.

**Gap 1 — CLI connection model: unified group-0 direct (resolved).**
The CLI does **not** depend on console-web. The CLI connects directly
to group-0 (any replica node of group-0) to fetch cluster info, then
resolves all downstream endpoints from group-0 sysdata. Console-web is
UI-only — it is no longer a CLI dependency. This applies to **all**
commands, not just bench: `cluster`, `kv`, `chunk`, `bench` all connect
to group-0 directly.

The global connection flags are renamed to avoid confusion with
console-web's `--ip`/`--port`:
- `--sysmd-ip` (default `127.0.0.1`, env `CROWDB_SYSMD_IP`) — IP of any
  group-0 replica node.
- `--sysmd-port` (default: group-0 REST port, env `CROWDB_SYSMD_PORT`)
  — port of the group-0 replica's REST endpoint.

The old `--ip`/`--port` flags (which pointed to console-web) are
removed. The CLI resolves the cluster topology, store/group layout,
and server endpoints from group-0 sysdata via `KVClusterMetaClient`
(see `design-crowdb-kv-group0.md`). Bench no longer needs
`ClusterHandle`/`runtime/<name>/handle.json` — it resolves the leader
endpoint from group-0 sysdata the same way as any other CLI command.

**Gap 2 — `kv server delete` semantics: graceful + require empty
(resolved).** All operations use graceful Paxos reconfiguration — no
force-kill path. `delete` requires the server to be **empty**: no
replicas, no groups, no stores hosted. The operator must delete in
bottom-up order — replicas → groups → stores → server → node — before
the server/node can be removed. This is a safety policy for destructive
operations: the CLI refuses `kv server delete` if the server still
hosts replicas, with an error listing the replicas/groups/stores that
must be removed first. Same policy applies to `cluster node remove` —
the node must have no running servers/services before removal.

Verb distinction after this decision:
- `kv server stop` — graceful process stop (keeps server entry, can
  restart later). No reconfiguration; replicas remain registered on
  peers.
- `kv server delete` — graceful removal (requires empty server: no
  replicas/groups/stores hosted). Removes the server entry after
  confirming emptiness. No cascading delete — the operator does the
  cascade manually in bottom-up order.

The "cascading delete + data-loss warning" annotation in the hierarchy
tree is superseded by this decision.

**Gap 3 — `cluster clean` boundary: wipe data, keep services + group-0
(resolved).** `cluster clean` wipes user-layer data across all storage
services, keeping services running and group-0 intact:

- **KV user data** — remove all user stores + groups via the existing
  store/group removal flow (cascades to replicas and on-disk WAL/tree
  cleanup). group-0/store-0 preserved.
- **chunkdb metadata** — chunkdb stores metadata in CROWDB KV (see
  `design-crowdb-chunkdb.md` §1: "all durable state to CROWDB KV").
  Cleaning the chunkdb KV store (same as any KV store removal) wipes
  chunkdb metadata. If chunkdb later writes metadata to a separate
  store, a dedicated clean path is added then.
- **diskio data** — diskio writes at positions it points to; later
  writes overwrite old data. No explicit clean needed — new writes
  supersede old data. (If a diskio clean API is added later, it can
  trim/reset backing devices.)
- **diskdb metadata + backing** — remove all diskdb metadata (clean the
  diskdb group(s) in KV sysdata). For file-simulated disks, trim or
  reset the backing file to reclaim space. For real devices, the
  metadata removal is sufficient (zones are reclaimed on next
  allocation).

Services (`crowdb-kv-server`, `crowdb-diskdb`, `crowdb-chunkdb`,
`crowdb-diskio`) stay running. group-0 leadership continues — leaders
are elected, not assigned; as long as group-0 replicas survive, they
elect a leader. Topology (racks/nodes/disk-groups/disks) is preserved.

### Implementation Gaps (resolved)

The concept hierarchy and its three semantic gaps are resolved. The
following five gaps were implementation-level consequences of those
decisions. All five are now resolved with decisions below.

**Impl Gap 1 — Backward compatibility strategy (resolved: hard
cut-over).** No legacy mode, no deprecation aliases. All scripts, tests,
and docs are rewritten to the new command names based on the latest
concept and design. The regression scripts
(`tools/bench-kv-read-regression.sh`,
`tools/bench-kv-scan-regression.sh`, `tools/bench-rpc-regression.sh`),
E2E tests (`app/crowdb-cli/tests/`), and user guide
(`doc/user-manual/user-guide.md` §7) are updated in the same
implementation commit. Old command names produce a clear error pointing
to the new name (no silent alias resolution).

**Impl Gap 2 — `design-crowdb-console.md` §7 revision (defined: doc
update task, no decision needed).** §7 mandates "Two layers max —
`crowdb-kv <group> <verb>`" (`doc/design/console/
design-crowdb-console.md` L428). The new hierarchy is universally three
layers for resource-typed verbs (`cluster rack add`, `kv server deploy`,
`chunk diskdb scan`). §7 must be revised: replace the two-layer rule
with a three-layer rule (`<domain> <resource> <verb>`), keeping two
layers for direct data-plane verbs (`kv put`, `chunk allocate`). The
verb vocabulary list in §7 must also be updated: `start` → alias of
`restart`, `list` dropped from kv data-plane (use `scan`), `delete`
added to server lifecycle, `set-status` added to disk/disk-group. This
is a doc update, not a code change — direction is clear from the
hierarchy above, no decision needed.

**Impl Gap 3 — `cluster init` chicken-and-egg (resolved: special case).
** With the group-0-direct model (Concept Gaps §1), the CLI connects to
group-0 to resolve cluster info. But `cluster init` **creates** group-0
— there is no group-0 to connect to yet. `cluster init` is a special
case: it takes `--nodes <n1,n2,n3>` directly (not `--sysmd-ip`/
`--sysmd-port`) and bootstraps group-0/store-0 on those nodes via direct
node REST calls (the same mechanism `cluster init` uses today, just
without the console-web intermediary). After `cluster init` completes,
subsequent commands use `--sysmd-ip`/`--sysmd-port` to connect to the
newly created group-0. No alternative approach is viable — this is the
only way to bootstrap without a pre-existing group-0.

**Impl Gap 4 — `kv server deploy` SSH/local-fork lifecycle (resolved:
hybrid config → group-0).** Today, server deploy goes through
console-web, which owns SSH credentials and the local-fork lifecycle
(`design-crowdb-console.md` §5). With console-web removed from the CLI
path, `kv server deploy` performs the SSH/local-fork spawn directly from
`crowdb-console-shared`. SSH credentials follow a two-phase lifecycle:

- **Bootstrap phase** (before group-0 exists) — SSH creds are stored in
  the same TOML config file the UI uses today:
  `runtime-data/crowdb.temp.toml` (via `TomlFileEngine::default_path()`
  in `lib/crowdb-console-shared/src/config.rs` — renamed from
  `crowdb-kv.db.toml`). This file already
  stores rack/node/server/store/group/disk-group/disk entries, with SSH
  creds in `NodeEntry` (`ssh_user`, `ssh_key`, `ssh_password`). The CLI
  uses the same `ConsoleConfig` + `TomlFileEngine` flow as the UI —
  `cluster rack add` / `cluster node add` write to this file, `kv
  server deploy` reads SSH creds from it. The CLI and UI share the same
  config data file layout; no separate CLI-only config file.
- **Steady-state phase** (after group-0 exists) — SSH creds are moved
  into group-0 sysdata, encrypted with a default key. Subsequent `kv
  server deploy` calls read creds from group-0 sysdata via
  `KVClusterMetaClient`. The TOML file is no longer the source of truth
  for SSH creds; group-0 is. The TOML file remains as a local cache /
  bootstrap fallback.

This is option (d) from the original list. The migration from TOML
config to group-0 happens during or after `cluster init` — the design
draft specifies the exact transition point. The CLI's current `--config`
(`-p`) flag (pointing to `~/.crowdb-kv/console.toml`) is replaced by
the shared `runtime-data/crowdb.temp.toml` default path.

**Impl Gap 5 — `cluster reset` self-contradiction (resolved: hybrid —
group-0 discovery + direct node teardown).** `cluster reset` is full
teardown. With console-web no longer a CLI dependency, `cluster reset`
is reimplemented in `crowdb-console-shared` as a hybrid operation:

1. **Discovery** — connect to group-0 (via `--sysmd-ip`/`--sysmd-port`)
   to enumerate all resources: user stores/groups/replicas, diskdb/
   chunkdb/diskio instances, server entries, topology.
2. **Teardown in dependency order** — erase resources one by one: user
   groups → user stores → chunkdb/diskio/diskdb instances → server
   entries (SIGTERM each node's processes). This mirrors the §13
   dependency order but executes from the CLI/shared layer, not
   console-web.
3. **Destroy group-0** — tear down group-0/store-0 itself (last, after
   all user resources are gone).
4. **Delete topology** — remove all nodes and racks from
   `runtime-data/crowdb.temp.toml` (the same TOML config used during
   bootstrap, Impl Gap 4).
5. **Fast path** — if group-0 is not created (e.g. `cluster init` failed
   or was never run), skip steps 1-3 and use the TOML config info
   (rack/node entries from when they were created) to clean up any
   stray processes and clear the config.

This is option (c) from the original list. The §13 `POST /internal/
reset` endpoint in console-web is deprecated for CLI use; the CLI
implements its own teardown. Console-web may retain its own reset for
the UI, but the CLI no longer depends on it.