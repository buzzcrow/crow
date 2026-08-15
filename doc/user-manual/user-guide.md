<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CROW User Guide

CROW is a storage platform — a foundation layer for building storage
systems where you own the hot path all the way down to the metal. Its
first component is **crow-kv**, a distributed key-value cluster built
on Multi-Paxos. This guide covers crow-kv operations: starting a
cluster, performing basic KV operations, managing topology, and
running upgrades.

crow-kv provides three interfaces for cluster management and data
access:

- **Web UI** — the `crow-web` service provides a visual dashboard
  with cluster topology, group health, a KV Operator panel (store/group
  selector, paginated scan, inline CRUD, demo data injection), and
  Swagger UI for browsing the OpenAPI spec of any registered
  `crow-kv-server` instance.
- **CLI** — the `crow-cli` CLI tool is a thin wrapper over the same
  service HTTP API. It talks to a `crow-web` service
  (`--ip` / `--port`, default `127.0.0.1:9920`); the service resolves
  upstream `crow-kv-server` nodes. Use `--json` for machine-readable
  output.
- **RESTful API** — the service HTTP API is the underlying transport
  for both the Web UI and the CLI. All endpoints are documented in
  §7 (API Reference).

The examples below show both CLI and curl for each operation. All
curl examples assume these shell variables are set once:

```bash
IP=127.0.0.1        # crow-web service IP
PORT=9920           # crow-web service port
```

CLI commands omit `--ip`/`--port` for brevity; they default to
`127.0.0.1:9920` (override with `--ip`/`--port` or the
`CROW_KV_IP`/`CROW_KV_PORT` env vars).

### Prerequisites

Before following the steps below:

- **Build the binaries** — `pixi run build` produces `crow-web`,
  `crow-kv-server`, and `crow-cli` in `target/debug/`.
- **Start `crow-web`** — the CLI and Web UI both talk to this service.
  ```bash
  crow-web --port 9920
  ```
  Add `--test-mode` for an in-memory config (no persisted TOML; changes
  are lost on restart).
- **Set `CROW_KV_SERVER_BIN`** — the `server deploy` command spawns
  `crow-kv-server` on each node. It searches for the binary in this
  order: `$CROW_KV_SERVER_BIN`, a sibling of the running `crow-web`
  process, then `$PATH`. Set the env var explicitly if the binary is
  elsewhere:
  ```bash
  export CROW_KV_SERVER_BIN=/path/to/crow-kv-server
  ```
- **Node hosts** — the examples below use `--host 127.0.0.1` for a
  single-machine deployment (all nodes on localhost). For a real
  multi-node cluster, use each machine's reachable hostname or IP.

---

## 1. Quick Start: Bootstrap a 3-Node Cluster

### 1.1 Register the physical topology

Create a rack, add nodes, and deploy a server on each node. The
`deploy` command starts `crow-kv-server` on the target node (via SSH
if `ssh_user` is set, or as a local subprocess otherwise) — no manual
start needed.

**CLI:**

```bash
# Create a rack
crow-cli rack add --id r1 --name "rack-one"

# Register each node (repeat for n2, n3)
crow-cli node add --id n1 --rack r1 --host 127.0.0.1

# Deploy a crow-kv-server process on each node (repeat for n2, n3)
crow-cli server deploy --node n1 --rest-port 2001 --rpc-port 20001
```

**curl:**

```bash
# Create a rack
curl -X POST "http://$IP:$PORT/api/racks" -H 'Content-Type: application/json' \
  -d '{"id":"r1"}'

# Register each node (repeat for n2, n3)
curl -X POST "http://$IP:$PORT/api/nodes" -H 'Content-Type: application/json' \
  -d '{"id":"n1","rack_id":"r1","host":"127.0.0.1","ssh_port":22,"ssh_user":""}'

# Deploy a crow-kv-server process on each node (repeat for n2, n3)
curl -X POST "http://$IP:$PORT/api/nodes/n1/server/deploy" \
  -H 'Content-Type: application/json' \
  -d '{"rest_port":2001,"rpc_port":20001}'
```

### 1.2 Initialize the cluster

Before creating data stores or groups, the cluster must be
initialized. This creates the system group (store 0, group 0) which
stores cluster topology metadata as KV entries, providing HA for
the topology itself.

**CLI:**

```bash
# Initialize with all deployed nodes
crow-cli cluster init --nodes n1,n2,n3
```

**curl:**

```bash
curl -X POST "http://$IP:$PORT/api/cluster/init" \
  -H 'Content-Type: application/json' \
  -d '{"nodes":["n1","n2","n3"]}'
```

This creates store 0 and group 0 on each selected node, wires remotes
for multi-node, persists topology in console config, and writes
hardware hierarchy + KV-cluster topology into group 0 via
`HardwareClient` + `KVClusterMetaClient` (text-path keys, JSON
values). After initialization, data store/group creation is unblocked.

For a single-node dev cluster, pass one node:

```bash
crow-cli cluster init --nodes n1
```

### 1.3 Create a store and group

A store is the logical container that owns one or more groups.

**CLI:**

```bash
# Create a store on n1
crow-cli store add --store-id 3 --nodes n1

# Create a group with an initial replica on n1
crow-cli paxos add \
  --store-id 3 --group-id 3 --replica-id 1 --nodes n1
```

**curl:**

```bash
curl -X POST "http://$IP:$PORT/api/stores" -H 'Content-Type: application/json' \
  -d '{"store_id":3,"nodes":["n1"]}'

curl -X POST "http://$IP:$PORT/api/stores/3/groups" -H 'Content-Type: application/json' \
  -d '{"group_id":3,"replica_id":1,"nodes":["n1"]}'
```

If the cluster has not been initialized, store/group creation returns
`409 Conflict` with a message directing you to run `cluster init` first.

### 1.4 Add the remaining replicas

**CLI:**

```bash
crow-cli replica add \
  --store-id 3 --group-id 3 --node n2 --replica-id 2

crow-cli replica add \
  --store-id 3 --group-id 3 --node n3 --replica-id 3
```

**curl:**

```bash
curl -X POST "http://$IP:$PORT/api/stores/3/groups/3/replicas" \
  -H 'Content-Type: application/json' \
  -d '{"node_id":"n2","replica_id":2}'

curl -X POST "http://$IP:$PORT/api/stores/3/groups/3/replicas" \
  -H 'Content-Type: application/json' \
  -d '{"node_id":"n3","replica_id":3}'
```

The service orchestrates the full add-replica flow: creates the local
group on the target node, wires remotes bidirectionally, and the new
replica catches up via snapshot streaming before joining the voting set.

### 1.5 Verify and smoke test

**CLI:**

```bash
# Check group health
crow-cli paxos inspect --store-id 3 --group-id 3
# Look for "leader=" and replica states

# Put / Get
crow-cli kv put --store-id 3 --group-id 3 \
  --key hello --value world

crow-cli kv get --store-id 3 --group-id 3 --key hello
```

**curl:**

```bash
curl "http://$IP:$PORT/api/stores/3/groups/3"

curl -X POST "http://$IP:$PORT/api/stores/3/groups/3/kv/put" \
  -H 'Content-Type: application/json' \
  -d '{"key":"hello","value":"world"}'

curl "http://$IP:$PORT/api/stores/3/groups/3/kv/get?key=hello"
```

---

## 2. KV Operations

All KV operations target a specific `(store_id, group_id)`.

**CLI:**

```bash
# Put
crow-cli kv put --store-id 3 --group-id 3 --key user:1 --value alice

# Get
crow-cli kv get --store-id 3 --group-id 3 --key user:1

# Delete
crow-cli kv delete --store-id 3 --group-id 3 --key user:1

# Prefix scan (list mode — fast, latest values, S3-list semantics)
crow-cli kv scan --store-id 3 --group-id 3 --prefix user: --limit 100
```

**curl:**

```bash
curl -X POST "http://$IP:$PORT/api/stores/3/groups/3/kv/put" \
  -H 'Content-Type: application/json' \
  -d '{"key":"user:1","value":"alice"}'

curl "http://$IP:$PORT/api/stores/3/groups/3/kv/get?key=user:1"

curl -X POST "http://$IP:$PORT/api/stores/3/groups/3/kv/delete" \
  -H 'Content-Type: application/json' \
  -d '{"key":"user:1"}'

curl "http://$IP:$PORT/api/stores/3/groups/3/kv/scan?prefix=user:&limit=100"
```

The Web UI KV Operator panel provides the same operations with a
store/group selector, paginated scan, and inline editing.

### 2.1 Scan modes

CROW provides two range-read modes for different use cases:

- **List scan** (`kv scan`) — the default scan. Fast, always returns the
  latest value per key at each page's read point. S3-list semantics:
  each page is independently consistent, but a key can vanish (deleted
  between pages) or a value can drift (overwritten between pages) within
  a single logical scan. No server-side state beyond the per-page read
  barrier. Use for interactive listing, key discovery, and the KV
  Operator UI.
- **Snapshot scan** (`snapshot create` + `snapshot scan`) —
  point-in-time-consistent. Pins a frozen view of the keyspace at a
  specific slot; every page is served from the same frozen view. No key
  vanishes, no value drifts, no phantom keys appear. Use for backup,
  analytics, and any consumer that needs a consistent point-in-time
  view. See §2.2 below.

### 2.2 Snapshot versioning

A snapshot scan pins a point-in-time view of the keyspace. Creating a
snapshot flushes the in-memory write buffer (L0) into the durable tree
(L1), then pins L1 at the current applied slot. The snapshot is a frozen,
immutable view — iterating it is pure array traversal with no concurrency
concerns. Each snapshot has a server-side handle with a lease (default 5
minutes); the handle is reaped if the client disconnects, preventing
unbounded pin retention.

**Create a snapshot:**

```bash
crow-cli snapshot create --store-id 3 --group-id 3
# Returns: snapshot_handle=42, at_slot=12345
```

**List active snapshots:**

```bash
crow-cli snapshot list --store-id 3 --group-id 3
# Returns: handle, at_slot, lease_remaining for each active snapshot
```

**Scan a snapshot (paginated, same prefix/start_after/limit as list scan):**

```bash
# First page
crow-cli snapshot scan --store-id 3 --group-id 3 \
  --handle 42 --prefix user: --limit 100

# Next page (start_after = last key from previous page)
crow-cli snapshot scan --store-id 3 --group-id 3 \
  --handle 42 --prefix user: --limit 100 \
  --start-after user:50
```

**Release a snapshot (free the pinned pages):**

```bash
crow-cli snapshot release --store-id 3 --group-id 3 --handle 42
```

**curl:**

```bash
# Create
curl -X POST "http://$IP:$PORT/api/stores/3/groups/3/snapshots"

# List
curl "http://$IP:$PORT/api/stores/3/groups/3/snapshots"

# Scan
curl "http://$IP:$PORT/api/stores/3/groups/3/snapshots/42/scan?prefix=user:&limit=100"

# Release
curl -X DELETE "http://$IP:$PORT/api/stores/3/groups/3/snapshots/42"
```

**GC and snapshots**: the engine's garbage collector reclaims tombstones
and stale versions with `slot <= gc_watermark`. Active snapshots protect
their pinned pages via refcount — GC never frees a page a live snapshot
still references. Once a snapshot is released (or its lease expires), the
next GC sweep can reclaim those pages. The GC watermark can be advanced
explicitly via the management API to control retention:

```bash
# Advance GC watermark (data with slot <= watermark becomes reclaimable)
curl -X POST "http://$IP:$PORT/api/stores/3/groups/3/gc-watermark" \
  -H 'Content-Type: application/json' \
  -d '{"slot":12000}'
```

---

## 3. Cluster Management

### 3.1 Check cluster health

**CLI:**

```bash
# High-level summary (servers + store/group counts)
crow-cli cluster status

# Full topology (logical stores/groups/replicas + physical nodes/servers)
crow-cli cluster topology

# Inspect a specific store, group, or node
crow-cli cluster inspect s3          # store 3
crow-cli cluster inspect s3/g3       # group 3 in store 3
crow-cli cluster inspect n1          # node n1
```

**curl:**

```bash
# All nodes
curl "http://$IP:$PORT/api/nodes"

# All deployed servers
curl "http://$IP:$PORT/api/servers"

# A specific group
curl "http://$IP:$PORT/api/stores/3/groups/3"
# healthy: all replicas up, leader known
# degraded: some replicas down, quorum + leader available
# unavailable: quorum lost
```

### 3.2 Add a read replica

**CLI:**

```bash
crow-cli replica add --store-id 3 --group-id 3 --node n4 --replica-id 4
```

**curl:**

```bash
curl -X POST "http://$IP:$PORT/api/stores/3/groups/3/replicas" \
  -H 'Content-Type: application/json' \
  -d '{"node_id":"n4","replica_id":4}'
```

The new replica streams a snapshot from the leader, catches up, then
joins the voting set automatically.

### 3.3 Remove a replica

**CLI:**

```bash
crow-cli replica remove --store-id 3 --group-id 3 --replica-id 3
```

**curl:**

```bash
curl -X DELETE "http://$IP:$PORT/api/stores/3/groups/3/replicas/3"
```

If the target is the leader, the service asks it to step down first,
waits for a new leader, then removes the replica.

### 3.4 Replace a failed node

1. Provision the new machine with the same node ID, management port,
   and gRPC port.
2. Deploy the server via the service. The server auto-loads its
   store/group configuration from `conf/node-config.json` on startup —
   no `--stores`/`--groups` CLI args needed for normal restart:

   **CLI:**

   ```bash
   crow-cli server deploy --node n1 --rest-port 2001 --rpc-port 20001
   ```

   **curl:**

   ```bash
   curl -X POST "http://$IP:$PORT/api/nodes/n1/server/deploy" \
     -H 'Content-Type: application/json' \
     -d '{"rest_port":2001,"rpc_port":20001}'
   ```

   If `node-config.json` is lost, fall back to explicit bootstrap args
   by starting `crow-kv-server` manually with `--stores`/`--groups`/
   `--replica`:

   ```bash
   crow-kv-server \
     --management-addr 0.0.0.0 --management-port 2001 \
     --ports 20001 --election-profile default \
     --stores 3 --groups 3 --replica 1
   ```

3. Verify group health.

If the WAL and config directory were also lost, add the replacement as
a new replica with a new replica ID instead of reusing the old one.

---

## 4. Rolling Upgrade

Upgrade one node at a time. Wait for each node to rejoin and catch up
before moving to the next.

For each node:

1. **Stop:**

   **CLI:**

   ```bash
   crow-cli server stop --node n1
   ```

   **curl:**

   ```bash
   curl -X POST "http://$IP:$PORT/api/nodes/n1/server/stop"
   ```

2. **Install the new binary** on the node.

3. **Restart the server.** The server auto-loads its store/group
   configuration from `conf/node-config.json` on startup:

   **CLI:**

   ```bash
   crow-cli server restart --node n1
   ```

   **curl:**

   ```bash
   curl -X POST "http://$IP:$PORT/api/nodes/n1/server/restart"
   ```

   If `node-config.json` is missing, start `crow-kv-server` manually
   with explicit args:

   ```bash
   crow-kv-server \
     --management-addr 0.0.0.0 --management-port 2001 \
     --ports 20001 --election-profile default \
     --stores 3 --groups 3 --replica 1
   ```

   `--stores`/`--groups` tells the server to reopen the WAL and rejoin
   as a full member. `--replica` must match the assigned replica ID.

4. **Wait for healthy:**

   ```bash
   crow-cli cluster status
   crow-cli paxos inspect --store-id 3 --group-id 3
   ```

5. **Smoke test:**

   ```bash
   crow-cli kv get --store-id 3 --group-id 3 --key hello
   ```

6. Move to the next node.

**What to watch:** after stopping a node, the remaining nodes elect a
new leader. Wait for the group view to show a leader before proceeding.
A brief latency spike during leader transition is normal.

---

## 5. Emergency: Loss of Quorum

If two of three nodes fail, the remaining node cannot elect itself
leader. Writes and linearizable reads block.

- **Restore the failed nodes** from backups and restart. The server
  auto-loads from `conf/node-config.json`; if the config is lost, fall
  back to `--stores`/`--groups`/`--replica` args. This is always the
  safest path.
- **Recover with data loss** (last resort): force the surviving node to
  become leader by manually truncating the log. Only safe when the
  other nodes are permanently lost.

Do not add a new node to a quorum-less group without first recovering
leadership.

---

## 6. Backup

CROW durability comes from the per-store WAL (`--wal-root`), the
per-node config cache (`--config-root`), and the durable KV engine
(`--data-root`). For disaster recovery, back up:

- `{wal-root}/store{store_id}/` for each store
- `{config-root}/node-config.json` — per-node store/group config cache
- `{data-root}/store{store_id}/group{group_id}/` if using crow-tree
  durable KV engine

Restore by placing these on the replacement node and starting the
server. With `node-config.json` present, no `--stores`/`--groups`
bootstrap args are needed. If the config is lost, use explicit
`--stores`/`--groups`/`--replica` args to recover from WAL.

---

## 7. API Reference

**CLI:**

The `crow-cli` CLI groups commands by resource type. All commands accept
`--ip <addr>` (default `127.0.0.1`), `--port <port>` (default `9920`),
and `--json` for JSON output.

- **`crow-cli cluster status`** — servers + store/group summary
- **`crow-cli cluster topology`** — full logical + physical hierarchy
- **`crow-cli cluster inspect <id>`** — `s<sid>`, `s<sid>/g<gid>`,
  `s<sid>/g<gid>/r<rid>`, or `<node-id>`
- **`crow-cli cluster init --nodes n1,n2,...`** — initialize cluster (system group)
- **`crow-cli rack add --id <id> [--name <name>]`**
- **`crow-cli rack remove --id <id>`**
- **`crow-cli rack list`**
- **`crow-cli node add --id <id> --rack <rack> [--host <host>] [--ssh-user <user>]`**
- **`crow-cli node remove --id <id>`**
- **`crow-cli node list`**
- **`crow-cli node ping <node>`**
- **`crow-cli server deploy --node <id> --rest-port <p> --rpc-port <p>`**
- **`crow-cli server restart --node <id>`**
- **`crow-cli server stop --node <id>`**
- **`crow-cli server list`**
- **`crow-cli store add --store-id <id> [--nodes n1,n2,...]`**
- **`crow-cli store remove --store-id <id>`**
- **`crow-cli store list`**
- **`crow-cli store inspect --store-id <id>`**
- **`crow-cli paxos add --store-id <s> --group-id <g> --replica-id <r> --nodes n1,n2,...`**
- **`crow-cli paxos remove --store-id <s> --group-id <g>`**
- **`crow-cli paxos list --store-id <s>`**
- **`crow-cli paxos inspect --store-id <s> --group-id <g>`**
- **`crow-cli replica add --store-id <s> --group-id <g> --node <n> [--replica-id <r>]`**
- **`crow-cli replica remove --store-id <s> --group-id <g> --replica-id <r>`**
- **`crow-cli kv put --store-id <s> --group-id <g> --key <k> --value <v>`**
- **`crow-cli kv get --store-id <s> --group-id <g> --key <k>`**
- **`crow-cli kv delete --store-id <s> --group-id <g> --key <k>`**
- **`crow-cli kv scan --store-id <s> --group-id <g> --prefix <p> [--limit <n>]`** — list scan (fast, latest values, S3-list semantics)
- **`crow-cli snapshot create --store-id <s> --group-id <g>`** — pin a point-in-time snapshot
- **`crow-cli snapshot list --store-id <s> --group-id <g>`** — list active snapshots
- **`crow-cli snapshot scan --store-id <s> --group-id <g> --handle <h> --prefix <p> [--limit <n>] [--start-after <k>]`** — scan a pinned snapshot
- **`crow-cli snapshot release --store-id <s> --group-id <g> --handle <h>`** — release a snapshot

**curl:**

#### Cluster lifecycle

| Operation | Endpoint |
| --- | --- |
| Initialize cluster | `POST /api/cluster/init` |

#### Physical topology

| Operation | Endpoint |
| --- | --- |
| List racks | `GET /api/racks` |
| Create rack | `POST /api/racks` |
| Delete rack | `DELETE /api/racks/{rack_id}` |
| List nodes | `GET /api/nodes` |
| Add node | `POST /api/nodes` |
| Get node | `GET /api/nodes/{id}` |
| Remove node | `DELETE /api/nodes/{id}` |
| Ping node | `POST /api/nodes/{id}/ping` |
| Get server info | `GET /api/nodes/{id}/server` |
| Deploy server | `POST /api/nodes/{id}/server/deploy` |
| Restart server | `POST /api/nodes/{id}/server/restart` |
| Stop server | `POST /api/nodes/{id}/server/stop` |

#### Logical topology (stores and groups)

| Operation | Endpoint |
| --- | --- |
| List stores | `GET /api/stores` |
| Create store | `POST /api/stores` |
| Get store | `GET /api/stores/{sid}` |
| Remove store | `DELETE /api/stores/{sid}` |
| List groups | `GET /api/stores/{sid}/groups` |
| Create group | `POST /api/stores/{sid}/groups` |
| Get group view | `GET /api/stores/{sid}/groups/{gid}` |
| Remove group | `DELETE /api/stores/{sid}/groups/{gid}` |
| List replicas | `GET /api/stores/{sid}/groups/{gid}/replicas` |
| Add replica | `POST /api/stores/{sid}/groups/{gid}/replicas` |
| Get replica | `GET /api/stores/{sid}/groups/{gid}/replicas/{rid}` |
| Remove replica | `DELETE /api/stores/{sid}/groups/{gid}/replicas/{rid}` |
| Resolve leader endpoint | `GET /api/stores/{sid}/groups/{gid}/endpoint` |

#### KV data plane

| Operation | Endpoint |
| --- | --- |
| Get | `GET /api/stores/{sid}/groups/{gid}/kv/get?key=...` |
| Put | `POST /api/stores/{sid}/groups/{gid}/kv/put` |
| Delete | `POST /api/stores/{sid}/groups/{gid}/kv/delete` |
| Scan (list mode) | `GET /api/stores/{sid}/groups/{gid}/kv/scan?prefix=...&limit=N` |
| Create snapshot | `POST /api/stores/{sid}/groups/{gid}/snapshots` |
| List snapshots | `GET /api/stores/{sid}/groups/{gid}/snapshots` |
| Snapshot scan | `GET /api/stores/{sid}/groups/{gid}/snapshots/{handle}/scan?prefix=...&limit=N&start_after=...` |
| Release snapshot | `DELETE /api/stores/{sid}/groups/{gid}/snapshots/{handle}` |
| Set GC watermark | `POST /api/stores/{sid}/groups/{gid}/gc-watermark` |

#### Server management (per-node, internal)

| Operation | Endpoint |
| --- | --- |
| System init (bootstrap group 0) | `POST /system/init` |
| Add store | `POST /stores` |
| Remove store | `DELETE /stores/{sid}` |
| Add group | `POST /stores/{sid}/groups` |
| Remove group | `DELETE /stores/{sid}/groups/{gid}` |
| Add remote replicas | `POST /stores/{sid}/groups/{gid}/remotes` |
| Step down leader | `POST /stores/{sid}/groups/{gid}/step-down` |
| Export topology | `GET /topology` |
| Health check | `GET /health` |
| Metrics | `GET /metrics` |

These endpoints are on the `crow-kv-server` management API (internal —
only called by `crow-kv-client`'s `KVClusterAdmin`). The console's
`POST /api/cluster/init` orchestrates
`/system/init` across nodes and auto-finalizes.
