<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CrowKV User Guide

This guide walks through starting a cluster, performing basic KV
operations, managing topology, and running upgrades.

CrowKV provides three interfaces for cluster management and data
access:

- **Web UI** — the `crowkv-web` service provides a visual dashboard
  with cluster topology, group health, a KV Operator panel (store/group
  selector, paginated scan, inline CRUD, demo data injection), and
  Swagger UI for browsing the OpenAPI spec of any registered
  `crowkv-server` instance.
- **CLI** — the `crowkv-cli` CLI tool is a thin wrapper over the same
  service HTTP API. It talks to a `crowkv-web` service
  (`--ip` / `--port`, default `127.0.0.1:9920`); the service resolves
  upstream `crowkv-server` nodes. Use `--json` for machine-readable
  output.
- **RESTful API** — the service HTTP API is the underlying transport
  for both the Web UI and the CLI. All endpoints are documented in
  §7 (API Reference).

The examples below show both CLI and curl for each operation. All
examples assume these shell variables are set once:

```bash
IP=127.0.0.1        # crowkv-web service IP
PORT=9920           # crowkv-web service port
```

---

## 1. Quick Start: Bootstrap a 3-Node Cluster

### 1.1 Start the servers

On each node, run `crowkv-server`. The first start is empty (no
`--stores`/`--groups`); the service creates the topology later.

```bash
# On n1
crowkv-server \
  --management-addr 0.0.0.0 --management-port 2001 \
  --ports 20001 --election-profile default

# On n2
crowkv-server \
  --management-addr 0.0.0.0 --management-port 2002 \
  --ports 20002 --election-profile default

# On n3
crowkv-server \
  --management-addr 0.0.0.0 --management-port 2003 \
  --ports 20003 --election-profile default
```

### 1.2 Register the physical topology

Create a rack, add nodes, and deploy a server on each node.

**CLI:**

```bash
# Create a rack
crowkv-cli --ip $IP --port $PORT rack add --id r1 --name "rack-one"

# Register each node (repeat for n2, n3)
crowkv-cli --ip $IP --port $PORT node add --id n1 --rack r1 --host n1.example.com

# Deploy a crowkv-server process on each node (repeat for n2, n3)
crowkv-cli --ip $IP --port $PORT server deploy \
  --node n1 --mgmt-port 2001 --grpc-port 20001
```

**curl:**

```bash
# Create a rack
curl -X POST "http://$IP:$PORT/api/racks" -H 'Content-Type: application/json' \
  -d '{"id":"r1"}'

# Register each node (repeat for n2, n3)
curl -X POST "http://$IP:$PORT/api/nodes" -H 'Content-Type: application/json' \
  -d '{"id":"n1","rack_id":"r1","host":"n1.example.com","ssh_port":22,"ssh_user":""}'

# Deploy a crowkv-server process on each node (repeat for n2, n3)
curl -X POST "http://$IP:$PORT/api/nodes/n1/server/deploy" \
  -H 'Content-Type: application/json' \
  -d '{"mgmt_port":2001,"grpc_port":20001}'
```

The `deploy` command spawns `crowkv-server` on the node (via SSH if
`ssh_user` is set, or as a local subprocess otherwise).

### 1.3 Initialize the cluster

Before creating data stores or groups, the cluster must be
initialized. This creates the system group (store 0, group 0) which
stores cluster topology metadata as KV entries, providing HA for
the topology itself.

**CLI:**

```bash
# Initialize with all deployed nodes
crowkv-cli --ip $IP --port $PORT cluster init --nodes n1,n2,n3
```

**curl:**

```bash
curl -X POST "http://$IP:$PORT/api/cluster/init" \
  -H 'Content-Type: application/json' \
  -d '{"nodes":["n1","n2","n3"]}'
```

This creates store 0 and group 0 on each selected node, wires remotes
for multi-node, and automatically finalizes the cutover (sets the
`/topology/ready` flag in group 0). After initialization, data
store/group creation is unblocked.

For a single-node dev cluster, pass one node:

```bash
crowkv-cli --ip $IP --port $PORT cluster init --nodes n1
```

### 1.4 Create a store and group

A store is the logical container that owns one or more groups.

**CLI:**

```bash
# Create a store on n1
crowkv-cli --ip $IP --port $PORT store add --store-id 3 --nodes n1

# Create a group with an initial replica on n1
crowkv-cli --ip $IP --port $PORT paxos add \
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

### 1.5 Add the remaining replicas

**CLI:**

```bash
crowkv-cli --ip $IP --port $PORT replica add \
  --store-id 3 --group-id 3 --node n2 --replica-id 2

crowkv-cli --ip $IP --port $PORT replica add \
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

### 1.6 Verify and smoke test

**CLI:**

```bash
# Check group health
crowkv-cli --ip $IP --port $PORT paxos inspect --store-id 3 --group-id 3
# Look for "leader=" and replica states

# Put / Get
crowkv-cli --ip $IP --port $PORT kv put --store-id 3 --group-id 3 \
  --key hello --value world

crowkv-cli --ip $IP --port $PORT kv get --store-id 3 --group-id 3 --key hello
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
crowkv-cli kv put --store-id 3 --group-id 3 --key user:1 --value alice

# Get
crowkv-cli kv get --store-id 3 --group-id 3 --key user:1

# Delete
crowkv-cli kv delete --store-id 3 --group-id 3 --key user:1

# Prefix scan
crowkv-cli kv scan --store-id 3 --group-id 3 --prefix user: --limit 100
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

curl "http://$IP:$PORT/api/stores/3/groups/3/kv/scan?key=user:"
```

The Web UI KV Operator panel provides the same operations with a
store/group selector, paginated scan, and inline editing.

---

## 3. Cluster Management

### 3.1 Check cluster health

**CLI:**

```bash
# High-level summary (servers + store/group counts)
crowkv-cli cluster status

# Full topology (logical stores/groups/replicas + physical nodes/servers)
crowkv-cli cluster topology

# Inspect a specific store, group, or node
crowkv-cli cluster inspect s3          # store 3
crowkv-cli cluster inspect s3/g3       # group 3 in store 3
crowkv-cli cluster inspect n1          # node n1
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
crowkv-cli replica add --store-id 3 --group-id 3 --node n4 --replica-id 4
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
crowkv-cli replica remove --store-id 3 --group-id 3 --replica-id 3
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
2. Deploy the server via the service:

   **CLI:**

   ```bash
   crowkv-cli server deploy --node n1 --mgmt-port 2001 --grpc-port 20001
   ```

   **curl:**

   ```bash
   curl -X POST "http://$IP:$PORT/api/nodes/n1/server/deploy" \
     -H 'Content-Type: application/json' \
     -d '{"mgmt_port":2001,"grpc_port":20001}'
   ```

3. Start the server. With R2, the server auto-loads its store/group
   configuration from `conf/node-config.json` — no `--stores`/`--groups`
   CLI args needed for normal restart:

   ```bash
   crowkv-server \
     --management-addr 0.0.0.0 --management-port 2001 \
     --ports 20001 --election-profile default
   ```

   If `node-config.json` is lost, fall back to explicit bootstrap args:

   ```bash
   crowkv-server \
     --management-addr 0.0.0.0 --management-port 2001 \
     --ports 20001 --election-profile default \
     --stores 3 --groups 3 --replica 1
   ```

4. Verify group health.

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
   crowkv-cli server stop --node n1
   ```

   **curl:**

   ```bash
   curl -X POST "http://$IP:$PORT/api/nodes/n1/server/stop"
   ```

2. **Install the new binary** on the node.

3. **Restart the server.** With R2, the server auto-loads its
   store/group configuration from `conf/node-config.json`:

   ```bash
   crowkv-server \
     --management-addr 0.0.0.0 --management-port 2001 \
     --ports 20001 --election-profile default
   ```

   If `node-config.json` is missing, fall back to explicit args:

   ```bash
   crowkv-server \
     --management-addr 0.0.0.0 --management-port 2001 \
     --ports 20001 --election-profile default \
     --stores 3 --groups 3 --replica 1
   ```

   `--stores`/`--groups` tells the server to reopen the WAL and rejoin
   as a full member. `--replica` must match the assigned replica ID.

4. **Wait for healthy:**

   ```bash
   crowkv-cli cluster status
   crowkv-cli paxos inspect --store-id 3 --group-id 3
   ```

5. **Smoke test:**

   ```bash
   crowkv-cli kv get --store-id 3 --group-id 3 --key hello
   ```

6. Move to the next node.

**What to watch:** after stopping a node, the remaining nodes elect a
new leader. Wait for the group view to show a leader before proceeding.
A brief latency spike during leader transition is normal.

---

## 5. Emergency: Loss of Quorum

If two of three nodes fail, the remaining node cannot elect itself
leader. Writes and linearizable reads block.

- **Restore the failed nodes** from backups and restart. With R2,
  the server auto-loads from `conf/node-config.json`; if the config is
  lost, fall back to `--stores`/`--groups`/`--replica` args. This is
  always the safest path.
- **Recover with data loss** (last resort): force the surviving node to
  become leader by manually truncating the log. Only safe when the
  other nodes are permanently lost.

Do not add a new node to a quorum-less group without first recovering
leadership.

---

## 6. Backup

CrowKV durability comes from the per-store WAL (`--wal-root`), the
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

The `crowkv-cli` CLI groups commands by resource type. All commands accept
`--ip <addr>` (default `127.0.0.1`), `--port <port>` (default `9920`),
and `--json` for JSON output.

- **`crowkv-cli cluster status`** — servers + store/group summary
- **`crowkv-cli cluster topology`** — full logical + physical hierarchy
- **`crowkv-cli cluster inspect <id>`** — `s<sid>`, `s<sid>/g<gid>`,
  `s<sid>/g<gid>/r<rid>`, or `<node-id>`
- **`crowkv-cli rack add --id <id> [--name <name>]`**
- **`crowkv-cli rack remove --id <id>`**
- **`crowkv-cli rack list`**
- **`crowkv-cli node add --id <id> --rack <rack> [--host <host>] [--ssh-user <user>]`**
- **`crowkv-cli node remove --id <id>`**
- **`crowkv-cli node list`**
- **`crowkv-cli node ping <node>`**
- **`crowkv-cli server deploy --node <id> --mgmt-port <p> --grpc-port <p>`**
- **`crowkv-cli server restart --node <id>`**
- **`crowkv-cli server stop --node <id>`**
- **`crowkv-cli server list`**
- **`crowkv-cli cluster init --nodes n1,n2,...`** — initialize cluster (system group)
- **`crowkv-cli store add --store-id <id> [--nodes n1,n2,...]`**
- **`crowkv-cli store remove --store-id <id>`**
- **`crowkv-cli store list`**
- **`crowkv-cli store inspect --store-id <id>`**
- **`crowkv-cli paxos add --store-id <s> --group-id <g> --replica-id <r> --nodes n1,n2,...`**
- **`crowkv-cli paxos remove --store-id <s> --group-id <g>`**
- **`crowkv-cli paxos list --store-id <s>`**
- **`crowkv-cli paxos inspect --store-id <s> --group-id <g>`**
- **`crowkv-cli replica add --store-id <s> --group-id <g> --node <n> [--replica-id <r>]`**
- **`crowkv-cli replica remove --store-id <s> --group-id <g> --replica-id <r>`**
- **`crowkv-cli kv put --store-id <s> --group-id <g> --key <k> --value <v>`**
- **`crowkv-cli kv get --store-id <s> --group-id <g> --key <k>`**
- **`crowkv-cli kv delete --store-id <s> --group-id <g> --key <k>`**
- **`crowkv-cli kv scan --store-id <s> --group-id <g> --prefix <p> [--limit <n>]`**

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
| Scan | `GET /api/stores/{sid}/groups/{gid}/kv/scan?key=...` |

#### Server management (per-node)

| Operation | Endpoint |
| --- | --- |
| System init (bootstrap group 0) | `POST /system/init` |
| Topology finalize (cutover) | `POST /topology/finalize` |
| Check topology ready | `GET /topology/ready` |

These endpoints are on the `crowkv-server` management API (not the
console web API). The console's `POST /api/cluster/init` orchestrates
`/system/init` across nodes and auto-finalizes.
