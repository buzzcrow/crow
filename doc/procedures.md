<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CrowKV Operational Procedures

This doc describes common day-to-day and infrequent operational procedures for a
standard CrowKV cluster. It uses `crowkv-server` and the `crowkv-console` HTTP
API as concrete examples. For design rationale, see the `design/design-*.md`
files; for the reconfiguration safety model, see
`design/design-reconfiguration.md`.

---

## 1. Prerequisites and conventions

- **Nodes**: three physical or virtual machines `n1`, `n2`, `n3` in the same rack
  (or with rack-aware placement if you have more than one rack).
- **Binary**: `crowkv-server` is installed at the same path on every node.
- **Console**: a `crowkv-web` console is reachable at `http://<console>:<port>`.
- **Store/Group IDs**: examples use store `3` and group `3`. IDs are arbitrary
  but must be unique within the cluster.
- **Replica IDs**: in the examples below, `n1` is replica `1`, `n2` is replica
  `2`, and `n3` is replica `3`. Replica IDs must be unique within a group.
- **Ports**: each node needs one management HTTP port and one gRPC port for
  the KV store. Examples use `2001/20001` for `n1`, `2002/20002` for `n2`,
  `2003/20003` for `n3`.

All examples use `curl` against the console HTTP API. The same operations are
available through the console web UI.

---

## 2. Bootstrap a fresh 3-node cluster

### 2.1 Start the servers

On each node, run `crowkv-server`. The first start of a new node is empty
(no `--stores`/`--groups`); the console creates the topology later.

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

### 2.2 Register the physical topology in the console

Create a rack, add nodes, and deploy a server on each node:

```bash
CONSOLE=http://console.example.com:8080

# Create a rack
curl -X POST "$CONSOLE/api/racks" -H 'Content-Type: application/json' \
  -d '{"id":"r1"}'

# Register each node (repeat for n2, n3)
curl -X POST "$CONSOLE/api/nodes" -H 'Content-Type: application/json' \
  -d '{"id":"n1","rack_id":"r1","host":"n1.example.com","ssh_port":22,"ssh_user":""}'

# Deploy a crowkv-server process on each node (repeat for n2, n3)
curl -X POST "$CONSOLE/api/nodes/n1/server/deploy" \
  -H 'Content-Type: application/json' \
  -d '{"mgmt_port":2001,"grpc_port":20001}'
```

The `deploy` endpoint spawns `crowkv-server` on the node's host (via SSH if
`ssh_user` is set, or as a local subprocess otherwise) and registers its
management URL in the console config.

### 2.3 Create the store

A store is the logical container that owns one or more groups. The console
fans out the store creation to the listed nodes:

```bash
curl -X POST "$CONSOLE/api/stores" -H 'Content-Type: application/json' \
  -d '{"store_id":3,"nodes":["n1"]}'
```

If `nodes` is omitted, the console picks the first node with a running server.

### 2.4 Create the group

Create a group with an initial replica on `n1`. The `nodes` array lists all
nodes that should host a replica; the console wires the remote-replica entries
between them:

```bash
curl -X POST "$CONSOLE/api/stores/3/groups" -H 'Content-Type: application/json' \
  -d '{"group_id":3,"replica_id":1,"nodes":["n1"]}'
```

For a single-node initial group, only `n1` gets a replica. The remaining
replicas are added in the next step.

### 2.5 Add the remaining replicas

```bash
curl -X POST "$CONSOLE/api/stores/3/groups/3/replicas" \
  -H 'Content-Type: application/json' \
  -d '{"node_id":"n2","replica_id":2}'

curl -X POST "$CONSOLE/api/stores/3/groups/3/replicas" \
  -H 'Content-Type: application/json' \
  -d '{"node_id":"n3","replica_id":3}'
```

The console orchestrates the full add-replica flow: creates a local group on
the target node, wires the new replica as a remote on every existing peer,
wires every existing peer as a remote on the new replica, and refreshes the
monitor cache. The new replica catches up via snapshot streaming and then
joins the voting set.

### 2.6 Verify the group is healthy

```bash
curl "$CONSOLE/api/stores/3/groups/3"
```

Look for `"state": "healthy"` and exactly one replica with `"role": "leader"`.

### 2.7 Smoke test

```bash
curl -X POST "$CONSOLE/api/stores/3/groups/3/kv/put" \
  -H 'Content-Type: application/json' \
  -d '{"key":"hello","value":"world"}'

curl "$CONSOLE/api/stores/3/groups/3/kv/get?key=hello"
```

---

## 3. Rolling upgrade

This is the recommended way to upgrade `crowkv-server` binaries (for example,
security patches or minor releases) without cluster downtime. The console
routes KV traffic away from stopped nodes and to the current leader.

**Compatibility rule**: only upgrade one node at a time. Wait for the upgraded
node to rejoin and catch up before moving to the next. Upgrades that change the
Paxos wire format or `membership_epoch` semantics require explicit version-compat
validation; do not roll those out blindly.

### 3.1 Procedure

For each node in the cluster:

1. **Stop the node gracefully.**

   ```bash
   ssh n1.example.com sudo systemctl stop crowkv-server
   ```

   Or via the console:

   ```bash
   curl -X POST "$CONSOLE/api/nodes/n1/server/stop"
   ```

2. **Install the new binary.**

   ```bash
   ssh n1.example.com sudo install -m 755 crowkv-server-new /usr/local/bin/crowkv-server
   ```

3. **Restart with the persisted store/group IDs.**

   ```bash
   ssh n1.example.com sudo crowkv-server \
     --management-addr 0.0.0.0 --management-port 2001 \
     --ports 20001 --election-profile default \
     --stores 3 --groups 3 --replica 1
   ```

   `--stores`/`--groups` tells the server to reopen the WAL and apply the
   persisted group config so it rejoins as a full member instead of booting
   empty. `--replica` must match the replica ID assigned to this node (1 for
   `n1`, 2 for `n2`, 3 for `n3`).

   Or via the console (if the server was originally deployed through the
   console):

   ```bash
   curl -X POST "$CONSOLE/api/nodes/n1/server/restart"
   ```

   Note: the console `restart` endpoint reuses the persisted ports and binary
   path but does not currently pass `--stores`/`--groups`/`--replica` bootstrap
   args. For rolling upgrades where the server must recover from WAL, use the
   direct CLI invocation above.

4. **Wait for the node to become healthy.**

   ```bash
   until curl -s "$CONSOLE/api/nodes/n1" | grep -q '"health":"Up"'; do
     sleep 0.5
   done
   ```

5. **Wait for the group to report `healthy` again.**

   ```bash
   curl "$CONSOLE/api/stores/3/groups/3"
   ```

   The output should show all three replicas and one leader.

6. **Smoke test.**

   ```bash
   curl "$CONSOLE/api/stores/3/groups/3/kv/get?key=hello"
   ```

7. Move to the next node.

### 3.2 What to watch

- **Leader**: after a node is stopped, the remaining two nodes elect a new
  leader. Wait for the console group view to show a leader on one of the live
  nodes before declaring the step complete.
- **Catch-up**: the restarted node rejoins as a follower and catches up on
  missed log entries. This is normally near-instant for low-throughput clusters;
  high-throughput clusters may take seconds.
- **Client retries**: KV clients retry on `NotLeader` and transport errors. A
  brief spike in read/write latency is expected during the leader transition.

---

## 4. Replace a failed node

If a node is permanently lost, replace it with a new machine and the same
replica ID.

1. Provision the new machine with the same node ID, management port, and gRPC
   port as the failed node.
2. Register the new server endpoint in the console if the host/IP changed:

   ```bash
   curl -X POST "$CONSOLE/api/nodes/n1/server/deploy" \
     -H 'Content-Type: application/json' \
     -d '{"mgmt_port":2001,"grpc_port":20001}'
   ```

3. Start `crowkv-server` on the new node with the bootstrap args for its
   store/group/replica:

   ```bash
   crowkv-server \
     --management-addr 0.0.0.0 --management-port 2001 \
     --ports 20001 --election-profile default \
     --stores 3 --groups 3 --replica 1
   ```

4. The new node replays WAL if the WAL directory was restored from backup, or
   joins empty and catches up from the leader if not.
5. Verify group health:

   ```bash
   curl "$CONSOLE/api/stores/3/groups/3"
   ```

**Note**: if the WAL and config directory were also lost, the replacement node
must be added as a new replica through the console and the old replica removed.
Do not reuse the old replica ID on a node that has no WAL; instead, remove the
old replica and add a fresh one with a new replica ID.

---

## 5. Add a read replica

Adding a replica increases read capacity. The console orchestrates the
non-voting-then-voting catch-up dance internally — the caller just provides
the target node and replica ID.

1. Ensure the target node is already registered in the console and has a
   running server.
2. Add the replica with a new, unused replica ID:

   ```bash
   curl -X POST "$CONSOLE/api/stores/3/groups/3/replicas" \
     -H 'Content-Type: application/json' \
     -d '{"node_id":"n4","replica_id":4}'
   ```

   The console creates the local group on `n4`, wires remotes bidirectionally,
   and the new replica streams a snapshot from the leader to catch up. Once
   caught up, it joins the voting set automatically.

3. The new replica starts serving reads. Linearizable reads go to the leader;
   `BoundedStale` and `BestEffort` reads can be served by any replica with a
   sufficient resolved-slot.

To remove it later:

```bash
curl -X DELETE "$CONSOLE/api/stores/3/groups/3/replicas/4"
```

---

## 6. Remove a replica

Removing a replica decreases the group size. For safety, do not drop the group
below three voting members in production.

```bash
curl -X DELETE "$CONSOLE/api/stores/3/groups/3/replicas/3"
```

The console handles the full removal flow:

1. If the target replica is the leader, ask it to step down via `StepDown` RPC.
2. Wait (bounded) for a new leader to be elected among the survivors.
3. Deregister the replica as a remote from every remaining peer.
4. Delete the local group on the target node.
5. Refresh the monitor cache and persist the config.

After removal, the target node is no longer part of the group but continues
running `crowkv-server`; it can host other stores/groups.

---

## 7. Emergency: loss of quorum

If two of three nodes fail, the remaining node cannot elect itself leader.
Writes and linearizable reads block. You have two options:

- **Restore the failed nodes** from backups and restart them with their
  `--stores`/`--groups`/`--replica` bootstrap args. This is always the safest
  path.
- **Recover with data loss** (last resort): use a disaster-recovery procedure
  to force the surviving node to become leader. This is outside the scope of
  this doc; it requires manually truncating the log and is only safe when the
  other nodes are known to be permanently lost.

Do not add a brand-new third node to a 1/3-dead group without first recovering
leadership; the new node cannot help elect a leader.

---

## 8. Check cluster health

### 8.1 Per-node health

```bash
curl "$CONSOLE/api/nodes"
```

Look for `"health": "Up"`. `Down` means the console cannot reach the node's
management API.

### 8.2 Per-group health

```bash
curl "$CONSOLE/api/stores/3/groups/3"
```

- `healthy`: all replicas are up and a leader is known.
- `degraded`: one or more replicas are down, but a quorum and leader remain
  available.
- `unavailable`: quorum lost; no leader can be elected.

### 8.3 Server logs

`crowkv-server` logs to `log/crowkv-server.log` relative to its working
directory. When deployed through the console, the working directory is the
node's workspace under the console's data root, and logs are in
`<workspace>/log/crowkv-server-<pid>.out.log`.

---

## 9. Backup

CrowKV durability comes from the per-store WAL (`--wal-root`) and the group
config files (`--config-root`, defaults to a sibling `conf` directory of the
WAL root). For disaster recovery, back up:

- `{wal-root}/store{store_id}/` for each store
- `{config-root}/store{store_id}_group{group_id}.json` for each group
- `{data-root}/store{store_id}/group{group_id}.ctdb` if using the crowtree
  durable KV engine

Restore by placing these directories on the replacement node and starting the
server with the matching `--stores`/`--groups`/`--replica` bootstrap args.

---

## 10. Reference commands

### Physical topology

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

### Logical topology (stores and groups)

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

### KV data plane

| Operation | Endpoint |
| --- | --- |
| Get | `GET /api/stores/{sid}/groups/{gid}/kv/get?key=...` |
| Put | `POST /api/stores/{sid}/groups/{gid}/kv/put` |
| Delete | `POST /api/stores/{sid}/groups/{gid}/kv/delete` |
| Scan | `GET /api/stores/{sid}/groups/{gid}/kv/scan?key=...` |

See `design/design-kv-server.md` and `design/design-reconfiguration.md` for the
underlying protocol and safety model details.

---

## Appendix A: SSH loopback dev setup

To test SSH transport on a dev box (loopback to localhost):

```bash
# 1. Generate SSH key if missing
ssh-keygen -t ed25519 -N '' -f ~/.ssh/id_ed25519

# 2. Authorize the key
cat ~/.ssh/id_ed25519.pub >> ~/.ssh/authorized_keys
chmod 600 ~/.ssh/authorized_keys

# 3. Ensure sshd is running
systemctl status sshd

# 4. Test loopback SSH
ssh $USER@127.0.0.1 echo ok

# 5. Run SSH tests (CI skips these by default)
CROWKV_TEST_SSH=1 pixi run test-cli
```

SSH host keys are persisted in a `known_hosts` file (`$CROWKV_KNOWN_HOSTS` or
`~/.crowkv/known_hosts`). On key mismatch the connection is refused; delete the
offending line to recover.

## Appendix B: Console registry format

The console registry is stored in `~/.crowkv/console.toml` (or
`$CROWKV_CONSOLE_CONFIG`):

```toml
[[racks]]
id = "my-rack"
name = "Production Rack"

[[nodes]]
id = "node-1"
rack_id = "my-rack"
host = "127.0.0.1"
ssh_port = 22
ssh_user = ""
ssh_key = null
ssh_password = null

[[nodes]]
id = "node-2"
rack_id = "my-rack"
host = "10.0.0.1"
ssh_port = 2222
ssh_user = "ubuntu"
ssh_key = "/home/ubuntu/.ssh/id_rsa"
ssh_password = null

[bench.stress.burst]
workload = "write"
threads = 64
connections = 16
duration_secs = 10
key_space = 10000
value_size = 128
```
