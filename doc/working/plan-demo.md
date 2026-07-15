<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CrowKV Demo Plan

Goal: two short GIFs for the README that show the web UI in action.

## Prerequisites

- `pixi run build` succeeds (crowkv-server + crowkv-web)
- 3 terminal windows ready (or one tmux session with 3 panes)
- Screen recorder: macOS built-in `screencapture` (see §Recording below)
- ffmpeg for GIF conversion (`pixi run gif-convert` or `tools/gif_convert.sh`)

## Demo 1: Cluster Lifecycle (8-10s GIF)

Shows: bootstrap a 3-node cluster via the web console, watch topology come alive.

### Setup

```bash
# Terminal 1-3: start 3 servers on localhost
crowkv-server --management-addr 127.0.0.1 --management-port 2001 --ports 20001 --election-profile default
crowkv-server --management-addr 127.0.0.1 --management-port 2002 --ports 20002 --election-profile default
crowkv-server --management-addr 127.0.0.1 --management-port 2003 --ports 20003 --election-profile default

# Terminal 4: start the web service
crowkv-web
```

Or use the CLI to bootstrap the cluster instead of clicking through the UI:

```bash
IP=127.0.0.1
PORT=9920

crowkv-cli --ip $IP --port $PORT rack add --id r1 --name "rack-one"
crowkv-cli --ip $IP --port $PORT node add --id n1 --rack r1 --host 127.0.0.1
crowkv-cli --ip $IP --port $PORT node add --id n2 --rack r1 --host 127.0.0.1
crowkv-cli --ip $IP --port $PORT node add --id n3 --rack r1 --host 127.0.0.1
crowkv-cli --ip $IP --port $PORT server deploy --node n1 --mgmt-port 2001 --grpc-port 20001
crowkv-cli --ip $IP --port $PORT server deploy --node n2 --mgmt-port 2002 --grpc-port 20002
crowkv-cli --ip $IP --port $PORT server deploy --node n3 --mgmt-port 2003 --grpc-port 20003
crowkv-cli --ip $IP --port $PORT store add --store-id 3 --nodes n1,n2,n3
crowkv-cli --ip $IP --port $PORT paxos add --store-id 3 --group-id 3 --replica-id 1 --nodes n1,n2,n3
```

### Recording script

1. Open browser → `http://localhost:9920`
2. Physical view: register 3 nodes (127.0.0.1:2001, :2002, :2003)
3. Create a rack, assign nodes to it
4. Switch to Logical view: create store, create group with 3 replicas on the 3 nodes
5. Watch the topology canvas update — nodes show leader/follower roles
6. Health pill turns green

Alternatively, use the CLI bootstrap commands above to set up the cluster
off-screen, then record only the UI showing the live topology.

### Recording (macOS)

Use QuickTime Player → File → New Screen Recording (⌘⇧5), or from terminal:

```bash
# Record a specific window for 10 seconds
screencapture -v -V 10 demo-cluster.mov
```

### Recording tips

- Slow down clicks slightly so viewers can follow
- Hover over nodes to show status tooltips
- Keep the browser window at 1280x800

### GIF conversion

```bash
tools/gif_convert.sh demo-cluster.mov doc/assets/demo-cluster.gif
```

Target: < 5MB, 800px wide.

## Demo 2: KV Operations (5-8s GIF)

Shows: put/get/delete keys through the KV Operator panel, activity log updates in real time.

### Recording script

1. Continue from Demo 1 state (cluster running)
2. Click the **KV** button in the header bar → KV Operator panel opens
3. Select store/group from the dropdowns (auto-selects first group)
4. Scan auto-runs, showing any existing keys
5. PUT key=`hello` value=`world` → success toast, scan list updates
6. GET key=`hello` → shows `world`
7. PUT key=`hello` value=`crowkv` → overwrite, scan list updates
8. GET key=`hello` → shows `crowkv`
9. Click trash icon on the `hello` row → confirm delete → success
10. GET key=`hello` → not found
11. Open Inspector → Activity tab shows the operation history

### GIF conversion

```bash
tools/gif_convert.sh demo-kv.mov doc/assets/demo-kv.gif
```

Target: < 3MB, 800px wide.

## Demo 3: Failover & Replica Management (10-15s GIF)

Shows: add a 4th node, expand group from 3 to 4 replicas, remove the leader
replica, watch re-election, then add the replica back. Even-numbered replica
counts fall back to odd quorum — functional but not recommended for production.

### Recording script

1. Cluster running with 3 nodes (from Demo 1)
2. Add a 4th node in the physical view
3. Add a 4th replica to the existing group, targeting the new node
4. Identify the leader replica (topology canvas shows L badge)
5. Remove the leader replica → remaining nodes re-elect a new leader
6. KV operations still work (GET returns data)
7. Add the removed replica back → rejoins as follower
8. Note: 4 replicas use odd quorum (3/4) — works but odd counts are recommended

## Asset Storage

- GIFs: `doc/assets/demo-cluster.gif`, `doc/assets/demo-kv.gif`, `doc/assets/demo-failover.gif`
- Raw recordings: keep locally, do not commit (.mov files)

## README Integration

After recording, uncomment the placeholder lines in README.md:

```markdown
![Cluster Lifecycle](doc/assets/demo-cluster.gif)
![KV Operations](doc/assets/demo-kv.gif)
![Failover](doc/assets/demo-failover.gif)
```

## Checklist

- [x] Record Demo 1 (cluster lifecycle)
- [x] Convert to GIF, verify < 5MB
- [x] Record Demo 2 (KV operations)
- [x] Convert to GIF, verify < 3MB
- [x] Uncomment README placeholders
- [x] Record Demo 3 (failover)
- [ ] Run benchmark, fill in Performance table
