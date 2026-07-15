<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# CrowKV Demo Plan

Goal: two short GIFs for the README that show the console UI in action.

## Prerequisites

- `pixi run build` succeeds (crowkv-server + crowkv-console-web)
- 3 terminal windows ready (or one tmux session with 3 panes)
- Screen recorder: QuickTime (macOS) or OBS
- ffmpeg for GIF conversion

## Demo 1: Cluster Lifecycle (8-10s GIF)

Shows: bootstrap a 3-node cluster via the web console, watch topology come alive.

### Setup

```bash
# Terminal 1-3: start 3 servers on localhost
crowkv-server --management-addr 127.0.0.1 --management-port 2001 --ports 20001 --election-profile default
crowkv-server --management-addr 127.0.0.1 --management-port 2002 --ports 20002 --election-profile default
crowkv-server --management-addr 127.0.0.1 --management-port 2003 --ports 20003 --election-profile default

# Terminal 4: start the console
crowkv-web
```

### Recording script

1. Open browser → `http://localhost:9920`
2. Physical view: register 3 nodes (127.0.0.1:2001, :2002, :2003)
3. Create a rack, assign nodes to it
4. Switch to Logical view: create store, create group with 3 replicas on the 3 nodes
5. Watch the topology canvas update — nodes show leader/follower roles
6. Health pill turns green

### Recording tips

- Slow down clicks slightly so viewers can follow
- Hover over nodes to show status tooltips
- Keep the browser window at 1280x800

### GIF conversion

```bash
ffmpeg -i demo-cluster.mov -vf "fps=10,scale=800:-1:flags=lanczos" -loop 0 doc/assets/demo-cluster.gif
```

Target: < 5MB, 800px wide.

## Demo 2: KV Operations (5-8s GIF)

Shows: put/get/delete keys through the console UI, activity panel updates in real time.

### Recording script

1. Continue from Demo 1 state (cluster running)
2. Select a group in the sidebar
3. Inspector → KV tab
4. PUT key=`hello` value=`world` → success toast
5. GET key=`hello` → shows `world`
6. PUT key=`hello` value=`crowkv` → overwrite
7. GET key=`hello` → shows `crowkv`
8. DELETE key=`hello` → success
9. GET key=`hello` → not found
10. Activity tab shows the operation history

### GIF conversion

```bash
ffmpeg -i demo-kv.mov -vf "fps=10,scale=800:-1:flags=lanczos" -loop 0 doc/assets/demo-kv.gif
```

Target: < 3MB, 800px wide.

## Demo 3 (optional): Failover (10-15s GIF)

Shows: kill the leader node, watch re-election, KV ops continue working.

### Recording script

1. Cluster running, identify the leader (topology canvas shows L badge)
2. Ctrl-C the leader's terminal
3. Console: health pill turns yellow → then green as new leader elected
4. KV operations still work (GET returns data)
5. Restart the killed node → rejoins as follower

## Asset Storage

- GIFs: `doc/assets/demo-cluster.gif`, `doc/assets/demo-kv.gif`
- Raw recordings: keep locally, do not commit (.mov files)

## README Integration

After recording, uncomment the placeholder lines in README.md:

```markdown
![Cluster Lifecycle](doc/assets/demo-cluster.gif)
![KV Operations](doc/assets/demo-kv.gif)
```

## Checklist

- [ ] Record Demo 1 (cluster lifecycle)
- [ ] Convert to GIF, verify < 5MB
- [ ] Record Demo 2 (KV operations)
- [ ] Convert to GIF, verify < 3MB
- [ ] Uncomment README placeholders
- [ ] (optional) Record Demo 3 (failover)
- [ ] Run benchmark, fill in Performance table
