# CrowKV Console Requirements

## Overview
`crowkv-console` is a unified management project for observing and operating CrowKV clusters. It ships two frontends that share the same operation core:
- **Web UI** — visual cluster management, topology graph, KV browser, demo-grade presentation.
- **CLI** — scripting, automation, CI/CD, and load testing.

All cluster access goes through `crowkv-server` public endpoints (HTTP management API + gRPC KV service). The console does not bypass the server.

## Project Name
- `crowkv-console` — the project/repo name, covering both web UI and CLI.

## Architecture: Shared Core, Multiple Frontends

### Shared Core Library (Rust)
- Business logic for cluster observation, management, and KV operations.
- API clients: HTTP (management API) + gRPC (KV / Paxos / health).
- Data models, error types, validation, configuration parsing.
- Reused by both CLI and Web UI backend; no duplication.

### Frontend: Web UI
- Backend: Rust (Axum) serving:
  - Static assets (bundled Swagger UI, web app bundle).
  - Proxy/facade endpoints to one or more `crowkv-server` instances.
  - Aggregated topology view across instances.
- Frontend: modern JS framework (React / Vue / Svelte — TBD).
- Topology graph library: evaluate Cytoscape.js, React Flow, or vis-network. Must look good enough for customer demos.
- Beautiful UI: status colors (healthy/degraded/failed), icons, animated transitions, responsive layout.

### Frontend: CLI
- Command set mirrors web UI operations (same core lib, thin command layer).
- Extra capability: load testing / benchmarking.
- Config file support for repeated / scripted workflows.

```
┌─────────────────────────────────────────┐
│         Shared Core Library (Rust)       │
│  - Business logic                        │
│  - API clients (HTTP + gRPC)             │
│  - Data models / errors / config         │
└─────────────────────────────────────────┘
              │              │
              ↓              ↓
┌──────────────────┐  ┌──────────────────┐
│   CrowKV CLI     │  │  CrowKV Web UI   │
│  (terminal)      │  │   (browser)      │
└──────────────────┘  └──────────────────┘
              │              │
              └──────┬───────┘
                    ↓
         crowkv-server instances
         (HTTP management + gRPC KV)
```

## Swagger UI Hosting
- Move Swagger UI hosting from `crowkv-server` to `crowkv-console` web backend.
- Rationale: GitHub download is unreliable; the console is the natural place to bundle and serve UI assets.
- Bundle Swagger UI static files in the repo (downloaded once, committed under `crowkv-console/static/swagger-ui/`).
- Console serves Swagger UI at `/api` and proxies OpenAPI JSON from the selected `crowkv-server` instance.
- `crowkv-server` stays focused on API + ToSchema generation; no Swagger UI dependency.

## Core Features

### 1. Cluster Observation
- Inventory of all `crowkv-server` instances registered with the console.
- Hierarchy view: **Rack → Node → Server Instance → Store → Group → Replica (local + remote)**.
- Topology graph with live status (leader, followers, remotes, health colors).
- Real-time metrics: key count, RPC rates, error counts, RTT.

### 2. Simulated Hardware Cluster
Goal: make it easy to test KV cluster behavior on a single physical host while still modeling a realistic topology.

- Concepts: **Rack → Node → Server Instance**.
  - One server instance per simulated node.
  - One store per server instance (enforced limit in console).
- In the current dev environment: single physical host.
  - Nodes are simulated using the same host IP but different ports.
  - Access model assumes SSH-style remote control (abstracted; today just local spawn).
- Node access model:
  - Every node is treated as **remote**, even when running on the local host.
  - Two transports per node:
    - **SSH** (user/password or key) — used for lifecycle operations: deploy / start / stop a `crowkv-server` process, copy binaries / config, tail logs.
    - **HTTP / gRPC** — used for runtime operations once the server is running: management API, KV ops, health.
  - Default credentials: SSH to `127.0.0.1` using the current user (must be set up so the user can SSH to themselves).
  - Before any operation on a node, the console performs a real SSH connect/handshake to confirm reachability; failure is reported back to the UI/CLI.
- Operations:
  - Create / delete racks.
  - Create / delete nodes within a rack (each node carries its SSH endpoint + credentials).
  - Deploy / stop a server instance on a node (via SSH; runtime control via HTTP/gRPC).
  - Create groups and choose which nodes host which replicas (leader + remotes).
  - Delete groups / replicas / stores.

### 3. Dynamic Management
- Add/remove stores, groups, replicas, remote endpoints via the server's management API.
- Modify group configuration (leader election settings, classic vs fast-path).
- All operations driven by user choice in UI or CLI command.

### 4. KV Operations
- Browse keys by `(store, group)`.
- Put / get / delete / list values.
- Display **full value content** in the UI (this is an internal demo console; data is not protected).
- Display value size and any available metadata (e.g. last-modified) alongside the value.
- Prefix search / filter:
  - **Future requirement (not in initial scope).** `crowkv-server` does not yet expose a prefix-list KV API.
  - The console UI will reserve a search box and CLI a `--prefix` flag, but until the server adds the underlying API, prefix browsing is unsupported. Tracked as a follow-up that requires a server-side change.

### 5. Load Testing (CLI only)
Load testing is a CLI-only capability used as an internal testing tool against a running cluster.

- Run benchmark workloads against a cluster.
- Configure: QPS target, duration, key distribution, value size, read/write mix.
- Report: latency percentiles, throughput, error rates.
- Support stress / capacity-planning scenarios.

### 6. API Integration
- Bundled Swagger UI served by console.
- Server-instance selector to switch the target `crowkv-server`.
- Proxy API calls to the selected instance.
- Optionally aggregate OpenAPI JSON across instances.

## CLI Command Hierarchy
The CLI uses a **two-layer command** structure: `crowkv <group> <verb> [options]`. Top-level groups separate concerns; verbs are consistent within a group.

```
crowkv
├── cluster              # observation
│   ├── status           # high-level health summary
│   ├── topology         # print full hierarchy (rack/node/store/group/replica)
│   └── inspect <id>     # detailed view of one node/store/group/replica
│
├── rack                 # simulated hardware: racks
│   ├── add <name>
│   ├── remove <name>
│   └── list
│
├── node                 # simulated hardware: nodes (host + ssh creds)
│   ├── add --rack <r> --host <addr> --ssh-user <u> [--ssh-pass | --ssh-key]
│   ├── remove <node>
│   ├── list
│   └── ping <node>      # validate ssh + http reachability
│
├── server               # crowkv-server lifecycle on a node
│   ├── deploy --node <n> [--port ...]
│   ├── start <server-id>
│   ├── stop <server-id>
│   └── list
│
├── store                # store mgmt within a server
│   ├── add --server <s> --store-id <id>
│   ├── remove --server <s> --store-id <id>
│   └── list [--server <s>]
│
├── group                # paxos group mgmt
│   ├── add --store <s> --group-id <id> --leader <node> --remotes <n1,n2,...>
│   ├── remove --store <s> --group-id <id>
│   ├── list [--store <s>]
│   └── inspect --store <s> --group-id <id>
│
├── replica              # add/remove individual replicas
│   ├── add --group <g> --node <n> [--voting]
│   └── remove --group <g> --replica-id <r>
│
├── kv                   # data plane
│   ├── put --group <g> <key> <value>
│   ├── get --group <g> <key>
│   ├── delete --group <g> <key>
│   ├── list --group <g> [--prefix <p>]   # --prefix pending server support
│   └── scan --group <g> [--limit N]
│
└── bench                # load testing (CLI-only)
    ├── run --workload <name> [--qps N --duration T --key-dist ... --value-size ... --read-ratio ...]
    ├── stress --duration T --target-qps N
    └── report <run-id>  # post-run summary / re-print percentiles
```

Design rules:
- **Two layers max** — `crowkv <group> <verb>`. No three-level chains.
- Verb vocabulary stays consistent: `add / remove / list / inspect`. Lifecycle verbs (`start / stop / deploy`) are reserved for `server`. Data verbs (`put / get / delete / list / scan`) are reserved for `kv`.
- Every command targets the same shared core library; CLI is a thin argument-parsing layer.
- Output: human-friendly table by default, `--json` flag for scripting.

## Out of Scope (for now)
- Authentication, authorization, RBAC, audit logging — deferred.
- Multi-tenant isolation.
- Persistent console-side state beyond local config (cluster registry may be file-backed initially).

