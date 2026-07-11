# CrowKV Console Design

Upstream: `doc/requirement-console.md`.

## 1. Goals and Non-Goals

### Goals
- Single workspace project `crowkv-console` delivering a Web UI and a CLI that share one Rust core.
- Operate against any number of `crowkv-server` instances via their public surfaces (HTTP management API + gRPC KV / health).
- Model a **Rack → Node → Server Instance → Store → Group → Replica** hierarchy, including a "simulated hardware" mode that runs entirely on `127.0.0.1`.
- Host Swagger UI offline using a single pinned, stable Swagger UI release (no GitHub fetch at build time).

### Non-Goals
- Bypassing `crowkv-server` to talk to Paxos / WAL / storage internals.
- Authentication, authorization, multi-tenancy, audit logging.
- Persisting console state beyond local config files.

## 2. High-Level Architecture

`crowkv-console` is **one project** (one top-level directory `crowkv-console/`), shipped as several small libs and binaries inside the existing Cargo workspace. No `crates/` subdirectory; every name starts with `crowkv-`.

```
crowkv-console/
  crowkv-console-core/    (lib)   data models, HTTP+gRPC clients, registry, aggregator, error model
  crowkv-console-ssh/     (lib)   russh-based SSH session pool, deploy/start/stop helpers
  crowkv-console-bench/   (lib)   workload generator, stats, reports — used only by CLI
  crowkv-console-web/     (bin)   Axum backend, static asset server, Swagger UI mount, proxy routes
  crowkv-console-cli/     (bin)   `crowkv` clap-based CLI; depends on core + bench
  static/swagger-ui/              committed Swagger UI assets (one pinned version)
  web/                            React + Vite frontend source (TS, shadcn/ui, React Flow)
```

Targets:
- `crowkv-console-core` → reusable lib for both frontends.
- `crowkv-console-web` → bin (`crowkv-console-web`), serves UI + API on `:9920`.
- `crowkv-console-cli` → bin (`crowkv`), the user-facing CLI.

Calls always go: frontend → `core` → `crowkv-server` (HTTP/gRPC) or `ssh` → `crowkv-server` host.

### 2.1 Reuse Boundary
- All "what to do" lives in `crowkv-console-core` (e.g. `add_group`, `deploy_server`, `kv_put`, `aggregate_topology`).
- `crowkv-console-web` (Axum) and `crowkv-console-cli` (clap) only parse input and render output.
- Web UI **does not** reimplement business logic; it calls `core` via the Axum backend.

## 3. Data Model

```rust
// crowkv-console-core::model
struct Rack { id, name, nodes: Vec<NodeId> }

struct Node {
    id, rack_id, host: String,            // 127.0.0.1 by default
    ssh: SshCreds,                        // user + (password|key)
    server: Option<ServerInstanceId>,     // 0 or 1 instance per node (UI/console enforced)
}

enum SshCreds {
    KeyDefault { user },                  // use ~/.ssh/* defaults
    KeyPath { user, key_path },
    Password { user, pass },
}

struct ServerInstance {
    id, node_id,
    mgmt_url: String,                     // http://host:port
    grpc_url: String,
    pid: Option<u32>,
    state: ProcState,                     // Stopped|Starting|Running|Failed
}

struct StoreView { server_id, store_id, listen_addr, groups: Vec<GroupView> }
struct GroupView { group_id, leader_id, local_replica: ReplicaView, remotes: Vec<RemoteReplicaView> }

// Aggregated cluster snapshot returned to UI/CLI
struct ClusterSnapshot {
    racks: Vec<Rack>,
    nodes: Vec<Node>,
    servers: Vec<ServerInstance>,
    stores: Vec<StoreView>,               // populated by polling each server's report_info()
}
```

`StoreView`, `GroupView`, `ReplicaView`, `RemoteReplicaView` are direct re-exports of `crowkv::cluster::info::*`. No duplication.

> **One server per node**: enforced by UI and `core` operations. The internal data model (`Node.server: Option<...>`) and the underlying `crowkv-server` are not architecturally limited to one — tests and lower layers may run multiple — but every console operation rejects a second deploy on the same node.

## 4. Console Backend Persistence
- Local config file: `~/.crowkv/console.toml` (or `$CROWKV_CONSOLE_CONFIG`).
- Stores: rack/node definitions and SSH creds. **Plaintext** is acceptable for v1 (internal demo); a `ConsoleConfig` struct is the single place that reads it, so swapping the source later (keyring, env) is local.
- Server runtime info (pid, state, KV/topology snapshots) is **never persisted**; always fetched fresh from `crowkv-server`.

## 5. Node Access Model

### 5.1 Two transports per node
| Purpose | Transport |
| --- | --- |
| Deploy / start / stop `crowkv-server` process; copy binary; tail logs | SSH |
| Runtime mgmt API (add store/group, list, health) | HTTP |
| Runtime KV ops, snapshot, paxos health | gRPC |

### 5.2 SSH defaults (russh)
- Crate: **`russh`** (pure Rust, async). No shell-out fallback.
- Default auth: `~/.ssh/*` keys (agent + standard key paths).
- Alternative auth: explicit key path; explicit `user/password`.
- Default host: `127.0.0.1` with the current OS user.
- Pre-flight: every operation calls `ssh::probe(node)` which performs a real handshake before any side-effecting work. Failure surfaces as `NodeUnreachable { node_id, reason }`.

### 5.3 Process lifecycle (deploy / start / stop)
1. SSH into node.
2. Ensure binary exists at `/opt/crowkv/bin/crowkv-server` (scp on first deploy).
3. Render config template; write to `/opt/crowkv/etc/<server-id>.toml`.
4. `nohup crowkv-server --config ... &`; capture pid; record in `core` registry.
5. Health-check via the new server's HTTP `/health` until ready or timeout.

`server start` and `server stop` accept either a server id or a `(rack, node)` chain — the registry resolves either form to one node.

## 6. Web UI Backend (Axum)

### 6.1 Routes

The web bin listens on `:9920`. Path scheme:
- `/` — SPA shell (HTML).
- `/assets/*` — bundled JS / CSS / fonts.
- `/api/*` — JSON REST surface used by the SPA and external scripts.
- `/api/swagger/*` — bundled Swagger UI (kept under `/api` for easy bookmarking).

| Method | Path | Purpose |
| --- | --- | --- |
| GET    | `/api/swagger/`                       | Swagger UI static assets (offline bundle). |
| GET    | `/api/openapi.json?server=<id>`       | Proxy `crowkv-server` OpenAPI JSON. Kept so Swagger UI can target any registered server. |
| GET    | `/api/cluster/snapshot`               | Aggregated `ClusterSnapshot`. |
| GET    | `/api/rack`, `/api/rack/:id`          | List racks / rack subtree. |
| POST   | `/api/rack`                           | Create rack. |
| DELETE | `/api/rack/:id`                       | Delete rack. |
| GET    | `/api/node`, `/api/node/:id`          | List nodes / node subtree (server, stores, groups, replicas). |
| POST   | `/api/node`                           | Create node (host + SSH creds). |
| DELETE | `/api/node/:id`                       | Delete node. |
| POST   | `/api/node/:id/ping`                  | SSH + HTTP reachability check. |
| GET    | `/api/server`, `/api/server/:id`      | List servers / server subtree. |
| POST   | `/api/server/deploy`                  | Deploy a server on a target node (body: `{ node_id, port? }`). |
| POST   | `/api/server/:id/start`               | Start by id (or `(rack,node)` resolved id). |
| POST   | `/api/server/:id/stop`                | Stop by id. |
| GET    | `/api/store`, `/api/store/:id`        | List stores / store subtree. |
| POST   | `/api/store`                          | Add store (proxies to crowkv-server). |
| DELETE | `/api/store/:id`                      | Delete store. |
| GET    | `/api/group`, `/api/group/:id`        | List groups / group subtree. |
| POST   | `/api/group`                          | Add group. |
| DELETE | `/api/group/:id`                      | Delete group. |
| GET    | `/api/replica`, `/api/replica/:id`    | List replicas / replica detail. |
| POST   | `/api/replica`                        | Add replica. |
| DELETE | `/api/replica/:id`                    | Delete replica. |
| GET    | `/api/kv/:group/:key`                 | Read value (full content). |
| PUT    | `/api/kv/:group/:key`                 | Create or **edit** value (idempotent put). |
| DELETE | `/api/kv/:group/:key`                 | Delete value. |
| GET    | `/api/kv/:group`                      | List keys (no prefix until server supports it). |

All `/api/*` handlers are thin wrappers that call `crowkv-console-core` functions.

### 6.2 Frontend
- Framework: **React + Vite** (TypeScript). Largest ecosystem of high-quality graph libs and component kits.
- Topology graph: **React Flow**, custom node types per layer (rack, node, server, store, group, replica). **No fallback** — if React Flow can't satisfy a need, stop and escalate. Library is cached in the local npm cache; no runtime CDN.
- Component library: **shadcn/ui + Tailwind**. Production-acceptable; not just demo styling.
- Status colors: green (healthy) / amber (degraded) / red (failed) / gray (unknown). Animated leader badge.
- Live updates: keep a long-lived HTTP/1.1 keep-alive (or HTTP/2) connection and poll `/api/cluster/snapshot` on a short interval. **No WebSocket / SSE.** The TCP connection is reused; only the JSON body changes per poll. Server-side response uses `Cache-Control: no-store` and 304-friendly ETags so unchanged snapshots are cheap.

## 7. CLI Design

- Binary: `crowkv` (the noun-verb command structure is self-explanatory).
- Parser: `clap` derive; one module per top-level group, each verb a subcommand struct.
- Output: pretty table (`comfy-table`) by default; `--json` global flag for scripting.
- Global flags: `--config <path>`, `--server <id>` (default target), `--json`, `-v` / `-vv`.
- Command hierarchy as defined in `requirement-console.md` §"CLI Command Hierarchy".

### 7.1 Bench subcommand internals
- `crowkv-console-bench` exposes a `Workload` trait with built-in implementations for read / write / list / mix.
- Connection model:
  - User picks `--connections N` (1..=64 per server, default 4); each connection is a real, separate TCP/HTTP2 channel to the target `crowkv-server` (no multiplexing all RPCs over a single channel).
  - User picks `--threads M` (1..=1000); each thread runs a **blocking** loop: issue op → await response → next op. This is the lowest-latency model and is easy to reason about.
  - Threads are mapped round-robin onto the connection pool.
- Stats: HDR histogram for latency; atomic counters for ops/errors; sampled snapshots written to a ring buffer to avoid hot-path contention.
- Performance discipline: the bench tool itself must not become the bottleneck. Hot paths avoid allocations per op (key/value buffers reused), avoid logging in the inner loop, and use lock-free histograms.
- Reports: JSON file at `~/.crowkv/bench/<run-id>.json`; `bench report <run-id>` re-renders a saved file.

### 7.2 Bench command shape
```
crowkv bench run read    --connections N --threads M --duration T [--keys-from <file>] [--key-range a..b]
crowkv bench run write   --connections N --threads M --duration T [--value-size B] [--key-dist uniform|zipf]
crowkv bench run list    --connections N --threads M --duration T [--page-size P]
crowkv bench run mix     --connections N --threads M --duration T --read-ratio R [--value-size B]
crowkv bench stress      <scenario>          # predesigned scenarios bundled with the binary
crowkv bench report      <run-id>
```
- `bench run <op>` is the workhorse; one op per command keeps the flag set small per command.
- `bench mix` adds `--read-ratio` and is the only knob for read/write mixing.
- `bench stress` invokes a **predesigned scenario** baked into the binary (`saturating-write`, `mostly-read`, `latency-tail`, …); one positional name, no further tuning.
- `bench report` is unchanged.

## 8. Swagger UI Hosting
- `crowkv-console/static/swagger-ui/` committed in repo: a single pinned Swagger UI release, extracted from a known-stable tag (e.g. `v5.17.14` or whichever is current/stable when bundling). The version is recorded in a sibling `VERSION` file so future maintainers know what to refresh.
- Axum mounts the directory at `/api/swagger/` using `tower_http::services::ServeDir`.
- `/api/openapi.json?server=<id>` proxies to the chosen `crowkv-server`'s `/openapi.json` via `crowkv-console-core`'s HTTP client.
- `crowkv-server` reverts to **no Swagger UI dependency**: keep `ToSchema` derives (OpenAPI JSON still generated), drop the `swagger-ui` Cargo feature and the `utoipa-swagger-ui` crate. The Swagger UI lives in the console only.

## 9. Error Model and Operation Logging

### 9.1 Error enum
- `crowkv-console-core::Error` enum:
  - `NodeUnreachable { node_id, reason }`
  - `ServerRpc { server_id, status }`
  - `Validation { field, message }`
  - `NotFound { kind, id }`
  - `Conflict { kind, id }`
- HTTP layer maps to `4xx` / `5xx`; CLI layer maps to exit codes (`0` ok, `1` user error, `2` cluster/network error).

### 9.2 Operation log (CLI + Web)
A console session writes **two streams**:
1. **Console stream** — short user-friendly messages and errors (CLI: stderr; Web: log panel).
2. **Operation log** — a per-session file under `~/.crowkv/log/console-<UTC-timestamp>-<pid>.log`. Each line is a structured record of one outbound action with enough detail to reproduce it manually:
   - HTTP: method, full URL, headers (excluding secrets), JSON body, response status + body summary.
   - gRPC: service / method, request proto JSON, response status.
   - SSH: target, full remote command line, exit status, stdout/stderr snippets.

A new file is created on every CLI invocation and on every `crowkv-console-web` startup. Files are not auto-rotated within a session; users can prune `~/.crowkv/log/` themselves.

This makes any failed operation reproducible by copy-pasting the recorded command (curl/grpcurl/ssh) into a terminal.

## 10. Observability
- `tracing` everywhere; `--vv` switches CLI to debug.
- Web backend exposes `/healthz`. **`/metrics` is deferred**: the Rust Prometheus story has multiple competing crates (`prometheus`, `metrics` + `metrics-exporter-prometheus`, `opentelemetry`); we will pick one once we start the broader observability work for `crowkv-server` itself. Until then, the operation log + tracing are the source of truth.
- All console-issued operations attach a correlation id and propagate it as `x-crowkv-corr-id` to `crowkv-server` request headers (already supported by `crowkv-server`'s tracing middleware).

## 11. Open Questions

- **SSH crate**: `russh` (decided). Defaults to `~/.ssh/*`; `(user, password)` is an explicit alternative.
- **Frontend bundle**: built on demand during development. The `web/` directory is the source; `npm run build` produces `dist/` which the Axum server serves. The committed repo does **not** include `web/dist/`. Production / release builds run `npm ci && npm run build` as part of `make`.
- **Credentials storage**: plaintext TOML, accessed only through the `ConsoleConfig` struct so the source can change later without touching call sites.
- **Multiple servers per node**: UI and console operations enforce one. The data model and `crowkv-server` itself remain unrestricted; lower-layer tests can still spawn many.
