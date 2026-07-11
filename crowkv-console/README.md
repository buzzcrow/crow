# CrowKV Console

Web UI and CLI for managing CrowKV clusters.

## SSH Loopback Dev Setup

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

# 5. Run SSH tests (CI defaults to CROWKV_TEST_SSH unset)
CROWKV_TEST_SSH=1 cargo test -p crowkv-console-shared
```

Note: CI skips real SSH tests by default (`CROWKV_TEST_SSH` unset). Set it to `1` to enable loopback SSH tests.

## C4 Status

C4 implements SSH transport using `russh` for remote server lifecycle.

### CLI Usage

```bash
# Rack management
crowkv rack add --id my-rack --name "Production Rack"
crowkv rack list
crowkv rack remove --id my-rack

# Node management
# Local-fork (no SSH): omit --ssh-user
crowkv node add --id node-1 --rack my-rack --host 127.0.0.1
# SSH-enabled: specify --ssh-user (and optionally --ssh-port, --ssh-password, --ssh-key)
crowkv node add --id node-2 --rack my-rack --host 10.0.0.1 --ssh-user ubuntu --ssh-port 2222 --ssh-password mypass
crowkv node add --id node-3 --rack my-rack --host 10.0.0.2 --ssh-user ubuntu --ssh-key ~/.ssh/id_rsa
crowkv node list
crowkv node remove --id node-1

# Node ping (SSH connectivity check)
crowkv node ping --id node-2

# Server deployment (local-fork or SSH based on node config)
# Server identity is node identity (one server per node)
crowkv server deploy --node-id node-1 --mgmt-port 9910 --grpc-port 9920
crowkv server stop --node-id node-1

# Set custom crowkv-server binary path (for SSH deploy, this is the remote path)
export CROWKV_SERVER_BIN=/path/to/crowkv-server
crowkv server deploy --node-id node-2 --mgmt-port 9911 --grpc-port 9921

# Cluster observation (uses deployed servers from registry)
crowkv cluster status
crowkv cluster topology
```

### Registry File Format

The registry is stored in `~/.crowkv/console.toml` (or `$CROWKV_CONSOLE_CONFIG`):

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
# Server process info (internal, not user-facing)
# server = { mgmt_url = "http://127.0.0.1:9910", grpc_url = "http://127.0.0.1:9920", pid = 12345 }

[[nodes]]
id = "node-2"
rack_id = "my-rack"
host = "10.0.0.1"
ssh_port = 2222
ssh_user = "ubuntu"
ssh_key = "/home/ubuntu/.ssh/id_rsa"
ssh_password = null

[bench]
# Optional: override built-in stress scenarios or define new ones
[bench.stress.burst]
workload = "write"
threads = 64
connections = 16
duration_secs = 10
key_space = 10000
value_size = 128

[bench.stress.custom_read]
workload = "read"
threads = 32
connections = 8
duration_secs = 30
key_space = 5000
value_size = 64
```

### Web UI

```bash
# Start the console web server
crowkv-web

# Open http://127.0.0.1:9920 in your browser
# Enter server URL to view cluster snapshot
# Note: C3 does not yet include a rack/node visualization; C5/C8 will add React UI
```

### SSH Known Hosts

The console persists SSH host keys in a `known_hosts` file for security:

**File location**: `$CROWKV_KNOWN_HOSTS` (if set) or `~/.crowkv/known_hosts` (default)

**Format**: One line per host with `host_id <algo> <base64-key>` format. The file is created automatically on first connection (TOFU - Trust On First Use).

**Mismatch behavior**: If a host presents a different key than what's stored, the connection is refused and a warning is logged with both the expected and presented algorithms. To recover, delete the offending line from the known_hosts file and re-connect.

**Process-wide store**: The known_hosts store is shared across all SSH connections in a single process via `OnceLock`, ensuring consistent behavior.

**Fallback**: If neither `$CROWKV_KNOWN_HOSTS` nor `$HOME` can be resolved, an in-memory store is used (keys won't persist across restarts), with a warning logged.

## C5 Status

C5 implements the cluster management plane (store/group/replica CRUD).

### CLI Usage

```bash
# Store management (logical, cluster-wide)
# Create a new store across multiple nodes
crowkv store add --store-id 1 --nodes node-1,node-2
crowkv store list
crowkv store inspect --store-id 1
crowkv store remove --store-id 1

# Paxos group management
crowkv group add --store-id 1 --group-id 1 --nodes node-1,node-2 [--leader node-1]
crowkv group list --store-id 1
crowkv group inspect --store-id 1 --group-id 1
crowkv group remove --store-id 1 --group-id 1

# Replica management (unified local/remote)
crowkv replica add --store-id 1 --group-id 1 --node node-3 [--replica-id 3]
crowkv replica remove --store-id 1 --group-id 1 --replica-id 3

# JSON output
crowkv --json store list
crowkv --json group inspect --store-id 1 --group-id 1
```

### HTTP API (Web Backend)

The console web server proxies management requests to upstream
`crowkv-server` instances. The API uses **logical entity addressing**:
store/group/replica/KV operations use cluster-wide logical IDs
(`:store_id`, `:group_id`, `:replica_id`); the backend resolves
placement from the topology cache. Server lifecycle uses `:node_id`
(one server per node).

> **Deprecated:** the previous `?server=<mgmt_url>` query-string
> contract and server-scoped paths (`/api/servers/:sid/...`) have been
> removed. See `doc/design/design-console.md` §6.1 and
> `doc/requirement.md` §15.4.6 (API routing rule) for the replacement.

```bash
# List stores (logical, cluster-wide)
curl http://127.0.0.1:9920/api/stores

# Add store (creates on specified nodes)
curl -X POST http://127.0.0.1:9920/api/stores \
  -H "Content-Type: application/json" \
  -d '{"store_id":1,"nodes":["node-1","node-2"]}'

# Get / remove store
curl        http://127.0.0.1:9920/api/stores/1
curl -X DELETE http://127.0.0.1:9920/api/stores/1

# Groups
curl        http://127.0.0.1:9920/api/stores/1/groups
curl -X POST http://127.0.0.1:9920/api/stores/1/groups \
  -H "Content-Type: application/json" \
  -d '{"group_id":1,"nodes":["node-1","node-2"],"leader_node":"node-1"}'
curl -X DELETE http://127.0.0.1:9920/api/stores/1/groups/1

# Replicas (unified local/remote)
curl        http://127.0.0.1:9920/api/stores/1/groups/1/replicas
curl -X POST http://127.0.0.1:9920/api/stores/1/groups/1/replicas \
  -H "Content-Type: application/json" \
  -d '{"node_id":"node-3","replica_id":3}'
curl -X DELETE http://127.0.0.1:9920/api/stores/1/groups/1/replicas/3

# OpenAPI for embedded Swagger UI (per-node)
curl http://127.0.0.1:9920/api/nodes/node-1/openapi.json
```

All endpoints return JSON. Upstream errors map to `502 Bad Gateway` to
distinguish console bugs from server errors. Unknown IDs return `404`.

## C6 Status

C6 implements KV data-plane operations (put/get/delete) over gRPC.

### CLI Usage

```bash
# Put a key/value (UTF-8 or binary via --value-file)
crowkv kv put --store-id 1 --group-id 1 --key mykey --value myvalue
crowkv kv put --store-id 1 --group-id 1 --key binkey --value-file /path/to/binary.dat

# Get a key (UTF-8 by default, hex with --hex)
crowkv kv get --store-id 1 --group-id 1 --key mykey
crowkv kv get --store-id 1 --group-id 1 --key binkey --hex

# Delete a key
crowkv kv delete --store-id 1 --group-id 1 --key mykey

# List/scan keys by prefix (tab-separated rows; empty prefix = all keys)
crowkv kv scan --store-id 1 --group-id 1 --prefix "" --limit 100
crowkv kv scan --store-id 1 --group-id 1 --prefix "user:" --limit 100

# List/scan with JSON output
crowkv --json kv scan --store-id 1 --group-id 1 --prefix "" --limit 100

# JSON output
crowkv --json kv get --store-id 1 --group-id 1 --key mykey
crowkv --json kv put --store-id 1 --group-id 1 --key mykey --value myvalue

# Idempotency tracking (client_id, seq)
crowkv kv put --store-id 1 --group-id 1 --key mykey --value myvalue --client-id 1 --seq 1
```

**Note**: `get` and `scan` are local follower reads in V1 and may return stale data. Not-found exits with code 3 for scriptability. `list`/`scan` output tab-separated `key\tvalue\n` rows (UTF-8 lossy); use `--json` for raw hex. When `--limit` is reached, the CLI prints `(truncated: more keys exist past --limit N)` to stderr.

### HTTP API (Web Backend)

The console web server proxies KV requests to upstream `crowkv-server` gRPC endpoints using logical paths:

```bash
# Get a key (GET /api/stores/:store_id/groups/:group_id/kv/get)
curl "http://127.0.0.1:9920/api/stores/1/groups/1/kv/get?key=mykey"
curl "http://127.0.0.1:9920/api/stores/1/groups/1/kv/get?key_hex=6d796b6579"

# Put a key (POST /api/stores/:store_id/groups/:group_id/kv/put)
curl -X POST "http://127.0.0.1:9920/api/stores/1/groups/1/kv/put" \
  -H "Content-Type: application/json" \
  -d '{"key":"mykey","value":"myvalue","client_id":0,"seq":0}'
curl -X POST "http://127.0.0.1:9920/api/stores/1/groups/1/kv/put" \
  -H "Content-Type: application/json" \
  -d '{"key_hex":"6d796b6579","value_hex":"6d7976616c7565","client_id":0,"seq":0}'

# Delete a key (POST /api/stores/:store_id/groups/:group_id/kv/delete)
curl -X POST "http://127.0.0.1:9920/api/stores/1/groups/1/kv/delete" \
  -H "Content-Type: application/json" \
  -d '{"key":"mykey","client_id":0,"seq":0}'

# Scan keys by prefix (GET /api/stores/:store_id/groups/:group_id/kv/scan)
curl "http://127.0.0.1:9920/api/stores/1/groups/1/kv/scan?prefix=user:&limit=100"
curl "http://127.0.0.1:9920/api/stores/1/groups/1/kv/scan?prefix_hex=757365723a&limit=100"
curl "http://127.0.0.1:9920/api/stores/1/groups/1/kv/scan?limit=0"  # no limit
```

All endpoints support both UTF-8 (`key`, `value`, `prefix`) and hex (`key_hex`, `value_hex`, `prefix_hex`) for binary safety. Scan responses include `key_utf8`, `key_hex`, `value_utf8`, `value_hex` per item, plus a `truncated` flag. Endpoint resolution rewrites the upstream's `0.0.0.0:N` listen_addr to use the management URL's host for remote access.

## C7 Status

C7 implements a CLI-only benchmarking engine for workload testing against `crowkv-server` gRPC endpoints.

### CLI Usage

```bash
# Run a workload (read, write, list, or mix)
crowkv bench run read --store-id 1 --group-id 1 --connections 4 --threads 8 --duration-secs 5 --key-space 1000 --value-size 64
crowkv bench run write --store-id 1 --group-id 1 --connections 4 --threads 8 --duration-secs 5 --key-space 1000 --value-size 64
crowkv bench run mix --store-id 1 --group-id 1 --connections 4 --threads 8 --duration-secs 5 --key-space 1000 --value-size 64

# Run a stress scenario (burst, soak, hotread)
crowkv bench stress burst --store-id 1 --group-id 1
crowkv bench stress soak --store-id 1 --group-id 1
crowkv bench stress hotread --store-id 1 --group-id 1

# Re-render a previously-saved report
crowkv bench report 2025-01-09T12-34-56-789Z

# JSON output
crowkv --json bench run read --store-id 1 --group-id 1
crowkv --json bench report 2025-01-09T12-34-56-789Z
```

**Parameters**:
- `--connections N`: Number of gRPC channels (1..=64, default 4)
- `--threads M`: Number of worker tasks (1..=1000, default 8)
- `--duration-secs`: Test duration in seconds (default 5)
- `--key-space`: Distinct keys per worker key space (default 1000)
- `--value-size`: Per-op value size in bytes (default 64)
- `--run-id`: Optional explicit run id; defaults to timestamp-based one

**Report location**: Reports are saved to `~/.crowkv/bench/<run-id>.json`. The CLI prints the report summary and the file path after each run.

**Note**: The `scan` workload always reports `error_rate=1.0` because the underlying `KvClient::scan` is a stub (C6 gap). The CLI accepts `bench run scan` for wiring exercise, but results are only useful as a "did the path connect" signal until the server adds prefix scan.

## C8 Status

C8 migrates Swagger UI from `crowkv-server` to `crowkv-web` and removes the `swagger-ui` Cargo feature from `crowkv-server`.

### Swagger UI

The console now serves a vendored Swagger UI at `/api/swagger/`. This provides an interactive API explorer for the `crowkv-server` management API.

```bash
# Access Swagger UI
open http://127.0.0.1:9920/api/swagger/

# Access OpenAPI JSON spec (per-node, proxied from selected node)
curl "http://127.0.0.1:9920/api/nodes/node-1/openapi.json"
```

**Vendored assets**: Swagger UI 5.17.14 is committed under `crowkv-console/web/swagger-ui/`. The directory includes:
- `swagger-ui.css`, `swagger-ui-bundle.js`, `swagger-ui-standalone-preset.js`
- Favicons and OAuth2 redirect page
- Hand-written `index.html` that accepts a `url` query parameter for the OpenAPI spec (e.g., `/api/swagger/?url=/api/nodes/node-1/openapi.json`)

**Bumping Swagger UI**: To upgrade to a new version:
1. Download the new version from unpkg.com (e.g., `curl -O https://unpkg.com/swagger-ui-dist@5.17.14/swagger-ui-bundle.js`)
2. Replace the files in `crowkv-console/web/swagger-ui/`
3. Update the version in `web/swagger-ui/VERSION`
4. Update the `index.html` script tags if the bundle names changed

**crowkv-server changes**: The `swagger-ui` Cargo feature and `utoipa-swagger-ui` dependency have been removed. The `/openapi.json` endpoint is now unconditional, and all `ToSchema` derives are kept. The `examples/openapi-export.rs` binary is now unconditional.

### React SPA (Vite + TypeScript + Tailwind)

The console root (`/` and any non-API path) now serves a React app built from `crowkv-console/web/ui/`. Node.js is **build-time only**; at runtime the compiled bundle in `web/ui/dist/` is served by Axum's SPA fallback handler.

**Repo layout**:

```
crowkv-console/web/ui/
  package.json       # pinned deps: react 18, vite 5, typescript 5, tailwind 3
  .nvmrc             # Node 20 LTS
  vite.config.ts     # dev server :5173, proxies /api -> 127.0.0.1:9920
  tailwind.config.js # dark theme matching the previous inline SPA
  index.html
  src/
    main.tsx
    App.tsx          # tabbed shell (Snapshot / Stores / KV / Swagger)
    api.ts           # typed wrappers over /api/*
    components/
      SnapshotTab.tsx
      StoresTab.tsx
      KvTab.tsx
```

**Build & run**:

```bash
# 1. Install Node 20 (use nvm if you have it)
nvm use     # picks up crowkv-console/web/ui/.nvmrc

# 2. One-time install of dev dependencies
make ui-install

# 3. Production build (emits dist/ which Axum serves)
make ui-build

# 4. Start the Axum backend
cargo run -p crowkv-web
# open http://127.0.0.1:9920/
```

**Hot-reload dev workflow**:

```bash
# Terminal 1: Vite dev server (auto-reloads on src/ changes)
make ui-dev
# open http://127.0.0.1:5173/  (Vite proxies /api/* and /healthz to :9920)
```

**Graceful fallback**: if `web/ui/dist/index.html` is missing (you ran `cargo run` without first running `make ui-build`), `/` returns a built-in instructional page pointing at the right `make` targets. This keeps `cargo build` and `cargo test` working with **no Node toolchain installed**, which is critical for CI and contributors who only touch the Rust crates.

**Reproducible builds**: `package-lock.json` is committed; the release pipeline runs `npm ci` (lockfile-strict) via `make ui-install`, then `npm run build` via `make ui-build`. `node_modules/` and `dist/` are gitignored.
