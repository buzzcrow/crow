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
CROWKV_TEST_SSH=1 cargo test -p crowkv-console-ssh
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
crowkv node ping --node node-2

# Server deployment (local-fork or SSH based on node config)
crowkv server deploy --id server-1 --node node-1 --mgmt-port 9910 --grpc-port 9920
crowkv server stop --id server-1

# Set custom crowkv-server binary path (for SSH deploy, this is the remote path)
export CROWKV_SERVER_BIN=/path/to/crowkv-server
crowkv server deploy --id server-2 --node node-2 --mgmt-port 9911 --grpc-port 9921

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

[[nodes]]
id = "node-2"
rack_id = "my-rack"
host = "10.0.0.1"
ssh_port = 2222
ssh_user = "ubuntu"
ssh_key = "/home/ubuntu/.ssh/id_rsa"
ssh_password = null

[[servers]]
id = "server-1"
url = "http://127.0.0.1:9910"
node_id = "node-1"
grpc_url = "http://127.0.0.1:9920"
pid = 12345
```

### Web UI

```bash
# Start the console web server
crowkv-console-web

# Open http://127.0.0.1:9920 in your browser
# Enter server URL to view cluster snapshot
# Note: C3 does not yet include a rack/node visualization; C5/C8 will add React UI
```

## C5 Status

C5 implements the cluster management plane (store/group/replica CRUD).

### CLI Usage

```bash
# Store management
# Create a new store (bootstrap group + local replica)
crowkv store add --store-id 1 --group-id 1 --replica-id 1 --port 9930
crowkv store list
crowkv store inspect --store-id 1
crowkv store remove --store-id 1

# Paxos group management
crowkv paxos add --store-id 1 --group-id 2 --replica-id 2
crowkv paxos list --store-id 1
crowkv paxos inspect --store-id 1 --group-id 1
crowkv paxos remove --store-id 1 --group-id 2

# Remote replica management
crowkv replica add --store-id 1 --group-id 1 --replica-id 3 --endpoint 10.0.0.1:9930
crowkv replica remove --store-id 1 --group-id 1 --replica-id 3

# Target a specific server (by URL or registry id)
crowkv --server http://127.0.0.1:9910 store list
crowkv --server server-1 paxos list --store-id 1

# JSON output
crowkv --json store list
crowkv --json paxos inspect --store-id 1 --group-id 1
```

### HTTP API (Web Backend)

The console web server proxies management requests to upstream `crowkv-server` instances:

```bash
# List stores (GET /api/stores?server=<url>)
curl http://127.0.0.1:9920/api/stores?server=http://127.0.0.1:9910

# Add store (POST /api/stores)
curl -X POST http://127.0.0.1:9920/api/stores?server=http://127.0.0.1:9910 \
  -H "Content-Type: application/json" \
  -d '{"store_id":1,"group_id":1,"replica_id":1,"port":9930}'

# Get store detail (GET /api/stores/{sid})
curl http://127.0.0.1:9920/api/stores/1?server=http://127.0.0.1:9910

# Remove store (DELETE /api/stores/{sid}) - returns 405 if not implemented upstream
curl -X DELETE http://127.0.0.1:9920/api/stores/1?server=http://127.0.0.1:9910

# List groups (GET /api/stores/{sid}/groups)
curl http://127.0.0.1:9920/api/stores/1/groups?server=http://127.0.0.1:9910

# Add group (POST /api/stores/{sid}/groups)
curl -X POST http://127.0.0.1:9920/api/stores/1/groups?server=http://127.0.0.1:9910 \
  -H "Content-Type: application/json" \
  -d '{"group_id":2,"replica_id":2}'

# Remove group (DELETE /api/stores/{sid}/groups/{gid})
curl -X DELETE http://127.0.0.1:9920/api/stores/1/groups/2?server=http://127.0.0.1:9910

# List remote replicas (GET /api/stores/{sid}/groups/{gid}/remotes)
curl http://127.0.0.1:9920/api/stores/1/groups/1/remotes?server=http://127.0.0.1:9910

# Add remote replicas (POST /api/stores/{sid}/groups/{gid}/remotes)
curl -X POST http://127.0.0.1:9920/api/stores/1/groups/1/remotes?server=http://127.0.0.1:9910 \
  -H "Content-Type: application/json" \
  -d '[{"replica_id":3,"endpoint":"10.0.0.1:9930"}]'

# Remove remote replica (DELETE /api/stores/{sid}/groups/{gid}/remotes/{rid})
curl -X DELETE http://127.0.0.1:9920/api/stores/1/groups/1/remotes/3?server=http://127.0.0.1:9910
```

All endpoints return JSON. Upstream errors map to `502 Bad Gateway` to distinguish console bugs from server errors.

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

# List/scan (not yet implemented on server)
crowkv kv list --store-id 1 --group-id 1 --prefix "" --limit 100
crowkv kv scan --store-id 1 --group-id 1 --prefix "" --limit 100

# Target a specific server (by URL or registry id)
crowkv --server http://127.0.0.1:9910 kv get --store-id 1 --group-id 1 --key mykey

# JSON output
crowkv --json kv get --store-id 1 --group-id 1 --key mykey
crowkv --json kv put --store-id 1 --group-id 1 --key mykey --value myvalue

# Idempotency tracking (client_id, seq)
crowkv kv put --store-id 1 --group-id 1 --key mykey --value myvalue --client-id 1 --seq 1
```

**Note**: `get` is a local follower read in V1 and may return stale data. Not-found exits with code 3 for scriptability. `list`/`scan` return a clear error until the server implements prefix scan.

### HTTP API (Web Backend)

The console web server proxies KV requests to upstream `crowkv-server` gRPC endpoints:

```bash
# Get a key (GET /api/stores/{sid}/groups/{gid}/kv/get)
curl "http://127.0.0.1:9920/api/stores/1/groups/1/kv/get?server=http://127.0.0.1:9910&key=mykey"
curl "http://127.0.0.1:9920/api/stores/1/groups/1/kv/get?server=http://127.0.0.1:9910&key_hex=6d796b6579"

# Put a key (POST /api/stores/{sid}/groups/{gid}/kv/put)
curl -X POST "http://127.0.0.1:9920/api/stores/1/groups/1/kv/put?server=http://127.0.0.1:9910" \
  -H "Content-Type: application/json" \
  -d '{"key":"mykey","value":"myvalue","client_id":0,"seq":0}'
curl -X POST "http://127.0.0.1:9920/api/stores/1/groups/1/kv/put?server=http://127.0.0.1:9910" \
  -H "Content-Type: application/json" \
  -d '{"key_hex":"6d796b6579","value_hex":"6d7976616c7565","client_id":0,"seq":0}'

# Delete a key (POST /api/stores/{sid}/groups/{gid}/kv/delete)
curl -X POST "http://127.0.0.1:9920/api/stores/1/groups/1/kv/delete?server=http://127.0.0.1:9910" \
  -H "Content-Type: application/json" \
  -d '{"key":"mykey","client_id":0,"seq":0}'
```

All endpoints support both UTF-8 (`key`, `value`) and hex (`key_hex`, `value_hex`) for binary safety. Responses include both `value_utf8` and `value_hex` fields. Endpoint resolution rewrites the upstream's `0.0.0.0:N` listen_addr to use the management URL's host for remote access.

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

# Target a specific server (by URL or registry id)
crowkv --server http://127.0.0.1:9910 bench run read --store-id 1 --group-id 1

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

**Note**: The `list` workload always reports `error_rate=1.0` because the underlying `KvClient::scan` is a stub (C6 gap). The CLI accepts `bench run list` for wiring exercise, but results are only useful as a "did the path connect" signal until the server adds prefix scan.

## C8 Status

C8 migrates Swagger UI from `crowkv-server` to `crowkv-console-web` and removes the `swagger-ui` Cargo feature from `crowkv-server`.

### Swagger UI

The console now serves a vendored Swagger UI at `/api/swagger/`. This provides an interactive API explorer for the `crowkv-server` management API.

```bash
# Access Swagger UI (uses default registered server or ?server=<url>)
open http://127.0.0.1:9920/api/swagger/

# Access OpenAPI JSON spec (proxied from upstream server)
curl "http://127.0.0.1:9920/api/openapi.json?server=http://127.0.0.1:9910"
curl "http://127.0.0.1:9920/api/openapi.json"  # uses default registered server
```

**Vendored assets**: Swagger UI 5.17.14 is committed under `crowkv-console/static/swagger-ui/`. The directory includes:
- `swagger-ui.css`, `swagger-ui-bundle.js`, `swagger-ui-standalone-preset.js`
- Favicons and OAuth2 redirect page
- Hand-written `index.html` that requests `/api/openapi.json` with optional `?server=` propagation

**Bumping Swagger UI**: To upgrade to a new version:
1. Download the new version from unpkg.com (e.g., `curl -O https://unpkg.com/swagger-ui-dist@5.17.14/swagger-ui-bundle.js`)
2. Replace the files in `crowkv-console/static/swagger-ui/`
3. Update the version in `static/swagger-ui/VERSION`
4. Update the `index.html` script tags if the bundle names changed

**crowkv-server changes**: The `swagger-ui` Cargo feature and `utoipa-swagger-ui` dependency have been removed. The `/openapi.json` endpoint is now unconditional, and all `ToSchema` derives are kept. The `examples/openapi-export.rs` binary is now unconditional.

## Phases

- **C0**: Skeleton (workspace + crates compile)
- **C1**: Core + Read-Only Observation
- **C2**: Multi-Server + Registry
- **C3**: Simulated Hardware (local spawn)
- **C4**: SSH Transport (russh)
- **C5**: Cluster Management Plane
- **C6**: KV Operations
- **C7**: Bench (CLI only)
- **C8**: Polish + Swagger

See [`doc/plan-console.md`](../doc/plan-console.md) for the full implementation plan.
