<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Cluster — Unify Port Usage & Test Port Dispatcher (R118)

This draft covers the implementation design for rescheduling all CROWDB
service ports to a unified >10000 scheme, wiring every server CLI to
accept explicit per-listener port flags with `ports.rs` defaults,
rejecting port 0 everywhere, replacing all existing port-picking code
(`test_ports` module, E2E fixture counter) with a single
flock-coordinated claim-file prober, and establishing group-0 as the
service discovery root.

- Backlog doc: `doc/backlog/R118-cluster-unify-port-usage.md`
- Root design doc: `doc/design/protocol/design-crowdb-protocol.md`
  (new "Port allocation" section — folding target).
- Already landed: `ports.rs` defines the current scheme (9910-9990,
  28001-28400); `test_ports` module (`unique_test_port*`); E2E fixture
  `freePort()` / `freePortRange()` counter; kv-server RPC port collapse
  (single crowdb-rpc server hosts both consensus + client handlers).

Architecture decisions and rationale are in the root design; this doc
does not repeat them.

## 1. Port reschedule — `ports.rs` rewrite

### 1.1 Why

The current port ranges (9910-9990, 28001-28400) are small (10-200
ports per service), use stride-2 paired logic for diskdb/chunkdb, and
have dead constants (`KV_RPC_BASE`, `KV_CLIENT_RPC_BASE`) from the
RPC port collapse. The new scheme gives each service type 1000 ports
with a shared prefix, all >10000, all stride 1 — simpler, more room
for test parallelism, and the prefix makes ports human-recognizable.

### 1.2 New port map

```
kv-server   10000-10999  (prefix 10)
  mgmt      10000-10099  (stride 1)
  listen    10100-10199  (stride 1; hosts consensus + client RPC)
  spare     10200-10999

diskdb      11000-11999  (prefix 11)
  listen    11000-11099  (stride 1)
  http      11100-11199  (stride 1)
  rpc       11200-11299  (stride 1)
  spare     11300-11999

chunkdb     12000-12999  (prefix 12)
  listen    12000-12099  (stride 1; vestigial — see §4)
  http      12100-12199  (stride 1)
  rpc       12200-12299  (stride 1)
  spare     12300-12999

diskio      13000-13999  (prefix 13)
  rpc       13000-13099  (stride 1)
  spare     13100-13999

web         14000-14999  (prefix 14)
  http      14000-14099  (stride 1)
  spare     14100-14999
```

### 1.3 Constant + enum changes

```rust
// kv-server
pub const KV_SERVER_MGMT_BASE: u16   = 10000;
pub const KV_SERVER_LISTEN_BASE: u16 = 10100;
// KV_RPC_BASE / KV_CLIENT_RPC_BASE — REMOVED (RPC port collapse accepted)

// diskdb
pub const DISKDB_LISTEN_BASE: u16 = 11000;
pub const DISKDB_HTTP_BASE: u16   = 11100;
pub const DISKDB_RPC_BASE: u16    = 11200;

// chunkdb
pub const CHUNKDB_LISTEN_BASE: u16 = 12000;
pub const CHUNKDB_HTTP_BASE: u16   = 12100;
pub const CHUNKDB_RPC_BASE: u16    = 12200;

// diskio — NEW
pub const DISKIO_RPC_BASE: u16 = 13000;

// web
pub const WEB_BASE: u16 = 14000;
```

`ServicePort` enum:

- Remove `KvServerRpc`, `KvServerClientRpc` (dead code).
- Add `DiskdbRpc` (was missing — `DISKDB_RPC_BASE` existed but no
  variant).
- Add `DiskioRpc` (new service).

```rust
pub enum ServicePort {
    KvServerMgmt,
    KvServerListen,
    DiskdbListen,
    DiskdbHttp,
    DiskdbRpc,      // NEW
    ChunkdbListen,
    ChunkdbHttp,
    ChunkdbRpc,
    DiskioRpc,      // NEW
    Web,
}
```

`stride()` returns `1` for all variants — no more stride-2 paired
logic. The `DiskdbListen | DiskdbHttp | ChunkdbListen | ChunkdbHttp`
paired branch is removed.

### 1.4 Migration

Hard cutover: search the codebase for all references to the old port
constants and literals (9910, 9920, 9931, 9941, 9942, 9961, 9971, 9972,
28001, 28101, 28201) and update them in one pass. Fix regressions as
they surface. No rolling-update coexistence.

Files with hardcoded port literals or references to old constants:
- `lib/crowdb-protocol/src/ports.rs` — constants themselves
- `lib/crowdb-protocol/src/lib.rs` — re-exports (remove `KV_RPC_BASE`,
  `KV_CLIENT_RPC_BASE`)
- `lib/crowdb-protocol/tests/ports_test.rs` — test assertions
- `app/crowdb-kv-server/src/cli.rs` — `default_value_t`
- `app/crowdb-diskdb/src/ddb_config.rs` — `ServerConfig::default()`
  string literals (`"0.0.0.0:9941"` etc.)
- `app/crowdb-chunkdb/src/chunkdb_config.rs` — same
- `app/crowdb-web/src/main.rs` — `default_value_t = WEB_BASE`
- `app/crowdb-cli/src/main.rs` — literal `9910` → `KV_SERVER_MGMT_BASE`
- `lib/crowdb-console-shared/src/lifecycle.rs` —
  `resolve_diskdb_config_path` derives `http_port = rpc_port + 1`,
  `rpc_listen_port = rpc_port + 2` (see §5)
- `lib/crowdb-console-shared/src/test_ports.rs` — deleted entirely (§6)
- `app/crowdb-web/ui/e2e/fixtures/crowClusterDeployer.ts` —
  `PORT_BASE`/`PORT_CEILING` deleted (§7)
- Design docs: `design-crowdb-kv-rpc.md` §4,
  `design-crowdb-kv-rpc-client.md` §1 §4 — update to collapsed reality

### 1.5 Edge cases

- Old config files on disk with hardcoded `9941` etc. — the config
  defaults change; existing config files with explicit ports still
  work (they override defaults). Only auto-generated configs
  (lifecycle.rs) use the new defaults.
- `ports_test.rs` must be rewritten to assert the new constants and
  stride-1 for all variants.

## 2. `crowdb-kv-server` CLI + start path

### 2.1 Why

`parse_port_list` accepts port 0 (validates u16 range only). `KvServer::start`
(lines 68-71) explicitly supports port 0 for OS-assigned ports via a
transient `TcpListener::bind`. Both contradict the "no port 0" invariant.

### 2.2 CLI changes

`app/crowdb-kv-server/src/cli.rs`:

- `--management-port`: add `value_parser` that rejects 0.
- `--ports`: `parse_port_list` adds a 0-rejection check — reject any
  parsed value that is 0 with a clear error message.

```rust
pub fn parse_port_list(input: &str) -> Result<Vec<u16>, String> {
    let ids = parse_id_list(input)?;
    ids.into_iter()
        .map(|v| {
            let p = u16::try_from(v)
                .map_err(|_| format!("port out of range (0-65535): {v}"))?;
            if p == 0 {
                return Err("port 0 is not allowed; pass an explicit non-zero port".into());
            }
            Ok(p)
        })
        .collect()
}
```

### 2.3 Start path changes

`lib/crowdb-kv/src/cluster/kv_server.rs`:

- Remove the transient `TcpListener::bind` block (lines 68-83) — the
  crowdb-rpc `RpcServer::listen` binds directly. Port 0 is no longer
  supported; the listen addr must be an explicit non-zero port.
- Remove `start_rpc_server` (lines 252-257) and
  `start_client_rpc_server` (lines 320-322) no-op stubs.
- `start()` binds `self.listen_addr` directly via `RpcServer::listen`.
  Bind failure returns a hard error with the listen addr and a
  "stop the conflicting process" hint.

### 2.4 Edge cases

- `listen_addr` with port 0 reaches `start()` — rejected at CLI parse
  (§2.2), so `start()` never sees port 0 in normal flow. If called
  directly (library API), `start()` should also reject port 0 with a
  clear error.

## 3. `crowdb-diskdb` CLI unification

### 3.1 Why

diskdb takes listen addresses from TOML config, not CLI flags with
`ports.rs` defaults. `rpc_listen_addr` has no CLI override. Port 0 is
not rejected. The paired-port derivation (`http = listen + 1`) is
being broken (D5).

### 3.2 CLI changes

`app/crowdb-diskdb/src/main.rs`:

Add three new CLI flags that override config:

```rust
#[arg(long)]
listen_port: Option<u16>,   // overrides config.server.listen_addr port

#[arg(long)]
http_port: Option<u16>,     // overrides config.server.http_listen_addr port

#[arg(long)]
rpc_port: Option<u16>,      // overrides config.server.rpc_listen_addr port
```

Each flag, when present, overrides the port in the corresponding
`*_addr` config field (preserving the bind address, replacing only
the port). When absent, the config value stands.

Add `value_parser` that rejects 0 on all three flags.

### 3.3 Config default changes

`app/crowdb-diskdb/src/ddb_config.rs` `ServerConfig::default()`:

```rust
listen_addr:      "0.0.0.0:11000",   // DISKDB_LISTEN_BASE
http_listen_addr: "0.0.0.0:11100",   // DISKDB_HTTP_BASE
rpc_listen_addr:  "0.0.0.0:11200",   // DISKDB_RPC_BASE
kv_server_mgmt_seeds: ["http://127.0.0.1:10000"]  // KV_SERVER_MGMT_BASE
```

### 3.4 Edge cases

- Operator passes `--listen-port` but not `--http-port` → listen
  changes, http stays at config default. No pairing enforced (D5).
- Config file with explicit `listen_addr = "0.0.0.0:9941"` (old port)
  → still works (config overrides default), but the operator should
  update the config file. Not a hard error — just a stale config.

## 4. `crowdb-chunkdb` CLI unification

### 4.1 Why

Same shape as diskdb, plus `listen_addr` is vestigial (parsed and
logged but never bound — the RPC server binds `rpc_listen_addr`, the
HTTP server binds `http_listen_addr`).

### 4.2 CLI changes

Same as diskdb (§3.2): add `--listen-port`, `--http-port`,
`--rpc-port` with 0-rejection.

### 4.3 Vestigial `listen_addr` removal

Remove `listen_addr` from `ServerConfig` and `--listen-addr` from the
CLI. The field is never bound; removing it eliminates confusion. The
RPC server binds `rpc_listen_addr`; the HTTP server binds
`http_listen_addr`. If a future listener is needed, it gets its own
named field.

### 4.4 Config default changes

`app/crowdb-chunkdb/src/chunkdb_config.rs` `ServerConfig::default()`:

```rust
// listen_addr — REMOVED
http_listen_addr: "0.0.0.0:12100",   // CHUNKDB_HTTP_BASE
rpc_listen_addr:  "0.0.0.0:12200",   // CHUNKDB_RPC_BASE
kv_server_mgmt_seeds: ["http://127.0.0.1:10000"]
```

### 4.5 Edge cases

- Config file with `listen_addr` still present → serde ignores it
  (field removed from struct). No error, just a stale field.

## 5. `lifecycle.rs` paired-port derivation removal

### 5.1 Why

`resolve_diskdb_config_path` (lifecycle.rs ~line 716) derives
`http_port = rpc_port + 1` and `rpc_listen_port = rpc_port + 2` from a
single `rpc_port`. With the paired-port invariant broken (D5), each
port must be independent.

### 5.2 Changes

`resolve_diskdb_config_path` signature changes to accept all three
ports independently:

```rust
fn resolve_diskdb_config_path(
    workspace_dir: &Path,
    listen_port: u16,
    http_port: u16,
    rpc_port: u16,
    kv_server_mgmt_seeds: &[String],
) -> Result<PathBuf>
```

The caller (`deploy_diskdb` in lifecycle.rs ~line 818) must pass all
three ports. The `DeployDiskdbRequest` struct gains `http_port` and
`listen_port` fields (or the caller derives them from the prober).

### 5.3 Edge cases

- Caller passes the same port for listen and http → config writes
  both with the same port → diskdb fails to bind the second one. This
  is a caller bug, not a lifecycle bug — the prober should hand out
  distinct ports.

## 6. `crowdb-diskio` CLI unification

### 6.1 Why

diskio is a service (standalone C++ binary, same tier as kv-server,
diskdb, chunkdb). It parses its own CLI args via
`DioConfig::parse_args`, including `--port` (currently required, no
default, accepts 0). The test harness (`diskio.rs` line 159) passes
`--port 0` to get OS-assigned ports. There is no `ports.rs`
integration. (The C++ *libraries* — crowdb-rpc, crowdb-tree — receive
config from Rust and don't parse config themselves; diskio is
different: it's a service binary.)

### 6.2 Changes

`app/crowdb-diskio/src/dio_config.h`:

- Change `listen_port` default from `0` to `13000` (the
  `DISKIO_RPC_BASE` value). Since diskio is a C++ binary that can't
  import the Rust constant, the literal `13000` is hardcoded here
  with a comment referencing `DISKIO_RPC_BASE` in `ports.rs`. The two
  must stay in sync via the comment contract. (A generated header
  from Rust constants could be added later if this proves fragile.)

`app/crowdb-diskio/src/dio_config.cpp`:

- `validate()`: reject port 0:

```cpp
if (listen_port <= 0 || listen_port > 65535) {
    err = "invalid listen port (must be 1-65535, 0 is not allowed)";
    return false;
}
```

`lib/crowdb-test-harness/src/diskio.rs`:

- Stop passing `--port 0` (line 159). Allocate a port via
  `port_alloc::alloc_port(ServicePort::DiskioRpc, instance, &cfg)`
  and pass `--port <allocated>`.

### 6.3 Edge cases

- `--port` not passed → uses default 13000. If 13000 is already in
  use, bind fails with a clear error.
- C++→Rust constant sync: `13000` in `dio_config.h` must match
  `DISKIO_RPC_BASE` in `ports.rs`. The comment is the contract; a
  build-time check is not practical across languages for a standalone
  binary.

## 7. `crowdb-web` / `crowdb-cli` port flags

### 7.1 Why

`crowdb-web --port 0` is accepted (no 0-rejection). `crowdb-cli
--sysmd-port` uses literal `9910` instead of `KV_SERVER_MGMT_BASE`.

### 7.2 Changes

`app/crowdb-web/src/main.rs`:

- Add `value_parser` on `--port` that rejects 0.

`app/crowdb-cli/src/main.rs`:

- Change `default_value_t = 9910` to `default_value_t =
  KV_SERVER_MGMT_BASE` (which is now `10000`). Add `use
  crowdb_protocol::KV_SERVER_MGMT_BASE;` import.

## 8. Port prober + claim file (`port_alloc`)

### 8.1 Why

The existing `test_ports` module binds `127.0.0.1:0` to get
OS-assigned ports. D6 rejects port 0 everywhere. The E2E fixture uses
a naive monotonic counter that doesn't probe. A single
flock-coordinated prober replaces both, using the `ServicePort` scheme
ports with actual bind probes.

### 8.2 Library API

`lib/crowdb-protocol/src/port_alloc.rs`:

```rust
/// Configuration for the port allocator.
pub struct PortAllocConfig {
    /// Workspace root directory. The claim file lives at
    /// `<root>/.crowdb-port-alloc/claims`. Default: current
    /// directory (project root).
    pub root: PathBuf,
    /// Port offset for multi-session isolation. Default: 0.
    /// Each service's base port is shifted by `offset`:
    /// `ServicePort::port(instance) + offset`.
    pub offset: u16,
}

/// Allocate a single port for the given service type + instance.
/// Probes the system (bind probe) for a free port in the service's
/// range, not already in the claim file. Writes the port to the
/// claim file under flock.
pub fn alloc_port(service: ServicePort, instance: u16, cfg: &PortAllocConfig) -> Result<u16>;

/// Allocate `count` consecutive ports for a service type starting at
/// `instance`. All ports are in the same service range and are
/// pairwise consecutive.
pub fn alloc_port_range(service: ServicePort, instance: u16, count: u16, cfg: &PortAllocConfig) -> Result<Vec<u16>>;

/// Mark a port as "tried-and-failed" in the claim file so the next
/// probe skips it. Called by the test harness when a server bind
/// fails (D1 TOCTOU mitigation).
pub fn mark_failed(port: u16, cfg: &PortAllocConfig) -> Result<()>;

/// Reset the claim file (delete it). Called by test shell between
/// runs to avoid exhaustion.
pub fn reset_claims(cfg: &PortAllocConfig) -> Result<()>;
```

### 8.3 Mechanism

a. `flock` the claim file at `<root>/.crowdb-port-alloc/claims`.
b. Read the file — one port per line, the set of already-claimed
   ports.
c. Compute the candidate port: `ServicePort::port(instance) + offset`.
d. Bind-probe `127.0.0.1:<candidate>` — if bind succeeds, the port is
   free. Drop the probe listener immediately.
e. If the candidate is in the claim file or bind fails, try the next
   instance index (instance + 1, instance + 2, ...) up to the range
   limit (100 ports per sub-range). If exhausted, return an error.
f. Write the selected port to the claim file.
g. Unlock.

### 8.4 CLI binary

`app/crowdb-port-alloc/` — a small `clap` binary that wraps the
library:

```rust
#[derive(Parser)]
struct Cli {
    #[arg(long)]
    root: Option<PathBuf>,    // default: current dir

    #[arg(long, default_value_t = 0)]
    offset: u16,

    #[arg(long)]
    service: String,          // "kv-mgmt", "kv-listen", "diskdb-rpc", etc.

    #[arg(long, default_value_t = 0)]
    instance: u16,

    #[arg(long, default_value_t = 1)]
    count: u16,               // for range allocation

    #[arg(long)]
    reset: bool,              // delete claim file and exit
}
```

Output: the allocated port number(s) to stdout (one per line for
range), or an error message to stderr. Shell scripts and the TS
fixture call it via `child_process.execSync` / `$()`.

### 8.5 Edge cases

- Two probers on same host, same root → flock serializes; second sees
  first's claimed port and skips it.
- Rapid restart within TIME_WAIT → bind probe skips TIME_WAIT ports;
  prober tries the next instance index.
- Claim file stale after crash → `reset_claims` (or `rm` the file)
  between runs.
- Claim file on NFS → flock unreliable; workspace root must be local
  (documented constraint).
- Offset overflow → `base + offset + instance * stride` exceeds u16 →
  return error.

## 9. Test harness integration

### 9.1 Why

~19 test files call `test_ports::unique_test_port*`. The E2E fixture
uses `freePort()` / `freePortRange()`. All must migrate to the prober.

### 9.2 Rust harness migration

Replace all `test_ports::unique_test_port()` calls with
`port_alloc::alloc_port(ServicePort::*, instance, &cfg)`. Replace
`unique_test_ports(count)` with `alloc_port_range`. Replace
`unique_test_port_range(count)` with `alloc_port_range`.

The test harness (`cluster_deployer.rs` / `local_deploy` in
`ops/cluster.rs`) gains a TOCTOU retry loop (D1):

a. Allocate port(s) via `port_alloc::alloc_port`.
b. Start the server with the allocated port(s).
c. If the server exits with a bind-failure error, call
   `port_alloc::mark_failed(port, &cfg)` and retry from step (a).
d. Max 3 retries; if still failing, return an error to the test.

### 9.3 E2E fixture migration

`app/crowdb-web/ui/e2e/fixtures/crowClusterDeployer.ts`:

- Delete `PORT_BASE`, `PORT_CEILING`, `nextPort`, `freePort()`,
  `freePortRange()`.
- Add a helper that shells out to `crowdb-port-alloc`:

```ts
import { execSync } from 'node:child_process';

function allocPort(service: string, instance: number = 0, offset: number = 0): number {
  const out = execSync(
    `crowdb-port-alloc --service ${service} --instance ${instance} --offset ${offset}`,
    { encoding: 'utf-8' },
  );
  return parseInt(out.trim(), 10);
}
```

- `deployKvServers` calls `allocPort("kv-mgmt", i)` and
  `allocPort("kv-listen", i)`.
- `deployDiskdbInstances` calls `allocPort("diskdb-rpc", i)`,
  `allocPort("diskdb-listen", i)`, `allocPort("diskdb-http", i)`
  independently (no more `freePortRange(3)`).
- `workers: 1` can be lifted to `workers > 1` once migration is
  verified.

### 9.4 `test_ports` module deletion

`lib/crowdb-console-shared/src/test_ports.rs` is deleted. The
`lib/crowdb-console-shared/src/lib.rs` re-export is removed. All
callers are migrated in §9.2.

### 9.5 Edge cases

- Test runs in CI without `crowdb-port-alloc` binary built → the
  E2E fixture must build it first or fail with a clear message.
- Parallel test workers on same host → each worker uses a different
  `--offset` (passed via env var or Playwright config).

## 10. Group-0 service discovery (split to new requirement)

### 10.1 Why

D4 requires every service to register its IP + port to group-0
sysdata on startup, and clients to discover peers via group-0. Even
single-host deployments use this style.

### 10.2 Current state

`crowdb-kv-client` has `ServiceRegistryClient` with keep-alive
heartbeat. `crowdb-kv-server` has `--instance-id` and
`--keepalive-interval` for service-registry registration. The
"group-0 is the discovery root" concept is not used consistently —
some code hardcodes `127.0.0.1:9910` instead of querying group-0.

### 10.3 Scope split

Group-0 service discovery is a larger scope item that spans beyond
the port reschedule. **It will be split into a new requirement**
(per user decision). R118 ships the port reschedule + prober + CLI
unification (§1-9); the new requirement covers:

a. **Service registration** — every server (kv-server, diskdb,
   chunkdb, diskio) calls `ServiceRegistryClient::register` on
   startup with its actual IP + port. The registration includes
   service type, instance ID, listen addresses.
b. **Client discovery** — clients (crowdb-cli, crowdb-web, test
   harnesses) query group-0 via `KVClusterMetaClient` to discover
   peer addresses instead of hardcoding `127.0.0.1:<port>`.
c. **Test exception** — simple test cases that don't use group-0 can
   pass service info via parameters directly (e.g. `--sysmd-ip
   127.0.0.1 --sysmd-port 10000`).

The new requirement must specify the sysdata keys, registration flow,
and client query path. R118's `ports.rs` changes (the famous mgmt
port 10000 for group-0 bootstrap) lay the groundwork — the new
requirement builds the discovery layer on top.

### 10.4 Edge cases (for the new requirement)

- group-0 not yet started when a service tries to register → retry
  with backoff; the service starts listening but registration is
  pending.
- group-0 leader change → re-register with the new leader.

## Scope

- `lib/crowdb-protocol/src/ports.rs` — rewrite all constants, enum,
  stride logic.
- `lib/crowdb-protocol/src/lib.rs` — update re-exports (remove
  `KV_RPC_BASE`, `KV_CLIENT_RPC_BASE`; add `DISKIO_RPC_BASE`).
- `lib/crowdb-protocol/src/port_alloc.rs` — NEW: prober library.
- `lib/crowdb-protocol/tests/ports_test.rs` — rewrite assertions.
- `app/crowdb-port-alloc/` — NEW: CLI binary wrapping `port_alloc`.
- `app/crowdb-kv-server/src/cli.rs` — reject port 0 on
  `--management-port` and `--ports`.
- `lib/crowdb-kv/src/cluster/kv_server.rs` — remove port-0 branch,
  remove no-op stubs.
- `app/crowdb-diskdb/src/main.rs` — add `--listen-port`,
  `--http-port`, `--rpc-port` flags.
- `app/crowdb-diskdb/src/ddb_config.rs` — update default ports.
- `app/crowdb-chunkdb/src/main.rs` — add port flags, remove
  `--listen-addr`.
- `app/crowdb-chunkdb/src/chunkdb_config.rs` — update defaults,
  remove `listen_addr`.
- `app/crowdb-diskio/src/dio_config.h` — default port 13000.
- `app/crowdb-diskio/src/dio_config.cpp` — reject port 0 in
  `validate()`.
- `app/crowdb-web/src/main.rs` — reject port 0 on `--port`.
- `app/crowdb-cli/src/main.rs` — replace literal `9910` with
  `KV_SERVER_MGMT_BASE`.
- `lib/crowdb-console-shared/src/test_ports.rs` — DELETE.
- `lib/crowdb-console-shared/src/lib.rs` — remove `test_ports`
  re-exports.
- `lib/crowdb-console-shared/src/lifecycle.rs` —
  `resolve_diskdb_config_path` accept independent ports.
- `lib/crowdb-console-shared/src/ops/cluster.rs` — migrate
  `local_deploy` / `deploy_servers` to `port_alloc`, add TOCTOU
  retry.
- `app/crowdb-web/ui/e2e/fixtures/crowClusterDeployer.ts` — replace
  `freePort`/`freePortRange` with `crowdb-port-alloc` calls.
- ~19 test files calling `unique_test_port*` — migrate to
  `port_alloc`.
- Design docs: `design-crowdb-kv-rpc.md` §4,
  `design-crowdb-kv-rpc-client.md` §1 §4 — update to collapsed
  reality.
- `doc/design/protocol/design-crowdb-protocol.md` — add "Port
  allocation" section (folding target).

## Complexity

**High.** The port reschedule touches every server binary and every
test harness in the workspace — a wide-but-shallow change. The prober
library is new code (flock + bind probe + claim file) but
mechanically simple. The main implementation challenge is the
migration of ~19 test files without breaking CI: each file must be
updated, tested, and verified in sequence. Group-0 service discovery
(§10) is split to a new requirement and is not part of R118's
complexity.

## Test Design

### Unit tests

- **ports_test.rs** — assert all new `*_BASE` constants match the
  port map. Assert `ServicePort::stride()` returns 1 for all
  variants. Assert `ServicePort::port(0)` == base for each variant.
  Assert `ServicePort::port(5)` == base + 5 (stride 1). Assert
  `DiskdbRpc` and `DiskioRpc` variants exist and compute correct
  ports.
- **parse_port_list 0-rejection** — `parse_port_list("0")` returns
  error; `parse_port_list("10000,0")` returns error;
  `parse_port_list("10000")` returns Ok.
- **port_alloc::alloc_port** — single allocation returns a free port
  in the service's range. Second allocation with same instance
  returns an error (already claimed). Allocation with instance + 1
  returns a different port.
- **port_alloc::alloc_port_range** — allocates `count` consecutive
  ports. All are in the claim file after allocation.
- **port_alloc::mark_failed** — after marking a port as failed, the
  next `alloc_port` for the same instance skips it.
- **port_alloc::reset_claims** — after reset, the claim file is empty
  and all ports are available again.
- **port_alloc offset** — with offset 100, `alloc_port(KvServerMgmt,
  0, offset=100)` returns a port in `[10100, 10200)`.
- **diskio validate** — `listen_port = 0` fails validation;
  `listen_port = 13000` passes; `listen_port = 70000` fails.

### E2E tests

- **kv-server default ports** — start `crowdb-kv-server --root
  /tmp/n1` with no port flags → mgmt on 10000, listen on 10100. E2E.
- **kv-server port 0 rejected** — `crowdb-kv-server --root /tmp/n1
  --management-port 0` → CLI parse error. Unit test.
- **kv-server bind failure** — start a server on 10000, start a
  second on 10000 → hard error with "stop the conflicting process"
  hint. Integration test.
- **diskdb default ports** — start with no port flags → listen 11000,
  http 11100, rpc 11200. E2E.
- **diskdb independent ports** — `--listen-port 11005 --http-port
  11110` → listens on those ports, no pairing. E2E.
- **chunkdb vestigial listen_addr removed** — config without
  `listen_addr` starts fine; `--listen-addr` flag is gone. Unit test
  (static check).
- **diskio default port** — start with no `--port` → listens on
  13000. E2E.
- **parallel cluster start** — 3 kv-server + 3 diskdb via prober → 0
  bind failures across 3 consecutive runs. E2E.
- **E2E fixture consolidation** — grep `freePort` / `PORT_BASE` in
  `app/crowdb-web/ui/e2e/` returns nothing. Static check.
- **E2E parallel** — representative E2E flow with `workers > 1` → 0
  port-conflict errors. E2E.

## Module Structure

```
lib/crowdb-protocol/src/
  ports.rs              — rewritten constants, enum, stride
  port_alloc.rs         — NEW: flock + bind-probe prober library
  lib.rs                — updated re-exports
lib/crowdb-protocol/tests/
  ports_test.rs         — rewritten assertions
app/crowdb-port-alloc/  — NEW: CLI binary
  src/main.rs           — clap CLI wrapping port_alloc
app/crowdb-kv-server/src/
  cli.rs                — port 0 rejection
lib/crowdb-kv/src/cluster/
  kv_server.rs          — remove port-0 branch, remove no-op stubs
app/crowdb-diskdb/src/
  main.rs               — add --listen-port, --http-port, --rpc-port
  ddb_config.rs         — update default ports
app/crowdb-chunkdb/src/
  main.rs               — add port flags, remove --listen-addr
  chunkdb_config.rs     — update defaults, remove listen_addr
app/crowdb-diskio/src/
  dio_config.h          — default port 13000
  dio_config.cpp        — reject port 0
app/crowdb-web/src/
  main.rs               — reject port 0
app/crowdb-cli/src/
  main.rs               — replace literal 9910 with constant
lib/crowdb-console-shared/src/
  test_ports.rs         — DELETED
  lib.rs                — remove test_ports re-exports
  lifecycle.rs          — independent ports in resolve_diskdb_config_path
  ops/cluster.rs        — migrate to port_alloc, add TOCTOU retry
app/crowdb-web/ui/e2e/fixtures/
  crowClusterDeployer.ts — replace freePort with crowdb-port-alloc
```

## Config Extensions

- `DdbConfig::ServerConfig` — default ports change (11000/11100/11200).
  No new fields.
- `ChunkdbConfig::ServerConfig` — `listen_addr` field removed.
  Default ports change (12100/12200).
- `DioConfig` — `listen_port` default changes from 0 to 13000.

## Server Wiring

1. `crowdb-port-alloc` binary is built first (`pixi run cargo build
   -p crowdb-port-alloc`).
2. `ports.rs` constants are updated — all downstream crates pick up
   new defaults via `default_value_t`.
3. Each server binary's CLI changes are independent — can be done in
   any order after `ports.rs`.
4. `test_ports` deletion is last — after all callers are migrated to
   `port_alloc`.
5. E2E fixture migration is last — after `crowdb-port-alloc` binary
   is available.

## Open Questions

1. **Group-0 discovery scope** — DECIDED: split to R128
   (`doc/backlog/R128-cluster-group0-service-discovery.md`).
   R118 ships the port reschedule + prober + CLI unification (§1-9)
   and lays the groundwork (famous mgmt port 10000 for group-0
   bootstrap). The full group-0 discovery implementation (sysdata
   keys, service self-registration, client query path) is R128.

2. **C++→Rust constant sync** — RESOLVED. The C++ *libraries*
   (crowdb-rpc, crowdb-tree) receive config from Rust and don't parse
   config themselves — no constant sync needed for them. diskio is a
   *service* (standalone C++ binary, same tier as kv-server/diskdb/
   chunkdb) that parses its own CLI args, so it hardcodes
   `DISKIO_RPC_BASE` (13000) in `dio_config.h` with a comment
   referencing `ports.rs`. The comment is the contract; a build-time
   check is not practical for a standalone binary. If this proves
   fragile, a generated header from Rust constants could be added
   later.