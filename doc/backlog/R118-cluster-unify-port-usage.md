<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R118: cluster — Unify Port Usage & Test Port Dispatcher

**Problem**

CROWDB runs many servers (crowdb-kv-server, crowdb-diskdb, crowdb-chunkdb,
crowdb-web), each listening on one or more ports. During tests, multiple
instances of the same server are started and torn down in short windows,
so port conflict (`Address already in use`) is a common failure that has
already cost real debugging/CI time. There is no single source of truth
for "which instance gets which port" and no dispatcher, so instances
collide on the famous ports or on whatever a test harness hardcodes.

**Current behavior + impact**

- `lib/crowdb-protocol/src/ports.rs` already defines base ports + stride
  rules + a `ServicePort` enum for all current services (kv-server
  mgmt/listen/consensus-rpc/client-rpc, diskdb listen/http, chunkdb
  listen/http/rpc, web). But adoption is incomplete and inconsistent.
  Note: `DISKDB_RPC_BASE` is defined as a constant but `DiskdbRpc` is
  **missing** from the `ServicePort` enum — so `ServicePort::port()`
  cannot compute diskdb RPC ports today; item 1 must add the variant.
  The diskio server has landed (see below) but has no `DISKIO_*_BASE`
  constant and no `ServicePort::DiskioRpc` variant — another gap.
  - `crowdb-kv-server` CLI (`app/crowdb-kv-server/src/cli.rs`) exposes
    `--management-port` (default `KV_SERVER_MGMT_BASE`) and `--ports`
    (listen pool, optional free-form list) but has **no CLI flag** for the
    consensus RPC port (`KV_RPC_BASE`) or the client-facing RPC port
    (`KV_CLIENT_RPC_BASE`). However, the current implementation does not
    use those constants at all — see the "RPC port collapse" gap below.
    `parse_port_list` validates the u16 range but does **not** reject
    port 0, so `--ports 0` is accepted today.
  - **RPC port collapse** — `KvServer::start`
    (`lib/crowdb-kv/src/cluster/kv_server.rs` lines 56-161) now starts a
    **single** crowdb-rpc server on the listen port that hosts **both**
    consensus (`PxRpcService`) and client (`KvRpcService`) handlers on
    the same server. `start_rpc_server` and `start_client_rpc_server`
    are no-ops kept for backward compatibility (lines 252-257, 320-322).
    `KV_RPC_BASE` and `KV_CLIENT_RPC_BASE` are defined in `ports.rs` and
    referenced by the design docs (`design-crowdb-kv-rpc.md` §4,
    `design-crowdb-kv-rpc-client.md` §1, §4) but are **not used by any
    server code** — only by `ports.rs` itself, `lib.rs` re-exports, and
    `ports_test.rs`. The design docs still describe separate consensus
    and client-facing ports (`rpc_port + 100` and `rpc_port + 200`); the
    implementation has diverged. R118 must decide: restore the separate-
    port design (adding `--consensus-rpc-port` / `--client-rpc-port` CLI
    flags and splitting `start()` back into two servers) or update the
    design docs to match the collapsed single-port reality and remove
    the unused constants/enum variants. This is the single largest
    design-vs-impl gap R118 must resolve.
  - `KvServer::start` (lines 68-71) explicitly supports **port 0 for
    OS-assigned** ports: "Bind a TCP listener to determine the actual
    port (supports port 0 for OS-assigned)". This contradicts the
    project flow — servers must listen on explicit/famous ports; port 0
    is not allowed. An OS-assigned port is not reproducible and not
    discoverable by peer clients without extra plumbing, breaking the
    deterministic-port flow the cluster expects.
  - `crowdb-diskdb` (`app/crowdb-diskdb/src/main.rs`) takes listen
    addresses from TOML config (`rpc_listen_addr`, `http_listen_addr`,
    `listen_addr`), not from CLI flags with `ports.rs` defaults — so the
    famous ports are not enforced at the CLI boundary and an operator
    cannot override per-start without editing config. CLI has
    `--listen-addr` (overrides `listen_addr` — the main listener) and
    `--http-addr` (overrides `http_listen_addr`) address overrides, but
    **no CLI override for `rpc_listen_addr`** — the crowdb-rpc listener
    port can only be changed by editing the config file. No per-port
    flags; `0` is not rejected.
  - `crowdb-chunkdb` (`app/crowdb-chunkdb/src/main.rs`, now landed) has
    the same shape as diskdb: `--listen-addr` / `--http-addr` address
    overrides (no `rpc_listen_addr` override), TOML config with
    `CHUNKDB_*_BASE` defaults, no per-port flags, no `0` rejection.
    Additionally, `listen_addr` is **vestigial** in chunkdb — it is
    parsed (line 66) and logged (line 196) but never actually bound;
    the RPC server binds `rpc_listen_addr` (line 183-188) and the HTTP
    server binds `http_listen_addr` (line 194). The `--listen-addr` flag
    overrides a field that has no effect on the running server.
  - `crowdb-diskio` (`app/crowdb-diskio/src/dio_main.cpp` + `dio_config.cpp`,
    now landed — C++ binary) takes `--port <port>` (required, no
    default) and `--bind <addr>` from CLI args. `validate()` checks
    `0..=65535` but does **not** reject port 0. There is no
    `DISKIO_*_BASE` constant in `ports.rs`, no `ServicePort::DiskioRpc`
    variant, and no `ports.rs` integration at all — the diskio server is
    entirely outside the `ServicePort` scheme. R118 item 1 must add the
    diskio base + variant (or decide diskio is out of scope).
  - The console UI E2E fixture ships its **own ad-hoc port allocator**
    disconnected from `ports.rs`: the counter now lives in
    `app/crowdb-web/ui/e2e/fixtures/crowClusterDeployer.ts` (lines 13-41;
    `consoleSetup.ts` is now a re-export shim) and defines
    `PORT_BASE = 30000` / `PORT_CEILING = 32768`
    (hardcoded, chosen only to stay below the Linux ephemeral range) and
    two allocators: `freePort()` — a bare monotonic counter
    (`return nextPort++`) that does **not** check whether a port is
    actually free, just hands out the next number — and
    `freePortRange(count)` — returns `count` consecutive ports (advances
    the counter by `count`), added for diskdb which derives
    `http_port = rpc_port + 1` and `rpc_listen_port = rpc_port + 2`
    from the single `rpc_port` passed to `deployDiskdb`. The fixture's
    own comment admits the contract: "Tests run sequentially (workers:
    1), so a counter is safe — each test cleans up its own servers
    before the next test starts." `setupCluster` then calls
    `deployNodeServer(baseURL, nodeId, freePort(), freePort())`, so every
    node's rest + rpc port comes from this counter, and `deployDiskdb`
    takes an `rpcPort` from `freePortRange(3)`. The
    `TopologyDescriptor` presets (`SIMPLE`, `COMPLEX`) no longer have a
    `portBase` field — it has been **removed** from both the TS and Rust
    `TopologyDescriptor` structs; ports are now sourced entirely from
    the counter (TS) or `unique_test_port()` (Rust), not from a
    topology-relative base. This is exactly the "check-existing-ports-
    and-pick-a-new-number" logic the user wants consolidated into one
    place — but it (a) only works because tests are forced serial, (b)
    never probes bind, so a stale TIME_WAIT socket or a non-CROWDB
    process on 30xxx silently collides, (c) is console-UI-test-only,
    not shared with the Rust integration tests or bench scripts, and
    (d) uses a range that has no relationship to the `ServicePort`
    scheme the rest of the project standardizes on.
  - The Rust test harnesses and bench targets use a **different stopgap**:
    `crowdb_console_shared::test_ports` (three functions:
    `unique_test_port()` — binds `127.0.0.1:0`, reads the OS-assigned
    port, drops the listener; `unique_test_ports(count)` — batch-binds
    `count` listeners to `:0` simultaneously so the OS assigns `count`
    distinct ports with no TOCTOU between them; `unique_test_port_range(
    count)` — finds `count` consecutive free ports for subprocesses that
    derive multiple ports from a single base). These are reliable for
    parallel tests but rely on port 0 — the very escape hatch R118 wants
    to remove from server CLIs. The Rust `CrowdbClusterDeployer`
    (`cluster_deployer.rs` lines 353-354) and `local_deploy`
    (`ops/cluster.rs` line 700) use `unique_test_port()` /
    `unique_test_port_range()` for kv-server and diskdb deploy.
    `tools/bench-rpc-regression.sh` no longer hardcodes a port — it
    uses `cluster local-deploy -t rpc` (CLI `--rpc-port` default 0)
    which calls `local_deploy_rpc` → `unique_test_port()` when port=0.
    So there are **three** parallel port-picking mechanisms (E2E
    counter, Rust `:0` bind, CLI `local-deploy` auto-allocate) none of
    which is the `ServicePort` scheme — though the Rust side has
    consolidated to the `test_ports` module, the TS E2E side has not.
- Impact: with no unified CLI surface and no dispatcher, test instances
  collide on famous ports, producing flaky `Address already in use`
  failures. The port-0 escape hatch makes it worse by hiding the actual
  port from peers. The console UI's hand-rolled `freePort()` forces E2E
  to run serial (`workers: 1`) and still cannot guarantee a port is
  free, so restart-within-TIME_WAIT and any non-CROWDB process on the
  30000-32768 range cause flaky failures. Tests cannot run in parallel
  safely.
- Root cause: deferred placeholder. `ports.rs` landed the constants and
  the `ServicePort::port(instance)` formula but never (a) wired every
  server to accept explicit per-listener port overrides with `ports.rs`
  defaults, (b) removed the port-0 path, or (c) built a single
  test/cluster port dispatcher that assigns non-conflicting instance
  indices — so each test harness (console UI E2E, Rust integration
  tests, bench scripts) reinvented its own port-picking logic. A second
  root cause is the **RPC port collapse**: the kv-server implementation
  diverged from the design docs (which specify separate consensus and
  client-facing ports) by collapsing both onto the listen port, leaving
  `KV_RPC_BASE` / `KV_CLIENT_RPC_BASE` and the `KvServerRpc` /
  `KvServerClientRpc` enum variants as dead code. A third root cause is
  the **diskio gap**: the diskio server landed without any `ports.rs`
  integration, so the `ServicePort` scheme is already incomplete for the
  current set of services.

**Design pointers**

- `doc/design/protocol/design-crowdb-protocol.md` — root protocol design.
  **Design gap:** this doc has no section covering the port-allocation
  scheme that `crowdb-protocol/src/ports.rs` already implements
  (base/stride/`ServicePort`). R118 must add a "Port allocation" section
  to the protocol design doc anchoring the scheme; the backlog
  references it as `§<new>` once added. Flagged here rather than
  inventing architecture in the backlog.
- `doc/design/kv/design-crowdb-kv-rpc.md` §4 and
  `doc/design/kv/design-crowdb-kv-rpc-client.md` §1, §4 — specify
  separate consensus (`rpc_port + 100`) and client-facing
  (`rpc_port + 200`) RPC ports. The implementation has collapsed both
  onto the listen port (G1). R118 must reconcile design vs impl here.
- `doc/design/kv/design-crowdb-kv-server.md` — `crowdb-kv-server` binary
  startup / HTTP management API / group lifecycle; the per-listener port
  wiring lands here.
- `doc/design/diskdb/design-crowdb-diskdb.md` and
  `doc/design/chunkdb/design-crowdb-chunkdb-rpc.md` — diskdb/chunkdb
  listen-address config; CLI-flag unification touches these.

**Use scenarios**

- **Operator single-instance start** — operator runs
  `crowdb-kv-server --root /node1` with no port flags → server listens on
  `KV_SERVER_MGMT_BASE` (mgmt) and `KV_SERVER_LISTEN_BASE` (listen pool
  first port, which also hosts both consensus and client RPC per the
  current collapsed implementation). Same for `crowdb-diskdb`,
  `crowdb-chunkdb`, and `crowdb-diskio` with their famous defaults. No
  port 0 anywhere. *(If R118 restores the separate consensus/client RPC
  ports per the design docs, this scenario expands to mention
  `KV_RPC_BASE` and `KV_CLIENT_RPC_BASE` — pending the RPC port
  collapse decision.)*
- **Operator explicit override** — operator runs
  `crowdb-kv-server --root /node1 --management-port 9915 --ports 28005`
  → server listens on exactly those ports; defaults ignored for the
  flags passed. Passing `0` on any port flag is rejected at CLI parse
  with a clear error. *(If R118 restores separate RPC ports, add
  `--consensus-rpc-port 28105 --client-rpc-port 28205` here.)*
- **Test parallel cluster start** — a test harness starts 3
  `crowdb-kv-server` instances + 3 `crowdb-diskdb` instances on one host in
  parallel. The port dispatcher assigns each instance a distinct
  instance index per service type; every listen port is computed via
  `ServicePort::port(instance)` and is non-conflicting across all
  instances and all service types. No `Address already in use`.
- **Test rapid restart** — a test starts a server, tears it down, and
  starts another on the same instance index within milliseconds
  (TIME_WAIT window). The dispatcher either reuses the index only after
  the socket is free or hands out a fresh index from a free pool; the
  test does not observe a bind failure.
- **Cluster bootstrap with dispatcher** — a cluster bootstrap tool asks
  the dispatcher for N kv-server instance indices (and their derived
  mgmt/listen ports) plus M diskdb instance indices, then starts each
  server with the explicit `--*-port` flags derived from the
  dispatcher. Peers learn each other's listen addresses from the
  assigned indices, not from runtime discovery of OS-assigned ports.
- **Console UI E2E uses the shared prober** — the Playwright fixture
  (`crowClusterDeployer.ts`) stops using its private `freePort()` /
  `freePortRange()` counter and `PORT_BASE`/`PORT_CEILING` magic
  numbers; instead it shells out to `crowdb-port-alloc` (the CLI
  binary) to get ports, then passes them to `deployNodeServer` /
  `deployDiskdb` as before. E2E can then lift the `workers: 1` serial
  constraint and run flows in parallel, and a restart-within-TIME_WAIT
  no longer collides because the prober probes the actual system state
  rather than blindly incrementing.

**Solution**

All decisions are resolved (see Decisions). The unification target:
every server accepts explicit per-listener port flags with `ports.rs`
defaults; port 0 rejected everywhere (including test helpers); one
`ServicePort::port(instance)` formula with all stride 1; port ranges
rescheduled to 1000 per server type with shared prefix, all >10000;
RPC port collapse accepted (no separate consensus/client ports);
paired-port invariant broken (http and listen independent);
flock-coordinated claim-file prober replaces all existing
port-picking code; cross-host discovery via group-0 registration. The
design draft must nail down the CLI surface details, the migration
path from old port ranges to new, and the harness retry loop. Items
needing user input before the design draft can be finalized are in
Pending User Input at the end.

**One-line summary**: Wire every server to accept explicit per-listener
ports (defaults from `crowdb-protocol::ports`), reject port 0
everywhere, reschedule port ranges to 1000 per server type (>10000),
replace all existing port-picking code with a single flock-coordinated
claim-file prober, and use group-0 registration for cross-host service
discovery — so tests and cluster bootstrap run in parallel without bind
collisions.

Numbered work items:

1. **Protocol design anchor + port reschedule** —
   `doc/design/protocol/design-crowdb-protocol.md` (new "Port allocation"
   section) + `lib/crowdb-protocol/src/ports.rs`. Document the
   base/stride/`ServicePort` scheme as design (currently code-only).
   **Reschedule all port ranges** (D3): kv-server 10000-10999, diskdb
   11000-11999, chunkdb 12000-12999, diskio 13000-13999, web 14000-14999
   — 1000 per server type, same prefix, all >10000, all stride 1 (D5
   breaks the paired-port invariant). Add missing `DiskdbRpc` +
   `DiskioRpc` variants. **Accept the RPC port collapse** (D3 implies
   this): remove the dead `KvServerRpc` / `KvServerClientRpc` variants
   and `KV_RPC_BASE` / `KV_CLIENT_RPC_BASE` constants — the listen port
   hosts both consensus and client RPC. Update the design docs
   (`design-crowdb-kv-rpc.md` §4, `design-crowdb-kv-rpc-client.md` §1,
   §4) to match the collapsed reality. State the "no port 0" rule as a
   design invariant (D6). Document the group-0 discovery groundwork
   (D4): group-0 kv-server mgmt port is famous (10000) for bootstrap
   discovery. The full group-0 discovery implementation is split to a
   new requirement. **Migration: hard cutover** — search and update
   all port references in one pass, fix regressions as they surface.
2. **`crowdb-kv-server` CLI unification** — `app/crowdb-kv-server/src/cli.rs`
   + `lib/crowdb-kv/src/cluster/kv_server.rs`. Reject `0` on
   `--management-port` and `--ports` (currently `parse_port_list`
   accepts 0). Remove the port-0 / OS-assigned branch in
   `KvServer::start` (lines 68-71) — bind exactly the requested port;
   bind failure is a hard error with a clear message. **Accept the RPC
   port collapse**: no `--consensus-rpc-port` / `--client-rpc-port`
   flags needed. Remove the `start_rpc_server` /
   `start_client_rpc_server` no-op stubs. Update the design docs to
   match the collapsed reality.
3. **`crowdb-diskdb` CLI unification** — `app/crowdb-diskdb/src/main.rs` +
   diskdb config. Add CLI flags `--listen-port` (default
   `DISKDB_LISTEN_BASE`), `--http-port` (default `DISKDB_HTTP_BASE`),
   `--rpc-port` (default `DISKDB_RPC_BASE`) that override config; keep
   config as the fallback when the flag is absent. **Also add
   `--rpc-listen-addr` / wire `rpc_listen_addr` to a CLI override** —
   currently only `listen_addr` and `http_listen_addr` have CLI
   overrides; `rpc_listen_addr` can only be changed via config. Reject
   `0`. **No paired-port invariant** (D5): http and listen are
   independent ports, each overridden individually.
4. **`crowdb-chunkdb` CLI unification** — `app/crowdb-chunkdb/src/main.rs`
   + chunkdb config. Same shape as diskdb: `--listen-port` / `--http-port`
   / `--rpc-port` with `CHUNKDB_*_BASE` defaults, reject `0`, add
   `--rpc-listen-addr` CLI override. **Also remove or repurpose the
   vestigial `listen_addr` field** — it is parsed and logged but never
   bound (the RPC server binds `rpc_listen_addr`, the HTTP server binds
   `http_listen_addr`). Either remove `listen_addr` + `--listen-addr`
   entirely, or wire it to the actual RPC bind (renaming `rpc_listen_addr`
   → `listen_addr` for consistency with diskdb).
5. **`crowdb-diskio` CLI unification** — `app/crowdb-diskio/src/dio_config.cpp`
   + `dio_config.h`. Add a `DISKIO_RPC_BASE` default for `--port`
   (currently required with no default); reject `0` in `validate()`
   (currently `0..=65535` is accepted). Wire the default to the
   `DISKIO_RPC_BASE` constant added in item 1 (the C++ binary reads it
   via the FFI `crowdb-protocol` header or a generated constant — the
   design draft must specify the C++→Rust constant bridge).
6. **`crowdb-web` / `crowdb-cli` port flags** — `app/crowdb-web/src/main.rs`,
   `app/crowdb-cli/src/main.rs`. `crowdb-web` uses `WEB_BASE` default
   but does **not** reject `0` — add a `value_parser` that rejects 0.
   `crowdb-cli` `--sysmd-port` uses a **literal `9910`** default, not
   `KV_SERVER_MGMT_BASE` — replace with the constant for consistency
   with the "no literal port numbers in CLI defaults" invariant.
7. **Port prober + claim file** — new module
   (`lib/crowdb-protocol/src/port_alloc.rs`) + a small CLI binary
   (`crowdb-port-alloc`, in `app/crowdb-port-alloc/`). The **single place**
   that picks ports. Mechanism: `flock` a claim file → probe the system
   for an actually-unused port (bind probe, not a counter) that is not
   already listed in the file → write the selected port to the file →
   unlock. The file is the set of claimed ports. Test shell scripts `rm`
   the file to reset the claim set between runs (avoids exhaustion).
   Ships as both a Rust library (for Rust test harnesses) and a CLI
   binary (for shell scripts and the console UI TS fixture, which can't
   call Rust directly). No daemon, no lifecycle to manage. Replaces the
   scattered per-harness logic (notably the console UI E2E fixture's
   `freePort()` / `freePortRange()` counter in `crowClusterDeployer.ts`).
   **Claim file location + format decided** — lives under a workspace
   root (default: project path, overridable via `--root`); plain text,
   one port per line; multi-session uses separate root paths + a port
   offset parameter so sessions probe disjoint ranges. The design
   draft must specify: the default base port, the offset stride, the
   probe algorithm (bind vs. connect probe). The claim-to-bind TOCTOU
   mitigation is **decided** — see Decision 1 and Edge cases.
8. **Test harness integration** — `crates/crowdb-kv/tests/`,
   `crates/crowdb-diskdb/tests/`, bench scripts under `tools/`, and the
   console UI E2E fixture (`app/crowdb-web/ui/e2e/fixtures/crowClusterDeployer.ts`).
   - Rust harnesses call the `port_alloc` library directly (replacing
     `test_ports::unique_test_port*` calls).
   - Shell scripts and the console UI TS fixture shell out to
     `crowdb-port-alloc` (TS via `child_process.execSync`).
   - Replace the console UI's private `freePort()` / `freePortRange()` /
     `PORT_BASE` / `PORT_CEILING` with `crowdb-port-alloc` calls;
     replace port-0 startup in the Rust integration tests and bench
     scripts likewise. (`TopologyDescriptor.portBase` is already gone —
     no removal needed.)
   - **TOCTOU retry lives here, not in the server** — the cluster
     deploy management code (`cluster_deployer.rs` /
     `local_deploy` in `ops/cluster.rs`) detects a server exit caused
     by bind failure, re-runs the prober for a fresh port, and
     restarts the server. The server itself just exits with a hard
     error; no retry logic in the server start path.
   - Goal: every test harness gets ports from the one prober so tests
     can run in parallel without serial gating (`workers: 1` can be
     lifted).

Flow diagram (shape only):

```
   Rust test  ──┐                        ┌── claim file (flock) ──┐
                │  port_alloc::alloc()   │   read claimed set     │
   shell       ─┤  (library call)        │   probe free port      │
   script      ─┤                        ├──▶ write port ── unlock │
   console UI  ─┘  crowdb-port-alloc       │                        │
   (TS)          (CLI, shells out)       └────────────────────────┘
                        │ selected port
                        ▼ explicit --*-port flags
            ┌────────────────────────────────────────┐
            │ crowdb-kv-server / crowdb-diskdb /          │
            │ crowdb-chunkdb / crowdb-diskio /            │
            │ crowdb-web                                   │
            │  - default = ports.rs BASE              │
            │  - reject port 0                        │
            │  - bind exactly requested port          │
            └────────────────────────────────────────┘
```

Edge cases at a glance:

- Port `0` passed on any flag → rejected at CLI parse with a clear
  error (no OS-assigned).
- Two probers on same host run concurrently → `flock` serializes them;
  the second sees the first's claimed port in the file and skips it;
  no double-assign.
- Rapid restart within TIME_WAIT → the prober's bind probe skips
  TIME_WAIT-bound ports; caller never sees `Address already in use`.
- Claim-to-bind TOCTOU → a port free at probe time is grabbed by a
  non-coordinated process (or enters TIME_WAIT) before the server binds
  it → server bind fails. **Decided**: server exits with a hard error
  (no retry inside the server); the **test harness** (cluster deploy
  management — `cluster_deployer.rs` / `local_deploy` in `ops/cluster.rs`)
  detects the server exit, re-runs `crowdb-port-alloc` for a fresh port,
  and restarts the server. The prober marks the failed port as
  "tried-and-failed" in the claim file so the next probe skips it
  within the same run. Outcome: the harness eventually binds a free
  port, no silent failure, no retry logic in the server start path.
- Claim file stale after a crashed test → the file lists ports no
  longer in use, shrinking the free pool. Reset by `rm` of the claim
  file (under the workspace root) between runs; outcome: next run
  starts with a fresh claim set, no exhaustion.
- Claim file on NFS → `flock` is unreliable on NFS. Tests run locally
  (workspace root defaults to the project path, a local filesystem),
  so this is a documentation constraint (workspace root must be on a
  local filesystem), not a blocker.
- All instances of a service exhausted (instance index beyond the
  documented range) → dispatcher returns a clear "no free index" error
  instead of silently wrapping into another service's range.
- Operator passes inconsistent paired ports (diskdb http ≠ listen + 1) →
  either rejected or accepted with documented override semantics
  (decided in design).
- A server started with explicit ports outside its service's documented
  range → accepted (operator override) but logged as a warning so
  cluster topology tools can detect misconfiguration.

**Dependencies**

- None on other `R**` items for the CLI unification (items 1–6) —
  `ports.rs` already exists. The chunkdb server binary has landed
  (`app/crowdb-chunkdb/`), so item 4 is unblocked. The diskio server
  binary has landed (`app/crowdb-diskio/`), so item 5 is unblocked.
- Item 7 (dispatcher) has no upstream `R**` dependency but is the input
  to item 8 (test harness integration) and to any future cluster-
  bootstrap tool.
- Downstream: any future service must follow the same scheme (pick a
  base outside the documented ranges, add a `ServicePort` variant, add
  CLI flags) — item 1's design section should name this as the
  extension path.

**Acceptance**

**CLI unification (kv-server)**:

- `crowdb-kv-server --root /tmp/n1` (no port flags) → mgmt listens on
  `KV_SERVER_MGMT_BASE`, listen pool first port on
  `KV_SERVER_LISTEN_BASE` (which also hosts consensus + client RPC per
  the collapsed implementation). E2E test. *(If item 1 restores
  separate RPC ports: also assert consensus on `KV_RPC_BASE`, client
  RPC on `KV_CLIENT_RPC_BASE`.)*
- `crowdb-kv-server --root /tmp/n1 --management-port 0` → CLI parse error
  mentioning port 0 is not allowed. Unit test (CLI parse).
- `crowdb-kv-server --root /tmp/n1 --ports 0` → CLI parse error
  mentioning port 0 is not allowed. Unit test (CLI parse).
- *(If item 1 restores separate RPC ports:*
  `crowdb-kv-server --root /tmp/n1 --consensus-rpc-port 28105
  --client-rpc-port 28205` → consensus listens on 28105, client RPC on
  28205, other ports stay at defaults. E2E test. *)*
- `KvServer::start` with a port already in use → returns a hard error
  with the listen addr and a "stop the conflicting process" hint; no
  port-0 fallback. Integration test.

**CLI unification (diskdb / chunkdb / diskio / web / cli)**:

- `crowdb-diskdb` with no port flags → listen on `DISKDB_LISTEN_BASE`,
  HTTP on `DISKDB_HTTP_BASE`, crowdb-rpc on `DISKDB_RPC_BASE`. E2E test.
- `crowdb-diskdb --listen-port 0` → CLI parse error. Unit test.
- `crowdb-diskdb --listen-port 9943 --http-port 9944` → listens on those
  ports; paired invariant http = listen + 1 holds. E2E test.
- `crowdb-diskdb --rpc-port 9931` → `rpc_listen_addr` overrides config
  (currently no CLI override exists for `rpc_listen_addr`). E2E test.
- `crowdb-chunkdb` equivalent of the above (server has landed; no
  skip). E2E test. Additionally: `crowdb-chunkdb` with no
  `--listen-addr` does not log a vestigial `listen_addr` (field removed
  or repurposed). Unit test (static check).
- `crowdb-diskio --port 0` → CLI parse / validate error. Unit test.
- `crowdb-diskio` with no `--port` → listens on `DISKIO_RPC_BASE`
  (currently `--port` is required with no default). E2E test.
- `crowdb-web --port 0` → CLI parse error. Unit test.
- `crowdb-cli --sysmd-port` `default_value_t` is `KV_SERVER_MGMT_BASE`,
  not a literal. Unit test (static check).

**Port dispatcher**:

- Two concurrent `Dispatcher::allocate(KvServerListen, 3)` calls on one
  host → return disjoint instance-index sets; no port collision when
  expanded via `ServicePort::port`. Integration test.
- `Dispatcher::allocate` for a service until the documented range is
  exhausted → returns a "no free index" error, does not wrap into
  another service's range. Unit test.
- Rapid `allocate → release → allocate` of the same index within the
  TIME_WAIT window → caller does not observe a bind failure on the
  server started with the released index (dispatcher either waits or
  hands a fresh index). Integration test.
- `Dispatcher::allocate` for multiple service types in one call → all
  derived ports are pairwise non-conflicting across all service types
  and instances. Integration test.

**Test harness integration**:

- A parallel test run that starts 3 kv-server + 3 diskdb instances
  concurrently via the dispatcher → 0 `Address already in use` errors
  across N consecutive runs (N pending design, default 3). E2E test.
- Existing tests that previously gated on a serial lock / hardcoded
  ports → run green in parallel after migration. E2E test.
- **Console UI E2E consolidation** — `crowClusterDeployer.ts` no longer
  defines `PORT_BASE` / `PORT_CEILING` / `nextPort` / `freePort()` /
  `freePortRange()`; every `deployNodeServer` / `deployDiskdb` call
  sources its ports from the shared dispatcher. Static check: grep for
  `freePort` / `PORT_BASE` in `app/crowdb-web/ui/e2e/` returns nothing.
  Unit test (static check).
- **Console UI E2E parallel** — after migration, a representative E2E
  flow runs green with `workers > 1` (exact worker count pending
  design) and 0 port-conflict errors across N consecutive runs. E2E
  test.

**Invariants**:

- No server binary in the workspace binds port 0 on any listener (grep
  `bind.*0` / `port.*0` in server start paths returns nothing). Unit
  test (static check).
- Every server CLI port flag has a `default_value_t` sourced from
  `crowdb-protocol::ports` (no literal port numbers in CLI defaults) —
  covers `crowdb-cli --sysmd-port` (currently literal `9910`). Unit
  test (static check).

**Test commands**: `pixi run test-protocol` (ports + dispatcher unit
tests), `pixi run cargo test -p crowdb-kv -p crowdb-diskdb` (server CLI +
dispatcher integration), relevant `pixi run test-tree-ct` only if C++
changes (expected for item 5 — diskio `dio_config.cpp`), plus
`pixi run cargo fmt --all -- --check` and
`pixi run cargo clippy --all-targets -- -D warnings`.

**Decisions**

1. **Claim-to-bind TOCTOU mitigation — DECIDED**: mitigation (i)
   (re-allocate on bind failure), with the retry loop in the **test
   harness** (cluster deploy management), not in the server. Server
   exits with a hard error on bind failure (no retry inside `start()`);
   the harness (`cluster_deployer.rs` / `local_deploy` in
   `ops/cluster.rs`) detects the exit, re-runs `crowdb-port-alloc` for
   a fresh port, and restarts the server. The prober marks the failed
   port as "tried-and-failed" in the claim file so the next probe
   skips it within the same run. This keeps the server start path
   simple (hard error, no retry) and centralizes the retry in the one
   place that already manages server lifecycle. The design draft must
   specify the exact harness retry loop (max attempts, backoff, exit
   code detection).
2. **Claim-file path + format — DECIDED**:
   - **Workspace root**: claim file lives under a workspace root
     directory (not `/tmp`). Default root is the **project path**; if
     none is provided, the prober supplies a default under the project.
     All subsequent log data and test data also go under this root, so
     one directory holds the claim file + logs + data for a test
     session. The root path is overridable via a parameter (`--root` on
     `crowdb-port-alloc`, or the library equivalent).
   - **Format**: plain text, one port per line — easiest to `cat`/
     `grep`/`rm` from shell scripts.
   - **Multi-session**: each concurrent test session uses its own root
     path (so each has its own claim file, no cross-session flock
     contention). To reduce inter-session port collisions, the prober
     accepts an additional **port offset** parameter. The offset shifts
     the base port for a session: e.g. with offset 100, session 1
     probes `[base, base+100)`, session 2 probes `[base+100,
     base+200)`. **Default offset is 0** (single session, no shift).
     Different service types already have different base ports (D3
     port map), so the offset is applied on top of each service's own
     base. The bind probe still catches any residual collision (two
     sessions accidentally overlapping), but the offset makes that
     rare. The offset is passed as a CLI flag on `crowdb-port-alloc`
     (`--offset N`); the library equivalent takes it as a parameter.
3. **Probe port range — DECIDED**: reschedule all port usage. Each
   server type gets a **1000-port block** with a **shared prefix**
   (same kind of server = same leading digits), all ports **>10000**.
   The prober scans the `ServicePort` ranges (now generously sized);
   tests and production share the same scheme. New port map:

   ```
   kv-server   10000-10999  (prefix 10)
     mgmt      10000-10099  (stride 1)
     listen    10100-10199  (stride 1; hosts consensus + client RPC
                             per the collapsed implementation)
     spare     10200-10999

   diskdb      11000-11999  (prefix 11)
     listen    11000-11099  (stride 1)
     http      11100-11199  (stride 1; no longer paired with listen)
     rpc       11200-11299  (stride 1)
     spare     11300-11999

   chunkdb     12000-12999  (prefix 12)
     listen    12000-12099  (stride 1; vestigial — see G8)
     http      12100-12199  (stride 1; no longer paired)
     rpc       12200-12299  (stride 1)
     spare     12300-12999

   diskio      13000-13999  (prefix 13)
     rpc       13000-13099  (stride 1)
     spare     13100-13999

   web         14000-14999  (prefix 14)
     http      14000-14099  (stride 1)
     spare     14100-14999
   ```

   All services now use **stride 1** (the paired-port stride-2 scheme is
   gone — see D5). The implementation must update every constant in
   `lib/crowdb-protocol/src/ports.rs`, add the missing `DiskdbRpc` and
   `DiskioRpc` variants, remove the dead `KvServerRpc` /
   `KvServerClientRpc` variants (accept the RPC port collapse), and
   update all references across the codebase (CLI defaults, config
   defaults, test assertions, design docs). **Migration: hard
   cutover** — search the codebase and update all port references in
   one pass, then fix regressions as they surface. No rolling-update
   coexistence; old and new ranges do not need to interoperate.
4. **Cross-host coordination — DECIDED**: each host starts services on
   their **default ports** (instance 0). Multiple instances on the same
   host use instance indices via the prober. Cross-host, ports can be
   the same because every service **registers its IP + port to group-0**
   sysdata. The **group-0 kv-server uses a famous, well-known mgmt port**
   (`KV_SERVER_MGMT_BASE` = 10000) so any client can bootstrap discovery
   by contacting group-0 and reading the service registry to learn all
   living services' IP + port. This concept — "group-0 is the
   discovery root, services self-register" — must be used consistently
   through all code. **Scope split**: R118 lays the groundwork (famous
   mgmt port 10000 for group-0 bootstrap, port reschedule, prober).
   The full group-0 discovery implementation (sysdata keys, service
   self-registration on startup, client query path) is split to a
   **new requirement**. **Test exception**: simple test cases that
   don't use group-0 can pass service info via parameters directly —
   these tests are typically simple enough that explicit parameter
   passing suffices.
5. **Paired-port override semantics — DECIDED**: **break the pair**.
   diskdb/chunkdb http and listen are independent ports — no invariant
   that http = listen + 1. Each gets its own sub-range within the
   server type's 1000-port block (see D3 port map). `--listen-port` and
   `--http-port` override independently; passing one does not shift the
   other. This simplifies `ServicePort::stride()` (all stride 1, no
   more stride-2 paired logic) and removes the confusing override
   semantics. The implementation must remove the stride-2 logic from
   `ports.rs` and update any code that assumes the pairing (notably
   `resolve_diskdb_config_path` in `lifecycle.rs` which derives
   `http_port = rpc_port + 1` and `rpc_listen_port = rpc_port + 2`).
6. **Port-0 rule — DECIDED**: **reject port 0 everywhere** — both on
   server CLI flags and in the test port-picking helpers. No code
   anywhere may bind port 0. The existing `test_ports` module
   (`unique_test_port`, `unique_test_ports`,
   `unique_test_port_range`) — which all bind `127.0.0.1:0` — must be
   **removed and replaced** by the new prober (`port_alloc` library /
   `crowdb-port-alloc` CLI). The rule is absolute: "no code anywhere
   may bind port 0". The implementer must remove `unique_test_port*`
   and migrate all ~19 test files that call them to the new prober.
7. **Flock claim-file prober — DECIDED**: **build the flock prober and
   replace all existing port-picking code**. The `test_ports` module
   (all three functions), the E2E fixture's `freePort()` /
   `freePortRange()` counter, and any other ad-hoc port logic are all
   replaced by the single `port_alloc` library + `crowdb-port-alloc`
   CLI. No composition with the old `:0` bind approach — the new
   solution is the only port-picking mechanism. This is in scope for
   R118 (not split into a separate requirement).

**Design / Impl Gaps**

Gaps remaining after the decisions (2026-09-02). Resolved gaps have
been removed; only open issues that still need implementation work are
listed. Each gap states what the code does vs what the decision
requires, and what R118 must do to close it.

- **G4. Protocol design doc has no "Port allocation" section** —
  `design-crowdb-protocol.md` has no section covering the
  base/stride/`ServicePort` scheme. The scheme is code-only. R118 item 1
  adds the design section, now including the new port map (D3), the
  group-0 discovery model (D4), and the "no port 0" invariant (D6).
- **G7. diskdb/chunkdb `rpc_listen_addr` has no CLI override** — both
  servers have `--listen-addr` (overrides `listen_addr`) and
  `--http-addr` (overrides `http_listen_addr`), but no flag for
  `rpc_listen_addr` — the crowdb-rpc listener port can only be changed
  via config file. R118 items 3-4 add `--rpc-port` /
  `--rpc-listen-addr` overrides.
- **G8. chunkdb `listen_addr` is vestigial** — parsed (line 66) and
  logged (line 196) but never bound. The RPC server binds
  `rpc_listen_addr`; the HTTP server binds `http_listen_addr`. R118
  item 4 removes or repurposes the field.
- **G10. `crowdb-cli --sysmd-port` uses literal `9910`** — not
  `KV_SERVER_MGMT_BASE`. Violates the "no literal port numbers in CLI
  defaults" invariant. R118 item 6 replaces the literal with the
  constant (which will be `10000` after the D3 reschedule).
- **G13. All port constants must be rescheduled** — every `*_BASE`
  constant in `lib/crowdb-protocol/src/ports.rs` must change to the new
  port map (D3): kv-server 10000-10999, diskdb 11000-11999, chunkdb
  12000-12999, diskio 13000-13999, web 14000-14999. All stride-2
  logic removed (D5 — all stride 1). Dead `KvServerRpc` /
  `KvServerClientRpc` variants removed (RPC port collapse accepted).
  Missing `DiskdbRpc` / `DiskioRpc` variants added. All references
  across the codebase updated (CLI defaults, config defaults, test
  assertions, design docs).
- **G14. Paired-port derivation in `lifecycle.rs` must be removed** —
  `resolve_diskdb_config_path` (lifecycle.rs ~line 727) derives
  `http_port = rpc_port + 1` and `rpc_listen_port = rpc_port + 2`
  from a single `rpc_port`. With the paired-port invariant broken (D5),
  each port must be independent. The diskdb deploy path must pass
  all three ports explicitly (or derive them from the prober
  independently).
- **G15. `test_ports` module must be removed and all callers migrated** —
  `lib/crowdb-console-shared/src/test_ports.rs` (all three functions:
  `unique_test_port`, `unique_test_ports`, `unique_test_port_range`)
  must be deleted. All ~19 test files that call them must migrate to
  the new `port_alloc` library / `crowdb-port-alloc` CLI (D6/D7). No
  code anywhere may bind port 0.
- **G16. Group-0 service discovery — split to new requirement** — the
  D4 decision requires every service to register its IP + port to
  group-0 sysdata on startup, and clients to discover peers via
  group-0. R118 lays the groundwork (famous mgmt port 10000 for
  group-0 bootstrap, port reschedule, prober). The full discovery
  implementation (sysdata keys, service self-registration, client
  query path) is split to a **new requirement**. The current code has
  partial service-registry support (`ServiceRegistryClient`,
  keep-alive heartbeat) but the "group-0 is the discovery root"
  concept is not used consistently through all code. **Test
  exception**: simple test cases that don't use group-0 can pass
  service info via parameters directly.
- **G17. E2E fixture `freePort()` / `freePortRange()` must be removed** —
  `app/crowdb-web/ui/e2e/fixtures/crowClusterDeployer.ts` (lines 13-41)
  defines `PORT_BASE`, `PORT_CEILING`, `nextPort`, `freePort()`,
  `freePortRange()`. All must be replaced by `crowdb-port-alloc` CLI
  calls (D7). The `workers: 1` serial constraint can then be lifted.

**Open Issues**

Items that may need further user input during implementation. All
design decisions are resolved (see Decisions above); these are
implementation details that may surface questions as code is written.

<!-- User: update this section as implementation proceeds. -->
    