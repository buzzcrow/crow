<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R118: cluster — Unify Port Usage & Test Port Dispatcher

**Problem**

CROW runs many servers (crow-kv-server, crow-diskdb, crow-chunkdb,
crow-web), each listening on one or more ports. During tests, multiple
instances of the same server are started and torn down in short windows,
so port conflict (`Address already in use`) is a common failure that has
already cost real debugging/CI time. There is no single source of truth
for "which instance gets which port" and no dispatcher, so instances
collide on the famous ports or on whatever a test harness hardcodes.

**Current behavior + impact**

- `lib/crow-protocol/src/ports.rs` already defines base ports + stride
  rules + a `ServicePort` enum for all current services (kv-server
  mgmt/grpc/consensus-rpc/client-rpc, diskdb grpc/http/rpc, chunkdb
  grpc/http/rpc, web). But adoption is incomplete and inconsistent:
  - `crow-kv-server` CLI (`app/crow-kv-server/src/cli.rs`) exposes
    `--management-port` (default `KV_SERVER_MGMT_BASE`) and `--ports`
    (gRPC pool, optional free-form list) but has **no CLI flag** for the
    consensus RPC port (`KV_RPC_BASE`) or the client-facing RPC port
    (`KV_CLIENT_RPC_BASE`). Those listeners fall back to constants the
    operator cannot override per-start.
  - `crow-diskdb` (`app/crow-diskdb/src/main.rs`) takes listen addresses
    from TOML config (`rpc_listen_addr`, `http_listen_addr`,
    `listen_addr`), not from CLI flags with `ports.rs` defaults — so the
    famous ports are not enforced at the CLI boundary and an operator
    cannot override per-start without editing config.
  - `KvServer::start` (`lib/crow-kv/src/cluster/kv_server.rs` ~lines
    68-71) explicitly supports **port 0 for OS-assigned** ports: "Bind a
    TCP listener to determine the actual port (supports port 0 for
    OS-assigned)". This contradicts the project flow — servers must
    listen on explicit/famous ports; port 0 is not allowed. An
    OS-assigned port is not reproducible and not discoverable by peer
    clients without extra plumbing, breaking the deterministic-port flow
    the cluster expects.
  - The console UI E2E fixture ships its **own ad-hoc port allocator**
    disconnected from `ports.rs`: `app/crow-web/ui/e2e/fixtures/consoleSetup.ts`
    ~lines 13-26 define `PORT_BASE = 30000` / `PORT_CEILING = 32768`
    (hardcoded, chosen only to stay below the Linux ephemeral range) and
    a `freePort()` that is a bare monotonic counter (`return nextPort++`)
    — it does **not** check whether a port is actually free, just hands
    out the next number. The fixture's own comment admits the contract:
    "Tests run sequentially (workers: 1), so a counter is safe — each
    test cleans up its own servers before the next test starts."
    `setupCluster` then calls
    `deployNodeServer(baseURL, nodeId, freePort(), freePort())`, so every
    node's rest + rpc port comes from this counter, and `deployDiskdb`
    takes an `rpcPort` from the same source. The `TopologyDescriptor`
    presets (`SIMPLE.portBase = 9800`, `COMPLEX.portBase = 9900`) are a
    second, parallel set of magic numbers. This is exactly the
    "check-existing-ports-and-pick-a-new-number" logic the user wants
    consolidated into one place — but it (a) only works because tests
    are forced serial, (b) never probes bind, so a stale TIME_WAIT
    socket or a non-CROW process on 30xxx silently collides, (c) is
    console-UI-test-only, not shared with the Rust integration tests or
    bench scripts, and (d) uses a range that has no relationship to the
    `ServicePort` scheme the rest of the project standardizes on.
- Impact: with no unified CLI surface and no dispatcher, test instances
  collide on famous ports, producing flaky `Address already in use`
  failures. The port-0 escape hatch makes it worse by hiding the actual
  port from peers. The console UI's hand-rolled `freePort()` forces E2E
  to run serial (`workers: 1`) and still cannot guarantee a port is
  free, so restart-within-TIME_WAIT and any non-CROW process on the
  30000-32768 range cause flaky failures. Tests cannot run in parallel
  safely.
- Root cause: deferred placeholder. `ports.rs` landed the constants and
  the `ServicePort::port(instance)` formula but never (a) wired every
  server to accept explicit per-listener port overrides with `ports.rs`
  defaults, (b) removed the port-0 path, or (c) built a single
  test/cluster port dispatcher that assigns non-conflicting instance
  indices — so each test harness (console UI E2E, Rust integration
  tests, bench scripts) reinvented its own port-picking logic.

**Design pointers**

- `doc/design/protocol/design-crow-protocol.md` — root protocol design.
  **Design gap:** this doc has no section covering the port-allocation
  scheme that `crow-protocol/src/ports.rs` already implements
  (base/stride/`ServicePort`). R118 must add a "Port allocation" section
  to the protocol design doc anchoring the scheme; the backlog
  references it as `§<new>` once added. Flagged here rather than
  inventing architecture in the backlog.
- `doc/design/kv/design-crow-kv-server.md` — `crow-kv-server` binary
  startup / HTTP management API / group lifecycle; the per-listener port
  wiring lands here.
- `doc/design/diskdb/design-crow-diskdb.md` and
  `doc/design/chunkdb/design-crow-chunkdb-rpc.md` — diskdb/chunkdb
  listen-address config; CLI-flag unification touches these.

**Use scenarios**

- **Operator single-instance start** — operator runs
  `crow-kv-server --root /node1` with no port flags → server listens on
  `KV_SERVER_MGMT_BASE` (mgmt), `KV_SERVER_GRPC_BASE` (gRPC pool first
  port), `KV_RPC_BASE` (consensus), `KV_CLIENT_RPC_BASE` (client RPC).
  Same for `crow-diskdb` and `crow-chunkdb` with their famous defaults.
  No port 0 anywhere.
- **Operator explicit override** — operator runs
  `crow-kv-server --root /node1 --management-port 9915 --grpc-port 28005
  --consensus-rpc-port 28105 --client-rpc-port 28205` → server listens
  on exactly those ports; defaults ignored for the flags passed.
  Passing `0` is rejected at CLI parse with a clear error.
- **Test parallel cluster start** — a test harness starts 3
  `crow-kv-server` instances + 3 `crow-diskdb` instances on one host in
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
  mgmt/grpc/consensus/client-rpc ports) plus M diskdb instance indices,
  then starts each server with the explicit `--*-port` flags derived
  from the dispatcher. Peers learn each other's listen addresses from
  the assigned indices, not from runtime discovery of OS-assigned ports.
- **Console UI E2E uses the shared prober** — the Playwright fixture
  (`consoleSetup.ts`) stops using its private `freePort()` counter and
  `PORT_BASE`/`PORT_CEILING`/`portBase` magic numbers; instead it
  shells out to `crow-port-alloc` (the CLI binary) to get ports, then
  passes them to `deployNodeServer` / `deployDiskdb` as before. E2E can
  then lift the `workers: 1` serial constraint and run flows in
  parallel, and a restart-within-TIME_WAIT no longer collides because
  the prober probes the actual system state rather than blindly
  incrementing.

**Solution**

The unification target is clear (every server accepts explicit
per-listener port flags with `ports.rs` defaults; port 0 rejected; one
`ServicePort::port(instance)` formula). The port-picking mechanism is
**decided: in-process port-prober + flock-coordinated claim file**
(no standalone server, no daemon lifecycle). The design draft still
needs to nail down the CLI surface, the claim-file format, and the
claim-to-bind TOCTOU mitigation, but the shape is settled.

**One-line summary**: Wire every server to accept explicit per-listener
ports (defaults from `crow-protocol::ports`), reject port 0, and add an
in-process port-prober + flock-coordinated claim file (library + small
CLI binary) that is the single place picking ports, so tests and cluster
bootstrap run in parallel without bind collisions.

Numbered work items:

1. **Protocol design anchor** — `doc/design/protocol/design-crow-protocol.md`
   (new "Port allocation" section) + `lib/crow-protocol/src/ports.rs`.
   Document the base/stride/`ServicePort` scheme as design (currently
   code-only), add any missing service types (e.g. diskio when it
   lands), and state the "no port 0" rule as a design invariant. Closes
   the design gap flagged above.
2. **`crow-kv-server` CLI unification** — `app/crow-kv-server/src/cli.rs`
   + `lib/crow-kv/src/cluster/kv_server.rs`. Add `--consensus-rpc-port`
   (default `KV_RPC_BASE`) and `--client-rpc-port` (default
   `KV_CLIENT_RPC_BASE`); keep `--management-port` and `--ports` (gRPC
   pool). All port flags reject `0`. Remove the port-0 / OS-assigned
   branch in `KvServer::start` (~lines 68-71) — bind exactly the
   requested port; bind failure is a hard error with a clear message.
3. **`crow-diskdb` CLI unification** — `app/crow-diskdb/src/main.rs` +
   diskdb config. Add CLI flags `--grpc-port` (default
   `DISKDB_GRPC_BASE`), `--http-port` (default `DISKDB_HTTP_BASE`),
   `--rpc-port` (default `DISKDB_RPC_BASE`) that override config; keep
   config as the fallback when the flag is absent. Reject `0`. The
   paired-port invariant (http = grpc + 1 per instance) is enforced or
   documented as overridden when flags are passed individually.
4. **`crow-chunkdb` CLI unification** — chunkdb server entry + config.
   Same shape as diskdb: `--grpc-port` / `--http-port` / `--rpc-port`
   with `CHUNKDB_*_BASE` defaults, reject `0`. Blocked on the chunkdb
   server component landing (see Dependencies).
5. **`crow-web` / `crow-cli` port flags** — `app/crow-web/src/main.rs`,
   `app/crow-cli/src/main.rs`. Already use `WEB_BASE` default; verify
   `0` is rejected and the flag name is consistent with the unified
   scheme.
6. **Port prober + claim file** — new module
   (`lib/crow-protocol/src/port_alloc.rs`) + a small CLI binary
   (`crow-port-alloc`, in `app/crow-port-alloc/`). The **single place**
   that picks ports. Mechanism: `flock` a claim file → probe the system
   for an actually-unused port (bind probe, not a counter) that is not
   already listed in the file → write the selected port to the file →
   unlock. The file is the set of claimed ports. Test shell scripts `rm`
   the file to reset the claim set between runs (avoids exhaustion).
   Ships as both a Rust library (for Rust test harnesses) and a CLI
   binary (for shell scripts and the console UI TS fixture, which can't
   call Rust directly). No daemon, no lifecycle to manage. Replaces the
   scattered per-harness logic (notably the console UI E2E fixture's
   `freePort()` counter in `consoleSetup.ts`). The design draft must
   specify: the claim-file path + format, the probe algorithm (port
   range, bind vs. connect probe), and the claim-to-bind TOCTOU
   mitigation (see Edge cases).
7. **Test harness integration** — `crates/crow-kv/tests/`,
   `crates/crow-diskdb/tests/`, bench scripts under `tools/`, and the
   console UI E2E fixture (`app/crow-web/ui/e2e/fixtures/consoleSetup.ts`).
   - Rust harnesses call the `port_alloc` library directly.
   - Shell scripts and the console UI TS fixture shell out to
     `crow-port-alloc` (TS via `child_process.execSync`).
   - Replace the console UI's private `freePort()` / `PORT_BASE` /
     `PORT_CEILING` / `TopologyDescriptor.portBase` with
     `crow-port-alloc` calls; replace hardcoded/port-0 startup in the
     Rust integration tests and bench scripts likewise.
   - Goal: every test harness gets ports from the one prober so tests
     can run in parallel without serial gating (`workers: 1` can be
     lifted).

Flow diagram (shape only):

```
   Rust test  ──┐                        ┌── claim file (flock) ──┐
                │  port_alloc::alloc()   │   read claimed set     │
   shell       ─┤  (library call)        │   probe free port      │
   script      ─┤                        ├──▶ write port ── unlock │
   console UI  ─┘  crow-port-alloc       │                        │
   (TS)          (CLI, shells out)       └────────────────────────┘
                        │ selected port
                        ▼ explicit --*-port flags
            ┌────────────────────────────────────────┐
            │ crow-kv-server / crow-diskdb /          │
            │ crow-chunkdb / crow-web                 │
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
  it → server bind fails. Mitigation: server retries bind with backoff
  and asks the prober for a fresh port on failure (the prober adds the
  failed port back to the claim file as "tried-and-failed" within the
  same flock session, or the server just re-runs `crow-port-alloc`).
  Outcome: the test eventually binds a free port, no silent failure.
  The design draft must specify the exact retry/re-allocate loop.
- Claim file stale after a crashed test → the file lists ports no
  longer in use, shrinking the free pool. Reset by `rm` in the test
  shell script between runs; outcome: next run starts with a fresh
  claim set, no exhaustion.
- Claim file on NFS → `flock` is unreliable on NFS. Tests run locally,
  so this is a documentation constraint (claim file must be on a local
  filesystem), not a blocker.
- All instances of a service exhausted (instance index beyond the
  documented range) → dispatcher returns a clear "no free index" error
  instead of silently wrapping into another service's range.
- Operator passes inconsistent paired ports (diskdb http ≠ grpc + 1) →
  either rejected or accepted with documented override semantics
  (decided in design).
- A server started with explicit ports outside its service's documented
  range → accepted (operator override) but logged as a warning so
  cluster topology tools can detect misconfiguration.

**Dependencies**

- None on other `R**` items for the CLI unification (items 1–5) —
  `ports.rs` already exists.
- Item 4 (chunkdb CLI) is blocked on the chunkdb server component
  landing (currently only a reserved proto surface per R83/R84 backlog
  notes); if not landed, item 4 is deferred.
- Item 6 (dispatcher) has no upstream `R**` dependency but is the input
  to item 7 (test harness integration) and to any future cluster-
  bootstrap tool.
- Downstream: any future `diskio` server must follow the same scheme
  (pick a base outside the documented ranges, add a `ServicePort`
  variant, add CLI flags) — item 1's design section should name this as
  the extension path.

**Acceptance**

**CLI unification (kv-server)**:

- `crow-kv-server --root /tmp/n1` (no port flags) → mgmt listens on
  `KV_SERVER_MGMT_BASE`, gRPC pool first port on `KV_SERVER_GRPC_BASE`,
  consensus on `KV_RPC_BASE`, client RPC on `KV_CLIENT_RPC_BASE`.
  E2E test.
- `crow-kv-server --root /tmp/n1 --management-port 0` → CLI parse error
  mentioning port 0 is not allowed. Unit test (CLI parse).
- `crow-kv-server --root /tmp/n1 --consensus-rpc-port 28105
  --client-rpc-port 28205` → consensus listens on 28105, client RPC on
  28205, other ports stay at defaults. E2E test.
- `KvServer::start` with a port already in use → returns a hard error
  with the listen addr and a "stop the conflicting process" hint; no
  port-0 fallback. Integration test.

**CLI unification (diskdb / chunkdb / web)**:

- `crow-diskdb` with no port flags → gRPC on `DISKDB_GRPC_BASE`, HTTP on
  `DISKDB_HTTP_BASE`, crow-rpc on `DISKDB_RPC_BASE`. E2E test.
- `crow-diskdb --grpc-port 0` → CLI parse error. Unit test.
- `crow-diskdb --grpc-port 9943 --http-port 9944` → listens on those
  ports; paired invariant http = grpc + 1 holds. E2E test.
- `crow-chunkdb` equivalent of the above (skipped if server not landed
  — stated reason). E2E test / skip.
- `crow-web --port 0` → CLI parse error. Unit test.

**Port dispatcher**:

- Two concurrent `Dispatcher::allocate(KvServerGrpc, 3)` calls on one
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
- **Console UI E2E consolidation** — `consoleSetup.ts` no longer
  defines `PORT_BASE` / `PORT_CEILING` / `nextPort` / `freePort()` or
  reads `TopologyDescriptor.portBase`; every `deployNodeServer` /
  `deployDiskdb` call sources its ports from the shared dispatcher.
  Static check: grep for `freePort` / `PORT_BASE` / `portBase` in
  `app/crow-web/ui/e2e/` returns nothing. Unit test (static check).
- **Console UI E2E parallel** — after migration, a representative E2E
  flow runs green with `workers > 1` (exact worker count pending
  design) and 0 port-conflict errors across N consecutive runs. E2E
  test.

**Invariants**:

- No server binary in the workspace binds port 0 on any listener (grep
  `bind.*0` / `port.*0` in server start paths returns nothing). Unit
  test (static check).
- Every server CLI port flag has a `default_value_t` sourced from
  `crow-protocol::ports` (no literal port numbers in CLI defaults).
  Unit test (static check).

**Test commands**: `pixi run test-protocol` (ports + dispatcher unit
tests), `pixi run cargo test -p crow-kv -p crow-diskdb` (server CLI +
dispatcher integration), relevant `pixi run test-tree-ct` only if C++
changes (none expected), plus `pixi run cargo fmt --all -- --check` and
`pixi run cargo clippy --all-targets -- -D warnings`.

**Open Questions**

1. **Claim-to-bind TOCTOU mitigation** — the chosen mechanism (in-process
   prober + flock-coordinated claim file) serializes *probers against
   each other*, but not *prober against the actual server bind*.
   Between the prober's unlock and the server's bind, a non-coordinated
   process could grab the port, or the port could enter TIME_WAIT. The
   server's bind then fails. Candidate mitigations:
   - **(i) Server bind-retry + re-allocate** — server retries bind with
     backoff and asks the prober for a fresh port on failure (re-runs
     `crow-port-alloc`). Fits CROW's existing server-start flow (servers
     already return a hard error on bind failure; adding a retry-and-
     re-ask loop is small). The prober should mark the failed port as
     "tried-and-failed" in the claim file so the next probe skips it
     within the same run.
   - **(ii) Prober holds flock until caller signals "bound"** — caller
     writes back the port it successfully bound, then the prober
     unlocks. Tighter race window but needs a hand-off seam (CROW
     servers bind themselves, they don't take a pre-bound socket), so
     the server must call back into the prober library after bind —
     doesn't work for the CLI binary path (shell/TS callers can't call
     back).
   - **(iii) Accept the residual race for tests** — the claim file
     makes it rare; rely on the server's hard-error + the test harness
     re-running. Simplest but a flaky test is exactly what R118 is
     trying to eliminate.
   Trade-off: mitigation (i) is the leading candidate (works for both
   library and CLI callers, small code change, no flaky tests) but adds
   a retry loop to every server's start path. Needs a decision on
   whether the retry loop is acceptable in the server start path or
   should live in the test harness only (harness re-runs
   `crow-port-alloc` + restarts the server on bind failure).
2. **Claim-file path + format** — where does the claim file live and
   what format? Candidates: a fixed path under `/tmp/crow-port-alloc/
   claims` (per-host, per-user via `$USER`); a path the caller passes
   (so different test sessions can use different files); a TOML/JSON/
   plain-text format. Plain text (one port per line) is the simplest
   and easiest to `cat`/`grep`/`rm` from a shell script. Needs a
   decision on whether multiple concurrent test sessions on one host
   share one claim file or use separate files (separate files avoid
   cross-session contention but can double-assign if they probe the
   same port range — unless the bind probe catches it, which it does).
3. **Probe port range** — does the prober scan the `ServicePort` ranges
   (9910-9990, 28001-28400) or a dedicated test-only range (e.g.
   30000-32768, what the console UI uses today)? The `ServicePort`
   ranges are small (10-200 ports per service) and shared with
   production defaults, so a test probing them could collide with a
   real server on the host. A dedicated test range avoids that but
   disconnects tests from the famous-port scheme. Needs a decision on
   whether tests should use the `ServicePort` ranges (with the prober
   skipping already-bound ports) or a separate test-only range.
4. **Cross-host coordination** — the chosen mechanism is single-host
   (the claim file is per-host). This is sufficient for tests. For real
   cluster bootstrap (two hosts must not pick overlapping client-facing
   ports that a client will contact), cross-host coordination needs a
   cluster-level registry (likely group-0 sysdata). Needs a decision on
   whether R118 covers cluster bootstrap or only test parallelism. If
   only test parallelism, cross-host is out of scope and this question
   is deferred to a future requirement.
5. **Paired-port override semantics** (diskdb/chunkdb http = grpc + 1)
   — when an operator passes `--grpc-port` but not `--http-port`, does
   http follow grpc + 1 (preserve pairing) or stay at its own default
   (break pairing)? Preserve-pairing is intuitive but surprising if the
   operator expected the default; stay-at-default is explicit but can
   produce a confusingly split pair. Needs a human decision on the
   override rule.
