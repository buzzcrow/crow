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
  - `crowdb-kv-server` CLI (`app/crowdb-kv-server/src/cli.rs`) exposes
    `--management-port` (default `KV_SERVER_MGMT_BASE`) and `--ports`
    (listen pool, optional free-form list) but has **no CLI flag** for the
    consensus RPC port (`KV_RPC_BASE`) or the client-facing RPC port
    (`KV_CLIENT_RPC_BASE`). Those listeners fall back to constants the
    operator cannot override per-start.
  - `crowdb-diskdb` (`app/crowdb-diskdb/src/main.rs`) takes listen addresses
    from TOML config (`rpc_listen_addr`, `http_listen_addr`,
    `listen_addr`), not from CLI flags with `ports.rs` defaults — so the
    famous ports are not enforced at the CLI boundary and an operator
    cannot override per-start without editing config. CLI has full
    `--listen-addr` / `--http-addr` address overrides (not per-port
    flags); `0` is not rejected.
  - `crowdb-chunkdb` (`app/crowdb-chunkdb/src/main.rs`, now landed) has
    the same shape as diskdb: `--listen-addr` / `--http-addr` address
    overrides, TOML config with `CHUNKDB_*_BASE` defaults, no per-port
    flags, no `0` rejection.
  - `KvServer::start` (`lib/crowdb-kv/src/cluster/kv_server.rs` ~lines
    68-71) explicitly supports **port 0 for OS-assigned** ports: "Bind a
    TCP listener to determine the actual port (supports port 0 for
    OS-assigned)". This contradicts the project flow — servers must
    listen on explicit/famous ports; port 0 is not allowed. An
    OS-assigned port is not reproducible and not discoverable by peer
    clients without extra plumbing, breaking the deterministic-port flow
    the cluster expects.
  - The console UI E2E fixture ships its **own ad-hoc port allocator**
    disconnected from `ports.rs`: the counter now lives in
    `app/crowdb-web/ui/e2e/fixtures/crowClusterDeployer.ts` (lines 13-26;
    `consoleSetup.ts` is now a re-export shim) and defines
    `PORT_BASE = 30000` / `PORT_CEILING = 32768`
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
    socket or a non-CROWDB process on 30xxx silently collides, (c) is
    console-UI-test-only, not shared with the Rust integration tests or
    bench scripts, and (d) uses a range that has no relationship to the
    `ServicePort` scheme the rest of the project standardizes on.
  - The Rust test harnesses and bench targets use a **different stopgap**:
    `crowdb_console_shared::test_ports::unique_test_port()` (binds
    `127.0.0.1:0`, reads the OS-assigned port, drops the listener). This
    is reliable for parallel tests but relies on port 0 — the very escape
    hatch R118 wants to remove from server CLIs. `tools/bench-rpc-
    regression.sh` hardcodes `SERVER_PORT=18080`. So there are **three**
    parallel port-picking mechanisms (E2E counter, Rust `:0` bind, shell
    hardcoded) none of which is the `ServicePort` scheme.
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
  tests, bench scripts) reinvented its own port-picking logic.

**Design pointers**

- `doc/design/protocol/design-crowdb-protocol.md` — root protocol design.
  **Design gap:** this doc has no section covering the port-allocation
  scheme that `crowdb-protocol/src/ports.rs` already implements
  (base/stride/`ServicePort`). R118 must add a "Port allocation" section
  to the protocol design doc anchoring the scheme; the backlog
  references it as `§<new>` once added. Flagged here rather than
  inventing architecture in the backlog.
- `doc/design/kv/design-crowdb-kv-server.md` — `crowdb-kv-server` binary
  startup / HTTP management API / group lifecycle; the per-listener port
  wiring lands here.
- `doc/design/diskdb/design-crowdb-diskdb.md` and
  `doc/design/chunkdb/design-crowdb-chunkdb-rpc.md` — diskdb/chunkdb
  listen-address config; CLI-flag unification touches these.

**Use scenarios**

- **Operator single-instance start** — operator runs
  `crowdb-kv-server --root /node1` with no port flags → server listens on
  `KV_SERVER_MGMT_BASE` (mgmt), `KV_SERVER_LISTEN_BASE` (listen pool first
  port), `KV_RPC_BASE` (consensus), `KV_CLIENT_RPC_BASE` (client RPC).
  Same for `crowdb-diskdb` and `crowdb-chunkdb` with their famous defaults.
  No port 0 anywhere.
- **Operator explicit override** — operator runs
  `crowdb-kv-server --root /node1 --management-port 9915 --listen-port 28005
  --consensus-rpc-port 28105 --client-rpc-port 28205` → server listens
  on exactly those ports; defaults ignored for the flags passed.
  Passing `0` is rejected at CLI parse with a clear error.
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
  mgmt/listen/consensus/client-rpc ports) plus M diskdb instance indices,
  then starts each server with the explicit `--*-port` flags derived
  from the dispatcher. Peers learn each other's listen addresses from
  the assigned indices, not from runtime discovery of OS-assigned ports.
- **Console UI E2E uses the shared prober** — the Playwright fixture
  (`consoleSetup.ts`) stops using its private `freePort()` counter and
  `PORT_BASE`/`PORT_CEILING`/`portBase` magic numbers; instead it
  shells out to `crowdb-port-alloc` (the CLI binary) to get ports, then
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
ports (defaults from `crowdb-protocol::ports`), reject port 0, and add an
in-process port-prober + flock-coordinated claim file (library + small
CLI binary) that is the single place picking ports, so tests and cluster
bootstrap run in parallel without bind collisions.

Numbered work items:

1. **Protocol design anchor** — `doc/design/protocol/design-crowdb-protocol.md`
   (new "Port allocation" section) + `lib/crowdb-protocol/src/ports.rs`.
   Document the base/stride/`ServicePort` scheme as design (currently
   code-only), add any missing service types (e.g. diskio when it
   lands), add the missing `DiskdbRpc` variant to the `ServicePort` enum
   (`DISKDB_RPC_BASE` exists but has no enum variant), and state the "no
   port 0" rule as a design invariant. Closes the design gap flagged
   above.
2. **`crowdb-kv-server` CLI unification** — `app/crowdb-kv-server/src/cli.rs`
   + `lib/crowdb-kv/src/cluster/kv_server.rs`. Add `--consensus-rpc-port`
   (default `KV_RPC_BASE`) and `--client-rpc-port` (default
   `KV_CLIENT_RPC_BASE`); keep `--management-port` and `--ports` (listen
   pool). All port flags reject `0`. Remove the port-0 / OS-assigned
   branch in `KvServer::start` (~lines 68-71) — bind exactly the
   requested port; bind failure is a hard error with a clear message.
3. **`crowdb-diskdb` CLI unification** — `app/crowdb-diskdb/src/main.rs` +
   diskdb config. Add CLI flags `--listen-port` (default
   `DISKDB_LISTEN_BASE`), `--http-port` (default `DISKDB_HTTP_BASE`),
   `--rpc-port` (default `DISKDB_RPC_BASE`) that override config; keep
   config as the fallback when the flag is absent. Reject `0`. The
   paired-port invariant (http = listen + 1 per instance) is enforced or
   documented as overridden when flags are passed individually.
4. **`crowdb-chunkdb` CLI unification** — `app/crowdb-chunkdb/src/main.rs`
   + chunkdb config. Same shape as diskdb: `--listen-port` / `--http-port`
   / `--rpc-port` with `CHUNKDB_*_BASE` defaults, reject `0`. The chunkdb
   server binary has landed; this item is now unblocked.
5. **`crowdb-web` / `crowdb-cli` port flags** — `app/crowdb-web/src/main.rs`,
   `app/crowdb-cli/src/main.rs`. Already use `WEB_BASE` default; verify
   `0` is rejected and the flag name is consistent with the unified
   scheme.
6. **Port prober + claim file** — new module
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
   `freePort()` counter in `consoleSetup.ts`). The design draft must
   specify: the claim-file path + format, the probe algorithm (port
   range, bind vs. connect probe), and the claim-to-bind TOCTOU
   mitigation (see Edge cases).
7. **Test harness integration** — `crates/crowdb-kv/tests/`,
   `crates/crowdb-diskdb/tests/`, bench scripts under `tools/`, and the
   console UI E2E fixture (`app/crowdb-web/ui/e2e/fixtures/consoleSetup.ts`).
   - Rust harnesses call the `port_alloc` library directly.
   - Shell scripts and the console UI TS fixture shell out to
     `crowdb-port-alloc` (TS via `child_process.execSync`).
   - Replace the console UI's private `freePort()` / `PORT_BASE` /
     `PORT_CEILING` / `TopologyDescriptor.portBase` with
     `crowdb-port-alloc` calls; replace hardcoded/port-0 startup in the
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
   console UI  ─┘  crowdb-port-alloc       │                        │
   (TS)          (CLI, shells out)       └────────────────────────┘
                        │ selected port
                        ▼ explicit --*-port flags
            ┌────────────────────────────────────────┐
            │ crowdb-kv-server / crowdb-diskdb /          │
            │ crowdb-chunkdb / crowdb-web                 │
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
  same flock session, or the server just re-runs `crowdb-port-alloc`).
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
- Operator passes inconsistent paired ports (diskdb http ≠ listen + 1) →
  either rejected or accepted with documented override semantics
  (decided in design).
- A server started with explicit ports outside its service's documented
  range → accepted (operator override) but logged as a warning so
  cluster topology tools can detect misconfiguration.

**Dependencies**

- None on other `R**` items for the CLI unification (items 1–5) —
  `ports.rs` already exists. The chunkdb server binary has landed
  (`app/crowdb-chunkdb/`), so item 4 is no longer blocked.
- Item 6 (dispatcher) has no upstream `R**` dependency but is the input
  to item 7 (test harness integration) and to any future cluster-
  bootstrap tool.
- Downstream: any future `diskio` server must follow the same scheme
  (pick a base outside the documented ranges, add a `ServicePort`
  variant, add CLI flags) — item 1's design section should name this as
  the extension path.

**Acceptance**

**CLI unification (kv-server)**:

- `crowdb-kv-server --root /tmp/n1` (no port flags) → mgmt listens on
  `KV_SERVER_MGMT_BASE`, listen pool first port on `KV_SERVER_LISTEN_BASE`,
  consensus on `KV_RPC_BASE`, client RPC on `KV_CLIENT_RPC_BASE`.
  E2E test.
- `crowdb-kv-server --root /tmp/n1 --management-port 0` → CLI parse error
  mentioning port 0 is not allowed. Unit test (CLI parse).
- `crowdb-kv-server --root /tmp/n1 --consensus-rpc-port 28105
  --client-rpc-port 28205` → consensus listens on 28105, client RPC on
  28205, other ports stay at defaults. E2E test.
- `KvServer::start` with a port already in use → returns a hard error
  with the listen addr and a "stop the conflicting process" hint; no
  port-0 fallback. Integration test.

**CLI unification (diskdb / chunkdb / web)**:

- `crowdb-diskdb` with no port flags → listen on `DISKDB_LISTEN_BASE`, HTTP on
  `DISKDB_HTTP_BASE`, crowdb-rpc on `DISKDB_RPC_BASE`. E2E test.
- `crowdb-diskdb --listen-port 0` → CLI parse error. Unit test.
- `crowdb-diskdb --listen-port 9943 --http-port 9944` → listens on those
  ports; paired invariant http = listen + 1 holds. E2E test.
- `crowdb-chunkdb` equivalent of the above (server has landed; no
  skip). E2E test.
- `crowdb-web --port 0` → CLI parse error. Unit test.

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
- **Console UI E2E consolidation** — `consoleSetup.ts` no longer
  defines `PORT_BASE` / `PORT_CEILING` / `nextPort` / `freePort()` or
  reads `TopologyDescriptor.portBase`; every `deployNodeServer` /
  `deployDiskdb` call sources its ports from the shared dispatcher.
  Static check: grep for `freePort` / `PORT_BASE` / `portBase` in
  `app/crowdb-web/ui/e2e/` returns nothing. Unit test (static check).
- **Console UI E2E parallel** — after migration, a representative E2E
  flow runs green with `workers > 1` (exact worker count pending
  design) and 0 port-conflict errors across N consecutive runs. E2E
  test.

**Invariants**:

- No server binary in the workspace binds port 0 on any listener (grep
  `bind.*0` / `port.*0` in server start paths returns nothing). Unit
  test (static check).
- Every server CLI port flag has a `default_value_t` sourced from
  `crowdb-protocol::ports` (no literal port numbers in CLI defaults).
  Unit test (static check).

**Test commands**: `pixi run test-protocol` (ports + dispatcher unit
tests), `pixi run cargo test -p crowdb-kv -p crowdb-diskdb` (server CLI +
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
     `crowdb-port-alloc`). Fits CROWDB's existing server-start flow (servers
     already return a hard error on bind failure; adding a retry-and-
     re-ask loop is small). The prober should mark the failed port as
     "tried-and-failed" in the claim file so the next probe skips it
     within the same run.
   - **(ii) Prober holds flock until caller signals "bound"** — caller
     writes back the port it successfully bound, then the prober
     unlocks. Tighter race window but needs a hand-off seam (CROWDB
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
   `crowdb-port-alloc` + restarts the server on bind failure).
2. **Claim-file path + format** — where does the claim file live and
   what format? Candidates: a fixed path under `/tmp/crowdb-port-alloc/
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
5. **Paired-port override semantics** (diskdb/chunkdb http = listen + 1)
   — when an operator passes `--listen-port` but not `--http-port`, does
   http follow listen + 1 (preserve pairing) or stay at its own default
   (break pairing)? Preserve-pairing is intuitive but surprising if the
   operator expected the default; stay-at-default is explicit but can
   produce a confusingly split pair. Needs a human decision on the
   override rule.
6. **Port-0 rule vs `unique_test_port()` conflict** — R118 wants to
   reject port 0 on every server CLI flag and remove the port-0 bind
   path in `KvServer::start`. But the Rust test harnesses and bench
   targets rely on `unique_test_port()` (bind `127.0.0.1:0` → read
   OS-assigned port → drop listener → pass that port to the server CLI
   as an explicit `--*-port`). So the "no port 0" rule applies to the
   **server CLI parse**, not to the test helper that picks the port.
   This is consistent (the server still binds an explicit non-zero
   port), but the doc should state this distinction clearly so the
   implementer does not remove `unique_test_port()`. Needs confirmation
   that the rule is "server CLIs reject literal `0` on a port flag" and
   not "no code anywhere may bind port 0".
7. **Is the flock claim-file prober over-engineered?** — the existing
   `unique_test_port()` (OS `:0` bind) already solves test parallelism
   reliably and is the standard pattern; it is used by ~15 test files.
   The flock-coordinated claim file adds complexity (TOCTOU window,
   stale-file cleanup, NFS caveat, claim-file path/format decision) for
   the marginal benefit of using `ServicePort`-scheme ports in tests
   instead of OS-assigned ones. Candidate simplification: (a) adopt
   `unique_test_port()` in the E2E fixture (replacing the naive counter,
   lifting `workers: 1`) and defer the flock prober unless a real need
   arises (e.g. cluster bootstrap needing famous-port coordination); or
   (b) build the flock prober as designed. Trade-off: simplicity vs.
   test-port discoverability/famous-port alignment. Needs a decision on
   whether the flock prober is in scope for R118 or should be split into
   a separate requirement.
