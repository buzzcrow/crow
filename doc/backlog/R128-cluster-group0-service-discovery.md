<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R128: cluster — Group-0 Service Discovery

**Problem**

CROWDB services (kv-server, diskdb, chunkdb, diskio) already register
themselves to group-0 sysdata under `/srv/<service>/<instance_id>` via
`ServiceRegistryClient` and heartbeat periodically. But **clients don't
use the registry for discovery** — they hardcode `127.0.0.1:<port>` or
require the operator to pass `--sysmd-ip` / `--sysmd-port` explicitly.
The "group-0 is the discovery root" concept (D4 from R118) is not
applied consistently: a client that knows group-0's address should be
able to discover all living services without any out-of-band
configuration.

R118 lays the groundwork — the group-0 kv-server mgmt port is famous
(`KV_SERVER_MGMT_BASE` = 10000) so any client can bootstrap by
contacting `127.0.0.1:10000` (or a configured group-0 address). R128
builds the discovery layer on top: clients query group-0 to find peer
addresses, and the "group-0 is the discovery root" concept is used
consistently through all code.

**Current behavior + impact**

- `ServiceRegistryClient` (`lib/crowdb-kv-client/src/service_registry.rs`)
  is fully implemented: `register`, `heartbeat`, `unregister`,
  `read_instance`, `read_all_instances` with TTL-based liveness
  filtering. All four services already use it:
  - **kv-server** — `app/crowdb-kv-server/src/keepalive.rs` runs a
    background keep-alive loop registering under `/srv/kv-server/`.
  - **diskdb** — `app/crowdb-diskdb/src/liveness/keepalive.rs`
    registers under `/srv/diskdb/` with `owned_dg_ids`.
  - **chunkdb** — `app/crowdb-chunkdb/src/main.rs`
    `spawn_chunkdb_keepalive` registers under `/srv/chunkdb/`.
  - **diskio** — `app/crowdb-diskio/src/group0/group0_sync.cpp`
    heartbeats via FFI `crowdb_svc_heartbeat_diskio` under
    `/srv/diskio/`.
- **But clients don't query the registry for discovery**:
  - `crowdb-cli` (`app/crowdb-cli/src/main.rs`) takes `--sysmd-ip`
    (default `127.0.0.1`) and `--sysmd-port` (default
    `KV_SERVER_MGMT_BASE`) to contact group-0. This is fine for
    bootstrap. But to connect to a diskdb or chunkdb, the CLI either
    hardcodes addresses or requires the operator to pass them
    explicitly — it does not query group-0 to discover living
    diskdb/chunkdb instances.
  - `crowdb-web` (`app/crowdb-web/src/main.rs`) similarly contacts
    group-0 via `--sysmd-ip` / `--sysmd-port` but does not discover
    other services via the registry.
  - `crowdb-console-shared` ops layer
    (`lib/crowdb-console-shared/src/ops/`) builds clients from
    hardcoded or operator-supplied addresses, not from group-0
    registry queries.
  - Test harnesses (`lib/crowdb-test-harness/src/`) deploy services
    and track their addresses in local state — they don't use the
    registry for discovery (acceptable for tests, but the pattern
    leaks into production code paths).
- **Impact**: an operator starting a new diskdb or chunkdb on a
  different host must manually tell every client where it is. There is
  no "start the service, and clients find it automatically" flow. The
  service registry data exists in group-0 but is unused for discovery.

**Design pointers**

- `doc/design/kv/design-crowdb-kv-group0.md` §4 (Service Registry) —
  registration, keep-alive, liveness, expiry. §4.2 lists which
  services are registered.
- `doc/design/kv/design-crowdb-kv-group0.md` §2.7 (kv-server
  keep-alive to group 0) — the keep-alive model.
- `doc/design/kv/design-crowdb-kv-group0.md` §5 (Bootstrap and
  Cutover) — two-phase bootstrap, group-0 as authoritative source.
- `doc/design/protocol/design-crowdb-protocol.md` — R118 adds a "Port
  allocation" section with the famous mgmt port (10000) for group-0
  bootstrap.

**Use scenarios**

- **Operator starts a new diskdb on host B** — the diskdb registers to
  group-0 under `/srv/diskdb/<instance_id>` with its IP + RPC port.
  The operator runs `crowdb-cli disk list` on host A — the CLI
  contacts group-0 at `127.0.0.1:10000` (or a configured address),
  queries `/srv/diskdb/`, finds the new diskdb's address, and connects
  to it. No manual address passing. E2E.
- **Operator starts crowdb-web** — the web console contacts group-0,
  discovers all living kv-server, diskdb, and chunkdb instances from
  the registry, and shows them in the UI. No per-service address
  configuration needed beyond group-0's address. E2E.
- **Service crashes and restarts on a different port** — the service
  re-registers with its new port. Clients that query group-0 see the
  new address on the next registry read. No stale-address connections.
  Integration test.
- **Service is expired (heartbeat missed)** — `read_all_instances`
  filters it out by TTL. Clients don't connect to dead instances.
  Unit test.
- **Simple test without group-0** — a test that deploys a single
  diskdb and connects to it directly via parameter-passed address
  (not via group-0 discovery). This is the test exception — simple
  tests can bypass discovery. Unit/integration test.
- **Multi-host cluster** — kv-server on host A, diskdb on host B,
  chunkdb on host C. Each registers to group-0. A client on host D
  contacts group-0, discovers all three, and connects to each. E2E.

**Solution**

**No clear solution yet — deferred to design.** The high-level shape
is clear (clients query group-0 via `ServiceRegistryClient` to
discover peers), but the exact integration points, caching strategy,
and bootstrap flow need design work.

**One-line summary**: Clients query group-0's service registry to
discover living service instances by IP + port, eliminating hardcoded
addresses; group-0's famous mgmt port (10000) is the bootstrap point.

Numbered work items:

1. **Discovery client API** — `lib/crowdb-kv-client/src/
   service_registry.rs` or a new `discovery.rs` module. Add a
   `ServiceDiscoveryClient` (or extend `ServiceRegistryClient`) that
   wraps `read_all_instances` with caching, TTL refresh, and a
   "discover one" / "discover all" API. The cache must handle group-0
   leader changes (re-seed topology). The design draft must specify:
   cache TTL, refresh strategy (poll vs. watch/notify), and how
   leader-change affects the discovery client.
2. **`crowdb-cli` discovery integration** — `app/crowdb-cli/src/
   main.rs` + command handlers. When the CLI needs to connect to a
   diskdb or chunkdb, it queries group-0 via the discovery client
   instead of requiring the operator to pass the address explicitly.
   The `--sysmd-ip` / `--sysmd-port` flags remain as the group-0
   bootstrap point. The design draft must specify: which commands use
   discovery, fallback behavior when the registry is empty, and
   whether `--diskdb-ip` / `--chunkdb-ip` flags are kept as overrides.
3. **`crowdb-web` discovery integration** — `app/crowdb-web/src/
   main.rs` + `lib/crowdb-console-shared/src/ops/`. The web console
   discovers services via group-0 instead of requiring per-service
   address configuration. The design draft must specify: how the
   `OpContext` obtains service addresses (discovery client vs.
   hardcoded), and how the UI reflects discovered vs. configured
   services.
4. **`crowdb-console-shared` ops layer** —
   `lib/crowdb-console-shared/src/ops/*.rs`. The ops layer builds
   clients from group-0 discovery instead of hardcoded addresses.
   The design draft must specify: which ops functions change, how
   they handle the test exception (simple tests pass addresses
   directly), and whether a "discovery vs. explicit" mode flag is
   needed.
5. **Consistent "group-0 is the discovery root" pattern** — audit all
   code that hardcodes `127.0.0.1:<port>` or requires explicit
   service addresses. Replace with group-0 discovery where
   appropriate. The design draft must specify: which hardcodes are
   replaced, which are kept (test exception, bootstrap), and the
   migration path.
6. **Design doc update** — `doc/design/kv/design-crowdb-kv-group0.md`
   §4. Add a "Client discovery" subsection documenting the discovery
   client API, caching strategy, and the bootstrap flow (group-0
   mgmt port → registry query → service connection).

**Flow diagram**

```
                    bootstrap (famous port 10000)
                          │
                          ▼
                   ┌─────────────┐
                   │  group-0    │
                   │  kv-server  │
                   │  (mgmt 10000)│
                   └──────┬──────┘
                          │
              ┌───────────┼───────────┐
              ▼           ▼           ▼
         /srv/kv-server/ /srv/diskdb/ /srv/chunkdb/
         /srv/diskio/
              │           │           │
              ▼           ▼           ▼
         ┌──────────────────────────────┐
         │  ServiceDiscoveryClient      │
         │  (cache + TTL refresh)       │
         └──────────────┬───────────────┘
                        │
           ┌────────────┼────────────┐
           ▼            ▼            ▼
      crowdb-cli   crowdb-web    test harness
      (discovers   (discovers   (test exception:
       peers)      peers)       passes addrs directly)
```

**Edge cases at a glance**

- group-0 not yet started → discovery client retries with backoff;
  clients wait or fail with "group-0 unreachable" — *handled: retry +
  clear error*.
- group-0 leader change → discovery client re-seeds topology from new
  leader; cache is stale until next refresh — *handled: topology
  re-seed*.
- Service expired (heartbeat missed) → `read_all_instances` filters
  by TTL; clients don't connect to dead instances — *handled: TTL
  filtering*.
- Registry empty (no instances of a service) → discovery returns
  empty list; client fails with "no living <service> instances" —
  *handled: clear error*.
- Test bypasses discovery → simple tests pass addresses directly via
  parameters — *handled: test exception*.
- Multiple instances of same service → discovery returns all living
  instances; client picks one (round-robin, or by instance ID) —
  *design needed: selection strategy*.

**Dependencies**

- **Depends on R118** — R118 reschedules ports so group-0 mgmt port
  is famous (10000), enabling bootstrap discovery. R128 builds the
  discovery client on top of R118's port scheme. If R118 is not yet
  landed, R128 can proceed with the current mgmt port (9910) but
  must update after R118 lands.
- **Depends on existing `ServiceRegistryClient`** — all four services
  already register and heartbeat. R128 adds the client-side discovery
  layer; no service-side changes needed.
- **No dependents** — R128 is a standalone discovery layer; future
  requirements that need service discovery will depend on R128.

**Acceptance**

**Discovery client (item 1)**:
- `ServiceDiscoveryClient::discover(service)` returns living
  instances from group-0, filtered by TTL. Setup: register 3 diskdb
  instances, let 1 expire (miss heartbeat) → discover("diskdb")
  returns 2. Unit test.
- `ServiceDiscoveryClient` cache: after first `discover`, subsequent
  calls within cache TTL return cached result without group-0 query.
  After TTL expiry, next call re-queries group-0. Unit test.
- group-0 leader change: discovery client re-seeds topology and
  continues returning valid results. Integration test.
- group-0 unreachable: `discover` returns error after retry budget
  exhausted. Unit test.

**CLI integration (item 2)**:
- `crowdb-cli disk list` discovers diskdb instances from group-0
  without `--diskdb-ip` flag. Setup: deploy diskdb, register to
  group-0 → `crowdb-cli disk list` shows it. E2E test.
- `--diskdb-ip` override still works (bypasses discovery). E2E test.
- Registry empty → `crowdb-cli disk list` fails with "no living
  diskdb instances". Integration test.

**Web integration (item 3)**:
- `crowdb-web` discovers all services from group-0 and shows them in
  the UI. Setup: deploy kv-server + diskdb + chunkdb, register to
  group-0 → web UI shows all three. E2E test.
- Service restarts on new port → web UI shows new port after cache
  refresh. Integration test.

**Ops layer (item 4)**:
- `ops::hardware` builds diskdb client from discovery, not hardcoded
  address. Integration test.
- Test exception: ops functions accept explicit address parameter,
  bypassing discovery. Unit test.

**Consistency (item 5)**:
- grep `127.0.0.1:99` or `127.0.0.1:10` in `app/` and `lib/` returns
  no hardcoded service addresses (except group-0 bootstrap and test
  exception). Static check.

**Design doc (item 6)**:
- `design-crowdb-kv-group0.md` §4 has a "Client discovery" subsection
  documenting the API, caching, and bootstrap flow. Static check.

**Test commands**: `pixi run cargo test -p crowdb-kv-client`,
`pixi run cargo test -p crowdb-console-shared`,
`pixi run test-console-ui-e2e`, `pixi run cargo fmt --all -- --check`,
`pixi run cargo clippy --all-targets -- -D warnings`.

**Open Questions**

1. **Cache refresh strategy** — poll-based (refresh cache every N
   seconds) vs. watch/notify (subscribe to group-0 changes for
   push-based refresh). Poll is simpler but has staleness window;
   watch/notify is real-time but adds complexity and depends on the
   watch/notify infrastructure. The design draft must decide.
2. **Instance selection strategy** — when multiple living instances
   of a service exist, which one does the client connect to?
   Round-robin, lowest-latency, by instance ID, or operator-specified?
   The design draft must decide.
3. **Discovery vs. explicit mode** — should there be a mode flag
   (`--discover` vs. `--explicit-address`) or should discovery be
   automatic with explicit addresses as overrides only? The design
   draft must decide.
4. **Test harness integration** — should the test harness use
   discovery for deployed services (to verify the discovery path
   works) or keep passing addresses directly (simpler, isolates test
   failures)? Likely a mix: E2E tests use discovery, unit tests pass
   directly. The design draft must specify.
