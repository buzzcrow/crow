<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Cluster — Unify Port Usage & Test Port Dispatcher Plan

Design draft: `doc/working/design-cluster-unify-port-usage.md`
Backlog doc: `doc/backlog/R118-cluster-unify-port-usage.md`

Goal: reschedule all CROWDB service ports to >10000, reject port 0
everywhere, replace all port-picking code with a single flock prober,
and wire group-0 service discovery.

## Phase 1: Port reschedule + `ports.rs` rewrite

- [ ] **Rewrite `ports.rs` constants + enum**: update all `*_BASE`
  constants to new port map (kv 10000/10100, diskdb 11000/11100/11200,
  chunkdb 12000/12100/12200, diskio 13000, web 14000). Remove
  `KV_RPC_BASE` / `KV_CLIENT_RPC_BASE`. Add `DISKIO_RPC_BASE`. Remove
  `KvServerRpc` / `KvServerClientRpc` enum variants. Add `DiskdbRpc` /
  `DiskioRpc` variants. Change all `stride()` to return 1. Files:
  `lib/crowdb-protocol/src/ports.rs`.
- [ ] **Update `lib.rs` re-exports**: remove `KV_RPC_BASE`,
  `KV_CLIENT_RPC_BASE`; add `DISKIO_RPC_BASE`. Files:
  `lib/crowdb-protocol/src/lib.rs`.
- [ ] **Rewrite `ports_test.rs`**: assert new constants, stride 1 for
  all variants, `DiskdbRpc` / `DiskioRpc` variants. Files:
  `lib/crowdb-protocol/tests/ports_test.rs`.
- [ ] **Run `pixi run test-protocol`**: verify ports tests pass.

## Phase 2: Port prober library + CLI

- [ ] **Write `port_alloc.rs` library**: `PortAllocConfig`,
  `alloc_port`, `alloc_port_range`, `mark_failed`, `reset_claims`.
  Flock + bind-probe mechanism. Files:
  `lib/crowdb-protocol/src/port_alloc.rs`.
- [ ] **Add `port_alloc` to `lib.rs` module + re-exports**: Files:
  `lib/crowdb-protocol/src/lib.rs`.
- [ ] **Write `crowdb-port-alloc` CLI binary**: clap CLI wrapping
  `port_alloc` library. `--root`, `--offset`, `--service`,
  `--instance`, `--count`, `--reset`. Files: `app/crowdb-port-alloc/
  src/main.rs`, `app/crowdb-port-alloc/Cargo.toml`.
- [ ] **Write port_alloc unit tests**: alloc, alloc_range, mark_failed,
  reset, offset. Files: `lib/crowdb-protocol/tests/port_alloc_test.rs`
  or inline `#[cfg(test)]`.
- [ ] **Run `pixi run test-protocol`**: verify prober tests pass.

## Phase 3: Server CLI unification

- [ ] **`crowdb-kv-server` CLI**: reject port 0 on `--management-port`
  and `--ports` (update `parse_port_list`). Files:
  `app/crowdb-kv-server/src/cli.rs`.
- [ ] **`KvServer::start` cleanup**: remove port-0 transient bind
  branch (lines 68-83), remove `start_rpc_server` /
  `start_client_rpc_server` no-op stubs. Files:
  `lib/crowdb-kv/src/cluster/kv_server.rs`.
- [ ] **`crowdb-diskdb` CLI**: add `--listen-port`, `--http-port`,
  `--rpc-port` flags with 0-rejection. Update config defaults in
  `ddb_config.rs`. Files: `app/crowdb-diskdb/src/main.rs`,
  `app/crowdb-diskdb/src/ddb_config.rs`.
- [ ] **`crowdb-chunkdb` CLI**: add `--listen-port`, `--http-port`,
  `--rpc-port` flags. Remove `--listen-addr` and `listen_addr` config
  field. Update config defaults. Files:
  `app/crowdb-chunkdb/src/main.rs`,
  `app/crowdb-chunkdb/src/chunkdb_config.rs`.
- [ ] **`crowdb-diskio` CLI**: default `listen_port` to 13000 in
  `dio_config.h`, reject 0 in `validate()`. Files:
  `app/crowdb-diskio/src/dio_config.h`,
  `app/crowdb-diskio/src/dio_config.cpp`.
- [ ] **`crowdb-web` CLI**: reject port 0 on `--port`. Files:
  `app/crowdb-web/src/main.rs`.
- [ ] **`crowdb-cli` CLI**: replace literal `9910` with
  `KV_SERVER_MGMT_BASE`. Files: `app/crowdb-cli/src/main.rs`.
- [ ] **Run `pixi run cargo clippy --all-targets -- -D warnings`**:
  verify all CLIs compile clean.

## Phase 4: Lifecycle + deploy path migration

- [ ] **`resolve_diskdb_config_path` independent ports**: change
  signature to accept `listen_port`, `http_port`, `rpc_port`
  independently. Remove `http_port = rpc_port + 1` /
  `rpc_listen_port = rpc_port + 2` derivation. Files:
  `lib/crowdb-console-shared/src/lifecycle.rs`.
- [ ] **`local_deploy` / `deploy_servers` migrate to `port_alloc`**:
  replace `test_ports::unique_test_port_range` with
  `port_alloc::alloc_port` / `alloc_port_range`. Add TOCTOU retry
  loop (max 3 retries, `mark_failed` on bind failure). Files:
  `lib/crowdb-console-shared/src/ops/cluster.rs`.
- [ ] **`local_deploy_rpc` migrate to `port_alloc`**: replace
  `test_ports::unique_test_port()`. Files:
  `lib/crowdb-console-shared/src/ops/cluster.rs`.
- [ ] **Run `pixi run cargo test -p crowdb-console-shared`**: verify
  deploy tests pass.

## Phase 5: Test harness migration

- [ ] **Migrate ~19 test files from `unique_test_port*` to
  `port_alloc`**: grep for `unique_test_port` across all test files,
  replace each call with `port_alloc::alloc_port` /
  `alloc_port_range`. Files: all `crates/*/tests/*.rs` and
  `lib/*/tests/*.rs` calling `test_ports::*`.
- [ ] **Delete `test_ports.rs`**: remove the module file and its
  re-exports from `lib.rs`. Files:
  `lib/crowdb-console-shared/src/test_ports.rs`,
  `lib/crowdb-console-shared/src/lib.rs`.
- [ ] **Run affected tests**: `pixi run cargo test -p crowdb-kv -p
  crowdb-diskdb -p crowdb-console-shared`.

## Phase 6: E2E fixture migration

- [ ] **Replace `freePort` / `freePortRange` with `crowdb-port-alloc`
  calls**: delete `PORT_BASE`, `PORT_CEILING`, `nextPort`, `freePort`,
  `freePortRange`. Add `allocPort` helper using
  `child_process.execSync`. Update `deployKvServers` and
  `deployDiskdbInstances`. Files:
  `app/crowdb-web/ui/e2e/fixtures/crowClusterDeployer.ts`.
- [ ] **Static check**: grep `freePort` / `PORT_BASE` in
  `app/crowdb-web/ui/e2e/` returns nothing.
- [ ] **Run E2E tests**: `pixi run test-console-ui-e2e` (or the
  relevant Playwright task).

## Phase 8: Design docs update

- [ ] **Update `design-crowdb-kv-rpc.md` §4**: remove separate
  consensus/client RPC port description, document collapsed
  single-port reality. Files: `doc/design/kv/design-crowdb-kv-rpc.md`.
- [ ] **Update `design-crowdb-kv-rpc-client.md` §1, §4**: same —
  document collapsed reality. Files:
  `doc/design/kv/design-crowdb-kv-rpc-client.md`.
- [ ] **Add "Port allocation" section to
  `design-crowdb-protocol.md`**: document the base/stride/
  `ServicePort` scheme, the new port map, the "no port 0" invariant,
  and the group-0 discovery model (reference to the new requirement
  for full discovery design). Files:
  `doc/design/protocol/design-crowdb-protocol.md`.

Note: Group-0 service discovery (§10 of the design draft) is split to
a new requirement. R118 lays the groundwork (famous mgmt port 10000
for group-0 bootstrap); the new requirement builds the discovery
layer.

## File list

- `lib/crowdb-protocol/src/ports.rs` — rewrite constants, enum, stride
- `lib/crowdb-protocol/src/port_alloc.rs` — NEW: prober library
- `lib/crowdb-protocol/src/lib.rs` — update re-exports
- `lib/crowdb-protocol/tests/ports_test.rs` — rewrite assertions
- `app/crowdb-port-alloc/src/main.rs` — NEW: CLI binary
- `app/crowdb-port-alloc/Cargo.toml` — NEW: crate manifest
- `app/crowdb-kv-server/src/cli.rs` — reject port 0
- `lib/crowdb-kv/src/cluster/kv_server.rs` — remove port-0 branch, stubs
- `app/crowdb-diskdb/src/main.rs` — add port flags
- `app/crowdb-diskdb/src/ddb_config.rs` — update defaults
- `app/crowdb-chunkdb/src/main.rs` — add port flags, remove --listen-addr
- `app/crowdb-chunkdb/src/chunkdb_config.rs` — update defaults, remove listen_addr
- `app/crowdb-diskio/src/dio_config.h` — default port 13000
- `app/crowdb-diskio/src/dio_config.cpp` — reject port 0
- `app/crowdb-web/src/main.rs` — reject port 0
- `app/crowdb-cli/src/main.rs` — replace literal with constant
- `lib/crowdb-console-shared/src/test_ports.rs` — DELETE
- `lib/crowdb-console-shared/src/lib.rs` — remove test_ports re-exports
- `lib/crowdb-console-shared/src/lifecycle.rs` — independent ports
- `lib/crowdb-console-shared/src/ops/cluster.rs` — migrate to port_alloc
- `app/crowdb-web/ui/e2e/fixtures/crowClusterDeployer.ts` — replace freePort
- ~19 test files — migrate from unique_test_port* to port_alloc
- `doc/design/kv/design-crowdb-kv-rpc.md` — update to collapsed reality
- `doc/design/kv/design-crowdb-kv-rpc-client.md` — update to collapsed reality
- `doc/design/protocol/design-crowdb-protocol.md` — add Port allocation section

## Test checklist

### Unit tests

- [ ] ports_test: all new constants, stride 1, DiskdbRpc/DiskioRpc variants
- [ ] parse_port_list: rejects 0
- [ ] port_alloc: alloc_port, alloc_port_range, mark_failed, reset, offset
- [ ] diskio validate: rejects port 0
- [ ] chunkdb: listen_addr field removed (static check)

### Integration tests

- [ ] kv-server: default ports (10000/10100)
- [ ] kv-server: port 0 rejected at CLI
- [ ] kv-server: bind failure is hard error
- [ ] diskdb: default ports (11000/11100/11200)
- [ ] diskdb: independent port override (no pairing)
- [ ] diskio: default port (13000)
- [ ] local_deploy: parallel cluster start, 0 bind failures
- [ ] local_deploy: TOCTOU retry on bind failure

### E2E tests

- [ ] E2E fixture: no freePort/PORT_BASE (static check)
- [ ] E2E: representative flow with workers > 1, 0 port conflicts
- [ ] E2E: parallel cluster start via prober

### Quality gate

- [ ] `pixi run cargo fmt --all -- --check`
- [ ] `pixi run cargo clippy --all-targets -- -D warnings`
- [ ] `pixi run test-protocol`
- [ ] `pixi run cargo test -p crowdb-kv -p crowdb-diskdb -p crowdb-console-shared`
- [ ] `pixi run test-tree-ct` (if C++ changes — diskio)
- [ ] E2E Playwright task
