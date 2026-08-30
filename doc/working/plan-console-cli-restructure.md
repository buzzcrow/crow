<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Console CLI command restructure plan (R126)

Plan for `doc/working/design-console-cli-restructure.md` and
`doc/backlog/R126-console-cli-command-restructure.md`.

## Dependencies

- `lib/crowdb-console-shared/src/ops/mod.rs` (OpContext) must be implemented
  before any CLI command wrapper can be implemented.
- `ops::cluster` `init` is needed before any other command can be tested
  end-to-end, because it creates group-0.
- `ops::kv_server` depends on `lifecycle.rs` and `config.rs` in `shared`.
- `ops::bench` depends on `ops::kv_data` (for resolving the leader).

## Phase 1 — Shared operations skeleton

- [ ] **Create `lib/crowdb-console-shared/src/ops/mod.rs`**: define `OpContext`
  with `CrowdbClient`, `KVClusterMetaClient`, `HardwareClient`, `reqwest::Client`,
  `ConsoleConfig`, and helper methods (`new`, `admin`, `node_mgmt_url`,
  `node_entry`). Export `Error` helpers. Files:
  `lib/crowdb-console-shared/src/ops/mod.rs`,
  `lib/crowdb-console-shared/src/lib.rs`.

- [ ] **Create `lib/crowdb-console-shared/src/ops/hardware.rs`**: rack,
  node, disk-group, disk CRUD plus `set-status`. Wraps `HardwareClient` and the
  local TOML `ConsoleConfig` for bootstrap. Files:
  `lib/crowdb-console-shared/src/ops/hardware.rs`.

- [ ] **Create `lib/crowdb-console-shared/src/ops/kv_logical.rs`**: store,
  group, replica add/remove/list/inspect using `KVClusterMetaClient` and
  `KVClusterAdmin`. Files:
  `lib/crowdb-console-shared/src/ops/kv_logical.rs`.

- [ ] **Create `lib/crowdb-console-shared/src/ops/kv_server.rs`**: server
  lifecycle `deploy`, `restart`, `stop`, `delete`, `list` using
  `lifecycle::deploy_local` and the TOML config. Files:
  `lib/crowdb-console-shared/src/ops/kv_server.rs`.

- [ ] **Create `lib/crowdb-console-shared/src/ops/kv_data.rs`**: put, get,
  delete, scan, snapshot create/list/scan/release. Resolves the leader endpoint
  from group-0 and calls `CrowdbClient`. Files:
  `lib/crowdb-console-shared/src/ops/kv_data.rs`.

- [ ] **Create `lib/crowdb-console-shared/src/ops/chunk.rs`**: diskdb
  lifecycle and maintenance (usage/scan-status/scan/recalc/compact/rebuild) plus
  chunkdb/diskio/data-plane stubs. Files:
  `lib/crowdb-console-shared/src/ops/chunk.rs`.

- [ ] **Create `lib/crowdb-console-shared/src/ops/cluster.rs`**: `status`,
  `topology`, `init`, `reset`, `clean`. This is the hardest piece and may be
  split into smaller steps. Files:
  `lib/crowdb-console-shared/src/ops/cluster.rs`.

- [ ] **Create `lib/crowdb-console-shared/src/ops/bench.rs`**: kv workload
  runners and rpc bench, using the shared `run_bench` runner logic. Files:
  `lib/crowdb-console-shared/src/ops/bench.rs`.

## Phase 2 — CLI restructure

- [ ] **Rewrite `app/crowdb-cli/src/main.rs`**: replace global flags with
  `--sysmd-ip`/`--sysmd-port`, introduce the four-domain `Domain` enum, dispatch
  to the new command modules. Files:
  `app/crowdb-cli/src/main.rs`.

- [ ] **Rewrite `app/crowdb-cli/src/commands.rs`**: re-export the new command
  modules and remove old ones. Files:
  `app/crowdb-cli/src/commands.rs`.

- [ ] **Create `app/crowdb-cli/src/commands/mod.rs`**: dispatch helpers,
  fail/render/print_json. Files:
  `app/crowdb-cli/src/commands/mod.rs`.

- [ ] **Create `app/crowdb-cli/src/commands/cluster.rs`**: `ClusterVerb` enum
  and thin handlers calling `ops::cluster`. Files:
  `app/crowdb-cli/src/commands/cluster.rs`.

- [ ] **Create `app/crowdb-cli/src/commands/hardware.rs`**: rack, node,
  disk-group, disk subcommand handlers. Files:
  `app/crowdb-cli/src/commands/hardware.rs`.

- [ ] **Create `app/crowdb-cli/src/commands/kv_server.rs`**: `ServerVerb`
  handlers. Files:
  `app/crowdb-cli/src/commands/kv_server.rs`.

- [ ] **Create `app/crowdb-cli/src/commands/kv_logical.rs`**: store, group,
  replica subcommand handlers. Files:
  `app/crowdb-cli/src/commands/kv_logical.rs`.

- [ ] **Create `app/crowdb-cli/src/commands/kv_data.rs`**: put, get, delete,
  scan, snapshot handlers. Files:
  `app/crowdb-cli/src/commands/kv_data.rs`.

- [ ] **Create `app/crowdb-cli/src/commands/chunk_diskdb.rs`**: diskdb
  subcommand handlers. Files:
  `app/crowdb-cli/src/commands/chunk_diskdb.rs`.

- [ ] **Create `app/crowdb-cli/src/commands/chunk_stub.rs`**: chunkdb,
  diskio, and data-plane stub handlers. Files:
  `app/crowdb-cli/src/commands/chunk_stub.rs`.

- [ ] **Create `app/crowdb-cli/src/commands/bench.rs`**: bench kv
  read/write/scan/mix and bench rpc handlers. Files:
  `app/crowdb-cli/src/commands/bench.rs`.

- [ ] **Delete old command modules and bench subdirectory**: remove
  `app/crowdb-cli/src/commands/{rack,node,store,paxos,replica,disk,disk_group,diskdb,kv,server,cluster,bench}.rs`
  and `app/crowdb-cli/src/commands/bench/`.

- [ ] **Update `app/crowdb-cli/src/utils/client.rs`**: replace
  `ConsoleClient` builder with `OpContext` builder. Files:
  `app/crowdb-cli/src/utils/client.rs`.

## Phase 3 — New commands

- [ ] **Implement `cluster reset`**: in `ops::cluster` and CLI handler. Full
  teardown in dependency order. Files:
  `lib/crowdb-console-shared/src/ops/cluster.rs`,
  `app/crowdb-cli/src/commands/cluster.rs`.

- [ ] **Implement `cluster clean`**: in `ops::cluster` and CLI handler. Wipe
  user data, keep group-0 and topology. Files:
  `lib/crowdb-console-shared/src/ops/cluster.rs`,
  `app/crowdb-cli/src/commands/cluster.rs`.

- [ ] **Implement `kv server delete`**: in `ops::kv_server` and CLI handler.
  Require-empty check, graceful stop, remove `ServerEntry`. Files:
  `lib/crowdb-console-shared/src/ops/kv_server.rs`,
  `app/crowdb-cli/src/commands/kv_server.rs`.

- [ ] **Implement `cluster node remove` require-empty check**: in
  `ops::hardware` and CLI handler. Files:
  `lib/crowdb-console-shared/src/ops/hardware.rs`,
  `app/crowdb-cli/src/commands/hardware.rs`.

## Phase 4 — Tests and scripts

- [ ] **Update `tools/bench-kv-read-regression.sh`**: use new cluster + bench
  commands. Files: `tools/bench-kv-read-regression.sh`.

- [ ] **Update `tools/bench-kv-scan-regression.sh`**: use new cluster + bench
  commands. Files: `tools/bench-kv-scan-regression.sh`.

- [ ] **Update `tools/bench-kv-write-regression.sh`**: use new cluster + bench
  commands. Files: `tools/bench-kv-write-regression.sh`.

- [ ] **Update `app/crowdb-cli/tests/cluster_cli_test.rs`**: new command
  names. Files: `app/crowdb-cli/tests/cluster_cli_test.rs`.

- [ ] **Update `app/crowdb-cli/tests/kv_cli_test.rs`**: new data-plane
  command names (`kv scan` only, no `kv list`). Files:
  `app/crowdb-cli/tests/kv_cli_test.rs`.

- [ ] **Update `app/crowdb-cli/tests/mgmt_cli_test.rs`**: store/group/replica,
  rack/node/disk-group/disk new command paths. Files:
  `app/crowdb-cli/tests/mgmt_cli_test.rs`.

- [ ] **Update `app/crowdb-cli/tests/lifecycle_cli_test.rs`**: `kv server`
  lifecycle. Files: `app/crowdb-cli/tests/lifecycle_cli_test.rs`.

- [ ] **Update `app/crowdb-cli/tests/diskdb_cli_test.rs`**: `chunk diskdb`
  commands. Files: `app/crowdb-cli/tests/diskdb_cli_test.rs`.

- [ ] **Update `app/crowdb-cli/tests/bench_benchmark_test.rs`**: new bench
  shape. Files: `app/crowdb-cli/tests/bench_benchmark_test.rs`.

## Phase 5 — Docs

- [ ] **Update `doc/user-manual/user-guide.md` §7**: command examples. Files:
  `doc/user-manual/user-guide.md`.

- [ ] **Revise `doc/design/console/design-crowdb-console.md` §7**: three-layer
  rule, verb vocabulary, remove ConsoleClient CLI routing paragraph. Files:
  `doc/design/console/design-crowdb-console.md`.

## Phase 6 — Verification and commit

- [ ] **Run `pixi run rs-fmt`** and fix formatting.
- [ ] **Run `pixi run rs-lint`** and fix clippy warnings (up to 3 passes).
- [ ] **Run `pixi run test-console-cli`** and fix failures (up to 5 retries).
- [ ] **Run `pixi run test-console-shared`** if ops unit tests are added.
- [ ] **Commit implementation + working docs**.
- [ ] **Fold design draft into `doc/design/console/design-crowdb-console.md`**
  and delete `doc/working/design-console-cli-restructure.md` and this plan.
- [ ] **Commit cleanup**.

## Blocked

None.