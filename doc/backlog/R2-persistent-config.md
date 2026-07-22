<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R2: Persistent node config

**Problem**: Cluster config (racks, nodes, stores, groups, replicas) is
managed in-memory by the console and persisted via `crowkv-console-db.toml`.
The per-node server config is not persisted independently — a node restart
relies on the console to re-push topology. A per-node config file would make
standalone startup deterministic.

**Priority**: Medium — may cause UT bugs as-is; console-less deployments need
it.

**Complexity**: Medium — design a per-node config format, load at startup,
reconcile with runtime API changes.

**Files**: `crowkv-server/src/main.rs`, `crowkv-server/src/store_registry.rs`,
new config module.

**Acceptance**: Node starts with config file, creates stores/groups/replicas
without console intervention. Config file survives restart.
