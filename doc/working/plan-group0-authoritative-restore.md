<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Group-0 Authoritative Restore Plan

Design: [`doc/working/design-group0-authoritative-restore.md`](design-group0-authoritative-restore.md).
Backlog: [`doc/backlog/R104-kv-server-group0-authoritative-restore.md`](../backlog/R104-kv-server-group0-authoritative-restore.md).
Goal: make the toml optional; on restart the server restores all stores/groups from local disk + group 0, requiring only `--root`.

## Config + CLI

- [ ] **`apply_root` helper**: add `CrowKVConfig::apply_root(&mut self, root: &Path)` deriving wal/config/data/log paths; add `node_root: Option<PathBuf>` (`#[serde(skip)]`). Files: `lib/crow-kv/src/common/config.rs`.
- [ ] **CLI changes**: add required `--root`; make `--config` optional (`Option<PathBuf>`); remove `--wal-root`/`--config-root`/`--data-root`. Files: `app/crow-kv-server/src/cli.rs`.
- [ ] **Config-load rewrite**: load toml only if `--config` given else `default()`; call `apply_root(&args.root)`; drop the per-path CLI override block; start config watcher only when `--config` is set. Files: `app/crow-kv-server/src/main.rs`.

## Restore mode (local-disk scan)

- [ ] **New `restore.rs`**: `scan_local_groups(wal_root) -> Vec<LocalGroup>`, `group0_exists(wal_root) -> bool`, `load_local_groups(&[LocalGroup], replica_id, &registry)`. Files: `app/crow-kv-server/src/restore.rs`, `app/crow-kv-server/src/main.rs` (mod decl).
- [ ] **Startup branch in `main.rs`**: scan local groups; if `group0_exists` → restore mode (`load_local_groups` + `wire_remotes_from_group0`), warn-ignore `--stores`/`--groups`; else first-boot mode (`create_and_start_stores` from CLI if given). Files: `app/crow-kv-server/src/main.rs`.

## Group-0 verification + fallback wiring

- [ ] **Rewrite `reconcile.rs`**: replace warn-only body with verification + fallback — scan `/kv/replica/`, decode `ReplicaValue`; for local groups with NO remotes (node-config.json missing/stale), seed remotes from group 0 via the `rebuild_group_with_new_remotes` pattern + `store.add_group`; for groups with remotes, log mismatches but don't overwrite. Files: `app/crow-kv-server/src/reconcile.rs`.

## Persist node root to group 0

- [ ] **Proto**: add `string data_root = 4;` to `KvServerExtra`. Files: `lib/crow-protocol/src/proto/sysdata_type.proto`.
- [ ] **Client threading**: thread `data_root` through `ServiceRegistryClient::register_kv_server`/`heartbeat_kv_server` into `KvServerExtra`. Files: `lib/crow-kv-client` (service registry module).
- [ ] **Keep-alive**: `KeepAliveLoop::spawn` takes `data_root: String` (from `config.node_root`); pass through to register/heartbeat. Files: `app/crow-kv-server/src/keepalive.rs`, `app/crow-kv-server/src/main.rs`.

## Tests

- [ ] **UT `restore.rs`**: `scan_local_groups` (multi-store, stray file, empty store, missing dir); `group0_exists` true/false; `apply_root` path derivation. Files: `app/crow-kv-server/tests/restore_test.rs`.
- [ ] **UT `reconcile.rs`**: `reconcile_with_group0` fallback (quorum=1 group → peer seeded from group 0); verify (peer present → no rebuild); verify-mismatch (peer in group 0 not local → warn, no rebuild). Files: `app/crow-kv-server/tests/reconcile_test.rs`.
- [ ] **E2E single-node**: `--root` only on empty dir → init group 0 → stop → delete toml → restart `--root` only → group 0 restored. Files: `app/crow-kv-server/tests/restore_test.rs`.
- [ ] **E2E two-node**: A hosts group 0 + group 1 leader, B follower; restart B `--root` only → wires A, rejoins quorum. Files: `app/crow-kv-server/tests/restore_test.rs`.
- [ ] **E2E edge**: `/kv/replica/` points here but no local WAL → gap logged, no crash. Files: `app/crow-kv-server/tests/restore_test.rs`.

## Doc fold (on merge)

- [ ] Fold into `doc/design/kv/design-crow-kv-server.md` §2.2 (restore mode) + `design-crow-kv-group0.md` §5.1 (Phase 2 landed); delete `R104` backlog doc + this plan + the design draft.

## File list

- `lib/crow-kv/src/common/config.rs` — `apply_root`, `node_root` field.
- `app/crow-kv-server/src/cli.rs` — `--root` required, `--config` optional, remove 3 path flags.
- `app/crow-kv-server/src/main.rs` — config-load rewrite, restore-mode branch, keep-alive `data_root`.
- `app/crow-kv-server/src/restore.rs` — new: scan + load local groups.
- `app/crow-kv-server/src/reconcile.rs` — `reconcile_with_group0` (verification + fallback wiring).
- `app/crow-kv-server/src/keepalive.rs` — `data_root` param.
- `lib/crow-protocol/src/proto/sysdata_type.proto` — `KvServerExtra.data_root`.
- `lib/crow-kv-client/.../service_registry.rs` — `data_root` threading.
- `app/crow-kv-server/tests/restore_test.rs` — new UT + E2E.
- `app/crow-kv-server/tests/reconcile_test.rs` — wiring UT.

## Test checklist

- [ ] `pixi run cargo test -p crow-kv-server`
- [ ] `pixi run cargo test -p crow-kv-client`
- [ ] `pixi run cargo fmt --all -- --check`
- [ ] `pixi run cargo clippy --all-targets -- -D warnings`
