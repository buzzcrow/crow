<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# R59 Plan: Two Scan Modes + Snapshot Versioning API

## Tasks

- [ ] 1. Proto: add `CreateSnapshot`/`ListSnapshots`/`SnapshotScan`/`ReleaseSnapshot`
      messages + RPCs to `kv.proto`.
- [ ] 2. Engine: add `snapshot_view()` method to `CrowTreeEngine` wrapping
      the existing FFI `Crowtree::snapshot_view()`.
- [ ] 3. Snapshot handle registry: add `SnapshotHandle` struct + per-group
      `DashMap<u64, Arc<SnapshotHandle>>` + lease/expiry sweep.
- [ ] 4. Store: implement `kv_create_snapshot`/`kv_list_snapshots`/`kv_snapshot_scan`/`kv_release_snapshot`
      in `px_kv_store.rs`.
- [ ] 5. RPC handlers: add 4 handlers in `kv_service.rs`.
- [ ] 6. Client: add `create_snapshot`/`list_snapshots`/`snapshot_scan`/`release_snapshot`
      methods in `client.rs`.
- [ ] 7. CLI: add `crow-cli kv snapshot create/list/scan/release` commands.
- [ ] 8. Tests: integration test for snapshot consistency under concurrent
      writes; lease expiry test; backward-compat test.
- [ ] 9. Build + run affected tests (test-kv-core, test-kv-server, test-console-cli).
- [ ] 10. Lint + commit.

## Files

- `lib/crow-kv/src/rpc/proto/kv.proto` — new messages + 4 RPCs
- `lib/crow-kv/src/rpc/kv_service.rs` — 4 new handlers
- `lib/crow-kv/src/cluster/px_kv_store.rs` — snapshot handle registry, 4 new methods
- `lib/crow-kv/src/cluster/px_group.rs` (or inline) — `SnapshotHandle` struct
- `lib/crow-kv/src/kv/crow_tree_engine.rs` — `snapshot_view()` wrapper
- `lib/crow-kv-client/src/client.rs` — 4 new client methods
- `app/crow-cli/src/commands/kv.rs` — snapshot subcommands
- `lib/crow-kv/tests/` — integration tests

## Test Checklist

- [ ] `test-kv-core` passes
- [ ] `test-kv-server` passes
- [ ] `test-console-cli` passes
- [ ] `cargo clippy --all-targets -- -D warnings` passes
- [ ] `cargo fmt --check` passes
