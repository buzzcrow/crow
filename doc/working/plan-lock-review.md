<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Lock Review Fixes Plan

Implements [the lock design](design-lock-review.md) for
[R121](../backlog/R121-tree-cpp-lock-review.md) and
[R122](../backlog/R122-kv-rust-lock-review.md).

## Safe Starting Changes

- [x] **Skip-list backoff**: add bounded CPU pause followed by scheduler yield
  and contention coverage. Files: `lib/crowdb-tree/include/crowdb-tree/skip_list.h`,
  `lib/crowdb-tree/tests/unit/skip_list_test.cpp`.
- [x] **Range-binding RCU**: replace `RwLock<Vec<_>>` with `ArcSwap<Vec<_>>`
  and test concurrent replace/route snapshots. Files:
  `lib/crowdb-kv-client/{Cargo.toml,src/range_binding.rs,tests/range_binding_test.rs}`.

## C++ Hot Paths

- [ ] **Buffer-pool reservation state**: define per-frame loading/writeback
  states and move miss/victim/flush I/O outside `mu_`. Files:
  `lib/crowdb-tree/{include/crowdb-tree/buffer_pool.h,src/buffer_pool.cpp,tests/unit/buffer_pool_test.cpp}`.
- [x] **Handler RCU table**: atomic immutable handler-table publication and
  late-registration test. Files: `lib/crowdb-rpc/include/crowdb-rpc/server/handler.h`,
  `lib/crowdb-rpc/tests/server_test.cpp`.
- [x] **Lock-free log name lookup**: eliminate shared lookup on formatting. Files:
  `lib/crowdb-common/cpp/{src/log.cpp,tests/log_test.cpp}`.

## Rust Hot Paths

- [ ] **Learner frontier state**: atomic slot/term pair, sharded gap storage,
  and bounded drain owner. Files: `lib/crowdb-kv/src/paxos/learner.rs` and
  learner integration tests.
- [ ] **WAL index shards**: partition updates by pipeline and snapshot shards
  for replay/GC. Files: `lib/crowdb-kv/src/wal/{wal_engine.rs,pipeline_writer.rs,index.rs}`
  and WAL tests.
- [x] **Client latency shards**: remove the request-wide histogram mutex while
  preserving flush counts. Files: `lib/crowdb-kv-client/src/metrics.rs` and tests.
- [ ] **Collector lock boundary**: snapshot references, collect unlocked, then
  register discoveries. Files: `app/crowdb-kv-server/src/engine_collector.rs`
  and metrics tests.

## Deferred Locks

- [ ] **Record retained-lock rationale**: fold the keep/defer decisions into
  the tree, RPC, KV, and client formal design sections after implementation.

## Files

- `doc/working/design-lock-review.md` — detailed design and per-lock decision.
- `doc/working/plan-lock-review.md` — progress and verification.
- Files named by each task above — implementation and focused tests.

## Tests

- [ ] C++ concurrency and regression tests pass with `pixi run test-tree-ct`.
- [ ] RPC tests pass with `pixi run test-rpc-ct`.
- [ ] KV tests pass with `pixi run test-kv-core`.
- [ ] Client tests pass with `pixi run test-kv-client`.
- [ ] Rust/C++ formatting, Clippy, and tree-lint gates pass.
