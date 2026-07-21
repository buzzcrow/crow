<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# R19 Plan: Unify Block Device Abstraction

## Task breakdown

- [ ] T1: Extract `MemBlockDevice` + `MemBlockSegment` from
      `block_backend.rs`
      - Move `segments`, `layouts`, `controller` fields + all
        in-memory code paths into `MemBlockDevice`
      - Move `write_unaligned`, `write_aligned`, in-memory `read_at`,
        `len`, `truncate` into `MemBlockSegment`
      - Keep `BlockDeviceController` shared (or owned by
        `MemBlockDevice` only)
      - Files: `crowkv/src/wal/block_backend.rs`

- [ ] T2: Simplify `BlockDevice` to real-file-only
      - Remove `use_real_files` field, `segments`, `layouts`,
        `controller`
      - Add `use_direct_io: bool` field
      - `BlockSegment.file` becomes `std::fs::File` (not `Option`)
      - Remove all `if self.use_real_files` branches
      - `open_segment` always opens real file, optionally with O_DIRECT
      - `rename_segment`/`unlink_segment`/`list_layout`/`create_layout`/
        `contains_path` delegate to `std::fs`
      - `fdatasync` always calls `file.sync_data()`
      - Constructors: `new()` (buffered aligned), `ssd()` (O_DIRECT
        aligned), `with_alignment(align, use_direct_io)`
      - Files: `crowkv/src/wal/block_backend.rs`

- [ ] T3: Update `IoBackend` enum + dispatch
      - `IoBackend::MemBlock(MemBlockDevice)` + `BlockDevice(BlockDevice)`
      - Update `open`, `rename`, `unlink`, `read_dir`, `create_dir_all`,
        `exists` dispatch
      - Add `block_buffered()` constructor
      - Files: `crowkv/src/wal/io_backend.rs`

- [ ] T4: Update `WalFileInner` + `WalFile` dispatch
      - `WalFileInner::MemBlock(MemBlockSegment)` + `Block(BlockSegment)`
      - Files: `crowkv/src/wal/wal_file.rs`

- [ ] T5: Update re-exports + `wal_engine.rs`
      - `mod.rs`: export `MemBlockDevice`
      - `wal_engine.rs`: `backend_name()` match arms
      - Files: `crowkv/src/wal/mod.rs`, `crowkv/src/wal/wal_engine.rs`

- [ ] T6: Update WAL bench
      - Add `Backend::Block` variant + cases (1, 32, 128 threads)
      - Use `BlockDevice::new()` + `wal_skip_fsync: true` + tempdir
      - Files: `crowkv/benches/wal.rs`

- [ ] T7: Update all test files
      - `block_backend_tests.rs`: `MemBlockDevice` for in-memory tests,
        `BlockDevice` for real-file tests (need tempdir)
      - `wal_engine_tests.rs`: `sim_backend()` →
        `IoBackend::MemBlock(MemBlockDevice::new())`
      - `segment_tests.rs`: `sim_backend()` + `aligned_backend()`
      - `replay_tests.rs`: `sim_backend()`
      - `group/maintenance_test.rs`: `sim_backend()`
      - `group/snapshot_slot_test.rs`: `sim_backend()`
      - Files: `crowkv/tests/wal/`, `crowkv/tests/group/`

- [ ] T8: Update `store_registry.rs`
      - `parse_wal_backend()` — verify mapping still works
      - Files: `crowkv-server/src/store_registry.rs`

- [ ] T9: Lint + test
      - `pixi run cargo fmt --check`
      - `pixi run cargo clippy -- -D warnings`
      - `pixi run test-core` (WAL tests)

## Test checklist

- [ ] `block_backend_tests.rs` — all 6 tests pass
- [ ] `wal_engine_tests.rs` — all tests pass
- [ ] `segment_tests.rs` — all tests pass
- [ ] `replay_tests.rs` — all tests pass
- [ ] `group/maintenance_test.rs` — all tests pass
- [ ] `group/snapshot_slot_test.rs` — all tests pass
- [ ] WAL bench compiles and `Block` case runs

## Dependency ordering

T1 → T2 → T3 → T4 → T5 → T6 (parallel with T7) → T7 → T8 → T9

T1 and T2 are the core refactor. T3-T5 are mechanical updates that
depend on T1+T2. T6 and T7 are independent of each other but both
depend on T3-T5. T8 depends on T3. T9 is the final gate.
