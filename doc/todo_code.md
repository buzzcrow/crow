<!-- Copyright 2026-present Gian <crow.db@outlook.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Code TODOs / FIXMEs

Tracked `TODO` and `FIXME` comments across the codebase. Update this file
when adding or resolving a tracked item.

## crowdb-rpc

- **`lib/crowdb-rpc/src/transport/rdma/rdma_transport.cpp`** (lines 81, 102,
  138, 149, 159, 170, 177, 184, 199) — RDMA transport is a work-in-progress
  stub. Major items: proper slab allocation from the registered memory
  region (currently malloc + reg per buffer), completion queue polling,
  send/recv path, connection management. All tracked inline; consolidate
  here when RDMA work resumes.

## crowdb-kv

- **`lib/crowdb-kv/src/wal/file_backend.rs:45`** — switch `write_vectored_at`
  to `FileExt::write_vectored_at` (`pwritev`) once it stabilizes
  (rust tracking issue #89517). Would halve syscall count (no `lseek`).

## crowdb-tree

- **`lib/crowdb-tree/include/crowdb-tree/mapping_table.h:120`** —
  `Options::mapping_segment_slots` is fixed at `kSegmentSize`; needs
  parameterization for variable segment slot counts.
- **`lib/crowdb-tree/src/persist.cpp`** — `prepare_snapshot_locked()` and
  `compact_sparse_blocks()` exceed the function-size limit and mix candidate
  selection, page relocation, snapshot encoding, and commit orchestration.
  Extract shared sparse-block selection and split snapshot preparation into
  focused passes without widening the writer-lock scope.
## crowdb-chunk-client

- **`lib/crowdb-chunk-client/tests/common/mod.rs`** — `LocalFileDiskWriter`
  uses synchronous `std::fs` I/O inside async `DiskWriter` methods (blocks
  the executor). Acceptable for tests; if tests need true async I/O, wrap
  in `spawn_blocking`. Also: `fsync` silently skips non-existent files
  (partial strips where not all disks were written). The production
  `DiskioBlockWriter` should handle this correctly via the diskio server.
- **`lib/crowdb-chunk-client/tests/write_stream.rs`** — `write_stream_whole_strip_retry`
  test no longer injects failures (the old `MockDiskWriter.with_fail_once`
  was removed). Whole-strip retry logic needs to be re-implemented in
  `EcStripWriter` and tested with a `LocalFileDiskWriter` variant that
  can inject failures.
- **`lib/crowdb-chunk-client/tests/write_stream.rs`** —
  `push_mode_drop_mid_write_deletes_partial` has no assertion on
  `delete_calls` — `LargeObjectWriter` doesn't implement `Drop` cleanup
  yet. Need a `Drop` impl that aborts the pipeline + deletes partial
  chunks.
