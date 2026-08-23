<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Code TODOs / FIXMEs

Tracked `TODO` and `FIXME` comments across the codebase. Update this file
when adding or resolving a tracked item.

## crow-rpc

- **`lib/crow-rpc/src/transport/rdma/rdma_transport.cpp`** (lines 81, 102,
  138, 149, 159, 170, 177, 184, 199) — RDMA transport is a work-in-progress
  stub. Major items: proper slab allocation from the registered memory
  region (currently malloc + reg per buffer), completion queue polling,
  send/recv path, connection management. All tracked inline; consolidate
  here when RDMA work resumes.

## crow-kv

- **`lib/crow-kv/src/wal/file_backend.rs:45`** — switch `write_vectored_at`
  to `FileExt::write_vectored_at` (`pwritev`) once it stabilizes
  (rust tracking issue #89517). Would halve syscall count (no `lseek`).

## crow-tree

- **`lib/crow-tree/include/crow-tree/mapping_table.h:120`** —
  `Options::mapping_segment_slots` is fixed at `kSegmentSize`; needs
  parameterization for variable segment slot counts.
