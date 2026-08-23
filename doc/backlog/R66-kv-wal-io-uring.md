<!-- Copyright 2026-present buzzcrow <126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R66: WAL io_uring Backend — Eliminate `spawn_blocking` on the Durability Path

**Problem**: The WAL's production I/O backend (`IoBackend::File` /
`IoBackend::BlockDevice`) routes `fdatasync` and file writes through
`tokio::fs` or `std::fs::File::sync_data`, both of which internally use
`spawn_blocking` to run the blocking syscall on Tokio's blocking thread
pool (default 512 threads). This is correct but suboptimal:

1. **Thread hop overhead** — every `fdatasync` and `pwrite` hops to a
   blocking thread and back, adding scheduling latency (~5-20 µs per
   hop) to the durability critical path. Under high write throughput
   (100k ops/s), this is 500-2000 thread hops/s for fsync alone.
2. **Blocking pool saturation** — under burst load, the blocking pool
   can become a bottleneck. `fdatasync` is a slow syscall (disk
   latency, typically 10-100 µs for NVMe, 1-10 ms for SSD). If 512
   fsync calls are in flight, the pool is fully consumed and other
   `spawn_blocking` calls (e.g. crow-tree FFI, lifecycle) queue behind
   them.
3. **No true async I/O** — `tokio::fs` is a thin `spawn_blocking`
   wrapper around `std::fs`, not a completion-based async I/O
   substrate. The WAL design doc (`design-crow-kv-wal.md` §4.6)
   acknowledges this: `File` is the "works everywhere" fallback, and
   io_uring is the planned production backend.

The crow-tree B-tree engine already has a mature io_uring engine
(`DiskIOUring` in `crow-common`, `lib/crow-common/cpp/src/diskio_uring.cpp`)
that submits read/write/fsync SQEs on io_uring pipelines and
dispatches CQE completions via callbacks. It is Linux-only (guarded by
`CROW_HAVE_LIBURING`), uses `liburing` (already in the pixi
environment as `liburing-2.14`), and handles `O_DIRECT` aligned I/O.
The WAL should reuse this infrastructure rather than building a
parallel io_uring path.

**Root cause**: The WAL's `IoBackend` abstraction was designed with
three variants (`File`, `MemBlock`, `BlockDevice`) but io_uring was
deferred to "V2" (`io_backend.rs:9`: "Future: `Uring` on Linux >= 5.11
(deferred to V2)"). The `DiskIOUring` engine has since matured, making it
feasible to add a `Uring` variant now.

**Solution**: Add a `Uring` variant to `IoBackend` that reuses
`DiskIOUring` for WAL file I/O, eliminating `spawn_blocking` on the
durability path.

1. **`IoBackend::Uring` variant** — a new backend that wraps
   `DiskIOUring` for WAL segment I/O. Selected at startup when
   `liburing` is available and the platform is Linux (capability probe
   in `IoBackend::detect`, replacing the current always-`File` default).

   - **DiskIOUring sharing**: the WAL creates its own `DiskIOUring`
     instance (one per store, not per group) — a single-pipeline
     topology is sufficient (WAL I/O is not CPU-bound; it's
     syscall-bound). Alternatively, the WAL can share the crow-tree
     engine's `DiskIOUring` if a store has both (preferred — one
     polling thread per store, serving both B-tree page I/O and WAL
     segment I/O). The design should evaluate both options and pick
     the simpler one.
   - **`O_DIRECT` alignment**: WAL segment writes go through
     `pwrite` with `O_DIRECT` (4K aligned) on the `Uring` backend,
     matching the `BlockDevice` variant's alignment discipline. The
     WAL's `pipeline_writer` already batches records into 4K-aligned
     buffers, so the alignment constraint is satisfied. `fdatasync`
     becomes `io_uring_prep_fsync` (no buffer needed).
   - **Fallback**: on non-Linux platforms or when `liburing` is not
     found, `IoBackend::detect` returns `File` (the current default).
     Tests continue to use `MemBlock` for deterministic in-memory I/O.

2. **`WalFile` integration** — add `WalFileInner::Uring` variant that
   implements `fdatasync`, `fsync`, `write_at`, `read_at`, `len`,
   `truncate` via `DiskIOUring` SQE submission + CQE completion futures.
   The Rust FFI layer (`crow-tree-ffi`) exposes `DiskIOUring`'s submit
   API as async functions (returning a `Future` that resolves on CQE
   completion). The WAL's `WalFile` dispatches to these.

   - **Completion-based futures**: `DiskIOUring`'s CQE callback resolves
     a oneshot channel or `ct_future` handle, which the Rust side
     polls as a `Future`. This is the same pattern crow-tree uses for
     `get`/`flush`/`snapshot` async operations — no `spawn_blocking`,
     no thread hop.
   - **Batched submission**: `DiskIOUring` batches SQEs and submits them
     in a single `io_uring_enter` call, amortizing the syscall cost.
     The WAL's `pipeline_writer` already batches writes — the `Uring`
     backend can submit the entire batch as multiple SQEs in one
     `io_uring_enter`, then await all CQEs.

3. **`pipeline_writer` changes** — the pipeline writer's
   `write_batch_sync` function (`pipeline_writer.rs:388`) currently
   calls `segment.fdatasync().await` which routes through
   `spawn_blocking`. On the `Uring` backend, this becomes a
   `DiskIOUring` fsync SQE + CQE await — no thread hop. The `write_at`
   calls for record writes also become `DiskIOUring` write SQEs.

   - **No API change**: `WalFile::fdatasync` and `WalFile::write_at`
     remain `async fn` — the `Uring` variant just has a different
     internal implementation (`DiskIOUring` SQE vs `spawn_blocking`).

4. **Shared io_uring module** — `DiskIOUring` already lives in
   `crow-common` and is shared by crow-tree and diskio. The WAL reuses
   it via FFI. Options:

   - **Option A (preferred)**: expose `DiskIOUring`'s submit API via
     FFI (`crow-tree-ffi`), letting the Rust WAL call
     `uring_submit_write` / `uring_submit_fsync` / `uring_submit_read`
     directly. `DiskIOUring` is already C++ with a C-callable submit
     API (`diskio_uring.h`'s `submit_read`/`submit_write`/`submit_fsync`).
     The FFI layer wraps these into Rust async functions. This reuses
     the mature C++ engine without duplicating logic.
   - **Option B**: rewrite `DiskIOUring` in Rust (using `io-uring` or
     `tokio-uring` crate). This is cleaner (pure Rust, no FFI) but
     duplicates the mature C++ engine and adds a new dependency.
     Defer unless Option A proves awkward.

   Option A is preferred because the C++ `DiskIOUring` is already proven
   (used by crow-tree's page store for demand-load reads, flush, and
   snapshot), handles `O_DIRECT` alignment, and has tested error
   paths. The FFI surface is small (3 submit functions + 1 completion
   poll).

**Scope**:
- `lib/crow-kv/src/wal/io_backend.rs` — add `IoBackend::Uring` variant.
  Update `IoBackend::detect` to probe for `liburing` + Linux and return
  `Uring` when available, `File` otherwise.
- `lib/crow-kv/src/wal/wal_file.rs` — add `WalFileInner::Uring` variant
  implementing `fdatasync` / `fsync` / `write_at` / `read_at` / `len` /
  `truncate` via `DiskIOUring` SQE submission.
- `lib/crow-kv/src/wal/file_backend.rs` — no change (fallback backend).
- `lib/crow-kv/src/wal/block_backend.rs` — no change (test/bench
  backend).
- `lib/crow-kv/src/wal/pipeline_writer.rs` — no API change; the
  `Uring` backend's `fdatasync` / `write_at` are drop-in async fn
  replacements.
- `lib/crow-tree/ffi/src/lib.rs` — expose `DiskIOUring` submit API as
  Rust async functions (`uring_write` / `uring_fsync` / `uring_read`).
  These return a `Future` that resolves on CQE completion.
- `lib/crow-common/cpp/include/crow-common/diskio_uring.h` — no change
  (submit API already exists: `submit_read` / `submit_write` /
  `submit_fsync`).
- `lib/crow-kv/src/wal/segment.rs` — no change (dispatches through
  `WalFile`).
- `lib/crow-kv/tests/wal/` — add tests for the `Uring` backend:
  - `fdatasync` via `DiskIOUring` resolves correctly (data durable
    after CQE).
  - `write_at` via `DiskIOUring` writes correct data at correct
    offset.
  - Batched writes + single fsync via `DiskIOUring`.
  - `O_DIRECT` alignment enforced.
  - Fallback to `File` when `liburing` not available (non-Linux).
  - Error injection: `DiskIOUring` CQE with error result propagates
    to `WalFile::fdatasync` caller.
- `doc/design/kv/design-crow-kv-wal.md` §4.6 — update to document the
  `Uring` variant and the `DiskIOUring` sharing model.

**Complexity**: Medium-High — the `DiskIOUring` submit API already
exists in C++ and is proven. The main work is the FFI bridge (exposing
`DiskIOUring` submit as Rust async functions) and the
`WalFileInner::Uring` variant. The `pipeline_writer` and `segment`
layers need no changes (they already use `async fn` interfaces). The
hardest part is `DiskIOUring` lifecycle management (creation, sharing
with crow-tree, shutdown ordering) and ensuring the CQE completion
future integrates cleanly with Tokio's polling model.

**Dependencies**: None (`DiskIOUring` and `liburing` already exist in
the build). This is a self-contained performance improvement.

**Alternatives considered**:

- **A: Rewrite `DiskIOUring` in Rust (`tokio-uring` crate).** Rejected
  for now — the C++ engine is mature, tested, and handles `O_DIRECT`
  alignment. Duplicating it in Rust adds maintenance burden and a new
  dependency. Revisit if the FFI bridge proves awkward or if we want
  to eliminate the C++ dependency for WAL-only deployments.

- **B: Use `tokio::fs` with a larger blocking pool.** Rejected —
  tuning the blocking pool size (`max_blocking_threads`) is a
  workaround, not a fix. The fundamental issue is thread-hop latency
  on the durability critical path. io_uring eliminates the hop
  entirely.

- **C: Keep `File` backend, add async `fdatasync` via `aio_fsync`.**
  Rejected — Linux `aio_fsync` is unreliable (not all filesystems
  support it, behavior varies). io_uring's `IORING_OP_FSYNC` is the
  modern, reliable async fsync.

- **D: Defer to V2 (as originally planned).** Rejected — `DiskIOUring`
  is now mature, `liburing` is in the pixi environment, and the WAL's
  `IoBackend` abstraction is ready for a new variant. The performance
  win (eliminating thread hops on the durability path) is worth doing
  now.

**Acceptance criteria**:
- `IoBackend::detect` returns `Uring` on Linux with `liburing`
  available, `File` otherwise.
- `WalFileInner::Uring` implements all `WalFile` operations via
  `DiskIOUring` SQE submission — no `spawn_blocking` on any WAL I/O
  path.
- `fdatasync` via `DiskIOUring` resolves only after the kernel
  confirms durability (CQE received).
- `O_DIRECT` alignment enforced for all writes on the `Uring` backend.
- WAL write + fsync latency improves (no thread-hop overhead) —
  measurable in `benches/wal.rs`.
- All existing WAL tests pass on `MemBlock` (unchanged).
- New tests pass on `Uring` (Linux + liburing only; skipped on other
  platforms).
- `pipeline_writer` and `segment` layers unchanged (drop-in async fn
  replacement).
- `DiskIOUring` lifecycle: one instance per store (shared with
  crow-tree if present), clean shutdown on store close.
