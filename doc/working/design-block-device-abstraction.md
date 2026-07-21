<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# R19 Design: Unify Block Device Abstraction

## Problem statement

The WAL `BlockDevice` (`crowkv/src/wal/block_backend.rs`) conflates two
unrelated responsibilities behind a single `use_real_files: bool` flag:

1. **Real file I/O** — opens OS files, uses `pwrite`/`pread` via
   `FileExt`, calls `sync_data` for `fdatasync`. This is the production
   path: open a path, do positional I/O with alignment. In Unix, a
   block device *is* a file — `open("/dev/nvme0n1")` and
   `open("/tmp/wal/seg0.dat")` use the same syscall. The BlockDevice
   shouldn't know or care which it got.

2. **In-memory simulation** — stores segments as
   `BTreeMap<PathBuf, Vec<u8>>`, supports error injection
   (`inject_io_error`, `inject_sync_error`, `set_full`), corruption
   injection (`corrupt_at_offset`), and layout tracking. This is a
   **test harness** for deterministic failure testing.

Every method (`open_segment`, `rename_segment`, `unlink_segment`,
`list_layout`, `create_layout`, `contains_path`, `write_at`, `read_at`,
`fdatasync`, `len`, `truncate`) branches on `if self.use_real_files` —
two completely different code paths in one struct.

A related issue: `ssd()` unconditionally sets `O_DIRECT` (Linux
`0o40000`). There is no way to get aligned I/O with real files but
without `O_DIRECT`. Benchmarks that need to exercise alignment/RMW code
paths at high TPS cannot use `ssd()` (disk-bound `fdatasync`) or
`BlockDevice::new()` (in-memory, no real syscalls).

## Current behavior

### WAL BlockDevice

- `BlockDevice::new()` — in-memory `Vec<u8>`, unaligned, `fdatasync` is
  no-op. Used by all unit/integration tests.
- `BlockDevice::ssd()` — real OS files, 4K aligned, `O_DIRECT`,
  `fdatasync` = `sync_data`. Used by `IoBackend::block_device()` (CLI
  `--wal-backend block-device`).
- `IoBackend::File` — `tokio::fs`, `fdatasync` is a **no-op** (only
  `fsync` on close does `sync_all`). Used by benchmarks and production.
- `IoBackend::MemBlock(BlockDevice::new())` — same as `BlockDevice::new()`.
- `IoBackend::BlockDevice(BlockDevice::ssd())` — same as
  `BlockDevice::ssd()`.

### WAL bench (`crowkv/benches/wal.rs`)

Tests `Mem` (in-memory `BlockDevice::new()`) and `File`
(`IoBackend::File`). Does not test the aligned `BlockDevice` path.

### Crowtree C++ storage (`crowtree/src/c_api.cpp`)

The C++ layer has clean backend separation:
- `TextPageStore` — debug text files, IU=1
- `MemPageStore` — in-memory, IU=1
- `BlockPageStore` — real block device, `O_DIRECT`, configurable IU

Each is a distinct C++ class. The Rust FFI `PageStoreBackend` enum
(`File`, `Block`, `MemBlock`) maps to these. The `Block` variant always
uses `O_DIRECT` — same inflexibility as the WAL side, but lower impact
since `SyncMode::kSkip` already handles the "skip fsync" case.

## Proposed approach

### Step 1: Extract `MemBlockDevice`

Move the in-memory simulation into a new `MemBlockDevice` struct:

- Owns `segments: Arc<Mutex<BTreeMap<PathBuf, Vec<u8>>>>`
- Owns `layouts: Arc<Mutex<BTreeSet<PathBuf>>>`
- Owns `BlockDeviceController` (error/corruption injection)
- Owns write/fdatasync counters, amplification metrics
- `MemBlockDevice::new()` — unaligned (IU=1, RAM/SCM/PMEM model)
- `MemBlockDevice::with_alignment(align)` — aligned in-memory (for
  testing RMW logic without real files)
- Methods: `open_segment`, `rename_segment`, `unlink_segment`,
  `list_layout`, `create_layout`, `contains_path` — all operate on the
  in-memory `BTreeMap`/`BTreeSet`, no `if use_real_files` branching.
- Returns `MemBlockSegment` handles that do in-memory `Vec<u8>` reads/
  writes with alignment planning + RMW + amplification tracking.

### Step 2: Simplify `BlockDevice`

`BlockDevice` becomes real-file-only:

- Fields: `alignment`, `use_direct_io: bool`, write/fdatasync counters,
  amplification metrics. No `segments` BTreeMap, no `layouts`, no
  `BlockDeviceController` (error injection is test-harness only).
- `BlockDevice::new()` — aligned (4K), real files, buffered (no
  O_DIRECT). The default production/block-bench constructor.
- `BlockDevice::ssd()` — aligned (4K), real files, O_DIRECT. The
  production SSD/NVMe constructor.
- `BlockDevice::with_alignment(align, use_direct_io)` — explicit.
- `open_segment` always opens a real file via `std::fs::OpenOptions`,
  optionally with `O_DIRECT` on Linux.
- `rename_segment`, `unlink_segment`, `list_layout`, `create_layout`,
  `contains_path` — all delegate to `std::fs` directly. No branching.
- `BlockSegment` always has `file: std::fs::File` (not `Option`).
- `fdatasync` always calls `file.sync_data()`.
- `write_at` / `write_vectored_at` always use `FileExt::write_at`
  (`pwrite`) with alignment planning + RMW + amplification tracking.

### Step 3: Update `IoBackend`

```rust
pub enum IoBackend {
    File,
    MemBlock(MemBlockDevice),
    BlockDevice(BlockDevice),
}
```

- `IoBackend::mem_block()` → `MemBlock(MemBlockDevice::new())`
- `IoBackend::block_device()` → `BlockDevice(BlockDevice::ssd())`
- New: `IoBackend::block_buffered()` →
  `BlockDevice(BlockDevice::new())` (aligned, buffered, real files)
- `open`, `rename`, `unlink`, `read_dir`, `create_dir_all`, `exists` —
  dispatch to `MemBlockDevice` or `BlockDevice` methods.

### Step 4: Update `WalFileInner`

```rust
pub(crate) enum WalFileInner {
    File(file_backend::FileBackendFile),
    MemBlock(block_backend::MemBlockSegment),
    Block(block_backend::BlockSegment),
}
```

### Step 5: Update bench

Add `Backend::Block` to `crowkv/benches/wal.rs`:
- Uses `BlockDevice::new()` (aligned, buffered, real files) with
  `wal_skip_fsync: true` in `WalConfig`.
- Uses `tempfile::tempdir()` for the WAL directory.
- Exercises: alignment planning, RMW, amplification tracking,
  `pwrite`/`pread` syscalls — all block code paths.
- High TPS because `fdatasync` is skipped per batch.

### Step 6: Update all call sites

- `sim_backend()` helpers in test files →
  `IoBackend::MemBlock(MemBlockDevice::new())`
- `BlockDevice::ssd()` in tests that need real aligned files →
  unchanged (still `BlockDevice::ssd()`, now via
  `IoBackend::BlockDevice(...)`)
- `BlockDevice::new()` in tests that need in-memory →
  `MemBlockDevice::new()` via `IoBackend::MemBlock(...)`
- `wal_engine.rs` `backend_name()` → add `"mem-block"` and `"block"`
  match arms.
- `store_registry.rs` `parse_wal_backend()` → unchanged mapping
  (`"mem-block"` → `mem_block()`, `"block-device"` → `block_device()`).

## Crowtree review

The crowtree C++ storage layer does **not** have the `use_real_files`
problem. Each backend is a distinct C++ class:

- `TextPageStore` (debug text files, `CT_BACKEND_FILE`)
- `MemPageStore` (in-memory, `CT_BACKEND_MEM_BLOCK`)
- `BlockPageStore` (real block device with O_DIRECT,
  `CT_BACKEND_BLOCK`)

The C++ `ct_open` function (`crowtree/src/c_api.cpp:196-329`) dispatches
to the correct class based on `ct_options.backend` and whether `path` is
set. There is no flag-based branching within a single class.

The only minor improvement: add a buffered-block option (aligned I/O
without O_DIRECT) to `PageStoreBackend` for benchmark parity. This is
lower priority since crowtree's `SyncMode::kSkip` already handles the
"skip fsync" case, and crowtree benchmarks are less affected by the
O_DIRECT/fsync coupling (the buffer pool absorbs most read traffic).

**No structural change needed on the crowtree side for R19.**

## Alternatives considered

- **Trait-based abstraction** (`trait BlockStorage` implemented by both
  `BlockDevice` and `MemBlockDevice`): rejected because the current
  `IoBackend` enum + `WalFileInner` enum dispatch is already the
  abstraction layer. Adding a trait would introduce dynamic dispatch
  and another indirection layer for no benefit — the enum dispatch is
  zero-cost and already works.

- **Keep `use_real_files`, just add `use_direct_io`**: rejected because
  it doesn't fix the core problem — every method still branches on two
  unrelated code paths in one struct, and the test harness (error
  injection, corruption, in-memory segments) is tangled with the
  production I/O path.

- **mmap + msync**: rejected — requires significant refactoring, doesn't
  match the current `pwrite`/`pread` API, and adds complexity for no
  benefit over the `pwrite` + `sync_data` approach.

## Acceptance criteria

- All existing WAL tests pass unchanged (using `MemBlockDevice` where
  they previously used `BlockDevice::new()`).
- `BlockDevice` no longer has `use_real_files` field — always opens
  real files.
- `MemBlockDevice` owns the in-memory `BTreeMap`,
  `BlockDeviceController`, and layout tracking.
- `BlockDevice::ssd()` uses O_DIRECT; `BlockDevice::new()` uses buffered
  I/O with alignment.
- WAL bench `Block` case runs with `wal_skip_fsync: true` and exercises
  alignment/RMW/`pwrite` code paths.
- `cargo fmt --check`, `cargo clippy -- -D warnings` pass.
- Crowtree C++ tests pass unchanged (no structural change on C++ side).
