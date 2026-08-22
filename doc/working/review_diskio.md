<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# diskio — Dummy Disk & Engine Review

Review of the dummy/simulated engine flow and decisions for restructuring
the diskio disk model. Records the discussion that started from the
question: can the dummy engine exercise the full io_uring round-trip
without touching real storage?

- **Root design doc**: [`doc/design/diskio/design-crow-diskio.md`](../design/diskio/design-crow-diskio.md)
- **Working design draft**: [`doc/working/design-diskio-disk-io-engine.md`](design-diskio-disk-io-engine.md)
- **Related**: [`doc/design/diskdb/design-crow-diskdb.md`](../design/diskdb/design-crow-diskdb.md)
  (DiskValue, hardware hierarchy, group-0 sync)

## 1. Problem

The current `DummyEngine` is a leaf `IoEngine` that calls `on_complete`
synchronously in the caller's thread. No ring, no SQE, no kernel
involvement. It measures RPC + callback overhead at memory speed but
does not exercise the io_uring path (SQE prep, `io_uring_submit`, CQE
dispatch, reactor batching).

`SimulatedEngine` is a separate wrapper engine that injects latency and
errors. It wraps another `IoEngine` (always `DummyEngine` in tests) and
spawns detached threads for delay injection.

Both are rejected by `dio_main.cpp`'s `create_engine` switch — the
production binary only accepts `Uring` and `Blocking`. The `--engine
dummy` and `--engine simulated` CLI flags parse successfully but the
binary exits with an error.

The gap: there is no way to run the full uring (or blocking) I/O path
without a real block device or file. Benchmarks and end-to-end tests
that want to measure uring overhead or verify I/O correctness without
disk hardware have no path through the production binary.

## 2. Options Considered

Three approaches for a "dummy block device" that exercises the full
uring flow:

- **Option A — `memfd_create` disk**: a disk backed by an anonymous
  in-memory file (tmpfs). Single fd, supports both `pwrite` and
  `pread`. Full `IORING_OP_READ`/`WRITE` path executes. No disk I/O.
  `ftruncate` sets size; writes at repeated offsets don't grow memory
  unboundedly. Use the existing `UringEngine` unchanged — the "dummy"
  is at the disk level.

- **Option B — `/dev/null` + `/dev/zero`**: writes to `/dev/null`
  (succeed, data discarded, zero storage); reads from `/dev/zero`
  (succeed, fill buffer with zeros, zero storage). Two different fds,
  but `Disk::fd()` returns one fd — would need splitting into
  `read_fd()`/`write_fd()` or per-op fd selection in the engine.

- **Option C — `IORING_OP_NOP`**: add `Reactor::submit_nop()` that
  preps `IORING_OP_NOP` instead of read/write. Exercises ring
  mechanics (SQE → CQE) but not the read/write syscall path (no fd
  lookup, no file system layer, no buffer copy). Less representative
  of real I/O overhead.

## 3. Decisions

### 3.1 Use both A and B — two dummy disk types

- **NullDisk** (default) — Option B. Drop writes (`/dev/null`), return
  predesigned data on reads (`/dev/zero` or a pattern buffer hack).
  Goes through the full uring/blocking `pwrite`/`pread` syscall path.
  For benchmark tests: measures uring overhead without storage.
  Default disk type when no real block device is configured.

- **MemDisk** — Option A. In-memory storage via `memfd_create` (or an
  in-process buffer). Actual data persistence within the process
  lifetime. For end-to-end correctness tests: verify that write data
  can be read back correctly. Split one mem device across all disks
  (each disk gets a slice of the memfd's address space by offset).
  Don't create large disks — this is for correctness, not capacity.

### 3.2 Merge DummyEngine + SimulatedEngine into one

`SimulatedEngine`'s fault injection (latency, error rate) moves into
the dummy disk itself. One unified dummy disk type with optional
`DiskProperties` (latency_min_ms, latency_max_ms, error_rate). When
properties are set, the disk injects latency and errors before
delegating to the real engine. No separate wrapper engine needed.

The dummy disk goes through the real engine (uring or blocking) —
fault injection wraps the completion callback, not the engine. This
means the full uring flow still executes even with fault injection.

### 3.3 Remove engine selection from CLI

No `--engine` flag. Auto-detect at startup:
- Try `UringEngine` first (Linux with liburing). If construction
  succeeds, use it.
- Fall back to `BlockingEngine` (pwrite/pread thread pool) if uring is
  unavailable or construction fails.

The engine is an implementation detail of the disk, not a user choice.
The `EngineType` enum, `parse_engine_type`, and the `--engine` CLI arg
are removed from `DioConfig`.

### 3.4 Add device_path to DiskValue

The current `DiskValue` proto (`lib/crow-protocol/src/proto/
diskdb_type.proto`) has no device path field:

```protobuf
message DiskValue {
  DiskType disk_type       = 1;
  uint64   capacity_units  = 2;
  uint64   zone_size_units = 3;
  uint32   unit_size_bytes = 4;
  uint32   zone_count      = 5;
  crow.common.HwStatus status = 6;
}
```

Add `string device_path = 7;` — the block device path on the node
(e.g. `/dev/nvme0n1`). diskio uses this to open the block device by
disk UUID → device path mapping. When creating new disks (by API or
UI), the operator selects the device from a list. For now, default to
dummy (no device path → NullDisk).

### 3.5 Device discovery via lsblk

The diskio server (or the console) enumerates available block devices
on the node via `lsblk` (or equivalent). The list excludes the system
disk (the disk containing `/` or the boot partition). The operator
selects a device when creating a disk record in group-0. This is a
future production feature; for now, dummy disks are the default.

### 3.6 Keepalive → group-0 disk info sync

diskio runs one process per node, in charge of all disks on that node.
When it sends keepalive to group-0 (via `ServiceRegistryClient`), it
also fetches the disk info (the UUID → device path map, disk status)
from group-0 via `HardwareClient`. This is the same sync pattern as
diskdb (fixed 10s interval, fetch metadata, update group-0 first on
local status changes).

In mem disk mode, the server can split one mem device across all
disks in the disk map — each disk gets a contiguous slice of the
memfd's offset range. This lets end-to-end tests verify I/O correctness
across multiple logical disks without allocating multiple memfds.

### 3.7 Tests never write to real disk

All diskio tests use dummy disks (NullDisk or MemDisk). No test opens
a real block device or writes to a real file on disk. The existing
`FileDisk`-based tests that write to temp files are replaced with
MemDisk-based tests.

## 4. Impact on Existing Code

### Files to change

- `app/crow-diskio/src/dio_main.cpp` — remove `EngineType` switch;
  auto-detect uring→blocking; construct dummy disks when no device
  path is configured.
- `app/crow-diskio/src/dio_config.h` / `.cpp` — remove `EngineType`
  enum, `parse_engine_type`, `--engine` flag. Add dummy disk config
  (disk type: null/mem, optional fault injection properties).
- `app/crow-diskio/src/engine/dummy/dummy_engine.{h,cpp}` — delete.
  Dummy is now a disk type, not an engine type. The real engine
  (uring/blocking) handles I/O; the dummy disk provides the fd.
- `app/crow-diskio/src/engine/simulated/simulated_engine.{h,cpp}` —
  delete. Fault injection moves into the dummy disk.
- `app/crow-diskio/src/disk/mem_disk.{h,cpp}` — rework: backed by
  `memfd_create` instead of a pure in-process pattern buffer. Support
  read-back of written data. Optional fault injection properties.
- `app/crow-diskio/src/disk/simulated_disk.{h,cpp}` — delete. Merged
  into the dummy disk.
- `app/crow-diskio/src/disk/disk.h` — add `DiskType::Null`. Disk base
  may need `read_fd()` / `write_fd()` if NullDisk uses split fds.
- `app/crow-diskio/src/disk/disk_set.{h,cpp}` — build dummy disks when
  no device path is configured.
- `app/crow-diskio/tests/dummy_engine_test.cpp` — rewrite as
  NullDisk/MemDisk tests through the real engine.
- `app/crow-diskio/tests/simulated_engine_test.cpp` — rewrite as
  fault-injection tests on the unified dummy disk.
- `lib/crow-protocol/src/proto/diskdb_type.proto` — add
  `device_path` field to `DiskValue`.
- `doc/design/diskio/design-crow-diskio.md` — update §5 (IoEngine),
  §6 (Disk Model), §11 (Configuration) to reflect the merged dummy
  disk, auto-detect engine, and device path.

### Files to add

- `app/crow-diskio/src/disk/null_disk.{h,cpp}` — NullDisk: memfd-backed
  dummy disk. Full pwrite/pread path executes; read callback overwrites
  buffer with deterministic pattern data. Optional fault injection
  properties.

## 5. Resolved Questions

- **NullDisk read content** — Resolved: run the full pread through the
  inner engine (uring/blocking), then hack the completion callback to
  overwrite the buffer with deterministic pattern data. NullDisk does
  not rely on `/dev/zero` content. Implemented in `DummyDiskEngine`
  with `hack_reads_=true` + `fill_pattern()`.

- **Disk interface split** — Resolved: use `memfd_create` (option b).
  A single memfd handles both read and write — no `Disk` interface
  change needed. The full `pwrite`/`pread` syscall path executes
  against tmpfs; writes are discarded (NullDisk never reads them
  back), reads execute the full pread then the callback overwrites
  with pattern data. A two-device wrapper (`/dev/null` + `/dev/zero`)
  would require splitting `Disk::fd()` into `read_fd()`/`write_fd()`,
  rippling through `UringEngine`, `BlockingEngine`, and the reactor —
  not worth it when memfd achieves the same goal with zero interface
  change.

- **lsblk integration** — Resolved: console UI server work. The
  console server SSHes to the target node, runs `lsblk`, filters out
  the system disk (the disk containing `/` or the boot partition, or
  by a configurable pattern), and returns the available block device
  list to the UI. The operator selects a device when creating a disk
  record in group-0 via `HardwareClient`. This is a future console
  feature, not part of the dummy disk rework.