# CrowKV - Design: Async Local Disk I/O

Depends on: [`design.md`](design.md), [`requirement.md`](requirement.md), [`plan.md`](plan.md) §5 (Concurrency Model)
Satisfies: [requirement.md §8.1](requirement.md#81-wal-write-ahead-log) (durability contract), and underpins [`design-wal.md`](design/design-wal.md), [`design-storage-engine.md`](design/design-storage-engine.md)

This document specifies the project-wide async local disk I/O abstraction. It is shared infrastructure: WAL fsync, ordered-file engine writes, snapshot file I/O, and any future on-disk subsystem all use it.

## Table of Contents

- [1. Goals and Non-Goals](#1-goals-and-non-goals)
- [2. Why a Dedicated Abstraction](#2-why-a-dedicated-abstraction)
- [3. Backend Survey](#3-backend-survey)
- [4. Selected Backend: `tokio-uring` with `spawn_blocking` Fallback](#4-selected-backend-tokio-uring-with-spawn_blocking-fallback)
- [5. Public API](#5-public-api)
- [6. Runtime Topology](#6-runtime-topology)
- [7. Capability Detection and Fallback](#7-capability-detection-and-fallback)
- [8. Buffer Management](#8-buffer-management)
- [9. Error Model](#9-error-model)
- [10. Testing Strategy](#10-testing-strategy)
- [11. Open Questions](#11-open-questions)

---

## 1. Goals and Non-Goals

**Goals:**

- Provide a single async I/O facade for all local disk operations: `open`, `read_at`, `write_at`, `fsync`/`fdatasync`, `close`.
- Avoid blocking the consensus async tasks on disk syscalls, in line with [`plan.md`](plan.md) §5 rule 1.
- On Linux ≥ 5.11, exploit io_uring for true async syscalls (kernel-side completion).
- Run on any environment with a working POSIX filesystem (CI, dev laptops, older kernels) via a transparent fallback.
- Single common API surface — callers do not branch on backend.

**Non-Goals:**

- Network I/O (handled by `tonic` / `tokio` directly).
- Block-device direct I/O / O_DIRECT (deferred until measured to be needed).
- Cross-platform completion-based I/O (Windows IOCP, macOS kqueue) — Linux is the only target per [requirement.md §3](requirement.md#3-dependencies-and-assumptions).
- Process-wide buffer pool tuning (revisit if profiling shows need).

---

## 2. Why a Dedicated Abstraction

Per [`plan.md`](plan.md) §5, every business-logic path in CrowKV is `async`. Disk syscalls (`fdatasync`, `pwrite`, `pread`) are blocking. Two options to bridge:

1. **`tokio::task::spawn_blocking` everywhere.** Simple. Pays one thread-pool hop per syscall (typically 5–20 μs on a healthy host). Tail latency under load suffers because the blocking pool is a shared resource and one slow disk can stall others.
2. **io_uring.** Kernel-side completion queue. No per-syscall thread hop. Lower tail latency under contention. Requires Linux ≥ 5.6 (basic) / 5.11 (mature feature set). Single ring per thread.

For CrowKV, the WAL fsync is on every write's critical path; the engine snapshot write is large but bulk. Both benefit from io_uring under load. We adopt option 2 with option 1 as a fallback.

A thin abstraction lets us:
- Swap backends behind a `cfg` flag without touching call sites.
- Inject a deterministic in-memory backend for `test_harness`-driven unit tests (per [`test.md`](test.md) §8.2).
- Keep the WAL and engine code free of `cfg(target_os = "linux")` branches.

---

## 3. Backend Survey

| Crate | Style | Tokio compat | Status | Notes |
|---|---|---|---|---|
| `tokio-uring` | Per-thread ring; runs alongside a regular `tokio` runtime | Yes (dedicated runtime + `LocalSet`-style scoping) | Maintained by tokio-rs | **Selected** |
| `io-uring` | Low-level safe bindings, no runtime | N/A | Maintained | Building block under `tokio-uring` |
| `monoio` | Thread-per-core; own runtime | No (incompatible) | Maintained (ByteDance) | Conflicts with our `tokio` decision |
| `glommio` | Thread-per-core; own runtime | No | Maintained (Datadog) | Same conflict |
| `compio` | Cross-platform completion | Limited | Active | Reconsider if we ever want Windows |
| `rio` | Higher-level io_uring wrapper | Limited | Less active | Skip |

The thread-per-core runtimes (`monoio`, `glommio`) would offer the absolute lowest latency but force every other module — RPC, leader election, dedup — to be ported to a non-`tokio` runtime. That contradicts the project-wide async decision and the use of `tonic` (which is `tokio`-only).

`tokio-uring` is the only option that lets us keep `tokio` everywhere.

---

## 4. Selected Backend: `tokio-uring` with `spawn_blocking` Fallback

**Linux ≥ 5.11 (kernel feature `IORING_FEAT_FAST_POLL` and stable `OP_FSYNC`):** use `tokio-uring`. `tokio-uring` exposes async file ops mapped to io_uring SQEs:

- `tokio_uring::fs::File::open` / `read_at` / `write_at` / `sync_all` / `sync_data` / `close`.
- `IORING_OP_FSYNC` with `IORING_FSYNC_DATASYNC` flag for `fdatasync`.

**Otherwise:** delegate to `tokio::fs` (which itself uses `spawn_blocking` internally). This covers older kernels, non-Linux dev hosts, and CI containers that may lack io_uring.

The selection is a **runtime decision** (see §7), not a compile-time `cfg`, so the same binary runs on both backends.

---

## 5. Public API

The crate-level facade lives in `io/mod.rs`. All operations are `async fn` and return `io::Result<T>`.

```rust
pub struct AsyncFile { /* opaque */ }

impl AsyncFile {
    pub async fn open(path: &Path, opts: OpenOptions) -> io::Result<Self>;
    pub async fn read_at(&self, buf: BufMut, offset: u64) -> io::Result<usize>;
    pub async fn write_at(&self, buf: Buf, offset: u64) -> io::Result<usize>;
    pub async fn fdatasync(&self) -> io::Result<()>;
    pub async fn fsync(&self) -> io::Result<()>;
    pub async fn close(self) -> io::Result<()>;
    pub async fn len(&self) -> io::Result<u64>;
    pub async fn truncate(&self, len: u64) -> io::Result<()>;
}

pub async fn rename(from: &Path, to: &Path) -> io::Result<()>;
pub async fn unlink(path: &Path) -> io::Result<()>;
pub async fn read_dir(path: &Path) -> io::Result<impl Stream<Item = io::Result<DirEntry>>>;
```

**Design note on buffers.** io_uring requires the kernel to retain ownership of buffers for the duration of the operation. `tokio-uring` models this with owned-buffer types: `read_at` consumes a `BufMut`, returns it back along with the result; `write_at` consumes a `Buf`. Our `AsyncFile` API mirrors this for the io_uring backend; the fallback backend wraps a regular `Vec<u8>` and does the equivalent move semantics so call sites are identical regardless of backend. See §8.

---

## 6. Runtime Topology

`tokio-uring` requires a per-thread ring. Two viable topologies:

### 6.1 Topology A — Dedicated I/O runtime thread (selected)

- Main `tokio` multi-threaded runtime hosts consensus, RPC, learner, etc.
- One additional thread runs a `tokio-uring` runtime (`tokio_uring::start`).
- All `AsyncFile` operations are submitted to that thread via a thin command channel; futures resolved back to caller.
- Pros: simple; isolates io_uring from the main runtime; easy fallback (just use `tokio::fs` on the main runtime instead).
- Cons: one extra thread; channel hop adds ~1 μs latency vs. running directly on the io_uring thread.

### 6.2 Topology B — Per-disk io_uring thread

- One io_uring thread per WAL disk plus one for the engine.
- Eliminates contention between disks at the kernel level.
- Pros: matches the parallelism story of [`design-wal.md`](design/design-wal.md) §3.
- Cons: more threads; more complexity in routing operations.

**Selected: Topology A for V1; revisit Topology B if benchmarks show single-thread io_uring becomes the bottleneck.** Topology A still gets per-disk parallelism *inside* the ring (multiple SQEs in flight to different fds in parallel).

---

## 7. Capability Detection and Fallback

At process startup:

1. Probe kernel version. If `< 5.11`, choose fallback.
2. Try `io_uring_setup(8, ...)`. If it returns `ENOSYS` or `EPERM` (containerized, seccomp), choose fallback.
3. Probe required ops: `IORING_OP_FSYNC`, `IORING_OP_READ`, `IORING_OP_WRITE`. If any missing, fallback.
4. Otherwise, install the io_uring backend.

The selection is logged once at INFO with the chosen backend and probe results. There is no runtime switching — once chosen, the backend is fixed for the process lifetime.

The fallback backend wraps `tokio::fs::File` and routes `fdatasync` through `spawn_blocking` calling `nix::unistd::fdatasync`.

---

## 8. Buffer Management

io_uring's safety contract: while a SQE is in flight, the buffer must not be read or written by user code, and must not be dropped before the corresponding CQE is reaped. `tokio-uring` enforces this at the type level by taking ownership of the buffer and returning it on completion.

**Project rule:** all callers use the same owned-buffer pattern even on the fallback backend. This means the WAL `Segment::write` accepts `Vec<u8>` and gets it back; no `&[u8]` passed across an `await` boundary.

**Pool reuse.** A small per-disk free-list of fixed-size 64 KiB buffers is sufficient for WAL fsync coalescing (matches `wal_fsync_batch_bytes` default in [`design-wal.md`](design/design-wal.md) §4.3). The engine snapshot path uses 1 MiB buffers (matches snapshot chunk size). No global pool — each subsystem owns its own.

**Fixed buffers (registered).** `IORING_REGISTER_BUFFERS` lets us pre-register buffers for zero-copy submission. **Deferred to V2** unless profiling shows submission overhead is a bottleneck.

---

## 9. Error Model

All `AsyncFile` operations return `std::io::Result<T>`. io_uring CQE error codes are mapped to `io::Error` with the same `ErrorKind` as the equivalent blocking syscall. Callers (WAL, engine) cannot distinguish backend from the error.

Specific cases:
- **EIO from fdatasync.** Bubbles up. WAL fsync worker treats this as "disk failed" per [`design-wal.md`](design/design-wal.md) §8.1.
- **ENOSPC.** Treated as "disk full"; WAL acceptor stops fsync; same recovery path as EIO.
- **Cancellation.** If the future is dropped before completion on the io_uring backend, `tokio-uring` issues an `IORING_OP_ASYNC_CANCEL`. The buffer is held until the cancel completes (no premature drop).

---

## 10. Testing Strategy

| Layer | Backend | Where |
|---|---|---|
| Unit tests for the I/O layer | Real `tokio-uring` on Linux CI; fallback elsewhere | `cargo test` |
| Unit tests for WAL, engine | A third **simulated** backend (`SimDisk`) that holds an in-memory `BTreeMap<u64, Vec<u8>>` and exposes the same `AsyncFile` API | `test_harness` (per [`test.md`](test.md) §8.2) |
| Integration tests | Real backend on a `tempfile`-managed directory | `cargo test --test wal_integration` |

The simulated backend supports failure injection: `SimDisk::set_full()`, `SimDisk::inject_io_error()`, `SimDisk::corrupt_at_offset()` — these are the same hooks listed in [`test.md`](test.md) §3.

**Determinism.** The simulated backend completes immediately (no `await` yield) so test scheduling stays deterministic under `start_paused = true`. The real io_uring backend is non-deterministic and is only used in non-unit tests.

---

## 11. Open Questions

- **Fixed buffer registration**: defer to V2 unless WAL p99 fsync latency benchmark shows submission-side overhead.
- **Direct I/O (O_DIRECT)**: not pursued; the kernel page cache is acceptable for WAL given we always `fdatasync` before ack.
- **Submission batching (SQE link chains)**: io_uring supports linking multiple SQEs (e.g. `write` then `fsync`) into one submission. Could shave a hop on the WAL hot path. Defer to V2; measure first.
- **Per-disk topology B**: revisit once we have multi-disk benchmark numbers from [`plan.md`](plan.md) §1 P2 M3.
