# CrowKV - Plan: WAL Implementation

Depends on: [`plan.md`](plan/plan.md), [`design-wal.md`](design-wal.md), [`plan-consensus.md`](plan/plan-consensus.md)
Satisfies: [requirement.md §8.1](requirement.md#81-wal-write-ahead-log), [requirement.md §8.2](requirement.md#82-acceptor)

Phase 2 implementation: multi-disk write-ahead log for acceptor persistence.

## 1. Milestones

### M0 — Async I/O facade

- Implement `io::AsyncFile` per [`design-async-io.md`](design-async-io.md) §5.
- io_uring backend via `tokio-uring` (gated by capability probe, §7 of that doc).
- Fallback backend via `tokio::fs` + `spawn_blocking`.
- Simulated `SimDisk` backend for unit tests.
- Capability probe + backend-selection logging at startup.

**Acceptance:** all three backends pass the same `AsyncFile` unit test matrix; backend selection logged once at INFO.

### M1 — Segment layout + record format

- `Segment` file format: header, length-prefixed records, footer with slot range
- `WALRecord`: magic, version, `record_type`, `group_id`, `term`, `slot`, `ballot`, payload, CRC32C
- `SegmentIndex`: in-memory `(slot → disk, segment_id, offset)` map, rebuildable from headers

**Acceptance:** write a segment, close, reopen, read back all records with valid CRC.

### M2 — Batched fsync per disk

- `FsyncWorker` per disk: a long-running async task driving an `mpsc` queue of pending records; batch by bytes/time/watchdog (defaults from [`design-wal.md`](design-wal.md) §9).
- All disk I/O goes through the project async I/O facade (`AsyncFile` in [`design-async-io.md`](design-async-io.md)). The worker calls `file.write_at(buf, off).await` then `file.fdatasync().await`; on Linux ≥ 5.11 these map to io_uring SQEs, otherwise to `spawn_blocking`. No call site changes needed across backends.
- Async completion future per `Accept` (returned by `WalManager::append(record).await`); `Accepted` response gated on future `Ok`.
- Single-disk throughput benchmark using `criterion`.

**Acceptance:** batch of 100 records fsynced in ≤ 3 individual fsync calls; documented latency at p50/p99.

### M3 — Multi-disk round-robin + slot assignment

- `WalManager`: distributes slots across configured disks
- Per-disk segment rotation when size threshold reached
- Aggregate throughput benchmark (2–4 disks)

**Acceptance:** aggregate fsync IOPS scales linearly with disk count up to 4.

### M4 — Replay on startup

- Discover segments, order by `segment_id`, walk records
- CRC failure → truncate at failure point, log warning, continue later segments (per `design-wal.md` §6.2)
- Rebuild acceptor in-memory state (`promised`, `accepted`) from records; highest-ballot accept wins per slot
- Rebuild `current_term` from max seen term
- Rebuild dedup cache from latest `DedupCheckpoint` + subsequent `Write` records

**Acceptance:** write 1000 records, simulate crash (drop last un-fsynced batch), restart, state deterministic; replay matches expected `(slot, value)` map.

### M5 — Garbage collection

- `gc_slot = min(safe_slot, snapshot_slot)`
- Unlink whole segments when all records have `slot < gc_slot`
- Disk-pressure eager GC trigger

**Acceptance:** GC removes segments below watermark; replay after GC skips them correctly.

## 2. Module Breakdown

Modules in `crowkv`: **`io`** (P2 M0, async I/O facade) and **`wal`** (P2 M1+).

| Module path (in `crowkv`) | Responsibility |
|---|---|
| `io` (whole module) | Project async I/O facade ([`design-async-io.md`](design-async-io.md)). **Built first as P2 M0** so WAL M1+ and the engine module can both use it. |
| `wal::record` | `WALRecord` shape, CRC32C (P2 M1) |
| `wal::segment` | File format, record encoding/decoding (P2 M1) |
| `wal::index` | In-memory slot-to-segment index (P2 M1) |
| `wal::fsync_worker` | Per-disk batch fsync, async completion (P2 M2) |
| `wal::manager` | Multi-disk routing, segment rotation, GC (P2 M3, M5) |
| `wal::replay` | Startup segment discovery, validation, truncation (P2 M4) |

`crowkv::wal` depends on `crowkv::io` and `crowkv::consensus` (for `PxLogEntry` shape).

## 3. Freeze Checklist

Before P4 (RPC) starts (P3 may proceed in parallel; storage engine is independent of WAL):
- [ ] `WALRecord` format frozen and versioned (header carries `magic`, `version`)
- [ ] Ack contract enforced: `Accepted` only after fsync completion
- [ ] Replay produces deterministic acceptor state for any prefix-fsynced record stream
- [ ] G2 milestone passes: simulated crash, restart, re-elect, no data loss

## 4. Out-of-Scope for P2

Deferred to later phases or future work:
- Snapshot install (P5) — only WAL is in scope here
- Multi-disk failure recovery beyond fail-out (`design-wal.md` §8.1) — implemented but not stress-tested in P2
- Compression / encryption of WAL records (future)
