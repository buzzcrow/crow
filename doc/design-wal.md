# CrowKV - Design: Write-Ahead Log

Depends on: [`requirement.md`](requirement.md), [`design.md`](design.md)
Satisfies: [requirement.md §8.1](requirement.md#81-wal-write-ahead-log), [requirement.md §8.2](requirement.md#82-acceptor)

This document specifies the Acceptor's write-ahead log. The WAL is the only persistent log in CrowKV; everything else (learner btrees, snapshots, dedup caches) is a derived projection.

## Table of Contents

- [1. Goals and Non-Goals](#1-goals-and-non-goals)
- [2. Logical Record Shape](#2-logical-record-shape)
- [3. Multi-Disk Segment Layout](#3-multi-disk-segment-layout)
- [4. Write Path and Batched Fsync](#4-write-path-and-batched-fsync)
- [5. Ack Contract and Failure Modes](#5-ack-contract-and-failure-modes)
- [6. Replay on Startup](#6-replay-on-startup)
- [7. Garbage Collection](#7-garbage-collection)
- [8. Disk Loss and Recovery](#8-disk-loss-and-recovery)
- [9. Tunables and Defaults](#9-tunables-and-defaults)

---

## 1. Goals and Non-Goals

**Goals:**

- Bound write latency by the slower of (one fsync) and (one quorum RTT).
- Saturate aggregate disk bandwidth by parallelizing across multiple physical disks.
- Survive arbitrary process crash with bounded recovery time.
- Survive single-disk loss without data loss (peer recovery via snapshot install).
- Be replayable into a deterministic state.

**Non-goals:**

- Cross-group log ordering. Each group has its own WAL; there is no cluster-wide log.
- Compression / deduplication at the WAL layer. (Possible future work.)
- Multi-version time-travel reads. The WAL is not queried by the read path.
- Erasure coding of records. Replication is at the consensus layer, not the disk layer.

---

## 2. Logical Record Shape

A WAL record persists one of the messages an acceptor must remember durably. Conceptually:

| Field | Purpose |
| --- | --- |
| `magic`, `version` | Format identifier, for upgrade compatibility |
| `record_type` | One of `{ Promised, Accepted, ConfigChange, DedupCheckpoint, SnapshotMarker }` |
| `group_id` | Which group this record belongs to |
| `term` | Acceptor's `current_term` at the time of record (for fence verification on replay) |
| `slot` | Slot number this record concerns (n/a for `DedupCheckpoint`) |
| `ballot` | `(round, leader_id)` for `Promised` / `Accepted` |
| `payload` | Type-specific body — for `Accepted`, the `PxLogEntry` (kind + batch); for `Promised`, empty; for `ConfigChange`, the new membership; etc. |
| `payload_len` | Length, for forward-skipping malformed records |
| `crc32c` | CRC of header + payload |

Records are self-describing and length-prefixed so a corruption in one record does not destroy the rest of the segment unless followed by a CRC failure (see §6).

**Design note:** the `Accepted` record's payload IS the `PxLogEntry` — there is no separate "raft log" or "state machine log" structure. This keeps disk I/O minimal: one fsync per accept, no further logging.

`Promised` records exist for safety: classical Paxos requires acceptors to remember promises across crashes. They are short (no payload).

---

## 3. Multi-Disk Segment Layout

### 3.1 Why multiple disks

A single disk's fsync latency caps single-group write throughput at `1 / fsync_latency` IOPS, which on commodity NVMe is ~10–30k. Multiple disks give us linear scaling up to the network/CPU limit.

### 3.2 Layout

Each acceptor configures `wal_disks = [path_1, path_2, ...]`. Each disk holds a sequence of **segments**. A segment is a single file containing a contiguous range of records.

```
   /wal_disk_1/groupN/seg-0000001.log   slots [1..120]
   /wal_disk_1/groupN/seg-0000004.log   slots [481..600]
   /wal_disk_2/groupN/seg-0000002.log   slots [121..240]
   /wal_disk_2/groupN/seg-0000005.log   slots [601..720]
   /wal_disk_3/groupN/seg-0000003.log   slots [241..480]
```

- Each segment is named with a monotonic `segment_id`.
- Segments are pre-allocated to a target size (`wal_segment_size`, default 64 MiB) and sealed when full.
- Each segment ends with a small footer carrying the slot range it covered, useful for fast scan during replay.

**Slot-to-segment mapping** is recorded in an in-memory **segment index** that points each slot to `(disk, segment_id, file_offset)`. The index is persisted in a small auxiliary file per group, but is rebuildable from segment headers if lost.

### 3.3 Slot-to-disk assignment

When the acceptor receives an `Accept` for slot N, it picks a target disk:

- **Round-robin** across `wal_disks` is the default.
- **Load-aware** mode (optional) picks the disk whose current pending-fsync queue is shortest.
- A new segment is opened on the chosen disk if no current segment is open.

Because apply order is determined by the slot index in memory, the *physical* order of records on disk does not affect correctness (Invariant I4). This is what allows multiple disks to fsync in parallel.

### 3.4 Segment rotation

A segment is rotated (sealed and a new one opened) when:

- It reaches `wal_segment_size`.
- Its slot range crosses a major boundary (configurable; helps with GC granularity).
- An admin command requests it.
- A group's leader changes (optional; helps with debugging).

Sealed segments are immutable.

---

## 4. Write Path and Batched Fsync

### 4.1 Goal of batching

Each individual `Accept` is small (tens of bytes overhead + payload). Per-record fsync is wasteful: an SSD's fsync amortizes over a batch nearly as well as over a single record. We batch up to a target (size or time) and then fsync once.

### 4.2 Batch coalescing

For each disk, the WAL maintains a **pending queue**:

```
   pending_queue[disk] = [ record_1, record_2, ..., record_k ]
```

Records arrive concurrently from many `Accept` handlers. They are appended to the in-memory segment buffer for that disk and pushed onto the pending queue. A single **fsync worker** per disk picks up the queue, issues `write()` for everything in the buffer, calls `fdatasync()`, and then **completes all pending records' futures together**.

### 4.3 Triggers for fsync

The fsync worker flushes when any of:

- Pending bytes ≥ `wal_fsync_batch_bytes` (default 64 KiB).
- Oldest pending record's age ≥ `wal_fsync_batch_interval` (default 1 ms).
- Watchdog timer (default 100 ms): protects against bugs where the size/time thresholds are never reached.
- Admin force-flush.

The 1 ms interval is the key knob for latency-vs-throughput. Lower → lower latency, lower batch efficiency. Higher → higher batch efficiency, higher tail latency.

### 4.4 Async write completion

Each `Accept` handler returns a future. The fsync worker resolves the future with `Ok` after fsync; on disk error it resolves with `Err` (which the acceptor escalates per §8). The acceptor only emits the network `Accepted` response after the future resolves — this is the **ack contract** ([§5](#5-ack-contract-and-failure-modes)).

The future-based interface decouples the acceptor's handler from disk timing and allows multiple in-flight `Accept`s to coalesce into one fsync naturally.

### 4.5 Why this maps onto Multi-Paxos parallelism

Different slots may be in flight on different disks at the same time. Slot N may be batching on disk 1 while slot N+1 batches on disk 2. Both fsyncs proceed in parallel. Aggregate fsync throughput is `sum over disks of (1 / fsync_latency)` — exactly the parallel benefit we want.

Crucially, the leader's **own** fsync is not on the critical path of remote acceptors' fsyncs. The leader fsyncs locally, then broadcasts; remote acceptors fsync in parallel. Two fsyncs total in serial: leader's, and the slowest of the quorum. With multiple disks each fsync can itself be parallelized internally, but the consensus-level parallelism is what dominates throughput.

---

## 5. Ack Contract and Failure Modes

### 5.1 The contract

> An `Accepted` response is sent to the leader only after the corresponding WAL record's fsync has completed.
> A client write is acked only after a quorum of acceptors have sent `Accepted`.

This is repeated from [requirement.md §8.1](requirement.md#81-wal-write-ahead-log) because everything else in this section is a consequence of it.

### 5.2 Failure cases

- **Crash before fsync.** The record may or may not be on disk. Replay reads what is on disk; the slot may end up with no record on this acceptor. That is fine — the leader either had a quorum from other acceptors (slot is chosen) or not (slot will be repaired). No client was acked, so no expectation is violated.
- **Crash after fsync, before sending `Accepted`.** Replay finds the record. After the acceptor rejoins the group, the leader observes the existing accept (via heartbeat / slot-status query) and includes this acceptor in the quorum.
- **Crash after sending `Accepted`, before the leader acked the client.** Same as above — the record is on disk; if a quorum was reached, the slot is chosen; the new leader's bulk Phase-1 will see this and re-confirm.
- **fsync failure (returns `Err`).** Treated as a disk fault. The acceptor stops accepting new records on that disk and marks itself failed for the affected group; see §8.

### 5.3 What we never do

- Ack a client write before the leader's own fsync.
- Ack a client write before quorum-fsync.
- Send `Accepted` based only on the in-memory state, expecting fsync to succeed later. (Some systems do this for low latency at the cost of correctness under crash; CrowKV does not.)

---

## 6. Replay on Startup

### 6.1 Procedure

1. Discover all segments under `/wal_disks/*/group_id/seg-*.log` and order them by `segment_id`.
2. For each segment, walk records in disk order:
   - Verify `magic`, `version`.
   - Verify `crc32c`.
   - On any failure (truncated record, bad CRC), **truncate the segment at this offset**, log a warning, and stop processing this segment. Records in later segments are still processed (CRC errors are local to one segment).
3. Apply each verified record to in-memory acceptor state, indexed by `(group_id, slot)`. Later records for the same `(group, slot)` overwrite earlier ones (the highest-ballot accept wins, per Paxos rules).
4. Reconstruct the per-group `current_term` from the maximum `term` seen across all `Promised` and `Accepted` records.
5. Reconstruct dedup cache from the latest `DedupCheckpoint` plus subsequent applied `Write` records (see [§8.6 of design.md](design.md#86-idempotency--dedup-cache)).
6. Hand off to the learner: replay slots in slot order into the storage engine.
7. Register with the group's current leader (or start an election if no leader is known).

### 6.2 Truncation safety

A truncated record is a record whose CRC fails or whose payload is shorter than `payload_len` (e.g. crash mid-write). The truncate-on-failure rule is safe because:

- WAL appends are append-only; older records are not modified.
- A truncated record is *the last* record of its segment (anything after it would not have been written without first writing this one fully — fsync is in batch order). Well, in the multi-disk case it is the last record on **that disk**; other disks are independent.
- After truncation, the affected slot may be missing on this acceptor. Either a quorum existed without it (slot was chosen) or not (slot will be repaired). Either way, correctness is preserved (Paxos safety).

If the truncation crosses a sealed-segment boundary (i.e. the corruption is in an old segment), a sanity check fires and replay aborts; this indicates either a bug or undetected disk damage. The node fails itself out of the group and rebuilds from peers via snapshot install.

### 6.3 Replay performance

Replay is sequential disk reads, parallelized across disks. For a 64 GiB total WAL across 4 disks at 1 GiB/s each, replay is ~16 s. In practice WAL sizes stay small because of GC (§7), so replays are sub-second.

The dominant cost is engine apply, not WAL read. The engine has its own bulk-load path for replay (§8.7 of design.md → `design-storage-engine.md`).

---

## 7. Garbage Collection

WAL GC is the mechanism that keeps disk usage bounded. It interacts with snapshots and the safe-slot.

### 7.1 GC watermark

The watermark for a group's WAL GC is:

```
   gc_slot = min(safe_slot, snapshot_slot)
```

Records with `slot < gc_slot` are eligible for GC. The two-watermark rule is justified in [`design-parallel-slots.md`](design-parallel-slots.md) §11.

### 7.2 GC granularity

GC runs at **segment granularity**, not record granularity. A whole segment is unlinked from disk when *every* record in it has `slot < gc_slot`. This is cheap (one `unlink()` per segment) and avoids any in-place rewriting.

### 7.3 GC trigger

GC is triggered by:

- Periodic tick (default 30 s) — the GC worker checks each disk and unlinks eligible segments.
- Disk-pressure signal — if any WAL disk's usage exceeds `wal_disk_high_watermark` (default 80%), GC runs immediately and may also force a snapshot to advance `snapshot_slot`.

### 7.4 Interaction with leader change

After leader change, the new leader may temporarily not know other acceptors' `contiguous_applied`, so `safe_slot` is conservatively low. GC pauses until safe-slot has stabilized. This is fine: GC is asynchronous to correctness.

### 7.5 Force-retain window

A configurable `wal_min_retention` (default 1 hour, optional) keeps even GC-eligible segments around for a grace period to support post-incident debugging. This is a forensics aid, not a correctness mechanism.

---

## 8. Disk Loss and Recovery

### 8.1 Single-disk failure

When `fdatasync` returns an error, or the OS reports the disk read-only, or repeated I/O errors exceed a threshold, the acceptor declares the disk failed:

1. Stop using the disk for new writes.
2. Mark the group affected (a multi-disk WAL with one disk lost has incomplete state for slots that landed on the lost disk).
3. **Fail the node out of the group:** the node sends a step-out RPC to the leader, the leader records the failed acceptor as not-eligible, and (if necessary) triggers a reconfiguration to maintain quorum.
4. Rebuild from peers via **snapshot install** ([§8.4 of design.md](design.md#84-snapshot-and-install)).

This is the **fail-out semantics** decided in [requirement.md §8.1](requirement.md#81-wal-write-ahead-log). We do not try to keep operating with partial WAL state on the surviving disks; that path requires per-slot replication across disks and adds disproportionate complexity.

### 8.2 All-disk failure

The node is non-functional; it cannot serve any group. It must be replaced by an operator. After replacement (with empty disks) the new node bootstraps via Group-0 and snapshot installs all groups it should host.

### 8.3 Detecting silent corruption

CRC32C catches all common bit-flips and torn writes. Logical corruption (e.g. a slot record with valid CRC but inconsistent term/ballot relative to other state) is detected by replay sanity checks; if found, the node fails out of the affected group.

### 8.4 Network-level data loss is not a WAL concern

The WAL is per-node. Inter-node consistency is the consensus layer's job. If a network drops a record, the leader retransmits; if it drops repeatedly, gap repair handles it.

---

## 9. Tunables and Defaults

| Parameter | Default | Range | Notes |
| --- | --- | --- | --- |
| `wal_disks` | required | ≥ 1 | More → higher throughput |
| `wal_segment_size` | 64 MiB | 1 MiB – 4 GiB | Trade GC granularity vs file count |
| `wal_fsync_batch_bytes` | 64 KiB | 0 – 16 MiB | Lower → lower latency |
| `wal_fsync_batch_interval` | 1 ms | 0 – 1 s | Latency knob |
| `wal_fsync_watchdog` | 100 ms | ≥ batch_interval | Catches batch-stuck bugs |
| `wal_disk_high_watermark` | 80% | 50% – 95% | Triggers eager GC + snapshot |
| `wal_min_retention` | 1 h | 0 – 30 d | Forensics retention |
| `gc_tick` | 30 s | 1 s – 10 min | GC scan cadence |

**Choosing fsync batching:**

- Latency-critical, low write rate → `batch_interval = 0` (per-write fsync).
- Throughput-oriented → `batch_interval = 1–5 ms`, `batch_bytes = 64–512 KiB`.
- WAN replication piggybacking → match `batch_interval` to RTT.

**Choosing segment size:**

- Few large segments → fewer files, faster replay scan.
- Many small segments → finer-grained GC, more files.
- 64 MiB is the engineering compromise.

**Choosing disk count:**

- 1 disk: single fsync limit. Fine for development.
- 2–4 disks: typical production for a write-heavy workload.
- More than 4: usually network or CPU becomes the bottleneck before more disks help.
