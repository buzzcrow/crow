# CrowKV - Design: Write-Ahead Log

Depends on: [`requirement.md`](requirement.md), [`design.md`](design.md)
Satisfies: [requirement.md §8.1](requirement.md#81-wal-write-ahead-log), [requirement.md §8.2](requirement.md#82-acceptor)

This document specifies CrowKV's write-ahead log. **There is exactly one durable
log per group: the replica's consensus log (the per-slot acceptor log).**
Everything else — the KV state machine (`KVEngine`), future snapshots, the dedup
cache, the chosen/applied watermarks — is a *derived projection* of that log.
The `Accepted` record carries the actual operation, so this single log doubles
as the replicated command log (Raft-style) and the state-machine input; there is
no separate "state-machine WAL" and no second write of the value.

Durability rule of thumb: **if an acknowledged write is in the WAL on a quorum,
it cannot be lost** — a value is *chosen* exactly when a quorum has durably
`Accepted` it, and replay + recovery reconstruct the committed state from those
durable accepts.

## Table of Contents

- [1. Goals and Non-Goals](#1-goals-and-non-goals)
- [2. Logical Record Shape](#2-logical-record-shape)
- [3. Multi-Disk Segment Layout](#3-multi-disk-segment-layout)
- [4. Write Path and Batched Durable Flush](#4-write-path-and-batched-durable-flush)
- [5. Ack Contract and Failure Modes](#5-ack-contract-and-failure-modes)
- [6. Replay, Restore, and Recovery](#6-replay-restore-and-recovery)
- [7. Garbage Collection](#7-garbage-collection)
- [8. Disk Loss and Recovery](#8-disk-loss-and-recovery)
- [9. Tunables and Defaults](#9-tunables-and-defaults)
- [10. Follow-up Implementation Scope](#10-follow-up-implementation-scope)

---

## 1. Goals and Non-Goals

**Goals:**

- Bound write latency by the slower of (one durable flush) and (one quorum RTT).
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
| `record_type` | One of `{ Promised, Accepted, VoteGranted, ConfigChange, DedupCheckpoint, SnapshotMarker }` (`ConfigChange` / `SnapshotMarker` reserved for P3/P4) |
| `group_id` | Which group this record belongs to |
| `term` | Acceptor's `current_term` at the time of record (for fence verification on replay) |
| `slot` | Slot number this record concerns (n/a for `DedupCheckpoint`) |
| `ballot` | `(round, leader_id)` for `Promised` / `Accepted` |
| `payload` | Type-specific body — for `Accepted`, the `PxLogEntry` (kind + batch); for `Promised`, empty; for `ConfigChange`, the new membership; etc. |
| `payload_len` | Length, for forward-skipping malformed records |
| `crc32c` | CRC of header + payload |

Records are self-describing and length-prefixed so a corruption in one record does not destroy the rest of the segment unless followed by a CRC failure (see §6).

**Design note:** the `Accepted` record's payload IS the `PxLogEntry` — there is no separate "raft log" or "state machine log" structure. This keeps disk I/O minimal: one durable flush per accepted batch, no further logging.

`Promised` records exist for safety: classical Paxos requires acceptors to remember promises across crashes. They are short (no payload).

### 2.4 When each record is written

The WAL is written **only on the acceptor/election critical path**, before the
handler replies (the ack contract, §5). The mapping is exhaustive:

| Handler (`PxLocalReplica`) | Trigger | Record | Payload |
| --- | --- | --- | --- |
| `on_prepare` | acceptor grants a promise | `Promised` | none (slot + ballot + term in header) |
| `on_accept` | acceptor accepts a value | `Accepted` | the `PxLogEntry` (kind + KV batch + `client_id`/`seq`) |
| `handle_request_vote` | acceptor grants a vote | `VoteGranted` | `voted_for` node id (term in header) |
| dedup checkpoint task | periodic | `DedupCheckpoint` | `(client_id, last_seq, last_slot)*` |

**`learn()` writes nothing.** Applying a chosen value to the `KVEngine` is a pure
in-memory projection; its durability comes entirely from the `Accepted` records
that a quorum already persisted. This is why "chosen" is not, by itself, a
durable *local* fact — see §6.2 / §6.4. (A durable commit watermark is planned
in P2 to make local recovery cheaper, not to add a second copy of the value.)

---

## 3. Multi-Disk Segment Layout

### 3.1 Why multiple disks

A single disk's fsync latency caps single-group write throughput at `1 / fsync_latency` IOPS, which on commodity NVMe is ~10–30k. Multiple disks give us linear scaling up to the network/CPU limit.

### 3.2 Layout

Each acceptor configures `wal_disks = [path_1, path_2, ...]`. Each disk holds a sequence of **segments**. A segment is a single append-only byte stream containing records in append order for that disk. It is **not** required to contain a contiguous slot interval.

With multiple disks, global slot order and physical record order intentionally diverge:

```
   /wal_disk_1/groupN/seg-0000001.log   slots {1,4,7,...}
   /wal_disk_1/groupN/seg-0000004.log   slots {361,364,367,...}
   /wal_disk_2/groupN/seg-0000002.log   slots {2,5,8,...}
   /wal_disk_2/groupN/seg-0000005.log   slots {362,365,368,...}
   /wal_disk_3/groupN/seg-0000003.log   slots {3,6,9,...}
```

- Each segment is named with a monotonic `segment_id` within the group.
- Segments are pre-allocated to a target size (`wal_segment_size`, default 64 MiB) and sealed when full.
- Each sealed segment records `(min_slot, max_slot, record_count)` as scan metadata; `min_slot..max_slot` may contain holes because slot ownership is distributed across disks and because non-slot records exist.
- Replay correctness comes from sorting / indexing records by `(group_id, slot, ballot, record_type)` after reading them, not from physical segment order.

**Slot-to-segment mapping** is recorded in an in-memory **segment index** that points each slot to one or more `(disk, segment_id, file_offset)` records. A persisted index may be used as a startup accelerator, but it is only a cache: replay must be able to rebuild the authoritative index by scanning segment records.

### 3.3 Slot-to-disk assignment

When the acceptor receives a slot-addressed record (`Promised` or `Accepted`) for slot N, it chooses the target disk by deterministic slot affinity:

```
   disk = hash(group_id, slot) % wal_disks.len()
```

This is the default and correctness-oriented mapping. All records for the same `(group_id, slot)` land on the same disk, so replay can recover that slot without merging same-slot histories from multiple disks. The mapping is stable across restart as long as `wal_disks` order is stable.

Each disk owns an independent append queue, active segment, flush worker, and durable-flush state. Concurrent slot writes still parallelize because adjacent slots hash across disks, but same-slot retries and higher-ballot re-accepts preserve disk locality.

Non-slot records use fixed lanes:

- `VoteGranted` and other election metadata go to the group metadata lane (`disk = hash(group_id, 0) % wal_disks.len()`).
- `DedupCheckpoint` goes to the same metadata lane.
- Future `ConfigChange` records must be persisted in the metadata lane and included in snapshot install / peer rebuild semantics.

A future load-aware mode is only legal if it preserves slot affinity, for example by moving whole slot shards through an explicit re-sharding protocol. Per-record load-aware placement is not allowed for slot-addressed records.

Because apply order is determined by the slot index in memory, the *physical* order of records on disk does not affect correctness (Invariant I4). This is what allows multiple disks to flush in parallel.

### 3.4 Segment rotation

A segment is rotated (sealed and a new one opened) when:

- It reaches `wal_segment_size`.
- Its slot range crosses a major boundary (configurable; helps with GC granularity).
- An admin command requests it.
- A group's leader changes (optional; helps with debugging).

Sealed segments are immutable.

---

## 4. Write Path and Batched Durable Flush

### 4.1 Goal of batching

Each individual `Accept` is small (tens of bytes overhead + payload). Per-record durable flush is wasteful: an SSD's flush amortizes over a batch nearly as well as over a single record. CrowKV therefore batches records per WAL disk and completes the record futures only after the selected backend has made the bytes durable.

The durable operation is backend-specific:

- **Filesystem file backend:** append with normal file writes, then `fdatasync` / equivalent durable flush.
- **Block-device backend:** issue aligned direct writes. If the device requires 4 KiB sectors, the WAL writer pads or read-modify-writes through the block backend's alignment planner; no filesystem `fdatasync` is involved.
- **RAM / SCM / simulated backends:** use the backend's own durability contract. A volatile RAM backend is for tests only and may complete immediately; persistent-memory backends may require cache-line flush / fence semantics instead of filesystem sync.

The acceptor code talks to `WalEngine::append_*` and does not branch on these backend details.

### 4.2 Batch coalescing

For each disk, the WAL maintains a **pending queue**:

```
   pending_queue[disk] = [ record_1, record_2, ..., record_k ]
```

Records arrive concurrently from many `Accept` handlers. They are appended to the in-memory segment buffer for that disk and pushed onto the disk's pending queue. A single **flush worker** per disk owns the active segment, drains queued records into one backend write batch, calls the backend-specific durable flush, and then **completes all pending records' futures together**.

The worker is event-driven, not timer-first:

1. If the queue is empty, the worker awaits a notification from the next enqueue.
2. When notified, it drains all records currently available without waiting for a fixed 1 ms interval.
3. If more records arrive while the write is being assembled, they are included until the batch reaches `wal_flush_batch_bytes` or the drain loop observes no immediately-ready records.
4. The worker issues one backend write / durable-flush operation for the drained batch.
5. On success, it resolves every drained record future; on error, it fails them and marks the affected disk / group unhealthy.

This gives low latency for a single record while still aggregating bursts naturally.

### 4.3 Triggers for durable flush

The flush worker flushes when any of:

- It is woken by a transition from empty queue to non-empty queue.
- Pending bytes ≥ `wal_flush_batch_bytes` (default 64 KiB).
- Optional coalescing budget expires (`wal_flush_coalesce_us`, default 0 µs). This is a microsecond-scale burst-aggregation hint, not a mandatory millisecond delay.
- Watchdog timer (default 100 ms): protects against bugs where notifications or thresholds are missed.
- Admin force-flush.

Default behavior is therefore **wake-drain-flush**: a lone record does not wait for 1 ms, while a concurrent burst gets batched because the worker drains all immediately-ready records before issuing I/O. Higher `wal_flush_coalesce_us` values may be enabled for throughput benchmarks, but they are not required for correctness.

### 4.4 Async write completion

Each `Accept` handler returns a future. The flush worker resolves the future with `Ok` after the backend durable flush completes; on disk error it resolves with `Err` (which the acceptor escalates per §8). The acceptor only emits the network `Accepted` response after the future resolves — this is the **ack contract** ([§5](#5-ack-contract-and-failure-modes)).

The future-based interface decouples the acceptor's handler from disk timing and allows multiple in-flight `Accept`s to coalesce into one durable flush naturally.

### 4.5 Why this maps onto Multi-Paxos parallelism

Different slots may be in flight on different disks at the same time. Slot N may be batching on disk 1 while slot N+1 batches on disk 2. Both durable flushes proceed in parallel. Aggregate flush throughput is `sum over disks of (1 / flush_latency)` — exactly the parallel benefit we want.

Crucially, the leader's **own** durable flush is not on the critical path of remote acceptors' durable flushes. The leader persists locally, then broadcasts; remote acceptors flush in parallel. Two durable flushes total in serial: leader's, and the slowest of the quorum. With multiple disks each durable flush can itself be parallelized internally, but the consensus-level parallelism is what dominates throughput.

---

## 5. Ack Contract and Failure Modes

### 5.1 The contract

> An `Accepted` response is sent to the leader only after the corresponding WAL record's backend durable flush has completed.
> A client write is acked only after a quorum of acceptors have sent `Accepted`.

This is repeated from [requirement.md §8.1](requirement.md#81-wal-write-ahead-log) because everything else in this section is a consequence of it.

### 5.2 Failure cases

- **Crash before durable flush.** The record may or may not be on disk. Replay reads what is on disk; the slot may end up with no record on this acceptor. That is fine — the leader either had a quorum from other acceptors (slot is chosen) or not (slot will be repaired). No client was acked, so no expectation is violated.

  On restart, the new leader computes its recovery ceiling from durable state, not from an assumed contiguous log tail: `ceiling = max(local highest_seen_slot, peers' highest_seen_slot, persisted next_slot - 1 if present)`. Because parallel writes may leave holes inside `[floor+1, ceiling]`, the leader must run bulk Phase 1 over the whole open interval and fill empty slots with `NoOp`. It does **not** rely on `latest_slot + window_size` to discover hidden data; any value absent from every acceptor's durable state could not have formed a durable quorum and therefore could not have been acknowledged.

- **Crash after durable flush, before sending `Accepted`.** Replay finds the record. After the acceptor rejoins the group, the leader observes the existing accept (via heartbeat / slot-status query) and includes this acceptor in the quorum.
- **Crash after sending `Accepted`, before the leader acked the client.** Same as above — the record is on disk; if a quorum was reached, the slot is chosen; the new leader's bulk Phase-1 will see this and re-confirm.
- **Durable-flush failure (returns `Err`).** Treated as a disk fault. The acceptor stops accepting new records on that disk and marks itself failed for the affected group; see §8.

### 5.3 What we never do

- Ack a client write before the leader's own durable flush.
- Ack a client write before quorum durable flush.
- Send `Accepted` based only on the in-memory state, expecting durable flush to succeed later. (Some systems do this for low latency at the cost of correctness under crash; CrowKV does not.)

---

## 6. Replay, Restore, and Recovery

Turning the on-disk WAL back into a live, serving replica has three stages, then
a steady state:

- **6.1 Replay** — rebuild in-memory *acceptor* state from the records.
- **6.2 Restore** — rebuild a live replica shell from acceptor state only; do not apply accepted values to the `KVEngine` unless they are covered by a durable commit watermark / snapshot.
- **6.3 Wiring** — attach a fresh WAL engine and seed the proposer conservatively.
- **6.4 Recovery** — a new leader re-confirms *chosen* values from a quorum and fills holes.
- **6.5 Steady-state apply** — followers keep their state machine current from the leader's commit watermark.

The split matters: replay/restore are purely *local* (this node's WAL), but `Accepted` ≠ `chosen` (a value is chosen only on a quorum). Therefore replay may restore promises and accepted values into the acceptor, but learner / `KVEngine` application is delayed until the value is known chosen by one of:

- A future durable commit watermark proving a contiguous chosen prefix.
- A snapshot marker proving the engine image already includes the slot.
- New-leader recovery / steady-state heartbeat catch-up re-learning the value from quorum-confirmed consensus.

### 6.1 Replay — rebuild acceptor state (`replay_group`)

1. Discover all segments under `<wal_disk>/group<gid>/seg-*.log`, order by `segment_id`.
2. Walk each segment's records in order; verify `magic`, `version`, `crc32c`.
   On the first failure, **truncate that segment at the offset and stop** (a torn
   tail from a crash mid-write); later segments are still processed.
3. Rebuild per-`(group, slot)` acceptor state keeping the **highest-ballot**
   `Promised` / `Accepted` per slot (later/higher-ballot records win — Paxos rule).
4. `current_term` = max `term` across all records.
5. `voted_for` = the node from the latest `VoteGranted` whose `term == current_term`. This is election safety state, not just debug metadata: after crash, the node must not grant a second vote in the same term.
6. Dedup cache = the latest `DedupCheckpoint` plus the `(client_id, seq)` of every `Accepted` after it.

**Dedup meaning:** client writes carry `(client_id, seq)` so a retried request can be recognized after timeout or leader change. The dedup cache stores the highest sequence and result slot already accepted for each client. It is an exactly-once / idempotency aid for client-visible behavior; it is not part of Paxos safety, but losing it can cause duplicate client operations after retry.

Output: `ReplayResult { records, max_segment_id, current_term, voted_for, dedup_cache }`.

### 6.2 Restore — rebuild live acceptor state (`restore_from_replay`)

A fresh `PxLocalReplica` is rebuilt without treating local accepts as committed state:

- Feed `Promised` through the acceptor restore path so the highest promise per slot is preserved.
- Feed `Accepted` through the acceptor restore path so the highest accepted value per slot is available to future Phase 1.
- Seed `current_term`, `voted_for`, role, and dedup cache from `ReplayResult`.
- Do **not** call `learn()` for arbitrary accepted slots during restore.

The learner / `KVEngine` is restored only from durable commit evidence:

- For P2, a durable commit watermark may allow applying the known-chosen contiguous prefix.
- For P3, a snapshot plus `SnapshotMarker` may restore an engine image and applied index directly.
- For slots above that proof, the engine waits for §6.4 / §6.5 to re-learn chosen values.

This avoids resurrecting a value that was accepted locally but never chosen, and it avoids over-claiming `contiguous_chosen` from a single replica's WAL.

### 6.3 Wiring a live group (`create_group_with_wal`)

`replay_group` → create `WalEngine` and resume `next_segment_id = max_segment_id + 1` → `restore_from_replay` → `set_wal` → wrap in `PxGroup`.

The group also records a conservative local slot tip:

```
   local_tip = max(highest_seen_slot, durable_commit_watermark, snapshot_slot)
```

A restarted node must not serve as leader until election and §6.4 recovery establish the quorum-derived ceiling. After recovery is issued, the leader seeds new client proposals at `next_slot = ceiling + 1`.

### 6.4 New-leader recovery (bulk Phase 1)

On winning election a leader sweeps slots `(floor, ceiling]`:

- `floor = local contiguous_chosen`; `ceiling = max(local highest_seen,
  next_slot − 1, peers' highest_seen)` (peers' values arrive in the vote replies).
- For each slot it runs Phase-1 `Prepare` across a quorum and **adopts the
  highest-ballot accepted value** observed (an empty slot becomes a `NoOp`), then
  re-`Accept`s under its term and `learn()`s it.
- Quorum intersection guarantees any chosen value is seen, so a committed value
  is **re-confirmed, not overwritten**. This repairs a leader that restored an
  incomplete or merely-accepted prefix.

(Steady-state gap repair below the frontier uses the same adopt-from-quorum logic, one slot at a time — see [`design-parallel-slots.md`](design-parallel-slots.md).)

Parallel slot assignment means holes are normal after crash / restart. Recovery resolves every slot in `(floor, ceiling]` independently:

- If Phase 1 observes an accepted value from any quorum member, the leader adopts the highest-ballot value and re-accepts it.
- If Phase 1 observes no accepted value for that slot, the leader writes a `NoOp` for the slot.
- `NoOp` has no KV payload, but it is a real chosen slot and advances contiguous watermarks; future replay sees it and does not need to repair the hole again once it is covered by the durable commit watermark.

Slot indexes are generated only by the active leader's proposer:

1. A leader tenure is fenced by election `term`; stale leaders are rejected by term / ballot checks.
2. During recovery, the leader computes `ceiling` from local and quorum-reported highest seen slots.
3. It resolves all holes up to `ceiling` with adopted values or `NoOp`.
4. It initializes `next_slot = ceiling + 1`.
5. Each admitted client proposal atomically fetches and increments `next_slot` exactly once.

This gives unique slot numbers within a leader tenure and a continuous chosen prefix after recovery, even though physical WAL placement is non-contiguous and multiple slots are processed in parallel.

### 6.5 Steady-state apply

- **Leader:** applies on commit (`propose` → quorum `Accept` → `learn`).
- **Follower:** does **not** apply on `Accept` (which only persists) nor on the
  payload-less `ChosenNotice` (which carries no value). Instead each heartbeat
  carries the leader's `committed_safe_slot`, and the follower applies its
  *contiguous accepted prefix* up to it (`apply_committed_up_to`). A follower
  missing a committed `Accept` is re-sent it by the leader's heartbeat catch-up,
  so its prefix converges over successive heartbeats.
- **Reads:** linearizable reads run on the leader behind a lease / ReadIndex
  barrier (non-leaders redirect); bounded-stale / best-effort reads serve local
  applied state.

### 6.6 Truncation safety

A truncated record is one whose CRC fails or whose payload is shorter than
`payload_len` (crash mid-write). Truncate-on-failure is safe because appends are
append-only and the bad record is the last durable one on that disk; the missing
slot was either chosen on a quorum without this node (recovered by §6.4) or never
chosen (no client was acked). A CRC failure inside a **sealed** segment is fatal:
the node fails out of the group and rebuilds from peers via snapshot install.

### 6.7 Replay performance

Replay is sequential disk reads, parallelized across disks; WAL sizes stay small
because of GC (§7), so replays are typically sub-second. The dominant cost is
engine apply, not WAL read.

---

## 7. Garbage Collection

WAL GC is the mechanism that keeps disk usage bounded. It interacts with snapshots and the safe-slot.

### 7.1 GC watermark

The watermark for a group's WAL GC is:

```
   gc_slot = min(safe_slot, snapshot_slot)
```

Records with `slot < gc_slot` are eligible for GC. The two-watermark rule is justified in [`design-parallel-slots.md`](design/design-parallel-slots.md) §11.

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

When the backend durable-flush operation returns an error, the OS reports the disk read-only, or repeated I/O errors exceed a threshold, the acceptor declares the disk failed:

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
| `wal_flush_batch_bytes` | 64 KiB | 0 – 16 MiB | Batch cap per durable flush |
| `wal_flush_coalesce_us` | 0 µs | 0 – 1000 µs | Optional micro coalescing; default wake-drain-flush |
| `wal_flush_watchdog` | 100 ms | ≥ coalesce budget | Catches batch-stuck bugs |
| `wal_disk_high_watermark` | 80% | 50% – 95% | Triggers eager GC + snapshot |
| `wal_min_retention` | 1 h | 0 – 30 d | Forensics retention |
| `gc_tick` | 30 s | 1 s – 10 min | GC scan cadence |

**Choosing durable-flush batching:**

- Latency-critical, low write rate → `wal_flush_coalesce_us = 0` (wake-drain-flush; no fixed delay).
- Throughput-oriented → `wal_flush_batch_bytes = 64–512 KiB`, optionally `wal_flush_coalesce_us = 50–250`.
- WAN replication piggybacking → tune coalescing only after measuring end-to-end RTT and tail latency.

**Choosing segment size:**

- Few large segments → fewer files, faster replay scan.
- Many small segments → finer-grained GC, more files.
- 64 MiB is the engineering compromise.

**Choosing disk count:**

- 1 disk: single durable-flush limit. Fine for development.
- 2–4 disks: typical production for a write-heavy workload.
- More than 4: usually network or CPU becomes the bottleneck before more disks help.

---

## 10. Follow-up Implementation Scope

The design deltas above have been implemented. Remaining ignored or pending tests are tracked in [`plan-ut.md`](../plan-ut.md):

- Slot-affinity WAL placement instead of per-record round-robin.
- Backend-specific durable flush and alignment semantics.
- Event-driven wake-drain-flush workers.
- Replay-only restore for acceptor state, with learner apply gated by durable commit evidence.
- New-leader recovery ceiling, `NoOp` hole fill, and post-recovery `next_slot` seeding.
- Dedup checkpoint documentation and tests.
