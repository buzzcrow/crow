<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Write Flow Analysis

End-to-end trace of the CrowKV write path, from client request to
response. Covers the single-proposal path, multi-slot concurrency
model, and tracked optimization opportunities.

---

## Write Flow — Single Proposal

```
Client PUT/DELETE/BatchWrite
  → PxKvStore::kv_put / kv_delete / kv_batch_write
    → encode payload (Vec<u8> → manual binary encode)
  → PxKvStore::propose_and_respond
    → PxGroup::propose(payload, client_id, seq)
      1. Leadership gate (role == Leader && current_term == proposing_term)
      2. Idempotency check (dedup_lookup by client_id + seq)
      3. Sliding-window admission (inflight_window.try_acquire)
      4. Slot allocation (next_slot.fetch_add)
      5. 'slot_retry loop (max_slot_retries = 3)
         a. base_entry(slot, payload.clone())
         b. 'paxos_attempt loop (max_paxos_retries = 3)
            i.  [if force_prepare] run_prepare_phase
                - local on_prepare (acceptor.prepare + WAL append Promised)
                - concurrent remote send_prepare RPCs (join_all, unary gRPC)
            ii. run_accept_phase
                - local on_accept (acceptor.accept + WAL append Accepted)
                - concurrent remote send_accept RPCs (join_all, bidi LearnerStream)
            iii. quorum check
            iv. [if chosen] learn_chosen (decode + KVEngine::apply)
            v.  [if chosen] fan_out_chosen_notice (fire-and-forget mpsc)
            vi. [if chosen] return ProposeResult::Chosen { slot }
      6. [if all retries exhausted] return ProposeResult::Err
  → KvResponse::ok_chosen(slot, ...) or error
```

### Key Data Structures

- **`PxLogEntry`** — `{ slot, ballot: {round, leader_id}, term, payload:
  Bytes }`. The unit of Paxos consensus. `payload` is `bytes::Bytes`
  for `O(1)` ref-count-bump clones.
- **`PxBallot`** — `{ round, leader_id }`. Monotonically increasing
  per slot; used for prepare/accept fencing.
- **`WALRecord`** — Encoded from `PxLogEntry` (or
  `{term, slot, ballot}` for Promised). Written via `WalEngine::append`
  which enqueues to a per-pipeline writer task and awaits `fdatasync`.
- **`AcceptRequest` / `AcceptedResponse`** — gRPC wire types for the
  accept phase. Sent over the per-peer bidi `PxLearnerStream`.
- **`PrepareRequest` / `PromiseResponse`** — gRPC wire types for the
  prepare phase. Sent as unary RPC (not over LearnerStream).

---

## Multi-Slot Concurrency Model

### Sliding-Window Admission

The group maintains a `tokio::sync::Semaphore` (`inflight_window`)
with `max_inflight_proposals` permits (default 16). Each `propose`
call acquires one permit before slot allocation and holds it for the
entire proposal duration (released on drop at every return path).

- **Full window** → `ProposeResult::Busy` (fast-fail, client retries).
- **No blocking** — `try_acquire` is used, never `acquire`.

This allows up to 16 proposals to be in-flight concurrently, each at
a different slot. Slots are allocated via `next_slot.fetch_add(1,
Ordering::Relaxed)` — a lock-free atomic counter.

### Per-Slot Independence

Each in-flight proposal operates on its own slot independently:

- Slot N may be in accept phase while slot N+1 is in prepare phase.
- Slots are chosen out of order — a higher slot may be chosen before
  a lower one (e.g. if the lower slot hits a retry).
- The learner tracks `contiguous_chosen` (highest contiguous chosen
  slot) and `last_chosen_slot` (highest chosen slot overall). Gaps
  between `contiguous_chosen` and `last_chosen_slot` are filled by
  background repair.

### Background Repair (`repair_once`)

Runs on the leader during steady-state. When
`contiguous_chosen < last_chosen_slot`, the leader runs classic Paxos
(prepare + accept) on `gap_slot = contiguous_chosen + 1` with an empty
payload (`NoOp` fill or adopted foreign value). This fills holes left
by out-of-order slot completion.

### Learner Stream Window

The per-peer bidi `PxLearnerStream` has an mpsc capacity of
`learner_stream_window_frames` (= `max_inflight_proposals * 4` = 64).
This gives 4× headroom over the proposer admission gate so the learner
channel never blocks before the proposer window is full.

When the learner stream mpsc is full, `dispatch` returns
`PxReplicaError::Internal("outbound queue full")`, which the proposer
maps to `PxPaxosError::Busy` (retryable).

### WAL Batch Aggregation

The WAL writer task drains all queued records in a single batch per
wake cycle:

1. `rx.recv()` — blocks until first record arrives.
2. `try_recv` drain — pulls all already-queued records.
3. Optional coalescing window (`wal_flush_coalesce_us`, default 0).
4. Single vectored `writev` + single `fdatasync` for the whole batch.
5. Resolve all pending oneshot acks.

This means multiple in-flight proposals hitting the same WAL pipeline
will have their records flushed together, amortizing fsync cost.

---

## Write Path Components — Detailed

### Payload Encoding

`PxKvStore::encode_kv_payload` / `encode_kv_batch_items` manually
binary-encode key-value pairs into a `Vec<u8>`. The `Vec<u8>` is
converted to `Bytes` once at `propose` entry (`Bytes::from(Vec<u8>)`
reuses the allocation, no copy).

### Leadership Gate

Checks both `role == Leader` and `current_term == proposing_term`.
If either fails, returns `NotLeader { leader_hint }` before slot
allocation. This drains in-flight client proposals early.

### Idempotency / Dedup

Before window admission, `dedup_lookup(client_id, seq)` checks if the
learner has already applied this `(client_id, seq)` pair. If so,
returns the cached commit slot without re-running Paxos. Duplicates
never consume a window permit.

### Prepare Phase (`run_prepare_phase`)

- **Local**: `on_prepare(slot, ballot, term)` → term fence →
  `acceptor.prepare(slot, ballot)` (lock-free CAS on slot node) →
  WAL append `Promised` record (awaits `fdatasync`) → return
  `PxPrepareReply`.
- **Remote**: Concurrent fan-out via `join_all` over
  `remote_replicas`, each `send_prepare` is a unary gRPC RPC
  (`PxServiceClient::prepare`). Replies are folded into
  accumulators (`promised`, `highest_rejected_round`,
  `highest_seen_term`, `epoch_mismatch`, `adopted`).
- **Quorum**: `promised >= quorum` → proceed; else retry or fail.

### Accept Phase (`run_accept_phase`)

- **Local**: `on_accept(entry)` → term fence →
  `acceptor.accept(entry.clone())` (lock-free CAS on slot node) →
  WAL append `Accepted` record (awaits `fdatasync`) → return
  `PxAcceptReply`.
- **Remote**: Concurrent fan-out via `join_all` over
  `remote_replicas`, each `send_accept` is sent over the per-peer
  bidi `PxLearnerStream` (awaits correlated reply via oneshot).
  Replies are folded into accumulators (`accepted`,
  `highest_rejected_round`, `highest_seen_term`, `epoch_mismatch`).
- **Quorum**: `accepted + 1 (local) >= quorum` → chosen; else retry
  or fail.

### Learn / Apply (`learn_chosen`)

- `learner.learn(entry, client_id, seq)` → decode `Batch` from
  payload → `KVEngine::apply(slot, &batch)` → update dedup map →
  advance frontiers.
- For `InMemKV`: apply is a `DashMap` insert — trivial.
- For `CrowtreeEngine`: apply is FFI → `ct_apply_put` / `ct_apply_delete`
  → memtable insert. May trigger memtable flush (compaction).
- `KVEngine::apply` is `async` but has no genuine `Pending` path
  today (no async apply C API) — it never actually suspends.

### Chosen Notice (`fan_out_chosen_notice`)

- Fire-and-forget `ChosenNotification` over each peer's
  `PxLearnerStream`. Non-blocking `try_send` on mpsc. Failures are
  logged at `debug!` and swallowed — next heartbeat re-converges
  peer frontiers.

---

## Tracked Issues

### R14 — Concurrent remote RPC fan-out in Paxos phases (IMPLEMENTED)

- **Where**: `run_prepare_phase` and `run_accept_phase` in
  `crowkv/src/cluster/group.rs` — previously sequential `.await` in
  `for remote in &self.remote_replicas` loops; now uses
  `futures::future::join_all` to issue all remote RPCs concurrently.
- **Impact**: One extra RPC round-trip per additional follower per
  phase was removed. With 3 nodes: ~20-30 µs overhead on loopback
  eliminated. Grows linearly with cluster size.
- **Tracking**: R14 in `new_requirements.md`.

### R15 — Zero-copy PxLogEntry in accept path

- **Where**: `on_accept` (`local_replica.rs:1099`) clones `entry`
  for the acceptor; `inner_accept` (`acceptor.rs:124`) clones again
  for `cas_accepted`; `base_entry` (`group.rs:1019`) clones
  `payload: Bytes` per slot retry.
- **Impact**: `Bytes::clone` is `O(1)` ref-count bump — negligible
  today. Goal is zero copy: acceptor takes `&PxLogEntry`, WAL encode
  borrows without clone. Only unavoidable clone is inside
  `cas_accepted` (slot node must own its copy).
- **Tracking**: R15 in `new_requirements.md`.

### R16 — Overlap local WAL fsync with remote RPC fan-out

- **Where**: `on_accept` (`local_replica.rs:1111-1116`) —
  `wal.append(&record).await` (fdatasync) completes before
  `PxAcceptReply::Accepted` is returned. The leader's local fsync
  is on the critical path *before* remote RPCs begin.
- **Impact**: Adds disk fsync latency (~10-100 µs NVMe, ~1-10 ms
  SSD/HDD) to the write path before any network I/O starts.
- **Concept change**: Weakens W6 ack contract for the local replica
  — accepted value may not be durably persisted before the accept
  reply is returned. Safe in Paxos (value not yet chosen) but
  changes durability ordering. Feature flag
  `wal_overlap_local_persist`, crash-recovery tests required.
- **Tracking**: R16 in `new_requirements.md`.

### R17 — Async engine apply after quorum

- **Where**: `propose` (`group.rs:1099`) —
  `replica.learn_chosen(&entry, client_id, seq).await` runs before
  `ProposeResult::Chosen` is returned to the client.
- **Impact**: Engine apply latency (FFI + memtable insert, potential
  flush) is on the write critical path. Significant for
  `CrowtreeEngine` under load.
- **Concept change**: Client receives "chosen" before local engine
  apply — breaks read-your-writes semantics. Mitigated via apply
  fence / read barrier. Feature flag `async_engine_apply`,
  read-after-write and crash-recovery tests required.
- **Note**: `fan_out_chosen_notice` (item 7) runs after
  `learn_chosen` but is a non-blocking mpsc enqueue — negligible
  cost, can stay where it is.
- **Tracking**: R17 in `new_requirements.md`.

### Items reviewed and not actionable

- **`fan_out_chosen_notice` synchronous** (item 7) — already
  fire-and-forget via `try_send` on mpsc. Non-blocking, negligible
  cost. No optimization needed.
- **`base_entry` payload clone per retry** (item 5) —
  `Bytes::clone` is `O(1)` ref-count bump. Covered by R15's
  zero-copy goal but not a standalone bottleneck.
- **Prepare phase on every slot in classic mode** (item 6) —
  already optimized via leader-lease mode (skips Phase 1 in
  steady state). Classic mode is the safety fallback.
- **`entry.clone()` in `on_accept`** (item 3) — `Bytes::clone` is
  `O(1)`. Covered by R15 but not a standalone bottleneck.

---

## Performance History — Write Path

### R14: Concurrent remote RPC fan-out (2026-07-20)

**Command:**
```
pixi run bench
# = cargo run --release -p crowkv-cli -- bench run \
#     --mode mem --duration-secs 12 --threads 2 --connections 1 \
#     --workload write
```

**Configuration:**
- 3-node cluster, in-memory WAL + in-memory KV (mem-block)
- 2 worker threads, 1 gRPC connection, write-only workload
- 512-byte values, 1M key space, 12-second duration

**Before (sequential fan-out):**
- Throughput: 10,034 ops/s
- Latency: avg=174us p50=142us p90=173us p99=238us p999=323us
- Per-window (steady state): avg=155us p50=140us p99=236us
- Errors: 4 (retries_exhausted), leader_query=16

**After (concurrent fan-out via `join_all`):**
- Throughput: 10,911 ops/s (+8.7%)
- Latency: avg=131us p50=123us p90=171us p99=235us p999=302us
- Per-window (steady state): avg=127us p50=121us p99=224us
- Errors: 0, leader_query=0

**Summary:**
- Throughput +8.7%, avg latency -25%, p50 latency -13%
- Tail latency improvement: max dropped from 772ms to 2.9ms
  (eliminated retry storms caused by sequential RPC timeouts)
- Zero errors in post-R14 run (vs 4 retries_exhausted before)

### Scaling sweep — 2026-07-20

All runs: 3-node cluster, in-memory WAL + in-memory KV (mem-block),
write-only, 512-byte values, 1M key space, 12-second duration,
`election_profile = e2e`. `max_inflight_proposals` was increased
from 16 → 32 → 64 as thread count grew to avoid `Busy` rejections.

| Threads | Conn | Inflight | Throughput (ops/s) | avg (us) | p50 (us) | p99 (us) | Errors |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | 1 | 16 | 8,404 | 118 | 97 | 183 | 3 |
| 2 | 1 | 16 | 10,911 | 131 | 123 | 235 | 0 |
| 16 | 1 | 32 | 23,400 | 683 | 669 | 1074 | 0 |
| 16 | 2 | 32 | 33,200 | 478 | 467 | 742 | 0 |
| 16 | 4 | 32 | 37,500 | 425 | 416 | 630 | 0 |
| 32 | 4 | 32 | 44,200 | 722 | 707 | 1111 | 0 |
| 32 | 8 | 32 | 44,600 | 715 | 703 | 1052 | 0 |
| 64 | 8 | 64 | 51,100 | 1249 | 1229 | 1853 | 0 |
| 64 | 16 | 64 | 50,700 | 1259 | 1243 | 1811 | 0 |

**Observations:**
- 1T→2T: +30% throughput (pipeline parallelism kicks in)
- 2T→16T/4C: 3.4× throughput from connection + thread scaling
- 16T/4C→32T/4C: +18% — inflight window bump (16→32) removed
  `Busy` rejections
- 32T/4C→32T/8C: +1% — gRPC channels no longer the bottleneck
- 32T→64T/8C: +15% throughput but 2× latency — diminishing returns
- 64T/8C→64T/16C: 0% — consensus path is the hard ceiling
- Scaling ceiling ~51K ops/s; per-proposal latency ~1.2ms at 64
  in-flight = 64/1.2ms ≈ 53K theoretical, matches observed
- Next gains require reducing per-proposal latency (R16: overlap
  WAL fsync with RPCs, R15: zero-copy accept path)

### Queue admission sweep — 2026-07-21 (R18)

All runs: 3-node cluster, in-memory WAL + in-memory KV (mem-block),
write-only, 512-byte values, 1M key space, 10-second duration,
`election_profile = e2e`. Admission policy = `Queue` (R18 default).
`Q` = inflight_queues (1 or 4).

**MI=32 (fixed inflight=32, sweep threads/connections/queues):**

| Threads | Conn | Q | Throughput (ops/s) | avg (us) | p50 (us) | p99 (us) | Errors |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | 1 | 1 | 11,355 | 104 | 95 | 154 | 0 |
| 2 | 1 | 1 | 16,742 | 119 | 112 | 199 | 0 |
| 4 | 1 | 1 | 21,560 | 185 | 173 | 289 | 0 |
| 4 | 2 | 1 | 24,960 | 160 | 151 | 251 | 0 |
| 4 | 4 | 1 | 27,040 | 147 | 139 | 231 | 0 |
| 4 | 8 | 1 | 23,560 | 169 | 159 | 269 | 0 |
| 8 | 2 | 1 | 26,761 | 297 | 287 | 473 | 0 |
| 8 | 4 | 1 | 29,632 | 268 | 263 | 395 | 0 |
| 16 | 4 | 1 | 32,536 | 490 | 440 | 659 | 0 |
| 16 | 8 | 1 | 36,880 | 432 | 424 | 633 | 0 |
| 32 | 8 | 1 | 36,192 | 882 | 876 | 1288 | 0 |
| 32 | 16 | 1 | 39,056 | 817 | 802 | 1219 | 0 |
| 32 | 16 | 4 | 39,924 | 799 | 785 | 1187 | 0 |
| 64 | 4 | 1 | 45,746 | 1396 | 1371 | 1996 | 0 |
| 64 | 8 | 1 | 43,846 | 1457 | 1428 | 1989 | 0 |

**MI=64 (inflight=64, sweep threads/connections/queues):**

| Threads | Conn | Q | Throughput (ops/s) | avg (us) | p50 (us) | p99 (us) | Errors |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 32 | 4 | 1 | 39,509 | 808 | 714 | 1101 | 0 |
| 32 | 8 | 1 | 43,989 | 725 | 712 | 1069 | 0 |
| 32 | 8 | 4 | 44,287 | 720 | 709 | 1053 | 0 |
| 32 | 16 | 1 | 44,015 | 725 | 712 | 1078 | 0 |
| 32 | 16 | 4 | 44,242 | 721 | 708 | 1081 | 0 |
| 64 | 4 | 1 | 48,813 | 1308 | 1276 | 2177 | 0 |
| 64 | 4 | 4 | 48,731 | 1310 | 1281 | 2131 | 0 |
| 64 | 8 | 1 | 50,454 | 1266 | 1248 | 1882 | 0 |
| 64 | 8 | 4 | 48,681 | 1312 | 1294 | 1951 | 0 |
| 64 | 16 | 1 | 44,302 | 1442 | 1282 | 1816 | 0 |

**MI=128 (inflight=128, sweep threads/connections/queues):**

| Threads | Conn | Q | Throughput (ops/s) | avg (us) | p50 (us) | p99 (us) | Errors |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 32 | 4 | 1 | 42,018 | 759 | 744 | 1185 | 0 |
| 32 | 8 | 4 | 42,698 | 747 | 735 | 1105 | 0 |
| 32 | 16 | 4 | 43,264 | 737 | 725 | 1091 | 0 |
| 64 | 4 | 4 | 48,468 | 1318 | 1291 | 2151 | 0 |
| 64 | 8 | 4 | 49,306 | 1296 | 1276 | 1949 | 0 |
| 64 | 16 | 4 | 49,004 | 1304 | 1291 | 1895 | 0 |

**Queue vs Reject comparison (best configs):**

| Mode | Best config | Throughput | avg (us) | p99 (us) | Errors |
| --- | --- | --- | --- | --- | --- |
| Reject (prev) | T64 C8 MI64 | 51,100 | 1249 | 1853 | 0 |
| Queue (R18) | T64 C8 Q1 MI64 | 50,454 | 1266 | 1882 | 0 |
| Queue (R18) | T64 C4 Q4 MI128 | 48,468 | 1318 | 2151 | 0 |
| Queue (R18) | T64 C4 Q1 MI32 | 45,746 | 1396 | 1996 | 0 |

**Observations:**
- Queue mode peak (50,454 ops/s) is within 1.3% of reject mode peak
  (51,100 ops/s) at the same MI=64 — the wait-queue overhead is
  negligible (a few atomic increments per proposal for metrics)
- Zero errors across all 70+ runs — no `Busy` rejections ever, no
  tuning of `max_inflight` needed to avoid reject storms
- MI=64 is the sweet spot for both modes; MI=128 shows no improvement
  (consensus path is the bottleneck, not admission gate)
- Multi-queue (Q=4) shows no measurable advantage at these thread
  counts — the semaphore is not the contention bottleneck; the
  consensus critical path (WAL append + quorum RPC) dominates
- MI=32 with queue mode (45,746 ops/s) nearly matches reject mode
  MI=64 (51,100 ops/s) — the queue absorbs contention that would
  otherwise require a larger inflight window
- Scaling ceiling remains ~50K ops/s; the bottleneck is per-proposal
  latency (~1.2ms at 64 in-flight), not admission control
- Queue mode simplifies operations: no need to tune `max_inflight`
  to avoid `Busy`; the queue naturally backpressures at any window size
