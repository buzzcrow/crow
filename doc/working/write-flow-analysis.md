<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Write Flow Analysis

End-to-end trace of the CrowKV write path. Mirrors the structure of
[`read-flow-analysis.md`](read-flow-analysis.md). Focuses on flow,
conclusions, and data — not rationale prose.

---

## Write Flow — Single Proposal

```
Client PUT/DELETE/BatchWrite
  → PxKvStore::kv_put / kv_delete / kv_batch_write
    → encode payload (Vec<u8> → manual binary encode)
       [copy: client key/value slices → contiguous Vec<u8>, unavoidable]
  → PxKvStore::propose_and_respond
    → PxGroup::propose(payload, client_id, seq)
       [move: Vec<u8> → Bytes reuses allocation, zero copy]
      1. Leadership gate (role == Leader && current_term == proposing_term)
      2. Idempotency check (dedup_lookup by client_id + seq)
      3. Inflight admission (InflightAdmission::acquire_permit().await)
         - Queue policy (default): blocks on semaphore until a permit
           is freed — eliminates Busy rejections and client retry storms
         - Reject policy (tests only): try_acquire, returns Busy if full
      4. Slot allocation (next_slot.fetch_add)
      5. 'slot_retry loop (max_slot_retries = 3)
         a. base_entry(slot, payload.clone())
            [O(1) ref-count: Bytes::clone per retry attempt]
         b. 'paxos_attempt loop (max_paxos_retries = 3)
            i.  [if force_prepare] run_prepare_phase
                - local on_prepare (acceptor.prepare + WAL append Promised)
                - concurrent remote send_prepare RPCs (join_all, unary gRPC)
            ii. run_accept_phase
                - local on_accept (acceptor.accept + WAL append Accepted)
                  [O(1) ref-count: PxLogEntry::clone for cas_accepted]
                  [copy: entry.payload.to_vec() for WALRecord, unavoidable
                   today — could be Bytes::clone if WALRecord stored Bytes]
                  [no copy: IoSlice borrows Bytes for vectored writev]
                - concurrent remote send_accept RPCs (join_all, bidi LearnerStream)
                  [O(1) ref-count: Bytes::clone for AcceptRequest]
                  [copy: payload → socket buffer on gRPC serialize, unavoidable]
                  [move: follower gRPC deserialize → PxLogEntry.payload Bytes]
            iii. quorum check
            iv. [if chosen] learn_chosen (decode + KVEngine::apply)
                [O(1) ref-count: PxLogEntry::clone for learner]
                [O(1) ref-count: Batch::decode uses Bytes::slice per key/value]
                [no copy: FFI ct_apply_batch_slices takes ct_kv_ref pointers
                 into caller's Bytes slices (R23, done)]
                [copy: C++ engine copies key/value into internal memtable,
                 unavoidable]
            v.  [if chosen] fan_out_chosen_notice (fire-and-forget mpsc)
            vi. [if chosen] return ProposeResult::Chosen { slot }
      6. [if all retries exhausted] return ProposeResult::Err
  → KvResponse::ok_chosen(slot, ...) or error
```

### Key Data Structures

- **`PxLogEntry`** — `{ slot, ballot: {round, leader_id}, term, payload:
  Bytes }`. Unit of Paxos consensus. `payload` is `bytes::Bytes` for
  `O(1)` ref-count-bump clones.
- **`PxBallot`** — `{ round, leader_id }`. Monotone per slot;
  prepare/accept fencing.
- **`WALRecord`** — encoded from `PxLogEntry` (or `{term, slot, ballot}`
  for Promised). Written via `WalEngine::append` → per-pipeline writer
  task → `fdatasync`.
- **`AcceptRequest` / `AcceptedResponse`** — gRPC wire types for the
  accept phase, over the per-peer bidi `PxLearnerStream`.
- **`PrepareRequest` / `PromiseResponse`** — gRPC wire types for the
  prepare phase, unary RPC (not over LearnerStream).

---

## Multi-Slot Concurrency

- **Inflight admission (R18)** — `InflightAdmission` gate backed by N
  `tokio::sync::Semaphore` queues (`inflight_queues`, default 1). Total
  permits = `max_inflight_proposals` (default 32). Each `propose`
  acquires one permit before slot allocation, holds it for the entire
  proposal duration (released on drop at every return path).
  - **Queue policy (default)** — `acquire_permit().await` blocks until
    a permit is freed; no `Busy` rejections, no client retry;
    semaphore fast path is lock-free (atomic CAS).
  - **Reject policy (tests only)** — `try_acquire`, returns
    `ProposeResult::Busy` if full.
  - **Multi-queue routing** — permits round-robin across N semaphores,
    each `ceil(max_inflight / N)`; reduces contention (no measurable
    effect at ≤64 threads; consensus path dominates).
  - **Metrics** — `inflight_queue_depth`, `inflight_total_enqueued`,
    `inflight_total_wait_us`, `inflight_occupied` via `GroupStatus`.
  - **Slot allocation** — `next_slot.fetch_add(1, Ordering::Relaxed)`,
    lock-free atomic.
- **Per-slot independence** — slot N may be in accept while N+1 is in
  prepare; slots chosen out of order (higher slot may be chosen before
  a lower one if the lower hits a retry). Learner tracks
  `contiguous_chosen` (highest contiguous chosen) and
  `last_chosen_slot` (highest overall); gaps filled by background
  repair.
- **Background repair (`repair_once`)** — leader steady-state: when
  `contiguous_chosen < last_chosen_slot`, runs classic Paxos (prepare +
  accept) on `gap_slot = contiguous_chosen + 1` with empty payload
  (`NoOp` fill or adopted foreign value).
- **Learner stream window** — per-peer bidi `PxLearnerStream` mpsc
  capacity `learner_stream_window_frames` (default 64) = 2× headroom
  over the default proposer window (32) so the learner channel never
  blocks before the proposer window is full. When full, `dispatch`
  returns `PxReplicaError::Internal("outbound queue full")` → proposer
  maps to `PxPaxosError::Busy` (retryable).
- **WAL batch aggregation** — writer task drains all queued records per
  wake cycle: `rx.recv()` (block until first) → `try_recv` drain →
  optional coalesce (`wal_flush_coalesce_us`, default 0) → single
  vectored `writev` + single `fdatasync` → resolve all pending oneshot
  acks. Multiple in-flight proposals on the same pipeline flush
  together, amortizing fsync cost.

---

## Write Path Components

- **Payload encoding** — `encode_kv_payload` /
  `encode_kv_batch_items` manually binary-encode key-value pairs into a
  `Vec<u8>`, converted to `Bytes` once at `propose` entry
  (`Bytes::from(Vec<u8>)` reuses the allocation, no copy).
- **Leadership gate** — checks `role == Leader` and
  `current_term == proposing_term`; either fails →
  `NotLeader { leader_hint }` before slot allocation. Drains in-flight
  client proposals early.
- **Idempotency / dedup** — before window admission,
  `dedup_lookup(client_id, seq)` checks if the learner already applied
  this pair; if so, returns the cached commit slot without re-running
  Paxos. Duplicates never consume a window permit.
- **Prepare phase (`run_prepare_phase`)** — local: `on_prepare` → term
  fence → `acceptor.prepare` (lock-free CAS on slot node) → WAL append
  `Promised` (awaits `fdatasync`) → `PxPrepareReply`. Remote:
  `join_all` over `remote_replicas`, each `send_prepare` unary gRPC;
  replies folded into `promised`, `highest_rejected_round`,
  `highest_seen_term`, `epoch_mismatch`, `adopted`. Quorum:
  `promised >= quorum` → proceed; else retry or fail.
- **Accept phase (`run_accept_phase`)** — local: `on_accept` → term
  fence → `acceptor.accept(entry.clone())` (lock-free CAS) → WAL append
  `Accepted` (awaits `fdatasync`) → `PxAcceptReply`. Remote: `join_all`
  over `remote_replicas`, each `send_accept` over the per-peer bidi
  `PxLearnerStream` (awaits correlated reply via oneshot); replies
  folded into `accepted`, `highest_rejected_round`,
  `highest_seen_term`, `epoch_mismatch`. Quorum:
  `accepted + 1 (local) >= quorum` → chosen; else retry or fail.
- **Learn / apply (`learn_chosen`)** —
  `learner.learn(entry, client_id, seq)` → decode `Batch` from payload
  → `KVEngine::apply(slot, &batch)` → update dedup map → advance
  frontiers. `InMemKV`: `DashMap` insert (trivial). `CrowtreeEngine`:
  FFI `ct_apply_put` / `ct_apply_delete` → memtable insert (may
  trigger flush/compaction). `KVEngine::apply` is `async` but has no
  genuine `Pending` path today (no async apply C API) — never
  suspends.
- **Chosen notice (`fan_out_chosen_notice`)** — fire-and-forget
  `ChosenNotification` over each peer's `PxLearnerStream`; non-blocking
  `try_send` on mpsc; failures logged at `debug!` and swallowed — next
  peer frontiers.

---

## Tracked Optimization Opportunities

- **R15 — Zero-copy PxLogEntry in accept path** (done): `on_accept` and
  `acceptor.accept` take `&PxLogEntry`; redundant clones eliminated;
  only remaining clone is inside `inner_accept` for `cas_accepted` (slot
  node must own its copy, O(1) ref-count bump with `Bytes` payload).
- **R16 — Overlap local WAL fsync with remote RPC fan-out**: local
  `fdatasync` is on the critical path before remote RPCs begin (~10-100
  µs on NVMe). Would weaken W6 ack contract for local replica; requires
  feature flag + crash-recovery tests.
- **R17 — Async engine apply after quorum**: `learn_chosen` runs before
  `ProposeResult::Chosen` is returned; engine apply (FFI + memtable
  insert) is on the write critical path. Would break read-your-writes;
  needs apply fence / read barrier.

---

## Benchmark Results — 2026-07-21

3-node cluster, in-memory WAL + in-memory KV (mem-block), write-only,
512-byte values, 1M key space, 12-second duration,
`election_profile = e2e`. Admission policy = `Queue` (R18 default).
TPS = success-only (total_attempts - total_errors / duration).

### Factors Affecting TPS

- **`max_inflight_proposals` (window)** — total semaphore permits
  across all admission queues; primary TPS lever. More permits = more
  pipeline parallelism = higher throughput, until the consensus
  critical path (WAL append + quorum RPC) becomes the bottleneck.
- **`threads` (worker tasks)** — client-side concurrency; more workers
  fill the pipeline faster; diminishing returns once server-side
  consensus is saturated.
- **`connections` (gRPC channels)** — per-endpoint channel pool size;
  must be sufficient to avoid gRPC stream head-of-line blocking; beyond
  ~4, no measurable effect (bottleneck moves to server-side consensus).

No measurable effect on TPS (at ≤64 threads):

- **`inflight_queues`** — multi-queue routing (1 vs 4 semaphores);
  semaphore is not the contention bottleneck; consensus path
  dominates. Default 1 is sufficient.
- **Window > 64** — MI=128 no improvement over MI=64. Consensus
  critical path ~1.2ms per proposal is the hard ceiling:
  64 / 1.2ms ≈ 53K theoretical, matches observed 50K peak.

### Scaling: 1T to 64T (MI=64, Q=1)

| Threads | Conn | Throughput (ops/s) | avg (µs) | p50 (µs) | p99 (µs) | Errors |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | 1 | 9,074 | 109 | 99 | 198 | 0 |
| 2 | 1 | 16,742 | 119 | 112 | 199 | 0 |
| 4 | 4 | 27,040 | 147 | 139 | 231 | 0 |
| 8 | 4 | 29,632 | 268 | 263 | 395 | 0 |
| 16 | 8 | 36,880 | 432 | 424 | 633 | 0 |
| 32 | 8 | 43,989 | 725 | 712 | 1069 | 0 |
| 64 | 8 | 50,107 | 1275 | 1251 | 1942 | 0 |

### Window impact (16T, 4C, Q=1)

| Window | Throughput (ops/s) | avg (µs) | p50 (µs) | p99 (µs) | Errors |
| --- | --- | --- | --- | --- | --- |
| 1 | 13,278 | 1203 | 1066 | 1360 | 0 |
| 16 | 36,661 | 434 | 426 | 642 | 0 |
| 32 | 36,792 | 433 | 424 | 635 | 0 |
| 64 | 36,543 | 436 | 427 | 643 | 0 |

### Conclusions

- **Window is the primary TPS lever** — W=1→W=16 gives 2.8× throughput
  (13K→37K at 16T/4C). W=16+ converge to ~37K at this thread count
  (pipeline already saturated).
- **Threads scale throughput until server saturation** — 1T→64T gives
  5.5× throughput (9K→50K at MI=64). Returns diminish after 32T.
- **Connections matter only at low counts** — 1C→4C gives +60% at 4T;
  beyond 4C, no effect (gRPC is not the bottleneck).
- **Queue mode: zero errors across all configurations** — no `Busy`
  rejections, no client retry, no window tuning to avoid reject storms;
  queue naturally backpressures at any window size.
- **Scaling ceiling ~50K ops/s** — per-proposal latency ~1.2ms at 64
  in-flight. Next gains require reducing per-proposal latency (R16:
  overlap WAL fsync with RPCs; R15: zero-copy accept path).

---

## Memory Copy Summary

Copy points are annotated inline in the flow diagram above. Summary
of what remains:

- **O(n) unavoidable** — payload encoding (client key/value slices →
  contiguous `Vec<u8>`); WAL encode (`entry.payload.to_vec()` for
  `WALRecord`); WAL replay (`Bytes::copy_from_slice` to reconstruct
  `PxLogEntry`); C++ engine apply (internal memtable copy); gRPC
  socket write (kernel user→socket buffer copy).
- **O(1) ref-count bumps (negligible)** — `base_entry` payload clone
  per slot retry; `inner_accept` entry clone for `cas_accepted`;
  `send_accept` payload clone for protobuf; `learn_chosen` entry clone
  for learner; WAL `encode_frame` payload clone for `RecordFrame`;
  Batch decode `Bytes::slice` per key/value (shares payload buffer).
- **Zero-copy (move/borrow)** — `Vec<u8>` → `Bytes` at `propose`
  entry; gRPC deserialization → `PxLogEntry` (move `Bytes`); WAL
  vectored write (`IoSlice` borrows `Bytes`); FFI batch apply
  `ct_kv_ref` pointer-length structs (R23, done).

### Optimization Opportunities

- **WAL encode** (`entry.payload.to_vec()`) — eliminable if `WALRecord`
  stored `Bytes` directly instead of round-tripping through `Vec<u8>`.
  `entry.payload` is already `Bytes`; `to_vec()` is redundant —
  `WALRecord.payload` could be `entry.payload.clone()` (O(1) ref-count).
  `encode_accepted_payload` exists only as a seam for future encoding
  changes; today it is a straight `to_vec()`.
- **Batch decode** — already zero-copy: `Batch::decode` uses
  `Bytes::slice` (O(1) ref-count), not `to_vec()`; `BatchOp` owns
  `Bytes` that share the payload buffer.
- **FFI batch encode** — already eliminated (R23, done):
  `ct_apply_batch_slices` accepts an array of `ct_kv_ref`
  pointer-length structs; no packing copy.
- **Client-side batch copy (R25)** — `CrowkvClient::batch_write` clones
  each `BatchOp` key/value (`Vec<u8>::clone`) into `KvBatchItem`, then
  clones the entire `items` vec per retry. Switching client `BatchOp`
  and proto `bytes` fields to `bytes::Bytes` (via `prost-build` config)
  makes these O(1) ref-count bumps. Medium complexity — type change
  ripples through call sites but no C++ or consensus changes.
