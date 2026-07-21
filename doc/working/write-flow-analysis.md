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
      3. Inflight admission (InflightAdmission::acquire_permit().await)
         - Queue policy (default): blocks on semaphore until a permit
           is freed — eliminates Busy rejections and client retry storms
         - Reject policy (tests only): try_acquire, returns Busy if full
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

### Inflight Admission (R18)

The group maintains an `InflightAdmission` gate backed by one or more
`tokio::sync::Semaphore` queues (`inflight_queues`, default 1). Total
permits across all queues = `max_inflight_proposals` (default 32).
Each `propose` call acquires one permit before slot allocation and holds
it for the entire proposal duration (released on drop at every return
path).

- **Queue policy (default)** — `acquire_permit().await` blocks until a
  permit is freed. No `Busy` rejections, no client retry needed. The
  semaphore's fast path is lock-free (atomic CAS).
- **Reject policy (tests only)** — `try_acquire`, returns
  `ProposeResult::Busy` if full.
- **Multi-queue routing** — permits distributed round-robin across N
  semaphores, each sized `ceil(max_inflight / N)`. Reduces semaphore
  contention under high concurrency (no measurable effect at ≤64
  threads; consensus path dominates).
- **Metrics** — `inflight_queue_depth`, `inflight_total_enqueued`,
  `inflight_total_wait_us`, `inflight_occupied` exposed via
  `GroupStatus`.

Slots are allocated via `next_slot.fetch_add(1, Ordering::Relaxed)` —
a lock-free atomic counter.

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
`learner_stream_window_frames` (default 64). This gives 2× headroom
over the default proposer admission window (32) so the learner channel
never blocks before the proposer window is full.

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

## Tracked Optimization Opportunities

- **R15 — Zero-copy PxLogEntry in accept path**: `on_accept` and
  `inner_accept` clone `entry` (Bytes, O(1) ref-count bump). Goal:
  acceptor takes `&PxLogEntry`, WAL encode borrows. Only unavoidable
  clone is inside `cas_accepted` (slot node must own its copy).
- **R16 — Overlap local WAL fsync with remote RPC fan-out**: Local
  `fdatasync` is on the critical path before remote RPCs begin. Adds
  ~10-100 µs (NVMe) to write latency. Would weaken W6 ack contract
  for local replica; requires feature flag + crash-recovery tests.
- **R17 — Async engine apply after quorum**: `learn_chosen` runs
  before `ProposeResult::Chosen` is returned. Engine apply (FFI +
  memtable insert) is on the write critical path. Would break
  read-your-writes; needs apply fence / read barrier.

---

## Benchmark Results — 2026-07-21

All runs: 3-node cluster, in-memory WAL + in-memory KV (mem-block),
write-only, 512-byte values, 1M key space, 12-second duration,
`election_profile = e2e`. Admission policy = `Queue` (R18 default).

### Queue admission — key configs

| Window | Threads | Conn | Throughput (ops/s) | avg (us) | p50 (us) | p99 (us) | Errors |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | 1 | 1 | 9,074 | 109 | 99 | 198 | 0 |
| 1 | 16 | 4 | 13,278 | 1203 | 1066 | 1360 | 0 |
| 16 | 1 | 1 | 8,969 | 110 | 98 | 168 | 0 |
| 16 | 16 | 4 | 36,661 | 434 | 426 | 642 | 0 |
| 32 | 64 | 4 | 45,746 | 1396 | 1371 | 1996 | 0 |
| 64 | 64 | 8 | 50,107 | 1275 | 1251 | 1942 | 0 |

### Queue vs Reject comparison

| Mode | Best config | Throughput | avg (us) | p99 (us) | Errors |
| --- | --- | --- | --- | --- | --- |
| Reject (pre-R18) | T64 C8 MI64 | 51,100 | 1249 | 1853 | 0 |
| Queue (R18) | T64 C8 Q1 MI64 | 50,454 | 1266 | 1882 | 0 |

### Conclusions

- **Queue mode peak (50K ops/s) matches reject mode** — semaphore
  queue overhead is negligible (a few atomic increments per proposal)
- **Zero `Busy` rejections across all 70+ runs** — no client retry
  logic needed, no `max_inflight` tuning to avoid reject storms
- **MI=64 is the sweet spot**; MI=128 shows no improvement (consensus
  path is the bottleneck, not admission gate)
- **Multi-queue (Q=4) has no measurable effect** at ≤64 threads —
  semaphore is not the contention bottleneck; WAL append + quorum RPC
  dominates
- **Scaling ceiling ~50K ops/s** — per-proposal latency ~1.2ms at 64
  in-flight = 64/1.2ms ≈ 53K theoretical, matches observed. Next
  gains require reducing per-proposal latency (R16, R15)
