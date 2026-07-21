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

- **R15 — Zero-copy PxLogEntry in accept path** (done): `on_accept`
  and `acceptor.accept` now take `&PxLogEntry`. All redundant clones
  eliminated; the only remaining clone is inside `inner_accept` for
  `cas_accepted` (slot node must own its copy, O(1) ref-count bump
  with `Bytes` payload). See Memory Copy Analysis below for full
  audit.
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
TPS = success-only (total_attempts - total_errors / duration).

### Factors Affecting TPS

Three tunable parameters influence write throughput:

- **`max_inflight_proposals` (window)** — Total semaphore permits
  across all admission queues. Controls how many proposals can be
  in-flight concurrently. This is the primary TPS lever: more permits
  = more pipeline parallelism = higher throughput, until the consensus
  critical path (WAL append + quorum RPC) becomes the bottleneck.
- **`threads` (worker tasks)** — Client-side concurrency. More workers
  fill the pipeline faster. Diminishing returns once the server-side
  consensus path is saturated.
- **`connections` (gRPC channels)** — Per-endpoint channel pool size.
  Must be sufficient to avoid gRPC stream head-of-line blocking.
  Beyond ~4 connections, no measurable effect — the bottleneck moves
  to server-side consensus.

Parameters with **no measurable effect** on TPS (at ≤64 threads):

- **`inflight_queues`** — Multi-queue routing (1 vs 4 semaphores).
  The semaphore is not the contention bottleneck; consensus path
  dominates. Default 1 is sufficient.
- **Window > 64** — MI=128 shows no improvement over MI=64. The
  consensus critical path (~1.2ms per proposal) is the hard ceiling:
  64 / 1.2ms ≈ 53K theoretical, matches observed 50K peak.

### Scaling: 1T to 64T (MI=64, Q=1)

| Threads | Conn | Throughput (ops/s) | avg (us) | p50 (us) | p99 (us) | Errors |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | 1 | 9,074 | 109 | 99 | 198 | 0 |
| 2 | 1 | 16,742 | 119 | 112 | 199 | 0 |
| 4 | 4 | 27,040 | 147 | 139 | 231 | 0 |
| 8 | 4 | 29,632 | 268 | 263 | 395 | 0 |
| 16 | 8 | 36,880 | 432 | 424 | 633 | 0 |
| 32 | 8 | 43,989 | 725 | 712 | 1069 | 0 |
| 64 | 8 | 50,107 | 1275 | 1251 | 1942 | 0 |

### Window impact (16T, 4C, Q=1)

| Window | Throughput (ops/s) | avg (us) | p50 (us) | p99 (us) | Errors |
| --- | --- | --- | --- | --- | --- |
| 1 | 13,278 | 1203 | 1066 | 1360 | 0 |
| 16 | 36,661 | 434 | 426 | 642 | 0 |
| 32 | 36,792 | 433 | 424 | 635 | 0 |
| 64 | 36,543 | 436 | 427 | 643 | 0 |

### Conclusions

- **Window is the primary TPS lever** — W=1→W=16 gives 2.8× throughput
  (13K→37K at 16T/4C). W=16+ all converge to ~37K at this thread count
  (pipeline is already saturated).
- **Threads scale throughput until server saturation** — 1T→64T gives
  5.5× throughput (9K→50K at MI=64). Returns diminish after 32T.
- **Connections matter only at low counts** — 1C→4C gives +60% at 4T.
  Beyond 4C, no effect (gRPC is not the bottleneck).
- **Queue mode: zero errors across all configurations** — no `Busy`
  rejections, no client retry logic needed, no window tuning to avoid
  reject storms. The queue naturally backpressures at any window size.
- **Scaling ceiling ~50K ops/s** — per-proposal latency ~1.2ms at 64
  in-flight. Next gains require reducing per-proposal latency (R16:
  overlap WAL fsync with RPCs, R15: zero-copy accept path).

---

## Memory Copy Analysis

Audit of every point in the write path where payload bytes are
allocated or copied. Focus is on O(n) operations (heap allocate +
memcpy proportional to payload size), not O(1) ref-count bumps.

### Notation

- **O(n) copy** = heap allocate + memcpy, proportional to payload size.
  These are the operations that matter for large (e.g. 1 MB) payloads.
- **O(1) ref-count** = `Bytes::clone` or `Arc::clone`, atomic ref-count
  increment. Negligible regardless of payload size.
- **move** = ownership transfer, zero cost.

### Write Path: Client → Consensus → WAL → Engine

**Step 1 — Payload encoding** (`px_kv_store.rs:608`)
`encode_kv_payload` builds a `Vec<u8>` by `extend_from_slice` for each
key and value.
- **O(n) copy** — one allocation + memcpy of all keys and values into
  a contiguous buffer. This is the initial encoding; unavoidable since
  the client sends separate key/value slices.

**Step 2 — Vec → Bytes conversion** (`group.rs:982`)
`Bytes::from(payload)` where `payload: Vec<u8>`.
- **move** — `Bytes::from(Vec<u8>)` reuses the existing allocation,
  zero copy.

**Step 3 — `base_entry` construction** (`group.rs:1310`)
`base_entry(slot, payload.clone())` — called per slot retry attempt.
- **O(1) ref-count** — `Bytes::clone` inside `PxLogEntry::clone`.
  The retry loop clones `payload` because it is reused across attempts;
  each clone is a ref-count bump.

**Step 4 — Local accept: `on_accept` → `acceptor.accept` → `inner_accept`**
(`local_replica.rs:1128`, `acceptor.rs:124`)
`self.acceptor.accept(entry)` where `entry: &PxLogEntry`.
`inner_accept` does `entry.clone()` for `cas_accepted`.
- **O(1) ref-count** — `PxLogEntry::clone` = 3×u64 bit copy + 1×
  `Bytes::clone` (ref-count bump). The slot node must own its copy
  (`Box::into_raw(Box::new(new))`), so this clone is unavoidable.

**Step 5 — WAL encode: `WALRecord::from_accepted`** (`record.rs:408`)
`encode_accepted_payload(entry)` does `entry.payload.to_vec()`
(`record.rs:487`).
- **O(n) copy** — heap allocate + memcpy of the entire payload. This
  is a real payload copy. The `Vec<u8>` is then wrapped in
  `Bytes::from(vec)` for the `WALRecord.payload` field.

**Step 6 — WAL frame encode** (`record.rs:199`)
`encode_frame()` does `payload: self.payload.clone()` for the
`RecordFrame`.
- **O(1) ref-count** — `Bytes::clone` on the WAL record's payload.

**Step 7 — WAL vectored write** (`record.rs:152`)
`IoSlice::new(&self.payload)` borrows the frame's `Bytes`.
- **No copy** — vectored `writev` writes directly from the `Bytes`
  buffer to the file descriptor.

**Step 8 — Remote accept: `send_accept`** (`remote_replica.rs:187`)
`payload: entry.payload.clone()` for the protobuf `AcceptRequest`.
- **O(1) ref-count** — `Bytes::clone` for the gRPC message. The gRPC
  serializer then writes from this `Bytes` into the HTTP/2 frame
  buffer (one copy into the socket buffer, unavoidable for network
  transport).

**Step 9 — Follower gRPC deserialization** (`px_service.rs:502-510`)
`PxLogEntry { payload: value.payload }` — moves the `Bytes` from the
deserialized protobuf message.
- **move** — no copy. The gRPC deserializer allocates the `Bytes`
  buffer from the network frame; ownership transfers to `PxLogEntry`.

**Step 10 — Learn: `learn_chosen`** (`local_replica.rs:1152`)
`entry.clone()` for the `Learner::learn` trait, then
`apply_entry(slot, payload.as_ref())` passes `&[u8]`.
- **O(1) ref-count** — `PxLogEntry::clone` for the learner call.

**Step 11 — Batch decode: `Batch::decode`** (`op.rs:54`)
`payload.get(..).to_vec()` for each key and value (`op.rs:70,75`).
- **O(n) copy** — heap allocate + memcpy per key and per value. The
  `Batch` owns its ops as `Vec<u8>` for each key and value.

**Step 12 — FFI encode: `encode_batch`** (`crowtree/ffi/src/lib.rs:415`)
Packs `BatchOp`s into a `Vec<u8>` for `ct_apply_batch`.
- **O(n) copy** — heap allocate + memcpy of all keys and values into
  the packed FFI buffer format.

**Step 13 — FFI apply: `ct_apply_batch`** (C++ engine)
The C++ engine copies the packed buffer into its internal memtable.
- **O(n) copy** — unavoidable; the engine owns its internal storage.

### WAL Replay Path

**`WALRecord::to_log_entry`** (`record.rs:451`) →
`decode_accepted_payload` does `Bytes::copy_from_slice(&rec.payload[..])`
(`record.rs:491`).
- **O(n) copy** — heap allocate + memcpy. Unavoidable: the WAL
  record's `Bytes` is a different allocation from the original
  `PxLogEntry`; replay must reconstruct the entry.

### Summary Table

- **O(n) copies (unavoidable):**
  - Payload encoding — client key/value slices → contiguous `Vec<u8>`.
  - WAL encode — `entry.payload.to_vec()` for `WALRecord`.
  - WAL replay — `Bytes::copy_from_slice` to reconstruct `PxLogEntry`.
  - Batch decode — `to_vec()` per key/value for `Batch` ops.
  - FFI encode — pack ops into `Vec<u8>` for `ct_apply_batch`.
  - C++ engine apply — internal memtable copy.
  - gRPC socket write — kernel copies from user buffer to socket
    buffer (unavoidable for network I/O).

- **O(1) ref-count bumps (negligible):**
  - `base_entry` payload clone per slot retry.
  - `inner_accept` entry clone for `cas_accepted`.
  - `send_accept` payload clone for protobuf.
  - `learn_chosen` entry clone for learner.
  - WAL `encode_frame` payload clone for `RecordFrame`.

- **Zero-copy (move or borrow):**
  - `Vec<u8>` → `Bytes` conversion at `propose` entry.
  - gRPC deserialization → `PxLogEntry` (move `Bytes`).
  - WAL vectored write (`IoSlice` borrows `Bytes`).

### Optimization Opportunities

- **WAL encode** (`entry.payload.to_vec()`): could be eliminated if
  `WALRecord` stored `Bytes` directly instead of round-tripping through
  `Vec<u8>`. Since `entry.payload` is already `Bytes`, the
  `to_vec()` is a redundant copy — `WALRecord.payload` could be
  `entry.payload.clone()` (O(1) ref-count). The `encode_accepted_payload`
  function exists only as a seam for future encoding changes; today it
  is a straight `to_vec()`.
- **Batch decode** (`to_vec()` per key/value): could be eliminated if
  `Batch` borrowed from the `Bytes` payload instead of owning
  `Vec<u8>`. This would require a lifetime parameter on `Batch` and
  ripple through the `KVEngine` trait — significant refactor.
- **FFI encode** (`encode_batch`): could be eliminated if the C++
  engine accepted `Bytes`-backed slices directly instead of a packed
  buffer. Would require a new C API.
