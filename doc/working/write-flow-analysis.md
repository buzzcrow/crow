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

## Benchmark Results — 2026-07-24

Systematic T:C:W sweep. 3-node cluster (bench fixture, in-process
console-web + 3 spawned `crowkv-server` processes), in-memory WAL +
in-memory KV (mem-block), write-only, 512-byte values, 1M key space,
12-second duration, `election_profile = e2e`, admission policy =
`Queue` (R18 default). Platform: AMD Ryzen 9 5950X (16 cores / 32
threads), Linux. 28 runs total, zero errors across all configs.

Script: `tools/bench-write-sweep.sh`. Regression subset:
`tools/bench-write-regression.sh`.

### Factors Affecting TPS

- **`max_inflight_proposals` (window)** — total semaphore permits
  across all admission queues; primary TPS lever. More permits = more
  pipeline parallelism = higher throughput, until the consensus
  critical path (WAL append + quorum RPC) becomes the bottleneck.
- **`threads` (worker tasks)** — client-side concurrency; more workers
  fill the pipeline faster; diminishing returns once server-side
  consensus is saturated (~24T).
- **`connections` (gRPC channels)** — **no measurable effect at any
  thread count**. Unlike reads (where T:C ratio matters due to the
  HTTP/2 connection lock), writes are bottlenecked by server-side
  consensus (WAL fsync + quorum RPC), not gRPC framing. C=3 and C=48
  produce identical throughput at 12T and 48T.

No measurable effect on TPS (at ≤64 threads):

- **`inflight_queues`** — multi-queue routing (1 vs 4 semaphores);
  semaphore is not the contention bottleneck; consensus path
  dominates. Default 1 is sufficient.
- **Window > 16** — MI=16, 32, 64 all converge to ~29K at 48T.
  Consensus critical path is the hard ceiling.
- **Connections** — C has zero effect on write throughput at any T.
  The write path's bottleneck is consensus, not gRPC.

### Phase 1 — Baseline 1T:1C scaling (MI=64)

| Threads | Conn | Throughput (ops/s) | avg (µs) | p50 (µs) | p99 (µs) | p999 (µs) | Errors |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | 1 | 3,249 | 305 | 339 | 444 | 562 | 0 |
| 6 | 6 | 19,922 | 299 | 288 | 475 | 984 | 0 |
| 12 | 12 | 25,802 | 462 | 449 | 740 | 1,684 | 0 |
| 24 | 24 | 28,761 | 832 | 820 | 1,264 | 2,751 | 0 |
| 48 | 48 | 28,898 | 1,658 | 1,643 | 2,419 | 5,399 | 0 |

Throughput plateaus at 24T (~29K). Adding threads beyond 24T
increases latency without improving throughput — the consensus
pipeline is saturated.

### Phase 2 — T:C ratio exploration (MI=64)

| Threads | Conn | Ratio | Throughput (ops/s) | avg (µs) | p99 (µs) | Errors |
| --- | --- | --- | --- | --- | --- | --- |
| 12 | 3 | 4:1 | 24,665 | 484 | 801 | 0 |
| 12 | 6 | 2:1 | 25,734 | 464 | 750 | 0 |
| 12 | 12 | 1:1 | 25,798 | 463 | 771 | 0 |
| 12 | 24 | 1:2 | 25,840 | 462 | 743 | 0 |
| 12 | 48 | 1:4 | 25,737 | 464 | 760 | 0 |
| 48 | 12 | 4:1 | 29,267 | 1,637 | 2,453 | 0 |
| 48 | 24 | 2:1 | 29,057 | 1,649 | 2,471 | 0 |
| 48 | 48 | 1:1 | 29,134 | 1,645 | 2,373 | 0 |
| 48 | 64 | 1:1.3 | 29,004 | 1,652 | 2,397 | 0 |

**Key finding: T:C ratio has zero effect on write throughput.** At
12T, C=3 and C=48 both give ~25K. At 48T, C=12 and C=64 both give
~29K. This is fundamentally different from reads, where the HTTP/2
connection lock makes T:C ratio critical. Writes are bottlenecked by
the consensus critical path (WAL append + quorum RPC), which is
server-side and independent of how many gRPC channels the client
opens.

### Phase 3 — Window impact at 48T:48C

| Window | Throughput (ops/s) | avg (µs) | p50 (µs) | p99 (µs) | p999 (µs) | Errors |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | 6,361 | 7,544 | 6,499 | 10,455 | 10,687 | 0 |
| 4 | 20,776 | 2,308 | 2,253 | 3,215 | 8,295 | 0 |
| 16 | 28,040 | 1,709 | 1,679 | 2,467 | 5,963 | 0 |
| 32 | 28,827 | 1,662 | 1,644 | 2,481 | 5,563 | 0 |
| 64 | 28,920 | 1,657 | 1,638 | 2,509 | 5,067 | 0 |

**Window is the primary TPS lever.** MI=1→16 gives 4.4× throughput
(6K→28K). MI=1 (effectively Raft-style sequential commit) serializes
all proposals through a single permit — 48 threads queue at 7.5ms
avg latency. MI=16+ converges: the consensus pipeline is saturated,
adding more permits doesn't help because the per-proposal critical
path (WAL fsync + quorum RPC) is the bottleneck.

### Phase 4 — Low thread count (MI=64)

| Threads | Conn | Ratio | Throughput (ops/s) | avg (µs) | p50 (µs) | p99 (µs) | Errors |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | 1 | 1:1 | 2,836 | 350 | 354 | 445 | 0 |
| 1 | 2 | 1:2 | 2,914 | 340 | 358 | 447 | 0 |
| 1 | 4 | 1:4 | 2,842 | 349 | 361 | 450 | 0 |
| 2 | 1 | 2:1 | 6,344 | 313 | 261 | 500 | 0 |
| 2 | 2 | 1:1 | 7,210 | 275 | 224 | 475 | 0 |
| 2 | 4 | 1:2 | 8,889 | 223 | 210 | 383 | 0 |
| 3 | 1 | 3:1 | 11,856 | 251 | 243 | 409 | 0 |
| 3 | 3 | 1:1 | 12,915 | 230 | 222 | 383 | 0 |
| 3 | 6 | 1:2 | 12,704 | 234 | 219 | 357 | 0 |

At 1T, C has no effect (~2.8K) — single-thread throughput is bounded
by per-proposal latency (~350us). At 2T, more connections help
(6.3K→8.9K) because 2 threads sharing 1 connection contend on the h2
lock; 2T:4C gives each thread its own connection. At 3T, 3C and 6C
converge (~12.8K).

### Comparison with previous test (2026-07-21)

The previous test reported 50K ops/s peak at 64T:8C. This sweep
measures ~29K at all 48T+ configs, including a direct 64T:8C
re-run (29,319 ops/s). The difference is likely due to code changes
between July 21 and July 24 (WAL restore, election fixes, group
wiring changes). The relative findings are consistent: window is the
primary lever, threads scale until consensus saturation, connections
have minimal effect. The absolute throughput regression is
not investigated here — it warrants a separate profiling pass.

### Conclusions

- **Window is the primary TPS lever** — MI=1→16 gives 4.4× throughput
  (6K→28K at 48T). MI=16+ converges (consensus pipeline saturated).
- **Threads scale until 24T, then plateau** — 1T→24T gives 10×
  throughput (3K→29K). 24T→48T adds latency without throughput gain.
- **T:C ratio has zero effect on writes** — unlike reads, C=3 and C=64
  produce identical throughput at the same T. The write bottleneck is
  server-side consensus (WAL fsync + quorum RPC), not gRPC framing.
  This is the key difference from reads, where the HTTP/2 connection
  lock makes T:C ratio critical.
- **Queue mode: zero errors across all 28 configurations** — no `Busy`
  rejections, no client retry; queue naturally backpressures at any
  window size.
- **Scaling ceiling ~29K ops/s** — per-proposal latency ~1.7ms at 48
  in-flight. Next gains require reducing per-proposal latency (R16:
  overlap WAL fsync with RPCs; R15: zero-copy accept path). The
  previous 50K ceiling suggests a regression worth investigating.

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
