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
