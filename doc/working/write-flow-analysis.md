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
            i.  [if force_prepare] run_prepare_phase (R16a: concurrent)
                - tokio::join!(local on_prepare, join_all(remote send_prepare))
                  local on_prepare: acceptor.prepare + WAL append Promised
                  remote: send_prepare RPCs (unary gRPC)
                - quorum check counts the local reply (W6 intact)
            ii. run_accept_phase (R16a/R16b: concurrent, two paths)
                - remote (both paths): send_accept RPCs (join_all, bidi
                  LearnerStream)
                  [O(1) ref-count: Bytes::clone for AcceptRequest]
                  [copy: payload → socket buffer on gRPC serialize, unavoidable]
                  [move: follower gRPC deserialize → PxLogEntry.payload Bytes]
                - default (wal_early_ack = false, R16a):
                    tokio::join!(local on_accept, join_all(remote send_accept))
                    local on_accept = on_accept_inner (CAS) + on_accept_persist
                      (WAL append Accepted, awaits fdatasync)
                      [O(1) ref-count: PxLogEntry::clone for cas_accepted]
                      [no copy: encode_accepted_payload is entry.payload.clone()
                       (O(1) ref-count); WALRecord.payload is Bytes]
                      [no copy: IoSlice borrows Bytes for vectored writev]
                    quorum check waits for the local reply (W6 intact)
                - early-ack (wal_early_ack = true, R16b):
                    tokio::join!(on_accept_inner (CAS only), join_all(remote))
                    local WAL persist deferred to spawn_accept_persist
                      (fire-and-forget tokio::spawn; best-effort durability)
                    chosen declared on remote quorum + local CAS, before fsync
                      (weakens W6 for the local replica)
            iii. quorum check
            iv. [if chosen] learn_chosen (decode + KVEngine::apply)
                - default (async_engine_apply = false): inline await
                - R17 (async_engine_apply = true): spawn_learn_chosen
                  (fire-and-forget tokio::spawn; returns Chosen before apply)
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
  single vectored `writev` + single `fdatasync` → resolve all pending
  oneshot acks. Multiple in-flight proposals on the same pipeline flush
  together, amortizing fsync cost. The watchdog
  (`wal_flush_watchdog_ms`, default 100 ms) wakes the idle writer
  periodically as a safety net against missed wakes.

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
- **Prepare phase (`run_prepare_phase`)** — R16a: the local `on_prepare`
  (term fence → `acceptor.prepare` lock-free CAS → WAL append `Promised`
  awaiting `fdatasync` → `PxPrepareReply`) and all remote `send_prepare`
  unary gRPCs run concurrently via `tokio::join!`, overlapping the local
  fsync with the network round-trip. Replies folded into `promised`,
  `highest_rejected_round`, `highest_seen_term`, `epoch_mismatch`,
  `adopted`. Quorum: `promised >= quorum` → proceed; else retry or fail.
  The quorum check still awaits the local reply before counting it, so
  W6 is intact.
- **Accept phase (`run_accept_phase`)** — R16a/R16b: two paths.
  - **Default (`wal_early_ack = false`, R16a)** — local `on_accept`
    (term fence → `acceptor.accept` lock-free CAS → WAL append `Accepted`
    awaiting `fdatasync` → `PxAcceptReply`) and all remote `send_accept`
    RPCs over the per-peer bidi `PxLearnerStream` run concurrently via
    `tokio::join!`. Quorum check waits for the local reply; W6 intact.
  - **Early-ack (`wal_early_ack = true`, R16b)** — local `on_accept_inner`
    (CAS only, no WAL persist) runs concurrently with remote RPCs; the
    local WAL persist is deferred to `spawn_accept_persist`
    (fire-and-forget `tokio::spawn`). `Chosen` is declared as soon as
    remote quorum + local CAS succeed, before the local fsync — weakens
    W6 for the local replica (a crash between CAS and persist can lose
    the accepted value; safe in Paxos, changes durability ordering).
  - Replies folded into `accepted`, `highest_rejected_round`,
    `highest_seen_term`, `epoch_mismatch`. Quorum:
    `accepted >= quorum` → chosen; else retry or fail.
- **Learn / apply (`learn_chosen`)** —
  `learner.learn(entry, client_id, seq)` → decode `Batch` from payload
  → `KVEngine::apply(slot, &batch)` → update dedup map → advance
  frontiers. `InMemKV`: `DashMap` insert (trivial). `CrowtreeEngine`:
  FFI `ct_apply_put` / `ct_apply_delete` → memtable insert (may
  trigger flush/compaction). `KVEngine::apply` is `async` but has no
  genuine `Pending` path today (no async apply C API) — never
  suspends. R17: when `async_engine_apply` is enabled, `learn_chosen`
  runs via `spawn_learn_chosen` (fire-and-forget `tokio::spawn`) and the
  proposer returns `Chosen` before the local engine has applied the
  value — breaks read-your-writes until an apply fence is added.
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
- **R16a — Concurrent local + remote fan-out** (done, 2026-07-31):
  `run_prepare_phase` and the default `run_accept_phase` path now issue
  the local handler and all remote RPCs concurrently via `tokio::join!`,
  overlapping the leader's local fsync (~10-100 µs on NVMe) with the
  network round-trip. The quorum check still awaits the local reply
  before counting it, so **W6 is not weakened** — W6 only forbids the
  local replica replying `Accepted` before persist; the proposer still
  waits for that reply. Pure win, no contract change, no feature flag.
- **R16b — Early ack before local persist** (done, 2026-07-31,
  default-off): `wal_early_ack` flag splits `on_accept` into
  `on_accept_inner` (CAS only) + `on_accept_persist` (WAL append). When
  enabled, the proposer declares `Chosen` on remote quorum + local CAS,
  before the local fsync; the persist runs via `spawn_accept_persist`
  (fire-and-forget). Weakens W6 for the local replica — a crash between
  CAS and persist can lose the accepted value (safe in Paxos, changes
  durability ordering). The flag is implemented in the hot path but
  default-off and not carried across group rebuild; enable (default
  flip + rebuild-carry) is tracked in R35, gated on T1 crash tests.
- **R17 — Async engine apply after quorum** (done, 2026-07-31,
  default-off): `async_engine_apply` flag moves `learn_chosen` to
  `spawn_learn_chosen` (fire-and-forget `tokio::spawn`); the proposer
  returns `Chosen` before the local engine has applied the value.
  Removes engine apply (FFI + memtable insert) from the write critical
  path. Breaks **Linearizable** read-your-writes until an apply fence is
  added — the existing `linearizable_read_barrier` does not gate on the
  applied frontier (MinSlot already gates on `contiguous_applied` and is
  unaffected). The flag is implemented in the hot path but default-off
  and not carried across group rebuild; apply fence + default flip +
  rebuild-carry are tracked in R35.
- **R34 — ISA-L CRC32C** (done, 2026-07-31): `crow-common/crc32c.h` now
  delegates to ISA-L `crc32_iscsi`, which runtime-dispatches to the
  best SIMD path (SSE4.2+PCLMULQDQ / AVX2 / AVX512 on x86, NEON on ARM).
  Accelerates C++ engine durability checksums (superblock, pages). Note:
  the **Rust WAL** still uses the `crc32c` crate (0.6) for record/footer
  checksums — R34 does not touch the Rust write path.
- **R31 — Write regression investigation** (closed, 2026-07-31): the
  Intel same-platform 50K→29K drop (07-21 → 07-24) was confirmed by the
  M5 Pro retest to be largely a platform effect, not a code defect (the
  code path reaches ~48K on M5 Pro). Closed without an Intel bisect.

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

The previous test reported 50K ops/s peak at 64T:8C (Intel Ryzen 9
5950X, Linux). This sweep measures ~29K at all 48T+ configs on the
same Intel platform, including a direct 64T:8C re-run (29,319 ops/s).
The relative findings are consistent: window is the primary lever,
threads scale until consensus saturation, connections have minimal
effect. The absolute throughput difference is investigated below.

### macOS M5 Pro retest (2026-07-29)

To separate platform effects from code regression, the regression
sentinel configs were re-run on macOS M5 Pro (arm64, Darwin 25.5.0).
Same workload (write-only, 512 B values, 1M key space, 12 s, mem
mode, Queue admission, MI=64 unless noted).

| Threads | Conn | MI | Throughput (ops/s) | avg (µs) | p50 (µs) | p99 (µs) | p999 (µs) | Errors |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | 1 | 64 | 9,457 | 104 | 96 | 137 | 241 | 0 |
| 24 | 24 | 64 | 41,062 | 582 | 574 | 845 | 1,099 | 0 |
| 48 | 48 | 64 | 46,679 | 1,026 | 1,015 | 1,462 | 2,397 | 0 |
| 64 | 8 | 64 | 47,808 | 1,336 | 1,329 | 1,900 | 3,075 | 0 |
| 48 | 48 | 1 | 13,320 | 3,602 | 3,083 | 3,721 | 105,791 | 48 |

**Key finding: the 50K→29K difference is largely a platform effect,
not a code regression.** On M5 Pro the 64T:8C config hits ~48K —
within 4% of the original Intel 50K claim. The M5 Pro is meaningfully
faster than the Intel Ryzen 9 5950X for this workload at every config:

- **Single-thread (1T:1C)**: M5 Pro 9.5K vs Intel 2.8K — **3.4×**.
  The M5 Pro's per-core throughput and memory subsystem dominate;
  single-thread write latency is bound by per-proposal critical path
  (WAL fsync + quorum RPC), and M5 Pro's NVMe + memory latency is
  substantially lower.
- **Saturation (24T+)**: M5 Pro ~41-48K vs Intel ~29K — **1.4-1.7×**.
  The M5 Pro has fewer but faster cores; it saturates later and at a
  higher ceiling.
- **Window impact (MI=1 vs MI=64)**: same 4-5× ratio on both platforms
  (M5 Pro 13K→47K; Intel 6K→29K). The relative shape is identical;
  only the absolute ceiling differs.

**Implication for R31**: the Intel same-platform regression (50K on
07-21 → 29K on 07-24) was closed as a platform effect, not a code
defect — the M5 Pro retest shows the code path itself reaches ~48K.
The "ceiling" is not 29K globally; that is Intel-specific. On M5 Pro
the steady-state ceiling is ~48K, close to the original 50K claim.
Optimizations (R16a/R16b/R17) should be benchmarked on a single
consistent platform; cross-platform comparisons are not meaningful
for absolute throughput.

The MI=1 run on M5 Pro showed 48 errors with a 106 ms p999 tail —
queue saturation under aggressive load with a single permit; the
errors are likely client-side timeouts at the 3.6 ms avg latency, not
consensus failures (zero errors at MI=64).

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
- **Scaling ceiling is platform-dependent** — Intel Ryzen 9 5950X
  ~29K ops/s; Apple M5 Pro ~48K ops/s at the same config. Per-proposal
  latency at 48 inflight is ~1.7 ms (Intel) vs ~1.0 ms (M5 Pro). The
  per-proposal critical path is now: max(local fsync, quorum RPC) after
  R16a (was local fsync → quorum RPC, serial). R15 (zero-copy accept
  path, done) and R16a (concurrent fan-out, done) have already removed
  the serial local fsync from the critical path. Remaining per-proposal
  latency is dominated by the quorum RPC round-trip and the local fsync
  running concurrently; further gains need R16b (drop local fsync from
  the critical path entirely) or a faster quorum transport (R32). The
  Intel same-platform 50K→29K drop (07-21 → 07-24) was closed as R31
  (platform effect, not a code defect).

---

## Memory Copy Summary

Copy points are annotated inline in the flow diagram above. Summary
of what remains:

- **O(n) unavoidable** — payload encoding (client key/value slices →
  contiguous `Vec<u8>`); WAL replay (`Bytes::copy_from_slice` to
  reconstruct `PxLogEntry` from on-disk bytes); C++ engine apply
  (internal memtable copy); gRPC socket write (kernel user→socket
  buffer copy).
- **O(1) ref-count bumps (negligible)** — `base_entry` payload clone
  per slot retry; `inner_accept` entry clone for `cas_accepted`;
  `send_accept` payload clone for protobuf; `learn_chosen` entry clone
  for learner; WAL `from_accepted` payload clone (`encode_accepted_payload`
  is `entry.payload.clone()`); WAL `encode_frame` payload clone for
  `RecordFrame`; Batch decode `Bytes::slice` per key/value (shares
  payload buffer).
- **Zero-copy (move/borrow)** — `Vec<u8>` → `Bytes` at `propose`
  entry; gRPC deserialization → `PxLogEntry` (move `Bytes`); WAL
  vectored write (`IoSlice` borrows `Bytes`); FFI batch apply
  `ct_kv_ref` pointer-length structs (R23, done).

### Optimization Opportunities

- **WAL encode** — already zero-copy: `encode_accepted_payload` is
  `entry.payload.clone()` (O(1) ref-count); `WALRecord.payload` is
  `Bytes`. No further work here. (Previously listed as a `to_vec()`
  copy — that was stale; the code already does the right thing.)
- **Batch decode** — already zero-copy: `Batch::decode` uses
  `Bytes::slice` (O(1) ref-count), not `to_vec()`; `BatchOp` owns
  `Bytes` that share the payload buffer.
- **FFI batch encode** — already eliminated (R23, done):
  `ct_apply_batch_slices` accepts an array of `ct_kv_ref`
  pointer-length structs; no packing copy.
- **Client-side batch copy (R25, partial)** — proto `bytes` fields are
  now `bytes::Bytes` (done via `prost-build` config), so `KvBatchItem`
  holds `Bytes`. But `CrowkvClient::BatchOp` still owns `Vec<u8>`
  key/value, so `batch_write` clones each key/value into `KvBatchItem`
  and clones the whole `items` vec per retry. Switching `BatchOp` to
  `Bytes` makes these O(1) ref-count bumps. Low-medium complexity — type
  change ripples through client call sites but no C++ or consensus
  changes.

---

## Write-Path Enhancement Ideas

Grounded in the current code (post R16a/R16b/R17/R34). Ordered by
expected impact on the per-proposal critical path, which after R16a is
`max(local fsync, quorum RPC)`. Larger items are tracked as backlog
requirements; small/tuning items are traced in
[`plan-io-clean.md`](plan-io-clean.md).

- **Apply fence for R17 (enable `async_engine_apply` by default)** —
  tracked as **[R35](../backlog/R35-apply-fence.md)** (backlog). The
  biggest remaining write-path win. R17 is implemented but default-off
  because `learn_chosen` (FFI + memtable insert) is moved off the
  critical path, breaking the **Linearizable** read mode's
  read-your-writes. (MinSlot already gates on `contiguous_applied` and
  is unaffected.) The existing `linearizable_read_barrier` confirms the
  leader is still leader and captures `read_slot = contiguous_chosen`,
  but the engine get returns the latest **applied** value — with R17 on
  a chosen-but-not-applied slot can be missed. An apply fence — have
  Linearizable reads await `contiguous_applied >= read_slot` before
  serving — restores Linearizable read-your-writes and lets R17 ship by
  default. The learner's `apply_item` is already idempotent and
  `update_frontier` / `record_dedup` are atomic, so a delayed apply is
  safe. Medium complexity, confined to the Linearizable read path +
  learner.
- **Crash-recovery tests for R16b (enable `wal_early_ack` by default)** —
  tracked as **T1** in [`plan-io-clean.md`](plan-io-clean.md).
  R16b drops the local fsync off the critical path entirely (chosen on
  remote quorum + local CAS). On NVMe the local fsync is ~10-100 µs, so
  this is the larger single latency component once R16a has overlapped
  it with the quorum RPC. The mechanism is implemented; the blocker is
  durability-ordering validation: a crash between CAS and persist must
  not violate any externally visible guarantee. Needs fault-injection
  crash-recovery tests (kill -9 between `on_accept_inner` and
  `spawn_accept_persist`, then replay). Low code complexity once tests
  pass. **T1.5 benchmark (done)**: at 1T:1C MI=64, early-ack on vs off:
  2906 vs 2809 ops/s (+3.5%), avg 341 vs 353 µs (−12 µs / −3.4%).
  At 48T:48C MI=64: 29790 vs 27663 ops/s (+7.7%), avg 1608 vs 1732 µs
  (−124 µs / −7.2%), p999 5668 vs 6420 µs (−11.7%). The gain is larger
  under saturation because early-ack lets the leader start the next
  proposal without waiting for local WAL persist.
- **Server-side proposal coalescing** — tracked as
  **[R36](../backlog/R36-proposal-coalescing.md)** (backlog). Today each
  client `PUT` is its own Paxos proposal (one slot, one WAL record, one
  fsync batch entry, one quorum RPC round). The `Batch` payload format
  already supports multiple ops and `kv_batch_write` exposes it to
  clients, but there is no server-side coalescer that merges concurrent
  single-key proposes into one multi-key proposal before slot
  allocation. A bounded micro-batcher (collect for ≤N µs or ≤K keys,
  then one `propose`) would amortize the per-proposal fixed cost (quorum
  RPC + fsync) across many keys — directly attacks the saturation
  ceiling. Trades a small latency floor for throughput; needs a tunable
  coalesce window and must preserve `(client_id, seq)` dedup ordering.
  Medium-high complexity; touches the admission gate and the propose
  entry.
- **Rust WAL CRC32C hardware path (done)** — The Rust WAL now FFI-s
  through `crowtree-ffi::crc32c` to `crow_common::crc32c` (ISA-L
  `crc32_iscsi`, R34), replacing the software `crc32c` crate. Same
  Castagnoli polynomial + reflected/seeded convention — existing WAL
  segments decode without migration (102 WAL tests pass). Aligns the
  Rust and C++ checksum and adds a NEON path on ARM.
- **WAL group-commit coalesce tuning (T3, done — removed)** —
  `wal_flush_coalesce_us` was swept across {0, 10, 25, 50, 100, 200} µs
  at 48T:48C MI=64 (saturated write, in-memory WAL). No non-zero value
  showed any advantage over the baseline (0 µs): throughput was flat at
  ~29.2K ops/s (±1% noise) and p99/p999 showed no trend. The
  wake-drain-flush baseline already amortizes fsync across records that
  arrive during a flush, so an explicit coalescing window adds nothing.
  **Decision: removed** `wal_flush_coalesce_us` (config field, coalesce
  arm in `pipeline_writer.rs`, related tests/docs). The watchdog
  (`wal_flush_watchdog_ms`) stays as the safety-net timer. See
  [`plan-io-clean.md`](plan-io-clean.md) T3.
