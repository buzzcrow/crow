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

- **Inflight admission (R18)** — `InflightAdmission` gate backed by a
  single `tokio::sync::Semaphore` of `max_inflight_proposals` permits
  (default 32). Each `propose`
  acquires one permit before slot allocation, holds it for the entire
  proposal duration (released on drop at every return path).
  - **Queue policy (default)** — `acquire_permit().await` blocks until
    a permit is freed; no `Busy` rejections, no client retry;
    semaphore fast path is lock-free (atomic CAS).
  - **Reject policy (tests only)** — `try_acquire`, returns
    `ProposeResult::Busy` if full.
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

## Benchmark Results — 2026-07-24

Systematic T:C:W sweep. 3-node cluster (bench fixture, in-process
console-web + 3 spawned `crowkv-server` processes), in-memory WAL +
in-memory KV (mem-block), write-only, 512-byte values, 1M key space,
12-second duration, `election_profile = e2e`, admission policy =
`Queue` (R18 default). Platform: AMD Ryzen 9 5950X (16 cores / 32
threads), Linux. 28 runs total, zero errors across all configs.

Regression sentinel: `tools/bench-write-regression.sh`.

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
  the serial local fsync from the critical path. R16b (early-ack,
  `wal_early_ack` default-on) then dropped the leader's local fsync from
  the critical path entirely — the per-proposal path is now the quorum
  RPC round-trip only (see the Early-ack A/B section below for the
  measured lift). Remaining per-proposal latency is dominated by the
  quorum RPC; further gains need a faster quorum transport (R32). The
  Intel same-platform 50K→29K drop (07-21 → 07-24) was closed as R31
  (platform effect, not a code defect).

### Early-ack A/B (`wal_early_ack` on vs off)

Same workload as the main sweep (write-only, 512 B values, 1M key space,
12 s, mem mode, Queue admission, MI=64), Linux (AMD Ryzen 9 5950X).
Compares the relaxed ack mode (§5.4 of `design-wal.md`: `Chosen` declared
on remote quorum durable flush + leader CAS, leader's local WAL persist
deferred to a background spawn) against the strict mode (leader's local
fsync on the critical path).

| Config | Mode | Throughput (ops/s) | avg (µs) | p50 (µs) | p99 (µs) | p999 (µs) | Errors |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1T:1C | early-ack on | 2,906 | 341 | 355 | 441 | 568 | 0 |
| 1T:1C | early-ack off | 2,809 | 353 | 360 | 443 | 577 | 0 |
| 48T:48C | early-ack on | 29,790 | 1,608 | 1,590 | 2,354 | 5,668 | 0 |
| 48T:48C | early-ack off | 27,663 | 1,732 | 1,585 | 2,206 | 6,420 | 0 |

- **1T:1C** — +3.5% throughput (2,809 → 2,906), −3.4% avg latency
  (353 → 341 µs). Single-proposal critical path drops the local fsync,
  which is the larger single component once R16a overlapped it with the
  quorum RPC.
- **48T:48C** — +7.7% throughput (27,663 → 29,790), −7.2% avg latency
  (1,732 → 1,608 µs), −11.7% p999 (6,420 → 5,668 µs). p99 is roughly
  flat-to-slightly-up (2,206 → 2,354 µs, +6.7%) — the deferred persist
  shifts some tail mass from p999 into p99 by adding a small amount of
  background-persist contention, but the net tail (p999) and the average
  both improve. The throughput gain is the saturation-ceiling lift from
  removing the leader's fsync from the bottleneck path.

### WAL flush coalesce sweep (`wal_flush_coalesce_us`)

Sweeps the coalesce budget at the saturated write config (48T:48C, MI=64),
Linux (AMD Ryzen 9 5950X). The coalesce budget was an explicit wait window
the flush worker would insert before draining, on top of the
wake-drain-flush baseline.

| coalesce (µs) | Throughput (ops/s) | avg (µs) | p50 (µs) | p99 (µs) | p999 (µs) | Errors |
| --- | --- | --- | --- | --- | --- | --- |
| 0 | 29,266 | 1,637 | 1,624 | 2,364 | 4,928 | 0 |
| 10 | 29,362 | 1,632 | 1,618 | 2,370 | 5,212 | 0 |
| 25 | 29,157 | 1,643 | 1,630 | 2,382 | 4,444 | 0 |
| 50 | 29,332 | 1,633 | 1,619 | 2,344 | 5,420 | 0 |
| 100 | 29,241 | 1,639 | 1,622 | 2,466 | 4,700 | 0 |
| 200 | 29,452 | 1,627 | 1,614 | 2,376 | 4,872 | 0 |

Throughput is flat at ~29.2K ops/s (±1% noise) across the whole range;
p99/p999 show no trend. No non-zero value showed any advantage over the
wake-drain-flush baseline (coalesce = 0): the baseline already amortizes
fsync across records that arrive during a flush, so an explicit wait
window adds no measurable gain on top of it. **Decision: removed.** The
`wal_flush_coalesce_us` config field and the coalesce arm in
`pipeline_writer.rs` were deleted; `wal_flush_watchdog_ms` stays as the
safety-net timer for the wake-drain-flush path (it wakes the idle writer
every `watchdog` ms to drain any queued record in case of a missed wake).

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
- **Client-side batch copy (R25, done)** — proto `bytes` fields are
  `bytes::Bytes` (via `prost-build` config), and `CrowkvClient::BatchOp`
  also holds `Bytes` key/value. `batch_write`'s `key.clone()` /
  `value.clone()` into `KvBatchItem` and the `items.clone()` per retry
  are all O(1) ref-count bumps, not copies. No further work here.

---

## Write-Path Enhancement Ideas

Grounded in the current code (post R16a/R16b/R17/R34). Ordered by
expected impact on the per-proposal critical path, which after R16b
(early-ack, default-on) is the quorum RPC round-trip only — the leader's
local fsync runs concurrently off the critical path. Larger items are
tracked as backlog requirements.

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
- **Server-side proposal coalescing (R36 → R45/R45b, done)** —
  implemented. R36 used a timer-based collect-then-flush; R45 replaced
  it with event-driven immediate flush + drain after round; R45b added
  a drain threshold (`coalesce_drain_threshold`, default `1`) that
  skips the drain at high load so the `max_keys` overflow path produces
  full batches. See
  [`design-slot.md` §23](../design/design-slot.md#23-server-side-proposal-coalescing-r36--r45r45b)
  for the full design. Benchmark results (10s mem mode, 3-node cluster,
  max_keys=32, connections=32):

  Standard bench command:
  `crowkv-cli bench run --mode mem --workload write --duration-secs 10 --threads {T} --connections 32 --coalesce-max-keys 32 [--coalesce-drain-threshold {N}]`

  | Threads | Baseline TPS | R36 TPS | R45b TPS | R36 WAL | R45b WAL |
  |---|---|---|---|---|---|
  | 32 | 27,787 | 33,029 | 47,485 | 31,090 | 139,404 |
  | 64 | 28,062 | 64,145 | 68,741 | 60,498 | 106,926 |
  | 128 | 28,260 | 97,554 | 101,537 | 92,752 | 101,350 |
  | 256 | 27,804 | 113,671 | 118,377 | 110,034 | 111,944 |

  R45b beats R36 at high load (128: 102K vs 98K, 256: 118K vs 114K)
  with no low-load regression (32 threads: 47K, matching event mode).
  The drain threshold eliminates the 1-op round fragmentation that
  caused R45 event mode's high-load gap (WAL 425K → 101K at 128
  threads).

  Coalescer race fix: `coalesce_flush_batch` previously did
  unconditional `replace(new_batch)`, overwriting batches created by
  ops arriving between drain's `take()` and flush's `replace()`. This
  dropped oneshot senders ("coalescer round dropped" errors, ~80 per
  10s at 256t/c32). Fix: only set new batch when coalescer is still
  `None`. After fix: zero drops, TPS unchanged.
- **Fan-out hardening (quorum short-circuit, RPC deadline, phase
  metrics)** — tracked as
  **[R43](../backlog/R43-write-path-fanout-hardening.md)** (backlog).
  Six items from the 2026-08 write-flow review: (1) both phases
  `join_all` ALL remote replies, so per-proposal latency is
  `max(all peers)` instead of the quorum-th fastest — a
  `FuturesUnordered` fold that returns on quorum + local reply (W6
  intact) with a detached straggler drain (preserving late
  TermStale/EpochMismatch side effects) is the largest remaining
  latency lever needing no new transport; (2) accept/heartbeat
  oneshots have no deadline, so a hung-but-connected peer stalls all
  writes indefinitely even with quorum reachable; (3) `MetricHandles`
  has read-path summaries only — no propose-e2e / prepare / accept /
  first-quorum-RPC / apply latency breakdown (the critical-path
  analysis above is inferred, not measured); (4) `retry_backoff` has
  no jitter and sleeps while holding the admission permit; (5)
  heartbeats share the 64-frame LearnerStream mpsc with accepts and
  can be `Busy`-rejected at peak write load, degrading lease/election
  stability; (6) the reply-fold `match` is triplicated (~150 lines)
  across prepare + both accept paths — extract a helper first to
  de-risk (1).
