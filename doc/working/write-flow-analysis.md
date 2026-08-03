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
      1. Leadership gate (role == Leader && current_term == proposing_term)
         → NotLeader { leader_hint } on miss (drains in-flight proposals)
      2. Idempotency check (dedup_lookup by client_id + seq)
         → cached ProposeResult::Chosen { slot } on hit — no permit, no
           Paxos round, no batch entry (duplicates never consume a window
           permit)
      3. Branch on coalesce_max_keys > 0 && self_weak set:
         a. Coalescing ON (R45/R45b) → coalesce_enqueue(payload, tag)
            - Idle (no pending batch): start a 1-op round immediately
              (no timer); open a fresh pending batch for concurrent ops
            - Batch exists: append op_body + tag + oneshot waiter;
              if op_count >= coalesce_max_keys → flush the full batch as
              a concurrent round (max_keys overflow path)
            - Round runs in a detached tokio::spawn → propose_inner;
              each coalesced caller awaits its oneshot for the shared
              ProposeResult (one slot, one quorum round per batch)
            - coalesce_drain_after_round: after a round, if the pending
              batch is non-empty AND inflight.occupied() <
              coalesce_drain_threshold, flush it as the next round
              immediately (zero-latency-floor at low load); else go idle.
              Default threshold = 0 → always drain (R45b threshold off)
            - Watchdog (WATCHDOG_US = 1 s): single long-running task,
              flushes a stuck non-empty batch if no coalescer activity
              for 1 s (safety net against missed wakes)
         b. Coalescing OFF (coalesce_max_keys == 0, default) →
            Bytes::from(payload) [move: reuses allocation, zero copy]
            → propose_inner(payload, dedup_tags) directly (caller awaits)
  → propose_inner(payload, dedup_tags):  [one inflight permit held]
      1. Re-check leadership gate — a step-down between coalescer batch
         collection and flush surfaces as NotLeader instead of racing
         into Paxos with stale identity
      2. Inflight admission (InflightAdmission::acquire_permit().await)
         - Queue policy (default): blocks on semaphore until a permit
           is freed — eliminates Busy rejections and client retry storms
         - Reject policy (tests only): try_acquire, returns Busy if full
      3. Slot allocation (next_slot.fetch_add)
      4. 'slot_retry loop (max_slot_retries = 3)
         a. base_entry(slot, payload.clone())
            [O(1) ref-count: Bytes::clone per retry attempt]
         b. 'paxos_attempt loop (max_paxos_retries = 3)
            i.  [if force_prepare] run_prepare_phase (R16a: concurrent)
                - tokio::join!(local on_prepare, join_all(remote send_prepare))
                  local on_prepare: acceptor.prepare + WAL append Promised
                  remote: send_prepare RPCs (unary gRPC)
                - quorum check counts the local reply (W6 intact)
                - on TermStale → become_follower + return NotLeader
                - on MembershipEpochMismatch → adopt responder epoch,
                  retry same slot
            ii. run_accept_phase (R16a/R16b: concurrent, two paths)
                - guard: wal_early_ack && cached_quorum > 1
                  (single-node groups always use the strict R16a path —
                  no survivors to re-drive a chosen-but-not-durable slot
                  after a crash, so the persist must be synchronous)
                - remote (both paths): send_accept RPCs (join_all, bidi
                  LearnerStream)
                  [O(1) ref-count: Bytes::clone for AcceptRequest]
                  [copy: payload → socket buffer on gRPC serialize, unavoidable]
                  [move: follower gRPC deserialize → PxLogEntry.payload Bytes]
                - strict (wal_early_ack = false, R16a; test default):
                    tokio::join!(local on_accept, join_all(remote send_accept))
                    local on_accept = on_accept_inner (CAS) + on_accept_persist
                      (WAL append Accepted, awaits fdatasync)
                      [O(1) ref-count: PxLogEntry::clone for cas_accepted]
                      [no copy: encode_accepted_payload is entry.payload.clone()
                       (O(1) ref-count); WALRecord.payload is Bytes]
                      [no copy: IoSlice borrows Bytes for vectored writev]
                    quorum check waits for the local reply (W6 intact)
                - early-ack (wal_early_ack = true, R16b; production default):
                    tokio::join!(on_accept_inner (CAS only), join_all(remote))
                    local WAL persist deferred to spawn_accept_persist
                      (fire-and-forget tokio::spawn; best-effort durability)
                    chosen declared on remote quorum + local CAS, before fsync
                      (weakens W6 for the local replica)
            iii. quorum check
            iv. [if chosen] learn_chosen (decode + KVEngine::apply)
                - sync (async_engine_apply = false; test default):
                  learn_chosen → apply_entry + advance both frontiers +
                  record dedup (inline await; contiguous_applied tracks
                  contiguous_chosen exactly, R35 fence is a no-op fast path)
                - async (async_engine_apply = true; production default, R17):
                  spawn_learn_chosen — sync update_chosen_frontier +
                  record_dedup_tags (MUST precede Chosen return so a
                  subsequent Linearizable read's read_slot reflects this
                  slot), then spawn apply_entry + advance_applied_frontier
                  (fire-and-forget tokio::spawn)
                  [R35 apply fence: Linearizable reads await_applied(slot)
                   before serving — restores read-your-writes; learner
                   apply_entry is idempotent so a delayed apply is safe]
                [O(1) ref-count: PxLogEntry::clone for learner]
                [O(1) ref-count: Batch::decode uses Bytes::slice per key/value]
                [no copy: FFI ct_apply_batch_slices takes ct_kv_ref pointers
                 into caller's Bytes slices (R23, done)]
                [copy: C++ engine copies key/value into internal memtable,
                 unavoidable]
            v.  [if chosen] foreign-value check: if adopted_foreign_value
                or entry.payload != payload → retry client value on a
                fresh next slot (continue 'slot_retry)
            vi. [if chosen] fan_out_chosen_notice (fire-and-forget mpsc)
            vii.[if chosen] return ProposeResult::Chosen { slot }
      5. [if all retries exhausted] return ProposeResult::Err
  → KvResponse::ok_chosen(slot, ...) or error
```

---

## Multi-Slot Concurrency and Component Design

The flow above traces a single proposal. The multi-slot concurrency
model (sliding-window admission, per-slot independence, background gap
repair, learner-stream window, WAL batch aggregation) and the per-
component design rationale (coalescer, prepare/accept phase fan-out,
learn/apply sync vs async, chosen notice) are covered in the design
docs:

- [`design-slot.md`](../design/design-slot.md) — parallel slots,
  sliding window and backpressure (§4), pipelined fanout (§5), gap
  repair (§9), tunables and defaults (§12), performance model (§21),
  server-side proposal coalescing R36 → R45/R45b (§23).
- [`design-wal.md`](../design/design-wal.md) — write path and batched
  durable flush (§4), ack contract and failure modes (§5, including
  the `wal_early_ack` early-ack mode), tunables and defaults (§9).

---

## Benchmark Results — 2026-07-24

Systematic T:C:W sweep. 3-node cluster (bench fixture, in-process
console-web + 3 spawned `crowkv-server` processes), in-memory WAL +
in-memory KV (mem-block), write-only, 512-byte values, 1M key space,
12-second duration, `election_profile = e2e`, admission policy =
`Queue` (R18 default). Platform: AMD Ryzen 9 5950X (16 cores / 32
threads), Linux. 28 runs total, zero errors across all configs.

Regression sentinel: `tools/bench-write-regression.sh`.

### Phase 1 — Baseline 1T:1C scaling (MI=64)

| Threads | Conn | Throughput (ops/s) | avg (µs) | p50 (µs) | p99 (µs) | p999 (µs) | Errors |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | 1 | 3,249 | 305 | 339 | 444 | 562 | 0 |
| 6 | 6 | 19,922 | 299 | 288 | 475 | 984 | 0 |
| 12 | 12 | 25,802 | 462 | 449 | 740 | 1,684 | 0 |
| 24 | 24 | 28,761 | 832 | 820 | 1,264 | 2,751 | 0 |
| 48 | 48 | 28,898 | 1,658 | 1,643 | 2,419 | 5,399 | 0 |

Throughput plateaus at 24T (~29K); beyond 24T adds latency without
throughput gain (consensus pipeline saturated).

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

**T:C ratio has zero effect on write throughput** (12T: C=3 and C=48
both ~25K; 48T: C=12 and C=64 both ~29K). Unlike reads (where the
HTTP/2 connection lock makes T:C ratio critical), writes are
bottlenecked by server-side consensus, not gRPC framing.

### Phase 3 — Window impact at 48T:48C

| Window | Throughput (ops/s) | avg (µs) | p50 (µs) | p99 (µs) | p999 (µs) | Errors |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | 6,361 | 7,544 | 6,499 | 10,455 | 10,687 | 0 |
| 4 | 20,776 | 2,308 | 2,253 | 3,215 | 8,295 | 0 |
| 16 | 28,040 | 1,709 | 1,679 | 2,467 | 5,963 | 0 |
| 32 | 28,827 | 1,662 | 1,644 | 2,481 | 5,563 | 0 |
| 64 | 28,920 | 1,657 | 1,638 | 2,509 | 5,067 | 0 |

**Window is the primary TPS lever** — MI=1→16 gives 4.4× (6K→28K).
MI=16+ converges (consensus critical path is the hard ceiling). See
[`design-slot.md` §4](../design/design-slot.md#4-sliding-window-and-backpressure)
for the sliding-window/backpressure design.

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

At 1T, C has no effect (~2.8K, bounded by per-proposal latency ~350µs).
At 2T, more connections help (6.3K→8.9K) — 2 threads sharing 1
connection contend on the h2 lock. At 3T, 3C and 6C converge (~12.8K).

### macOS M5 Pro retest (2026-07-29)

Same workload, macOS M5 Pro (arm64, Darwin 25.5.0), MI=64 unless noted.
Re-run to separate platform effects from code regression after the
Intel 50K→29K drop (07-21 → 07-24).

| Threads | Conn | MI | Throughput (ops/s) | avg (µs) | p50 (µs) | p99 (µs) | p999 (µs) | Errors |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | 1 | 64 | 9,457 | 104 | 96 | 137 | 241 | 0 |
| 24 | 24 | 64 | 41,062 | 582 | 574 | 845 | 1,099 | 0 |
| 48 | 48 | 64 | 46,679 | 1,026 | 1,015 | 1,462 | 2,397 | 0 |
| 64 | 8 | 64 | 47,808 | 1,336 | 1,329 | 1,900 | 3,075 | 0 |
| 48 | 48 | 1 | 13,320 | 3,602 | 3,083 | 3,721 | 105,791 | 48 |

**The 50K→29K difference is a platform effect, not a code regression**
(R31). M5 Pro 64T:8C hits ~48K, within 4% of the original Intel 50K
claim. M5 Pro is faster at every config: single-thread 3.4× (9.5K vs
2.8K), saturation 1.4-1.7× (~41-48K vs ~29K). The window-impact shape
is identical across platforms (MI=1→64: 4-5× on both). The MI=1 run
showed 48 errors with a 106 ms p999 tail — client-side timeouts under
single-permit queue saturation, not consensus failures.

### Conclusions

- **Window is the primary TPS lever** — MI=1→16 gives 4.4× (6K→28K at
  48T); MI=16+ converges.
- **Threads scale until 24T, then plateau** — 1T→24T gives 10×
  (3K→29K); 24T→48T adds latency only.
- **T:C ratio has zero effect on writes** — the write bottleneck is
  server-side consensus, not gRPC framing (the key difference from
  reads).
- **Queue mode: zero errors across all 28 configurations** — no `Busy`
  rejections; queue naturally backpressures at any window size.
- **Scaling ceiling is platform-dependent** — Intel ~29K, M5 Pro ~48K
  at the same config. Per-proposal latency at 48 inflight: ~1.7 ms
  (Intel) vs ~1.0 ms (M5 Pro). After R16b (early-ack) + R17 (async
  apply, R35-fenced), the per-proposal critical path is the quorum RPC
  round-trip only; further gains need a faster quorum transport (R32).

### Early-ack A/B (`wal_early_ack` on vs off)

Same workload, Linux (AMD Ryzen 9 5950X), MI=64. Compares the relaxed
ack mode against the strict mode (design:
[`design-wal.md` §5`](../design/design-wal.md#5-ack-contract-and-failure-modes)).

| Config | Mode | Throughput (ops/s) | avg (µs) | p50 (µs) | p99 (µs) | p999 (µs) | Errors |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1T:1C | early-ack on | 2,906 | 341 | 355 | 441 | 568 | 0 |
| 1T:1C | early-ack off | 2,809 | 353 | 360 | 443 | 577 | 0 |
| 48T:48C | early-ack on | 29,790 | 1,608 | 1,590 | 2,354 | 5,668 | 0 |
| 48T:48C | early-ack off | 27,663 | 1,732 | 1,585 | 2,206 | 6,420 | 0 |

1T:1C +3.5% throughput, −3.4% avg latency. 48T:48C +7.7% throughput,
−7.2% avg latency, −11.7% p999 (p99 roughly flat — the deferred persist
shifts some tail mass from p999 into p99). The gain is the saturation-
ceiling lift from removing the leader's fsync from the bottleneck path.

### WAL flush coalesce sweep (`wal_flush_coalesce_us`)

48T:48C, MI=64, Linux. Sweeps an explicit wait window the flush worker
would insert before draining (on top of the wake-drain-flush baseline;
design:
[`design-wal.md` §4`](../design/design-wal.md#4-write-path-and-batched-durable-flush)).

| coalesce (µs) | Throughput (ops/s) | avg (µs) | p50 (µs) | p99 (µs) | p999 (µs) | Errors |
| --- | --- | --- | --- | --- | --- | --- |
| 0 | 29,266 | 1,637 | 1,624 | 2,364 | 4,928 | 0 |
| 10 | 29,362 | 1,632 | 1,618 | 2,370 | 5,212 | 0 |
| 25 | 29,157 | 1,643 | 1,630 | 2,382 | 4,444 | 0 |
| 50 | 29,332 | 1,633 | 1,619 | 2,344 | 5,420 | 0 |
| 100 | 29,241 | 1,639 | 1,622 | 2,466 | 4,700 | 0 |
| 200 | 29,452 | 1,627 | 1,614 | 2,376 | 4,872 | 0 |

Throughput flat at ~29.2K (±1% noise); no non-zero value beat the
wake-drain-flush baseline (coalesce = 0). **Decision: removed** —
`wal_flush_coalesce_us` and the coalesce arm in `pipeline_writer.rs`
were deleted; `wal_flush_watchdog_ms` stays as the safety-net timer.

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
- **Zero-copy (move/borrow)** — `Vec<u8>` → `Bytes` at `propose_inner`
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

Grounded in the current code (post R16a/R16b/R17/R23/R25/R34/R35/R45b).
Ordered by expected impact on the per-proposal critical path, which after
R16b (early-ack, production default-on) and R17 (async engine apply,
production default-on, gated by the R35 apply fence) is the quorum RPC
round-trip only — the leader's local fsync and engine apply both run
off the critical path. Larger items are tracked as backlog requirements.

- **Apply fence for R17 (R35, done)** — `async_engine_apply` is now
  production default-on. R17 moves `learn_chosen`'s engine apply
  (FFI + memtable insert) off the write critical path via
  `spawn_learn_chosen`: chosen frontier + dedup advance synchronously
  (so a subsequent Linearizable read's `read_slot` reflects the slot),
  then `apply_entry` + `advance_applied_frontier` spawn. The **R35 apply
  fence** (`PxLearner::await_applied`) has Linearizable reads await
  `contiguous_applied >= read_slot` before serving, restoring
  read-your-writes; `apply_entry` is idempotent and the frontier/dedup
  updates are atomic, so a delayed apply is safe. (MinSlot reads already
  gate on `contiguous_applied` and are unaffected.) Test profiles
  (`for_tests`, `PxGroup::new`) opt back out for determinism.
- **Server-side proposal coalescing (R36 → R45/R45b, done)** —
  implemented. R36 used a timer-based collect-then-flush; R45 replaced
  it with event-driven immediate flush + drain after round; R45b added
  a drain threshold (`coalesce_drain_threshold`, default `0` = always
  drain) that, when set above 0, skips the drain at high load so the
  `max_keys` overflow path produces full batches. See
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
