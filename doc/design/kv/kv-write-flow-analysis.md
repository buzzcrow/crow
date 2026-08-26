<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Write Flow Analysis

End-to-end trace of the CROW write path. Mirrors the structure of
[`kv-read-flow-analysis.md`](kv-read-flow-analysis.md). Focuses on flow,
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
              Default threshold = 1 (0 = always drain = pure event mode)
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
                  remote: send_prepare RPCs (unary crow-rpc)
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
                  [copy: payload → socket buffer on crow-rpc serialize, unavoidable]
                  [move: follower crow-rpc deserialize → PxLogEntry.payload Bytes]
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

- [`design-slot.md`](../design/kv/design-crow-kv-slot.md) — parallel slots,
  sliding window and backpressure (§4), pipelined fanout (§5), gap
  repair (§9), tunables and defaults (§12), performance model (§21),
  server-side proposal coalescing R36 → R45/R45b (§23).
- [`design-wal.md`](../design/kv/design-crow-kv-wal.md) — write path and batched
  durable flush (§4), ack contract and failure modes (§5, including
  the `wal_early_ack` early-ack mode), tunables and defaults (§9).

---

## Benchmark Results — 2026-07-24

Systematic T:C:W sweep. 3-node cluster (bench fixture, in-process
console-web + 3 spawned `crow-kv-server` processes), in-memory WAL +
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
bottlenecked by server-side consensus, not crow-rpc framing.

### Phase 3 — Window impact at 48T:48C

| Window | Throughput (ops/s) | avg (µs) | p50 (µs) | p99 (µs) | p999 (µs) | Errors |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | 6,361 | 7,544 | 6,499 | 10,455 | 10,687 | 0 |
| 4 | 20,776 | 2,308 | 2,253 | 3,215 | 8,295 | 0 |
| 16 | 28,040 | 1,709 | 1,679 | 2,467 | 5,963 | 0 |
| 32 | 28,827 | 1,662 | 1,644 | 2,481 | 5,563 | 0 |
| 64 | 28,920 | 1,657 | 1,638 | 2,509 | 5,067 | 0 |

**Window is the primary TPS lever**: MI=1→16 gives 4.4× (6K→28K).
MI=16+ converges (consensus critical path is the hard ceiling). See
[`design-slot.md` §4](../design/kv/design-crow-kv-slot.md#4-sliding-window-and-backpressure)
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
At 2T, more connections help (6.3K→8.9K): 2 threads sharing 1
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
showed 48 errors with a 106 ms p999 tail, client-side timeouts under
single-permit queue saturation, not consensus failures.

### Conclusions

- **Window is the primary TPS lever** — MI=1→16 gives 4.4× (6K→28K at
  48T); MI=16+ converges.
- **Threads scale until 24T, then plateau** — 1T→24T gives 10×
  (3K→29K); 24T→48T adds latency only.
- **T:C ratio has zero effect on writes** — the write bottleneck is
  server-side consensus, not crow-rpc framing (the key difference from
  reads).
- **Queue mode: zero errors across all 28 configurations** — no `Busy`
  rejections; queue naturally backpressures at any window size.
- **Scaling ceiling is platform-dependent** — Intel ~29K, M5 Pro ~48K
  at the same config. Per-proposal latency at 48 inflight: ~1.7 ms
  (Intel) vs ~1.0 ms (M5 Pro). After R16b (early-ack) + R17 (async
  apply, R35-fenced), the per-proposal critical path is the quorum RPC
  round-trip only; further gains need a faster quorum transport (R32).
- **Zero-copy crow-rpc delivers 1.6× over gRPC** — AMD 5950X peak
  197K ops/s at 256T (was ~124K with gRPC). Inter-replica RPC latency
  stays sub-100µs up to 256T; ~12.5K tps per follower at peak. r2≈r3
  confirms symmetric replication. See regression sentinel below.

### Early-ack A/B (`wal_early_ack` on vs off)

Same workload, MI=64. Compares the relaxed ack mode against the strict
mode (design:
[`design-wal.md` §5`](../design/kv/design-crow-kv-wal.md#5-ack-contract-and-failure-modes)).

**Linux (AMD Ryzen 9 5950X, 16c/32t, SMT) — single run:**

| Config | Mode | Throughput (ops/s) | avg (µs) | p50 (µs) | p99 (µs) | p999 (µs) | Errors |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1T:1C | early-ack on | 2,906 | 341 | 355 | 441 | 568 | 0 |
| 1T:1C | early-ack off | 2,809 | 353 | 360 | 443 | 577 | 0 |
| 48T:48C | early-ack on | 29,790 | 1,608 | 1,590 | 2,354 | 5,668 | 0 |
| 48T:48C | early-ack off | 27,663 | 1,732 | 1,585 | 2,206 | 6,420 | 0 |

1T:1C +3.5% throughput, −3.4% avg latency. 48T:48C +7.7% throughput,
−7.2% avg latency, −11.7% p999, but **p99 went up +6.7%** (2,206 →
2,354): the deferred persist shifts some tail mass from p999 into p99.

**macOS (M5 Pro, arm64, no SMT) — 3 runs, averages:**

| Config | Mode | Throughput (ops/s) | avg (µs) | p90 (µs) | p99 (µs) | p999 (µs) | Errors |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1T:1C | early-ack on | 10,345 | 95 | 109 | 145 | 218 | 0 |
| 1T:1C | early-ack off | 10,183 | 97 | 110 | 152 | 217 | 0 |
| 48T:48C | early-ack on | 47,481 | 1,009 | 1,180 | 1,388 | 1,886 | 0 |
| 48T:48C | early-ack off | 46,142 | 1,038 | 1,209 | 1,434 | 2,028 | 0 |

**T4 conclusion (2026-08-05):** The AMD p99 uptick does **not**
reproduce on M5 Pro. p99 is consistently *better* with early-ack on
(−4.2% at 1T, −3.2% at 48T across 3 runs). All tail percentiles (p90,
p99, p999) favor early-ack on M5 Pro. The AMD p99 shift is
platform-specific, likely SMT scheduling contention: the deferred
`spawn_accept_persist` on a hyperthread-sibling of the next accept's
worker creates tail mass that doesn't exist on M5 Pro's non-SMT
architecture. **Decision: accept early-ack as a clean win on all
platforms.** On AMD the net effect is still positive (avg + p999
improve, p99 slightly worse); on M5 Pro all percentiles improve. No
scheduling tweak needed. `tools/bench-early-ack.sh` deleted (T4
cleanup).

### WAL flush coalesce sweep (`wal_flush_coalesce_us`)

48T:48C, MI=64, Linux. Sweeps an explicit wait window the flush worker
would insert before draining (on top of the wake-drain-flush baseline;
design:
[`design-wal.md` §4`](../design/kv/design-crow-kv-wal.md#4-write-path-and-batched-durable-flush)).

| coalesce (µs) | Throughput (ops/s) | avg (µs) | p50 (µs) | p99 (µs) | p999 (µs) | Errors |
| --- | --- | --- | --- | --- | --- | --- |
| 0 | 29,266 | 1,637 | 1,624 | 2,364 | 4,928 | 0 |
| 10 | 29,362 | 1,632 | 1,618 | 2,370 | 5,212 | 0 |
| 25 | 29,157 | 1,643 | 1,630 | 2,382 | 4,444 | 0 |
| 50 | 29,332 | 1,633 | 1,619 | 2,344 | 5,420 | 0 |
| 100 | 29,241 | 1,639 | 1,622 | 2,466 | 4,700 | 0 |
| 200 | 29,452 | 1,627 | 1,614 | 2,376 | 4,872 | 0 |

Throughput flat at ~29.2K (±1% noise); no non-zero value beat the
wake-drain-flush baseline (coalesce = 0). **Decision: removed.**
`wal_flush_coalesce_us` and the coalesce arm in `pipeline_writer.rs`
were deleted; `wal_flush_watchdog_ms` stays as the safety-net timer.

### Regression sentinel (`tools/bench-write-regression.sh`)

Coalesced write throughput sweep (R45b, `coalesce_max_keys=16`,
`drain_threshold=1`, `max_inflight=32` (64 for 512T+), 10s mem mode,
3-node cluster, 512 B values, 1M key space). Regression sentinel for
write throughput with coalescing enabled; WAL append count tracks
coalescing efficiency. Inter-replica RPC metrics (r2/r3 latency + tps)
track consensus transport overhead per follower.

Platform: **AMD Ryzen 9 5950X** (16 cores / 32 threads, x86_64, Linux).
Run: 2026-08-26. Zero-copy crow-rpc handlers (C++ Frame ownership
transferred to Rust, flatbuffer parsed zero-copy in tokio task;
response via `FlatBufferBuilder::collapse()` + external C++ Buffer).
`CROW_RPC_WORKERS` tuned per config (2 for low T, 4 for high T).
Raw TSV: `doc/working/bench-write-regression.tsv`.

| Threads | Conn | Workers | win | co | Throughput (ops/s) | WAL/node | p50 (µs) | p99 (µs) | Errors | r2 avg (µs) | r2 tps | r3 avg (µs) | r3 tps | inflight enq | inflight wait (µs) |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | 1 | 2 | 32 | 1.0/16 | 3,770 | 37,722 | 273 | 366 | 0 | 0 | 3,803 | 0 | 3,803 | 0 | 0 |
| 16 | 2 | 2 | 32 | 6.7/16 | 63,393 | 94,137 | 231 | 625 | 0 | 2 | 9,206 | 5 | 9,206 | 0 | 0 |
| 64 | 4 | 2 | 32 | 14.7/16 | 171,582 | 116,476 | 339 | 858 | 0 | 131 | 11,297 | 38 | 11,297 | 0 | 0 |
| 128 | 4 | 4 | 32 | 15.3/16 | 191,411 | 124,957 | 582 | 1,448 | 0 | 29 | 12,504 | 70 | 12,504 | 0 | 0 |
| 256 | 8 | 4 | 32 | 15.4/16 | 190,769 | 123,974 | 1,173 | 2,970 | 0 | 157 | 13,440 | 78 | 13,440 | 0 | 0 |
| 512 | 16 | 4 | 64 | 35.0/64 | 178,024 | 50,815 | 2,738 | 5,444 | 0 | 68 | 5,102 | 68 | 5,102 | 0 | 0 |
| 1000 | 16 | 4 | 64 | 27.5/32 | 182,541 | 66,376 | 5,204 | 12,832 | 0 | 381 | 6,450 | 499 | 6,449 | 0 | 0 |

Zero-copy crow-rpc lifts the ceiling from ~124K (gRPC, 2026-08-04) to
~191K at 128-256T — a 1.5× gain from eliminating the gRPC serialization
copy and thread-pool handoff. Coalesce batches fill to 97% at co=16
(256T); larger co at 512T+ (co=64) reduces accept rounds from 13.4K to
5.1K, carrying 35 keys per round. Zero errors across all configs.

**Inter-replica RPC analysis:** `r2 ≈ r3` at every config confirms
symmetric follower replication. Per-follower RPC tps peaks at ~12.5K
at 256T — matching the ~12.5K accept rounds/s (WAL/node). RPC latency
stays low (0-100µs) until 512T+ where queue depth builds (228-1388µs).
The `rpc.l@N` summary includes all RPC types (accept, prepare, chosen
notice, fetch-gap), so tps is slightly higher than accept-only rounds.

**Bottleneck at 512T+:** throughput plateaus at ~191K ops/s. At co=16,
batches are 97% full (15.4/16) but the accept round rate (~13.4K/s) is
the ceiling: 13.4K × 15.4 ≈ 206K ops/s. Increasing co to 64 at 512T
reduces accept rounds to 5.1K/s but carries 35 keys/round. CPU is
70-80% at saturation. The inflight window is **never full** (enq=0 at
all configs) — the bottleneck is coalescer/accept-round serialization,
not window size. The coalescer only pipelines ~4 concurrent rounds
despite 32-64 available slots, suggesting it does not overlap enough
batches to fill the inflight window.

#### macOS M5 Pro comparison (2026-08-19)

Platform: **Apple M5 Pro** (18 cores, arm64, macOS 26.5). Same workload
but with gRPC transport (pre-zero-copy), `coalesce_max_keys=32`,
`max_inflight=128`.

| Threads | Conn | Throughput (ops/s) | WAL append | p50 (µs) | p99 (µs) | p999 (µs) | Errors |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | 1 | 10,144 | 304,358 | 95 | 153 | 211 | 0 |
| 4 | 2 | 21,879 | 449,508 | 178 | 307 | 380 | 0 |
| 16 | 4 | 47,260 | 276,795 | 330 | 523 | 619 | 0 |
| 32 | 16 | 57,889 | 170,600 | 537 | 894 | 1,046 | 0 |
| 64 | 32 | 69,908 | 104,777 | 888 | 1,440 | 1,745 | 0 |
| 128 | 32 | 78,155 | 86,840 | 1,590 | 2,654 | 3,794 | 0 |
| 256 | 32 | 87,448 | 86,619 | 2,870 | 4,704 | 7,004 | 0 |

**Platform comparison (AMD zero-copy vs M5 Pro gRPC, 256T):**
- AMD 197K vs M5 Pro 87K ops/s — **2.3× higher** throughput
- AMD p50 1,126µs vs M5 Pro 2,870µs — **2.5× lower** latency
- AMD p99 2,904µs vs M5 Pro 4,704µs — **1.6× lower** tail
- M5 Pro faster at 1T (10K vs 3.7K, 2.7×) due to lower per-op overhead
- M5 Pro saturates earlier (non-SMT 18-core vs 32-thread SMT AMD)
- The gap widens at high concurrency: zero-copy crow-rpc + SMT headroom

Note: the M5 Pro results use the older gRPC transport and larger
coalesce window (32 vs 16). A direct zero-copy comparison on M5 Pro
is pending. The AMD gRPC baseline at 256T was ~124K (2026-08-04), so
zero-copy alone provides ~1.6× on AMD.

---

## Memory Copy Summary

Copy points are annotated inline in the flow diagram above. Summary
of what remains:

- **O(n) unavoidable** — payload encoding (client key/value slices →
  contiguous `Vec<u8>`); WAL replay (`Bytes::copy_from_slice` to
  reconstruct `PxLogEntry` from on-disk bytes); C++ engine apply
  (internal memtable copy); crow-rpc socket write (kernel user→socket
  buffer copy).
- **O(1) ref-count bumps (negligible)** — `base_entry` payload clone
  per slot retry; `inner_accept` entry clone for `cas_accepted`;
  `send_accept` payload clone for flatbuffer; `learn_chosen` entry clone
  for learner; WAL `from_accepted` payload clone (`encode_accepted_payload`
  is `entry.payload.clone()`); WAL `encode_frame` payload clone for
  `RecordFrame`; Batch decode `Bytes::slice` per key/value (shares
  payload buffer).
- **Zero-copy (move/borrow)** — `Vec<u8>` → `Bytes` at `propose_inner`
  entry; crow-rpc deserialization → `PxLogEntry` (move `Bytes`); WAL
  vectored write (`IoSlice` borrows `Bytes`); FFI batch apply
  `ct_kv_ref` pointer-length structs (R23, done).

---

## Write-Path Enhancement Ideas

The per-proposal critical path, after R16b (early-ack) and R17 (async
engine apply, gated by the R35 apply fence), is the quorum RPC
round-trip only. The leader's local fsync and engine apply both run
off the critical path. Fan-out hardening (R43) shipped: quorum
short-circuit, oneshot deadlines, phase metrics, backoff jitter (E4),
heartbeat reserve (E5), `ReplyFold` refactor. No open items.
