<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R45: Event-driven proposal coalescing (replace R36 timer)

**Problem**: R36 coalescing uses a timer (`coalesce_window_us` sleep
then flush) to accumulate concurrent single-key proposes into one
multi-key Paxos proposal. The timer adds a fixed latency floor for the
first op in a sparse batch: even with no contention, the first op
waits the full window before the Paxos round starts. At high
concurrency this is irrelevant (batches fill to `max_keys` before the
timer fires), but at low-to-moderate concurrency the timer tax is pure
overhead with no amortization benefit.

## Terms

- **Op** — a single-key propose request from a client. Carries a
  payload (key + value) and a dedup tag `(client_id, seq)`.
- **Batch** — `PendingBatch` struct: `op_bodies` (concatenated op
  payloads), `op_count`, `tags` (dedup tags), `waiters` (oneshot
  channels to return results to each op's caller). Held in
  `coalescer: Mutex<Option<PendingBatch>>`. `None` = idle (no batch
  accumulating, no round in flight). `Some` = batch open for
  accumulation.
- **Slot-task** — a one-shot tokio task that runs one `propose_inner`
  round: acquire one inflight permit → allocate one Paxos slot → run
  Accept phase (quorum RPCs) → fan `ProposeResult` to the batch's
  waiters → release permit → exit. Each slot-task produces one Paxos
  slot carrying one batch (1 to `max_keys` ops).
- **Inflight window** — `max_inflight` permits (semaphore). Each
  slot-task acquires one permit for the duration of its round. When
  all permits are taken, new slot-tasks block on `acquire().await`
  (tokio task parking, not OS thread blocking). Caps concurrent
  rounds.
- **In-flight count** — `occupied + waiting` from `InflightAdmission`:
  `occupied` = slot-tasks holding permits (running rounds right now);
  `waiting` = slot-tasks parked on `acquire().await` (window full).
  This is the total count of slot-tasks alive at any moment.
- **Idle** — coalescer is `None`: no batch accumulating, no round in
  flight, no slot-task alive. The system is waiting for the next op.
- **Overflow** — while a round is in flight, ops arriving join the
  pending batch. When the batch fills to `max_keys`, it is flushed
  immediately as a **concurrent** slot-task (acquires another inflight
  permit), and a fresh empty batch is opened. This is the high-load
  batching path — it produces full batches without waiting for the
  in-flight round to complete.
- **Drain** — `coalesce_drain_after_round`: after a slot-task finishes
  its round, it takes the pending batch (ops that accumulated *during*
  the round) and spawns the next slot-task with it. This is the
  concurrency-maintenance path — it keeps rounds flowing when the
  overflow path alone can't keep enough rounds in flight.
- **Watchdog** — a single background task per group that sleeps 1000ms,
  then checks if there's been no coalescer activity for 1000ms. If so,
  it flushes any stuck non-empty batch. Safety net for edge cases
  (drain panic, spawn failure). Zero overhead during normal operation.

## Strategy 1: R36 — timer-based collect-then-flush

**Core idea**: delay the slot-task spawn by a fixed timer so ops
accumulate in the batch *before* the round starts.

**Op arrival** (`coalesce_enqueue`):
1. Coalescer is `None` (idle) → create a new batch with this op, arm a
   500us timer. Do NOT start a round. The op waits for the timer.
2. Coalescer is `Some` (batch exists) → append op to the batch. If
   `op_count >= max_keys` → flush the batch immediately (overflow).

**Timer fires** (500us after first op):
- Take the batch from `coalescer` (set to `None`), spawn a slot-task
  with it. The slot-task runs `propose_inner`, fans results, exits.

**Slot-task finishes**:
- No drain. The slot-task exits. The coalescer is already `None` (set
  by the timer/overflow flush). The system goes idle until the next op
  arrives and arms a new timer.

**Behavior by load**:
- **High load**: 32 ops arrive in microseconds, far before the 500us
  timer fires. The batch fills to `max_keys` → overflow spawns a
  full-batch slot-task immediately. The timer never fires. Every round
  carries ~32 ops.
- **Low load**: few ops arrive in 500us. The timer fires with a small
  batch (1-few ops). The timer tax — the first op waits 500us even
  with no contention.

**No fragmentation**: R36 slot-tasks are fire-and-forget. Each batch
is independently collected to full (or timer-fired) before its
slot-task spawns. No drain, no racing to take a shared batch.

## Strategy 2: R45 event — immediate flush, drain after round

**Core idea**: start the round immediately (no timer), and batch ops
that arrive *during* the round. The amortization is "free" — ops that
would have waited for the round anyway now share it.

**Op arrival** (`coalesce_enqueue`):
1. Coalescer is `None` (idle) → create a new batch with this op, start
   a 1-op slot-task **immediately** (no timer). Open a fresh empty
   batch for ops that arrive during this round.
2. Coalescer is `Some` (batch exists) → append op to the batch. If
   `op_count >= max_keys` → overflow: take the batch, spawn a
   concurrent slot-task, open a new empty batch.

**Slot-task finishes** → drain (`coalesce_drain_after_round`):
1. Take the pending batch from `coalescer` (set to `None`).
2. If `op_count > 0` → spawn the next slot-task with this batch. Open
   a fresh empty batch for ops during the next round.
3. If `op_count == 0` → go idle. The slot-task exits.

**Behavior by load**:
- **Low load**: 1 op arrives → starts a 1-op round immediately (no
  timer tax). If a second op arrives during the round, it joins the
  pending batch and gets a free ride in the next round.
- **High load**: the drain runs once per slot-task. With N concurrent
  slot-tasks finishing at staggered times, each calls
  `coalescer.lock().take()` on the single shared batch. The first
  finisher takes the batch (whatever accumulated during the round);
  the rest get an empty batch or `None`, exit, and the coalescer goes
  idle. The next op starts a 1-op round. This alternates: 1-op round,
  N-op round, 1-op round — inflating WAL appends and wasting quorum
  RPCs. This is the **high-load gap**.

## Strategy 3: R45b — drain threshold (skip drain at high load)

**Core idea**: at high load, the overflow path already produces full
batches. The drain is only needed to maintain concurrency when few
slot-tasks are in flight. Skip the drain when enough slot-tasks are
already running.

**Op arrival**: same as R45 event (immediate flush, overflow at
`max_keys`).

**Slot-task finishes** → drain with threshold check:
```
coalesce_drain_after_round():
    in_flight = inflight.occupied() + inflight.waiting()
    if in_flight >= coalesce_drain_threshold:
        return                        // skip: leave batch for overflow
    batch = coalescer.lock().take()
    if batch.op_count == 0:
        return                        // empty: go idle
    coalesce_flush_batch(batch)       // start next round
```

**Liveness**: The inflight permit is released (in `propose_inner`, on
permit drop) **before** `coalesce_drain_after_round` is called. So
when a slot-task reads `in_flight`, its own contribution is already
removed. With N slot-tasks and threshold T: the first N-T finishers
see `in_flight >= T` and skip; the last T finishers each see
`in_flight < T` and attempt to take the batch (first one gets it, rest
get `None`/empty and go idle). The batch is never stuck — at least one
finisher always takes it. The 1000ms watchdog is a backstop.

**Behavior by load** (threshold=`max_inflight / 4` = 8):
- **High load** (128-256 threads): 8+ slot-tasks always in flight →
  drain skips. Batches fill to `max_keys=32` via overflow — full
  batches, like R36. No fragmentation, no 1-op rounds.
- **Moderate load** (64 threads): 1-8 slot-tasks in flight → drain
  fires when the count drops below 8, takes the accumulated batch,
  starts the next round. Maintains concurrency without fragmenting.
- **Low load** (32 threads): 1-2 slot-tasks in flight → drain always
  fires (count well below 8). Same as pure event mode — no timer tax,
  small batches OK.

## Config

- `coalesce_max_keys`: batch size cap. `0` = coalescing off (one
  proposal per key). Default `0` (opt-in).
- `coalesce_drain_threshold`: skip drain when in-flight count >= this
  value. Default `max_inflight / 4` (set automatically when coalescing
  is enabled and no explicit value given). `0` = always drain
  (disables the heuristic, reverts to pure event mode / Strategy 2).

## Benchmark results (10s mem mode, 3-node cluster, max_keys=32)

| Threads | R36 TPS | R45 event TPS | R45b TPS | R36 WAL | R45 event WAL | R45b WAL |
|---|---|---|---|---|---|---|
| 32 | 33,029 | 48,346 | 47,485 | 31,090 | 401,897 | 139,404 |
| 64 | 64,145 | 68,201 | 68,741 | 60,498 | 377,591 | 106,926 |
| 128 | 97,554 | 86,759 | 101,537 | 92,752 | 425,484 | 101,350 |
| 256 | 113,671 | 97,865 | 118,377 | 110,034 | 437,744 | 111,944 |

R45b (threshold=`max_inflight / 4`) beats R36 at high load (128: 102K
vs 98K, 256: 118K vs 114K) while cutting WAL 3-4x vs pure event mode
(425K→101K at 128, 438K→112K at 256). At 64 threads it matches event
mode and beats R36 (69K vs 64K). At 32 threads it matches event mode
(47K vs 48K) — the threshold of 8 is high enough that drains always
fire at low load, preserving the zero-latency-floor behavior.

## What stays unchanged across R45/R45b

- Idle path (coalescer is `None`, first op arrives): spawns 1-op
  slot-task immediately, no timer tax.
- Overflow path (batch hits `max_keys`): spawns full-batch slot-task
  immediately.
- Watchdog (1000ms): backstop for stuck batches.
- `DedupTag` threading and `AcceptRequest` proto from R36: unchanged.

## Acceptance

- `coalesce_max_keys` controls on/off (0 = off).
- `coalesce_drain_threshold` defaults to `max_inflight / 4` when
  coalescing is on.
- At high load (128+ threads): TPS beats R36 (101K+ at 128, 118K+ at
  256).
- At moderate load (64 threads): TPS matches event mode, beats R36.
- At low load (32 threads): TPS matches event mode (no regression).
- All coalescing tests pass (dedup, ordering, max_keys, engine apply,
  sequential batches).

## Complexity

Low. One `if` check in `coalesce_drain_after_round`, one config field,
one CLI flag. No new tasks, no timer, no mode switch, no ring buffer.
