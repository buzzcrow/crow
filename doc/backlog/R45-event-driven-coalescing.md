<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R45: Event-driven proposal coalescing (replace R36 timer)

**Problem**: R36 coalescing uses a timer (`coalesce_window_us` sleep then
flush) to accumulate concurrent single-key proposes into one multi-key
Paxos proposal. The timer adds a fixed latency floor for the first op in
a sparse batch: even with no contention, the first op waits the full
window before the Paxos round starts. At high concurrency this is
irrelevant (batches fill to `max_keys` before the timer fires), but at
low-to-moderate concurrency the timer tax is pure overhead with no
amortization benefit.

**Proposed approach**: Replace the timer with an event-driven flush,
modeled on the existing `PendingReadBarrier` pattern in
`group.rs::linearizable_read_barrier`. The core idea: **aggregate from
the start, use more inflight permits only when batches overflow**.

State machine (per group):

```
coalesce_state: Mutex<Option<PendingBatch>>   // None = idle, Some = accumulating
inflight_rounds: Semaphore (existing max_inflight permits)
```

Flow:

- Op arrives, no pending batch (`coalesce_state == None`):
  - Open a `PendingBatch`, add this op, start a Paxos round **immediately**
    with this 1-op payload (acquire 1 inflight permit). Do NOT wait.
  - The pending batch stays open to accumulate ops that arrive *during*
    this round.
- Op arrives, pending batch exists (round in flight):
  - Append op body + tag to the pending batch, register a waiter.
  - If batch fills to `coalesce_max_keys`: start a **second** round
    immediately (acquire another inflight permit), open a new pending
    batch for the next wave. This is the "multiple pipelines" path —
    only triggers when a single round can't keep up.
- Round completes (callback in `propose_inner`'s spawn):
  - Fan the `ProposeResult` to that round's waiters.
  - Drain the pending batch. If non-empty, start the next round with
    whatever accumulated (could be 1 op, could be `max_keys`). If empty,
    the cycle ends — back to idle.

This eliminates the `coalesce_window_us` config knob entirely. The
coalescer is either on (`coalesce_max_keys > 0`) or off
(`coalesce_max_keys == 0`). No timer, no latency floor.

**Why aggregate from the start (not fill inflight window first)**:

Two possible event-driven designs were considered:

- **Option A (fill window first)**: Each op starts its own round until
  the inflight window is full, then aggregate. At medium load (8 ops),
  this issues 8 separate rounds — zero aggregation. Aggregation only
  kicks in under saturation.
- **Option B (aggregate from start)** — **chosen**: The first op starts
  a round immediately, but every subsequent op that arrives during any
  in-flight round joins the pending batch. The inflight window is a
  flow-control backstop, not the primary aggregation trigger.

| Load | Option A | Option B (chosen) |
|---|---|---|
| 1 op | 1 round, no tax | 1 round, no tax |
| 8 ops | 8 rounds, no aggregation | 2 rounds (1+7), good aggregation |
| 64 ops | 32 rounds, then aggregate | ~2 rounds of 32, then more as needed |
| 64+ ops | 32 concurrent rounds, then aggregate | 32 concurrent rounds, each full |

Option B is strictly better: aggregation happens whenever ops arrive
during an in-flight round — not only when the inflight window is
saturated. The first op never waits (starts immediately), and every
subsequent op that arrives during any in-flight round gets a free ride
in the next batch.

**Interaction with the inflight window**: The existing `max_inflight`
permits cap concurrent Paxos rounds. With Option B, each round carries
up to `coalesce_max_keys` ops, so the effective in-flight key count is
`max_inflight × coalesce_max_keys` (e.g. 32 × 32 = 1024 keys). When all
permits are taken, the pending batch keeps accumulating (up to
`max_keys`) without starting a new round — when a round completes and
frees a permit, the accumulated batch flushes as the next round. This
is strictly better than R36's timer approach, which under a full
inflight window creates many small blocked batches (each timer flush
spawns a task that immediately blocks on `acquire_permit`).

**Existing pattern**: `PxGroup::pending_read_barrier`
(`group.rs:204`) batches `ReadIndex` reads that arrive while a heartbeat
round is in flight, then drains all waiters on round completion. The
coalescer uses the same shape: `parking_lot::Mutex<Option<PendingBatch>>`,
drained on round completion, `None` when idle. The difference is R45
allows multiple concurrent rounds (inflight permits) instead of just 1.

**Key difference from R36**: R36 holds the batch open for a fixed time
window *before* starting the round. R45 starts the round immediately and
batches ops that arrive *during* the round. The amortization is "free"
(ops that would have waited for the round anyway now share it) with zero
added latency.

**R36 benchmark results (timer-based, 10s mem mode, 3-node cluster)**:

| Config | Threads | Window | TPS | Avg latency | WAL appends |
|---|---|---|---|---|---|
| baseline (no coalesce) | 32 | 0 | 27,787 | 1,149us | 833,669 |
| R36 coalesce | 32 | 500us | 33,029 | 965us | 31,090 |
| R36 coalesce | 32 | 1ms | 33,182 | 961us | 31,124 |
| baseline (no coalesce) | 64 | 0 | 28,062 | 2,278us | 841,907 |
| R36 coalesce | 64 | 500us | 64,145 | 993us | 60,498 |

Batch size distribution (500us window, 32 threads, 165K ops):
- 99% of batches hit `max_keys=32` (fill before timer)
- Average batch size: ~32 ops
- Timer fires only on tail batches (size 1-4)

**R45 hypothesis**: At high concurrency (64 threads), R45 should match
or exceed R36 (batches fill during the round anyway, and the inflight
window is used more efficiently — no blocked small batches). At low
concurrency (1-8 threads), R45 should beat R36 because the first op
starts the round immediately instead of waiting `window_us`, and
subsequent ops still aggregate during the round. The win spans both
regimes: no timer tax at low load, better inflight utilization at high
load.

**Acceptance**:
- `coalesce_window_us` config knob and CLI flag removed; coalescing is
  controlled by `coalesce_max_keys` alone (0 = off).
- No timer task; flush is triggered by round completion or `max_keys`.
- TPS at 64 threads matches or exceeds R36 (64K+).
- TPS at 1-8 threads exceeds R36 (no timer latency floor).
- All R36 coalescing tests pass unchanged (dedup, ordering, max_keys,
  engine apply, sequential batches).
- `PendingReadBarrier` pattern is referenced as the design template.

**Complexity**: Medium. The coalescer state machine changes from
"accumulate then flush" to "flush immediately, accumulate during round,
flush again on completion". The `propose_inner` completion callback
becomes the flush trigger. The `DedupTag` threading and `AcceptRequest`
proto from R36 are unchanged.
