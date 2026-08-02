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
`group.rs::linearizable_read_barrier`:

- The first op to arrive when no batch is in flight starts the Paxos
  round **immediately** (no sleep). It also opens a `PendingBatch` that
  accumulates ops arriving *while the round is in flight*.
- Each subsequent op that arrives while the round is in flight appends
  to the pending batch and registers a waiter.
- When the in-flight round completes, the leader drains the pending
  batch and starts the next round with whatever accumulated (could be 1
  op, could be `max_keys`). If the batch is empty, the cycle ends.
- `max_keys` still triggers an immediate flush (same as R36) — but only
  matters when a round is already in flight and the pending batch fills.

This eliminates the `coalesce_window_us` config knob entirely. The
coalescer is either on (`coalesce_max_keys > 0`) or off
(`coalesce_max_keys == 0`). No timer, no latency floor.

**Existing pattern**: `PxGroup::pending_read_barrier`
(`group.rs:204`) batches `ReadIndex` reads that arrive while a heartbeat
round is in flight, then drains all waiters on round completion. The
coalescer would use the same shape: `parking_lot::Mutex<Option<PendingBatch>>`,
drained on round completion, `None` when idle.

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
R36 (batches fill during the round anyway). At low concurrency (1-8
threads), R45 should beat R36 because the first op starts the round
immediately instead of waiting `window_us`. The win is in the
low-to-moderate concurrency regime where R36's timer tax is pure
overhead.

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
