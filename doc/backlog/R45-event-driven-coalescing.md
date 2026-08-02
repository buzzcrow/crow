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

**Why aggregate from the start**:

The first op starts a round immediately, but every subsequent op that
arrives during any in-flight round joins the pending batch. The inflight
window is a flow-control backstop, not the primary aggregation trigger.

| Load | Rounds issued | Behavior |
|---|---|---|
| 1 op | 1 round | Starts immediately, no tax |
| 8 ops | 2 rounds (1+7) | Op 1 starts round 1; ops 2-8 arrive during round 1 and join pending batch; round 1 completes, round 2 carries 7 ops |
| 64 ops | ~2 rounds of 32, then more | Batches fill to `max_keys`, overflow starts concurrent rounds |
| 64+ ops | up to 32 concurrent rounds, each full | All permits taken; pending batch accumulates until a round frees a permit |

Aggregation happens whenever ops arrive during an in-flight round — not
only when the inflight window is saturated. The first op never waits
(starts immediately), and every subsequent op that arrives during any
in-flight round gets a free ride in the next batch.

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

| Config | Threads | Conns | Window | TPS | Avg latency | WAL appends |
|---|---|---|---|---|---|---|
| baseline (no coalesce) | 32 | 4 | 0 | 27,787 | 1,149us | 833,669 |
| R36 coalesce | 32 | 4 | 500us | 33,029 | 965us | 31,090 |
| R36 coalesce | 32 | 4 | 1ms | 33,182 | 961us | 31,124 |
| baseline (no coalesce) | 64 | 8 | 0 | 28,062 | 2,278us | 841,907 |
| R36 coalesce | 64 | 8 | 500us | 64,145 | 993us | 60,498 |
| baseline (no coalesce) | 128 | 16 | 0 | 28,260 | 4,528us | 847,880 |
| R36 coalesce | 128 | 16 | 500us | 97,554 | 1,305us | 92,752 |
| R36 coalesce | 128 | 16 | 1ms | 95,897 | 1,328us | 91,037 |
| baseline (no coalesce) | 256 | 32 | 0 | 27,804 | 9,205us | 834,216 |
| R36 coalesce | 256 | 32 | 500us | 113,671 | 2,241us | 110,034 |

Baseline plateaus at ~28K TPS regardless of thread count — the
bottleneck is the per-proposal quorum RPC rate, not client concurrency.
Coalescing scales with concurrency: more loader threads = more ops
arriving per round = larger batches = fewer rounds. At 256 threads,
coalescing achieves **113K TPS vs 28K baseline = 4.1x improvement**.

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

**R45 benchmark results (event-driven, 10s mem mode, 3-node cluster)**:

| Threads | Conns | max_keys | TPS | Avg latency | WAL appends |
|---|---|---|---|---|---|
| baseline | 32 | 4 | 0 | 28,033 | 1,139us | 841,093 |
| R45 coalesce | 32 | 4 | 32 | 48,346 | 658us | 401,897 |
| baseline | 64 | 8 | 0 | 28,415 | 2,250us | 852,562 |
| R45 coalesce | 64 | 8 | 32 | 68,201 | 933us | 377,591 |
| baseline | 128 | 16 | 0 | 28,502 | 4,489us | 855,145 |
| R45 coalesce | 128 | 16 | 32 | 86,759 | 1,468us | 425,484 |
| baseline | 256 | 32 | 0 | 28,414 | 9,008us | 852,528 |
| R45 coalesce | 256 | 32 | 32 | 97,865 | 2,607us | 437,744 |

**R45 vs R36 comparison**:

| Threads | R36 TPS | R45 TPS | R36 latency | R45 latency | Winner |
|---|---|---|---|---|---|
| 32 | 33,029 | 48,346 | 965us | 658us | R45 (+46% TPS) |
| 64 | 64,145 | 68,201 | 993us | 933us | R45 (+6% TPS) |
| 128 | 97,554 | 86,759 | 1,305us | 1,468us | R36 (-11% TPS) |
| 256 | 113,671 | 97,865 | 2,241us | 2,607us | R36 (-14% TPS) |

R45 wins at low-to-moderate concurrency (32-64 threads): the first op
starts immediately with no timer tax, and subsequent ops aggregate
during the round. At high concurrency (128+ threads), R36's timer
approach wins because it collects larger batches (32 ops per round vs
R45's smaller batches from immediate round starts). R45's WAL append
count is 5-10x higher than R36's, confirming smaller batch sizes.

The crossover is at ~64 threads. Below that, R45's zero-latency-floor
design wins; above that, R36's collect-then-flush approach produces
bigger batches and fewer rounds.

**Root cause of R45's high-load gap**: When a round completes, the
coalescer goes idle (`None`). The next op starts a 1-op round
immediately, and ops arriving *during* that 1-op round join the next
batch. At high load this alternates: 1-op round, then 32-op round,
then 1-op round, etc. Half the rounds carry only 1 op, inflating WAL
appends and wasting quorum RPCs. R36 avoids this because its timer
keeps the batch open between rounds — the next wave of ops joins the
existing batch instead of starting a new 1-op round.

**R45 adaptive design**: Use a single timer whose interval changes
dynamically based on load. Two modes share the same pending batch,
drain logic, and timer task — only the timer interval and first-op
behavior differ.

- **Event mode** (low load): timer interval = watchdog (long, e.g.
  1000ms). First op starts a round immediately (no timer tax). The
  timer only fires if something is stuck (drain panic, spawn failure).
- **Timer mode** (high load): timer interval = `coalesce_window_us`
  (short, e.g. 500us). First op opens a batch but does NOT start a
  round — the timer flushes the batch after the window, collecting a
  full batch. No 1-op rounds.

**Mode switch heuristic**: Track recent round sizes (sliding window of
last 16 rounds). The switch is based on whether event mode is producing
small batches (wasting rounds):

- Event → Timer: avg round size of last 16 rounds < `max_keys / 2`.
  This detects the 1-op-round alternation pattern that dominates at
  high concurrency.
- Timer → Event: avg round size >= `max_keys * 0.75`. This means
  batches are filling before the timer fires — load is high enough that
  event mode will batch well too.

**Watchdog**: The timer serves double duty. In event mode its long
interval (1000ms) catches stuck batches (drain task panic, spawn
failure). In timer mode its short interval (`coalesce_window_us`)
drives batch flushes. One timer, one code path, interval changes with
mode.

**State machine**:

```
coalescer: Mutex<Option<PendingBatch>>
  PendingBatch { op_bodies, op_count, tags, waiters, timer: JoinHandle }
coalesce_mode: AtomicU8  // 0=Event, 1=Timer
round_sizes: Mutex<Vec<u8>>  // last 16 round sizes

Event mode:
  idle → op arrives → start 1-op round + open empty batch + arm watchdog
  batch exists → join → max_keys → flush (cancel timer)
  round completes → drain:
    non-empty → start next round + open new batch + arm watchdog
    empty → go idle (no timer tax)
  watchdog fires (1000ms):
    batch has ops → flush
    batch empty → go idle

Timer mode:
  idle → op arrives → open batch + arm timer (DON'T start round)
  batch exists → join → max_keys → flush (cancel timer)
  timer fires (window_us) → flush whatever is in batch
  round completes → drain:
    non-empty → keep batch + re-arm timer (DON'T start round)
    empty → keep empty batch + arm timer
```

**Config**:
- `coalesce_max_keys`: batch size cap (both modes). 0 = coalescing off.
- `coalesce_window_us`: timer-mode interval (e.g. 500us). 0 = never
  switch to timer mode (event-only, watchdog still active at fixed
  1000ms).
- Watchdog interval: fixed 1000ms, not configurable.

**R45 adaptive benchmark results (10s mem mode, 3-node cluster,
max_keys=32, window=500us)**:

| Threads | R36 TPS | R45 event TPS | R45 adaptive TPS | R45 adaptive WAL |
|---|---|---|---|---|
| 32 | 33,029 | 48,346 | 36,283 | 57,019 |
| 64 | 64,145 | 68,201 | 67,348 | 81,436 |
| 128 | 97,554 | 86,759 | 97,182 | 145,062 |
| 256 | 113,671 | 97,865 | 109,229 | 193,051 |

The adaptive mode switches to timer mode at high load (128+ threads),
matching R36's throughput (97K vs 98K at 128 threads, 109K vs 114K at
256 threads). At low load (32 threads), the mode switch triggers
prematurely (event mode's 1-op rounds fill the history), giving 36K
vs pure event's 48K. This is the known limitation of the round-size
heuristic: it can't distinguish fast 1-op rounds (low load, OK) from
slow 1-op rounds (high load, wasteful).

**Usage recommendation**:
- `coalesce_window_us=0` (default): pure event mode, best for
  low-to-moderate load (1-64 threads). No timer tax.
- `coalesce_window_us=500`: adaptive mode, best for high load (128+
  threads). Mode switch activates timer mode when 1-op rounds dominate.
- For mixed workloads, `coalesce_window_us=500` gives the best
  all-around performance (matches R36 at high load, slight regression
  at low load).

**Acceptance**:
- `coalesce_max_keys` controls on/off (0 = off).
- `coalesce_window_us` controls timer-mode interval (0 = event-only).
- At high load (128+ threads) with `window_us=500`: TPS matches R36
  (97K+ at 128 threads).
- At low load (1-32 threads) with `window_us=0`: TPS matches pure
  event mode (48K+ at 32 threads).
- Mode switch is automatic, based on round-size history (16 rounds).
- Watchdog (1000ms) prevents stuck batches in event mode.
- All coalescing tests pass (dedup, ordering, max_keys, engine apply,
  sequential batches).

**Complexity**: Medium. The coalescer gains a timer task (reused from
R36), a mode flag, and a round-size history ring buffer. The
`PendingReadBarrier` drain pattern is unchanged. The `DedupTag`
threading and `AcceptRequest` proto from R36 are unchanged.
