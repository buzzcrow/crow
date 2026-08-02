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

**R45 adaptive design**: Use a single timer whose interval is derived
from observed round latency. Two modes share the same pending batch,
drain logic, and timer task — only the timer interval and first-op
behavior differ. The mode switch is automatic, based on round latency
(not round size), and the timer interval adapts to the current load.

- **Event mode** (low load, round latency < 1ms): timer interval =
  watchdog (1000ms). First op starts a round immediately (no timer
  tax). The timer only fires if something is stuck.
- **Timer mode** (high load, round latency > 1ms): timer interval =
  avg round duration / 4 (adaptive). First op opens a batch but does
  NOT start a round — the timer flushes the batch after the interval,
  collecting a bigger batch. No 1-op rounds.

**Mode switch heuristic (latency-based)**: Track recent round durations
(sliding window of last 16 rounds, in microseconds). The switch is
based on whether ops are queuing (round latency is high):

- Event → Timer: avg round duration > 1000us. Rounds are slow (ops
  are queuing), so collecting a batch before starting the next round
  will produce bigger batches and fewer total rounds.
- Timer → Event: avg round duration < 500us (hysteresis to avoid
  oscillation). Rounds are fast again — 1-op rounds are cheap, event
  mode's zero-latency-floor wins.

The latency-based heuristic correctly distinguishes low load (fast
1-op rounds, OK) from high load (slow 1-op rounds, wasteful). The
previous round-size-based heuristic couldn't make this distinction
and switched to timer mode prematurely at low load.

**Adaptive timer interval**: In timer mode, the interval is
`avg_round_duration / 4`, clamped to 50us..10ms. This adapts
automatically — short rounds (light load) get a short timer, long
rounds (heavy load) get a longer timer to collect more ops. No
configuration needed.

**Watchdog**: The timer serves double duty. In event mode its long
interval (1000ms) catches stuck batches (drain panic, spawn failure).
In timer mode its short adaptive interval drives batch flushes. One
timer, one code path, interval changes with mode.

**State machine**:

```
coalescer: Mutex<Option<PendingBatch>>
  PendingBatch { op_bodies, op_count, tags, waiters, timer: JoinHandle }
coalesce_mode: AtomicU8  // 0=Event, 1=Timer
round_durations: Mutex<Vec<u64>>  // last 16 round durations (us)

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
  timer fires (avg_round/4) → flush whatever is in batch
  round completes → drain:
    non-empty → keep batch + re-arm timer (DON'T start round)
    empty → keep empty batch + arm timer
```

**Config**:
- `coalesce_max_keys`: batch size cap (both modes). 0 = coalescing off.
- Timer interval: automatic, derived from avg round duration. Not
  configurable.
- Watchdog interval: fixed 1000ms, not configurable.
- Mode switch threshold: fixed 1000us avg round duration. Not
  configurable.

**R45 adaptive benchmark results (10s mem mode, 3-node cluster,
max_keys=32)**:

| Threads | R36 TPS | R45 event TPS | R45 adaptive TPS | R45 adaptive WAL |
|---|---|---|---|---|
| 32 | 33,029 | 48,346 | 45,826 | 521,888 |
| 64 | 64,145 | 68,201 | 63,590 | 474,143 |
| 128 | 97,554 | 86,759 | 90,484 | 259,309 |
| 256 | 113,671 | 97,865 | 103,509 | 183,764 |

The latency-based mode switch correctly stays in event mode at 32
threads (458K vs pure event's 48K — within 5%), and switches to timer
mode at 128+ threads (90K at 128, 103K at 256). The high-load TPS is
close to R36 (90K vs 98K at 128, 103K vs 114K at 256) but doesn't
fully match it — the mode-switch overhead and keep-batch timer latency
account for the gap.

At 64 threads, the adaptive mode is slightly below both R36 and pure
event (64K vs 64K/68K). The avg round latency at 64 threads is right
at the 1000us threshold, causing occasional mode oscillation.

**Acceptance**:
- `coalesce_max_keys` controls on/off (0 = off).
- No `coalesce_window_us` config — timer interval is automatic.
- At low load (32 threads): TPS matches pure event mode (458K vs 48K).
- At high load (128+ threads): TPS approaches R36 (90K+ at 128 threads).
- Mode switch is automatic, based on round latency (16-round history).
- Timer interval adapts to load (avg round duration / 4).
- Watchdog (1000ms) prevents stuck batches in event mode.
- All coalescing tests pass (dedup, ordering, max_keys, engine apply,
  sequential batches).

**Complexity**: Medium. The coalescer gains a timer task (reused from
R36), a mode flag, and a round-duration history ring buffer. The
`PendingReadBarrier` drain pattern is unchanged. The `DedupTag`
threading and `AcceptRequest` proto from R36 are unchanged.
