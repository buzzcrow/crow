<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Design — ReadIndex batching for linearizable reads (R27)

Working design draft for R27. To be folded into
`doc/design/design-leader-election.md` §7.2 on merge.

## Problem

When the leader's read lease is not effective, every linearizable read
falls back to the ReadIndex path: one `run_heartbeat_round` per read
(`crowkv/src/cluster/group_election.rs:694`). Under a burst of N
concurrent linearizable reads with an expired lease this multiplies
heartbeat traffic (N rounds → N× quorum fan-out) and makes each read's
latency one full RTT instead of one RTT amortized across the burst.

Current behavior (`linearizable_read_barrier`, `group_election.rs:666`):

- Lease fast path: `lease_read_valid(now)` → serve at
  `contiguous_chosen`, no round-trip.
- ReadIndex path: capture `read_slot = contiguous_chosen()`, run one
  `run_heartbeat_round`, map `HeartbeatOutcome` → `ReadBarrierOutcome`,
  return. One round per call; no coalescing.

## Proposed approach

Consensus-layer-only change to `linearizable_read_barrier`. The
heartbeat round itself is unchanged; pending ReadIndex barriers are
coalesced onto a single in-flight round.

### State (on `PxGroup`)

- `pending_read_barrier: parking_lot::Mutex<Option<PendingReadBarrier>>`
  where:
  - `PendingReadBarrier { read_slot: SlotIndex, waiters: Vec<oneshot::Sender<ReadBarrierOutcome>> }`
  - `read_slot` is the `contiguous_chosen` captured by the round leader
    *before* it starts the heartbeat.
  - `waiters` are the batched reads that arrived while the round was in
    flight.

### Flow

`linearizable_read_barrier` (lease fast path unchanged):

1. `is_leader` / `leader_read_ready` / lease checks exactly as today.
2. Lease valid → `Ready { read_slot }` immediately (no queueing).
3. ReadIndex path — lock `pending_read_barrier`:
   - **Round in flight** (`Some`): create a `oneshot`, push the sender
     into `waiters`, drop the lock, `await` the receiver, return the
     received outcome. Each waiter records its own `barrier.l` (from
     its own `barrier_start`, includes queue wait + round) and incs
     `readindex_path.c` on `Ready`.
   - **No round in flight** (`None`): capture `read_slot =
     contiguous_chosen()`, store `Some(PendingReadBarrier { read_slot,
     waiters: vec![] })`, drop the lock, run `run_heartbeat_round`, map
     to `outcome`. Then re-lock, `take` the `PendingReadBarrier`
     (clearing in-flight), drop the lock, send `outcome.clone()` to
     every waiter, return `outcome` for self. The round leader records
     `barrier.l` and incs `readindex_path.c` on `Ready` as today.

The mutex serializes enqueue (waiters) and dequeue (round leader's
`take`), so no waiter is lost: a read either enqueues before the `take`
(→ resolved with the batch) or sees `None` after the `take` (→ starts a
new batch). Both are correct.

### Correctness

Identical argument to the single-read ReadIndex path
(`design-leader-election.md` §7.1, `read-flow-analysis.md` §"Linearizable
Read Barrier"):

- The heartbeat quorum at the leader's term confirms no higher-term
  election displaced this leader during the round, so every committed
  write is reflected in the leader's local state.
- The engine get (`px_kv_store.rs:53`,
  `learner.engine_get_bytes(key)`) returns the **latest** applied value
  for the key (single-version, highest-slot-wins), not a value pinned to
  `read_slot`. Each batched read performs its own `engine_get_bytes`
  after the barrier resolves, so it observes the freshest local state at
  its serve time (≥ round completion). The shared `read_slot` reported
  in the response is a **conservative freshness floor** (the pre-round
  `contiguous_chosen`), never an over-estimate.
- A write that commits *during* the round has slot > `read_slot`; the
  batched read's `engine_get_bytes` returns that write's value (latest),
  so late-arriving batched reads are not stale. The reported `read_slot`
  floor being lower than the true serve slot is safe (it only
  under-reports freshness).

Step-down (`HeartbeatOutcome::SteppedDown`) and no-quorum
(`Continued { quorum_acked: false }`) fan out to all waiters as
`NotLeader` / `NoQuorum` respectively. A waiter whose round-leader
future is dropped (cancellation) gets `Err` from its `oneshot::Receiver`,
mapped to `NoQuorum` (safe retry).

### Metric

New counter `read.readindex_rounds.c` on `ReadRegistryHandles`,
incremented once per ReadIndex heartbeat round (by the round leader,
after the round). Validation:

- `read.readindex_path.c` (reads taking the ReadIndex path) is
  unchanged — each read still incs it on `Ready`.
- `read.readindex_rounds.c` drops from N (today, one round per read) to
  1 for a batched burst.
- Average batch size = `readindex_path.c / readindex_rounds.c`.
- `read.barrier.l` avg drops toward one RTT amortized across the batch.

## Alternatives considered

- **Per-read `read_slot` + shared round.** Each batched read captures
  its own `contiguous_chosen` at enqueue time and shares only the
  heartbeat. Also correct (each read linearizes at its own arrival
  instant, which is within the round). Rejected: the engine already
  returns the latest value, so pinning a per-read `read_slot` adds
  bookkeeping with no correctness or freshness benefit over the shared
  conservative floor. The shared-`read_slot` form matches the R27 spec
  and the existing single-read response shape.
- **Batch the engine get too.** Rejected: engine gets are per-key and
  cheap (lock-free `InMemKV` / FFI fast path); coalescing them adds
  grouping latency and complexity for no consensus-resource savings.
  The heartbeat round is the only expensive, consensus-busy resource
  worth batching.
- **`tokio::sync::Notify` / shared `watch` instead of per-waiter
  `oneshot`.** Rejected: per-waiter `oneshot` gives each waiter its own
  `ReadBarrierOutcome` delivery and natural cancellation semantics
  (dropped sender → `NoQuorum`), with no shared mutable outcome cell.

## Acceptance test plan

- `readindex_batch_serves_n_reads_with_one_round`: single-voter leader
  group (lease expired, no driver), test-only round gate holds the
  ReadIndex round open; fire N concurrent linearizable gets → all return
  `Ready` with the same `read_slot`; `read.readindex_rounds.c == 1` and
  `read.readindex_path.c == N`.
- `readindex_batch_propagates_no_quorum_to_all_waiters`: 3-member group
  (leader + 2 unreachable remotes, quorum 2), round gate; fire N
  concurrent gets → all return `Unavailable` (NoQuorum); one round.
- Existing R19 tests (`read_metrics_test.rs`) unchanged in semantics:
  `lease_path + readindex_path == linearizable get count`; the new
  `readindex_rounds.c` is additive.

## Files

- `crowkv/src/cluster/group.rs` — `PendingReadBarrier` struct,
  `pending_read_barrier` field on `PxGroup`, `readindex_rounds` handle
  on `ReadRegistryHandles`, registration in `set_metrics_registry`,
  test-only gate field + accessors under `test-util`.
- `crowkv/src/cluster/group_election.rs` — rewrite of the ReadIndex
  branch of `linearizable_read_barrier` (queue join / round-leader
  drain); `ReadBarrierOutcome` gains `Clone`.
- `crowkv/tests/store/readindex_batch_test.rs` — new integration test
  (success batching + no-quorum fan-out).
