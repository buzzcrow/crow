<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R27: ReadIndex batching for linearizable reads

**Problem**: When the leader's read lease has expired, every
linearizable read falls back to the ReadIndex path — one heartbeat
round per read. Under a burst of linearizable reads with an expired
lease, this multiplies heartbeat traffic: N reads → N heartbeat
rounds, each awaiting quorum ack. The latency per read is also
dominated by the round-trip rather than the engine get.

Documented as "not yet implemented" in `../design/design-leader-election.md`
§7.2. Full gap analysis: G9 in
[`read-flow-analysis.md`](../working/read-flow-analysis.md).

**Approach**: Consensus-layer change to `linearizable_read_barrier`.

- The leader maintains a queue of pending ReadIndex barriers
  (`Vec<oneshot::Sender<ReadBarrierOutcome>>`).
- When a ReadIndex request arrives and a heartbeat round is already
  in flight, the request is enqueued rather than triggering a new
  round.
- When the in-flight round completes with quorum, all pending reads
  are resolved at once at the `contiguous_chosen` captured *before*
  the round (same correctness argument as the single-read case: every
  slot ≤ this is already chosen and applied on the leader).
- If the round steps down (higher term) or fails quorum, all pending
  reads receive `NotLeader` / `NoQuorum` respectively.
- The lease fast path is unchanged — lease-valid reads still serve
  immediately without queueing.

**Concept change**: none — the correctness argument is identical to
the single-read ReadIndex path (the read slot is captured before the
heartbeat; quorum confirms leadership at this term). This is purely a
latency/throughput optimization that batches the same kind of round
that already runs per-read.

**Priority**: Medium — only matters under high linearizable read load
with an expired lease; the lease fast path already covers the common
case. Worth doing after R19 so the batching effect is measurable
(`read.readindex_path.c` stays the same, `read.barrier.l` avg should
drop toward one RTT amortized across batch size).

**Complexity**: Medium — pending-barrier queue, wiring into
`linearizable_read_barrier`, careful handling of the
step-down/quorum-failure fan-out to all waiters. No protocol change;
the heartbeat round itself is unchanged.

**Dependencies**: R19 (Read performance profiling and metrics) — need
`read.readindex_path.c` and `read.barrier.l` to validate that
batching reduces heartbeat traffic and amortizes barrier latency.

**Files**: `crowkv/src/cluster/group_election.rs`
(`linearizable_read_barrier` — pending-barrier queue, batch
resolution on round completion), `crowkv/src/cluster/local_replica.rs`
(ReadRegistryHandles — optional new counter for batched-barrier
batch size, if useful for validation).

**Acceptance**:
- Under a burst of N concurrent linearizable reads with an expired
  lease, one heartbeat round serves all N (not N rounds).
- `read.readindex_path.c` is unchanged (same number of reads take the
  ReadIndex path), but heartbeat RPC count drops by ~N×.
- `read.barrier.l` avg drops toward one heartbeat RTT amortized across
  the batch (rather than one RTT per read).
- Lease-path reads are unaffected (still immediate, no queueing).
- Correctness: all batched reads return the same `read_slot` (the
  `contiguous_chosen` captured before the round); step-down and
  no-quorum propagate to all waiters.
