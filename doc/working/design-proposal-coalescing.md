<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# R36 — Server-side Proposal Coalescing (Design)

## Problem

Each client `PUT`/`DELETE` is its own Paxos proposal: one slot, one
quorum RPC round, one WAL record, one learner apply. WAL batch
aggregation already amortizes `fsync` across concurrent proposals (K
records → 1 `fdatasync`), and R16b removed the leader's local `fsync`
from the critical path, so the remaining per-proposal fixed cost is the
**quorum RPC round** (plus per-accept follower CPU and learner apply).
The write-flow sweep shows throughput plateaus at ~29K (Intel) / ~48K
(M5 Pro) once the consensus pipeline saturates, independent of the
inflight window above MI=16. The bottleneck is the per-proposal quorum
RPC rate, not `fsync`.

The `Batch` payload format already supports multiple ops per slot and
`kv_batch_write` exposes it, but there is no server-side coalescer:
concurrent single-key proposes each take a distinct slot and pay the
full quorum round. Under N concurrent writers the leader issues N
parallel quorum rounds where one round carrying N keys would suffice.

## Current behavior (code-grounded)

- `PxKvStore::propose_and_respond` (`px_kv_store.rs:594`) encodes one
  op into a payload and calls `PxGroup::propose(payload, client_id,
  seq)`.
- `PxGroup::propose` (`group.rs:1153`) is called **only** from the
  client path (repair/election use `learn_chosen`/`run_accept_phase`
  directly, never `propose`). It: (1) leadership gate, (2) dedup
  lookup, (3) inflight permit, (4) slot alloc, (5) paxos retry loop,
  (6) `learn_chosen(entry, client_id, seq)` on chosen.
- Dedup `(client_id, seq) → slot` is recorded on **both** leader and
  followers: the leader records in `learn_chosen`/`spawn_learn_chosen`
  (`local_replica.rs:1217`/`1234`); followers record in
  `handle_accept_inner` (`px_service.rs:547`) which calls
  `learn_chosen` with the `(client_id, seq)` carried on the `Accept`
  RPC (`AcceptRequest.client_id/seq`, proto fields 9/10). This is how
  dedup survives leader failover: a follower that accepted a value
  has the dedup entry when it later becomes leader.
- The `Accept` RPC carries a **single** `(client_id, seq)`.
- Payload format (`kv/op.rs::Batch::decode`):
  `[op_count:u8]([is_delete:u8][klen:u32 LE][key][vlen:u32 LE][value?])*`.
  Each op body is self-delimited, so K single-op payloads merge by
  prepending `K` and concatenating the op bodies (dropping each
  payload's leading count byte).

## Proposed approach

A bounded micro-batcher (the **coalescer**) at the `PxGroup::propose`
entry merges concurrent single-key proposes into one multi-key Paxos
proposal — one slot, one quorum RPC, one learner apply — amortizing
the per-proposal fixed cost across many keys.

### Coalescer mechanics

Per-group state on `PxGroup`:

```
coalescer: parking_lot::Mutex<Option<PendingBatch>>
```

```
struct PendingBatch {
    op_bodies: Vec<u8>,   // concatenated op bodies (no count prefix)
    tags: Vec<DedupTag>,
    waiters: Vec<oneshot::Sender<ProposeResult>>,
    timer: JoinHandle<()>, // armed sleep(window_us) -> flush
}
```

`propose` flow (refactored):

1. Leadership gate (as today).
2. Dedup lookup (as today) — a hit returns the cached slot
   immediately, never enters a batch.
3. If `coalesce_window_us == 0` (disabled): call `propose_inner(payload,
   &[tag])` directly — bit-identical to today's path.
4. Else (coalescing on): lock the coalescer:
   - No pending batch → start one with this op, arm a timer task
     (`sleep(window_us)` then `flush`). Register a waiter oneshot,
     return its `Receiver`.
   - Pending batch exists → append this op's body + tag, register a
     waiter. If `op_count == coalesce_max_keys`: cancel the timer,
     take the batch, spawn the flush. Return the `Receiver`.
5. `await` the waiter's `ProposeResult`.

`flush` (called by both the timer task and the max_keys path):

1. Lock coalescer, `take` the pending batch (`None` wins the race; the
   loser no-ops).
2. Build merged payload: `[op_count as u8][op_bodies]`.
3. `tokio::spawn` `propose_inner(Arc::clone(&group), payload, tags)`.
   On completion, fan the `ProposeResult` (now `Clone`) to all
   waiters.

Spawning the flush (rather than running it inline) decouples the
triggering caller from the paxos round and lets the next batch start
collecting immediately. Multiple in-flight batches are bounded by the
existing inflight gate (`max_inflight` permits; each batch holds one).

### Backpressure

While a flush is in flight the pending slot is `None`, so new ops
start a fresh batch immediately. The inflight gate caps concurrent
paxos rounds (batches); a flush whose `propose_inner` blocks on the
gate simply waits (Queue policy). The number of awaiting `propose`
calls is bounded by client concurrency (the RPC handler tasks), same
as today. Coalescing *reduces* the inflight permits consumed (one per
batch, not one per key).

### Dedup threading (the K-tag problem)

A coalesced batch carries K `(client_id, seq)` tags but one slot. To
preserve the existing dedup-on-all-replicas invariant (so a follower
that becomes leader can return cached slots for retried coalesced
ops), all K tags must reach every replica that accepts the batch.

**Decision: extend the `Accept` RPC.** Add to `AcceptRequest`:

```proto
message DedupTag { uint64 client_id = 1; uint64 seq = 2; }
// in AcceptRequest:
repeated DedupTag dedup_tags = 13;
```

Threading:

- `propose_inner(payload, tags: &[DedupTag])` replaces the single
  `(client_id, seq)` in the paxos loop.
- `run_accept_phase(.., tags: &[DedupTag], ..)` →
  `send_accept(entry, tags, ..)` populates `dedup_tags` (and keeps
  legacy `client_id`/`seq` set to the first tag, or 0, for
  backward-compat with older followers during rolling upgrade).
- Follower `handle_accept_inner`: if `dedup_tags` non-empty, use it;
  else fall back to legacy `client_id`/`seq` (older leader during
  rolling upgrade). Calls `learn_chosen_batch(entry, tags)`.
- `learn_chosen_batch` / `spawn_learn_chosen_batch` (new, on
  `PxLocalReplica`) and `Learner::learn` (signature change to
  `&[DedupTag]`) apply the batch once and `record_dedup` for each tag.

`DedupTag { client_id: u64, seq: u64 }` (non-optional; `client_id == 0`
is the existing no-dedup sentinel). Repair/election/restore paths pass
`&[]` (no tags → no dedup recording, identical to today's `None, None`).

### Config

`PaxosConfig` gains two fields (compile-time `const DEFAULT`):

- `coalesce_window_us: u64` — max wait to fill a batch. `0` disables
  (default `0` = current behavior; opt-in).
- `coalesce_max_keys: usize` — max ops per batch (cap 255, the payload
  count byte). Default e.g. 32.

CLI: `--coalesce-window-us`, `--coalesce-max-keys` on `crowkv-server`,
applied in `main.rs` into `config.paxos`. Wired into the group via
`set_from_config` (the coalescer reads `self.config.paxos.*`).

### Correctness

- **Dedup**: each coalesced tag is recorded on leader + all accepting
  followers → a retried `(client_id, seq)` returns the shared slot on
  any replica that has it; outside the window, safe to re-propose
  (per-key highest-slot-wins makes a re-propose idempotent at the
  engine level). Identical guarantee shape as today.
- **Per-key ordering**: unchanged — all ops in a batch share one slot;
  across batches, per-key highest-slot-wins applies as today.
- **`ProposeResult::Chosen { slot }` contract**: every coalesced
  waiter receives the same slot. `ProposeResult` gains `Clone`.
- **`coalesce_window_us = 0`**: `propose` calls `propose_inner` with a
  1-tag slice — the paxos loop is unchanged from today; the only
  difference is the `&[DedupTag]` vs `(Option, Option)` plumbing,
  which records the same single dedup entry. No behavior change.
- **Leadership**: re-checked inside `propose_inner`; a step-down
  between batch collection and flush surfaces as `NotLeader` to all
  waiters.
- **Empty batch / shutdown**: the timer task holds an `Arc<PxGroup>`;
  on shutdown the pending batch is dropped (waiters get a closed
  oneshot → mapped to `Err`). The coalescer honors `tenure_cancel` so
  a step-down does not fire a flush into a stale tenure.

## Alternatives considered

- **Leader-only dedup recording** (no proto change): record all K tags
  only on the leader; send `client_id=0` to followers. Smaller scope
  (matches the R36 doc's file list) but regresses follower dedup for
  coalesced batches — a follower becoming leader would re-propose
  retried coalesced ops (safe but wasteful, and a subtle behavior
  change). Rejected: the project wires dedup through the accept RPC
  precisely so it survives failover; silently dropping that for
  coalesced batches is a correctness-property regression.
- **Encode dedup tags in the KV payload**: couples the consensus
  dedup layer with the `Batch` payload format and requires stripping
  tags before engine apply. Rejected: layering violation.
- **Dedicated coalescer driver task (mppsc loop)**: a long-lived task
  per group receiving ops and ticking. Rejected: the mutex + armed
  timer is simpler, needs no extra channel, and still allows parallel
  in-flight batches via spawned flushes.

## Acceptance test plan

- `coalesce_window_us = 0`: existing paxos tests pass unchanged (no
  regression path).
- Coalescing on, K concurrent single-key puts to distinct keys: all
  return `Chosen` with the **same** slot; all keys readable
  post-apply.
- Dedup: retry a `(client_id, seq)` that was coalesced → returns the
  cached slot without a new paxos round (on the leader and on a
  follower promoted to leader).
- Per-key ordering: two ops on the same key in separate batches land
  in submission order (highest-slot-wins).
- Backpressure: `coalesce_max_keys` caps batch size; inflight gate
  caps concurrent batches.
- Benchmark sweep (`tools/bench-write-*.sh`): record
  throughput/latency vs `coalesce_window_us` × `coalesce_max_keys` ×
  `max_inflight` in `write-flow-analysis.md`; confirm no 1T
  regression (single writer can't fill the window).
