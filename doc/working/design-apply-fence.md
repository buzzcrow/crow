<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# R35 — Apply Fence for Async Engine Apply

## Problem

R17 (`async_engine_apply`) moves `learn_chosen` (FFI memtable insert) off the
write critical path via `spawn_learn_chosen` (fire-and-forget
`tokio::spawn`), so the proposer returns `Chosen` before the local engine
has applied the value. This is the biggest remaining write-path win, but R17
ships default-off because it breaks the **Linearizable** read mode's
read-your-writes guarantee.

- **MinSlot already has the fence.** `resolve_read_point` gates on
  `contiguous_applied >= min_slot` (`px_kv_store.rs` L513) — the client
  passes `min_slot` = the slot its write returned, and the replica redirects
  to the leader until the local applied frontier catches up. R17 does not
  break MinSlot.
- **Linearizable lacks the fence.** `linearizable_read_barrier`
  (`group_election.rs` L668) captures `read_slot = contiguous_chosen`, but
  the engine get returns the latest **applied** value, not a
  `read_slot`-pinned value. With R17 off, `apply == learn` (V1):
  `contiguous_applied` and `contiguous_chosen` are set together in
  `update_frontier` (`learner.rs` L285-287), so the gap is invisible. With
  R17 on, a chosen slot may not yet be applied when the read lands — a
  linearizable read can miss a just-chosen write, violating linearizability.

## Current behavior (code-grounded)

- `linearizable_read_barrier` returns `ReadBarrierOutcome::Ready { read_slot }`
  with `read_slot = replica.contiguous_chosen()` captured **before** the
  lease/ReadIndex confirmation (L676). The barrier confirms leadership only;
  it never awaits the applied frontier.
- `resolve_read_point` (`px_kv_store.rs` L476) maps `Ready { read_slot }` to
  `ReadDecision::Serve { read_slot, safe_slot }`; `kv_get` then calls
  `engine_get_bytes` immediately (L53). No apply fence exists.
- `PxLearner::contiguous_applied` is an `AtomicU64` (`learner.rs` L78),
  advanced in `update_frontier` (L287) right after `contiguous_chosen`.
- `PxLearner` is held as `Arc<PxLearner>` on `PxLocalReplica::learner`
  (`local_replica.rs` L188, `pub`), so a `Notify` placed on the learner is
  reachable from the read path via `group.local_replica().learner`.
- R17 dispatch is at `group.rs` L1315: `if self.config.async_engine_apply {
  replica.spawn_learn_chosen(...) } else { replica.learn_chosen(...).await }`.
- `async_engine_apply` default is `false` (`config.rs` L412); the setter
  `set_async_engine_apply` (`group.rs` L277) is called nowhere today.
- Rebuild-carry: `rebuild_group_with_same_config` (`mgmt_api.rs` L1534)
  already calls `new_group.set_from_config(group.config())`, which carries
  the whole `CrowKVConfig` wholesale — including `async_engine_apply` and
  `wal_early_ack`. The per-flag carry blocks the R35 doc referenced are
  already gone (superseded by config unification); the carry requirement is
  already satisfied.

## Proposed approach

Two parts: a **write-side split** (so the chosen frontier stays current) and
a **read-side fence** (so a read waits for the apply).

### Write-side split (required — the R35 doc's read-only fence is insufficient)

The R35 doc assumed only `contiguous_applied` lags under R17, so a read-side
fence on `contiguous_applied >= read_slot` (with `read_slot =
contiguous_chosen`) would suffice. That assumption is wrong: the current
`spawn_learn_chosen` spawns the **entire** `learn` (`apply_entry` +
`update_frontier` + `record_dedup`), and `update_frontier` is the **only**
writer of `contiguous_chosen` (`learner.rs` L319, called only from `learn`
L425). So with R17 on, `contiguous_chosen` lags the just-chosen slot too —
the barrier captures `read_slot = contiguous_chosen < client_slot`, the
fence passes trivially, and the engine get returns the stale pre-write
value. Read-your-writes is still broken.

Fix: split `learn` so the **chosen frontier + dedup advance synchronously**
in the propose path (before `propose` returns `Chosen`), and **only the
engine apply + applied frontier are deferred**. This matches R17's stated
intent ("move `learn_chosen` (FFI + memtable insert) off the write critical
path") — the cheap atomic bookkeeping stays on the path; the FFI/memtable
insert is what moves off.

- `update_frontier` → split into `update_chosen_frontier(slot, term)`
  (advances `contiguous_chosen`, `last_chosen_slot`/`term`, chosen
  out-of-order drain — **no** `contiguous_applied`) and
  `advance_applied_frontier(slot)` (advances `contiguous_applied` with its
  own out-of-order drain + `notify_waiters`).
- New `applied_out_of_order: Mutex<BTreeMap<SlotIndex, ()>>` — spawned
  applies can complete out of order, so `contiguous_applied` needs the same
  drain pattern `contiguous_chosen` already has. On the leader, propose
  slots are sequential so this stays empty in steady state; it matters only
  under spawn reordering.
- `Learner::learn` (V1 sync path — followers, restore, R17-off leader):
  `apply_entry` → `update_chosen_frontier` → `advance_applied_frontier` →
  `record_dedup`. Both frontiers advance together after the sync apply
  (unchanged V1 semantics).
- `spawn_learn_chosen` (R17 leader path): `update_chosen_frontier` +
  `record_dedup` synchronously, then spawn `apply_entry` +
  `advance_applied_frontier`. `contiguous_chosen` is current before
  `propose` returns; `contiguous_applied` lags by the spawned apply.

### Read-side fence (Linearizable-only)

After the barrier confirms leadership and captures `read_slot =
contiguous_chosen`, await `contiguous_applied >= read_slot` before serving
the engine get. With the write-side split, `read_slot >= client_slot`, so
the fence guarantees the client's write is applied before the read serves.

- **Await/wake mechanism — `Notify`-per-learner.** `advance_applied_frontier`
  calls `notify_waiters()` whenever `contiguous_applied` advances. The fence
  registers the `notified()` future **before** loading `contiguous_applied`
  (register-before-load) so a wake that fires between load and registration
  is not missed — the subsequent `Acquire` load observes the `Release`-stored
  new value and the fence returns without waiting.
- **Fast path.** When R17 is off (or the slot is already applied),
  `contiguous_applied >= read_slot` holds at the instant the barrier
  resolves, so the fence is one `AtomicU64::load(Acquire)` + compare and
  returns. No wait, no wake.
- **Slow path.** Only when R17 is on AND a read races a just-chosen-but-
  not-applied write does the fence actually await. The wait duration is the
  apply latency (memtable insert, µs) — exactly the latency R17 removed from
  the write path. The fence redistributes that µs to an occasional read; it
  does not add new latency.
- **R27 ReadIndex batching.** The fence applies after the barrier resolves,
  per-read. Batched reads share the barrier outcome (the round leader's
  `read_slot` floor) but each awaits its own `contiguous_applied >=
  read_slot` check; `notify_waiters` wakes all waiters together.
- **MinSlot untouched** — it already gates on `contiguous_applied`.

### Control surface (prerequisite to enabling R17)

1. Flip `CrowKVConfig::default().async_engine_apply` to `true`. Keep the
   `PxGroup::new` test-path override and `CrowKVConfig::for_tests()` at
   `false` so direct-`PxGroup::new` / `for_tests` test groups stay
   synchronous and deterministic (mirrors the existing `wal_early_ack:
   false` test-path pattern). Production overwrites via `set_from_config`;
   the `set_async_engine_apply` setter remains for tests that opt in.
2. Rebuild-carry is already done via `set_from_config(group.config())` — no
   change needed.
3. `wal_early_ack` default is already `true` and already carried across
   rebuild — no change needed (R16b's enable is gated on T1, not R35).

## Alternatives considered

- **`NotAppliedYet { slot }` hint (client retries instead of blocking).**
  The R35 doc names this as a fallback escape hatch for a tail under bursty
  read+write load, not the default. Not implemented now — the bounded wait is
  sufficient; the hint path can be added later if a tail appears in
  benchmarks.
- **Wait queue keyed on the awaited slot.** More precise wakeups but more
  state. The `Notify`-per-learner wakes all waiters on any advance; since
  applies are contiguous and fast, the extra woken waiters re-check and
  return immediately. Simpler and correct.
- **Fence inside `linearizable_read_barrier`.** Co-locates fence with
  barrier, but batched waiters receive the outcome via oneshot and return
  before any fence could run. Putting the fence in `resolve_read_point`
  (after the barrier, before `Serve`) covers both lease and ReadIndex paths
  uniformly, including batched waiters.

## Acceptance test plan

- `async_engine_apply` defaults to `true` in `CrowKVConfig::default()`;
  `for_tests()` and the `PxGroup::new` test path remain `false`.
- A dedicated test with `set_async_engine_apply(true)`: a `put` followed by
  a linearizable `get` of the same key on the leader returns the written
  value (read-your-writes holds through the fence).
- Existing read-path tests (ReadIndex batching, lease fast path, MinSlot)
  pass unchanged.
- No regression: read-path / write-path benchmarks at sentinel configs show
  no throughput/latency regression vs R17-off (characterized via the new
  `apply_fence` latency metric; the fast path is a single atomic load).

## Files

- `crowkv/src/paxos/learner.rs` — split `update_frontier` into
  `update_chosen_frontier` + `advance_applied_frontier`; `applied_out_of_order`
  map; `Notify` field + `notify_waiters` in `advance_applied_frontier`;
  `await_applied` method; `learn` uses the split; `pub(crate)` the pieces
  `spawn_learn_chosen` needs.
- `crowkv/src/cluster/local_replica.rs` — `spawn_learn_chosen` does the
  chosen-frontier + dedup sync, spawns only apply + applied-frontier;
  `await_apply_fence` helper delegating to the learner.
- `crowkv/src/cluster/px_kv_store.rs` — await the fence in
  `resolve_read_point` Linearizable `Ready` arm before returning `Serve`.
- `crowkv/src/cluster/group.rs` — `apply_fence` metric handle in
  `ReadRegistryHandles`; `PxGroup::new` test-path `async_engine_apply: false`.
- `crowkv/src/common/config.rs` — flip `async_engine_apply` default to
  `true`; `for_tests()` explicit `false`.
- `crowkv/tests/...` — R35 read-your-writes test.
