<!-- Copyright 2026-present buzzcrow <buzzcrow:126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# Design: Value-less Catch-up + Background Apply Loop (R63)

Depends on: R53 (dedicated heartbeat channel)
Upstream: `doc/design/kv/design-crow-kv-leader-election.md` §5, `doc/design/kv/design-crow-kv-rpc.md` §3

## Problem

Two inefficiencies in the follower catch-up and apply path:

### 1. Full-payload catch-up replay

When a follower's `contiguous_applied` lags behind the leader's
`contiguous_chosen`, the heartbeat round's inline catch-up replay
(`group_election.rs:527–630`) sends full `send_accept` RPCs with 16 KiB
payloads for up to 64 slots per round. In the common case the follower
already has the value in its acceptor slot — it accepted the original
proposal; only the engine apply is lagging. Only slots rejected during
election churn are truly missing.

Cost: 64 × 16 KiB = ~1 MiB of wire bytes per round-trip when 0 slots
actually need the payload. The serialization/deserialization CPU cost
is proportional.

### 2. Synchronous apply in the heartbeat hot path

The follower's `handle_heartbeat` calls
`apply_committed_up_to(commit_slot).await` inline
(`local_replica.rs:1523`), which walks the contiguous-applied prefix
and calls `learner.learn()` (engine apply) for each slot. Engine apply
is synchronous (`KVFuture::ready`), so each slot blocks the heartbeat
handler.

Cost: with 64 lagging slots at ~10–50 µs per apply, a single heartbeat
reply spends 0.6–3 ms in the apply loop — delaying the heartbeat reply
and inflating the leader's observed round-trip time.

## Current flow (baseline at `d001428`)

### Catch-up replay (`group_election.rs:527–630`)

Inside `run_heartbeat_round`, after collecting heartbeat replies:

- For each peer where `hb.contiguous_applied < committed_safe_slot`:
  - Loop `slot = hb.contiguous_applied+1 ..= catchup_end` (bounded by
    `MAX_CATCHUP_PER_ROUND = 64`):
    - Read `replica.accepted_at(slot).await` — if missing, break
    - `remote.send_accept(&entry, &[], ...).await` — full payload RPC
    - On `Rejected`: escalate ballot above peer's promise, retry once
    - On `Accepted`: continue to next slot
    - On `TermStale`/`EpochMismatch`/`Err`: break

### Follower apply (`local_replica.rs:1276–1285`)

`apply_committed_up_to(commit_slot)`:
- Walk `next = contiguous_applied+1 ..= commit_slot`
- For each slot: `acceptor.accepted_at(next)` → if `None`, **break**
  (first gap stops the walk)
- `learner.learn(entry, &[]).await` — apply + advance both frontiers

Called from `handle_heartbeat` (`local_replica.rs:1523`).

### Follower accept (`px_service.rs:564–566`)

`handle_accept_inner`:
- After `on_accept` succeeds: `replica.learn_chosen(&entry, &dedup_tags).await`
  — applies to engine + advances frontiers synchronously

### ChosenNotice (`px_service.rs:436–454`)

Single-slot `ChosenNotice` frame handler:
- `replica.note_chosen(slot, term)` — advances `last_chosen_slot` only
  (no apply, no `contiguous_chosen` advance)

## Proposed approach

### Change 1: BatchChosenNotice (value-less catch-up)

Add a fire-and-forget `BatchChosenNotification` frame to the
`LearnerStreamRequest` oneof. The leader sends a single lightweight
message covering the lagging slot range, carrying only
`(start_slot, end_slot, term, leader_id)` — no per-slot payload.

#### Proto (`pxos.proto`)

```protobuf
message BatchChosenNotification {
  uint32 version = 1;
  uint64 group_id = 2;
  uint64 start_slot = 3;
  uint64 end_slot = 4;
  uint64 term = 5;
  uint64 leader_id = 6;
}

message LearnerStreamRequest {
  oneof frame {
    AcceptRequest           accept       = 1;
    HeartbeatRequest        heartbeat    = 2;
    ChosenNotification      chosen       = 3;
    BatchChosenNotification batch_chosen = 4;  // new
  }
}
```

#### Sender side

- `learner_stream.rs`: `send_batch_chosen(batch)` — fire-and-forget
  dispatch with `reply_tx: None` (same pattern as existing
  `ChosenNotification`).
- `remote_replica.rs`: `send_batch_chosen_notice(start, end, term,
  leader_id, group_id)` — builds the `BatchChosenNotification` and
  calls `send_batch_chosen`.

#### Follower side (`px_service.rs`)

New `BatchChosen` frame handler in the LearnerStream bidi loop:

```rust
learner_stream_request::Frame::BatchChosen(batch) => {
    if let Some(group) = store.get_group(batch.group_id) {
        let replica = group.local_replica();
        for slot in batch.start_slot..=batch.end_slot {
            if replica.accepted_at(slot).await.is_some() {
                replica.learner.update_chosen_frontier(slot, batch.term);
            }
        }
        // Tell the apply loop it can apply up to end_slot.
        replica.advance_known_commit_slot(batch.end_slot);
        replica.wake_apply_loop();
    }
    None  // fire-and-forget, no reply frame
}
```

For each slot the follower already has in its acceptor:
`update_chosen_frontier(slot, term)` marks it chosen. Missing slots
remain gaps — the leader's full-accept phase fills them.

#### Leader side (`group_election.rs`)

In the catch-up replay section of `run_heartbeat_round`, **before** the
full-accept loop:

```rust
// Phase 1: value-less batch chosen notice
remote.send_batch_chosen_notice(
    peer_applied + 1, catchup_end, leader_term, replica.id, group_id,
);
// Phase 2: full accepts for missing slots (existing logic)
for slot in peer_applied+1..=catchup_end { ... send_accept ... }
```

The batch notice is fire-and-forget — no round-trip wait. The
full-accept loop runs immediately after. Slots the follower already has
are re-accepted (idempotent CAS, cheap). Slots the follower is missing
get the real value. The batch notice ensures the follower's chosen
frontier advances for present slots without waiting for the full-accept
round-trip.

#### Why not a two-phase request-reply protocol

The R63 backlog doc originally proposed a two-phase protocol where the
follower replies with a "need value" list and the leader sends full
accepts only for those slots. This was rejected because:

- The fire-and-forget batch notice + unconditional full-accept fallback
  is simpler (no new reply frame type, no pending-state tracking on
  either side).
- The full-accept fallback is idempotent for present slots (cheap CAS),
  so the extra payload transfer for already-present slots is negligible
  compared to the batch notice's savings on the chosen-frontier advance.
- The batch notice's main win is advancing the chosen frontier
  immediately so the apply loop can start processing present slots
  without waiting for the full-accept round-trip.

### Change 2: Background apply loop

Decouple engine apply from the heartbeat handler.

#### New fields on `PxLocalReplica` (`local_replica.rs`)

```rust
known_commit_slot: Arc<AtomicU64>,       // latest commit point from heartbeat/notice
apply_notify: Arc<tokio::sync::Notify>,  // wakes the apply loop
apply_loop_handle: parking_lot::Mutex<Option<JoinHandle<()>>>,  // for cancellation
```

#### `handle_heartbeat` change (`local_replica.rs:1492`)

Change from async to sync. Replace:

```rust
self.apply_committed_up_to(req.committed_safe_slot).await;
self.note_heartbeat_received();
self.deadline_reset_signal.notify_one();
```

With:

```rust
self.deadline_reset_signal.notify_one();  // reset timer first (liveness)
self.note_heartbeat_received();
self.known_commit_slot.fetch_max(req.committed_safe_slot, Ordering::AcqRel);
self.apply_notify.notify_one();
```

The reply returns immediately with the current `contiguous_applied`
(which may lag behind `known_commit_slot`). The leader's catch-up
replay handles convergence; the background loop advances
`contiguous_applied` at its own pace.

**Key ordering**: `deadline_reset_signal` is signalled **before** the
apply loop is woken. The follower has confirmed the leader is alive
(term check passed) and must not time out regardless of how long the
background apply takes.

#### `handle_accept_inner` change (`px_service.rs:564`)

Replace:

```rust
replica.learn_chosen(&entry, &dedup_tags).await;
```

With:

```rust
let learner = replica.learner.clone();
learner.update_chosen_frontier(entry.slot, entry.term);
learner.record_dedup_tags(&dedup_tags, entry.slot);
replica.advance_known_commit_slot(entry.slot);
replica.wake_apply_loop();
```

The accept path advances the chosen frontier + dedup synchronously
(cheap atomics) and defers engine apply to the background loop. This
keeps the serial LearnerStream handler fast.

**Critical correctness point**: the accept path **must** advance
`known_commit_slot` (not just `contiguous_chosen`). Without this, a
replica that accepts slots as a follower and then wins an election
stops receiving heartbeats (leaders don't get heartbeats) and the
apply loop never learns about those slots — a linearizable read fencing
on `contiguous_chosen` hangs forever. This was the root cause of the
test hangs in the previous R63 attempt.

#### `spawn_apply_loop` (`local_replica.rs`)

New method. Spawns a background task:

```rust
pub fn spawn_apply_loop(&self, cancel: &CancellationToken) {
    let known = Arc::clone(&self.known_commit_slot);
    let notify = Arc::clone(&self.apply_notify);
    let learner = Arc::clone(&self.learner);
    let acceptor = Arc::clone(&self.acceptor);
    let cancel = cancel.clone();
    let handle = tokio::spawn(async move {
        loop {
            if cancel.is_cancelled() { return; }
            let target = known.load(Ordering::Acquire);
            let current = learner.contiguous_applied();
            if current >= target {
                notify.notified().await;
                continue;
            }
            // Collect entries from acceptor, skip missing slots
            let mut entries = Vec::new();
            let mut next = current + 1;
            let end = target.min(next + MAX_APPLY_PER_BATCH - 1);
            while next <= end {
                if let Some(entry) = acceptor.accepted_at(next) {
                    entries.push(entry);
                }
                next += 1;  // skip missing, continue
            }
            if entries.is_empty() {
                notify.notified().await;
                continue;
            }
            let learner_clone = Arc::clone(&learner);
            let cancel_clone = cancel.clone();
            tokio::task::spawn_blocking(move || {
                for entry in entries {
                    if cancel_clone.is_cancelled() { return; }
                    learner_clone.apply_entry_blocking(entry.slot, &entry.payload);
                    learner_clone.advance_applied_frontier(entry.slot);
                }
            }).await.ok();
        }
    });
    *self.apply_loop_handle.lock() = Some(handle);
}
```

Key differences from the old `apply_committed_up_to`:
- **Skip-and-continue**: missing slots are skipped (not breaking), so a
  single gap doesn't block the entire apply backlog. `contiguous_applied`
  stays at the gap until it's filled; subsequent available slots are
  applied via `advance_applied_frontier` (which handles out-of-order
  applies).
- **`spawn_blocking`**: engine apply is synchronous (`KVFuture::ready`),
  so it runs on the blocking pool (512 threads), not an async worker.
- **Bounded batch**: `MAX_APPLY_PER_BATCH` (e.g. 64) limits the
  blocking-pool task size, yielding between batches so cancellation
  and new `known_commit_slot` advances are observed.

#### Lifecycle

- **Spawn**: lazily on first heartbeat or election-driver start. The
  election driver calls `replica.spawn_apply_loop(&tenure_cancel)` when
  entering leader or follower state.
- **Stop**: `stop_apply_loop()` aborts the `JoinHandle`. Called in
  `PxLocalReplica::shutdown()`.

#### `apply_committed_up_to` removal

The old `apply_committed_up_to` is removed — its logic is absorbed into
the background apply loop. The `handle_heartbeat` call site is replaced
by the store-and-signal pattern above.

## Alternatives considered

### A: Keep synchronous apply, just add BatchChosenNotice

Rejected — the synchronous apply in `handle_heartbeat` blocks the
tokio worker for 0.6–3 ms per heartbeat. Under burst load this
compounds and delays heartbeat replies, inflating the leader's observed
round-trip time. The background loop moves this to `spawn_blocking`,
freeing the async worker.

### B: Two-phase request-reply for value-less catch-up

Rejected — see "Why not a two-phase request-reply protocol" above. The
fire-and-forget + unconditional full-accept fallback is simpler and
the extra CAS cost for present slots is negligible.

### C: Apply in `handle_accept_inner`, only defer for heartbeat-driven apply

Rejected — the accept path runs in the serial LearnerStream handler.
A synchronous apply there blocks the handler, starving other frames on
the same stream (including heartbeat frames during rolling upgrades
where the LearnerStream still carries heartbeats). Deferring to the
background loop keeps the handler fast.

## Acceptance criteria

- `BatchChosenNotification` frame added to proto; round-trips between
  leader and follower.
- Follower `BatchChosen` handler advances `contiguous_chosen` for
  present slots without payload transfer.
- `handle_heartbeat` returns immediately (no `await` on engine apply);
  `known_commit_slot` is advanced via `fetch_max`.
- `handle_accept_inner` advances `known_commit_slot` + wakes apply loop
  (not just `contiguous_chosen`).
- Background apply loop skips missing slots and continues applying
  subsequent available slots.
- All existing `crow-kv` tests pass (541 tests at baseline).
- New tests:
  - Apply loop skips a missing slot and applies subsequent slots.
  - BatchChosenNotice advances chosen frontier for present slots.
  - Heartbeat reply is not delayed by engine apply (verify
    `contiguous_applied` in reply may lag `known_commit_slot`).
  - Replica accepts slots as follower, wins election, apply loop
    applies the accepted slots (the deadlock regression test).
