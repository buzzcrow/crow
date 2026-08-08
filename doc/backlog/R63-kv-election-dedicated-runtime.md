<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R63: Value-less Catch-up + Background Apply Loop

**Problem**: Two inefficiencies in the follower catch-up and apply path:

1. **Full-payload catch-up replay.** When a follower's
   `contiguous_applied` lags behind the leader's `contiguous_chosen`,
   the heartbeat round's inline catch-up replay (lines 527–630 of
   `group_election.rs`) sends full `send_accept` RPCs with 16 KiB
   payloads for up to 64 slots per round. In the common case the
   follower already has the value in its acceptor slot — it accepted
   the original proposal; only the engine apply is lagging. Only slots
   rejected during election churn are truly missing. This wastes
   bandwidth and CPU on serialization for the common case: 64 × 16 KiB
   = ~1 MiB of wire bytes per round-trip when 0 slots actually need the
   payload.

2. **Synchronous apply in the heartbeat hot path.** The follower's
   `handle_heartbeat` calls `apply_committed_up_to(commit_slot).await`
   inline (line 1523 of `local_replica.rs`), which walks the
   contiguous-applied prefix and calls `learner.learn()` (engine apply)
   for each slot. Engine apply is synchronous (`KVFuture::ready`), so
   each slot blocks the heartbeat handler. With 64 lagging slots at
   ~10–50 µs per apply, a single heartbeat reply can spend
   0.6–3 ms in the apply loop — delaying the heartbeat reply and
   inflating the leader's observed round-trip time. Under burst write
   load this compounds: the leader's heartbeat round takes longer, the
   lease renewal window shrinks, and in the worst case the leader stops
   sending heartbeats fast enough to maintain its lease.

**Solution**: Two changes, in implementation order:

1. **Value-less catch-up (BatchChosenNotice)** — add a fire-and-forget
   `BatchChosenNotification` frame to the `LearnerStreamRequest` oneof.
   The leader sends a single lightweight message covering the lagging
   slot range `[follower_applied+1, leader_chosen]`, carrying only
   `(start_slot, end_slot, term, leader_id)` — no per-slot payload. The
   follower checks its local acceptor for each slot in the range:

   - **Has value** (`acceptor.accepted_at(slot).is_some()`): advance the
     chosen frontier via `update_chosen_frontier(slot, term)`. The value
     is already in local state; the apply path picks it up from the
     acceptor slot — no payload transfer needed.
   - **Missing value**: leave as a gap. The leader's full-accept phase
     (existing `send_accept` with payload) fills these slots. In steady
     state this is zero slots; only election-churn-rejected slots need
     it.

   The catch-up replay in `run_heartbeat_round` sends the batch notice
   first, then falls back to full accepts for the same range. Slots the
   follower already has are re-accepted (idempotent CAS, cheap); slots
   the follower is missing get the real value. The batch notice ensures
   the follower's chosen frontier advances for present slots without
   waiting for the full-accept round-trip.

   - **Infrastructure already exists**: `ChosenNotice`
     (`fan_out_chosen_notice` / `send_chosen_notice`) is already a
     payload-less per-slot notice over the LearnerStream. The batch
     version extends this to a range with a single frame.
   - **No size threshold needed**: the batch notice is one frame
     covering the entire range (~40 bytes vs. 64 × 16 KiB). Even for
     1 KiB values, a 64-slot range saves 64 payload serializations and
     ~64 KiB of wire bytes for one cheap fire-and-forget send.

2. **Background apply loop** — decouple engine apply from the heartbeat
   handler. The follower's `handle_heartbeat` currently calls
   `apply_committed_up_to(commit_slot).await` synchronously. Replace
   this with a background `spawn_apply_loop` task that applies committed
   entries at its own pace, driven by a `known_commit_slot` atomic
   (`AtomicU64`) and an `apply_notify` (`tokio::sync::Notify`).

   - `handle_heartbeat` stores `committed_safe_slot` via
     `fetch_max` into `known_commit_slot` and signals `apply_notify`,
     then returns immediately with the current `contiguous_applied`
     (which may lag behind `known_commit_slot`). The heartbeat reply is
     no longer blocked on engine apply.
   - The background apply loop reads `known_commit_slot`, collects
     entries from `acceptor.accepted_at(slot)` for the contiguous range,
     and applies them via `spawn_blocking` (engine apply is synchronous
     — `KVFuture::ready` — so it must not block an async worker thread).
   - **Skip-and-continue for missing slots**: the current
     `apply_committed_up_to` breaks on the first missing slot. The
     background loop skips missing slots (they remain gaps until the
     leader's full-accept catch-up fills them) and continues applying
     subsequent available slots, so a single gap doesn't block the
     entire apply backlog.
   - **`handle_accept_inner` in `px_service.rs`**: the accept path
     currently calls `learn_chosen()` (apply + advance frontier)
     synchronously. Change it to advance the chosen frontier
     (`update_chosen_frontier` + `record_dedup_tags`) only, and advance
     `known_commit_slot` + wake the apply loop. This keeps the serial
     LearnerStream handler fast so it doesn't starve the tokio runtime
     and block heartbeat processing.
   - **Apply loop lifecycle**: spawned lazily (on first heartbeat or
     election-driver start), aborted on shutdown. The loop owns a
     `CancellationToken` for clean cancellation.

   - **Why not keep synchronous apply**: the heartbeat handler runs on
     the tokio worker pool. A 0.6–3 ms synchronous apply blocks the
     worker thread, delaying other tasks on the same worker (including
     other heartbeat replies if the runtime is undersized). The
     background loop moves this to `spawn_blocking` (blocking pool,
     512 threads by default), freeing the async worker.

**Scope**:
- `lib/crow-kv/src/rpc/proto/pxos.proto` — add `BatchChosenNotification`
  message + `batch_chosen` frame in `LearnerStreamRequest` oneof.
- `lib/crow-kv/src/cluster/learner_stream.rs` — add
  `send_batch_chosen()` (fire-and-forget, no reply channel).
- `lib/crow-kv/src/cluster/remote_replica.rs` — add
  `send_batch_chosen_notice()` wrapper.
- `lib/crow-kv/src/rpc/px_service.rs` — handle `BatchChosen` frame:
  loop over slots, `update_chosen_frontier` for present ones, advance
  `known_commit_slot`, wake apply loop. Change `handle_accept_inner` to
  defer apply to the background loop.
- `lib/crow-kv/src/cluster/local_replica.rs` — add `known_commit_slot`,
  `apply_notify`, `spawn_apply_loop`, `stop_apply_loop`,
  `advance_known_commit_slot`, `wake_apply_loop`. Change
  `handle_heartbeat` from async apply to store-and-signal. Change
  `apply_committed_up_to` to skip-and-continue (or replace with the
  background loop).
- `lib/crow-kv/src/cluster/group_election.rs` — in the catch-up replay
  section of `run_heartbeat_round`, send `BatchChosenNotice` before the
  full-accept loop.
- `lib/crow-kv/tests/election/` — verify value-less catch-up converges
  with missing slots; verify background apply loop skips gaps and
  continues; verify heartbeat reply is not delayed by apply.

**Complexity**: Medium — the value-less catch-up protocol is a new RPC
frame type + follower-side acceptor lookup, building on existing
`ChosenNotice` infrastructure. The background apply loop is a
straightforward extraction of the existing `apply_committed_up_to` logic
into a `spawn_blocking` task, with the skip-and-continue fix for missing
slots. No runtime split, no cross-runtime coordination — all work stays
on the main tokio runtime.

**Dependencies**: None (builds on R53 heartbeat channel isolation).
