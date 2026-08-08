<!-- Copyright 2026-present buzzcrow <126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R63: Election Driver — Dedicated Runtime + Decouple Catch-up Replay + Value-less Catch-up

**Problem**: The leader's `run_leader_state` loop and all election-related
work (heartbeat rounds, catch-up replay, follower apply loop) run on the
shared tokio worker pool. Under burst write load (e.g. 100k × 16 KiB
pre-populate), the propose path saturates the shared workers, starving
the election driver. Additionally, `run_heartbeat_round` performs
synchronous catch-up replay (up to 64 `send_accept().await` calls per
round) inline with the heartbeat reply processing — when a follower
lags behind, each heartbeat round becomes a multi-hundred-millisecond
catch-up operation that delays subsequent heartbeats. The combination
causes the leader to stop sending heartbeats for seconds at a time,
triggering spurious leader elections.

The heartbeat channel itself (R53) is already isolated on a dedicated
gRPC connection, and the follower's heartbeat reply is now non-blocking
(async apply loop). The remaining contention is at the tokio runtime
level and within the heartbeat round's inline catch-up logic.

A second problem: the catch-up replay sends **full 16 KiB payloads** in
every `send_accept` RPC, even though the follower almost always already
has the value in its acceptor slot (it accepted the original proposal;
it just hasn't applied it to the engine yet). Only slots rejected during
election churn are truly missing. This wastes bandwidth and CPU on
serialization for the common case.

**Solution**: Three changes:

1. **Dedicated runtime for election work** — spawn the election driver
   (`run_election_driver`) on a dedicated `multi_thread` tokio runtime
   with 2 worker threads on its own. Only election-related tasks run
   there: the leader state machine, heartbeat rounds, follower apply
   loop, election deadlines. The propose path, gRPC service handlers,
   and client-facing RPCs stay on the main runtime. Cross-runtime
   coordination via `tokio::sync::Notify` (`commit_advance_notify`) and
   shared `Arc<PxGroup>` state (protected by `Mutex`/`RwLock`/atomics)
   works transparently across runtimes. `spawn_blocking` for engine
   apply uses the dedicated runtime's blocking pool (default 512
   threads).

   - **Why 2 workers, not 1**: the election driver is a single state
     machine, but a single thread can be accidentally blocked by an
     unexpected synchronous stall (e.g. a `spawn_blocking` overflow, a
     tonic channel poll that doesn't yield, a debug log flush). A
     2-worker pool ensures heartbeats can still be sent even if one
     worker is momentarily stuck. The cost is negligible (one extra
     idle OS thread per group).

2. **Decouple follower catch-up replay from heartbeat round** — the
   follower catch-up replay (currently inline in
   `run_heartbeat_round`, lines 527–630 of `group_election.rs`) is
   distinct from `repair_once` (which fills the leader's own open-
   prefix gaps). The follower catch-up replays accepted entries to
   lagging followers (up to 64 `send_accept().await` per round). Move
   it into a separate background task or a separate `select!` arm in
   `run_leader_state`. The heartbeat round should only send heartbeats
   and collect replies — it should return immediately after quorum is
   confirmed. Follower catch-up replay runs concurrently and does not
   block the next heartbeat tick.

   - The heartbeat reply carries `contiguous_applied`; the leader
     records the follower's lag in a shared atomic or small struct.
   - A separate catch-up task (or `select!` arm) picks up lagging
     followers and replays accepted entries at its own pace, bounded
     by a configurable rate.
   - The heartbeat round's `run_heartbeat_with_repair` calls
     `repair_once()` — review whether this should also be decoupled.

3. **Value-less catch-up replay** — the catch-up replay currently
   sends full `AcceptRequest` with 16 KiB payload for every lagging
   slot. In the common case the follower already has the value in its
   acceptor slot (it accepted the original proposal; only apply is
   lagging). Optimize to a two-phase protocol:

   - **Phase 1 — value-less range notice**: leader sends a lightweight
     batch message covering the lagging slot range `[follower_applied+1,
     leader_chosen]`, carrying only `(slot, ballot)` per entry — no
     payload. This is essentially a batch `ChosenNotice`. The follower
     checks its local acceptor for each slot:
     - **Has value**: mark as chosen locally; the background apply loop
       picks it up from the acceptor slot and applies it (no RPC
       needed — the apply loop already reads from `acceptor.accepted_at()`).
     - **Missing value**: add slot to a "need value" reply list.
   - **Phase 2 — on-demand fetch**: leader sends full `AcceptRequest`
     (with payload) only for the slots the follower reported missing.
     In steady state this is zero slots; only election-churn-rejected
     slots need it.

   - **No size threshold needed**: because catch-up is a batch range
     operation, the lightweight round-trip overhead is amortized over
     the entire range. Even for 1 KiB values, a 64-slot range saves 64
     payload serializations and ~64 KiB of wire bytes for one cheap
     round-trip. The on-demand fetch for missing slots is sparse
     (typically 0 slots), so the extra latency is negligible.
   - **Infrastructure already partially exists**: `ChosenNotice`
     (`fan_out_chosen_notice` / `send_chosen_notice`) is already a
     payload-less per-slot notice. The batch version extends this to a
     range. The follower's background apply loop already reads values
     from `acceptor.accepted_at()` (local_replica.rs:1324), so
     value-present slots need no new code path — just advance the
     chosen frontier and let the apply loop run.
   - **Follower apply loop `None => break` gap**: the current apply
     loop breaks on the first missing slot
     (`local_replica.rs:1326`). With value-less catch-up, the loop
     should skip missing slots (record them for later fetch) and
     continue applying subsequent slots that are available, so a
     single missing slot doesn't block the entire apply backlog.

**Scope**:
- `lib/crow-kv/src/cluster/group_election.rs` — spawn election driver
  on dedicated runtime; extract catch-up replay from
  `run_heartbeat_round`; implement value-less catch-up protocol.
- `lib/crow-kv/src/cluster/group.rs` — runtime lifecycle management
  (create, store, shutdown the dedicated runtime).
- `lib/crow-kv/src/cluster/local_replica.rs` — follower apply loop
  moves with the election driver to the dedicated runtime; fix
  `None => break` to skip-and-continue for missing slots.
- `lib/crow-kv/src/cluster/remote_replica.rs` — add value-less batch
  catch-up RPC (or extend `send_chosen_notice` to batch form).
- `lib/crow-kv/src/rpc/px_service.rs` — handle value-less catch-up
  request on follower side (check acceptor, reply with missing list).
- `lib/crow-kv/tests/election/` — verify heartbeats are not delayed
  under burst load; verify election still works after the runtime
  split; verify value-less catch-up converges with missing slots.

**Complexity**: Medium — the runtime split is mechanical but requires
careful lifecycle management (shutdown ordering: cancel election driver
→ wait for it to finish → drop dedicated runtime). The catch-up replay
extraction requires rethinking the `run_leader_state` loop structure.
The value-less catch-up protocol needs a new RPC frame type and
follower-side acceptor lookup logic, but builds on existing
`ChosenNotice` infrastructure.

**Dependencies**: None (builds on R53 heartbeat channel and the
async-apply fix already committed).
