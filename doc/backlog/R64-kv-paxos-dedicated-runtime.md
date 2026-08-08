<!-- Copyright 2026-present buzzcrow <126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R64: Dedicated Runtime for Paxos + Decouple Catch-up Replay from Heartbeat

**Problem**: The leader's `run_leader_state` loop and all
election-related work (heartbeat rounds, catch-up replay, follower
apply loop) run on the shared tokio worker pool. Under burst write load
(e.g. 100k × 16 KiB pre-populate), the propose path saturates the
shared workers, starving the election driver. The leader stops sending
heartbeats for seconds at a time, triggering spurious leader elections.

The heartbeat channel itself (R53) is already isolated on a dedicated
gRPC connection, and R63's background apply loop has removed the
synchronous engine apply from the heartbeat handler. The remaining
contention is at two levels:

1. **Tokio runtime level** — the election driver (`run_leader_state`,
   heartbeat rounds, catch-up replay, follower apply loop) competes
   with the propose path, gRPC service handlers, and client-facing RPCs
   for the same worker threads. When the propose path saturates all
   workers, the election driver is not scheduled and heartbeats are
   delayed.

2. **Heartbeat round's inline catch-up replay** —
   `run_heartbeat_round` performs synchronous catch-up replay (up to 64
   `send_accept().await` calls per round) inline with the heartbeat
   reply processing. When a follower lags behind, each heartbeat round
   becomes a multi-hundred-millisecond catch-up operation that delays
   subsequent heartbeats. Even with R63's value-less batch notice
   reducing payload transfer, the full-accept fallback for missing slots
   still runs inline.

**Root cause**: Paxos-critical work (election, heartbeats) shares the
same tokio worker pool as non-Paxos work (propose path, snapshot
transfer, HTTP management API, client I/O). There is no priority
scheduling — tokio schedules tasks FIFO per worker. A burst of propose
work can monopolize all workers, and the election driver's heartbeat
tick is not scheduled until a worker is free.

**Solution**: Two changes:

1. **Dedicated runtime for election work** — spawn the election driver
   (`run_election_driver`) on a dedicated `multi_thread` tokio runtime
   with 2 worker threads on its own. Only election-related tasks run
   there: the leader state machine, heartbeat rounds, catch-up replay,
   follower apply loop, election deadlines. The propose path, gRPC
   service handlers, and client-facing RPCs stay on the main runtime.
   Cross-runtime coordination via `tokio::sync::Notify`
   (`commit_advance_notify`) and shared `Arc<PxGroup>` state (protected
   by `Mutex`/`RwLock`/atomics) works transparently across runtimes.
   `spawn_blocking` for engine apply uses the dedicated runtime's
   blocking pool (default 512 threads).

   - **Why 2 workers, not 1**: the election driver is a single state
     machine, but a single thread can be accidentally blocked by an
     unexpected synchronous stall (e.g. a `spawn_blocking` overflow, a
     tonic channel poll that doesn't yield, a debug log flush). A
     2-worker pool ensures heartbeats can still be sent even if one
     worker is momentarily stuck. The cost is negligible (one extra
     idle OS thread per group).

   - **Runtime lifecycle**: created lazily in `start_election_loop`
     (`group_election.rs`), stored in `PxGroup::election_runtime`
     (`group.rs`). Shutdown ordering: cancel election driver
     (`CancellationToken`) → wait for driver `JoinHandle` to resolve →
     drop the dedicated `Runtime` via `spawn_blocking(drop(rt))` (Runtime
     drop blocks until all spawned tasks complete, so it must not run on
     an async worker thread).

   - **Follower apply loop placement**: the background apply loop
     spawned in R63 moves with the election driver to the dedicated
     runtime. This is natural — the apply loop is driven by
     `known_commit_slot` which is advanced by heartbeats (on the
     dedicated runtime) and `BatchChosenNotice` (on the LearnerStream
     dispatch loop, still on the main runtime). The `AtomicU64` +
     `Notify` cross-runtime signaling works transparently.

2. **Decouple catch-up replay from heartbeat round** — extract the
   inline catch-up replay from `run_heartbeat_round` (lines 527–630 of
   `group_election.rs`) into a separate `select!` arm in
   `run_leader_state`. The heartbeat round should only send heartbeats
   and collect replies — it returns immediately after quorum is
   confirmed. Catch-up replay runs concurrently on its own ticker and
   does not block the next heartbeat tick.

   - The heartbeat reply carries `contiguous_applied`; the leader
     records the follower's lag in `peer_applied` (shared map, already
     exists).
   - A separate `catchup_ticker` arm in `run_leader_state`'s `select!`
     picks up lagging followers and replays accepted entries at its own
     pace, bounded by `MAX_CATCHUP_PER_ROUND` slots per peer.
   - The catch-up replay sends R63's `BatchChosenNotice` first (value-less
     phase), then falls back to full accepts for missing slots (payload
     phase). Both run on the catch-up ticker, not the heartbeat ticker.
   - `repair_once` (which fills the leader's own open-prefix gaps) also
     moves to a separate `repair_ticker` arm so its RPCs (which go
     through the LearnerStream) never delay lease renewal.
   - **Commit-advance notify**: add `commit_advance_notify: Notify` to
     `PxGroup`, signalled in the propose path after
     `fan_out_chosen_notice` so the leader's heartbeat loop fires an
     immediate heartbeat (with a min-interval guard of 4× ticker rate)
     when a write is chosen. Followers learn the new commit point
     without waiting for the next tick. `Notify` coalesces multiple
     signals into one wakeup, so a burst of writes produces one extra
     heartbeat, not one per write.

**Scope**:
- `lib/crow-kv/src/cluster/group.rs` — add `election_runtime`
  (`Mutex<Option<Runtime>>`), `commit_advance_notify` (`Notify`).
  Runtime lifecycle in `shutdown` (drop after driver resolves).
  Signal `commit_advance_notify` in propose path after
  `fan_out_chosen_notice`.
- `lib/crow-kv/src/cluster/group_election.rs` — spawn election driver
  on dedicated runtime in `spawn()`. Restructure `run_leader_state`
  `select!` into four arms: heartbeat ticker, commit-advance notify,
  catch-up ticker, repair ticker. Extract `run_catchup_replay()` and
  `run_heartbeat_round_only()`.
- `lib/crow-kv/src/cluster/local_replica.rs` — move `spawn_apply_loop`
  to the dedicated runtime (spawn via `election_runtime` handle).
- `lib/crow-kv/tests/election/` — verify heartbeats are not delayed
  under burst load; verify election still works after the runtime
  split; verify catch-up replay runs independently of heartbeat
  rounds.

**Complexity**: Medium — the runtime split is mechanical but requires
careful lifecycle management (shutdown ordering). The catch-up replay
extraction requires restructuring the `run_leader_state` loop from a
single biased `select!` arm into four independent arms. No new RPC
frame types (R63's `BatchChosenNotice` is reused).

**Dependencies**: R63 (value-less catch-up + background apply loop).
R63's `BatchChosenNotice` and `spawn_apply_loop` must exist first —
the catch-up replay extraction uses the batch notice, and the apply
loop moves to the dedicated runtime.

**Risk**: The previous attempt at this requirement (bundled with R63
in one commit) caused test hangs due to a deadlock between
`known_commit_slot` and `contiguous_chosen` — a replica that accepted
slots as a follower and then won an election stopped receiving
heartbeats (leaders don't get heartbeats) and the accept path was
changed to not advance `known_commit_slot`, so the apply loop never
applied those slots. The fix is to ensure `handle_accept_inner`
advances `known_commit_slot` (not just `contiguous_chosen`) so the
apply loop is always aware of newly chosen slots regardless of whether
they arrive via accept, heartbeat, or `BatchChosenNotice`. This is
addressed in R63's design — the accept path must advance
`known_commit_slot` + wake the apply loop.
