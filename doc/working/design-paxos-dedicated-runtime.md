# Paxos Dedicated Runtime — Isolate All Consensus Work

## Problem

R63 moved the election driver (heartbeat rounds, catch-up replay, follower
apply loop) to a dedicated tokio runtime. However, several Paxos-critical
code paths still run on the **main runtime** and can be delayed by non-Paxos
work (snapshot transfer, HTTP management API, client I/O, high-concurrency
propose pressure):

- **Propose path** (`run_prepare_phase` + `run_accept_phase` in `group.rs`)
  — the core Phase 1 + Phase 2 Paxos round for client writes runs on the
  main runtime. Under burst load the main runtime's worker threads are
  saturated, inflating propose tail latency.
- **LearnerStream dispatch loop** (`run_learner_stream` in
  `learner_stream.rs`) — the per-peer bidi stream dispatcher is
  `tokio::spawn`-ed on the main runtime (whoever first calls
  `learner_stream()` triggers lazy init). All `send_accept` calls go
  through an mpsc channel to this dispatch loop and wait for a oneshot
  reply. When the election driver (on the dedicated runtime) calls
  `send_accept` for catch-up replay or `repair_once`, it crosses
  runtimes: the dedicated-runtime task blocks on a oneshot that is
  fulfilled by the main-runtime dispatch loop. If the main runtime is
  busy, the dispatch loop is not scheduled, and the election driver's
  repair/catch-up work stalls.
- **`fan_out_chosen_notice`** — runs synchronously in the propose path on
  the main runtime.
- **Follower gRPC handlers** (the unary `prepare`, `heartbeat`,
  `request_vote`, `pre_vote`, `step_down` RPCs and the server-side
  `learner_stream` bidi-stream loop in `px_service.rs`) — these are tonic
  service handlers that run on whatever runtime the gRPC server was started
  on (the main runtime). The unary handlers delegate to `ReplicaHandler`
  trait methods (`on_prepare`, `on_heartbeat`, etc. on `PxLocalReplica`);
  the LearnerStream loop calls `handle_accept_inner` /
  `handle_heartbeat_inner`. A busy main runtime delays the follower's
  response to the leader's Prepare/Accept/Heartbeat RPCs.

### Root cause

Paxos work is split across two runtimes with an asymmetric dependency —
the dedicated runtime (election) depends on the main runtime
(LearnerStream dispatch, propose path) via mpsc + oneshot channels. The
main runtime has no such back-dependency. This means non-Paxos load on
the main runtime can indirectly stall Paxos-critical work on the
dedicated runtime.

## Current Architecture (post-R63)

- **Dedicated runtime** (`election_runtime` on `PxGroup`, 2 workers):
  election driver (`run_election_driver`), heartbeat rounds, catch-up
  replay, follower apply loop. Created lazily in `spawn()`
  (`group_election.rs:974`), stored in `PxGroup::election_runtime`
  (`group.rs:276`), dropped in `shutdown` (`group.rs:1062`).
- **Main runtime** (default `#[tokio::main]`, core-count workers):
  gRPC server, HTTP management API, snapshot transfer, client I/O,
  propose path, LearnerStream dispatch loop, follower gRPC handlers.

The runtime handle is not exposed beyond `spawn()` — only the full
`Runtime` is stored (for shutdown drop). No other component can spawn on
the dedicated runtime.

## Proposed Approach

Move all Paxos work to the dedicated runtime so it is fully
self-contained. The main runtime keeps only the gRPC server surface, HTTP
management API, snapshot transfer, and client I/O. The boundary between
runtimes becomes a `Handle::spawn` + `JoinHandle` await — the dedicated
runtime's handle spawns the Paxos work, and the main-runtime caller
awaits the `JoinHandle` (which yields the worker thread while waiting).

### Key design decision: `Handle::spawn` + `JoinHandle` await, not mpsc channels

The R64 backlog doc mentions "channel handoff" (mpsc + oneshot). This
design uses `Handle::spawn` + `JoinHandle::await` instead, because:

1. **Consistency with R63** — R63 already uses `handle.spawn()` for the
   election driver. Using the same primitive for all Paxos work is
   consistent.
2. **Simplicity** — no new mpsc/oneshot boilerplate per path. The
   `JoinHandle` is a future that completes when the spawned task
   completes; it can be polled from any runtime. The main-runtime worker
   yields while awaiting, so it is free to run other tasks.
3. **Backpressure is already handled** — the propose path's inflight
   admission gate (semaphore) bounds concurrent proposals. Follower
   handler concurrency is bounded by gRPC stream concurrency. No
   additional channel-based backpressure is needed.
4. **Cancellation semantics** — if the gRPC client disconnects, the
   `JoinHandle` is dropped, detaching the spawned task. The Paxos round
   completes (the value gets chosen), the permit is released, and the
   client simply doesn't get the response. This is the same correctness
   outcome as a channel-based approach (the request is already queued).
   Detached tasks complete quickly (Paxos round is fast), so the inflight
   window does not accumulate.

### Change 1: Expose the dedicated runtime handle

Add `election_runtime_handle: OnceLock<tokio::runtime::Handle>` to
`PxGroup`. Set it in `spawn()` (`group_election.rs`) alongside storing
the full `Runtime`. Add a method `PxGroup::election_handle()` returning
`Option<&Handle>`.

This lets any component with access to the group spawn on the dedicated
runtime. The `OnceLock` is set once (when the runtime is created) and
read by all Paxos paths.

### Change 2: LearnerStream dispatch loop → dedicated runtime

Change `PxLearnerStream::new` to accept an `Option<tokio::runtime::Handle>`
and use `handle.spawn(run_learner_stream(...))` when `Some`, falling back to
`tokio::spawn(...)` when `None`. The caller, `PxRemoteReplica::learner_stream()`,
passes `self.election_handle.get()`.

The `None` fallback is required for test groups and
`election_driver_disabled` groups, which never create the dedicated runtime
but still use `send_accept` through the LearnerStream (e.g. pinned-leader
testkit groups that propose without an election driver). Panicking
(`.expect()`) would break these paths. The fallback preserves the current
behavior (spawn on whatever runtime the caller is on), so correctness is
unchanged.

To thread the handle to `PxRemoteReplica` without adding a group
back-reference (which would create a circular `Arc` chain), store the
handle on `PxRemoteReplica` as `OnceLock<Handle>`. The group sets it in
`spawn()` — after creating the runtime, iterate all existing remote
replicas and set their handle.

The other remote-wiring methods (`add_remote_replica`, `set_remote_replicas`)
take `&mut self` and are only callable during group construction — **before**
the group is shared via `Arc` and before the runtime is created in `spawn()`.
So the runtime cannot exist when those methods run, and there is no need to
set the handle there. (There is no `batch_add_remote_replicas` function —
the name appears only in a comment at `local_replica.rs:407`.)

By the time `learner_stream()` is first called in production (only when this
replica is leader or running catch-up, both of which require the election
driver to have started), the handle is guaranteed to be set. However, test
groups and `election_driver_disabled` groups never create the runtime, so
the handle remains `None` — `learner_stream()` must handle this (see Change
2 fallback below).

### Change 3: Propose path → dedicated runtime

In `PxKvStore::propose_and_respond` (`px_kv_store.rs:648`), instead of
calling `group.propose(...).await` directly on the main runtime, spawn
the propose on the dedicated runtime handle and await the `JoinHandle`:

```rust
let result = if let Some(h) = group.election_handle() {
    let g = group.clone();
    h.spawn(async move { g.propose(payload, client_id, seq).await })
        .await
        .unwrap_or(ProposeResult::Err("propose task panicked".into()))
} else {
    group.propose(payload, client_id, seq).await
};
```

The `else` branch handles test groups without a dedicated runtime
(election driver disabled or not yet started). The group is `Arc`, so
cloning it for the `'static` spawn future is cheap.

All sub-tasks spawned by `propose` via `tokio::spawn` (coalescer flush,
watchdog, prepare/accept side-effect drains, `spawn_learn_chosen`)
inherit the current runtime — so they also run on the dedicated runtime
when `propose` is spawned there. No changes needed to those spawn sites.

### Change 4: Follower gRPC handlers → dedicated runtime

For **unary RPCs** (`prepare`, `heartbeat`, `request_vote`, `pre_vote`,
`step_down` in `px_service.rs`): after group lookup, spawn the core
handler work (`on_prepare` / `on_heartbeat` / etc.) on the dedicated
runtime handle and await the `JoinHandle`. The response building stays
inside the spawned task (it accesses the replica's term snapshot, which
is cheap to read on the dedicated runtime).

**Epoch check placement**: the `prepare` handler does a membership-epoch
fence check before calling `on_prepare` (`px_service.rs:130-147`). This
check must move **inside** the spawned task to avoid a TOCTOU race — the
epoch could change between the check on the main runtime and `on_prepare`
running on the dedicated runtime. This is benign for correctness (ballot-
based safety is the primary fence, not the epoch), but moving the check
inside keeps the fast-path rejection on the dedicated runtime and avoids
a stale-epoch false rejection. The `heartbeat` handler has no epoch check,
so it is unaffected.

For the **server-side LearnerStream** (`learner_stream` method in
`px_service.rs`): the frame-processing loop is currently
`tokio::spawn`-ed on the main runtime. Change it to spawn on the
dedicated runtime handle. The loop reads from the gRPC inbound stream
(`'static + Send`) and writes responses to an mpsc channel that feeds
the outbound stream (which stays on the main runtime). Both are
runtime-agnostic. `handle_accept_inner`, `handle_heartbeat_inner`, and
the `ChosenNotification` / `BatchChosen` handlers all run on the
dedicated runtime inside this loop.

### Change 5: Runtime sizing

Increase the dedicated runtime from 2 to 4 worker threads in `spawn()`
(`group_election.rs`). The dedicated runtime now carries propose +
follower-handler + election load, so 2 workers is insufficient. 4 is the
starting point; the post-implementation benchmark will confirm.

The main runtime is left at the default (core-count workers) for now.
R64 says it "can shrink to 2-4" but that is optional tuning, not required
for correctness.

## Alternatives Considered

### A1: mpsc + oneshot channel handoff (R64 backlog doc's literal text)

Each cross-runtime path uses a bounded mpsc channel for requests and a
oneshot for the reply. Rejected because:

- More boilerplate (channel setup, request/reply enums, dispatch loops)
  per path.
- The admission gate already bounds propose concurrency; gRPC bounds
  follower-handler concurrency. Channel-based backpressure is redundant.
- `Handle::spawn` + `JoinHandle` achieves the same runtime isolation with
  less code and the same cancellation semantics.

### A2: Create the dedicated runtime eagerly at group construction

Create the runtime in `PxGroup::new` instead of lazily in `spawn()`.
Rejected because:

- Every test group would spawn a dedicated runtime (2-4 OS threads),
  inflating test resource usage and slowing the test suite.
- The lazy creation ties the runtime lifecycle to the election driver
  lifecycle, which is correct — the runtime only exists while the driver
  is active.

### A3: Move only the LearnerStream and propose, leave follower handlers on main

Skip Change 4 (follower handlers). Rejected because:

- A busy main runtime would still delay follower responses to
  Prepare/Accept/Heartbeat RPCs, which can cause the leader to think the
  follower is dead and trigger spurious elections. This is the same
  safety issue R63 solved for the leader side.

## Safety Analysis

- **Cross-runtime state access**: all shared state (`PxGroup`, `PxLocalReplica`,
  `PxRemoteReplica`) uses `Arc`, `Mutex`, `RwLock`, atomics, and `Notify` —
  all of which are safe across runtimes. `tokio::sync` primitives work
  transparently across runtimes. `spawn_blocking` uses the current
  runtime's blocking pool, which is correct (engine I/O runs on the
  dedicated runtime's blocking pool).
- **LearnerStream cross-runtime reversal**: before R64, the election driver
  (dedicated) called `send_accept` which crossed to the main-runtime
  dispatch loop. After R64, the dispatch loop is on the dedicated runtime,
  so the election driver's `send_accept` stays on the dedicated runtime.
  The propose path (now also on the dedicated runtime) also stays
  on-runtime. No cross-runtime oneshot waits remain for Paxos-critical
  work.
- **Cancellation**: dropping a `JoinHandle` detaches the task. A detached
  propose completes the Paxos round (value chosen, learned, notified) —
  the client just doesn't get the response. This is safe: the chosen
  value is committed regardless of whether the originating client is
  still connected. The inflight permit is released when the task
  completes.
- **Shutdown ordering**: `shutdown` cancels `tenure_cancel`, awaits the
  driver handle, then drops the runtime (on a blocking thread). The
  runtime's `drop` blocks until all spawned tasks complete. After R64, the
  dedicated runtime hosts not just the election driver but also in-flight
  propose tasks, coalescer sub-tasks (flush, watchdog), side-effect drains,
  `spawn_learn_chosen` tasks, follower-handler tasks, and LearnerStream
  dispatch loops. The existing shutdown comment ("The driver was the only
  long-lived task on it... so this is effectively instantaneous") must be
  updated.

  Of these, only the **side-effect drain tasks** and the **election driver**
  check `tenure_cancel` (via `tokio::select!` on `cancel.cancelled()`). The
  **propose retry loop does not check `tenure_cancel`** — it uses bounded
  retries (`max_paxos_retries = 3`, `max_slot_retries = 3`) with backoff
  sleeps. The **coalescer watchdog** is a long-running loop that does not
  check `tenure_cancel` either. So `drop(rt)` blocks until these tasks
  complete naturally:

  - Propose tasks: bounded by 9 retries × backoff (max ~seconds). When the
    driver exits via `cancel.cancelled()`, it does **not** call
    `become_follower` — it just cancels the local `tenure_cancel` and
    returns (`group_election.rs:1278`). So the leader's role stays `Leader`
    and the propose path's leadership gate still passes; in-flight proposes
    won't fast-fail on the role check. They exhaust retries against
    shutting-down peers, then return `Err`/`NotLeader`. Bounded, not
    infinite.
  - Coalescer watchdog: runs forever until the runtime is dropped. `drop(rt)`
    cancels it (runtime drop cancels all spawned tasks). This is safe — the
    watchdog only flushes stuck batches, which is a no-op during shutdown.
  - `spawn_learn_chosen` tasks: short-lived (one `apply_entry` + frontier
    advance), complete quickly.

  To bound shutdown latency explicitly, switch from bare `drop(rt)` to
  `rt.shutdown_timeout(duration)` (e.g. 5 s) on the blocking thread. This
  cancels remaining tasks after the timeout instead of blocking
  indefinitely. Alternatively, add `tenure_cancel` checks to the propose
  retry loop's backoff sleeps (`tokio::select!` on `cancel.cancelled()` vs
  `sleep`), which lets `tenure_cancel` abort in-flight proposes promptly.
  The `shutdown_timeout` approach is simpler and covers all cases; the
  `tenure_cancel` approach is more targeted but requires editing the retry
  loop. **Decision**: use `shutdown_timeout` — it is a one-line change in
  `shutdown()` and covers the coalescer watchdog and any other unaccounted
  long-lived task.

## Acceptance Test Plan

- **New unit test**: propose path runs on the dedicated runtime. Verify
  via `Handle::id()` comparison — inside `propose_inner_impl`, record
  (test-only) whether `Handle::try_current()` matches the election
  handle. After a propose, assert the flag is true.
- **Existing R63 tests pass**: `apply_and_runtime_test.rs`,
  `heartbeat_test.rs`, `lease_test.rs`, `election/` suite.
- **Existing paxos tests pass**: `paxos/` suite (acceptor, learner, error,
  roles).
- **Full test suite passes**: `pixi run test-suite`.
- **Benchmark (post-implementation)**: read + scan regression benchmarks
  run via existing scripts; results recorded in flow-analysis docs. No
  performance regression expected (R64 moves write-path work, not
  read-path).

## Files Changed

- `lib/crow-kv/src/cluster/group.rs` — `election_runtime_handle` field,
  `election_handle()` method, set handle on remotes in `spawn()`;
  `shutdown()` switch from bare `drop(rt)` to `rt.shutdown_timeout()`;
  update the stale shutdown comment.
- `lib/crow-kv/src/cluster/group_election.rs` — set
  `election_runtime_handle` + remote handles in `spawn()`, increase
  worker threads to 4.
- `lib/crow-kv/src/cluster/learner_stream.rs` — `PxLearnerStream::new`
  accepts `Option<Handle>`, uses `handle.spawn()` when `Some`, falls back
  to `tokio::spawn()` when `None`.
- `lib/crow-kv/src/cluster/remote_replica.rs` —
  `election_handle: OnceLock<Handle>` field, `learner_stream()` passes
  `self.election_handle.get()` to `PxLearnerStream::new`.
- `lib/crow-kv/src/cluster/px_kv_store.rs` — `propose_and_respond`
  spawns propose on dedicated runtime.
- `lib/crow-kv/src/rpc/px_service.rs` — unary handlers (with epoch check
  moved inside spawned task for `prepare`) + LearnerStream loop spawn on
  dedicated runtime.
- `lib/crow-kv/tests/election/apply_and_runtime_test.rs` — new test:
  propose runs on dedicated runtime.
- `doc/design/kv/design-crow-kv.md` §13 Concurrency Model — update to
  document the two-runtime split (merged in Step 7).
