<!-- Copyright 2026-present buzzcrow <126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R64: Paxos Dedicated Runtime — Isolate All Consensus Work from the Main Runtime

**Problem**: R63 moved the election driver (heartbeat rounds, catch-up
replay, follower apply loop) to a dedicated tokio runtime. However,
several Paxos-critical code paths still run on the **main runtime** and
can be delayed by non-Paxos work (snapshot transfer, HTTP management
API, client I/O, high-concurrency propose pressure):

- **Propose path** (`run_prepare_phase` + `run_accept_phase` in
  `group.rs`) — the core Phase 1 + Phase 2 Paxos round for client
  writes runs on the main runtime. Under burst load the main runtime's
  worker threads are saturated, inflating propose tail latency.
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
- **`fan_out_chosen_notice`** — runs synchronously in the propose path
  on the main runtime.
- **Follower gRPC handlers** (`on_prepare`, `on_accept`, `on_heartbeat`
  in `px_service.rs`) — these are tonic service handlers that run on
  whatever runtime the gRPC server was started on (the main runtime).
  A busy main runtime delays the follower's response to the leader's
  Prepare/Accept/Heartbeat RPCs.

**Root cause**: Paxos work is split across two runtimes with an
asymmetric dependency — the dedicated runtime (election) depends on the
main runtime (LearnerStream dispatch, propose path) via mpsc + oneshot
channels. The main runtime has no such back-dependency. This means
non-Paxos load on the main runtime can indirectly stall Paxos-critical
work on the dedicated runtime.

**Solution**: Move all Paxos work to the dedicated runtime so it is
fully self-contained. The main runtime keeps only the gRPC server
surface, HTTP management API, snapshot transfer, and client I/O. The
boundary between runtimes becomes a channel handoff (microsecond cost)
rather than a shared-task dependency.

Changes, in priority order:

1. **LearnerStream dispatch loop → dedicated runtime** — change
   `PxLearnerStream::new` to accept a runtime handle (or spawn via the
   group's `election_runtime` handle) instead of bare `tokio::spawn`.
   This eliminates the cross-runtime oneshot wait for the election
   driver's `send_accept` calls. Propose-path `send_accept` calls (from
   the main runtime) now cross runtimes in the opposite direction, but
   propose latency is less safety-critical than lease renewal and can
   tolerate backpressure.

2. **Propose path → dedicated runtime** — `run_prepare_phase` +
   `run_accept_phase` (and `fan_out_chosen_notice`) move to the
   dedicated runtime. The gRPC `Propose` handler on the main runtime
   sends the request through a channel to the dedicated runtime, which
   runs the Paxos round and returns the result. The channel handoff is
   a single `mpsc::send` + `oneshot` await — negligible compared to the
   Paxos round itself.

3. **Follower gRPC handlers → dedicated runtime** — `on_prepare`,
   `on_accept`, `on_heartbeat` handlers dispatch their work to the
   dedicated runtime via a channel, keeping the gRPC server thread free
   to accept new connections and read frames. The handler awaits the
   result on a oneshot.

4. **Runtime sizing** — the dedicated runtime may need more than 2
   workers once it carries propose + follower-handler load. Start with
   4 and tune via benchmark. The main runtime can shrink to 2–4 workers
   (it only does I/O relay + channel handoff).

**Non-goals**:
- Changing the wire protocol or gRPC service definitions.
- Splitting the LearnerStream into multiple connections (the bidi
  stream architecture stays; only the dispatch loop's runtime
  placement changes).
- Moving the storage engine (crow-tree FFI) or WAL to the dedicated
  runtime — those are I/O-bound and already use `spawn_blocking`.

**Risk**: Moving propose to the dedicated runtime concentrates all
Paxos load on fewer threads. If a Paxos round blocks on engine I/O
(`spawn_blocking`), it consumes a blocking-pool slot, not a worker
thread, so worker-thread count is unaffected. The main risk is
under-provisioning worker threads; benchmark with 4/6/8 workers to
find the sweet spot.

**Verification**:
- Benchmark: write workload, 256 threads, 16 KiB values, 60s — 0
  errors, 0 leader changes, p99 propose latency stable.
- Benchmark: mixed write + snapshot transfer — snapshot must not
  inflate propose p99 or cause lease expiry.
- Existing R63 tests pass.
- New unit test: propose path runs on dedicated runtime (verify via
  `RuntimeId` or thread-name assertion).
