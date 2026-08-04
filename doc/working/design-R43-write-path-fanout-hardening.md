<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# R43 Design — Write-Path Fan-Out Hardening

Working design draft for R43. Folds into `design/design-rpc.md` (§3, §6, §7)
and `design/design-observability.md` (Instrumentation Points) after merge.

## Problem

After R16a/R16b the per-proposal critical path is
`max(local fsync, quorum RPC)`, but the fan-out that implements the
"quorum RPC" term has four latency/availability/observability gaps and two
robustness gaps. All six live in the same code (`group.rs` prepare/accept
phases + `learner_stream.rs`), so they are best done as one pass.

Current behavior (code-grounded):

- **E1 — `join_all` over all peers.** `run_prepare_phase`
  (`group.rs:1874`) and `run_accept_phase` (`group.rs:2060` R16b,
  `group.rs:2139` R16a) all do `tokio::join!(local, join_all(remote_futs))`.
  Replies fold only after *every* remote answers. In a 3-node group quorum =
  local + 1 remote, yet every proposal waits for both remotes — per-proposal
  latency is `max(all peers)`, not the quorum-th fastest. One slow
  (connected) follower drags every write.
- **E2 — no RPC deadline.** `send_accept` / `send_heartbeat` await their
  correlation oneshot with no timeout (`learner_stream.rs:119`, `:146`).
  `send_prepare` is a unary gRPC with no timeout (`remote_replica.rs:116`).
  A connected-yet-unresponsive peer (GC pause, half-open socket, overloaded
  server) blocks `join_all` indefinitely — one hung follower freezes the
  group. E1 removes the latency exposure; E2 still reclaims the pending-map
  entry, the oneshot, and (transitively) the admission permit.
- **E3 — no write-path phase metrics.** `ReadRegistryHandles`
  (`group.rs:39`) + `set_metrics_registry` (`group.rs:448`) carry only
  read-path summaries. The write side has WAL fsync latency and admission
  counters but no propose-e2e / prepare / accept / first-quorum-RPC / apply
  summaries; the benchmark analysis *infers* the critical path instead of
  measuring it.
- **E4 — deterministic backoff.** `retry_backoff` (`group.rs:2293`) is
  `base * 2^attempt` with no jitter; proposers that collide on a slot retry
  in lockstep.
- **E5 — heartbeats compete with accepts on one queue.** `dispatch`
  (`learner_stream.rs:174`) is a `try_send` into one shared mpsc (capacity
  `learner_stream_window_frames`) for Accept, Heartbeat, and
  ChosenNotification. Under write saturation a heartbeat can queue behind
  ~128 accepts or be `Busy`-rejected — heartbeat RTT (lease/election
  stability) degrades at peak write load.
- **E6 — triplicated reply fold.** The `Accepted/Rejected/TermStale/
  EpochMismatch/Err` match fold is duplicated across the prepare phase and
  both accept paths (~150 duplicated lines in `group.rs`). E1 rewrites
  exactly this code; extracting a helper first makes E1 a small diff.

## Approach

### E6 — `ReplyFold` accumulator (enabler, pure refactor)

Extract a struct holding the common accumulator shared by all three fold
sites:

```rust
struct ReplyFold {
    accepted: usize,              // promised for prepare, accepted for accept
    highest_rejected_round: Option<u64>,
    highest_seen_term: Option<u64>,
    epoch_mismatch: Option<u64>,
    adopted: Option<PxLogEntry>,  // prepare-only (value recovery)
    local_folded: bool,           // W6: set true once the local reply is folded
}
```

Methods (reply-typed, since `PxPrepareReply` and `PxAcceptReply` differ and
the local accept R16b path is infallible `PxAcceptReply` while R16a is
`Result<PxAcceptReply, _>`):

- `fold_prepare_local(reply: Result<PxPrepareReply, _>)`
- `fold_prepare_remote(remote, reply: Result<PxPrepareReply, _>)`
- `fold_accept_local(reply)` — accepts both `Result<PxAcceptReply, _>` (R16a)
  and `PxAcceptReply` (R16b) via two entry points or an `Into`-style adapter.
- `fold_accept_remote(remote, reply: Result<PxAcceptReply, _>)`

Each fold updates the counters and `highest_*` fields exactly as the inline
code does today. `consider_accepted` is called inside the prepare folds. No
behavior change; the three call sites shrink to a loop over the helper.

### E1 — Quorum short-circuit

Replace `tokio::join!(local, join_all(remote_futs))` with a single
`FuturesUnordered` that includes the local future as one tagged entry plus
each remote future tagged with `(remote_idx, voting)`. A `tokio::select!`
loop (or `StreamExt::next` loop) folds replies as they arrive via `ReplyFold`.

Termination rules:

1. **Success short-circuit.** Once `fold.accepted >= quorum` AND
   `fold.local_folded` (W6: the local WAL persist / CAS reply must be
   counted before `Chosen`/`Proceed` is returned), stop awaiting and return.
   The still-pending futures are moved into a **detached drain task** that
   keeps folding for side effects only.
2. **Failure path.** If all replies have folded and `accepted < quorum`,
   decide `Retry` / `Fail` exactly as today (highest rejected round → Retry;
   highest seen term → Fail `TermStale`; epoch mismatch → Fail
   `MembershipEpochMismatch`; else `QuorumUnavailable`).
3. **Late side effects (detached drain).** A late `TermStale` from a slow
   peer triggers `become_follower`; a late `EpochMismatch` adopts the
   responder epoch. The drain task captures `self_weak` (`group.rs:230`),
   upgrades to `Arc<PxGroup>`, and calls `group.local_replica.become_follower`
   / `group.adopt_membership_epoch` directly — it does **not** return to
   `propose_inner` (the proposal already returned `Chosen`). Failure
   detection is preserved; only the latency path leaves it behind.

W6 preservation: the success short-circuit is gated on `local_folded`. For
R16a the local future is `on_accept` (CAS + WAL persist); for R16b it is
`on_accept_inner` (CAS only) — matching today's `tokio::join!` pairing. The
R16b `spawn_accept_persist` still fires after the short-circuit returns
`Chosen`, unchanged.

The detached drain is best-effort: it runs to completion (folds all
remaining replies) then drops. It must not outlive the group — it captures
`self_weak`, so a dropped group lets the upgrade fail and the task exits
cleanly. It honors `tenure_cancel` so a step-down aborts the drain.

### E2 — RPC deadline

Wrap the oneshot await in `send_accept` / `send_heartbeat` with
`tokio::time::timeout`. On expiry: remove the pending-map entry (the recv
half inserts/removes by `request_id`; a timed-out entry is stale and must be
reclaimed) and surface a typed retryable error that maps to the existing
`Busy`/unavailable handling (the proposer already treats
`PxReplicaError::Internal` from a peer as retryable). The recv-half's
`dispatch_response` already no-ops on a missing pending entry (late ack),
so a reply arriving after timeout is logged at `debug!` and dropped.

Wrap the unary `send_prepare` with `tokio::time::timeout` over the
`client.prepare(...).await` call.

Config knob: `learner_stream_rpc_timeout_ms` on `PxElectionConfig` (default
2000 ms — aligned with the 2 s election max so a hung peer is surfaced as a
retryable failure well before it threatens availability; for_tests profile
uses 500 ms so a hung-peer test fails fast under `start_paused`). Plumbed
through `PxRemoteReplica` (stored alongside `learner_stream_window_frames`)
into `PxLearnerStream::new`, which passes it to the bg task. The bg task
itself does not apply the timeout — the timeout lives in `send_accept` /
`send_heartbeat` / `send_prepare` at the await site, where the oneshot /
gRPC future is owned.

Belt-and-braces: enable h2 keepalive on the tonic `Channel` in
`get_client` (`remote_replica.rs:412`) and the learner_stream connect
(`learner_stream.rs:223`) via `Endpoint::keep_alive_while_idle(true)` +
`http2_keep_alive_interval`, so a silent half-open connection is detected
at the transport layer independent of the application timeout.

### E3 — Write-path phase metrics

Add a `WriteRegistryHandles` struct parallel to `ReadRegistryHandles`,
registered in `set_metrics_registry`:

- `propose_e2e: LatencySummary` — `propose_inner` entry → `Chosen`/`Err`
  return (the full client-observed proposal latency).
- `prepare_phase: LatencySummary` — `run_prepare_phase` entry → return.
- `accept_phase: LatencySummary` — `run_accept_phase` entry → return.
- `accept_quorum_rpc: LatencySummary` — accept-phase start → first-quorum
  reached (meaningful after E1; records the k-th-fastest remote latency).
- `engine_apply: LatencySummary` — `learner.rs apply_entry` entry → return
  (registered on the local replica's metrics, observed in `learn_chosen`).

Naming per `design-observability.md`:
`s.{store_id}.g.{group_id}.write.{metric}.l` (e.g. `...write.propose_e2e.l`).
Stored in a `OnceLock<WriteRegistryHandles>` on `PxGroup`, mirroring
`read_handles`. Cheap: one `Instant::now()` pair per phase. The
`accept_quorum_rpc` timer is recorded only on the success short-circuit
path (E1); on the failure path it is not recorded (no quorum was reached).

### E4 — Backoff jitter

Add ±50% random jitter to `retry_backoff`: `base * 2^attempt * (0.5 + r)`
where `r ∈ [0, 1)`. Reuse the existing `XorShift64` PRNG
(`group_election.rs:1070`) — the codebase deliberately avoids a `rand`
runtime dependency for one-line RNG needs. A thread-local `XorShift64`
seeded from `(node_id, now_nanos)` (matching the election PRNG's seeding
philosophy) makes the call deterministic per process start but varies
across retries and across nodes. Keep the admission permit held during
backoff (releasing it would let a duplicate `(client_id, seq)` admit
concurrently and complicates dedup ordering) — document this choice at the
call site with a one-line comment.

### E5 — Heartbeat reserved capacity

Reserved-capacity variant (the safe choice per the R43 analysis; the
priority-lane variant needs a separate ordering check). Track the frame
kind in `OutboundCmd` (add a `kind: FrameKind { Accept, Heartbeat, Chosen }`
field). In `dispatch`, compute the effective capacity:

- Accept / Chosen: reject (`Busy`) when `cmd_tx.capacity() == 0` AND the
  number of non-heartbeat frames in flight is at the reserve threshold.
  Concretely: accepts are rejected when the queue depth is at
  `capacity - heartbeat_reserve`; heartbeats are accepted up to full
  capacity.
- Heartbeat: `try_send` always (up to full capacity).

This preserves FIFO ordering (heartbeats and accepts share the same mpsc,
so a heartbeat still cannot race ahead of an accept it logically follows —
the `design-rpc.md` §3 ordering invariant) while guaranteeing heartbeats
can never be starved of a slot: `heartbeat_reserve` slots are always
available to heartbeats even when accepts have filled the rest.

Config knob: `learner_stream_heartbeat_reserve` on `PxElectionConfig`
(default 8 — enough for a few heartbeat rounds in flight; for_tests uses
2). Plumbed through `PxRemoteReplica` → `PxLearnerStream`. The bg task's
mpsc is created with the full `learner_stream_window_frames` capacity; the
reserve is enforced in `dispatch` by checking `cmd_tx.max_capacity() -
cmd_tx.capacity() >= (window - reserve)` before accepting a non-heartbeat
frame.

## Alternatives Considered

- **E1: `select_all` instead of `FuturesUnordered`.** `select_all` returns
  `(result, idx, remaining)` per step — usable but allocates a Vec per
  step. `FuturesUnordered` + `next()` is the idiomatic tokio pattern and
  avoids per-step allocation. Chosen: `FuturesUnordered`.
- **E1: drop late replies entirely (no detached drain).** Simpler, but
  loses late `TermStale` / `EpochMismatch` side effects — a slow peer that
  observed a higher term would not step the leader down, weakening failure
  detection. Rejected: the drain is the whole point of "failure detection
  not lost, just off the latency path."
- **E1: release the admission permit on short-circuit, before the drain
  finishes.** Would let the next proposal admit sooner, but the late reply
  could still mutate group state (`become_follower`) while the next
  proposal is mid-flight — confusing. The permit is released when
  `propose_inner` returns (today's behavior); the drain runs detached and
  does not hold the permit. Chosen: keep today's permit lifetime.
- **E2: deadline inside the bg task, not at the await site.** The bg task
  does not own the caller's oneshot future; it only inserts/removes the
  pending entry. The timeout must wrap the `rx.await` in `send_accept`,
  where the oneshot is owned. Chosen: await-site timeout.
- **E5: priority lane (second mpsc, `biased` select).** Would let
  heartbeats jump the queue, but risks reordering a heartbeat ahead of an
  accept it follows — violating `design-rpc.md` §3 invariant 1. Rejected
  pending a design-doc check; reserved capacity is safe and sufficient.
- **E3: histograms instead of summaries.** `LatencySummary` (count + sum +
  max) matches the existing read-path `barrier` / `engine_get` pattern and
  is cheaper; percentile precision is not needed for phase-level
  attribution (the bench client already has `PreciseHistogram` for
  client-observed p99). Chosen: `LatencySummary`.

## Files

- `crowkv/src/cluster/group.rs` — `ReplyFold` struct + fold methods (E6);
  `run_prepare_phase` / `run_accept_phase` rewrite to `FuturesUnordered` +
  short-circuit + detached drain (E1); `WriteRegistryHandles` +
  `set_metrics_registry` extension + phase timing (E3); `retry_backoff`
  jitter (E4).
- `crowkv/src/cluster/learner_stream.rs` — `OutboundCmd::kind` +
  reserved-capacity `dispatch` (E5); `tokio::time::timeout` on
  `send_accept` / `send_heartbeat` await + pending-map cleanup on expiry
  (E2); keepalive on the connect `Endpoint` (E2).
- `crowkv/src/cluster/remote_replica.rs` — `tokio::time::timeout` on
  `send_prepare` (E2); keepalive on `get_client` `Endpoint` (E2); store
  `learner_stream_rpc_timeout_ms` + `learner_stream_heartbeat_reserve` and
  plumb into `PxLearnerStream::new` (E2/E5).
- `crowkv/src/common/config.rs` — `learner_stream_rpc_timeout_ms` +
  `learner_stream_heartbeat_reserve` on `PxElectionConfig` (DEFAULT,
  for_tests, for_e2e).
- `crowkv/src/paxos/learner.rs` — `engine_apply` summary observe in
  `apply_entry` (E3).
- `crowkv/src/cluster/local_replica.rs` — test-util `accept_delay_ms` hook
  for the E1 acceptance test (E1 test infra).

## Acceptance Criteria

- **E1:** a 3-node group with one follower's `on_accept_inner` delayed
  (test-util `accept_delay_ms`) proposes at the latency of the fast
  follower, not the slow one; a late `TermStale` from the slow follower
  still steps the leader down. W6 holds: `Chosen` is never returned before
  the local reply is folded.
- **E2:** a hung (connected, non-responding) peer causes accept RPCs to
  fail within `learner_stream_rpc_timeout_ms`; proposals keep committing
  via the remaining quorum; the pending map does not leak entries.
- **E3:** `GroupStatus`/registry expose propose-e2e, prepare, accept,
  first-quorum-RPC, and apply latency summaries; a benchmark run shows the
  summaries populated and consistent with client-observed latency.
- **E4:** backoff between identical attempts varies across retries
  (jitter observable in a unit test with a fixed RNG seed).
- **E5:** under a saturated accept load (outbound queue full),
  `send_heartbeat` still succeeds; existing LearnerStream ordering tests
  pass.
- Write regression sentinel (`tools/bench-write-regression.sh`) shows no
  throughput regression; single-degraded-follower scenario shows p99
  improvement.
