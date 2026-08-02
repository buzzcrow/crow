<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R43: Write-path fan-out hardening (quorum short-circuit, RPC deadline, phase metrics)

**Problem**: the write-path review (see
`doc/working/write-flow-analysis.md`) found that after R16a the
per-proposal critical path is `max(local fsync, quorum RPC)` — but the
"quorum RPC" term is currently `max(ALL peers)`, not `max(fastest
quorum)`, and the fan-out has no deadline, no phase-level visibility,
and several smaller robustness gaps. Six enhancements, grouped in one
requirement because they all touch the same fan-out code
(`run_prepare_phase` / `run_accept_phase` and the `PxLearnerStream`)
and are best implemented as one coherent pass:

- **E1 — Quorum short-circuit.** Both phases fan out with
  `join_all` and fold replies only after every remote answers
  (`group.rs` L1547 prepare; L1739 R16b accept; L1817 R16a accept).
  In a 3-node group quorum = local + 1 remote, yet every proposal
  waits for both remotes — per-proposal latency is `max(all peers)`
  instead of the quorum-th fastest. One slow (but connected) follower
  drags every write even though quorum is reachable. This is the
  largest remaining latency lever that needs no new transport (R32)
  and no durability tradeoff (R16b).
- **E2 — No deadline on accept/prepare RPCs.** `send_accept` awaits
  its correlation oneshot with no timeout (`learner_stream.rs` L119);
  nothing in `remote_replica.rs` / `learner_stream.rs` sets a
  deadline. A disconnected peer fast-fails via the reconnect loop,
  but a connected-yet-unresponsive peer (GC pause, half-open
  connection, overloaded server) blocks `join_all` indefinitely —
  today one hung follower freezes the whole group. E1 removes the
  latency exposure; E2 is still required to reclaim the pending-map
  entry, the oneshot, and (transitively) the admission permit.
- **E3 — No write-path phase metrics.** `MetricHandles` carries only
  read-path summaries (`barrier`, `engine_get` — `group.rs` L35-45).
  The write side has WAL fsync latency and admission counters but no
  propose-e2e, prepare-phase, accept-quorum-RPC, or apply latency
  summaries; the benchmark analysis *infers* the critical path
  instead of measuring it. Phase summaries directly quantify E1's
  win and settle the `max(fsync, RPC)` hypothesis.
- **E4 — Retry backoff: no jitter, permit held while sleeping.**
  `retry_backoff` is deterministic `base * 2^attempt` (`group.rs`
  L1980), so proposers that collide on a slot retry in lockstep.
  All backoff sleeps also run while the admission permit is held,
  shrinking the effective window exactly when the group is
  contended.
- **E5 — Heartbeats compete with accepts on the LearnerStream
  queue.** `dispatch` is a `try_send` into one shared
  `learner_stream_window_frames` (64) mpsc for Accept, Heartbeat,
  and ChosenNotification (`learner_stream.rs` L174-191). Under write
  saturation a heartbeat can queue behind ~64 accepts or be
  `Busy`-rejected outright — heartbeat RTT (and thus lease/election
  stability) degrades precisely at peak write load.
- **E6 — Reply-fold duplication.** The remote-reply `match` fold
  (Accepted/Rejected/TermStale/EpochMismatch/Err) is triplicated
  across the prepare phase and both accept paths (~150 duplicated
  lines in `group.rs`). Not user-visible, but E1 rewrites exactly
  this code — extracting a fold helper first makes E1 a small diff.

**Approach** (ordered; E6 → E1 → E2 form the core sequence, E3/E4/E5
are independent):

- **E6 (enabler)**: extract a `ReplyFold` accumulator struct
  (`accepted/promised` count, `highest_rejected_round`,
  `highest_seen_term`, `epoch_mismatch`, `adopted`) with
  `fold_local(reply)` / `fold_remote(remote, reply)` methods; the
  three call sites shrink to loops over the helper. Pure refactor, no
  behavior change.
- **E1**: replace `join_all` with `FuturesUnordered` (or
  `select_all`-style loop) tagged with each remote's voting flag.
  Fold replies as they arrive; once `accepted >= quorum` AND the
  local reply has been folded (W6: the local WAL persist must be
  counted, R16a semantics unchanged; in the R16b path the local CAS
  reply), stop awaiting and return. Move the still-pending futures
  into a detached drain task that keeps folding for side effects
  only: a late `TermStale` triggers `become_follower`, a late
  `EpochMismatch` adopts the epoch — so failure detection is not
  lost, it just leaves the latency path. Negative outcomes
  (rejection/term/epoch) still require draining enough replies to
  decide Retry vs Fail, as today.
- **E2**: wrap the oneshot await in `send_accept` /
  `send_heartbeat` with `tokio::time::timeout` (config:
  `learner_stream_rpc_timeout_ms`, default aligned with the election
  heartbeat interval budget, e.g. 1-2s); on expiry remove the
  pending-map entry and surface a typed retryable error (maps to the
  existing `Busy`/unavailable handling). Unary `send_prepare` gets a
  tonic request timeout. Consider h2 keepalive on the channel as
  belt-and-braces for silent half-open connections.
- **E3**: extend `MetricHandles` with write-path summaries —
  `propose_e2e`, `prepare_phase`, `accept_phase`,
  `accept_quorum_rpc` (first-quorum latency, meaningful after E1),
  plus an `engine_apply` summary at the learner. Registered in
  `set_metrics_registry` alongside the read-path handles; naming per
  `design/design-observability.md` conventions. Cheap: one
  `Instant::now()` pair per phase.
- **E4**: add ±50% random jitter to `retry_backoff`. Keep the permit
  held during backoff (releasing it would let a duplicate
  `(client_id, seq)` admit concurrently and complicates dedup
  ordering) — document the choice at the call site.
- **E5**: reserve a priority lane for heartbeats: either a second
  small mpsc drained with `biased` priority by the send-half task,
  or reserved capacity (reject accepts at `capacity - k`, allow
  heartbeats up to full). Ordering constraint to preserve: a
  heartbeat must not race *ahead* of an accept it logically follows
  — the current design multiplexes them for this reason
  (`design/design-rpc.md`). The safe variant is reserved capacity
  (heartbeats keep FIFO order with accepts but can never be starved
  of a slot); the priority-lane variant needs a design-doc check
  first.

Out of scope (already tracked elsewhere): server-side proposal
coalescing (R36), apply fence / async engine apply default (R35),
custom RPC transport (R32), per-proposal `tokio::spawn` in
`spawn_accept_persist` / `spawn_learn_chosen` (only matters once
R16b/R17 default on — fold into T1/R35 if measurable).

**Performance impact**:
- E1 turns accept-phase remote latency from `max(all remotes)` into
  `k-th fastest` (k = quorum - 1 remotes when local votes). With 3
  nodes this is `min` of 2 remotes instead of `max` — direct p50 and
  especially p99 reduction, and near-total insulation from one
  degraded follower. With 5 nodes the effect grows.
- E2 converts an unbounded stall into a bounded, retryable failure —
  availability, not throughput.
- E3/E4/E5 are observability and robustness; no measurable hot-path
  cost (E3 is two `Instant` reads per phase; E4 is one RNG call per
  retry, not per proposal; E5 only changes behavior when the queue
  is near-full).

**Dependencies**: none hard. E1 composes with R16a/R16b as-is.
E3's first-quorum metric presumes E1. R32 (custom RPC) would rewrite
the transport under E2/E5 but E1/E6 survive unchanged.

**Priority**: Medium-high — E1/E2 are the top remaining write-path
latency and availability items; the rest ride along in the same
files.

**Complexity**: Medium — E1 is the delicate part (quorum-with-W6
early return + detached drain preserving TermStale/Epoch side
effects); E6 de-risks it. E2/E3/E4 are small and mechanical. E5
needs a short design check against the LearnerStream ordering
contract.

**Files**: `crowkv/src/cluster/group.rs` (fold helper, phase
fan-out, backoff, metrics handles), `crowkv/src/cluster/learner_stream.rs`
(RPC timeout, heartbeat lane), `crowkv/src/cluster/remote_replica.rs`
(unary prepare timeout, channel keepalive),
`crowkv/src/common/config.rs` (timeout + reserve knobs),
`crowkv/src/paxos/learner.rs` (apply summary),
`doc/design/design-rpc.md` + `doc/design/design-observability.md`
(fold final design in after merge).

**Acceptance**:
- E1: a 3-node group with one follower delayed (test-util injected
  latency) proposes at the latency of the fast follower, not the
  slow one; a late `TermStale` from the slow follower still steps
  the leader down. W6 holds: `Chosen` is never returned before the
  local reply is folded (R16a: local WAL persisted; R16b: local CAS).
- E2: a hung (connected, non-responding) peer causes accept RPCs to
  fail within the configured timeout; proposals keep committing via
  the remaining quorum; the pending map does not leak entries.
- E3: `GroupStatus`/registry expose propose-e2e, prepare, accept,
  first-quorum-RPC, and apply latency summaries; benchmark run shows
  the summaries are populated and consistent with client-observed
  latency.
- E4: backoff between identical attempts varies across retries
  (jitter observable in logs/tests).
- E5: under a saturated accept load (outbound queue full),
  `send_heartbeat` still succeeds; existing LearnerStream ordering
  tests pass.
- Write regression sentinels (`tools/bench-write-regression.sh`)
  show no throughput regression; single-degraded-follower scenario
  shows p99 improvement.
