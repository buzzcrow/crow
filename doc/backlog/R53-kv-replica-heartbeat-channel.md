<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R53: Separate gRPC Channel for Leader Heartbeats

**Problem**: the per-peer `LearnerStream` multiplexes `Accept`,
`Heartbeat`, and `ChosenNotification` frames FIFO through one h2 bidi
stream per `(group_id, peer_id)` pair. The send-half loop
(`learner_stream.rs`) drains `cmd_rx` in arrival order and calls
`out_tx.send(frame).await`, which blocks on h2 flow-control until the
frame is flushed to the wire. E5 (heartbeat reserved capacity)
guarantees a heartbeat can be *admitted* to the outbound mpsc queue
even when the accept path has saturated the shared portion — but once
admitted, the heartbeat still sits behind every accept already queued,
and `out_tx.send().await` flushes them in order. With 16 KiB values,
the cumulative wire-flush time of N accepts can exceed the election
timeout. The follower's election deadline fires, a spurious election
challenges the leader, and the leader loses quorum.

Observed as intermittent `kv scan failed: not leader` errors in the
`valuesize_16KiB` scan bench (452 errors in one run, 0 in others). The
scan path itself is correct; the linearizable read barrier fails
because the leader cannot maintain quorum — its heartbeats are stuck
behind data on the shared connection. This is a
**correctness/availability** issue, not a throughput issue: the leader
is alive but its liveness messages are starved.

**Why the FIFO invariant can be relaxed.** The `LearnerStream` design
doc (`design-crow-kv-rpc.md` §3) justifies the single-stream design
with an ordering hazard: "a heartbeat reorders ahead of an Accept →
the follower could reject the Accept while already having promised not
to vote." Code analysis against the current implementation shows this
hazard does not hold:

- `handle_heartbeat` (`local_replica.rs`) mutates `election_state`
  (`current_term`, `voted_for`, `leader_id`, `vote_lockout_until`).
  `on_accept_inner` checks the term fence then calls the acceptor's
  per-slot ballot CAS. The two operate on **independent state**.
- `vote_lockout_until` only gates `handle_request_vote`, not
  `on_accept`. A heartbeat extending the lockout cannot cause an accept
  to be rejected.
- The only coupling is `current_term`, and the term fence
  (`req.term < local_term → TermStale`) handles all cross-term
  reordering correctly — a stale-term accept being rejected is correct
  behavior (the old leader lost leadership).
- Same-term reordering is harmless: heartbeat and accept mutate
  independent state.

The `term` **is** the epoch mechanism — no new timestamp or epoch field
is needed to make separate connections safe. The heartbeat already
carries `t_send_ms_mono` / `lease_grant_until_ms_mono`, but those are
for lease management, not ordering.

**Solution**: route steady-state heartbeats over a separate gRPC
`Channel` (separate TCP connection) using the **existing unary
`heartbeat` RPC** (`px_service.rs`). The unary RPC and its handler
(`handle_heartbeat_inner`) already exist and are currently unused for
steady-state heartbeats — `remote_replica.rs::send_heartbeat`
explicitly routes through the `LearnerStream` instead, for FIFO
ordering that is not a hard safety requirement. Accepts and
`ChosenNotification` stay on the `LearnerStream`.

This is a wiring change in `remote_replica.rs` (`send_heartbeat` →
separate channel + unary RPC), not new infrastructure. Heartbeats get
their own connection, never blocked behind data.

**Scope**:
- `remote_replica.rs`: `send_heartbeat` routes via a dedicated
  `PxServiceClient<Channel>` on a separate TCP connection, using the
  unary `heartbeat` RPC instead of `learner_stream().send_heartbeat`.
  The channel is established lazily on first heartbeat (mirroring the
  `LearnerStream` connect-on-first-use pattern) and reused across
  heartbeats for the peer's lifetime.
- `learner_stream.rs`: remove `FrameKind::Heartbeat` and the
  `send_heartbeat` path from `PxLearnerStream` (heartbeats no longer
  flow through the bidi stream). E5 heartbeat reserve becomes moot for
  the `LearnerStream` — the entire window is available to accepts and
  `Chosen`. Keep the reserve logic defensive (no harm if heartbeats
  never appear) or remove it, per implementation judgment.
- `px_service.rs`: the unary `heartbeat` RPC handler is already
  correct and shared (`handle_heartbeat_inner`) — no handler change
  needed.
- `design-crow-kv-rpc.md` §3: update the "Why a Dedicated Bidi Stream"
  section to document that heartbeats move to a separate unary channel
  and why the FIFO invariant is not required (term fence + independent
  state).

**Complexity**: Low–medium. The unary RPC and handler already exist;
the work is channel management in `remote_replica.rs` (lazy connect,
reconnect on transport failure, timeout) and removing the heartbeat
path from `LearnerStream`. The channel-management code mirrors the
existing `get_client()` pattern already used for `Prepare` / `PreVote`
/ `RequestVote` / `StepDown` unary RPCs.

**Independence from R32**: this is a pure gRPC change. R32 (custom
Rust RPC transport) targets the h2 *connection lock* (a throughput
issue). R53 targets *wire serialization* of heartbeats behind data (an
availability issue). Both stem from the single-connection design but
have independent solutions. R53 is shippable now; R32 remains deferred.

**Acceptance**:
- Steady-state heartbeats flow over a dedicated gRPC `Channel` separate
  from the `LearnerStream`; accepts and `ChosenNotification` remain on
  the `LearnerStream`.
- The `valuesize_16KiB` scan bench no longer produces intermittent
  `not leader` errors under write backpressure (the 452-error run
  becomes a 0-error run).
- A heartbeat delayed by accept backpressure on the `LearnerStream` is
  no longer possible — the heartbeat connection has no data traffic.
- Existing heartbeat/lease/election tests pass unchanged (the unary
  handler is shared, so semantics are identical).
- `design-crow-kv-rpc.md` §3 documents the relaxed FIFO invariant and
  the term-fence safety argument.

**Note**: the symptom analysis lives in
`doc/design/kv/kv-scan-flow-analysis.md` "Existing Problems" section.
