<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

# R53 Design — Separate gRPC Channel for Leader Heartbeats

## Problem

The per-peer `LearnerStream` multiplexes `Accept`, `Heartbeat`, and
`ChosenNotification` frames FIFO through one h2 bidi stream per
`(group_id, peer_id)` pair. The send-half loop
(`learner_stream.rs:399-416`) drains `cmd_rx` in arrival order and calls
`out_tx.send(frame).await`, which blocks on h2 flow-control until the
frame is flushed to the wire. E5 (heartbeat reserved capacity) guarantees
a heartbeat can be *admitted* to the outbound mpsc queue even when the
accept path has saturated the shared portion — but once admitted, the
heartbeat still sits behind every accept already queued, and
`out_tx.send().await` flushes them in order. With 16 KiB values, the
cumulative wire-flush time of N accepts can exceed the election timeout.
The follower's election deadline fires, a spurious election challenges
the leader, and the leader loses quorum.

Observed as intermittent `kv scan failed: not leader` errors in the
`valuesize_16KiB` scan bench (452 errors in one run, 0 in others). See
`kv-scan-flow-analysis.md` "Existing Problems" §R53.

## Current behavior

- `remote_replica.rs:340-382` — `send_heartbeat` builds an
  `RpcHeartbeatRequest` and calls `self.learner_stream().send_heartbeat(rpc_req)`,
  routing through the bidi stream for FIFO ordering with accepts.
- `learner_stream.rs:184-214` — `PxLearnerStream::send_heartbeat` enqueues
  a `FrameKind::Heartbeat` cmd and awaits the reply oneshot.
- `learner_stream.rs:233-280` — `dispatch` implements E5: non-heartbeat
  frames are rejected when `non_heartbeat_count >= window - heartbeat_reserve`;
  heartbeats bypass the count check.
- `px_service.rs:320-358` — unary `heartbeat` RPC exists, calls
  `on_heartbeat`. Currently unused for steady-state heartbeats.
- `px_service.rs:427-435` — bidi `learner_stream` handler dispatches
  `Heartbeat` frames via `handle_heartbeat_inner` (shared helper that
  also calls `on_heartbeat`).

## Why the FIFO invariant can be relaxed

`design-crow-kv-rpc.md` §3 invariant 1 states: a heartbeat reordering
ahead of an Accept could cause the follower to reject the Accept while
already having promised not to vote. Code analysis shows this hazard
does not hold:

- `handle_heartbeat` (`local_replica.rs:1492-1513`) mutates `election_state`
  (`current_term`, `voted_for`, `leader_id`, `vote_lockout_until`).
  `on_accept_inner` (`local_replica.rs:1159-1184`) checks the term fence
  then calls the acceptor's per-slot ballot CAS. The two operate on
  **independent state**.
- `vote_lockout_until` only gates `handle_request_vote` /
  `handle_pre_vote` (`local_replica.rs:1406,1441`), not `on_accept`. A
  heartbeat extending the lockout cannot cause an accept to be rejected.
- The only coupling is `current_term`, and the term fence
  (`req.term < local_term → TermStale`) handles all cross-term reordering
  correctly — a stale-term accept being rejected is correct behavior
  (the old leader lost leadership).
- Same-term reordering is harmless: heartbeat and accept mutate
  independent state.

The `term` **is** the epoch mechanism — no new timestamp or epoch field
is needed. The heartbeat already carries `t_send_ms_mono` /
`lease_grant_until_ms_mono` for lease management, not ordering.

## Proposed approach

Route steady-state heartbeats over a **dedicated gRPC `Channel`**
(separate TCP connection) using the existing unary `heartbeat` RPC.
Accepts and `ChosenNotification` stay on the `LearnerStream`.

### Dedicated channel vs. reuse `get_client()`

R53 specifies a dedicated channel, separate from both the
`LearnerStream` and the `get_client()` channel used by
Prepare/PreVote/RequestVote/StepDown. Rationale:

- **Isolation**: the control RPCs (Prepare, RequestVote) can burst
  during elections; a dedicated heartbeat channel keeps liveness
  messages isolated from any future control-RPC bulk traffic.
- **Lifecycle clarity**: the heartbeat channel is established lazily on
  first heartbeat (mirroring the `LearnerStream` connect-on-first-use
  pattern) and reused for the peer's lifetime. A separate `OnceCell`
  makes the lifecycle independent of the control-RPC channel.
- **Cost**: one additional TCP connection per peer (3 connections total
  per peer instead of 2). Negligible — heartbeats are tiny and
  infrequent (150 ms interval in production).

### E5 reserve removal

The E5 heartbeat reserved capacity (`FrameKind::Heartbeat`,
`non_heartbeat_count`, `heartbeat_reserve`, the CAS logic in `dispatch`)
becomes dead code once heartbeats leave the `LearnerStream`. **Remove it
entirely** rather than keep it defensive:

- Dead code adds complexity without benefit — the `FrameKind` enum, the
  `non_heartbeat_count` atomic, the `heartbeat_reserve` field, and the
  CAS loop in `dispatch` all become meaningless.
- The config field `learner_stream_heartbeat_reserve` is internal to
  `crow-kv` (not exposed to clients or the management API), so removing
  it is a clean internal change.
- The `LearnerStream` window is fully available to accepts and
  `ChosenNotification` after removal, which is the correct behavior —
  those are the only frame kinds it carries.

### Server-side bidi handler

Keep the `learner_stream_request::Frame::Heartbeat` branch in the
server-side `learner_stream` handler (`px_service.rs:427-435`) and
`handle_heartbeat_inner` for backward compatibility during rolling
upgrades — a follower still sending heartbeat frames over the bidi
stream must be handled. The proto `HeartbeatRequest heartbeat = 2` field
in `LearnerStreamRequest` stays (removing a oneof variant is a breaking
proto change). The branch is harmless: it just won't be exercised once
all peers upgrade.

## Alternatives considered

- **Reuse `get_client()` for heartbeats**: would also satisfy the core
  goal (heartbeats off the accept-saturated `LearnerStream`). Rejected
  because control RPCs (Prepare, RequestVote) can burst during
  elections and share that channel — a dedicated channel gives
  heartbeats unconditional isolation.
- **Wire-level priority on the existing bidi stream** (h2 stream
  priority): h2 priority is advisory and widely ignored by
  implementations; not a reliable fix. Also doesn't help with the
  send-half mpsc ordering.
- **Custom transport (R32)**: targets the h2 connection lock
  (throughput), a different problem. R53 is shippable now without R32.

## File-level changes

- `remote_replica.rs`:
  - Add `heartbeat_client: OnceCell<Box<PxServiceClient<Channel>>>`.
  - Add `get_heartbeat_client()` (mirrors `get_client()`, separate
    `Endpoint`/channel).
  - Rewrite `send_heartbeat` to call `client.heartbeat(rpc_req)` via
    `get_heartbeat_client()`, with `tokio::time::timeout` (mirrors
    `send_prepare`).
  - Remove `heartbeat_reserve` field, `learner_stream_heartbeat_reserve`
    from `with_config` / `learner_stream()` cfg construction.
  - Update module doc comment.
- `learner_stream.rs`:
  - Remove `FrameKind` enum, `kind` field from `OutboundCmd`.
  - Remove `LearnerStreamReply::Heartbeat` variant.
  - Remove `send_heartbeat` method.
  - Remove `heartbeat_reserve`, `non_heartbeat_count`, `window` fields.
  - Simplify `dispatch` to a plain `try_send` (no CAS, no kind check).
  - Remove `non_heartbeat_count` from `run_learner_stream`,
    `fail_queued_commands`.
  - Remove `Heartbeat` branch from `dispatch_response`.
  - Update module doc comment.
- `config.rs`:
  - Remove `learner_stream_heartbeat_reserve` field and its values in
    `DEFAULT`, `for_tests`, `for_e2e`.
  - Update the field's doc comment block.
- `px_service.rs`: no change (server-side bidi heartbeat branch stays
  for backward compat).
- `pxos.proto`: no change (proto fields stay for backward compat).
- `design-crow-kv-rpc.md`:
  - §3: update "Why a Dedicated Bidi Stream" — heartbeats move to a
    separate unary channel; document the relaxed FIFO invariant and
    the term-fence safety argument.
  - §6.3: remove "Heartbeat Reserved Capacity" section (E5 is gone).
  - §4: update "Bidi stream" description — now carries `Accept` +
    `Chosen` (no `Heartbeat`).

## Acceptance test plan

- Existing heartbeat/lease/election tests pass unchanged (the unary
  handler calls the same `on_heartbeat`, so semantics are identical).
  Key tests: `g4_learner_stream_test.rs` (rapid-fire writes, chosen
  notification), all `group/` election tests.
- `cargo clippy -- -D warnings` and `cargo fmt --check` pass.
- `pixi run test-core` passes (the relevant test crate).
- The `valuesize_16KiB` scan bench no longer produces intermittent
  `not leader` errors under write backpressure (the 452-error run
  becomes a 0-error run) — verified separately by running the bench.
