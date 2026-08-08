<!-- Copyright 2026-present buzzcrow <buzzcrow@126.com> -->
<!-- Licensed under the Apache License, Version 2.0. -->

### R65: Apply Correctness Fix + ChosenNotice Out-of-order Apply + Async Catch-up + Snapshot Fallback

**Supersedes**: R64 (dedicated runtime rejected — see bug-report-r63-test-hang.md;
catch-up approach revised from `select!` arm to independent task).
**Depends on**: R63 (value-less catch-up + background apply loop).
**Fixes**: R63 correctness bug — follower applies un-chosen values.

**Problem**: Four issues in the leader → follower replication and
catch-up path. The first is a correctness bug; the rest are performance
and liveness problems that can stall heartbeat delivery and trigger
spurious leader elections under production configs where
`heartbeat_interval` is 500 ms (single-DC) or 3 s (WAN):

1. **Follower applies un-chosen values (correctness bug).** R63's
   `handle_accept_inner` (`px_service.rs:611-614`) advances
   `known_commit_slot` and wakes the apply loop when a follower accepts
   a single Accept RPC. But Accept (Paxos Phase 2) only means "this
   acceptor has accepted the value" — it does **not** mean the value is
   chosen (chosen = quorum of acceptors accepted). The leader has not
   yet received quorum confirmation when the follower processes the
   Accept. If the leader crashes before quorum and a new leader
   proposes a different value (or NoOp) at the same slot, the follower
   has already applied the old value to its engine — a state divergence
   that apply cannot reverse.

   Scenario:
   - Leader sends `Accept(slot=S, value=V)` to F1, F2.
   - F1 accepts → R63 advances `known_commit_slot=S` → apply loop
     writes V to engine.
   - Leader crashes before F2 replies. S is not chosen.
   - New leader runs bulk Phase 1; F2 has no accepted value at S.
   - New leader proposes `NoOp` at S → NoOp is chosen.
   - F1 has V in engine; S was actually chosen as NoOp. Divergence.

   This violates Paxos safety. Raft's follower never applies on
   `AppendEntries` — it only appends to the log and applies when
   `leaderCommit` advances (leader-confirmed commit point). CROW must
   follow the same discipline: apply only after the leader confirms
   quorum.

2. **ChosenNotice doesn't trigger apply (missed out-of-order
   opportunity).** `fan_out_chosen_notice` (`group.rs:2542`) is called
   after the leader confirms quorum — at this point the slot is
   genuinely chosen. But the follower's `note_chosen` handler
   (`learner.rs:302`) only updates the `last_chosen_slot` high-water
   mark; it does not advance `contiguous_chosen`, does not trigger
   apply. This means a follower that already has the value in its
   acceptor (from the Accept RPC) cannot apply it until the next
   heartbeat delivers a `committed_safe_slot` that covers this slot —
   even though the leader has already confirmed it chosen.

   This is especially wasteful under CROW's parallel slot model: slots
   can be chosen out-of-order (slot 5 chosen while slot 3 is still in
   gap repair). The heartbeat's `committed_safe_slot` is a continuous
   prefix (`contiguous_chosen`), so it cannot advance past slot 3 until
   slot 3 is resolved. Slot 5's value sits in the follower's acceptor,
   chosen but unapplied, waiting for the continuous prefix to catch up.
   ChosenNotice should trigger immediate out-of-order apply for slots
   the follower already has — this is CROW's parallel-slot advantage
   over Raft's sequential log.

3. **Catch-up replay blocks the heartbeat round.**
   `run_heartbeat_round` (`group_election.rs:527-630`) performs
   synchronous catch-up inline: for each lagging follower it sends up
   to 64 `send_accept().await` calls (full payload, one round-trip
   each) before the heartbeat round returns. With 64 lagging slots at
   1 MB each and 10 ms cross-AZ RTT, a single heartbeat round spends
   640 ms in catch-up — exceeding the 500 ms `heartbeat_interval` and
   eating into the 4 s `election_min`. The leader stops sending
   heartbeats fast enough to maintain its lease, triggering
   re-election. R63's `BatchChosenNotice` reduces payload transfer for
   present slots, but the full-accept fallback for missing slots still
   runs inline. The catch-up is also **leader-driven and blind**: the
   leader resends the entire range without knowing which specific slots
   the follower is missing, wasting bandwidth on re-sending values the
   follower already has.

4. **No snapshot fallback for severely lagging followers.** A follower
   that falls behind by thousands of slots (e.g. after a long network
   partition or restart) is catch-up-replayed slot-by-slot, round after
   round, 64 slots at a time. This can take minutes, during which the
   follower's `contiguous_applied` stays low and the leader keeps
   retrying. Raft and TiKV both fall back to snapshot install when the
   lag exceeds a threshold, bypassing log replay entirely. CROW has a
   snapshot mechanism (`design-crow-kv-state-machine.md` §6.4) but no
   catch-up trigger for it.

**Comparison with Raft and TiKV**:

- **Raft**: `AppendEntries` is both heartbeat and replication, but the
  leader never blocks its tick waiting for a follower to catch up. Each
  `AppendEntries` sends one batch of entries (bounded by `maxEntries`),
  advances `nextIndex` on success, and retries on the next tick. Catch-up
  is incremental across many ticks. A severely lagging follower triggers
  `InstallSnapshot` (when `nextIndex` falls before the log start).
  Critically, the follower **appends** entries on `AppendEntries` but
  **applies** only when `leaderCommit` advances — the leader-confirmed
  commit point. CROW's current Accept-path apply violates this
  discipline. Raft has no out-of-order apply because its log is strictly
  sequential; CROW's parallel slots allow out-of-order apply, but only
  if ChosenNotice triggers it (currently it doesn't).
- **TiKV**: separates the Raft tick from entry sending via the `ready`
  mechanism — the tick produces a `Ready` struct (entries to send,
  commit index, etc.) and an async sender dispatches it without blocking
  the tick. `max_size_per_msg` (default 1 MB) bounds each message;
  `max_inflight_msgs` (default 256) bounds per-follower concurrency;
  `raft_log_gc_threshold` triggers snapshot when the gap is too large.
- **CROW current**: catch-up is synchronous inside the heartbeat round,
  no size bound per round, no snapshot trigger, followers apply
  un-chosen values, and ChosenNotice doesn't trigger out-of-order apply.
  This is the worst of the three designs for both correctness and
  heartbeat latency.

**Solution**: Four changes:

1. **Fix apply correctness — Accept path stores only** — align CROW's
   apply discipline with Raft's. The follower's `handle_accept_inner`
   must **not** advance `known_commit_slot` or wake the apply loop. It
   only runs `on_accept` (stores the value in the acceptor + persists to
   WAL). The value is accepted but not yet chosen — same as Raft's
   "append to log but don't apply."

   - **Accept path** (`handle_accept_inner`): `on_accept` only (store
     to acceptor + WAL persist). Remove `update_chosen_frontier`,
     `advance_known_commit_slot`, `wake_apply_loop` from this path.
   - **R63 bug fix (follower → leader transition)**: R63 advanced
     `known_commit_slot` in the Accept path specifically to handle the
     case where a follower accepts slots, wins an election, and becomes
     leader (leaders don't receive heartbeats or ChosenNotice, so
     without some advancement the apply loop would never learn about
     those slots). The correct fix is in the new leader transition
     (bulk Phase 1): the sweep range is `[contiguous_chosen + 1 ..
     highest_seen_slot]`, which includes the entire in-flight slot
     window. For each slot in this range, Phase 1 Prepare recovers any
     accepted value from the quorum (or fills NoOp if none), then Phase
     2 Accept chooses it. After the sweep, `contiguous_chosen` has
     advanced to cover all previously in-flight slots, and the new
     leader advances `known_commit_slot = max(known_commit_slot,
     contiguous_chosen)` + wakes the apply loop. All slots up to
     `contiguous_chosen` are now confirmed chosen by the quorum that
     responded to Prepare — safe to apply. This also handles slots the
     new leader accepted as a follower but never applied: they're
     either in the continuous prefix (already chosen) or in the sweep
     range (re-chosen via Phase 1).

2. **Fix ChosenNotice to trigger out-of-order apply (with ballot
   verification)** — ChosenNotice is sent by the leader **after** it
   confirms quorum (`AcceptAttempt::Chosen` branch in
   `group.rs:1454-1465`), so the slot is genuinely chosen when the
   follower receives the notice. The follower-side handler should check
   whether it has the value in its acceptor **at the same ballot** as
   the chosen value, and if so, trigger immediate apply — enabling
   CROW's parallel-slot out-of-order apply advantage.

   - **Ballot verification (correctness)**: the current ChosenNotice
     frame (`RpcChosenNotification`) carries `(slot, term, leader_id)`
     but **not** the ballot round. This is insufficient for safe apply.
     A follower may have accepted a value at a **lower ballot** than the
     chosen value — the follower's value is stale, not the chosen value.
     Applying it would diverge from the true chosen value.

     Scenario:
     - Old leader L1 sends `Accept(S, ballot=(0,L1), V1)` to F1, F2.
     - F1 accepts V1 at ballot `(0,L1)`.
     - L1 crashes before F2 replies. V1 is **not chosen**.
     - New leader L2 wins election, runs bulk Phase 1 at ballot
       `(T2,L2)`. F2 has no accepted value at S → L2 proposes NoOp.
     - Quorum (L2 + F2) accepts NoOp at `(T2,L2)` → **NoOp is chosen**.
     - L2 sends `ChosenNotice(S, term=T2, leader_id=L2)`.
     - F1 receives ChosenNotice, checks `acceptor.accepted_at(S)` →
       finds V1 at ballot `(0,L1)`.
     - **Without ballot verification**: F1 applies V1 — **wrong**, the
       chosen value is NoOp. F1 never received the Accept for NoOp
       (partitioned or lost).
     - **With ballot verification**: F1 sees `(0,L1) < (T2,L2)` → V1
       is stale, **not** the chosen value. F1 records S as a gap →
       FetchGap. Leader replies with NoOp at `(T2,L2)`. F1 overwrites
       V1 with NoOp in its acceptor, applies NoOp. Correct.

   - **ChosenNotice proto change**: add `ballot_round: u64` to
     `RpcChosenNotification` (currently carries only `leader_id`, not
     the round). The full chosen ballot is `(ballot_round, leader_id)`.
     This lets the follower compare its accepted ballot against the
     chosen ballot.
   - **ChosenNotice handler** (`px_service.rs`, `Chosen` frame): change
     from `note_chosen` (high-water mark only) to:
     - If `acceptor.accepted_at(slot).ballot == chosen_ballot`: the
       follower has the chosen value at the chosen ballot — call
       `update_chosen_frontier(slot, term)` (advances `contiguous_chosen`
       with out-of-order drain) + `wake_apply_loop()`. Apply now.
     - If `acceptor.accepted_at(slot).ballot < chosen_ballot`: the
       follower has a **stale** value (accepted at a lower ballot, not
       the chosen value). Do NOT apply. Record slot as gap → FetchGap.
       The FetchGap reply will carry the chosen value at the chosen
       ballot; the follower overwrites the stale value in its acceptor
       and then applies.
     - If `acceptor.accepted_at(slot).is_none()`: call `note_chosen(slot,
       term)` (high-water mark only). The follower doesn't have the value
       at all — record as gap → FetchGap.
   - **Apply loop target**: the apply loop's target becomes
     `max(known_commit_slot, last_chosen_slot)` — `known_commit_slot`
     covers the continuous prefix (from heartbeat), `last_chosen_slot`
     covers out-of-order chosen slots (from ChosenNotice). The
     skip-and-continue logic (from R63) handles gaps: slots that are
     accepted-but-not-chosen are skipped; slots that are chosen and
     present in the acceptor at the chosen ballot are applied.
   - **Chosen-ness check**: the apply loop must distinguish
     "accepted but not chosen" from "chosen" before applying. A slot
     ≤ `contiguous_chosen` is chosen (continuous prefix). A slot in the
     learner's out-of-order set is chosen (individually confirmed via
     ChosenNotice with ballot match). A slot in the acceptor but not in
     either is accepted-but-not-chosen — skip. This check prevents
     applying values that the leader hasn't confirmed.
   - **BatchChosenNotice handler** (catch-up path): same ballot
     verification logic — for each slot in the range, if the follower
     has the value at the chosen ballot, advance `known_commit_slot` /
     `update_chosen_frontier` + wake apply loop. Otherwise record as
     gap → FetchGap.
   - **Frame count unchanged**: per-propose frames remain `2*(N-1)`
     (Accept + ChosenNotice). Both frames now have distinct, necessary
     roles: Accept pushes the value, ChosenNotice confirms chosen (with
     ballot) and triggers apply. This is the CROW analog of Raft's single
     `AppendEntries` (which bundles both), split into two frames to
     support parallel-slot out-of-order apply.

3. **Follower-driven catch-up via FetchGap** — replace the current
   leader-driven blind catch-up with a follower-driven, on-demand
   design. In steady state, the Accept + ChosenNotice path delivers
   every chosen value to every follower — there are no gaps. Gaps arise
   only from **accidental** causes: election churn (follower rejected an
   Accept because it promised a higher ballot), temporary network
   partition, or LearnerStream not yet connected. These are few and
   specific, not bulk. The catch-up mechanism should reflect this: the
   follower knows exactly which slots it's missing, and requests only
   those.

   - **Gap detection on the follower**: when a follower receives a
     ChosenNotice (or BatchChosenNotice) for a slot that it does not
     have in its acceptor (`accepted_at(slot).is_none()`), it records
     the slot as a gap. The heartbeat's `committed_safe_slot` may also
     reveal gaps: if `committed_safe_slot > contiguous_applied` and the
     apply loop finds missing slots in the range, those are gaps.
   - **FetchGap request**: the follower sends a `FetchGap(slot)` request
     to the leader for each missing slot. This is a new LearnerStream
     frame type — a lightweight request carrying only `(slot, term,
     group_id)`. The follower can batch multiple FetchGap requests into
     one frame if several slots are missing.
   - **Leader response**: the leader handles `FetchGap` as follows:
     - **Leader has the value** (`acceptor.accepted_at(slot).is_some()`):
       reply with the full entry (payload + ballot + term). The follower
       overwrites any stale lower-ballot value in its acceptor with the
       chosen value, then runs `update_chosen_frontier` +
       `wake_apply_loop` — apply the chosen value.
     - **Leader doesn't have the value** (the leader itself has a gap at
       this slot — possible under parallel slots if the leader's own
       gap repair hasn't reached this slot yet): the leader runs
       classic Paxos (Prepare → Accept) to resolve the slot, then
       replies with the resolved value. This reuses the existing
       `repair_once` gap-repair machinery. If the slot is truly
       un-chosen (no quorum ever accepted a value), the leader proposes
       NoOp, chooses it, and replies with NoOp.
   - **Stale value overwrite**: when the FetchGap reply arrives, the
     follower's acceptor may have a stale value at a lower ballot. The
     reply carries the chosen ballot (higher), so overwriting is
     Paxos-safe: a higher-ballot accepted value always supersedes a
     lower-ballot one. The follower stores the chosen value at the
     chosen ballot in its acceptor, discarding the stale value. The
     stale value is never applied to the engine.
   - **No BatchChosenNotice in catch-up**: the BatchChosenNotice
     mechanism from R63 is no longer needed for catch-up. ChosenNotice
     (per-slot, in the propose path) already tells the follower which
     slots are chosen. The follower requests only the specific slots
     it's missing. BatchChosenNotice can be removed or retained for
     future use, but the primary catch-up path is FetchGap.
   - **Catch-up is follower-local, not a leader task**: there is no
     `run_catchup_loop` on the leader. The follower's apply loop
     detects gaps and sends FetchGap requests as part of its normal
     operation. This means:
     - No `peer_state` map on the leader for catch-up.
     - No `catchup_notify` signal from heartbeat to a catch-up task.
     - No `peer_catchup_cursor` — the follower tracks its own gaps.
     - The heartbeat round stays pure liveness + lease (no catch-up
       coupling at all).
   - **Bounded FetchGap**: the follower limits the number of
     outstanding FetchGap requests (`MAX_INFLIGHT_FETCHGAP`, default 16)
     to avoid overwhelming the leader. Gaps are resolved incrementally
     as FetchGap replies arrive.
   - **Snapshot fallback**: if the follower's gap count exceeds
     `catchup_snapshot_threshold` (default: `bulk_prepare_window` =
     1024 slots), the follower requests a snapshot install instead of
     individual FetchGap requests. This handles the severe-lag case
     (restart, long partition) where per-slot FetchGap would be too
     slow. The snapshot install uses the existing mechanism
     (`design-crow-kv-state-machine.md` §6.4).
   - **No commit-advance notify needed**: ChosenNotice (change 2) is the
     low-traffic freshness mechanism. It is fire-and-forget, sent
     immediately after quorum confirmation, and triggers per-slot apply
     on the follower without waiting for a heartbeat round-trip. The
     regular heartbeat (at `heartbeat_interval`) advances
     `committed_safe_slot` (the continuous prefix) for safe-slot /
     follower-read calculations — this is not latency-sensitive and
     does not need acceleration. R64's `commit_advance_notify` idea was
     considered and rejected: it would add a `Notify` + `select!` arm +
     min-interval guard for the sole benefit of advancing the continuous
     prefix ~500 ms sooner, which is not worth the complexity when
     ChosenNotice already handles per-slot apply.

4. **Snapshot fallback for severely lagging followers** — folded into
   change 3's FetchGap design. When the follower's gap count exceeds
   `catchup_snapshot_threshold` (default: `bulk_prepare_window` = 1024
   slots), the follower requests a snapshot install instead of
   individual FetchGap requests. The leader sends a snapshot via the
   existing snapshot install mechanism
   (`design-crow-kv-state-machine.md` §6.4). After snapshot install,
   the follower's `contiguous_applied` jumps to the snapshot slot,
   and normal FetchGap handles any remaining gap.

   - **Threshold rationale**: `bulk_prepare_window` (1024) is the same
     bound the new-leader bulk Phase-1 uses. A follower lagging more
     than one bulk-prepare window is effectively a "new joiner" —
     per-slot FetchGap would require 1024+ requests. A snapshot is one
     transfer regardless of slot count.
   - **No new RPC**: uses the existing snapshot install path. The
     follower detects the threshold, requests snapshot install, and
     skips FetchGap until the snapshot completes.

**Architecture after changes**:

```
Propose path (steady state):
  propose → Accept to all peers (N-1 frames, with payload)
    → follower: on_accept only (store to acceptor + WAL persist)
       NO apply — value is accepted but not yet chosen
  → quorum confirmed → leader learn_chosen (apply to leader engine)
    → fan_out_chosen_notice (N-1 frames, with chosen ballot)
    → follower ChosenNotice handler:
       if has value at chosen ballot: update_chosen_frontier + wake_apply_loop
       → apply loop applies this slot (out-of-order OK — parallel slot advantage)
       if has stale value (lower ballot): record as gap → FetchGap
         (stale value is NOT applied; FetchGap overwrites with chosen value)
       if missing value: note_chosen (high-water mark only)
         → follower records slot as gap → sends FetchGap to leader
  → ack client

Heartbeat path (pure liveness + lease + continuous-prefix commit):
  heartbeat tick (regular interval)
    → send HeartbeatRequest(committed_safe_slot) to all peers
    → follower: known_commit_slot.fetch_max(committed_safe_slot)
                + wake_apply_loop()  ← advances continuous prefix apply
    → collect replies, check higher term, renew lease
    → return (one RTT, no catch-up, no peer_state tracking)

Catch-up path (follower-driven, on-demand):
  follower detects gap:
    (a) ChosenNotice for slot where acceptor has no value, OR
    (b) ChosenNotice for slot where acceptor has stale value (lower ballot), OR
    (c) apply loop finds missing slot in committed range
    → if gap count > snapshot_threshold: request snapshot install, done
    → else: send FetchGap(slot) to leader (batched, ≤ MAX_INFLIGHT_FETCHGAP)
    → leader:
      → if leader has value: reply with full entry (payload + ballot + term)
      → if leader doesn't have value: run classic Paxos (Prepare → Accept)
        to resolve slot, then reply with resolved value (or NoOp)
    → follower: overwrite stale/missing value with chosen value in acceptor
      → update_chosen_frontier + wake_apply_loop → apply chosen value
    (catch-up runs entirely on follower side, does not involve heartbeat round)

New leader transition (bulk Phase 1):
  sweep range: [contiguous_chosen + 1 .. highest_seen_slot]
    (includes the in-flight slot window — slots the old leader opened
     but may not have chosen before the election)
  for each slot in range:
    → Phase 1 Prepare (empty payload) → adopts any peer's accepted value
      (recovers in-flight values that a quorum member accepted)
    → if no peer accepted: NoOp (fills the gap, advances contiguous_chosen)
    → Phase 2 Accept → chosen
    → leader learn_chosen + fan_out_chosen_notice
  after sweep:
    next_slot = highest_seen_slot + 1  (new proposals start past the window)
    known_commit_slot = max(known_commit_slot, contiguous_chosen)
    wake_apply_loop()
    → apply loop processes:
      (a) slots the new leader accepted as follower but never applied
          (leaders don't receive heartbeats/ChosenNotice)
      (b) slots resolved by the sweep (recovered in-flight values + NoOps)
    → all slots up to contiguous_chosen are now chosen and apply-eligible

Apply path (follower background):
  apply loop:
    target = max(known_commit_slot, last_chosen_slot)
    for slot in [contiguous_applied+1 .. target]:
      if slot is chosen (≤ contiguous_chosen OR in out-of-order set)
         AND acceptor has value at chosen ballot:
        spawn_blocking apply
      else if slot is chosen but acceptor missing/stale value:
        record as gap → FetchGap request (catch-up path)
      else:
        skip (accepted-but-not-chosen)
```

**Two-layer apply trigger design**:

The apply loop has two complementary triggers, each serving a distinct
purpose:

- **ChosenNotice (per-slot, out-of-order)**: sent by leader after quorum
  confirmation. Triggers immediate apply for individual chosen slots
  that the follower already has in its acceptor. This is CROW's
  parallel-slot advantage — slot 5 can be applied before slot 3 is
  resolved, because CROW only has blind operations (Put/Delete) and
  per-key slot tracking (engine accepts higher-slot values for the same
  key, lower-slot writes are idempotent no-ops).
- **Heartbeat `committed_safe_slot` (continuous prefix)**: advances
  `known_commit_slot`, the continuous-prefix commit point. Drives
  apply for the contiguous range and keeps `contiguous_chosen` fresh
  for safe-slot computation (follower reads, linearizable scans).

Both advance the apply loop's target via `max(known_commit_slot,
last_chosen_slot)`. The apply loop's chosen-ness check ensures only
genuinely chosen slots are applied — accepted-but-not-chosen slots are
skipped.

**Scope**:
- `lib/crow-kv/src/rpc/proto/pxos.proto` — add `ballot_round: u64` to
  `RpcChosenNotification` (currently carries only `leader_id`, not the
  round — needed for ballot verification on the follower). Add
  `FetchGapRequest` message (slot, term, group_id, leader_id) +
  `fetch_gap` frame in `LearnerStreamRequest` oneof. Add
  `FetchGapResponse` message (slot, term, ballot_round, leader_id,
  payload, group_id) + `fetch_gap_reply` frame in
  `LearnerStreamResponse` oneof.
- `lib/crow-kv/src/rpc/px_service.rs` — `handle_accept_inner`: remove
  `update_chosen_frontier`, `advance_known_commit_slot`,
  `wake_apply_loop` (lines 611-614). Accept path becomes `on_accept`
  only. `Chosen` frame handler: change from `note_chosen` to
  ballot-verified apply — if accepted ballot == chosen ballot:
  `update_chosen_frontier` + `wake_apply_loop`; if accepted ballot <
  chosen ballot (stale): record gap → FetchGap; if no accepted value:
  `note_chosen` + record gap → FetchGap. New `FetchGap` frame handler
  (leader side): look up slot in acceptor, reply with value + ballot
  or trigger classic Paxos repair then reply.
- `lib/crow-kv/src/cluster/group.rs` — keep `fan_out_chosen_notice`
  calls in propose path (lines 1465, 1829) — ChosenNotice is now
  necessary (triggers out-of-order apply). Update
  `fan_out_chosen_notice` to include `ballot_round` in the
  `RpcChosenNotification`. Add `FetchGap` handler method: resolve slot
  from acceptor or trigger `repair_once` for that slot.
- `lib/crow-kv/src/cluster/group_election.rs` — strip catch-up replay
  from `run_heartbeat_round` (lines 527-630). Heartbeat round becomes
  pure liveness + lease (no catch-up, no peer_state, no catchup_notify).
  `run_bulk_phase1` (lines 184-297) already sweeps the correct range
  `[contiguous_chosen+1 .. highest_seen_slot]` including the in-flight
  window — verify this logic is preserved. After bulk Phase 1
  completion: advance `known_commit_slot = max(known_commit_slot,
  contiguous_chosen)` + `wake_apply_loop()` (new addition — currently
  missing, which was the R63 bug motivation).
- `lib/crow-kv/src/cluster/local_replica.rs` — `handle_heartbeat`:
  keep `known_commit_slot.fetch_max(committed_safe_slot)` +
  `wake_apply_loop()`. Apply loop: change target to
  `max(known_commit_slot, last_chosen_slot)`, add chosen-ness check
  before applying, record gaps for FetchGap. Add `send_fetch_gap`
  method + `MAX_INFLIGHT_FETCHGAP` bound. Add snapshot threshold check
  in gap-detection logic.
- `lib/crow-kv/src/cluster/remote_replica.rs` — add `send_fetch_gap`
  wrapper (sends FetchGap frame via LearnerStream, awaits reply).
- `lib/crow-kv/src/paxos/learner.rs` — expose `is_chosen(slot)` query
  (checks `contiguous_chosen` or out-of-order set) for the apply loop's
  chosen-ness check.
- `lib/crow-tree/ffi/src/lib.rs` — remove `AsyncCrowtree::apply_put`,
  `AsyncCrowtree::apply_delete`, `AsyncCrowtree::put`, `AsyncCrowtree::del`
  (the `spawn_blocking` wrappers, lines 1574-1602). These are only used
  by tests and benchmarks, not the production data path (which uses
  `apply_batch_external` via `CrowTreeEngine::apply`). Removing them
  eliminates unnecessary `spawn_blocking` thread hops in the test/bench
  path.
- `lib/crow-tree/ffi/tests/ffi_test.rs` — replace all
  `t.apply_put(slot, key, value)` / `t.apply_delete(slot, key)` calls
  (18 occurrences) with `t.handle().apply_batch_external(slot, ops)` or
  the synchronous `Crowtree::apply_put` / `Crowtree::apply_delete` on
  the inner handle (no `spawn_blocking` needed for in-memory test
  trees). For async test contexts, use the synchronous `Crowtree` API
  directly via `t.handle()`.
- `lib/crow-tree/ffi/examples/async_get_bench.rs` — replace
  `tree.apply_put(slot, key, value).await` (1 occurrence) with
  `tree.handle().apply_batch_external(slot, ops)` (synchronous, no
  `spawn_blocking`).
- `lib/crow-kv/tests/election/` — verify:
  - Follower does NOT apply on Accept (value in acceptor but not
    engine until ChosenNotice or heartbeat).
  - Follower applies on ChosenNotice when it has the value (out-of-order
    apply: slot 5 applied before slot 3).
  - Follower applies on heartbeat `committed_safe_slot` (continuous
    prefix).
  - Follower sends FetchGap for missing slots; leader replies with value.
  - Leader runs classic Paxos to resolve a slot it doesn't have, then
    replies to FetchGap.
  - New leader applies accepted-as-follower slots after bulk Phase 1.
  - Heartbeat round latency is independent of follower lag.
  - Snapshot fallback triggers when gap count exceeds threshold.
  - Large-value FetchGap does not block heartbeat delivery.

**Complexity**: Medium — the apply correctness fix (change 1) is small
code but large correctness impact. The ChosenNotice fix (change 2)
requires a proto change (add `ballot_round`) + ballot-verified handler
+ apply loop target adjustment. The follower-driven FetchGap catch-up
(change 3) is the largest piece: new proto frame type, follower-side
gap detection + FetchGap sending, leader-side FetchGap handler with
classic Paxos fallback + stale value overwrite. The snapshot fallback
(change 4) reuses the existing snapshot install path. The
`AsyncCrowtree` cleanup (scope item) is mechanical: delete 4 functions,
update 19 call sites in tests/bench. Two proto changes (`ballot_round`
on ChosenNotice, new `FetchGap` frame). No runtime split (rejected by
R64 bug report).

**Relationship to R64**: This requirement supersedes R64. R64 proposed
(1) a dedicated tokio runtime for election work and (2) catch-up as a
`select!` arm. The dedicated runtime was attempted in commit `791d6ae`
and caused test hangs and panics (see `doc/working/bug-report-r63-test-hang.md`):
`Cannot drop a runtime` panics, paused-clock test failures, `shutdown()`
deadlocks. The root issue is that `Runtime::drop` blocks until all
spawned tasks complete, which is unsafe in async context and untestable
under `tokio::time::pause()`. R65 keeps everything on the main runtime
and solves the heartbeat-staleness problem by making the heartbeat
round cheap (no inline catch-up) rather than by isolating it on a
separate runtime. R64's `commit_advance_notify` idea was considered
and rejected (ChosenNotice is a better low-traffic freshness mechanism).
R64 should be marked as superseded and its backlog entry removed or
updated to point to R65.

**Relationship to R63**: R65 fixes a correctness bug introduced by R63.
R63's `handle_accept_inner` advances `known_commit_slot` on Accept,
causing followers to apply values that are not yet chosen. R65 changes
the Accept path to store-only (matching Raft's append-but-don't-apply
discipline) and drives apply from two leader-confirmed sources:
ChosenNotice (per-slot, out-of-order) and heartbeat
`committed_safe_slot` (continuous prefix). The R63 bug that motivated
the Accept-path advancement (follower → leader transition deadlock) is
fixed differently: the new leader's bulk Phase 1 already sweeps the
in-flight window `[contiguous_chosen+1 .. highest_seen_slot]`,
recovering accepted values from the quorum via Phase 1 Prepare and
choosing them via Phase 2 Accept. After the sweep, the new leader
advances `known_commit_slot = max(known_commit_slot, contiguous_chosen)`
+ wakes the apply loop — all slots up to `contiguous_chosen` are now
confirmed chosen. R63's `BatchChosenNotice` is superseded by the
follower-driven FetchGap design — the follower requests only the
specific slots it's missing, rather than the leader blindly resending
ranges. R63's background apply loop and `known_commit_slot` /
`apply_notify` infrastructure are retained and extended.

**Alternatives considered**:

- **A: Keep Accept-path apply, add a "chosen" flag to Accept RPC.**
  Rejected — the leader doesn't know whether the value is chosen at
  the time it sends the Accept (it hasn't received quorum yet). Adding
  a flag doesn't help; the fundamental issue is that chosen-ness is a
  quorum property that only the leader can confirm after collecting
  replies. ChosenNotice (sent after quorum) is the correct carrier.

- **B: Remove ChosenNotice, rely only on heartbeat for apply.**
  Rejected — heartbeat's `committed_safe_slot` is a continuous prefix
  (`contiguous_chosen`). Under CROW's parallel slot model, slots can be
  chosen out-of-order (slot 5 chosen while slot 3 is in gap repair).
  Relying only on the continuous prefix would force slot 5 to wait for
  slot 3 — wasting CROW's parallel-slot advantage. ChosenNotice enables
  per-slot out-of-order apply, which is the whole point of parallel
  slots. Also, under production heartbeat intervals (500 ms – 3 s),
  relying only on heartbeat would add unacceptable apply latency for
  low-traffic workloads.

- **C: Catch-up as a `select!` arm in `run_leader_state` (R64's
  approach).** Rejected — a `select!` arm still shares the same task
  as the heartbeat tick. If the catch-up arm is executing (sending
  accepts, awaiting replies), the heartbeat tick arm is not polled.
  This is better than inline catch-up (the `select!` can be biased to
  prefer heartbeat), but it still couples catch-up progress to the
  leader state machine's scheduling. An independent task fully
  decouples them — the heartbeat loop never waits for catch-up, even
  if catch-up is mid-RPC.

- **D: Dedicated runtime (R64's approach).** Rejected — see
  `doc/working/bug-report-r63-test-hang.md`. `Runtime::drop` is
  blocking, panics in async context, and breaks `tokio::time::pause()`
  in tests. The complexity of cross-runtime coordination (`Notify`,
  `Arc` sharing, shutdown ordering) is not justified when the real
  problem is catch-up blocking the heartbeat round, not runtime
  contention. Making the heartbeat round cheap (no catch-up) is the
  correct fix.

- **E: Increase `MAX_CATCHUP_PER_ROUND` to catch up faster.** Rejected
  — larger batches mean longer blocking. The problem is that catch-up
  runs inside the heartbeat round at all, not that the batch is too
  small. The fix is to move it out, not to tune its size.

- **G: Leader-driven catch-up (BatchChosenNotice + blind full-accept
  replay, R63's approach).** Rejected — the leader blindly resends the
  entire lagging range without knowing which specific slots the follower
  is missing. In steady state there are zero gaps (Accept + ChosenNotice
  delivers everything); gaps are few and specific (election churn,
  partition). A follower-driven FetchGap requests only the missing
  slots, saving bandwidth and leader CPU. The leader-driven approach
  also cannot handle the case where the leader itself has a gap at the
  requested slot — FetchGap triggers classic Paxos repair on the leader
  side, resolving the gap for both leader and follower.

- **F: `commit_advance_notify` (R64's idea) for immediate heartbeat on
  write.** Rejected — ChosenNotice (change 2) already provides
  per-slot, fire-and-forget apply triggering immediately after quorum
  confirmation, without a heartbeat round-trip. `commit_advance_notify`
  would add a `Notify` + `select!` arm + min-interval guard for the
  sole benefit of advancing the continuous prefix (`committed_safe_slot`)
  ~500 ms sooner. The continuous prefix is used for safe-slot / follower-read
  calculations, which are not latency-sensitive. The extra complexity
  is not justified.

**Metrics**: add counters and gauges to the Paxos replication flow so
that abnormal branches are observable in tests and production. Normal
steady-state paths (Accept stored, ChosenNotice applied with ballot
match) are **not** counted — only the exception branches that indicate
election churn, stale values, gaps, or catch-up activity.

**Naming convention**: all metrics use the existing prefix pattern
`s.{store_id}.g.{group_id}` (see `local_replica.rs:909`,
`group.rs:479`). R65 extends the prefix with the replica's ID and
current role to distinguish which replica and which role produced the
counter:

```
s.{store_id}.g.{group_id}.r.{replica_id}{role_tag}.paxos.{metric}.{suffix}
```

Where:
- `{replica_id}` — `PxLocalReplica.id` (u64), the replica's own ID.
- `{role_tag}` — single-letter role tag: `L` if Leader, omitted if
  Follower (the default). This matches the GUI convention where a
  replica is either the Leader or a regular replica — no intermediate
  states are tracked in metrics (PreCandidate/Candidate are transient
  and not replication-flow relevant). Examples: `r.2L` = replica 2 as
  Leader, `r.2` = replica 2 as Follower.
- `{suffix}` — `.c` for counters (monotonic), `.g` for gauges
  (non-monotonic), matching the existing convention.

Example: `s.1.g.3.r.2.paxos.chosen_notice.stale_ballot.c` = store 1,
group 3, replica 2 as Follower, stale-ballot counter.
`s.1.g.3.r.1L.paxos.fetchgap.received.c` = store 1, group 3, replica 1
as Leader, FetchGap-received counter.

All counters are per-replica, registered via the existing
`MetricsRegistry` pattern (`ElectionRegistryHandles` /
`ReadRegistryHandles` on `PxLocalReplica` / `PxGroup`).

Counters (monotonic `Counter`, never reset except on process restart):

- `paxos.chosen_notice.stale_ballot.c` (follower-side) — follower
  received ChosenNotice where `accepted.ballot < chosen.ballot` (stale
  value, not applied → FetchGap). Indicates election churn: the
  follower accepted a value from an old leader that was never chosen.
  Zero in healthy steady state; spikes after leader changes.
- `paxos.chosen_notice.missing_value.c` (follower-side) — follower
  received ChosenNotice where `acceptor.accepted_at(slot).is_none()`
  (no value at all → FetchGap). Indicates the follower missed the
  Accept RPC entirely (partition, LearnerStream not connected). Zero in
  healthy steady state; spikes after network blips or restarts.
- `paxos.fetchgap.sent.c` (follower-side) — follower sent a FetchGap
  request to the leader (covers both stale-ballot and missing-value
  cases). The sum of `stale_ballot` + `missing_value` should equal
  `fetchgap.sent` (one FetchGap per detected gap).
- `paxos.fetchgap.received.c` (leader-side) — leader received a
  FetchGap request from a follower. Should equal the sum of
  `fetchgap.sent` across all followers in the group (modulo in-flight +
  leader change).
- `paxos.fetchgap.leader_has_value.c` (leader-side) — leader had the
  requested value in its acceptor and replied directly (no Paxos round
  needed). The common catch-up case.
- `paxos.fetchgap.leader_classic_paxos.c` (leader-side) — leader did
  not have the requested value and ran classic Paxos (Prepare →
  Accept) to resolve the slot before replying. Indicates the leader
  itself had a gap — non-zero only after election churn with parallel
  slots.
- `paxos.fetchgap.noop_filled.c` (leader-side) — leader's classic
  Paxos resolved the slot as NoOp (no peer had accepted any value). The
  slot was truly un-chosen; NoOp fills it so `contiguous_chosen` can
  advance.
- `paxos.snapshot.catchup_triggered.c` (follower-side) — follower's
  gap count exceeded `catchup_snapshot_threshold` and requested
  snapshot install instead of FetchGap. Indicates severe lag (restart,
  long partition).
- `paxos.bulk_phase1.sweep_slots.c` (leader-side) — new leader's bulk
  Phase 1 swept this many slots (the in-flight window size). Should
  equal `highest_seen_slot - contiguous_chosen` at leader transition.
  Spikes after leader changes with high in-flight slot count.
- `paxos.bulk_phase1.noop_filled.c` (leader-side) — new leader's bulk
  Phase 1 filled this many slots with NoOp (no peer had accepted a
  value). Indicates slots that were opened but never accepted by any
  quorum member before the old leader crashed.
- `paxos.bulk_phase1.value_recovered.c` (leader-side) — new leader's
  bulk Phase 1 recovered an accepted value from a peer (Prepare
  adopted a previously-accepted value). Indicates in-flight slots that
  were partially accepted before the old leader crashed.
- `paxos.apply.skipped_not_chosen.c` (follower + leader) — apply loop
  skipped a slot because it is accepted-but-not-chosen (in the
  acceptor but not in `contiguous_chosen` or the out-of-order set).
  Normal for in-flight slots; should drain as ChosenNotice / heartbeat
  advances the frontiers. A persistently-high rate indicates the apply
  loop is spinning on slots that never get chosen (stuck gap).

Gauges (current value, non-monotonic):

- `paxos.gap_count.g` (follower-side) — follower's current number of
  detected gaps (missing or stale slots awaiting FetchGap). Zero in
  steady state; spikes after election churn / partition; should drain
  to zero as FetchGap replies arrive.
- `paxos.fetchgap.inflight.g` (follower-side) — follower's current
  number of outstanding FetchGap requests (bounded by
  `MAX_INFLIGHT_FETCHGAP`). Zero in steady state.
- `paxos.last_chosen_slot.g` (follower + leader) — `last_chosen_slot`
  (highest slot ever seen as chosen, gaps allowed). For monitoring
  out-of-order apply progress.
- `paxos.known_commit_slot.g` (follower + leader) — `known_commit_slot`
  (continuous prefix from heartbeat). For monitoring heartbeat-driven
  apply progress. The gap between `last_chosen_slot` and
  `known_commit_slot` shows how many slots are chosen-but-not-yet-in-
  the-continuous-prefix (out-of-order apply window).

**Scope (metrics)**:
- `lib/crow-kv/src/cluster/local_replica.rs` — add
  `ReplicationRegistryHandles` struct (mirrors
  `ElectionRegistryHandles`) with the follower-side counters and
  gauges above. Register via `set_metrics_registry` (existing pattern),
  extending the prefix to `s.{store_id}.g.{group_id}.r.{replica_id}{role_tag}`
  where `role_tag` is `L` if `self.is_leader()`, omitted otherwise.
  Increment at each branch point in the ChosenNotice handler, FetchGap
  send, apply loop skip, and snapshot threshold check.
- `lib/crow-kv/src/cluster/group.rs` — add leader-side FetchGap and
  bulk Phase 1 counters (`fetchgap.received`,
  `fetchgap.leader_has_value`, `fetchgap.leader_classic_paxos`,
  `fetchgap.noop_filled`, `bulk_phase1.sweep_slots`,
  `bulk_phase1.noop_filled`, `bulk_phase1.value_recovered`) to a new
  `LeaderReplicationHandles` struct. Register with the same
  `r.{replica_id}L` prefix pattern (leader-side counters always carry
  the `L` tag).
- `lib/crow-kv/tests/election/` — assert counter values in tests:
  - `stale_ballot` increments when follower has stale value.
  - `missing_value` increments when follower has no value.
  - `fetchgap.sent` == `stale_ballot` + `missing_value`.
  - `fetchgap.leader_has_value` increments when leader replies directly.
  - `fetchgap.leader_classic_paxos` increments when leader runs Paxos.
  - `bulk_phase1.noop_filled` increments for un-chosen slots.
  - `bulk_phase1.value_recovered` increments for recovered values.
  - `gap_count` gauge drains to zero after catch-up completes.
  - Counter names include the correct `r.{replica_id}{role_tag}` prefix.


- `handle_accept_inner` does NOT call `update_chosen_frontier`,
  `advance_known_commit_slot`, or `wake_apply_loop`. It only calls
  `on_accept` (store to acceptor + WAL).
- Follower applies on ChosenNotice when it has the value in acceptor
  **at the chosen ballot** (out-of-order apply: a higher slot can be
  applied before a lower gap is resolved).
- Follower does NOT apply on ChosenNotice when its accepted ballot is
  **lower** than the chosen ballot (stale value) — records gap →
  FetchGap instead.
- Follower applies on heartbeat `committed_safe_slot` (continuous
  prefix).
- Follower does NOT apply accepted-but-not-chosen slots (chosen-ness
  check in apply loop).
- Follower sends FetchGap for missing or stale slots; leader replies
  with chosen value + ballot; follower overwrites stale value and
  applies.
- New leader advances `known_commit_slot` after bulk Phase 1 and wakes
  apply loop (fixes R63's follower→leader deadlock without Accept-path
  apply).
- `fan_out_chosen_notice` remains in the propose path (triggers
  out-of-order apply on followers).
- `run_heartbeat_round` does not call `send_accept` or
  `send_batch_chosen_notice`; it only sends heartbeats and collects
  replies.
- Catch-up is follower-driven (FetchGap), not leader-driven (no
  `run_catchup_loop`, no `peer_state`, no `catchup_notify`).
- FetchGap is bounded by `MAX_INFLIGHT_FETCHGAP`.
- Followers with gap count exceeding `catchup_snapshot_threshold`
  request snapshot install instead of FetchGap.
- Heartbeat round latency is ≤ one RPC round-trip regardless of
  follower lag.
- All existing `crow-kv` tests pass.
- New tests:
  - Follower does not apply on Accept (value in acceptor, not engine).
  - Follower applies on ChosenNotice when accepted ballot == chosen ballot.
  - Follower does NOT apply on ChosenNotice when accepted ballot <
    chosen ballot (stale value → FetchGap instead).
  - Out-of-order apply: slot 5 applied before slot 3 is resolved.
  - Follower applies on heartbeat `committed_safe_slot`.
  - Accepted-but-not-chosen slot is NOT applied (chosen-ness check).
  - Follower sends FetchGap for missing slot; leader replies with value.
  - Follower sends FetchGap for stale slot; leader replies with chosen
    value; follower overwrites stale value and applies chosen value.
  - Leader resolves own gap via classic Paxos then replies to FetchGap.
  - New leader applies accepted-as-follower slots after bulk Phase 1.
  - Heartbeat not delayed when follower lags by 1000+ slots.
  - Snapshot fallback triggers when gap count exceeds threshold.
  - Large-value (1 MB) FetchGap does not block heartbeat delivery.
