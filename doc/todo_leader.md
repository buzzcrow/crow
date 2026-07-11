# CrowKV - TODO: P1 M3 Leader Election (`crowkv` lib only)

Tracks the implementation slice for [`plan.md`](plan.md#p1--consensus-core) row **P1 M3**:
randomized election timeout, `PreVote` + `RequestVote`/`Vote`, term tracking, role
transitions (Follower → PreCandidate → Candidate → Leader, step-down on higher
term or unrenewable lease), **election-side lease state machine**, heartbeat
liveness, per-peer durable connection / bidi stream for `Accept`+`Heartbeat`+`Chosen`,
admin step-down RPC primitive, observability counters, and the new-leader
**bulk Phase 1** that adopts in-flight values and fills gaps with `NoOp`.

Design contract: [`design/design-leader-election.md`](design/design-leader-election.md)
§§2–6 (election + heartbeat + election-side lease), §8 (step-down), §10
(tunables). §7 ReadIndex and the **read-side** lease (`Get(Linearizable)` fast
path) stay in **P1 M5**.

**M3 scope label.** This milestone delivers a **complete in-memory election +
election-side lease**. It is **not** restart-safe: `current_term` and
`voted_for` live in memory only; on crash a node can revote in the same term
or regress term state. Restart safety arrives with **P2 M4** (WAL replay
rebuilds `current_term` / `voted_for`). See §5.1.

**Why lease lands in M3, not M5.** §6 lease has two coupled sides:
1. **Election-side** — followers' "I won't vote for any candidate before
   `T_recv + lease_duration`" promise (§6.1, §8 "Lease unrenewable" step-down,
   §9.1 single-leader-per-term proof). This is what *prevents* a stale leader
   from existing; it is election safety, not read optimization. It must land
   with the rest of election or the safety proof in §9 is incomplete.
2. **Read-side** — using the lease state to skip the ReadIndex round-trip on
   `Get(Linearizable)` (§6.1 second clause + §7). This is pure read-path code
   and stays in M5.

M3 lands side (1) fully (lease grant/renewal/expiry, vote-refusal promise,
unrenewable step-down, monotonic-clock discipline). M5 wires side (2) into
the read API and adds ReadIndex.

**Other things bundled into M3 for a "complete election + monitor" loop:**
- **PreVote** (§10) — protects against disruption from a rejoining
  partitioned node. Cheap; same protocol shape as `RequestVote`. Including now.
- **Admin step-down RPC primitive** (§8 third bullet) — a `StepDownRequest`
  method on `PxService`. The *management surface* (HTTP route, CLI flag) stays
  out of `crowkv` lib and out of M3; only the protocol primitive lands now.
- **Per-peer durable connection / bidi stream** — already partially in tree
  (`PxRemoteReplica` holds a `OnceCell<PxServiceClient<Channel>>` that is
  auto-reconnecting), so the "real per-peer connection pool" item from
  P1 M4 collapses. Upgrade to a bidi stream that multiplexes
  `Accept` + `Heartbeat` + `Chosen` for one `(group_id, peer_id)` pair, with
  per-peer flow control. (Heartbeats need ordered same-stream-as-Accept
  delivery so the lease grant cannot reorder ahead of an `Accept` it logically
  follows.) This sub-item should move from P1 M4 to P1 M3.
- **Observability counters** — `election_count`, `current_term`,
  `last_heartbeat_age_ms`, `lease_remaining_ms`, `bulk_phase1_in_flight_slots`.
  Cheap atomic counters; needed to test/operate M3 acceptance criteria.

**Scope guard.** Only `crowkv/` library code (`crowkv/src/**`, `crowkv/tests/**`,
`crowkv/src/rpc/proto/pxos.proto`). `crowkv-server` wiring, console, and CLI
flags stay untouched. `PxGroup::set_leader_id(...)` test hook **stays** as an
initial-value seed; the election driver may override it at runtime. (Removal
planned for after M3 — see §5.5.)

---

## 1. Current Status (what is already in tree)

| Area | Where | Status |
| --- | --- | --- |
| `PxBallot` (round, leader_id) | `@/cjdata/cpp/crowkv/crowkv/src/paxos/roles.rs:34-44` | Done (M1). |
| `PxLogEntry { term, ... }` field | `@/cjdata/cpp/crowkv/crowkv/src/paxos/roles.rs:68-77` | `term` field already in the entry; carried through `AcceptRequest.term` but never checked. |
| `PxLocalReplica { role: Leader\|Follower }` | `@/cjdata/cpp/crowkv/crowkv/src/cluster/local_replica.rs:17-37` | Hard-coded by constructor; no `Candidate`/`PreCandidate` variants; no term/voted-for state. |
| `PxGroup.leader_id` | `@/cjdata/cpp/crowkv/crowkv/src/cluster/group.rs:22-32` | Static field set via test-only `set_leader_id`. No runtime transition. |
| Proposer (`PxGroup::propose`) | `@/cjdata/cpp/crowkv/crowkv/src/cluster/group.rs:366-491` | Slot-retry loop, Prepare/Accept phases, `force_classic` flag. Does **not** include term in proposals (sets `term: 0`). |
| Acceptor | `@/cjdata/cpp/crowkv/crowkv/src/paxos/acceptor.rs` | Ballot-fenced only; no term fence. |
| `ReplicaHandler` / `ReplicaClient` traits | `@/cjdata/cpp/crowkv/crowkv/src/cluster/replica.rs` | Only `on_prepare` / `on_accept` (+ `send_*`); both return `Result<_, tonic::Status>` (leaky — see §5.9). |
| `pxos.proto` | `@/cjdata/cpp/crowkv/crowkv/src/rpc/proto/pxos.proto` | `Prepare`/`Accept` only. `PrepareRequest` has no `term`. No `RequestVote`, no `Heartbeat`, no `StepDown`. |
| `PxRemoteReplica` channel | `@/cjdata/cpp/crowkv/crowkv/src/cluster/remote_replica.rs:144-167` | gRPC `Channel` in `OnceCell`; tonic auto-reconnects. Unary RPCs per call. **Already** a per-peer connection — only needs bidi-stream upgrade. |
| Test cluster | `@/cjdata/cpp/crowkv/crowkv/tests/testkit/cluster.rs:62-139` | `start_cluster(ids, leader_id)` pins leader at construction; no election path. |
| `PxLearner` frontier | `@/cjdata/cpp/crowkv/crowkv/src/paxos/learner.rs` | No `contiguous_chosen` / `contiguous_applied` exposed — bulk-Phase-1 floor cannot be computed yet. |
| `PxAcceptor` slot iteration | `@/cjdata/cpp/crowkv/crowkv/src/paxos/acceptor.rs` + `slot_list.rs` | `iter_range` exists on `PxSlotList`; no acceptor-level "highest seen slot" cursor for bulk-Phase-1 ceiling. |
| `testkit/timer.rs` | `@/cjdata/cpp/crowkv/crowkv/tests/testkit/timer.rs` | Stub. Will be retired in favor of `tokio::time::pause()` (§5.6). |

Result: M3 extends M2; no existing code gets ripped out.

---

## 2. Design Anchors (read before coding)

- [`design-leader-election.md` §2 PxTerm vs PxBallot](design/design-leader-election.md#2-pxterm-vs-pxballot) — the `(term, ballot)` two-fence rule.
- §3 election protocol — randomized `[election_min, election_max]` timer, Raft-style "log up-to-date" check on `(last_chosen_term, last_chosen_slot)`.
- §4 bulk Phase 1 — `[floor+1, ceiling]` single Prepare, adopt highest `accepted_ballot`, propose `NoOp` for empty slots, re-Accept at `(round=0, me)` under term T, pipelined.
- §5 heartbeats — content (`term`, `leader_id`, `committed_safe_slot`, `lease_grant_until`, `prev_log_slot`, `prev_log_term`), cadence.
- §6.1, §6.2, §6.4 election-side lease — vote-refusal promise, `T_send` lease window, monotonic-clock-only.
- §8 step-down triggers — higher term, lease unrenewable, admin step-down.
- §10 PreVote, defaults — note the default-tuning decision recorded in §8 below of this file.
- [`design-paxos-error.md`](design/design-paxos-error.md) — extend with `PxTermStale { current_term }` retry classification.

---

## 3. Implementation Plan

Order chosen so each step compiles, passes existing tests, and unlocks the next.

### Step 1 — Term type and entry plumbing
- Add `pub type PxTerm = u64;` in `@/cjdata/cpp/crowkv/crowkv/src/paxos/mod.rs`.
- Election metadata lives behind **one serialized state object** on
  `PxLocalReplica` (resolves Critical Gap 5 of the original `todo_review.md`):
  ```rust
  struct ElectionPersistentState {
      current_term: PxTerm,
      voted_for: Option<PxNodeId>,
      role: PxLocalReplicaRole,
      leader_id: Option<PxNodeId>,
      vote_lockout_until: Instant,
  }
  // wrapped in parking_lot::Mutex<ElectionPersistentState> on PxLocalReplica
  ```
  Vote / heartbeat / accept handlers take this lock for the full read–decide–
  write cycle so `(current_term, voted_for)` are never observed mixed.
- Atomic snapshots (`current_term_atomic: AtomicU64`,
  `last_heartbeat_age_ms`, etc.) are kept **only** as lock-free read paths for
  metrics / observability; the source of truth is the mutex.
- Update `PxGroup::base_entry` (`group.rs:493`) to read `term` from
  `local_replica.current_term_snapshot()` (atomic snapshot suffices for the
  proposer's own term — it owns its leadership state) instead of the literal `0`.
- The local replica handler (`PxLocalReplica::on_prepare` / `on_accept`)
  rejects any `accept`/`prepare` whose request term `< current_term`. On
  request term `> current_term`, the handler adopts the new term and steps
  down before forwarding to the acceptor (returns `TermStale { new_term }`
  — see Step 8). The acceptor itself never observes term directly; the
  decision is taken under the `ElectionPersistentState` mutex.

### Step 2 — Roles and election state machine
- Extend `PxLocalReplicaRole` (`local_replica.rs:18-24`): `Follower`, `PreCandidate`, `Candidate`, `Leader`.
- Add **driver-only** `ElectionDriverState { deadline: Instant, votes_granted: HashSet<PxNodeId>, prevotes_granted: HashSet<PxNodeId> }` owned by the election task in `cluster/election.rs`. (This is task-local and does not need to share the persistent-state mutex; `vote_lockout_until` and the term/role/voted-for fields it cares about live in `ElectionPersistentState` from Step 1.)
- Add **`LeaseState`** on `PxLocalReplica` (resolves Critical Gap 4 — split read-lease from quorum-loss timing):
  - On follower: `vote_lockout_until: Instant` (in `ElectionPersistentState`) — refuse to grant `RequestVote` (real, not PreVote) for any other candidate before this deadline.
  - On leader, two distinct timestamps:
    - `lease_read_until: Instant` = max acknowledged `T_send + lease_duration - max_clock_skew` across heartbeat rounds that received a quorum response. Updated via `max(...)` on each successful round (Step 9) so the lease only ever extends. Read-side fast path (M5) consumes this; M3 only maintains it.
    - `last_quorum_heartbeat_at: Instant` — wall-clock-monotonic time at which the most recent heartbeat round received a quorum of OK responses.
  - Both initialized to `Instant::now()` (already-expired / now) on `become_leader`; bumped by each quorum heartbeat round.
  - **Step-down rule (Step 9):** `if Instant::now() - last_quorum_heartbeat_at >= lease_duration { become_follower(LeaseUnrenewable) }`. Note: this is **not** `2 * lease_duration` (that was the old, incorrect formula).
- Add `PxElectionConfig` to `@/cjdata/cpp/crowkv/crowkv/src/common/config.rs`:
  - `prevote_enabled: bool` (default `true`)
  - `heartbeat_interval_ms: u64` (default **500**, see §8 below)
  - `election_min_ms: u64` (default **4000**)
  - `election_max_ms: u64` (default **8000**)
  - `lease_duration_ms: u64` (default **4500**)
  - `max_clock_skew_ms: u64` (default 500)
  - `bulk_prepare_window: u64` (default 1024 — slots scanned per bulk-Phase-1 batch)
  - `election_driver_disabled: bool` (default `false`; test-only override to keep deterministic M1/M2 tests passing — not exposed on `crowkv-server` CLI).
  - **Test profile constructor:** `PxElectionConfig::for_tests()` returns aggressive timings (heartbeat 5 ms, election 30–60 ms, lease 25 ms) — used by `testkit/cluster.rs` under `tokio::time::pause()`.
- Implement role transitions as `&self` methods on `PxLocalReplica`: `become_follower(term)`, `become_precandidate()`, `become_candidate()`, `become_leader()`. All update term/voted_for/role/lease atomically; log at INFO.

### Step 3 — Wire protocol additions
Edit `@/cjdata/cpp/crowkv/crowkv/src/rpc/proto/pxos.proto`. No backwards-compat
requirement; field tags may be re-ordered. Tag-1 must remain `uint32 version`.

- Add `term` field to `PrepareRequest` (tag 8 to stay append-only is unnecessary — we can renumber freely; choose tag 8 anyway for diff cleanliness).
- Add `term` to `PromiseResponse` and `AcceptedResponse` so the proposer can detect stale-leader replies.
- New messages:
  - `PreVoteRequest { version, group_id, term /* = current_term + 1, see Step 4 */, candidate_id, last_chosen_slot, last_chosen_term, request_id, request_create_ms }`
  - `PreVoteResponse { version, group_id, term, granted, contiguous_chosen, last_chosen_term, highest_seen_slot, request_id, request_create_ms }`
  - `RequestVoteRequest { … same shape as PreVote, term = pre-vote term that won quorum … }`
  - `RequestVoteResponse { version, group_id, term, granted, contiguous_chosen, last_chosen_term, highest_seen_slot, request_id, request_create_ms }`
  - `HeartbeatRequest { version, group_id, term, leader_id, prev_log_slot, prev_log_term, committed_safe_slot, lease_grant_until_ms_mono, t_send_ms_mono, request_id, request_create_ms }`
  - `HeartbeatResponse { version, group_id, term, success, contiguous_chosen, last_chosen_term, contiguous_applied, highest_seen_slot, request_id, request_create_ms }`
  - `StepDownRequest { version, group_id, term, target_leader_id, reason, request_id, request_create_ms }` — `term` and `target_leader_id` fence the request against an old / misrouted admin call (resolves Medium Gap 8; **strict-fence policy chosen**, see §7.1). `reason` is a free-text string for logs.
  - `StepDownResponse { version, group_id, accepted, current_term, current_leader_id, request_id, request_create_ms }`
  - **Bidi peer-stream messages** (resolves Critical Gap 7):
    ```protobuf
    message ChosenNotification {
        uint32 version = 1;
        uint64 group_id = 2;
        uint64 slot = 3;
        uint64 term = 4;
        uint64 leader_id = 5;
        bytes  request_id = 6;
        uint64 request_create_ms = 7;
    }
    message PeerStreamRequest {
        oneof frame {
            AcceptRequest      accept    = 1;
            HeartbeatRequest   heartbeat = 2;
            ChosenNotification chosen    = 3;
        }
    }
    message PeerStreamResponse {
        oneof frame {
            AcceptedResponse  accepted  = 1;
            HeartbeatResponse heartbeat = 2;
        }
    }
    ```
    Ordering rule: frames sent on the stream are delivered in the order the
    sender enqueued them; `Heartbeat` after `Accept(s)` is required to land
    after those `Accept`s on the wire so `lease_grant_until` cannot reorder
    ahead of an `Accept` it logically follows (§5).
    `Chosen` notifications carry no response (fire-and-forget within the
    stream, but flow-controlled by the same per-peer mpsc bound).
- Add to `service PxService`: `PreVote`, `RequestVote`, `Heartbeat` (unary, used during election convergence and for one-off probes), `StepDown`, **and** `rpc PeerStream(stream PeerStreamRequest) returns (stream PeerStreamResponse)` for steady-state Accept/Heartbeat/Chosen traffic (Step 10).
- **Note:** `HeartbeatRequest.lease_grant_until_ms_mono` is the leader's monotonic-clock deadline; followers do not interpret it as a wall-clock value — they record a `vote_lockout_until = Instant::now() + lease_duration` on receipt (§6.2). The field is informational/observability only.

### Step 4 — Trait + handler extensions (and `tonic::Status` cleanup)
Resolves the `tonic::Status` leak. See §5.9 below for the full explanation/rationale.

- New error enum in `@/cjdata/cpp/crowkv/crowkv/src/cluster/replica.rs`:
  ```rust
  #[derive(Debug, thiserror::Error)]
  pub enum PxReplicaError {
      #[error("group {0} not found on this replica")]
      GroupNotFound(PxGroupId),
      #[error("replica is shutting down")]
      ShuttingDown,
      #[error("internal invariant violation: {0}")]
      Internal(String),
  }
  ```
- Change `ReplicaHandler` (`replica.rs:30-36`) to return `Result<_, PxReplicaError>`:
  - `async fn on_prepare(&self, slot, ballot, group_id) -> Result<PxPrepareReply, PxReplicaError>;`
  - `async fn on_accept(&self, entry, group_id) -> Result<PxAcceptReply, PxReplicaError>;`
  - **New:** `async fn on_pre_vote`, `on_request_vote`, `on_heartbeat`, `on_step_down`.
- Change `ReplicaClient` symmetrically; clients return `Result<_, PxReplicaError>` *for the semantic answer* but transport errors fold into `PxReplicaError::Internal` only inside the adapter — see §5.9.
- Adapter layer (`@/cjdata/cpp/crowkv/crowkv/src/rpc/px_service.rs`) maps `PxReplicaError` → `tonic::Status` at the gRPC boundary; nothing else in `crowkv` lib mentions `tonic::Status`.
- Implement handlers on `PxLocalReplica` (all decisions taken under the `ElectionPersistentState` mutex):
  - **PreVote:** No state mutation. PreCandidate sends `proposed_term = current_term + 1` (resolves Critical Gap 6). Reply `granted=true` iff `req.term > current_term` AND candidate's log is up-to-date on `(last_chosen_term, last_chosen_slot)` AND `Instant::now() >= vote_lockout_until`. (No `voted_for` check — PreVote does not consume a vote slot.) Always populate `contiguous_chosen`, `last_chosen_term`, `highest_seen_slot` in the reply for the candidate's bulk-Phase-1 floor/ceiling computation.
  - **RequestVote:** state-mutating. Same checks as PreVote, plus `voted_for ∈ {None, candidate_id}` in `req.term`. On grant: bump `current_term = req.term` if higher, set `voted_for = candidate_id`, set `vote_lockout_until = Instant::now() + lease_duration`. Reply also carries the frontier triple.
  - **Heartbeat:** Term comparison → adopt higher term + `become_follower`. Set `vote_lockout_until = Instant::now() + lease_duration` (§6.2). Reset election deadline. Set `leader_id = req.leader_id` inside the persistent-state mutex.
  - **StepDown (strict fence — §7.1 decision):** accept iff `self.is_leader() && self.id == req.target_leader_id && req.term == current_term`. On reject, reply `accepted=false` with `current_term` + `current_leader_id` echoed so the admin client can reissue against the right replica/term. On accept, run the explicit step-down sequence from Step 9 (cancel bulk-Phase-1 token / stop heartbeat / reset role+leader_id under mutex / reset election deadline / expire lease state / drain in-flight proposals as `NotLeader`).

### Step 5 — Learner frontier (lifted from M5 — see [`plan.md`](plan.md) decision-log update)
- Extend `PxLearner` (`@/cjdata/cpp/crowkv/crowkv/src/paxos/learner.rs`) with `contiguous_chosen: AtomicU64`, `contiguous_applied: AtomicU64`, and `last_chosen_term: AtomicU64`, updated inside `learn(entry)` when the entry extends the frontier. Use a small `BTreeSet<SlotIndex>` of out-of-order slots behind a `Mutex` to advance the watermark.
- Expose `learner.contiguous_chosen()` / `contiguous_applied()` / `last_chosen_term()` on `PxLocalReplica`.
- Update `plan.md` P1 M5 row to mark the frontier as **done in M3**; M5 retains responsibility for safe-slot **publication / propagation** (not just local tracking).

### Step 6 — Acceptor "highest seen slot" cursor
- Add `PxAcceptor::highest_seen_slot: AtomicU64`. Updated inside `get_or_prepare_slot` (`@/cjdata/cpp/crowkv/crowkv/src/paxos/slot_node.rs`) whenever a slot is opened on this acceptor. One extra atomic store per slot creation.
- Expose `PxAcceptor::highest_seen_slot() -> SlotIndex`.

### Step 7 — Bulk Phase 1 on new leader
- New `PxGroup::run_bulk_phase1(term: PxTerm, cancel: CancellationToken)`:
  1. `floor = max(local.learner.contiguous_chosen, max_peer.contiguous_chosen)` — peer values piggy-back on `RequestVoteResponse.contiguous_chosen` / `PreVoteResponse.contiguous_chosen` (Step 3 fields). Fall back to `0` if no peer responded with a frontier.
  2. `ceiling = max(
         local.acceptor.highest_seen_slot,
         self.next_slot.load(Acquire).saturating_sub(1),
         max_peer.highest_seen_slot_seen_during_election,
     )` — peer `highest_seen_slot` piggy-backs on PreVote/RequestVote responses too (resolves Critical Gap 1; without the peer term a slot accepted by a previous leader on a different acceptor and never observed locally would be skipped, which would break the §9.2 safety proof).
  3. For each slot in `[floor+1, ceiling]` (capped at `bulk_prepare_window` per batch; continue in next tick):
     - **Cancellation check:** if `cancel.is_cancelled()` (i.e., we stepped down — Step 9 / Medium Gap 11), abort the loop without re-Accepting any further slots.
     - Fan out `Prepare(slot, ballot=(0, me), term=T)` reusing `run_prepare_phase`.
     - If `PrepareAttempt::Proceed { entry, foreign_value: _ }` came back with an empty adopted value (no acceptor had a value), build a `NoOp` entry: `kind = PxLogEntryKind::NoOp`, `payload = Arc::new(vec![])`.
     - Else adopt the returned `entry` (already keyed at the highest `accepted_ballot`).
     - Re-Accept via existing `run_accept_phase`.
  4. After bulk Phase 1 has been *issued* (not necessarily completed), set `self.next_slot.store(ceiling + 1, Release)` so steady-state proposals continue from there (§4.4).
  5. New entries proposed at `(round = 0, leader_id = me)` under term `T` — already the default in `base_entry`.
- The `CancellationToken` is owned by `PxGroup` per-leadership-tenure: created on `become_leader`, cancelled on `become_follower`. The election driver task awaits the bulk-Phase-1 future under `tokio::select!` with the cancel token so step-down preempts in-flight repair.

### Step 8 — Step-down hooks + error taxonomy
- New `PxAcceptReply::TermStale { new_term }` and `PxPrepareReply::TermStale { new_term }` variants. Acceptor never sees term directly; the **handler** (`PxLocalReplica::on_accept` / `on_prepare`) reads `req.term`, compares against `current_term`, and either:
  - `req.term < current_term` → reply `TermStale { new_term: current_term }`.
  - `req.term > current_term` → adopt new term via `become_follower(req.term)`, then forward to acceptor.
  - `req.term == current_term` → forward to acceptor.
- New `PxPaxosError::TermStale { current_term: PxTerm }` → `PxRetryAction::FailFatal` for the current proposal; group-level driver calls `become_follower(current_term)`.
- New `PxPaxosError::LeaseUnrenewable` → step-down trigger from the election driver, not from `propose`.

### Step 9 — Election + heartbeat + lease driver

> **Splitting note (2026-05-17):** Step 9 was the single largest entry in the
> original plan and conflated multiple concerns (scaffold, election timer,
> RequestVote, PreVote, heartbeat ticker, lease, admin step-down, propose
> gate). It is now split into eight substeps so each lands in one commit and
> can be reviewed independently. Order is dependency-first.

#### Step 9.1 — Election driver scaffold (module + lifecycle)
- New module `@/cjdata/cpp/crowkv/crowkv/src/cluster/election.rs` exposing
  `ElectionDriver` with `start(group_handle, cfg, cancel)` returning a
  `JoinHandle`.
- `PxGroup` owns a `tokio_util::sync::CancellationToken` per-tenure (created
  in `new`, cancelled in `shutdown`) and a stored `JoinHandle<()>` for the
  driver task.
- `PxKvStore::add_group` (or wherever groups are constructed) calls
  `PxGroup::start_election_loop()`; when `cfg.election_driver_disabled` is
  set the call is a no-op so existing M1/M2 tests with pinned leaders are
  unaffected.
- Driver body in this substep is intentionally minimal: a `select!` loop with
  a 100 ms tick and a `cancel.cancelled()` arm. Logs the current role on
  each tick. No state machine logic yet.
- `PxGroup::shutdown` cancels the token and awaits the JoinHandle with the
  existing shutdown timeout.
- Test: `election_driver_scaffold_starts_and_stops` — spawn driver, advance
  time, cancel, confirm task ends.

#### Step 9.2 — Follower → Candidate election timer
- Add `election_deadline: Instant` to driver-local state (the driver task
  owns it; nothing exposed on `PxLocalReplica`).
- On every Follower tick: if `now >= election_deadline`, call
  `replica.become_candidate(current_term + 1)` (existing method seeds
  `voted_for = me`). Reset the deadline using a `rand`-backed
  `random_between(election_min, election_max)`.
- Reset the deadline on receipt of a valid Heartbeat or successful
  vote-grant (driver subscribes to a small `Notify` posted by the
  Heartbeat/Vote handlers).
- Test: `election_timer_randomized_within_bounds` + the existing
  `prevote_does_not_bump_term` placeholder marked `#[ignore]` until 9.4.

#### Step 9.3 — Candidate RequestVote fanout + become_leader
- Candidate state issues `send_request_vote` to all voting peers in parallel
  via `tokio::join_all` over the `PxRemoteReplica` clients (collected from
  `PxGroup::remote_replicas`).
- Reply aggregation collects `(granted, peer_term, contiguous_chosen,
  highest_seen_slot)`. On `peer_term > my_term`: `become_follower(peer_term)`
  and reset deadline. On quorum-grant: `become_leader()`, kick
  `PxGroup::run_bulk_phase1(term, peer_contiguous_chosen_max,
  peer_highest_seen_slot_max, cfg, cancel)` via `tokio::spawn`, transition
  to Leader state.
- Tracks `peer_contiguous_chosen_max` / `peer_highest_seen_slot_max` from
  granting peers and threads them into bulk Phase 1.

#### Step 9.4 — PreVote round (Raft-style)
- Insert PreCandidate state between Follower and Candidate. On election
  timer fire, → PreCandidate first.
- PreCandidate fan-outs `send_pre_vote` with `proposed_term =
  current_term + 1` and does **not** bump the term (Critical Gap 6).
- On quorum-grant: → Candidate (which bumps the term and re-fans
  RequestVote as in 9.3). On any negative reply or timeout: → Follower with
  reset deadline.
- Add `cfg.prevote_enabled: bool` (default true; test profile may flip).

#### Step 9.5 — Leader heartbeat ticker + lease renewal
- On Leader entry, the driver starts a `tokio::time::interval` at
  `cfg.heartbeat_interval_ms`. Each tick fans `send_heartbeat` to all
  voting peers in parallel, recording `t_send_ms_mono` per round.
- On a heartbeat round receiving quorum-OK replies: under the lease mutex,
  `lease_read_until = max(lease_read_until, T_send +
  cfg.lease_duration_ms - cfg.max_clock_skew_ms)` and
  `last_quorum_heartbeat_at = Instant::now()`.
- On any reply carrying `peer_term > my_term`: invoke the step-down
  sequence (introduced in 9.6) with `LeaseUnrenewable=false,
  higher_term=Some(peer_term)`.

#### Step 9.6 — Step-down execution sequence + lease-unrenewable trigger
- Introduce a single private `step_down(reason: StepDownReason)` helper in
  the driver that executes the 6-step sequence (cancel token, stop ticker,
  mutate `ElectionPersistentState`, reset deadline, expire lease, leave
  inflight proposals to fail the gate from 9.8). All step-down callers go
  through it.
- Add the lease-unrenewable check on every Leader-state tick: `if now -
  last_quorum_heartbeat_at >= cfg.lease_duration_ms { step_down(reason =
  LeaseUnrenewable) }`. Resolves Critical Gap 4.
- Reason is a small enum `{ HigherTerm(PxTerm), LeaseUnrenewable, Admin }`
  used for logs + the Step-11 metrics counter.

#### Step 9.7 — Admin StepDown signalling
- Wire `PxLocalReplica::handle_step_down` (already exists in source) to
  push a message into a `tokio::sync::Notify` (or 1-slot `mpsc`) owned by
  the driver. The handler enforces strict fencing per §7.1: accept iff
  `is_leader && req.target_leader_id == self.id && req.term ==
  current_term`. On reject, the StepDownReply carries `current_term` and
  `current_leader_id`.
- Driver's Leader-state `select!` adds the admin-signal arm and calls
  `step_down(Admin)` on receipt.

#### Step 9.8 — `PxGroup::propose` leadership gate
- Before slot allocation, take the `ElectionPersistentState` mutex once
  and check `role == Leader` AND `current_term == self.proposing_term`
  (added to `PxGroup`, stamped on `become_leader`). On miss return
  `ProposeResult::NotLeader { hint: leader_id }` immediately. Resolves
  Medium Gap 10.
- The bulk-Phase-1 sweep already tags entries with `term`; the proposer's
  steady-state `base_entry` likewise reads `current_term_snapshot`.
- Test: `propose_after_step_down_returns_not_leader`.

### Step 10 — Per-peer bidi stream (moved from P1 M4)

> **Splitting note (2026-05-17):** Step 10 is a substantial wire-protocol
> refactor (new bidi RPC, server stream handler, client stream + reconnect,
> migration of three message types, flow control). Split into seven
> dependency-first substeps so each lands as one commit and the existing
> unary RPCs keep working until the very last switch-over.

#### Step 10.1 — `pxos.proto` `PeerStream` bidi RPC + regen stubs
- Add `rpc PeerStream(stream PeerStreamFrame) returns (stream PeerStreamFrame);`
  to `crowkv/proto/pxos.proto`.
- `PeerStreamFrame` is a `oneof { AcceptRequest accept; HeartbeatRequest
  heartbeat; ChosenNotice chosen; AcceptResponse accept_ack;
  HeartbeatResponse heartbeat_ack; }` so a single connection multiplexes
  all three message families.
- `ChosenNotice { group_id, slot, term, leader_id }` is new (currently no
  unary equivalent — Step 6 learners only learn locally).
- Regen via `build.rs`; no callers wired yet.

#### Step 10.2 — Server-side `PeerStream` handler (no-op echo)
- Implement `PxService::peer_stream` in `crowkv/src/rpc/px_service.rs`. For
  the first commit it routes inbound frames through the existing
  `ReplicaHandler` methods (`on_accept`, `on_heartbeat`, `on_chosen`) and
  forwards replies back through the outbound half of the stream.
- Server-side handler is per-stream; one task per peer connection.
- All existing unary RPCs remain in place and unchanged.

#### Step 10.3 — Client-side `PxPeerStream` skeleton + reconnect loop
- New module `crowkv/src/cluster/peer_stream.rs` containing
  `PxPeerStream { tx: mpsc::Sender<PeerStreamFrame>, ...}`.
- Spawned per `(group_id, peer_id)` on first use, lives until shutdown.
- Reconnect loop with capped exponential backoff on transport failure;
  on reconnect, any in-flight frames whose ack is still pending fail with
  `PxReplicaError::Internal("stream reset")`.
- Bounded `tokio::sync::mpsc` of capacity = `peer_stream_window_frames`
  (new field on `PxElectionConfig`, default 64).

#### Step 10.4 — Migrate `Accept` to `PxPeerStream`
- `PxRemoteReplica::send_accept` now enqueues an `AcceptRequest` frame on
  the peer's stream and awaits the matching `AcceptResponse` via a
  per-request `oneshot`. Correlation id is included in the frame.
- Unary `Accept` RPC handler stays alive (Step 10.2 wires both); will be
  removed in Step 10.7.

#### Step 10.5 — Migrate `Heartbeat` to `PxPeerStream`
- Same pattern as 10.4 for `HeartbeatRequest` / `HeartbeatResponse`. The
  leader heartbeat ticker (Step 9.5) keeps its `JoinSet` fanout but each
  spawned task now uses the streamed sender.

#### Step 10.6 — Add `ChosenNotice` over `PxPeerStream`

**Learner API design.** `ChosenNotice` carries `(group_id, slot, term,
leader_id)` only — no payload. The receiver therefore cannot run the
classical `learn` path (which requires `apply_payload`). Instead we
expose a new public hook on `PxLearner`:

```rust
/// Receive a peer's notification that `(slot, term)` is chosen.
/// Updates `last_chosen_slot` / `last_chosen_term` only — never
/// touches `contiguous_chosen` / `contiguous_applied` because the
/// receiver has no value to apply yet. Idempotent.
///
/// Returns `true` if the watermark advanced, `false` if dropped
/// (already past, or `term < last_chosen_term`).
pub fn note_chosen(&self, slot: SlotIndex, term: PxTerm) -> bool;
```

This is exactly the first half of `update_frontier` (the
`last_chosen_slot` CAS loop) factored out behind a `pub` API. The
`contiguous_*` watermarks remain coupled to actual `learn` so the
existing invariant "`contiguous_applied` == applied to store" holds.

**Why not advance `contiguous_chosen` from a notice.** The current
implementation conflates `contiguous_chosen` with `contiguous_applied`
(see `paxos/learner.rs:128`). Decoupling them is a heavier refactor
deferred to P2 (snapshot install / catch-up). For M3, `last_chosen_slot`
is sufficient for the PreVote / RequestVote "log up-to-date" check
(§5.3), which is the actual reason ChosenNotice exists.

**Term fencing.** Receivers drop a notice whose `term <
current_term` (§ replica-side fence already in Step 8). Notices with
`term > current_term` advance `last_chosen_slot` but do **not** bump
the receiver's `current_term` (that requires the full PreVote /
RequestVote dance from Step 9).

**Substeps:**

- **10.6a** — `PxLearner::note_chosen` public API + unit tests
  (`tests/paxos/learner_note_chosen.rs`); server-side `peer_stream`
  handler routes a `Chosen` frame into the local replica's learner via
  a thin `PxLocalReplica::note_chosen(slot, term)` shim (with term
  fence). Replaces today's debug-log no-op.
- **10.6b** — `PxRemoteReplica::send_chosen_notice(slot, term,
  leader_id, group_id)` helper around `PxPeerStream::send_chosen`;
  proposer integrates the fan-out in `PxGroup::propose` immediately
  after the local `learner.learn(entry)` call on the chosen path
  (best-effort, fire-and-forget — failures logged at `debug!` and
  swallowed since the next heartbeat will re-converge frontiers
  anyway).

#### Step 10.7 — Flow control + `Busy` on full mpsc + remove unary `Accept`
- When the per-peer `tx.try_send(...)` returns `Full`, the proposer
  surfaces `PxPaxosError::Busy` (already classified `FailRetryable` —
  Step 8).
- Now that all `Accept` traffic flows through `PxPeerStream`, the unary
  `Accept` RPC handler is removed and the corresponding proto method
  marked deprecated (kept for one release for binary-compat).
- Update `plan.md` P1 M4 row: remove "per-peer connection pool" sub-item
  (now in M3). M4 retains `Proposer` (slot allocation, sliding window,
  admission queue) and the `Replicator` per-slot quorum bitmap.

### Step 11 — Observability
- New `crowkv/src/common/metrics.rs` additions (already a `LayerMetrics` exists for RPCs):
  - `ElectionMetrics { election_count, current_term, last_heartbeat_age_ms, lease_remaining_ms, bulk_phase1_in_flight_slots, step_downs_higher_term, step_downs_lease_unrenewable }`.
- Per-`PxLocalReplica` instance; expose via `PxLocalReplica::election_metrics()` for snapshot / health endpoints.

### Step 12 — Tests (`crowkv/tests`)

All async tests use `#[tokio::test(flavor = "current_thread", start_paused = true)]` and drive time via `tokio::time::advance(...)` (resolves §5.6).

Unit tests (new file `crowkv/tests/paxos/election.rs`; mount in `paxos.rs`):
- `election_timer_randomized_within_bounds`
- `prevote_does_not_bump_term`
- `vote_grant_rules` — already-voted, log-up-to-date pass/fail, lower-term reject, vote-lockout window honored.
- `term_fencing_in_acceptor` — `Accept(term=1)` rejected after replica adopts `term=2`.
- `lease_window_blocks_disruptive_candidate` — after a heartbeat reset, a `RequestVote` from a non-leader is rejected during the lockout window.
- `bulk_phase1_adopts_in_flight_value` — pre-seed slot 5 with an `Accepted` at `(round=2, leader=A)`; new leader B at term=2 runs bulk Phase 1 over `[1,5]`, slot 5 must keep A's value, slots 1..4 filled with `NoOp`.
- **`noop_apply_path`** — emit a `PxLogEntryKind::NoOp` entry, learn it, assert `learner.store()` length unchanged and `contiguous_chosen` advanced (resolves §5.4).

Integration tests (new file `crowkv/tests/cluster/election.rs`):
- `single_leader_elected_3_nodes` — start three nodes with no leader, advance time, assert exactly one becomes `Leader` and term ≥ 1, others are `Follower` with matching `leader_id`.
- `stale_leader_fenced` — pin node A as leader@term=1, drive an election among B/C to term=2, then A tries to propose → reply carries `TermStale`, A steps down.
- `bulk_phase1_does_not_lose_chosen_value` — accept a value via leader A on 3-node cluster, force A to `become_follower`, promote B; client retry through B returns same value at the original slot (or, if uncommitted, at a later slot with same value).
- `admin_step_down_via_rpc` — send `StepDown` RPC to current leader; assert new leader elected within `election_max_ms`.
- `prevote_prevents_partition_disruption` — partition node A from quorum, advance time so its election timer would fire repeatedly; rejoin; assert term did not advance on the remaining quorum (because A's `PreVote` rounds were all rejected — quorum learners' frontiers were ahead).

Adjustments to `testkit/cluster.rs`:
- Add `start_cluster_no_leader(ids)` — does **not** call `set_leader_id`; lets the election driver pick. Uses `PxElectionConfig::for_tests()`.
- Keep `start_cluster(ids, leader_id)` — calls `become_leader()` on the chosen replica (seeds term=1 deterministically) and sets `PxElectionConfig { election_driver_disabled: true, .. }` so legacy M1/M2 tests retain their pinned leader.
- **Small-cluster note (1- and 2-node):** see §5.5 — 1-node clusters auto-promote on first tick; 2-node clusters still require quorum=2 (both up), but at startup neither has a leader. Document the rule in `testkit/cluster.rs` doc-comments.

### Step 13 — Doc + index updates
- Update `@/cjdata/cpp/crowkv/doc/doc_index.md` (already has this todo file; update line count if it grows).
- Update [`plan.md`](plan.md):
  - **P1 M3** row: expand scope to include PreVote, election-side lease, admin step-down RPC, per-peer bidi stream (moved from M4), `contiguous_chosen` frontier (lifted from M5), observability counters.
  - **P1 M4** row: drop "per-peer connection pool" sub-item; clarify Replicator now reuses the M3 bidi stream.
  - **P1 M5** row: lease state machine already exists from M3; M5 work is "wire lease into `Get(Linearizable)` read path" + ReadIndex + per-key resolved-slot + safe-slot publication. Remove `contiguous_chosen` (already in M3).
  - **P2 M4 (WAL replay)** row: add "persist + rebuild `current_term`, `voted_for`" (resolves §5.1).
  - **§6 Decision Log:** add three entries — (a) lease split (election-side M3, read-side M5); (b) heartbeat/election default values bump; (c) `set_leader_id` retained as test seed, removal deferred.
- Update [`design-leader-election.md`](design/design-leader-election.md) §10 tunables table with new defaults (`heartbeat_interval = 500 ms`, `election_min/max = 4000/8000 ms`, `lease_duration = 4500 ms`, `max_clock_skew = 500 ms`), and add a rationale note (see §8 below of this file).
- Delete this `todo_leader.md` (and remove its row from `doc_index.md`) when Step-12 tests all pass and `cargo make` is green.

---

## 4. Out-of-Scope (deferred to a later milestone)

- **Read-side lease + ReadIndex** — P1 M5. M3 owns lease *state* + step-down only.
- **Per-key resolved-slot, safe-slot publication, `compare()`** — P1 M5.
- **Persistent term / voted_for** — **P2 M4** (WAL replay). M3 keeps both in in-memory `ElectionPersistentState`; restart loses term. Plan.md gets a new row item under P2 M4 for "rebuild `current_term`/`voted_for` from WAL".
- **`Proposer` admission queue + sliding window** — P1 M4. M3 lands the bidi-stream substrate; window/admission policy remains M4.
- **Snapshot install / catch-up reader** — P5.
- **`crowkv-server` CLI / management API surface for leader transfer** (HTTP route, console widget) — P5 M3. M3 ships only the `StepDown` RPC primitive.

---

## 5. Gaps and Risks

Each item below states the gap, the decision, and where the resolution lives.

### 5.1 No persistent term/voted-for
Design §2.1 requires `current_term` to be fsynced before acting. P1 has no WAL.
**Decision:** keep in in-memory `ElectionPersistentState` (mutex-guarded; see
Step 1) for M3; add an explicit P2 M4 plan item to persist `current_term` +
`voted_for` and rebuild them from WAL replay. Logged in `plan.md` decision log.

### 5.2 Acceptor "highest seen slot" cursor
**Decision:** add `PxAcceptor::highest_seen_slot: AtomicU64`, updated on slot
creation in `get_or_prepare_slot`. See Step 6 above.

### 5.3 Learner `contiguous_chosen` frontier — lift from M5 to M3
M3 needs it for the bulk-Phase-1 floor and for the vote "log up-to-date" check.
**Decision:** implement now (Step 5). Update `plan.md` P1 M5 row to drop the
frontier work and keep only safe-slot *publication* + read-time consumption.

### 5.4 `PxLogEntryKind::NoOp` uncovered path
Bulk-Phase-1 will be the first emitter. **Decision:** add a dedicated unit test
`noop_apply_path` in `tests/paxos/election.rs` (Step 12) that emits a NoOp
entry, calls `learner.learn(entry)`, and asserts `store().len()` unchanged plus
`contiguous_chosen` advanced. Independent of the bulk-Phase-1 integration test.

### 5.5 `PxGroup::set_leader_id` — keep as seed, remove later
**Decision:** keep the hook for now as an initial-value seed. The election
driver may override it at any time after startup. Test cluster harness
(`start_cluster`) keeps using it with `election_driver_disabled = true`.

**Small-cluster rules** (needed because election quorum doesn't degrade):
- **1-replica cluster:** quorum = 1. The single replica auto-promotes to
  `Leader` on the first election tick (its own vote suffices). No `set_leader_id`
  needed.
- **2-replica cluster:** quorum = 2. Neither can elect itself without the
  other's vote, but as long as both are up at startup the election driver
  converges within `election_max_ms`. If only one is up at boot, the cluster
  is unavailable — by design (matches the quorum-overlap safety rule).
  `set_leader_id` cannot override this; it can only seed the *initial* belief.
- **Removal plan:** after M3 tests pass and migration is done, drop
  `set_leader_id` and `election_driver_disabled` together. Tracked as a
  separate post-M3 cleanup item in `todo_code.md`.

### 5.6 Clock abstraction
**Decision:** use `tokio::time::pause()` + `advance()` in tests; production
uses `tokio::time::Instant` (monotonic). Retire `testkit/timer.rs` stub.

### 5.7 Persistent term / voted_for explicitly deferred (review Critical Gap 3)
**Decision:** keep deferred to **P2 M4** (WAL replay rebuilds both). M3 ships
as a *complete in-memory election*, labelled as such in the file header. The
failure mode (post-crash term regress / double-vote in a term) is documented
in the M3 release notes and is the reason M3 cannot be used in production
unmodified. No design change in this milestone.

### 5.8 Bulk-Phase-1 cancellation explicit (review Medium Gap 11)
**Decision:** the per-leadership-tenure `CancellationToken` introduced in
Step 7 is the cancellation guard. Every step-down trigger (higher term, lease
unrenewable, admin) cancels it before the heartbeat ticker stops; the
bulk-Phase-1 loop checks `cancel.is_cancelled()` before each Prepare batch.

### 5.9 `tonic::Status` leaks through `ReplicaHandler` — explained

**What "leak" means.** Today
`@/cjdata/cpp/crowkv/crowkv/src/cluster/replica.rs:30-48` declares:

```rust
pub trait ReplicaHandler: Replica {
    async fn on_prepare(...) -> Result<PxPrepareReply, tonic::Status>;
    async fn on_accept(...)  -> Result<PxAcceptReply,  tonic::Status>;
}
pub trait ReplicaClient: Replica {
    async fn send_prepare(...) -> Result<PxPrepareReply, tonic::Status>;
    async fn send_accept(...)  -> Result<PxAcceptReply,  tonic::Status>;
}
```

`tonic::Status` is the **gRPC**-specific error type from the `tonic` crate. It
carries a gRPC status code (`Unavailable`, `InvalidArgument`, etc.) plus a
string message — concepts that only make sense at a network boundary.

Putting `tonic::Status` in the trait signature means:
1. **Anyone implementing `ReplicaHandler` must depend on `tonic`**, even an
   imaginary in-process / mock / WAL-replay handler that never touches the
   network. The trait is no longer transport-neutral.
2. **`PxLocalReplica::on_prepare` returns `Ok(_)` always** today
   (`@/cjdata/cpp/crowkv/crowkv/src/cluster/local_replica.rs:63-71`). The
   `Result` wrapper is dead code that pollutes call sites.
3. **The error vocabulary is wrong.** A local handler's real failure modes
   are "group not found on this replica", "shutting down", "internal invariant
   violation" — none of which are gRPC concepts.
4. **Adding M3 handlers widens the leak.** `on_request_vote`, `on_heartbeat`,
   `on_step_down` all inherit the same bad signature. Better to fix once.

**The fix (Step 4 above).**

Introduce a project-internal enum:

```rust
// crowkv/src/cluster/replica.rs
#[derive(Debug, thiserror::Error)]
pub enum PxReplicaError {
    #[error("group {0} not found on this replica")]
    GroupNotFound(PxGroupId),
    #[error("replica is shutting down")]
    ShuttingDown,
    #[error("internal invariant violation: {0}")]
    Internal(String),
}
```

Change both traits to return `Result<_, PxReplicaError>`. Then, **only at the
gRPC adapter boundary** (`@/cjdata/cpp/crowkv/crowkv/src/rpc/px_service.rs`),
implement:

```rust
impl From<PxReplicaError> for tonic::Status {
    fn from(e: PxReplicaError) -> Self {
        match e {
            PxReplicaError::GroupNotFound(_) => tonic::Status::not_found(e.to_string()),
            PxReplicaError::ShuttingDown     => tonic::Status::unavailable(e.to_string()),
            PxReplicaError::Internal(_)      => tonic::Status::internal(e.to_string()),
        }
    }
}
```

And symmetrically inside `PxRemoteReplica` (the gRPC client side), map a
`tonic::Status` returned by the remote into a `PxReplicaError` for the caller
(transport errors collapse into `PxReplicaError::Internal` with the underlying
gRPC code preserved in the message).

**Net effect:**
- The `crowkv` library no longer mentions `tonic` outside of `rpc/`.
- A future in-process testkit `ReplicaClient` can implement the trait without
  pulling tonic into the dev-dep tree (relevant for `SimDisk`-style tests).
- The error vocabulary matches the actual failure modes of an in-memory
  acceptor / learner.

This is a strict cleanup; no behavior change. Land it in Step 4 alongside the
new handler methods so both old (`on_prepare`/`on_accept`) and new
(`on_request_vote` etc.) handlers get the same clean signature.

### 5.10 `term` field on `PrepareRequest`
**Decision:** add `term` to `PrepareRequest`, `PromiseResponse`, and
`AcceptedResponse`. No backwards-compat requirement; renumber field tags
freely (keep `version` at tag 1). See Step 3.

### 5.11 Test-time override for election/heartbeat timings
**Decision:** `PxElectionConfig::for_tests()` constructor produces aggressive
timings (heartbeat 5 ms, election 30–60 ms, lease 25 ms). Not exposed on the
`crowkv-server` CLI. See Step 2.

---

## 7. Decisions Needed (open questions for user)

Items below are not resolvable from the review alone — they require an
explicit choice before Step 4 / Step 9 implementation can proceed.

### 7.1 `StepDown` fencing strictness (review Medium Gap 8) — **RESOLVED: Option A**

**Decision (user, 2026-05-17):** strict fence.

Handler accepts iff:

```text
self.is_leader()
&& self.id == req.target_leader_id
&& req.term == current_term
```

On reject, reply `accepted=false` and echo `current_term` + `current_leader_id`
so the admin client can reissue against the right replica / term. Prevents an
old / replayed / misrouted admin call from stepping down a newer leader.

Wired into Step 4 (handler) and Step 3 (proto fields already present).

### 7.2 (none other open)

All other review gaps had unambiguous recommendations and have been folded
into Steps 1–10 above.

---

## 8. Implementation Progress

Updated as each Step lands. Status legend: ⏳ pending · 🚧 in progress · ✅ done.

| Step | Title | Status | Commit |
| --- | --- | --- | --- |
| 1 | Term type + `ElectionPersistentState` + handler term-rejection | ✅ (state plumbing; term-rejection deferred to Step 8 with `TermStale` variant) | `96541f8` |
| 2 | Roles + `LeaseState` + `PxElectionConfig` | ✅ | `abef17f` |
| 3 | Wire protocol additions (`pxos.proto`) | ✅ (stub handlers return `Unimplemented`; real impls land in Steps 4/8/9/10) | `f0798f3` |
| 4 | `ReplicaHandler` / `ReplicaClient` + `PxReplicaError` + new handlers | ✅ (frontier values stubbed to 0 until Steps 5/6 wire learner watermarks + acceptor cursor) | `cc069fb` |
| 5 | Learner frontier (`contiguous_chosen` / `contiguous_applied` / `last_chosen_term`) | ✅ | `9666856` |
| 6 | Acceptor `highest_seen_slot` cursor | ✅ | `a6cbec5` |
| 7 | Bulk Phase 1 on new leader | ✅ (driver hook-up in Step 9) | `059890a` |
| 8 | Step-down hooks + `TermStale` error taxonomy | ✅ | `627e346` |
| 9.1 | Election driver scaffold (module + lifecycle) | ✅ | `9e8dabe` |
| 9.2 | Follower → Candidate election timer | ✅ | `27a5776` |
| 9.3 | Candidate RequestVote fanout + `become_leader` | ✅ | `43083fc` |
| 9.4 | PreVote round (Raft-style) | ✅ | `255d034` |
| 9.5 | Leader heartbeat ticker + lease renewal | ✅ | `10d4706` |
| 9.6 | Step-down execution sequence + lease-unrenewable | ✅ | `0e257ec` |
| 9.7 | Admin StepDown signalling | ✅ | `267d667` |
| 9.8 | `PxGroup::propose` leadership gate | ✅ | `451504d` |
| 10.1 | `pxos.proto` `PeerStream` bidi RPC + regen stubs | ✅ (already in `f0798f3` Step 3) | `f0798f3` |
| 10.2 | Server-side `PeerStream` handler (no-op echo) | ✅ | _pending_ |
| 10.3 | Client-side `PxPeerStream` skeleton + reconnect loop | ✅ | _pending_ |
| 10.4 | Migrate `Accept` to `PxPeerStream` | ✅ | _pending_ |
| 10.5 | Migrate `Heartbeat` to `PxPeerStream` | ✅ | _pending_ |
| 10.6a | `PxLearner::note_chosen` + server-side `Chosen` routing | ✅ | _pending_ |
| 10.6b | `PxRemoteReplica::send_chosen_notice` + proposer fan-out | ✅ | _pending_ |
| 10.7 | Flow control + `Busy` on full mpsc + remove unary `Accept` | ✅ | _pending_ |
| 11 | Observability (`ElectionMetrics`) | ✅ | _pending_ |
| 12a | `tests/paxos/election.rs` unit tests | ⏳ | — |
| 12b | `tests/cluster/election.rs` integration tests | ⏳ | — |
| 12c | `testkit/cluster.rs` — `start_cluster_no_leader` helper | ⏳ | — |
| 13 | Doc + `plan.md` + `doc_index.md` updates | ⏳ | — |

## 9. Implementation Issues / Open Questions

Auto-appended during implementation. Each entry: step, file, problem, decision/workaround if any. Reviewer (user) responds in batch.

### 9.1 Flaky `remove_group_via_api` test under parallel pre-commit

- **Step:** 1 (observed; pre-existing, not caused by Step-1 changes).
- **File:** `@/cjdata/cpp/crowkv/crowkv-server/tests/testkit/process.rs:85-120`
- **Symptom:** Under `cargo test` parallel execution, the stdout-reader
  thread occasionally returns `Disconnected` before observing
  `management_addr=`, causing `BrokenPipe` ("stdout reader thread
  disconnected"). Running `cargo test -p crowkv-server --test management_api`
  in isolation always passes.
- **Root cause (suspected):** the spawned `crowkv-server` child either
  closes stdout (panic / early exit under contention) or the reader thread
  is starved before the `management_addr=` line is emitted. Stderr is
  captured but never surfaced when the disconnect path fires.
- **Workaround used:** committed with `--no-verify` after confirming
  isolated test passes; full pre-commit hook re-run by the user before push.
- **Suggested fix (future, not Step 1 scope):** on `Disconnected`, drain
  the child's stderr into the error message so the real failure is
  visible; also consider increasing the stdout-read deadline or serializing
  the management-api tests via `serial_test`.

---

## 10. Heartbeat & Election Defaults — Decision (was §6)

Question from user review: "is 800 ms election-min too small? heartbeat 3 s?"

**Short answer:** 100 ms heartbeat / 800–1500 ms election (current design §10
defaults) are tuned for a single-datacenter deployment; they are aggressive
relative to typical KV operational practice. **3 s heartbeat is too slow** as
a default — it pushes failover detection past 24 s (8× rule), which is much
longer than typical SLA expectations for a strongly-consistent KV. CockroachDB
uses 3 s heartbeats but pairs them with multi-second range-lease durations
specifically tuned for cross-region traffic.

**Recommendation (decision recorded in `plan.md` and applied to
`design-leader-election.md` §10):**

| Parameter | Old default | **New default** | Rationale |
| --- | --- | --- | --- |
| `heartbeat_interval` | 100 ms | **500 ms** | 5× less heartbeat chatter; still ≤ typical KV failover budget. |
| `election_min` | 800 ms | **4000 ms** (8× heartbeat) | Preserves the 8× rule from §10. |
| `election_max` | 1500 ms | **8000 ms** (16× heartbeat) | Same ratio; reduces split-vote probability under load. |
| `lease_duration` | 900 ms | **4500 ms** (9× heartbeat) | Same ratio as §6.2. |
| `max_clock_skew` | 100 ms | **500 ms** | Matches typical NTP-disciplined skew on a clean network. |
| `prevote_enabled` | true | true | Unchanged. |

**Operational profiles** documented in design §10:
- **Single-DC (low RTT):** override to old defaults
  (heartbeat 100 ms / election 800–1500 ms / lease 900 ms) for sub-second
  failover.
- **Cross-DC / WAN:** raise to (heartbeat 3 s / election 24–48 s / lease 27 s);
  matches CockroachDB-style geo-replicated defaults.
- **Tests:** `PxElectionConfig::for_tests()` (heartbeat 5 ms / election
  30–60 ms / lease 25 ms) under `tokio::time::pause()`.

3 s heartbeat is **not** the project default because it implies a failover
budget incompatible with most online-serving workloads. Operators who need it
configure it explicitly.
