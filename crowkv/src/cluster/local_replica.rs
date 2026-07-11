//! `PxLocalReplica` — local acceptor/learner for a single `CrowKV` group member.
//!
//! Wraps the consensus state (`PxAcceptor`) and learner (`PxLearner`).
//! All proposer/KV logic has been moved to `PxGroup` and `KvStore`.

use crate::cluster::health::HealthReport;
use crate::cluster::replica::{
    HeartbeatReply, HeartbeatRequestPayload, PxReplicaError, Replica, ReplicaHandler, StepDownReply, StepDownRequestPayload, VoteReply, VoteRequestPayload,
};
use crate::cluster::shutdown::ShutdownReport;
use crate::cluster::snapshot::{KvStoreSnapshot, LocalReplicaSnapshot};
use crate::paxos::acceptor::PxAcceptor;
use crate::paxos::learner::PxLearner;
use crate::paxos::roles::{Acceptor, Learner, PxAcceptReply, PxBallot, PxLogEntry, PxPrepareReply, SlotIndex};
use crate::paxos::{PxNodeId, PxTerm};
use parking_lot::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::time::{Duration, Instant};
use tracing::{debug, info};

/// Role of a local replica in the leader-election state machine.
///
/// Transitions are driven by the election task (`cluster::election`) and the
/// `Heartbeat` / `RequestVote` / `StepDown` handlers via the
/// `become_follower` / `become_precandidate` / `become_candidate` /
/// `become_leader` methods on [`PxLocalReplica`]. The role is mirrored in an
/// `AtomicU8` for lock-free reads; the mutex inside
/// [`ElectionPersistentState`] is the source of truth.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum PxLocalReplicaRole {
    /// Only serves Acceptor handlers. Awaits heartbeats; may time out and
    /// transition to `PreCandidate`.
    Follower = 0,
    /// Probing peers with `PreVote` to check whether a real election could
    /// succeed without disrupting the cluster. Does not bump term.
    PreCandidate = 1,
    /// Actively soliciting `RequestVote` grants under a bumped term.
    Candidate = 2,
    /// Drives Prepare / Accept rounds, sends heartbeats, holds the
    /// election-side lease.
    Leader = 3,
}

impl PxLocalReplicaRole {
    #[must_use]
    fn as_u8(self) -> u8 {
        self as u8
    }

    fn from_u8(v: u8) -> Self {
        match v {
            0 => Self::Follower,
            1 => Self::PreCandidate,
            2 => Self::Candidate,
            3 => Self::Leader,
            // Atomic mirror is only written under the mutex with a valid
            // enum value; any other byte indicates memory corruption.
            _ => unreachable!("invalid PxLocalReplicaRole discriminant: {v}"),
        }
    }
}

/// Mutex-guarded election metadata.
///
/// All term-changing decisions (`RequestVote` / `Heartbeat` /
/// Accept-with-higher-term / `become_*`) take this lock for the full
/// read–decide–write cycle so `(current_term, voted_for, role)` are never
/// observed mixed.
///
/// Atomic mirrors (`current_term_atomic`, `role_atomic`) on
/// [`PxLocalReplica`] provide a lock-free snapshot for the proposer's
/// `base_entry` hot path and for observability counters; the mutex is the
/// source of truth.
#[derive(Debug, Clone)]
pub struct ElectionPersistentState {
    /// Latest term this replica has observed. Monotonic.
    pub current_term: PxTerm,
    /// Candidate this replica granted a `RequestVote` to in `current_term`,
    /// if any. Reset to `None` on every term bump.
    pub voted_for: Option<PxNodeId>,
    /// Current role in the election state machine.
    pub role: PxLocalReplicaRole,
    /// This replica's belief of the current leader id, if any. Set by
    /// `on_heartbeat`, by an explicit `become_leader` on this node, or by a
    /// successful `RequestVote` grant.
    pub leader_id: Option<PxNodeId>,
    /// Election-side lease: refuse to grant `RequestVote` (real, not `PreVote`)
    /// for any other candidate before this deadline. Bumped on every heartbeat
    /// from the current leader and on every successful `RequestVote` grant.
    pub vote_lockout_until: Instant,
}

impl ElectionPersistentState {
    fn initial(role: PxLocalReplicaRole) -> Self {
        let leader_id = if role == PxLocalReplicaRole::Leader { Some(0) } else { None };
        Self {
            current_term: 0,
            voted_for: None,
            role,
            leader_id,
            // Initialized as already-expired so a brand-new replica with no
            // heartbeat history can still grant the first RequestVote it sees.
            vote_lockout_until: Instant::now(),
        }
    }
}

/// Leader-side lease bookkeeping, mutex-guarded.
///
/// Both timestamps are monotonic (`Instant`). `lease_read_until` is the read
/// fast-path deadline (consumed by `Get(Linearizable)` in M5 — maintained but
/// not yet read in M3). `last_quorum_heartbeat_at` is the leadership-liveness
/// timestamp; if `now - last_quorum_heartbeat_at >= lease_duration` the leader
/// must step down (`LeaseUnrenewable`, Step 9).
///
/// On a follower these values are stale; the election driver only reads them
/// while the replica's role is `Leader`.
#[derive(Debug, Clone)]
pub struct LeaseState {
    /// Max acknowledged `T_send + lease_duration - max_clock_skew` across all
    /// heartbeat rounds that received a quorum response. Only ever extends.
    pub lease_read_until: Instant,
    /// Wall-clock-monotonic instant at which the most recent heartbeat round
    /// received a quorum of OK responses. Drives the lease-unrenewable
    /// step-down rule.
    pub last_quorum_heartbeat_at: Instant,
}

impl LeaseState {
    fn expired() -> Self {
        let now = Instant::now();
        Self {
            lease_read_until: now,
            last_quorum_heartbeat_at: now,
        }
    }
}

/// Runtime container for one local group member.
///
/// Pure acceptor/learner. Proposer logic lives in `PxGroup`.
///
/// Key work: term/voted-for state (Step 1), role state machine (Step 2),
/// election-side lease (Step 2), election + heartbeat handlers (Step 4).
pub struct PxLocalReplica {
    pub id: u64,
    pub acceptor: PxAcceptor,
    pub learner: PxLearner,
    pub voting: bool,
    /// Election metadata (source of truth).
    election_state: Mutex<ElectionPersistentState>,
    /// Lock-free mirror of `election_state.current_term`. Updated under the
    /// mutex via `Release`; readers use `Acquire`. Used by the proposer hot
    /// path (`base_entry`) and by metrics snapshots.
    current_term_atomic: AtomicU64,
    /// Lock-free mirror of `election_state.role`. Updated under the mutex via
    /// `Release`; readers use `Acquire`. Cheap hot-path check (e.g. proposer's
    /// leadership gate, `is_leader`).
    role_atomic: AtomicU8,
    /// Leader-side lease bookkeeping. Stale on non-leader replicas.
    lease_state: Mutex<LeaseState>,
    /// Notify signalled by accepted admin `StepDown` RPCs (Step 9.7). The
    /// election driver's leader-state `select!` waits on this so the
    /// canonical step-down sequence runs immediately rather than on the
    /// next heartbeat tick. Cheap: a single `Notify` and a sticky boolean.
    pub(crate) admin_step_down_signal: tokio::sync::Notify,
    /// Idempotency gate for [`Self::shutdown`].
    shutdown_started: AtomicBool,
}

impl std::fmt::Debug for PxLocalReplica {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PxLocalReplica")
            .field("id", &self.id)
            .field("role", &self.role())
            .field("voting", &self.voting)
            .field("current_term", &self.current_term_snapshot())
            .finish_non_exhaustive()
    }
}

impl Replica for PxLocalReplica {
    fn id(&self) -> u64 {
        self.id
    }

    fn endpoint(&self) -> Option<&str> {
        None
    }

    fn voting(&self) -> bool {
        self.voting
    }
}

impl ReplicaHandler for PxLocalReplica {
    async fn on_prepare(&self, slot: u64, ballot: PxBallot, term: PxTerm, _group_id: u64) -> Result<PxPrepareReply, PxReplicaError> {
        Ok(self.on_prepare(slot, ballot, term).await)
    }

    async fn on_accept(&self, entry: PxLogEntry, _group_id: u64) -> Result<PxAcceptReply, PxReplicaError> {
        Ok(self.on_accept(entry).await)
    }

    async fn on_pre_vote(&self, req: VoteRequestPayload, _group_id: u64) -> Result<VoteReply, PxReplicaError> {
        Ok(self.handle_pre_vote(req))
    }

    async fn on_request_vote(&self, req: VoteRequestPayload, _group_id: u64) -> Result<VoteReply, PxReplicaError> {
        Ok(self.handle_request_vote(req))
    }

    async fn on_heartbeat(&self, req: HeartbeatRequestPayload, _group_id: u64) -> Result<HeartbeatReply, PxReplicaError> {
        Ok(self.handle_heartbeat(req))
    }

    async fn on_step_down(&self, req: StepDownRequestPayload, _group_id: u64) -> Result<StepDownReply, PxReplicaError> {
        Ok(self.handle_step_down(&req))
    }
}

impl PxLocalReplica {
    #[must_use]
    pub fn new(id: u64, role: PxLocalReplicaRole) -> Self {
        Self {
            id,
            acceptor: PxAcceptor::new(),
            learner: PxLearner::new(),
            voting: true,
            election_state: Mutex::new(ElectionPersistentState::initial(role)),
            current_term_atomic: AtomicU64::new(0),
            role_atomic: AtomicU8::new(role.as_u8()),
            lease_state: Mutex::new(LeaseState::expired()),
            admin_step_down_signal: tokio::sync::Notify::new(),
            shutdown_started: AtomicBool::new(false),
        }
    }

    /// Lock-free snapshot of the current role.
    ///
    /// Suitable for hot-path checks such as the proposer's leadership gate.
    #[must_use]
    pub fn role(&self) -> PxLocalReplicaRole {
        PxLocalReplicaRole::from_u8(self.role_atomic.load(Ordering::Acquire))
    }

    /// Lock-free snapshot of the current term.
    ///
    /// Suitable for the proposer's `base_entry` hot path — the proposer
    /// already owns its leadership tenure, so the snapshot cannot regress
    /// underneath it. For decisions that mutate state (vote grant, term adopt)
    /// take the mutex via [`Self::with_election_state`] instead.
    #[must_use]
    pub fn current_term_snapshot(&self) -> PxTerm {
        self.current_term_atomic.load(Ordering::Acquire)
    }

    /// Locked read of the current term (matches the source-of-truth mutex).
    #[must_use]
    pub fn current_term(&self) -> PxTerm {
        self.election_state.lock().current_term
    }

    /// Locked read of `voted_for`.
    #[must_use]
    pub fn voted_for(&self) -> Option<PxNodeId> {
        self.election_state.lock().voted_for
    }

    /// Locked read of `vote_lockout_until`.
    #[must_use]
    pub fn vote_lockout_until(&self) -> Instant {
        self.election_state.lock().vote_lockout_until
    }

    /// Take a read–decide–write critical section over [`ElectionPersistentState`].
    ///
    /// The closure receives a `&mut` borrow of the mutex contents. The atomic
    /// mirror is refreshed automatically when the closure returns.
    pub fn with_election_state<R>(&self, f: impl FnOnce(&mut ElectionPersistentState) -> R) -> R {
        let mut guard = self.election_state.lock();
        let out = f(&mut guard);
        // Mirror after the closure runs so observers never see a term that the
        // mutex itself hasn't committed to yet.
        self.current_term_atomic.store(guard.current_term, Ordering::Release);
        out
    }

    /// Snapshot of the entire election state (cheap clone of small POD).
    #[must_use]
    pub fn election_state_snapshot(&self) -> ElectionPersistentState {
        self.election_state.lock().clone()
    }

    /// Cascade shutdown into acceptor + learner.
    ///
    /// Acceptor and learner currently rely on `Drop` for resource release
    /// (slot list reclaim, in-memory KV map drop). The explicit cascade is a
    /// hook for future persistence layers (P3) which will need to flush.
    #[tracing::instrument(level = "debug", skip_all, fields(replica_l_id = self.id))]
    #[allow(clippy::unused_async)] // async kept for cascade uniformity (P3 will await flush)
    pub async fn shutdown(&self, _per_layer_timeout: Duration) -> ShutdownReport {
        if self.shutdown_started.swap(true, Ordering::AcqRel) {
            debug!(replica_l_id = self.id, "PxLocalReplica::shutdown is a no-op (already shut down)");
            return ShutdownReport::new();
        }
        info!(replica_l_id = self.id, "PxLocalReplica shutdown (acceptor/learner cleanup deferred to Drop)");
        ShutdownReport::new()
    }

    #[must_use]
    pub fn with_voting(mut self, voting: bool) -> Self {
        self.voting = voting;
        self
    }

    /// Convenience: is this replica the leader?
    #[must_use]
    pub fn is_leader(&self) -> bool {
        self.role() == PxLocalReplicaRole::Leader
    }

    /// Locked snapshot of [`LeaseState`].
    #[must_use]
    pub fn lease_state_snapshot(&self) -> LeaseState {
        self.lease_state.lock().clone()
    }

    /// Update [`LeaseState`] under its mutex.
    pub fn with_lease_state<R>(&self, f: impl FnOnce(&mut LeaseState) -> R) -> R {
        let mut guard = self.lease_state.lock();
        f(&mut guard)
    }

    /// Transition to `Follower` and adopt `new_term` if higher.
    ///
    /// Resets `voted_for` on every term bump (Raft-style — one vote per
    /// term per peer). Expires leader lease state. Callers that want to
    /// reset the election deadline must do so on the election driver task.
    #[tracing::instrument(level = "info", skip_all, fields(replica_l_id = self.id, new_term = new_term))]
    pub fn become_follower(&self, new_term: PxTerm) {
        self.with_election_state(|s| {
            if new_term > s.current_term {
                s.current_term = new_term;
                s.voted_for = None;
            }
            s.role = PxLocalReplicaRole::Follower;
        });
        self.role_atomic.store(PxLocalReplicaRole::Follower.as_u8(), Ordering::Release);
        // Lease is no longer meaningful as a non-leader. Expire it so any
        // stale read fast-path attempt rejects.
        *self.lease_state.lock() = LeaseState::expired();
        info!(replica_l_id = self.id, current_term = new_term, "become_follower");
    }

    /// Transition to `PreCandidate`. Does not bump term (per Raft `PreVote`).
    #[tracing::instrument(level = "info", skip_all, fields(replica_l_id = self.id))]
    pub fn become_precandidate(&self) {
        self.with_election_state(|s| {
            s.role = PxLocalReplicaRole::PreCandidate;
        });
        self.role_atomic.store(PxLocalReplicaRole::PreCandidate.as_u8(), Ordering::Release);
        info!(replica_l_id = self.id, current_term = self.current_term_snapshot(), "become_precandidate");
    }

    /// Transition to `Candidate`. Bumps term to `new_term`, votes for self.
    ///
    /// Caller is responsible for fanning out `RequestVote` to peers.
    #[tracing::instrument(level = "info", skip_all, fields(replica_l_id = self.id, new_term = new_term))]
    pub fn become_candidate(&self, new_term: PxTerm) {
        self.with_election_state(|s| {
            s.current_term = new_term;
            s.voted_for = Some(self.id);
            s.role = PxLocalReplicaRole::Candidate;
        });
        self.role_atomic.store(PxLocalReplicaRole::Candidate.as_u8(), Ordering::Release);
        info!(replica_l_id = self.id, current_term = new_term, "become_candidate");
    }

    /// Transition to `Leader`. Initializes lease state as already-expired so
    /// the first heartbeat round must extend it before the read fast-path
    /// becomes available.
    #[tracing::instrument(level = "info", skip_all, fields(replica_l_id = self.id))]
    pub fn become_leader(&self) {
        self.with_election_state(|s| {
            s.role = PxLocalReplicaRole::Leader;
        });
        self.role_atomic.store(PxLocalReplicaRole::Leader.as_u8(), Ordering::Release);
        *self.lease_state.lock() = LeaseState::expired();
        info!(replica_l_id = self.id, current_term = self.current_term_snapshot(), "become_leader");
    }

    /// Point-in-time snapshot for the topology endpoint. Exposes only cheap
    /// (`O(1)`) data: the kv-store key count via `DashMap::len()`.
    #[allow(clippy::cast_possible_truncation)]
    #[must_use]
    pub fn snapshot(&self) -> LocalReplicaSnapshot {
        let role = match self.role() {
            PxLocalReplicaRole::Leader => "leader",
            PxLocalReplicaRole::Follower => "follower",
            PxLocalReplicaRole::PreCandidate => "pre_candidate",
            PxLocalReplicaRole::Candidate => "candidate",
        };
        LocalReplicaSnapshot {
            id: self.id,
            role,
            voting: self.voting,
            kv_store: KvStoreSnapshot {
                key_count: self.learner.store().len() as u64,
            },
        }
    }

    /// Cached health for this local replica.
    ///
    /// V1 reports `Unhealthy` if `shutdown()` has run (resources released);
    /// otherwise `Ok`. P3 will extend this to surface acceptor/learner
    /// persistence I/O errors.
    #[must_use]
    pub fn health(&self) -> HealthReport {
        if self.shutdown_started.load(Ordering::Acquire) {
            HealthReport::unhealthy(format!("local replica {} has been shut down", self.id))
        } else {
            HealthReport::ok()
        }
    }

    /// Phase-1 `Prepare` handler with election-term fence.
    ///
    /// Two-fence rule (`doc/todo_leader.md` Step 8):
    /// - `req.term < current_term` → `PxPrepareReply::TermStale { new_term }`.
    /// - `req.term > current_term` → adopt via [`Self::become_follower`], then
    ///   forward to the acceptor (this replica is now in the new term).
    /// - `req.term == current_term` → forward to the acceptor unchanged.
    pub async fn on_prepare(&self, slot: u64, ballot: PxBallot, term: PxTerm) -> PxPrepareReply {
        let local_term = self.current_term_snapshot();
        if term < local_term {
            return PxPrepareReply::TermStale { slot, new_term: local_term };
        }
        if term > local_term {
            self.become_follower(term);
        }
        self.acceptor.prepare(slot, ballot).await
    }

    /// Phase-2 `Accept` handler with election-term fence.
    ///
    /// Same two-fence rule as [`Self::on_prepare`] but the term lives on
    /// `entry.term` (because the accept message carries the value).
    pub async fn on_accept(&self, entry: PxLogEntry) -> PxAcceptReply {
        let req_term = entry.term;
        let local_term = self.current_term_snapshot();
        if req_term < local_term {
            return PxAcceptReply::TermStale { slot: entry.slot, new_term: local_term };
        }
        if req_term > local_term {
            self.become_follower(req_term);
        }
        self.acceptor.accept(entry).await
    }

    /// Learn a chosen entry (apply to state machine).
    pub fn learn(&self, entry: &PxLogEntry) {
        self.learner.learn(entry.clone());
    }

    /// Read the currently accepted value at a slot (for verification).
    #[allow(clippy::unused_async)]
    pub async fn accepted_at(&self, slot: u64) -> Option<PxLogEntry> {
        self.acceptor.accepted_at(slot)
    }

    /// Read the currently promised ballot at a slot (for verification).
    #[allow(clippy::unused_async)]
    pub async fn promised_at(&self, slot: u64) -> Option<PxBallot> {
        self.acceptor.promised_at(slot)
    }

    // ---------------- Learner / acceptor frontier accessors ----------------
    //
    // Step 5 wires the learner watermarks. Step 6 adds the acceptor cursor.

    /// Highest slot ever seen as chosen by the local learner (gaps allowed).
    #[must_use]
    pub fn last_chosen_slot(&self) -> SlotIndex {
        self.learner.last_chosen_slot()
    }

    /// Term of the entry at [`Self::last_chosen_slot`].
    #[must_use]
    pub fn last_chosen_term(&self) -> PxTerm {
        self.learner.last_chosen_term()
    }

    /// Highest contiguous-chosen slot.
    #[must_use]
    pub fn contiguous_chosen(&self) -> SlotIndex {
        self.learner.contiguous_chosen()
    }

    /// Highest contiguous-applied slot.
    #[must_use]
    pub fn contiguous_applied(&self) -> SlotIndex {
        self.learner.contiguous_applied()
    }

    /// Highest slot ever opened on this replica's acceptor (Step 6 cursor).
    #[must_use]
    pub fn highest_seen_slot(&self) -> SlotIndex {
        self.acceptor.highest_seen_slot()
    }

    // ---------------- Election handler internals ----------------

    /// Compute the responder's frontier triple (used by `PreVote` /
    /// `RequestVote` / `Heartbeat` replies).
    fn frontier_triple(&self) -> (SlotIndex, PxTerm, SlotIndex) {
        (self.contiguous_chosen(), self.last_chosen_term(), self.highest_seen_slot())
    }

    /// Candidate's log is at least as up-to-date as ours iff
    /// `(last_chosen_term, last_chosen_slot)` is lexicographically `>=` ours.
    fn candidate_log_up_to_date(&self, req: &VoteRequestPayload) -> bool {
        let my_term = self.last_chosen_term();
        let my_slot = self.last_chosen_slot();
        (req.last_chosen_term, req.last_chosen_slot) >= (my_term, my_slot)
    }

    /// `PreVote` decision (no state mutation). Reply iff:
    /// - `req.term > current_term` (we'd vote in that term), AND
    /// - candidate's log is up-to-date, AND
    /// - `now >= vote_lockout_until`.
    fn handle_pre_vote(&self, req: VoteRequestPayload) -> VoteReply {
        let now = Instant::now();
        let state = self.election_state.lock();
        let granted = req.term > state.current_term && self.candidate_log_up_to_date(&req) && now >= state.vote_lockout_until;
        let term = state.current_term;
        drop(state);
        let (contiguous_chosen, last_chosen_term, highest_seen_slot) = self.frontier_triple();
        debug!(
            replica_l_id = self.id,
            candidate_id = req.candidate_id,
            proposed_term = req.term,
            current_term = term,
            granted,
            "on_pre_vote"
        );
        VoteReply {
            term,
            granted,
            contiguous_chosen,
            last_chosen_term,
            highest_seen_slot,
        }
    }

    /// `RequestVote` decision (state-mutating). Same checks as `handle_pre_vote`
    /// plus a `voted_for ∈ {None, candidate_id}` check in `req.term`. On grant
    /// the responder adopts `req.term`, sets `voted_for = candidate_id`, and
    /// extends `vote_lockout_until`.
    fn handle_request_vote(&self, req: VoteRequestPayload) -> VoteReply {
        // We need the lease duration to extend vote_lockout_until. The driver
        // (Step 9) owns the config; for now use the default lease.
        let lease = Duration::from_millis(crate::common::config::PxElectionConfig::DEFAULT.lease_duration_ms);
        let now = Instant::now();
        let log_up_to_date = self.candidate_log_up_to_date(&req);

        let (granted, term, current_leader_id) = {
            let mut state = self.election_state.lock();
            let lockout_ok = now >= state.vote_lockout_until;
            let term_ok = req.term > state.current_term
                || (req.term == state.current_term && state.voted_for.map_or(true, |v| v == req.candidate_id));
            let mut granted = false;
            if term_ok && log_up_to_date && lockout_ok {
                if req.term > state.current_term {
                    state.current_term = req.term;
                    state.voted_for = None;
                    state.role = PxLocalReplicaRole::Follower;
                    state.leader_id = None;
                }
                state.voted_for = Some(req.candidate_id);
                state.vote_lockout_until = now + lease;
                granted = true;
            }
            (granted, state.current_term, state.leader_id)
        };
        // Mirror the atomic snapshots if we mutated.
        if granted {
            self.current_term_atomic.store(term, Ordering::Release);
            self.role_atomic.store(PxLocalReplicaRole::Follower.as_u8(), Ordering::Release);
        }
        let (contiguous_chosen, last_chosen_term, highest_seen_slot) = self.frontier_triple();
        let _ = current_leader_id; // not echoed in VoteReply
        info!(
            replica_l_id = self.id,
            candidate_id = req.candidate_id,
            req_term = req.term,
            current_term = term,
            granted,
            "on_request_vote"
        );
        VoteReply {
            term,
            granted,
            contiguous_chosen,
            last_chosen_term,
            highest_seen_slot,
        }
    }

    /// `Heartbeat` handler. Adopts a higher term, records the leader id, bumps
    /// the vote-lockout window. Returns `success=true` iff the term is `>=`
    /// our own; on lower term we reply `false` so the stale leader can step
    /// down on its next bookkeeping check.
    fn handle_heartbeat(&self, req: HeartbeatRequestPayload) -> HeartbeatReply {
        let lease = Duration::from_millis(crate::common::config::PxElectionConfig::DEFAULT.lease_duration_ms);
        let now = Instant::now();
        let term;
        let success;
        {
            let mut state = self.election_state.lock();
            if req.term < state.current_term {
                term = state.current_term;
                success = false;
            } else {
                if req.term > state.current_term {
                    state.current_term = req.term;
                    state.voted_for = None;
                }
                state.role = PxLocalReplicaRole::Follower;
                state.leader_id = Some(req.leader_id);
                state.vote_lockout_until = now + lease;
                term = state.current_term;
                success = true;
            }
        }
        if success {
            self.current_term_atomic.store(term, Ordering::Release);
            self.role_atomic.store(PxLocalReplicaRole::Follower.as_u8(), Ordering::Release);
        }
        let (contiguous_chosen, last_chosen_term, highest_seen_slot) = self.frontier_triple();
        let contiguous_applied = self.contiguous_applied();
        debug!(
            replica_l_id = self.id,
            leader_id = req.leader_id,
            req_term = req.term,
            current_term = term,
            success,
            "on_heartbeat"
        );
        HeartbeatReply {
            term,
            success,
            contiguous_chosen,
            last_chosen_term,
            contiguous_applied,
            highest_seen_slot,
        }
    }

    /// `StepDown` handler. Strict-fence policy per `doc/todo_leader.md` §7.1:
    /// accept iff `self.is_leader() && self.id == req.target_leader_id &&
    /// req.term == current_term`. On accept the replica becomes a follower in
    /// the same term; the election driver (Step 9) picks up the role change
    /// on its next tick and runs the full step-down sequence (cancel bulk
    /// Phase 1, stop heartbeats, drain proposals).
    pub(crate) fn handle_step_down(&self, req: &StepDownRequestPayload) -> StepDownReply {
        let snapshot = self.election_state_snapshot();
        let accepted = snapshot.role == PxLocalReplicaRole::Leader && self.id == req.target_leader_id && req.term == snapshot.current_term;
        if accepted {
            info!(
                replica_l_id = self.id,
                req_term = req.term,
                reason = %req.reason,
                "on_step_down accepted (strict fence)"
            );
            // Stay in the same term; only the role flips. The election
            // driver (Step 9.7) waits on `admin_step_down_signal` and
            // runs the canonical step-down sequence (cancel per-tenure
            // token, drain proposals, log reason=Admin) on its next
            // wakeup. We still flip the role here so any concurrent
            // proposer leadership check observes Follower without
            // needing the driver to advance first.
            self.become_follower(snapshot.current_term);
            self.admin_step_down_signal.notify_waiters();
        } else {
            info!(
                replica_l_id = self.id,
                req_term = req.term,
                self_term = snapshot.current_term,
                self_role = ?snapshot.role,
                target_leader_id = req.target_leader_id,
                reason = %req.reason,
                "on_step_down rejected by strict fence"
            );
        }
        StepDownReply {
            accepted,
            current_term: snapshot.current_term,
            current_leader_id: snapshot.leader_id.unwrap_or(0),
        }
    }
}
