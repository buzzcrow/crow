// Copyright 2026-present buzzcrow <buzzcrow@126.com>
// Licensed under the Apache License, Version 2.0.

//! `PxLocalReplica` — local acceptor/learner for a single `CrowKV` group member.
//!
//! Wraps the consensus state (`PxAcceptor`) and learner (`PxLearner`).
//! All proposer/KV logic has been moved to `PxGroup` and `KvStore`.

use crate::cluster::replica::{
    HeartbeatReply, HeartbeatRequestPayload, PxReplicaError, Replica, ReplicaHandler, StepDownReply,
    StepDownRequestPayload, VoteReply, VoteRequestPayload,
};
use crate::cluster::status::{ElectionStateView, KvStoreStatus, ReplicaStatus, StatusLevel};
use crate::common::metrics::{ElectionMetrics, ElectionMetricsSnapshot};
use crate::common::report::OperationReport;
use crate::common::time::{anchor_ms_to_instant, instant_to_anchor_ms};
use crate::metrics::{Counter, Gauge, MetricsRegistry};
use crate::paxos::acceptor::PxAcceptor;
use crate::paxos::learner::PxLearner;
#[cfg(feature = "test-util")]
use crate::paxos::roles::DedupTag;
use crate::paxos::roles::{PxAcceptReply, PxBallot, PxLogEntry, PxPrepareReply, SlotIndex};
use crate::paxos::{PxNodeId, PxTerm};
use crate::wal::WalEngine;
use parking_lot::Mutex;
use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};

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
    pub(super) fn as_u8(self) -> u8 {
        self as u8
    }

    pub(super) fn from_u8(v: u8) -> Self {
        match v {
            0 => Self::Follower,
            1 => Self::PreCandidate,
            2 => Self::Candidate,
            3 => Self::Leader,
            // Atomic mirror is only written under the mutex with a valid
            // enum value; any other byte indicates memory corruption.
            // Per the no-panic-in-non-test-code policy, log a critical
            // error and fall back to `Follower`, the safest role (it
            // cannot serve client writes or grant pre-votes incorrectly).
            // The next legitimate role transition will overwrite this
            // mirror with a valid value.
            other => {
                tracing::error!(
                    "critical: invalid PxLocalReplicaRole discriminant {other}; \
                     falling back to Follower. next step: investigate atomic-mirror \
                     corruption (memory corruption / mismatched as_u8/from_u8)"
                );
                Self::Follower
            }
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
    fn initial(id: PxNodeId, role: PxLocalReplicaRole) -> Self {
        // A locally-constructed leader knows itself as the leader
        // immediately. Followers / candidates have no believed leader
        // until they receive a heartbeat or grant a vote.
        let leader_id = if role == PxLocalReplicaRole::Leader {
            Some(id)
        } else {
            None
        };
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

/// Leader-side lease bookkeeping snapshot.
///
/// Both timestamps are monotonic (`Instant`). `lease_read_until` is the read
/// fast-path deadline (consumed by `Get(Linearizable)` in M5 — maintained but
/// not yet read in M3). `last_quorum_heartbeat_at` is the leadership-liveness
/// timestamp; if `now - last_quorum_heartbeat_at >= lease_duration` the leader
/// must step down.
///
/// On a follower these values are stale; the election driver only reads them
/// while the replica's role is `Leader`. The canonical state lives in two
/// `AtomicU64`s on [`PxLocalReplica`]; this struct is just a snapshot.
#[derive(Debug, Clone, Copy)]
pub struct LeaseState {
    /// Max acknowledged `T_send + lease_duration - max_clock_skew` across all
    /// heartbeat rounds that received a quorum response. Only ever extends
    /// while leadership is held.
    pub lease_read_until: Instant,
    /// Monotonic instant at which the most recent heartbeat round
    /// received a quorum of OK responses. Drives the lease-unrenewable
    /// step-down rule.
    pub last_quorum_heartbeat_at: Instant,
}

/// Runtime container for one local group member.
///
/// Pure acceptor/learner. Proposer logic lives in `PxGroup`.
///
/// Key work: term/voted-for state, role state machine, election-side
/// lease, election + heartbeat handlers.
pub struct PxLocalReplica {
    pub id: u64,
    /// This replica's listen address (set by the store when the group is
    /// added). Used when persisting group config so all nodes share the
    /// same member list.
    pub endpoint: parking_lot::Mutex<Option<String>>,
    /// `Arc`-wrapped so [`Self::new_inheriting_election_state`] can share
    /// the prior replica's per-slot promised/accepted state across a
    /// rebuild (e.g. `add_remote_replicas`). Without sharing, a rebuilt replica
    /// forgets its Paxos promises, violating the safety property
    /// ("never accept a value below the highest promised ballot").
    pub acceptor: Arc<PxAcceptor>,
    /// `Arc`-wrapped so [`Self::new_inheriting_election_state`] can share
    /// the prior replica's learner store (committed KV log + watermarks)
    /// across a rebuild. Without sharing, every `add_remote_replicas` /
    /// `remove_remote_replica` call would silently wipe all previously committed
    /// values from the local replica.
    pub learner: Arc<PxLearner>,
    pub voting: bool,
    /// Election metadata (source of truth).
    pub(super) election_state: Mutex<ElectionPersistentState>,
    /// Lock-free mirror of `election_state.current_term`. Updated under the
    /// mutex via `Release`; readers use `Acquire`. Used by the proposer hot
    /// path (`base_entry`) and by metrics snapshots.
    pub(super) current_term_atomic: AtomicU64,
    /// Lock-free mirror of `election_state.role`. Updated under the mutex via
    /// `Release`; readers use `Acquire`. Cheap hot-path check (e.g. proposer's
    /// leadership gate, `is_leader`).
    pub(super) role_atomic: AtomicU8,
    /// Leader-side lease deadline (read fast-path) encoded as millis since
    /// ``process_anchor``. Updated lock-free via `fetch_max` on heartbeat
    /// renewal and via plain store on tenure transitions. Stale on non-
    /// leader replicas; the election driver only reads it while role ==
    /// `Leader`.
    lease_read_until_ms: AtomicU64,
    /// Monotonic millis since ``process_anchor`` of the most recent quorum
    /// heartbeat round. Drives the lease-unrenewable step-down rule.
    last_quorum_heartbeat_at_ms: AtomicU64,
    /// Lease duration (ms) for this replica. Mirrors the group's
    /// [`crate::common::config::PxElectionConfig::lease_duration_ms`] so
    /// vote/heartbeat handlers can extend `vote_lockout_until` with the
    /// configured (not hard-coded) value. Updated by
    /// [`crate::cluster::group::PxGroup::set_election_config`].
    lease_duration_ms: AtomicU64,
    /// Notify signalled by accepted admin `StepDown` RPCs. The election
    /// driver's leader-state `select!` waits on this so the canonical
    /// step-down sequence runs immediately rather than on the next
    /// heartbeat tick. Cheap: a single `Notify` and a sticky boolean.
    pub(crate) admin_step_down_signal: tokio::sync::Notify,
    /// Notify signalled whenever a valid heartbeat is accepted or a vote
    /// is granted to another candidate. The election driver's
    /// follower-state `select!` waits on this in addition to the
    /// election deadline so a follower receiving steady heartbeats from
    /// the current leader does not spuriously time out and challenge it.
    /// Mirrors Raft's "reset election timer on `AppendEntries`" rule.
    pub(crate) deadline_reset_signal: tokio::sync::Notify,
    /// Optional WAL manager for durability. `None` in P1 (no WAL).
    pub(super) wal: Option<Arc<WalEngine>>,
    /// Idempotency gate for [`Self::shutdown`].
    shutdown_started: AtomicBool,
    /// Per-replica leader-election counters. Cheap `Relaxed` atomic
    /// increments on the election hot path; consumed by
    /// `election_metrics_snapshot` for health / management API.
    pub(super) election_metrics: ElectionMetrics,
    /// Wall-clock-monotonic instant of the most recent accepted
    /// heartbeat (follower side; `None` before the first one). Read
    /// by `election_metrics_snapshot` to compute
    /// `last_heartbeat_age_ms`.
    last_heartbeat_at: Mutex<Option<Instant>>,
    /// Optional registry handles mirroring election counters to the
    /// metrics log file. Set via [`Self::set_metrics_registry`] when
    /// a registry is wired. `None` in tests / no-registry mode.
    pub(super) election_handles: OnceLock<ElectionRegistryHandles>,
    /// R65: replication flow counters + gauges. Set via
    /// [`Self::set_metrics_registry`] when a registry is wired.
    pub(crate) replication_handles: OnceLock<ReplicationRegistryHandles>,
    /// Test-only gate that blocks `spawn_accept_persist`'s background
    /// task before `wal.append`, so a test can deterministically kill
    /// the replica in the CAS→persist window (R16b early-ack). Set via
    /// `set_persist_gate_for_tests` under the `test-util` feature;
    /// `None` in production.
    #[cfg(feature = "test-util")]
    pub(super) persist_gate: Mutex<Option<Arc<tokio::sync::Notify>>>,
    /// R63: latest commit point from heartbeat / batch-chosen / accept.
    /// Advanced via `fetch_max` by `handle_heartbeat` and
    /// `handle_accept_inner`. The background apply loop reads this to know
    /// how far it can apply. `Arc`-wrapped so the spawned apply task can
    /// hold a `'static` clone.
    pub(super) known_commit_slot: Arc<AtomicU64>,
    /// R63: wakes the background apply loop when `known_commit_slot`
    /// advances. `Arc`-wrapped so the spawned apply task can hold a
    /// `'static` clone.
    pub(crate) apply_notify: Arc<tokio::sync::Notify>,
    /// R63: cancellation token for the background apply loop. Owned by the
    /// replica (not per-tenure) so the loop survives role transitions.
    pub(super) apply_loop_cancel: tokio_util::sync::CancellationToken,
    /// R63: join handle for the background apply loop. `None` until the
    /// loop is lazily spawned by `ensure_apply_loop`.
    pub(super) apply_loop_handle: parking_lot::Mutex<Option<tokio::task::JoinHandle<()>>>,
    /// R65: set of slots that need `FetchGap` (missing or stale value).
    /// Populated by the `ChosenNotice` handler and the apply loop;
    /// consumed by the `FetchGap` driver. `Arc`-wrapped so the spawned
    /// `FetchGap` driver task can hold a `'static` clone.
    pub(crate) gap_slots: Arc<Mutex<BTreeSet<SlotIndex>>>,
    /// R65: count of in-flight `FetchGap` requests. Bounded by
    /// `MAX_INFLIGHT_FETCHGAP` to prevent flooding the leader.
    pub(crate) fetchgap_inflight: Arc<AtomicU64>,
}

/// Registry-based metric handles for election counters and gauges.
/// Created by [`PxLocalReplica::set_metrics_registry`] and stored in
/// `OnceLock` for lock-free hot-path reads.
pub(crate) struct ElectionRegistryHandles {
    pub(crate) elections: Arc<Counter>,
    pub(crate) step_downs_higher_term: Arc<Counter>,
    pub(crate) step_downs_lease: Arc<Counter>,
    pub(crate) step_downs_admin: Arc<Counter>,
    pub(crate) inflight_slots: Arc<Gauge>,
}

/// R65: Registry-based metric handles for replication flow counters and
/// gauges. Created by [`PxLocalReplica::set_metrics_registry`] and stored
/// in `OnceLock` for lock-free hot-path reads.
pub(crate) struct ReplicationRegistryHandles {
    pub(crate) chosen_notice_stale_ballot: Arc<Counter>,
    pub(crate) chosen_notice_missing_value: Arc<Counter>,
    pub(crate) fetchgap_sent: Arc<Counter>,
    pub(crate) fetchgap_received: Arc<Counter>,
    pub(crate) fetchgap_failed: Arc<Counter>,
    pub(crate) apply_loop_skip: Arc<Counter>,
    pub(crate) gap_count: Arc<Gauge>,
    pub(crate) fetchgap_inflight: Arc<Gauge>,
    pub(crate) last_chosen_slot: Arc<Gauge>,
    pub(crate) known_commit_slot: Arc<Gauge>,
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
        // Not ideal (locks + clones) but this is only called on the
        // status/diagnostic path, not the hot path.
        None
    }

    fn voting(&self) -> bool {
        self.voting
    }
}

impl ReplicaHandler for PxLocalReplica {
    async fn on_prepare(
        &self,
        slot: u64,
        ballot: PxBallot,
        term: PxTerm,
        _group_id: u64,
    ) -> Result<PxPrepareReply, PxReplicaError> {
        Ok(self.on_prepare(slot, ballot, term).await)
    }

    async fn on_accept(&self, entry: &PxLogEntry, _group_id: u64) -> Result<PxAcceptReply, PxReplicaError> {
        Ok(self.on_accept(entry).await)
    }

    async fn on_pre_vote(
        &self,
        req: VoteRequestPayload,
        _group_id: u64,
    ) -> Result<VoteReply, PxReplicaError> {
        Ok(self.handle_pre_vote(req))
    }

    async fn on_request_vote(
        &self,
        req: VoteRequestPayload,
        _group_id: u64,
    ) -> Result<VoteReply, PxReplicaError> {
        Ok(self.handle_request_vote(req).await)
    }

    async fn on_heartbeat(
        &self,
        req: HeartbeatRequestPayload,
        _group_id: u64,
    ) -> Result<HeartbeatReply, PxReplicaError> {
        Ok(self.handle_heartbeat(req))
    }

    async fn on_step_down(
        &self,
        req: &StepDownRequestPayload,
        _group_id: u64,
    ) -> Result<StepDownReply, PxReplicaError> {
        Ok(self.handle_step_down(req))
    }
}

impl PxLocalReplica {
    #[must_use]
    pub fn new(id: u64, role: PxLocalReplicaRole) -> Self {
        Self {
            id,
            endpoint: parking_lot::Mutex::new(None),
            acceptor: Arc::new(PxAcceptor::new()),
            learner: Arc::new(PxLearner::new()),
            voting: true,
            election_state: Mutex::new(ElectionPersistentState::initial(id, role)),
            current_term_atomic: AtomicU64::new(0),
            role_atomic: AtomicU8::new(role.as_u8()),
            lease_read_until_ms: AtomicU64::new(instant_to_anchor_ms(Instant::now())),
            last_quorum_heartbeat_at_ms: AtomicU64::new(instant_to_anchor_ms(Instant::now())),
            lease_duration_ms: AtomicU64::new(
                crate::common::config::PxElectionConfig::DEFAULT.lease_duration_ms,
            ),
            wal: None,
            admin_step_down_signal: tokio::sync::Notify::new(),
            deadline_reset_signal: tokio::sync::Notify::new(),
            shutdown_started: AtomicBool::new(false),
            election_metrics: ElectionMetrics::new(),
            last_heartbeat_at: Mutex::new(None),
            election_handles: OnceLock::new(),
            replication_handles: OnceLock::new(),
            #[cfg(feature = "test-util")]
            persist_gate: Mutex::new(None),
            known_commit_slot: Arc::new(AtomicU64::new(0)),
            apply_notify: Arc::new(tokio::sync::Notify::new()),
            apply_loop_cancel: tokio_util::sync::CancellationToken::new(),
            apply_loop_handle: parking_lot::Mutex::new(None),
            gap_slots: Arc::new(Mutex::new(BTreeSet::new())),
            fetchgap_inflight: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Like [`Self::new`], but with a caller-supplied [`PxLearner`] (already
    /// wrapping whichever [`KVEngine`] backend was chosen) instead of a
    /// freshly-default-constructed one. Used by
    /// [`Self::restore_from_replay_with_engine`].
    #[must_use]
    pub(super) fn new_with_learner(id: u64, role: PxLocalReplicaRole, learner: PxLearner) -> Self {
        Self {
            learner: Arc::new(learner),
            ..Self::new(id, role)
        }
    }

    /// Construct a fresh `PxLocalReplica` that inherits the election
    /// persistent state (`current_term`, `voted_for`, `role`,
    /// `leader_id`, `vote_lockout_until`) from `prior`. Used by the
    /// management-API rebuild path (`add_remote_replicas`, `remove_remote_replica`,
    /// `batch_add_remote_replicas`) so wiring a new remote into a group does
    /// **not** reset the cluster's election state. Without this, every
    /// remote-add forces another election round, preventing leadership
    /// from converging when a multi-replica group is being built up.
    ///
    /// The `id`, `voting` flag, and the [`PxAcceptor`] / [`PxLearner`]
    /// are inherited from `prior` (the latter two via `Arc::clone` so
    /// per-slot promises and the committed KV log survive the rebuild).
    /// Per-tenure ephemeral signals (`Notify`s, `shutdown_started`) are
    /// freshly initialised on the new instance.
    #[must_use]
    pub fn new_inheriting_election_state(prior: &Self) -> Self {
        let snapshot = prior.election_state.lock().clone();
        let term = snapshot.current_term;
        let role = snapshot.role;
        Self {
            id: prior.id,
            endpoint: parking_lot::Mutex::new(prior.endpoint.lock().clone()),
            // Share the Paxos acceptor + learner with the prior replica.
            // The acceptor must persist across rebuild for safety (Paxos
            // P2b: an acceptor must not violate prior promises); the
            // learner must persist or all previously committed KV writes
            // disappear from this replica's local store.
            acceptor: Arc::clone(&prior.acceptor),
            learner: Arc::clone(&prior.learner),
            voting: prior.voting,
            election_state: Mutex::new(snapshot),
            current_term_atomic: AtomicU64::new(term),
            role_atomic: AtomicU8::new(role.as_u8()),
            lease_read_until_ms: AtomicU64::new(instant_to_anchor_ms(Instant::now())),
            last_quorum_heartbeat_at_ms: AtomicU64::new(instant_to_anchor_ms(Instant::now())),
            lease_duration_ms: AtomicU64::new(prior.lease_duration_ms.load(Ordering::Acquire)),
            wal: prior.wal.clone(),
            admin_step_down_signal: tokio::sync::Notify::new(),
            deadline_reset_signal: tokio::sync::Notify::new(),
            shutdown_started: AtomicBool::new(false),
            election_metrics: ElectionMetrics::new(),
            last_heartbeat_at: Mutex::new(None),
            election_handles: OnceLock::new(),
            replication_handles: OnceLock::new(),
            #[cfg(feature = "test-util")]
            persist_gate: Mutex::new(None),
            known_commit_slot: Arc::new(AtomicU64::new(0)),
            apply_notify: Arc::new(tokio::sync::Notify::new()),
            apply_loop_cancel: tokio_util::sync::CancellationToken::new(),
            apply_loop_handle: parking_lot::Mutex::new(None),
            gap_slots: Arc::new(Mutex::new(BTreeSet::new())),
            fetchgap_inflight: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Attach a WAL manager for durable persistence (P2 W6).
    ///
    /// After this is set, `on_accept`/`on_prepare` will persist to the WAL
    /// before returning success (the ack contract). `handle_request_vote`
    /// will persist `voted_for` changes.
    pub fn set_wal(&mut self, wal: Arc<WalEngine>) {
        self.wal = Some(wal);
    }

    /// Install a `Notify` gate that blocks `spawn_accept_persist`'s
    /// background task before `wal.append`. The test keeps the `Arc`;
    /// the background task waits on `notify.notified()` and only
    /// proceeds once the test calls `notify_one()`. Used by T1
    /// crash-recovery tests to deterministically hit the CAS→persist
    /// window.
    #[cfg(feature = "test-util")]
    pub fn set_persist_gate_for_tests(&self, notify: Arc<tokio::sync::Notify>) {
        *self.persist_gate.lock() = Some(notify);
    }

    /// R63 test-only: simulate the `handle_accept_inner` deferred-apply path
    /// (advance chosen frontier + dedup + `known_commit_slot` + wake apply
    /// loop) without going through the RPC handler. Used by the
    /// follower-wins-election deadlock regression test.
    #[cfg(feature = "test-util")]
    pub fn simulate_accept_deferred_apply(&self, entry: &PxLogEntry, dedup_tags: &[DedupTag]) {
        self.learner.update_chosen_frontier(entry.slot, entry.term);
        self.learner.record_dedup_tags(dedup_tags, entry.slot);
        self.advance_known_commit_slot(entry.slot);
        self.wake_apply_loop();
    }

    /// Set this replica's listen endpoint (called by the store when the
    /// group is added). Used when persisting group config so all nodes
    /// share the same member list.
    pub fn set_endpoint(&self, endpoint: String) {
        *self.endpoint.lock() = Some(endpoint);
    }

    /// Get this replica's listen endpoint, if set.
    #[must_use]
    pub fn get_endpoint(&self) -> Option<String> {
        self.endpoint.lock().clone()
    }

    /// Read-only access to the optional WAL engine.
    #[must_use]
    pub fn wal(&self) -> Option<&Arc<WalEngine>> {
        self.wal.as_ref()
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
        self.current_term_atomic
            .store(guard.current_term, Ordering::Release);
        out
    }

    /// Snapshot of the entire election state (cheap clone of small POD).
    #[must_use]
    pub fn election_state_snapshot(&self) -> ElectionPersistentState {
        self.election_state.lock().clone()
    }

    /// Cascade shutdown: flush WAL + engine state, then rely on
    /// `Drop` for acceptor/learner resource release (slot list reclaim,
    /// in-memory KV map drop).
    ///
    /// The maintenance loop is already cancelled by `PxGroup::shutdown`
    /// before this method is called, so there is no concurrent
    /// flush/snapshot risk. The WAL is durably flushed (real `fsync`)
    /// regardless of `--no-fsync` — shutdown always persists. Then
    /// `engine.flush()` drains L0 memtable into L1 B+tree (in-memory);
    /// `persist_snapshot()` writes dirty L1 pages + superblock to the
    /// page store (disk). For `InMemKV` both are no-ops.
    #[tracing::instrument(level = "debug", skip_all, fields(replica_l_id = self.id))]
    #[allow(clippy::unused_async)] // async kept for cascade uniformity
    pub async fn shutdown(&self, _per_layer_timeout: Duration) -> OperationReport {
        if self.shutdown_started.swap(true, Ordering::AcqRel) {
            debug!(
                replica_l_id = self.id,
                "PxLocalReplica::shutdown is a no-op (already shut down)"
            );
            return OperationReport::new();
        }

        // R63: stop the background apply loop before flushing the engine.
        self.stop_apply_loop();

        // Durably flush the WAL (real fsync, regardless of --no-fsync).
        // This ensures shutdown always persists data to disk.
        if let Some(wal) = &self.wal {
            if let Err(e) = wal.flush_all().await {
                warn!(
                    replica_l_id = self.id,
                    error = %e,
                    "WAL flush_all failed during shutdown"
                );
            } else {
                debug!(replica_l_id = self.id, "WAL flush_all completed during shutdown");
            }
        }

        let engine = self.learner.engine();
        engine.flush();
        let snap_slot = engine.persist_snapshot();
        if snap_slot > 0 {
            info!(
                replica_l_id = self.id,
                snapshot_slot = snap_slot,
                "PxLocalReplica shutdown: engine snapshot persisted"
            );
        } else {
            debug!(
                replica_l_id = self.id,
                "PxLocalReplica shutdown: no durable snapshot (non-durable engine or no data)"
            );
        }
        info!(
            replica_l_id = self.id,
            "PxLocalReplica shutdown complete (acceptor/learner cleanup deferred to Drop)"
        );
        OperationReport::new()
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

    /// Believed leader id (i.e. who this replica thinks is currently the
    /// leader of its group). `None` means "unknown" — the cluster is in the
    /// middle of an election or this replica has not yet received a
    /// heartbeat from a leader. Updated by:
    ///
    /// - [`Self::become_leader`] (sets `Some(self.id)`)
    /// - [`Self::become_follower`] (clears to `None`)
    /// - `on_heartbeat` (sets to the heartbeating leader's id)
    /// - `on_request_vote` (clears when a vote is granted)
    /// - [`Self::set_believed_leader`] (test/admin override)
    #[must_use]
    pub fn believed_leader_id(&self) -> Option<PxNodeId> {
        self.election_state.lock().leader_id
    }

    /// Test/admin override for [`Self::believed_leader_id`]. Used by the
    /// pinned-leader testkit (which skips the election driver). Production
    /// code paths update the believed leader through the role transitions /
    /// heartbeat / vote handlers, not this setter.
    pub fn set_believed_leader(&self, leader_id: PxNodeId) {
        self.election_state.lock().leader_id = Some(leader_id);
    }

    pub fn clear_believed_leader(&self) {
        self.election_state.lock().leader_id = None;
    }

    pub fn clear_vote_lockout(&self) {
        self.election_state.lock().vote_lockout_until = Instant::now();
    }

    /// Lock-free snapshot of [`LeaseState`].
    #[must_use]
    pub fn lease_state_snapshot(&self) -> LeaseState {
        LeaseState {
            lease_read_until: anchor_ms_to_instant(self.lease_read_until_ms.load(Ordering::Acquire)),
            last_quorum_heartbeat_at: anchor_ms_to_instant(
                self.last_quorum_heartbeat_at_ms.load(Ordering::Acquire),
            ),
        }
    }

    /// Snapshot of `lease_read_until` only. Cheaper than the full state.
    #[must_use]
    pub fn lease_read_until(&self) -> Instant {
        anchor_ms_to_instant(self.lease_read_until_ms.load(Ordering::Acquire))
    }

    /// Whether a linearizable read may be served locally from this replica's
    /// own applied state without a quorum round-trip: the replica must be the
    /// leader and hold a still-valid read lease at `now`.
    ///
    /// A freshly elected leader starts with an expired lease
    /// (`reset_lease_to` on tenure entry), so the first quorum heartbeat must
    /// extend it before the read fast path opens. When this returns `false`
    /// the caller falls back to a `ReadIndex` quorum check.
    #[must_use]
    pub fn lease_read_valid(&self, now: Instant) -> bool {
        self.is_leader() && self.lease_read_until() > now
    }

    /// Snapshot of `last_quorum_heartbeat_at` only.
    #[must_use]
    pub fn last_quorum_heartbeat_at(&self) -> Instant {
        anchor_ms_to_instant(self.last_quorum_heartbeat_at_ms.load(Ordering::Acquire))
    }

    /// Replace both lease timestamps with `now`. Used on tenure entry
    /// (`become_leader`) and on tenure exit (`become_follower`) to drop a
    /// stale tenure's lease deadline.
    pub fn reset_lease_to(&self, now: Instant) {
        let ms = instant_to_anchor_ms(now);
        self.lease_read_until_ms.store(ms, Ordering::Release);
        self.last_quorum_heartbeat_at_ms.store(ms, Ordering::Release);
    }

    /// Monotonically extend `lease_read_until` to `candidate` (no-op if
    /// already past). Heartbeat-tick hot path.
    pub fn extend_lease_read_until(&self, candidate: Instant) {
        let ms = instant_to_anchor_ms(candidate);
        self.lease_read_until_ms.fetch_max(ms, Ordering::AcqRel);
    }

    /// Record a successful quorum heartbeat at `now`. Heartbeat-tick hot
    /// path; lock-free.
    pub fn record_quorum_heartbeat(&self, now: Instant) {
        self.last_quorum_heartbeat_at_ms
            .store(instant_to_anchor_ms(now), Ordering::Release);
    }

    /// Update the cached lease duration (ms). Called from
    /// [`crate::cluster::group::PxGroup::set_election_config`].
    pub fn set_lease_duration_ms(&self, ms: u64) {
        self.lease_duration_ms.store(ms, Ordering::Release);
    }

    /// Quorum-OK heartbeat post-processing: extend `lease_read_until`
    /// to `t_send + lease_duration - max_clock_skew` and bump
    /// `last_quorum_heartbeat_at` to "now".
    ///
    /// Two atomic operations rather than a mutex round-trip; both
    /// fields only ever advance, so a brief torn snapshot from a
    /// concurrent reader cannot regress either value. Saturating sub
    /// avoids underflow when `skew >= lease_duration`.
    pub fn renew_lease(&self, t_send: Instant, cfg: &crate::common::config::PxElectionConfig) {
        let lease_dur = std::time::Duration::from_millis(cfg.lease_duration_ms);
        let skew = std::time::Duration::from_millis(cfg.max_clock_skew_ms);
        let extended_until = t_send + lease_dur.saturating_sub(skew);
        self.extend_lease_read_until(extended_until);
        self.record_quorum_heartbeat(Instant::now());
    }

    /// Snapshot of the configured lease duration (ms).
    #[must_use]
    pub fn lease_duration_ms(&self) -> u64 {
        self.lease_duration_ms.load(Ordering::Acquire)
    }

    /// Borrow the per-replica election counter handle so the election
    /// driver / step-down sequence can bump counters without going
    /// through additional accessor noise.
    #[must_use]
    pub(crate) fn election_metrics(&self) -> &ElectionMetrics {
        &self.election_metrics
    }

    /// Borrow optional registry handles for election counters. Returns
    /// `None` when no metrics registry is wired (tests / no-registry mode).
    #[must_use]
    pub(crate) fn election_registry_handles(&self) -> Option<&ElectionRegistryHandles> {
        self.election_handles.get()
    }

    /// Register election counters and WAL append summary with the metrics
    /// registry. Called once during group creation when a registry is
    /// available. Stores handles in `OnceLock` for lock-free hot-path reads.
    ///
    /// # Panics
    ///
    /// Panics if the metrics registry mutex is poisoned.
    pub fn set_metrics_registry(
        &self,
        registry: &Arc<std::sync::Mutex<MetricsRegistry>>,
        store_id: u64,
        group_id: u64,
    ) {
        let mut r = registry.lock().expect("metrics registry poisoned");
        let prefix = format!("s.{store_id}.g.{group_id}");
        let handles = ElectionRegistryHandles {
            elections: r.register_counter(format!("{prefix}.paxos.elections.c")),
            step_downs_higher_term: r.register_counter(format!("{prefix}.paxos.step_downs.higher_term.c")),
            step_downs_lease: r.register_counter(format!("{prefix}.paxos.step_downs.lease.c")),
            step_downs_admin: r.register_counter(format!("{prefix}.paxos.step_downs.admin.c")),
            inflight_slots: r.register_gauge(format!("{prefix}.paxos.inflight_slots.g")),
        };
        let _ = self.election_handles.set(handles);
        // R65: replication flow metrics.
        let repl_handles = ReplicationRegistryHandles {
            chosen_notice_stale_ballot: r
                .register_counter(format!("{prefix}.paxos.chosen_notice.stale_ballot.c")),
            chosen_notice_missing_value: r
                .register_counter(format!("{prefix}.paxos.chosen_notice.missing_value.c")),
            fetchgap_sent: r.register_counter(format!("{prefix}.paxos.fetchgap.sent.c")),
            fetchgap_received: r.register_counter(format!("{prefix}.paxos.fetchgap.received.c")),
            fetchgap_failed: r.register_counter(format!("{prefix}.paxos.fetchgap.failed.c")),
            apply_loop_skip: r.register_counter(format!("{prefix}.paxos.apply_loop.skip.c")),
            gap_count: r.register_gauge(format!("{prefix}.paxos.gap_count.g")),
            fetchgap_inflight: r.register_gauge(format!("{prefix}.paxos.fetchgap.inflight.g")),
            last_chosen_slot: r.register_gauge(format!("{prefix}.paxos.last_chosen_slot.g")),
            known_commit_slot: r.register_gauge(format!("{prefix}.paxos.known_commit_slot.g")),
        };
        let _ = self.replication_handles.set(repl_handles);
        let engine_apply = r.register_summary(format!("{prefix}.write.engine_apply.l"));
        self.learner.set_engine_apply_summary(engine_apply);
        if let Some(ref wal) = self.wal {
            let bl = wal.backend_label();
            let append_summary = r.register_summary(format!("{prefix}.wal.{bl}.append.l"));
            wal.set_append_summary(append_summary);
            let fsync_summary = r.register_summary(format!("{prefix}.wal.{bl}.fsync.l"));
            let write_bw = r.register_bandwidth(format!("{prefix}.wal.{bl}.write.bw"));
            wal.set_fsync_metrics(fsync_summary, write_bw);
            if wal.backend().is_block_device() {
                let handles = crate::wal::wal_engine::BlockDeviceCounterHandles {
                    logical_bytes: r.register_counter(format!("{prefix}.wal.{bl}.logical_bytes.c")),
                    physical_bytes: r.register_counter(format!("{prefix}.wal.{bl}.physical_bytes.c")),
                    rmw: r.register_counter(format!("{prefix}.wal.{bl}.rmw.c")),
                };
                wal.set_block_device_counters(handles);
            }
        }
    }

    /// Combined election + lease snapshot for the management API /
    /// health endpoint. Combines the monotonic counters with derived
    /// gauges computed at read time so we don't have to keep extra
    /// atomics in sync with the canonical mutex-guarded state.
    #[must_use]
    pub fn election_metrics_snapshot(&self, bulk_phase1_in_flight_slots: u64) -> ElectionMetricsSnapshot {
        let counters = self.election_metrics.counters();
        let now = Instant::now();
        let last_heartbeat_age_ms = self.last_heartbeat_at.lock().map(|inst| {
            now.saturating_duration_since(inst)
                .as_millis()
                .try_into()
                .unwrap_or(u64::MAX)
        });
        let lease_remaining_ms = if self.is_leader() {
            // `lease_read_until` is the publishable read-lease deadline.
            // When the lease is in the past we report `None`.
            let lease_read_until = self.lease_read_until();
            if lease_read_until > now {
                Some(
                    lease_read_until
                        .saturating_duration_since(now)
                        .as_millis()
                        .try_into()
                        .unwrap_or(u64::MAX),
                )
            } else {
                None
            }
        } else {
            None
        };
        if let Some(h) = self.election_handles.get() {
            h.inflight_slots.set(bulk_phase1_in_flight_slots);
        }
        ElectionMetricsSnapshot {
            election_count: counters.election_count,
            current_term: self.current_term_snapshot(),
            last_heartbeat_age_ms,
            lease_remaining_ms,
            bulk_phase1_in_flight_slots,
            step_downs_higher_term: counters.step_downs_higher_term,
            step_downs_lease_unrenewable: counters.step_downs_lease_unrenewable,
            step_downs_admin: counters.step_downs_admin,
        }
    }

    /// Record the wall-clock-monotonic instant of the most recently
    /// accepted heartbeat (follower side). Called from
    /// [`Self::handle_heartbeat`] on the success path so the management
    /// snapshot can report `last_heartbeat_age_ms`.
    pub(crate) fn note_heartbeat_received(&self) {
        *self.last_heartbeat_at.lock() = Some(Instant::now());
    }

    /// Point-in-time status for the topology endpoint. Exposes only cheap
    /// (`O(1)`) data: the kv-store key count via `DashMap::len`.
    #[allow(clippy::cast_possible_truncation)]
    #[must_use]
    pub fn status(&self) -> ReplicaStatus {
        let mut status = StatusLevel::Ok;
        let mut messages = Vec::new();
        if self.shutdown_started.load(Ordering::Acquire) {
            status = StatusLevel::Unhealthy;
            messages.push(format!("local replica {} has been shut down", self.id));
        }
        let engine_healthy = self.learner.engine().is_healthy();
        if !engine_healthy {
            status = StatusLevel::Unhealthy;
            messages.push(format!(
                "local replica {}'s KV engine reports unhealthy (durable I/O fault latched)",
                self.id
            ));
        }
        let crowtree_stats = self
            .learner
            .engine()
            .as_any()
            .downcast_ref::<crate::kv::CrowTreeEngine>()
            .map(|e| crate::cluster::status::crow_tree_stats_to_view(e.stats()));
        let role = match self.role() {
            PxLocalReplicaRole::Leader => "leader",
            PxLocalReplicaRole::Follower => "follower",
            PxLocalReplicaRole::PreCandidate => "pre_candidate",
            PxLocalReplicaRole::Candidate => "candidate",
        };
        // Read the current inflight_slots gauge so election_metrics_snapshot
        // re-sets it to its current value (no-op) rather than zeroing it.
        let bulk_inflight = self
            .election_handles
            .get()
            .map_or(0, |h| h.inflight_slots.snapshot());
        let election = Some(ElectionStateView::from(
            self.election_metrics_snapshot(bulk_inflight),
        ));
        ReplicaStatus {
            id: self.id,
            role: role.to_string(),
            voting: self.voting,
            status,
            messages,
            kv_store: KvStoreStatus {
                key_count: self.learner.live_key_count() as u64,
                engine_healthy,
                crowtree_stats,
            },
            election,
        }
    }
}
