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
use crate::cluster::status::{CrowtreeStatsView, KvStoreStatus, ReplicaStatus, StatusLevel};
use crate::common::metrics::{ElectionMetrics, ElectionMetricsSnapshot};
use crate::common::report::OperationReport;
use crate::common::time::{anchor_ms_to_instant, instant_to_anchor_ms};
use crate::kv::{CrowtreeBackend, CrowtreeEngine, CrowtreeOptions, KVEngine};
use crate::metrics::{Counter, Gauge, MetricsRegistry};
use crate::paxos::acceptor::PxAcceptor;
use crate::paxos::learner::PxLearner;
use crate::paxos::roles::{
    Acceptor, Learner, PxAcceptReply, PxBallot, PxLogEntry, PxPrepareReply, SlotIndex,
};
use crate::paxos::{PxNodeId, PxTerm};
use crate::wal::record::WALRecord;
use crate::wal::replay::ReplayResult;
use crate::wal::WalEngine;
use parking_lot::Mutex;
use std::io;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};
use tracing::{debug, info, trace};

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
    election_state: Mutex<ElectionPersistentState>,
    /// Lock-free mirror of `election_state.current_term`. Updated under the
    /// mutex via `Release`; readers use `Acquire`. Used by the proposer hot
    /// path (`base_entry`) and by metrics snapshots.
    current_term_atomic: AtomicU64,
    /// Lock-free mirror of `election_state.role`. Updated under the mutex via
    /// `Release`; readers use `Acquire`. Cheap hot-path check (e.g. proposer's
    /// leadership gate, `is_leader`).
    role_atomic: AtomicU8,
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
    wal: Option<Arc<WalEngine>>,
    /// Idempotency gate for [`Self::shutdown`].
    shutdown_started: AtomicBool,
    /// Per-replica leader-election counters. Cheap `Relaxed` atomic
    /// increments on the election hot path; consumed by
    /// `election_metrics_snapshot` for health / management API.
    election_metrics: ElectionMetrics,
    /// Wall-clock-monotonic instant of the most recent accepted
    /// heartbeat (follower side; `None` before the first one). Read
    /// by `election_metrics_snapshot` to compute
    /// `last_heartbeat_age_ms`.
    last_heartbeat_at: Mutex<Option<Instant>>,
    /// Optional registry handles mirroring election counters to the
    /// metrics log file. Set via [`Self::set_metrics_registry`] when
    /// a registry is wired. `None` in tests / no-registry mode.
    election_handles: OnceLock<ElectionRegistryHandles>,
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

    async fn on_accept(&self, entry: PxLogEntry, _group_id: u64) -> Result<PxAcceptReply, PxReplicaError> {
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
        Ok(self.handle_heartbeat(req).await)
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
        }
    }

    /// Like [`Self::new`], but with a caller-supplied [`PxLearner`] (already
    /// wrapping whichever [`KVEngine`] backend was chosen) instead of a
    /// freshly-default-constructed one. Used by
    /// [`Self::restore_from_replay_with_engine`].
    #[must_use]
    fn new_with_learner(id: u64, role: PxLocalReplicaRole, learner: PxLearner) -> Self {
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

    /// Rebuild a fresh local replica from WAL replay output, using the
    /// default [`KVEngine`] (crowtree with mem-block backend, in-memory).
    /// See [`Self::restore_from_replay_with_engine`] for the full argument
    /// and for injecting a durable backend (e.g. [`crate::kv::CrowtreeEngine`]
    /// with file/block storage).
    ///
    /// # Errors
    ///
    /// Returns `InvalidData` if any replayed promised/accepted record cannot be
    /// re-applied through the normal replica handlers.
    pub async fn restore_from_replay(
        id: u64,
        role: PxLocalReplicaRole,
        replay: &ReplayResult,
    ) -> io::Result<Self> {
        let opt = CrowtreeOptions {
            backend: CrowtreeBackend::MemBlock,
            ..Default::default()
        };
        let engine = CrowtreeEngine::open(&opt)
            .map_err(|e| io::Error::other(format!("crowtree mem-block open failed: {e:?}")))?;
        Self::restore_from_replay_with_engine(id, role, replay, Box::new(engine)).await
    }

    /// Rebuild a fresh local replica from WAL replay output, backed by a
    /// caller-supplied [`KVEngine`] instead of the default in-memory one.
    ///
    /// Replays the recovered records through the normal acceptor / learner APIs
    /// so restored state follows the same invariants as live traffic. A
    /// durable engine that reports a non-zero [`KVEngine::resume_from_slot`]
    /// (e.g. [`crate::kv::CrowtreeEngine`] recovered from an on-disk snapshot)
    /// skips re-`learn`ing that already-durable prefix — see Pass 2 below
    /// for how the learner's frontier is seeded to match what a full replay
    /// would have produced.
    ///
    /// # Errors
    ///
    /// Returns `InvalidData` if any replayed promised/accepted record cannot be
    /// re-applied through the normal replica handlers.
    pub async fn restore_from_replay_with_engine(
        id: u64,
        role: PxLocalReplicaRole,
        replay: &ReplayResult,
        engine: Box<dyn KVEngine>,
    ) -> io::Result<Self> {
        // Read before the engine is wrapped/used: `resume_from_slot`'s
        // contract only promises an accurate floor for a freshly-recovered
        // engine that hasn't taken any `apply` calls yet in this process.
        let resume_from = engine.resume_from_slot();
        let replica = Self::new_with_learner(id, role, PxLearner::with_engine(engine));

        // Pass 1: rebuild acceptor (promise + accept) state from the WAL.
        for record in &replay.records {
            match record.record_type {
                crate::wal::record::RecordType::Promised => {
                    let _ = replica.acceptor.prepare(record.slot, record.ballot).await;
                }
                crate::wal::record::RecordType::Accepted => {
                    let entry = record.to_log_entry().ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            "restore replay accepted missing log entry",
                        )
                    })?;
                    let _ = replica.acceptor.accept(entry).await;
                }
                crate::wal::record::RecordType::VoteGranted => {}
            }
        }

        replica.with_election_state(|state| {
            state.current_term = replay.current_term;
            state.voted_for = replay.voted_for;
            state.role = role;
            state.leader_id = if role == PxLocalReplicaRole::Leader {
                Some(id)
            } else {
                None
            };
        });
        replica.role_atomic.store(role.as_u8(), Ordering::Release);

        // Pass 2: rebuild the learner's committed KV state from the acceptor.
        //
        // The acceptor was fully rebuilt in Pass 1 (highest-ballot-per-slot
        // wins).  Now we walk every slot that has an accepted entry and
        // `learn` it into the state machine.  This is safe because:
        //
        // - `KVEngine::apply` is idempotent: an op is skipped when
        //   `slot <= resolved_slot(key)`, so re-applying the same slot is a
        //   no-op.
        // - `apply` uses highest-slot-wins per key, so out-of-order replay
        //   still produces the correct final KV state.
        // - `update_frontier` handles out-of-order slots via the
        //   `out_of_order` BTreeMap, so watermarks stay correct even with
        //   gaps.
        // - NoOp entries (empty payload) are skipped by `apply_entry` and
        //   do not corrupt the KV state.
        //
        // If the engine reported a resume floor (`resume_from > 0`), skip
        // re-`learn`ing that prefix and start the walk at `resume_from +
        // 1` -- always, even if the term at `resume_from` can't be
        // recovered below. This is not just an optimization: an engine with
        // its own internal durable-floor gate (e.g. crowtree's
        // `MemTable::durable_floor`, set from `resume_from_slot`'s exact
        // value at `flush` time) rejects *any* write at `slot <= floor`
        // regardless of key -- stronger than the per-key highest-slot-wins
        // `KVEngine::apply` documents -- so re-attempting a write below the
        // floor isn't just redundant, it can silently no-op a key that slot
        // legitimately touches. There is no safe way to "fall back" to
        // replaying it once the engine is past that floor.
        //
        // Seed the frontier to `(resume_from, term-at-resume_from)` via
        // `seed_resume_frontier` when the just-rebuilt acceptor has an
        // accepted entry at that exact slot (the expected case: an engine
        // can only ever have durably applied a slot that was itself
        // accepted and WAL-logged, and Pass 1 rebuilds the *entire* WAL
        // history). If it's missing (e.g. a WAL segment lost/GC'd after the
        // engine already durably flushed that slot -- not expected, but not
        // an invariant this restore path should trust blindly), leave the
        // frontier at the fresh learner's default (`0`) rather than guess a
        // term: under-reporting `contiguous_chosen`/`last_chosen_term` only
        // costs more conservative heartbeat catch-up / safe-read bounds,
        // never incorrectness, unlike attempting the skipped replay.
        let highest = replica.acceptor.highest_seen_slot();
        let resume_from = resume_from.min(highest);
        let start_slot = if resume_from > 0 {
            if let Some(entry) = replica.acceptor.accepted_at(resume_from) {
                replica.learner.seed_resume_frontier(resume_from, entry.term);
            }
            resume_from + 1
        } else {
            1
        };
        for slot in start_slot..=highest {
            if let Some(entry) = replica.acceptor.accepted_at(slot) {
                replica.learner.learn(entry, None, None).await;
            }
        }

        Ok(replica)
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

    /// Cascade shutdown into acceptor + learner.
    ///
    /// Acceptor and learner currently rely on `Drop` for resource release
    /// (slot list reclaim, in-memory KV map drop). The explicit cascade is a
    /// hook for future persistence layers (P3) which will need to flush.
    #[tracing::instrument(level = "debug", skip_all, fields(replica_l_id = self.id))]
    #[allow(clippy::unused_async)] // async kept for cascade uniformity (P3 will await flush)
    pub async fn shutdown(&self, _per_layer_timeout: Duration) -> OperationReport {
        if self.shutdown_started.swap(true, Ordering::AcqRel) {
            debug!(
                replica_l_id = self.id,
                "PxLocalReplica::shutdown is a no-op (already shut down)"
            );
            return OperationReport::new();
        }
        info!(
            replica_l_id = self.id,
            "PxLocalReplica shutdown (acceptor/learner cleanup deferred to Drop)"
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
    pub fn election_metrics(&self) -> &ElectionMetrics {
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
        if let Some(ref wal) = self.wal {
            let summary = r.register_summary(format!("{prefix}.wal.append.l"));
            wal.set_append_summary(summary);
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
            // Stepping down: the believed leader is unknown until the
            // next heartbeat / vote round establishes one.
            s.leader_id = None;
        });
        self.role_atomic
            .store(PxLocalReplicaRole::Follower.as_u8(), Ordering::Release);
        // Lease is no longer meaningful as a non-leader. Expire it so any
        // stale read fast-path attempt rejects.
        self.reset_lease_to(Instant::now());
        info!(replica_l_id = self.id, current_term = new_term, "become_follower");
    }

    /// Transition to `PreCandidate`. Does not bump term (per Raft `PreVote`).
    #[tracing::instrument(level = "info", skip_all, fields(replica_l_id = self.id))]
    pub fn become_precandidate(&self) {
        self.with_election_state(|s| {
            s.role = PxLocalReplicaRole::PreCandidate;
        });
        self.role_atomic
            .store(PxLocalReplicaRole::PreCandidate.as_u8(), Ordering::Release);
        info!(
            replica_l_id = self.id,
            current_term = self.current_term_snapshot(),
            "become_precandidate"
        );
    }

    /// Transition to `Candidate`. Bumps term to `new_term`, votes for self.
    ///
    /// Caller is responsible for fanning out `RequestVote` to peers.
    #[tracing::instrument(level = "info", skip_all, fields(replica_l_id = self.id, new_term = new_term))]
    pub fn become_candidate(&self, new_term: PxTerm) {
        let lease = Duration::from_millis(self.lease_duration_ms());
        let now = Instant::now();
        self.with_election_state(|s| {
            s.current_term = new_term;
            s.voted_for = Some(self.id);
            s.role = PxLocalReplicaRole::Candidate;
            // Extend vote lockout on self-vote, consistent with
            // handle_request_vote which extends it on external grants.
            // Without this a leader that just won could immediately
            // grant PreVote/RequestVote to a challenger before it has
            // sent its first heartbeat round.
            s.vote_lockout_until = now + lease;
        });
        self.role_atomic
            .store(PxLocalReplicaRole::Candidate.as_u8(), Ordering::Release);
        // One election attempt initiated.
        self.election_metrics.record_election();
        if let Some(h) = self.election_handles.get() {
            h.elections.inc();
        }
        info!(
            replica_l_id = self.id,
            current_term = new_term,
            "become_candidate"
        );
    }

    /// Transition to `Leader`. Initializes lease state as already-expired so
    /// the first heartbeat round must extend it before the read fast-path
    /// becomes available.
    #[tracing::instrument(level = "info", skip_all, fields(replica_l_id = self.id))]
    pub fn become_leader(&self) {
        self.with_election_state(|s| {
            s.role = PxLocalReplicaRole::Leader;
            s.leader_id = Some(self.id);
        });
        self.role_atomic
            .store(PxLocalReplicaRole::Leader.as_u8(), Ordering::Release);
        self.reset_lease_to(Instant::now());
        info!(
            replica_l_id = self.id,
            current_term = self.current_term_snapshot(),
            "become_leader"
        );
    }

    pub async fn persist_current_vote(&self) {
        if let Some(wal) = &self.wal {
            let term = self.current_term_snapshot();
            let voted_for = self.voted_for().unwrap_or(self.id);
            let record = WALRecord::from_vote_granted(wal.group_id(), term, voted_for);
            if let Err(e) = wal.append(&record).await {
                tracing::error!(term, voted_for, error = %e, "WAL persist VoteGranted failed");
            }
        }
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
            .downcast_ref::<crate::kv::CrowtreeEngine>()
            .map(|e| CrowtreeStatsView::from(e.stats()));
        let role = match self.role() {
            PxLocalReplicaRole::Leader => "leader",
            PxLocalReplicaRole::Follower => "follower",
            PxLocalReplicaRole::PreCandidate => "pre_candidate",
            PxLocalReplicaRole::Candidate => "candidate",
        };
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
        }
    }

    /// Phase-1 `Prepare` handler with election-term fence.
    ///
    /// Two-fence rule (term fencing + acceptor ballot fencing):
    /// - `req.term < current_term` → `PxPrepareReply::TermStale { new_term }`.
    /// - `req.term > current_term` → adopt via [`Self::become_follower`], then
    ///   forward to the acceptor (this replica is now in the new term).
    /// - `req.term == current_term` → forward to the acceptor unchanged.
    pub async fn on_prepare(&self, slot: u64, ballot: PxBallot, term: PxTerm) -> PxPrepareReply {
        let local_term = self.current_term_snapshot();
        if term < local_term {
            return PxPrepareReply::TermStale {
                slot,
                new_term: local_term,
            };
        }
        if term > local_term {
            self.become_follower(term);
        }
        let reply = self.acceptor.prepare(slot, ballot).await;

        // Ack contract (W6): persist Promised record before replying.
        if matches!(reply, PxPrepareReply::Promised { .. }) {
            if let Some(wal) = &self.wal {
                let record = WALRecord::from_promised(wal.group_id(), term, slot, ballot);
                if let Err(e) = wal.append(&record).await {
                    tracing::error!(slot, ?ballot, error = %e, "WAL persist Promised failed");
                }
            }
        }

        reply
    }

    /// Phase-2 `Accept` handler with election-term fence.
    ///
    /// Same two-fence rule as [`Self::on_prepare`] but the term lives on
    /// `entry.term` (because the accept message carries the value).
    pub async fn on_accept(&self, entry: PxLogEntry) -> PxAcceptReply {
        let req_term = entry.term;
        let local_term = self.current_term_snapshot();
        if req_term < local_term {
            return PxAcceptReply::TermStale {
                slot: entry.slot,
                new_term: local_term,
            };
        }
        if req_term > local_term {
            self.become_follower(req_term);
        }

        // Keep a reference for the WAL persist below.
        let slot = entry.slot;
        let ballot = entry.ballot;
        let reply = self.acceptor.accept(entry.clone()).await;

        // Ack contract (W6): persist Accepted record before replying.
        if matches!(reply, PxAcceptReply::Accepted { .. }) {
            debug!(
                replica_l_id = self.id,
                slot,
                round = ballot.round,
                leader_id = ballot.leader_id,
                term = entry.term,
                "on_accept: accepted leader proposal"
            );
            if let Some(wal) = &self.wal {
                let record = WALRecord::from_accepted(wal.group_id(), &entry);
                if let Err(e) = wal.append(&record).await {
                    tracing::error!(slot, ?ballot, error = %e, "WAL persist Accepted failed");
                }
            }
        }

        reply
    }

    /// Learn a chosen entry (apply to state machine).
    pub async fn learn_chosen(&self, entry: &PxLogEntry, client_id: Option<u64>, seq: Option<u64>) {
        self.learner.learn(entry.clone(), client_id, seq).await;
    }

    /// Apply locally-accepted entries to the state machine up to `commit_slot`
    /// (the leader's committed/chosen frontier, carried on each heartbeat).
    ///
    /// Walks the contiguous-applied prefix forward: for each next slot it reads
    /// the highest-ballot value the acceptor holds and `learn`s it. Stops at
    /// the first gap (a slot this replica has not accepted yet) — the prefix is
    /// contiguous by construction, and the leader's heartbeat catch-up
    /// re-sends the missing `Accepted` so the next heartbeat can continue.
    /// Idempotent: re-applying an already-applied slot is a no-op in the
    /// learner. Used by followers, which otherwise never apply in steady state
    /// (`on_accept` only persists; `ChosenNotice` only moves the watermark).
    async fn apply_committed_up_to(&self, commit_slot: SlotIndex) {
        let mut next = self.learner.contiguous_applied().saturating_add(1);
        while next <= commit_slot {
            let Some(entry) = self.acceptor.accepted_at(next) else {
                break;
            };
            self.learner.learn(entry, None, None).await;
            next += 1;
        }
    }

    /// Receive a peer-side `ChosenNotice` for `(slot, term)`.
    ///
    /// Advances the `(last_chosen_slot, last_chosen_term)` high-water mark only
    /// (never the contiguous-chosen / contiguous-applied watermarks, since a
    /// `ChosenNotice` carries no payload to apply). The high-water mark is the
    /// follower's signal that committed slots exist past its applied frontier,
    /// which drives `repair_once` and heartbeat catch-up to fetch the real
    /// values via [`Self::apply_committed_up_to`].
    ///
    /// W7: this used to be neutered (always `false`) because the election
    /// log-up-to-date check read `last_chosen_slot`, so advancing it from a
    /// payload-less notice could let a value-missing replica win leadership
    /// (the missing-key / resurrection bug). The check
    /// now compares the **durable acceptor log tip** instead
    /// ([`Self::candidate_log_up_to_date`] → [`Self::accepted_log_tip`]), so the
    /// notice no longer influences electability and the advance is safe again.
    ///
    /// Returns `true` if the high-water mark advanced.
    pub fn note_chosen(&self, slot: SlotIndex, term: PxTerm) -> bool {
        let advanced = self.learner.note_chosen(slot, term);
        trace!(
            replica_l_id = self.id,
            slot,
            term,
            advanced,
            "note_chosen: advanced chosen high-water mark"
        );
        advanced
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
    // Learner watermarks and acceptor cursor accessors.

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

    /// Highest slot ever opened on this replica's acceptor.
    #[must_use]
    pub fn highest_seen_slot(&self) -> SlotIndex {
        self.acceptor.highest_seen_slot()
    }

    #[must_use]
    pub fn accepted_log_tip(&self) -> (SlotIndex, PxTerm) {
        self.acceptor.accepted_log_tip().unwrap_or((0, 0))
    }

    // ---------------- Election handler internals ----------------

    /// Compute the responder's frontier triple (used by `PreVote` /
    /// `RequestVote` / `Heartbeat` replies).
    fn frontier_triple(&self) -> (SlotIndex, PxTerm, SlotIndex) {
        (
            self.contiguous_chosen(),
            self.last_chosen_term(),
            self.highest_seen_slot(),
        )
    }

    /// Candidate's durable acceptor log is at least as up-to-date as ours iff
    /// `(accepted_log_tip_term, accepted_log_tip_slot)` is lexicographically
    /// `>=` ours.
    fn candidate_log_up_to_date(&self, req: &VoteRequestPayload) -> bool {
        let (my_slot, my_term) = self.accepted_log_tip();
        (req.accepted_log_tip_term, req.accepted_log_tip_slot) >= (my_term, my_slot)
    }

    /// `PreVote` decision (no state mutation). Reply iff:
    /// - `req.term > current_term` (we'd vote in that term), AND
    /// - candidate's log is up-to-date, AND
    /// - `now >= vote_lockout_until`.
    fn handle_pre_vote(&self, req: VoteRequestPayload) -> VoteReply {
        let now = Instant::now();
        let state = self.election_state.lock();
        let granted = req.term > state.current_term
            && self.candidate_log_up_to_date(&req)
            && now >= state.vote_lockout_until;
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
    async fn handle_request_vote(&self, req: VoteRequestPayload) -> VoteReply {
        // Vote lockout extension reuses the configured lease duration so
        // followers honor the per-group profile (e.g. WAN) rather than the
        // hard-coded default.
        let lease = Duration::from_millis(self.lease_duration_ms());
        let now = Instant::now();
        let log_up_to_date = self.candidate_log_up_to_date(&req);

        let (granted, term) = {
            let mut state = self.election_state.lock();
            let lockout_ok = now >= state.vote_lockout_until;
            let term_ok = req.term > state.current_term
                || (req.term == state.current_term
                    && state.voted_for.map_or(true, |v| v == req.candidate_id));
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
            (granted, state.current_term)
        };
        // Mirror the atomic snapshots if we mutated.
        if granted {
            self.current_term_atomic.store(term, Ordering::Release);
            self.role_atomic
                .store(PxLocalReplicaRole::Follower.as_u8(), Ordering::Release);
            self.persist_current_vote().await;
            // After granting a vote, reset our own election deadline so we
            // give the candidate a chance to win quorum and start sending
            // heartbeats before we time out and challenge it ourselves.
            self.deadline_reset_signal.notify_one();
        }
        let (contiguous_chosen, last_chosen_term, highest_seen_slot) = self.frontier_triple();
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
    async fn handle_heartbeat(&self, req: HeartbeatRequestPayload) -> HeartbeatReply {
        let lease = Duration::from_millis(self.lease_duration_ms());
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
            self.role_atomic
                .store(PxLocalReplicaRole::Follower.as_u8(), Ordering::Release);
            // Apply committed entries up to the leader's commit point (Raft
            // `leaderCommit` rule). Followers do not apply on `on_accept`
            // (which only persists) nor on `ChosenNotice` (watermark only), so
            // without this a follower's local KVEngine never reflects committed
            // writes in steady state.
            self.apply_committed_up_to(req.committed_safe_slot).await;
            // Timestamp the heartbeat so the metrics snapshot can
            // report `last_heartbeat_age_ms`.
            self.note_heartbeat_received();
            // Reset the follower's election deadline (Raft heartbeat-resets-timer
            // rule). Without this signal, a follower receiving steady heartbeats
            // from the current leader will still spuriously fire its election
            // deadline and challenge the leader at a higher term, causing
            // leadership churn.
            self.deadline_reset_signal.notify_one();
        }
        let (contiguous_chosen, last_chosen_term, highest_seen_slot) = self.frontier_triple();
        let contiguous_applied = self.contiguous_applied();
        // This replica's own durable-snapshot watermark (`WalEngine::snapshot_slot`,
        // set by `group_maintenance::run_pass` whenever `KVEngine::persist_snapshot`
        // advances) -- `0` when there is no WAL (e.g. testkit setups), matching the
        // always-safe default `KVEngine::persist_snapshot` itself documents. The
        // leader aggregates this across every voting peer's heartbeat reply into
        // `PxGroup::group_snapshot_slot`.
        let durable_snapshot_slot = self.wal().map_or(0, |w| w.snapshot_slot());
        trace!(
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
            durable_snapshot_slot,
        }
    }

    /// `StepDown` handler. Strict-fence policy:
    /// accept iff `self.is_leader && self.id == req.target_leader_id &&
    /// req.term == current_term`. On accept the replica becomes a follower in
    /// the same term; the election driver picks up the role change
    /// on its next tick and runs the full step-down sequence (cancel bulk
    /// Phase 1, stop heartbeats, drain proposals).
    pub fn handle_step_down(&self, req: &StepDownRequestPayload) -> StepDownReply {
        let snapshot = self.election_state_snapshot();
        let accepted = snapshot.role == PxLocalReplicaRole::Leader
            && self.id == req.target_leader_id
            && req.term == snapshot.current_term;
        if accepted {
            info!(
                replica_l_id = self.id,
                req_term = req.term,
                reason = %req.reason,
                "on_step_down accepted (strict fence)"
            );
            // Stay in the same term; only the role flips. The election
            // driver waits on `admin_step_down_signal` and runs the
            // canonical step-down sequence (cancel per-tenure
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
