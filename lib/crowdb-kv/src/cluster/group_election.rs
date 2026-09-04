// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#![allow(clippy::cast_possible_truncation)]

//! Leader-election surface of [`PxGroup`].
//!
//! This module defines the [`LeaderElection`] trait and implements it for
//! [`PxGroup`]. All election lifecycle methods (driver spawn, per-tenure
//! cancel, proposing-term stamp, bulk Phase-1 repair sweep) live here so
//! the group module proper can stay focused on Paxos proposer logic and
//! membership bookkeeping.
//!
//! The trait is intentionally not object-safe (uses `self: &Arc<Self>` and
//! `async fn`); it exists to group the election API into a single
//! discoverable surface and to encourage other call sites to depend on
//! the trait rather than directly on `PxGroup`.

use std::sync::atomic::Ordering;
use std::sync::{Arc, Weak};
use std::time::Duration;

use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, trace, warn, Instrument};

use crate::cluster::group::{AcceptAttempt, PrepareAttempt, PxGroup, RemoteReplicaKind};
use crate::cluster::local_replica::PxLocalReplicaRole;
use crate::common::config::PxElectionConfig;
use crate::paxos::roles::SlotIndex;
use crate::paxos::PxNodeId;

/// Bundle handed off from `run_candidate_election` to `run_leader_state`
/// when a candidate wins quorum. Carries the floor / ceiling needed by
/// [`LeaderElection::run_bulk_phase1`] under the new tenure's cancel
/// token.
#[derive(Clone, Copy, Debug)]
pub struct PendingLeaderHandoff {
    pub term: u64,
    pub peer_floor: u64,
    pub peer_ceiling: u64,
}

/// Leader-election surface of a Paxos group.
///
/// Implementors expose:
/// - configuration & tenure-cancellation accessors,
/// - the proposing-term stamp consulted by the propose leadership gate,
/// - the per-group election driver lifecycle,
/// - the bulk Phase-1 repair sweep run on tenure entry.
pub trait LeaderElection {
    /// Snapshot of the active election configuration.
    fn election_config(&self) -> PxElectionConfig;

    /// Override the election driver configuration before
    /// [`Self::start_election_loop`] is called.
    fn set_election_config(&mut self, cfg: PxElectionConfig);

    /// Borrow of the group's per-leadership-tenure cancellation token.
    fn tenure_cancel(&self) -> CancellationToken;

    /// Stamp the term under which this group accepts proposals. Called
    /// by the election driver on `become_leader`.
    fn stamp_proposing_term(&self, term: u64);

    /// Current value of [`Self::stamp_proposing_term`].
    fn proposing_term(&self) -> u64;

    /// Node IDs of every voting real remote replica. Used by the election
    /// driver to fan out `RequestVote` / `Heartbeat`.
    fn voting_remote_ids(&self) -> Vec<PxNodeId>;

    /// Spawn the per-group election driver task.
    ///
    /// Must be called after the group is wrapped in an [`Arc`] so the
    /// driver can hold a [`Weak`] back-reference. No-op when
    /// `config.election.election_driver_disabled` is set or when the driver
    /// has already been started.
    fn start_election_loop(self: &Arc<Self>) -> impl std::future::Future<Output = ()> + Send;

    /// Bulk Phase 1: a new leader's open-prefix repair sweep over
    /// `[floor + 1, ceiling]`.
    ///
    /// Inputs:
    ///
    /// - `floor` = `max(local.contiguous_chosen, peer_contiguous_chosen_max)`
    ///   — values from peer `RequestVote` / `PreVote` replies (the election
    ///   driver supplies the aggregate via `peer_contiguous_chosen_max`).
    /// - `ceiling` = `max(local.acceptor.highest_seen_slot,
    ///                   self.next_slot - 1,
    ///                   peer_highest_seen_slot_max)`.
    ///
    /// For each slot in `[floor + 1, ceiling]` (batched by
    /// `cfg.bulk_prepare_window`):
    ///
    /// 1. Run Phase-1 `Prepare` at ballot `(0, me)` under term `T`.
    /// 2. If `PrepareAttempt::Proceed` adopted a previously-Accepted value,
    ///    re-Accept that value. Otherwise emit a `NoOp` entry so the slot
    ///    is decided (and the contiguous-chosen watermark can advance).
    /// 3. Re-Accept via the existing `run_accept_phase`.
    ///
    /// After issuing (not waiting on completion of) the batch, `next_slot`
    /// is bumped to `ceiling + 1` so steady-state proposals continue past
    /// the repaired range (§4.4).
    ///
    /// Cancellation: the `cancel` token is checked before each slot. On
    /// cancel the loop aborts without re-Accepting any further slots — the
    /// next leader will redo the sweep (§8 "Cancel any in-flight bulk
    /// Phase-1 repair").
    fn run_bulk_phase1(
        &self,
        term: u64,
        peer_contiguous_chosen_max: u64,
        peer_highest_seen_slot_max: u64,
        cfg: PxElectionConfig,
        cancel: CancellationToken,
    ) -> impl std::future::Future<Output = ()> + Send;
}

impl LeaderElection for PxGroup {
    fn election_config(&self) -> PxElectionConfig {
        self.config.election
    }

    fn set_election_config(&mut self, cfg: PxElectionConfig) {
        self.config.election = cfg;
        // Mirror onto the local replica so vote/heartbeat handlers extend
        // `vote_lockout_until` with the configured profile rather than the
        // hard-coded default.
        self.local_replica().set_lease_duration_ms(cfg.lease_duration_ms);
    }

    fn tenure_cancel(&self) -> CancellationToken {
        self.tenure_cancel.clone()
    }

    fn stamp_proposing_term(&self, term: u64) {
        self.proposing_term.store(term, Ordering::Release);
    }

    fn proposing_term(&self) -> u64 {
        self.proposing_term.load(Ordering::Acquire)
    }

    fn voting_remote_ids(&self) -> Vec<PxNodeId> {
        self.remote_replicas
            .iter()
            .filter_map(|r| match r {
                RemoteReplicaKind::Real(remote) if remote.voting => Some(remote.node_id),
                _ => None,
            })
            .collect()
    }

    async fn start_election_loop(self: &Arc<Self>) {
        if self.config.election.election_driver_disabled {
            debug!(
                g = self.group_id,
                replica = self.local_replica().id,
                "election driver disabled by config; not spawning"
            );
            return;
        }
        let mut driver_guard = self.driver_handle.lock().await;
        if driver_guard.is_some() {
            debug!(
                g = self.group_id,
                replica = self.local_replica().id,
                "election driver already running; not spawning again"
            );
            return;
        }
        let weak: Weak<Self> = Arc::downgrade(self);
        let handle = spawn(weak, self.config.election, self.tenure_cancel.clone());
        *driver_guard = Some(handle);
    }

    #[tracing::instrument(level = "info", skip_all, fields(s = self.log_store_id().unwrap_or(0), g = self.group_id, replica = self.local_replica().id, term))]
    async fn run_bulk_phase1(
        &self,
        term: u64,
        peer_contiguous_chosen_max: u64,
        peer_highest_seen_slot_max: u64,
        cfg: PxElectionConfig,
        cancel: CancellationToken,
    ) {
        let replica = self.local_replica();
        let group_id = self.group_id;
        // Voting-only quorum -- see the matching fix/comment on
        // `PxGroup::propose` in `group.rs`.
        let quorum = self.quorum();

        // Floor is the leader's OWN committed frontier — deliberately NOT maxed
        // with `peer_contiguous_chosen_max`. After a restart a replica can win
        // election while missing committed slots it never received (the value
        // lived only as a `ChosenNotice` watermark, which carries no payload).
        // Maxing the floor with the peers' commit point would SKIP exactly
        // those slots, leaving the new leader serving a stale/old value for a
        // committed key (e.g. a resurrected delete, or a missing put). Sweeping
        // from the leader's own frontier forces Phase 1 to re-derive every
        // higher slot from the quorum and adopt the real chosen value. This is
        // safe because Phase 1 contacts a quorum: a committed value is held by
        // at least one quorum member, so it is recovered rather than
        // overwritten.
        let _ = peer_contiguous_chosen_max;
        let floor = replica.contiguous_chosen();
        let local_ceiling = replica.highest_seen_slot();
        let next_slot_minus_one = self.next_slot.load(Ordering::Acquire).saturating_sub(1);
        let ceiling = local_ceiling
            .max(next_slot_minus_one)
            .max(peer_highest_seen_slot_max);

        debug!(
            group_id,
            term,
            floor,
            ceiling,
            local_contiguous_chosen = replica.contiguous_chosen(),
            local_highest_seen_slot = local_ceiling,
            peer_contiguous_chosen_max,
            peer_highest_seen_slot_max,
            "bulk phase 1 start"
        );

        if ceiling <= floor {
            self.leader_read_ready.store(true, Ordering::Release);
            debug!(term, "bulk phase 1 skipped (empty range)");
            return;
        }

        let mut slots_repaired = 0u64;
        let window = cfg.bulk_prepare_window.max(1);

        for slot in (floor + 1)..=ceiling {
            if cancel.is_cancelled() {
                warn!(
                    group_id,
                    term, slot, slots_repaired, "bulk phase 1 cancelled (step down)"
                );
                return;
            }
            if slots_repaired >= window {
                tokio::task::yield_now().await;
                slots_repaired = 0;
            }

            // Issue Phase-1 Prepare at ballot (0, me) under term T with an
            // empty payload so any adopted foreign value comes strictly
            // from a remote's previously-Accepted entry; if none exist
            // the entry stays a NoOp (we re-tag below).
            let attempt = self
                .run_prepare_phase(replica, slot, bytes::Bytes::new(), quorum, 0)
                .await;
            let mut entry = match attempt {
                PrepareAttempt::Proceed { entry, .. } => entry,
                PrepareAttempt::Retry { error, .. } | PrepareAttempt::Fail { error } => {
                    warn!(
                        group_id,
                        term,
                        slot,
                        error = error.keyword(),
                        "bulk phase 1 prepare failed; will be retried by next leader"
                    );
                    continue;
                }
            };

            entry.term = term;

            match self.run_accept_phase(replica, &entry, &[], quorum).await {
                AcceptAttempt::Chosen => {
                    replica.learn_chosen(&entry, &[]).await;
                    self.fan_out_chosen_notice(&entry, group_id);
                    slots_repaired += 1;
                }
                AcceptAttempt::Retry { error, .. } | AcceptAttempt::Fail { error } => {
                    warn!(
                        group_id,
                        term,
                        slot,
                        error = error.keyword(),
                        "bulk phase 1 accept failed; will be retried by next leader"
                    );
                }
            }
        }

        let next = ceiling.saturating_add(1);
        self.next_slot.fetch_max(next, Ordering::AcqRel);
        self.leader_read_ready.store(true, Ordering::Release);
        // R65: advance `known_commit_slot` to the leader's own
        // `contiguous_chosen` after the sweep. Leaders don't receive
        // heartbeats or ChosenNotice, so without this the apply loop
        // would never learn about slots the leader accepted as a follower
        // (before winning the election) or slots resolved by the sweep.
        // All slots up to `contiguous_chosen` are now confirmed chosen by
        // the quorum that responded to Prepare — safe to apply.
        let cc = replica.contiguous_chosen();
        replica.advance_known_commit_slot(cc);
        replica.wake_apply_loop();
        debug!(
            group_id,
            term,
            ceiling,
            next_slot = next,
            contiguous_chosen = cc,
            "bulk phase 1 done"
        );
    }
}

// ── Driver-state primitives moved from `cluster::election` ───────────
//
// These were previously free functions taking `&Arc<PxGroup>` /
// `&PxGroup`. They are inherent `pub(crate) async fn` so the driver
// loop in `cluster::election` can call them as `group.method(...)`,
// keeping all leadership-state mutations behind `PxGroup`.

/// Reason a leader is stepping down. Used for logs and metrics.
#[derive(Clone, Copy, Debug)]
pub(crate) enum StepDownReason {
    HigherTerm(u64),
    LeaseUnrenewable,
    Admin,
}

/// Outcome of one heartbeat-fanout round.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum HeartbeatOutcome {
    /// Round completed without observing a higher term. `quorum_acked` is
    /// `true` when a quorum (including self) acknowledged this round — the
    /// condition a `ReadIndex` read requires to confirm the local replica is
    /// still a legitimate leader before serving from local state.
    Continued { quorum_acked: bool },
    /// A peer reply carried `peer_term > leader_term`; the leader-state
    /// loop steps down to follower in the observed term.
    SteppedDown { peer_term: u64 },
}

/// Outcome of a linearizable read barrier (`linearizable_read_barrier`).
/// `Clone` so the round leader can fan the same outcome out to every
/// batched waiter queued onto a shared `ReadIndex` heartbeat round.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ReadBarrierOutcome {
    /// Read may be served from local applied state; every committed write up
    /// to `read_slot` is reflected locally.
    Ready { read_slot: SlotIndex },
    /// This replica is not the leader (or stepped down during the barrier);
    /// the caller should forward to / retry at the current leader.
    NotLeader,
    /// Leadership could not be confirmed this round (no reachable quorum);
    /// the read cannot be proven fresh and should be retried.
    NoQuorum,
}

/// Outcome of one `PreVote` fanout.
#[derive(Debug)]
pub(crate) enum PreVoteOutcome {
    /// Quorum of peers (including self) granted the pre-vote for
    /// `proposed_term`; safe to bump the term and start a real election.
    Won { proposed_term: u64 },
    /// At least one peer reply carried a strictly higher term; the
    /// driver must step down rather than start an election.
    HigherTerm(u64),
    /// Quorum could not be gathered (rejections / timeouts / errors).
    Lost,
}

impl PxGroup {
    /// Canonical step-down execution sequence.
    ///
    /// 1. Cancel the per-tenure [`CancellationToken`] — aborts in-flight
    ///    bulk Phase 1 and any future tenure-bound spawned work.
    /// 2. Stop the heartbeat ticker (handled by returning from the
    ///    driver leader-state loop).
    /// 3. Persistent state: `role = Follower`. `current_term` is only
    ///    raised here when the trigger is `HigherTerm`; `voted_for` is
    ///    preserved.
    /// 4. Reset election deadline (done by the outer driver loop on
    ///    return).
    /// 5. Expire `LeaseState` (`become_follower` already calls
    ///    `LeaseState::expired`).
    /// 6. Drain in-flight proposals via the propose leadership gate.
    pub(crate) fn step_down(&self, tenure_cancel: &CancellationToken, my_term: u64, reason: StepDownReason) {
        info!(
            g = self.group_id,
            replica = self.local_replica().id,
            my_term,
            ?reason,
            "stepping down from leader"
        );
        let handles = self.local_replica().election_registry_handles();
        match reason {
            StepDownReason::HigherTerm(_) => {
                if let Some(h) = handles {
                    h.step_downs_higher_term.inc();
                }
            }
            StepDownReason::LeaseUnrenewable => {
                if let Some(h) = handles {
                    h.step_downs_lease.inc();
                }
            }
            StepDownReason::Admin => {
                if let Some(h) = handles {
                    h.step_downs_admin.inc();
                }
            }
        }
        tenure_cancel.cancel();
        let target_term = match reason {
            StepDownReason::HigherTerm(t) => t.max(my_term),
            StepDownReason::LeaseUnrenewable | StepDownReason::Admin => my_term,
        };
        // `become_follower` clears the local replica's believed leader id
        // so observers reading `group.leader_id` after this point see
        // the "unknown" sentinel until the next heartbeat / vote round.
        self.local_replica().become_follower(target_term);
        // Watch/notify: clear the registry (drops all watcher tx
        // senders, closing client streams for clean reconnect to the
        // new leader).
        self.watch_registry.clear();
    }
}

// ── Driver task entry-point and scheduling utilities ─────────────────
//
// These were previously the contents of `cluster::election`. They are
// kept as a free `spawn` fn (called by tests and by
// `LeaderElection::start_election_loop`) plus a tiny PRNG /
// deadline-jitter helper. The async state-machine loops themselves
// (`run_election_driver`, `run_leader_state`) are inherent methods on
// [`PxGroup`] below.

/// Spawn the per-group election driver task.
///
/// Returns the spawned `JoinHandle`; the caller (currently
/// [`LeaderElection::start_election_loop`]) stores it on the group so
/// [`PxGroup::shutdown`] can `cancel` and `await` it deterministically.
///
/// `group` is held weakly inside the task so a forgotten/dropped group
/// does not leak the driver — the task exits the first time `upgrade`
/// fails.
#[must_use]
pub fn spawn(group: Weak<PxGroup>, cfg: PxElectionConfig, cancel: CancellationToken) -> JoinHandle<()> {
    let span = group.upgrade().map_or_else(tracing::Span::none, |group| {
        let g = group.group_id();
        let replica = group.local_replica().id;
        group.log_store_id().map_or_else(
            || tracing::info_span!("election_driver", g, replica),
            |s| tracing::info_span!("election_driver", s, g, replica),
        )
    });
    tokio::spawn(PxGroup::run_election_driver(group, cfg, cancel).instrument(span))
}

/// Tiny xorshift64* PRNG used to randomize the per-replica election
/// deadline. Seeded from `(group_id, replica_id, now_nanos)` so
/// concurrent tests with paused tokio time still observe distinct
/// sequences.
///
/// Avoids pulling in `rand` as a runtime dependency for a one-line need.
#[derive(Debug)]
pub struct XorShift64 {
    state: u64,
}

impl XorShift64 {
    #[must_use]
    pub fn new(seed: u64) -> Self {
        // 0 is a fixed point for xorshift; substitute a non-zero constant.
        Self {
            state: if seed == 0 { 0x9E37_79B9_7F4A_7C15 } else { seed },
        }
    }

    pub fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    /// Uniform-ish in `[lo, hi]` (inclusive). Caller guarantees `lo <= hi`.
    pub fn random_between_ms(&mut self, lo: u64, hi: u64) -> u64 {
        if lo == hi {
            return lo;
        }
        let span = hi - lo + 1;
        lo + (self.next_u64() % span)
    }
}

/// Schedule the next election deadline based on `[election_min, election_max]`.
fn next_election_deadline(now: Instant, cfg: &PxElectionConfig, rng: &mut XorShift64) -> Instant {
    let jitter_ms = rng.random_between_ms(cfg.election_min_ms, cfg.election_max_ms);
    now + Duration::from_millis(jitter_ms)
}

impl PxGroup {
    /// Top-level election driver loop. Spawned by [`spawn`]; holds the
    /// group weakly so a forgotten/dropped group does not leak the task.
    #[tracing::instrument(level = "info", name = "election_driver", skip_all)]
    pub(crate) async fn run_election_driver(
        group: Weak<PxGroup>,
        cfg: PxElectionConfig,
        cancel: CancellationToken,
    ) {
        let (store_group_id, replica_l_id) = if let Some(g) = group.upgrade() {
            (g.group_id, g.local_replica().id)
        } else {
            debug!("election driver started after group was dropped; exiting");
            return;
        };
        info!(
            election_min_ms = cfg.election_min_ms,
            election_max_ms = cfg.election_max_ms,
            heartbeat_interval_ms = cfg.heartbeat_interval_ms,
            lease_duration_ms = cfg.lease_duration_ms,
            "election driver started"
        );

        // Seed mixes group / replica identity with the wall clock so paused-time
        // tests covering multiple replicas in the same runtime still get
        // distinct deadline sequences.
        // Truncating the high bits of the wall-clock nanos is fine: the seed is
        // mixed with replica identity below and only feeds an xorshift PRNG.
        #[allow(clippy::cast_possible_truncation)]
        let seed_nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0u64, |d| d.as_nanos() as u64);
        let mut rng = XorShift64::new(
            seed_nanos.rotate_left(13)
                ^ store_group_id.wrapping_mul(0x9E37_79B9_7F4A_7C15)
                ^ replica_l_id.wrapping_mul(0xBF58_476D_1CE4_E5B9),
        );

        let mut election_deadline = next_election_deadline(Instant::now(), &cfg, &mut rng);

        loop {
            if cancel.is_cancelled() {
                info!("election driver cancelled");
                return;
            }
            let Some(g) = group.upgrade() else {
                debug!("election driver: group dropped; exiting");
                return;
            };
            let role = g.local_replica().role();

            match role {
                PxLocalReplicaRole::Leader => {
                    // 9.5: ticking heartbeats + lease renewal until step-down
                    // or cancel. On step-down the replica is back to Follower
                    // with a fresh deadline.
                    g.run_leader_state(&cfg, &cancel).await;
                    election_deadline = next_election_deadline(Instant::now(), &cfg, &mut rng);
                }
                PxLocalReplicaRole::Follower
                | PxLocalReplicaRole::PreCandidate
                | PxLocalReplicaRole::Candidate => {
                    // Borrow the deadline-reset signal *while still holding the
                    // strong Arc* so the future stays valid for the duration of
                    // this select. We drop the upgraded `g` only after the
                    // select completes.
                    let reset_fut = g.local_replica().deadline_reset_signal.notified();
                    tokio::pin!(reset_fut);
                    tokio::select! {
                        biased;
                        () = cancel.cancelled() => {
                            info!("election driver cancelled");
                            return;
                        }
                        () = &mut reset_fut => {
                            // A heartbeat was accepted or a vote was granted
                            // to a peer; reset the election deadline (Raft rule).
                            election_deadline = next_election_deadline(Instant::now(), &cfg, &mut rng);
                            trace!("election deadline reset on heartbeat / granted vote");
                        }
                        () = tokio::time::sleep_until(election_deadline) => {
                            let role = g.local_replica().role();
                            let term = g.local_replica().current_term_snapshot();
                            debug!(?role, current_term = term, "election deadline fired");
                            match role {
                                PxLocalReplicaRole::Follower | PxLocalReplicaRole::PreCandidate | PxLocalReplicaRole::Candidate => {
                                    g.run_election_attempt(&cfg, &cancel).await;
                                }
                                PxLocalReplicaRole::Leader => { /* race: handled next iteration */ }
                            }
                            election_deadline = next_election_deadline(Instant::now(), &cfg, &mut rng);
                        }
                    }
                    // `reset_fut` (and the borrow on `g`) goes out of scope here.
                }
            }
        }
    }
}
