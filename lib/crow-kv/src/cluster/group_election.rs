// Copyright 2026-present buzzcrow <buzzcrow@126.com>
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
use std::time::Instant as StdInstant;

use tokio::task::{JoinHandle, JoinSet};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, trace, warn};

use crate::cluster::group::{AcceptAttempt, PendingReadBarrier, PrepareAttempt, PxGroup, RemoteReplicaKind};
use crate::cluster::local_replica::PxLocalReplicaRole;
use crate::cluster::replica::{
    HeartbeatReply, HeartbeatRequestPayload, PxReplicaError, ReplicaClient, VoteReply, VoteRequestPayload,
};
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
                group_id = self.group_id,
                replica_l_id = self.local_replica().id,
                "election driver disabled by config; not spawning"
            );
            return;
        }
        let mut driver_guard = self.driver_handle.lock().await;
        if driver_guard.is_some() {
            debug!(
                group_id = self.group_id,
                replica_l_id = self.local_replica().id,
                "election driver already running; not spawning again"
            );
            return;
        }
        let weak: Weak<Self> = Arc::downgrade(self);
        let handle = spawn(weak, self.config.election, self.tenure_cancel.clone());
        *driver_guard = Some(handle);
    }

    #[tracing::instrument(level = "info", skip_all, fields(group_id = self.group_id, term = term))]
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
            debug!(group_id, term, "bulk phase 1 skipped (empty range)");
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
        debug!(group_id, term, ceiling, next_slot = next, "bulk phase 1 done");
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
            group_id = self.group_id,
            replica_l_id = self.local_replica().id,
            my_term,
            ?reason,
            "stepping down from leader"
        );
        let metrics = self.local_replica().election_metrics();
        let handles = self.local_replica().election_registry_handles();
        match reason {
            StepDownReason::HigherTerm(_) => {
                metrics.record_step_down_higher_term();
                if let Some(h) = handles {
                    h.step_downs_higher_term.inc();
                }
            }
            StepDownReason::LeaseUnrenewable => {
                metrics.record_step_down_lease_unrenewable();
                if let Some(h) = handles {
                    h.step_downs_lease.inc();
                }
            }
            StepDownReason::Admin => {
                metrics.record_step_down_admin();
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
    }

    /// Promote a winning candidate to leader: stash the bulk-Phase-1
    /// handoff, flip the role, stamp the proposing term.
    pub(crate) fn finalize_leader(&self, term: u64, peer_floor: u64, peer_ceiling: u64) {
        let replica = self.local_replica();
        info!(
            group_id = self.group_id,
            replica_l_id = replica.id,
            term,
            peer_floor,
            peer_ceiling,
            "candidate won quorum; becoming leader"
        );

        // Advance next_slot BEFORE become_leader so proposals can't
        // reuse slots from the previous term.  Uses the same ceiling
        // calculation as run_bulk_phase1 (max of local ceiling,
        // current next_slot, and peer_ceiling from RequestVote
        // replies).
        let local_ceiling = replica.highest_seen_slot();
        let next_slot_minus_one = self.next_slot.load(Ordering::Acquire).saturating_sub(1);
        let ceiling = local_ceiling.max(next_slot_minus_one).max(peer_ceiling);
        let next = ceiling.saturating_add(1);
        let prev_next_slot = self.next_slot.fetch_max(next, Ordering::AcqRel);
        info!(
            group_id = self.group_id,
            replica_l_id = replica.id,
            term,
            local_ceiling,
            next_slot_minus_one,
            peer_ceiling,
            ceiling,
            next,
            prev_next_slot,
            "finalize_leader: bumping next_slot"
        );

        *self.pending_leader_handoff.lock() = Some(PendingLeaderHandoff {
            term,
            peer_floor,
            peer_ceiling,
        });
        replica.become_leader();
        self.proposing_term.store(term, Ordering::Release);
    }

    /// Fan out one heartbeat round to all voting peers.
    ///
    /// On quorum-OK: extends `lease_read_until` and bumps
    /// `last_quorum_heartbeat_at`. On any peer reply with
    /// `term > leader_term`: returns [`HeartbeatOutcome::SteppedDown`].
    #[allow(clippy::too_many_lines)]
    pub(crate) async fn run_heartbeat_round(
        self: &Arc<Self>,
        cfg: &PxElectionConfig,
        leader_term: u64,
    ) -> HeartbeatOutcome {
        let replica = self.local_replica();
        let group_id = self.group_id;
        let voting_peers = self.voting_remote_ids();
        let quorum = self.quorum();
        let t_send = StdInstant::now();
        // `t_send_ms_mono` is monotonic millis since the process-start
        // anchor shared with `common::time::process_anchor`.
        let t_send_ms_mono = crate::common::time::instant_to_anchor_ms(t_send);

        let payload = HeartbeatRequestPayload {
            term: leader_term,
            leader_id: replica.id,
            prev_log_slot: replica.last_chosen_slot(),
            prev_log_term: replica.last_chosen_term(),
            committed_safe_slot: replica.contiguous_chosen(),
            lease_grant_until_ms_mono: t_send_ms_mono.saturating_add(cfg.lease_duration_ms),
            t_send_ms_mono,
        };

        // Single-voter clusters trivially have quorum; renew lease without RPCs.
        let mut acks: usize = 1;
        if acks >= quorum {
            replica.renew_lease(t_send, cfg);
            return HeartbeatOutcome::Continued { quorum_acked: true };
        }

        let mut joinset: JoinSet<(PxNodeId, Result<HeartbeatReply, PxReplicaError>)> = JoinSet::new();
        for peer_id in voting_peers {
            let group_for_task = self.clone();
            let req = payload;
            joinset.spawn(async move {
                let result = if let Some(remote) = group_for_task.get_remote_replica(peer_id) {
                    remote.send_heartbeat(req, group_for_task.group_id).await
                } else {
                    Err(PxReplicaError::Internal(format!("peer {peer_id} not present")))
                };
                (peer_id, result)
            });
        }

        while let Some(joined) = joinset.join_next().await {
            let Ok((peer_id, reply)) = joined else { continue };
            match reply {
                Ok(hb) => {
                    if hb.term > leader_term {
                        info!(
                            group_id,
                            leader_id = replica.id,
                            my_term = leader_term,
                            peer_id,
                            peer_term = hb.term,
                            "heartbeat saw higher term"
                        );
                        joinset.abort_all();
                        return HeartbeatOutcome::SteppedDown { peer_term: hb.term };
                    }
                    if hb.success {
                        acks += 1;
                        if hb.contiguous_applied < payload.committed_safe_slot {
                            if let Some(remote) = self.get_remote_replica(peer_id) {
                                // Bound catch-up replay per heartbeat round so a
                                // lagging follower doesn't inflate ReadIndex-
                                // fallback read latency. Remaining slots converge
                                // on subsequent heartbeats; quorum confirmation
                                // does not depend on replay completion.
                                const MAX_CATCHUP_PER_ROUND: u64 = 64;
                                let catchup_end =
                                    (hb.contiguous_applied.saturating_add(MAX_CATCHUP_PER_ROUND))
                                        .min(payload.committed_safe_slot);
                                // R63: Phase 1 — fire-and-forget batch chosen
                                // notice covering the lagging range. The follower
                                // advances its chosen frontier for slots it already
                                // has (no payload transfer), so the apply loop can
                                // start processing them immediately. Missing slots
                                // remain gaps for the full-accept Phase 2 below.
                                let peer_applied = hb.contiguous_applied;
                                if catchup_end > peer_applied {
                                    if let Err(err) = remote.send_batch_chosen_notice(
                                        peer_applied.saturating_add(1),
                                        catchup_end,
                                        leader_term,
                                        replica.id,
                                        group_id,
                                    ) {
                                        debug!(
                                            group_id,
                                            peer_id,
                                            start_slot = peer_applied.saturating_add(1),
                                            end_slot = catchup_end,
                                            error = %err,
                                            "heartbeat catch-up: batch chosen notice failed (best-effort)"
                                        );
                                    }
                                }
                                // R63: Phase 2 — full accepts for missing slots
                                // (existing logic). Slots the follower already has
                                // are re-accepted (idempotent CAS, cheap); slots the
                                // follower is missing get the real value.
                                for slot in hb.contiguous_applied.saturating_add(1)..=catchup_end {
                                    let Some(mut entry) = replica.accepted_at(slot).await else {
                                        debug!(
                                            group_id,
                                            peer_id, slot, "heartbeat catch-up: local accepted entry missing"
                                        );
                                        break;
                                    };
                                    // Every slot in this range is `<= committed_safe_slot`
                                    // (= the leader's `contiguous_chosen`), i.e. ALREADY
                                    // CHOSEN on a quorum and therefore immutable (Paxos
                                    // P2c): no other value can ever be chosen here. A
                                    // lagging peer may still REJECT a replayed accept
                                    // because it promised a higher ballot during election
                                    // churn — and `ChosenNotice` only advances its
                                    // watermark without applying or persisting the value,
                                    // so without this the peer's engine/WAL never receives
                                    // the committed write (a deleted key fails to
                                    // converge, and after restart resurrects). Since the
                                    // value can no longer change, re-accept the SAME value
                                    // at a ballot just above the peer's promise so it
                                    // converges and persists a durable `Accepted` record.
                                    let mut caught_up = false;
                                    for catchup_attempt in 0..2u8 {
                                        match remote
                                            .send_accept(&entry, &[], group_id, self.membership_epoch())
                                            .await
                                        {
                                            Ok(crate::paxos::roles::PxAcceptReply::Accepted { .. }) => {
                                                caught_up = true;
                                                break;
                                            }
                                            Ok(crate::paxos::roles::PxAcceptReply::Rejected {
                                                current_promised,
                                                ..
                                            }) => {
                                                if catchup_attempt == 0 {
                                                    // Escalate above the peer's promise and
                                                    // retry once. Safe: the value is chosen.
                                                    entry.ballot = crate::paxos::roles::PxBallot::new(
                                                        current_promised.round.saturating_add(1),
                                                        replica.id,
                                                    );
                                                    continue;
                                                }
                                                debug!(
                                                    group_id,
                                                    peer_id,
                                                    slot,
                                                    rejected_round = current_promised.round,
                                                    rejected_leader_id = current_promised.leader_id,
                                                    "heartbeat catch-up: peer still rejected after ballot escalation"
                                                );
                                                break;
                                            }
                                            Ok(crate::paxos::roles::PxAcceptReply::TermStale {
                                                new_term,
                                                ..
                                            }) => {
                                                debug!(
                                                    group_id,
                                                    peer_id,
                                                    slot,
                                                    new_term,
                                                    "heartbeat catch-up: peer reported higher term during replay"
                                                );
                                                break;
                                            }
                                            Ok(crate::paxos::roles::PxAcceptReply::EpochMismatch {
                                                responder_epoch,
                                            }) => {
                                                self.adopt_membership_epoch(responder_epoch);
                                                debug!(
                                                    group_id,
                                                    peer_id,
                                                    slot,
                                                    proposer_epoch = self.membership_epoch(),
                                                    responder_epoch,
                                                    "heartbeat catch-up: peer rejected by membership-epoch fence; adopted responder epoch"
                                                );
                                                break;
                                            }
                                            Err(err) => {
                                                debug!(group_id, peer_id, slot, error = %err, "heartbeat catch-up: replay accept failed");
                                                break;
                                            }
                                        }
                                    }
                                    if !caught_up {
                                        break;
                                    }
                                }
                            }
                        }
                        // Refresh this peer's applied watermark and recompute
                        // the group safe-slot used by bounded/safe-slot reads.
                        self.note_peer_applied(peer_id, hb.contiguous_applied);
                        // Refresh this peer's durable-snapshot watermark and
                        // recompute the group's real "durable on leader +
                        // >=1 peer" snapshot-slot.
                        self.note_peer_durable(peer_id, hb.durable_snapshot_slot);
                        if acks >= quorum {
                            replica.renew_lease(t_send, cfg);
                            // Keep draining remaining replies; no further
                            // state changes happen unless a higher term shows.
                        }
                    }
                }
                Err(err) => {
                    debug!(group_id, peer_id, error = ?err, "heartbeat transport error");
                }
            }
        }

        HeartbeatOutcome::Continued {
            quorum_acked: acks >= quorum,
        }
    }

    /// Establish a linearizable read point on the leader.
    ///
    /// Returns the slot at which a local read is safe to serve (the commit
    /// index captured up-front; under V1 apply==learn it is already applied).
    /// Two paths, mirroring the leader-read fencing model:
    /// - **Lease fast path:** if the read lease is still valid the leader is
    ///   guaranteed to be the only one that could have committed anything, so
    ///   the local applied state is linearizable with no round-trip.
    /// - **`ReadIndex` fallback:** otherwise run one quorum heartbeat. A quorum
    ///   ack confirms no higher term displaced us, so every committed write is
    ///   reflected locally; a higher term steps us down; no quorum means we
    ///   cannot prove freshness and the read must be retried elsewhere.
    pub(crate) async fn linearizable_read_barrier(self: &Arc<Self>) -> ReadBarrierOutcome {
        let barrier_start = StdInstant::now();
        let replica = self.local_replica();
        if !replica.is_leader() {
            return ReadBarrierOutcome::NotLeader;
        }
        // Capture the commit index before confirmation: every slot <= this is
        // already chosen on this leader.
        let read_slot = replica.contiguous_chosen();

        if !self.leader_read_ready() {
            return ReadBarrierOutcome::NoQuorum;
        }

        let lease_valid = replica.lease_read_valid(StdInstant::now());
        if let Some(h) = self.read_handles() {
            h.lease_valid.set(u64::from(lease_valid));
        }
        if lease_valid {
            if let Some(h) = self.read_handles() {
                h.barrier.observe(barrier_start.elapsed().as_nanos() as u64);
                h.lease_path.inc();
            }
            return ReadBarrierOutcome::Ready { read_slot };
        }

        // ReadIndex path. Coalesce concurrent barriers onto a single
        // in-flight heartbeat round: the first read to arrive (the "round
        // leader") starts the round and registers a pending batch with the
        // `read_slot` it captured above; later reads that arrive while the
        // round is in flight enqueue a waiter and adopt the same outcome
        // (and the same conservative `read_slot` freshness floor). The
        // mutex serializes enqueue/dequeue so no waiter is lost — a read
        // either joins the batch (sees `Some`) or, after the leader drains,
        // starts a fresh batch (sees `None`). Correctness is identical to
        // the single-read ReadIndex path: the heartbeat quorum confirms
        // leadership at this term, and the engine get returns the latest
        // applied value (not a `read_slot`-pinned value), so batched reads
        // observe fresh state; the shared `read_slot` only under-reports
        // freshness.
        let joined_rx = {
            let mut guard = self.pending_read_barrier.lock();
            if let Some(pending) = guard.as_mut() {
                let (tx, rx) = tokio::sync::oneshot::channel();
                pending.waiters.push(tx);
                Some(rx)
            } else {
                guard.replace(PendingReadBarrier { waiters: Vec::new() });
                None
            }
        };

        // Batched onto an in-flight round: wait for the round leader to
        // resolve us. A dropped sender (round-leader cancellation) maps to
        // `NoQuorum` so the caller retries safely.
        if let Some(rx) = joined_rx {
            let outcome = rx.await.unwrap_or(ReadBarrierOutcome::NoQuorum);
            if let Some(h) = self.read_handles() {
                h.barrier.observe(barrier_start.elapsed().as_nanos() as u64);
                if matches!(outcome, ReadBarrierOutcome::Ready { .. }) {
                    h.readindex_path.inc();
                }
            }
            return outcome;
        }

        // Round leader: a test-only gate may hold the round open so
        // concurrent reads deterministically enqueue before the round
        // runs. No-op in production (`None`). The guard is dropped at the
        // `;` so the `Receiver` is awaited without a non-Send lock guard
        // held across the await.
        #[cfg(feature = "test-util")]
        {
            let gate = self.readindex_round_gate.lock().take();
            if let Some(gate) = gate {
                let _ = gate.await;
            }
        }

        let cfg = self.config.election;
        let term = replica.current_term_snapshot();
        let outcome = match self.run_heartbeat_round(&cfg, term).await {
            HeartbeatOutcome::SteppedDown { .. } => ReadBarrierOutcome::NotLeader,
            HeartbeatOutcome::Continued { quorum_acked: true } => ReadBarrierOutcome::Ready { read_slot },
            HeartbeatOutcome::Continued { quorum_acked: false } => ReadBarrierOutcome::NoQuorum,
        };

        // Drain the batch and fan the outcome out to every waiter that
        // joined this round. Taking the slot clears "in flight" so the
        // next ReadIndex read starts a fresh batch.
        let waiters = self
            .pending_read_barrier
            .lock()
            .take()
            .map_or_else(Vec::new, |p| p.waiters);
        for tx in waiters {
            let _ = tx.send(outcome.clone());
        }

        if let Some(h) = self.read_handles() {
            h.barrier.observe(barrier_start.elapsed().as_nanos() as u64);
            h.readindex_rounds.inc();
            if matches!(outcome, ReadBarrierOutcome::Ready { .. }) {
                h.readindex_path.inc();
            }
        }
        outcome
    }

    /// Drive one full election attempt: optional `PreVote` →
    /// `Candidate` `RequestVote` → `Leader` on win, otherwise stay
    /// `Follower` / `Candidate` until the next deadline.
    pub(crate) async fn run_election_attempt(
        self: &Arc<Self>,
        cfg: &PxElectionConfig,
        cancel: &CancellationToken,
    ) {
        let replica = self.local_replica();

        if cfg.prevote_enabled {
            replica.become_precandidate();
            match self.run_prevote_round(cancel).await {
                PreVoteOutcome::Won { proposed_term } => {
                    replica.become_candidate(proposed_term);
                    replica.persist_current_vote().await;
                    self.run_candidate_election(cancel).await;
                }
                PreVoteOutcome::HigherTerm(t) => {
                    replica.become_follower(t);
                }
                PreVoteOutcome::Lost => {
                    // Step back to Follower in the same term; next
                    // deadline will retry (with a fresh randomized jitter).
                    replica.become_follower(replica.current_term_snapshot());
                }
            }
        } else {
            let new_term = replica.current_term_snapshot() + 1;
            replica.become_candidate(new_term);
            replica.persist_current_vote().await;
            self.run_candidate_election(cancel).await;
        }
    }

    /// Run a single `PreVote` round.
    ///
    /// Fans out `PreVote(proposed_term)` without bumping `current_term`.
    pub(crate) async fn run_prevote_round(self: &Arc<Self>, cancel: &CancellationToken) -> PreVoteOutcome {
        let group_id = self.group_id;
        let replica = self.local_replica();
        let term = replica.current_term_snapshot();
        let proposed_term = term + 1;
        let candidate_id: PxNodeId = replica.id;
        let (accepted_log_tip_slot, accepted_log_tip_term) = replica.accepted_log_tip();

        let payload = VoteRequestPayload {
            term: proposed_term,
            candidate_id,
            accepted_log_tip_slot,
            accepted_log_tip_term,
        };

        let voting_peers = self.voting_remote_ids();
        let quorum = self.quorum();
        // Self-vote only counts if the local replica is itself a voting
        // member -- `quorum` is sized from voting members only, and a
        // non-voting replica has no vote to cast (mirrors the
        // remote-side `voting_remote_ids` filter just above).
        let mut grants: usize = usize::from(replica.voting);

        debug!(
            group_id,
            candidate_id,
            my_term = term,
            proposed_term,
            peer_count = voting_peers.len(),
            quorum,
            "precandidate fanning out PreVote"
        );

        // Trivial-cluster fast path: only win without contacting peers when the
        // group is truly a singleton (no configured voting peers) *and* the
        // local replica is itself voting -- a non-voting singleton has no
        // vote to cast and cannot self-elect.
        if voting_peers.is_empty() && replica.voting {
            return PreVoteOutcome::Won { proposed_term };
        }

        let mut joinset: JoinSet<(PxNodeId, Result<VoteReply, PxReplicaError>)> = JoinSet::new();
        for peer_id in voting_peers {
            let group_for_task = self.clone();
            let req = payload;
            joinset.spawn(async move {
                let result = if let Some(remote) = group_for_task.get_remote_replica(peer_id) {
                    remote.send_pre_vote(req, group_for_task.group_id).await
                } else {
                    Err(PxReplicaError::Internal(format!("peer {peer_id} not present")))
                };
                (peer_id, result)
            });
        }

        while let Some(joined) = joinset.join_next().await {
            if cancel.is_cancelled() {
                joinset.abort_all();
                return PreVoteOutcome::Lost;
            }
            let Ok((peer_id, reply)) = joined else { continue };
            match reply {
                Ok(vote) => {
                    if vote.term > proposed_term {
                        joinset.abort_all();
                        return PreVoteOutcome::HigherTerm(vote.term);
                    }
                    if vote.granted {
                        grants += 1;
                        if grants >= quorum {
                            joinset.abort_all();
                            return PreVoteOutcome::Won { proposed_term };
                        }
                    }
                }
                Err(err) => {
                    debug!(group_id, candidate_id, proposed_term, peer_id, error = ?err, "PreVote transport error");
                }
            }
        }

        info!(
            group_id,
            candidate_id, proposed_term, grants, quorum, "precandidate failed to gather pre-vote quorum"
        );
        PreVoteOutcome::Lost
    }

    /// Run a single round of `Candidate`-state vote gathering.
    ///
    /// 1. If any peer reply carries `term > my_term` →
    ///    `become_follower(term)` and return.
    /// 2. If `grants >= quorum` → [`Self::finalize_leader`] and return.
    /// 3. Otherwise → stay in `Candidate` and let the next election
    ///    deadline restart the election in `current_term + 1`.
    #[allow(clippy::too_many_lines)]
    pub(crate) async fn run_candidate_election(self: &Arc<Self>, cancel: &CancellationToken) {
        let group_id = self.group_id;
        let replica = self.local_replica();
        let term = replica.current_term_snapshot();
        let candidate_id: PxNodeId = replica.id;
        let (accepted_log_tip_slot, accepted_log_tip_term) = replica.accepted_log_tip();

        let payload = VoteRequestPayload {
            term,
            candidate_id,
            accepted_log_tip_slot,
            accepted_log_tip_term,
        };

        let voting_peers = self.voting_remote_ids();
        let quorum = self.quorum();
        // Local replica votes for itself in `become_candidate`, but only
        // if it is itself a voting member -- see the matching guard in
        // `run_prevote_round`.
        let mut grants: usize = usize::from(replica.voting);
        let mut peer_floor: u64 = replica.contiguous_chosen();
        let mut peer_ceiling: u64 = replica.highest_seen_slot();

        debug!(
            group_id,
            candidate_id,
            term,
            peer_count = voting_peers.len(),
            quorum,
            "candidate fanning out RequestVote"
        );

        // Trivial-cluster fast path: local replica alone constitutes quorum.
        // Only self-elect when there are no configured voting peers *and*
        // the local replica is itself voting (a non-voting replica has no
        // vote to cast and cannot self-elect, mirroring `run_prevote_round`).
        // A restarted node that has recovered a multi-replica persisted
        // config (W9) will have voting peers, so it must actually contact
        // them before becoming leader and running bulk Phase 1. This
        // prevents a quorum=1 self-election in the restore window from
        // overwriting committed data.
        if voting_peers.is_empty() && replica.voting {
            self.finalize_leader(term, peer_floor, peer_ceiling);
            return;
        }

        let mut joinset: JoinSet<(PxNodeId, Result<VoteReply, PxReplicaError>)> = JoinSet::new();
        for peer_id in voting_peers {
            let group_for_task = self.clone();
            let req = payload;
            joinset.spawn(async move {
                let result = if let Some(remote) = group_for_task.get_remote_replica(peer_id) {
                    remote.send_request_vote(req, group_for_task.group_id).await
                } else {
                    Err(PxReplicaError::Internal(format!("peer {peer_id} not present")))
                };
                (peer_id, result)
            });
        }

        while let Some(joined) = joinset.join_next().await {
            if cancel.is_cancelled() {
                joinset.abort_all();
                return;
            }
            let (peer_id, reply) = match joined {
                Ok(pair) => pair,
                Err(join_err) => {
                    error!(group_id, error = %join_err, "RequestVote task panicked");
                    continue;
                }
            };
            match reply {
                Ok(vote) => {
                    if vote.term > term {
                        info!(
                            group_id,
                            candidate_id,
                            my_term = term,
                            peer_id,
                            peer_term = vote.term,
                            "RequestVote observed higher term; stepping down to follower"
                        );
                        replica.become_follower(vote.term);
                        joinset.abort_all();
                        return;
                    }
                    if vote.granted {
                        grants += 1;
                        peer_floor = peer_floor.max(vote.contiguous_chosen);
                        peer_ceiling = peer_ceiling.max(vote.highest_seen_slot);
                        debug!(
                            group_id,
                            candidate_id, term, peer_id, grants, quorum, "RequestVote granted"
                        );
                        if grants >= quorum {
                            joinset.abort_all();
                            self.finalize_leader(term, peer_floor, peer_ceiling);
                            return;
                        }
                    } else {
                        debug!(
                            group_id,
                            candidate_id,
                            term,
                            peer_id,
                            peer_term = vote.term,
                            "RequestVote rejected"
                        );
                    }
                }
                Err(err) => {
                    debug!(
                        group_id,
                        candidate_id,
                        term,
                        peer_id,
                        error = ?err,
                        "RequestVote transport error"
                    );
                }
            }
        }

        info!(
            group_id,
            candidate_id,
            term,
            grants,
            quorum,
            "candidate failed to gather quorum; will retry on next deadline"
        );
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
    tokio::spawn(PxGroup::run_election_driver(group, cfg, cancel))
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
            group_id = store_group_id,
            replica_l_id,
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
                info!(
                    group_id = store_group_id,
                    replica_l_id, "election driver cancelled"
                );
                return;
            }
            let Some(g) = group.upgrade() else {
                debug!(
                    group_id = store_group_id,
                    replica_l_id, "election driver: group dropped; exiting"
                );
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
                            info!(group_id = store_group_id, replica_l_id, "election driver cancelled");
                            return;
                        }
                        () = &mut reset_fut => {
                            // A heartbeat was accepted or a vote was granted
                            // to a peer; reset the election deadline (Raft rule).
                            election_deadline = next_election_deadline(Instant::now(), &cfg, &mut rng);
                            trace!(group_id = store_group_id, replica_l_id, "election deadline reset on heartbeat / granted vote");
                        }
                        () = tokio::time::sleep_until(election_deadline) => {
                            let role = g.local_replica().role();
                            let term = g.local_replica().current_term_snapshot();
                            debug!(group_id = store_group_id, replica_l_id, ?role, current_term = term, "election deadline fired");
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

    /// Leader-state inner loop: tick heartbeats + lease bookkeeping
    /// until step-down (higher term, lease unrenewable, admin) or
    /// driver cancel.
    ///
    /// Handles heartbeat ticker, lease renewal, per-tenure cancel token
    /// for bulk Phase 1, lease-unrenewable step-down trigger, and the
    /// canonical step-down execution sequence.
    pub(crate) async fn run_leader_state(
        self: &Arc<Self>,
        cfg: &PxElectionConfig,
        cancel: &CancellationToken,
    ) {
        let replica = self.local_replica();
        let group_id = self.group_id;
        let leader_id: PxNodeId = replica.id;
        let leader_term = replica.current_term_snapshot();

        // If we entered leader state while already in the Leader role (e.g., a
        // single-replica group created via the management API, or a leader
        // restored from replay), the proposing_term may not have been stamped.
        // Stamp it now so the proposal leadership gate opens.
        if replica.is_leader() {
            self.stamp_proposing_term(leader_term);
        }

        // Reset lease state at the start of the tenure. The first heartbeat
        // round that gets quorum extends the lease and unlocks read fast-path.
        replica.reset_lease_to(StdInstant::now());

        // Drop any safe-slot / per-peer watermarks inherited from a prior
        // tenure. `group_safe_slot` only advances within a tenure, so a new
        // leader must start conservative (0) and re-establish it from fresh
        // heartbeats rather than overstate freshness for bounded-stale reads.
        self.reset_safe_slot_tracking();
        // A single-replica group has no peers to repair from; the leader can
        // serve reads immediately. Multi-replica leaders must finish bulk
        // Phase 1 / the first heartbeat round before `leader_read_ready` is
        // set by those paths.
        if self.quorum() == 1 {
            self.leader_read_ready.store(true, Ordering::Release);
        } else {
            self.leader_read_ready.store(false, Ordering::Release);
        }

        // Per-leadership-tenure cancel token. Cancelled by the step-down
        // sequence; aborts in-flight bulk Phase 1 and any future
        // tenure-bound work. Always a child of the driver-lifetime
        // `cancel` so shutdown still wins.
        let tenure_cancel = CancellationToken::new();
        {
            let parent = cancel.clone();
            let child = tenure_cancel.clone();
            tokio::spawn(async move {
                parent.cancelled().await;
                child.cancel();
            });
        }

        // Consume the handoff stashed by finalize_leader and spawn bulk
        // Phase 1 on the per-tenure cancel token.
        if let Some(handoff) = self.pending_leader_handoff.lock().take() {
            let group_for_task = self.clone();
            let cfg_for_task = *cfg;
            let cancel_for_task = tenure_cancel.clone();
            tokio::spawn(async move {
                group_for_task
                    .run_bulk_phase1(
                        handoff.term,
                        handoff.peer_floor,
                        handoff.peer_ceiling,
                        cfg_for_task,
                        cancel_for_task,
                    )
                    .await;
            });
        } else {
            self.leader_read_ready.store(true, Ordering::Release);
        }

        let mut ticker = tokio::time::interval(Duration::from_millis(cfg.heartbeat_interval_ms));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        // First tick fires immediately; consume it so the loop starts clean.
        ticker.tick().await;

        info!(
            group_id,
            replica_l_id = leader_id,
            term = leader_term,
            heartbeat_interval_ms = cfg.heartbeat_interval_ms,
            lease_duration_ms = cfg.lease_duration_ms,
            "entering leader state"
        );

        loop {
            if replica.role() != PxLocalReplicaRole::Leader {
                info!(
                    group_id,
                    replica_l_id = leader_id,
                    "leader state exiting: role changed externally"
                );
                tenure_cancel.cancel();
                return;
            }
            if cancel.is_cancelled() {
                tenure_cancel.cancel();
                return;
            }
            // Lease-unrenewable check on every Leader tick.
            let last_quorum = replica.last_quorum_heartbeat_at();
            if StdInstant::now().duration_since(last_quorum) >= Duration::from_millis(cfg.lease_duration_ms) {
                self.step_down(&tenure_cancel, leader_term, StepDownReason::LeaseUnrenewable);
                return;
            }
            tokio::select! {
                biased;
                () = cancel.cancelled() => {
                    tenure_cancel.cancel();
                    return;
                }
                // Admin step-down via StepDown RPC.
                () = replica.admin_step_down_signal.notified() => {
                    self.step_down(&tenure_cancel, leader_term, StepDownReason::Admin);
                    return;
                }
                _ = ticker.tick() => {
                    match self.run_heartbeat_round(cfg, leader_term).await {
                        HeartbeatOutcome::Continued { .. } => {
                            // Opportunistic background repair: close the lowest
                            // gap in the open prefix so the contiguous frontier
                            // (and group safe-slot) can advance past abandoned
                            // slots. A no-gap leader returns immediately without
                            // any RPCs, so steady state pays nothing.
                            let _ = self.repair_once().await;
                        }
                        HeartbeatOutcome::SteppedDown { peer_term } => {
                            self.step_down(&tenure_cancel, leader_term, StepDownReason::HigherTerm(peer_term));
                            return;
                        }
                    }
                }
            }
        }
    }
}
