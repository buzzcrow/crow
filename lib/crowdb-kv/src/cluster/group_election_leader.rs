// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#![allow(clippy::cast_possible_truncation)]

//! Leader-state methods of [`PxGroup`], split out from
//! [`crate::cluster::group_election`]: leader finalization, the
//! heartbeat fanout round, the linearizable read barrier, and the
//! leader-state inner loop.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant as StdInstant;

use tokio_util::sync::CancellationToken;
use tracing::{debug, info};

use crate::cluster::group::{PendingReadBarrier, PxGroup};
use crate::cluster::group_election::{
    HeartbeatOutcome, LeaderElection, PendingLeaderHandoff, ReadBarrierOutcome, StepDownReason,
};
use crate::cluster::local_replica::PxLocalReplicaRole;
use crate::cluster::replica::{HeartbeatReply, HeartbeatRequestPayload, PxReplicaError, ReplicaClient};
use crate::common::config::PxElectionConfig;
use crate::paxos::PxNodeId;

impl PxGroup {
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

        let mut joinset: tokio::task::JoinSet<(PxNodeId, Result<HeartbeatReply, PxReplicaError>)> =
            tokio::task::JoinSet::new();
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
                        // R65: catch-up is now follower-driven (FetchGap).
                        // The heartbeat round is pure liveness + lease: send
                        // heartbeats, collect replies, check higher term,
                        // renew lease, note peer applied. No catch-up.
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
